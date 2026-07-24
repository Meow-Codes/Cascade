#pragma once
// Cascade :: core::media :: AdaptiveSender
//
// Ties BitrateController's chosen quality to an actual send-rate budget
// and drains a two-level PriorityPacketQueue against it: High-priority
// (control/NACK) traffic is exempt from the budget entirely; Normal
// (audio frames) is what gets throttled/dropped under constraint. That
// exemption is what makes "prioritization" meaningful -- throttling both
// queues equally would just be one queue with extra steps.
//
// Uses a small local DeterministicTokenBucket rather than reusing Phase
// 2's net::RateLimiter, even though it's the same algorithm: RateLimiter
// reads std::chrono::steady_clock internally, which would break this
// phase's now_ms-driven determinism (NetworkConditionEstimator,
// BitrateController, and JitterBuffer before it all take explicit now_ms
// specifically so tests never need real sleep()s).
//
// Dropped Normal frames are NOT requeued for the next drain() -- a stale
// audio frame past its moment is worthless, unlike Phase 3's durable log
// where nothing is ever silently discarded. This is the real-time-media
// vs. durable-log distinction this whole project's premise rests on.

#include <algorithm>
#include <cstdint>
#include <vector>

#include "media/bitrate_controller.hpp"
#include "media/priority_queue.hpp"

namespace cascade::core::media {

class DeterministicTokenBucket {
public:
    void reconfigure(double rate_bytes_per_sec, double capacity_bytes, std::uint64_t now_ms) {
        if (initialized_) {
            refill(now_ms); // settle existing tokens under the OLD rate before switching
        } else {
            tokens_ = capacity_bytes; // first configuration starts with a full burst allowance
            last_refill_ms_ = now_ms;
            initialized_ = true;
        }
        rate_bytes_per_sec_ = rate_bytes_per_sec;
        capacity_bytes_ = capacity_bytes;
        tokens_ = std::min(tokens_, capacity_bytes_);
    }

    bool try_consume(double bytes, std::uint64_t now_ms) {
        refill(now_ms);
        if (tokens_ >= bytes) { tokens_ -= bytes; return true; }
        return false;
    }

private:
    void refill(std::uint64_t now_ms) {
        double elapsed_sec = now_ms > last_refill_ms_ ? static_cast<double>(now_ms - last_refill_ms_) / 1000.0 : 0.0;
        last_refill_ms_ = now_ms;
        tokens_ = std::min(capacity_bytes_, tokens_ + elapsed_sec * rate_bytes_per_sec_);
    }

    double rate_bytes_per_sec_ = 0.0;
    double capacity_bytes_ = 0.0;
    double tokens_ = 0.0;
    std::uint64_t last_refill_ms_ = 0;
    bool initialized_ = false;
};

class AdaptiveSender {
public:
    explicit AdaptiveSender(BitrateController& controller) : controller_(controller) {}

    void enqueue(PacketPriority priority, std::vector<std::uint8_t> data) {
        queue_.push(priority, std::move(data));
    }

    std::vector<std::vector<std::uint8_t>> drain(std::uint64_t now_ms) {
        controller_.evaluate(now_ms);
        double kbps = controller_.current().bitrate_kbps;
        double bytes_per_sec = (kbps * 1000.0) / 8.0;
        bucket_.reconfigure(bytes_per_sec, bytes_per_sec * 0.5, now_ms);

        std::vector<std::vector<std::uint8_t>> sent;
        while (auto next = queue_.pop_with_priority()) {
            auto& [priority, data] = *next;
            if (priority == PacketPriority::High) {
                sent.push_back(std::move(data));
                continue;
            }
            if (!bucket_.try_consume(static_cast<double>(data.size()), now_ms)) {
                dropped_count_++;
                continue;
            }
            sent.push_back(std::move(data));
        }
        return sent;
    }

    std::size_t dropped_count() const { return dropped_count_; }
    std::size_t queued_count() const { return queue_.size(); }

private:
    BitrateController& controller_;
    PriorityPacketQueue queue_;
    DeterministicTokenBucket bucket_;
    std::size_t dropped_count_ = 0;
};

} // namespace cascade::core::media