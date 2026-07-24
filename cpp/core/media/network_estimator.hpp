#pragma once
// Cascade :: core::media :: NetworkConditionEstimator
//
// Tracks three independent signals feeding the bitrate controller:
// received bandwidth (sliding window byte-rate), loss rate (EMA over
// per-packet-slot loss samples), and RTT (EMA over explicit RTT samples).
// Three separate signals, not one composite score -- BitrateController
// owns the policy of how to weigh them; this class is pure measurement.
//
// Same now_ms-driven determinism as JitterBuffer (Phase 6) -- assumes
// monotonically non-decreasing now_ms across calls.

#include <cstdint>
#include <deque>
#include <mutex>

namespace cascade::core::media {

class NetworkConditionEstimator {
public:
    explicit NetworkConditionEstimator(std::uint32_t bandwidth_window_ms = 2000,
                                        double loss_ema_alpha = 0.2,
                                        double rtt_ema_alpha = 0.2)
        : bandwidth_window_ms_(bandwidth_window_ms), loss_alpha_(loss_ema_alpha), rtt_alpha_(rtt_ema_alpha) {}

    void record_received_bytes(std::uint32_t bytes, std::uint64_t now_ms) {
        std::lock_guard<std::mutex> lock(mutex_);
        samples_.push_back({now_ms, bytes});
        evict_old_locked(now_ms);
    }

    // Call once per expected packet "slot" (arrived or not).
    void record_loss_sample(bool lost) {
        std::lock_guard<std::mutex> lock(mutex_);
        double sample = lost ? 1.0 : 0.0;
        loss_ema_ = loss_initialized_ ? (loss_alpha_ * sample + (1 - loss_alpha_) * loss_ema_) : sample;
        loss_initialized_ = true;
    }

    void record_rtt_sample(double rtt_ms) {
        std::lock_guard<std::mutex> lock(mutex_);
        rtt_ema_ = rtt_initialized_ ? (rtt_alpha_ * rtt_ms + (1 - rtt_alpha_) * rtt_ema_) : rtt_ms;
        rtt_initialized_ = true;
    }

    double estimated_bandwidth_kbps(std::uint64_t now_ms) const {
        std::lock_guard<std::mutex> lock(mutex_);
        const_cast<NetworkConditionEstimator*>(this)->evict_old_locked(now_ms);
        if (samples_.empty()) return 0.0;
        std::uint64_t total_bytes = 0;
        for (auto& s : samples_) total_bytes += s.bytes;
        std::uint64_t span_ms = now_ms > samples_.front().timestamp_ms ? now_ms - samples_.front().timestamp_ms : 1;
        return (static_cast<double>(total_bytes) * 8.0 / 1000.0) / (static_cast<double>(span_ms) / 1000.0);
    }

    double estimated_loss_rate() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return loss_initialized_ ? loss_ema_ : 0.0;
    }

    double smoothed_rtt_ms() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return rtt_initialized_ ? rtt_ema_ : 0.0;
    }

private:
    struct Sample { std::uint64_t timestamp_ms; std::uint32_t bytes; };

    void evict_old_locked(std::uint64_t now_ms) {
        while (!samples_.empty() && now_ms - samples_.front().timestamp_ms > bandwidth_window_ms_) {
            samples_.pop_front();
        }
    }

    std::uint32_t bandwidth_window_ms_;
    double loss_alpha_;
    double rtt_alpha_;

    std::deque<Sample> samples_;
    double loss_ema_ = 0.0;
    bool loss_initialized_ = false;
    double rtt_ema_ = 0.0;
    bool rtt_initialized_ = false;

    mutable std::mutex mutex_;
};

} // namespace cascade::core::media  