#pragma once
// Cascade :: core::media :: JitterBuffer
//
// Reorders and paces incoming audio frames for smooth, in-order playout
// despite network jitter and loss. Deliberately clock-driven via an
// explicit `now_ms` parameter on every call (same philosophy as
// TimerWheel::tick() from Phase 1) rather than reading a wall clock
// internally -- this is what makes loss/reorder masking behavior
// deterministic and unit-testable without real sleeps.
//
// Model: frames are expected at a fixed cadence (frame_interval_ms).
// push() inserts an arrived frame by sequence number into a sparse map
// (naturally absorbing reordering -- a late-arriving lower sequence just
// lands in the map, no special-casing needed). pull(now_ms) asks "is it
// time to play next_sequence_ yet?" and:
//   - if that frame has arrived: play it immediately (in order).
//   - if it hasn't arrived AND now_ms is past its playout deadline
//     (arrival-of-first-frame + target_delay_ms + its slot in the
//     cadence): conceal it -- basic PLC (packet loss concealment) via
//     last-good-frame repeat, the simplest concealment strategy that
//     still avoids a hard audio dropout.
//   - if it hasn't arrived but there's still time left in the jitter
//     window: NotReady -- caller should not advance playout yet.
//
// target_delay_ms is the buffering latency this adds to the end-to-end
// path -- the fundamental jitter-vs-latency trade-off: a bigger window
// masks more loss/jitter but adds more delay. Exposed directly rather
// than auto-tuned, since auto-adjusting jitter buffers (a real feature
// in production WebRTC stacks) is a legitimate stretch goal, not v1 scope.

#include <cstdint>
#include <mutex>
#include <unordered_map>
#include <vector>

namespace cascade::core::media {

class JitterBuffer {
public:
    JitterBuffer(std::uint32_t frame_interval_ms, std::uint32_t target_delay_ms)
        : frame_interval_ms_(frame_interval_ms), target_delay_ms_(target_delay_ms) {}

    void push(std::uint32_t sequence, std::vector<std::uint8_t> payload, std::uint64_t arrival_time_ms) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!started_) {
            started_ = true;
            base_arrival_ms_ = arrival_time_ms;
            base_sequence_ = sequence;
            next_sequence_ = sequence;
        }
        if (sequence < next_sequence_) {
            late_after_deadline_++; // arrived after its slot was already played/concealed
            return;
        }
        buffer_[sequence] = std::move(payload);
    }

    struct PullResult {
        enum class Kind { Packet, Concealed, NotReady } kind = Kind::NotReady;
        std::uint32_t sequence = 0;
        std::vector<std::uint8_t> payload;
    };

    PullResult pull(std::uint64_t now_ms) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!started_) return {};

        std::uint64_t slot = next_sequence_ - base_sequence_;
        std::uint64_t deadline = base_arrival_ms_ + target_delay_ms_ + slot * frame_interval_ms_;

        auto it = buffer_.find(next_sequence_);
        if (it != buffer_.end()) {
            PullResult r{PullResult::Kind::Packet, next_sequence_, std::move(it->second)};
            last_good_payload_ = r.payload;
            has_last_good_ = true;
            buffer_.erase(it);
            next_sequence_++;
            packets_played_++;
            return r;
        }

        if (now_ms >= deadline) {
            PullResult r{PullResult::Kind::Concealed, next_sequence_,
                         has_last_good_ ? last_good_payload_ : std::vector<std::uint8_t>{}};
            next_sequence_++;
            concealed_count_++;
            packets_played_++;
            return r;
        }

        return {}; // still within the jitter window; not time to play this slot yet
    }

    std::size_t concealed_count() const { std::lock_guard<std::mutex> l(mutex_); return concealed_count_; }
    std::size_t packets_played() const { std::lock_guard<std::mutex> l(mutex_); return packets_played_; }
    double effective_loss_rate() const {
        std::lock_guard<std::mutex> l(mutex_);
        return packets_played_ ? static_cast<double>(concealed_count_) / packets_played_ : 0.0;
    }
    std::uint32_t target_delay_ms() const { return target_delay_ms_; }

private:
    std::uint32_t frame_interval_ms_;
    std::uint32_t target_delay_ms_;

    bool started_ = false;
    std::uint64_t base_arrival_ms_ = 0;
    std::uint32_t base_sequence_ = 0;
    std::uint32_t next_sequence_ = 0;

    std::unordered_map<std::uint32_t, std::vector<std::uint8_t>> buffer_;
    std::vector<std::uint8_t> last_good_payload_;
    bool has_last_good_ = false;

    std::size_t concealed_count_ = 0;
    std::size_t packets_played_ = 0;
    std::size_t late_after_deadline_ = 0;

    mutable std::mutex mutex_;
};

} // namespace cascade::core::media