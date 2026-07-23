#pragma once
// Cascade :: core::media :: PriorityPacketQueue
//
// Two-level outgoing packet queue: control traffic (NACKs, heartbeats)
// drains before normal audio data. Two levels, not a general N-level
// priority scheme, because that's all this project's traffic mix
// currently needs -- real-time media plus a thin control channel. Kept
// as a simple mutex-protected pair of deques rather than adapting Phase
// 1's lock-free MpmcQueue: priority ordering across two queues needs a
// check-both-then-pick decision per pop() that a single lock-free queue
// doesn't give you for free, and this queue's throughput demands are
// nowhere near what justified the lock-free design for Phase 1's queues.

#include <cstdint>
#include <deque>
#include <mutex>
#include <optional>
#include <vector>

namespace cascade::core::media {

enum class PacketPriority : std::uint8_t { High = 0, Normal = 1 };

class PriorityPacketQueue {
public:
    void push(PacketPriority priority, std::vector<std::uint8_t> data) {
        std::lock_guard<std::mutex> lock(mutex_);
        queues_[static_cast<std::size_t>(priority)].push_back(std::move(data));
    }

    std::optional<std::vector<std::uint8_t>> pop() {
        std::lock_guard<std::mutex> lock(mutex_);
        for (auto& q : queues_) {
            if (!q.empty()) {
                auto front = std::move(q.front());
                q.pop_front();
                return front;
            }
        }
        return std::nullopt;
    }

    std::size_t size() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return queues_[0].size() + queues_[1].size();
    }

private:
    std::array<std::deque<std::vector<std::uint8_t>>, 2> queues_;
    mutable std::mutex mutex_;
};

} // namespace cascade::core::media