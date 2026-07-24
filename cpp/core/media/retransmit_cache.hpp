#pragma once
// Cascade :: core::media :: RetransmitCache
//
// Sender-side ring buffer of recently-sent packet bytes, keyed by
// sequence number, so the sender can respond to a receiver's NACK
// (retransmit-lite: no retry/backoff, no RTT estimation -- fire the
// cached packet back once and hope it lands. A production implementation
// would track RTT and retry with backoff; that's a Phase 12-style
// optimization, not required to demonstrate the recovery mechanism.)

#include <cstdint>
#include <deque>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <vector>

namespace cascade::core::media {

class RetransmitCache {
public:
    explicit RetransmitCache(std::size_t capacity = 256) : capacity_(capacity) {}

    void store(std::uint32_t sequence, std::vector<std::uint8_t> packet_bytes) {
        std::lock_guard<std::mutex> lock(mutex_);
        cache_[sequence] = std::move(packet_bytes);
        order_.push_back(sequence);
        while (order_.size() > capacity_) {
            cache_.erase(order_.front());
            order_.pop_front();
        }
    }

    std::optional<std::vector<std::uint8_t>> get(std::uint32_t sequence) const {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = cache_.find(sequence);
        if (it == cache_.end()) return std::nullopt;
        return it->second;
    }

private:
    std::size_t capacity_;
    std::unordered_map<std::uint32_t, std::vector<std::uint8_t>> cache_;
    std::deque<std::uint32_t> order_;
    mutable std::mutex mutex_;
};

} // namespace cascade::core::media