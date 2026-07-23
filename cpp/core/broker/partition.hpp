#pragma once
// Cascade :: core::broker :: Partition
//
// One partition = one storage::Log (Phase 3) plus lag-based producer
// backpressure.
//
// Backpressure design: publish() enforces a configurable max_lag_records
// ceiling -- the gap between the log's high watermark (next_offset) and
// the slowest *registered* consumer group's committed offset for this
// partition. try_publish() is the deterministic, non-blocking primitive
// (used by tests, and by anything that wants to react to backpressure
// itself rather than block on it); publish() is a blocking convenience
// wrapper implemented as a short poll loop rather than a
// condition_variable, deliberately: the signal that relieves
// backpressure (a consumer committing) happens in a different object
// (OffsetStore) than the one blocking (Partition), and wiring a
// cross-object condvar for a v1 feature added more coupling than the
// benefit justified. Worth revisiting if profiling ever shows contention
// here.

#include <atomic>
#include <chrono>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>

#include "broker/offset_store.hpp"
#include "storage/log.hpp"

namespace cascade::core::broker {

class BackpressureTimeout : public std::runtime_error {
public:
    BackpressureTimeout() : std::runtime_error("publish() timed out waiting for consumers to catch up") {}
};

class Partition {
public:
    Partition(std::string topic, int index, std::string dir, std::size_t max_segment_bytes,
              OffsetStore& offset_store, std::uint64_t max_lag_records)
        : topic_(std::move(topic)), index_(index), log_(std::move(dir), max_segment_bytes),
          offset_store_(offset_store), max_lag_records_(max_lag_records) {}

    // Non-blocking: returns nullopt if this partition is currently over
    // its lag limit rather than writing. Deterministic -- prefer this in
    // tests over the timing-dependent publish().
    std::optional<std::uint64_t> try_publish(const std::uint8_t* payload, std::uint32_t len) {
        if (over_lag_limit()) return std::nullopt;
        return log_.append(payload, len);
    }

    // Blocking convenience wrapper: retries try_publish() until it
    // succeeds or timeout_ms elapses.
    std::uint64_t publish(const std::uint8_t* payload, std::uint32_t len, std::uint64_t timeout_ms = 5000) {
        auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
        while (true) {
            if (auto offset = try_publish(payload, len)) return *offset;
            if (std::chrono::steady_clock::now() >= deadline) throw BackpressureTimeout();
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }

    std::optional<storage::ReadResult> read(std::uint64_t offset) const { return log_.read(offset); }
    std::uint64_t next_offset() const { return log_.next_offset(); }

    const std::string& topic() const { return topic_; }
    int index() const { return index_; }

private:
    bool over_lag_limit() const {
        if (max_lag_records_ == 0) return false; // 0 == unlimited (opt-out of backpressure)
        auto min_committed = offset_store_.min_committed_offset_for(topic_, index_);
        if (!min_committed.has_value()) return false; // no registered consumer groups yet
        std::uint64_t hwm = log_.next_offset();
        std::uint64_t committed = *min_committed;
        std::uint64_t lag = (hwm > committed) ? (hwm - committed) : 0;
        return lag >= max_lag_records_;
    }

    std::string topic_;
    int index_;
    storage::Log log_;
    OffsetStore& offset_store_;
    std::uint64_t max_lag_records_;
};

} // namespace cascade::core::broker