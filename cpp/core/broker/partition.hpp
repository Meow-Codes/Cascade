#pragma once
// Cascade :: core::broker :: Partition
//
// CORRECTNESS FIX vs the original Phase 4 version: publish() previously
// never called log_.flush()/msync() before returning, which violated the
// ack-ordering decision fixed back in the Phase 0 diagram ("Append log ->
// WAL confirms -> Broker ACKs producer"). try_publish() now flushes by
// default (FlushPolicy::PerMessage) before returning the offset. This
// will visibly slow down bench_broker's publish throughput compared to
// your earlier run -- that's the correct, honest number, not a
// regression. FlushPolicy::Manual is available if you want to
// deliberately trade durability for throughput and measure the delta.

#include <atomic>
#include <chrono>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>

#include "broker/offset_store.hpp"
#include "metrics/metrics_registry.hpp"
#include "storage/log.hpp"

namespace cascade::core::broker {

class BackpressureTimeout : public std::runtime_error {
public:
    BackpressureTimeout() : std::runtime_error("publish() timed out waiting for consumers to catch up") {}
};

enum class FlushPolicy { PerMessage, Manual };

class Partition {
public:
    Partition(std::string topic, int index, std::string dir, std::size_t max_segment_bytes,
              OffsetStore& offset_store, std::uint64_t max_lag_records,
              FlushPolicy flush_policy = FlushPolicy::PerMessage)
        : topic_(std::move(topic)), index_(index), log_(std::move(dir), max_segment_bytes),
          offset_store_(offset_store), max_lag_records_(max_lag_records), flush_policy_(flush_policy) {}

    void attach_metrics(metrics::MetricsRegistry& registry) {
        metrics_ = &registry;
        publish_counter_ =
            &registry.counter("cascade_publish_total");
        publish_latency_ =
            &registry.histogram("cascade_publish_latency_us",
                {10,25,50,100,250,500,1000,5000,10000});
    }

    std::optional<std::uint64_t> try_publish(const std::uint8_t* payload, std::uint32_t len) {
        auto t0 = std::chrono::steady_clock::now();
        if (over_lag_limit()) {
            if (metrics_) metrics_->counter("cascade_publish_rejected_backpressure_total").inc();
            return std::nullopt;
        }
        auto offset = log_.append(payload, len);

        if (flush_policy_ == FlushPolicy::PerMessage)
            log_.flush();

        if (publish_counter_) {
            publish_counter_->inc();

            // Sample latency rather than timing every publish.
            // This keeps metrics lightweight on the hot path.
            auto sample = latency_sample_counter_.fetch_add(
                1, std::memory_order_relaxed);

            if ((sample & 0xF) == 0) { // 1 out of every 16 publishes
                auto t1 = std::chrono::steady_clock::now();

                publish_latency_->observe(
                    std::chrono::duration<double, std::micro>(t1 - t0).count());
            }
        }

        return offset;
    }

    std::uint64_t publish(const std::uint8_t* payload, std::uint32_t len, std::uint64_t timeout_ms = 5000) {
        auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
        while (true) {
            if (auto offset = try_publish(payload, len)) return *offset;
            if (std::chrono::steady_clock::now() >= deadline) throw BackpressureTimeout();
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }

    void flush() { log_.flush(); } // for FlushPolicy::Manual callers

    std::optional<storage::ReadResult> read(std::uint64_t offset) const { return log_.read(offset); }
    std::uint64_t next_offset() const { return log_.next_offset(); }

    const std::string& topic() const { return topic_; }
    int index() const { return index_; }

private:
    bool over_lag_limit() const {
        if (max_lag_records_ == 0) return false;
        auto min_committed = offset_store_.min_committed_offset_for(topic_, index_);
        if (!min_committed.has_value()) return false;
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
    FlushPolicy flush_policy_;

    metrics::MetricsRegistry* metrics_ = nullptr;
    metrics::Counter* publish_counter_ = nullptr;
    metrics::Histogram* publish_latency_ = nullptr;
    std::atomic<std::uint32_t> latency_sample_counter_{0};
};

} // namespace cascade::core::broker