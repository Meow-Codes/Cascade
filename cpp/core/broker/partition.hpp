#pragma once
// Cascade :: core::broker :: Partition
//
// One partition = one storage::Log + lag-based producer backpressure.
//
// Ack ordering: a publish returns only after the record is flushed
// (FlushPolicy::PerMessage). publish_batch() flushes ONCE for the whole batch
// and acknowledges all of it together -- same guarantee, amortized cost.
//
// Batch caveats: (1) the lag limit is soft for batches: admitted when
// lag < max_lag_records, so lag can overshoot by up to batch_size - 1;
// (2) a batch is not atomic across a crash (see storage/log.hpp).

#include <atomic>
#include <chrono>
#include <cstdint>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <thread>

#include "broker/offset_store.hpp"
#include "metrics/metrics_registry.hpp"
#include "storage/log.hpp"

namespace cascade::core::broker {

using PayloadView = storage::PayloadView;

class BackpressureTimeout : public std::runtime_error {
public:
    BackpressureTimeout() : std::runtime_error("publish() timed out waiting for consumers to catch up") {}
};

enum class FlushPolicy { PerMessage, Manual };

struct BatchAck {
    std::uint64_t first_offset; // records occupy first_offset .. first_offset + count - 1
    std::size_t count;
};

class Partition {
public:
    Partition(std::string topic, int index, std::string dir, std::size_t max_segment_bytes,
              OffsetStore& offset_store, std::uint64_t max_lag_records,
              FlushPolicy flush_policy = FlushPolicy::PerMessage)
        : topic_(std::move(topic)), index_(index), log_(std::move(dir), max_segment_bytes),
          offset_store_(offset_store), max_lag_records_(max_lag_records), flush_policy_(flush_policy) {}

    void attach_metrics(metrics::MetricsRegistry& registry) {
        metrics_ = &registry;
        publish_latency_ = &registry.histogram("cascade_publish_latency_us",
            {10, 25, 50, 100, 250, 500, 1000, 5000, 10000});
        batch_latency_ = &registry.histogram("cascade_publish_batch_latency_us",
            {100, 250, 500, 1000, 2500, 5000, 10000, 50000});
    }

    std::optional<std::uint64_t> try_publish(const std::uint8_t* payload, std::uint32_t len) {
        auto t0 = std::chrono::steady_clock::now();
        if (over_lag_limit()) {
            if (metrics_) metrics_->counter("cascade_publish_rejected_backpressure_total").inc();
            return std::nullopt;
        }
        auto offset = log_.append(payload, len);
        if (flush_policy_ == FlushPolicy::PerMessage) log_.flush();

        if (metrics_) {
            metrics_->counter("cascade_publish_total").inc();
            auto t1 = std::chrono::steady_clock::now();
            publish_latency_->observe(std::chrono::duration<double, std::micro>(t1 - t0).count());
        }
        return offset;
    }

    std::optional<BatchAck> try_publish_batch(std::span<const PayloadView> payloads) {
        if (payloads.empty()) return BatchAck{log_.next_offset(), 0};

        auto t0 = std::chrono::steady_clock::now();
        if (over_lag_limit()) {
            if (metrics_) metrics_->counter("cascade_publish_rejected_backpressure_total").inc();
            return std::nullopt;
        }
        const std::uint64_t first = log_.append_batch(payloads, flush_policy_ == FlushPolicy::PerMessage);

        if (metrics_) {
            metrics_->counter("cascade_publish_total").inc(payloads.size());
            metrics_->counter("cascade_publish_batches_total").inc();
            auto t1 = std::chrono::steady_clock::now();
            batch_latency_->observe(std::chrono::duration<double, std::micro>(t1 - t0).count());
        }
        return BatchAck{first, payloads.size()};
    }

    std::uint64_t publish(const std::uint8_t* payload, std::uint32_t len, std::uint64_t timeout_ms = 5000) {
        auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
        while (true) {
            if (auto offset = try_publish(payload, len)) return *offset;
            if (std::chrono::steady_clock::now() >= deadline) throw BackpressureTimeout();
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }

    BatchAck publish_batch(std::span<const PayloadView> payloads, std::uint64_t timeout_ms = 5000) {
        auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
        while (true) {
            if (auto ack = try_publish_batch(payloads)) return *ack;
            if (std::chrono::steady_clock::now() >= deadline) throw BackpressureTimeout();
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }

    void flush() { log_.flush(); } // for FlushPolicy::Manual callers

    std::optional<storage::ReadResult> read(std::uint64_t offset) const { return log_.read(offset); }
    std::uint64_t next_offset() const { return log_.next_offset(); }
    std::uint64_t flush_calls() const { return log_.flush_calls(); }

    const std::string& topic() const { return topic_; }
    int index() const { return index_; }

private:
    bool over_lag_limit() const {
        if (max_lag_records_ == 0) return false;
        auto min_committed = offset_store_.min_committed_offset_for(topic_, index_);
        if (!min_committed.has_value()) return false;
        std::uint64_t hwm = log_.next_offset();
        std::uint64_t lag = (hwm > *min_committed) ? (hwm - *min_committed) : 0;
        return lag >= max_lag_records_;
    }

    std::string topic_;
    int index_;
    storage::Log log_;
    OffsetStore& offset_store_;
    std::uint64_t max_lag_records_;
    FlushPolicy flush_policy_;

    metrics::MetricsRegistry* metrics_ = nullptr;
    metrics::Histogram* publish_latency_ = nullptr;
    metrics::Histogram* batch_latency_ = nullptr;
};

} // namespace cascade::core::broker