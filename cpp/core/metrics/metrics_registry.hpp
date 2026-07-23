#pragma once
// Cascade :: core::metrics :: MetricsRegistry
//
// Minimal Prometheus-compatible registry: Counter (monotonic), Gauge
// (arbitrary up/down), Histogram (fixed buckets + sum + count, same
// shape as a Prometheus histogram). Deliberately NOT a singleton -- each
// Broker owns one MetricsRegistry and passes it by reference to whatever
// it wants instrumented, matching how OffsetStore is threaded through
// explicitly in Phase 4, so metrics stay testable in isolation.

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

namespace cascade::core::metrics {

class Counter {
public:
    void inc(std::uint64_t n = 1) { value_.fetch_add(n, std::memory_order_relaxed); }
    std::uint64_t value() const { return value_.load(std::memory_order_relaxed); }
private:
    std::atomic<std::uint64_t> value_{0};
};

class Gauge {
public:
    void set(std::int64_t v) { value_.store(v, std::memory_order_relaxed); }
    void add(std::int64_t delta) { value_.fetch_add(delta, std::memory_order_relaxed); }
    std::int64_t value() const { return value_.load(std::memory_order_relaxed); }
private:
    std::atomic<std::int64_t> value_{0};
};

// Fixed-bucket histogram. This project uses microseconds for every
// latency histogram, matching the benchmark harnesses from earlier phases.
class Histogram {
public:
    explicit Histogram(std::vector<double> bucket_bounds)
        : bounds_(std::move(bucket_bounds)), bucket_counts_(bounds_.size() + 1) {}

    void observe(double v) {
        count_.fetch_add(1, std::memory_order_relaxed);
        double old_sum = sum_.load(std::memory_order_relaxed);
        while (!sum_.compare_exchange_weak(old_sum, old_sum + v, std::memory_order_relaxed)) {}

        auto it = std::upper_bound(bounds_.begin(), bounds_.end(), v);
        std::size_t idx = static_cast<std::size_t>(it - bounds_.begin());
        bucket_counts_[idx].fetch_add(1, std::memory_order_relaxed);
    }

    std::uint64_t count() const { return count_.load(std::memory_order_relaxed); }
    double sum() const { return sum_.load(std::memory_order_relaxed); }
    const std::vector<double>& bounds() const { return bounds_; }

    // Cumulative count for bucket_bounds_[i] ("le" bucket), matching
    // Prometheus's cumulative histogram semantics.
    std::uint64_t cumulative_count(std::size_t bound_index) const {
        std::uint64_t total = 0;
        for (std::size_t i = 0; i <= bound_index; ++i) total += bucket_counts_[i].load(std::memory_order_relaxed);
        return total;
    }

private:
    std::vector<double> bounds_;
    std::vector<std::atomic<std::uint64_t>> bucket_counts_;
    std::atomic<std::uint64_t> count_{0};
    std::atomic<double> sum_{0.0};
};

class MetricsRegistry {
public:
    Counter& counter(const std::string& name) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = counters_.find(name);
        if (it != counters_.end()) return *it->second;
        auto [inserted, ok] = counters_.emplace(name, std::make_unique<Counter>());
        return *inserted->second;
    }

    Gauge& gauge(const std::string& name) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = gauges_.find(name);
        if (it != gauges_.end()) return *it->second;
        auto [inserted, ok] = gauges_.emplace(name, std::make_unique<Gauge>());
        return *inserted->second;
    }

    Histogram& histogram(const std::string& name, std::vector<double> bucket_bounds) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = histograms_.find(name);
        if (it != histograms_.end()) return *it->second;
        auto [inserted, ok] = histograms_.emplace(name, std::make_unique<Histogram>(std::move(bucket_bounds)));
        return *inserted->second;
    }

    std::string render_prometheus() const {
        std::lock_guard<std::mutex> lock(mutex_);
        std::ostringstream out;

        for (auto& [name, c] : counters_) {
            out << "# TYPE " << name << " counter\n" << name << " " << c->value() << "\n";
        }
        for (auto& [name, g] : gauges_) {
            out << "# TYPE " << name << " gauge\n" << name << " " << g->value() << "\n";
        }
        for (auto& [name, h] : histograms_) {
            out << "# TYPE " << name << " histogram\n";
            auto& bounds = h->bounds();
            for (std::size_t i = 0; i < bounds.size(); ++i) {
                out << name << "_bucket{le=\"" << bounds[i] << "\"} " << h->cumulative_count(i) << "\n";
            }
            out << name << "_bucket{le=\"+Inf\"} " << h->count() << "\n";
            out << name << "_sum " << h->sum() << "\n";
            out << name << "_count " << h->count() << "\n";
        }
        return out.str();
    }

private:
    mutable std::mutex mutex_;
    std::unordered_map<std::string, std::unique_ptr<Counter>> counters_;
    std::unordered_map<std::string, std::unique_ptr<Gauge>> gauges_;
    std::unordered_map<std::string, std::unique_ptr<Histogram>> histograms_;
};

} // namespace cascade::core::metrics