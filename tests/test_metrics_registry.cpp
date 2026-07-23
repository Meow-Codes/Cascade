#include <gtest/gtest.h>
#include <thread>
#include <vector>
#include "metrics/metrics_registry.hpp"

using namespace cascade::core::metrics;

TEST(MetricsRegistry, CounterIncrementsExactlyOncePerEvent) {
    MetricsRegistry registry;
    auto& c = registry.counter("test_counter");
    constexpr int kThreads = 8;
    constexpr int kIncsPerThread = 10000;
    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&] { for (int i = 0; i < kIncsPerThread; ++i) c.inc(); });
    }
    for (auto& th : threads) th.join();
    EXPECT_EQ(c.value(), static_cast<std::uint64_t>(kThreads * kIncsPerThread));
}

TEST(MetricsRegistry, GaugeSetAndAdd) {
    MetricsRegistry registry;
    auto& g = registry.gauge("test_gauge");
    g.set(10);
    g.add(5);
    g.add(-3);
    EXPECT_EQ(g.value(), 12);
}

TEST(MetricsRegistry, HistogramBucketsAndSum) {
    MetricsRegistry registry;
    auto& h = registry.histogram("test_hist", {10, 100, 1000});
    h.observe(5);
    h.observe(50);
    h.observe(500);
    h.observe(5000);

    EXPECT_EQ(h.count(), 4u);
    EXPECT_DOUBLE_EQ(h.sum(), 5 + 50 + 500 + 5000);
    EXPECT_EQ(h.cumulative_count(0), 1u);
    EXPECT_EQ(h.cumulative_count(2), 3u);
}

TEST(MetricsRegistry, RenderProducesPrometheusFormat) {
    MetricsRegistry registry;
    registry.counter("requests_total").inc(5);
    registry.gauge("queue_depth").set(42);
    auto text = registry.render_prometheus();
    EXPECT_NE(text.find("requests_total 5"), std::string::npos);
    EXPECT_NE(text.find("queue_depth 42"), std::string::npos);
    EXPECT_NE(text.find("# TYPE requests_total counter"), std::string::npos);
}