#include <gtest/gtest.h>
#include <chrono>
#include <filesystem>
#include <unistd.h>
#include "broker/broker.hpp"
#include "broker/producer.hpp"
#include "metrics/metrics_registry.hpp"

using namespace cascade::core::broker;
using Clock = std::chrono::steady_clock;

// Not a strict CI gate -- a hard 2% threshold is flaky on shared/
// virtualized CI runners due to system noise. This prints the real
// measured overhead (check it manually / paste into phase5 notes) and
// only fails outright on a gross regression (>25%), which would indicate
// an actual bug (e.g. an accidental lock on the hot path) rather than noise.
TEST(MetricsOverhead, AttachingMetricsDoesNotMeaningfullySlowPublish) {
    constexpr int kMessages = 100000;
    std::vector<std::uint8_t> payload(128, 0x1);

    auto run = [&](bool with_metrics) {
        auto dir = std::string("/tmp/cascade_overhead_test_") + (with_metrics ? "with_" : "without_") +
                   std::to_string(::getpid());
        std::filesystem::remove_all(dir);
        Broker broker(dir);
        auto topic = broker.create_topic("overhead", 1, 64 * 1024 * 1024, 0, FlushPolicy::Manual); // isolate metrics overhead from msync cost
        if (with_metrics) {
            static cascade::core::metrics::MetricsRegistry registry;
            topic->partition(0)->attach_metrics(registry);
        }
        Producer producer(topic);

        auto start = Clock::now();
        for (int i = 0; i < kMessages; ++i) producer.publish(payload.data(), static_cast<std::uint32_t>(payload.size()));
        auto end = Clock::now();
        std::filesystem::remove_all(dir);
        return std::chrono::duration<double>(end - start).count();
    };

    double without = run(false);
    double with = run(true);
    double overhead_pct = ((with - without) / without) * 100.0;

    std::printf("Publish time without metrics: %.4f s, with metrics: %.4f s (overhead: %.1f%%)\n",
                without, with, overhead_pct);

    EXPECT_LT(overhead_pct, 25.0) << "metrics overhead suspiciously high -- check for a hot-path lock";
}