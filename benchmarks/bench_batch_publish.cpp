#include <algorithm>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <unistd.h>
#include <vector>

#include "broker/broker.hpp"
#include "broker/producer.hpp"

using namespace cascade::core;
using Clock = std::chrono::steady_clock;

struct Row { double msgs_per_sec, mib_per_sec, p50_us, p99_us; };

static Row run(std::size_t batch, std::size_t total, broker::FlushPolicy policy) {
    auto dir = "/tmp/cascade_bench_batch_" + std::to_string(::getpid()) + "_" + std::to_string(batch);
    std::filesystem::remove_all(dir);
    Row row{};
    {
        broker::Broker broker(dir);
        auto topic = broker.create_topic("b", 1, 64u * 1024 * 1024, 0, policy);
        broker::Producer producer(topic);

        constexpr std::size_t kPayload = 128; // same payload size as bench_broker
        std::vector<std::uint8_t> payload(kPayload, 0x5A);
        std::vector<broker::PayloadView> views(batch, broker::PayloadView(payload.data(), payload.size()));

        const std::size_t batches = total / batch;
        std::vector<double> lat;
        lat.reserve(batches);

        const auto start = Clock::now();
        for (std::size_t i = 0; i < batches; ++i) {
            auto t0 = Clock::now();
            producer.publish_batch(views);
            lat.push_back(std::chrono::duration<double, std::micro>(Clock::now() - t0).count());
        }
        const double secs = std::chrono::duration<double>(Clock::now() - start).count();

        std::sort(lat.begin(), lat.end());
        const double msgs = static_cast<double>(batches * batch);
        row = {msgs / secs, (msgs * kPayload) / (1024.0 * 1024.0) / secs,
               lat[lat.size() * 50 / 100], lat[lat.size() * 99 / 100]};
    }
    std::filesystem::remove_all(dir);
    return row;
}

int main() {
    std::printf("--- Batch publish: durable (one msync per batch), 128 B payloads ---\n");
    std::printf("%6s  %12s  %10s  %12s  %12s\n", "batch", "msgs/sec", "MiB/s", "ack p50 (us)", "ack p99 (us)");
    for (std::size_t batch : {1u, 2u, 4u, 8u, 16u, 32u, 64u, 128u, 256u, 512u, 1024u}) {
        const std::size_t total = std::max<std::size_t>(8192, batch * 200);
        Row r = run(batch, total, broker::FlushPolicy::PerMessage);
        std::printf("%6zu  %12.0f  %10.2f  %12.1f  %12.1f\n", batch, r.msgs_per_sec, r.mib_per_sec, r.p50_us, r.p99_us);
        std::fflush(stdout);
    }

    std::printf("\n--- Ceiling: batch=1024 with NO flush (memcpy + CRC32 only) ---\n");
    Row c = run(1024, 1024 * 200, broker::FlushPolicy::Manual);
    std::printf("%6d  %12.0f  %10.2f  %12.1f  %12.1f\n", 1024, c.msgs_per_sec, c.mib_per_sec, c.p50_us, c.p99_us);
    return 0;
}