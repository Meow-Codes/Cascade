#include <algorithm>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <unistd.h>
#include <vector>
#include "broker/broker.hpp"
#include "broker/producer.hpp"
#include "broker/consumer.hpp"

using namespace cascade::core::broker;
using Clock = std::chrono::steady_clock;

static void bench_throughput() {
    auto dir = std::string("/tmp/cascade_bench_broker_throughput_") + std::to_string(::getpid());
    std::filesystem::remove_all(dir);
    Broker broker(dir);
    auto topic = broker.create_topic("bench", 4, 64 * 1024 * 1024);
    Producer producer(topic);

    constexpr int kMessages = 500000;
    std::vector<std::uint8_t> payload(128, 0x42);

    auto start = Clock::now();
    for (int i = 0; i < kMessages; ++i) {
        producer.publish(payload.data(), static_cast<std::uint32_t>(payload.size()));
    }
    auto end = Clock::now();
    double secs = std::chrono::duration<double>(end - start).count();
    std::printf("Publish throughput: %d msgs in %.4f s -> %.0f msgs/sec\n", kMessages, secs, kMessages / secs);

    Consumer consumer(topic, "bench-group", 1, 0, broker.offset_store());
    int consumed = 0;
    start = Clock::now();
    while (consumed < kMessages) {
        auto batch = consumer.poll(1000);
        if (batch.empty()) break;
        consumed += static_cast<int>(batch.size());
    }
    end = Clock::now();
    secs = std::chrono::duration<double>(end - start).count();
    std::printf("Consume throughput: %d msgs in %.4f s -> %.0f msgs/sec\n", consumed, secs, consumed / secs);

    std::filesystem::remove_all(dir);
}

static void bench_end_to_end_latency() {
    auto dir = std::string("/tmp/cascade_bench_broker_latency_") + std::to_string(::getpid());
    std::filesystem::remove_all(dir);
    Broker broker(dir);
    auto topic = broker.create_topic("bench-latency", 1, 64 * 1024 * 1024);
    Producer producer(topic);
    Consumer consumer(topic, "latency-group", 1, 0, broker.offset_store());

    constexpr int kIterations = 20000;
    std::vector<double> latencies_us;
    latencies_us.reserve(kIterations);
    std::string payload = "ping";

    for (int i = 0; i < kIterations; ++i) {
        auto t0 = Clock::now();
        producer.publish(reinterpret_cast<const std::uint8_t*>(payload.data()), payload.size());
        std::vector<PolledRecord> batch;
        while (batch.empty()) batch = consumer.poll(1);
        auto t1 = Clock::now();
        consumer.commit();
        latencies_us.push_back(std::chrono::duration<double, std::micro>(t1 - t0).count());
    }

    std::sort(latencies_us.begin(), latencies_us.end());
    double p50 = latencies_us[latencies_us.size() * 50 / 100];
    double p99 = latencies_us[latencies_us.size() * 99 / 100];
    std::printf("Publish-to-consume latency (n=%d): p50 = %.2f us, p99 = %.2f us\n", kIterations, p50, p99);

    std::filesystem::remove_all(dir);
}

int main() {
    std::printf("--- Broker pub/sub benchmark ---\n");
    bench_throughput();
    bench_end_to_end_latency();
    return 0;
}