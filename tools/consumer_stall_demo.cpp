// Standalone reproduction of the BrokerBackpressure unit test (Phase 4),
// as a real process a fault-injection script can run/observe/kill --
// proves backpressure holds under a genuinely stalled consumer, not just
// in a gtest process.
#include <cstdio>
#include <filesystem>
#include <unistd.h>
#include "broker/broker.hpp"
#include "broker/producer.hpp"
#include "broker/consumer.hpp"

using namespace cascade::core::broker;

int main() {
    auto dir = std::string("/tmp/cascade_consumer_stall_") + std::to_string(::getpid());
    std::filesystem::remove_all(dir);

    constexpr std::uint64_t kMaxLag = 50;
    Broker broker(dir);
    auto topic = broker.create_topic("stall-demo", 1, 1 << 20, kMaxLag);
    Producer producer(topic);
    Consumer consumer(topic, "stalled-group", 1, 0, broker.offset_store()); // registers, never polls/commits

    std::string payload = "x";
    int published = 0;
    while (true) {
        auto result = producer.try_publish(0, reinterpret_cast<const std::uint8_t*>(payload.data()), payload.size());
        if (!result.has_value()) break;
        published++;
    }
    std::printf("producer stalled by backpressure after %d messages (max_lag=%llu)\n",
                published, static_cast<unsigned long long>(kMaxLag));

    auto records = consumer.poll(10000);
    consumer.commit();
    std::printf("consumer caught up: consumed=%zu\n", records.size());

    auto after = producer.try_publish(0, reinterpret_cast<const std::uint8_t*>(payload.data()), payload.size());
    std::printf("producer resumed after commit: %s\n", after.has_value() ? "YES" : "NO");

    std::filesystem::remove_all(dir);
    return after.has_value() ? 0 : 1;
}