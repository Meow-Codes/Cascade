#include <gtest/gtest.h>
#include <filesystem>
#include <unistd.h>
#include "broker/broker.hpp"
#include "broker/producer.hpp"
#include "broker/consumer.hpp"

using namespace cascade::core::broker;

TEST(BrokerBackpressure, TryPublishFailsAtLagLimitAndRecoversAfterCommit) {
    auto dir = std::string("/tmp/cascade_broker_backpressure_test_") + std::to_string(::getpid());
    std::filesystem::remove_all(dir);

    constexpr std::uint64_t kMaxLag = 20;
    Broker broker(dir);
    auto topic = broker.create_topic("bp", /*num_partitions=*/1, /*max_segment_bytes=*/1 << 20, kMaxLag);
    Producer producer(topic);

    // Registering the consumer group (even before it polls anything) is
    // what makes this partition subject to lag tracking at all.
    Consumer consumer(topic, "slow-group", /*group_size=*/1, /*consumer_index=*/0, broker.offset_store());

    std::string payload = "x";
    int published = 0;
    while (true) {
        auto result = producer.try_publish(0, reinterpret_cast<const std::uint8_t*>(payload.data()), payload.size());
        if (!result.has_value()) break;
        published++;
        ASSERT_LT(published, 10000) << "backpressure never kicked in -- limit not enforced";
    }

    EXPECT_GE(published, static_cast<int>(kMaxLag));
    EXPECT_LT(published, static_cast<int>(kMaxLag) + 5); // tight, some slack

    auto still_blocked = producer.try_publish(0, reinterpret_cast<const std::uint8_t*>(payload.data()), payload.size());
    EXPECT_FALSE(still_blocked.has_value());

    auto records = consumer.poll(1000);
    EXPECT_EQ(records.size(), static_cast<std::size_t>(published));
    consumer.commit();

    auto after_commit = producer.try_publish(0, reinterpret_cast<const std::uint8_t*>(payload.data()), payload.size());
    EXPECT_TRUE(after_commit.has_value());

    std::filesystem::remove_all(dir);
}

TEST(BrokerBackpressure, BlockingPublishThrowsOnTimeoutWhenConsumerNeverCatchesUp) {
    auto dir = std::string("/tmp/cascade_broker_backpressure_timeout_test_") + std::to_string(::getpid());
    std::filesystem::remove_all(dir);

    Broker broker(dir);
    auto topic = broker.create_topic("bp2", 1, 1 << 20, /*max_lag_records=*/5);
    auto partition = topic->partition(0);

    Consumer consumer(topic, "stuck-group", 1, 0, broker.offset_store()); // registers, never commits

    std::string payload = "y";
    for (int i = 0; i < 5; ++i) {
        partition->publish(reinterpret_cast<const std::uint8_t*>(payload.data()), payload.size());
    }

    EXPECT_THROW(
        partition->publish(reinterpret_cast<const std::uint8_t*>(payload.data()), payload.size(), /*timeout_ms=*/100),
        BackpressureTimeout);

    std::filesystem::remove_all(dir);
}