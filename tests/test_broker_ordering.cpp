#include <gtest/gtest.h>
#include <filesystem>
#include <unistd.h>
#include "broker/broker.hpp"
#include "broker/producer.hpp"
#include "broker/consumer.hpp"

using namespace cascade::core::broker;

static std::string temp_dir(const char* name) {
    auto dir = std::string("/tmp/cascade_broker_test_") + name + "_" + std::to_string(::getpid());
    std::filesystem::remove_all(dir);
    return dir;
}

TEST(BrokerOrdering, StrictOrderWithinSinglePartition) {
    auto dir = temp_dir("ordering");
    Broker broker(dir);
    auto topic = broker.create_topic("events", /*num_partitions=*/1);
    Producer producer(topic);

    constexpr int kMessages = 1000;
    for (int i = 0; i < kMessages; ++i) {
        std::string payload = std::to_string(i);
        producer.publish(reinterpret_cast<const std::uint8_t*>(payload.data()), payload.size());
    }

    Consumer consumer(topic, "test-group", /*group_size=*/1, /*consumer_index=*/0, broker.offset_store());
    auto records = consumer.poll(kMessages);
    ASSERT_EQ(records.size(), static_cast<std::size_t>(kMessages));
    for (int i = 0; i < kMessages; ++i) {
        EXPECT_EQ(std::string(records[i].payload.begin(), records[i].payload.end()), std::to_string(i))
            << "order violated at index " << i;
        EXPECT_EQ(records[i].offset, static_cast<std::uint64_t>(i));
    }

    std::filesystem::remove_all(dir);
}

TEST(BrokerOrdering, KeyedMessagesOrderedWithinTheirPartition) {
    auto dir = temp_dir("keyed_ordering");
    Broker broker(dir);
    auto topic = broker.create_topic("keyed", /*num_partitions=*/4);
    Producer producer(topic);

    std::vector<PublishResult> results;
    for (int i = 0; i < 50; ++i) {
        std::string payload = "session-A-" + std::to_string(i);
        results.push_back(producer.publish_keyed(
            "session-A", reinterpret_cast<const std::uint8_t*>(payload.data()), payload.size()));
    }
    int partition = results.front().partition;
    for (auto& r : results) EXPECT_EQ(r.partition, partition); // all landed in the same partition

    Consumer consumer(topic, "g", topic->num_partitions(), partition, broker.offset_store());
    auto records = consumer.poll(1000);
    ASSERT_EQ(records.size(), 50u);
    for (int i = 0; i < 50; ++i) {
        EXPECT_EQ(std::string(records[i].payload.begin(), records[i].payload.end()),
                  "session-A-" + std::to_string(i));
    }

    std::filesystem::remove_all(dir);
}