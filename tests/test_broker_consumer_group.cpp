#include <gtest/gtest.h>
#include <filesystem>
#include <set>
#include <unistd.h>
#include "broker/broker.hpp"
#include "broker/producer.hpp"
#include "broker/consumer.hpp"

using namespace cascade::core::broker;

TEST(BrokerConsumerGroup, NoDoubleDeliveryAcrossGroupMembers) {
    auto dir = std::string("/tmp/cascade_broker_group_test_") + std::to_string(::getpid());
    std::filesystem::remove_all(dir);

    Broker broker(dir);
    auto topic = broker.create_topic("shared", /*num_partitions=*/4);
    Producer producer(topic);

    constexpr int kMessages = 2000;
    std::set<std::string> published;
    for (int i = 0; i < kMessages; ++i) {
        std::string payload = "msg-" + std::to_string(i);
        published.insert(payload);
        producer.publish(reinterpret_cast<const std::uint8_t*>(payload.data()), payload.size());
    }

    // Two consumers sharing one group, splitting the 4 partitions 2-and-2.
    Consumer consumer_a(topic, "group-1", /*group_size=*/2, /*consumer_index=*/0, broker.offset_store());
    Consumer consumer_b(topic, "group-1", /*group_size=*/2, /*consumer_index=*/1, broker.offset_store());

    std::set<std::string> received;
    int total_received = 0;

    auto drain = [&](Consumer& c) {
        while (true) {
            auto batch = c.poll(500);
            if (batch.empty()) break;
            for (auto& rec : batch) {
                std::string payload(rec.payload.begin(), rec.payload.end());
                ASSERT_TRUE(received.insert(payload).second) << "DUPLICATE DELIVERY: " << payload;
                total_received++;
            }
            c.commit();
        }
    };
    drain(consumer_a);
    drain(consumer_b);

    EXPECT_EQ(total_received, kMessages);
    EXPECT_EQ(received, published); // exact set match: nothing missing, nothing duplicated

    std::filesystem::remove_all(dir);
}