#include <gtest/gtest.h>
#include "media/priority_queue.hpp"

using namespace cascade::core::media;

TEST(PriorityPacketQueue, HighPriorityDrainsFirst) {
    PriorityPacketQueue q;
    q.push(PacketPriority::Normal, {1});
    q.push(PacketPriority::Normal, {2});
    q.push(PacketPriority::High, {99});

    auto first = q.pop();
    ASSERT_TRUE(first.has_value());
    EXPECT_EQ(*first, (std::vector<std::uint8_t>{99}));

    auto second = q.pop();
    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(*second, (std::vector<std::uint8_t>{1})); // Normal FIFO order preserved after High drained
}

TEST(PriorityPacketQueue, EmptyReturnsNullopt) {
    PriorityPacketQueue q;
    EXPECT_FALSE(q.pop().has_value());
}