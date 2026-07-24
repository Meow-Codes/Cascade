#include <gtest/gtest.h>
#include "media/retransmit_cache.hpp"

using namespace cascade::core::media;

TEST(RetransmitCache, StoresAndRetrieves) {
    RetransmitCache cache(4);
    cache.store(1, {1, 2, 3});
    auto v = cache.get(1);
    ASSERT_TRUE(v.has_value());
    EXPECT_EQ(*v, (std::vector<std::uint8_t>{1, 2, 3}));
}

TEST(RetransmitCache, MissingSequenceReturnsNullopt) {
    RetransmitCache cache(4);
    EXPECT_FALSE(cache.get(99).has_value());
}

TEST(RetransmitCache, EvictsOldestBeyondCapacity) {
    RetransmitCache cache(3);
    for (std::uint32_t i = 0; i < 5; ++i) cache.store(i, {static_cast<std::uint8_t>(i)});
    EXPECT_FALSE(cache.get(0).has_value()); // evicted
    EXPECT_FALSE(cache.get(1).has_value()); // evicted
    EXPECT_TRUE(cache.get(4).has_value());  // most recent, still present
}