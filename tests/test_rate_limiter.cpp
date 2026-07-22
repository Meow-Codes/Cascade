// tests/test_rate_limiter.cpp
#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include "net/rate_limiter.hpp"

using namespace cascade::core::net;
using namespace std::chrono_literals;

TEST(RateLimiter, AllowsBurstUpToCapacity) {
    RateLimiter limiter(/*rate_per_sec=*/10.0, /*burst_capacity=*/5.0);
    for (int i = 0; i < 5; ++i) EXPECT_TRUE(limiter.try_consume(1.0));
    EXPECT_FALSE(limiter.try_consume(1.0)); // bucket exhausted
}

TEST(RateLimiter, RefillsOverTime) {
    RateLimiter limiter(/*rate_per_sec=*/100.0, /*burst_capacity=*/2.0);
    EXPECT_TRUE(limiter.try_consume(2.0)); // drain it
    EXPECT_FALSE(limiter.try_consume(1.0));

    std::this_thread::sleep_for(50ms); // should refill ~5 tokens' worth at 100/sec, capped at capacity=2
    EXPECT_TRUE(limiter.try_consume(1.0));
}

TEST(RateLimiter, NeverExceedsBurstCapacity) {
    RateLimiter limiter(1000.0, 3.0);
    std::this_thread::sleep_for(100ms); // plenty of time to over-refill if capping were broken
    EXPECT_NEAR(limiter.tokens_available(), 3.0, 0.01);
}