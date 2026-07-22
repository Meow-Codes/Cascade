#include <gtest/gtest.h>
#include <thread>
#include <atomic>
#include <numeric>
#include "queue/spsc_queue.hpp"

using cascade::core::SpscQueue;

TEST(SpscQueue, PushPopSingleThread) {
    SpscQueue<int> q(8);
    EXPECT_TRUE(q.try_push(1));
    EXPECT_TRUE(q.try_push(2));
    EXPECT_EQ(q.try_pop().value(), 1);
    EXPECT_EQ(q.try_pop().value(), 2);
    EXPECT_FALSE(q.try_pop().has_value());
}

TEST(SpscQueue, FullQueueRejectsPush) {
    SpscQueue<int> q(4); // rounds up to power of 2 internally
    int pushed = 0;
    while (q.try_push(pushed)) pushed++;
    EXPECT_GT(pushed, 0);
    EXPECT_FALSE(q.try_push(999));
}

TEST(SpscQueue, PreservesFIFOOrder) {
    SpscQueue<int> q(1024);
    for (int i = 0; i < 500; ++i) EXPECT_TRUE(q.try_push(i));
    for (int i = 0; i < 500; ++i) EXPECT_EQ(q.try_pop().value(), i);
}

TEST(SpscQueue, ConcurrentProducerConsumerNoLoss) {
    constexpr int kCount = 1'000'000;
    SpscQueue<int> q(4096);
    std::atomic<bool> done{false};
    long long sum_produced = 0, sum_consumed = 0;

    std::thread producer([&] {
        for (int i = 0; i < kCount; ++i) {
            while (!q.try_push(i)) { /* spin */ }
            sum_produced += i;
        }
        done.store(true, std::memory_order_release);
    });

    std::thread consumer([&] {
        int consumed_count = 0;
        while (consumed_count < kCount) {
            if (auto v = q.try_pop()) {
                sum_consumed += *v;
                consumed_count++;
            }
        }
    });

    producer.join();
    consumer.join();
    EXPECT_EQ(sum_produced, sum_consumed);
}