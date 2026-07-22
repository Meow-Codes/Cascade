#include <gtest/gtest.h>
#include <thread>
#include <vector>
#include <atomic>
#include "queue/mpmc_queue.hpp"

using cascade::core::MpmcQueue;

TEST(MpmcQueue, PushPopSingleThread) {
    MpmcQueue<int> q(8);
    EXPECT_TRUE(q.try_push(42));
    EXPECT_EQ(q.try_pop().value(), 42);
    EXPECT_FALSE(q.try_pop().has_value());
}

TEST(MpmcQueue, FullQueueRejectsPush) {
    MpmcQueue<int> q(4);
    int pushed = 0;
    while (q.try_push(pushed)) pushed++;
    EXPECT_FALSE(q.try_push(999));
}

// The core stress test: N producers, M consumers, verify every produced
// value is consumed exactly once (no loss, no duplication). Run this
// binary under ThreadSanitizer (see build instructions below) — that's
// what actually proves the lock-free logic, not just this assertion.
TEST(MpmcQueue, StressNProducersMConsumersNoLossNoDuplication) {
    constexpr int kProducers = 4;
    constexpr int kConsumers = 4;
    constexpr int kItemsPerProducer = 50'000;
    constexpr int kTotal = kProducers * kItemsPerProducer;

    MpmcQueue<int> q(1024);
    std::atomic<int> produced_count{0};
    std::atomic<int> consumed_count{0};
    std::vector<std::atomic<int>> seen(kTotal); // seen[i] counts how many times value i was popped
    for (auto& s : seen) s = 0;

    std::vector<std::thread> producers, consumers;

    for (int p = 0; p < kProducers; ++p) {
        producers.emplace_back([&, p] {
            for (int i = 0; i < kItemsPerProducer; ++i) {
                int value = p * kItemsPerProducer + i; // globally unique value
                while (!q.try_push(value)) { std::this_thread::yield(); }
                produced_count.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    for (int c = 0; c < kConsumers; ++c) {
        consumers.emplace_back([&] {
            while (consumed_count.load(std::memory_order_acquire) < kTotal) {
                if (auto v = q.try_pop()) {
                    seen[*v].fetch_add(1, std::memory_order_relaxed);
                    consumed_count.fetch_add(1, std::memory_order_relaxed);
                } else {
                    std::this_thread::yield();
                }
            }
        });
    }

    for (auto& t : producers) t.join();
    for (auto& t : consumers) t.join();

    EXPECT_EQ(produced_count.load(), kTotal);
    EXPECT_EQ(consumed_count.load(), kTotal);
    for (int i = 0; i < kTotal; ++i) {
        EXPECT_EQ(seen[i].load(), 1) << "value " << i << " seen " << seen[i].load() << " times";
    }
}