#include <gtest/gtest.h>
#include <atomic>
#include <vector>
#include "threadpool/thread_pool.hpp"

using cascade::core::ThreadPool;

TEST(ThreadPool, RunsSimpleTaskAndReturnsResult) {
    ThreadPool pool(4);
    auto future = pool.submit([] { return 21 * 2; });
    EXPECT_EQ(future.get(), 42);
}

TEST(ThreadPool, RunsManyTasksConcurrently) {
    ThreadPool pool(8);
    constexpr int kTasks = 10'000;
    std::atomic<int> counter{0};

    std::vector<std::future<void>> futures;
    futures.reserve(kTasks);
    for (int i = 0; i < kTasks; ++i) {
        futures.push_back(pool.submit([&] { counter.fetch_add(1, std::memory_order_relaxed); }));
    }
    for (auto& f : futures) f.get();

    EXPECT_EQ(counter.load(), kTasks);
}

TEST(ThreadPool, PropagatesExceptions) {
    ThreadPool pool(2);
    auto future = pool.submit([] () -> int { throw std::runtime_error("boom"); });
    EXPECT_THROW(future.get(), std::runtime_error);
}