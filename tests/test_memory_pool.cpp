#include <gtest/gtest.h>
#include <thread>
#include <vector>
#include <unordered_set>
#include <mutex>
#include "memory/memory_pool.hpp"

using cascade::core::MemoryPool;

struct Block64 { char data[64]; };

TEST(MemoryPool, AllocateDeallocateSingleThread) {
    MemoryPool<Block64> pool(16);
    void* p1 = pool.allocate();
    void* p2 = pool.allocate();
    EXPECT_NE(p1, p2);
    EXPECT_EQ(pool.live_allocations(), 2u);
    pool.deallocate(p1);
    EXPECT_EQ(pool.live_allocations(), 1u);
    pool.deallocate(p2);
    EXPECT_EQ(pool.live_allocations(), 0u);
}

TEST(MemoryPool, ReusesFreedBlocks) {
    MemoryPool<Block64> pool(4);
    void* p1 = pool.allocate();
    pool.deallocate(p1);
    void* p2 = pool.allocate();
    EXPECT_EQ(p1, p2); // freed block should be recycled, not a fresh slab block
}

TEST(MemoryPool, GrowsWhenExhausted) {
    MemoryPool<Block64> pool(4); // small slab to force growth
    std::vector<void*> ptrs;
    for (int i = 0; i < 20; ++i) ptrs.push_back(pool.allocate());
    EXPECT_GT(pool.slab_count(), 1u);
    for (auto* p : ptrs) pool.deallocate(p);
}

TEST(MemoryPool, ConcurrentAllocDeallocNoOverlap) {
    MemoryPool<Block64> pool(64);
    constexpr int kThreads = 8;
    constexpr int kOpsPerThread = 20'000;

    std::mutex seen_mutex;
    std::unordered_set<void*> globally_live;

    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&] {
            std::vector<void*> local;
            for (int i = 0; i < kOpsPerThread; ++i) {
                void* p = pool.allocate();
                {
                    std::lock_guard<std::mutex> lock(seen_mutex);
                    // No other live thread should currently hold this exact pointer.
                    ASSERT_TRUE(globally_live.insert(p).second) << "pointer handed out twice while live!";
                }
                local.push_back(p);

                if (local.size() > 8) {
                    void* to_free = local.back();
                    local.pop_back();
                    {
                        std::lock_guard<std::mutex> lock(seen_mutex);
                        globally_live.erase(to_free);
                    }
                    pool.deallocate(to_free);
                }
            }
            for (auto* p : local) {
                {
                    std::lock_guard<std::mutex> lock(seen_mutex);
                    globally_live.erase(p);
                }
                pool.deallocate(p);
            }
        });
    }
    for (auto& th : threads) th.join();
    EXPECT_EQ(pool.live_allocations(), 0u);
}