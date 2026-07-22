#include <chrono>
#include <cstdio>
#include <vector>
#include "memory/memory_pool.hpp"

using cascade::core::MemoryPool;
using Clock = std::chrono::steady_clock;

struct Block64 { char data[64]; };

int main() {
    constexpr int kIters = 2'000'000;

    // Baseline: raw new/delete
    {
        auto start = Clock::now();
        for (int i = 0; i < kIters; ++i) {
            auto* p = new Block64();
            asm volatile("" : : "g"(p) : "memory"); // prevent optimizing away
            delete p;
        }
        auto end = Clock::now();
        double secs = std::chrono::duration<double>(end - start).count();
        std::printf("new/delete     : %d ops in %.4f s -> %.1f ns/op\n",
                    kIters, secs, (secs * 1e9) / kIters);
    }

    // Pooled allocation
    {
        MemoryPool<Block64> pool(4096);
        auto start = Clock::now();
        for (int i = 0; i < kIters; ++i) {
            void* p = pool.allocate();
            pool.deallocate(p);
        }
        auto end = Clock::now();
        double secs = std::chrono::duration<double>(end - start).count();
        std::printf("pooled alloc   : %d ops in %.4f s -> %.1f ns/op\n",
                    kIters, secs, (secs * 1e9) / kIters);
    }

    return 0;
}