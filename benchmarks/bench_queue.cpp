#include <chrono>
#include <cstdio>
#include <thread>
#include <vector>
#include "queue/spsc_queue.hpp"
#include "queue/mpmc_queue.hpp"

using cascade::core::SpscQueue;
using cascade::core::MpmcQueue;
using Clock = std::chrono::steady_clock;

static void bench_spsc(int items) {
    SpscQueue<int> q(1 << 16);
    auto start = Clock::now();

    std::thread producer([&] {
        for (int i = 0; i < items; ++i) while (!q.try_push(i)) {}
    });
    std::thread consumer([&] {
        int count = 0;
        while (count < items) if (q.try_pop()) count++;
    });
    producer.join();
    consumer.join();

    auto end = Clock::now();
    double secs = std::chrono::duration<double>(end - start).count();
    std::printf("SPSC  1P/1C : %10d ops in %8.4f s -> %12.0f ops/sec\n",
                items, secs, items / secs);
}

static void bench_mpmc(int producers, int consumers, int items_per_producer) {
    MpmcQueue<int> q(1 << 16);
    int total = producers * items_per_producer;
    std::atomic<int> consumed{0};

    auto start = Clock::now();

    std::vector<std::thread> prod, cons;
    for (int p = 0; p < producers; ++p) {
        prod.emplace_back([&, p] {
            for (int i = 0; i < items_per_producer; ++i) {
                int v = p * items_per_producer + i;
                while (!q.try_push(v)) {}
            }
        });
    }
    for (int c = 0; c < consumers; ++c) {
        cons.emplace_back([&] {
            while (consumed.load(std::memory_order_relaxed) < total) {
                if (q.try_pop()) consumed.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }
    for (auto& t : prod) t.join();
    for (auto& t : cons) t.join();

    auto end = Clock::now();
    double secs = std::chrono::duration<double>(end - start).count();
    std::printf("MPMC %dP/%dC : %10d ops in %8.4f s -> %12.0f ops/sec\n",
                producers, consumers, total, secs, total / secs);
}

int main() {
    std::printf("--- Queue throughput benchmark ---\n");
    bench_spsc(5'000'000);

    bench_mpmc(1, 1, 1'000'000);
    bench_mpmc(2, 2, 1'000'000);
    bench_mpmc(4, 4, 1'000'000);
    bench_mpmc(8, 8, 1'000'000);
    return 0;
}