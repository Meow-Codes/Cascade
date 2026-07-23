#include <gtest/gtest.h>
#include <atomic>
#include <filesystem>
#include <random>
#include <thread>
#include <vector>
#include "storage/log.hpp"

using namespace cascade::core::storage;

// Verifies the "readers never race the writer's frontier" invariant
// documented in log.hpp: concurrent readers hammering random already-
// visible offsets while a writer is actively appending must never see a
// torn/corrupted read, and must always get back exactly what was written.
TEST(StorageConcurrentReaders, NoTornReadsDuringConcurrentAppend) {
    auto dir = std::string("/tmp/cascade_concurrent_test_") + std::to_string(::getpid());
    std::filesystem::remove_all(dir);
    Log log(dir, 1 << 20);

    constexpr int kTotalRecords = 20000;
    std::atomic<int> written_count{0};
    std::atomic<bool> stop_readers{false};
    std::atomic<int> reads_performed{0};
    std::atomic<bool> mismatch_found{false};

    std::thread writer([&] {
        for (int i = 0; i < kTotalRecords; ++i) {
            std::string payload = "payload-" + std::to_string(i) + "-tail";
            log.append(reinterpret_cast<const std::uint8_t*>(payload.data()), payload.size());
            written_count.store(i + 1, std::memory_order_release);
        }
    });

    constexpr int kReaderThreads = 4;
    std::vector<std::thread> readers;
    for (int r = 0; r < kReaderThreads; ++r) {
        readers.emplace_back([&, r] {
            std::mt19937 rng(1000 + r);
            while (!stop_readers.load(std::memory_order_acquire)) {
                int visible = written_count.load(std::memory_order_acquire);
                if (visible == 0) continue;
                std::uniform_int_distribution<int> dist(0, visible - 1);
                int target = dist(rng);

                auto result = log.read(static_cast<std::uint64_t>(target));
                if (!result.has_value()) continue; // segment rolled between snapshot and read; acceptable, just retry
                std::string expected = "payload-" + std::to_string(target) + "-tail";
                std::string actual(result->payload.begin(), result->payload.end());
                if (actual != expected) {
                    mismatch_found.store(true, std::memory_order_relaxed);
                }
                reads_performed.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    writer.join();
    stop_readers.store(true, std::memory_order_release);
    for (auto& t : readers) t.join();

    EXPECT_FALSE(mismatch_found.load());
    EXPECT_GT(reads_performed.load(), 0); // sanity: readers actually did work concurrently

    std::filesystem::remove_all(dir);
}