#include <cstdio>
#include <filesystem>
#include <random>
#include <string>
#include <unistd.h>
#include <vector>

#include "perf/alloc_counter.hpp"
#include "storage/log.hpp"

using namespace cascade::core;

int main() {
    auto dir =
        std::string("/tmp/cascade_bench_alloc_") +
        std::to_string(::getpid());

    std::filesystem::remove_all(dir);

    storage::Log log(dir, 64 * 1024 * 1024);

    std::vector<std::uint8_t> payload(128, 0x1);

    constexpr int kRecords = 10000;

    for (int i = 0; i < kRecords; ++i) {
        log.append(payload.data(), payload.size());
    }

    constexpr int kReads = 100000;

    std::mt19937 rng(1);
    std::uniform_int_distribution<int> dist(0, kRecords - 1);

    auto before = perf::g_alloc_count.load();

    for (int i = 0; i < kReads; ++i) {
        volatile auto result =
            log.read(static_cast<std::uint64_t>(dist(rng)));
        (void)result;
    }

    auto after = perf::g_alloc_count.load();

    std::printf(
        "Allocations for %d reads: %lu total (%.2f per read)\n",
        kReads,
        after - before,
        static_cast<double>(after - before) / kReads);

    std::filesystem::remove_all(dir);

    return 0;
}