#include <chrono>
#include <cstdio>
#include <filesystem>
#include <random>
#include <string>
#include <vector>
#include "storage/log.hpp"

using namespace cascade::core::storage;
using Clock = std::chrono::steady_clock;

static void bench_write_throughput() {
    auto dir = std::string("/tmp/cascade_bench_write_") + std::to_string(::getpid());
    std::filesystem::remove_all(dir);
    Log log(dir, 64 * 1024 * 1024); // 64 MiB segments

    constexpr int kRecords = 200000;
    constexpr int kPayloadSize = 512;
    std::vector<std::uint8_t> payload(kPayloadSize, 0xAB);

    auto start = Clock::now();
    for (int i = 0; i < kRecords; ++i) {
        log.append(payload.data(), kPayloadSize);
    }
    log.flush();
    auto end = Clock::now();

    double secs = std::chrono::duration<double>(end - start).count();
    double mb = (static_cast<double>(kRecords) * kPayloadSize) / (1024.0 * 1024.0);
    std::printf("Write throughput: %d records x %d bytes = %.1f MB in %.4f s -> %.1f MB/s\n",
                kRecords, kPayloadSize, mb, secs, mb / secs);

    std::filesystem::remove_all(dir);
}

static void bench_read_latency() {
    auto dir = std::string("/tmp/cascade_bench_read_") + std::to_string(::getpid());
    std::filesystem::remove_all(dir);
    Log log(dir, 64 * 1024 * 1024);

    constexpr int kRecords = 100000;
    std::vector<std::uint8_t> payload(256, 0xCD);
    for (int i = 0; i < kRecords; ++i) log.append(payload.data(), payload.size());
    log.flush();

    std::mt19937 rng(7);
    std::uniform_int_distribution<int> dist(0, kRecords - 1);
    constexpr int kReads = 50000;
    std::vector<double> latencies_us;
    latencies_us.reserve(kReads);

    for (int i = 0; i < kReads; ++i) {
        int target = dist(rng);
        auto t0 = Clock::now();
        auto result = log.read(static_cast<std::uint64_t>(target));
        auto t1 = Clock::now();
        (void)result;
        latencies_us.push_back(std::chrono::duration<double, std::micro>(t1 - t0).count());
    }

    std::sort(latencies_us.begin(), latencies_us.end());
    double p50 = latencies_us[latencies_us.size() * 50 / 100];
    double p99 = latencies_us[latencies_us.size() * 99 / 100];
    std::printf("Read latency (random offset, n=%d): p50 = %.2f us, p99 = %.2f us\n", kReads, p50, p99);

    std::filesystem::remove_all(dir);
}

static void bench_recovery_time(int record_count) {
    auto dir = std::string("/tmp/cascade_bench_recovery_") + std::to_string(::getpid());
    std::filesystem::remove_all(dir);
    {
        Log log(dir, 64 * 1024 * 1024);
        std::vector<std::uint8_t> payload(256, 0xEF);
        for (int i = 0; i < record_count; ++i) log.append(payload.data(), payload.size());
        log.flush();
    }

    auto start = Clock::now();
    Log reopened(dir, 64 * 1024 * 1024);
    auto end = Clock::now();
    (void)reopened;

    double ms = std::chrono::duration<double, std::milli>(end - start).count();
    std::printf("Recovery time for %d records: %.2f ms\n", record_count, ms);

    std::filesystem::remove_all(dir);
}

int main() {
    std::printf("--- Storage engine benchmark ---\n");
    bench_write_throughput();
    bench_read_latency();
    bench_recovery_time(10000);
    bench_recovery_time(100000);
    bench_recovery_time(500000);
    return 0;
}