#include <gtest/gtest.h>
#include <filesystem>
#include "storage/log.hpp"

using namespace cascade::core::storage;

static std::string temp_dir(const char* name) {
    auto dir = std::string("/tmp/cascade_log_test_") + name + "_" + std::to_string(::getpid());
    std::filesystem::remove_all(dir);
    return dir;
}

TEST(LogRecovery, WritesAcrossMultipleSegmentsAndReopensCleanly) {
    auto dir = temp_dir("multisegment");
    constexpr int kRecords = 500;
    constexpr std::size_t kSmallSegmentSize = 4096; // forces several rollovers

    {
        Log log(dir, kSmallSegmentSize);
        for (int i = 0; i < kRecords; ++i) {
            std::string payload = "record-" + std::to_string(i);
            log.append(reinterpret_cast<const std::uint8_t*>(payload.data()), payload.size());
        }
        log.flush();
    } // Log destructs, all Segments seal via their own destructors' munmap

    Log reopened(dir, kSmallSegmentSize);
    EXPECT_GT(reopened.segment_count(), 1u); // confirms rollover actually happened
    EXPECT_EQ(reopened.next_offset(), static_cast<std::uint64_t>(kRecords));

    for (int i = 0; i < kRecords; ++i) {
        auto result = reopened.read(static_cast<std::uint64_t>(i));
        ASSERT_TRUE(result.has_value()) << "missing offset " << i;
        std::string expected = "record-" + std::to_string(i);
        EXPECT_EQ(std::string(result->payload.begin(), result->payload.end()), expected);
    }

    std::filesystem::remove_all(dir);
}

TEST(LogRecovery, RecoversPartiallyWrittenActiveSegment) {
    auto dir = temp_dir("partial");
    {
        Log log(dir, 1 << 20);
        for (int i = 0; i < 10; ++i) {
            std::string payload = "ok-" + std::to_string(i);
            log.append(reinterpret_cast<const std::uint8_t*>(payload.data()), payload.size());
        }
        log.flush();
        // No explicit seal -- this simulates the active segment at time of
        // crash: file is still sparse/max-sized on disk, not truncated.
    }

    Log reopened(dir, 1 << 20);
    EXPECT_EQ(reopened.next_offset(), 10u);
    auto result = reopened.read(5);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(std::string(result->payload.begin(), result->payload.end()), "ok-5");

    std::filesystem::remove_all(dir);
}