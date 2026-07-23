#include <gtest/gtest.h>
#include <random>
#include <filesystem>
#include <vector>
#include "storage/log.hpp"

using namespace cascade::core::storage;

// Property: for any sequence of random-length writes, reading back any
// offset in any order returns exactly the bytes that were written there.
TEST(StorageProperty, RandomWriteReadRoundTripAnyOrder) {
    auto dir = std::string("/tmp/cascade_property_test_") + std::to_string(::getpid());
    std::filesystem::remove_all(dir);
    Log log(dir, 1 << 16); // small-ish segments to also exercise rollover

    std::mt19937 rng(42); // fixed seed: reproducible failures
    std::uniform_int_distribution<int> len_dist(0, 512);
    std::uniform_int_distribution<int> byte_dist(0, 255);

    constexpr int kRecords = 2000;
    std::vector<std::vector<std::uint8_t>> written(kRecords);

    for (int i = 0; i < kRecords; ++i) {
        int len = len_dist(rng);
        if (len == 0) len = 1; // avoid the documented offset-0-empty-payload edge case
        std::vector<std::uint8_t> payload(len);
        for (auto& b : payload) b = static_cast<std::uint8_t>(byte_dist(rng));
        written[i] = payload;
        auto offset = log.append(payload.data(), static_cast<std::uint32_t>(payload.size()));
        ASSERT_EQ(offset, static_cast<std::uint64_t>(i));
    }
    log.flush();

    // Read back in a shuffled order -- reading is not required to follow
    // write order, unlike a naive sequential-only log reader.
    std::vector<int> read_order(kRecords);
    for (int i = 0; i < kRecords; ++i) read_order[i] = i;
    std::shuffle(read_order.begin(), read_order.end(), rng);

    for (int idx : read_order) {
        auto result = log.read(static_cast<std::uint64_t>(idx));
        ASSERT_TRUE(result.has_value()) << "missing offset " << idx;
        EXPECT_EQ(result->payload, written[idx]) << "mismatch at offset " << idx;
    }

    std::filesystem::remove_all(dir);
}