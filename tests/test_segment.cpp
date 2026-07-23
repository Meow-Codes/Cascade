#include <gtest/gtest.h>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <unistd.h>
#include "storage/segment.hpp"

using namespace cascade::core::storage;

static std::string temp_path(const char* name) {
    return std::string("/tmp/cascade_test_") + name + "_" + std::to_string(::getpid()) + ".seg";
}

TEST(Segment, AppendReadRoundTrip) {
    auto path = temp_path("roundtrip");
    ::remove(path.c_str());
    {
        Segment seg(path, 0, 1 << 20);
        std::string a = "first record";
        std::string b = "second record, a bit longer";
        auto off_a = seg.append(reinterpret_cast<const std::uint8_t*>(a.data()), a.size());
        auto off_b = seg.append(reinterpret_cast<const std::uint8_t*>(b.data()), b.size());
        ASSERT_TRUE(off_a.has_value());
        ASSERT_TRUE(off_b.has_value());
        EXPECT_EQ(*off_a, 0u);
        EXPECT_EQ(*off_b, 1u);

        auto read_a = seg.read_from(0, 0);
        auto read_b = seg.read_from(1, 0);
        ASSERT_TRUE(read_a.has_value());
        ASSERT_TRUE(read_b.has_value());
        EXPECT_EQ(std::string(read_a->payload.begin(), read_a->payload.end()), a);
        EXPECT_EQ(std::string(read_b->payload.begin(), read_b->payload.end()), b);
    }
    ::remove(path.c_str());
}

TEST(Segment, RejectsAppendPastCapacity) {
    auto path = temp_path("capacity");
    ::remove(path.c_str());
    Segment seg(path, 0, 64); // tiny segment
    std::string payload(100, 'x'); // won't fit
    auto off = seg.append(reinterpret_cast<const std::uint8_t*>(payload.data()), payload.size());
    EXPECT_FALSE(off.has_value());
    ::remove(path.c_str());
}

TEST(Segment, DetectsTornWriteViaCrcAndTruncatesCorrectly) {
    auto path = temp_path("torn");
    ::remove(path.c_str());
    {
        Segment seg(path, 0, 1 << 20);
        std::string a = "intact record";
        std::string b = "this one will be corrupted";
        seg.append(reinterpret_cast<const std::uint8_t*>(a.data()), a.size());
        seg.append(reinterpret_cast<const std::uint8_t*>(b.data()), b.size());
        seg.flush();
    } // Segment destructs: munmap + close

    // Directly corrupt one byte of record b's payload, simulating a torn
    // write that got past the header but has inconsistent payload bytes.
    int fd = ::open(path.c_str(), O_RDWR);
    ASSERT_GE(fd, 0);
    // record a: 16 header + 13 payload = 29 bytes. record b payload starts at 29+16=45.
    std::uint8_t corrupt_byte = 0xFF;
    ssize_t written = ::pwrite(fd, &corrupt_byte, 1, 45);
    ASSERT_EQ(written, 1);
    ::close(fd);

    Segment reopened(path, 0, 1 << 20);
    // Record a should still be readable; record b should have been
    // truncated away by replay() due to CRC mismatch.
    auto read_a = reopened.read_from(0, 0);
    auto read_b = reopened.read_from(1, 0);
    ASSERT_TRUE(read_a.has_value());
    EXPECT_FALSE(read_b.has_value());
    EXPECT_EQ(reopened.next_offset(), 1u); // recovery stopped after record 0

    ::remove(path.c_str());
}

TEST(Segment, SealTruncatesFileToActualSize) {
    auto path = temp_path("seal");
    ::remove(path.c_str());
    Segment seg(path, 0, 1 << 20); // 1 MiB pre-allocated
    std::string a = "small";
    seg.append(reinterpret_cast<const std::uint8_t*>(a.data()), a.size());
    seg.seal();

    struct stat st{};
    ::stat(path.c_str(), &st);
    EXPECT_LT(static_cast<std::size_t>(st.st_size), 1u << 20); // much smaller than max_bytes now
    EXPECT_EQ(static_cast<std::size_t>(st.st_size), seg.write_pos());

    ::remove(path.c_str());
}