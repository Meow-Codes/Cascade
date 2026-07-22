#include <gtest/gtest.h>
#include "net/framing.hpp"

using namespace cascade::core::net;

TEST(Framing, EncodeDecodeRoundTrip) {
    std::string msg = "hello cascade";
    auto frame = encode_frame(msg);

    FrameDecoder decoder;
    decoder.feed(frame.data(), frame.size());
    auto out = decoder.try_extract();
    ASSERT_TRUE(out.has_value());
    std::string result(out->begin(), out->end());
    EXPECT_EQ(result, msg);
}

TEST(Framing, HandlesSplitAcrossMultipleFeeds) {
    std::string msg = "a message split across several recv() calls";
    auto frame = encode_frame(msg);

    FrameDecoder decoder;
    // simulate TCP splitting the frame into 3 arbitrary chunks
    decoder.feed(frame.data(), 2);
    EXPECT_FALSE(decoder.try_extract().has_value());
    decoder.feed(frame.data() + 2, 3);
    EXPECT_FALSE(decoder.try_extract().has_value());
    decoder.feed(frame.data() + 5, frame.size() - 5);

    auto out = decoder.try_extract();
    ASSERT_TRUE(out.has_value());
    EXPECT_EQ(std::string(out->begin(), out->end()), msg);
}

TEST(Framing, HandlesMultipleFramesInOneFeed) {
    auto f1 = encode_frame(std::string("first"));
    auto f2 = encode_frame(std::string("second"));
    std::vector<std::uint8_t> combined;
    combined.insert(combined.end(), f1.begin(), f1.end());
    combined.insert(combined.end(), f2.begin(), f2.end());

    FrameDecoder decoder;
    decoder.feed(combined.data(), combined.size());

    auto out1 = decoder.try_extract();
    auto out2 = decoder.try_extract();
    ASSERT_TRUE(out1.has_value());
    ASSERT_TRUE(out2.has_value());
    EXPECT_EQ(std::string(out1->begin(), out1->end()), "first");
    EXPECT_EQ(std::string(out2->begin(), out2->end()), "second");
}

// Fuzz-lite: malformed/hostile length prefixes must throw, never crash or
// hang. Run this under ASan/UBSan in CI — that's your fuzz coverage for
// Phase 2's "fuzz the deserializer" requirement at the framing layer.
TEST(Framing, RejectsOversizedLengthPrefixInsteadOfCrashing) {
    std::uint8_t evil[4] = {0xFF, 0xFF, 0xFF, 0xFF}; // length = 4294967295
    FrameDecoder decoder;
    decoder.feed(evil, 4);
    EXPECT_THROW(decoder.try_extract(), std::runtime_error);
}

TEST(Framing, EmptyPayloadRoundTrips) {
    auto frame = encode_frame(std::string(""));
    FrameDecoder decoder;
    decoder.feed(frame.data(), frame.size());
    auto out = decoder.try_extract();
    ASSERT_TRUE(out.has_value());
    EXPECT_TRUE(out->empty());
}