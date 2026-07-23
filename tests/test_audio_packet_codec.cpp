#include <gtest/gtest.h>
#include "media/audio_packet.hpp"

using namespace cascade::core::media;

TEST(AudioPacketCodec, AudioRoundTrip) {
    std::vector<std::uint8_t> payload = {1, 2, 3, 4, 5};
    auto bytes = encode_audio_packet(42, 840, kFlagSilence, payload.data(), payload.size());
    auto decoded = decode_packet(bytes.data(), bytes.size());

    ASSERT_EQ(decoded.kind, PacketKind::Audio);
    EXPECT_EQ(decoded.sequence, 42u);
    EXPECT_EQ(decoded.timestamp_ms, 840u);
    EXPECT_TRUE(decoded.flags & kFlagSilence);
    EXPECT_EQ(decoded.payload, payload);
}

TEST(AudioPacketCodec, NackRoundTrip) {
    auto bytes = encode_nack(777);
    auto decoded = decode_packet(bytes.data(), bytes.size());
    ASSERT_EQ(decoded.kind, PacketKind::Nack);
    EXPECT_EQ(decoded.sequence, 777u);
}

TEST(AudioPacketCodec, RejectsTruncatedAudioPacketWithoutCrashing) {
    std::vector<std::uint8_t> payload = {1, 2, 3};
    auto bytes = encode_audio_packet(1, 20, 0, payload.data(), payload.size());
    bytes.resize(bytes.size() - 1); // truncate: length field now lies
    auto decoded = decode_packet(bytes.data(), bytes.size());
    EXPECT_EQ(decoded.kind, PacketKind::Malformed);
}

TEST(AudioPacketCodec, EmptyBufferIsMalformedNotCrash) {
    auto decoded = decode_packet(nullptr, 0);
    EXPECT_EQ(decoded.kind, PacketKind::Malformed);
}