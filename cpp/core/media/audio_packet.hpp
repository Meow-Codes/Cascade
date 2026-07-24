#pragma once
// Cascade :: core::media :: audio_packet
//
// Wire format for audio frames and NACK (retransmit request) control
// messages sharing one UDP port. First byte is a packet-type discriminator
// so decode_packet() can dispatch without a separate control channel.
//
// Audio packet: [type=0][seq:4][timestamp_ms:4][flags:1][payload_len:2][payload...]
// NACK packet:  [type=1][seq:4]   (the single sequence being requested)
//
// flags bit 0: silence (set by the sender's SilenceDetector, Phase 6's
// "silence detection" requirement -- carried on the wire so a receiver-
// side dashboard/metrics layer could later track silence ratio without
// re-running detection itself).

#include <cstdint>
#include <cstring>
#include <optional>
#include <vector>

namespace cascade::core::media {

constexpr std::uint8_t kFlagSilence = 0x01;

enum class PacketKind { Audio, Nack, Malformed };

struct DecodedPacket {
    PacketKind kind = PacketKind::Malformed;
    std::uint32_t sequence = 0;
    std::uint32_t timestamp_ms = 0;
    std::uint8_t flags = 0;
    std::vector<std::uint8_t> payload; // only for Audio
};

inline std::vector<std::uint8_t> encode_audio_packet(std::uint32_t sequence, std::uint32_t timestamp_ms,
                                                       std::uint8_t flags, const std::uint8_t* payload,
                                                       std::size_t payload_len) {
    std::vector<std::uint8_t> out(1 + 4 + 4 + 1 + 2 + payload_len);
    std::size_t pos = 0;
    out[pos++] = 0; // type = Audio
    std::memcpy(out.data() + pos, &sequence, 4); pos += 4;
    std::memcpy(out.data() + pos, &timestamp_ms, 4); pos += 4;
    out[pos++] = flags;
    std::uint16_t len16 = static_cast<std::uint16_t>(payload_len);
    std::memcpy(out.data() + pos, &len16, 2); pos += 2;
    if (payload != nullptr && payload_len > 0) {
        std::memcpy(out.data() + pos, payload, payload_len);
    }
    return out;
}

inline std::vector<std::uint8_t> encode_nack(std::uint32_t sequence) {
    std::vector<std::uint8_t> out(5);
    out[0] = 1; // type = Nack
    std::memcpy(out.data() + 1, &sequence, 4);
    return out;
}

inline DecodedPacket decode_packet(const std::uint8_t* data, std::size_t len) {
    DecodedPacket result;
    if (len < 1) return result;

    if (data[0] == 1) {
        if (len != 5) return result; // malformed NACK
        result.kind = PacketKind::Nack;
        std::memcpy(&result.sequence, data + 1, 4);
        return result;
    }

    if (data[0] != 0) return result; // unknown type
    constexpr std::size_t kHeaderSize = 1 + 4 + 4 + 1 + 2;
    if (len < kHeaderSize) return result;

    std::size_t pos = 1;
    std::memcpy(&result.sequence, data + pos, 4); pos += 4;
    std::memcpy(&result.timestamp_ms, data + pos, 4); pos += 4;
    result.flags = data[pos]; pos += 1;
    std::uint16_t payload_len;
    std::memcpy(&payload_len, data + pos, 2); pos += 2;

    if (pos + payload_len != len) return result; // length mismatch: malformed/truncated
    result.payload.assign(data + pos, data + pos + payload_len);
    result.kind = PacketKind::Audio;
    return result;
}

} // namespace cascade::core::media