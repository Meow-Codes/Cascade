#pragma once
// Cascade :: core::net :: framing
//
// Length-prefixed message framing over a byte stream (TCP has no message
// boundaries, so this is required for anything beyond toy examples).
//
// Wire format: [4-byte big-endian length][payload bytes], length excludes
// the 4-byte header itself.
//
// This is deliberately protocol-agnostic — the payload bytes are opaque
// here. Protobuf serialization plugs in as "the thing that produces/parses
// the payload," kept as a separate concern from framing so framing can be
// unit-tested (including fuzzed) without pulling in the protobuf runtime.
// A minimal handwritten protobuf-wire-format encoder is out of scope for
// Phase 2 core; integrate real `libprotobuf` via `find_package(Protobuf)`
// when you get to defining your first .proto schema — the framing below
// doesn't change either way.

#include <cstdint>
#include <cstring>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

namespace cascade::core::net {

constexpr std::uint32_t kMaxFrameSize = 16 * 1024 * 1024; // 16 MiB hard cap: a malformed/hostile
                                                            // length prefix should never make us
                                                            // try to allocate gigabytes.

inline std::vector<std::uint8_t> encode_frame(const std::uint8_t* payload, std::uint32_t len) {
    if (len > kMaxFrameSize) throw std::runtime_error("payload exceeds kMaxFrameSize");
    std::vector<std::uint8_t> out(4 + len);
    out[0] = static_cast<std::uint8_t>((len >> 24) & 0xFF);
    out[1] = static_cast<std::uint8_t>((len >> 16) & 0xFF);
    out[2] = static_cast<std::uint8_t>((len >> 8) & 0xFF);
    out[3] = static_cast<std::uint8_t>(len & 0xFF);
    if (len > 0) std::memcpy(out.data() + 4, payload, len);
    return out;
}

inline std::vector<std::uint8_t> encode_frame(const std::string& payload) {
    return encode_frame(reinterpret_cast<const std::uint8_t*>(payload.data()),
                         static_cast<std::uint32_t>(payload.size()));
}

// Incremental frame parser: feed it arbitrary chunks of bytes as they
// arrive off the socket (which may split or coalesce frames arbitrarily —
// that's the whole reason framing is needed) and drain complete frames
// as they become available.
class FrameDecoder {
public:
    // Appends raw bytes received from the socket.
    void feed(const std::uint8_t* data, std::size_t len) {
        buffer_.insert(buffer_.end(), data, data + len);
    }

    // Returns the next complete frame's payload if one is available, else
    // nullopt. Call repeatedly after each feed() — a single feed() can
    // contain zero, one, or many complete frames.
    // Throws std::runtime_error on a malformed (oversized) length prefix —
    // callers should treat this as "drop the connection," not a crash.
    std::optional<std::vector<std::uint8_t>> try_extract() {
        if (buffer_.size() < 4) return std::nullopt;

        std::uint32_t len = (static_cast<std::uint32_t>(buffer_[0]) << 24) |
                             (static_cast<std::uint32_t>(buffer_[1]) << 16) |
                             (static_cast<std::uint32_t>(buffer_[2]) << 8) |
                             (static_cast<std::uint32_t>(buffer_[3]));

        if (len > kMaxFrameSize) {
            throw std::runtime_error("frame length exceeds kMaxFrameSize: malformed or hostile input");
        }

        if (buffer_.size() < 4u + len) return std::nullopt; // incomplete frame, wait for more bytes

        std::vector<std::uint8_t> payload(buffer_.begin() + 4, buffer_.begin() + 4 + len);
        buffer_.erase(buffer_.begin(), buffer_.begin() + 4 + len);
        return payload;
    }

    std::size_t buffered_bytes() const { return buffer_.size(); }

private:
    std::vector<std::uint8_t> buffer_;
};

} // namespace cascade::core::net