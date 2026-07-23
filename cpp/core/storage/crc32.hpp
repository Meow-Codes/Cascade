#pragma once
// Cascade :: core::storage :: crc32
//
// Standard IEEE 802.3 CRC-32 (same polynomial as zlib/gzip/PNG/Ethernet).
// Table-based for reasonable throughput without an external dependency
// for a well-known, tiny algorithm. Used to detect torn/corrupted writes
// during segment replay.

#include <array>
#include <cstddef>
#include <cstdint>

namespace cascade::core::storage {

namespace detail {
inline std::array<std::uint32_t, 256> make_crc32_table() {
    std::array<std::uint32_t, 256> table{};
    for (std::uint32_t i = 0; i < 256; ++i) {
        std::uint32_t c = i;
        for (int k = 0; k < 8; ++k) {
            c = (c & 1) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
        }
        table[i] = c;
    }
    return table;
}
inline const std::array<std::uint32_t, 256>& crc32_table() {
    static const auto table = make_crc32_table();
    return table;
}
} // namespace detail

inline std::uint32_t crc32(const std::uint8_t* data, std::size_t len) {
    const auto& table = detail::crc32_table();
    std::uint32_t c = 0xFFFFFFFFu;
    for (std::size_t i = 0; i < len; ++i) {
        c = table[(c ^ data[i]) & 0xFF] ^ (c >> 8);
    }
    return c ^ 0xFFFFFFFFu;
}

} // namespace cascade::core::storage