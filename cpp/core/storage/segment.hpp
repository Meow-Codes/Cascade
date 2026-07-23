#pragma once
// Cascade :: core::storage :: Segment
//
// A single mmap-backed segment file: append-only, fixed maximum capacity,
// pre-allocated (sparse) via ftruncate at creation so the mapping never
// needs resizing while being written to. Writes are plain memcpy into the
// mapped region -- this IS the "mmap-backed writes" requirement, not
// write()+fsync with mmap only used for reads.
//
// Record layout (16-byte header, fields always accessed via memcpy at
// byte-granularity -- never reinterpret_cast -- because records are
// variable-length so a given record's header is not guaranteed to start
// at an 8-byte-aligned file offset):
//   [8 bytes: uint64 offset][4 bytes: uint32 payload_len][4 bytes: uint32 crc32][payload...]
//
// Crash-safety model: because writes are memcpy into shared-mapped pages,
// any record whose memcpy fully completed before a process crash (kill -9,
// not power loss) survives in the page cache even without an explicit
// msync -- the kernel keeps dirty pages resident, it just hasn't
// necessarily written them to the block device yet. replay() scans
// records front-to-back, validating each one's CRC32, and treats the
// first invalid/incomplete/zeroed header as the true end of the log. That
// is what makes a torn write safe: recovery stops exactly at the last
// fully-intact record instead of reading garbage.
//
// Known limitation (documented, not silently swallowed): a segment whose
// base_offset is 0 and whose very first record legitimately has an empty
// payload produces a header bit-pattern indistinguishable from untouched
// sparse memory (offset=0, len=0, crc32(empty)=0). Replay would stop one
// record early in that specific case. Not an issue for this project --
// control/media frames are never legitimately empty -- but worth knowing
// if this code is reused elsewhere.

#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

#include "storage/crc32.hpp"

namespace cascade::core::storage {

constexpr std::size_t kRecordHeaderSize = 16;

struct ReadResult {
    std::uint64_t offset;
    std::vector<std::uint8_t> payload;
};

class Segment {
public:
    Segment(const std::string& path, std::uint64_t base_offset, std::size_t max_bytes)
        : path_(path), base_offset_(base_offset), max_bytes_(max_bytes) {

        bool exists = (::access(path.c_str(), F_OK) == 0);

        fd_ = ::open(path.c_str(), O_RDWR | O_CREAT, 0644);
        if (fd_ < 0) throw std::runtime_error("failed to open segment file: " + path);

        struct stat st{};
        if (::fstat(fd_, &st) != 0) throw std::runtime_error("fstat failed on " + path);

        std::size_t map_len = max_bytes_;
        if (exists && static_cast<std::size_t>(st.st_size) > 0 &&
            static_cast<std::size_t>(st.st_size) < max_bytes_) {
            // A previously-rolled (sealed) segment: file was truncated
            // down to its actual used size on a prior run.
            map_len = static_cast<std::size_t>(st.st_size);
            sealed_ = true;
        } else if (!exists || st.st_size == 0) {
            if (::ftruncate(fd_, static_cast<off_t>(max_bytes_)) != 0) {
                throw std::runtime_error("ftruncate failed on " + path);
            }
            map_len = max_bytes_;
        }
        // else: file already sized to max_bytes_ -- an active segment
        // that crashed before being sealed. map_len stays max_bytes_.

        mapped_len_ = map_len;
        int prot = sealed_ ? PROT_READ : (PROT_READ | PROT_WRITE);
        void* addr = ::mmap(nullptr, mapped_len_, prot, MAP_SHARED, fd_, 0);
        if (addr == MAP_FAILED) throw std::runtime_error("mmap failed on " + path);
        data_ = static_cast<std::uint8_t*>(addr);

        replay();
    }

    ~Segment() {
        if (data_) ::munmap(data_, mapped_len_);
        if (fd_ >= 0) ::close(fd_);
    }

    Segment(const Segment&) = delete;
    Segment& operator=(const Segment&) = delete;

    // Returns the assigned offset, or nullopt if the segment lacks room
    // (caller should seal() this segment and roll to a new one).
    std::optional<std::uint64_t> append(const std::uint8_t* payload, std::uint32_t len) {
        if (sealed_) return std::nullopt;

        std::size_t needed = kRecordHeaderSize + len;
        if (write_pos_ + needed > mapped_len_) return std::nullopt;

        std::uint64_t offset = next_offset_;
        std::uint32_t crc = crc32(payload, len);
        std::size_t record_pos = write_pos_;

        std::uint8_t* dst = data_ + record_pos;
        std::memcpy(dst, &offset, sizeof(offset));
        std::memcpy(dst + 8, &len, sizeof(len));
        std::memcpy(dst + 12, &crc, sizeof(crc));
        if (len > 0) std::memcpy(dst + kRecordHeaderSize, payload, len);

        write_pos_ += needed;
        next_offset_ = offset + 1;
        record_count_++;
        maybe_index(offset, record_pos, needed);
        return offset;
    }

    // Durability barrier: forces dirty pages up to write_pos_ to the
    // underlying block device. Callers decide cadence (e.g. ack the
    // producer only after this returns, per the durability-ordering
    // decision from the Phase 0 diagram fix).
    void flush() {
        if (write_pos_ > 0) ::msync(data_, write_pos_, MS_SYNC);
    }

    std::optional<ReadResult> read_from(std::uint64_t target_offset, std::size_t start_hint_pos) const {
        std::size_t pos = start_hint_pos;
        while (pos + kRecordHeaderSize <= write_pos_) {
            std::uint64_t rec_offset;
            std::uint32_t len, crc;
            std::memcpy(&rec_offset, data_ + pos, sizeof(rec_offset));
            std::memcpy(&len, data_ + pos + 8, sizeof(len));
            std::memcpy(&crc, data_ + pos + 12, sizeof(crc));

            if (pos + kRecordHeaderSize + len > write_pos_) break; // defensive; shouldn't happen post-replay

            if (rec_offset == target_offset) {
                ReadResult result;
                result.offset = rec_offset;
                result.payload.assign(data_ + pos + kRecordHeaderSize, data_ + pos + kRecordHeaderSize + len);
                return result;
            }
            if (rec_offset > target_offset) return std::nullopt;

            pos += kRecordHeaderSize + len;
        }
        return std::nullopt;
    }

    // Binary search over the sparse index for the best scan-start position
    // for target_offset. Returns 0 (scan from the beginning) if the index
    // is empty or target_offset precedes the first indexed entry.
    std::size_t position_hint_for(std::uint64_t target_offset) const {
        if (index_.empty()) return 0;
        auto it = std::upper_bound(index_.begin(), index_.end(), target_offset,
            [](std::uint64_t off, const IndexEntry& e) { return off < e.offset; });
        if (it == index_.begin()) return 0;
        --it;
        return it->pos;
    }

    // Seals the segment: truncates the backing file down to the actual
    // used size (undoing the sparse pre-allocation) and remaps read-only.
    // Called on rollover so old segments don't waste disk space equal to
    // max_bytes each.
    void seal() {
        if (sealed_) return;
        ::msync(data_, write_pos_, MS_SYNC);
        ::munmap(data_, mapped_len_);
        ::ftruncate(fd_, static_cast<off_t>(write_pos_));

        mapped_len_ = write_pos_ > 0 ? write_pos_ : 1; // mmap requires length > 0
        void* addr = ::mmap(nullptr, mapped_len_, PROT_READ, MAP_SHARED, fd_, 0);
        if (addr == MAP_FAILED) throw std::runtime_error("mmap failed while sealing " + path_);
        data_ = static_cast<std::uint8_t*>(addr);
        sealed_ = true;
    }

    std::uint64_t base_offset() const { return base_offset_; }
    std::uint64_t next_offset() const { return next_offset_; }
    std::size_t write_pos() const { return write_pos_; }
    std::size_t record_count() const { return record_count_; }
    bool is_sealed() const { return sealed_; }
    const std::string& path() const { return path_; }

private:
    struct IndexEntry { std::uint64_t offset; std::size_t pos; };
    static constexpr std::size_t kIndexIntervalBytes = 4096;

    void maybe_index(std::uint64_t offset, std::size_t pos, std::size_t record_size) {
        bytes_since_last_index_ += record_size;
        if (index_.empty() || bytes_since_last_index_ >= kIndexIntervalBytes) {
            index_.push_back({offset, pos});
            bytes_since_last_index_ = 0;
        }
    }

    void replay() {
        std::size_t pos = 0;
        std::uint64_t expected_offset = base_offset_;
        index_.clear();
        bytes_since_last_index_ = 0;

        while (pos + kRecordHeaderSize <= mapped_len_) {
            std::uint64_t rec_offset;
            std::uint32_t len, crc;
            std::memcpy(&rec_offset, data_ + pos, sizeof(rec_offset));
            std::memcpy(&len, data_ + pos + 8, sizeof(len));
            std::memcpy(&crc, data_ + pos + 12, sizeof(crc));

            if (rec_offset == 0 && len == 0 && crc == 0) break; // untouched sparse territory (see known limitation above)
            if (rec_offset != expected_offset) break;            // gap or corruption
            if (pos + kRecordHeaderSize + len > mapped_len_) break; // truncated record: crash mid-write

            std::uint32_t actual_crc = crc32(data_ + pos + kRecordHeaderSize, len);
            if (actual_crc != crc) break; // torn write: CRC mismatch, stop exactly here

            maybe_index(rec_offset, pos, kRecordHeaderSize + len);
            pos += kRecordHeaderSize + len;
            expected_offset++;
            record_count_++;
        }

        write_pos_ = pos;
        next_offset_ = expected_offset;
    }

    std::string path_;
    std::uint64_t base_offset_;
    std::size_t max_bytes_;

    int fd_ = -1;
    std::uint8_t* data_ = nullptr;
    std::size_t mapped_len_ = 0;

    std::size_t write_pos_ = 0;
    std::uint64_t next_offset_ = 0;
    std::size_t record_count_ = 0;
    bool sealed_ = false;

    std::vector<IndexEntry> index_;
    std::size_t bytes_since_last_index_ = 0;
};

} // namespace cascade::core::storage