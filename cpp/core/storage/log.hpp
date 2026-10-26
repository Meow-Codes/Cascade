#pragma once
// Cascade :: core::storage :: Log
//
// Ordered sequence of Segments for one partition: routes appends to the
// active (newest) segment, rolls over when full, routes reads by offset.
//
// Concurrency: writers take the rw_mutex_ exclusively. Readers take it
// shared, so readers run in parallel with each other (not with a writer).
// segments_ is a deque so push_back never moves existing elements.
//
// Batch append: append_batch() writes N records then flushes ONCE. The
// batch occupies consecutive offsets (the exclusive lock is held throughout).
// It is NOT atomic across a crash: a prefix of an unacknowledged batch may
// survive. Once append_batch returns with flush_after == true, every record
// in it is durable.

#include <algorithm>
#include <atomic>
#include <cstdio>
#include <deque>
#include <fcntl.h>
#include <filesystem>
#include <limits>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <span>
#include <stdexcept>
#include <unistd.h>
#include <vector>

#include "storage/segment.hpp"

namespace cascade::core::storage {

using PayloadView = std::span<const std::uint8_t>;

class Log {
public:
    Log(std::string dir, std::size_t max_segment_bytes)
        : dir_(std::move(dir)), max_segment_bytes_(max_segment_bytes) {
        std::filesystem::create_directories(dir_);
        recover_existing_segments();
        if (segments_.empty()) roll_new_segment(0);
    }

    std::uint64_t append(const std::uint8_t* payload, std::uint32_t len) {
        std::unique_lock<std::shared_mutex> lock(rw_mutex_);

        Segment* active = segments_.back().get();
        auto offset = active->append(payload, len);
        if (offset.has_value()) {
            next_offset_.store(*offset + 1, std::memory_order_release);
            return *offset;
        }

        active->seal();
        roll_new_segment(active->next_offset());
        auto retry = segments_.back()->append(payload, len);
        if (!retry.has_value()) throw std::runtime_error("record too large for configured segment size");
        next_offset_.store(*retry + 1, std::memory_order_release);
        return *retry;
    }

    // Appends every payload, then (if flush_after) performs ONE flush.
    // Returns the offset of the first record; the rest follow consecutively.
    // Throws std::length_error, before writing anything, if any record can
    // never fit in a segment.
    std::uint64_t append_batch(std::span<const PayloadView> payloads, bool flush_after = true) {
        for (const auto& p : payloads) {
            if (p.size() > std::numeric_limits<std::uint32_t>::max() ||
                kRecordHeaderSize + p.size() > max_segment_bytes_) {
                throw std::length_error("record in batch cannot fit in a segment of max_segment_bytes");
            }
        }

        std::unique_lock<std::shared_mutex> lock(rw_mutex_);
        const std::uint64_t first = segments_.back()->next_offset();
        if (payloads.empty()) return first;

        try {
            for (const auto& p : payloads) {
                const auto len = static_cast<std::uint32_t>(p.size());
                Segment* active = segments_.back().get();
                auto off = active->append(p.data(), len);
                if (!off.has_value()) {
                    active->seal(); // msyncs the old segment before truncating it
                    roll_new_segment(active->next_offset());
                    off = segments_.back()->append(p.data(), len);
                    if (!off.has_value()) throw std::runtime_error("record too large for configured segment size");
                }
            }
        } catch (...) {
            // Keep the cached high-water mark honest even if we fail midway
            // (e.g. disk full while creating a segment); the appended prefix is real.
            next_offset_.store(segments_.back()->next_offset(), std::memory_order_release);
            throw;
        }
        next_offset_.store(segments_.back()->next_offset(), std::memory_order_release);

        if (flush_after) {
            segments_.back()->flush();
            flush_calls_.fetch_add(1, std::memory_order_relaxed);
        }
        return first;
    }

    void flush() {
        std::unique_lock<std::shared_mutex> lock(rw_mutex_);
        segments_.back()->flush();
        flush_calls_.fetch_add(1, std::memory_order_relaxed);
    }

    std::optional<ReadResult> read(std::uint64_t offset) const {
        std::shared_lock<std::shared_mutex> lock(rw_mutex_);

        auto it = std::upper_bound(segments_.begin(), segments_.end(), offset,
            [](std::uint64_t off, const std::shared_ptr<Segment>& s) { return off < s->base_offset(); });
        if (it == segments_.begin()) return std::nullopt;
        --it;

        Segment* seg = it->get();
        return seg->read_from(offset, seg->position_hint_for(offset));
    }

    std::uint64_t next_offset() const { return next_offset_.load(std::memory_order_acquire); }

    std::size_t segment_count() const {
        std::shared_lock<std::shared_mutex> lock(rw_mutex_);
        return segments_.size();
    }

    // Number of flush (msync) calls issued by this Log instance. Exposed so
    // tests can assert "N records, one flush" without timing anything.
    std::uint64_t flush_calls() const { return flush_calls_.load(std::memory_order_relaxed); }

private:
    void roll_new_segment(std::uint64_t base_offset) {
        std::string path = dir_ + "/" + segment_filename(base_offset);
        segments_.push_back(std::make_shared<Segment>(path, base_offset, max_segment_bytes_));
        fsync_dir(); // make the new file's directory entry durable before we ack records in it
    }

    void fsync_dir() const {
        int dfd = ::open(dir_.c_str(), O_RDONLY | O_DIRECTORY);
        if (dfd >= 0) {
            ::fsync(dfd);
            ::close(dfd);
        }
    }

    static std::string segment_filename(std::uint64_t base_offset) {
        char buf[32];
        std::snprintf(buf, sizeof(buf), "%020llu.seg", static_cast<unsigned long long>(base_offset));
        return std::string(buf);
    }

    void recover_existing_segments() {
        if (!std::filesystem::exists(dir_)) return;

        std::vector<std::filesystem::path> files;
        for (auto& entry : std::filesystem::directory_iterator(dir_)) {
            if (entry.path().extension() == ".seg") files.push_back(entry.path());
        }
        std::sort(files.begin(), files.end());

        for (auto& path : files) {
            std::uint64_t base_offset = std::stoull(path.stem().string());
            segments_.push_back(std::make_shared<Segment>(path.string(), base_offset, max_segment_bytes_));
        }

        if (!segments_.empty()) {
            // Every segment except possibly the last was full on a prior run.
            for (std::size_t i = 0; i + 1 < segments_.size(); ++i) {
                if (!segments_[i]->is_sealed()) segments_[i]->seal();
            }
            next_offset_.store(segments_.back()->next_offset(), std::memory_order_release);
        }
    }

    std::string dir_;
    std::size_t max_segment_bytes_;
    std::deque<std::shared_ptr<Segment>> segments_;
    std::atomic<std::uint64_t> next_offset_{0};
    std::atomic<std::uint64_t> flush_calls_{0};
    mutable std::shared_mutex rw_mutex_;
};

} // namespace cascade::core::storage