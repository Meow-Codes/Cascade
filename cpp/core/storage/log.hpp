#pragma once
// Cascade :: core::storage :: Log
//
// Manages an ordered sequence of Segments for one partition-like log:
// routes appends to the active (newest) segment, rolls over when full,
// and routes reads to whichever segment contains the requested offset.
// This is the class application code (the future Broker) actually talks
// to -- Segment is an implementation detail below this layer, same split
// as Kafka's Log/LogSegment.
//
// Concurrency model: a single mutex serializes writers (this project has
// exactly one writer per log/partition, matching the architecture
// diagram's per-partition-leader design). Readers do NOT take that lock.
// This is safe because:
//   (a) segments_ is only ever appended to, never mutated/erased, so
//       read()'s snapshot (a cheap shared_ptr vector copy under a brief
//       lock) is stable to iterate even while a writer concurrently
//       rolls a new segment in.
//   (b) within a single Segment, write_pos_ only grows, and a reader only
//       ever reads bytes at positions < the write_pos_ it observed. A
//       reader that starts scanning before an in-progress append's
//       memcpy finishes simply won't see that record yet -- it's still
//       scanning old write_pos_ territory -- not read it half-written.
//       This "readers never race the frontier" invariant is what the
//       ConcurrentReaders test in this phase exists to verify empirically,
//       not just assert in a comment.

#include <algorithm>
#include <atomic>
#include <cstdio>
#include <filesystem>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <vector>
#include <deque>
#include <shared_mutex>

#include "storage/segment.hpp"

namespace cascade::core::storage {

class Log {
public:
    Log(std::string dir, std::size_t max_segment_bytes)
        : dir_(std::move(dir)), max_segment_bytes_(max_segment_bytes) {
        std::filesystem::create_directories(dir_);
        recover_existing_segments();
        if (segments_.empty()) {
            roll_new_segment(0);
        }
    }

    std::uint64_t append(const std::uint8_t* payload, std::uint32_t len) {
        std::lock_guard<std::shared_mutex> lock(rw_mutex_);

        Segment* active = segments_.back().get();
        auto offset = active->append(payload, len);
        if (offset.has_value()) {
            next_offset_.store(*offset + 1, std::memory_order_release);
            return *offset;
        }

        active->seal();
        roll_new_segment(active->next_offset());
        auto retry = segments_.back()->append(payload, len);
        if (!retry.has_value()) {
            throw std::runtime_error("record too large for configured segment size");
        }
        next_offset_.store(*retry + 1, std::memory_order_release);
        return *retry;
    }

    void flush() {
        std::lock_guard<std::shared_mutex> lock(rw_mutex_);
        segments_.back()->flush();
    }

    std::optional<ReadResult> read(std::uint64_t offset) const {
        std::shared_lock<std::shared_mutex> lock(rw_mutex_);  // readers proceed concurrently with each other now

        auto it = std::upper_bound(segments_.begin(), segments_.end(), offset,
            [](std::uint64_t off, const std::shared_ptr<Segment>& s) { return off < s->base_offset(); });
        if (it == segments_.begin()) return std::nullopt;
        --it;

        Segment* seg = it->get(); // safe: deque never moves existing elements, and we hold the lock
        std::size_t hint = seg->position_hint_for(offset);
        return seg->read_from(offset, hint);
    }

    std::uint64_t next_offset() const { return next_offset_.load(std::memory_order_acquire); }

    std::size_t segment_count() const {
        std::lock_guard<std::shared_mutex> lock(rw_mutex_);
        return segments_.size();
    }

private:
    void roll_new_segment(std::uint64_t base_offset) {
        std::string path = dir_ + "/" + segment_filename(base_offset);
        auto seg = std::make_shared<Segment>(path, base_offset, max_segment_bytes_);
        segments_.push_back(seg);
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
            auto seg = std::make_shared<Segment>(path.string(), base_offset, max_segment_bytes_);
            segments_.push_back(seg);
        }

        if (!segments_.empty()) {
            // Every segment except possibly the last was, by construction,
            // already full on a prior run -- treat as sealed even if it
            // crashed before a clean seal().
            for (std::size_t i = 0; i + 1 < segments_.size(); ++i) {
                if (!segments_[i]->is_sealed()) segments_[i]->seal();
            }
            next_offset_.store(segments_.back()->next_offset(), std::memory_order_release);
        }
    }

    // std::vector<std::shared_ptr<Segment>> segments_snapshot() const {
    //     std::lock_guard<std::shared_mutex> lock(rw_mutex_);
    //     return std::vector<std::shared_ptr<Segment>>(segments_.begin(), segments_.end());
    // }

    std::string dir_;
    std::size_t max_segment_bytes_;
    std::deque<std::shared_ptr<Segment>> segments_;  // deque, not vector: push_back never
                                                        // invalidates existing elements' addresses,
                                                        // which is what makes lock-free-style
                                                        // concurrent reads (below) safe.
    std::atomic<std::uint64_t> next_offset_{0};
    mutable std::shared_mutex rw_mutex_;  // was std::mutex writer_mutex_
};

} // namespace cascade::core::storage