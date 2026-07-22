#pragma once
// Cascade :: core::memory
//
// Fixed-size block memory pool ("slab allocator").
//
// Design notes:
//  - Blocks are carved out of large contiguous slabs allocated up front,
//    avoiding a malloc() per object on the hot path.
//  - Freed blocks go onto a lock-free free list so allocate()/deallocate()
//    are safe to call concurrently without a mutex.
//  - INDEX-based rather than pointer-based: the free-list head packs a
//    32-bit block index with a 32-bit generation counter into a single
//    64-bit word. This sidesteps ABA the same way a tagged pointer would,
//    but keeps the atomic a plain, universally lock-free 8-byte word
//    instead of requiring 16-byte double-width CAS. Blocks are only ever
//    recycled (never returned to the OS), so a stale index can never point
//    at freed memory — the generation counter guards against a *logical*
//    ABA reuse race, not a use-after-free.
//  - Limit: up to 2^32 - 1 live blocks per pool. Fine at this project's
//    scale; documented as a known limit rather than a bug.

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <mutex>
#include <new>
#include <vector>

namespace cascade::core {

template <typename T, std::size_t BlockSize = 4096>
class MemoryPool {
public:
    explicit MemoryPool(std::size_t blocks_per_slab = BlockSize)
        : blocks_per_slab_(blocks_per_slab) {
        grow();
    }

    MemoryPool(const MemoryPool&) = delete;
    MemoryPool& operator=(const MemoryPool&) = delete;

    ~MemoryPool() {
        for (auto& slab : slabs_) {
            ::operator delete(slab, std::align_val_t(alignof(T)));
        }
    }

    // Returns raw, uninitialized memory sized for T. Caller does
    // placement-new / manual destruction (mirrors allocator_traits'
    // allocate/deallocate split, not new/delete).
    void* allocate() {
        while (true) {
            std::uint64_t old_head = free_head_.load(std::memory_order_acquire);
            std::uint32_t index = index_of(old_head);

            if (index == kNullIndex) {
                grow_if_still_empty();
                continue;
            }

            std::uint32_t next = next_of(index);
            std::uint64_t new_head = pack(next, generation_of(old_head) + 1);

            if (free_head_.compare_exchange_weak(old_head, new_head,
                                                  std::memory_order_acq_rel,
                                                  std::memory_order_acquire)) {
                allocated_.fetch_add(1, std::memory_order_relaxed);
                return block_ptr(index);
            }
        }
    }

    void deallocate(void* p) noexcept {
        if (!p) return;
        std::uint32_t index = index_from_ptr(p);

        std::uint64_t old_head = free_head_.load(std::memory_order_acquire);
        std::uint64_t new_head;
        do {
            set_next(index, index_of(old_head));
            new_head = pack(index, generation_of(old_head) + 1);
        } while (!free_head_.compare_exchange_weak(old_head, new_head,
                                                     std::memory_order_acq_rel,
                                                     std::memory_order_acquire));
        allocated_.fetch_sub(1, std::memory_order_relaxed);
    }

    std::size_t live_allocations() const { return allocated_.load(std::memory_order_relaxed); }
    std::size_t slab_count() const { return slabs_.size(); }
    std::size_t block_capacity() const { return total_blocks_; }

private:
    static constexpr std::uint32_t kNullIndex = 0xFFFFFFFFu;
    static constexpr std::size_t stride_ = sizeof(T) < sizeof(std::uint32_t) ? sizeof(std::uint32_t) : sizeof(T);

    static std::uint64_t pack(std::uint32_t index, std::uint32_t generation) {
        return (static_cast<std::uint64_t>(generation) << 32) | index;
    }
    static std::uint32_t index_of(std::uint64_t v) { return static_cast<std::uint32_t>(v & 0xFFFFFFFFu); }
    static std::uint32_t generation_of(std::uint64_t v) { return static_cast<std::uint32_t>(v >> 32); }

    void* block_ptr(std::uint32_t global_index) const {
        std::size_t slab_idx = global_index / blocks_per_slab_;
        std::size_t offset = global_index % blocks_per_slab_;
        return slabs_[slab_idx] + offset * stride_;
    }

    std::uint32_t next_of(std::uint32_t global_index) const {
        std::uint32_t v;
        std::memcpy(&v, block_ptr(global_index), sizeof(v));
        return v;
    }

    void set_next(std::uint32_t global_index, std::uint32_t next) {
        std::memcpy(block_ptr(global_index), &next, sizeof(next));
    }

    std::uint32_t index_from_ptr(void* p) const {
        auto* byte_ptr = static_cast<std::byte*>(p);
        for (std::size_t s = 0; s < slabs_.size(); ++s) {
            auto* base = slabs_[s];
            auto* end = base + stride_ * blocks_per_slab_;
            if (byte_ptr >= base && byte_ptr < end) {
                std::size_t offset = static_cast<std::size_t>(byte_ptr - base) / stride_;
                return static_cast<std::uint32_t>(s * blocks_per_slab_ + offset);
            }
        }
        return kNullIndex; // programmer error: pointer not owned by this pool
    }

    void grow_if_still_empty() {
        std::lock_guard<std::mutex> lock(grow_mutex_);
        if (index_of(free_head_.load(std::memory_order_acquire)) != kNullIndex) return;
        grow();
    }

    void grow() {
        auto* raw = static_cast<std::byte*>(
            ::operator new(stride_ * blocks_per_slab_, std::align_val_t(alignof(T))));
        std::uint32_t base_index = static_cast<std::uint32_t>(total_blocks_);
        slabs_.push_back(raw);
        total_blocks_ += blocks_per_slab_;

        for (std::size_t i = blocks_per_slab_; i-- > 0;) {
            std::uint32_t idx = base_index + static_cast<std::uint32_t>(i);
            std::uint64_t old_head = free_head_.load(std::memory_order_acquire);
            std::uint64_t new_head;
            do {
                set_next(idx, index_of(old_head));
                new_head = pack(idx, generation_of(old_head) + 1);
            } while (!free_head_.compare_exchange_weak(old_head, new_head,
                                                         std::memory_order_acq_rel,
                                                         std::memory_order_acquire));
        }
    }

    std::size_t blocks_per_slab_;
    std::atomic<std::uint64_t> free_head_{pack(kNullIndex, 0)};
    std::atomic<std::size_t> allocated_{0};
    std::size_t total_blocks_ = 0;

    std::mutex grow_mutex_;
    std::vector<std::byte*> slabs_;
};

} // namespace cascade::core