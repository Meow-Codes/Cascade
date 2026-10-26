#pragma once
// Cascade :: core::memory :: MemoryPool
//
// Fixed-size block pool with a lock-free free list.
//
// Design notes:
//  - Free-list links live in a SIDE TABLE (one atomic<uint32_t> per block),
//    not inside the blocks. The earlier version stored the link in the first
//    4 bytes of each free block with plain memcpy; a thread that had just lost
//    the pop race could still read a stale link while the winner was already
//    writing to that block. The CAS discarded the stale value, but the
//    unsynchronized read/write pair is still a data race (TSan reported it).
//    Now every link access is atomic and the pool never touches memory the
//    caller owns.
//  - The head packs a 32-bit block index and a 32-bit generation into one
//    64-bit atomic (no 16-byte CAS; the generation defeats ABA).
//  - Growth is the only locked path. Slab and link-table pointers sit in
//    fixed-size arrays of atomics, published with release and read with
//    acquire, so readers never see a container being resized.
//  - Limits: kMaxSlabs slabs per pool; total blocks must stay below 2^32 - 1.

#include <array>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <new>

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
        const std::size_t n = slab_count_.load(std::memory_order_acquire);
        for (std::size_t i = 0; i < n; ++i) {
            ::operator delete(slabs_[i].load(std::memory_order_relaxed), std::align_val_t(alignof(T)));
            delete[] links_[i].load(std::memory_order_relaxed);
        }
    }

    // Returns raw, uninitialized memory sized for T (caller placement-news).
    void* allocate() {
        while (true) {
            std::uint64_t old_head = free_head_.load(std::memory_order_acquire);
            const std::uint32_t index = index_of(old_head);

            if (index == kNullIndex) {
                grow_if_still_empty();
                continue;
            }

            // Atomic read. If a racing pop+push rewrites this link the
            // generation changes and the CAS below fails.
            const std::uint32_t next = link(index).load(std::memory_order_relaxed);
            const std::uint64_t new_head = pack(next, generation_of(old_head) + 1);

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
        const std::uint32_t index = index_from_ptr(p);
        if (index == kNullIndex) {
            assert(false && "Pointer does not belong to this MemoryPool");
            return;
        }

        std::uint64_t old_head = free_head_.load(std::memory_order_acquire);
        std::uint64_t new_head;
        do {
            link(index).store(index_of(old_head), std::memory_order_relaxed);
            new_head = pack(index, generation_of(old_head) + 1);
        } while (!free_head_.compare_exchange_weak(old_head, new_head,
                                                     std::memory_order_acq_rel,
                                                     std::memory_order_acquire));
        allocated_.fetch_sub(1, std::memory_order_relaxed);
    }

    std::size_t live_allocations() const { return allocated_.load(std::memory_order_relaxed); }
    std::size_t slab_count() const { return slab_count_.load(std::memory_order_acquire); }
    std::size_t block_capacity() const { return slab_count() * blocks_per_slab_; }

private:
    static constexpr std::size_t kMaxSlabs = 1024;
    static constexpr std::uint32_t kNullIndex = 0xFFFFFFFFu;
    static constexpr std::size_t stride_ = sizeof(T); // sizeof is a multiple of alignof

    static std::uint64_t pack(std::uint32_t index, std::uint32_t generation) {
        return (static_cast<std::uint64_t>(generation) << 32) | index;
    }
    static std::uint32_t index_of(std::uint64_t v) { return static_cast<std::uint32_t>(v & 0xFFFFFFFFu); }
    static std::uint32_t generation_of(std::uint64_t v) { return static_cast<std::uint32_t>(v >> 32); }

    void* block_ptr(std::uint32_t global_index) const {
        const std::size_t slab_idx = global_index / blocks_per_slab_;
        const std::size_t offset = global_index % blocks_per_slab_;
        return slabs_[slab_idx].load(std::memory_order_acquire) + offset * stride_;
    }

    std::atomic<std::uint32_t>& link(std::uint32_t global_index) const {
        const std::size_t slab_idx = global_index / blocks_per_slab_;
        const std::size_t offset = global_index % blocks_per_slab_;
        return links_[slab_idx].load(std::memory_order_acquire)[offset];
    }

    std::uint32_t index_from_ptr(void* p) const {
        auto* byte_ptr = static_cast<std::byte*>(p);
        const std::size_t n = slab_count_.load(std::memory_order_acquire);
        for (std::size_t s = 0; s < n; ++s) {
            auto* base = slabs_[s].load(std::memory_order_acquire);
            auto* end = base + stride_ * blocks_per_slab_;
            if (byte_ptr >= base && byte_ptr < end) {
                const std::size_t offset = static_cast<std::size_t>(byte_ptr - base) / stride_;
                return static_cast<std::uint32_t>(s * blocks_per_slab_ + offset);
            }
        }
        return kNullIndex;
    }

    void grow_if_still_empty() {
        std::lock_guard<std::mutex> lock(grow_mutex_);
        if (index_of(free_head_.load(std::memory_order_acquire)) != kNullIndex) return;
        grow();
    }

    // Caller holds grow_mutex_ (or is the constructor).
    void grow() {
        const std::size_t slab_idx = slab_count_.load(std::memory_order_relaxed);
        if (slab_idx >= kMaxSlabs || blocks_per_slab_ == 0) throw std::bad_alloc();

        constexpr std::size_t kMaxBlocks = kNullIndex; // valid indices: 0 .. kNullIndex-1
        const std::size_t base = slab_idx * blocks_per_slab_;
        if (base >= kMaxBlocks || blocks_per_slab_ > kMaxBlocks - base) throw std::bad_alloc();

        std::unique_ptr<std::atomic<std::uint32_t>[]> links(new std::atomic<std::uint32_t>[blocks_per_slab_]());
        auto* raw = static_cast<std::byte*>(
            ::operator new(stride_ * blocks_per_slab_, std::align_val_t(alignof(T))));

        // Chain the new blocks base -> base+1 -> ... -> last privately; only
        // the last link depends on the current head and is set inside the CAS loop.
        const auto base_index = static_cast<std::uint32_t>(base);
        for (std::size_t i = 0; i + 1 < blocks_per_slab_; ++i) {
            links[i].store(base_index + static_cast<std::uint32_t>(i) + 1, std::memory_order_relaxed);
        }
        std::atomic<std::uint32_t>* links_ptr = links.release();
        const std::size_t last = blocks_per_slab_ - 1;

        slabs_[slab_idx].store(raw, std::memory_order_release);
        links_[slab_idx].store(links_ptr, std::memory_order_release);
        slab_count_.store(slab_idx + 1, std::memory_order_release);

        // One CAS splices the whole chain onto the free list.
        std::uint64_t old_head = free_head_.load(std::memory_order_acquire);
        std::uint64_t new_head;
        do {
            links_ptr[last].store(index_of(old_head), std::memory_order_relaxed);
            new_head = pack(base_index, generation_of(old_head) + 1);
        } while (!free_head_.compare_exchange_weak(old_head, new_head,
                                                     std::memory_order_acq_rel,
                                                     std::memory_order_acquire));
    }

    std::size_t blocks_per_slab_;
    std::atomic<std::uint64_t> free_head_{pack(kNullIndex, 0)};
    std::atomic<std::size_t> allocated_{0};

    std::array<std::atomic<std::byte*>, kMaxSlabs> slabs_{};
    std::array<std::atomic<std::atomic<std::uint32_t>*>, kMaxSlabs> links_{};
    std::atomic<std::size_t> slab_count_{0};
    std::mutex grow_mutex_;
};

} // namespace cascade::core