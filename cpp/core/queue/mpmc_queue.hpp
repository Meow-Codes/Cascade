#pragma once
// Cascade :: core::queue :: MpmcQueue
//
// Bounded multi-producer, multi-consumer lock-free queue.
//
// Classic Dmitry Vyukov bounded MPMC ring buffer:
//  - Each slot carries its own "sequence" counter alongside the data.
//  - A producer claims a slot by CAS-incrementing the shared tail counter,
//    spins briefly until the slot's sequence says "ready for a producer,"
//    writes the value, then bumps the sequence to "ready for a consumer."
//  - Consumers mirror this on the head counter.
//  - Avoids both ABA/lost-update bugs of a naive MPMC ring buffer AND a
//    single global lock — throughput degrades gracefully under contention
//    instead of serializing on a mutex.
//  - Built after SpscQueue deliberately: SPSC validates the memory-ordering
//    reasoning in a simpler setting before tackling multi-writer/reader.

#include <atomic>
#include <cstddef>
#include <new>
#include <optional>

#include "queue/spsc_queue.hpp" // reuses kCacheLineSize

namespace cascade::core {

template <typename T>
class MpmcQueue {
public:
    explicit MpmcQueue(std::size_t capacity) : capacity_(round_up_pow2(capacity)), mask_(capacity_ - 1) {
        slots_ = static_cast<Slot*>(::operator new[](capacity_ * sizeof(Slot), std::align_val_t(alignof(Slot))));
        for (std::size_t i = 0; i < capacity_; ++i) {
            ::new (&slots_[i]) Slot();
            slots_[i].sequence.store(i, std::memory_order_relaxed);
        }
    }

    ~MpmcQueue() {
        while (try_pop()) { /* drain */ }
        for (std::size_t i = 0; i < capacity_; ++i) slots_[i].~Slot();
        ::operator delete[](slots_, std::align_val_t(alignof(Slot)));
    }

    MpmcQueue(const MpmcQueue&) = delete;
    MpmcQueue& operator=(const MpmcQueue&) = delete;

    template <typename U>
    bool try_push(U&& value) {
        Slot* slot;
        std::size_t pos = tail_.load(std::memory_order_relaxed);

        while (true) {
            slot = &slots_[pos & mask_];
            std::size_t seq = slot->sequence.load(std::memory_order_acquire);
            std::intptr_t diff = static_cast<std::intptr_t>(seq) - static_cast<std::intptr_t>(pos);

            if (diff == 0) {
                if (tail_.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) {
                    break;
                }
            } else if (diff < 0) {
                return false; // queue full: this slot hasn't been consumed yet
            } else {
                pos = tail_.load(std::memory_order_relaxed); // another producer moved tail_; resync
            }
        }

        ::new (static_cast<void*>(&slot->storage)) T(std::forward<U>(value));
        slot->sequence.store(pos + 1, std::memory_order_release);
        return true;
    }

    std::optional<T> try_pop() {
        Slot* slot;
        std::size_t pos = head_.load(std::memory_order_relaxed);

        while (true) {
            slot = &slots_[pos & mask_];
            std::size_t seq = slot->sequence.load(std::memory_order_acquire);
            std::intptr_t diff = static_cast<std::intptr_t>(seq) - static_cast<std::intptr_t>(pos + 1);

            if (diff == 0) {
                if (head_.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) {
                    break;
                }
            } else if (diff < 0) {
                return std::nullopt; // queue empty: producer hasn't published here yet
            } else {
                pos = head_.load(std::memory_order_relaxed);
            }
        }

        T* value_ptr = std::launder(reinterpret_cast<T*>(&slot->storage));
        std::optional<T> result(std::move(*value_ptr));
        value_ptr->~T();
        slot->sequence.store(pos + mask_ + 1, std::memory_order_release); // ready for next lap
        return result;
    }

    std::size_t capacity() const { return capacity_; }

private:
    struct Slot {
        std::atomic<std::size_t> sequence{0};
        alignas(T) std::byte storage[sizeof(T)];
    };

    static std::size_t round_up_pow2(std::size_t v) {
        std::size_t p = 1;
        while (p < v) p <<= 1;
        return p == 0 ? 1 : p;
    }

    const std::size_t capacity_;
    const std::size_t mask_;
    Slot* slots_;

    alignas(kCacheLineSize) std::atomic<std::size_t> head_{0};
    alignas(kCacheLineSize) std::atomic<std::size_t> tail_{0};
};

} // namespace cascade::core