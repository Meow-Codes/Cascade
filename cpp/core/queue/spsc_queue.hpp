#pragma once
// Cascade :: core::queue :: SpscQueue
//
// Single-producer, single-consumer lock-free ring buffer.
//
// Design notes:
//  - Capacity is rounded up to a power of two so index wraparound is a
//    cheap bitmask instead of a modulo.
//  - head_ is only ever written by the consumer, tail_ only by the
//    producer. Each side reads the *other* side's index with acquire and
//    publishes its own with release — the standard SPSC pattern, and what
//    makes it safe without locks or CAS at all.
//  - Cache-line padding separates head_ and tail_ so producer/consumer
//    threads don't false-share a cache line, which would silently wreck
//    throughput despite the algorithm being "correct."
//  - Capacity is fixed at construction — growth would require coordination
//    between producer and consumer, defeating the point of lock-free SPSC.

#include <atomic>
#include <cstddef>
#include <new>
#include <optional>

namespace cascade::core {

inline constexpr std::size_t kCacheLineSize = 64;

template <typename T>
class SpscQueue {
public:
    explicit SpscQueue(std::size_t capacity) : capacity_(round_up_pow2(capacity)), mask_(capacity_ - 1) {
        buffer_ = static_cast<T*>(::operator new[](capacity_ * sizeof(T), std::align_val_t(alignof(T))));
    }

    ~SpscQueue() {
        while (try_pop()) { /* drain */ }
        ::operator delete[](buffer_, std::align_val_t(alignof(T)));
    }

    SpscQueue(const SpscQueue&) = delete;
    SpscQueue& operator=(const SpscQueue&) = delete;

    // Producer side only. Returns false if the queue is full.
    template <typename U>
    bool try_push(U&& value) {
        const std::size_t tail = tail_.load(std::memory_order_relaxed);
        const std::size_t next_tail = tail + 1;

        if (next_tail - head_cached_ > capacity_) {
            head_cached_ = head_.load(std::memory_order_acquire);
            if (next_tail - head_cached_ > capacity_) {
                return false; // genuinely full
            }
        }

        ::new (static_cast<void*>(&buffer_[tail & mask_])) T(std::forward<U>(value));
        tail_.store(next_tail, std::memory_order_release);
        return true;
    }

    // Consumer side only. Returns std::nullopt if the queue is empty.
    std::optional<T> try_pop() {
        const std::size_t head = head_.load(std::memory_order_relaxed);

        if (head >= tail_cached_) {
            tail_cached_ = tail_.load(std::memory_order_acquire);
            if (head >= tail_cached_) {
                return std::nullopt; // genuinely empty
            }
        }

        T* slot = &buffer_[head & mask_];
        std::optional<T> result(std::move(*slot));
        slot->~T();
        head_.store(head + 1, std::memory_order_release);
        return result;
    }

    std::size_t size_approx() const {
        std::size_t tail = tail_.load(std::memory_order_acquire);
        std::size_t head = head_.load(std::memory_order_acquire);
        return tail - head;
    }

    std::size_t capacity() const { return capacity_; }

private:
    static std::size_t round_up_pow2(std::size_t v) {
        std::size_t p = 1;
        while (p < v) p <<= 1;
        return p == 0 ? 1 : p;
    }

    const std::size_t capacity_;
    const std::size_t mask_;
    T* buffer_;

    alignas(kCacheLineSize) std::atomic<std::size_t> head_{0};
    std::size_t tail_cached_ = 0;

    alignas(kCacheLineSize) std::atomic<std::size_t> tail_{0};
    std::size_t head_cached_ = 0;
};

} // namespace cascade::core