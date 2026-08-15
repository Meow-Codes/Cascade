#pragma once
// Cascade :: core::perf :: alloc_counter
//
// Global operator new/delete override for BENCHMARK BINARIES ONLY --
// never include this in production code paths (cascade_core itself).
// Gives an exact allocation count rather than relying on wall-clock
// timing alone to judge whether a hot-path allocation was actually
// eliminated -- noise-free where perf's software counters can be noisy
// under WSL2's virtualization.

#include <atomic>
#include <cstdlib>
#include <new>

namespace cascade::core::perf {
inline std::atomic<std::uint64_t> g_alloc_count{0};
}

inline void* operator new(std::size_t size) {
    cascade::core::perf::g_alloc_count.fetch_add(1, std::memory_order_relaxed);
    void* p = std::malloc(size);
    if (!p) throw std::bad_alloc();
    return p;
}
inline void operator delete(void* p) noexcept { std::free(p); }
inline void operator delete(void* p, std::size_t) noexcept { std::free(p); }