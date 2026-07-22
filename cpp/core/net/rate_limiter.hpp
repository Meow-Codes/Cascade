#pragma once
// Cascade :: core::net :: RateLimiter
//
// Classic token-bucket rate limiter. Bucket refills continuously at
// `rate_per_sec`, capped at `burst_capacity`. try_consume(n) is the only
// operation callers need — no separate "refill" call, refill amount is
// computed lazily from elapsed wall-clock time on each call, which keeps
// the limiter accurate without needing a background timer/thread.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <mutex>

namespace cascade::core::net {

class RateLimiter {
public:
    RateLimiter(double rate_per_sec, double burst_capacity)
        : rate_per_sec_(rate_per_sec),
          capacity_(burst_capacity),
          tokens_(burst_capacity),
          last_refill_(Clock::now()) {}

    bool try_consume(double tokens = 1.0) {
        std::lock_guard<std::mutex> lock(mutex_);
        refill();
        if (tokens_ >= tokens) {
            tokens_ -= tokens;
            return true;
        }
        return false;
    }

    double tokens_available() const {
        std::lock_guard<std::mutex> lock(mutex_);
        const_cast<RateLimiter*>(this)->refill(); // const-correctness note: refill is logically
                                                     // observational (recomputes token count from
                                                     // elapsed time), safe to allow from a const call
        return tokens_;
    }

private:
    using Clock = std::chrono::steady_clock;

    void refill() {
        auto now = Clock::now();
        double elapsed = std::chrono::duration<double>(now - last_refill_).count();
        last_refill_ = now;
        tokens_ = std::min(capacity_, tokens_ + elapsed * rate_per_sec_);
    }

    double rate_per_sec_;
    double capacity_;
    double tokens_;
    Clock::time_point last_refill_;
    mutable std::mutex mutex_;
};

} // namespace cascade::core::net