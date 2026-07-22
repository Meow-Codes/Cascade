#pragma once
// Cascade :: core::timer
//
// Hashed timer wheel for scheduling many timers cheaply (O(1) insert,
// amortized O(1) tick) — the standard technique used in things like Kafka
// brokers and Netty for connection timeouts / heartbeats at scale, which
// is exactly StreamForge's use case (heartbeats, session timeouts, jitter
// buffer expiry).
//
// Design notes:
//  - Fixed number of "slots" (buckets) arranged in a ring. A timer with
//    delay D is placed in slot (current_slot + D/tick_ms) % num_slots.
//    If D exceeds one full revolution, we also store how many additional
//    revolutions must pass before the timer is actually due (classic
//    hashed/hierarchical wheel technique) so long timers don't need a
//    huge slot count.
//  - Single-threaded tick() by design: callers drive the wheel from one
//    dedicated timer thread (or an event loop) rather than the wheel
//    trying to be safe for concurrent tick() calls, which would add
//    locking overhead to the hot path for no real benefit — nothing about
//    a wheel benefits from multiple concurrent tickers.
//  - add_timer()/cancel() ARE safe to call from other threads (mutex-
//    protected) since producers of new timers are typically different
//    threads (networking, media layer) than the ticking thread.

#include <cstdint>
#include <functional>
#include <mutex>
#include <unordered_map>
#include <vector>
#include <algorithm>

namespace cascade::core {

class TimerWheel {
public:
    using Callback = std::function<void()>;
    using TimerId = std::uint64_t;

    TimerWheel(std::size_t num_slots, std::uint32_t tick_ms)
        : slots_(num_slots), tick_ms_(tick_ms) {}

    // Schedules callback to fire after delay_ms (relative to "now" in wheel
    // ticks). Returns a TimerId usable with cancel().
    TimerId add_timer(std::uint64_t delay_ms, Callback cb) {
        std::lock_guard<std::mutex> lock(mutex_);

        std::uint64_t ticks = delay_ms / tick_ms_;
        if (ticks == 0) ticks = 1; // minimum one tick out, never "fires in the past"

        std::size_t slot_index =
        (current_slot_ + ticks - 1) % slots_.size();
        std::uint64_t revolutions = (ticks - 1) / slots_.size();

        TimerId id = next_id_++;
        slots_[slot_index].push_back(Entry{id, revolutions, std::move(cb)});
        id_to_slot_[id] = slot_index;
        return id;
    }

    // Best-effort cancellation. Returns true if the timer was found and
    // removed before firing.
    bool cancel(TimerId id) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = id_to_slot_.find(id);
        if (it == id_to_slot_.end()) return false;

        auto& bucket = slots_[it->second];
        auto entry_it = std::find_if(bucket.begin(), bucket.end(),
                                      [id](const Entry& e) { return e.id == id; });
        if (entry_it == bucket.end()) return false;

        bucket.erase(entry_it);
        id_to_slot_.erase(it);
        return true;
    }

    // Advances the wheel by one tick, firing (and removing) any timers due
    // this tick. Intended to be called periodically (every tick_ms) by a
    // single dedicated thread.
    void tick() {
        std::vector<Callback> due;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            auto& bucket = slots_[current_slot_];

            std::vector<Entry> remaining;
            remaining.reserve(bucket.size());
            for (auto& entry : bucket) {
                if (entry.remaining_revolutions == 0) {
                    due.push_back(std::move(entry.callback));
                    id_to_slot_.erase(entry.id);
                } else {
                    entry.remaining_revolutions--;
                    remaining.push_back(std::move(entry));
                }
            }
            bucket = std::move(remaining);

            current_slot_ = (current_slot_ + 1) % slots_.size();
        }

        // Fire callbacks outside the lock so a callback that itself calls
        // add_timer()/cancel() doesn't deadlock.
        for (auto& cb : due) {
            if (cb) cb();
        }
    }

    std::uint32_t tick_ms() const { return tick_ms_; }
    std::size_t pending_count() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return id_to_slot_.size();
    }

private:
    struct Entry {
        TimerId id;
        std::uint64_t remaining_revolutions;
        Callback callback;
    };

    std::vector<std::vector<Entry>> slots_;
    std::uint32_t tick_ms_;
    std::size_t current_slot_ = 0;
    TimerId next_id_ = 1;

    std::unordered_map<TimerId, std::size_t> id_to_slot_;
    mutable std::mutex mutex_;
};

} // namespace cascade::core