#pragma once
// Cascade :: core::net :: ConnectionManager
//
// Tracks liveness of connections via heartbeats, independent of TcpServer
// so it can supervise TCP *or* UDP-based sessions equally (media sessions
// in Phase 6+ are UDP but still need liveness tracking). Built on top of
// Phase 1's TimerWheel: each connection gets a re-armed timeout timer that
// fires (and reports the connection dead) if no heartbeat is recorded
// before it expires. Deliberately push-based (on_heartbeat_received)
// rather than the manager polling — callers already know exactly when a
// heartbeat arrives (from TcpServer::OnMessage or a UDP recv), so a pull
// model would just add a redundant lookup pass.

#include <functional>
#include <mutex>
#include <unordered_map>

#include "timer/timer_wheel.hpp"

namespace cascade::core::net {

class ConnectionManager {
public:
    using ConnectionId = int;
    using OnTimeout = std::function<void(ConnectionId)>;

    // timeout_ms: how long without a heartbeat before a connection is
    // declared dead. wheel_tick_ms should be small relative to timeout_ms
    // (e.g. timeout_ms / 10) for reasonably tight detection latency.
    ConnectionManager(TimerWheel& wheel, std::uint64_t timeout_ms)
        : wheel_(wheel), timeout_ms_(timeout_ms) {}

    void set_on_timeout(OnTimeout cb) { on_timeout_ = std::move(cb); }

    void register_connection(ConnectionId id) {
        std::lock_guard<std::mutex> lock(mutex_);
        arm_timer_locked(id);
    }

    void unregister_connection(ConnectionId id) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = timers_.find(id);
        if (it != timers_.end()) {
            wheel_.cancel(it->second);
            timers_.erase(it);
        }
    }

    // Call this whenever a heartbeat (or any traffic you count as
    // liveness) is received from `id`. Re-arms the timeout.
    void on_heartbeat_received(ConnectionId id) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = timers_.find(id);
        if (it == timers_.end()) return; // not registered (already timed out / removed)
        wheel_.cancel(it->second);
        arm_timer_locked(id);
    }

    std::size_t tracked_count() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return timers_.size();
    }

private:
    void arm_timer_locked(ConnectionId id) {
        auto timer_id = wheel_.add_timer(timeout_ms_, [this, id] { handle_timeout(id); });
        timers_[id] = timer_id;
    }

    void handle_timeout(ConnectionId id) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            timers_.erase(id);
        }
        if (on_timeout_) on_timeout_(id);
    }

    TimerWheel& wheel_;
    std::uint64_t timeout_ms_;
    std::unordered_map<ConnectionId, TimerWheel::TimerId> timers_;
    OnTimeout on_timeout_;
    mutable std::mutex mutex_;
};

} // namespace cascade::core::net