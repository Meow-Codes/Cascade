#include <gtest/gtest.h>
#include <atomic>
#include "net/connection_manager.hpp"

using namespace cascade::core::net;
using cascade::core::TimerWheel;

TEST(ConnectionManager, DetectsTimeoutWithinExpectedTicks) {
    TimerWheel wheel(64, /*tick_ms=*/10);
    ConnectionManager mgr(wheel, /*timeout_ms=*/100); // ~10 ticks

    std::atomic<bool> timed_out{false};
    mgr.set_on_timeout([&](ConnectionManager::ConnectionId) { timed_out.store(true); });

    mgr.register_connection(1);

    for (int i = 0; i < 9; ++i) { wheel.tick(); EXPECT_FALSE(timed_out.load()); }
    wheel.tick();
    EXPECT_TRUE(timed_out.load());
}

TEST(ConnectionManager, HeartbeatResetsTimeout) {
    TimerWheel wheel(64, 10);
    ConnectionManager mgr(wheel, 100);

    std::atomic<bool> timed_out{false};
    mgr.set_on_timeout([&](ConnectionManager::ConnectionId) { timed_out.store(true); });

    mgr.register_connection(1);

    for (int i = 0; i < 5; ++i) wheel.tick(); // halfway to timeout
    mgr.on_heartbeat_received(1);              // reset the clock
    for (int i = 0; i < 8; ++i) { wheel.tick(); EXPECT_FALSE(timed_out.load()); }
}

TEST(ConnectionManager, UnregisterPreventsTimeoutCallback) {
    TimerWheel wheel(64, 10);
    ConnectionManager mgr(wheel, 50);

    std::atomic<bool> timed_out{false};
    mgr.set_on_timeout([&](ConnectionManager::ConnectionId) { timed_out.store(true); });

    mgr.register_connection(1);
    mgr.unregister_connection(1);

    for (int i = 0; i < 10; ++i) wheel.tick();
    EXPECT_FALSE(timed_out.load());
}