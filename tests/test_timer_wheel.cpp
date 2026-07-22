#include <gtest/gtest.h>
#include <atomic>
#include "timer/timer_wheel.hpp"

using cascade::core::TimerWheel;

TEST(TimerWheel, FiresAfterCorrectNumberOfTicks) {
    TimerWheel wheel(16, /*tick_ms=*/10);
    int fired = 0;
    wheel.add_timer(30, [&] { fired++; }); // ~3 ticks out

    for (int i = 0; i < 2; ++i) { wheel.tick(); EXPECT_EQ(fired, 0); }
    wheel.tick();
    EXPECT_EQ(fired, 1);
}

TEST(TimerWheel, HandlesWraparoundWithRevolutions) {
    TimerWheel wheel(4, /*tick_ms=*/10); // small wheel, forces >1 revolution
    int fired = 0;
    wheel.add_timer(90, [&] { fired++; }); // 9 ticks, wheel size 4 -> 2 revolutions + 1

    for (int i = 0; i < 8; ++i) { wheel.tick(); EXPECT_EQ(fired, 0); }
    wheel.tick();
    EXPECT_EQ(fired, 1);
}

TEST(TimerWheel, CancelPreventsFiring) {
    TimerWheel wheel(16, 10);
    int fired = 0;
    auto id = wheel.add_timer(20, [&] { fired++; });
    EXPECT_TRUE(wheel.cancel(id));
    for (int i = 0; i < 5; ++i) wheel.tick();
    EXPECT_EQ(fired, 0);
}

TEST(TimerWheel, MultipleTimersFireIndependently) {
    TimerWheel wheel(32, 10);
    std::atomic<int> a{0}, b{0};
    wheel.add_timer(10, [&] { a++; });
    wheel.add_timer(50, [&] { b++; });

    for (int i = 0; i < 6; ++i) wheel.tick();
    EXPECT_EQ(a.load(), 1);
    EXPECT_EQ(b.load(), 1);
}