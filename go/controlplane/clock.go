package controlplane

import "time"

// Clock mirrors the C++ side's now_ms-driven determinism philosophy
// (JitterBuffer, BitrateController, NetworkConditionEstimator all took
// an explicit now_ms instead of reading the system clock). Same idea
// here: Membership takes a Clock interface so heartbeat-timeout tests
// can advance time instantly and deterministically instead of using
// real time.Sleep() calls.
type Clock interface {
	Now() time.Time
}

type RealClock struct{}

func (RealClock) Now() time.Time { return time.Now() }

// FakeClock for tests: starts at a fixed instant, advances only when
// Advance() is called.
type FakeClock struct {
	current time.Time
}

func NewFakeClock(start time.Time) *FakeClock {
	return &FakeClock{current: start}
}

func (c *FakeClock) Now() time.Time { return c.current }

func (c *FakeClock) Advance(d time.Duration) {
	c.current = c.current.Add(d)
}