package base

import (
	"sync"
	"time"
)

// Clock is the time source for paced playback. Tests inject a fake clock.
type Clock interface {
	Now() time.Time
	NewTimer(d time.Duration) Timer
}

// Timer fires once on C. Stop prevents the fire when it has not happened.
type Timer interface {
	C() <-chan time.Time
	Stop() bool
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

func (realClock) NewTimer(d time.Duration) Timer {
	if d < 0 {
		d = 0
	}
	return stdTimer{t: time.NewTimer(d)}
}

type stdTimer struct{ t *time.Timer }

func (s stdTimer) C() <-chan time.Time { return s.t.C }
func (s stdTimer) Stop() bool          { return s.t.Stop() }

// FakeClock is a manually advanced clock for pacing tests.
type FakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeTimer
}

// NewFakeClock starts at start, or Unix epoch when start is zero.
func NewFakeClock(start time.Time) *FakeClock {
	if start.IsZero() {
		start = time.Unix(0, 0)
	}
	return &FakeClock{now: start}
}

// Now reports the frozen time.
func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Advance moves the clock forward and fires due timers.
func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	now := c.now
	var fire []*fakeTimer
	keep := c.timers[:0]
	for _, t := range c.timers {
		if t.stopped {
			continue
		}
		if !t.deadline.After(now) {
			t.fired = true
			fire = append(fire, t)
			continue
		}
		keep = append(keep, t)
	}
	c.timers = keep
	c.mu.Unlock()
	for _, t := range fire {
		t.ch <- now
	}
}

// NewTimer implements Clock.
func (c *FakeClock) NewTimer(d time.Duration) Timer {
	ch := make(chan time.Time, 1)
	c.mu.Lock()
	defer c.mu.Unlock()
	deadline := c.now.Add(d)
	if d <= 0 || !deadline.After(c.now) {
		ch <- c.now
		return &fakeTimer{ch: ch, fired: true}
	}
	ft := &fakeTimer{c: c, ch: ch, deadline: deadline}
	c.timers = append(c.timers, ft)
	return ft
}

type fakeTimer struct {
	c        *FakeClock
	ch       chan time.Time
	deadline time.Time
	fired    bool
	stopped  bool
}

func (t *fakeTimer) C() <-chan time.Time { return t.ch }

func (t *fakeTimer) Stop() bool {
	if t.c == nil {
		return false
	}
	t.c.mu.Lock()
	defer t.c.mu.Unlock()
	if t.fired || t.stopped {
		return false
	}
	t.stopped = true
	return true
}
