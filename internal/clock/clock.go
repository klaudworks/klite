package clock

import (
	"sync"
	"time"
)

// Clock abstracts time operations for testability.
type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
	NewTicker(d time.Duration) *Ticker
}

// Ticker wraps a channel so test clocks can drive it.
type Ticker struct {
	C    <-chan time.Time
	stop func()
}

// Stop stops the ticker.
func (t *Ticker) Stop() { t.stop() }

// FakeClock is a manually controlled clock for deterministic tests.
type FakeClock struct {
	mu  sync.Mutex
	now time.Time
}

// NewFakeClock creates a FakeClock set to the given time.
func NewFakeClock(now time.Time) *FakeClock {
	return &FakeClock{now: now}
}

// Now returns the current fake time.
func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Advance moves the clock forward by d.
func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

// Set sets the clock to a specific time.
func (c *FakeClock) Set(t time.Time) {
	c.mu.Lock()
	c.now = t
	c.mu.Unlock()
}

// After delegates to time.After (FakeClock doesn't control timers).
func (c *FakeClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// NewTicker delegates to RealClock (FakeClock doesn't control tickers).
func (c *FakeClock) NewTicker(d time.Duration) *Ticker {
	return RealClock{}.NewTicker(d)
}

// RealClock delegates to the time package. Used in production.
type RealClock struct{}

// Now returns the current time.
func (RealClock) Now() time.Time { return time.Now() }

// After waits for the duration to elapse and then sends the current time on the returned channel.
func (RealClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// NewTicker returns a new Ticker containing a channel that will send the current time
// on the channel after each tick.
func (RealClock) NewTicker(d time.Duration) *Ticker {
	t := time.NewTicker(d)
	return &Ticker{C: t.C, stop: t.Stop}
}
