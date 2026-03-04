package clock

import "time"

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
