package clock

import (
	"container/heap"
	"runtime"
	"sync"
	"time"
)

// Clock abstracts time operations so tests can use a fake clock.
type Clock interface {
	Now() time.Time
	Since(t time.Time) time.Duration
	After(d time.Duration) <-chan time.Time
	NewTicker(d time.Duration) *Ticker
	NewTimer(d time.Duration) *Timer
	AfterFunc(d time.Duration, f func()) *Timer
	Sleep(d time.Duration)
}

// Ticker wraps a stoppable channel-based ticker.
type Ticker struct {
	C    <-chan time.Time
	stop func()
}

func (t *Ticker) Stop() { t.stop() }

// Timer wraps a stoppable/resettable channel-based timer.
type Timer struct {
	C    <-chan time.Time
	stop func() bool
	// reset is only usable with RealClock timers.
	reset func(d time.Duration) bool
}

func (t *Timer) Stop() bool                 { return t.stop() }
func (t *Timer) Reset(d time.Duration) bool { return t.reset(d) }

// ---------------------------------------------------------------------------
// RealClock
// ---------------------------------------------------------------------------

type RealClock struct{}

func (RealClock) Now() time.Time                         { return time.Now() }
func (RealClock) Since(t time.Time) time.Duration        { return time.Since(t) }
func (RealClock) After(d time.Duration) <-chan time.Time { return time.After(d) }
func (RealClock) Sleep(d time.Duration)                  { time.Sleep(d) }

func (RealClock) NewTicker(d time.Duration) *Ticker {
	t := time.NewTicker(d)
	return &Ticker{C: t.C, stop: t.Stop}
}

func (RealClock) NewTimer(d time.Duration) *Timer {
	t := time.NewTimer(d)
	return &Timer{C: t.C, stop: t.Stop, reset: t.Reset}
}

func (RealClock) AfterFunc(d time.Duration, f func()) *Timer {
	t := time.AfterFunc(d, f)
	return &Timer{
		C:     nil,
		stop:  t.Stop,
		reset: t.Reset,
	}
}

// ---------------------------------------------------------------------------
// FakeClock — deterministic clock for tests, driven by Advance().
// ---------------------------------------------------------------------------

type FakeClock struct {
	mu      sync.Mutex
	now     time.Time
	waiters timerHeap
	id      uint64 // monotonic ID for stable heap ordering
}

func NewFakeClock(now time.Time) *FakeClock {
	return &FakeClock{now: now}
}

func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *FakeClock) Since(t time.Time) time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now.Sub(t)
}

func (c *FakeClock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = t
}

// WaitForTimers spins until at least n timers are registered on the heap,
// or timeout elapses. Returns true if the condition was met. This is useful
// for test synchronization: after unblocking a goroutine via Advance, call
// WaitForTimers to ensure the goroutine has re-registered its next timer
// before advancing again.
func (c *FakeClock) WaitForTimers(n int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		count := len(c.waiters)
		c.mu.Unlock()
		if count >= n {
			return true
		}
		runtime.Gosched()
	}
	return false
}

// Advance moves the fake clock forward by d and fires all pending timers
// whose deadline is at or before the new time. Timers fire in deadline order.
// AfterFunc callbacks run synchronously inside Advance so tests are
// deterministic. Advance must not be called concurrently with itself.
func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	now := c.now
	c.mu.Unlock()

	c.fireReady(now)
}

// fireReady fires all timers whose deadline <= now.
func (c *FakeClock) fireReady(now time.Time) {
	for {
		c.mu.Lock()
		if len(c.waiters) == 0 || c.waiters[0].deadline.After(now) {
			c.mu.Unlock()
			return
		}
		w := heap.Pop(&c.waiters).(*fakeTimer)
		c.mu.Unlock()

		w.fire(now)

		// Re-register tickers for the next interval.
		if w.interval > 0 && !w.stopped {
			c.mu.Lock()
			// deadline was already advanced by fire()
			heap.Push(&c.waiters, w)
			c.mu.Unlock()
		}
	}
}

func (c *FakeClock) After(d time.Duration) <-chan time.Time {
	t := c.NewTimer(d)
	return t.C
}

func (c *FakeClock) Sleep(d time.Duration) {
	ch := c.After(d)
	<-ch
}

func (c *FakeClock) NewTicker(d time.Duration) *Ticker {
	if d <= 0 {
		panic("clock: non-positive interval for NewTicker")
	}
	ch := make(chan time.Time, 1)
	ft := c.newFakeTimer(d, ch, d)
	return &Ticker{C: ch, stop: func() { ft.cancel(c) }}
}

func (c *FakeClock) NewTimer(d time.Duration) *Timer {
	ch := make(chan time.Time, 1)
	ft := c.newFakeTimer(d, ch, 0)
	return &Timer{
		C:    ch,
		stop: func() bool { return ft.cancel(c) },
		reset: func(nd time.Duration) bool {
			return ft.resetTimer(c, nd)
		},
	}
}

func (c *FakeClock) AfterFunc(d time.Duration, f func()) *Timer {
	ft := c.newFakeTimer(d, nil, 0)
	ft.fn = f
	return &Timer{
		C:    nil,
		stop: func() bool { return ft.cancel(c) },
		reset: func(nd time.Duration) bool {
			return ft.resetTimer(c, nd)
		},
	}
}

// newFakeTimer creates and registers a timer on the heap.
// If interval > 0, the timer repeats (ticker behavior).
func (c *FakeClock) newFakeTimer(d time.Duration, ch chan time.Time, interval time.Duration) *fakeTimer {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.id++
	ft := &fakeTimer{
		deadline: c.now.Add(d),
		ch:       ch,
		interval: interval,
		id:       c.id,
	}
	heap.Push(&c.waiters, ft)
	return ft
}

// ---------------------------------------------------------------------------
// fakeTimer — an entry in the FakeClock's timer heap.
// ---------------------------------------------------------------------------

type fakeTimer struct {
	deadline time.Time
	ch       chan time.Time // nil for AfterFunc timers
	fn       func()         // non-nil for AfterFunc timers
	interval time.Duration  // >0 for tickers
	id       uint64         // for stable ordering
	index    int            // heap index
	stopped  bool
}

// fire sends on the channel or calls fn, then re-registers if it's a ticker.
func (ft *fakeTimer) fire(now time.Time) {
	if ft.fn != nil {
		ft.fn()
	} else if ft.ch != nil {
		select {
		case ft.ch <- now:
		default:
			// Drop if receiver isn't ready (matches time.Ticker behavior).
		}
	}

	if ft.interval > 0 && !ft.stopped {
		ft.deadline = ft.deadline.Add(ft.interval)
		// Re-insert manually; we don't have a pointer to the clock here,
		// but fire is always called from fireReady which will re-check.
		// We handle re-registration via the afterFire path below.
	}
}

// cancel removes the timer from the clock's heap. Returns true if it was
// pending (hadn't fired yet).
func (ft *fakeTimer) cancel(c *FakeClock) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	ft.stopped = true
	if ft.index >= 0 && ft.index < len(c.waiters) && c.waiters[ft.index] == ft {
		heap.Remove(&c.waiters, ft.index)
		return true
	}
	return false
}

func (ft *fakeTimer) resetTimer(c *FakeClock, d time.Duration) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	wasPending := false
	if ft.index >= 0 && ft.index < len(c.waiters) && c.waiters[ft.index] == ft {
		heap.Remove(&c.waiters, ft.index)
		wasPending = true
	}
	ft.stopped = false
	ft.deadline = c.now.Add(d)
	heap.Push(&c.waiters, ft)
	return wasPending
}

// ---------------------------------------------------------------------------
// timerHeap — min-heap ordered by (deadline, id).
// ---------------------------------------------------------------------------

type timerHeap []*fakeTimer

func (h timerHeap) Len() int { return len(h) }
func (h timerHeap) Less(i, j int) bool {
	if h[i].deadline.Equal(h[j].deadline) {
		return h[i].id < h[j].id
	}
	return h[i].deadline.Before(h[j].deadline)
}

func (h timerHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *timerHeap) Push(x interface{}) {
	ft := x.(*fakeTimer)
	ft.index = len(*h)
	*h = append(*h, ft)
}

func (h *timerHeap) Pop() interface{} {
	old := *h
	n := len(old)
	ft := old[n-1]
	old[n-1] = nil
	ft.index = -1
	*h = old[:n-1]
	return ft
}
