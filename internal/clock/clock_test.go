package clock

import (
	"sync/atomic"
	"testing"
	"time"
)

var epoch = time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

func TestRealClockSatisfiesInterface(t *testing.T) {
	var _ Clock = RealClock{}
}

func TestFakeClockSatisfiesInterface(t *testing.T) {
	var _ Clock = &FakeClock{}
}

func TestFakeClock_Now(t *testing.T) {
	c := NewFakeClock(epoch)
	if got := c.Now(); !got.Equal(epoch) {
		t.Fatalf("Now() = %v, want %v", got, epoch)
	}
}

func TestFakeClock_Since(t *testing.T) {
	c := NewFakeClock(epoch.Add(10 * time.Second))
	if got := c.Since(epoch); got != 10*time.Second {
		t.Fatalf("Since() = %v, want 10s", got)
	}
}

func TestFakeClock_Set(t *testing.T) {
	c := NewFakeClock(epoch)
	target := epoch.Add(42 * time.Hour)
	c.Set(target)
	if got := c.Now(); !got.Equal(target) {
		t.Fatalf("Now() after Set = %v, want %v", got, target)
	}
}

func TestFakeClock_Advance(t *testing.T) {
	c := NewFakeClock(epoch)
	c.Advance(5 * time.Second)
	want := epoch.Add(5 * time.Second)
	if got := c.Now(); !got.Equal(want) {
		t.Fatalf("Now() after Advance = %v, want %v", got, want)
	}
}

func TestFakeClock_After(t *testing.T) {
	c := NewFakeClock(epoch)
	ch := c.After(10 * time.Second)

	// Should not fire yet.
	select {
	case <-ch:
		t.Fatal("After fired too early")
	default:
	}

	c.Advance(9 * time.Second)
	select {
	case <-ch:
		t.Fatal("After fired too early at 9s")
	default:
	}

	c.Advance(1 * time.Second)
	select {
	case got := <-ch:
		want := epoch.Add(10 * time.Second)
		if !got.Equal(want) {
			t.Fatalf("After sent %v, want %v", got, want)
		}
	default:
		t.Fatal("After did not fire at 10s")
	}
}

func TestFakeClock_Sleep(t *testing.T) {
	c := NewFakeClock(epoch)
	done := make(chan struct{})
	go func() {
		c.Sleep(5 * time.Second)
		close(done)
	}()

	// Wait for the goroutine to register its sleep timer.
	if !c.WaitForTimers(1, time.Second) {
		t.Fatal("timed out waiting for sleep timer to register")
	}

	c.Advance(5 * time.Second)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Sleep did not unblock after Advance")
	}
}

func TestFakeClock_NewTimer(t *testing.T) {
	c := NewFakeClock(epoch)
	timer := c.NewTimer(3 * time.Second)

	c.Advance(2 * time.Second)
	select {
	case <-timer.C:
		t.Fatal("timer fired too early")
	default:
	}

	c.Advance(1 * time.Second)
	select {
	case <-timer.C:
	default:
		t.Fatal("timer did not fire")
	}
}

func TestFakeClock_TimerStop(t *testing.T) {
	c := NewFakeClock(epoch)
	timer := c.NewTimer(3 * time.Second)

	wasPending := timer.Stop()
	if !wasPending {
		t.Fatal("Stop() should return true for pending timer")
	}

	c.Advance(5 * time.Second)
	select {
	case <-timer.C:
		t.Fatal("stopped timer should not fire")
	default:
	}
}

func TestFakeClock_TimerReset(t *testing.T) {
	c := NewFakeClock(epoch)
	timer := c.NewTimer(3 * time.Second)

	timer.Reset(10 * time.Second)

	c.Advance(5 * time.Second)
	select {
	case <-timer.C:
		t.Fatal("timer should not fire at 5s after reset to 10s")
	default:
	}

	c.Advance(5 * time.Second)
	select {
	case <-timer.C:
	default:
		t.Fatal("timer should fire at 10s")
	}
}

func TestFakeClock_AfterFunc(t *testing.T) {
	c := NewFakeClock(epoch)
	var called atomic.Bool
	c.AfterFunc(5*time.Second, func() {
		called.Store(true)
	})

	c.Advance(4 * time.Second)
	if called.Load() {
		t.Fatal("AfterFunc fired too early")
	}

	c.Advance(1 * time.Second)
	if !called.Load() {
		t.Fatal("AfterFunc did not fire at 5s")
	}
}

func TestFakeClock_AfterFuncStop(t *testing.T) {
	c := NewFakeClock(epoch)
	var called atomic.Bool
	timer := c.AfterFunc(5*time.Second, func() {
		called.Store(true)
	})
	timer.Stop()

	c.Advance(10 * time.Second)
	if called.Load() {
		t.Fatal("stopped AfterFunc should not fire")
	}
}

func TestFakeClock_NewTicker(t *testing.T) {
	c := NewFakeClock(epoch)
	ticker := c.NewTicker(3 * time.Second)
	defer ticker.Stop()

	// Tick 1 at 3s
	c.Advance(3 * time.Second)
	select {
	case <-ticker.C:
	default:
		t.Fatal("ticker should fire at 3s")
	}

	// Tick 2 at 6s
	c.Advance(3 * time.Second)
	select {
	case <-ticker.C:
	default:
		t.Fatal("ticker should fire at 6s")
	}

	// No tick at 8s (only 2s since last)
	c.Advance(2 * time.Second)
	select {
	case <-ticker.C:
		t.Fatal("ticker should not fire at 8s")
	default:
	}

	// Tick 3 at 9s
	c.Advance(1 * time.Second)
	select {
	case <-ticker.C:
	default:
		t.Fatal("ticker should fire at 9s")
	}
}

func TestFakeClock_TickerStop(t *testing.T) {
	c := NewFakeClock(epoch)
	ticker := c.NewTicker(3 * time.Second)
	ticker.Stop()

	c.Advance(10 * time.Second)
	select {
	case <-ticker.C:
		t.Fatal("stopped ticker should not fire")
	default:
	}
}

func TestFakeClock_MultipleTimersFIFO(t *testing.T) {
	c := NewFakeClock(epoch)

	var order []int
	c.AfterFunc(3*time.Second, func() { order = append(order, 1) })
	c.AfterFunc(5*time.Second, func() { order = append(order, 2) })
	c.AfterFunc(1*time.Second, func() { order = append(order, 3) })

	c.Advance(5 * time.Second)

	if len(order) != 3 {
		t.Fatalf("expected 3 fires, got %d", len(order))
	}
	// Should fire in deadline order: 1s, 3s, 5s
	if order[0] != 3 || order[1] != 1 || order[2] != 2 {
		t.Fatalf("fire order = %v, want [3 1 2]", order)
	}
}

func TestFakeClock_LargeAdvanceFiresMultipleTickerTicks(t *testing.T) {
	c := NewFakeClock(epoch)
	ticker := c.NewTicker(2 * time.Second)
	defer ticker.Stop()

	// Advance 7s — should fire at 2s, 4s, 6s (3 ticks).
	// Channel buffer is 1, so only the first tick is buffered;
	// subsequent ticks are dropped (same as time.Ticker behavior).
	c.Advance(7 * time.Second)

	select {
	case <-ticker.C:
	default:
		t.Fatal("expected at least one tick")
	}
}

func TestFakeClock_ZeroDuration_After(t *testing.T) {
	c := NewFakeClock(epoch)
	ch := c.After(0)

	// Zero-duration timer should fire immediately on next Advance(0).
	c.Advance(0)
	select {
	case <-ch:
	default:
		t.Fatal("After(0) should fire on Advance(0)")
	}
}
