package bench

import (
	"sync"
	"time"
)

// ThroughputThrottler limits throughput to approximately targetThroughput
// records/sec. Mirrors Kafka's ThroughputThrottler: accumulates a sleep
// deficit and sleeps when it exceeds 2ms.
type ThroughputThrottler struct {
	targetThroughput float64
	startTime        time.Time
	sleepTimeNs      int64
	sleepDeficitNs   int64

	mu     sync.Mutex
	wakeup bool
}

const minSleepNs = 2_000_000 // 2ms

// NewThroughputThrottler creates a throttler. If targetThroughput < 0, no
// throttling occurs.
func NewThroughputThrottler(targetThroughput float64, startTime time.Time) *ThroughputThrottler {
	sleepTimeNs := int64(0)
	if targetThroughput > 0 {
		sleepTimeNs = int64(float64(time.Second) / targetThroughput)
	}
	return &ThroughputThrottler{
		targetThroughput: targetThroughput,
		startTime:        startTime,
		sleepTimeNs:      sleepTimeNs,
	}
}

// ShouldThrottle returns true if the caller should call Throttle().
func (t *ThroughputThrottler) ShouldThrottle(amountSoFar int64, sendStart time.Time) bool {
	if t.targetThroughput < 0 {
		return false
	}
	elapsedSec := sendStart.Sub(t.startTime).Seconds()
	return elapsedSec > 0 && (float64(amountSoFar)/elapsedSec) > t.targetThroughput
}

// Throttle sleeps to maintain the target throughput.
func (t *ThroughputThrottler) Throttle() {
	if t.targetThroughput == 0 {
		// Block indefinitely until woken.
		t.mu.Lock()
		for !t.wakeup {
			t.mu.Unlock()
			time.Sleep(time.Millisecond)
			t.mu.Lock()
		}
		t.wakeup = false
		t.mu.Unlock()
		return
	}

	t.sleepDeficitNs += t.sleepTimeNs
	if t.sleepDeficitNs >= minSleepNs {
		time.Sleep(time.Duration(t.sleepDeficitNs))
		t.sleepDeficitNs = 0
	}
}

// Wakeup unblocks a throttler sleeping with targetThroughput == 0.
func (t *ThroughputThrottler) Wakeup() {
	t.mu.Lock()
	t.wakeup = true
	t.mu.Unlock()
}
