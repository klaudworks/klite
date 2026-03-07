package cluster

import "sync/atomic"

// FetchMetrics tracks fetch long-poll behavior with atomic counters.
// Used to diagnose whether fetch waiters are being woken by producers
// or timing out. All methods are safe for concurrent use.
type FetchMetrics struct {
	// Fetch handler counters
	ImmediateData   atomic.Int64 // fetch had data on first try (no long-poll)
	LongPollEntered atomic.Int64 // fetch entered long-poll (totalBytes < MinBytes)
	WokenByNotify   atomic.Int64 // waiter channel fired (producer woke us)
	WokenByTimer    atomic.Int64 // MaxWaitMs timer expired
	WokenByShutdown atomic.Int64 // broker shutdown

	// Long-poll wait time buckets (ms): how long select blocked before waking
	WaitUnder1ms   atomic.Int64
	WaitUnder5ms   atomic.Int64
	WaitUnder20ms  atomic.Int64
	WaitUnder100ms atomic.Int64
	WaitOver100ms  atomic.Int64

	// Handler total duration buckets (ms): time from handler entry to return
	HandlerUnder1ms   atomic.Int64
	HandlerUnder5ms   atomic.Int64
	HandlerUnder20ms  atomic.Int64
	HandlerUnder100ms atomic.Int64
	HandlerOver100ms  atomic.Int64

	// Waiter registration: how many partitions each long-poll registers on
	RegisteredOnZero atomic.Int64 // long-poll registered waiter on 0 partitions
	RegisteredOnOne  atomic.Int64 // long-poll registered waiter on 1 partition
	RegisteredOnMany atomic.Int64 // long-poll registered waiter on 2+ partitions

	// Why reg_on_0 happens (only counted when regCount==0)
	SkippedNilPD  atomic.Int64 // partition data was nil (topic/partition unknown)
	SkippedError  atomic.Int64 // partition had a non-zero error code
	ErrBelowStart atomic.Int64 // fetchOffset < logStart
	ErrAboveHW    atomic.Int64 // fetchOffset > hw

	// Partition-side counters
	NotifyCalls      atomic.Int64 // NotifyWaiters() invoked
	NotifyWaitersHit atomic.Int64 // total waiters found across all NotifyWaiters calls
}

// RecordWait records how long the long-poll select blocked.
func (m *FetchMetrics) RecordWait(ms int64) {
	switch {
	case ms < 1:
		m.WaitUnder1ms.Add(1)
	case ms < 5:
		m.WaitUnder5ms.Add(1)
	case ms < 20:
		m.WaitUnder20ms.Add(1)
	case ms < 100:
		m.WaitUnder100ms.Add(1)
	default:
		m.WaitOver100ms.Add(1)
	}
}

// RecordHandlerDuration records total fetch handler duration.
func (m *FetchMetrics) RecordHandlerDuration(ms int64) {
	switch {
	case ms < 1:
		m.HandlerUnder1ms.Add(1)
	case ms < 5:
		m.HandlerUnder5ms.Add(1)
	case ms < 20:
		m.HandlerUnder20ms.Add(1)
	case ms < 100:
		m.HandlerUnder100ms.Add(1)
	default:
		m.HandlerOver100ms.Add(1)
	}
}

// Snapshot returns a point-in-time copy of all counters.
func (m *FetchMetrics) Snapshot() FetchMetricsSnapshot {
	return FetchMetricsSnapshot{
		ImmediateData:     m.ImmediateData.Load(),
		LongPollEntered:   m.LongPollEntered.Load(),
		WokenByNotify:     m.WokenByNotify.Load(),
		WokenByTimer:      m.WokenByTimer.Load(),
		WokenByShutdown:   m.WokenByShutdown.Load(),
		WaitUnder1ms:      m.WaitUnder1ms.Load(),
		WaitUnder5ms:      m.WaitUnder5ms.Load(),
		WaitUnder20ms:     m.WaitUnder20ms.Load(),
		WaitUnder100ms:    m.WaitUnder100ms.Load(),
		WaitOver100ms:     m.WaitOver100ms.Load(),
		HandlerUnder1ms:   m.HandlerUnder1ms.Load(),
		HandlerUnder5ms:   m.HandlerUnder5ms.Load(),
		HandlerUnder20ms:  m.HandlerUnder20ms.Load(),
		HandlerUnder100ms: m.HandlerUnder100ms.Load(),
		HandlerOver100ms:  m.HandlerOver100ms.Load(),
		RegisteredOnZero:  m.RegisteredOnZero.Load(),
		RegisteredOnOne:   m.RegisteredOnOne.Load(),
		RegisteredOnMany:  m.RegisteredOnMany.Load(),
		SkippedNilPD:      m.SkippedNilPD.Load(),
		SkippedError:      m.SkippedError.Load(),
		ErrBelowStart:     m.ErrBelowStart.Load(),
		ErrAboveHW:        m.ErrAboveHW.Load(),
		NotifyCalls:       m.NotifyCalls.Load(),
		NotifyWaitersHit:  m.NotifyWaitersHit.Load(),
	}
}

// SnapshotAndReset atomically loads and resets all counters, returning the
// values accumulated since the last reset. Used for per-interval reporting.
func (m *FetchMetrics) SnapshotAndReset() FetchMetricsSnapshot {
	return FetchMetricsSnapshot{
		ImmediateData:     m.ImmediateData.Swap(0),
		LongPollEntered:   m.LongPollEntered.Swap(0),
		WokenByNotify:     m.WokenByNotify.Swap(0),
		WokenByTimer:      m.WokenByTimer.Swap(0),
		WokenByShutdown:   m.WokenByShutdown.Swap(0),
		WaitUnder1ms:      m.WaitUnder1ms.Swap(0),
		WaitUnder5ms:      m.WaitUnder5ms.Swap(0),
		WaitUnder20ms:     m.WaitUnder20ms.Swap(0),
		WaitUnder100ms:    m.WaitUnder100ms.Swap(0),
		WaitOver100ms:     m.WaitOver100ms.Swap(0),
		HandlerUnder1ms:   m.HandlerUnder1ms.Swap(0),
		HandlerUnder5ms:   m.HandlerUnder5ms.Swap(0),
		HandlerUnder20ms:  m.HandlerUnder20ms.Swap(0),
		HandlerUnder100ms: m.HandlerUnder100ms.Swap(0),
		HandlerOver100ms:  m.HandlerOver100ms.Swap(0),
		RegisteredOnZero:  m.RegisteredOnZero.Swap(0),
		RegisteredOnOne:   m.RegisteredOnOne.Swap(0),
		RegisteredOnMany:  m.RegisteredOnMany.Swap(0),
		SkippedNilPD:      m.SkippedNilPD.Swap(0),
		SkippedError:      m.SkippedError.Swap(0),
		ErrBelowStart:     m.ErrBelowStart.Swap(0),
		ErrAboveHW:        m.ErrAboveHW.Swap(0),
		NotifyCalls:       m.NotifyCalls.Swap(0),
		NotifyWaitersHit:  m.NotifyWaitersHit.Swap(0),
	}
}

// FetchMetricsSnapshot is a point-in-time copy of all counters.
type FetchMetricsSnapshot struct {
	ImmediateData     int64
	LongPollEntered   int64
	WokenByNotify     int64
	WokenByTimer      int64
	WokenByShutdown   int64
	WaitUnder1ms      int64
	WaitUnder5ms      int64
	WaitUnder20ms     int64
	WaitUnder100ms    int64
	WaitOver100ms     int64
	HandlerUnder1ms   int64
	HandlerUnder5ms   int64
	HandlerUnder20ms  int64
	HandlerUnder100ms int64
	HandlerOver100ms  int64
	RegisteredOnZero  int64
	RegisteredOnOne   int64
	RegisteredOnMany  int64
	SkippedNilPD      int64
	SkippedError      int64
	ErrBelowStart     int64
	ErrAboveHW        int64
	NotifyCalls       int64
	NotifyWaitersHit  int64
}

// IsZero returns true if no fetch activity occurred in this snapshot.
func (s FetchMetricsSnapshot) IsZero() bool {
	return s.LongPollEntered == 0 && s.ImmediateData == 0 && s.NotifyCalls == 0
}
