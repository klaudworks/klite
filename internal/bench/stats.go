package bench

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"
)

// WindowSnapshot is one JSON Lines entry written every reporting interval.
type WindowSnapshot struct {
	TimestampUTC string  `json:"ts"`
	ElapsedSec   float64 `json:"elapsed_s"`
	Type         string  `json:"type"` // "window" or "final"

	// Window produce metrics (this interval only)
	WindowRecords int64   `json:"w_records"`
	WindowRecsSec float64 `json:"w_recs_sec"`
	WindowMBSec   float64 `json:"w_mb_sec"`
	WindowAvgMs   float64 `json:"w_avg_ms"`
	WindowP50Ms   int     `json:"w_p50_ms"`
	WindowP95Ms   int     `json:"w_p95_ms"`
	WindowP99Ms   int     `json:"w_p99_ms"`
	WindowP999Ms  int     `json:"w_p999_ms"`
	WindowMaxMs   int     `json:"w_max_ms"`

	// Window e2e latency (produce-consume mode only, omitted otherwise)
	WindowE2EP50Ms  int `json:"w_e2e_p50_ms,omitempty"`
	WindowE2EP95Ms  int `json:"w_e2e_p95_ms,omitempty"`
	WindowE2EP99Ms  int `json:"w_e2e_p99_ms,omitempty"`
	WindowE2EP999Ms int `json:"w_e2e_p999_ms,omitempty"`
	WindowE2EMaxMs  int `json:"w_e2e_max_ms,omitempty"`

	// Window consume metrics (produce-consume mode only)
	WindowConsumed int64 `json:"w_consumed,omitempty"`

	// Cumulative metrics (since warmup completed)
	CumRecords  int64   `json:"c_records"`
	CumRecsSec  float64 `json:"c_recs_sec"`
	CumMBSec    float64 `json:"c_mb_sec"`
	CumAvgMs    float64 `json:"c_avg_ms"`
	CumMaxMs    int     `json:"c_max_ms"`
	CumConsumed int64   `json:"c_consumed,omitempty"`
}

// Stats tracks latency and throughput metrics.
// Per-window latencies are used for percentile calculation —
// no global latency array, so memory usage stays bounded over long runs.
type Stats struct {
	mu sync.Mutex

	measurementStarted bool
	warmupTotal        int64 // stored for log message only

	start time.Time

	count        int64
	bytes        int64
	totalLatency int64
	maxLatency   int

	windowStart        time.Time
	windowCount        int64
	windowBytes        int64
	windowTotalLatency int64
	windowMaxLatency   int
	windowLatencies    []int

	// For the final summary we keep a running merge of all window percentiles.
	// Each window's percentiles are appended here (fixed 4 entries per window).
	allWindowP50  []int
	allWindowP95  []int
	allWindowP99  []int
	allWindowP999 []int

	// E2E latency tracking (produce-consume mode).
	// Uses a separate mutex to avoid contention with produce path.
	e2eMu              sync.Mutex
	e2eEnabled         bool
	windowE2ELatencies []int
	windowE2EMax       int
	windowConsumed     int64
	cumConsumed        int64
	allWindowE2EP50    []int
	allWindowE2EP95    []int
	allWindowE2EP99    []int
	allWindowE2EP999   []int
	cumE2EMax          int

	reportingInterval time.Duration

	out     io.Writer
	jsonOut io.Writer // JSON Lines writer (nil = disabled)
}

// NewStats creates a Stats tracker.
// Callers are responsible for not calling Record() for warmup records.
// On the first Record() call, Stats resets the start time (so warmup
// time is excluded from throughput calculations).
func NewStats(numRecords, warmupRecords int64, reportingInterval time.Duration, out io.Writer) *Stats {
	now := time.Now()
	return &Stats{
		start:              now,
		windowStart:        now,
		warmupTotal:        warmupRecords,
		measurementStarted: warmupRecords <= 0,
		reportingInterval:  reportingInterval,
		out:                out,
	}
}

// SetJSONOutput sets the writer for JSON Lines time-series output.
func (s *Stats) SetJSONOutput(w io.Writer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.jsonOut = w
}

// EnableE2E turns on end-to-end latency tracking for produce-consume mode.
func (s *Stats) EnableE2E() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.e2eEnabled = true
}

// RecordE2E records a single consumed record's end-to-end latency.
// Called from the consumer side. Uses a separate lock from Record()
// to avoid contention between producer and consumer hot paths.
func (s *Stats) RecordE2E(latencyMs int) {
	s.e2eMu.Lock()
	defer s.e2eMu.Unlock()

	s.windowE2ELatencies = append(s.windowE2ELatencies, latencyMs)
	s.windowConsumed++
	s.cumConsumed++
	if latencyMs > s.windowE2EMax {
		s.windowE2EMax = latencyMs
	}
	if latencyMs > s.cumE2EMax {
		s.cumE2EMax = latencyMs
	}
}

// Record records a single completed send.
// Callers must NOT call this for warmup records.
func (s *Stats) Record(latencyMs, bytes int, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.measurementStarted {
		s.start = now
		s.windowStart = now
		s.measurementStarted = true
		if s.out != nil {
			_, _ = fmt.Fprintf(s.out, "Warmup complete (%d records), starting measurement.\n", s.warmupTotal)
		}
	}

	s.count++
	s.bytes += int64(bytes)
	s.totalLatency += int64(latencyMs)
	if latencyMs > s.maxLatency {
		s.maxLatency = latencyMs
	}

	s.windowCount++
	s.windowBytes += int64(bytes)
	s.windowTotalLatency += int64(latencyMs)
	if latencyMs > s.windowMaxLatency {
		s.windowMaxLatency = latencyMs
	}
	s.windowLatencies = append(s.windowLatencies, latencyMs)

	if now.Sub(s.windowStart) >= s.reportingInterval {
		s.emitWindow(now)
		s.newWindow(now)
	}
}

func (s *Stats) emitWindow(now time.Time) {
	elapsed := now.Sub(s.windowStart)
	elapsedMs := elapsed.Milliseconds()
	if elapsedMs == 0 {
		elapsedMs = 1
	}
	wRecsPerSec := 1000.0 * float64(s.windowCount) / float64(elapsedMs)
	wMBPerSec := 1000.0 * float64(s.windowBytes) / float64(elapsedMs) / (1024.0 * 1024.0)
	wAvgLatency := float64(s.windowTotalLatency) / float64(max64(s.windowCount, 1))
	percs := percentiles(s.windowLatencies, 0.5, 0.95, 0.99, 0.999)

	// Track for final summary.
	s.allWindowP50 = append(s.allWindowP50, percs[0])
	s.allWindowP95 = append(s.allWindowP95, percs[1])
	s.allWindowP99 = append(s.allWindowP99, percs[2])
	s.allWindowP999 = append(s.allWindowP999, percs[3])

	// Snapshot e2e stats under separate lock.
	// Swap the slice out quickly, then sort outside the lock so consumer
	// goroutines calling RecordE2E() are never blocked during the sort.
	var e2ePercs []int
	var e2eMax int
	var wConsumed, cConsumed int64
	var e2eSnap []int
	if s.e2eEnabled {
		s.e2eMu.Lock()
		e2eSnap = s.windowE2ELatencies
		s.windowE2ELatencies = make([]int, 0, len(e2eSnap))
		e2eMax = s.windowE2EMax
		wConsumed = s.windowConsumed
		cConsumed = s.cumConsumed
		s.windowE2EMax = 0
		s.windowConsumed = 0
		s.e2eMu.Unlock()

		e2ePercs = percentiles(e2eSnap, 0.5, 0.95, 0.99, 0.999)
		s.allWindowE2EP50 = append(s.allWindowE2EP50, e2ePercs[0])
		s.allWindowE2EP95 = append(s.allWindowE2EP95, e2ePercs[1])
		s.allWindowE2EP99 = append(s.allWindowE2EP99, e2ePercs[2])
		s.allWindowE2EP999 = append(s.allWindowE2EP999, e2ePercs[3])
	}

	// Cumulative.
	totalElapsedMs := now.Sub(s.start).Milliseconds()
	if totalElapsedMs == 0 {
		totalElapsedMs = 1
	}
	cRecsPerSec := 1000.0 * float64(s.count) / float64(totalElapsedMs)
	cMBPerSec := 1000.0 * float64(s.bytes) / float64(totalElapsedMs) / (1024.0 * 1024.0)
	cAvgLatency := float64(s.totalLatency) / float64(max64(s.count, 1))

	// Print human-readable.
	if s.e2eEnabled {
		_, _ = fmt.Fprintf(s.out, "%d records sent, %.1f records/sec (%.2f MB/sec), "+
			"produce %d/%d/%d/%d ms (p50/p95/p99/p999), "+
			"e2e %d/%d/%d/%d ms, consumed %d.\n",
			s.windowCount, wRecsPerSec, wMBPerSec,
			percs[0], percs[1], percs[2], percs[3],
			e2ePercs[0], e2ePercs[1], e2ePercs[2], e2ePercs[3],
			wConsumed)
	} else {
		_, _ = fmt.Fprintf(s.out, "%d records sent, %.1f records/sec (%.2f MB/sec), "+
			"%.1f ms avg latency, %d ms p50, %d ms p95, %d ms p99, %d ms p999, %d ms max.\n",
			s.windowCount, wRecsPerSec, wMBPerSec, wAvgLatency,
			percs[0], percs[1], percs[2], percs[3], s.windowMaxLatency)
	}

	// Write JSON Lines.
	if s.jsonOut != nil {
		snap := WindowSnapshot{
			TimestampUTC:  now.UTC().Format(time.RFC3339),
			ElapsedSec:    float64(totalElapsedMs) / 1000.0,
			Type:          "window",
			WindowRecords: s.windowCount,
			WindowRecsSec: wRecsPerSec,
			WindowMBSec:   wMBPerSec,
			WindowAvgMs:   wAvgLatency,
			WindowP50Ms:   percs[0],
			WindowP95Ms:   percs[1],
			WindowP99Ms:   percs[2],
			WindowP999Ms:  percs[3],
			WindowMaxMs:   s.windowMaxLatency,
			CumRecords:    s.count,
			CumRecsSec:    cRecsPerSec,
			CumMBSec:      cMBPerSec,
			CumAvgMs:      cAvgLatency,
			CumMaxMs:      s.maxLatency,
		}
		if s.e2eEnabled {
			snap.WindowE2EP50Ms = e2ePercs[0]
			snap.WindowE2EP95Ms = e2ePercs[1]
			snap.WindowE2EP99Ms = e2ePercs[2]
			snap.WindowE2EP999Ms = e2ePercs[3]
			snap.WindowE2EMaxMs = e2eMax
			snap.WindowConsumed = wConsumed
			snap.CumConsumed = cConsumed
		}
		data, _ := json.Marshal(snap)
		_, _ = fmt.Fprintf(s.jsonOut, "%s\n", data)
	}
}

func (s *Stats) newWindow(now time.Time) {
	s.windowStart = now
	s.windowCount = 0
	s.windowBytes = 0
	s.windowTotalLatency = 0
	s.windowMaxLatency = 0
	s.windowLatencies = s.windowLatencies[:0]
}

// PrintTotal prints the final summary.
func (s *Stats) PrintTotal() {
	s.mu.Lock()
	defer s.mu.Unlock()

	elapsed := time.Since(s.start).Milliseconds()
	if elapsed == 0 {
		elapsed = 1
	}
	recsPerSec := 1000.0 * float64(s.count) / float64(elapsed)
	mbPerSec := 1000.0 * float64(s.bytes) / float64(elapsed) / (1024.0 * 1024.0)
	avgLatency := float64(s.totalLatency) / float64(max64(s.count, 1))

	// Use median of window percentiles for the final summary.
	p50 := medianInt(s.allWindowP50)
	p95 := medianInt(s.allWindowP95)
	p99 := medianInt(s.allWindowP99)
	p999 := medianInt(s.allWindowP999)

	if s.e2eEnabled {
		s.e2eMu.Lock()
		e2eP50 := medianInt(s.allWindowE2EP50)
		e2eP95 := medianInt(s.allWindowE2EP95)
		e2eP99 := medianInt(s.allWindowE2EP99)
		e2eP999 := medianInt(s.allWindowE2EP999)
		consumed := s.cumConsumed
		s.e2eMu.Unlock()

		_, _ = fmt.Fprintf(s.out, "%d records sent, %f records/sec (%.2f MB/sec), "+
			"produce %d/%d/%d/%d ms (p50/p95/p99/p999), "+
			"e2e %d/%d/%d/%d ms, consumed %d.\n",
			s.count, recsPerSec, mbPerSec,
			p50, p95, p99, p999,
			e2eP50, e2eP95, e2eP99, e2eP999,
			consumed)
	} else {
		_, _ = fmt.Fprintf(s.out, "%d records sent, %f records/sec (%.2f MB/sec), "+
			"%.2f ms avg latency, %d ms max latency, "+
			"%d ms 50th, %d ms 95th, %d ms 99th, %d ms 99.9th.\n",
			s.count, recsPerSec, mbPerSec,
			avgLatency, s.maxLatency,
			p50, p95, p99, p999)
	}

	// Write final JSON line.
	if s.jsonOut != nil {
		now := time.Now()
		snap := WindowSnapshot{
			TimestampUTC: now.UTC().Format(time.RFC3339),
			ElapsedSec:   float64(elapsed) / 1000.0,
			Type:         "final",
			WindowP50Ms:  p50,
			WindowP95Ms:  p95,
			WindowP99Ms:  p99,
			WindowP999Ms: p999,
			WindowMaxMs:  s.maxLatency,
			CumRecords:   s.count,
			CumRecsSec:   recsPerSec,
			CumMBSec:     mbPerSec,
			CumAvgMs:     avgLatency,
			CumMaxMs:     s.maxLatency,
		}
		if s.e2eEnabled {
			s.e2eMu.Lock()
			snap.WindowE2EP50Ms = medianInt(s.allWindowE2EP50)
			snap.WindowE2EP95Ms = medianInt(s.allWindowE2EP95)
			snap.WindowE2EP99Ms = medianInt(s.allWindowE2EP99)
			snap.WindowE2EP999Ms = medianInt(s.allWindowE2EP999)
			snap.WindowE2EMaxMs = s.cumE2EMax
			snap.CumConsumed = s.cumConsumed
			s.e2eMu.Unlock()
		}
		data, _ := json.Marshal(snap)
		_, _ = fmt.Fprintf(s.jsonOut, "%s\n", data)
	}
}

// Result returns the final metrics as a structured ProducerResult.
func (s *Stats) Result() *ProducerResult {
	s.mu.Lock()
	defer s.mu.Unlock()

	elapsed := time.Since(s.start).Milliseconds()
	if elapsed == 0 {
		elapsed = 1
	}
	recsPerSec := 1000.0 * float64(s.count) / float64(elapsed)
	mbPerSec := 1000.0 * float64(s.bytes) / float64(elapsed) / (1024.0 * 1024.0)
	avgLatency := float64(s.totalLatency) / float64(max64(s.count, 1))

	return &ProducerResult{
		Records:     s.count,
		Bytes:       s.bytes,
		ElapsedMs:   elapsed,
		RecsPerSec:  recsPerSec,
		MBPerSec:    mbPerSec,
		AvgLatency:  avgLatency,
		MaxLatency:  float64(s.maxLatency),
		P50Latency:  medianInt(s.allWindowP50),
		P95Latency:  medianInt(s.allWindowP95),
		P99Latency:  medianInt(s.allWindowP99),
		P999Latency: medianInt(s.allWindowP999),
	}
}

// TotalCount returns the total number of records recorded.
func (s *Stats) TotalCount() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.count
}

// CumConsumed returns the total consumed record count (e2e mode).
func (s *Stats) CumConsumed() int64 {
	s.e2eMu.Lock()
	defer s.e2eMu.Unlock()
	return s.cumConsumed
}

func percentiles(latencies []int, pcts ...float64) []int {
	n := len(latencies)
	if n == 0 {
		return make([]int, len(pcts))
	}
	sorted := make([]int, n)
	copy(sorted, latencies)
	sort.Ints(sorted)

	result := make([]int, len(pcts))
	for i, p := range pcts {
		idx := int(p * float64(n))
		if idx >= n {
			idx = n - 1
		}
		result[i] = sorted[idx]
	}
	return result
}

func medianInt(vals []int) int {
	n := len(vals)
	if n == 0 {
		return 0
	}
	sorted := make([]int, n)
	copy(sorted, vals)
	sort.Ints(sorted)
	return sorted[n/2]
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
