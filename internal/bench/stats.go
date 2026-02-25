package bench

import (
	"fmt"
	"io"
	"sort"
	"sync"
	"time"
)

// Stats tracks latency and throughput metrics, mirroring Kafka's
// ProducerPerformance.Stats. It samples latencies (up to 500k samples)
// and prints periodic window reports plus a final summary with percentiles.
type Stats struct {
	mu sync.Mutex

	start     time.Time
	sampling  int64 // record every Nth latency
	latencies []int // sampled latency values in ms
	index     int

	count        int64
	bytes        int64
	totalLatency int64
	maxLatency   int

	// Warmup: records before this threshold are not counted.
	warmupRecords int64
	warmupCount   int64

	windowStart        time.Time
	windowCount        int64
	windowBytes        int64
	windowTotalLatency int64
	windowMaxLatency   int

	iteration         int64
	reportingInterval time.Duration

	out io.Writer
}

// NewStats creates a Stats tracker.
// numRecords is used to size the latency sample array.
// warmupRecords is the number of initial records to discard from stats.
// reportingInterval controls how often window stats are printed.
func NewStats(numRecords, warmupRecords int64, reportingInterval time.Duration, out io.Writer) *Stats {
	sampling := numRecords / min64(numRecords, 500000)
	if sampling < 1 {
		sampling = 1
	}
	now := time.Now()
	return &Stats{
		start:             now,
		windowStart:       now,
		sampling:          sampling,
		latencies:         make([]int, 0, numRecords/sampling+1),
		warmupRecords:     warmupRecords,
		reportingInterval: reportingInterval,
		out:               out,
	}
}

// Record records a single completed send with latency in ms and payload size.
func (s *Stats) Record(latencyMs int, bytes int, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.iteration++

	// During warmup, just count but don't record stats.
	if s.warmupCount < s.warmupRecords {
		s.warmupCount++
		if s.warmupCount == s.warmupRecords {
			// Warmup complete — reset the clock.
			s.start = now
			s.windowStart = now
			if s.out != nil {
				fmt.Fprintf(s.out, "Warmup complete (%d records), starting measurement.\n", s.warmupRecords)
			}
		}
		return
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

	if (s.count-1)%s.sampling == 0 {
		s.latencies = append(s.latencies, latencyMs)
		s.index++
	}

	if now.Sub(s.windowStart) >= s.reportingInterval {
		s.printWindow()
		s.newWindow(now)
	}
}

func (s *Stats) printWindow() {
	elapsed := time.Since(s.windowStart)
	elapsedMs := elapsed.Milliseconds()
	if elapsedMs == 0 {
		elapsedMs = 1
	}
	recsPerSec := 1000.0 * float64(s.windowCount) / float64(elapsedMs)
	mbPerSec := 1000.0 * float64(s.windowBytes) / float64(elapsedMs) / (1024.0 * 1024.0)
	avgLatency := float64(s.windowTotalLatency) / float64(max64(s.windowCount, 1))

	fmt.Fprintf(s.out, "%d records sent, %.1f records/sec (%.2f MB/sec), %.1f ms avg latency, %.1f ms max latency.\n",
		s.windowCount, recsPerSec, mbPerSec, avgLatency, float64(s.windowMaxLatency))
}

func (s *Stats) newWindow(now time.Time) {
	s.windowStart = now
	s.windowCount = 0
	s.windowBytes = 0
	s.windowTotalLatency = 0
	s.windowMaxLatency = 0
}

// PrintTotal prints the final summary line matching Kafka's output format:
// N records sent, X records/sec (Y MB/sec), A ms avg latency, B ms max latency, P50 ms 50th, ...
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

	percs := percentiles(s.latencies[:s.index], 0.5, 0.95, 0.99, 0.999)

	fmt.Fprintf(s.out, "%d records sent, %f records/sec (%.2f MB/sec), "+
		"%.2f ms avg latency, %.2f ms max latency, "+
		"%d ms 50th, %d ms 95th, %d ms 99th, %d ms 99.9th.\n",
		s.count, recsPerSec, mbPerSec,
		avgLatency, float64(s.maxLatency),
		percs[0], percs[1], percs[2], percs[3])
}

// Result returns the final metrics as a structured ProducerResult.
// Must be called after PrintTotal or after all records are in.
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

	percs := percentiles(s.latencies[:s.index], 0.5, 0.95, 0.99, 0.999)

	return &ProducerResult{
		Records:     s.count,
		Bytes:       s.bytes,
		ElapsedMs:   elapsed,
		RecsPerSec:  recsPerSec,
		MBPerSec:    mbPerSec,
		AvgLatency:  avgLatency,
		MaxLatency:  float64(s.maxLatency),
		P50Latency:  percs[0],
		P95Latency:  percs[1],
		P99Latency:  percs[2],
		P999Latency: percs[3],
	}
}

// TotalCount returns the total number of records recorded.
func (s *Stats) TotalCount() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.count
}

func percentiles(latencies []int, pcts ...float64) []int {
	n := len(latencies)
	if n == 0 {
		result := make([]int, len(pcts))
		return result
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

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
