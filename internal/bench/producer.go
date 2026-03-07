package bench

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// ProducerConfig holds producer benchmark configuration.
type ProducerConfig struct {
	Brokers            []string
	Topic              string
	NumRecords         int64
	RecordSize         int
	Throughput         float64 // records/sec, <0 means unlimited
	Acks               int     // -1 (all), 0, 1
	BatchMaxBytes      int32
	LingerMs           int
	MaxBufferedRecords int
	NumProducers       int // number of concurrent producer clients
	WarmupRecords      int64
	ReportingInterval  time.Duration
	Out                io.Writer
	JSONOut            io.Writer // JSON Lines time-series output (nil = disabled)
}

// DefaultProducerConfig returns sensible defaults.
func DefaultProducerConfig() ProducerConfig {
	return ProducerConfig{
		Brokers:            []string{"localhost:9092"},
		Topic:              "bench",
		NumRecords:         1_000_000,
		RecordSize:         1000,
		Throughput:         -1,
		Acks:               1,
		BatchMaxBytes:      1_048_576, // 1 MiB
		LingerMs:           0,
		MaxBufferedRecords: 8192,
		NumProducers:       1,
		ReportingInterval:  5 * time.Second,
		Out:                os.Stdout,
	}
}

// ProducerResult holds the final metrics from a producer run.
type ProducerResult struct {
	Records     int64
	Bytes       int64
	Errors      int64
	ElapsedMs   int64
	RecsPerSec  float64
	MBPerSec    float64
	AvgLatency  float64
	MaxLatency  float64
	P50Latency  int
	P95Latency  int
	P99Latency  int
	P999Latency int
}

// RunProducer executes the producer performance test. It spawns NumProducers
// concurrent producer clients, each with its own TCP connection, dividing
// NumRecords evenly across them. All producers share a single Stats instance.
// If Throughput > 0, each producer is throttled to Throughput/NumProducers.
func RunProducer(ctx context.Context, cfg ProducerConfig) (*ProducerResult, error) {
	if cfg.NumProducers < 1 {
		cfg.NumProducers = 1
	}

	stats := NewStats(cfg.NumRecords, cfg.WarmupRecords, cfg.ReportingInterval, cfg.Out)
	if cfg.JSONOut != nil {
		stats.SetJSONOutput(cfg.JSONOut)
	}
	var totalErrors atomic.Int64

	// Divide measured records, warmup, and throughput across producers.
	// Each producer sends warmup + measured records (warmup first).
	baseMeasured := cfg.NumRecords / int64(cfg.NumProducers)
	measuredRemainder := cfg.NumRecords % int64(cfg.NumProducers)
	baseWarmup := cfg.WarmupRecords / int64(cfg.NumProducers)
	warmupRemainder := cfg.WarmupRecords % int64(cfg.NumProducers)
	perProducerThroughput := cfg.Throughput
	if cfg.Throughput > 0 {
		perProducerThroughput = cfg.Throughput / float64(cfg.NumProducers)
	}

	var wg sync.WaitGroup
	errs := make([]error, cfg.NumProducers)

	for p := 0; p < cfg.NumProducers; p++ {
		measured := baseMeasured
		if int64(p) < measuredRemainder {
			measured++
		}
		warmup := baseWarmup
		if int64(p) < warmupRemainder {
			warmup++
		}
		totalForProducer := warmup + measured

		wg.Add(1)
		go func(producerIdx int, total, warmupCount int64) {
			defer wg.Done()
			errs[producerIdx] = runSingleProducer(ctx, cfg, total, warmupCount,
				perProducerThroughput, stats, &totalErrors)
		}(p, totalForProducer, warmup)
	}

	wg.Wait()
	stats.PrintTotal()

	result := stats.Result()
	result.Errors = totalErrors.Load()

	if result.Errors > 0 {
		_, _ = fmt.Fprintf(cfg.Out, "%d records failed to produce.\n", result.Errors)
		return result, fmt.Errorf("%d records failed", result.Errors)
	}

	// Check for client-level errors (connection failures etc).
	for _, err := range errs {
		if err != nil {
			return result, err
		}
	}
	return result, nil
}

func runSingleProducer(ctx context.Context, cfg ProducerConfig,
	numRecords, warmupRecords int64, throughput float64, stats *Stats, errorCount *atomic.Int64,
) error {
	gen := NewPayloadGenerator(cfg.RecordSize)

	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.DefaultProduceTopic(cfg.Topic),
		kgo.ProducerBatchMaxBytes(cfg.BatchMaxBytes),
		kgo.ProducerLinger(time.Duration(cfg.LingerMs) * time.Millisecond),
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)),
		kgo.MaxBufferedRecords(cfg.MaxBufferedRecords),
		kgo.RetryBackoffFn(func(int) time.Duration { return 100 * time.Millisecond }),
		kgo.RequestRetries(10),
	}

	switch cfg.Acks {
	case 0:
		opts = append(opts, kgo.RequiredAcks(kgo.NoAck()), kgo.DisableIdempotentWrite())
	case 1:
		opts = append(opts, kgo.RequiredAcks(kgo.LeaderAck()), kgo.DisableIdempotentWrite())
	default: // -1 / all
		opts = append(opts, kgo.RequiredAcks(kgo.AllISRAcks()))
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return fmt.Errorf("creating producer: %w", err)
	}
	defer client.Close()

	startTime := time.Now()
	throttler := NewThroughputThrottler(throughput, startTime)

	for i := int64(0); i < numRecords; i++ {
		if ctx.Err() != nil {
			break
		}

		val := gen.GenerateCopy()
		isWarmup := i < warmupRecords

		record := &kgo.Record{
			Value: val,
		}

		sendStart := time.Now()
		client.Produce(ctx, record, func(_ *kgo.Record, err error) {
			now := time.Now()
			latencyMs := int(now.Sub(sendStart).Milliseconds())
			if err != nil {
				errorCount.Add(1)
				return
			}
			if !isWarmup {
				stats.Record(latencyMs, len(val), now)
			}
		})

		if throttler.ShouldThrottle(i+1, sendStart) {
			throttler.Throttle()
		}
	}

	_ = client.Flush(ctx)
	return nil
}
