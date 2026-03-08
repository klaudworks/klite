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

// syncErr captures the first error from concurrent callbacks.
type syncErr struct {
	once sync.Once
	err  error
}

func (s *syncErr) Set(err error) {
	s.once.Do(func() { s.err = err })
}

func (s *syncErr) Get() error {
	return s.err
}

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
	JSONOut            io.Writer  // JSON Lines time-series output (nil = disabled)
	KgoOpts            []kgo.Opt  // extra kgo client options (e.g. custom dialer)
	KeyEncoder         KeyEncoder // optional key encoder (e.g. for e2e latency timestamps)
	Stats              *Stats     // shared Stats instance; if nil, RunProducer creates its own
}

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

	ownStats := cfg.Stats == nil
	stats := cfg.Stats
	if ownStats {
		stats = NewStats(cfg.NumRecords, cfg.WarmupRecords, cfg.ReportingInterval, cfg.Out)
		if cfg.JSONOut != nil {
			stats.SetJSONOutput(cfg.JSONOut)
		}
	}

	var totalErrors atomic.Int64
	var firstErr syncErr

	params := producerCoreParams{
		brokers:            cfg.Brokers,
		topic:              cfg.Topic,
		recordSize:         cfg.RecordSize,
		acks:               cfg.Acks,
		batchMaxBytes:      cfg.BatchMaxBytes,
		lingerMs:           cfg.LingerMs,
		maxBufferedRecords: cfg.MaxBufferedRecords,
		kgoOpts:            cfg.KgoOpts,
	}

	perProducerThroughput := cfg.Throughput
	if cfg.Throughput > 0 {
		perProducerThroughput = cfg.Throughput / float64(cfg.NumProducers)
	}

	baseMeasured := cfg.NumRecords / int64(cfg.NumProducers)
	measuredRemainder := cfg.NumRecords % int64(cfg.NumProducers)
	baseWarmup := cfg.WarmupRecords / int64(cfg.NumProducers)
	warmupRemainder := cfg.WarmupRecords % int64(cfg.NumProducers)

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
			errs[producerIdx] = runProducerCore(ctx, params, total, warmupCount,
				perProducerThroughput, cfg.KeyEncoder, stats, &totalErrors, &firstErr)
		}(p, totalForProducer, warmup)
	}

	wg.Wait()

	if ownStats {
		stats.PrintTotal()
	}

	result := stats.Result()
	result.Errors = totalErrors.Load()

	if result.Errors > 0 {
		if fe := firstErr.Get(); fe != nil {
			_, _ = fmt.Fprintf(cfg.Out, "%d records failed to produce, first error: %v\n", result.Errors, fe)
			return result, fmt.Errorf("%d records failed, first error: %w", result.Errors, fe)
		}
		_, _ = fmt.Fprintf(cfg.Out, "%d records failed to produce.\n", result.Errors)
		return result, fmt.Errorf("%d records failed", result.Errors)
	}

	for _, err := range errs {
		if err != nil {
			return result, err
		}
	}
	return result, nil
}

// producerCoreParams holds the kgo-client-level settings for the core
// produce loop, separate from higher-level orchestration concerns.
type producerCoreParams struct {
	brokers            []string
	topic              string
	recordSize         int
	acks               int
	batchMaxBytes      int32
	lingerMs           int
	maxBufferedRecords int
	kgoOpts            []kgo.Opt
}

// KeyEncoder is called for every record to produce a key. If nil, records
// have no key (plain produce mode). The buffer is reusable; implementations
// must copy if they need the key to survive past the call.
type KeyEncoder func(seq int64, isWarmup bool, buf []byte) []byte

// runProducerCore is the core produce loop. When KeyEncoder is nil, records
// have no key (plain produce). When set, each record gets a key via the encoder
// (e.g. timestamp-stamping for e2e latency tracking).
func runProducerCore(ctx context.Context, p producerCoreParams,
	numRecords, warmupRecords int64, throughput float64,
	keyEnc KeyEncoder, stats *Stats, errorCount *atomic.Int64, firstErr *syncErr,
) error {
	gen := NewPayloadGenerator(p.recordSize)

	opts := []kgo.Opt{
		kgo.SeedBrokers(p.brokers...),
		kgo.DefaultProduceTopic(p.topic),
		kgo.ProducerBatchMaxBytes(p.batchMaxBytes),
		kgo.ProducerLinger(time.Duration(p.lingerMs) * time.Millisecond),
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)),
		kgo.MaxBufferedRecords(p.maxBufferedRecords),
		kgo.RetryBackoffFn(func(int) time.Duration { return 100 * time.Millisecond }),
		kgo.RequestRetries(10),
	}
	opts = append(opts, p.kgoOpts...)

	switch p.acks {
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

	var keyBuf []byte
	if keyEnc != nil {
		keyBuf = make([]byte, 0, 16)
	}

	for i := int64(0); i < numRecords; i++ {
		if ctx.Err() != nil {
			break
		}

		val := gen.GenerateCopy()
		isWarmup := i < warmupRecords

		record := &kgo.Record{Value: val}

		if keyEnc != nil {
			keyBuf = keyEnc(i, isWarmup, keyBuf[:0])
			key := make([]byte, len(keyBuf))
			copy(key, keyBuf)
			record.Key = key
		}

		sendStart := time.Now()
		client.Produce(ctx, record, func(_ *kgo.Record, err error) {
			now := time.Now()
			latencyMs := int(now.Sub(sendStart).Milliseconds())
			if err != nil {
				errorCount.Add(1)
				firstErr.Set(err)
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

	return client.Flush(ctx)
}
