package bench

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// ProduceConsumeConfig holds configuration for the combined benchmark.
type ProduceConsumeConfig struct {
	Brokers            []string
	Topic              string
	NumRecords         int64
	RecordSize         int
	Throughput         float64
	Acks               int
	BatchMaxBytes      int32
	LingerMs           int
	MaxBufferedRecords int
	NumProducers       int
	NumConsumers       int
	WarmupRecords      int64
	ReportingInterval  time.Duration
	DrainTimeout       time.Duration // how long to wait for consumers to catch up after produce finishes
	Out                io.Writer
	JSONOut            io.Writer
}

// DefaultProduceConsumeConfig returns sensible defaults.
func DefaultProduceConsumeConfig() ProduceConsumeConfig {
	return ProduceConsumeConfig{
		Brokers:            []string{"localhost:9092"},
		Topic:              "bench",
		NumRecords:         1_000_000,
		RecordSize:         1000,
		Throughput:         -1,
		Acks:               1,
		BatchMaxBytes:      1_048_576,
		LingerMs:           0,
		MaxBufferedRecords: 8192,
		NumProducers:       1,
		NumConsumers:       1,
		ReportingInterval:  5 * time.Second,
		DrainTimeout:       30 * time.Second,
		Out:                os.Stdout,
	}
}

// ProduceConsumeResult holds the final metrics.
type ProduceConsumeResult struct {
	ProducerResult
	Consumed int64
}

// RunProduceConsume runs producers and consumers in parallel on the same topic.
// Records carry a 9-byte key: [warmup_flag(1) | send_timestamp_nanos(8)].
// The consumer uses this to compute e2e latency and skip warmup records.
// After producers finish, consumers drain for up to DrainTimeout.
func RunProduceConsume(ctx context.Context, cfg ProduceConsumeConfig) (*ProduceConsumeResult, error) {
	if cfg.NumProducers < 1 {
		cfg.NumProducers = 1
	}
	if cfg.NumConsumers < 1 {
		cfg.NumConsumers = 1
	}

	// Monotonic epoch — all timestamps are offsets from this point.
	epoch := time.Now()

	stats := NewStats(cfg.NumRecords, cfg.WarmupRecords, cfg.ReportingInterval, cfg.Out)
	stats.EnableE2E()
	if cfg.JSONOut != nil {
		stats.SetJSONOutput(cfg.JSONOut)
	}

	var totalErrors atomic.Int64
	var produceDone atomic.Bool
	expectedRecords := cfg.NumRecords // warmup is extra, NumRecords = measured

	// Start consumers first so they're ready when records arrive.
	consumerCtx, consumerCancel := context.WithCancel(ctx)
	defer consumerCancel()

	var consumerWg sync.WaitGroup
	consumerClients := make([]*kgo.Client, cfg.NumConsumers)
	groupID := fmt.Sprintf("bench-pc-%d", time.Now().UnixNano()%100000)

	for i := range cfg.NumConsumers {
		opts := []kgo.Opt{
			kgo.SeedBrokers(cfg.Brokers...),
			kgo.ConsumeTopics(cfg.Topic),
			kgo.ConsumerGroup(groupID),
			kgo.FetchMaxPartitionBytes(cfg.BatchMaxBytes),
			kgo.FetchMaxWait(500 * time.Millisecond),
			kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
			kgo.InstanceID(fmt.Sprintf("bench-pc-%s-%d", groupID, i)),
		}
		client, err := kgo.NewClient(opts...)
		if err != nil {
			for j := range i {
				consumerClients[j].Close()
			}
			return nil, fmt.Errorf("creating consumer %d: %w", i, err)
		}
		consumerClients[i] = client

		consumerWg.Add(1)
		go func(client *kgo.Client) {
			defer consumerWg.Done()
			consumeLoop(consumerCtx, client, epoch, stats, &produceDone, expectedRecords, cfg.DrainTimeout)
		}(client)
	}

	// Start producers. Distribute measured records and warmup evenly.
	// Each producer sends warmup + measured records (warmup first).
	baseMeasured := cfg.NumRecords / int64(cfg.NumProducers)
	measuredRemainder := cfg.NumRecords % int64(cfg.NumProducers)
	baseWarmup := cfg.WarmupRecords / int64(cfg.NumProducers)
	warmupRemainder := cfg.WarmupRecords % int64(cfg.NumProducers)
	perProducerThroughput := cfg.Throughput
	if cfg.Throughput > 0 {
		perProducerThroughput = cfg.Throughput / float64(cfg.NumProducers)
	}

	var producerWg sync.WaitGroup
	producerErrs := make([]error, cfg.NumProducers)

	for p := range cfg.NumProducers {
		measured := baseMeasured
		if int64(p) < measuredRemainder {
			measured++
		}
		warmup := baseWarmup
		if int64(p) < warmupRemainder {
			warmup++
		}
		totalForProducer := warmup + measured
		producerWg.Add(1)
		go func(idx int, total, warmupCount int64) {
			defer producerWg.Done()
			producerErrs[idx] = runPCProducer(ctx, cfg, epoch, stats,
				total, warmupCount, perProducerThroughput, &totalErrors)
		}(p, totalForProducer, warmup)
	}

	// Wait for producers to finish.
	producerWg.Wait()
	produceDone.Store(true)

	// Wait for consumers to drain.
	consumerWg.Wait()

	// Close consumer clients.
	for _, c := range consumerClients {
		c.Close()
	}

	stats.PrintTotal()

	result := stats.Result()
	result.Errors = totalErrors.Load()

	pcResult := &ProduceConsumeResult{
		ProducerResult: *result,
		Consumed:       stats.CumConsumed(),
	}

	// Sanity check.
	produced := result.Records
	consumed := pcResult.Consumed
	if consumed == produced {
		fmt.Fprintf(cfg.Out, "OK: produced == consumed (%d records)\n", produced)
	} else {
		diff := produced - consumed
		fmt.Fprintf(cfg.Out, "WARNING: produced %d but consumed %d (%d missing, %.2f%%)\n",
			produced, consumed, diff, 100.0*float64(diff)/float64(produced))
	}

	if result.Errors > 0 {
		return pcResult, fmt.Errorf("%d records failed to produce", result.Errors)
	}
	for _, err := range producerErrs {
		if err != nil {
			return pcResult, err
		}
	}
	return pcResult, nil
}

func runPCProducer(ctx context.Context, cfg ProduceConsumeConfig, epoch time.Time,
	stats *Stats, numRecords int64, warmupRecords int64, throughput float64, errorCount *atomic.Int64) error {

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
		opts = append(opts, kgo.RequiredAcks(kgo.NoAck()))
		opts = append(opts, kgo.DisableIdempotentWrite())
	case 1:
		opts = append(opts, kgo.RequiredAcks(kgo.LeaderAck()))
		opts = append(opts, kgo.DisableIdempotentWrite())
	default:
		opts = append(opts, kgo.RequiredAcks(kgo.AllISRAcks()))
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return fmt.Errorf("creating producer: %w", err)
	}
	defer client.Close()

	startTime := time.Now()
	throttler := NewThroughputThrottler(throughput, startTime)

	// Reusable key buffer: [warmup(1) | nanos(8)]
	keyBuf := make([]byte, 9)

	for i := int64(0); i < numRecords; i++ {
		if ctx.Err() != nil {
			break
		}

		val := gen.GenerateCopy()

		// Encode key: warmup flag + monotonic timestamp.
		// Warmup is determined by the loop counter, not by async callbacks.
		isWarmup := i < warmupRecords
		if isWarmup {
			keyBuf[0] = 0x00
		} else {
			keyBuf[0] = 0x01
		}
		binary.BigEndian.PutUint64(keyBuf[1:], uint64(time.Since(epoch).Nanoseconds()))

		key := make([]byte, 9)
		copy(key, keyBuf)

		record := &kgo.Record{
			Key:   key,
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

	client.Flush(ctx)
	return nil
}

func consumeLoop(ctx context.Context, client *kgo.Client, epoch time.Time,
	stats *Stats, produceDone *atomic.Bool, expectedRecords int64, drainTimeout time.Duration) {

	var drainDeadline time.Time

	for {
		if ctx.Err() != nil {
			return
		}

		// Once producers are done, start the drain timer.
		if produceDone.Load() {
			if drainDeadline.IsZero() {
				drainDeadline = time.Now().Add(drainTimeout)
			}
			if stats.CumConsumed() >= expectedRecords {
				return
			}
			if time.Now().After(drainDeadline) {
				return
			}
		}

		pollCtx, pollCancel := context.WithTimeout(ctx, 200*time.Millisecond)
		fetches := client.PollFetches(pollCtx)
		pollCancel()

		fetches.EachRecord(func(r *kgo.Record) {
			// Timestamp each record individually rather than sharing a
			// single timestamp across the entire fetch batch.
			now := time.Since(epoch).Nanoseconds()

			if len(r.Key) != 9 {
				return
			}
			if r.Key[0] == 0x00 {
				return // warmup
			}
			sendNanos := int64(binary.BigEndian.Uint64(r.Key[1:9]))
			e2eMs := int((now - sendNanos) / 1_000_000)
			if e2eMs < 0 {
				e2eMs = 0
			}
			stats.RecordE2E(e2eMs)
		})
	}
}
