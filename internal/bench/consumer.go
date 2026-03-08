package bench

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type ConsumerConfig struct {
	Brokers           []string
	Topic             string
	NumRecords        int64
	FetchMaxBytes     int32
	NumConsumers      int // number of concurrent consumers in the same group
	Timeout           time.Duration
	ReportingInterval time.Duration
	Out               io.Writer
	JSONOut           io.Writer     // JSON Lines time-series output (nil = disabled)
	KgoOpts           []kgo.Opt     // extra kgo client options (e.g. custom dialer)
	Stats             *Stats        // shared Stats instance; if nil, RunConsumer creates its own
	WarmupRecords     int64         // warmup count passed to Stats (only used when Stats is nil)
	E2EEpoch          time.Time     // when non-zero, decode 9-byte keys for e2e latency tracking
	ProduceDone       *atomic.Bool  // when non-nil, use drain-based termination instead of timeout
	DrainTimeout      time.Duration // how long to wait after ProduceDone becomes true
}

func DefaultConsumerConfig() ConsumerConfig {
	return ConsumerConfig{
		Brokers:           []string{"localhost:9092"},
		Topic:             "bench",
		NumRecords:        1_000_000,
		FetchMaxBytes:     1_048_576, // 1 MiB per partition
		NumConsumers:      1,
		Timeout:           60 * time.Second,
		ReportingInterval: 5 * time.Second,
		Out:               os.Stdout,
	}
}

type ConsumerResult struct {
	Records    int64
	Bytes      int64
	ElapsedMs  int64
	RecsPerSec float64
	MBPerSec   float64
}

// RunConsumer executes the consumer performance test. When NumConsumers > 1,
// it creates multiple consumer clients with static group membership to avoid
// rebalance storms. Each consumer gets a stable member ID so all consumers
// join the group once without triggering cascading rebalances.
//
// Two modes are supported:
//   - Standalone (E2EEpoch is zero): tracks throughput via RecordConsume.
//     Terminates when NumRecords are consumed or Timeout elapses with no progress.
//   - E2E (E2EEpoch is non-zero): decodes 9-byte keys [warmup(1)|nanos(8)]
//     and tracks end-to-end latency via RecordE2E. Terminates when ProduceDone
//     is true and either all expected records are consumed or DrainTimeout elapses.
func RunConsumer(ctx context.Context, cfg ConsumerConfig) (*ConsumerResult, error) {
	if cfg.NumConsumers < 1 {
		cfg.NumConsumers = 1
	}

	e2eMode := !cfg.E2EEpoch.IsZero()

	ownStats := cfg.Stats == nil
	stats := cfg.Stats
	if ownStats {
		stats = NewStats(cfg.NumRecords, cfg.WarmupRecords, cfg.ReportingInterval, cfg.Out)
		if cfg.JSONOut != nil {
			stats.SetJSONOutput(cfg.JSONOut)
		}
		if e2eMode {
			stats.EnableE2E()
		}
	}

	groupID := fmt.Sprintf("perf-consumer-%d", rand.IntN(100_000))

	var (
		fetchErrors    atomic.Int64
		firstFetchErr  syncErr
		lastConsumedAt atomic.Int64 // unix nanos — used for timeout-based termination
	)
	lastConsumedAt.Store(time.Now().UnixNano())

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	clients := make([]*kgo.Client, cfg.NumConsumers)
	for i := 0; i < cfg.NumConsumers; i++ {
		opts := []kgo.Opt{
			kgo.SeedBrokers(cfg.Brokers...),
			kgo.ConsumeTopics(cfg.Topic),
			kgo.ConsumerGroup(groupID),
			kgo.FetchMaxPartitionBytes(cfg.FetchMaxBytes),
			kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
			kgo.InstanceID(fmt.Sprintf("perf-%s-%d", groupID, i)),
		}
		opts = append(opts, cfg.KgoOpts...)
		client, err := kgo.NewClient(opts...)
		if err != nil {
			for j := 0; j < i; j++ {
				clients[j].Close()
			}
			return nil, fmt.Errorf("creating consumer %d: %w", i, err)
		}
		clients[i] = client
	}
	defer func() {
		for _, c := range clients {
			c.Close()
		}
	}()

	consumedCount := func() int64 {
		if e2eMode {
			return stats.CumConsumed()
		}
		return stats.TotalCount()
	}

	for i := 0; i < cfg.NumConsumers; i++ {
		go func(client *kgo.Client) {
			for {
				if ctx.Err() != nil {
					return
				}
				if consumedCount() >= cfg.NumRecords {
					return
				}

				pollCtx, pollCancel := context.WithTimeout(ctx, 200*time.Millisecond)
				fetches := client.PollFetches(pollCtx)
				pollCancel()

				fetches.EachError(func(_ string, _ int32, err error) {
					fetchErrors.Add(1)
					firstFetchErr.Set(err)
				})

				if e2eMode {
					epoch := cfg.E2EEpoch
					fetches.EachRecord(func(r *kgo.Record) {
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
					lastConsumedAt.Store(time.Now().UnixNano())
				} else {
					var fetchRecords int64
					var fetchBytes int64
					fetches.EachRecord(func(r *kgo.Record) {
						fetchRecords++
						if r.Key != nil {
							fetchBytes += int64(len(r.Key))
						}
						if r.Value != nil {
							fetchBytes += int64(len(r.Value))
						}
					})
					if fetchRecords > 0 {
						now := time.Now()
						stats.RecordConsume(fetchRecords, fetchBytes, now)
						lastConsumedAt.Store(now.UnixNano())
					}
				}
			}
		}(clients[i])
	}

	// Termination logic: two modes.
	if cfg.ProduceDone != nil {
		// Drain-based: wait for producer to finish, then drain up to DrainTimeout.
		waitForDrain(ctx, cancel, cfg, consumedCount)
	} else {
		// Timeout-based: exit when NumRecords consumed or idle timeout.
		waitForTimeout(ctx, cancel, cfg, consumedCount, &lastConsumedAt)
	}

	if ownStats {
		if e2eMode {
			stats.PrintTotal()
		} else {
			stats.PrintConsumeTotal()
		}
	}

	result := stats.ConsumerResult()

	if result.Records < cfg.NumRecords && !e2eMode {
		if fe := firstFetchErr.Get(); fe != nil {
			return result, fmt.Errorf("consumed %d/%d records (%d fetch errors, first: %w)",
				result.Records, cfg.NumRecords, fetchErrors.Load(), fe)
		}
		return result, fmt.Errorf("consumed %d/%d records (timeout)", result.Records, cfg.NumRecords)
	}
	return result, nil
}

func waitForDrain(ctx context.Context, cancel context.CancelFunc, cfg ConsumerConfig, consumedCount func() int64) {
	var drainDeadline time.Time
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		if cfg.ProduceDone.Load() {
			if drainDeadline.IsZero() {
				drainDeadline = time.Now().Add(cfg.DrainTimeout)
			}
			if consumedCount() >= cfg.NumRecords {
				cancel()
				return
			}
			if time.Now().After(drainDeadline) {
				cancel()
				return
			}
		}
	}
}

func waitForTimeout(ctx context.Context, cancel context.CancelFunc, cfg ConsumerConfig, consumedCount func() int64, lastConsumedAt *atomic.Int64) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		if consumedCount() >= cfg.NumRecords {
			cancel()
			return
		}

		now := time.Now()
		lastTs := time.Unix(0, lastConsumedAt.Load())
		if now.Sub(lastTs) > cfg.Timeout {
			_, _ = fmt.Fprintf(cfg.Out, "WARNING: Exiting before consuming the expected number of records: "+
				"timeout (%v) exceeded. You can use the --timeout option to increase the timeout.\n", cfg.Timeout)
			cancel()
			return
		}
	}
}
