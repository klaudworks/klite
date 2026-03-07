package bench

import (
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// ConsumerConfig holds consumer benchmark configuration.
type ConsumerConfig struct {
	Brokers           []string
	Topic             string
	NumRecords        int64
	FetchMaxBytes     int32
	NumConsumers      int // number of concurrent consumers in the same group
	Timeout           time.Duration
	ReportingInterval time.Duration
	ShowDetailedStats bool
	Out               io.Writer
}

// DefaultConsumerConfig returns sensible defaults.
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

// ConsumerResult holds the final metrics from a consumer run.
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
func RunConsumer(ctx context.Context, cfg ConsumerConfig) (*ConsumerResult, error) {
	if cfg.NumConsumers < 1 {
		cfg.NumConsumers = 1
	}

	groupID := fmt.Sprintf("perf-consumer-%d", rand.IntN(100_000))

	var (
		recordsRead    atomic.Int64
		bytesRead      atomic.Int64
		lastConsumedAt atomic.Int64 // unix nanos
	)
	lastConsumedAt.Store(time.Now().UnixNano())
	startTime := time.Now()

	if !cfg.ShowDetailedStats {
		_, _ = fmt.Fprintf(cfg.Out, "start.time, end.time, data.consumed.in.MB, MB.sec, data.consumed.in.nMsg, nMsg.sec, rebalance.time.ms, fetch.time.ms, fetch.MB.sec, fetch.nMsg.sec\n")
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Create all consumers. Each gets a static InstanceID to avoid rebalance churn.
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
		client, err := kgo.NewClient(opts...)
		if err != nil {
			// Close any already-created clients.
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

	// Start polling goroutines.
	doneCh := make(chan struct{})
	for i := 0; i < cfg.NumConsumers; i++ {
		go func(client *kgo.Client) {
			for {
				if ctx.Err() != nil {
					return
				}
				if recordsRead.Load() >= cfg.NumRecords {
					return
				}

				pollCtx, pollCancel := context.WithTimeout(ctx, 200*time.Millisecond)
				fetches := client.PollFetches(pollCtx)
				pollCancel()

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
					recordsRead.Add(fetchRecords)
					bytesRead.Add(fetchBytes)
					lastConsumedAt.Store(time.Now().UnixNano())
				}
			}
		}(clients[i])
	}

	// Monitor loop.
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	var (
		lastReportTime  = startTime
		lastRecordsRead int64
		lastBytesRead   int64
	)

	for {
		select {
		case <-ctx.Done():
			goto done
		case <-ticker.C:
		}

		now := time.Now()

		if recordsRead.Load() >= cfg.NumRecords {
			cancel()
			goto done
		}

		lastTs := time.Unix(0, lastConsumedAt.Load())
		if now.Sub(lastTs) > cfg.Timeout {
			_, _ = fmt.Fprintf(cfg.Out, "WARNING: Exiting before consuming the expected number of records: "+
				"timeout (%v) exceeded. You can use the --timeout option to increase the timeout.\n", cfg.Timeout)
			cancel()
			goto done
		}

		if cfg.ShowDetailedStats && now.Sub(lastReportTime) >= cfg.ReportingInterval {
			curRecords := recordsRead.Load()
			curBytes := bytesRead.Load()
			printConsumerWindow(cfg.Out, curBytes, lastBytesRead, curRecords, lastRecordsRead,
				lastReportTime, now)
			lastReportTime = now
			lastRecordsRead = curRecords
			lastBytesRead = curBytes
		}
	}

done:
	close(doneCh)

	endTime := time.Now()
	elapsedMs := endTime.Sub(startTime).Milliseconds()
	if elapsedMs == 0 {
		elapsedMs = 1
	}
	elapsedSec := float64(elapsedMs) / 1000.0
	totalRecs := recordsRead.Load()
	totalBytes := bytesRead.Load()
	totalMB := float64(totalBytes) / (1024.0 * 1024.0)

	if !cfg.ShowDetailedStats {
		_, _ = fmt.Fprintf(cfg.Out, "%s, %s, %.4f, %.4f, %d, %.4f, %d, %d, %.4f, %.4f\n",
			startTime.Format("2006-01-02 15:04:05.000"),
			endTime.Format("2006-01-02 15:04:05.000"),
			totalMB,
			totalMB/elapsedSec,
			totalRecs,
			float64(totalRecs)/elapsedSec,
			0,
			elapsedMs,
			totalMB/elapsedSec,
			float64(totalRecs)/elapsedSec,
		)
	}

	return &ConsumerResult{
		Records:    totalRecs,
		Bytes:      totalBytes,
		ElapsedMs:  elapsedMs,
		RecsPerSec: float64(totalRecs) / elapsedSec,
		MBPerSec:   totalMB / elapsedSec,
	}, nil
}

func printConsumerWindow(out io.Writer, bytesRead, lastBytesRead, recordsRead, lastRecordsRead int64,
	startTime, endTime time.Time,
) {
	elapsedMs := endTime.Sub(startTime).Milliseconds()
	if elapsedMs == 0 {
		elapsedMs = 1
	}
	intervalMB := float64(bytesRead-lastBytesRead) / (1024.0 * 1024.0)
	mbPerSec := 1000.0 * intervalMB / float64(elapsedMs)
	recsPerSec := 1000.0 * float64(recordsRead-lastRecordsRead) / float64(elapsedMs)
	totalMB := float64(bytesRead) / (1024.0 * 1024.0)

	_, _ = fmt.Fprintf(out, "%s, %.4f, %.4f, %d, %.4f\n",
		endTime.Format("2006-01-02 15:04:05.000"),
		totalMB, mbPerSec, recordsRead, recsPerSec)
}
