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
	KgoOpts            []kgo.Opt // extra kgo client options (e.g. custom dialer)
}

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

type ProduceConsumeResult struct {
	ProducerResult
	Consumed int64
}

// RunProduceConsume runs producers and consumers in parallel on the same topic.
// Records carry a 9-byte key: [warmup_flag(1) | send_timestamp_nanos(8)].
// The consumer uses this to compute e2e latency and skip warmup records.
// After producers finish, consumers drain for up to DrainTimeout.
//
// Internally this composes RunProducer and RunConsumer with a shared Stats
// instance and e2e latency tracking enabled.
func RunProduceConsume(ctx context.Context, cfg ProduceConsumeConfig) (*ProduceConsumeResult, error) {
	// Monotonic epoch — all timestamps are offsets from this point.
	epoch := time.Now()

	stats := NewStats(cfg.NumRecords, cfg.WarmupRecords, cfg.ReportingInterval, cfg.Out)
	stats.EnableE2E()
	if cfg.JSONOut != nil {
		stats.SetJSONOutput(cfg.JSONOut)
	}

	var produceDone atomic.Bool

	// Key encoder stamps [warmup(1) | monotonic_nanos(8)] for e2e latency.
	keyEnc := func(_ int64, isWarmup bool, buf []byte) []byte {
		buf = buf[:0]
		if isWarmup {
			buf = append(buf, 0x00)
		} else {
			buf = append(buf, 0x01)
		}
		var nanos [8]byte
		binary.BigEndian.PutUint64(nanos[:], uint64(time.Since(epoch).Nanoseconds()))
		buf = append(buf, nanos[:]...)
		return buf
	}

	pCfg := ProducerConfig{
		Brokers:            cfg.Brokers,
		Topic:              cfg.Topic,
		NumRecords:         cfg.NumRecords,
		RecordSize:         cfg.RecordSize,
		Throughput:         cfg.Throughput,
		Acks:               cfg.Acks,
		BatchMaxBytes:      cfg.BatchMaxBytes,
		LingerMs:           cfg.LingerMs,
		MaxBufferedRecords: cfg.MaxBufferedRecords,
		NumProducers:       cfg.NumProducers,
		WarmupRecords:      cfg.WarmupRecords,
		Out:                cfg.Out,
		KgoOpts:            cfg.KgoOpts,
		KeyEncoder:         keyEnc,
		Stats:              stats,
	}

	cCfg := ConsumerConfig{
		Brokers:       cfg.Brokers,
		Topic:         cfg.Topic,
		NumRecords:    cfg.NumRecords,
		FetchMaxBytes: cfg.BatchMaxBytes,
		NumConsumers:  cfg.NumConsumers,
		Out:           cfg.Out,
		KgoOpts:       cfg.KgoOpts,
		Stats:         stats,
		E2EEpoch:      epoch,
		ProduceDone:   &produceDone,
		DrainTimeout:  cfg.DrainTimeout,
	}

	// Run consumer in background.
	var consumerWg sync.WaitGroup
	var consumerErr error
	consumerWg.Add(1)
	go func() {
		defer consumerWg.Done()
		_, consumerErr = RunConsumer(ctx, cCfg)
	}()

	// Run producer (blocks until all records are produced + flushed).
	producerResult, producerErr := RunProducer(ctx, pCfg)
	produceDone.Store(true)

	consumerWg.Wait()

	stats.PrintTotal()

	consumed := stats.CumConsumed()
	pcResult := &ProduceConsumeResult{
		ProducerResult: *producerResult,
		Consumed:       consumed,
	}

	produced := producerResult.Records
	if consumed == produced {
		_, _ = fmt.Fprintf(cfg.Out, "OK: produced == consumed (%d records)\n", produced)
	} else {
		diff := produced - consumed
		_, _ = fmt.Fprintf(cfg.Out, "WARNING: produced %d but consumed %d (%d missing, %.2f%%)\n",
			produced, consumed, diff, 100.0*float64(diff)/float64(produced))
	}

	if producerErr != nil {
		return pcResult, producerErr
	}
	if consumerErr != nil {
		return pcResult, consumerErr
	}
	return pcResult, nil
}
