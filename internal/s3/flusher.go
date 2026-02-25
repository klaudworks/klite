package s3

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/klaudworks/klite/internal/clock"
	"github.com/klaudworks/klite/internal/wal"
)

// PartitionProvider provides per-partition data to the S3 flusher.
// Implemented by a broker-level adapter to avoid circular dependencies.
type PartitionProvider interface {
	// FlushablePartitions returns all partitions with unflushed WAL data.
	FlushablePartitions() []FlushPartition
}

// FlushPartition represents a partition with unflushed data ready for S3.
type FlushPartition struct {
	Topic       string
	Partition   int32
	TopicID     [16]byte
	S3Watermark int64
	HW          int64

	// Batches to flush (collected by the provider)
	Batches []BatchData

	// AdvanceWatermark is called after successful flush to advance the watermark.
	AdvanceWatermark func(newWatermark int64)
}

// FlusherConfig holds configuration for the S3 flusher.
type FlusherConfig struct {
	Client            *Client
	WALWriter         *wal.Writer
	WALIndex          *wal.Index
	FlushInterval     time.Duration // Default 10m
	UploadConcurrency int           // Default 4
	Clock             clock.Clock
	Logger            *slog.Logger

	// MetadataUploader is called after all partitions are flushed to upload metadata.log.
	MetadataUploader func(ctx context.Context) error
}

// Flusher manages the S3 flush pipeline. It periodically flushes all partitions
// with unflushed data to S3, then uploads metadata.log.
type Flusher struct {
	cfg      FlusherConfig
	client   *Client
	provider PartitionProvider
	logger   *slog.Logger

	stopCh chan struct{}
	done   chan struct{}
}

// NewFlusher creates a new S3 flusher.
func NewFlusher(cfg FlusherConfig, provider PartitionProvider) *Flusher {
	if cfg.FlushInterval == 0 {
		cfg.FlushInterval = 10 * time.Minute
	}
	if cfg.UploadConcurrency == 0 {
		cfg.UploadConcurrency = 4
	}
	if cfg.Clock == nil {
		cfg.Clock = clock.RealClock{}
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	return &Flusher{
		cfg:      cfg,
		client:   cfg.Client,
		provider: provider,
		logger:   cfg.Logger,
		stopCh:   make(chan struct{}),
		done:     make(chan struct{}),
	}
}

// Start begins the periodic flush goroutine.
func (f *Flusher) Start() {
	go f.run()
}

// Stop triggers a final flush and stops the flusher.
func (f *Flusher) Stop() {
	close(f.stopCh)
	<-f.done
}

// FlushAll performs a unified S3 sync cycle: flush all partitions + upload metadata.
// Can be called externally (e.g., for graceful shutdown).
func (f *Flusher) FlushAll(ctx context.Context) error {
	return f.unifiedSync(ctx)
}

func (f *Flusher) run() {
	defer close(f.done)

	ticker := f.cfg.Clock.NewTicker(f.cfg.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			if err := f.unifiedSync(ctx); err != nil {
				f.logger.Error("S3 unified sync failed", "err", err)
			}
			cancel()
		case <-f.stopCh:
			// Final flush on stop
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			if err := f.unifiedSync(ctx); err != nil {
				f.logger.Error("S3 shutdown flush failed", "err", err)
			}
			cancel()
			return
		}
	}
}

// unifiedSync performs the unified S3 sync cycle:
//  1. Flush all partitions with unflushed data
//  2. Upload metadata.log
func (f *Flusher) unifiedSync(ctx context.Context) error {
	partitions := f.provider.FlushablePartitions()

	// Filter to only those with batches
	var toFlush []FlushPartition
	for _, fp := range partitions {
		if len(fp.Batches) > 0 {
			toFlush = append(toFlush, fp)
		}
	}

	if len(toFlush) == 0 {
		// Still upload metadata.log even if no partition data to flush
		if f.cfg.MetadataUploader != nil {
			return f.cfg.MetadataUploader(ctx)
		}
		return nil
	}

	f.logger.Info("S3 unified sync starting", "partitions", len(toFlush))
	start := time.Now()

	// Flush partitions with bounded concurrency
	sem := make(chan struct{}, f.cfg.UploadConcurrency)
	var wg sync.WaitGroup
	var errMu sync.Mutex
	var firstErr error

	for _, fp := range toFlush {
		fp := fp // capture loop var

		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			if err := f.flushPartition(ctx, fp); err != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				errMu.Unlock()
				f.logger.Error("S3 partition flush failed",
					"topic", fp.Topic, "partition", fp.Partition, "err", err)
			}
		}()
	}

	wg.Wait()

	if firstErr != nil {
		return fmt.Errorf("partition flush: %w", firstErr)
	}

	// Upload metadata.log
	if f.cfg.MetadataUploader != nil {
		if err := f.cfg.MetadataUploader(ctx); err != nil {
			return fmt.Errorf("metadata upload: %w", err)
		}
	}

	f.logger.Info("S3 unified sync complete",
		"partitions", len(toFlush),
		"duration", time.Since(start).Round(time.Millisecond))

	return nil
}

// flushPartition builds and uploads an S3 object for a single partition.
func (f *Flusher) flushPartition(ctx context.Context, fp FlushPartition) error {
	if len(fp.Batches) == 0 {
		return nil
	}

	// Build the S3 object (data + footer)
	objectData := BuildObject(fp.Batches)
	baseOffset := fp.Batches[0].BaseOffset
	key := ObjectKey(f.client.prefix, fp.Topic, fp.Partition, baseOffset)

	// Upload with retry
	if err := f.uploadWithRetry(ctx, key, objectData); err != nil {
		return err
	}

	// Advance the S3 flush watermark
	lastBatch := fp.Batches[len(fp.Batches)-1]
	newWatermark := lastBatch.BaseOffset + int64(lastBatch.LastOffsetDelta) + 1
	if fp.AdvanceWatermark != nil {
		fp.AdvanceWatermark(newWatermark)
	}

	f.logger.Debug("S3 partition flushed",
		"topic", fp.Topic, "partition", fp.Partition,
		"base_offset", baseOffset, "watermark", newWatermark,
		"batches", len(fp.Batches), "bytes", len(objectData))

	return nil
}

// uploadWithRetry uploads data to S3 with exponential backoff retry.
func (f *Flusher) uploadWithRetry(ctx context.Context, key string, data []byte) error {
	backoff := time.Second
	maxBackoff := 60 * time.Second
	maxRetries := 10

	for attempt := 0; attempt <= maxRetries; attempt++ {
		err := f.client.PutObject(ctx, key, data)
		if err == nil {
			return nil
		}

		if attempt == maxRetries {
			return fmt.Errorf("s3 upload failed after %d retries: %w", maxRetries, err)
		}

		f.logger.Warn("S3 upload failed, retrying",
			"key", key, "attempt", attempt+1, "backoff", backoff, "err", err)

		select {
		case <-f.cfg.Clock.After(backoff):
		case <-ctx.Done():
			return ctx.Err()
		case <-f.stopCh:
			return fmt.Errorf("flusher stopped during retry")
		}

		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}

	return nil // unreachable
}

// CollectWALBatches collects unflushed batches from the WAL index for a partition.
// Returns BatchData slices suitable for building an S3 object.
func CollectWALBatches(walWriter *wal.Writer, walIndex *wal.Index, topicID [16]byte, partition int32, s3Watermark int64) []BatchData {
	tp := wal.TopicPartition{TopicID: topicID, Partition: partition}
	entries := walIndex.PartitionEntries(tp)

	var batches []BatchData
	for _, e := range entries {
		// Skip entries already flushed to S3
		if e.LastOffset < s3Watermark {
			continue
		}

		// Read batch from WAL
		data, err := walWriter.ReadBatch(e)
		if err != nil {
			slog.Warn("S3 flush: skipping unreadable WAL batch",
				"topic_id", topicID, "partition", partition,
				"base_offset", e.BaseOffset, "err", err)
			continue
		}

		if len(data) < RecordBatchHeaderSize {
			slog.Warn("S3 flush: skipping short WAL batch",
				"topic_id", topicID, "partition", partition,
				"base_offset", e.BaseOffset, "len", len(data))
			continue
		}
		lastOffsetDelta := int32(binary.BigEndian.Uint32(data[23:27]))

		batches = append(batches, BatchData{
			RawBytes:        data,
			BaseOffset:      e.BaseOffset,
			LastOffsetDelta: lastOffsetDelta,
		})
	}

	return batches
}
