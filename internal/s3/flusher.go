package s3

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/klaudworks/klite/internal/chunk"
	"github.com/klaudworks/klite/internal/clock"
	"github.com/klaudworks/klite/internal/wal"
)

// PartitionProvider provides per-partition data to the S3 flusher.
// Implemented by a broker-level adapter to avoid circular dependencies.
type PartitionProvider interface {
	FlushablePartitions() []FlushPartition
}

type FlushPartition struct {
	Topic       string
	Partition   int32
	TopicID     [16]byte
	S3Watermark int64
	HW          int64

	// SealedBytes is the total bytes in sealed chunks (used for size threshold).
	SealedBytes int64

	// OldestChunkTime is the creation time of the oldest sealed/current chunk.
	OldestChunkTime time.Time

	// DetachChunks detaches sealed chunks from the partition under lock.
	// If flushAll is true, also seals and detaches the current chunk.
	// Returns the detached chunks and the chunk pool for release after upload.
	DetachChunks func(flushAll bool) ([]*chunk.Chunk, *chunk.Pool)

	// AdvanceWatermark is called after successful flush to advance the watermark.
	AdvanceWatermark func(newWatermark int64)
}

type FlusherConfig struct {
	Client            *Client
	WALWriter         *wal.Writer
	WALIndex          *wal.Index
	FlushInterval     time.Duration // Max age before flush (default 60s)
	CheckInterval     time.Duration // How often to scan partitions (default 5s)
	TargetObjectSize  int64         // Flush when unflushed bytes >= this (default 64 MiB)
	UploadConcurrency int           // Default 8
	Clock             clock.Clock
	Logger            *slog.Logger

	// TriggerCh receives signals from the chunk pool for emergency flush
	// when pool pressure reaches 75%.
	TriggerCh <-chan struct{}

	// MetadataUploader is called periodically to upload metadata.log.
	MetadataUploader func(ctx context.Context) error

	// Reader is used to invalidate listing caches after new objects are uploaded.
	Reader *Reader
}

// Flusher periodically scans partitions and flushes those that exceed size
// or age thresholds. Reads from the chunk pool (zero disk I/O).
type Flusher struct {
	cfg      FlusherConfig
	client   *Client
	provider PartitionProvider
	logger   *slog.Logger

	stopCh chan struct{}
	done   chan struct{}
}

func NewFlusher(cfg FlusherConfig, provider PartitionProvider) *Flusher {
	if cfg.FlushInterval == 0 {
		cfg.FlushInterval = 60 * time.Second
	}
	if cfg.CheckInterval == 0 {
		cfg.CheckInterval = 1 * time.Second
	}
	if cfg.CheckInterval > cfg.FlushInterval {
		cfg.CheckInterval = cfg.FlushInterval
	}
	if cfg.TargetObjectSize == 0 {
		cfg.TargetObjectSize = 64 * 1024 * 1024 // 64 MiB
	}
	if cfg.UploadConcurrency == 0 {
		cfg.UploadConcurrency = 8
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

func (f *Flusher) Start() {
	f.stopCh = make(chan struct{})
	f.done = make(chan struct{})
	go f.run()
}

func (f *Flusher) Stop() {
	select {
	case <-f.stopCh:
		return // already stopped
	default:
	}
	close(f.stopCh)
	<-f.done
}

func (f *Flusher) FlushAll(ctx context.Context) error {
	return f.scanAndFlush(ctx, true)
}

func (f *Flusher) run() {
	defer close(f.done)

	checkTicker := f.cfg.Clock.NewTicker(f.cfg.CheckInterval)
	defer checkTicker.Stop()

	metaTicker := f.cfg.Clock.NewTicker(f.cfg.FlushInterval)
	defer metaTicker.Stop()

	for {
		select {
		case <-checkTicker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			if err := f.scanAndFlush(ctx, false); err != nil {
				f.logger.Error("S3 flush scan failed", "err", err)
			}
			cancel()

		case <-f.triggerCh():
			f.logger.Warn("emergency flush triggered by chunk pool pressure")
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			if err := f.scanAndFlush(ctx, true); err != nil {
				f.logger.Error("S3 emergency flush failed", "err", err)
			}
			cancel()

		case <-metaTicker.C:
			if f.cfg.MetadataUploader != nil {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				if err := f.cfg.MetadataUploader(ctx); err != nil {
					f.logger.Error("metadata.log upload failed", "err", err)
				}
				cancel()
			}

		case <-f.stopCh:
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			if err := f.scanAndFlush(ctx, true); err != nil {
				f.logger.Error("S3 shutdown flush failed", "err", err)
			}
			if f.cfg.MetadataUploader != nil {
				if err := f.cfg.MetadataUploader(ctx); err != nil {
					f.logger.Error("S3 shutdown metadata upload failed", "err", err)
				}
			}
			cancel()
			return
		}
	}
}

func (f *Flusher) triggerCh() <-chan struct{} {
	return f.cfg.TriggerCh
}

func (f *Flusher) scanAndFlush(ctx context.Context, flushAll bool) error {
	partitions := f.provider.FlushablePartitions()
	if len(partitions) == 0 {
		return nil
	}

	now := f.cfg.Clock.Now()

	// Age-triggered and flushAll include the current chunk;
	// size-triggered only needs sealed chunks.
	type flushCandidate struct {
		fp             FlushPartition
		includeCurrent bool // true = detach current chunk too
	}
	var toFlush []flushCandidate
	for _, fp := range partitions {
		if flushAll {
			toFlush = append(toFlush, flushCandidate{fp, true})
			continue
		}

		if fp.SealedBytes >= f.cfg.TargetObjectSize {
			toFlush = append(toFlush, flushCandidate{fp, false})
			continue
		}

		// Include current chunk on age trigger — in low-throughput scenarios
		// the current chunk may never fill up and seal on its own.
		if !fp.OldestChunkTime.IsZero() {
			age := now.Sub(fp.OldestChunkTime)
			if age >= f.cfg.FlushInterval {
				toFlush = append(toFlush, flushCandidate{fp, true})
				continue
			}
		}
	}

	if len(toFlush) == 0 {
		return nil
	}

	type flushJob struct {
		FlushPartition
		chunks []*chunk.Chunk
		pool   *chunk.Pool
	}
	var jobs []flushJob
	for _, fc := range toFlush {
		chunks, pool := fc.fp.DetachChunks(fc.includeCurrent)
		if len(chunks) == 0 {
			continue
		}
		jobs = append(jobs, flushJob{
			FlushPartition: fc.fp,
			chunks:         chunks,
			pool:           pool,
		})
	}

	if len(jobs) == 0 {
		return nil
	}

	f.logger.Info("S3 flush: flushing partitions", "count", len(jobs), "flush_all", flushAll)
	start := time.Now()

	sem := make(chan struct{}, f.cfg.UploadConcurrency)
	var wg sync.WaitGroup
	var errMu sync.Mutex
	var firstErr error

	for i := range jobs {
		job := jobs[i]

		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			batches := collectBatchesFromChunks(job.chunks)

			if len(batches) == 0 {
				job.pool.ReleaseMany(job.chunks)
				return
			}

			objectData := BuildObject(batches)
			baseOffset := batches[0].BaseOffset
			key := ObjectKey(f.client.prefix, job.Topic, job.TopicID, job.Partition, baseOffset)

			if err := f.uploadWithRetry(ctx, key, objectData); err != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				errMu.Unlock()
				f.logger.Error("S3 partition flush failed",
					"topic", job.Topic, "partition", job.Partition, "err", err)
				// Data is still in WAL for retry on next scan
				job.pool.ReleaseMany(job.chunks)
				return
			}

			if f.cfg.Reader != nil {
				f.cfg.Reader.InvalidateListings(job.Topic, job.TopicID, job.Partition)
			}

			lastBatch := batches[len(batches)-1]
			newWatermark := lastBatch.BaseOffset + int64(lastBatch.LastOffsetDelta) + 1
			if job.AdvanceWatermark != nil {
				job.AdvanceWatermark(newWatermark)
			}

			job.pool.ReleaseMany(job.chunks)

			f.logger.Debug("S3 partition flushed",
				"topic", job.Topic, "partition", job.Partition,
				"base_offset", baseOffset, "watermark", newWatermark,
				"batches", len(batches), "bytes", len(objectData))
		}()
	}

	wg.Wait()

	if f.cfg.WALWriter != nil {
		f.cfg.WALWriter.TryCleanupSegments()
	}

	f.logger.Info("S3 flush complete",
		"partitions", len(jobs),
		"duration", time.Since(start).Round(time.Millisecond))

	if firstErr != nil {
		return fmt.Errorf("partition flush: %w", firstErr)
	}

	return nil
}

func collectBatchesFromChunks(chunks []*chunk.Chunk) []BatchData {
	var batches []BatchData
	for _, c := range chunks {
		for _, b := range c.Batches {
			raw := make([]byte, b.Size)
			copy(raw, c.Data[b.Offset:b.Offset+b.Size])

			if len(raw) < RecordBatchHeaderSize {
				continue
			}

			batches = append(batches, BatchData{
				RawBytes:        raw,
				BaseOffset:      b.BaseOffset,
				LastOffsetDelta: b.LastOffsetDelta,
			})
		}
	}
	return batches
}

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
// Used during WAL replay/recovery only (no rate limiting).
func CollectWALBatches(walWriter *wal.Writer, walIndex *wal.Index, topicID [16]byte, partition int32, s3Watermark int64) []BatchData {
	tp := wal.TopicPartition{TopicID: topicID, Partition: partition}
	entries := walIndex.PartitionEntries(tp)

	var batches []BatchData
	for _, e := range entries {
		if e.LastOffset < s3Watermark {
			continue
		}

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
