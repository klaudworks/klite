package s3

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/klaudworks/klite/internal/clock"
	"golang.org/x/time/rate"
)

type CompactorConfig struct {
	Client *Client
	Reader *Reader
	Logger *slog.Logger
	Clock  clock.Clock

	// WindowBytes is the max total source object size per compaction window (default 256 MiB).
	WindowBytes int64

	// S3Concurrency is the max concurrent S3 GETs for compaction (default 4).
	S3Concurrency int

	// MinDirtyObjects is the minimum number of dirty S3 objects before compaction triggers (default 4).
	MinDirtyObjects int

	// PersistWatermark is called after successful compaction to persist
	// the cleanedUpTo watermark to metadata.log.
	PersistWatermark func(topic string, partition int32, cleanedUpTo int64) error

	// DeleteRetentionMs is the topic-level delete.retention.ms (default 24h).
	// Tombstones older than this are removed during compaction.
	DeleteRetentionMs int64

	// ReadRateLimit is the maximum S3 read throughput in bytes/sec for compaction.
	// 0 means unlimited. Default 50 MiB/s.
	ReadRateLimit int
}

type Compactor struct {
	cfg         CompactorConfig
	client      *Client
	reader      *Reader
	logger      *slog.Logger
	clock       clock.Clock
	rateLimiter *rate.Limiter // nil if unlimited
}

func NewCompactor(cfg CompactorConfig) *Compactor {
	if cfg.WindowBytes == 0 {
		cfg.WindowBytes = 256 * 1024 * 1024 // 256 MiB
	}
	if cfg.S3Concurrency == 0 {
		cfg.S3Concurrency = 4
	}
	if cfg.MinDirtyObjects == 0 {
		cfg.MinDirtyObjects = 4
	}
	if cfg.Clock == nil {
		cfg.Clock = clock.RealClock{}
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.DeleteRetentionMs == 0 {
		cfg.DeleteRetentionMs = 86400000 // 24 hours
	}
	var rl *rate.Limiter
	if cfg.ReadRateLimit > 0 {
		rl = rate.NewLimiter(rate.Limit(cfg.ReadRateLimit), cfg.ReadRateLimit)
	}

	return &Compactor{
		cfg:         cfg,
		client:      cfg.Client,
		reader:      cfg.Reader,
		logger:      cfg.Logger,
		clock:       cfg.Clock,
		rateLimiter: rl,
	}
}

func (c *Compactor) SetDeleteRetentionMs(ms int64) {
	c.cfg.DeleteRetentionMs = ms
}

// CompactPartition compacts all dirty objects for a single partition.
// Returns the new cleanedUpTo watermark.
func (c *Compactor) CompactPartition(
	ctx context.Context,
	topic string,
	topicID [16]byte,
	partition int32,
	cleanedUpTo int64,
	minCompactionLagMs int64,
	acquireLock func(),
	releaseLock func(),
) (int64, error) {
	prefix := ObjectKeyPrefix(c.client.prefix, topic, topicID, partition)

	objects, err := c.client.ListObjects(ctx, prefix)
	if err != nil {
		return cleanedUpTo, fmt.Errorf("list objects: %w", err)
	}

	if len(objects) == 0 {
		return cleanedUpTo, nil
	}

	var parsed []windowObj
	for _, obj := range objects {
		baseOff := parseBaseOffset(obj.Key, prefix)
		if baseOff < 0 {
			continue
		}
		parsed = append(parsed, windowObj{
			key:          obj.Key,
			size:         obj.Size,
			baseOffset:   baseOff,
			lastModified: obj.LastModified,
		})
	}
	sort.Slice(parsed, func(i, j int) bool {
		return parsed[i].baseOffset < parsed[j].baseOffset
	})

	// Orphan cleanup: delete objects whose offset range is fully covered by a later object
	if err := c.orphanCleanup(ctx, parsed); err != nil {
		c.logger.Warn("orphan cleanup failed", "topic", topic, "partition", partition, "err", err)
	}

	objects, err = c.client.ListObjects(ctx, prefix)
	if err != nil {
		return cleanedUpTo, fmt.Errorf("re-list objects: %w", err)
	}

	parsed = parsed[:0]
	for _, obj := range objects {
		baseOff := parseBaseOffset(obj.Key, prefix)
		if baseOff < 0 {
			continue
		}
		parsed = append(parsed, windowObj{
			key:          obj.Key,
			size:         obj.Size,
			baseOffset:   baseOff,
			lastModified: obj.LastModified,
		})
	}
	sort.Slice(parsed, func(i, j int) bool {
		return parsed[i].baseOffset < parsed[j].baseOffset
	})

	if len(parsed) == 0 {
		return cleanedUpTo, nil
	}

	anchorIdx := -1
	for i, p := range parsed {
		if p.baseOffset <= cleanedUpTo {
			anchorIdx = i
		}
	}

	windows := c.formWindows(parsed, anchorIdx, minCompactionLagMs)
	if len(windows) == 0 {
		return cleanedUpTo, nil
	}

	for _, window := range windows {
		if ctx.Err() != nil {
			return cleanedUpTo, ctx.Err()
		}

		// Skip single-object windows (no key can be superseded within a window of one)
		if len(window) == 1 {
			// Unless it's an already-compacted anchor being re-examined
			// with no dirty objects after it
			continue
		}

		acquireLock()
		newWatermark, err := c.compactWindow(ctx, topic, topicID, partition, window, cleanedUpTo)
		releaseLock()

		if err != nil {
			return cleanedUpTo, fmt.Errorf("compact window: %w", err)
		}
		if newWatermark > cleanedUpTo {
			cleanedUpTo = newWatermark
		}
	}

	return cleanedUpTo, nil
}

func (c *Compactor) compactWindow(
	ctx context.Context,
	topic string,
	topicID [16]byte,
	partition int32,
	window []windowObj,
	currentCleanedUpTo int64,
) (int64, error) {
	type cachedObj struct {
		key      string
		rawBytes []byte
		footer   *Footer
	}
	cached := make([]cachedObj, len(window))

	sem := make(chan struct{}, c.cfg.S3Concurrency)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var fetchErr error

	for i, wo := range window {
		wg.Add(1)
		sem <- struct{}{}
		go func(idx int, w windowObj) {
			defer wg.Done()
			defer func() { <-sem }()

			// Rate-limit S3 reads to avoid starving consumer fetches.
			if c.rateLimiter != nil {
				if err := waitRateLimiter(ctx, c.rateLimiter, int(w.size)); err != nil {
					mu.Lock()
					if fetchErr == nil {
						fetchErr = fmt.Errorf("rate limit wait: %w", err)
					}
					mu.Unlock()
					return
				}
			}

			data, err := c.client.GetObject(ctx, w.key)
			if err != nil {
				mu.Lock()
				if fetchErr == nil {
					fetchErr = fmt.Errorf("GET %s: %w", w.key, err)
				}
				mu.Unlock()
				return
			}

			footer, err := ParseFooter(data, int64(len(data)))
			if err != nil {
				mu.Lock()
				if fetchErr == nil {
					fetchErr = fmt.Errorf("parse footer %s: %w", w.key, err)
				}
				mu.Unlock()
				return
			}

			mu.Lock()
			cached[idx] = cachedObj{key: w.key, rawBytes: data, footer: footer}
			mu.Unlock()
		}(i, wo)
	}
	wg.Wait()

	if fetchErr != nil {
		return currentCleanedUpTo, fetchErr
	}

	offsetMap := make(map[string]int64)

	for _, co := range cached {
		if co.footer == nil || len(co.rawBytes) == 0 {
			continue
		}
		if err := c.buildOffsetMap(co.rawBytes, co.footer, offsetMap); err != nil {
			return currentCleanedUpTo, fmt.Errorf("build offset map: %w", err)
		}
	}

	var outputBatches []BatchData
	nowMs := c.clock.Now().UnixMilli()

	for _, co := range cached {
		if co.footer == nil || len(co.rawBytes) == 0 {
			continue
		}
		batches, err := c.filterBatches(co.rawBytes, co.footer, offsetMap, nowMs)
		if err != nil {
			return currentCleanedUpTo, fmt.Errorf("filter batches: %w", err)
		}
		outputBatches = append(outputBatches, batches...)
	}

	if len(outputBatches) == 0 {
		var lastOffset int64
		for _, co := range cached {
			if co.footer != nil {
				lo := co.footer.LastOffset()
				if lo > lastOffset {
					lastOffset = lo
				}
			}
		}

		if c.cfg.PersistWatermark != nil {
			if err := c.cfg.PersistWatermark(topic, partition, lastOffset); err != nil {
				return currentCleanedUpTo, fmt.Errorf("persist watermark: %w", err)
			}
		}

		var keys []string
		for _, co := range cached {
			keys = append(keys, co.key)
		}
		c.client.DeleteObjectsBatch(ctx, keys)

		c.reader.InvalidateFooters(topic, topicID, partition)

		return lastOffset, nil
	}

	outputData := BuildObject(outputBatches)
	baseOffset := outputBatches[0].BaseOffset
	outputKey := ObjectKey(c.client.prefix, topic, topicID, partition, baseOffset)

	if err := c.client.PutObject(ctx, outputKey, outputData); err != nil {
		return currentCleanedUpTo, fmt.Errorf("PUT compacted object: %w", err)
	}

	c.reader.InvalidateFooters(topic, topicID, partition)

	lastBatch := outputBatches[len(outputBatches)-1]
	newWatermark := lastBatch.BaseOffset + int64(lastBatch.LastOffsetDelta)

	// Persist watermark BEFORE deleting source objects — crash between
	// persist and delete is safe (re-compaction is idempotent).
	if c.cfg.PersistWatermark != nil {
		if err := c.cfg.PersistWatermark(topic, partition, newWatermark); err != nil {
			return currentCleanedUpTo, fmt.Errorf("persist watermark: %w", err)
		}
	}

	var deleteKeys []string
	for _, co := range cached {
		if co.key != outputKey {
			deleteKeys = append(deleteKeys, co.key)
		}
	}
	if len(deleteKeys) > 0 {
		c.client.DeleteObjectsBatch(ctx, deleteKeys)
	}

	return newWatermark, nil
}

func (c *Compactor) buildOffsetMap(rawBytes []byte, footer *Footer, offsetMap map[string]int64) error {
	for _, entry := range footer.Entries {
		if int(entry.BytePosition)+int(entry.BatchLength) > len(rawBytes) {
			continue
		}
		batchRaw := rawBytes[entry.BytePosition : entry.BytePosition+entry.BatchLength]

		header, err := ParseBatchHeaderFromRaw(batchRaw)
		if err != nil {
			c.logger.Warn("compactor: skipping batch with unparseable header in buildOffsetMap", "err", err)
			continue
		}

		if header.IsControlBatch() {
			continue
		}

		codec := header.CompressionCodec()
		decompressed, err := DecompressRecords(batchRaw, codec)
		if err != nil {
			c.logger.Warn("compactor: skipping batch with decompression error in buildOffsetMap",
				"base_offset", header.BaseOffset, "err", err)
			continue
		}

		err = IterateRecords(decompressed, func(rec Record) bool {
			if rec.Key == nil {
				return true // null keys are always retained
			}
			absOffset := header.BaseOffset + int64(rec.OffsetDelta)
			key := string(rec.Key)
			if existing, ok := offsetMap[key]; !ok || absOffset > existing {
				offsetMap[key] = absOffset
			}
			return true
		})
		if err != nil {
			c.logger.Debug("error iterating records for offset map", "err", err)
		}
	}
	return nil
}

func (c *Compactor) filterBatches(rawBytes []byte, footer *Footer, offsetMap map[string]int64, nowMs int64) ([]BatchData, error) {
	var result []BatchData

	for _, entry := range footer.Entries {
		if int(entry.BytePosition)+int(entry.BatchLength) > len(rawBytes) {
			continue
		}
		batchRaw := rawBytes[entry.BytePosition : entry.BytePosition+entry.BatchLength]

		header, err := ParseBatchHeaderFromRaw(batchRaw)
		if err != nil {
			c.logger.Warn("compactor: skipping batch with unparseable header in filterBatches", "err", err)
			continue
		}

		// Keep control batches as-is for now (simplified transactional handling)
		if header.IsControlBatch() {
			bd := BatchData{
				RawBytes:        make([]byte, len(batchRaw)),
				BaseOffset:      header.BaseOffset,
				LastOffsetDelta: header.LastOffsetDelta,
			}
			copy(bd.RawBytes, batchRaw)
			result = append(result, bd)
			continue
		}

		codec := header.CompressionCodec()
		decompressed, err := DecompressRecords(batchRaw, codec)
		if err != nil {
			c.logger.Warn("compactor: keeping batch as-is due to decompression error in filterBatches",
				"base_offset", header.BaseOffset, "err", err)
			bd := BatchData{
				RawBytes:        make([]byte, len(batchRaw)),
				BaseOffset:      header.BaseOffset,
				LastOffsetDelta: header.LastOffsetDelta,
			}
			copy(bd.RawBytes, batchRaw)
			result = append(result, bd)
			continue
		}

		var retained []Record
		err = IterateRecords(decompressed, func(rec Record) bool {
			absOffset := header.BaseOffset + int64(rec.OffsetDelta)

			if rec.Key == nil {
				retained = append(retained, rec)
				return true
			}

			latestOffset, inMap := offsetMap[string(rec.Key)]

			if !inMap || absOffset >= latestOffset {
				if rec.Value == nil && c.cfg.DeleteRetentionMs > 0 {
					var recordTs int64
					if header.TimestampType() == 1 {
						// LogAppendTime: use MaxTimestamp from header
						recordTs = header.MaxTimestamp
					} else {
						// CreateTime: use individual record's timestamp
						recordTs = header.BaseTimestamp + rec.TimestampDelta
					}
					if recordTs > 0 && nowMs-recordTs > c.cfg.DeleteRetentionMs {
						return true
					}
				}
				retained = append(retained, rec)
				return true
			}

			return true
		})
		if err != nil {
			c.logger.Debug("error iterating records for filter", "err", err)
			continue
		}

		if len(retained) == 0 {
			continue
		}

		// All records survived — keep original bytes to avoid re-encoding
		if int32(len(retained)) == header.NumRecords {
			bd := BatchData{
				RawBytes:        make([]byte, len(batchRaw)),
				BaseOffset:      header.BaseOffset,
				LastOffsetDelta: header.LastOffsetDelta,
			}
			copy(bd.RawBytes, batchRaw)
			result = append(result, bd)
			continue
		}

		newBatchBytes, err := BuildRecordBatch(header, retained, codec)
		if err != nil {
			c.logger.Warn("failed to build compacted batch, keeping original", "err", err)
			bd := BatchData{
				RawBytes:        make([]byte, len(batchRaw)),
				BaseOffset:      header.BaseOffset,
				LastOffsetDelta: header.LastOffsetDelta,
			}
			copy(bd.RawBytes, batchRaw)
			result = append(result, bd)
			continue
		}

		lastOD := retained[len(retained)-1].OffsetDelta
		result = append(result, BatchData{
			RawBytes:        newBatchBytes,
			BaseOffset:      header.BaseOffset,
			LastOffsetDelta: int32(lastOD),
		})
	}

	return result, nil
}

type windowObj struct {
	key          string
	size         int64
	baseOffset   int64
	lastModified time.Time
}

// formWindows groups objects into compaction windows bounded by WindowBytes.
// Objects newer than minCompactionLagMs (based on LastModified) are excluded,
// except for the anchor object which is always included.
func (c *Compactor) formWindows(parsed []windowObj, anchorIdx int, minCompactionLagMs int64) [][]windowObj {
	startIdx := 0
	if anchorIdx >= 0 {
		startIdx = anchorIdx
	}

	now := c.clock.Now()
	lagCutoff := now.Add(-time.Duration(minCompactionLagMs) * time.Millisecond)

	var windows [][]windowObj
	var currentWindow []windowObj
	var currentSize int64

	for i := startIdx; i < len(parsed); i++ {
		p := parsed[i]

		// Skip objects that are too recent, unless it's the anchor object.
		if minCompactionLagMs > 0 && i != anchorIdx &&
			!p.lastModified.IsZero() && p.lastModified.After(lagCutoff) {
			continue
		}

		if currentSize+p.size > c.cfg.WindowBytes && len(currentWindow) > 0 {
			windows = append(windows, currentWindow)
			currentWindow = nil
			currentSize = 0
		}

		currentWindow = append(currentWindow, p)
		currentSize += p.size
	}

	if len(currentWindow) > 0 {
		windows = append(windows, currentWindow)
	}

	return windows
}

// orphanCleanup deletes objects whose offset range is fully covered by a later object.
func (c *Compactor) orphanCleanup(ctx context.Context, parsed []windowObj) error {
	if len(parsed) < 2 {
		return nil
	}

	type objRange struct {
		key       string
		baseOff   int64
		lastOff   int64
		footerErr bool
	}

	ranges := make([]objRange, len(parsed))
	for i, p := range parsed {
		ranges[i] = objRange{
			key:     p.key,
			baseOff: p.baseOffset,
			lastOff: p.baseOffset, // minimum
		}

		footer, err := c.reader.GetFooter(ctx, p.key, p.size)
		if err != nil {
			ranges[i].footerErr = true
			continue
		}
		if lo := footer.LastOffset(); lo >= 0 {
			ranges[i].lastOff = lo
		}
	}

	orphanSet := make(map[string]bool)
	for i := 0; i < len(ranges); i++ {
		if ranges[i].footerErr {
			continue
		}
		for j := 0; j < len(ranges); j++ {
			if i == j || ranges[j].footerErr {
				continue
			}
			// If object i's range is fully covered by object j, object i is orphan
			if ranges[i].baseOff >= ranges[j].baseOff && ranges[i].lastOff <= ranges[j].lastOff && ranges[i].key != ranges[j].key {
				orphanSet[ranges[i].key] = true
				break
			}
		}
	}
	var orphanKeys []string
	for k := range orphanSet {
		orphanKeys = append(orphanKeys, k)
	}

	if len(orphanKeys) > 0 {
		c.logger.Info("compaction orphan cleanup",
			"orphans", len(orphanKeys))
		c.client.DeleteObjectsBatch(ctx, orphanKeys)
	}

	return nil
}

func parseBaseOffset(key, prefix string) int64 {
	rel := strings.TrimPrefix(key, prefix)
	if !strings.HasSuffix(rel, ".obj") {
		return -1
	}
	rel = strings.TrimSuffix(rel, ".obj")
	var offset int64
	if _, err := fmt.Sscanf(rel, "%d", &offset); err != nil {
		return -1
	}
	return offset
}

type CompactionEligibility struct {
	Eligible     bool
	DirtyObjects int32
	Topic        string
	Partition    int32
}

type DirtyPartitionInfo struct {
	DirtyObjects  int32
	LastCompacted time.Time
	CleanedUpTo   int64
}

// waitRateLimiter waits for n tokens from the rate limiter, breaking large
// reservations into burst-sized chunks to avoid exceeding the limiter's burst.
func waitRateLimiter(ctx context.Context, rl *rate.Limiter, n int) error {
	burst := rl.Burst()
	for n > 0 {
		chunk := n
		if chunk > burst {
			chunk = burst
		}
		if err := rl.WaitN(ctx, chunk); err != nil {
			return err
		}
		n -= chunk
	}
	return nil
}
