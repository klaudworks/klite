package broker

import (
	"context"
	"strconv"
	"time"

	"github.com/klaudworks/klite/internal/clock"
	"github.com/klaudworks/klite/internal/cluster"
	s3store "github.com/klaudworks/klite/internal/s3"
)

// retentionLoop runs the retention enforcement goroutine.
func (b *Broker) retentionLoop(ctx context.Context) {
	interval := b.cfg.RetentionCheckInterval
	if interval == 0 {
		interval = 1 * time.Hour
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			b.enforceRetention(ctx)
		}
	}
}

// enforceRetention deletes expired S3 objects based on time and size retention.
// Only operates when S3 is enabled; in-memory data is bounded by the ring buffer.
func (b *Broker) enforceRetention(ctx context.Context) {
	if b.s3Client == nil {
		return
	}

	clk := b.cfg.Clock
	if clk == nil {
		clk = clock.RealClock{}
	}
	nowMs := clk.Now().UnixMilli()
	topics := b.state.GetAllTopics()

	for _, td := range topics {
		retentionMs := int64(604800000) // 7 days default
		retentionBytes := int64(-1)     // infinite default

		if v, ok := td.Configs["retention.ms"]; ok {
			if parsed, err := strconv.ParseInt(v, 10, 64); err == nil {
				retentionMs = parsed
			}
		}
		if v, ok := td.Configs["retention.bytes"]; ok {
			if parsed, err := strconv.ParseInt(v, 10, 64); err == nil {
				retentionBytes = parsed
			}
		}

		if retentionMs < 0 && retentionBytes < 0 {
			continue // infinite retention
		}

		for _, pd := range td.Partitions {
			if ctx.Err() != nil {
				return
			}
			b.enforcePartitionRetention(ctx, td.Name, pd, retentionMs, retentionBytes, nowMs)
		}
	}
}

// enforcePartitionRetention enforces retention on a single partition's S3 objects.
func (b *Broker) enforcePartitionRetention(
	ctx context.Context,
	topic string,
	pd *cluster.PartData,
	retentionMs int64,
	retentionBytes int64,
	nowMs int64,
) {
	prefix := s3store.ObjectKeyPrefix(b.s3Client.Prefix(), topic, pd.Index)
	objects, err := b.s3Client.ListObjects(ctx, prefix)
	if err != nil {
		b.logger.Warn("retention: list objects failed",
			"topic", topic, "partition", pd.Index, "err", err)
		return
	}
	if len(objects) == 0 {
		return
	}

	// Load footers for all objects (usually cached by the reader)
	type objInfo struct {
		key      string
		dataSize int64 // logical data bytes (sum of batch lengths, no footer overhead)
		footer   *s3store.Footer
	}
	var infos []objInfo
	for _, obj := range objects {
		footer, err := b.s3Reader.GetFooter(ctx, obj.Key, obj.Size)
		if err != nil {
			b.logger.Warn("retention: read footer failed",
				"key", obj.Key, "err", err)
			continue
		}
		infos = append(infos, objInfo{key: obj.Key, dataSize: footer.DataSize(), footer: footer})
	}
	if len(infos) == 0 {
		return
	}

	// Determine which objects to delete.
	// Time-based: delete objects where MaxTimestamp < cutoff (entire object expired).
	// Size-based: sum sizes from newest to oldest, delete oldest that exceed budget.
	// Take the union: an object is deleted if EITHER policy says so.

	timeCutoff := nowMs - retentionMs // only meaningful if retentionMs >= 0

	// Mark objects expired by time
	expiredByTime := make([]bool, len(infos))
	if retentionMs >= 0 {
		for i, info := range infos {
			if info.footer.MaxTimestamp() >= 0 && info.footer.MaxTimestamp() < timeCutoff {
				expiredByTime[i] = true
			}
		}
	}

	// Mark objects expired by size (delete oldest objects that push total over budget).
	// Uses logical data size (sum of batch lengths) to match Kafka's retention.bytes
	// semantics, which refers to log segment data, not on-disk overhead.
	expiredBySize := make([]bool, len(infos))
	if retentionBytes >= 0 {
		var totalSize int64
		for _, info := range infos {
			totalSize += info.dataSize
		}
		// Walk from oldest to newest, marking objects for deletion until within budget
		for i := 0; i < len(infos) && totalSize > retentionBytes; i++ {
			// Never delete the last remaining object
			if i == len(infos)-1 {
				break
			}
			expiredBySize[i] = true
			totalSize -= infos[i].dataSize
		}
	}

	// Collect objects to delete (expired by either policy).
	// Never delete the last remaining object for a partition.
	var deleteKeys []string
	for i, info := range infos {
		if expiredByTime[i] || expiredBySize[i] {
			if len(deleteKeys)+1 >= len(infos) {
				break // never delete the last remaining object
			}
			deleteKeys = append(deleteKeys, info.key)
		}
	}

	if len(deleteKeys) == 0 {
		return
	}

	// Delete objects BEFORE advancing logStartOffset. If a delete fails,
	// we stop and only advance past the successfully deleted objects.
	// This prevents orphaned data where logStart has moved past objects
	// that still exist.
	var deleted int
	for _, key := range deleteKeys {
		if err := b.s3Client.DeleteObject(ctx, key); err != nil {
			b.logger.Warn("retention: delete failed, stopping",
				"key", key, "err", err)
			break
		}
		deleted++
	}

	if deleted == 0 {
		return
	}

	// Recalculate newLogStart based on what was actually deleted.
	// deleteKeys and infos are ordered oldest-first, so we find the
	// highest LastOffset among the first `deleted` expired objects.
	var actualLogStart int64
	deletedSet := make(map[string]bool, deleted)
	for i := 0; i < deleted; i++ {
		deletedSet[deleteKeys[i]] = true
	}
	for _, info := range infos {
		if !deletedSet[info.key] {
			continue
		}
		lastOff := info.footer.LastOffset()
		if lastOff+1 > actualLogStart {
			actualLogStart = lastOff + 1
		}
	}

	pd.CompactionMu.Lock()
	err = pd.AdvanceLogStartOffset(actualLogStart, b.metaLog)
	pd.CompactionMu.Unlock()
	if err != nil {
		b.logger.Warn("retention: failed to advance logStartOffset",
			"topic", topic, "partition", pd.Index,
			"new_log_start", actualLogStart, "err", err)
		return
	}

	b.s3Reader.InvalidateFooters(topic, pd.Index)

	b.logger.Info("retention: deleted S3 objects",
		"topic", topic, "partition", pd.Index,
		"objects_deleted", deleted, "new_log_start", actualLogStart)
}
