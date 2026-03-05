package broker

import (
	"context"
	"strconv"
	"strings"
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
			b.enforcePartitionRetention(ctx, td.Name, td.ID, pd, retentionMs, retentionBytes, nowMs)
		}
	}
}

// enforcePartitionRetention enforces retention on a single partition's S3 objects.
func (b *Broker) enforcePartitionRetention(
	ctx context.Context,
	topic string,
	topicID [16]byte,
	pd *cluster.PartData,
	retentionMs int64,
	retentionBytes int64,
	nowMs int64,
) {
	prefix := s3store.ObjectKeyPrefix(b.s3Client.Prefix(), topic, topicID, pd.Index)
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

	b.s3Reader.InvalidateFooters(topic, topicID, pd.Index)

	b.logger.Info("retention: deleted S3 objects",
		"topic", topic, "partition", pd.Index,
		"objects_deleted", deleted, "new_log_start", actualLogStart)
}

// s3GCLoop runs a background goroutine that deletes S3 objects for deleted topics.
func (b *Broker) s3GCLoop(ctx context.Context) {
	// First tick: scan for orphans left by prior crashes, then GC them
	// together with any topics deleted during startup replay.
	orphans := b.scanOrphanedS3Topics()
	pending := b.state.DrainDeletedTopics()
	b.deleteTopicObjects(ctx, append(pending, orphans...))

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if pending := b.state.DrainDeletedTopics(); len(pending) > 0 {
				b.deleteTopicObjects(ctx, pending)
			}
		}
	}
}

// deleteTopicObjects deletes all S3 objects for the given deleted topics.
func (b *Broker) deleteTopicObjects(ctx context.Context, topics []cluster.DeletedTopic) {
	for _, dt := range topics {
		if ctx.Err() != nil {
			return
		}
		b.gcDeletedTopic(ctx, dt)
	}
}

// gcDeletedTopic deletes all S3 objects for a single deleted topic.
func (b *Broker) gcDeletedTopic(ctx context.Context, dt cluster.DeletedTopic) {
	topicDir := s3store.TopicDir(dt.Name, dt.TopicID)
	prefix := b.s3Client.Prefix() + "/" + topicDir + "/"

	objects, err := b.s3Client.ListObjects(ctx, prefix)
	if err != nil {
		b.logger.Warn("S3 GC: list failed, will retry",
			"topic", dt.Name, "err", err)
		b.state.AddDeletedTopic(dt)
		return
	}

	if len(objects) == 0 {
		b.logger.Info("S3 GC: no objects to delete", "topic", dt.Name)
		return
	}

	var keys []string
	for _, obj := range objects {
		keys = append(keys, obj.Key)
	}
	b.s3Client.DeleteObjectsBatch(ctx, keys)

	b.logger.Info("S3 GC: deleted objects for topic",
		"topic", dt.Name, "objects", len(keys))
}

// scanOrphanedS3Topics lists all topic directories in S3 and returns any
// that don't match a live topic (same name AND same ID). This catches
// orphans left by crashes between topic deletion and GC completion.
func (b *Broker) scanOrphanedS3Topics() []cluster.DeletedTopic {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	prefix := b.s3Client.Prefix() + "/"
	objects, err := b.s3Client.ListObjects(ctx, prefix)
	if err != nil {
		b.logger.Warn("S3 orphan scan: list failed", "err", err)
		return nil
	}

	// Build set of live topic dirs: "topicName-topicID"
	liveDirs := make(map[string]bool)
	for _, td := range b.state.GetAllTopics() {
		liveDirs[s3store.TopicDir(td.Name, td.ID)] = true
	}

	// Extract unique topic dirs from S3 keys.
	// The only non-topic object is the metadata.log backup at prefix level.
	orphanDirs := make(map[string]bool)
	metaKey := b.s3Client.Prefix() + "/metadata.log"
	for _, obj := range objects {
		if obj.Key == metaKey {
			continue
		}
		rel := strings.TrimPrefix(obj.Key, prefix)
		dir, _, _ := strings.Cut(rel, "/")
		if dir != "" && !liveDirs[dir] {
			orphanDirs[dir] = true
		}
	}

	if len(orphanDirs) == 0 {
		return nil
	}

	var result []cluster.DeletedTopic
	for dir := range orphanDirs {
		name, id := s3store.ParseTopicDir(dir)
		result = append(result, cluster.DeletedTopic{
			Name:    name,
			TopicID: id,
		})
	}

	b.logger.Info("S3 orphan scan: found orphaned topic dirs",
		"orphans", len(result))
	return result
}
