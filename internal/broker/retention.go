package broker

import (
	"context"
	"strconv"
	"time"

	"github.com/klaudworks/klite/internal/cluster"
	s3store "github.com/klaudworks/klite/internal/s3"
)

const defaultRetentionPartitionsPerTick = 64

type retentionPartitionKey struct {
	topicID   [16]byte
	partition int32
}

type retentionWorkItem struct {
	topic          string
	topicID        [16]byte
	partition      *cluster.PartData
	retentionMs    int64
	retentionBytes int64
}

func (b *Broker) retentionLoop(ctx context.Context) {
	interval := b.cfg.RetentionCheckInterval
	ticker := b.cfg.Clock.NewTicker(interval)
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

func (b *Broker) enforceRetention(ctx context.Context) {
	if b.s3Client == nil {
		return
	}

	nowMs := b.cfg.Clock.Now().UnixMilli()
	work := b.selectRetentionWork()
	for _, item := range work {
		if ctx.Err() != nil {
			return
		}
		if b.enforcePartitionRetention(ctx, item.topic, item.topicID, item.partition, item.retentionMs, item.retentionBytes, nowMs) {
			b.clearRetentionDirty(item.topicID, item.partition.Index)
		}
	}
}

func (b *Broker) selectRetentionWork() []retentionWorkItem {
	topics := b.state.GetAllTopics()
	var candidates []retentionWorkItem
	for _, td := range topics {
		retentionMs, retentionBytes := parseTopicRetention(td)
		if retentionMs < 0 && retentionBytes < 0 {
			continue
		}
		for _, pd := range td.Partitions {
			candidates = append(candidates, retentionWorkItem{
				topic:          td.Name,
				topicID:        td.ID,
				partition:      pd,
				retentionMs:    retentionMs,
				retentionBytes: retentionBytes,
			})
		}
	}

	if len(candidates) == 0 {
		return nil
	}

	budget := b.retentionPartitionsPerTick
	if budget <= 0 {
		budget = defaultRetentionPartitionsPerTick
	}
	if budget > len(candidates) {
		budget = len(candidates)
	}

	b.retentionMu.Lock()
	defer b.retentionMu.Unlock()

	live := make(map[retentionPartitionKey]struct{}, len(candidates))
	for _, c := range candidates {
		live[retentionPartitionKey{topicID: c.topicID, partition: c.partition.Index}] = struct{}{}
	}
	for key := range b.retentionDirty {
		if _, ok := live[key]; !ok {
			delete(b.retentionDirty, key)
		}
	}

	selected := make([]retentionWorkItem, 0, budget)
	selectedIdx := make(map[int]struct{}, budget)
	for i, c := range candidates {
		if len(selected) >= budget {
			break
		}
		key := retentionPartitionKey{topicID: c.topicID, partition: c.partition.Index}
		if _, ok := b.retentionDirty[key]; ok {
			selected = append(selected, c)
			selectedIdx[i] = struct{}{}
		}
	}

	start := b.retentionCursor
	if len(candidates) > 0 {
		start %= len(candidates)
	}
	roundRobinAdded := 0
	for checked := 0; checked < len(candidates) && len(selected) < budget; checked++ {
		idx := (start + checked) % len(candidates)
		if _, alreadySelected := selectedIdx[idx]; alreadySelected {
			continue
		}
		selected = append(selected, candidates[idx])
		roundRobinAdded++
	}
	if len(candidates) > 0 {
		b.retentionCursor = (start + roundRobinAdded) % len(candidates)
	}

	return selected
}

func parseTopicRetention(td *cluster.TopicData) (retentionMs, retentionBytes int64) {
	retentionMs = int64(604800000) // 7 days default
	retentionBytes = int64(-1)     // infinite default

	if v, ok := td.GetConfig("retention.ms"); ok {
		if parsed, err := strconv.ParseInt(v, 10, 64); err == nil {
			retentionMs = parsed
		}
	}
	if v, ok := td.GetConfig("retention.bytes"); ok {
		if parsed, err := strconv.ParseInt(v, 10, 64); err == nil {
			retentionBytes = parsed
		}
	}

	return retentionMs, retentionBytes
}

func (b *Broker) markRetentionDirty(topicID [16]byte, partition int32) {
	b.retentionMu.Lock()
	if b.retentionDirty == nil {
		b.retentionDirty = make(map[retentionPartitionKey]struct{})
	}
	b.retentionDirty[retentionPartitionKey{topicID: topicID, partition: partition}] = struct{}{}
	b.retentionMu.Unlock()
}

func (b *Broker) clearRetentionDirty(topicID [16]byte, partition int32) {
	b.retentionMu.Lock()
	delete(b.retentionDirty, retentionPartitionKey{topicID: topicID, partition: partition})
	b.retentionMu.Unlock()
}

func (b *Broker) enforcePartitionRetention(
	ctx context.Context,
	topic string,
	topicID [16]byte,
	pd *cluster.PartData,
	retentionMs int64,
	retentionBytes int64,
	nowMs int64,
) bool {
	prefix := s3store.ObjectKeyPrefix(b.s3Client.Prefix(), topic, topicID, pd.Index)
	objects, err := b.s3Client.ListObjects(ctx, prefix)
	if err != nil {
		b.logger.Warn("retention: list objects failed",
			"topic", topic, "partition", pd.Index, "err", err)
		return false
	}
	if len(objects) == 0 {
		return true
	}

	type objInfo struct {
		key        string
		baseOffset int64
		dataSize   int64 // logical data bytes (sum of batch lengths, no footer overhead)
		footer     *s3store.Footer
	}
	var infos []objInfo
	for _, obj := range objects {
		info := objInfo{
			key:        obj.Key,
			baseOffset: s3store.ParseBaseOffsetFromKey(obj.Key),
			dataSize:   obj.Size,
		}
		footer, err := b.s3Reader.GetFooter(ctx, obj.Key, obj.Size)
		if err != nil {
			b.logger.Warn("retention: read footer failed",
				"key", obj.Key, "err", err)
			infos = append(infos, info)
			continue
		}
		info.dataSize = footer.DataSize()
		info.footer = footer
		infos = append(infos, info)
	}
	if len(infos) == 0 {
		return true
	}

	timeCutoff := nowMs - retentionMs

	expiredByTime := make([]bool, len(infos))
	if retentionMs >= 0 {
		for i, info := range infos {
			if info.footer == nil {
				continue
			}
			if info.footer.MaxTimestamp() >= 0 && info.footer.MaxTimestamp() < timeCutoff {
				expiredByTime[i] = true
			}
		}
	}

	// Uses logical data size (sum of batch lengths) to match Kafka's retention.bytes
	// semantics, which refers to log segment data, not on-disk overhead.
	expiredBySize := make([]bool, len(infos))
	if retentionBytes >= 0 {
		var totalSize int64
		for _, info := range infos {
			totalSize += info.dataSize
		}
		for i := 0; i < len(infos) && totalSize > retentionBytes; i++ {
			if i == len(infos)-1 {
				break
			}
			expiredBySize[i] = true
			totalSize -= infos[i].dataSize
		}
	}

	var deleteKeys []string
	for i, info := range infos {
		if expiredByTime[i] || expiredBySize[i] {
			if len(deleteKeys)+1 >= len(infos) {
				break
			}
			deleteKeys = append(deleteKeys, info.key)
		}
	}

	if len(deleteKeys) == 0 {
		return true
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
		return false
	}

	deletedSet := make(map[string]struct{}, deleted)
	for i := 0; i < deleted; i++ {
		deletedSet[deleteKeys[i]] = struct{}{}
	}

	actualLogStart := int64(-1)
	for _, info := range infos {
		if _, wasDeleted := deletedSet[info.key]; wasDeleted {
			continue
		}
		if info.baseOffset < 0 {
			continue
		}
		actualLogStart = info.baseOffset
		break
	}

	if actualLogStart < 0 {
		pd.RLock()
		actualLogStart = pd.LogStart()
		pd.RUnlock()
		for _, info := range infos {
			if _, wasDeleted := deletedSet[info.key]; !wasDeleted {
				continue
			}
			if info.footer == nil {
				continue
			}
			lastOff := info.footer.LastOffset()
			if lastOff+1 > actualLogStart {
				actualLogStart = lastOff + 1
			}
		}
	}

	pd.CompactionMu.Lock()
	err = pd.AdvanceLogStartOffset(actualLogStart, b.metaLog)
	pd.CompactionMu.Unlock()
	if err != nil {
		b.logger.Warn("retention: failed to advance logStartOffset",
			"topic", topic, "partition", pd.Index,
			"new_log_start", actualLogStart, "err", err)
		return false
	}

	b.s3Reader.InvalidateFooters(topic, topicID, pd.Index)

	b.logger.Info("retention: deleted S3 objects",
		"topic", topic, "partition", pd.Index,
		"objects_deleted", deleted, "new_log_start", actualLogStart)

	return true
}

func (b *Broker) s3GCLoop(ctx context.Context) {
	orphans := b.scanOrphanedS3Topics()
	pending := b.state.DrainDeletedTopics()
	b.deleteTopicObjects(ctx, append(pending, orphans...))

	ticker := b.cfg.Clock.NewTicker(30 * time.Second)
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

func (b *Broker) deleteTopicObjects(ctx context.Context, topics []cluster.DeletedTopic) {
	for _, dt := range topics {
		if ctx.Err() != nil {
			return
		}
		b.gcDeletedTopic(ctx, dt)
	}
}

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
	failed := b.s3Client.DeleteObjectsBatch(ctx, keys)
	if failed > 0 {
		b.logger.Warn("S3 GC: delete failed for some objects, will retry",
			"topic", dt.Name, "failed", failed, "total", len(keys))
		b.state.AddDeletedTopic(dt)
		return
	}

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
	dirs, err := b.s3Client.ListChildDirs(ctx, prefix)
	if err != nil {
		b.logger.Warn("S3 orphan scan: list failed", "err", err)
		return nil
	}

	liveDirs := make(map[string]bool)
	for _, td := range b.state.GetAllTopics() {
		liveDirs[s3store.TopicDir(td.Name, td.ID)] = true
	}

	var result []cluster.DeletedTopic
	for _, dir := range dirs {
		if liveDirs[dir] {
			continue
		}
		name, id := s3store.ParseTopicDir(dir)
		result = append(result, cluster.DeletedTopic{
			Name:    name,
			TopicID: id,
		})
	}

	if len(result) == 0 {
		return nil
	}

	b.logger.Info("S3 orphan scan: found orphaned topic dirs",
		"orphans", len(result))
	return result
}
