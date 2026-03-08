package broker

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/klaudworks/klite/internal/chunk"
	"github.com/klaudworks/klite/internal/cluster"
	"github.com/klaudworks/klite/internal/metadata"
	s3store "github.com/klaudworks/klite/internal/s3"
)

func (b *Broker) initS3() error {
	s3api := b.cfg.S3API
	if s3api == nil {
		var err error
		s3api, err = createAWSS3Client(b.cfg)
		if err != nil {
			return fmt.Errorf("create AWS S3 client: %w", err)
		}
	}

	prefix := "klite-" + b.clusterID
	if b.cfg.S3Prefix != "" {
		prefix = b.cfg.S3Prefix + "/" + prefix
	}

	b.s3Client = s3store.NewClient(s3store.ClientConfig{
		S3Client: s3api,
		Bucket:   b.cfg.S3Bucket,
		Prefix:   prefix,
		Logger:   b.logger,
	})

	b.s3Reader = s3store.NewReader(b.s3Client, b.logger)

	fetchAdapter := &s3store.ReaderAdapter{Reader: b.s3Reader}
	b.state.SetS3Fetcher(fetchAdapter)

	flushInterval := b.cfg.S3FlushInterval
	if flushInterval == 0 {
		flushInterval = 60 * time.Second
	}
	checkInterval := b.cfg.S3FlushCheckInterval
	if checkInterval == 0 {
		checkInterval = 5 * time.Second
	}
	targetObjSize := b.cfg.S3TargetObjectSize
	if targetObjSize == 0 {
		targetObjSize = 64 * 1024 * 1024 // 64 MiB
	}

	triggerCh := make(chan struct{}, 1)
	if b.chunkPool != nil {
		b.chunkPool.SetTriggerCh(triggerCh)
	}

	partAdapter := &s3PartitionAdapter{
		state: b.state,
	}

	flusherCfg := s3store.FlusherConfig{
		Client:            b.s3Client,
		WALWriter:         b.walWriter,
		WALIndex:          b.walIndex,
		FlushInterval:     flushInterval,
		CheckInterval:     checkInterval,
		TargetObjectSize:  targetObjSize,
		UploadConcurrency: 16,
		Logger:            b.logger,
		TriggerCh:         triggerCh,
		MetadataUploader:  b.uploadMetadataLog,
		Reader:            b.s3Reader,
		WatermarkProvider: partAdapter,
	}

	b.s3Flusher = s3store.NewFlusher(flusherCfg, partAdapter)

	// In replication mode, the flusher is started/stopped by onElected/shutdownPrimary.
	// In single-node mode, start it immediately.
	if b.cfg.ReplicationAddr == "" {
		b.s3Flusher.Start()
	}

	// Probe S3 to discover HW for partitions that have data in S3
	// but no local WAL data (disaster recovery scenario)
	b.probeS3Watermarks()

	// Rehydrate dirty object counters for compacted partitions so
	// compaction resumes after restart without new writes.
	b.rehydrateDirtyCounters()

	b.logger.Info("S3 storage initialized",
		"bucket", b.cfg.S3Bucket,
		"prefix", prefix,
		"flush_interval", flushInterval,
		"check_interval", checkInterval,
		"target_object_size", targetObjSize,
	)

	return nil
}

func (b *Broker) uploadMetadataLog(ctx context.Context) error {
	if b.metaLog == nil || b.s3Client == nil {
		return nil
	}

	data, err := os.ReadFile(b.metaLog.Path())
	if err != nil {
		return fmt.Errorf("read metadata.log: %w", err)
	}

	key := b.s3Client.Prefix() + "/metadata.log"
	return b.s3Client.PutObject(ctx, key, data)
}

// maybeRecoverFromS3 checks if metadata.log is missing locally and attempts
// to download it from S3 (disaster recovery scenario).
func (b *Broker) maybeRecoverFromS3() error {
	metaPath := filepath.Join(b.cfg.DataDir, "metadata.log")
	if _, err := os.Stat(metaPath); err == nil {
		return nil // metadata.log exists locally, no recovery needed
	}

	// metadata.log is missing. Try to download from S3.
	s3api := b.cfg.S3API
	if s3api == nil {
		var err error
		s3api, err = createAWSS3Client(b.cfg)
		if err != nil {
			return fmt.Errorf("create AWS S3 client for recovery: %w", err)
		}
	}

	prefix := "klite-" + b.clusterID
	if b.cfg.S3Prefix != "" {
		prefix = b.cfg.S3Prefix + "/" + prefix
	}

	client := s3store.NewClient(s3store.ClientConfig{
		S3Client: s3api,
		Bucket:   b.cfg.S3Bucket,
		Prefix:   prefix,
		Logger:   b.logger,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	metaKey := prefix + "/metadata.log"
	data, err := client.GetObject(ctx, metaKey)
	if err != nil {
		b.logger.Info("no metadata.log backup found in S3, attempting topic inference from S3 keys", "key", metaKey, "err", err)
		return b.inferTopicsFromS3(ctx, client, prefix, metaPath)
	}

	if err := os.WriteFile(metaPath, data, 0o644); err != nil {
		return fmt.Errorf("write recovered metadata.log: %w", err)
	}

	b.logger.Info("recovered metadata.log from S3",
		"key", metaKey, "size", len(data))

	return nil
}

// inferTopicsFromS3 reconstructs a metadata.log from S3 object keys when both
// the local metadata.log and the S3 backup are missing (full disaster recovery).
// It lists objects under prefix, parses topic/partition from key structure
// (prefix/topic/partition/offset.obj), generates new topic IDs, and writes
// a fresh metadata.log with CREATE_TOPIC entries.
func (b *Broker) inferTopicsFromS3(ctx context.Context, client *s3store.Client, prefix, metaPath string) error {
	objects, err := client.ListObjects(ctx, prefix+"/")
	if err != nil {
		return fmt.Errorf("list S3 objects for inference: %w", err)
	}

	if len(objects) == 0 {
		b.logger.Info("no S3 objects found, nothing to infer")
		return nil
	}

	// Parse topic/partition from keys. Key format: prefix/topicName-topicID/partition/offset.obj
	// Skip non-data keys like prefix/metadata.log.
	type topicInfo struct {
		maxPartition int32
		topicID      [16]byte // extracted from the key's hex-encoded topic ID
	}
	topics := make(map[string]*topicInfo)

	trimPrefix := prefix + "/"
	for _, obj := range objects {
		rel := strings.TrimPrefix(obj.Key, trimPrefix)
		parts := strings.Split(rel, "/")
		if len(parts) != 3 {
			continue // not a data object (e.g. metadata.log)
		}
		if !strings.HasSuffix(parts[2], ".obj") {
			continue
		}

		// parts[0] is "topicName-hexTopicID" (32-char hex suffix)
		topicDir := parts[0]
		topicName, parsedID := s3store.ParseTopicDir(topicDir)
		if topicName == "" {
			continue
		}

		partIdx, err := strconv.Atoi(parts[1])
		if err != nil {
			continue
		}

		ti, ok := topics[topicName]
		if !ok {
			ti = &topicInfo{maxPartition: -1, topicID: parsedID}
			topics[topicName] = ti
		}
		if int32(partIdx) > ti.maxPartition {
			ti.maxPartition = int32(partIdx)
		}
	}

	if len(topics) == 0 {
		b.logger.Info("no topic data found in S3 objects")
		return nil
	}

	// Sort topic names for deterministic metadata.log output.
	topicNames := make([]string, 0, len(topics))
	for name := range topics {
		topicNames = append(topicNames, name)
	}
	sort.Strings(topicNames)

	// Build metadata.log entries with generated topic IDs.
	crc32cTable := crc32.MakeTable(crc32.Castagnoli)
	f, err := os.Create(metaPath)
	if err != nil {
		return fmt.Errorf("create inferred metadata.log: %w", err)
	}
	defer f.Close() //nolint:errcheck // best-effort close

	for _, name := range topicNames {
		ti := topics[name]
		idBytes := ti.topicID
		var zeroID [16]byte
		if idBytes == zeroID {
			topicUUID := uuid.New()
			copy(idBytes[:], topicUUID[:])
		}

		entry := metadata.MarshalCreateTopic(&metadata.CreateTopicEntry{
			TopicName:      name,
			PartitionCount: ti.maxPartition + 1,
			TopicID:        idBytes,
		})

		// Frame: [4 bytes length][4 bytes CRC32c][payload]
		frameSize := 4 + 4 + len(entry)
		frame := make([]byte, frameSize)
		binary.BigEndian.PutUint32(frame[0:4], uint32(4+len(entry)))
		binary.BigEndian.PutUint32(frame[4:8], crc32.Checksum(entry, crc32cTable))
		copy(frame[8:], entry)

		if _, err := f.Write(frame); err != nil {
			return fmt.Errorf("write inferred metadata entry for %s: %w", name, err)
		}

		b.logger.Info("inferred topic from S3",
			"topic", name,
			"partitions", ti.maxPartition+1,
			"topic_id", hex.EncodeToString(idBytes[:]))
	}

	if err := f.Sync(); err != nil {
		return fmt.Errorf("fsync inferred metadata.log: %w", err)
	}

	b.logger.Info("inferred metadata.log from S3 key structure",
		"topics", len(topics))
	return nil
}

// rehydrateDirtyCounters counts S3 objects per compacted partition and sets
// the dirty object counter so compaction can trigger after restart without
// new writes. For each compacted partition, objects above cleanedUpTo are
// counted as dirty.
func (b *Broker) rehydrateDirtyCounters() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	topics := b.state.GetAllTopics()
	for _, td := range topics {
		policy, ok := td.GetConfig("cleanup.policy")
		if !ok {
			policy = "delete"
		}
		if !strings.Contains(policy, "compact") {
			continue
		}

		for _, pd := range td.Partitions {
			prefix := s3store.ObjectKeyPrefix(b.s3Client.Prefix(), td.Name, td.ID, pd.Index)
			objects, err := b.s3Client.ListObjects(ctx, prefix)
			if err != nil {
				b.logger.Debug("dirty counter rehydration failed",
					"topic", td.Name, "partition", pd.Index, "err", err)
				continue
			}

			pd.Lock()
			cleanedUpTo := pd.CleanedUpTo()
			dirty := int32(0)
			for _, obj := range objects {
				if !strings.HasSuffix(obj.Key, ".obj") {
					continue
				}
				baseOff := s3store.ParseBaseOffsetFromKey(obj.Key)
				if baseOff > cleanedUpTo {
					dirty++
				}
			}
			if dirty > 0 {
				pd.SetDirtyObjects(dirty)
				b.logger.Debug("dirty counter rehydrated",
					"topic", td.Name, "partition", pd.Index, "dirty", dirty)
			}
			pd.Unlock()
		}
	}
}

// probeS3Watermarks discovers high-water marks from S3 for each partition
// and updates the cluster state. Always probes, even when a watermark already
// exists, because the standby's watermark may lag the actual S3 state — the
// old primary may have flushed more data after the last watermark was
// replicated. For partitions with no WAL data (disaster recovery), both HW
// and S3 flush watermark are set from S3.
func (b *Broker) probeS3Watermarks() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	topics := b.state.GetAllTopics()
	updated := 0
	for _, td := range topics {
		for _, pd := range td.Partitions {
			s3HW, err := b.s3Reader.DiscoverHW(ctx, td.Name, td.ID, pd.Index)
			if err != nil {
				b.logger.Debug("S3 HW probe failed", "topic", td.Name, "partition", pd.Index, "err", err)
				continue
			}
			if s3HW > 0 {
				pd.Lock()
				if s3HW > pd.HW() {
					pd.SetHW(s3HW)
				}
				pd.SetS3FlushWatermark(s3HW)
				pd.Unlock()
				updated++
			}
		}
	}

	if updated > 0 {
		b.logger.Info("S3 watermark probe complete", "partitions_updated", updated)
	}
}

func createAWSS3Client(cfg Config) (s3store.S3API, error) {
	region := cfg.S3Region
	if region == "" {
		region = "us-east-1"
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion(region),
	)
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}

	var s3Opts []func(*s3sdk.Options)
	if cfg.S3Endpoint != "" {
		s3Opts = append(s3Opts, func(o *s3sdk.Options) {
			o.BaseEndpoint = &cfg.S3Endpoint
			o.UsePathStyle = true
		})
	}

	return s3sdk.NewFromConfig(awsCfg, s3Opts...), nil
}

type s3PartitionAdapter struct {
	state *cluster.State
}

func (a *s3PartitionAdapter) FlushablePartitions() []s3store.FlushPartition {
	parts := a.state.FlushablePartitions()
	var result []s3store.FlushPartition

	for _, fp := range parts {
		pd := fp.Partition_ // captured partition reference

		pd.RLock()
		hasData := pd.HasChunkData()
		sealedBytes := pd.SealedChunkBytes()
		oldestTime := pd.OldestSealedChunkTime()
		pd.RUnlock()

		if !hasData {
			continue
		}

		result = append(result, s3store.FlushPartition{
			Topic:           fp.Topic,
			Partition:       fp.Partition,
			TopicID:         fp.TopicID,
			S3Watermark:     fp.S3Watermark,
			HW:              fp.HW,
			SealedBytes:     sealedBytes,
			OldestChunkTime: oldestTime,
			DetachChunks: func(flushAll bool) ([]*chunk.Chunk, *chunk.Pool) {
				pd.Lock()
				chunks := pd.DetachSealedChunks(flushAll)
				pool := pd.ChunkPool()
				pd.Unlock()
				return chunks, pool
			},
			ReattachChunks: func(chunks []*chunk.Chunk) {
				pd.Lock()
				pd.ReattachSealedChunks(chunks)
				pd.Unlock()
			},
			AdvanceWatermark: func(newWatermark int64) {
				pd.Lock()
				pd.SetS3FlushWatermark(newWatermark)
				pd.IncrementDirtyObjects()
				pd.Unlock()
			},
		})
	}

	return result
}

func (a *s3PartitionAdapter) AllPartitionWatermarks() []s3store.PartitionWatermarkInfo {
	wms := a.state.AllPartitionWatermarks()
	result := make([]s3store.PartitionWatermarkInfo, len(wms))
	for i, w := range wms {
		result[i] = s3store.PartitionWatermarkInfo{
			TopicID:     w.TopicID,
			Partition:   w.Partition,
			S3Watermark: w.S3Watermark,
		}
	}
	return result
}
