package broker

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/klaudworks/klite/internal/chunk"
	"github.com/klaudworks/klite/internal/cluster"
	"github.com/klaudworks/klite/internal/metadata"
	"github.com/klaudworks/klite/internal/sasl"
	"github.com/klaudworks/klite/internal/wal"
)

func (b *Broker) initMetadataLog() error {
	ml, err := metadata.NewLog(metadata.LogConfig{
		DataDir: b.cfg.DataDir,
		Logger:  b.logger,
	})
	if err != nil {
		return err
	}

	ml.SetCallbacks(
		func(e metadata.CreateTopicEntry) {
			b.state.CreateTopicFromReplay(e.TopicName, int(e.PartitionCount), e.TopicID, e.Configs)
		},
		func(e metadata.DeleteTopicEntry) {
			b.state.DeleteTopic(e.TopicName)
		},
		func(e metadata.AlterConfigEntry) {
			b.state.SetTopicConfig(e.TopicName, e.Key, e.Value)
		},
		func(e metadata.OffsetCommitEntry) {
			b.state.SetCommittedOffsetFromReplay(e.Group, e.Topic, e.Partition, e.Offset, e.Metadata)
		},
		func(e metadata.ProducerIDEntry) {
			b.state.PIDManager().SetNextPID(e.NextProducerID)
		},
		func(e metadata.LogStartOffsetEntry) {
			b.state.SetLogStartOffsetFromReplay(e.TopicName, e.Partition, e.LogStartOffset)
		},
	)

	ml.SetCompactionWatermarkCallback(func(e metadata.CompactionWatermarkEntry) {
		b.state.SetCompactionWatermarkFromReplay(e.TopicName, e.Partition, e.CleanedUpTo)
	})

	ml.SetPartitionCountCallback(func(e metadata.PartitionCountEntry) {
		b.state.AddPartitionsFromReplay(e.TopicName, e.TopicID, int(e.PartitionCount))
	})

	if b.saslStore != nil {
		ml.SetScramCallbacks(
			func(e metadata.ScramCredentialEntry) {
				auth := sasl.ScramAuthFromPreHashed(
					scramMechName(e.Mechanism),
					int(e.Iterations),
					e.SaltedPass,
					e.Salt,
				)
				switch e.Mechanism {
				case 1:
					b.saslStore.AddScram256(e.Username, auth)
				case 2:
					b.saslStore.AddScram512(e.Username, auth)
				}
			},
			func(e metadata.ScramCredentialDeleteEntry) {
				switch e.Mechanism {
				case 1:
					b.saslStore.DeleteScram256(e.Username)
				case 2:
					b.saslStore.DeleteScram512(e.Username)
				}
			},
		)
	}

	count, err := ml.Replay()
	if err != nil {
		_ = ml.Close()
		return fmt.Errorf("replay metadata.log: %w", err)
	}

	if count > 0 {
		b.logger.Info("metadata.log replay complete", "entries", count)
	}

	ml.SetSnapshotFn(b.state.SnapshotEntries)
	ml.Compact()

	b.metaLog = ml
	b.state.SetMetadataLog(ml)

	return nil
}

func (b *Broker) initWAL() error {
	walDir := filepath.Join(b.cfg.DataDir, "wal")

	idx := wal.NewIndex()
	b.walIndex = idx

	syncInterval := time.Duration(b.cfg.WALSyncIntervalMs) * time.Millisecond
	segMaxBytes := b.cfg.WALSegmentMaxBytes
	maxDiskSize := b.cfg.WALMaxDiskSize

	cfg := wal.WriterConfig{
		Dir:             walDir,
		SyncInterval:    syncInterval,
		SegmentMaxBytes: segMaxBytes,
		MaxDiskSize:     maxDiskSize,
		FsyncEnabled:    true,
		S3Configured:    b.cfg.S3Bucket != "",
		Logger:          b.logger,
	}

	w, err := wal.NewWriter(cfg, idx)
	if err != nil {
		return fmt.Errorf("create WAL writer: %w", err)
	}

	b.walWriter = w

	// Initialize chunk pool only when S3 is configured. The pool exists
	// solely for the S3 flush pipeline — without a flusher, chunks are
	// never released and producers would deadlock on Acquire().
	// When S3 is disabled, all reads are served from the WAL.
	var pool *chunk.Pool
	if b.cfg.S3Bucket != "" {
		pool = chunk.NewPool(b.cfg.ChunkPoolMemory, cluster.DefaultMaxMessageBytes)
		b.chunkPool = pool
	}
	b.state.SetWALConfig(w, idx, pool)

	if err := b.replayWAL(w); err != nil {
		return fmt.Errorf("replay WAL: %w", err)
	}

	if err := w.Start(); err != nil {
		return fmt.Errorf("start WAL writer: %w", err)
	}

	b.logger.Info("WAL initialized",
		"dir", walDir,
		"sync_interval", syncInterval,
		"segment_max_bytes", segMaxBytes,
	)

	return nil
}

// replayWAL scans existing WAL segments and rebuilds in-memory state.
// Authority rules:
//   - metadata.log is authoritative for topic existence, partition counts, and logStartOffset.
//   - WAL entries for unknown topics or out-of-range partitions are skipped with a warning.
//   - WAL entries whose last offset is below the partition's logStartOffset (from metadata.log) are skipped.
//   - WAL replay can only advance HW and logStartOffset, never reduce them.
func (b *Broker) replayWAL(w *wal.Writer) error {
	entryCount := 0
	skippedUnknown := 0
	skippedPartition := 0
	skippedBelowLogStart := 0
	skippedCorrupt := 0

	// Track open transactions across replay to reconstruct abortedTxns index.
	type replayTxnKey struct {
		partition  int32
		producerID int64
	}
	replayOpenTxns := make(map[replayTxnKey]int64) // key -> first data offset

	err := w.Replay(func(entry wal.Entry, segmentSeq uint64, fileOffset int64) error {
		// Look up the topic by ID — metadata.log has already been replayed
		td := b.state.GetTopicByID(entry.TopicID)
		if td == nil {
			// WAL entry for topic not in metadata.log. Should not happen
			// (CREATE_TOPIC is written and fsync'd before produce can succeed).
			// Log a warning and skip.
			if skippedUnknown == 0 {
				b.logger.Warn("WAL replay: skipping entry for unknown topic ID",
					"topic_id", fmt.Sprintf("%x", entry.TopicID),
					"segment", segmentSeq, "offset", fileOffset)
			}
			skippedUnknown++
			return nil
		}

		if int(entry.Partition) >= len(td.Partitions) {
			// Partition index beyond the topic's partition count — indicates corruption.
			if skippedPartition == 0 {
				b.logger.Warn("WAL replay: skipping entry for out-of-range partition",
					"topic", td.Name, "partition", entry.Partition,
					"partition_count", len(td.Partitions),
					"segment", segmentSeq, "offset", fileOffset)
			}
			skippedPartition++
			return nil
		}

		pd := td.Partitions[entry.Partition]

		// Parse batch header to get metadata
		meta, err := cluster.ParseBatchHeader(entry.Data)
		if err != nil {
			if skippedCorrupt == 0 {
				b.logger.Warn("WAL replay: skipping entry with corrupted batch header",
					"topic", td.Name, "partition", entry.Partition,
					"segment", segmentSeq, "offset", fileOffset, "err", err)
			}
			skippedCorrupt++
			return nil
		}

		// Reconstruct producer dedup state from committed batches so that
		// duplicate detection works after restart or failover.
		if meta.ProducerID >= 0 && meta.BaseSequence >= 0 {
			ctp := cluster.TopicPartition{Topic: td.Name, Partition: entry.Partition}
			b.state.PIDManager().ReplayBatch(ctp, meta, entry.Offset)
		}

		// Skip WAL entries whose last offset is below the persisted logStartOffset
		lastOffset := entry.Offset + int64(meta.LastOffsetDelta)
		pd.RLock()
		logStart := pd.LogStart()
		pd.RUnlock()
		if lastOffset < logStart {
			skippedBelowLogStart++
			return nil
		}

		// Pre-acquire spare chunk before taking the partition lock.
		spare := pd.AcquireSpareChunk(len(entry.Data))

		// Rebuild partition state: append to chunk pool and advance HW.
		pd.Lock()

		// Advance HW if needed (WAL can only advance, never reduce)
		endOffset := lastOffset + 1
		if endOffset > pd.HW() {
			pd.SetHW(endOffset)
		}

		// Reconstruct transaction state from WAL entries.
		if cluster.IsTransactionalBatch(entry.Data) && meta.ProducerID >= 0 {
			if cluster.IsControlBatch(entry.Data) {
				pd.RemoveOpenTxn(meta.ProducerID)
				if !cluster.IsCommitControlBatch(entry.Data) {
					if firstOff, ok := replayOpenTxns[replayTxnKey{entry.Partition, meta.ProducerID}]; ok {
						pd.AddAbortedTxn(cluster.AbortedTxnEntry{
							ProducerID:  meta.ProducerID,
							FirstOffset: firstOff,
							LastOffset:  entry.Offset,
						})
					}
				}
				delete(replayOpenTxns, replayTxnKey{entry.Partition, meta.ProducerID})
			} else {
				pd.AddOpenTxn(meta.ProducerID, entry.Offset)
				key := replayTxnKey{entry.Partition, meta.ProducerID}
				if _, exists := replayOpenTxns[key]; !exists {
					replayOpenTxns[key] = entry.Offset
				}
			}
		}

		// Append batch data to chunk pool (replaces ring buffer push).
		// Data is already offset-assigned in the WAL.
		spare = pd.AppendToChunk(entry.Data, chunk.ChunkBatch{
			BaseOffset:      entry.Offset,
			LastOffsetDelta: meta.LastOffsetDelta,
			MaxTimestamp:    meta.MaxTimestamp,
			NumRecords:      meta.NumRecords,
		}, spare)
		pd.Unlock()
		pd.ReleaseSpareChunk(spare)

		// Add to WAL index
		tp := wal.TopicPartition{TopicID: entry.TopicID, Partition: entry.Partition}
		idxEntry := wal.IndexEntry{
			BaseOffset:  entry.Offset,
			LastOffset:  lastOffset,
			SegmentSeq:  segmentSeq,
			FileOffset:  fileOffset,
			EntrySize:   int32(4 + 4 + 8 + 16 + 4 + 8 + len(entry.Data)), // length + crc + fixed payload + data
			BatchSize:   int32(len(entry.Data)),
			WALSequence: entry.Sequence,
		}
		b.walIndex.Add(tp, idxEntry)

		entryCount++
		return nil
	})
	if err != nil {
		return err
	}

	if entryCount > 0 || skippedUnknown > 0 || skippedPartition > 0 || skippedBelowLogStart > 0 || skippedCorrupt > 0 {
		b.logger.Info("WAL replay complete",
			"entries", entryCount,
			"skipped_unknown_topic", skippedUnknown,
			"skipped_invalid_partition", skippedPartition,
			"skipped_below_log_start", skippedBelowLogStart,
			"skipped_corrupt_batch", skippedCorrupt,
		)
	}
	return nil
}

// rebuildHWFromWAL scans the WAL index and advances the high watermark
// for every partition that has WAL data. This catches entries from a
// previous primary tenure that were written before the receiver was active.
func (b *Broker) rebuildHWFromWAL() {
	if b.walIndex == nil {
		return
	}
	allTP := b.walIndex.AllPartitions()
	rebuilt := 0
	for _, tp := range allTP {
		maxOff := b.walIndex.MaxOffset(tp)
		if maxOff == 0 {
			continue
		}
		td := b.state.GetTopicByID(tp.TopicID)
		if td == nil {
			b.logger.Warn("rebuildHW: topic not found in state (may indicate missed metadata entry)", "topic_id", tp.TopicID, "partition", tp.Partition)
			continue
		}
		if int(tp.Partition) >= len(td.Partitions) {
			continue
		}
		pd := td.Partitions[tp.Partition]
		pd.Lock()
		if maxOff > pd.HW() {
			b.logger.Debug("rebuildHW: advancing HW", "topic", td.Name, "partition", tp.Partition, "old_hw", pd.HW(), "new_hw", maxOff)
			pd.SetHW(maxOff)
			rebuilt++
		}
		pd.Unlock()
	}
	if rebuilt > 0 {
		b.logger.Info("rebuilt high watermarks from WAL index", "partitions", rebuilt)
	} else if len(allTP) > 0 {
		b.logger.Debug("rebuildHW: no HW changes needed", "wal_partitions", len(allTP))
	}
}

// rebuildChunksFromWAL repopulates in-memory chunks from WAL entries that are
// above the partition's S3 flush watermark. This is necessary on promotion
// because the replication receiver writes WAL entries to disk without adding
// them to chunks. Without this, the S3 flusher skips WAL-only data, creating
// a gap between what S3 has and what chunks cover.
//
// Called during onElected, before the Kafka listener or S3 flusher starts,
// so no concurrent produce or flush activity exists.
func (b *Broker) rebuildChunksFromWAL() {
	if b.walIndex == nil || b.chunkPool == nil {
		return
	}

	allTP := b.walIndex.AllPartitions()
	var rebuilt int
	for _, tp := range allTP {
		td := b.state.GetTopicByID(tp.TopicID)
		if td == nil {
			continue
		}
		if int(tp.Partition) >= len(td.Partitions) {
			continue
		}
		pd := td.Partitions[tp.Partition]

		pd.Lock()
		s3WM := pd.S3FlushWatermark()

		// Detach all existing chunks — they were populated from WAL replay
		// at startup and may cover a subset of what's now in the WAL. We
		// rebuild from scratch using the full WAL index.
		oldChunks := pd.DetachSealedChunks(true)
		pd.Unlock()
		if len(oldChunks) > 0 {
			b.chunkPool.ReleaseMany(oldChunks)
		}

		entries := b.walIndex.PartitionEntries(tp)
		var loaded int
		var skippedS3, skippedRead int
		var firstLoadedOffset, lastLoadedOffset int64
		firstLoadedOffset = -1
		// Track open txns per-producer to reconstruct abortedTxns index.
		openTxnFirstOffset := make(map[int64]int64) // producerID -> first data offset
		for _, e := range entries {
			data, err := b.walWriter.ReadBatch(e)
			if err != nil {
				skippedRead++
				b.logger.Warn("rebuildChunks: skipping unreadable WAL entry",
					"topic_id", tp.TopicID, "partition", tp.Partition,
					"base_offset", e.BaseOffset, "err", err)
				continue
			}

			meta, err := cluster.ParseBatchHeader(data)
			if err != nil {
				b.logger.Warn("rebuildChunks: skipping corrupt batch",
					"topic_id", tp.TopicID, "partition", tp.Partition,
					"base_offset", e.BaseOffset, "err", err)
				continue
			}

			// Replay dedup state for ALL entries so the PIDManager knows
			// about producers whose batches were already flushed to S3.
			if meta.ProducerID >= 0 && meta.BaseSequence >= 0 {
				ctp := cluster.TopicPartition{Topic: td.Name, Partition: int32(tp.Partition)}
				b.state.PIDManager().ReplayBatch(ctp, meta, e.BaseOffset)
			}

			// Skip chunk rebuild for entries already flushed to S3.
			if e.LastOffset < s3WM {
				skippedS3++
				continue
			}

			spare := pd.AcquireSpareChunk(len(data))
			pd.Lock()

			// Reconstruct transaction state.
			if cluster.IsTransactionalBatch(data) && meta.ProducerID >= 0 {
				if cluster.IsControlBatch(data) {
					pd.RemoveOpenTxn(meta.ProducerID)
					if !cluster.IsCommitControlBatch(data) {
						if firstOff, ok := openTxnFirstOffset[meta.ProducerID]; ok {
							pd.AddAbortedTxn(cluster.AbortedTxnEntry{
								ProducerID:  meta.ProducerID,
								FirstOffset: firstOff,
								LastOffset:  e.BaseOffset,
							})
						}
					}
					delete(openTxnFirstOffset, meta.ProducerID)
				} else {
					pd.AddOpenTxn(meta.ProducerID, e.BaseOffset)
					if _, exists := openTxnFirstOffset[meta.ProducerID]; !exists {
						openTxnFirstOffset[meta.ProducerID] = e.BaseOffset
					}
				}
			}

			spare = pd.AppendToChunk(data, chunk.ChunkBatch{
				BaseOffset:      e.BaseOffset,
				LastOffsetDelta: meta.LastOffsetDelta,
				MaxTimestamp:    meta.MaxTimestamp,
				NumRecords:      meta.NumRecords,
			}, spare)
			pd.Unlock()
			pd.ReleaseSpareChunk(spare)
			if firstLoadedOffset < 0 {
				firstLoadedOffset = e.BaseOffset
			}
			lastLoadedOffset = e.LastOffset
			loaded++
		}

		if loaded > 0 {
			rebuilt++
			b.logger.Debug("rebuildChunks: loaded WAL entries into chunks",
				"topic", td.Name, "partition", tp.Partition,
				"entries", loaded, "s3_watermark", s3WM,
				"first_offset", firstLoadedOffset, "last_offset", lastLoadedOffset,
				"skipped_s3", skippedS3, "skipped_read", skippedRead,
				"total_wal_entries", len(entries))
		}
	}

	if rebuilt > 0 {
		b.logger.Info("rebuilt chunks from WAL entries", "partitions", rebuilt)
	}
}

// abortOrphanedTransactions sweeps all partitions for open transactions that
// survived a restart. After WAL replay, any transaction still in openTxnPIDs
// is definitionally orphaned — the producer either committed/aborted (and we'd
// have a control record) or crashed. We write abort control batches for each
// orphan so LSO can advance.
func (b *Broker) abortOrphanedTransactions() {
	topics := b.state.GetAllTopics()
	var aborted int
	for _, td := range topics {
		for _, pd := range td.Partitions {
			pd.RLock()
			openPIDs := pd.OpenTxnPIDs()
			pd.RUnlock()
			if len(openPIDs) == 0 {
				continue
			}

			for pid, firstOffset := range openPIDs {
				raw := cluster.BuildControlBatch(pid, 0, false, b.cfg.Clock.Now().UnixMilli())
				meta, err := cluster.ParseBatchHeader(raw)
				if err != nil {
					b.logger.Warn("abortOrphanedTransactions: failed to parse control batch", "err", err)
					continue
				}

				spare := pd.AcquireSpareChunk(len(raw))
				pd.Lock()
				baseOffset, spare := pd.PushBatch(raw, meta, spare)
				pd.RemoveOpenTxn(pid)
				pd.AddAbortedTxn(cluster.AbortedTxnEntry{
					ProducerID:  pid,
					FirstOffset: firstOffset,
					LastOffset:  baseOffset,
				})
				pd.Unlock()
				pd.ReleaseSpareChunk(spare)

				aborted++
			}
		}
	}
	if aborted > 0 {
		b.logger.Info("aborted orphaned transactions after WAL replay", "count", aborted)
	}
}
