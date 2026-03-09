package broker

import (
	"context"
	"math"
	"strconv"
	"strings"

	"github.com/klaudworks/klite/internal/clock"
	"github.com/klaudworks/klite/internal/cluster"
	"github.com/klaudworks/klite/internal/metadata"
	s3store "github.com/klaudworks/klite/internal/s3"
)

func (b *Broker) compactionLoop(ctx context.Context) {
	interval := b.cfg.CompactionCheckInterval
	minDirty := b.cfg.CompactionMinDirtyObjects

	compactor := s3store.NewCompactor(s3store.CompactorConfig{
		Client: b.s3Client,
		Reader: b.s3Reader,
		Logger: b.logger,
		PersistWatermark: func(topic string, partition int32, cleanedUpTo int64) error {
			if b.metaLog == nil {
				return nil
			}
			entry := metadata.MarshalCompactionWatermark(&metadata.CompactionWatermarkEntry{
				TopicName:   topic,
				Partition:   partition,
				CleanedUpTo: cleanedUpTo,
			})
			return b.metaLog.AppendSync(entry)
		},
		WindowBytes:   b.cfg.CompactionWindowBytes,
		S3Concurrency: b.cfg.CompactionS3Concurrency,
		ReadRateLimit: b.cfg.CompactionReadRate,
	})

	ticker := b.cfg.Clock.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			b.logger.Debug("compaction scan tick", "interval", interval, "min_dirty_objects", minDirty)
			b.compactOneDirtyPartition(ctx, compactor, int32(minDirty))
		}
	}
}

// selectDirtyPartition picks the best partition eligible for compaction from
// the given topics. Returns nil, nil if no partition qualifies.
func selectDirtyPartition(topics []*cluster.TopicData, minDirty int32, clk clock.Clock) (*cluster.TopicData, *cluster.PartData) {
	var bestPD *cluster.PartData
	var bestTD *cluster.TopicData
	var bestDirty int32

	for _, td := range topics {
		policy, ok := td.GetConfig("cleanup.policy")
		if !ok {
			policy = "delete"
		}
		if !strings.Contains(policy, "compact") {
			continue
		}

		for _, pd := range td.Partitions {
			dirty := pd.DirtyObjects()

			// Eligibility check
			if dirty < minDirty {
				// Check staleness guarantee.
				lc := pd.LastCompacted()
				switch {
				case lc.IsZero():
					continue
				case dirty <= 0:
					continue
				default:
					// Check max.compaction.lag.ms
					maxLagMs := int64(math.MaxInt64)
					if v, ok := td.GetConfig("max.compaction.lag.ms"); ok {
						if parsed, err := strconv.ParseInt(v, 10, 64); err == nil {
							maxLagMs = parsed
						}
					}
					if maxLagMs < int64(math.MaxInt64) && clk.Since(lc).Milliseconds() > maxLagMs {
						// Stale — eligible
					} else {
						continue
					}
				}
			}

			if dirty > bestDirty || bestPD == nil {
				bestPD = pd
				bestTD = td
				bestDirty = dirty
			}
		}
	}

	return bestTD, bestPD
}

func (b *Broker) compactOneDirtyPartition(ctx context.Context, compactor *s3store.Compactor, minDirty int32) {
	topics := b.state.GetAllTopics()

	bestTD, bestPD := selectDirtyPartition(topics, minDirty, b.cfg.Clock)
	if bestPD == nil {
		b.logger.Debug("compaction scan found no eligible partitions", "min_dirty_objects", minDirty)
		return
	}
	dirtyObjects := bestPD.DirtyObjects()

	minCompactionLagMs := int64(0)
	if v, ok := bestTD.GetConfig("min.compaction.lag.ms"); ok {
		if parsed, err := strconv.ParseInt(v, 10, 64); err == nil {
			minCompactionLagMs = parsed
		}
	}

	deleteRetentionMs := int64(86400000) // 24h default
	if v, ok := bestTD.GetConfig("delete.retention.ms"); ok {
		if parsed, err := strconv.ParseInt(v, 10, 64); err == nil {
			deleteRetentionMs = parsed
		}
	}

	bestPD.RLock()
	cleanedUpTo := bestPD.CleanedUpTo()
	bestPD.RUnlock()

	newWatermark, err := compactor.CompactPartition(
		ctx,
		bestTD.Name,
		bestTD.ID,
		bestPD.Index,
		cleanedUpTo,
		minCompactionLagMs,
		deleteRetentionMs,
		func() { bestPD.CompactionMu.Lock() },
		func() { bestPD.CompactionMu.Unlock() },
	)
	if err != nil {
		b.logger.Warn("compaction failed",
			"topic", bestTD.Name, "partition", bestPD.Index, "err", err)
		return
	}

	if newWatermark > cleanedUpTo {
		bestPD.Lock()
		bestPD.SetCleanedUpTo(newWatermark)
		bestPD.ResetDirtyObjects(b.cfg.Clock.Now())
		bestPD.Unlock()
		b.logger.Info("compaction complete",
			"topic", bestTD.Name, "partition", bestPD.Index,
			"cleaned_up_to", newWatermark,
			"dirty_objects", dirtyObjects)
	}
}
