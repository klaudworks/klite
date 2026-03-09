package broker

import (
	"context"
	"time"

	"github.com/klaudworks/klite/internal/chunk"
	"github.com/klaudworks/klite/internal/cluster"
	"github.com/klaudworks/klite/internal/wal"
)

// txnTimeoutLoop periodically checks for expired transactions and writes abort
// control batches for them. This handles the case where a transactional producer
// crashes mid-transaction — without this sweep, the open transaction blocks LSO
// advancement indefinitely.
func (b *Broker) txnTimeoutLoop(ctx context.Context) {
	ticker := b.cfg.Clock.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			b.abortExpiredTransactions()
		}
	}
}

func (b *Broker) abortExpiredTransactions() {
	expired := b.state.PIDManager().ExpiredTransactions()
	for _, snap := range expired {
		endState, errCode := b.state.PIDManager().PrepareEndTxn(snap.ProducerID, snap.Epoch, false)
		if errCode != 0 {
			continue
		}
		if endState.TxnPartitions == nil {
			continue
		}

		controlBatch := cluster.BuildControlBatch(endState.ProducerID, endState.Epoch, false, b.cfg.Clock.Now().UnixMilli())

		// Two-phase WAL write: submit all entries first, then commit after fsync.
		type pendingAbort struct {
			pd         *cluster.PartData
			baseOffset int64
			meta       cluster.BatchMeta
			stored     []byte
			errCh      <-chan error
			tp         cluster.TopicPartition
		}
		var pending []pendingAbort

		for tp := range endState.TxnPartitions {
			td := b.state.GetTopic(tp.Topic)
			if td == nil || int(tp.Partition) >= len(td.Partitions) {
				continue
			}
			pd := td.Partitions[tp.Partition]

			meta, err := cluster.ParseBatchHeader(controlBatch)
			if err != nil {
				continue
			}

			raw := make([]byte, len(controlBatch))
			copy(raw, controlBatch)

			pd.Lock()
			baseOffset := pd.ReserveOffset(meta)
			cluster.AssignOffset(raw, baseOffset)

			walEntry := &wal.Entry{
				TopicID:   pd.TopicID,
				Partition: pd.Index,
				Offset:    baseOffset,
				Data:      raw,
			}
			errCh, walErr := b.walWriter.AppendAsync(walEntry)
			if walErr != nil {
				pd.RollbackReserve(baseOffset)
				pd.Unlock()
				b.logger.Error("abortExpiredTransactions: WAL submit failed",
					"topic", tp.Topic, "partition", tp.Partition, "err", walErr)
				continue
			}
			pd.Unlock()

			pending = append(pending, pendingAbort{
				pd:         pd,
				baseOffset: baseOffset,
				meta:       meta,
				stored:     raw,
				errCh:      errCh,
				tp:         tp,
			})
		}

		for i := range pending {
			pc := &pending[i]

			walErr := <-pc.errCh
			if walErr != nil {
				b.logger.Error("abortExpiredTransactions: WAL write failed",
					"topic", pc.tp.Topic, "partition", pc.tp.Partition, "err", walErr)
				pc.pd.Lock()
				pc.pd.SkipOffsets(pc.baseOffset, int64(pc.meta.LastOffsetDelta)+1)
				pc.pd.RemoveOpenTxn(endState.ProducerID)
				pc.pd.Unlock()
				continue
			}

			spare := pc.pd.AcquireSpareChunk(len(pc.stored))
			pc.pd.Lock()
			spare = pc.pd.AppendToChunk(pc.stored, chunk.ChunkBatch{
				BaseOffset:      pc.baseOffset,
				LastOffsetDelta: pc.meta.LastOffsetDelta,
				MaxTimestamp:    pc.meta.MaxTimestamp,
				NumRecords:      pc.meta.NumRecords,
			}, spare)
			pc.pd.CommitBatch(cluster.StoredBatch{
				BaseOffset:      pc.baseOffset,
				LastOffsetDelta: pc.meta.LastOffsetDelta,
				RawBytes:        pc.stored,
				MaxTimestamp:    pc.meta.MaxTimestamp,
				NumRecords:      pc.meta.NumRecords,
			})
			pc.pd.RemoveOpenTxn(endState.ProducerID)
			if firstOffset, ok := endState.TxnFirstOffsets[pc.tp]; ok {
				pc.pd.AddAbortedTxn(cluster.AbortedTxnEntry{
					ProducerID:  endState.ProducerID,
					FirstOffset: firstOffset,
					LastOffset:  pc.baseOffset,
				})
			}
			pc.pd.Unlock()
			pc.pd.ReleaseSpareChunk(spare)
			pc.pd.NotifyWaiters()
		}

		b.logger.Info("aborted expired transaction",
			"producer_id", snap.ProducerID,
			"txn_id", snap.TxnID,
			"partitions", len(endState.TxnPartitions))
	}
}
