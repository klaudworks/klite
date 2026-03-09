package handler

import (
	"log/slog"

	"github.com/klaudworks/klite/internal/chunk"
	"github.com/klaudworks/klite/internal/clock"
	"github.com/klaudworks/klite/internal/cluster"
	"github.com/klaudworks/klite/internal/server"
	"github.com/klaudworks/klite/internal/wal"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type endTxnWALWriter interface {
	AppendAsync(entry *wal.Entry, opts ...wal.AppendOpts) (<-chan error, error)
}

func HandleEndTxn(state *cluster.State, walWriter endTxnWALWriter, clk clock.Clock) server.Handler {
	return func(req kmsg.Request) (kmsg.Response, error) {
		r := req.(*kmsg.EndTxnRequest)
		resp := r.ResponseKind().(*kmsg.EndTxnResponse)

		minV, maxV, ok := VersionRange(26)
		if !ok || r.Version < minV || r.Version > maxV {
			return resp, nil
		}

		endState, errCode := state.PIDManager().PrepareEndTxn(r.ProducerID, r.ProducerEpoch, r.Commit)
		if errCode != 0 {
			resp.ErrorCode = errCode
			return resp, nil
		}

		if endState.TxnPartitions == nil {
			return resp, nil
		}

		finalized := false
		defer func() {
			if finalized {
				return
			}
			state.PIDManager().FinalizeEndTxn(endState.ProducerID, endState.Epoch, endState.Commit, false)
		}()

		controlBatch := cluster.BuildControlBatch(endState.ProducerID, endState.Epoch, endState.Commit, clk.Now().UnixMilli())

		// Phase 1: Reserve offsets and submit WAL writes for all partitions.
		type pendingControl struct {
			pd         *cluster.PartData
			baseOffset int64
			meta       cluster.BatchMeta
			stored     []byte
			errCh      <-chan error
			tp         cluster.TopicPartition
		}
		var pending []pendingControl

		for tp := range endState.TxnPartitions {
			td := state.GetTopic(tp.Topic)
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
			errCh, walErr := walWriter.AppendAsync(walEntry)
			if walErr != nil {
				pd.RollbackReserve(baseOffset)
				pd.Unlock()
				slog.Error("EndTxn WAL submit failed",
					"topic", tp.Topic, "partition", tp.Partition, "err", walErr)
				resp.ErrorCode = kerr.KafkaStorageError.Code
				return resp, nil
			}
			pd.Unlock()

			pending = append(pending, pendingControl{
				pd:         pd,
				baseOffset: baseOffset,
				meta:       meta,
				stored:     raw,
				errCh:      errCh,
				tp:         tp,
			})
		}

		// Phase 2: Wait for WAL fsync for every partition first.
		walFailed := false
		for i := range pending {
			pc := &pending[i]

			walErr := <-pc.errCh
			if walErr != nil {
				slog.Error("EndTxn WAL write failed",
					"topic", pc.tp.Topic, "partition", pc.tp.Partition,
					"baseOffset", pc.baseOffset, "err", walErr)
				walFailed = true
			}
		}

		if walFailed {
			for i := range pending {
				pc := &pending[i]
				pc.pd.Lock()
				pc.pd.SkipOffsets(pc.baseOffset, int64(pc.meta.LastOffsetDelta)+1)
				pc.pd.Unlock()
			}
			resp.ErrorCode = kerr.KafkaStorageError.Code
			return resp, nil
		}

		// Phase 3: WAL is durable for all partitions; now commit to chunks.
		for i := range pending {
			pc := &pending[i]

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
			if !endState.Commit {
				if firstOffset, ok := endState.TxnFirstOffsets[pc.tp]; ok {
					pc.pd.AddAbortedTxn(cluster.AbortedTxnEntry{
						ProducerID:  endState.ProducerID,
						FirstOffset: firstOffset,
						LastOffset:  pc.baseOffset,
					})
				}
			}
			pc.pd.Unlock()
			pc.pd.ReleaseSpareChunk(spare)
			pc.pd.NotifyWaiters()
		}

		if endState.Commit {
			for groupID, offsets := range endState.TxnOffsets {
				g := state.GetOrCreateGroup(groupID)
				g.Control(func() {
					for tp, po := range offsets {
						g.ApplyTxnOffset(tp, po)
					}
				})
			}
		}

		if state.PIDManager().FinalizeEndTxn(endState.ProducerID, endState.Epoch, endState.Commit, resp.ErrorCode == 0) != 0 {
			resp.ErrorCode = kerr.InvalidTxnState.Code
			return resp, nil
		}
		finalized = true

		return resp, nil
	}
}
