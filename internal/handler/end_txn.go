package handler

import (
	"github.com/klaudworks/klite/internal/cluster"
	"github.com/klaudworks/klite/internal/server"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// HandleEndTxn returns the EndTxn handler (API key 26).
// Supports v0-v4.
func HandleEndTxn(state *cluster.State) server.Handler {
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
			// Retry case or empty transaction
			return resp, nil
		}

		// Write control batches to each partition in the transaction
		controlBatch := cluster.BuildControlBatch(endState.ProducerID, endState.Epoch, endState.Commit)

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

			// Make a copy for each partition
			raw := make([]byte, len(controlBatch))
			copy(raw, controlBatch)

			pd.Lock()
			baseOffset := pd.PushBatch(raw, meta)
			// Remove open txn tracking
			pd.RemoveOpenTxn(endState.ProducerID)
			// If aborting, record the aborted transaction index entry
			if !endState.Commit {
				if firstOffset, ok := endState.TxnFirstOffsets[tp]; ok {
					pd.AddAbortedTxn(cluster.AbortedTxnEntry{
						ProducerID:  endState.ProducerID,
						FirstOffset: firstOffset,
						LastOffset:  baseOffset,
					})
				}
			}
			pd.Unlock()
			pd.NotifyWaiters()
		}

		// Apply offset commits if committing
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

		return resp, nil
	}
}
