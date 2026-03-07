package handler

import (
	"github.com/klaudworks/klite/internal/cluster"
	"github.com/klaudworks/klite/internal/server"
	"github.com/twmb/franz-go/pkg/kmsg"
)

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

			raw := make([]byte, len(controlBatch))
			copy(raw, controlBatch)

			spare := pd.AcquireSpareChunk(len(raw))
			pd.Lock()
			baseOffset, spare := pd.PushBatch(raw, meta, spare)
			pd.RemoveOpenTxn(endState.ProducerID)
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
			pd.ReleaseSpareChunk(spare)
			pd.NotifyWaiters()
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

		return resp, nil
	}
}
