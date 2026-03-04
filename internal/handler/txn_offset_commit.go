package handler

import (
	"github.com/klaudworks/klite/internal/cluster"
	"github.com/klaudworks/klite/internal/server"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// HandleTxnOffsetCommit returns the TxnOffsetCommit handler (API key 28).
// Supports v0-v4.
func HandleTxnOffsetCommit(state *cluster.State) server.Handler {
	return func(req kmsg.Request) (kmsg.Response, error) {
		r := req.(*kmsg.TxnOffsetCommitRequest)
		resp := r.ResponseKind().(*kmsg.TxnOffsetCommitResponse)

		minV, maxV, ok := VersionRange(28)
		if !ok || r.Version < minV || r.Version > maxV {
			return resp, nil
		}

		for _, rt := range r.Topics {
			st := kmsg.NewTxnOffsetCommitResponseTopic()
			st.Topic = rt.Topic

			for _, rp := range rt.Partitions {
				sp := kmsg.NewTxnOffsetCommitResponseTopicPartition()
				sp.Partition = rp.Partition

				var metaStr string
				if rp.Metadata != nil {
					metaStr = *rp.Metadata
				}

				errCode := state.PIDManager().StoreTxnOffset(
					r.ProducerID, r.ProducerEpoch,
					r.Group, rt.Topic, rp.Partition,
					rp.Offset, rp.LeaderEpoch, metaStr,
				)
				sp.ErrorCode = errCode
				st.Partitions = append(st.Partitions, sp)
			}
			resp.Topics = append(resp.Topics, st)
		}

		return resp, nil
	}
}
