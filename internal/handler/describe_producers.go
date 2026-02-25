package handler

import (
	"github.com/klaudworks/klite/internal/cluster"
	"github.com/klaudworks/klite/internal/server"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// HandleDescribeProducers returns the DescribeProducers handler (API key 61).
// Supports v0.
func HandleDescribeProducers(state *cluster.State) server.Handler {
	return func(req kmsg.Request) (kmsg.Response, error) {
		r := req.(*kmsg.DescribeProducersRequest)
		resp := r.ResponseKind().(*kmsg.DescribeProducersResponse)

		for _, rt := range r.Topics {
			st := kmsg.NewDescribeProducersResponseTopic()
			st.Topic = rt.Topic

			td := state.GetTopic(rt.Topic)

			for _, p := range rt.Partitions {
				sp := kmsg.NewDescribeProducersResponseTopicPartition()
				sp.Partition = p

				if td == nil || int(p) < 0 || int(p) >= len(td.Partitions) {
					sp.ErrorCode = kerr.UnknownTopicOrPartition.Code
					st.Partitions = append(st.Partitions, sp)
					continue
				}

				// Get producers that have written to this partition
				snapshots := state.PIDManager().GetProducersForPartition(rt.Topic, p)
				for _, snap := range snapshots {
					active := kmsg.NewDescribeProducersResponseTopicPartitionActiveProducer()
					active.ProducerID = snap.ProducerID
					active.ProducerEpoch = int32(snap.Epoch)
					active.CoordinatorEpoch = -1
					active.LastSequence = int32(snap.LastSequence)
					active.CurrentTxnStartOffset = snap.TxnStartOffset

					if !snap.TxnStartTime.IsZero() {
						active.LastTimestamp = snap.TxnStartTime.UnixMilli()
					} else {
						active.LastTimestamp = -1
					}

					sp.ActiveProducers = append(sp.ActiveProducers, active)
				}
				st.Partitions = append(st.Partitions, sp)
			}
			resp.Topics = append(resp.Topics, st)
		}

		return resp, nil
	}
}
