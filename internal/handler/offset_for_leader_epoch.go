package handler

import (
	"github.com/klaudworks/klite/internal/cluster"
	"github.com/klaudworks/klite/internal/server"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func HandleOffsetForLeaderEpoch(state *cluster.State) server.Handler {
	return func(req kmsg.Request) (kmsg.Response, error) {
		r := req.(*kmsg.OffsetForLeaderEpochRequest)
		resp := r.ResponseKind().(*kmsg.OffsetForLeaderEpochResponse)

		for _, rt := range r.Topics {
			st := kmsg.NewOffsetForLeaderEpochResponseTopic()
			st.Topic = rt.Topic

			td := state.GetTopic(rt.Topic)
			for _, rp := range rt.Partitions {
				sp := kmsg.NewOffsetForLeaderEpochResponseTopicPartition()
				sp.Partition = rp.Partition

				// Only consumer requests are supported (ReplicaID < 0).
				// ReplicaID -1 = normal consumer, -2 = debug consumer (kadm default).
				// ReplicaID >= 0 = inter-broker replication, not supported.
				if r.ReplicaID >= 0 {
					sp.ErrorCode = kerr.UnknownServerError.Code
					st.Partitions = append(st.Partitions, sp)
					continue
				}

				if td == nil {
					sp.ErrorCode = kerr.UnknownTopicOrPartition.Code
					st.Partitions = append(st.Partitions, sp)
					continue
				}

				if int(rp.Partition) >= len(td.Partitions) {
					sp.ErrorCode = kerr.UnknownTopicOrPartition.Code
					st.Partitions = append(st.Partitions, sp)
					continue
				}

				pd := td.Partitions[rp.Partition]
				pd.RLock()

				// Pre-WAL: epoch is always 0
				const currentEpoch int32 = 0

				if rp.CurrentLeaderEpoch > currentEpoch {
					sp.ErrorCode = kerr.UnknownLeaderEpoch.Code
					pd.RUnlock()
					st.Partitions = append(st.Partitions, sp)
					continue
				}

				// For single-broker pre-WAL, all requests for epoch 0
				// return the current HW. Future epochs return -1/-1.
				switch {
				case rp.LeaderEpoch == currentEpoch:
					sp.LeaderEpoch = currentEpoch
					sp.EndOffset = pd.HW()
				case rp.LeaderEpoch > currentEpoch:
					sp.LeaderEpoch = -1
					sp.EndOffset = -1
				default:
					sp.LeaderEpoch = currentEpoch
					sp.EndOffset = pd.HW()
				}

				pd.RUnlock()
				st.Partitions = append(st.Partitions, sp)
			}

			resp.Topics = append(resp.Topics, st)
		}

		return resp, nil
	}
}
