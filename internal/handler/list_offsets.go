package handler

import (
	"github.com/klaudworks/klite/internal/cluster"
	"github.com/klaudworks/klite/internal/server"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// HandleListOffsets returns the ListOffsets handler (API key 2).
// Supports v1-8. IsolationLevel (v2+) controls whether Latest (-1) returns
// HW (read_uncommitted) or LSO (read_committed).
func HandleListOffsets(state *cluster.State) server.Handler {
	return func(req kmsg.Request) (kmsg.Response, error) {
		r := req.(*kmsg.ListOffsetsRequest)
		resp := r.ResponseKind().(*kmsg.ListOffsetsResponse)

		// Version validation
		minV, maxV, ok := VersionRange(2)
		if !ok || r.Version < minV || r.Version > maxV {
			return resp, nil
		}

		for _, rt := range r.Topics {
			st := kmsg.NewListOffsetsResponseTopic()
			st.Topic = rt.Topic

			td := state.GetTopic(rt.Topic)

			for _, rp := range rt.Partitions {
				sp := kmsg.NewListOffsetsResponseTopicPartition()
				sp.Partition = rp.Partition

				if td == nil {
					sp.ErrorCode = kerr.UnknownTopicOrPartition.Code
					sp.Offset = -1
					sp.Timestamp = -1
					st.Partitions = append(st.Partitions, sp)
					continue
				}

				if int(rp.Partition) < 0 || int(rp.Partition) >= len(td.Partitions) {
					sp.ErrorCode = kerr.UnknownTopicOrPartition.Code
					sp.Offset = -1
					sp.Timestamp = -1
					st.Partitions = append(st.Partitions, sp)
					continue
				}

				pd := td.Partitions[rp.Partition]

				pd.RLock()
				offset, ts := pd.ListOffsets(rp.Timestamp, r.IsolationLevel)
				sp.LeaderEpoch = 0 // single broker, always epoch 0
				pd.RUnlock()

				sp.Offset = offset
				sp.Timestamp = ts
				st.Partitions = append(st.Partitions, sp)
			}

			resp.Topics = append(resp.Topics, st)
		}

		return resp, nil
	}
}
