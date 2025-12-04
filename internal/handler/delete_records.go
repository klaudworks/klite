package handler

import (
	"github.com/klaudworks/klite/internal/cluster"
	"github.com/klaudworks/klite/internal/server"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// HandleDeleteRecords returns the DeleteRecords handler (API key 21).
func HandleDeleteRecords(state *cluster.State) server.Handler {
	return func(req kmsg.Request) (kmsg.Response, error) {
		r := req.(*kmsg.DeleteRecordsRequest)
		resp := r.ResponseKind().(*kmsg.DeleteRecordsResponse)

		for _, rt := range r.Topics {
			st := kmsg.NewDeleteRecordsResponseTopic()
			st.Topic = rt.Topic

			td := state.GetTopic(rt.Topic)
			for _, rp := range rt.Partitions {
				sp := kmsg.NewDeleteRecordsResponseTopicPartition()
				sp.Partition = rp.Partition

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
				pd.Lock()

				targetOffset := rp.Offset
				if targetOffset == -1 {
					targetOffset = pd.HW()
				}

				if targetOffset < pd.LogStart() || targetOffset > pd.HW() {
					sp.ErrorCode = kerr.OffsetOutOfRange.Code
					pd.Unlock()
					st.Partitions = append(st.Partitions, sp)
					continue
				}

				pd.AdvanceLogStart(targetOffset)
				sp.LowWatermark = targetOffset

				pd.Unlock()
				st.Partitions = append(st.Partitions, sp)
			}

			resp.Topics = append(resp.Topics, st)
		}

		return resp, nil
	}
}
