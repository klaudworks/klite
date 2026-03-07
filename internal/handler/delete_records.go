package handler

import (
	"github.com/klaudworks/klite/internal/cluster"
	"github.com/klaudworks/klite/internal/metadata"
	"github.com/klaudworks/klite/internal/server"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func HandleDeleteRecords(state *cluster.State) server.Handler {
	return func(req kmsg.Request) (kmsg.Response, error) {
		r := req.(*kmsg.DeleteRecordsRequest)
		resp := r.ResponseKind().(*kmsg.DeleteRecordsResponse)

		var metaLog *metadata.Log
		if ml := state.MetadataLog(); ml != nil {
			metaLog = ml
		}

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

				pd.RLock()
				targetOffset := rp.Offset
				if targetOffset == -1 {
					targetOffset = pd.HW()
				}
				if targetOffset < pd.LogStart() || targetOffset > pd.HW() {
					sp.ErrorCode = kerr.OffsetOutOfRange.Code
					pd.RUnlock()
					st.Partitions = append(st.Partitions, sp)
					continue
				}
				pd.RUnlock()

				// Lock ordering: compactionMu → mu
				pd.CompactionMu.Lock()
				if err := pd.AdvanceLogStartOffset(targetOffset, metaLog); err != nil {
					sp.ErrorCode = kerr.KafkaStorageError.Code
				} else {
					sp.LowWatermark = targetOffset
				}
				pd.CompactionMu.Unlock()

				st.Partitions = append(st.Partitions, sp)
			}

			resp.Topics = append(resp.Topics, st)
		}

		return resp, nil
	}
}
