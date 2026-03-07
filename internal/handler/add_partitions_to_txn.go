package handler

import (
	"github.com/klaudworks/klite/internal/cluster"
	"github.com/klaudworks/klite/internal/server"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func HandleAddPartitionsToTxn(state *cluster.State) server.Handler {
	return func(req kmsg.Request) (kmsg.Response, error) {
		r := req.(*kmsg.AddPartitionsToTxnRequest)
		resp := r.ResponseKind().(*kmsg.AddPartitionsToTxnResponse)

		minV, maxV, ok := VersionRange(24)
		if !ok || r.Version < minV || r.Version > maxV {
			return resp, nil
		}

		var validPartitions []cluster.TopicPartition
		for _, rt := range r.Topics {
			td := state.GetTopic(rt.Topic)
			for _, p := range rt.Partitions {
				tp := cluster.TopicPartition{Topic: rt.Topic, Partition: p}
				if td == nil || int(p) < 0 || int(p) >= len(td.Partitions) {
					// Will be reported as error below
					continue
				}
				validPartitions = append(validPartitions, tp)
			}
		}

		errCode := state.PIDManager().AddPartitionsToTxn(r.ProducerID, r.ProducerEpoch, validPartitions)

		for _, rt := range r.Topics {
			st := kmsg.NewAddPartitionsToTxnResponseTopic()
			st.Topic = rt.Topic

			td := state.GetTopic(rt.Topic)
			for _, p := range rt.Partitions {
				sp := kmsg.NewAddPartitionsToTxnResponseTopicPartition()
				sp.Partition = p

				if td == nil || int(p) < 0 || int(p) >= len(td.Partitions) {
					sp.ErrorCode = kerr.UnknownTopicOrPartition.Code
				} else if errCode != 0 {
					sp.ErrorCode = errCode
				}
				st.Partitions = append(st.Partitions, sp)
			}
			resp.Topics = append(resp.Topics, st)
		}

		return resp, nil
	}
}
