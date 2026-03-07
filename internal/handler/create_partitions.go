package handler

import (
	"github.com/klaudworks/klite/internal/cluster"
	"github.com/klaudworks/klite/internal/server"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func HandleCreatePartitions(state *cluster.State) server.Handler {
	return func(req kmsg.Request) (kmsg.Response, error) {
		r := req.(*kmsg.CreatePartitionsRequest)
		resp := r.ResponseKind().(*kmsg.CreatePartitionsResponse)

		for _, rt := range r.Topics {
			st := kmsg.NewCreatePartitionsResponseTopic()
			st.Topic = rt.Topic

			td := state.GetTopic(rt.Topic)
			if td == nil {
				st.ErrorCode = kerr.UnknownTopicOrPartition.Code
				resp.Topics = append(resp.Topics, st)
				continue
			}

			currentCount := int32(len(td.Partitions))
			if rt.Count <= currentCount {
				st.ErrorCode = kerr.InvalidPartitions.Code
				resp.Topics = append(resp.Topics, st)
				continue
			}

			if !r.ValidateOnly {
				state.AddPartitions(rt.Topic, int(rt.Count))
			}

			resp.Topics = append(resp.Topics, st)
		}

		return resp, nil
	}
}
