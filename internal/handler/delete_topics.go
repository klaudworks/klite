package handler

import (
	"github.com/klaudworks/klite/internal/cluster"
	"github.com/klaudworks/klite/internal/server"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// HandleDeleteTopics returns the DeleteTopics handler (API key 20).
func HandleDeleteTopics(state *cluster.State) server.Handler {
	return func(req kmsg.Request) (kmsg.Response, error) {
		r := req.(*kmsg.DeleteTopicsRequest)
		resp := r.ResponseKind().(*kmsg.DeleteTopicsResponse)

		// v0-v5: uses TopicNames field; v6+: uses Topics field with optional TopicID
		if r.Version <= 5 {
			for _, name := range r.TopicNames {
				rt := kmsg.NewDeleteTopicsRequestTopic()
				n := name
				rt.Topic = &n
				r.Topics = append(r.Topics, rt)
			}
		}

		for _, rt := range r.Topics {
			st := kmsg.NewDeleteTopicsResponseTopic()
			st.Topic = rt.Topic
			st.TopicID = rt.TopicID

			var topicName string
			if rt.Topic != nil {
				topicName = *rt.Topic
			} else {
				// Look up by topic ID
				td := state.GetTopicByID(rt.TopicID)
				if td == nil {
					st.ErrorCode = kerr.UnknownTopicID.Code
					resp.Topics = append(resp.Topics, st)
					continue
				}
				topicName = td.Name
				n := topicName
				st.Topic = &n
			}

			if topicName == "" {
				st.ErrorCode = kerr.UnknownTopicOrPartition.Code
				resp.Topics = append(resp.Topics, st)
				continue
			}

			td := state.GetTopic(topicName)
			if td == nil {
				st.ErrorCode = kerr.UnknownTopicOrPartition.Code
			} else {
				st.TopicID = td.ID
				if !state.DeleteTopic(topicName) {
					st.ErrorCode = kerr.UnknownTopicOrPartition.Code
				}
			}

			resp.Topics = append(resp.Topics, st)
		}

		return resp, nil
	}
}
