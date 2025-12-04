package handler

import (
	"github.com/klaudworks/klite/internal/cluster"
	"github.com/klaudworks/klite/internal/server"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// HandleIncrementalAlterConfigs returns the IncrementalAlterConfigs handler (API key 44).
func HandleIncrementalAlterConfigs(state *cluster.State) server.Handler {
	return func(req kmsg.Request) (kmsg.Response, error) {
		r := req.(*kmsg.IncrementalAlterConfigsRequest)
		resp := r.ResponseKind().(*kmsg.IncrementalAlterConfigsResponse)

		for i := range r.Resources {
			rr := &r.Resources[i]
			st := kmsg.NewIncrementalAlterConfigsResponseResource()
			st.ResourceName = rr.ResourceName
			st.ResourceType = rr.ResourceType

			switch rr.ResourceType {
			case kmsg.ConfigResourceTypeTopic:
				td := state.GetTopic(rr.ResourceName)
				if td == nil {
					st.ErrorCode = kerr.UnknownTopicOrPartition.Code
					resp.Resources = append(resp.Resources, st)
					continue
				}

				if !r.ValidateOnly {
					for j := range rr.Configs {
						rc := &rr.Configs[j]
						switch rc.Op {
						case kmsg.IncrementalAlterConfigOpSet:
							if rc.Value != nil {
								state.SetTopicConfig(rr.ResourceName, rc.Name, *rc.Value)
							}
						case kmsg.IncrementalAlterConfigOpDelete:
							state.DeleteTopicConfig(rr.ResourceName, rc.Name)
						}
					}
				}

			case kmsg.ConfigResourceTypeBroker:
				// Broker config changes are accepted but no-op for now

			default:
				st.ErrorCode = kerr.InvalidRequest.Code
			}

			resp.Resources = append(resp.Resources, st)
		}

		return resp, nil
	}
}

// HandleAlterConfigs returns the AlterConfigs handler (API key 33).
// This is the legacy non-incremental API — it replaces all configs.
func HandleAlterConfigs(state *cluster.State) server.Handler {
	return func(req kmsg.Request) (kmsg.Response, error) {
		r := req.(*kmsg.AlterConfigsRequest)
		resp := r.ResponseKind().(*kmsg.AlterConfigsResponse)

		for i := range r.Resources {
			rr := &r.Resources[i]
			st := kmsg.NewAlterConfigsResponseResource()
			st.ResourceName = rr.ResourceName
			st.ResourceType = rr.ResourceType

			switch rr.ResourceType {
			case kmsg.ConfigResourceTypeTopic:
				td := state.GetTopic(rr.ResourceName)
				if td == nil {
					st.ErrorCode = kerr.UnknownTopicOrPartition.Code
					resp.Resources = append(resp.Resources, st)
					continue
				}

				if !r.ValidateOnly {
					// Full replacement: clear existing configs, apply new ones
					state.ReplaceTopicConfigs(rr.ResourceName, rr.Configs)
				}

			case kmsg.ConfigResourceTypeBroker:
				// Accepted but no-op

			default:
				st.ErrorCode = kerr.InvalidRequest.Code
			}

			resp.Resources = append(resp.Resources, st)
		}

		return resp, nil
	}
}
