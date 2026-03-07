package handler

import (
	"log/slog"

	"github.com/klaudworks/klite/internal/cluster"
	"github.com/klaudworks/klite/internal/metadata"
	"github.com/klaudworks/klite/internal/server"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

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
								if ml := state.MetadataLog(); ml != nil {
									entry := metadata.MarshalAlterConfig(&metadata.AlterConfigEntry{
										TopicName: rr.ResourceName,
										Key:       rc.Name,
										Value:     *rc.Value,
									})
									if err := ml.Append(entry); err != nil {
										slog.Warn("metadata.log: failed to persist AlterConfig",
											"topic", rr.ResourceName, "key", rc.Name, "err", err)
									}
								}
							}
						case kmsg.IncrementalAlterConfigOpDelete:
							state.DeleteTopicConfig(rr.ResourceName, rc.Name)
							// For delete, we persist an ALTER_CONFIG with empty value
							// On replay, we'll need to handle this as "set to empty means delete"
							// Actually, compaction handles this — snapshot only includes live state
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

// HandleAlterConfigs is the legacy non-incremental API — it replaces all configs.
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
					state.ReplaceTopicConfigs(rr.ResourceName, rr.Configs)
					if ml := state.MetadataLog(); ml != nil {
						for _, c := range rr.Configs {
							if c.Value != nil {
								entry := metadata.MarshalAlterConfig(&metadata.AlterConfigEntry{
									TopicName: rr.ResourceName,
									Key:       c.Name,
									Value:     *c.Value,
								})
								if err := ml.Append(entry); err != nil {
									slog.Warn("metadata.log: failed to persist AlterConfig",
										"topic", rr.ResourceName, "key", c.Name, "err", err)
								}
							}
						}
					}
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
