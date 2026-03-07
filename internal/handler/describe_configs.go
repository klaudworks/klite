package handler

import (
	"fmt"

	"github.com/klaudworks/klite/internal/cluster"
	"github.com/klaudworks/klite/internal/server"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// topicConfigDefaults are the default values for supported topic configs.
var topicConfigDefaults = map[string]string{
	"cleanup.policy":         "delete",
	"compression.type":       "producer",
	"retention.ms":           "604800000",
	"retention.bytes":        "-1",
	"max.message.bytes":      "1048588",
	"segment.bytes":          "67108864",
	"message.timestamp.type": "CreateTime",
	"min.compaction.lag.ms":  "0",
	"max.compaction.lag.ms":  "9223372036854775807",
	"delete.retention.ms":    "86400000",
}

// DescribeConfigsConfig holds config for the DescribeConfigs handler.
type DescribeConfigsConfig struct {
	NodeID int32
	State  *cluster.State
}

func HandleDescribeConfigs(cfg DescribeConfigsConfig) server.Handler {
	return func(req kmsg.Request) (kmsg.Response, error) {
		r := req.(*kmsg.DescribeConfigsRequest)
		resp := r.ResponseKind().(*kmsg.DescribeConfigsResponse)

		for i := range r.Resources {
			rr := &r.Resources[i]
			st := kmsg.NewDescribeConfigsResponseResource()
			st.ResourceName = rr.ResourceName
			st.ResourceType = rr.ResourceType

			switch rr.ResourceType {
			case kmsg.ConfigResourceTypeBroker:
				addBrokerConfig := func(name, value string, src kmsg.ConfigSource) {
					rc := kmsg.NewDescribeConfigsResponseResourceConfig()
					rc.Name = name
					v := value
					rc.Value = &v
					rc.Source = src
					rc.IsDefault = src == kmsg.ConfigSourceDefaultConfig
					rc.ReadOnly = src == kmsg.ConfigSourceStaticBrokerConfig
					st.Configs = append(st.Configs, rc)
				}
				nodeStr := fmt.Sprintf("%d", cfg.NodeID)
				addBrokerConfig("broker.id", nodeStr, kmsg.ConfigSourceStaticBrokerConfig)

			case kmsg.ConfigResourceTypeTopic:
				td := cfg.State.GetTopic(rr.ResourceName)
				if td == nil {
					st.ErrorCode = kerr.UnknownTopicOrPartition.Code
					resp.Resources = append(resp.Resources, st)
					continue
				}

				for name, defaultVal := range topicConfigDefaults {
					rc := kmsg.NewDescribeConfigsResponseResourceConfig()
					rc.Name = name
					if override, ok := td.Configs[name]; ok {
						v := override
						rc.Value = &v
						rc.Source = kmsg.ConfigSourceDynamicTopicConfig
						rc.IsDefault = false
					} else {
						v := defaultVal
						rc.Value = &v
						rc.Source = kmsg.ConfigSourceDefaultConfig
						rc.IsDefault = true
					}
					st.Configs = append(st.Configs, rc)
				}

			default:
				st.ErrorCode = kerr.InvalidRequest.Code
			}

			if len(rr.ConfigNames) > 0 {
				names := make(map[string]struct{})
				for _, n := range rr.ConfigNames {
					names[n] = struct{}{}
				}
				keep := st.Configs[:0]
				for _, rc := range st.Configs {
					if _, ok := names[rc.Name]; ok {
						keep = append(keep, rc)
					}
				}
				st.Configs = keep
			}

			resp.Resources = append(resp.Resources, st)
		}

		return resp, nil
	}
}
