package handler

import (
	"github.com/klaudworks/klite/internal/cluster"
	"github.com/klaudworks/klite/internal/server"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// validTopicConfigs lists all topic-level config keys this broker supports.
// Unknown keys are rejected with INVALID_CONFIG.
var validTopicConfigs = map[string]bool{
	"cleanup.policy":         true,
	"compression.type":       true,
	"retention.ms":           true,
	"retention.bytes":        true,
	"max.message.bytes":      true,
	"segment.bytes":          true,
	"message.timestamp.type": true,
}

// HandleCreateTopics returns the CreateTopics handler (API key 19).
// Supports v2-v7.
func HandleCreateTopics(state *cluster.State) server.Handler {
	return func(req kmsg.Request) (kmsg.Response, error) {
		r := req.(*kmsg.CreateTopicsRequest)
		resp := r.ResponseKind().(*kmsg.CreateTopicsResponse)

		// Version validation
		minV, maxV, ok := VersionRange(19)
		if !ok || r.Version < minV || r.Version > maxV {
			return resp, nil
		}

		// Check for duplicates in the request
		uniq := make(map[string]struct{}, len(r.Topics))
		for _, rt := range r.Topics {
			if _, dup := uniq[rt.Topic]; dup {
				// Duplicate topic in same request — reject all
				for _, rt2 := range r.Topics {
					st := kmsg.NewCreateTopicsResponseTopic()
					st.Topic = rt2.Topic
					st.ErrorCode = kerr.InvalidRequest.Code
					resp.Topics = append(resp.Topics, st)
				}
				return resp, nil
			}
			uniq[rt.Topic] = struct{}{}
		}

		// Build normalized name map for collision detection within this request
		normalizedInReq := make(map[string]string, len(r.Topics))
		for _, rt := range r.Topics {
			normalizedInReq[cluster.NormalizeTopicName(rt.Topic)] = rt.Topic
		}

		for _, rt := range r.Topics {
			st := kmsg.NewCreateTopicsResponseTopic()
			st.Topic = rt.Topic

			// 1. Validate topic name
			if errMsg := cluster.ValidateTopicName(rt.Topic); errMsg != "" {
				st.ErrorCode = kerr.InvalidTopicException.Code
				resp.Topics = append(resp.Topics, st)
				continue
			}

			// 2. Check if topic already exists
			if state.TopicExists(rt.Topic) {
				st.ErrorCode = kerr.TopicAlreadyExists.Code
				resp.Topics = append(resp.Topics, st)
				continue
			}

			// 3. Check collision with existing topics (dot/underscore normalization)
			if colliding := state.CheckTopicCollision(rt.Topic); colliding != "" {
				st.ErrorCode = kerr.InvalidTopicException.Code
				resp.Topics = append(resp.Topics, st)
				continue
			}

			// 4. Check collision within the request itself
			normalized := cluster.NormalizeTopicName(rt.Topic)
			if orig := normalizedInReq[normalized]; orig != rt.Topic {
				st.ErrorCode = kerr.InvalidTopicException.Code
				resp.Topics = append(resp.Topics, st)
				continue
			}

			// 5. Determine partition count
			var numPartitions int
			if len(rt.ReplicaAssignment) > 0 {
				// ReplicaAssignment provided: NumPartitions and ReplicationFactor must be -1
				if rt.NumPartitions != -1 || rt.ReplicationFactor != -1 {
					st.ErrorCode = kerr.InvalidRequest.Code
					resp.Topics = append(resp.Topics, st)
					continue
				}
				// Validate: consecutive 0-based partition IDs, non-empty replicas
				valid := true
				ids := make(map[int32]struct{}, len(rt.ReplicaAssignment))
				for _, ra := range rt.ReplicaAssignment {
					if _, dup := ids[ra.Partition]; dup {
						valid = false
						break
					}
					ids[ra.Partition] = struct{}{}
					if len(ra.Replicas) == 0 {
						valid = false
						break
					}
				}
				if valid {
					for i := int32(0); i < int32(len(rt.ReplicaAssignment)); i++ {
						if _, ok := ids[i]; !ok {
							valid = false
							break
						}
					}
				}
				if !valid {
					st.ErrorCode = kerr.InvalidReplicaAssignment.Code
					resp.Topics = append(resp.Topics, st)
					continue
				}
				numPartitions = len(rt.ReplicaAssignment)
			} else {
				// No ReplicaAssignment
				if rt.NumPartitions == 0 {
					st.ErrorCode = kerr.InvalidPartitions.Code
					resp.Topics = append(resp.Topics, st)
					continue
				}
				numPartitions = int(rt.NumPartitions)
				if numPartitions < 0 {
					numPartitions = state.DefaultPartitions()
				}
			}

			// 6. Parse and validate configs
			configs := make(map[string]string)
			configInvalid := false
			for _, c := range rt.Configs {
				if !validTopicConfigs[c.Name] {
					st.ErrorCode = kerr.InvalidConfig.Code
					configInvalid = true
					break
				}
				if c.Value != nil {
					configs[c.Name] = *c.Value
				}
			}
			if configInvalid {
				resp.Topics = append(resp.Topics, st)
				continue
			}

			// 7. ValidateOnly: don't actually create
			if r.ValidateOnly {
				st.NumPartitions = int32(numPartitions)
				st.ReplicationFactor = 1 // always 1 for single broker
				for k, v := range configs {
					rc := kmsg.NewCreateTopicsResponseTopicConfig()
					rc.Name = k
					vCopy := v
					rc.Value = &vCopy
					st.Configs = append(st.Configs, rc)
				}
				resp.Topics = append(resp.Topics, st)
				continue
			}

			// 8. Create the topic
			td, created := state.CreateTopicWithConfigs(rt.Topic, numPartitions, configs)
			if !created {
				// Race: topic was created between our check and create call
				st.ErrorCode = kerr.TopicAlreadyExists.Code
				resp.Topics = append(resp.Topics, st)
				continue
			}

			// 9. Populate response
			st.TopicID = td.ID
			st.NumPartitions = int32(numPartitions)
			st.ReplicationFactor = 1 // always 1 for single broker
			for k, v := range configs {
				rc := kmsg.NewCreateTopicsResponseTopicConfig()
				rc.Name = k
				vCopy := v
				rc.Value = &vCopy
				st.Configs = append(st.Configs, rc)
			}
			resp.Topics = append(resp.Topics, st)
		}

		return resp, nil
	}
}
