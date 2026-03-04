package handler

import (
	"net"
	"strconv"

	"github.com/klaudworks/klite/internal/cluster"
	"github.com/klaudworks/klite/internal/server"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// MetadataConfig holds the configuration needed by the Metadata handler.
type MetadataConfig struct {
	NodeID         int32
	AdvertisedAddr string // host:port
	ClusterID      string
	State          *cluster.State
}

// HandleMetadata returns the Metadata handler (API key 3).
// Supports v4-v12.
func HandleMetadata(cfg MetadataConfig) server.Handler {
	// Pre-parse the advertised address into host and port.
	advHost, advPortStr, err := net.SplitHostPort(cfg.AdvertisedAddr)
	if err != nil {
		// Fallback: use the address as host with default port.
		advHost = cfg.AdvertisedAddr
		advPortStr = "9092"
	}
	advPort, _ := strconv.Atoi(advPortStr)

	return func(req kmsg.Request) (kmsg.Response, error) {
		r := req.(*kmsg.MetadataRequest)
		resp := r.ResponseKind().(*kmsg.MetadataResponse)

		// Version validation
		minV, maxV, ok := VersionRange(3)
		if !ok || r.Version < minV || r.Version > maxV {
			// This shouldn't normally happen since dispatch checks versions,
			// but handle gracefully.
			resp.Topics = nil
			return resp, nil
		}

		// Broker list (single broker)
		broker := kmsg.NewMetadataResponseBroker()
		broker.NodeID = cfg.NodeID
		broker.Host = advHost
		broker.Port = int32(advPort)
		resp.Brokers = append(resp.Brokers, broker)

		// Cluster metadata
		clusterID := cfg.ClusterID
		resp.ClusterID = &clusterID
		resp.ControllerID = cfg.NodeID

		// Determine if auto-create is allowed for this request.
		// v4+ has AllowAutoTopicCreation field. Before v4, auto-create
		// is always enabled (if configured on the broker).
		allowAuto := cfg.State.AutoCreateEnabled()
		if r.Version >= 4 {
			allowAuto = allowAuto && r.AllowAutoTopicCreation
		}

		if r.Topics == nil {
			// Null topics array: return ALL topics
			allTopics := cfg.State.GetAllTopics()
			for _, td := range allTopics {
				resp.Topics = append(resp.Topics, buildTopicMetadata(td, cfg.NodeID, r.Version))
			}
		} else {
			// Specific topics requested
			for _, rt := range r.Topics {
				// Handle topic lookup by ID (v10+)
				if rt.TopicID != [16]byte{} {
					// Topic ID lookup - not yet supported in our simple state.
					// Return UNKNOWN_TOPIC_ID for now.
					st := kmsg.NewMetadataResponseTopic()
					st.TopicID = rt.TopicID
					st.ErrorCode = kerr.UnknownTopicID.Code
					resp.Topics = append(resp.Topics, st)
					continue
				}

				if rt.Topic == nil {
					continue
				}
				topicName := *rt.Topic

				// Validate topic name
				if errMsg := cluster.ValidateTopicName(topicName); errMsg != "" {
					st := kmsg.NewMetadataResponseTopic()
					st.Topic = kmsg.StringPtr(topicName)
					st.ErrorCode = kerr.InvalidTopicException.Code
					resp.Topics = append(resp.Topics, st)
					continue
				}

				// Look up or auto-create
				td := cfg.State.GetTopic(topicName)
				if td == nil {
					if !allowAuto {
						st := kmsg.NewMetadataResponseTopic()
						st.Topic = kmsg.StringPtr(topicName)
						st.ErrorCode = kerr.UnknownTopicOrPartition.Code
						resp.Topics = append(resp.Topics, st)
						continue
					}
					// Auto-create
					var created bool
					td, created, _ = cfg.State.GetOrCreateTopic(topicName)
					if td == nil {
						// Shouldn't happen since we checked allowAuto
						st := kmsg.NewMetadataResponseTopic()
						st.Topic = kmsg.StringPtr(topicName)
						st.ErrorCode = kerr.UnknownTopicOrPartition.Code
						resp.Topics = append(resp.Topics, st)
						continue
					}
					_ = created // auto-created
				}

				resp.Topics = append(resp.Topics, buildTopicMetadata(td, cfg.NodeID, r.Version))
			}
		}

		return resp, nil
	}
}

// buildTopicMetadata creates a MetadataResponseTopic from a TopicData.
func buildTopicMetadata(td *cluster.TopicData, nodeID int32, version int16) kmsg.MetadataResponseTopic {
	st := kmsg.NewMetadataResponseTopic()
	st.Topic = kmsg.StringPtr(td.Name)
	if version >= 10 {
		st.TopicID = td.ID
	}

	for _, pd := range td.Partitions {
		sp := kmsg.NewMetadataResponseTopicPartition()
		sp.Partition = pd.Index
		sp.Leader = nodeID
		sp.LeaderEpoch = 0 // single broker, always epoch 0
		sp.Replicas = []int32{nodeID}
		sp.ISR = []int32{nodeID}
		st.Partitions = append(st.Partitions, sp)
	}

	return st
}
