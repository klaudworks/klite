package handler

import (
	"strconv"
	"time"

	"github.com/klaudworks/klite/internal/cluster"
	"github.com/klaudworks/klite/internal/server"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// HandleProduce returns the Produce handler (API key 0).
// Supports v3-11.
func HandleProduce(state *cluster.State) server.Handler {
	return func(req kmsg.Request) (kmsg.Response, error) {
		r := req.(*kmsg.ProduceRequest)

		// acks=0: process the request but do NOT send a response.
		// Returning (nil, nil) tells the dispatch system to skip the response.
		suppressResponse := r.Acks == 0

		resp := r.ResponseKind().(*kmsg.ProduceResponse)

		// Version validation
		minV, maxV, ok := VersionRange(0)
		if !ok || r.Version < minV || r.Version > maxV {
			if suppressResponse {
				return nil, nil
			}
			return resp, nil
		}

		now := time.Now().UnixMilli()

		for _, rt := range r.Topics {
			st := kmsg.NewProduceResponseTopic()
			st.Topic = rt.Topic

			// Look up or auto-create the topic
			td, _, err := state.GetOrCreateTopic(rt.Topic)
			if err != nil {
				// Topic doesn't exist and auto-create is disabled
				for _, rp := range rt.Partitions {
					sp := kmsg.NewProduceResponseTopicPartition()
					sp.Partition = rp.Partition
					sp.ErrorCode = kerr.UnknownTopicOrPartition.Code
					sp.BaseOffset = -1
					sp.LogStartOffset = -1
					st.Partitions = append(st.Partitions, sp)
				}
				resp.Topics = append(resp.Topics, st)
				continue
			}

			// Get topic configs for validation
			maxMessageBytes := getMaxMessageBytes(td)
			isLogAppendTime := getTimestampType(td) == "LogAppendTime"

			for _, rp := range rt.Partitions {
				sp := kmsg.NewProduceResponseTopicPartition()
				sp.Partition = rp.Partition
				sp.BaseOffset = -1
				sp.LogStartOffset = -1

				// Validate partition index
				if int(rp.Partition) < 0 || int(rp.Partition) >= len(td.Partitions) {
					sp.ErrorCode = kerr.UnknownTopicOrPartition.Code
					st.Partitions = append(st.Partitions, sp)
					continue
				}

				pd := td.Partitions[rp.Partition]
				raw := rp.Records

				// Step 1: Validate minimum size and parse batch header
				if len(raw) < 61 {
					sp.ErrorCode = kerr.CorruptMessage.Code
					st.Partitions = append(st.Partitions, sp)
					continue
				}

				meta, parseErr := cluster.ParseBatchHeader(raw)
				if parseErr != nil {
					sp.ErrorCode = kerr.CorruptMessage.Code
					st.Partitions = append(st.Partitions, sp)
					continue
				}

				// Step 2: Validate the batch covers the full Records slice
				// BatchLength = total batch size - 12 (BaseOffset + BatchLength fields)
				if int(meta.BatchLength) != len(raw)-12 {
					sp.ErrorCode = kerr.CorruptMessage.Code
					st.Partitions = append(st.Partitions, sp)
					continue
				}

				// Step 3: Validate batch contents (BEFORE taking partition lock)
				// 3a. Magic byte must be 2
				if meta.Magic != 2 {
					sp.ErrorCode = kerr.UnsupportedForMessageFormat.Code
					st.Partitions = append(st.Partitions, sp)
					continue
				}

				// 3b. Batch size check against max.message.bytes
				if len(raw) > maxMessageBytes {
					sp.ErrorCode = kerr.MessageTooLarge.Code
					st.Partitions = append(st.Partitions, sp)
					continue
				}

				// 3c. LogAppendTime: overwrite timestamps and recalculate CRC
				if isLogAppendTime {
					cluster.SetLogAppendTime(raw, now, &meta)
				}

				// Step 4: Append to partition under write lock
				pd.Lock()
				baseOffset := pd.PushBatch(raw, meta)
				logStart := pd.LogStart()
				pd.Unlock()

				// Notify fetch waiters after releasing the partition lock
				pd.NotifyWaiters()

				// Step 5: Populate response
				sp.BaseOffset = baseOffset
				sp.LogAppendTime = -1
				if isLogAppendTime {
					sp.LogAppendTime = now
				}
				if r.Version >= 5 {
					sp.LogStartOffset = logStart
				}
				st.Partitions = append(st.Partitions, sp)
			}

			resp.Topics = append(resp.Topics, st)
		}

		if suppressResponse {
			return nil, nil
		}

		return resp, nil
	}
}

// getMaxMessageBytes returns the max.message.bytes config value for the topic.
// Falls back to the default if not set or invalid.
func getMaxMessageBytes(td *cluster.TopicData) int {
	if v, ok := td.Configs["max.message.bytes"]; ok {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return cluster.DefaultMaxMessageBytes
}

// getTimestampType returns the message.timestamp.type config value for the topic.
// Returns "CreateTime" if not set.
func getTimestampType(td *cluster.TopicData) string {
	if v, ok := td.Configs["message.timestamp.type"]; ok {
		return v
	}
	return "CreateTime"
}
