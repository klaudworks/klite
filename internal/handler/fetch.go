package handler

import (
	"github.com/klaudworks/klite/internal/cluster"
	"github.com/klaudworks/klite/internal/server"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// HandleFetch returns the Fetch handler (API key 1).
// Supports v4-16.
//
// This is a minimal implementation for Phase 1 that serves available data
// without long-polling. Task 09-fetch will add long-polling, size limits,
// fetch sessions, and full KIP-74 behavior.
func HandleFetch(state *cluster.State, shutdownCh <-chan struct{}) server.Handler {
	return func(req kmsg.Request) (kmsg.Response, error) {
		r := req.(*kmsg.FetchRequest)
		resp := r.ResponseKind().(*kmsg.FetchResponse)

		// Version validation
		minV, maxV, ok := VersionRange(1)
		if !ok || r.Version < minV || r.Version > maxV {
			return resp, nil
		}

		// Fetch sessions: return FETCH_SESSION_ID_NOT_FOUND for non-zero session ID.
		// This forces clients to fall back to full fetches.
		if r.SessionID != 0 {
			resp.ErrorCode = 70 // FETCH_SESSION_ID_NOT_FOUND
			return resp, nil
		}

		for _, rt := range r.Topics {
			st := kmsg.NewFetchResponseTopic()
			st.Topic = rt.Topic
			if r.Version >= 13 {
				st.TopicID = rt.TopicID
			}

			// Resolve topic
			var td *cluster.TopicData
			if r.Version >= 13 && rt.TopicID != [16]byte{} {
				// Topic ID lookup - scan all topics
				for _, t := range state.GetAllTopics() {
					if t.ID == rt.TopicID {
						td = t
						break
					}
				}
			} else {
				td = state.GetTopic(rt.Topic)
			}

			for _, rp := range rt.Partitions {
				sp := kmsg.NewFetchResponseTopicPartition()
				sp.Partition = rp.Partition

				if td == nil {
					sp.ErrorCode = kerr.UnknownTopicOrPartition.Code
					sp.HighWatermark = -1
					sp.LastStableOffset = -1
					sp.LogStartOffset = -1
					st.Partitions = append(st.Partitions, sp)
					continue
				}

				if int(rp.Partition) < 0 || int(rp.Partition) >= len(td.Partitions) {
					sp.ErrorCode = kerr.UnknownTopicOrPartition.Code
					sp.HighWatermark = -1
					sp.LastStableOffset = -1
					sp.LogStartOffset = -1
					st.Partitions = append(st.Partitions, sp)
					continue
				}

				pd := td.Partitions[rp.Partition]

				pd.RLock()
				hw := pd.HW()
				logStart := pd.LogStart()

				// Validate fetch offset
				fetchOffset := rp.FetchOffset
				if fetchOffset < logStart || fetchOffset > hw {
					pd.RUnlock()
					sp.ErrorCode = kerr.OffsetOutOfRange.Code
					sp.HighWatermark = hw
					sp.LastStableOffset = hw // no transactions, LSO = HW
					sp.LogStartOffset = logStart
					st.Partitions = append(st.Partitions, sp)
					continue
				}

				// Fetch batches
				maxBytes := rp.PartitionMaxBytes
				if maxBytes <= 0 {
					maxBytes = 1024 * 1024 // 1MB default
				}
				batches := pd.FetchFrom(fetchOffset, maxBytes)
				pd.RUnlock()

				sp.HighWatermark = hw
				sp.LastStableOffset = hw // no transactions, LSO = HW
				sp.LogStartOffset = logStart

				// Serialize batch data into Records
				if len(batches) > 0 {
					var totalSize int
					for _, b := range batches {
						totalSize += len(b.RawBytes)
					}
					records := make([]byte, 0, totalSize)
					for _, b := range batches {
						records = append(records, b.RawBytes...)
					}
					sp.RecordBatches = records
				}

				st.Partitions = append(st.Partitions, sp)
			}

			resp.Topics = append(resp.Topics, st)
		}

		return resp, nil
	}
}
