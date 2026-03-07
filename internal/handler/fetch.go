package handler

import (
	"time"

	"github.com/klaudworks/klite/internal/cluster"
	"github.com/klaudworks/klite/internal/server"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// HandleFetch returns the Fetch handler (API key 1).
// Supports v4-16.
//
// Implements long-polling (MaxWaitMs/MinBytes), size limits (MaxBytes,
// PartitionMaxBytes), KIP-74 (first batch always returned even if oversized),
// fetch sessions (FETCH_SESSION_ID_NOT_FOUND for non-zero), and TopicID
// resolution for v13+.
func HandleFetch(state *cluster.State, shutdownCh <-chan struct{}, fm *cluster.FetchMetrics) server.Handler {
	return func(req kmsg.Request) (kmsg.Response, error) {
		handlerStart := time.Now()
		r := req.(*kmsg.FetchRequest)
		resp := r.ResponseKind().(*kmsg.FetchResponse)

		// Version validation
		minV, maxV, ok := VersionRange(1)
		if !ok || r.Version < minV || r.Version > maxV {
			return resp, nil
		}

		// Fetch sessions: return FETCH_SESSION_ID_NOT_FOUND for non-zero session ID.
		// This forces clients to fall back to full fetches (Phase 1 simplification).
		if r.SessionID != 0 {
			resp.ErrorCode = kerr.FetchSessionIDNotFound.Code
			return resp, nil
		}

		// Resolve all topics and partitions upfront.
		type partFetchInfo struct {
			td          *cluster.TopicData
			pd          *cluster.PartData
			topicIdx    int // index into r.Topics
			partIdx     int // index into r.Topics[topicIdx].Partitions
			topicName   string
			topicID     [16]byte
			partitionID int32
		}

		var allParts []partFetchInfo

		for ti, rt := range r.Topics {
			// Resolve topic
			var td *cluster.TopicData
			if r.Version >= 13 && rt.TopicID != [16]byte{} {
				td = state.GetTopicByID(rt.TopicID)
			} else {
				td = state.GetTopic(rt.Topic)
			}

			for pi, rp := range rt.Partitions {
				info := partFetchInfo{
					td:          td,
					topicIdx:    ti,
					partIdx:     pi,
					topicName:   rt.Topic,
					topicID:     rt.TopicID,
					partitionID: rp.Partition,
				}
				if td != nil && int(rp.Partition) >= 0 && int(rp.Partition) < len(td.Partitions) {
					info.pd = td.Partitions[rp.Partition]
				}
				allParts = append(allParts, info)
			}
		}

		// Perform the initial fetch and check if we need to long-poll.
		type partResult struct {
			sp       kmsg.FetchResponseTopicPartition
			hasData  bool
			dataSize int
		}

		doFetch := func() ([]partResult, int) {
			results := make([]partResult, len(allParts))
			totalBytes := 0

			for i, info := range allParts {
				sp := kmsg.NewFetchResponseTopicPartition()
				sp.Partition = info.partitionID
				// Use empty (non-nil) RecordBatches so the wire encoding
				// produces compact-bytes length 0 (uvarint 1) instead of
				// null (uvarint 0).  librdkafka reads this field with
				// rd_kafka_buf_read_arraycnt which treats null (-1) as an
				// invalid MessageSetSize, causing parse failures.
				sp.RecordBatches = []byte{}

				if info.td == nil || info.pd == nil {
					sp.ErrorCode = kerr.UnknownTopicOrPartition.Code
					sp.HighWatermark = -1
					sp.LastStableOffset = -1
					sp.LogStartOffset = -1
					results[i] = partResult{sp: sp}
					continue
				}

				pd := info.pd
				rp := r.Topics[info.topicIdx].Partitions[info.partIdx]

				maxBytes := rp.PartitionMaxBytes
				if maxBytes <= 0 {
					maxBytes = 1024 * 1024 // 1MB default
				}

				// Fetch validates the offset and reads chunks under a single
				// RLock, so the HW used for validation is the same HW used
				// for the visibility gate. Cold reads (WAL/S3) happen after
				// the lock is released.
				fr := pd.Fetch(rp.FetchOffset, maxBytes)

				if fr.Err != 0 {
					if rp.FetchOffset < fr.LogStart {
						fm.ErrBelowStart.Add(1)
					} else {
						fm.ErrAboveHW.Add(1)
					}
					sp.ErrorCode = fr.Err
					sp.HighWatermark = fr.HW
					sp.LastStableOffset = fr.LSO
					sp.LogStartOffset = fr.LogStart
					results[i] = partResult{sp: sp}
					continue
				}

				batches := fr.Batches

				// For READ_COMMITTED (IsolationLevel=1), filter and cap at LSO
				readCommitted := r.IsolationLevel == 1
				var abortedTxns []cluster.AbortedTxnEntry
				if readCommitted && len(batches) > 0 {
					// Filter out batches at or beyond LSO
					var filtered []cluster.StoredBatch
					for _, b := range batches {
						if b.BaseOffset >= fr.LSO {
							break
						}
						filtered = append(filtered, b)
					}
					batches = filtered

					// Get aborted transaction index for this range
					if len(batches) > 0 {
						pd.RLock()
						lastBatch := batches[len(batches)-1]
						lastOffset := lastBatch.BaseOffset + int64(lastBatch.LastOffsetDelta) + 1
						abortedTxns = pd.AbortedTxnsInRange(rp.FetchOffset, lastOffset)
						pd.RUnlock()
					}
				}

				sp.HighWatermark = fr.HW
				sp.LastStableOffset = fr.LSO
				sp.LogStartOffset = fr.LogStart

				// Add aborted transaction info to response
				if readCommitted {
					for _, at := range abortedTxns {
						sp.AbortedTransactions = append(sp.AbortedTransactions, kmsg.FetchResponseTopicPartitionAbortedTransaction{
							ProducerID:  at.ProducerID,
							FirstOffset: at.FirstOffset,
						})
					}
				}

				// Serialize batch data into Records
				dataSize := 0
				if len(batches) > 0 {
					for _, b := range batches {
						dataSize += len(b.RawBytes)
					}
					records := make([]byte, 0, dataSize)
					for _, b := range batches {
						records = append(records, b.RawBytes...)
					}
					sp.RecordBatches = records
				}

				results[i] = partResult{sp: sp, hasData: dataSize > 0, dataSize: dataSize}
				totalBytes += dataSize
			}

			return results, totalBytes
		}

		results, totalBytes := doFetch()

		// Long-polling: if we have less than MinBytes and MaxWaitMs > 0, wait
		// for new data or timeout.
		needsLongPoll := int32(totalBytes) < r.MinBytes && r.MaxWaitMillis > 0
		if !needsLongPoll {
			fm.ImmediateData.Add(1)
		} else {
			fm.LongPollEntered.Add(1)

			// Create a shared waiter and register on partitions that had no data
			w := cluster.NewFetchWaiter()
			var regCount int
			var skippedNilPD, skippedError int
			for i, info := range allParts {
				if info.pd == nil {
					skippedNilPD++
					continue
				}
				if results[i].sp.ErrorCode != 0 {
					skippedError++
					continue
				}
				if !results[i].hasData {
					info.pd.RegisterWaiter(w)
					regCount++
				}
			}
			if regCount == 0 {
				if skippedNilPD > 0 {
					fm.SkippedNilPD.Add(1)
				}
				if skippedError > 0 {
					fm.SkippedError.Add(1)
				}
			}
			switch regCount {
			case 0:
				fm.RegisteredOnZero.Add(1)
			case 1:
				fm.RegisteredOnOne.Add(1)
			default:
				fm.RegisteredOnMany.Add(1)
			}

			// If no partitions were registered, skip the wait entirely —
			// nothing can wake us, so we'd always hit the timer.
			if regCount > 0 {
				// Block until woken, timeout, or shutdown
				waitStart := time.Now()
				timer := time.NewTimer(time.Duration(r.MaxWaitMillis) * time.Millisecond)
				select {
				case <-w.Ch():
					fm.WokenByNotify.Add(1)
				case <-timer.C:
					fm.WokenByTimer.Add(1)
				case <-shutdownCh:
					fm.WokenByShutdown.Add(1)
				}
				timer.Stop()
				fm.RecordWait(time.Since(waitStart).Milliseconds())
			}

			// Re-fetch ALL partitions (not just the one that woke us)
			results, _ = doFetch()
		}

		// Apply response-level MaxBytes across all partitions.
		// Iterate through partitions and trim when we exceed MaxBytes,
		// but always include at least one complete partition's data (KIP-74).
		if r.MaxBytes > 0 {
			var responseTotalBytes int32
			firstPartitionWithData := true
			for i := range results {
				dataSize := int32(results[i].dataSize)
				if dataSize == 0 {
					continue
				}
				if !firstPartitionWithData && responseTotalBytes+dataSize > r.MaxBytes {
					// Trim this partition's data to fit
					results[i].sp.RecordBatches = []byte{}
					results[i].dataSize = 0
					continue
				}
				responseTotalBytes += dataSize
				firstPartitionWithData = false
			}
		}

		// Build the response
		// Group results back into topic responses
		topicMap := make(map[int]int) // topicIdx -> index in resp.Topics
		for i, info := range allParts {
			tidx, exists := topicMap[info.topicIdx]
			if !exists {
				st := kmsg.NewFetchResponseTopic()
				st.Topic = info.topicName
				if r.Version >= 13 {
					st.TopicID = info.topicID
				}
				tidx = len(resp.Topics)
				topicMap[info.topicIdx] = tidx
				resp.Topics = append(resp.Topics, st)
			}
			resp.Topics[tidx].Partitions = append(resp.Topics[tidx].Partitions, results[i].sp)
		}

		fm.RecordHandlerDuration(time.Since(handlerStart).Milliseconds())
		return resp, nil
	}
}
