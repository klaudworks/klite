package handler

import (
	"log/slog"
	"strconv"
	"time"

	"github.com/klaudworks/klite/internal/cluster"
	"github.com/klaudworks/klite/internal/server"
	"github.com/klaudworks/klite/internal/wal"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// HandleProduce returns the Produce handler (API key 0).
// Supports v3-11.
//
// Uses the durable WAL path:
//
//	reserveOffset -> WAL append -> fsync -> commitBatch
func HandleProduce(state *cluster.State, walWriter *wal.Writer) server.Handler {
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
					slog.Debug("corrupt message: too short",
						"topic", rt.Topic, "partition", rp.Partition, "len", len(raw))
					sp.ErrorCode = kerr.CorruptMessage.Code
					st.Partitions = append(st.Partitions, sp)
					continue
				}

				meta, parseErr := cluster.ParseBatchHeader(raw)
				if parseErr != nil {
					slog.Debug("corrupt message: parse error",
						"topic", rt.Topic, "partition", rp.Partition, "error", parseErr)
					sp.ErrorCode = kerr.CorruptMessage.Code
					st.Partitions = append(st.Partitions, sp)
					continue
				}

				// Step 2: Validate the batch covers the full Records slice
				if int(meta.BatchLength) != len(raw)-12 {
					slog.Debug("corrupt message: batch length mismatch",
						"topic", rt.Topic, "partition", rp.Partition,
						"batchLength", meta.BatchLength, "rawLen", len(raw),
						"expected", len(raw)-12)
					sp.ErrorCode = kerr.CorruptMessage.Code
					st.Partitions = append(st.Partitions, sp)
					continue
				}

				// Step 3: Validate batch contents (BEFORE taking partition lock)
				if meta.Magic != 2 {
					sp.ErrorCode = kerr.UnsupportedForMessageFormat.Code
					st.Partitions = append(st.Partitions, sp)
					continue
				}

				if len(raw) > maxMessageBytes {
					sp.ErrorCode = kerr.MessageTooLarge.Code
					st.Partitions = append(st.Partitions, sp)
					continue
				}

				if isLogAppendTime {
					cluster.SetLogAppendTime(raw, now, &meta)
				}

				// Idempotent / transactional produce dedup check.
				// Produce requests are handled inline in the connection's
				// read loop (not in a goroutine), so requests from the same
				// connection are naturally serialized in wire order. This
				// guarantees the sequence window sees batches in the order
				// the client sent them — matching Kafka's per-connection
				// serialization model.
				isIdempotent := meta.ProducerID >= 0
				isTransactional := meta.Attributes&0x0010 != 0
				tp := cluster.TopicPartition{Topic: rt.Topic, Partition: rp.Partition}

				if isIdempotent {
					pd.RLock()
					tentativeOffset := pd.HW()
					pd.RUnlock()

					errCode, isDup, dupOffset := state.PIDManager().ValidateAndDedup(
						meta.ProducerID, meta.ProducerEpoch, tp,
						meta.BaseSequence, meta.NumRecords, tentativeOffset,
					)
					if errCode != 0 {
						slog.Debug("idempotent produce rejected",
							"topic", rt.Topic, "partition", rp.Partition,
							"pid", meta.ProducerID, "epoch", meta.ProducerEpoch,
							"baseSeq", meta.BaseSequence, "numRecords", meta.NumRecords,
							"errCode", errCode)
						sp.ErrorCode = errCode
						st.Partitions = append(st.Partitions, sp)
						continue
					}
					if isDup {
						sp.BaseOffset = dupOffset
						sp.LogAppendTime = -1
						if isLogAppendTime {
							sp.LogAppendTime = now
						}
						pd.RLock()
						if r.Version >= 5 {
							sp.LogStartOffset = pd.LogStart()
						}
						pd.RUnlock()
						st.Partitions = append(st.Partitions, sp)
						continue
					}
				}

				// Step 4: Append to partition via WAL
				baseOffset, logStart, walErr := produceWithWAL(pd, td, raw, meta, walWriter)
				if walErr != nil {
					sp.ErrorCode = kerr.KafkaStorageError.Code
					st.Partitions = append(st.Partitions, sp)
					continue
				}

				// Update dedup window with actual offset
				if isIdempotent {
					state.PIDManager().UpdateDedupOffset(meta.ProducerID, tp, meta.BaseSequence, baseOffset)
				}

				// Track transactional batch
				if isTransactional && meta.ProducerID >= 0 {
					state.PIDManager().RecordTxnBatch(meta.ProducerID, rt.Topic, rp.Partition, baseOffset)
					pd.Lock()
					pd.AddOpenTxn(meta.ProducerID, baseOffset)
					pd.Unlock()
				}

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

// produceWithWAL implements the durable produce path:
//  1. Reserve offset under partition lock
//  2. Write to WAL (lock released during fsync wait)
//  3. Commit batch under partition lock (advances HW in order)
func produceWithWAL(pd *cluster.PartData, td *cluster.TopicData, raw []byte, meta cluster.BatchMeta, walWriter *wal.Writer) (baseOffset int64, logStart int64, err error) {
	// Make a copy so we own the bytes
	stored := make([]byte, len(raw))
	copy(stored, raw)

	// Step 1: Reserve offset (brief write lock)
	pd.Lock()
	baseOffset = pd.ReserveOffset(meta)
	logStart = pd.LogStart()
	pd.Unlock()

	// Assign the server-side offset into the raw bytes
	cluster.AssignOffset(stored, baseOffset)

	// Step 2: Write to WAL (pd.mu NOT held — Fetches can proceed)
	walEntry := &wal.Entry{
		TopicID:   td.ID,
		Partition: pd.Index,
		Offset:    baseOffset,
		Data:      stored,
	}

	if err := walWriter.Append(walEntry); err != nil {
		return 0, 0, err
	}

	// Step 3: Commit batch (brief write lock — advances HW in order)
	batch := cluster.StoredBatch{
		BaseOffset:      baseOffset,
		LastOffsetDelta: meta.LastOffsetDelta,
		RawBytes:        stored,
		MaxTimestamp:    meta.MaxTimestamp,
		NumRecords:      meta.NumRecords,
	}

	pd.Lock()
	pd.CommitBatch(batch)
	pd.Unlock()

	pd.NotifyWaiters()

	return baseOffset, logStart, nil
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
