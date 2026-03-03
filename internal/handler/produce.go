package handler

import (
	"log/slog"
	"strconv"
	"time"

	"github.com/klaudworks/klite/internal/chunk"
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

		// Two-phase produce: first submit all WAL writes (phase 1), then
		// wait for the single fsync and commit all batches (phase 2).
		// This batches all partition writes in one request into a single
		// fsync cycle instead of one fsync per partition.

		// pendingCommit ties a pending WAL entry to its response slot.
		type pendingCommit struct {
			pending         pendingWAL
			topicIdx        int
			partIdx         int
			isIdempotent    bool
			isTransactional bool
			isLogAppendTime bool
			topic           string
			partition       int32
			meta            cluster.BatchMeta
		}
		var pendingCommits []pendingCommit

		for _, rt := range r.Topics {
			st := kmsg.NewProduceResponseTopic()
			st.Topic = rt.Topic

			// Look up or auto-create the topic
			td, _, err := state.GetOrCreateTopic(rt.Topic)
			if err != nil {
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

			maxMessageBytes := getMaxMessageBytes(td)
			isLogAppendTime := getTimestampType(td) == "LogAppendTime"

			topicIdx := len(resp.Topics)

			for _, rp := range rt.Partitions {
				sp := kmsg.NewProduceResponseTopicPartition()
				sp.Partition = rp.Partition
				sp.BaseOffset = -1
				sp.LogStartOffset = -1

				if int(rp.Partition) < 0 || int(rp.Partition) >= len(td.Partitions) {
					sp.ErrorCode = kerr.UnknownTopicOrPartition.Code
					st.Partitions = append(st.Partitions, sp)
					continue
				}

				pd := td.Partitions[rp.Partition]
				raw := rp.Records

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

				if int(meta.BatchLength) != len(raw)-12 {
					slog.Debug("corrupt message: batch length mismatch",
						"topic", rt.Topic, "partition", rp.Partition,
						"batchLength", meta.BatchLength, "rawLen", len(raw),
						"expected", len(raw)-12)
					sp.ErrorCode = kerr.CorruptMessage.Code
					st.Partitions = append(st.Partitions, sp)
					continue
				}

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

				// Phase 1: reserve offset, append to chunk, submit WAL async
				pending, walErr := produceSubmitWAL(pd, td, raw, meta, walWriter)
				if walErr != nil {
					sp.ErrorCode = kerr.KafkaStorageError.Code
					st.Partitions = append(st.Partitions, sp)
					continue
				}

				partIdx := len(st.Partitions)

				// Placeholder response — will be filled in phase 2
				st.Partitions = append(st.Partitions, sp)

				pendingCommits = append(pendingCommits, pendingCommit{
					pending:         pending,
					topicIdx:        topicIdx,
					partIdx:         partIdx,
					isIdempotent:    isIdempotent,
					isTransactional: isTransactional,
					isLogAppendTime: isLogAppendTime,
					topic:           rt.Topic,
					partition:       rp.Partition,
					meta:            meta,
				})
			}

			resp.Topics = append(resp.Topics, st)
		}

		// Phase 2: wait for fsync and commit all batches.
		// All pending entries share the same fsync cycle in the WAL writer,
		// so this is effectively one fsync wait regardless of partition count.
		for _, pc := range pendingCommits {
			produceCommitWAL(pc.pending)

			sp := &resp.Topics[pc.topicIdx].Partitions[pc.partIdx]
			sp.BaseOffset = pc.pending.baseOffset
			sp.LogAppendTime = -1
			if pc.isLogAppendTime {
				sp.LogAppendTime = now
			}
			if r.Version >= 5 {
				sp.LogStartOffset = pc.pending.logStart
			}

			if pc.isIdempotent {
				tp := cluster.TopicPartition{Topic: pc.topic, Partition: pc.partition}
				state.PIDManager().UpdateDedupOffset(pc.meta.ProducerID, tp, pc.meta.BaseSequence, pc.pending.baseOffset)
			}

			if pc.isTransactional && pc.meta.ProducerID >= 0 {
				state.PIDManager().RecordTxnBatch(pc.meta.ProducerID, pc.topic, pc.partition, pc.pending.baseOffset)
				pd := state.GetTopic(pc.topic).Partitions[pc.partition]
				pd.Lock()
				pd.AddOpenTxn(pc.meta.ProducerID, pc.pending.baseOffset)
				pd.Unlock()
			}
		}

		if suppressResponse {
			return nil, nil
		}

		return resp, nil
	}
}

// pendingWAL holds state for a partition batch between the async WAL submit
// and the post-fsync commit. Used by the two-phase produce path to batch
// all WAL writes in a single produce request into one fsync cycle.
type pendingWAL struct {
	pd         *cluster.PartData
	baseOffset int64
	logStart   int64
	meta       cluster.BatchMeta
	stored     []byte
	doneCh     <-chan struct{}
}

// produceSubmitWAL is phase 1: reserve offset, enqueue the WAL entry, then
// append to chunk memory. Returns a pendingWAL that the caller must pass to
// produceCommitWAL after the fsync channel signals.
//
// Ordering: AcquireSpareChunk (may block, no lock held) → pd.Lock() →
// reserve offset → AppendAsync → AppendToChunk → pd.Unlock().
//
// The spare chunk is acquired before taking the partition lock so that
// backpressure blocking never holds pd.mu, preventing deadlock with the
// S3 flusher which needs pd.mu to detach and release chunks.
func produceSubmitWAL(pd *cluster.PartData, td *cluster.TopicData, raw []byte, meta cluster.BatchMeta, walWriter *wal.Writer) (pendingWAL, error) {
	stored := make([]byte, len(raw))
	copy(stored, raw)

	// Pre-acquire a spare chunk before taking the partition lock.
	// This may block if the pool is exhausted (backpressure), which is
	// safe because we don't hold pd.mu yet.
	spare := pd.AcquireSpareChunk(len(stored))

	pd.Lock()
	baseOffset := pd.ReserveOffset(meta)
	logStart := pd.LogStart()
	cluster.AssignOffset(stored, baseOffset)

	// Submit WAL entry (non-blocking enqueue). On failure, roll back the
	// offset reservation so no gap is created.
	walEntry := &wal.Entry{
		TopicID:   td.ID,
		Partition: pd.Index,
		Offset:    baseOffset,
		Data:      stored,
	}
	doneCh, err := walWriter.AppendAsync(walEntry)
	if err != nil {
		pd.RollbackReserve(baseOffset)
		pd.Unlock()
		pd.ReleaseSpareChunk(spare)
		return pendingWAL{}, err
	}

	// WAL enqueue succeeded — safe to place data in the chunk.
	spare = pd.AppendToChunk(stored, chunk.ChunkBatch{
		BaseOffset:      baseOffset,
		LastOffsetDelta: meta.LastOffsetDelta,
		MaxTimestamp:    meta.MaxTimestamp,
		NumRecords:      meta.NumRecords,
	}, spare)
	pd.Unlock()
	pd.ReleaseSpareChunk(spare)

	return pendingWAL{
		pd:         pd,
		baseOffset: baseOffset,
		logStart:   logStart,
		meta:       meta,
		stored:     stored,
		doneCh:     doneCh,
	}, nil
}

// produceCommitWAL is phase 2: wait for the fsync channel, then commit the
// batch (advance HW) and notify fetch waiters.
func produceCommitWAL(p pendingWAL) {
	<-p.doneCh

	batch := cluster.StoredBatch{
		BaseOffset:      p.baseOffset,
		LastOffsetDelta: p.meta.LastOffsetDelta,
		RawBytes:        p.stored,
		MaxTimestamp:    p.meta.MaxTimestamp,
		NumRecords:      p.meta.NumRecords,
	}
	p.pd.Lock()
	p.pd.CommitBatch(batch)
	p.pd.Unlock()

	p.pd.NotifyWaiters()
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
