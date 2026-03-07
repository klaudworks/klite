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

func HandleProduce(state *cluster.State, walWriter *wal.Writer) server.Handler {
	return func(req kmsg.Request) (kmsg.Response, error) {
		r := req.(*kmsg.ProduceRequest)

		// acks=0: process the request but do NOT send a response.
		// Returning (nil, nil) tells the dispatch system to skip the response.
		suppressResponse := r.Acks == 0

		resp := r.ResponseKind().(*kmsg.ProduceResponse)

		minV, maxV, ok := VersionRange(0)
		if !ok || r.Version < minV || r.Version > maxV {
			if suppressResponse {
				return nil, nil
			}
			return resp, nil
		}

		now := time.Now().UnixMilli()

		// Two-phase produce: submit all WAL writes (phase 1), then wait for
		// a single fsync and commit all batches (phase 2). This batches all
		// partition writes into one fsync cycle instead of one per partition.
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

				pending, walErr := produceSubmitWAL(pd, td, raw, meta, walWriter)
				if walErr != nil {
					sp.ErrorCode = kerr.KafkaStorageError.Code
					st.Partitions = append(st.Partitions, sp)
					continue
				}

				partIdx := len(st.Partitions)

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

		for i := range pendingCommits {
			pc := &pendingCommits[i]
			sp := &resp.Topics[pc.topicIdx].Partitions[pc.partIdx]

			if walErr := produceCommitWAL(pc.pending); walErr != nil {
				slog.Error("WAL write failed for produce",
					"topic", pc.topic, "partition", pc.partition,
					"baseOffset", pc.pending.baseOffset, "err", walErr)
				sp.ErrorCode = kerr.KafkaStorageError.Code
				sp.BaseOffset = -1
				continue
			}

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

// pendingWAL holds state between the async WAL submit and the post-fsync commit.
type pendingWAL struct {
	pd         *cluster.PartData
	baseOffset int64
	logStart   int64
	meta       cluster.BatchMeta
	stored     []byte
	errCh      <-chan error
}

// produceSubmitWAL reserves offset and enqueues the WAL entry. Chunk pool
// write is deferred until after fsync so data never appears in memory
// before it is durable on disk.
func produceSubmitWAL(pd *cluster.PartData, td *cluster.TopicData, raw []byte, meta cluster.BatchMeta, walWriter *wal.Writer) (pendingWAL, error) {
	stored := make([]byte, len(raw))
	copy(stored, raw)

	pd.Lock()
	baseOffset := pd.ReserveOffset(meta)
	logStart := pd.LogStart()
	cluster.AssignOffset(stored, baseOffset)

	// Submit WAL entry (non-blocking enqueue). On failure, roll back the
	// offset reservation so no gap is created (still under lock).
	walEntry := &wal.Entry{
		TopicID:   td.ID,
		Partition: pd.Index,
		Offset:    baseOffset,
		Data:      stored,
	}
	errCh, err := walWriter.AppendAsync(walEntry)
	if err != nil {
		pd.RollbackReserve(baseOffset)
		pd.Unlock()
		return pendingWAL{}, err
	}
	pd.Unlock()

	return pendingWAL{
		pd:         pd,
		baseOffset: baseOffset,
		logStart:   logStart,
		meta:       meta,
		stored:     stored,
		errCh:      errCh,
	}, nil
}

// produceCommitWAL waits for WAL fsync, then writes to the chunk pool and
// commits (advances HW). On WAL failure, skips offsets so HW keeps advancing.
func produceCommitWAL(p pendingWAL) error {
	walErr := <-p.errCh

	if walErr != nil {
		// WAL write failed. Skip these offsets so subsequent commits
		// can drain past the gap and HW keeps advancing.
		p.pd.Lock()
		p.pd.SkipOffsets(p.baseOffset, int64(p.meta.LastOffsetDelta)+1)
		p.pd.Unlock()
		return walErr
	}

	// Acquire spare chunk outside the lock to avoid deadlock with the S3 flusher.
	spare := p.pd.AcquireSpareChunk(len(p.stored))

	p.pd.Lock()
	spare = p.pd.AppendToChunk(p.stored, chunk.ChunkBatch{
		BaseOffset:      p.baseOffset,
		LastOffsetDelta: p.meta.LastOffsetDelta,
		MaxTimestamp:    p.meta.MaxTimestamp,
		NumRecords:      p.meta.NumRecords,
	}, spare)
	p.pd.CommitBatch(cluster.StoredBatch{
		BaseOffset:      p.baseOffset,
		LastOffsetDelta: p.meta.LastOffsetDelta,
		RawBytes:        p.stored,
		MaxTimestamp:    p.meta.MaxTimestamp,
		NumRecords:      p.meta.NumRecords,
	})
	p.pd.Unlock()
	p.pd.ReleaseSpareChunk(spare)

	p.pd.NotifyWaiters()
	return nil
}

func getMaxMessageBytes(td *cluster.TopicData) int {
	if v, ok := td.Configs["max.message.bytes"]; ok {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return cluster.DefaultMaxMessageBytes
}

func getTimestampType(td *cluster.TopicData) string {
	if v, ok := td.Configs["message.timestamp.type"]; ok {
		return v
	}
	return "CreateTime"
}
