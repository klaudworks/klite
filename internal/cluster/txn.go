package cluster

import (
	"encoding/binary"
	"hash/crc32"
	"log/slog"
	"sync"
	"time"

	"github.com/klaudworks/klite/internal/clock"
)

type TxnState int8

const (
	TxnNone TxnState = iota
	TxnOngoing
	TxnEnding
)

type ProducerState struct {
	ProducerID int64
	Epoch      int16
	TxnID      string // empty for non-transactional

	Sequences map[TopicPartition]*SequenceWindow

	TxnState        TxnState
	TxnPartitions   map[TopicPartition]bool                        // partitions in current txn
	TxnBatches      []*TxnBatchRef                                 // data batches written in current txn
	TxnGroups       []string                                       // consumer groups in current txn (AddOffsetsToTxn)
	TxnOffsets      map[string]map[TopicPartition]PendingTxnOffset // group -> tp -> offset
	TxnFirstOffsets map[TopicPartition]int64                       // first offset per partition in this txn
	TxnStartTime    time.Time
	TxnTimeoutMs    int32
	LastWasCommit   bool
	TxnEndCommit    bool
}

type PendingTxnOffset struct {
	Offset      int64
	LeaderEpoch int32
	Metadata    string
}

type TxnBatchRef struct {
	Topic     string
	Partition int32
	Offset    int64
}

// SequenceWindow implements a 5-slot ring buffer for dedup, matching Kafka's window.
type SequenceWindow struct {
	seq     [5]int32 // expected next sequence at each slot
	offsets [5]int64 // base offset for each slot
	at      uint8    // current write position
	epoch   int16    // last seen epoch
	filled  bool     // whether we've seen any writes
}

// PushAndValidate checks a batch's sequence against the window.
// Returns (ok, isDuplicate, dupOffset).
//   - ok=true, isDuplicate=false: new batch accepted, window updated
//   - ok=true, isDuplicate=true: duplicate detected, dupOffset is the original offset
//   - ok=false: out-of-order sequence
func (w *SequenceWindow) PushAndValidate(epoch int16, firstSeq, numRecords int32, baseOffset int64) (ok, isDuplicate bool, dupOffset int64) {
	// Epoch change: reset window, require seq=0
	if !w.filled || epoch != w.epoch {
		if firstSeq != 0 {
			return false, false, 0
		}
		w.epoch = epoch
		w.filled = true
		w.at = 0
		for i := range w.seq {
			w.seq[i] = 0
			w.offsets[i] = 0
		}
		w.seq[0] = 0
		w.offsets[0] = baseOffset
		nextSeq := (firstSeq + numRecords)
		w.at = 1
		w.seq[1] = nextSeq
		return true, false, 0
	}

	// Check for duplicate: scan all slots
	nextSeq := (firstSeq + numRecords)
	for i := 0; i < 5; i++ {
		next := (i + 1) % 5
		if w.seq[i] == firstSeq && w.seq[next] == nextSeq {
			return true, true, w.offsets[i]
		}
	}

	// Check for expected sequence at current position
	if w.seq[w.at] != firstSeq {
		return false, false, 0
	}

	// Accept: store and advance
	w.offsets[w.at] = baseOffset
	w.at = (w.at + 1) % 5
	w.seq[w.at] = nextSeq
	return true, false, 0
}

// LastSequence returns the last committed sequence number, or -1 if none.
func (w *SequenceWindow) LastSequence() int32 {
	if !w.filled {
		return -1
	}
	return w.seq[w.at] - 1
}

// Replay unconditionally updates the window from a committed WAL entry.
// Unlike PushAndValidate, no validation is performed — the batch is
// already durably committed. Replaying the same entries twice is safe
// (idempotent) because each call overwrites the current slot.
func (w *SequenceWindow) Replay(epoch int16, firstSeq, numRecords int32, baseOffset int64) {
	nextSeq := firstSeq + numRecords
	if !w.filled || epoch != w.epoch {
		w.epoch = epoch
		w.filled = true
		w.at = 0
		for i := range w.seq {
			w.seq[i] = 0
			w.offsets[i] = 0
		}
		w.seq[0] = firstSeq
		w.offsets[0] = baseOffset
		w.at = 1
		w.seq[1] = nextSeq
		return
	}
	w.offsets[w.at] = baseOffset
	w.seq[w.at] = firstSeq
	w.at = (w.at + 1) % 5
	w.seq[w.at] = nextSeq
}

type AbortedTxnEntry struct {
	ProducerID  int64
	FirstOffset int64 // first data offset of the txn on this partition
	LastOffset  int64 // offset of the abort control record
}

type ProducerIDManager struct {
	mu        sync.Mutex
	nextPID   int64                     // monotonic counter
	producers map[int64]*ProducerState  // PID -> state
	byTxnID   map[string]*ProducerState // txnID -> state
	clk       clock.Clock

	// Rate-limiting for "unknown PID" log messages.
	unknownPIDLogTime  time.Time
	unknownPIDLogCount int64
}

func NewProducerIDManager() *ProducerIDManager {
	return &ProducerIDManager{
		nextPID:   1,
		producers: make(map[int64]*ProducerState),
		byTxnID:   make(map[string]*ProducerState),
		clk:       clock.RealClock{},
	}
}

func (m *ProducerIDManager) SetClock(c clock.Clock) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.clk = c
}

func (m *ProducerIDManager) SetNextPID(next int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if next > m.nextPID {
		m.nextPID = next
	}
}

func (m *ProducerIDManager) NextPID() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.nextPID
}

func (m *ProducerIDManager) InitProducerID(txnID string, txnTimeoutMs int32) (int64, int16, int16) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if txnID != "" {
		if ps, ok := m.byTxnID[txnID]; ok {
			if ps.TxnState == TxnOngoing {
				ps.TxnState = TxnNone
				ps.resetTxnState()
			}
			ps.Epoch++
			if ps.Epoch < 0 {
				// Epoch overflow: allocate new PID
				delete(m.producers, ps.ProducerID)
				newPID := m.nextPID
				m.nextPID++
				ps.ProducerID = newPID
				ps.Epoch = 0
				m.producers[newPID] = ps
			}
			ps.TxnTimeoutMs = txnTimeoutMs
			return ps.ProducerID, ps.Epoch, 0
		}

		pid := m.nextPID
		m.nextPID++
		ps := &ProducerState{
			ProducerID:      pid,
			Epoch:           0,
			TxnID:           txnID,
			Sequences:       make(map[TopicPartition]*SequenceWindow),
			TxnPartitions:   make(map[TopicPartition]bool),
			TxnOffsets:      make(map[string]map[TopicPartition]PendingTxnOffset),
			TxnFirstOffsets: make(map[TopicPartition]int64),
			TxnTimeoutMs:    txnTimeoutMs,
		}
		m.producers[pid] = ps
		m.byTxnID[txnID] = ps
		return pid, 0, 0
	}

	pid := m.nextPID
	m.nextPID++
	ps := &ProducerState{
		ProducerID: pid,
		Epoch:      0,
		Sequences:  make(map[TopicPartition]*SequenceWindow),
	}
	m.producers[pid] = ps
	return pid, 0, 0
}

func (m *ProducerIDManager) GetProducer(pid int64) *ProducerState {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.producers[pid]
}

func (m *ProducerIDManager) GetProducerByTxnID(txnID string) *ProducerState {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.byTxnID[txnID]
}

func (m *ProducerIDManager) ValidateAndDedup(pid int64, epoch int16, tp TopicPartition, firstSeq, numRecords int32, baseOffset int64) (errCode int16, isDup bool, dupOffset int64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	ps, ok := m.producers[pid]
	if !ok {
		now := m.clk.Now()
		m.unknownPIDLogCount++
		if now.Sub(m.unknownPIDLogTime) >= 5*time.Second {
			slog.Info("ValidateAndDedup: unknown PID, accepting as new producer",
				"pid", pid, "epoch", epoch, "topic", tp.Topic, "partition", tp.Partition,
				"firstSeq", firstSeq, "numRecords", numRecords,
				"occurrences_since_last_log", m.unknownPIDLogCount)
			m.unknownPIDLogTime = now
			m.unknownPIDLogCount = 0
		}
		// Register the PID so subsequent batches get dedup protection.
		// After failover the new primary doesn't know PIDs allocated by
		// the old primary; we accept the first batch and track state from
		// here so retries are properly deduped. Use Replay (not
		// PushAndValidate) because the sequence may be non-zero.
		ps = &ProducerState{
			ProducerID: pid,
			Epoch:      epoch,
			Sequences:  make(map[TopicPartition]*SequenceWindow),
		}
		m.producers[pid] = ps
		w := &SequenceWindow{}
		ps.Sequences[tp] = w
		w.Replay(epoch, firstSeq, numRecords, baseOffset)
		return 0, false, 0
	}

	if epoch != ps.Epoch {
		if epoch < ps.Epoch {
			slog.Warn("ValidateAndDedup: PRODUCER_FENCED",
				"pid", pid, "epoch", epoch, "currentEpoch", ps.Epoch)
			return 90, false, 0 // PRODUCER_FENCED
		}
		// KIP-360: accept a higher epoch from the same PID. The client
		// bumps the epoch locally after a disconnect/failover and resets
		// sequences to 0. We must accept this and clear old dedup state.
		if firstSeq != 0 {
			slog.Warn("ValidateAndDedup: epoch bump with non-zero firstSeq",
				"pid", pid, "epoch", epoch, "currentEpoch", ps.Epoch,
				"firstSeq", firstSeq)
			return 45, false, 0 // OUT_OF_ORDER_SEQUENCE_NUMBER — new epoch must start at seq 0
		}
		slog.Warn("ValidateAndDedup: epoch bump accepted (KIP-360)",
			"pid", pid, "oldEpoch", ps.Epoch, "newEpoch", epoch,
			"topic", tp.Topic, "partition", tp.Partition,
			"numRecords", numRecords)
		ps.Epoch = epoch
		ps.Sequences = map[TopicPartition]*SequenceWindow{}
		w := &SequenceWindow{}
		ps.Sequences[tp] = w
		w.PushAndValidate(epoch, firstSeq, numRecords, baseOffset)
		return 0, false, 0
	}

	w, ok := ps.Sequences[tp]
	if !ok {
		w = &SequenceWindow{}
		ps.Sequences[tp] = w
	}

	ok2, isDup, dupOff := w.PushAndValidate(epoch, firstSeq, numRecords, baseOffset)
	if isDup {
		return 0, true, dupOff
	}
	if !ok2 {
		return 45, false, 0 // OUT_OF_ORDER_SEQUENCE_NUMBER
	}
	return 0, false, 0
}

// UpdateDedupOffset updates the dedup window's offset for the last accepted batch.
// Called after the actual offset is assigned (which may differ from the tentative
// one passed to ValidateAndDedup). ValidateAndDedup stores the offset at position
// (at-1) before advancing at, so the most recently accepted batch's offset is
// always at slot (at+4)%5.
func (m *ProducerIDManager) UpdateDedupOffset(pid int64, tp TopicPartition, firstSeq int32, actualOffset int64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	ps, ok := m.producers[pid]
	if !ok {
		return
	}
	w, ok := ps.Sequences[tp]
	if !ok {
		return
	}
	prev := (w.at + 4) % 5 // slot written by the last PushAndValidate call
	w.offsets[prev] = actualOffset
}

// ReplayBatch reconstructs producer dedup state from a committed WAL entry.
// Called during WAL replay (startup) and chunk rebuild (promotion) so that
// duplicate detection works after restart or failover. Only processes
// idempotent batches (ProducerID >= 0) that are not control records
// (BaseSequence >= 0).
func (m *ProducerIDManager) ReplayBatch(tp TopicPartition, meta BatchMeta, baseOffset int64) {
	if meta.ProducerID < 0 || meta.BaseSequence < 0 {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	ps, ok := m.producers[meta.ProducerID]
	if !ok {
		ps = &ProducerState{
			ProducerID: meta.ProducerID,
			Epoch:      meta.ProducerEpoch,
			Sequences:  make(map[TopicPartition]*SequenceWindow),
		}
		m.producers[meta.ProducerID] = ps
	}
	if meta.ProducerEpoch > ps.Epoch {
		ps.Epoch = meta.ProducerEpoch
	}

	w, ok := ps.Sequences[tp]
	if !ok {
		w = &SequenceWindow{}
		ps.Sequences[tp] = w
	}
	w.Replay(meta.ProducerEpoch, meta.BaseSequence, meta.NumRecords, baseOffset)
}

func (m *ProducerIDManager) AddPartitionsToTxn(pid int64, epoch int16, partitions []TopicPartition) int16 {
	m.mu.Lock()
	defer m.mu.Unlock()

	ps, ok := m.producers[pid]
	if !ok {
		return 3 // UNKNOWN_TOPIC_OR_PARTITION as proxy for invalid PID
	}
	if epoch != ps.Epoch {
		if epoch < ps.Epoch {
			return 90 // PRODUCER_FENCED
		}
		return 47 // INVALID_PRODUCER_EPOCH
	}
	if ps.TxnID == "" {
		return 49 // TRANSACTIONAL_ID_AUTHORIZATION_FAILED — not a transactional producer
	}

	if ps.TxnState == TxnEnding {
		return 53 // INVALID_TXN_STATE
	}

	if ps.TxnState != TxnOngoing {
		ps.TxnState = TxnOngoing
		ps.TxnStartTime = m.clk.Now()
		ps.TxnPartitions = make(map[TopicPartition]bool)
		ps.TxnBatches = nil
		ps.TxnGroups = nil
		ps.TxnOffsets = make(map[string]map[TopicPartition]PendingTxnOffset)
		ps.TxnFirstOffsets = make(map[TopicPartition]int64)
	}

	for _, tp := range partitions {
		ps.TxnPartitions[tp] = true
	}
	return 0
}

func (m *ProducerIDManager) AddOffsetsToTxn(pid int64, epoch int16, groupID string) int16 {
	m.mu.Lock()
	defer m.mu.Unlock()

	ps, ok := m.producers[pid]
	if !ok {
		return 3
	}
	if epoch != ps.Epoch {
		if epoch < ps.Epoch {
			return 90 // PRODUCER_FENCED
		}
		return 47
	}
	if ps.TxnID == "" {
		return 49
	}

	if ps.TxnState == TxnEnding {
		return 53 // INVALID_TXN_STATE
	}

	if ps.TxnState != TxnOngoing {
		ps.TxnState = TxnOngoing
		ps.TxnStartTime = m.clk.Now()
		ps.TxnPartitions = make(map[TopicPartition]bool)
		ps.TxnBatches = nil
		ps.TxnGroups = nil
		ps.TxnOffsets = make(map[string]map[TopicPartition]PendingTxnOffset)
		ps.TxnFirstOffsets = make(map[TopicPartition]int64)
	}

	ps.TxnGroups = append(ps.TxnGroups, groupID)
	return 0
}

func (m *ProducerIDManager) StoreTxnOffset(pid int64, epoch int16, groupID, topic string, partition int32, offset int64, leaderEpoch int32, metadata string) int16 {
	m.mu.Lock()
	defer m.mu.Unlock()

	ps, ok := m.producers[pid]
	if !ok {
		return 3
	}
	if epoch != ps.Epoch {
		if epoch < ps.Epoch {
			return 90 // PRODUCER_FENCED
		}
		return 47
	}
	if ps.TxnState != TxnOngoing {
		return 53 // INVALID_TXN_STATE
	}

	if ps.TxnOffsets == nil {
		ps.TxnOffsets = make(map[string]map[TopicPartition]PendingTxnOffset)
	}
	gOffsets, ok := ps.TxnOffsets[groupID]
	if !ok {
		gOffsets = make(map[TopicPartition]PendingTxnOffset)
		ps.TxnOffsets[groupID] = gOffsets
	}
	gOffsets[TopicPartition{Topic: topic, Partition: partition}] = PendingTxnOffset{
		Offset:      offset,
		LeaderEpoch: leaderEpoch,
		Metadata:    metadata,
	}
	return 0
}

func (m *ProducerIDManager) RecordTxnBatch(pid int64, topic string, partition int32, baseOffset int64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	ps, ok := m.producers[pid]
	if !ok {
		return
	}
	ps.TxnBatches = append(ps.TxnBatches, &TxnBatchRef{
		Topic:     topic,
		Partition: partition,
		Offset:    baseOffset,
	})
	tp := TopicPartition{Topic: topic, Partition: partition}
	if _, exists := ps.TxnFirstOffsets[tp]; !exists {
		ps.TxnFirstOffsets[tp] = baseOffset
	}
}

type EndTxnState struct {
	ProducerID      int64
	Epoch           int16
	Commit          bool
	TxnPartitions   map[TopicPartition]bool
	TxnGroups       []string
	TxnOffsets      map[string]map[TopicPartition]PendingTxnOffset
	TxnFirstOffsets map[TopicPartition]int64
}

func (m *ProducerIDManager) PrepareEndTxn(pid int64, epoch int16, commit bool) (*EndTxnState, int16) {
	m.mu.Lock()
	defer m.mu.Unlock()

	ps, ok := m.producers[pid]
	if !ok {
		return nil, 3
	}
	if epoch != ps.Epoch {
		if epoch < ps.Epoch {
			return nil, 90 // PRODUCER_FENCED
		}
		return nil, 47 // INVALID_PRODUCER_EPOCH
	}
	if ps.TxnState == TxnNone {
		if ps.LastWasCommit == commit {
			return &EndTxnState{
				ProducerID: ps.ProducerID,
				Epoch:      ps.Epoch,
				Commit:     commit,
			}, 0
		}
		return nil, 53 // INVALID_TXN_STATE
	}
	if ps.TxnState == TxnEnding {
		if ps.TxnEndCommit != commit {
			return nil, 53 // INVALID_TXN_STATE
		}
		return &EndTxnState{
			ProducerID:      ps.ProducerID,
			Epoch:           ps.Epoch,
			Commit:          commit,
			TxnPartitions:   ps.TxnPartitions,
			TxnGroups:       ps.TxnGroups,
			TxnOffsets:      ps.TxnOffsets,
			TxnFirstOffsets: ps.TxnFirstOffsets,
		}, 0
	}

	state := &EndTxnState{
		ProducerID:      ps.ProducerID,
		Epoch:           ps.Epoch,
		Commit:          commit,
		TxnPartitions:   ps.TxnPartitions,
		TxnGroups:       ps.TxnGroups,
		TxnOffsets:      ps.TxnOffsets,
		TxnFirstOffsets: ps.TxnFirstOffsets,
	}

	ps.TxnState = TxnEnding
	ps.TxnEndCommit = commit

	return state, 0
}

func (m *ProducerIDManager) FinalizeEndTxn(pid int64, epoch int16, commit, success bool) int16 {
	m.mu.Lock()
	defer m.mu.Unlock()

	ps, ok := m.producers[pid]
	if !ok {
		return 3
	}
	if epoch != ps.Epoch {
		if epoch < ps.Epoch {
			return 90 // PRODUCER_FENCED
		}
		return 47 // INVALID_PRODUCER_EPOCH
	}

	if ps.TxnState != TxnEnding {
		if ps.TxnState == TxnNone && ps.LastWasCommit == commit {
			return 0
		}
		return 53 // INVALID_TXN_STATE
	}
	if ps.TxnEndCommit != commit {
		return 53 // INVALID_TXN_STATE
	}

	if success {
		ps.TxnState = TxnNone
		ps.LastWasCommit = commit
		ps.TxnEndCommit = false
		ps.resetTxnState()
		return 0
	}

	ps.TxnState = TxnOngoing
	ps.TxnEndCommit = false
	return 0
}

func (ps *ProducerState) resetTxnState() {
	ps.TxnPartitions = make(map[TopicPartition]bool)
	ps.TxnBatches = nil
	ps.TxnGroups = nil
	ps.TxnOffsets = make(map[string]map[TopicPartition]PendingTxnOffset)
	ps.TxnFirstOffsets = make(map[TopicPartition]int64)
	ps.TxnStartTime = time.Time{}
}

// ProducerSnapshot is a point-in-time copy of ProducerState fields.
// Safe to read without holding any lock.
type ProducerSnapshot struct {
	ProducerID     int64
	Epoch          int16
	TxnID          string
	TxnState       TxnState
	TxnTimeoutMs   int32
	TxnStartTime   time.Time
	LastSequence   int32                   // for a specific partition (set by GetProducersForPartition)
	TxnStartOffset int64                   // for a specific partition (set by GetProducersForPartition)
	TxnPartitions  map[TopicPartition]bool // copied map
}

func snapshotProducer(ps *ProducerState) ProducerSnapshot {
	snap := ProducerSnapshot{
		ProducerID:     ps.ProducerID,
		Epoch:          ps.Epoch,
		TxnID:          ps.TxnID,
		TxnState:       ps.TxnState,
		TxnTimeoutMs:   ps.TxnTimeoutMs,
		TxnStartTime:   ps.TxnStartTime,
		LastSequence:   -1,
		TxnStartOffset: -1,
	}
	if len(ps.TxnPartitions) > 0 {
		snap.TxnPartitions = make(map[TopicPartition]bool, len(ps.TxnPartitions))
		for tp, v := range ps.TxnPartitions {
			snap.TxnPartitions[tp] = v
		}
	}
	return snap
}

func (m *ProducerIDManager) AllTransactions() []ProducerSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()

	var result []ProducerSnapshot
	for _, ps := range m.producers {
		if ps.TxnID != "" {
			result = append(result, snapshotProducer(ps))
		}
	}
	return result
}

func (m *ProducerIDManager) GetProducerByTxnIDSnapshot(txnID string) (ProducerSnapshot, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ps, ok := m.byTxnID[txnID]
	if !ok {
		return ProducerSnapshot{}, false
	}
	return snapshotProducer(ps), true
}

func (m *ProducerIDManager) GetProducersForPartition(topic string, partition int32) []ProducerSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()

	tp := TopicPartition{Topic: topic, Partition: partition}
	var result []ProducerSnapshot
	for _, ps := range m.producers {
		w, ok := ps.Sequences[tp]
		if !ok {
			continue
		}
		snap := snapshotProducer(ps)
		snap.LastSequence = w.LastSequence()
		if ps.TxnState == TxnOngoing {
			if firstOff, ok2 := ps.TxnFirstOffsets[tp]; ok2 {
				snap.TxnStartOffset = firstOff
			}
		}
		result = append(result, snap)
	}
	return result
}

func BuildControlBatch(producerID int64, producerEpoch int16, commit bool, nowMs int64) []byte {
	now := nowMs

	var controlType int16
	if commit {
		controlType = 1
	}

	recordKey := make([]byte, 4)
	binary.BigEndian.PutUint16(recordKey[0:2], 0) // version=0
	binary.BigEndian.PutUint16(recordKey[2:4], uint16(controlType))

	record := encodeRecord(recordKey, nil)
	numRecordBytes := len(record)
	totalBatch := 61 + numRecordBytes
	raw := make([]byte, totalBatch)

	binary.BigEndian.PutUint64(raw[0:8], 0)
	binary.BigEndian.PutUint32(raw[8:12], uint32(totalBatch-12))
	binary.BigEndian.PutUint32(raw[12:16], 0)
	raw[16] = 2
	binary.BigEndian.PutUint16(raw[21:23], 0x0030) // transactional + control
	binary.BigEndian.PutUint32(raw[23:27], 0)
	binary.BigEndian.PutUint64(raw[27:35], uint64(now))
	binary.BigEndian.PutUint64(raw[35:43], uint64(now))
	binary.BigEndian.PutUint64(raw[43:51], uint64(producerID))
	binary.BigEndian.PutUint16(raw[51:53], uint16(producerEpoch))
	binary.BigEndian.PutUint32(raw[53:57], 0xFFFFFFFF) // BaseSequence = -1 for control records
	binary.BigEndian.PutUint32(raw[57:61], 1)
	copy(raw[61:], record)

	crcVal := crc32.Checksum(raw[21:], crc32cTable)
	binary.BigEndian.PutUint32(raw[17:21], crcVal)

	return raw
}

func encodeRecord(key, value []byte) []byte {
	var buf []byte

	var body []byte
	body = append(body, 0)       // attributes
	body = appendVarint(body, 0) // timestamp delta
	body = appendVarint(body, 0) // offset delta
	if key == nil {
		body = appendVarint(body, -1)
	} else {
		body = appendVarint(body, int64(len(key)))
		body = append(body, key...)
	}
	if value == nil {
		body = appendVarint(body, -1)
	} else {
		body = appendVarint(body, int64(len(value)))
		body = append(body, value...)
	}
	body = appendVarint(body, 0) // headers count

	buf = appendVarint(buf, int64(len(body)))
	buf = append(buf, body...)

	return buf
}

func appendVarint(buf []byte, v int64) []byte {
	uv := uint64((v << 1) ^ (v >> 63)) // zigzag encode
	for uv >= 0x80 {
		buf = append(buf, byte(uv)|0x80)
		uv >>= 7
	}
	buf = append(buf, byte(uv))
	return buf
}

func IsControlBatch(raw []byte) bool {
	if len(raw) < 23 {
		return false
	}
	attrs := binary.BigEndian.Uint16(raw[21:23])
	return attrs&0x0020 != 0
}

func IsTransactionalBatch(raw []byte) bool {
	if len(raw) < 23 {
		return false
	}
	attrs := binary.BigEndian.Uint16(raw[21:23])
	return attrs&0x0010 != 0
}

// IsCommitControlBatch returns true if the raw batch is a COMMIT control record.
// Returns false for ABORT control records and non-control batches.
// The control type is stored in the first record's key at bytes 2-3: 0=ABORT, 1=COMMIT.
func IsCommitControlBatch(raw []byte) bool {
	if len(raw) < 62 {
		return false
	}
	if !IsControlBatch(raw) {
		return false
	}
	// Records start at offset 61. Record format:
	//   varint(recordLen), byte(attrs), varint(tsDelta), varint(offDelta), varint(keyLen), key...
	pos := 61
	// Skip record length varint
	for pos < len(raw) && raw[pos]&0x80 != 0 {
		pos++
	}
	pos++ // final byte of varint
	if pos >= len(raw) {
		return false
	}
	pos++ // skip attributes byte
	// Skip timestamp delta varint
	for pos < len(raw) && raw[pos]&0x80 != 0 {
		pos++
	}
	pos++
	// Skip offset delta varint
	for pos < len(raw) && raw[pos]&0x80 != 0 {
		pos++
	}
	pos++
	// Read key length varint (zigzag-encoded)
	keyLen, n := decodeVarint(raw[pos:])
	if n == 0 || keyLen < 4 {
		return false
	}
	pos += n
	if pos+4 > len(raw) {
		return false
	}
	// Key bytes: [version(2)][controlType(2)]
	controlType := binary.BigEndian.Uint16(raw[pos+2 : pos+4])
	return controlType == 1
}

func decodeVarint(buf []byte) (int64, int) {
	var uv uint64
	for i, b := range buf {
		if i >= 10 {
			return 0, 0
		}
		uv |= uint64(b&0x7f) << (7 * uint(i))
		if b&0x80 == 0 {
			// zigzag decode
			v := int64((uv >> 1) ^ -(uv & 1))
			return v, i + 1
		}
	}
	return 0, 0
}

// ExpiredTransactions returns snapshots of producers with ongoing transactions
// that have exceeded their timeout. The caller is responsible for aborting
// these transactions by writing abort control batches.
func (m *ProducerIDManager) ExpiredTransactions() []ProducerSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.clk.Now()
	var result []ProducerSnapshot
	for _, ps := range m.producers {
		if ps.TxnState != TxnOngoing || ps.TxnTimeoutMs <= 0 {
			continue
		}
		deadline := ps.TxnStartTime.Add(time.Duration(ps.TxnTimeoutMs) * time.Millisecond)
		if now.After(deadline) {
			result = append(result, snapshotProducer(ps))
		}
	}
	return result
}
