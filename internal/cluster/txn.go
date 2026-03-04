package cluster

import (
	"encoding/binary"
	"hash/crc32"
	"sync"
	"time"
)

// TxnState represents the state of a transactional producer.
type TxnState int8

const (
	TxnNone TxnState = iota
	TxnOngoing
)

// ProducerState holds per-producer state for idempotency and transactions.
type ProducerState struct {
	ProducerID int64
	Epoch      int16
	TxnID      string // empty for non-transactional

	// Per-partition sequence tracking (for dedup)
	Sequences map[TopicPartition]*SequenceWindow

	// Transaction state
	TxnState      TxnState
	TxnPartitions map[TopicPartition]bool           // partitions in current txn
	TxnBatches    []*TxnBatchRef                    // data batches written in current txn
	TxnGroups     []string                          // consumer groups in current txn (AddOffsetsToTxn)
	TxnOffsets    map[string]map[TopicPartition]PendingTxnOffset // group -> tp -> offset
	TxnFirstOffsets map[TopicPartition]int64         // first offset per partition in this txn
	TxnStartTime  time.Time
	TxnTimeoutMs  int32
	LastWasCommit bool
}

// PendingTxnOffset holds an offset commit pending transaction resolution.
type PendingTxnOffset struct {
	Offset      int64
	LeaderEpoch int32
	Metadata    string
}

// TxnBatchRef tracks a batch written as part of a transaction.
type TxnBatchRef struct {
	Topic     string
	Partition int32
	Offset    int64
}

// SequenceWindow implements a 5-slot ring buffer for dedup, matching Kafka's window.
type SequenceWindow struct {
	seq     [5]int32  // expected next sequence at each slot
	offsets [5]int64  // base offset for each slot
	at      uint8     // current write position
	epoch   int16     // last seen epoch
	filled  bool      // whether we've seen any writes
}

// PushAndValidate checks a batch's sequence against the window.
// Returns (ok, isDuplicate, dupOffset).
//   - ok=true, isDuplicate=false: new batch accepted, window updated
//   - ok=true, isDuplicate=true: duplicate detected, dupOffset is the original offset
//   - ok=false: out-of-order sequence
func (w *SequenceWindow) PushAndValidate(epoch int16, firstSeq int32, numRecords int32, baseOffset int64) (ok bool, isDuplicate bool, dupOffset int64) {
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

// AbortedTxnEntry tracks an aborted transaction on a partition.
type AbortedTxnEntry struct {
	ProducerID  int64
	FirstOffset int64 // first data offset of the txn on this partition
	LastOffset  int64 // offset of the abort control record
}

// ProducerIDManager manages producer IDs and transactional state.
type ProducerIDManager struct {
	mu        sync.Mutex
	nextPID   int64                      // monotonic counter
	producers map[int64]*ProducerState   // PID -> state
	byTxnID   map[string]*ProducerState  // txnID -> state
}

// NewProducerIDManager creates a new producer ID manager.
func NewProducerIDManager() *ProducerIDManager {
	return &ProducerIDManager{
		nextPID:   1,
		producers: make(map[int64]*ProducerState),
		byTxnID:   make(map[string]*ProducerState),
	}
}

// SetNextPID sets the next producer ID counter (used during replay).
func (m *ProducerIDManager) SetNextPID(next int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if next > m.nextPID {
		m.nextPID = next
	}
}

// NextPID returns the current next producer ID value (for snapshot).
func (m *ProducerIDManager) NextPID() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.nextPID
}

// InitProducerID handles the InitProducerID logic. Returns (producerID, epoch, errorCode).
// errorCode is 0 on success.
func (m *ProducerIDManager) InitProducerID(txnID string, txnTimeoutMs int32) (int64, int16, int16) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if txnID != "" {
		// Transactional producer
		if ps, ok := m.byTxnID[txnID]; ok {
			// Re-init: bump epoch, abort any open txn
			if ps.TxnState == TxnOngoing {
				// Will be aborted by the caller after getting the state
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

		// New transactional producer
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

	// Idempotent (non-transactional) producer
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

// GetProducer returns the producer state for the given PID, or nil.
func (m *ProducerIDManager) GetProducer(pid int64) *ProducerState {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.producers[pid]
}

// GetProducerByTxnID returns the producer state for the given transactional ID.
func (m *ProducerIDManager) GetProducerByTxnID(txnID string) *ProducerState {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.byTxnID[txnID]
}

// ValidateAndDedup validates a produce request's PID/epoch/sequence and checks for duplicates.
// Returns (errorCode, isDuplicate, dupBaseOffset).
// errorCode 0 means success.
func (m *ProducerIDManager) ValidateAndDedup(pid int64, epoch int16, tp TopicPartition, firstSeq int32, numRecords int32, baseOffset int64) (errCode int16, isDup bool, dupOffset int64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	ps, ok := m.producers[pid]
	if !ok {
		return 0, false, 0 // unknown PID — likely idempotent producer that restarted
	}

	if epoch != ps.Epoch {
		if epoch < ps.Epoch {
			return 35, false, 0 // PRODUCER_FENCED (kerr.ProducerFenced.Code)
		}
		return 47, false, 0 // INVALID_PRODUCER_EPOCH
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
// This is called after the actual offset is assigned (which may differ from the tentative one).
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
	// Find the slot that has this sequence and update its offset
	for i := 0; i < 5; i++ {
		if w.offsets[i] != actualOffset && w.seq[i] == firstSeq {
			w.offsets[i] = actualOffset
			return
		}
	}
	// Also check the most recently written slot (at-1)
	prev := (w.at + 4) % 5 // at-1 mod 5
	w.offsets[prev] = actualOffset
}

// AddPartitionsToTxn registers partitions as part of the current transaction.
// Returns error code (0 = success).
func (m *ProducerIDManager) AddPartitionsToTxn(pid int64, epoch int16, partitions []TopicPartition) int16 {
	m.mu.Lock()
	defer m.mu.Unlock()

	ps, ok := m.producers[pid]
	if !ok {
		return 3 // UNKNOWN_TOPIC_OR_PARTITION as proxy for invalid PID
	}
	if epoch != ps.Epoch {
		if epoch < ps.Epoch {
			return 35 // PRODUCER_FENCED
		}
		return 47 // INVALID_PRODUCER_EPOCH
	}
	if ps.TxnID == "" {
		return 49 // TRANSACTIONAL_ID_AUTHORIZATION_FAILED — not a transactional producer
	}

	if ps.TxnState != TxnOngoing {
		ps.TxnState = TxnOngoing
		ps.TxnStartTime = time.Now()
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

// AddOffsetsToTxn registers a consumer group's offsets partition in the transaction.
// Returns error code (0 = success).
func (m *ProducerIDManager) AddOffsetsToTxn(pid int64, epoch int16, groupID string) int16 {
	m.mu.Lock()
	defer m.mu.Unlock()

	ps, ok := m.producers[pid]
	if !ok {
		return 3
	}
	if epoch != ps.Epoch {
		if epoch < ps.Epoch {
			return 35
		}
		return 47
	}
	if ps.TxnID == "" {
		return 49
	}

	if ps.TxnState != TxnOngoing {
		ps.TxnState = TxnOngoing
		ps.TxnStartTime = time.Now()
		ps.TxnPartitions = make(map[TopicPartition]bool)
		ps.TxnBatches = nil
		ps.TxnGroups = nil
		ps.TxnOffsets = make(map[string]map[TopicPartition]PendingTxnOffset)
		ps.TxnFirstOffsets = make(map[TopicPartition]int64)
	}

	ps.TxnGroups = append(ps.TxnGroups, groupID)
	return 0
}

// StoreTxnOffset stages an offset commit for a transactional producer.
func (m *ProducerIDManager) StoreTxnOffset(pid int64, epoch int16, groupID, topic string, partition int32, offset int64, leaderEpoch int32, metadata string) int16 {
	m.mu.Lock()
	defer m.mu.Unlock()

	ps, ok := m.producers[pid]
	if !ok {
		return 3
	}
	if epoch != ps.Epoch {
		if epoch < ps.Epoch {
			return 35
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

// RecordTxnBatch records that a batch was written as part of a transaction.
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

// EndTxnState holds the state needed to complete a transaction.
type EndTxnState struct {
	ProducerID    int64
	Epoch         int16
	Commit        bool
	TxnPartitions map[TopicPartition]bool
	TxnGroups     []string
	TxnOffsets    map[string]map[TopicPartition]PendingTxnOffset
	TxnFirstOffsets map[TopicPartition]int64
}

// PrepareEndTxn validates and prepares the EndTxn operation.
// Returns the state needed for the caller to write control batches and apply offsets,
// or an error code if the request is invalid.
func (m *ProducerIDManager) PrepareEndTxn(pid int64, epoch int16, commit bool) (*EndTxnState, int16) {
	m.mu.Lock()
	defer m.mu.Unlock()

	ps, ok := m.producers[pid]
	if !ok {
		return nil, 3
	}
	if epoch != ps.Epoch {
		if epoch < ps.Epoch {
			return nil, 35 // PRODUCER_FENCED
		}
		return nil, 47 // INVALID_PRODUCER_EPOCH
	}
	if ps.TxnState != TxnOngoing {
		// Retry detection: previous EndTxn already completed
		if ps.LastWasCommit == commit {
			return &EndTxnState{
				ProducerID: ps.ProducerID,
				Epoch:      ps.Epoch,
				Commit:     commit,
			}, 0
		}
		return nil, 53 // INVALID_TXN_STATE
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

	// Reset producer's transaction state
	ps.TxnState = TxnNone
	ps.LastWasCommit = commit
	ps.resetTxnState()

	return state, 0
}

// resetTxnState clears transaction-specific state on a producer.
func (ps *ProducerState) resetTxnState() {
	ps.TxnPartitions = make(map[TopicPartition]bool)
	ps.TxnBatches = nil
	ps.TxnGroups = nil
	ps.TxnOffsets = make(map[string]map[TopicPartition]PendingTxnOffset)
	ps.TxnFirstOffsets = make(map[TopicPartition]int64)
	ps.TxnStartTime = time.Time{}
}

// AllTransactions returns information about all transactional producers.
func (m *ProducerIDManager) AllTransactions() []*ProducerState {
	m.mu.Lock()
	defer m.mu.Unlock()

	var result []*ProducerState
	for _, ps := range m.producers {
		if ps.TxnID != "" {
			result = append(result, ps)
		}
	}
	return result
}

// GetProducersForPartition returns active producers that have written to the given partition.
func (m *ProducerIDManager) GetProducersForPartition(topic string, partition int32) []*ProducerState {
	m.mu.Lock()
	defer m.mu.Unlock()

	tp := TopicPartition{Topic: topic, Partition: partition}
	var result []*ProducerState
	for _, ps := range m.producers {
		if _, ok := ps.Sequences[tp]; ok {
			result = append(result, ps)
		}
	}
	return result
}

// BuildControlBatch creates a control batch (commit or abort marker) as raw bytes.
// The batch is a valid RecordBatch with attributes bits 4+5 set.
func BuildControlBatch(producerID int64, producerEpoch int16, commit bool) []byte {
	now := time.Now().UnixMilli()

	// Control record key: version(int16=0) + type(int16: 0=abort, 1=commit)
	var controlType int16
	if commit {
		controlType = 1
	}

	// Build the control record value (just the 4-byte key)
	recordKey := make([]byte, 4)
	binary.BigEndian.PutUint16(recordKey[0:2], 0) // version=0
	binary.BigEndian.PutUint16(recordKey[2:4], uint16(controlType))

	// Build the record using varint encoding (Kafka record format)
	record := encodeRecord(recordKey, nil)

	// Build the RecordBatch
	// Total batch = 61 bytes header + record bytes
	numRecordBytes := len(record)
	totalBatch := 61 + numRecordBytes
	raw := make([]byte, totalBatch)

	// BaseOffset (bytes 0-7) — will be overwritten by offset assignment
	binary.BigEndian.PutUint64(raw[0:8], 0)
	// BatchLength (bytes 8-11) = total - 12
	binary.BigEndian.PutUint32(raw[8:12], uint32(totalBatch-12))
	// PartitionLeaderEpoch (bytes 12-15)
	binary.BigEndian.PutUint32(raw[12:16], 0)
	// Magic (byte 16)
	raw[16] = 2
	// CRC (bytes 17-20) — computed below
	// Attributes (bytes 21-22): bits 4+5 set (transactional + control)
	binary.BigEndian.PutUint16(raw[21:23], 0x0030)
	// LastOffsetDelta (bytes 23-26) = 0 (single record)
	binary.BigEndian.PutUint32(raw[23:27], 0)
	// BaseTimestamp (bytes 27-34)
	binary.BigEndian.PutUint64(raw[27:35], uint64(now))
	// MaxTimestamp (bytes 35-42)
	binary.BigEndian.PutUint64(raw[35:43], uint64(now))
	// ProducerID (bytes 43-50)
	binary.BigEndian.PutUint64(raw[43:51], uint64(producerID))
	// ProducerEpoch (bytes 51-52)
	binary.BigEndian.PutUint16(raw[51:53], uint16(producerEpoch))
	// BaseSequence (bytes 53-56) = -1 for control records
	binary.BigEndian.PutUint32(raw[53:57], 0xFFFFFFFF)
	// NumRecords (bytes 57-60)
	binary.BigEndian.PutUint32(raw[57:61], 1)
	// Records
	copy(raw[61:], record)

	// Compute CRC-32C over bytes 21+
	crcVal := crc32.Checksum(raw[21:], crc32cTable)
	binary.BigEndian.PutUint32(raw[17:21], crcVal)

	return raw
}

// encodeRecord encodes a single Kafka record in the v2 format (varint encoding).
func encodeRecord(key, value []byte) []byte {
	var buf []byte

	// We need to compute the record body first, then prepend the length as varint

	var body []byte
	// Attributes (int8) = 0
	body = append(body, 0)
	// TimestampDelta (varint) = 0
	body = appendVarint(body, 0)
	// OffsetDelta (varint) = 0
	body = appendVarint(body, 0)
	// Key length (varint)
	if key == nil {
		body = appendVarint(body, -1)
	} else {
		body = appendVarint(body, int64(len(key)))
		body = append(body, key...)
	}
	// Value length (varint)
	if value == nil {
		body = appendVarint(body, -1)
	} else {
		body = appendVarint(body, int64(len(value)))
		body = append(body, value...)
	}
	// Headers count (varint) = 0
	body = appendVarint(body, 0)

	// Prepend body length as varint
	buf = appendVarint(buf, int64(len(body)))
	buf = append(buf, body...)

	return buf
}

// appendVarint appends a zigzag-encoded varint to buf.
func appendVarint(buf []byte, v int64) []byte {
	// Zigzag encode
	uv := uint64((v << 1) ^ (v >> 63))
	for uv >= 0x80 {
		buf = append(buf, byte(uv)|0x80)
		uv >>= 7
	}
	buf = append(buf, byte(uv))
	return buf
}

// IsControlBatch returns true if the raw batch has the control bit set (bit 5 of Attributes).
func IsControlBatch(raw []byte) bool {
	if len(raw) < 23 {
		return false
	}
	attrs := binary.BigEndian.Uint16(raw[21:23])
	return attrs&0x0020 != 0
}

// IsTransactionalBatch returns true if the raw batch has the transactional bit set (bit 4).
func IsTransactionalBatch(raw []byte) bool {
	if len(raw) < 23 {
		return false
	}
	attrs := binary.BigEndian.Uint16(raw[21:23])
	return attrs&0x0010 != 0
}
