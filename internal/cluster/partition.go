package cluster

import (
	"sort"
	"sync"
)

// StoredBatch holds raw bytes and metadata extracted from the batch header.
// Used for both in-memory storage and as the return type from WAL/S3 reads.
type StoredBatch struct {
	BaseOffset      int64
	LastOffsetDelta int32
	RawBytes        []byte // Original RecordBatch bytes from the client
	MaxTimestamp    int64
	NumRecords      int32
}

// FetchWaiter represents a Fetch request waiting for new data (long polling).
// A single FetchWaiter is shared across multiple partitions. Multiple partitions
// may call CloseOnce concurrently — sync.Once prevents double-close panic.
type FetchWaiter struct {
	once sync.Once       // ensures ch is closed exactly once
	ch   chan struct{}    // closed when data arrives (wake-up signal only, no data)
}

// NewFetchWaiter creates a new fetch waiter with a fresh wake channel.
func NewFetchWaiter() *FetchWaiter {
	return &FetchWaiter{ch: make(chan struct{})}
}

// Ch returns the wake-up channel. Select on this to be notified when data arrives.
func (w *FetchWaiter) Ch() <-chan struct{} {
	return w.ch
}

// CloseOnce safely closes the wake channel. Multiple partitions may call this
// concurrently for the same shared waiter — sync.Once prevents double-close panic.
func (w *FetchWaiter) CloseOnce() {
	w.once.Do(func() { close(w.ch) })
}

// PartData holds the state for a single partition.
// Each partition has its own RWMutex. Produce takes a write lock,
// Fetch takes a read lock. Different partitions are fully concurrent.
//
// Callers are responsible for holding the appropriate lock before
// calling methods. Write methods (PushBatch) require mu.Lock().
// Read methods (FetchFrom, SearchOffset, ListOffsets) require mu.RLock().
type PartData struct {
	mu sync.RWMutex // protects all fields below

	Topic    string
	Index    int32
	batches  []StoredBatch // Phase 1: unbounded append-only slice
	logStart int64         // Earliest available offset (0 initially)
	hw       int64         // High watermark = next offset to assign (= LEO for single broker)

	// For ListOffsets timestamp -3 (KIP-734): index of batch with max timestamp
	maxTimestampBatchIdx int // -1 if no batches

	// Fetch waiters (long polling) -- see 09-fetch.md
	waiterMu sync.Mutex      // separate lock for waiter list (not RWMutex)
	waiters  []*FetchWaiter  // each waiter holds a shared wake channel
}

// HW returns the current high watermark (next offset to be assigned).
// Caller must hold pd.mu.RLock() or pd.mu.Lock().
func (pd *PartData) HW() int64 {
	return pd.hw
}

// LogStart returns the log start offset.
// Caller must hold pd.mu.RLock() or pd.mu.Lock().
func (pd *PartData) LogStart() int64 {
	return pd.logStart
}

// Lock acquires the partition write lock.
func (pd *PartData) Lock() {
	pd.mu.Lock()
}

// Unlock releases the partition write lock.
func (pd *PartData) Unlock() {
	pd.mu.Unlock()
}

// RLock acquires the partition read lock.
func (pd *PartData) RLock() {
	pd.mu.RLock()
}

// RUnlock releases the partition read lock.
func (pd *PartData) RUnlock() {
	pd.mu.RUnlock()
}

// PushBatch appends a batch, assigns offset. Returns base offset.
// Caller must hold pd.mu.Lock(). After releasing mu, caller should
// call pd.NotifyWaiters() to wake long-polling Fetch requests.
func (pd *PartData) PushBatch(raw []byte, meta BatchMeta) int64 {
	baseOffset := pd.hw

	// Make a copy of the raw bytes so we own them (the caller's buffer may be reused)
	stored := make([]byte, len(raw))
	copy(stored, raw)

	// Assign server-side base offset and PartitionLeaderEpoch
	assignOffset(stored, baseOffset)

	// Advance high watermark
	pd.hw = baseOffset + int64(meta.LastOffsetDelta) + 1

	batch := StoredBatch{
		BaseOffset:      baseOffset,
		LastOffsetDelta: meta.LastOffsetDelta,
		RawBytes:        stored,
		MaxTimestamp:    meta.MaxTimestamp,
		NumRecords:      meta.NumRecords,
	}

	pd.batches = append(pd.batches, batch)

	// Update maxTimestamp tracking for ListOffsets -3 (KIP-734)
	batchIdx := len(pd.batches) - 1
	if pd.maxTimestampBatchIdx < 0 || meta.MaxTimestamp >= pd.batches[pd.maxTimestampBatchIdx].MaxTimestamp {
		pd.maxTimestampBatchIdx = batchIdx
	}

	return baseOffset
}

// NotifyWaiters wakes all registered fetch waiters.
// Caller must NOT hold pd.mu (takes pd.waiterMu internally).
func (pd *PartData) NotifyWaiters() {
	pd.waiterMu.Lock()
	waiters := pd.waiters
	pd.waiters = nil
	pd.waiterMu.Unlock()

	for _, w := range waiters {
		w.CloseOnce() // safe even if another partition already closed it
	}
}

// RegisterWaiter registers a shared fetch waiter on this partition.
// The same waiter can be registered on multiple partitions (shared channel).
// Caller must NOT hold pd.mu.
func (pd *PartData) RegisterWaiter(w *FetchWaiter) {
	pd.waiterMu.Lock()
	pd.waiters = append(pd.waiters, w)
	pd.waiterMu.Unlock()
}

// SearchOffset finds the batch containing the given offset via binary search.
// Returns (index, found, atEnd).
//   - found=true: batches[index] contains the offset
//   - found=false, atEnd=true: offset >= HW (past the end)
//   - found=false, atEnd=false: offset < logStart (before start)
//
// Caller must hold pd.mu.RLock().
func (pd *PartData) SearchOffset(offset int64) (index int, found bool, atEnd bool) {
	if len(pd.batches) == 0 {
		return 0, false, true
	}

	if offset >= pd.hw {
		return len(pd.batches), false, true
	}

	if offset < pd.logStart {
		return 0, false, false
	}

	// Binary search: find the last batch whose BaseOffset <= offset
	idx := sort.Search(len(pd.batches), func(i int) bool {
		return pd.batches[i].BaseOffset > offset
	})
	// idx is the first batch with BaseOffset > offset, so idx-1 is the batch containing offset
	idx--

	if idx < 0 {
		return 0, false, false
	}

	// Verify the offset is within this batch's range
	b := &pd.batches[idx]
	lastOffset := b.BaseOffset + int64(b.LastOffsetDelta)
	if offset >= b.BaseOffset && offset <= lastOffset {
		return idx, true, false
	}

	// Should not happen with well-formed data, but handle gracefully
	return idx, false, false
}

// FetchFrom collects batches starting at offset, up to maxBytes.
// KIP-74: always includes at least one complete batch even if it exceeds maxBytes.
// Returns nil if no batches match.
// Caller must hold pd.mu.RLock().
func (pd *PartData) FetchFrom(offset int64, maxBytes int32) []StoredBatch {
	if len(pd.batches) == 0 || offset >= pd.hw {
		return nil
	}

	idx, found, _ := pd.SearchOffset(offset)
	if !found {
		// If offset equals logStart, start from the first batch
		if offset <= pd.logStart && len(pd.batches) > 0 {
			idx = 0
		} else {
			return nil
		}
	}

	var result []StoredBatch
	var totalBytes int32

	for i := idx; i < len(pd.batches); i++ {
		b := &pd.batches[i]
		batchSize := int32(len(b.RawBytes))

		// KIP-74: always include at least one batch
		if len(result) > 0 && totalBytes+batchSize > maxBytes {
			break
		}

		result = append(result, *b)
		totalBytes += batchSize

		// After including at least one batch, check byte limit
		if totalBytes >= maxBytes {
			break
		}
	}

	return result
}

// ListOffsets resolves a timestamp query for this partition.
//
//	timestamp -1 (Latest):       returns HW (next offset to be produced)
//	timestamp -2 (Earliest):     returns logStart
//	timestamp -3 (MaxTimestamp):  returns offset and timestamp of batch with max timestamp (KIP-734)
//	timestamp >= 0:              returns the offset of the first batch with MaxTimestamp >= timestamp
//
// Returns (offset, timestamp). For -1 and -2, timestamp is -1.
// Caller must hold pd.mu.RLock().
func (pd *PartData) ListOffsets(timestamp int64) (offset int64, ts int64) {
	switch timestamp {
	case -1: // Latest
		return pd.hw, -1
	case -2: // Earliest
		return pd.logStart, -1
	case -3: // MaxTimestamp (KIP-734)
		if len(pd.batches) == 0 || pd.maxTimestampBatchIdx < 0 {
			return pd.logStart, -1
		}
		b := &pd.batches[pd.maxTimestampBatchIdx]
		// Return the offset AFTER the batch (next offset), as per Kafka spec for MaxTimestamp
		return b.BaseOffset + int64(b.LastOffsetDelta) + 1, b.MaxTimestamp
	default: // timestamp >= 0: find first batch with MaxTimestamp >= timestamp
		if len(pd.batches) == 0 {
			return pd.hw, -1
		}

		// Linear scan — batches are in offset order, timestamps may not be
		// strictly ordered (CreateTime mode allows client-set timestamps).
		// For correctness, we find the first batch whose MaxTimestamp >= timestamp.
		for i := range pd.batches {
			if pd.batches[i].MaxTimestamp >= timestamp {
				return pd.batches[i].BaseOffset, pd.batches[i].MaxTimestamp
			}
		}

		// No batch has timestamp >= requested: return HW
		return pd.hw, -1
	}
}

// BatchCount returns the number of stored batches.
// Caller must hold pd.mu.RLock().
func (pd *PartData) BatchCount() int {
	return len(pd.batches)
}
