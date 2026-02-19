package cluster

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/klaudworks/klite/internal/metadata"
	"github.com/klaudworks/klite/internal/wal"
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

// S3Fetcher is the interface for reading from S3 in the fetch cascade.
// Implemented by s3.Reader. Kept as an interface to avoid circular imports.
type S3Fetcher interface {
	FetchBatches(ctx context.Context, topic string, partition int32, offset int64, maxBytes int32) ([]S3BatchData, error)
}

// S3BatchData holds batch data returned from S3.
type S3BatchData struct {
	RawBytes        []byte
	BaseOffset      int64
	LastOffsetDelta int32
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

// pendingBatch holds a batch waiting for sequential commit ordering.
type pendingBatch struct {
	StoredBatch
}

// PartData holds the state for a single partition.
// Each partition has its own RWMutex. Produce takes a write lock,
// Fetch takes a read lock. Different partitions are fully concurrent.
//
// Lock ordering: compactionMu → mu
// Always acquire compactionMu before mu. Never hold mu when acquiring
// compactionMu. Callers of AdvanceLogStartOffset must hold compactionMu.
//
// Callers are responsible for holding the appropriate lock before
// calling methods. Write methods (PushBatch, ReserveOffset, CommitBatch)
// require mu.Lock(). Read methods (FetchFrom, SearchOffset, ListOffsets)
// require mu.RLock().
//
// Phase 1 (ring==nil): uses batches slice, PushBatch does assign+store.
// Phase 3 (ring!=nil): uses ring buffer, ReserveOffset+CommitBatch flow.
type PartData struct {
	CompactionMu sync.Mutex   // held by compaction, retention, or DeleteRecords
	mu           sync.RWMutex // protects all fields below

	Topic    string
	Index    int32
	TopicID  [16]byte      // Topic UUID, used for WAL entries
	batches  []StoredBatch // Phase 1: unbounded append-only slice (used when ring==nil)
	logStart int64         // Earliest available offset (0 initially)
	hw       int64         // High watermark = next offset to assign (= LEO for single broker)

	// For ListOffsets timestamp -3 (KIP-734): index of batch with max timestamp
	maxTimestampBatchIdx int // -1 if no batches

	// Fetch waiters (long polling) -- see 09-fetch.md
	waiterMu sync.Mutex      // separate lock for waiter list (not RWMutex)
	waiters  []*FetchWaiter  // each waiter holds a shared wake channel

	// Phase 3+ fields (set when WAL is enabled)
	ring               *RingBuffer    // in-memory ring buffer for recent batches
	walWriter          *wal.Writer    // reference to WAL writer (nil in Phase 1)
	walIndex           *wal.Index     // reference to WAL index for read-path lookups
	nextReserve        int64          // next offset to hand out (advances on reserve)
	nextCommit         int64          // next offset expected in commitBatch
	pendingCommits     []pendingBatch // out-of-order commits waiting for earlier ones
	totalBytes         int64          // running total of raw RecordBatch bytes
	maxTimestampVal    int64          // max timestamp value (ring buffer mode)
	maxTimestampOffset int64          // offset associated with max timestamp (ring buffer mode)

	// Phase 4 (S3)
	s3FlushWatermark int64          // highest offset+1 flushed to S3
	s3Fetch          S3Fetcher      // S3 reader for tier 3 reads (nil if S3 not configured)

	// Phase 4 (Transactions)
	abortedTxns []AbortedTxnEntry // aborted transaction index, sorted by LastOffset
	openTxnPIDs map[int64]int64   // producerID -> first offset of open txn on this partition

	// Phase 6 (Compaction)
	cleanedUpTo   int64     // highest offset that has been compacted (persisted via metadata.log)
	dirtyObjects  int32     // objects flushed since last compaction (volatile)
	lastCompacted time.Time // wall clock time of last successful compaction (volatile)
}

// HW returns the current high watermark (next offset to be assigned).
// Caller must hold pd.mu.RLock() or pd.mu.Lock().
func (pd *PartData) HW() int64 {
	return pd.hw
}

// CleanedUpTo returns the highest offset that has been compacted.
// Caller must hold pd.mu.RLock() or pd.mu.Lock().
func (pd *PartData) CleanedUpTo() int64 {
	return pd.cleanedUpTo
}

// SetCleanedUpTo sets the compaction watermark.
// Caller must hold pd.mu.Lock().
func (pd *PartData) SetCleanedUpTo(offset int64) {
	if offset > pd.cleanedUpTo {
		pd.cleanedUpTo = offset
	}
}

// DirtyObjects returns the number of dirty S3 objects since last compaction.
func (pd *PartData) DirtyObjects() int32 {
	pd.mu.RLock()
	defer pd.mu.RUnlock()
	return pd.dirtyObjects
}

// IncrementDirtyObjects atomically increments the dirty object counter.
// Called after S3 flush. Caller must hold pd.mu.Lock().
func (pd *PartData) IncrementDirtyObjects() {
	pd.dirtyObjects++
}

// ResetDirtyObjects resets the dirty counter and updates lastCompacted.
// Caller must hold pd.mu.Lock().
func (pd *PartData) ResetDirtyObjects(now time.Time) {
	pd.dirtyObjects = 0
	pd.lastCompacted = now
}

// SetDirtyObjects sets the dirty object count (used during startup rehydration).
// Caller must hold pd.mu.Lock().
func (pd *PartData) SetDirtyObjects(count int32) {
	pd.dirtyObjects = count
}

// LastCompacted returns the time of last successful compaction.
func (pd *PartData) LastCompacted() time.Time {
	pd.mu.RLock()
	defer pd.mu.RUnlock()
	return pd.lastCompacted
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

// InitWAL initializes the WAL-aware fields on this partition.
// Called when the broker enables WAL persistence.
func (pd *PartData) InitWAL(ring *RingBuffer, writer *wal.Writer, idx *wal.Index) {
	pd.ring = ring
	pd.walWriter = writer
	pd.walIndex = idx
	pd.nextReserve = pd.hw
	pd.nextCommit = pd.hw
}

// SetHW sets the high watermark directly. Used during WAL replay.
// Caller must hold pd.mu.Lock().
func (pd *PartData) SetHW(hw int64) {
	pd.hw = hw
	if pd.nextReserve < hw {
		pd.nextReserve = hw
	}
	if pd.nextCommit < hw {
		pd.nextCommit = hw
	}
}

// HasWAL returns whether this partition has WAL enabled.
func (pd *PartData) HasWAL() bool {
	return pd.ring != nil
}

// S3FlushWatermark returns the highest offset+1 flushed to S3.
// Caller must hold pd.mu.RLock() or pd.mu.Lock().
func (pd *PartData) S3FlushWatermark() int64 {
	return pd.s3FlushWatermark
}

// SetS3FlushWatermark sets the S3 flush watermark.
// Caller must hold pd.mu.Lock().
func (pd *PartData) SetS3FlushWatermark(w int64) {
	if w > pd.s3FlushWatermark {
		pd.s3FlushWatermark = w
	}
}

// SetS3Fetcher sets the S3 fetcher for tier 3 reads.
func (pd *PartData) SetS3Fetcher(f S3Fetcher) {
	pd.s3Fetch = f
}

// HasS3 returns whether S3 is configured for this partition.
func (pd *PartData) HasS3() bool {
	return pd.s3Fetch != nil
}

// PushBatch appends a batch, assigns offset. Returns base offset.
// Caller must hold pd.mu.Lock(). After releasing mu, caller should
// call pd.NotifyWaiters() to wake long-polling Fetch requests.
//
// Phase 1 path (no WAL): assigns offset, stores in batches slice.
// When ring is set, use ReserveOffset + CommitBatch instead.
func (pd *PartData) PushBatch(raw []byte, meta BatchMeta) int64 {
	baseOffset := pd.hw

	// Make a copy of the raw bytes so we own them (the caller's buffer may be reused)
	stored := make([]byte, len(raw))
	copy(stored, raw)

	// Assign server-side base offset and PartitionLeaderEpoch
	AssignOffset(stored, baseOffset)

	// Advance high watermark
	pd.hw = baseOffset + int64(meta.LastOffsetDelta) + 1

	batch := StoredBatch{
		BaseOffset:      baseOffset,
		LastOffsetDelta: meta.LastOffsetDelta,
		RawBytes:        stored,
		MaxTimestamp:    meta.MaxTimestamp,
		NumRecords:      meta.NumRecords,
	}

	if pd.ring != nil {
		// Phase 3: use ring buffer
		pd.ring.Push(batch)
		pd.nextReserve = pd.hw
		pd.nextCommit = pd.hw
		pd.totalBytes += int64(len(stored))
	} else {
		// Phase 1: unbounded slice
		pd.batches = append(pd.batches, batch)
	}

	// Update maxTimestamp tracking for ListOffsets -3 (KIP-734)
	if pd.ring != nil {
		pd.updateMaxTimestampRing(batch)
	} else {
		batchIdx := len(pd.batches) - 1
		if pd.maxTimestampBatchIdx < 0 || meta.MaxTimestamp >= pd.batches[pd.maxTimestampBatchIdx].MaxTimestamp {
			pd.maxTimestampBatchIdx = batchIdx
		}
	}

	return baseOffset
}

// ReserveOffset reserves an offset range for a batch without storing data.
// Called under pd.mu.Lock(). Returns the assigned base offset.
// After this call, the caller should write to WAL (without holding the lock),
// wait for fsync, then call CommitBatch.
func (pd *PartData) ReserveOffset(meta BatchMeta) int64 {
	base := pd.nextReserve
	pd.nextReserve = base + int64(meta.LastOffsetDelta) + 1
	return base
}

// CommitBatch stores a batch in the ring buffer after WAL fsync completes.
// Called under pd.mu.Lock(). Advances HW in strict offset order.
func (pd *PartData) CommitBatch(batch StoredBatch) {
	if batch.BaseOffset == pd.nextCommit {
		// In order: apply immediately
		pd.ring.Push(batch)
		pd.totalBytes += int64(len(batch.RawBytes))
		pd.nextCommit = batch.BaseOffset + int64(batch.LastOffsetDelta) + 1
		pd.hw = pd.nextCommit
		pd.updateMaxTimestampRing(batch)

		// Drain any queued commits that are now in order
		for len(pd.pendingCommits) > 0 && pd.pendingCommits[0].BaseOffset == pd.nextCommit {
			next := pd.pendingCommits[0].StoredBatch
			pd.pendingCommits = pd.pendingCommits[1:]
			pd.ring.Push(next)
			pd.totalBytes += int64(len(next.RawBytes))
			pd.nextCommit = next.BaseOffset + int64(next.LastOffsetDelta) + 1
			pd.hw = pd.nextCommit
			pd.updateMaxTimestampRing(next)
		}
	} else {
		// Out of order: queue until earlier offsets commit
		pd.insertPendingCommit(batch)
	}
}

// insertPendingCommit adds a batch to the pending commits queue in sorted order.
func (pd *PartData) insertPendingCommit(batch StoredBatch) {
	pb := pendingBatch{StoredBatch: batch}
	idx := sort.Search(len(pd.pendingCommits), func(i int) bool {
		return pd.pendingCommits[i].BaseOffset > batch.BaseOffset
	})
	pd.pendingCommits = append(pd.pendingCommits, pendingBatch{})
	copy(pd.pendingCommits[idx+1:], pd.pendingCommits[idx:])
	pd.pendingCommits[idx] = pb
}

// updateMaxTimestampRing updates max timestamp tracking for ring buffer mode.
// Uses the ring buffer's newest offset as the reference.
func (pd *PartData) updateMaxTimestampRing(batch StoredBatch) {
	if pd.maxTimestampBatchIdx < 0 {
		// Use a sentinel value that means "check ring buffer"
		pd.maxTimestampBatchIdx = 0
		pd.maxTimestampVal = batch.MaxTimestamp
		pd.maxTimestampOffset = batch.BaseOffset + int64(batch.LastOffsetDelta)
	} else if batch.MaxTimestamp >= pd.maxTimestampVal {
		pd.maxTimestampVal = batch.MaxTimestamp
		pd.maxTimestampOffset = batch.BaseOffset + int64(batch.LastOffsetDelta)
	}
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
// Phase 1 only (uses batches slice). In Phase 3, use FetchFrom directly.
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
//
// In Phase 3 (ring != nil), implements the three-tier read cascade:
// 1. Ring buffer (memory)
// 2. WAL index + pread
// 3. (Phase 4: S3 range read)
func (pd *PartData) FetchFrom(offset int64, maxBytes int32) []StoredBatch {
	if offset >= pd.hw {
		return nil
	}

	if pd.ring != nil {
		return pd.fetchFromTiered(offset, maxBytes)
	}

	return pd.fetchFromSlice(offset, maxBytes)
}

// fetchFromSlice is the Phase 1 fetch path using the batches slice.
func (pd *PartData) fetchFromSlice(offset int64, maxBytes int32) []StoredBatch {
	if len(pd.batches) == 0 {
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

// fetchFromTiered implements the Phase 3 three-tier read cascade.
func (pd *PartData) fetchFromTiered(offset int64, maxBytes int32) []StoredBatch {
	// Tier 1: Ring buffer (memory) — only if the requested offset is within the ring
	oldest := pd.ring.OldestOffset()
	if oldest >= 0 && offset >= oldest {
		if batches := pd.ring.CollectFrom(offset, maxBytes); len(batches) > 0 {
			return batches
		}
	}

	// Determine the lowest offset available in WAL. If the requested offset
	// is below the WAL range and S3 is available, skip WAL and try S3 first.
	// This handles the disaster recovery case where WAL was deleted but S3
	// still has the old data.
	walHasOffset := false
	if pd.walIndex != nil && pd.walWriter != nil {
		tp := wal.TopicPartition{TopicID: pd.TopicID, Partition: pd.Index}
		entries := pd.walIndex.Lookup(tp, offset, maxBytes)
		if len(entries) > 0 {
			// Only use WAL if its entries actually start at or near the
			// requested offset. If the first entry's BaseOffset is far
			// beyond the requested offset, the data for the requested
			// offset is not in the WAL — try S3 first.
			if entries[0].BaseOffset <= offset || pd.s3Fetch == nil {
				walHasOffset = true
				var result []StoredBatch
				for _, e := range entries {
					data, err := pd.walWriter.ReadBatch(e)
					if err != nil {
						break
					}
					meta, err := ParseBatchHeader(data)
					if err != nil {
						break
					}
					result = append(result, StoredBatch{
						BaseOffset:      e.BaseOffset,
						LastOffsetDelta: meta.LastOffsetDelta,
						RawBytes:        data,
						MaxTimestamp:    meta.MaxTimestamp,
						NumRecords:      meta.NumRecords,
					})
				}
				if len(result) > 0 {
					return result
				}
			}
		}
	}

	// Tier 3 (Phase 4): S3 range read
	if pd.s3Fetch != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		s3Batches, err := pd.s3Fetch.FetchBatches(ctx, pd.Topic, pd.Index, offset, maxBytes)
		if err == nil && len(s3Batches) > 0 {
			var result []StoredBatch
			for _, sb := range s3Batches {
				meta, parseErr := ParseBatchHeader(sb.RawBytes)
				if parseErr != nil {
					continue
				}
				result = append(result, StoredBatch{
					BaseOffset:      sb.BaseOffset,
					LastOffsetDelta: sb.LastOffsetDelta,
					RawBytes:        sb.RawBytes,
					MaxTimestamp:    meta.MaxTimestamp,
					NumRecords:      meta.NumRecords,
				})
			}
			if len(result) > 0 {
				return result
			}
		}
	}

	// Fallback: if S3 didn't have data but WAL does (for a higher range), use WAL
	if !walHasOffset && pd.walIndex != nil && pd.walWriter != nil {
		tp := wal.TopicPartition{TopicID: pd.TopicID, Partition: pd.Index}
		entries := pd.walIndex.Lookup(tp, offset, maxBytes)
		if len(entries) > 0 {
			var result []StoredBatch
			for _, e := range entries {
				data, err := pd.walWriter.ReadBatch(e)
				if err != nil {
					break
				}
				meta, err := ParseBatchHeader(data)
				if err != nil {
					break
				}
				result = append(result, StoredBatch{
					BaseOffset:      e.BaseOffset,
					LastOffsetDelta: meta.LastOffsetDelta,
					RawBytes:        data,
					MaxTimestamp:    meta.MaxTimestamp,
					NumRecords:      meta.NumRecords,
				})
			}
			if len(result) > 0 {
				return result
			}
		}
	}
	return nil
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
		if pd.ring != nil {
			// Ring buffer mode
			if pd.maxTimestampBatchIdx < 0 {
				return -1, -1
			}
			return pd.maxTimestampOffset, pd.maxTimestampVal
		}
		// Phase 1 slice mode
		if len(pd.batches) == 0 || pd.maxTimestampBatchIdx < 0 {
			return -1, -1
		}
		b := &pd.batches[pd.maxTimestampBatchIdx]
		return b.BaseOffset + int64(b.LastOffsetDelta), b.MaxTimestamp
	default: // timestamp >= 0: find first batch with MaxTimestamp >= timestamp
		if pd.ring != nil {
			return pd.listOffsetsRingTimestamp(timestamp)
		}
		if len(pd.batches) == 0 {
			return -1, -1
		}
		for i := range pd.batches {
			if pd.batches[i].MaxTimestamp >= timestamp {
				return pd.batches[i].BaseOffset, pd.batches[i].MaxTimestamp
			}
		}
		return -1, -1
	}
}

// listOffsetsRingTimestamp scans the ring buffer for a timestamp match.
func (pd *PartData) listOffsetsRingTimestamp(timestamp int64) (int64, int64) {
	if pd.ring == nil || pd.ring.Len() == 0 {
		return -1, -1
	}
	for seq := pd.ring.head; seq < pd.ring.tail; seq++ {
		slot := int(seq % int64(pd.ring.capacity))
		b := pd.ring.batches[slot]
		if b.MaxTimestamp >= timestamp {
			return b.BaseOffset, b.MaxTimestamp
		}
	}
	// TODO: Phase 3+ could also check WAL index for older data
	return -1, -1
}

// LSO returns the last stable offset: min(HW, oldest open transaction's first offset).
// For read_committed consumers. Caller must hold pd.mu.RLock().
func (pd *PartData) LSO() int64 {
	if len(pd.openTxnPIDs) == 0 {
		return pd.hw
	}
	lso := pd.hw
	for _, firstOffset := range pd.openTxnPIDs {
		if firstOffset < lso {
			lso = firstOffset
		}
	}
	return lso
}

// AddOpenTxn records that a transactional producer has started writing to this partition.
// Caller must hold pd.mu.Lock().
func (pd *PartData) AddOpenTxn(producerID int64, firstOffset int64) {
	if pd.openTxnPIDs == nil {
		pd.openTxnPIDs = make(map[int64]int64)
	}
	if _, exists := pd.openTxnPIDs[producerID]; !exists {
		pd.openTxnPIDs[producerID] = firstOffset
	}
}

// RemoveOpenTxn removes the open transaction tracking for a producer.
// Caller must hold pd.mu.Lock().
func (pd *PartData) RemoveOpenTxn(producerID int64) {
	delete(pd.openTxnPIDs, producerID)
}

// AddAbortedTxn adds an aborted transaction entry to this partition.
// Caller must hold pd.mu.Lock().
func (pd *PartData) AddAbortedTxn(entry AbortedTxnEntry) {
	pd.abortedTxns = append(pd.abortedTxns, entry)
}

// AbortedTxnsInRange returns aborted transactions overlapping the given offset range.
// Caller must hold pd.mu.RLock().
func (pd *PartData) AbortedTxnsInRange(fetchOffset int64, lastOffset int64) []AbortedTxnEntry {
	var result []AbortedTxnEntry
	for _, e := range pd.abortedTxns {
		if e.LastOffset < fetchOffset {
			continue
		}
		if e.FirstOffset >= lastOffset {
			continue
		}
		result = append(result, e)
	}
	return result
}

// RetentionScan evaluates time-based and size-based retention policies on this
// partition under a read lock. Returns the new logStartOffset to advance to, or
// the current logStart if no trimming is needed.
//
// retentionMs < 0 means infinite (skip time check).
// retentionBytes < 0 means infinite (skip size check).
// nowMs is the current wall clock time in milliseconds.
// Caller must NOT hold pd.mu.
func (pd *PartData) RetentionScan(retentionMs int64, retentionBytes int64, nowMs int64) (newLogStart int64, origLogStart int64) {
	pd.mu.RLock()
	defer pd.mu.RUnlock()

	origLogStart = pd.logStart
	newLogStart = origLogStart
	var bytesToDrop int64

	cutoff := nowMs - retentionMs // only meaningful if retentionMs >= 0

	if pd.ring != nil {
		// Ring buffer mode: also scan WAL index entries for older data
		// For now, scan ring buffer batches (most recent data).
		// WAL index entries are not included in the retention scan because
		// we don't store MaxTimestamp in the index. Ring buffer + in-memory
		// batches are what we can scan.
		for seq := pd.ring.head; seq < pd.ring.tail; seq++ {
			slot := int(seq % int64(pd.ring.capacity))
			b := pd.ring.batches[slot]
			if b.RawBytes == nil {
				continue
			}
			lastOffset := b.BaseOffset + int64(b.LastOffsetDelta)
			shouldTrim := false

			if retentionMs >= 0 && b.MaxTimestamp < cutoff {
				shouldTrim = true
			}
			if retentionBytes >= 0 && (pd.totalBytes-bytesToDrop) > retentionBytes {
				shouldTrim = true
			}

			if shouldTrim {
				newLogStart = lastOffset + 1
				bytesToDrop += int64(len(b.RawBytes))
			} else {
				break
			}
		}
	} else {
		// Phase 1 slice mode
		for i := range pd.batches {
			b := &pd.batches[i]
			lastOffset := b.BaseOffset + int64(b.LastOffsetDelta)
			shouldTrim := false

			totalBytesForCalc := pd.TotalBytes()
			if retentionMs >= 0 && b.MaxTimestamp < cutoff {
				shouldTrim = true
			}
			if retentionBytes >= 0 && (totalBytesForCalc-bytesToDrop) > retentionBytes {
				shouldTrim = true
			}

			if shouldTrim {
				newLogStart = lastOffset + 1
				bytesToDrop += int64(len(b.RawBytes))
			} else {
				break
			}
		}
	}

	return newLogStart, origLogStart
}

// BatchCount returns the number of stored batches.
// Caller must hold pd.mu.RLock().
func (pd *PartData) BatchCount() int {
	if pd.ring != nil {
		return pd.ring.Len()
	}
	return len(pd.batches)
}

// TotalBytes returns the total bytes of stored batch data.
// Caller must hold pd.mu.RLock().
func (pd *PartData) TotalBytes() int64 {
	if pd.ring != nil {
		return pd.totalBytes
	}
	var total int64
	for i := range pd.batches {
		total += int64(len(pd.batches[i].RawBytes))
	}
	return total
}

// AdvanceLogStart advances the log start offset without metadata.log persistence.
// Used by DeleteRecords when metadata.log is not available (Phase 1 mode).
// Caller must hold pd.mu.Lock().
func (pd *PartData) AdvanceLogStart(offset int64) {
	if offset <= pd.logStart {
		return
	}
	if offset > pd.hw {
		offset = pd.hw
	}
	pd.trimBatchesLocked(offset)
}

// AdvanceLogStartOffset advances the partition's logStartOffset to newOffset,
// trims old batches, persists the change to metadata.log, prunes the WAL
// index, and updates the partition size counter.
//
// Called by: retention enforcement, DeleteRecords handler.
// Caller must NOT hold pd.mu (this method acquires it internally).
// Caller must hold pd.CompactionMu.
//
// If metaLog is nil, the persistence step is skipped (Phase 1 / tests).
func (pd *PartData) AdvanceLogStartOffset(newOffset int64, metaLog *metadata.Log) error {
	pd.mu.Lock()
	if pd.logStart >= newOffset {
		pd.mu.Unlock()
		return nil
	}
	if newOffset > pd.hw {
		newOffset = pd.hw
	}

	// Persist to metadata.log FIRST, while still holding pd.mu.
	if metaLog != nil {
		entry := metadata.MarshalLogStartOffset(&metadata.LogStartOffsetEntry{
			TopicName:      pd.Topic,
			Partition:      pd.Index,
			LogStartOffset: newOffset,
		})
		if err := metaLog.AppendSync(entry); err != nil {
			pd.mu.Unlock()
			return err
		}
	}

	// Trim all batches below newOffset and set logStart.
	pd.trimBatchesLocked(newOffset)
	pd.mu.Unlock()

	// Prune WAL index entries for this partition below newOffset.
	if pd.walIndex != nil {
		tp := wal.TopicPartition{TopicID: pd.TopicID, Partition: pd.Index}
		pd.walIndex.PruneBefore(tp, newOffset)
	}

	return nil
}

// trimBatchesLocked removes all batches whose last offset is below
// newLogStart, then sets pd.logStart = newLogStart. Updates pd.totalBytes.
// Caller must hold pd.mu.Lock().
func (pd *PartData) trimBatchesLocked(newLogStart int64) {
	if pd.ring != nil {
		// Ring buffer mode: advance ring.head past trimmed batches.
		for pd.ring.head < pd.ring.tail {
			slot := int(pd.ring.head % int64(pd.ring.capacity))
			b := pd.ring.batches[slot]
			lastOffset := b.BaseOffset + int64(b.LastOffsetDelta)
			if lastOffset >= newLogStart {
				break
			}
			pd.totalBytes -= int64(len(b.RawBytes))
			pd.ring.batches[slot] = StoredBatch{} // zero out for GC
			pd.ring.head++
		}
	} else {
		// Phase 1 slice mode: trim batches
		trimIdx := 0
		for trimIdx < len(pd.batches) {
			b := &pd.batches[trimIdx]
			lastOffset := b.BaseOffset + int64(b.LastOffsetDelta)
			if lastOffset < newLogStart {
				trimIdx++
			} else {
				break
			}
		}

		if trimIdx > 0 {
			copy(pd.batches, pd.batches[trimIdx:])
			for i := len(pd.batches) - trimIdx; i < len(pd.batches); i++ {
				pd.batches[i] = StoredBatch{} // zero out for GC
			}
			pd.batches = pd.batches[:len(pd.batches)-trimIdx]

			// Recalculate maxTimestampBatchIdx
			pd.maxTimestampBatchIdx = -1
			for i := range pd.batches {
				if pd.maxTimestampBatchIdx < 0 || pd.batches[i].MaxTimestamp >= pd.batches[pd.maxTimestampBatchIdx].MaxTimestamp {
					pd.maxTimestampBatchIdx = i
				}
			}
		}
	}

	pd.logStart = newLogStart
}


