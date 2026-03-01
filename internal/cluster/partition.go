package cluster

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/klaudworks/klite/internal/chunk"
	"github.com/klaudworks/klite/internal/metadata"
	"github.com/klaudworks/klite/internal/wal"
)

// StoredBatch holds raw bytes and metadata extracted from the batch header.
// Used in ring buffers, WAL reads, and S3 reads.
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
	FetchBatches(ctx context.Context, topic string, topicID [16]byte, partition int32, offset int64, maxBytes int32) ([]S3BatchData, error)
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
	once sync.Once     // ensures ch is closed exactly once
	ch   chan struct{} // closed when data arrives (wake-up signal only, no data)
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
// Lock ordering: compactionMu -> mu
// Always acquire compactionMu before mu. Never hold mu when acquiring
// compactionMu. Callers of AdvanceLogStartOffset must hold compactionMu.
//
// Callers are responsible for holding the appropriate lock before
// calling methods. Write methods (PushBatch, ReserveOffset, CommitBatch)
// require mu.Lock(). Read methods (FetchFrom, ListOffsets) require mu.RLock().
//
// Storage: uses chunk pool for recent batches in memory, WAL on disk,
// and optionally S3 for older data. The produce path uses
// ReserveOffset + appendToChunk + WAL append + CommitBatch for durability.
type PartData struct {
	CompactionMu sync.Mutex   // held by compaction, retention, or DeleteRecords
	mu           sync.RWMutex // protects all fields below

	Topic    string
	Index    int32
	TopicID  [16]byte // Topic UUID, used for WAL entries
	logStart int64    // Earliest available offset (0 initially)
	hw       int64    // High watermark = next offset to assign (= LEO for single broker)

	hasMaxTimestamp bool // false until first batch committed (for ListOffsets -3 / KIP-734)

	// Fetch waiters (long polling)
	waiterMu sync.Mutex     // separate lock for waiter list (not RWMutex)
	waiters  []*FetchWaiter // each waiter holds a shared wake channel

	// Chunk pool state (replaces ring buffer)
	chunkPool    *chunk.Pool    // reference to global pool (nil if not configured)
	chunkCurrent *chunk.Chunk   // chunk being written to (nil until first produce)
	chunkSealed  []*chunk.Chunk // full chunks awaiting S3 flush, ordered by write time

	walWriter          *wal.Writer    // reference to WAL writer
	walIndex           *wal.Index     // reference to WAL index for read-path lookups
	nextReserve        int64          // next offset to hand out (advances on reserve)
	nextCommit         int64          // next offset expected in commitBatch
	pendingCommits     []pendingBatch // out-of-order commits waiting for earlier ones
	totalBytes         int64          // running total of raw RecordBatch bytes
	maxTimestampVal    int64          // max timestamp value
	maxTimestampOffset int64          // offset associated with max timestamp

	// Phase 4 (S3)
	s3FlushWatermark int64     // highest offset+1 flushed to S3
	s3Fetch          S3Fetcher // S3 reader for tier 3 reads (nil if S3 not configured)

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
func (pd *PartData) InitWAL(pool *chunk.Pool, writer *wal.Writer, idx *wal.Index) {
	pd.chunkPool = pool
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
// Used by WAL replay and transaction control batches (EndTxn).
// The normal produce path uses ReserveOffset + appendToChunk + WAL append + CommitBatch.
func (pd *PartData) PushBatch(raw []byte, meta BatchMeta) int64 {
	baseOffset := pd.hw

	// Make a copy of the raw bytes so we own them (the caller's buffer may be reused)
	stored := make([]byte, len(raw))
	copy(stored, raw)

	// Assign server-side base offset and PartitionLeaderEpoch
	AssignOffset(stored, baseOffset)

	// Advance high watermark
	pd.hw = baseOffset + int64(meta.LastOffsetDelta) + 1

	// Append to chunk pool
	pd.appendToChunk(stored, chunk.ChunkBatch{
		BaseOffset:      baseOffset,
		LastOffsetDelta: meta.LastOffsetDelta,
		MaxTimestamp:    meta.MaxTimestamp,
		NumRecords:      meta.NumRecords,
	})

	pd.nextReserve = pd.hw
	pd.nextCommit = pd.hw
	pd.totalBytes += int64(len(stored))

	pd.updateMaxTimestamp(baseOffset, meta.LastOffsetDelta, meta.MaxTimestamp)

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

// CommitBatch advances HW after WAL fsync completes.
// The batch data is already in the chunk pool (written during appendToChunk
// before WAL fsync). This method only advances HW and nextCommit.
// Called under pd.mu.Lock().
func (pd *PartData) CommitBatch(batch StoredBatch) {
	if batch.BaseOffset == pd.nextCommit {
		// In order: advance HW immediately
		pd.totalBytes += int64(len(batch.RawBytes))
		pd.nextCommit = batch.BaseOffset + int64(batch.LastOffsetDelta) + 1
		pd.hw = pd.nextCommit
		pd.updateMaxTimestamp(batch.BaseOffset, batch.LastOffsetDelta, batch.MaxTimestamp)

		// Drain any queued commits that are now in order
		for len(pd.pendingCommits) > 0 && pd.pendingCommits[0].BaseOffset == pd.nextCommit {
			next := pd.pendingCommits[0].StoredBatch
			pd.pendingCommits = pd.pendingCommits[1:]
			pd.totalBytes += int64(len(next.RawBytes))
			pd.nextCommit = next.BaseOffset + int64(next.LastOffsetDelta) + 1
			pd.hw = pd.nextCommit
			pd.updateMaxTimestamp(next.BaseOffset, next.LastOffsetDelta, next.MaxTimestamp)
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

// updateMaxTimestamp updates max timestamp tracking.
func (pd *PartData) updateMaxTimestamp(baseOffset int64, lastOffsetDelta int32, maxTimestamp int64) {
	if !pd.hasMaxTimestamp || maxTimestamp >= pd.maxTimestampVal {
		pd.hasMaxTimestamp = true
		pd.maxTimestampVal = maxTimestamp
		pd.maxTimestampOffset = baseOffset + int64(lastOffsetDelta)
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

// FetchFrom collects batches starting at offset, up to maxBytes.
// KIP-74: always includes at least one complete batch even if it exceeds maxBytes.
// Returns nil if no batches match.
//
// This method manages its own locking: it acquires RLock for in-memory reads
// and releases it before any disk/network I/O (WAL pread, S3 HTTP).
// Caller must NOT hold pd.mu.
//
// Implements the three-tier read cascade:
// 1. Chunk pool (memory) — under RLock, gated by HW
// 2. WAL index + pread — no lock held
// 3. S3 range read (if configured) — no lock held
func (pd *PartData) FetchFrom(offset int64, maxBytes int32) []StoredBatch {
	// Tier 1: Chunk pool (fast path, under RLock)
	pd.mu.RLock()
	hw := pd.hw
	if offset >= hw {
		pd.mu.RUnlock()
		return nil
	}
	if batches := pd.fetchFromChunks(offset, maxBytes, hw); len(batches) > 0 {
		// Only use chunk results if they actually cover the requested offset.
		// If the first batch starts beyond the requested offset, the earlier
		// data must be in S3 (or WAL) — fall through to cold storage so the
		// consumer sees records in order.
		first := batches[0]
		if first.BaseOffset <= offset || first.BaseOffset == pd.logStart {
			pd.mu.RUnlock()
			return batches
		}
	}
	// Capture immutable references needed for cold reads before releasing lock.
	topicID := pd.TopicID
	partIdx := pd.Index
	topic := pd.Topic
	walIdx := pd.walIndex
	walW := pd.walWriter
	s3Fetch := pd.s3Fetch
	pd.mu.RUnlock()

	// Tier 2 & 3: WAL and S3 reads (no lock held)
	return fetchFromCold(offset, maxBytes, topicID, partIdx, topic, walIdx, walW, s3Fetch)
}

// readFromWAL reads batches from WAL index entries.
func readFromWAL(walW *wal.Writer, entries []wal.IndexEntry) []StoredBatch {
	var result []StoredBatch
	for _, e := range entries {
		data, err := walW.ReadBatch(e)
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
	return result
}

// readFromS3 reads batches from S3.
func readFromS3(s3Fetch S3Fetcher, topic string, topicID [16]byte, partition int32, offset int64, maxBytes int32) []StoredBatch {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s3Batches, err := s3Fetch.FetchBatches(ctx, topic, topicID, partition, offset, maxBytes)
	if err != nil || len(s3Batches) == 0 {
		return nil
	}
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
	return result
}

// fetchFromCold implements the WAL + S3 tiers of the read cascade.
// Called without any partition lock held.
func fetchFromCold(offset int64, maxBytes int32, topicID [16]byte, partIdx int32, topic string, walIdx *wal.Index, walW *wal.Writer, s3Fetch S3Fetcher) []StoredBatch {
	// Look up WAL entries once and reuse for both the primary and fallback paths.
	var walEntries []wal.IndexEntry
	if walIdx != nil && walW != nil {
		tp := wal.TopicPartition{TopicID: topicID, Partition: partIdx}
		walEntries = walIdx.Lookup(tp, offset, maxBytes)
	}

	// Try WAL first, then S3, with fallback logic for disaster recovery.
	walHasOffset := false
	if len(walEntries) > 0 {
		// Only use WAL if its entries actually start at or near the
		// requested offset. If the first entry's BaseOffset is far
		// beyond the requested offset, the data for the requested
		// offset is not in the WAL — try S3 first.
		if walEntries[0].BaseOffset <= offset || s3Fetch == nil {
			walHasOffset = true
			if result := readFromWAL(walW, walEntries); len(result) > 0 {
				return result
			}
		}
	}

	// S3 range read
	if s3Fetch != nil {
		if result := readFromS3(s3Fetch, topic, topicID, partIdx, offset, maxBytes); len(result) > 0 {
			return result
		}
	}

	// Fallback: if S3 didn't have data but WAL does (for a higher range), use WAL
	if !walHasOffset && len(walEntries) > 0 {
		if result := readFromWAL(walW, walEntries); len(result) > 0 {
			return result
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
func (pd *PartData) ListOffsets(timestamp int64, isolationLevel int8) (offset int64, ts int64) {
	switch timestamp {
	case -1: // Latest
		if isolationLevel == 1 { // read_committed: return LSO
			return pd.LSO(), -1
		}
		return pd.hw, -1
	case -2: // Earliest
		return pd.logStart, -1
	case -3: // MaxTimestamp (KIP-734)
		if !pd.hasMaxTimestamp {
			return -1, -1
		}
		return pd.maxTimestampOffset, pd.maxTimestampVal
	default: // timestamp >= 0: find first batch with MaxTimestamp >= timestamp
		return pd.listOffsetsChunkTimestamp(timestamp)
	}
}

// listOffsetsChunkTimestamp scans chunks for a timestamp match.
func (pd *PartData) listOffsetsChunkTimestamp(timestamp int64) (int64, int64) {
	// Scan sealed chunks first (oldest data), then current chunk
	for _, c := range pd.chunkSealed {
		for _, b := range c.Batches {
			if b.MaxTimestamp >= timestamp {
				return b.BaseOffset, b.MaxTimestamp
			}
		}
	}
	if pd.chunkCurrent != nil {
		for _, b := range pd.chunkCurrent.Batches {
			if b.MaxTimestamp >= timestamp {
				return b.BaseOffset, b.MaxTimestamp
			}
		}
	}
	// Data flushed to S3 is not searched (accepted limitation — see 10-list-offsets.md).
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

// BatchCount returns the number of logically visible batches (those not fully
// below logStart). Chunk-based batches are not physically trimmed until the
// flusher detaches sealed chunks, so this filters by offset.
// Caller must hold pd.mu.RLock().
func (pd *PartData) BatchCount() int {
	n := 0
	for _, c := range pd.chunkSealed {
		for _, b := range c.Batches {
			if b.BaseOffset+int64(b.LastOffsetDelta) >= pd.logStart {
				n++
			}
		}
	}
	if pd.chunkCurrent != nil {
		for _, b := range pd.chunkCurrent.Batches {
			if b.BaseOffset+int64(b.LastOffsetDelta) >= pd.logStart {
				n++
			}
		}
	}
	return n
}

// TotalBytes returns the total bytes of stored batch data.
// Caller must hold pd.mu.RLock().
func (pd *PartData) TotalBytes() int64 {
	return pd.totalBytes
}

// AdvanceLogStartOffset advances the partition's logStartOffset to newOffset,
// trims old batches, persists the change to metadata.log, prunes the WAL
// index, and updates the partition size counter.
//
// Called by: retention enforcement, DeleteRecords handler.
// Caller must NOT hold pd.mu (this method acquires it internally).
// Caller must hold pd.CompactionMu.
//
// If metaLog is nil, the persistence step is skipped (tests only).
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

	// Signal the WAL writer to clean up segments that are no longer referenced.
	if pd.walWriter != nil {
		pd.walWriter.TryCleanupSegments()
	}

	return nil
}

// trimBatchesLocked updates logStart. Chunk-based batches are not trimmed
// individually — they are released in bulk when the flusher detaches sealed
// chunks. totalBytes is not adjusted here since chunk data is immutable
// until flush.
// Caller must hold pd.mu.Lock().
func (pd *PartData) trimBatchesLocked(newLogStart int64) {
	pd.logStart = newLogStart
}

// --- Chunk pool integration ---

// AppendToChunk appends a batch to the partition's current chunk.
// Acquires a chunk lazily on first call. Seals and acquires a new chunk
// when the current one is full.
// Caller must hold pd.mu.Lock().
func (pd *PartData) AppendToChunk(raw []byte, meta chunk.ChunkBatch) {
	pd.appendToChunk(raw, meta)
}

// appendToChunk is the internal implementation.
func (pd *PartData) appendToChunk(raw []byte, meta chunk.ChunkBatch) {
	if pd.chunkPool == nil {
		return // chunk pool not configured (tests without WAL)
	}

	// Lazy acquisition: first produce after startup or after flush released current chunk
	if pd.chunkCurrent == nil {
		pd.chunkCurrent = pd.chunkPool.Acquire()
	}

	// Seal if this batch wouldn't fit. Since chunkSize >= max.message.bytes,
	// a batch always fits in an empty chunk.
	if pd.chunkCurrent.Used+len(raw) > pd.chunkPool.ChunkSize() {
		pd.chunkSealed = append(pd.chunkSealed, pd.chunkCurrent)
		pd.chunkCurrent = pd.chunkPool.Acquire()
	}

	offset := pd.chunkCurrent.Used
	copy(pd.chunkCurrent.Data[offset:], raw)
	pd.chunkCurrent.Used += len(raw)

	meta.Offset = offset
	meta.Size = len(raw)
	pd.chunkCurrent.Batches = append(pd.chunkCurrent.Batches, meta)
}

// fetchFromChunks scans sealed + current chunks for batches matching
// the requested offset range. Only returns batches whose BaseOffset < hw
// (HW visibility gate — prevents exposing uncommitted data).
// Caller must hold pd.mu.RLock().
func (pd *PartData) fetchFromChunks(offset int64, maxBytes int32, hw int64) []StoredBatch {
	var result []StoredBatch
	var totalBytes int32

	collectFromChunk := func(c *chunk.Chunk) bool {
		for _, b := range c.Batches {
			// HW visibility gate: only return committed batches
			if b.BaseOffset >= hw {
				return false // stop — all subsequent batches are also >= hw
			}
			lastOffset := b.BaseOffset + int64(b.LastOffsetDelta)
			if lastOffset < offset {
				continue // batch ends before requested offset
			}

			batchSize := int32(b.Size)
			if len(result) > 0 && totalBytes+batchSize > maxBytes {
				return false // respect maxBytes (but first batch is always included)
			}

			// Copy raw bytes from chunk — the chunk may be reused after flush
			raw := make([]byte, b.Size)
			copy(raw, c.Data[b.Offset:b.Offset+b.Size])

			result = append(result, StoredBatch{
				BaseOffset:      b.BaseOffset,
				LastOffsetDelta: b.LastOffsetDelta,
				RawBytes:        raw,
				MaxTimestamp:    b.MaxTimestamp,
				NumRecords:      b.NumRecords,
			})
			totalBytes += batchSize
		}
		return true
	}

	// Scan sealed chunks (oldest first)
	for _, c := range pd.chunkSealed {
		if !collectFromChunk(c) {
			return result
		}
	}

	// Scan current chunk
	if pd.chunkCurrent != nil {
		collectFromChunk(pd.chunkCurrent)
	}

	return result
}

// DetachSealedChunks removes sealed chunks from the partition and returns them.
// If includeCurrentIfNonEmpty is true, the current chunk is also sealed and detached.
// After detaching, the partition holds zero chunks until the next produce.
// Caller must hold pd.mu.Lock().
func (pd *PartData) DetachSealedChunks(includeCurrentIfNonEmpty bool) []*chunk.Chunk {
	if includeCurrentIfNonEmpty && pd.chunkCurrent != nil && pd.chunkCurrent.Used > 0 {
		pd.chunkSealed = append(pd.chunkSealed, pd.chunkCurrent)
		pd.chunkCurrent = nil
	}

	result := pd.chunkSealed
	pd.chunkSealed = nil
	return result
}

// SealedChunkBytes returns the total bytes in sealed chunks.
// Caller must hold pd.mu.RLock().
func (pd *PartData) SealedChunkBytes() int64 {
	var total int64
	for _, c := range pd.chunkSealed {
		total += int64(c.Used)
	}
	return total
}

// OldestSealedChunkTime returns the creation time of the oldest sealed chunk,
// or zero time if no sealed chunks exist.
// Caller must hold pd.mu.RLock().
func (pd *PartData) OldestSealedChunkTime() time.Time {
	if len(pd.chunkSealed) == 0 {
		// Fall back to current chunk's creation time for age-based flush
		if pd.chunkCurrent != nil && pd.chunkCurrent.Used > 0 {
			return pd.chunkCurrent.CreatedAt
		}
		return time.Time{}
	}
	return pd.chunkSealed[0].CreatedAt
}

// HasChunkData returns whether the partition has any unflushed chunk data
// (sealed or current).
// Caller must hold pd.mu.RLock().
func (pd *PartData) HasChunkData() bool {
	if len(pd.chunkSealed) > 0 {
		return true
	}
	return pd.chunkCurrent != nil && pd.chunkCurrent.Used > 0
}

// ChunkPool returns the chunk pool reference for this partition.
func (pd *PartData) ChunkPool() *chunk.Pool {
	return pd.chunkPool
}

// WalIndex returns the WAL index reference.
func (pd *PartData) WalIndex() *wal.Index {
	return pd.walIndex
}
