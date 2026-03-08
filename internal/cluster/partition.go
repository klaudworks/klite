package cluster

import (
	"context"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klaudworks/klite/internal/chunk"
	"github.com/klaudworks/klite/internal/metadata"
	"github.com/klaudworks/klite/internal/wal"
)

var (
	lastEmptyFetchLog    atomic.Int64
	lastColdGapLog       atomic.Int64
	lastChunkFallbackLog atomic.Int64
)

const ErrCodeOffsetOutOfRange int16 = 1

type StoredBatch struct {
	BaseOffset      int64
	LastOffsetDelta int32
	RawBytes        []byte
	MaxTimestamp    int64
	NumRecords      int32
}

// S3Fetcher abstracts S3 reads to avoid circular imports with s3.Reader.
type S3Fetcher interface {
	FetchBatches(ctx context.Context, topic string, topicID [16]byte, partition int32, offset int64, maxBytes int32) ([]S3BatchData, error)
}

type S3BatchData struct {
	RawBytes        []byte
	BaseOffset      int64
	LastOffsetDelta int32
}

// FetchWaiter is shared across partitions for long-poll wake-up.
// sync.Once prevents double-close when multiple partitions wake the same waiter.
type FetchWaiter struct {
	once sync.Once
	ch   chan struct{}
}

func NewFetchWaiter() *FetchWaiter {
	return &FetchWaiter{ch: make(chan struct{})}
}

func (w *FetchWaiter) Ch() <-chan struct{} {
	return w.ch
}

func (w *FetchWaiter) CloseOnce() {
	w.once.Do(func() { close(w.ch) })
}

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
// require mu.Lock(). Read methods (Fetch, ListOffsets) require mu.RLock().
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

// Caller must hold pd.mu.RLock() or pd.mu.Lock().
func (pd *PartData) HW() int64 {
	return pd.hw
}

// Caller must hold pd.mu.RLock() or pd.mu.Lock().
func (pd *PartData) CleanedUpTo() int64 {
	return pd.cleanedUpTo
}

// Caller must hold pd.mu.Lock().
func (pd *PartData) SetCleanedUpTo(offset int64) {
	if offset > pd.cleanedUpTo {
		pd.cleanedUpTo = offset
	}
}

func (pd *PartData) DirtyObjects() int32 {
	pd.mu.RLock()
	defer pd.mu.RUnlock()
	return pd.dirtyObjects
}

// Caller must hold pd.mu.Lock().
func (pd *PartData) IncrementDirtyObjects() {
	pd.dirtyObjects++
}

// Caller must hold pd.mu.Lock().
func (pd *PartData) ResetDirtyObjects(now time.Time) {
	pd.dirtyObjects = 0
	pd.lastCompacted = now
}

// Caller must hold pd.mu.Lock().
func (pd *PartData) SetDirtyObjects(count int32) {
	pd.dirtyObjects = count
}

func (pd *PartData) LastCompacted() time.Time {
	pd.mu.RLock()
	defer pd.mu.RUnlock()
	return pd.lastCompacted
}

// Caller must hold pd.mu.RLock() or pd.mu.Lock().
func (pd *PartData) LogStart() int64 {
	return pd.logStart
}

func (pd *PartData) Lock()    { pd.mu.Lock() }
func (pd *PartData) Unlock()  { pd.mu.Unlock() }
func (pd *PartData) RLock()   { pd.mu.RLock() }
func (pd *PartData) RUnlock() { pd.mu.RUnlock() }

func (pd *PartData) InitWAL(pool *chunk.Pool, writer *wal.Writer, idx *wal.Index) {
	pd.chunkPool = pool
	pd.walWriter = writer
	pd.walIndex = idx
	pd.nextReserve = pd.hw
	pd.nextCommit = pd.hw
}

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

// Caller must hold pd.mu.RLock() or pd.mu.Lock().
func (pd *PartData) S3FlushWatermark() int64 {
	return pd.s3FlushWatermark
}

// Caller must hold pd.mu.Lock().
func (pd *PartData) SetS3FlushWatermark(w int64) {
	if w > pd.s3FlushWatermark {
		pd.s3FlushWatermark = w
	}
}

func (pd *PartData) SetS3Fetcher(f S3Fetcher) {
	pd.s3Fetch = f
}

func (pd *PartData) HasS3() bool {
	return pd.s3Fetch != nil
}

// PushBatch assigns an offset and appends a batch. Returns base offset and
// any unused spare chunk. Caller must hold pd.mu.Lock().
// Used by WAL replay and EndTxn; normal produce uses ReserveOffset + CommitBatch.
func (pd *PartData) PushBatch(raw []byte, meta BatchMeta, spare *chunk.Chunk) (int64, *chunk.Chunk) {
	baseOffset := pd.hw

	stored := make([]byte, len(raw))
	copy(stored, raw)

	AssignOffset(stored, baseOffset)
	pd.hw = baseOffset + int64(meta.LastOffsetDelta) + 1

	spare = pd.appendToChunk(stored, chunk.ChunkBatch{
		BaseOffset:      baseOffset,
		LastOffsetDelta: meta.LastOffsetDelta,
		MaxTimestamp:    meta.MaxTimestamp,
		NumRecords:      meta.NumRecords,
	}, spare)

	pd.nextReserve = pd.hw
	pd.nextCommit = pd.hw
	pd.totalBytes += int64(len(stored))

	pd.updateMaxTimestamp(baseOffset, meta.LastOffsetDelta, meta.MaxTimestamp)

	return baseOffset, spare
}

// ReserveOffset reserves an offset range without storing data.
// Caller must hold pd.mu.Lock().
func (pd *PartData) ReserveOffset(meta BatchMeta) int64 {
	base := pd.nextReserve
	pd.nextReserve = base + int64(meta.LastOffsetDelta) + 1
	return base
}

// Caller must hold pd.mu.Lock().
func (pd *PartData) RollbackReserve(baseOffset int64) {
	pd.nextReserve = baseOffset
}

// SkipOffsets advances past offsets that will never be committed (e.g. WAL failure).
// Caller must hold pd.mu.Lock().
func (pd *PartData) SkipOffsets(baseOffset, count int64) {
	endOffset := baseOffset + count
	if endOffset <= pd.nextCommit {
		return // already past this range
	}
	if baseOffset == pd.nextCommit {
		pd.nextCommit = endOffset
		pd.hw = endOffset
		pd.drainPendingCommits()
	} else {
		// Out-of-order skip: insert sentinel so it drains when we catch up.
		sentinel := StoredBatch{
			BaseOffset:      baseOffset,
			LastOffsetDelta: int32(count - 1),
			RawBytes:        nil,
		}
		pd.insertPendingCommit(sentinel)
	}
}

// CommitBatch advances HW after WAL fsync. Caller must hold pd.mu.Lock().
func (pd *PartData) CommitBatch(batch StoredBatch) {
	if batch.BaseOffset == pd.nextCommit {
		pd.applyCommit(batch)
		pd.drainPendingCommits()
	} else {
		pd.insertPendingCommit(batch)
	}
}

func (pd *PartData) applyCommit(batch StoredBatch) {
	pd.nextCommit = batch.BaseOffset + int64(batch.LastOffsetDelta) + 1
	pd.hw = pd.nextCommit
	if batch.RawBytes != nil {
		pd.totalBytes += int64(len(batch.RawBytes))
		pd.updateMaxTimestamp(batch.BaseOffset, batch.LastOffsetDelta, batch.MaxTimestamp)
	}
}

func (pd *PartData) drainPendingCommits() {
	for len(pd.pendingCommits) > 0 && pd.pendingCommits[0].BaseOffset == pd.nextCommit {
		next := pd.pendingCommits[0].StoredBatch
		pd.pendingCommits = pd.pendingCommits[1:]
		pd.applyCommit(next)
	}
}

func (pd *PartData) insertPendingCommit(batch StoredBatch) {
	pb := pendingBatch{StoredBatch: batch}
	idx := sort.Search(len(pd.pendingCommits), func(i int) bool {
		return pd.pendingCommits[i].BaseOffset > batch.BaseOffset
	})
	pd.pendingCommits = append(pd.pendingCommits, pendingBatch{})
	copy(pd.pendingCommits[idx+1:], pd.pendingCommits[idx:])
	pd.pendingCommits[idx] = pb
}

func (pd *PartData) updateMaxTimestamp(baseOffset int64, lastOffsetDelta int32, maxTimestamp int64) {
	if !pd.hasMaxTimestamp || maxTimestamp >= pd.maxTimestampVal {
		pd.hasMaxTimestamp = true
		pd.maxTimestampVal = maxTimestamp
		pd.maxTimestampOffset = baseOffset + int64(lastOffsetDelta)
	}
}

// Caller must NOT hold pd.mu.
func (pd *PartData) NotifyWaiters() {
	pd.waiterMu.Lock()
	waiters := pd.waiters
	pd.waiters = nil
	pd.waiterMu.Unlock()

	for _, w := range waiters {
		w.CloseOnce()
	}
}

// Caller must NOT hold pd.mu.
func (pd *PartData) RegisterWaiter(w *FetchWaiter) {
	pd.waiterMu.Lock()
	pd.waiters = append(pd.waiters, w)
	pd.waiterMu.Unlock()
}

type FetchResponse struct {
	HW       int64
	LogStart int64
	LSO      int64
	Batches  []StoredBatch
	Err      int16
}

// Fetch validates the offset and returns batches. Hot-path reads (chunks)
// happen under RLock; cold-path reads (WAL, S3) happen after releasing it.
// Caller must NOT hold pd.mu.
func (pd *PartData) Fetch(fetchOffset int64, maxBytes int32) FetchResponse {
	pd.mu.RLock()
	hw := pd.hw
	logStart := pd.logStart
	lso := pd.LSO()

	if fetchOffset < logStart || fetchOffset > hw {
		pd.mu.RUnlock()
		return FetchResponse{HW: hw, LogStart: logStart, LSO: lso, Err: ErrCodeOffsetOutOfRange}
	}

	if fetchOffset == hw {
		pd.mu.RUnlock()
		return FetchResponse{HW: hw, LogStart: logStart, LSO: lso}
	}

	chunkBatches := pd.fetchFromChunks(fetchOffset, maxBytes, hw)
	if len(chunkBatches) > 0 {
		first := chunkBatches[0]
		if first.BaseOffset <= fetchOffset || first.BaseOffset == pd.logStart {
			pd.mu.RUnlock()
			return FetchResponse{HW: hw, LogStart: logStart, LSO: lso, Batches: chunkBatches}
		}
	}

	topicID := pd.TopicID
	partIdx := pd.Index
	topic := pd.Topic
	walIdx := pd.walIndex
	walW := pd.walWriter
	s3Fetch := pd.s3Fetch
	s3WM := pd.s3FlushWatermark
	chunkFirstOffset := int64(-1)
	if len(chunkBatches) > 0 {
		chunkFirstOffset = chunkBatches[0].BaseOffset
	}
	pd.mu.RUnlock()

	coldBatches := fetchFromCold(fetchOffset, maxBytes, topicID, partIdx, topic, walIdx, walW, s3Fetch)
	if len(coldBatches) > 0 {
		coldBatches = filterBatchesByHW(coldBatches, hw)
		if len(coldBatches) > 0 {
			if coldBatches[0].BaseOffset > fetchOffset {
				// Cold path returned data starting after the requested offset.
				// Suppress to avoid the consumer skipping the gap.
				if now := time.Now().UnixNano(); now-lastColdGapLog.Load() > int64(time.Second) {
					lastColdGapLog.Store(now)
					slog.Warn("fetch: suppressing cold path gap",
						"topic", topic, "partition", partIdx,
						"fetch_offset", fetchOffset,
						"cold_base_offset", coldBatches[0].BaseOffset,
						"gap", coldBatches[0].BaseOffset-fetchOffset,
						"hw", hw, "s3_watermark", s3WM,
						"chunk_first_offset", chunkFirstOffset)
				}
			} else {
				return FetchResponse{HW: hw, LogStart: logStart, LSO: lso, Batches: coldBatches}
			}
		}
	}

	// Diagnostic: offset is below HW but neither chunks nor cold path returned data.
	if now := time.Now().UnixNano(); now-lastEmptyFetchLog.Load() > int64(time.Second) {
		lastEmptyFetchLog.Store(now)
		slog.Warn("fetch: empty response for valid offset",
			"topic", topic, "partition", partIdx,
			"fetch_offset", fetchOffset, "hw", hw,
			"s3_watermark", s3WM,
			"chunk_first_offset", chunkFirstOffset,
			"cold_len", len(coldBatches),
			"wal_index_nil", walIdx == nil, "s3_fetch_nil", s3Fetch == nil)
	}

	// Never return chunk batches that start after the requested offset —
	// this would cause the consumer to skip records in the gap. Return an
	// empty response so the consumer retries (and triggers the long-poll
	// wait path). The data should eventually appear via S3 listing refresh.
	if len(chunkBatches) > 0 && chunkBatches[0].BaseOffset > fetchOffset {
		if now := time.Now().UnixNano(); now-lastChunkFallbackLog.Load() > int64(time.Second) {
			lastChunkFallbackLog.Store(now)
			slog.Warn("fetch: suppressing chunk fallback gap",
				"topic", topic, "partition", partIdx,
				"fetch_offset", fetchOffset,
				"chunk_base_offset", chunkBatches[0].BaseOffset,
				"gap", chunkBatches[0].BaseOffset-fetchOffset,
				"hw", hw, "s3_watermark", s3WM)
		}
		return FetchResponse{HW: hw, LogStart: logStart, LSO: lso}
	}

	return FetchResponse{HW: hw, LogStart: logStart, LSO: lso, Batches: chunkBatches}
}

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

func readFromS3(s3Fetch S3Fetcher, topic string, topicID [16]byte, partition int32, offset int64, maxBytes int32) []StoredBatch {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s3Batches, err := s3Fetch.FetchBatches(ctx, topic, topicID, partition, offset, maxBytes)
	if err != nil {
		slog.Warn("readFromS3: error", "topic", topic, "partition", partition,
			"offset", offset, "err", err)
		return nil
	}
	if len(s3Batches) == 0 {
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

func filterBatchesByHW(batches []StoredBatch, hw int64) []StoredBatch {
	n := 0
	for _, b := range batches {
		if b.BaseOffset+int64(b.LastOffsetDelta)+1 > hw {
			break
		}
		n++
	}
	return batches[:n]
}

var lastColdReadLog atomic.Int64

func fetchFromCold(offset int64, maxBytes int32, topicID [16]byte, partIdx int32, topic string, walIdx *wal.Index, walW *wal.Writer, s3Fetch S3Fetcher) []StoredBatch {
	var walEntries []wal.IndexEntry
	if walIdx != nil && walW != nil {
		tp := wal.TopicPartition{TopicID: topicID, Partition: partIdx}
		walEntries = walIdx.Lookup(tp, offset, maxBytes)
	}

	walHasOffset := false
	if len(walEntries) > 0 {
		if walEntries[0].BaseOffset <= offset || s3Fetch == nil {
			walHasOffset = true
			result := readFromWAL(walW, walEntries)
			if len(result) > 0 {
				return result
			}
			// WAL index had entries but read failed (segment deleted?).
			if now := time.Now().UnixNano(); now-lastColdReadLog.Load() > int64(time.Second) {
				lastColdReadLog.Store(now)
				slog.Warn("fetchFromCold: WAL read empty despite index hit",
					"topic", topic, "partition", partIdx,
					"fetch_offset", offset,
					"wal_entry_base", walEntries[0].BaseOffset,
					"wal_entry_last", walEntries[0].LastOffset,
					"wal_entry_seg", walEntries[0].SegmentSeq,
					"wal_entries", len(walEntries))
			}
		}
	}

	if s3Fetch != nil {
		if result := readFromS3(s3Fetch, topic, topicID, partIdx, offset, maxBytes); len(result) > 0 {
			return result
		}
	}

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
func (pd *PartData) ListOffsets(timestamp int64, isolationLevel int8) (offset, ts int64) {
	switch timestamp {
	case -1: // Latest
		if isolationLevel == 1 { // read-committed isolation: return LSO
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

func (pd *PartData) listOffsetsChunkTimestamp(timestamp int64) (int64, int64) {
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

// LSO returns min(HW, oldest open txn first offset). Caller must hold pd.mu.RLock().
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

// Caller must hold pd.mu.Lock().
func (pd *PartData) AddOpenTxn(producerID, firstOffset int64) {
	if pd.openTxnPIDs == nil {
		pd.openTxnPIDs = make(map[int64]int64)
	}
	if _, exists := pd.openTxnPIDs[producerID]; !exists {
		pd.openTxnPIDs[producerID] = firstOffset
	}
}

// Caller must hold pd.mu.Lock().
func (pd *PartData) RemoveOpenTxn(producerID int64) {
	delete(pd.openTxnPIDs, producerID)
}

// OpenTxnPIDs returns a copy of the open transaction map (producerID -> first offset).
// Caller must hold pd.mu.RLock() or pd.mu.Lock().
func (pd *PartData) OpenTxnPIDs() map[int64]int64 {
	if len(pd.openTxnPIDs) == 0 {
		return nil
	}
	cp := make(map[int64]int64, len(pd.openTxnPIDs))
	for pid, off := range pd.openTxnPIDs {
		cp[pid] = off
	}
	return cp
}

// Caller must hold pd.mu.Lock().
func (pd *PartData) AddAbortedTxn(entry AbortedTxnEntry) {
	pd.abortedTxns = append(pd.abortedTxns, entry)
}

// Caller must hold pd.mu.RLock().
func (pd *PartData) AbortedTxnsInRange(fetchOffset, lastOffset int64) []AbortedTxnEntry {
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

// Caller must hold pd.mu.RLock().
func (pd *PartData) TotalBytes() int64 {
	return pd.totalBytes
}

// AdvanceLogStartOffset advances logStartOffset, persists to metadata.log,
// and prunes the WAL index. Caller must hold pd.CompactionMu, must NOT hold pd.mu.
func (pd *PartData) AdvanceLogStartOffset(newOffset int64, metaLog *metadata.Log) error {
	pd.mu.Lock()
	if pd.logStart >= newOffset {
		pd.mu.Unlock()
		return nil
	}
	if newOffset > pd.hw {
		newOffset = pd.hw
	}
	topic := pd.Topic
	partIdx := pd.Index
	pd.mu.Unlock()

	// Persist outside the partition lock to avoid blocking Fetch/Produce
	// during the fsync. CompactionMu (held by caller) prevents concurrent
	// AdvanceLogStartOffset calls, so the offset can't regress between
	// our read above and the write below.
	if metaLog != nil {
		entry := metadata.MarshalLogStartOffset(&metadata.LogStartOffsetEntry{
			TopicName:      topic,
			Partition:      partIdx,
			LogStartOffset: newOffset,
		})
		if err := metaLog.AppendSync(entry); err != nil {
			return err
		}
	}

	pd.mu.Lock()
	pd.trimBatchesLocked(newOffset)
	pd.mu.Unlock()

	return nil
}

// Caller must hold pd.mu.Lock().
func (pd *PartData) trimBatchesLocked(newLogStart int64) {
	pd.logStart = newLogStart
}

// AcquireSpareChunk pre-acquires a chunk if needed for the next append.
// May block on pool exhaustion. Caller must NOT hold pd.mu.
func (pd *PartData) AcquireSpareChunk(batchSize int) *chunk.Chunk {
	if pd.chunkPool == nil {
		return nil
	}

	pd.mu.RLock()
	needsNew := pd.chunkCurrent == nil || pd.chunkCurrent.Used+batchSize > pd.chunkPool.ChunkSize()
	pd.mu.RUnlock()

	if needsNew {
		return pd.chunkPool.Acquire()
	}
	return nil
}

func (pd *PartData) ReleaseSpareChunk(spare *chunk.Chunk) {
	if spare != nil && pd.chunkPool != nil {
		pd.chunkPool.Release(spare)
	}
}

// Caller must hold pd.mu.Lock().
func (pd *PartData) AppendToChunk(raw []byte, meta chunk.ChunkBatch, spare *chunk.Chunk) *chunk.Chunk {
	return pd.appendToChunk(raw, meta, spare)
}

func (pd *PartData) appendToChunk(raw []byte, meta chunk.ChunkBatch, spare *chunk.Chunk) *chunk.Chunk {
	if pd.chunkPool == nil {
		return spare
	}

	if pd.chunkCurrent == nil {
		if spare != nil {
			pd.chunkCurrent = spare
			spare = nil
		} else {
			pd.chunkCurrent = pd.chunkPool.Acquire()
			if pd.chunkCurrent == nil {
				return spare // pool closed during shutdown
			}
		}
	}

	if pd.chunkCurrent.Used+len(raw) > pd.chunkPool.ChunkSize() {
		pd.chunkSealed = append(pd.chunkSealed, pd.chunkCurrent)
		if spare != nil {
			pd.chunkCurrent = spare
			spare = nil
		} else {
			pd.chunkCurrent = pd.chunkPool.Acquire()
			if pd.chunkCurrent == nil {
				return spare // pool closed during shutdown
			}
		}
	}

	offset := pd.chunkCurrent.Used
	copy(pd.chunkCurrent.Data[offset:], raw)
	pd.chunkCurrent.Used += len(raw)

	meta.Offset = offset
	meta.Size = len(raw)
	pd.chunkCurrent.Batches = append(pd.chunkCurrent.Batches, meta)

	return spare
}

// Caller must hold pd.mu.RLock().
func (pd *PartData) fetchFromChunks(offset int64, maxBytes int32, hw int64) []StoredBatch {
	var result []StoredBatch
	var totalBytes int32

	collectFromChunk := func(c *chunk.Chunk) bool {
		for _, b := range c.Batches {
			batchEnd := b.BaseOffset + int64(b.LastOffsetDelta) + 1
			if batchEnd > hw {
				return false
			}
			lastOffset := b.BaseOffset + int64(b.LastOffsetDelta)
			if lastOffset < offset {
				continue
			}

			batchSize := int32(b.Size)
			if len(result) > 0 && totalBytes+batchSize > maxBytes {
				return false
			}

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

	for _, c := range pd.chunkSealed {
		if !collectFromChunk(c) {
			return result
		}
	}

	if pd.chunkCurrent != nil {
		collectFromChunk(pd.chunkCurrent)
	}

	return result
}

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

// ReattachSealedChunks prepends chunks back onto the sealed list.
// Used by the S3 flusher to re-enqueue chunks after a failed upload so they
// can be retried on the next flush cycle instead of being lost.
// Caller must hold pd.mu.Lock().
func (pd *PartData) ReattachSealedChunks(chunks []*chunk.Chunk) {
	pd.chunkSealed = append(chunks, pd.chunkSealed...)
}

// Caller must hold pd.mu.RLock().
func (pd *PartData) SealedChunkBytes() int64 {
	var total int64
	for _, c := range pd.chunkSealed {
		total += int64(c.Used)
	}
	return total
}

// Caller must hold pd.mu.RLock().
func (pd *PartData) OldestSealedChunkTime() time.Time {
	if len(pd.chunkSealed) == 0 {
		if pd.chunkCurrent != nil && pd.chunkCurrent.Used > 0 {
			return pd.chunkCurrent.CreatedAt
		}
		return time.Time{}
	}
	return pd.chunkSealed[0].CreatedAt
}

// Caller must hold pd.mu.RLock().
func (pd *PartData) HasChunkData() bool {
	if len(pd.chunkSealed) > 0 {
		return true
	}
	return pd.chunkCurrent != nil && pd.chunkCurrent.Used > 0
}

func (pd *PartData) ChunkPool() *chunk.Pool {
	return pd.chunkPool
}

func (pd *PartData) WalIndex() *wal.Index {
	return pd.walIndex
}
