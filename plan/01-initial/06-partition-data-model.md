# Partition Data Model

Core data structures for partition state: `partData`, `storedBatch`,
`parseBatchHeader`, and offset assignment. These are used by Produce, Fetch,
ListOffsets, and CreateTopics handlers.

This work unit defines the structures and methods. The handlers that use them
are in `07-create-topics.md`, `08-produce.md`, `09-fetch.md`, `10-list-offsets.md`.

Prerequisite: `05-metadata.md` complete (broker can return topic metadata).

Read before starting: `01-foundations.md` (concurrency model — per-partition
RWMutex), `research/kafka-internals.md` (RecordBatch format).

---

## Data Structures

```go
// storedBatch: raw bytes + metadata extracted from the batch header.
// Used for both in-memory storage and as the return type from WAL/S3 reads.
type storedBatch struct {
    BaseOffset      int64
    LastOffsetDelta int32
    RawBytes        []byte   // Original RecordBatch bytes from the client
    MaxTimestamp    int64
    NumRecords      int32
}

// partData: per-partition state. Concrete struct, not an interface.
// Each partition has its own RWMutex. Produce takes a write lock,
// Fetch takes a read lock. Different partitions are fully concurrent.
//
// Callers are responsible for holding the appropriate lock before
// calling methods. Write methods (pushBatch) require mu.Lock().
// Read methods (fetchFrom, searchOffset, listOffsets) require mu.RLock().
type partData struct {
    mu       sync.RWMutex   // protects all fields below

    topic    string
    index    int32
    batches  []storedBatch  // Phase 1: unbounded append-only slice (replaced by ring buffer in Phase 3)
    logStart int64          // Earliest available offset (0 initially)
    hw       int64          // High watermark = next offset to assign (= LEO for single broker)

    // For ListOffsets timestamp -3 (KIP-734): index of batch with max timestamp
    maxTimestampBatchIdx int  // -1 if no batches

    // Fetch waiters (long polling) -- see 09-fetch.md
    waiterMu sync.Mutex      // separate lock for waiter list (not RWMutex)
    waiters  []*fetchWaiter  // each waiter holds a shared wake channel
}
```

**Why a separate `waiterMu`:** Fetch waiters are registered under `waiterMu`
(not `mu`) so that registering a waiter doesn't require a write lock on the
partition. Produce notifies waiters under `waiterMu` after releasing `mu`.
This prevents Produce from holding the partition write lock while iterating
waiters (which could block on channel sends).

## partData Methods

```go
// pushBatch appends a batch, assigns offset. Returns base offset.
// Caller must hold pd.mu.Lock(). After releasing mu, caller should
// call pd.notifyWaiters() to wake long-polling Fetch requests.
// Phase 1: this is the only write method. Phase 3 splits into
// reserveOffset + commitBatch (see 15-wal-format-and-writer.md).
func (pd *partData) pushBatch(raw []byte, meta batchMeta) int64

// notifyWaiters wakes all registered fetch waiters.
// Caller must NOT hold pd.mu (takes pd.waiterMu internally).
func (pd *partData) notifyWaiters()

// searchOffset finds the batch containing the given offset via binary search.
// Returns (index, found, atEnd).
// Caller must hold pd.mu.RLock().
func (pd *partData) searchOffset(offset int64) (index int, found bool, atEnd bool)

// fetchFrom collects batches starting at offset, up to maxBytes.
// KIP-74: always includes at least one complete batch even if it exceeds maxBytes.
// Caller must hold pd.mu.RLock().
func (pd *partData) fetchFrom(offset int64, maxBytes int32) []storedBatch

// listOffsets resolves a timestamp query (-1=latest, -2=earliest, -3=maxTimestamp, >=0=lookup).
// Caller must hold pd.mu.RLock().
func (pd *partData) listOffsets(timestamp int64) (offset int64, ts int64)
```

---

## Opaque Batch Storage

The broker does NOT decode individual records within a batch. It:
1. Reads the 61-byte RecordBatch header to extract metadata (base offset, record
   count, max timestamp, compression type, attributes)
2. Assigns server-side base offsets (overwriting the client's 0-based offsets)
3. Stores the entire batch as raw bytes
4. Serves the raw bytes back on Fetch

This matches how Kafka works and avoids the cost of full deserialization.

**Do NOT use `kmsg.RecordBatch.ReadFrom()` for the produce/store path.**
Parsing batches into typed structs and re-encoding with `AppendTo` has
three problems:
1. **CRC invalidation.** Re-encoding changes byte layout. BaseOffset (bytes
   0-7) is outside the CRC-covered region (bytes 21+), so in-place overwrite
   of the raw bytes preserves CRC validity without recalculation.
2. **Storage pipeline mismatch.** `storedBatch.RawBytes` flows directly to
   WAL writes, S3 uploads, and fetch responses. Struct round-tripping adds
   a re-encode step at every boundary.
3. **Unnecessary allocations.** `ReadFrom` allocates and copies all header
   fields. We only need 4-5 fields, readable with `binary.BigEndian` calls
   at known offsets. Zero allocations.

Instead, use `parseBatchHeader()` (direct byte reads on the raw `[]byte`) to
extract metadata, and store `rp.Records` as-is. See `parseBatchHeader` below.

**Performance rationale:** Opaque batch storage is the single most important
performance decision. A 1KB message batch with 10 records would require ~10
object allocations to deserialize into structs. With opaque storage, it's a
single `[]byte` copy. At 500K msgs/sec, this is the difference between 5M
allocations/sec (constant GC pressure) and near-zero allocations.

---

## Offset Assignment

Client sends batches with base offset 0. Broker must:
1. Read `LastOffsetDelta` from the batch header (byte offset 23, 4 bytes, big-endian)
2. Read `RecordsCount` from the batch header (byte offset 57, 4 bytes, big-endian)
3. Assign `BaseOffset = partition.HW`
4. Write the assigned base offset into the first 8 bytes of the batch
5. Write `PartitionLeaderEpoch = 0` into bytes 12-15 of the batch
6. Advance `partition.HW += int64(LastOffsetDelta) + 1`

This is a mutation of the raw bytes. The 8-byte base offset at position 0 and
the 4-byte PartitionLeaderEpoch at position 12 are overwritten in-place.

**PartitionLeaderEpoch**: The broker always writes 0 (single-broker, no leader
elections). This field is outside the CRC-covered region (CRC covers bytes 21+),
so no recalculation is needed. Clients that use `OffsetsForLeaderEpoch` (key 23)
or Fetch with `CurrentLeaderEpoch` compare against this value, so it must be
set to a consistent known value rather than left as whatever the client sent.

**CRC recalculation**: The RecordBatch CRC (bytes 17-20) covers bytes 21 onward,
which does NOT include the base offset (bytes 0-7) or PartitionLeaderEpoch
(bytes 12-15). So overwriting these fields does NOT invalidate the CRC. No
recalculation needed.

Reference: `research/kafka-internals.md` RecordBatch format section.

---

## parseBatchHeader Helper

```go
// batchMeta holds fields extracted from the 61-byte RecordBatch header.
// Extracted via direct byte reads — no kmsg.RecordBatch.ReadFrom().
type batchMeta struct {
    BatchLength     int32
    Magic           int8
    CRC             uint32
    Attributes      int16
    LastOffsetDelta int32
    BaseTimestamp   int64
    MaxTimestamp    int64
    ProducerID      int64
    ProducerEpoch   int16
    BaseSequence    int32
    NumRecords      int32
}

// parseBatchHeader reads the 61-byte RecordBatch header from raw bytes.
// Returns an error if raw is shorter than 61 bytes.
// Does NOT validate CRC, Magic, or any field values — caller does that.
func parseBatchHeader(raw []byte) (batchMeta, error) {
    if len(raw) < 61 {
        return batchMeta{}, errShortBatch
    }
    return batchMeta{
        BatchLength:     int32(binary.BigEndian.Uint32(raw[8:12])),
        Magic:           int8(raw[16]),
        CRC:             binary.BigEndian.Uint32(raw[17:21]),
        Attributes:      int16(binary.BigEndian.Uint16(raw[21:23])),
        LastOffsetDelta: int32(binary.BigEndian.Uint32(raw[23:27])),
        BaseTimestamp:   int64(binary.BigEndian.Uint64(raw[27:35])),
        MaxTimestamp:    int64(binary.BigEndian.Uint64(raw[35:43])),
        ProducerID:      int64(binary.BigEndian.Uint64(raw[43:51])),
        ProducerEpoch:   int16(binary.BigEndian.Uint16(raw[51:53])),
        BaseSequence:    int32(binary.BigEndian.Uint32(raw[53:57])),
        NumRecords:      int32(binary.BigEndian.Uint32(raw[57:61])),
    }, nil
}
```

All byte offsets match the RecordBatch Header Quick Reference table below.
`BaseOffset` (bytes 0-7) and `PartitionLeaderEpoch` (bytes 12-15) are
deliberately omitted from `batchMeta` — both are overwritten by the broker
during offset assignment (BaseOffset = HW, PartitionLeaderEpoch = 0). They
are not trusted from the client.

---

## Phase Evolution

**Phase 1 (in-memory):** Handler takes `pd.mu.Lock()`, calls `pd.pushBatch()`,
releases lock, calls `pd.notifyWaiters()`. Synchronous, single step. `fetchFrom`
does binary search over `pd.batches` under `pd.mu.RLock()`.

The `[]storedBatch` slice grows unbounded in Phase 1 — this is fine because
there's no persistence and tests are short-lived. No eviction needed.

**Phase 3+ replaces the unbounded slice with a ring buffer and adds WAL
durability.** Handlers (`pushBatch`, `fetchFrom`) keep the same signatures;
only internals change. See `15-wal-format-and-writer.md`
for full details.

---

## RecordBatch Header Quick Reference

Byte layout (big-endian, 61 bytes total):

| Offset | Size | Field |
|--------|------|-------|
| 0 | 8 | BaseOffset |
| 8 | 4 | BatchLength (bytes after this field) |
| 12 | 4 | PartitionLeaderEpoch |
| 16 | 1 | Magic (must be 2) |
| 17 | 4 | CRC32C (covers bytes 21+) |
| 21 | 2 | Attributes |
| 23 | 4 | LastOffsetDelta |
| 27 | 8 | BaseTimestamp |
| 35 | 8 | MaxTimestamp |
| 43 | 8 | ProducerID |
| 51 | 2 | ProducerEpoch |
| 53 | 4 | BaseSequence |
| 57 | 4 | NumRecords |
| 61+ | var | Records (possibly compressed) |

Total batch size = BatchLength + 12 (BaseOffset + BatchLength fields).

---

## Compression

### Decision: Pass-Through

The broker does NOT decompress or recompress record batches on the produce/fetch
hot path. Compressed bytes from the client are stored and served as-is.

The compression codec is indicated in the batch `Attributes` field (bits 0-2):
- 0 = none, 1 = gzip, 2 = snappy, 3 = lz4, 4 = zstd

The broker needs to know the codec only for:
- Logging/metrics (which codecs are in use)
- ListOffsets MAX_TIMESTAMP (batch header has MaxTimestamp, no decompression needed)
- Log compaction (Phase 3+, must decompress to read keys)
- Topic-level `compression.type` config enforcement: if set to a specific codec,
  the broker must recompress. **Decision: support `producer` (pass-through) only
  in Phase 1.** Don't enforce topic-level compression rewriting until needed.

### What This Means for Tests

Compression round-trip tests (gzip, snappy, lz4, zstd) should pass with our
pass-through approach because the client compresses and the client decompresses.
The broker just stores bytes.

---

## When Stuck

These references are for understanding correct behavior and gathering context,
not for copying patterns. Find the optimal solution for our architecture.

1. Read `research/kafka-internals.md` for the authoritative RecordBatch byte
   layout and CRC coverage rules.
2. For offset assignment behavior, check `.cache/repos/franz-go/pkg/kfake/00_produce.go`
   -- see how kfake assigns offsets and handles batch headers.
3. For binary search over batches, check how kfake's fetch handler finds the
   right batch in `.cache/repos/franz-go/pkg/kfake/01_fetch.go`.
4. See `00b-testing.md` "When Stuck: Escalation Path" for additional tools.

---

## Acceptance Criteria

- `partData`, `storedBatch`, `batchMeta`, and `parseBatchHeader` compile
- `parseBatchHeader` correctly extracts all fields from a 61-byte header
- Offset assignment overwrites base offset in raw bytes without invalidating CRC
- `pushBatch` appends batch and advances HW correctly
- `fetchFrom` returns batches starting at requested offset with KIP-74 behavior
- `searchOffset` performs binary search correctly
- `listOffsets` handles all four timestamp modes (-1, -2, -3, >=0)
- Unit tests pass for `parseBatchHeader` and `searchOffset`

### Verify

```bash
# This work unit:
go test ./internal/cluster/ -run 'TestParseBatchHeader|TestSearchOffset|TestPushBatch|TestFetchFrom|TestListOffsets' -v -race -count=5

# Regression check (all prior work units):
go test ./... -race -count=5
```

### Commit

After tests pass, stage and commit all changes:

```bash
git add -A
git commit -m "06: partition data model — partData, storedBatch, parseBatchHeader, offset assignment"
```
