# Log Compaction

Background compaction engine for topics with `cleanup.policy: compact`. Phase 6
— requires S3 storage (Phase 4) since compaction operates on S3 objects.

Without implementing compaction, advertising `compact` as a supported
`cleanup.policy` value is a contract violation. Until this is implemented, the
broker must reject `cleanup.policy: compact` (or `compact,delete`) with
`INVALID_CONFIG` in CreateTopics and AlterConfigs.

Prerequisite: Phase 4 complete (S3 storage, per-partition S3 objects).

Read before starting: `18-s3-storage.md` (S3 object format with batch index
footer, key naming, flush pipeline, range-read fetch path),
`06-partition-data-model.md` (partData, storedBatch, RecordBatch header
format), `16-metadata-log.md` (metadata persistence — compaction watermark
stored here), `research/kafka-internals.md` (RecordBatch format — compaction
needs to read individual records within batches).

---

## Overview

Log compaction retains at least the last value for each key in a partition.
Older records with the same key are removed, keeping only the most recent
version. This is used for changelog topics (Kafka Streams KTable state
stores) and any application-level compacted topic. Note: klite stores
consumer group offsets in `metadata.log` (see `16-metadata-log.md`), not in
a `__consumer_offsets` topic, so the most common Kafka compaction use case
doesn't apply here. Compaction is needed for client compatibility — any Kafka
client that creates a compacted topic expects compaction semantics to work.

### Compaction vs. Deletion

| Policy | Behavior |
|--------|----------|
| `delete` | Remove entire batches/segments older than `retention.ms` or over `retention.bytes` (handled by `21-retention.md`) |
| `compact` | Remove superseded records (same key, older offset), keep latest per key |
| `compact,delete` | Both: compact first, then apply time/size retention to compacted data |

### Why Compaction Operates on S3 Objects

klite uses an interleaved WAL (all partitions in one log) for
durability, with per-partition S3 objects for long-term storage. The WAL is a
transient durability buffer — segments are deleted after S3 flush.

Compaction cannot operate on WAL segments because:
1. WAL segments interleave all partitions — rewriting one partition requires
   rewriting the entire segment.
2. WAL segments are transient — they're deleted after S3 flush. Compacting
   them is wasted work.
3. WAL data is recent (minutes old) — it rarely has enough key duplication to
   justify compaction overhead.

Instead, compaction reads and rewrites **per-partition S3 objects**. Each S3
object contains data for one partition (see `18-s3-storage.md`), making it the
natural unit for per-partition compaction. S3 PUT atomicity provides crash
safety: an object either exists completely or not at all.

---

## S3 Cost Model

Compaction is I/O-intensive. Every S3 object being compacted must be fully
read (GET), and compacted output must be written back (PUT). The primary cost
is not dollars — S3 API pricing is cheap ($0.0004/1000 GETs, $0.005/1000
PUTs, same-region data transfer is free) — but **latency and bandwidth**.
Each GET has ~50ms first-byte latency, and reading large objects competes with
consumer fetch requests for the broker's network link.

### Design goal: read each S3 object exactly once

A naive two-pass algorithm (pass 1: scan all objects to build offset map,
pass 2: re-read all objects to filter) doubles the S3 read volume. For 10 GB
of eligible data, that's 20 GB of GETs and ~200 seconds at 100 MB/s.

Instead, we use a **single-read, two-phase algorithm**: each object is read
from S3 exactly once and its raw (compressed) bytes are cached locally.
Phase 1 decompresses to build the offset map (keys + offsets only). Phase 2
re-decompresses from the cached raw bytes to filter and rewrite. The S3 cost
is halved (one GET per object, not two). The re-decompression in phase 2 is
CPU work only (~2 GB/s for zstd), not another S3 round-trip.

---

## Compaction Algorithm

### High-Level Flow

```
1. Select a dirty partition (has uncompacted S3 objects beyond min.compaction.lag.ms)
2. List S3 objects for that partition (below flush watermark)
3. Run orphan cleanup: delete any objects whose offset range is fully covered
   by a later object (handles prior crash-recovery leftovers)
4. Form windows of --compaction-window-bytes (default 256 MiB), starting from
   the anchor object (last compacted object containing cleanedUpTo)
5. For each window of N objects (oldest to newest):

     Phase 1 — Read & map (one S3 GET per object):
       a. GET each object, store raw bytes in local cache (memory or temp file)
       b. Decompress batches, iterate records, extract keys + offsets only
       c. Build offset map: key → highest offset across the window
       d. Discard decompressed data (only raw bytes + offset map retained)

     Phase 2 — Filter & rewrite (no S3 reads, CPU only):
       e. Re-decompress each cached object's raw bytes
       f. Filter: discard any record whose key has a higher offset in the map
       g. Build compacted batches from retained records, recompress, fix CRC
       h. Build batch index footer for the compacted output
       i. PUT one compacted output object (data + footer) to S3
       j. Invalidate the S3 reader's footer cache for this partition
       k. Advance cleanedUpTo, persist to metadata.log
       l. DELETE the N source objects (best-effort; orphan cleanup handles failures)

6. Repeat for next window until all dirty objects are processed
```

### Memory Model

The critical insight: **don't cache parsed records**. Parsing a record into
(key []byte, value []byte, offset, timestamp, headers) copies the full
decompressed payload into Go heap. A 256 MiB window of compressed S3 data
might decompress to 500 MiB–1 GB (zstd typically achieves 3–4x). Caching
all parsed records would consume 500 MiB–1 GB of RAM — far more than the
window size suggests.

Instead, cache the **raw compressed bytes** from the S3 GET (which IS
bounded by `--compaction-window-bytes`) and decompress twice:

| Phase | What's in memory | Size |
|-------|-----------------|------|
| Phase 1 (map build) | Raw object bytes + one decompressed batch at a time + offset map (keys only) | ~window + 1 batch + map |
| Between phases | Raw object bytes + offset map | ~window + map |
| Phase 2 (filter) | Raw object bytes + one decompressed batch at a time + output buffer | ~window + 1 batch + output |

**Peak memory per window:**
- Raw bytes cache: `--compaction-window-bytes` (256 MiB default)
- Offset map: ~80 bytes per unique key with ~16-byte keys (1M keys = ~80 MiB).
  The 80-byte estimate includes Go map per-entry overhead (~56 bytes) + key
  string (~16 bytes) + int64 value (8 bytes). For larger keys (e.g., 256-byte
  Avro keys), budget ~320 bytes per entry. If the offset map exceeds 25% of
  `--compaction-window-bytes`, the compactor logs a warning; consider reducing
  window size or using shorter keys.
- One decompressed batch: typically <1 MiB
- Output buffer: bounded by the **decompressed** size of the input, not the
  compressed size. Compaction removes records but the surviving records may
  lose their compression advantage (fewer records compress poorly). In the
  worst case (heavy removal from a well-compressed batch), the output batch
  can be larger than the corresponding compressed input. However, the output
  can never exceed the total decompressed size of all input batches. With
  typical 3-4x compression ratios, the output buffer may reach up to
  ~window × 3-4 in pathological cases. To avoid this, the compactor streams
  compacted batches to a temporary file when the output buffer exceeds
  `--compaction-window-bytes`, then uploads from the file.
- **Typical total: ~340 MiB** for the default 256 MiB window with 1M
  small keys and moderate compaction ratios. **Worst case: ~window × 2 + map**
  (when streaming output to a temp file caps the in-memory output buffer at
  `--compaction-window-bytes`).

The re-decompression in phase 2 is cheap CPU work. Zstd decompresses at
~2 GB/s, snappy at ~4 GB/s, lz4 at ~5 GB/s. Re-decompressing a 256 MiB
window adds <200ms of CPU time — negligible compared to the S3 GET latency
it avoids (~50ms × N objects).

### Why Windowed Processing

Processing all S3 objects at once would require holding the entire partition's
raw S3 data in memory. A 10 GB partition = 10 GB of cached raw bytes.

Windowed processing bounds memory to `--compaction-window-bytes` (default
256 MiB). Trade-off: keys that span multiple windows won't be deduplicated
in a single pass. Example: if key K appears in window 1 and window 3, the
copy in window 1 survives until window 1 is reprocessed in a future
compaction cycle (when the newer objects from window 3 are in the same
window). This is acceptable — Kafka's own compaction makes the same trade-off
with `log.cleaner.dedupe.buffer.size`, and each compaction pass makes
progress toward a fully deduplicated state.

### Optimization: Skip Clean Objects

Before GETting an object, check whether it could possibly contain
superseded records:

- **Single-object partition**: If there's only one eligible object, and it
  has already been compacted (offset ≤ cleanedUpTo), skip it entirely.
- **No overlapping keys possible**: If the window contains only one object,
  no key can be superseded within the window. Skip it unless it contains
  tombstones past `delete.retention.ms`.

These checks avoid reading objects that compaction cannot improve, reducing
unnecessary S3 GETs.

### Object Selection

Not all S3 objects are eligible for compaction:

- **Objects above the S3 flush watermark** (data still in WAL, not yet flushed
  to S3): Not eligible. Compaction only touches committed S3 objects.
- **Objects within `min.compaction.lag.ms`**: Not yet eligible. Determined by
  the MaxTimestamp of the last batch in the object. Default is 0 (compact
  immediately), but operators often set this to delay compaction.
- **The `cleanedUpTo` watermark and window formation**: `cleanedUpTo` tracks
  the highest offset that has been compacted. It controls which objects
  are considered "dirty" (needing compaction) vs. "clean". However, the
  **last compacted object** (the output of the previous compaction window,
  whose offset range straddles `cleanedUpTo`) is always included as the
  **first object** of the next window. This ensures cross-window key
  deduplication makes progress.

  Example: objects A (offsets 0-100), B (101-200), C (201-300). Window 1
  compacts A+B into AB' (offsets 0-200), sets `cleanedUpTo = 200`. Window 2
  includes AB' as the first object plus C. If key K appears in AB' (offset
  50) and C (offset 250), window 2 deduplicates K correctly — offset 250
  wins, and offset 50 in AB' is removed.

  Without this overlap, AB' would be excluded (its base offset 0 is below
  `cleanedUpTo = 200`), and key K at offset 50 would persist forever.

  To find the last compacted object: list S3 objects for the partition and
  find the object whose offset range contains `cleanedUpTo`. This object
  is the "anchor" for the next window. All objects after it (up to the
  flush watermark and respecting `min.compaction.lag.ms`) are the dirty
  objects that form subsequent windows.

### Offset Map

The offset map is the core data structure. It maps each record key to the
highest offset seen for that key within the current compaction window:

```go
type offsetMap struct {
    m map[string]int64  // key bytes (as string) -> highest offset
}
```

**Build direction: oldest to newest.** Scan objects in offset order,
overwriting the map entry on each key encounter. After scanning all objects
in the window, the map contains the highest offset per key. This is simpler
than Kafka's newest-to-oldest scan (which records first-seen offset per key)
because a Go `map` has no size limit — Kafka scans newest-first because its
fixed-size `SkimpyOffsetMap` uses hash collisions for eviction, where
first-seen wins. With a Go `map`, overwrite-on-encounter produces the same
result and aligns with the natural forward read direction of S3 objects.

The offset map is rebuilt for each window and discarded after. Peak memory
for the map depends on key sizes: ~80 bytes per unique key with ~16-byte
keys, ~320 bytes per unique key with ~256-byte keys. See Memory Model
above for detailed estimates and the warning threshold.

### Record-Level Processing

Compaction is the one place where the broker must decompress and parse
individual records within a RecordBatch (normally batches are opaque).

**Phase 1 — Build offset map (extract keys only, discard values):**

For each S3 object in the window:

1. GET the object from S3 (the only S3 read for this object — full object,
   not a range read, since compaction needs all bytes for phase 2)
2. Store the raw bytes in the local cache
3. Parse the batch index footer (last N bytes of the object, see
   `18-s3-storage.md` S3 Object Format). The footer provides batch
   boundaries (bytePosition, batchLength) without scanning the data
   section.
4. For each batch (using footer-provided positions):
   a. Read the batch header to get Attributes, BaseOffset, LastOffsetDelta
   b. Decompress the records section
   c. Iterate individual records — extract only key bytes and offset delta
   d. Update offset map: `offsetMap[key] = BaseOffset + offsetDelta`
   e. Discard decompressed data (it will be re-decompressed in phase 2)

After all objects in the window are scanned, the offset map is complete.

**Phase 2 — Filter and rewrite (no S3 reads, CPU only):**

For each cached object's raw bytes (same order as phase 1):

5. Walk RecordBatch boundaries again
6. For each batch:
   a. Decompress the records section (from the cached raw bytes)
   b. For each record: check if this is the highest offset for its key
   c. Retain if: key is null, OR this is the highest offset for this key
      in the offset map, OR the record is a tombstone within
      `delete.retention.ms` (see Tombstone age computation above)
   d. If no records are retained, skip this batch entirely (do not emit
      an empty batch — a RecordBatch with NumRecords=0 is invalid in
      the Kafka protocol)
   e. Build a new batch from retained records only
   f. Recompress using the original codec
   g. Update header fields: `NumRecords`, `LastOffsetDelta`, `MaxTimestamp`
   h. Recompute CRC-32C over bytes 21+
7. Concatenate compacted batches into the data section of a new S3 object
8. Build the batch index footer (see `18-s3-storage.md` S3 Object Format):
   record (baseOffset, bytePosition, batchLength, lastOffsetDelta) for
   each compacted batch, append entry count + magic (0x4B4C4958)
9. PUT the complete object (data + footer) to S3

**Null vs. empty keys:** A null key (absent in the record — encoded as
varint length -1) is always retained because there is no key to deduplicate
on. A zero-length key (present but empty — varint length 0) IS a valid
deduplication key and participates in compaction normally.

**CRC recalculation:** Unlike the produce path (where BaseOffset is outside
the CRC-covered region and can be overwritten without recalculation),
compaction changes the batch payload (bytes 21+), which IS inside the CRC
region. The CRC-32C must be recomputed after building the new batch. Fields
that change: records payload, `NumRecords` (byte offset 57), `LastOffsetDelta`
(byte offset 23), and potentially `MaxTimestamp` (byte offset 35).

**Sparse offsets after compaction:** Compaction does not change record
offsets — it creates "holes" in the offset sequence by removing records.
After compaction, a batch may have non-contiguous offsets (e.g., BaseOffset=0,
records at offsets 0, 5, 12 — offsets 1-4 and 6-11 were removed). The fetch
path handles this correctly: Fetch responses return whole batches, and
consumers skip records below their requested offset. `ListOffsets EARLIEST`
returns `logStartOffset`, which is unaffected by compaction. The first
available record may be at a higher offset than `logStartOffset`, which is
standard Kafka behavior for compacted topics.

### Tombstones

A record with a non-null key and null value is a **tombstone** (delete marker).
Tombstones are retained during compaction for `delete.retention.ms` (default:
24 hours) so that downstream consumers see the deletion. After this period,
tombstones are removed in the next compaction pass.

**Tombstone age computation:** The record's absolute timestamp depends on the
batch's timestamp type (Attributes bit 3):

- **CreateTime (bit 3 = 0):** The record's timestamp is
  `BaseTimestamp + timestampDelta` (where `timestampDelta` is the varint
  delta encoded in each individual record). Each record has its own
  timestamp set by the producer.
- **LogAppendTime (bit 3 = 1):** All records in the batch share the batch's
  `MaxTimestamp` as their effective timestamp (the broker overwrote the
  producer's timestamps at append time). Use `MaxTimestamp` from the batch
  header, ignoring individual record timestamp deltas.

The tombstone is eligible for removal when:
`recordTimestamp < now - delete.retention.ms`.

### Transactional Records

Compaction must handle transactional batches (Attributes bit 4 set) and
control batches (Attributes bit 5 set) correctly:

- **Records from committed transactions**: Treated normally — deduplicated
  by key like any other record.
- **Records from aborted transactions**: Must be removed entirely during
  compaction. These records should never be the "latest" value for a key.
  The compactor checks the batch's ProducerID and ProducerEpoch against the
  partition's aborted transaction index (built during S3 flush or maintained
  in memory).
- **Control batches** (COMMIT/ABORT markers): Retained until all data batches
  from their transaction have been compacted away. In practice, control
  batches are removed when the compactor encounters no data batches
  referencing the same ProducerID+ProducerEpoch in the offset range being
  compacted.

For the initial implementation, a simpler approach is acceptable: skip
transactional batches during compaction (leave them as-is). This is safe
because uncommitted/aborted records are already filtered at read time by
read_committed consumers. The only cost is that aborted transaction data
is not reclaimed. Add full transactional compaction support as a follow-up.

---

## S3 Object Rewrite Protocol

### Crash Safety via S3 Atomicity

S3 PUTs are atomic — an object either exists completely or not at all. This
gives us crash safety without the `.cleaned`/`.swap` rename protocol that
Kafka uses for local files. Each window's rewrite follows this sequence:

```
1. GET each source object in the window (already done during scan phase)
2. Filter and build compacted data + batch index footer in memory
3. PUT the compacted output object (data + footer)
4. Invalidate the S3 reader's footer cache for this partition
5. On success: update cleanedUpTo in metadata.log (persisted before delete)
6. On success: DELETE the source objects (best-effort cleanup)
```

**Why persist `cleanedUpTo` before deleting source objects:** This ordering
ensures that after any crash, the compactor never re-processes objects that
have already been compacted. If deletion fails, the source objects become
orphans (duplicate data) but are cleaned up by the orphan cleanup routine
(see below). The alternative (delete-then-persist) risks a partial DELETE
crash creating a mix of compacted and surviving source objects that the
compactor re-processes into a second compacted object — orphaning the first.

**Crash at step 3 (PUT fails):** Source objects still exist, no data loss.
`cleanedUpTo` is not advanced. Next compaction pass retries the same window.

**Crash at step 5 (metadata.log update fails):** `cleanedUpTo` is stale.
Both source and compacted objects exist. Next compaction pass re-compacts
the same window. This is idempotent — compacting already-clean data
produces identical output. The re-compaction reads both source objects and
the compacted output, but since they cover overlapping offset ranges, the
offset map correctly resolves to the same latest offsets. Wasteful but safe.

**Crash at step 6 (DELETE fails, partial or complete):** `cleanedUpTo` has
been advanced, so the compactor won't re-process these objects. The source
objects become orphans — present in S3 but below `cleanedUpTo`. They are
cleaned up by the orphan cleanup routine on the next compaction cycle.

### Orphan Cleanup

After any crash, S3 may contain orphaned source objects that were not
deleted (step 6 failed). These orphans waste storage but don't affect
correctness — the compacted output already contains the authoritative data,
and `cleanedUpTo` prevents the compactor from re-processing them.

The orphan cleanup runs at the start of each partition's compaction cycle,
before selecting windows:

```
1. List all S3 objects for the partition
2. Sort by base offset
3. For any two objects whose offset ranges overlap (i.e., object A's last
   offset >= object B's base offset), the newer object (higher last offset
   or later upload) supersedes the older one
4. DELETE all superseded objects (batch delete, up to 1000 per call)
```

This is a lightweight check — it only compares base offsets from the S3
listing (no GETs required). It runs at most once per partition per
compaction cycle and handles all crash-recovery edge cases: partial
DELETEs, repeated PUTs from retried compaction, and any other scenario
that leaves overlapping objects.

### New Object Key

The compacted object uses the same key format as regular S3 objects:

```
s3://<bucket>/<prefix>/<topic>/<partition>/<baseOffset>.obj
```

The `baseOffset` is the lowest offset retained in the compacted object.
After compaction, the new object replaces multiple source objects that
covered a contiguous offset range. The base offset of the compacted object
equals the base offset of the first source object.

If compaction removes all records from an entire window (every key was
superseded by a later window), the source objects are deleted with no
replacement.

### Window → One Object

Each compaction window produces at most one output S3 object, merging
multiple source objects. This naturally reduces S3 object count (fewer
LIST/GET operations on the read path) while also deduplicating keys.

The output object size is bounded by the input window size: compaction can
only remove records, never add them. Since the input is bounded by
`--compaction-window-bytes` (default 256 MiB), the compressed output is
always ≤ 256 MiB. (In rare cases where heavy record removal causes poor
re-compression, the output may briefly exceed the compressed input for
individual batches — but the total is still bounded by the decompressed
input size, and the compactor streams to a temp file if needed; see
Memory Model above.) S3's maximum PUT size is 5 GiB, well above this
bound. No output splitting is needed.

### S3 Operation Summary (Per Partition Compaction Cycle)

| Operation | Count | Notes |
|-----------|-------|-------|
| ListObjectsV2 | 1 | One-time listing before compaction starts (also serves orphan cleanup) |
| DELETE (orphans) | 0-1 | Batch delete of orphaned objects from prior crash, if any |
| GET | N (source objects) | One GET per source object per window, read once |
| PUT | W (windows) | One output object per window |
| DELETE (sources) | W (batch deletes) | One batch delete per window, after persist |

For a partition with 40 source objects processed in 4 windows of 10:
1 LIST + 40 GETs + 4 PUTs + 4 batch DELETEs + 0-1 orphan DELETE = 49-50 S3 operations total.

---

## Background Goroutine

### Design

A single `compactionLoop` goroutine, similar to `retentionLoop` (see
`21-retention.md`). It wakes periodically and selects one dirty partition at
a time.

```go
func (b *Broker) compactionLoop(ctx context.Context) {
    ticker := time.NewTicker(b.cfg.CompactionCheckInterval) // default 30s
    defer ticker.Stop()
    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            b.compactOneDirtyPartition(ctx)
        }
    }
}
```

### When Compaction Triggers

Compaction must avoid two failure modes: (1) wasting S3 LIST calls polling
for work that doesn't exist, and (2) letting dirty data accumulate unbounded
because nothing triggers compaction.

The trigger is **local, not S3-based**: the S3 flush pipeline (see
`18-s3-storage.md`) already runs periodically and knows when it creates new
S3 objects. After each flush, it increments a per-partition dirty object
counter on `partData`:

```go
type partData struct {
    // ... existing fields ...
    dirtyObjects  int32  // objects flushed since last compaction (atomically updated)
    lastCompacted time.Time  // wall clock time of last successful compaction
}
```

The `compactionLoop` goroutine reads these in-memory counters — no S3 LIST
calls needed to decide *whether* to compact. It only issues a LIST call
once it has selected a partition and begins actual compaction.

### Dirty Counter Rehydration on Startup

`dirtyObjects` and `lastCompacted` are volatile — they're lost on restart.
Without rehydration, no partition is eligible for compaction until new data
is flushed to S3 (because `dirtyObjects` is 0 and `lastCompacted` is zero).
This means a read-heavy deployment that restarts would never compact
pre-restart dirty data.

On startup, after replaying `metadata.log` (which restores `cleanedUpTo`
per partition), the compaction system rehydrates dirty counters for
partitions with `cleanup.policy` containing `compact`:

```
For each compacted partition:
    List S3 objects for the partition (one LIST call)
    Count objects with base offset > cleanedUpTo
    Set dirtyObjects = count
    Set lastCompacted = time.Time{} (zero — forces staleness check to pass)
```

This is a one-time cost at startup: one ListObjectsV2 per compacted
partition. For 100 compacted partitions, this adds ~5 seconds to startup
(100 × ~50ms LIST latency). Non-compacted partitions are skipped.

### Eligibility Rules

A partition is eligible for compaction when ANY of:

1. **Dirty object threshold**: `dirtyObjects >= --compaction-min-dirty-objects`
   (default: 4). This means at least 4 S3 flush cycles have deposited new
   objects since the last compaction. With the default 10-minute flush
   interval, compaction triggers after ~40 minutes of writes. This
   heuristic approximates Kafka's `min.cleanable.dirty.ratio` without
   requiring size calculations.

2. **Staleness guarantee**: `time.Since(lastCompacted) > max.compaction.lag.ms`
   AND `dirtyObjects > 0`. Forces compaction even if the dirty count is low,
   ensuring no partition goes uncompacted indefinitely.

### Partition Selection

`compactOneDirtyPartition` scans all partitions with `cleanup.policy`
containing `compact` and picks the one with the highest `dirtyObjects`
count. Ties are broken by `lastCompacted` (oldest wins). This selects the
partition that will benefit most from compaction without any S3 calls.

### S3 Request Concurrency

Compaction shares the broker's S3 connection pool with the fetch path and
flush pipeline. It must not starve those paths.

**GETs** (reading source objects): Compaction acquires from the same
`s3.read.concurrency` semaphore (default 64) that the fetch path uses.
Within a window, source objects are fetched in parallel up to
`--compaction-s3-concurrency` (default 4) concurrent GETs. This means
compaction uses at most 4 of the 64 available GET slots, leaving 60 for
consumer fetches.

**PUTs** (writing compacted output): One PUT per window. Acquires from the
same `s3.upload.concurrency` semaphore (default 4) that the flush pipeline
uses.

**DELETEs** (removing source objects): S3 supports batch delete
(DeleteObjects, up to 1000 keys per call). Use a single batch DELETE per
window instead of N individual calls.

### Bandwidth Throttling

Beyond concurrency limits, compaction also caps its S3 read throughput using
a `golang.org/x/time/rate.Limiter` (configurable via `--compaction-read-rate`,
default: 50 MB/s). This provides a hard bandwidth ceiling even when the
semaphore slots are available, preventing compaction from saturating the
network link during low-traffic periods when many semaphore slots are free.

### Graceful Behavior

- Between windows, check `ctx.Done()` for graceful shutdown
- Compact one partition at a time (not parallel across partitions)
- If a compaction window takes longer than `--compaction-check-interval`,
  the next tick is skipped (the ticker just fires again after the current
  compaction completes)

---

## Interaction with Retention (`compact,delete`)

When `cleanup.policy` is `compact,delete`, both compaction and retention
apply. The two goroutines (`compactionLoop` and `retentionLoop`) must not
process the same partition concurrently.

### Coordination

A per-partition `compactionMu sync.Mutex` prevents concurrent compaction and
retention on the same partition. Both goroutines acquire this lock before
operating on a partition's S3 objects. The lock is separate from `partData.mu`
(which protects the hot produce/fetch path) to avoid blocking data flow
during background maintenance.

```go
type partData struct {
    // Lock ordering: compactionMu → mu
    // Always acquire compactionMu before mu. Never hold mu when acquiring
    // compactionMu. advanceLogStartOffset (called by retention and
    // DeleteRecords) acquires mu internally, so callers must hold
    // compactionMu first.
    compactionMu sync.Mutex  // held by compaction, retention, or DeleteRecords
    mu           sync.RWMutex  // protects partition state (produce/fetch hot path)
    // ...
}
```

**Lock ordering: `compactionMu` → `pd.mu`.** Both compaction and retention
call methods that acquire `pd.mu` internally (compaction may read
`logStartOffset` under `pd.mu.RLock()`; retention calls
`advanceLogStartOffset` which acquires `pd.mu.Lock()`). To prevent deadlocks,
`compactionMu` must always be acquired before `pd.mu`. No code path may hold
`pd.mu` and then acquire `compactionMu`. See `21-retention.md` "Lock Ordering"
for the full specification.

**Lock granularity: per-window, not per-partition.** The compaction loop
acquires `compactionMu` at the start of each window and releases it after
the window completes (after PUT + persist `cleanedUpTo` + DELETE). Between
windows, the lock is released. This means the retention goroutine can run
between compaction windows, reducing the maximum time retention is blocked
for a partition. A 256 MiB window at 50 MB/s takes ~5 seconds; without
per-window release, a partition with 10 windows would block retention for
~50 seconds.

```go
// In compactPartition:
for _, window := range windows {
    pd.compactionMu.Lock()
    err := compactWindow(ctx, window)
    pd.compactionMu.Unlock()
    if err != nil { return err }
    // Retention can run here between windows
    if ctx.Err() != nil { return ctx.Err() }
}
```

### Processing Order

For `compact,delete`:
1. Compaction runs first (deduplicates keys within existing data)
2. Retention runs on the compacted data (deletes old compacted objects by
   time/size)

This matches Kafka's behavior: compaction produces a "clean" log, then
retention trims the clean log. The two goroutines don't need explicit
ordering — the `compactionMu` prevents overlap, and each is idempotent.
Because the lock is released between windows, retention may interleave
with multi-window compaction runs on the same partition.

---

## Persistence: `cleanedUpTo` Watermark

### New metadata.log Entry

Add a new entry type to `metadata.log` (see `16-metadata-log.md`):

| Type | Code | Payload | When Appended |
|------|------|---------|---------------|
| COMPACTION_WATERMARK | 0x07 | topic name, partition, cleanedUpTo offset | After successful compaction |

On startup replay, the most recent COMPACTION_WATERMARK per partition sets
the `cleanedUpTo` field on `partData`. This avoids re-compacting
already-clean data after a restart.

### In-Memory State

```go
type partData struct {
    // ... existing fields ...
    cleanedUpTo int64  // highest offset that has been compacted (persisted)
}
```

### Semantics

`cleanedUpTo` is the highest offset in the compacted output object. It
identifies where the "clean" portion of the partition ends. The object
containing `cleanedUpTo` (the "anchor" object) is always included as the
first object in the next compaction window. All objects whose base offset
is above `cleanedUpTo` are dirty and need compaction. Objects whose entire
offset range is below `cleanedUpTo` are fully superseded and can be deleted
as orphans (if they still exist from a prior crash).

---

## Configuration

### New Topic Configs

These are standard Kafka compaction configs:

| Config | Default | Description |
|--------|---------|-------------|
| `min.compaction.lag.ms` | 0 | Minimum time before a record is eligible for compaction |
| `max.compaction.lag.ms` | `math.MaxInt64` | Maximum time before compaction is forced |
| `delete.retention.ms` | 86400000 (24h) | How long tombstones are retained after compaction |

### New Broker Config

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--compaction-check-interval` | `KLITE_COMPACTION_CHECK_INTERVAL` | `30s` | How often to scan in-memory dirty counters for eligible partitions |
| `--compaction-min-dirty-objects` | `KLITE_COMPACTION_MIN_DIRTY_OBJECTS` | `4` | Minimum dirty S3 objects before a partition is eligible for compaction |
| `--compaction-window-bytes` | `KLITE_COMPACTION_WINDOW_BYTES` | `268435456` (256 MiB) | Max total source object size per compaction window. Controls peak memory during compaction. |
| `--compaction-s3-concurrency` | `KLITE_COMPACTION_S3_CONCURRENCY` | `4` | Max concurrent S3 GETs for compaction. These count against the shared `s3.read.concurrency` semaphore. |
| `--compaction-read-rate` | `KLITE_COMPACTION_READ_RATE` | `52428800` (50 MiB/s) | Max S3 read throughput for compaction. Prevents compaction from starving consumer fetches. Set to 0 for unlimited. |

---

## Interaction with CreateTopics / AlterConfigs

### Before Compaction Is Implemented

Until this work unit is complete, the broker must reject `cleanup.policy`
values containing `compact`:

```go
// In topic config validation:
if policy == "compact" || policy == "compact,delete" {
    return INVALID_CONFIG  // "log compaction not yet supported"
}
```

This avoids silently accepting a config that implies compaction behavior
without actually providing it.

### After Compaction Is Implemented

Accept `compact` and `compact,delete`. The compaction goroutine will
automatically pick up topics with these policies. Remove the
`TestCompactionRejectedBeforeImpl` guard test as part of this work unit.

---

## Dependencies

### Compression Libraries

Compaction requires decompressing and recompressing RecordBatches. The
required libraries are already in the dependency list (see `01-foundations.md`):

- `github.com/klauspost/compress/zstd`
- `github.com/klauspost/compress/snappy`
- `github.com/pierrec/lz4/v4`
- `compress/gzip` (stdlib)

### Additional Dependency

- `golang.org/x/time/rate` — for I/O throttling (may already be in the
  dependency tree via other packages)

### Package Location

Compaction logic lives in the `internal/s3/` package alongside the S3
reader/writer, since it operates exclusively on S3 objects. No separate
`internal/compaction/` package — compaction is an S3 maintenance operation.

```
internal/
  s3/
    flusher.go          # S3 flush pipeline (existing)
    reader.go           # S3 range reads (existing)
    compactor.go        # Compaction: offset map, record filtering, object rewrite
    compactor_test.go   # Unit tests with synthetic RecordBatches
    records.go          # RecordBatch decompression/recompression, record iteration
    records_test.go     # Unit tests for record-level parsing
```

The `records.go` file extracts the record-level decompression/iteration logic
that compaction needs. This is the only place in the broker that parses
individual records within a batch (the produce/fetch hot path treats batches
as opaque). Keep it isolated to avoid accidentally pulling record parsing
into the hot path.

---

## When Stuck

1. Search `.cache/repos/kafka/` for `LogCleaner` and `Cleaner` — Kafka's
   compaction implementation. Focus on `Cleaner.cleanInto()` for the core
   record-filtering loop and `SkimpyOffsetMap` for the offset map design.
2. Search `.cache/repos/kafka/` for `DefaultRecord` — the individual record
   encoding within a RecordBatch (varint deltas, key/value layout).
3. Read `research/kafka-internals.md` for RecordBatch format details (needed
   to parse individual records within compressed batches).
4. For `compact,delete` interaction, search for `CleanupPolicy` in the Kafka
   source — it shows how both policies are applied together.
5. WarpStream's compaction documentation (public) describes a streaming k-way
   merge over S3 objects: https://docs.warpstream.com/warpstream/kafka/reference/protocol-and-feature-support/compacted-topics
6. See `00b-testing.md` "When Stuck: Escalation Path" for additional tools.

---

## Acceptance Criteria

- Unit tests:
  - `TestOffsetMapBuild` — build offset map from synthetic RecordBatches,
    verify latest offset per key
  - `TestCompactionBasicDedup` — write S3 objects with duplicate keys, compact,
    verify only latest per key remains in the output object
  - `TestCompactionNilKeyRetained` — records with null keys are never removed
  - `TestCompactionEmptyKeyDedup` — records with zero-length (non-null) keys
    are deduplicated normally
  - `TestCompactionTombstoneRetention` — tombstones retained within
    delete.retention.ms, removed after. Tests both CreateTime and
    LogAppendTime batches.
  - `TestCompactionMinLag` — objects within min.compaction.lag.ms are not
    compacted
  - `TestCompactionPreservesOrder` — relative order of retained records is
    preserved within and across batches
  - `TestCompactionCRCValid` — compacted batches have valid CRC-32C
  - `TestCompactionCompression` — round-trip through each compression codec
    (gzip, snappy, lz4, zstd, none)
  - `TestCompactionEmptyBatchSkipped` — batch where all records are removed
    produces no output batch (no NumRecords=0 batches)
  - `TestCompactionSparseOffsets` — after compaction, batches have
    non-contiguous offsets; verify fetch returns correct data
  - `TestCompactionOrphanCleanup` — simulate crash after PUT but before
    DELETE (leave overlapping objects in S3); verify next compaction cycle
    detects and deletes orphans
  - `TestCompactionCrossWindowDedup` — key appears in compacted object and
    newer dirty object; verify the anchor object is included in the next
    window and the older record is deduplicated
  - `TestCompactionOutputFooter` — compacted output object has a valid batch
    index footer; footer entries match actual batch boundaries in the data
    section; S3 reader can use footer for range reads on compacted objects
  - `TestCompactionFooterCacheInvalidation` — after compaction, verify the
    S3 reader's footer cache no longer holds entries for the old objects;
    fetches use the new compacted object's footer
  - `TestRecordIteration` — parse individual records from compressed batches
    with varint deltas

- Integration tests:
  - `TestCompactionEndToEnd` — create compacted topic, produce duplicates,
    trigger S3 flush, wait for compaction, consume and verify only latest
    values per key
  - `TestCompactDeletePolicy` — cleanup.policy=compact,delete, verify both
    compaction and retention apply
  - `TestCompactionS3ObjectCount` — verify that compaction reduces the number
    of S3 objects for a partition
  - `TestCompactionIdempotent` — run compaction twice on already-clean data,
    verify output is identical
  - `TestCompactionWatermarkSurvivesRestart` — compact, restart broker,
    verify cleanedUpTo is restored from metadata.log
  - `TestCompactionDirtyCounterRestart` — flush S3 objects, restart broker,
    verify dirtyObjects counter is rehydrated and compaction triggers
    without new writes

### Verify

```bash
# This work unit:
go test ./internal/s3/ -run 'TestCompaction|TestOffsetMap|TestRecord' -v -race -count=5
go test ./test/integration/ -run 'TestCompaction|TestCompactDelete' -v -race -count=5

# Regression check:
go test ./... -race -count=5
```

### Commit

After tests pass, stage and commit all changes:

```bash
git add -A
git commit -m "22: log compaction — S3 object rewrite, offset map, tombstones, orphan cleanup"
```

---

## Design Decisions Log

Decisions made during review, recorded for future reference:

1. **Persist-before-delete ordering**: `cleanedUpTo` is persisted to
   `metadata.log` before deleting source objects. This ensures crash recovery
   never re-processes already-compacted data. Trade-off: crashes during DELETE
   leave orphaned objects, cleaned up by the orphan cleanup routine.

2. **No `--compaction-max-object-bytes`**: Output is always ≤ input window
   size (compaction removes data). S3's 5 GiB PUT limit is well above the
   default 256 MiB window. Removed to avoid an unspecified splitting code path.

3. **Per-window lock release**: `compactionMu` is released between windows,
   allowing retention to interleave with multi-window compaction. This caps
   retention blocking at one window duration (~5s) instead of an entire
   partition's compaction run.

4. **Anchor object in windows**: The last compacted object is always included
   as the first object in the next window. This ensures cross-window key
   deduplication makes progress toward a fully deduplicated state.

5. **Dirty counter rehydration**: On startup, one LIST per compacted partition
   restores dirty object counts. This ensures compaction resumes after a restart
   even without new writes.

6. **Output buffer streaming**: When the compacted output exceeds
   `--compaction-window-bytes`, it's streamed to a temp file instead of held
   in memory. This handles the pathological case where heavy record removal
   degrades re-compression ratios.

7. **Batch index footer on output objects**: Compacted output objects include
   the same batch index footer as flush-produced objects (see
   `18-s3-storage.md`). This ensures the S3 fetch path can use range reads
   on compacted objects identically to non-compacted ones. The compactor
   invalidates the S3 reader's footer cache after rewriting objects for a
   partition, so stale footers are never served.

8. **Batch index footer over file-based fetch cache**: The footer enables
   proportional S3 reads (read amplification ≈ 1x) without any local state.
   A file-based fetch cache was considered and rejected: it adds disk budget
   management, LRU eviction, and cache invalidation complexity, while
   degrading under disaster recovery (all consumers start cold, cache
   churns). With the footer, each fetch reads only what the consumer needs.
   A lightweight in-memory read-ahead buffer is noted as a future
   optimization for sequential throughput but is not required. See
   `18-s3-storage.md` "Future Optimization: Read-Ahead Buffer".
