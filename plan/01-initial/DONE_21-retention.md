# Retention Enforcement

Background goroutine that enforces `retention.ms` and `retention.bytes` topic
configs by deleting expired data. Phase 6 — requires WAL (Phase 3) and
ideally S3 (Phase 4) to be meaningful.

Without retention enforcement, `retention.ms` and `retention.bytes` configs
are decorative — the broker stores them but never acts on them. This is a
contract violation if clients rely on data being cleaned up.

Prerequisite: Phase 3 complete (WAL persistence, metadata.log). Phase 4 (S3)
is recommended but not required — retention can enforce on WAL-only data.

Read before starting: `15-wal-format-and-writer.md` (WAL segment structure,
segment rotation, WAL index), `18-s3-storage.md` (S3 object key naming, flush
pipeline), `06-partition-data-model.md` (partData, logStartOffset, storedBatch),
`16-metadata-log.md` (LOG_START_OFFSET entry type — retention must persist
logStartOffset advances), `22-log-compaction.md` (compactionMu — coordination
for `compact,delete` topics).

---

## Overview

Retention enforcement is a background process that periodically scans all
partitions and deletes data that exceeds configured retention limits. It
operates by calling `advanceLogStartOffset` on each partition — a shared
method also used by DeleteRecords (key 21, see `14-admin-apis.md`).

Two retention dimensions, evaluated independently:

| Config | Behavior |
|--------|----------|
| `retention.ms` | Delete batches older than this duration (based on batch MaxTimestamp) |
| `retention.bytes` | Delete oldest batches until partition size is under this limit (per-partition, not per-topic) |

If both are set, the more aggressive policy wins (whichever deletes more).

A value of `-1` means infinite (no retention limit). This is the default for
`retention.bytes`. The default for `retention.ms` is 604800000 (7 days).

**Note on `retention.bytes`:** This is a per-partition limit, not per-topic.
A 3-partition topic with `retention.bytes=1000` allows up to 3000 bytes total
across the topic. This matches Kafka's behavior.

---

## `advanceLogStartOffset` — Shared Method

Both retention enforcement and DeleteRecords (key 21) need to advance
`logStartOffset`, trim old data, persist the change, and clean up the WAL
index. This shared method is the single implementation for all of that:

```go
// advanceLogStartOffset advances the partition's logStartOffset to newOffset,
// trims old batches, persists the change to metadata.log, prunes the WAL
// index, and updates the partition size counter.
//
// Called by: retention enforcement, DeleteRecords handler.
// Caller must NOT hold pd.mu (this method acquires it internally).
// Caller must hold pd.compactionMu (see "Lock Ordering" section below).
//
// Concurrent safety: multiple goroutines may call this concurrently (e.g.,
// retention and DeleteRecords on the same partition). The early-return check
// is inside pd.mu.Lock(), so there is no TOCTOU race. If two callers race,
// the first one wins and the second is a no-op. The metadata.log append is
// also inside the lock, so at most one LOG_START_OFFSET entry is written per
// advancement.
//
// Lock ordering: compactionMu → pd.mu. Caller always holds compactionMu.
// See "Lock Ordering" section below.
func (pd *partData) advanceLogStartOffset(newOffset int64, metaLog *metadata.Log) error {
    pd.mu.Lock()
    if pd.logStart >= newOffset {
        pd.mu.Unlock()
        return nil  // already at or past newOffset — nothing to do
    }

    // 1. Persist to metadata.log FIRST, while still holding pd.mu.
    //    Crash safety: if we crash after this but before trimming,
    //    restart replays the LOG_START_OFFSET entry and re-trims.
    //    Holding pd.mu during the append ensures that concurrent callers
    //    cannot both persist — only one caller observes logStart < newOffset.
    //
    //    AppendLogStartOffset performs a synchronous fsync (see
    //    "metadata.log Fsync Semantics" section below). The fsync adds
    //    ~2ms under the lock, but this path runs at most once per
    //    retention cycle per partition (every 5 minutes), so the stall
    //    is negligible.
    if err := metaLog.AppendLogStartOffset(pd.topic, pd.index, newOffset); err != nil {
        pd.mu.Unlock()
        return err
    }

    // 2. Trim all batches below newOffset and set logStart.
    pd.trimBatchesLocked(newOffset)
    pd.mu.Unlock()

    // 3. Prune WAL index entries for this partition below newOffset.
    pd.walIndex.PruneBefore(pd.topic, pd.index, newOffset)

    // 4. Trigger WAL segment cleanup check. On idle systems where no new
    //    data is being produced, segment rotation never fires, so stale
    //    segments could linger. This call reads per-partition logStartOffset
    //    values under pd.mu.RLock() per partition (see
    //    "TryCleanupSegments Synchronization" below) and deletes segments
    //    whose data is fully below all partitions' logStartOffset. It is
    //    cheap (in-memory scan, no disk I/O beyond unlink) and idempotent.
    pd.walWriter.TryCleanupSegments()

    return nil
}
```

**Key properties:**

- **No TOCTOU race:** The early-return check (`pd.logStart >= newOffset`)
  and the metadata.log append both happen under `pd.mu.Lock()`. If two
  goroutines call `advanceLogStartOffset` concurrently, only one observes
  `logStart < newOffset` and persists the new value. The other sees the
  updated `logStart` and returns immediately. No redundant LOG_START_OFFSET
  entries, no wasted work.
- **Persistence first:** The LOG_START_OFFSET entry is written to
  `metadata.log` (with synchronous fsync) before any in-memory trimming.
  If the broker crashes mid-trim, restart replays the entry and re-trims.
  No data reappears.
- **Single lock acquisition for trim:** Trimming happens in one pass under
  a single `pd.mu.Lock()` acquisition. Even trimming 100K batches at ~100ns
  each takes only ~10ms — a one-time stall that happens at most once per
  partition per retention cycle (every 5 minutes). This is comparable to a
  Go GC pause and is acceptable for a single-broker system. The simpler
  single-pass design eliminates the chunked-loop complexity and the edge
  cases it introduces (logStart not being set correctly between chunks,
  continuation loop exit conditions).
- **`logStart` always set to `newOffset`:** After trimming, `pd.logStart`
  is set to exactly `newOffset`, not to the base offset of the surviving
  batch. This matches the value persisted in metadata.log and ensures
  consistent behavior before and after restart. A surviving batch whose
  base offset is below `newOffset` but whose last offset is >= `newOffset`
  is correctly retained — Fetch skips records below `logStartOffset` within
  such a batch.
- **WAL index pruning:** After trimming, stale WAL index entries pointing
  to deleted segments are removed. Without this, a fetch for an offset near
  the old logStartOffset would find a stale index entry and attempt to
  pread a deleted segment file, causing a file-not-found error.
- **WAL segment cleanup:** After pruning the index, `TryCleanupSegments()`
  checks whether any WAL segments are now fully below `logStartOffset` for
  all partitions. This handles the idle-system case where no segment
  rotation would otherwise trigger cleanup.

### `trimBatchesLocked`

```go
// trimBatchesLocked removes all batches whose last offset is below
// newLogStart, then sets pd.logStart = newLogStart. Updates pd.totalBytes.
// Caller must hold pd.mu.Lock().
//
// Multi-tier trim order: data is trimmed from oldest to newest, which
// corresponds to the natural tier ordering. Each tier is trimmed explicitly
// in sequence — no polymorphic dispatch:
//
//   1. WAL batch index entries (oldest data, on-disk in WAL segments):
//      Remove entries whose lastOffset < newLogStart. Decrements totalBytes
//      by each entry's BatchSize. The actual WAL segment files are deleted
//      later by TryCleanupSegments() (only when ALL partitions' data in
//      that segment is trimmed).
//
//   2. Ring buffer batches (newest data, in memory):
//      Advance ring.head past batches whose lastOffset < newLogStart.
//      Decrements totalBytes by len(batch.RawBytes).
//
//   3. S3 offset list entries (Phase 4, oldest cold data):
//      Remove entries whose lastOffset < newLogStart. Decrements totalBytes
//      by the recorded batch size. Actual S3 object deletion happens in the
//      next S3 flush cycle (see "S3 Object Cleanup" below).
//
// The key invariant: totalBytes always reflects the sum of batch sizes
// across all tiers. Only trimBatchesLocked decrements totalBytes (never
// ring buffer wrap-around eviction — evicted batches still exist in WAL
// or S3).
//
// Batch size consistency: totalBytes is incremented in commitBatch by
// len(batch.RawBytes). The WAL batch index stores BatchSize = len(RawBytes)
// at write time. trimBatchesLocked decrements by the same value from
// whichever tier holds the metadata. These must always agree.
func (pd *partData) trimBatchesLocked(newLogStart int64) {
    // Tier 1: Trim S3 offset list (oldest cold data, Phase 4).
    // S3 entries are metadata-only (base offset, last offset, byte size).
    for pd.s3OffsetList.Len() > 0 {
        entry := pd.s3OffsetList.Front()
        if entry.LastOffset >= newLogStart {
            break
        }
        pd.totalBytes -= int64(entry.BatchSize)
        pd.s3OffsetList.RemoveFront()
    }

    // Tier 2: Trim WAL batch index (older on-disk data).
    // WAL index entries have (BaseOffset, LastOffset, BatchSize).
    for pd.walBatchIndex.Len() > 0 {
        entry := pd.walBatchIndex.Front()
        if entry.LastOffset >= newLogStart {
            break
        }
        pd.totalBytes -= int64(entry.BatchSize)
        pd.walBatchIndex.RemoveFront()
    }

    // Tier 3: Trim ring buffer (newest in-memory data).
    for pd.ring.head < pd.ring.tail {
        slot := int(pd.ring.head % int64(len(pd.ring.batches)))
        b := pd.ring.batches[slot]
        lastOffset := b.BaseOffset + int64(b.LastOffsetDelta)
        if lastOffset >= newLogStart {
            break
        }
        pd.totalBytes -= int64(len(b.RawBytes))
        pd.ring.head++
    }

    // Always set logStart to the requested value, not to the base offset
    // of the oldest surviving batch. A batch may straddle newLogStart
    // (base offset < newLogStart, last offset >= newLogStart) — that batch
    // is retained, and Fetch skips records below logStartOffset within it.
    // This matches the value persisted in metadata.log, ensuring consistent
    // behavior before and after restart.
    pd.logStart = newLogStart
}
```

---

## Partition Size Tracking

Size-based retention (`retention.bytes`) needs to know the total byte size
of data in each partition. With WAL + S3 active, data spans multiple tiers
(ring buffer, WAL segments, S3 objects), so computing size on the fly would
require scanning the WAL index and listing S3 objects — too expensive to do
every 5 minutes for every partition.

Instead, maintain a running `totalBytes` counter on `partData`:

```go
type partData struct {
    // ... existing fields ...
    totalBytes int64  // total raw RecordBatch bytes across all tiers
}
```

**Updated on:**
- **Produce:** `totalBytes += int64(len(batch.RawBytes))` in `commitBatch`
- **Trim:** `totalBytes -= int64(len(batch.RawBytes))` (or the equivalent
  `BatchSize` from the WAL/S3 batch index) in `trimBatchesLocked`
  (see `advanceLogStartOffset` above)

**Batch size consistency:** The WAL batch index stores `BatchSize` for each
entry at write time, set to `len(batch.RawBytes)`. `trimBatchesLocked` uses
this stored value when trimming WAL-only or S3-only batches (where `RawBytes`
is not in memory). The invariant is that `BatchSize` always equals the
original `len(RawBytes)` — no WAL entry header overhead or other framing is
included. This is enforced at the WAL write path: `BatchSize = len(rawBytes)`
is set in the same code that writes the WAL entry.

**Ring buffer eviction:** When the ring buffer wraps around and evicts an
old batch (see `15-wal-format-and-writer.md` "Ring Buffer"), `totalBytes`
is NOT decremented. The evicted batch still exists in the WAL (and eventually
S3) — it is only logically removed from the fast in-memory read tier. Only
`trimBatchesLocked` decrements `totalBytes`, and only when the batch is
being permanently removed across all tiers.

**Recovery:** On startup, `totalBytes` is rebuilt during WAL replay (sum of
`len(batch.RawBytes)` for each batch per partition). For S3-only data (not
in WAL), `totalBytes` is rebuilt from S3 object listings during disaster
recovery: `ListObjectsV2` returns a `Size` field for each object, and these
sizes are summed per partition during the S3 discovery scan (see
`18-s3-storage.md` "Disaster Recovery" for the updated protocol).

**Used by:** `retention.bytes` enforcement and `DescribeLogDirs` (key 35,
see `14-admin-apis.md`). The DescribeLogDirs handler can read `totalBytes`
under `pd.mu.RLock()` instead of computing approximate sizes.

---

## `metadata.log` Fsync Semantics

`AppendLogStartOffset` must perform a **synchronous fsync** before returning.
This is different from OFFSET_COMMIT entries (which are frequent and can be
buffered) — LOG_START_OFFSET has a stricter durability requirement because
`advanceLogStartOffset` deletes data after persisting:

1. Append LOG_START_OFFSET entry to `metadata.log`
2. `fdatasync()` the file
3. Return success
4. Caller proceeds to trim in-memory batches and prune WAL index

If step 2 is skipped (buffered write only), a crash between the write and the
next buffer flush would lose the LOG_START_OFFSET entry. On restart, the
replayed `logStartOffset` would be the old value, but if WAL segments were
deleted (by `TryCleanupSegments`) and S3 objects not yet cleaned up, the
deleted data could reappear from S3. The synchronous fsync closes this window.

**Performance impact:** The fsync adds ~2ms of latency under `pd.mu.Lock()`.
This is acceptable because it happens at most once per partition per retention
cycle (default: every 5 minutes) or per DeleteRecords call (rare admin
operation). It is NOT on the produce/fetch hot path.

**Contrast with OFFSET_COMMIT:** Committed offsets are written frequently
(20+/sec) and losing a few on crash is tolerable — the consumer re-commits.
LOG_START_OFFSET is written rarely and losing it can cause deleted data to
reappear. Different durability requirements justify different fsync policies.

See `16-metadata-log.md` for the framing format. The metadata log writer
should expose two methods: `Append(entry)` (buffered, for OFFSET_COMMIT) and
`AppendSync(entry)` (fsync'd, for LOG_START_OFFSET and CREATE_TOPIC).

---

## `TryCleanupSegments` Synchronization

`TryCleanupSegments()` is called from multiple goroutines (WAL writer during
rotation, `advanceLogStartOffset` from handler/retention goroutines). It must
read each partition's `logStartOffset` to determine whether a WAL segment's
data is fully below all partitions' thresholds. These reads require
synchronization.

**Strategy:** `TryCleanupSegments` acquires `pd.mu.RLock()` per partition
to read `pd.logStart`. It does NOT hold all partition locks simultaneously —
it reads them one at a time and collects the values. This means a partition's
`logStartOffset` could advance between the read and the deletion decision,
but that can only make the check more conservative (a segment that becomes
deletable after the read is caught in the next call). The function never
deletes a segment prematurely because it only deletes segments whose data is
below all `logStartOffset` values at the time they were read.

**Implementation sketch:**

```go
func (w *WALWriter) TryCleanupSegments() {
    // Collect per-partition logStartOffset under individual read locks.
    // No pd.mu is held across multiple partitions — no lock-ordering risk.
    minLogStart := make(map[TopicPartition]int64)
    for _, pd := range w.partitions {
        pd.mu.RLock()
        minLogStart[TopicPartition{pd.topic, pd.index}] = pd.logStart
        pd.mu.RUnlock()
    }

    // For each segment, check if ALL partition entries in that segment
    // are below the corresponding logStartOffset. If so, delete it.
    // ... (in-memory segment list scan, then unlink) ...
}
```

This is safe to call from any goroutine. The per-partition `RLock()` does
not conflict with concurrent produces (which hold `pd.mu.Lock()` briefly
for offset reservation and commit) because `RLock` is compatible with
`RLock` and only blocks against `Lock`.

---

## Lock Ordering

When multiple locks are involved, a consistent acquisition order prevents
deadlocks. The required lock ordering for retention and compaction:

```
compactionMu → pd.mu
```

- **`compactionMu` is always acquired before `pd.mu`.** Both retention and
  DeleteRecords unconditionally acquire `compactionMu` before calling
  `advanceLogStartOffset` (which acquires `pd.mu` internally). For
  non-compact topics, `compactionMu` is uncontended (~15ns overhead).
- **The compaction goroutine** (`22-log-compaction.md`) acquires `compactionMu`
  per-window, and may read partition state under `pd.mu.RLock()` (e.g.,
  `logStartOffset`, `s3FlushWatermark`). This is safe because it follows the
  same ordering: `compactionMu` first, then `pd.mu`.
- **No code path may acquire `pd.mu` first and then `compactionMu`.** Doing
  so would create a deadlock with the retention/compaction paths.

This ordering must be documented on the `partData` struct:

```go
type partData struct {
    // Lock ordering: compactionMu → mu
    // Always acquire compactionMu before mu. Never hold mu when acquiring
    // compactionMu. Callers of advanceLogStartOffset must hold compactionMu.
    compactionMu sync.Mutex  // held by compaction, retention, or DeleteRecords
    mu           sync.RWMutex  // protects all partition state fields below
    // ...
}
```

The cluster-level `c.mu` (topic map) is independent — it is always acquired
and released before any partition-level lock. No ordering constraint with
`compactionMu` or `pd.mu` because the cluster lock is never held while
calling `advanceLogStartOffset` or compaction methods.

---

## Background Goroutine

### Design

A single `retentionLoop` goroutine runs for the lifetime of the broker. It
wakes on a configurable interval (default: 5 minutes) and scans all partitions.

```go
func (b *Broker) retentionLoop(ctx context.Context) {
    ticker := time.NewTicker(b.cfg.RetentionCheckInterval) // default 5m
    defer ticker.Stop()
    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            b.enforceRetention()
        }
    }
}
```

### Per-Partition Enforcement

```
For each topic:
    retentionMs    = topic config "retention.ms"   (default from broker config)
    retentionBytes = topic config "retention.bytes" (default from broker config)

    For each partition in topic:
        // ── Phase 1: Compute newLogStart under read lock (single pass) ──
        //
        // Hold pd.mu.RLock() while reading logStartOffset, totalBytes, and
        // scanning batches. This ensures a consistent snapshot: totalBytes
        // corresponds to the batch list we're scanning. Without the lock,
        // a concurrent produce could update totalBytes between our read of
        // the batch list and totalBytes, causing over- or under-trimming.

        pd.mu.RLock()
        origLogStart = pd.logStart
        newLogStart  = origLogStart
        bytesToDrop  = 0  // accumulated bytes of batches we plan to trim

        // Single forward scan from oldest batch to newest.
        // Both time and size checks are evaluated per batch in one pass.
        cutoff = now - retentionMs   // only meaningful if retentionMs >= 0

        for each batch in pd (oldest to newest):
            lastOffset = batch.BaseOffset + batch.LastOffsetDelta
            shouldTrim = false

            // Time-based: batch is older than retention cutoff
            if retentionMs >= 0 && batch.MaxTimestamp < cutoff:
                shouldTrim = true

            // Size-based: even after trimming batches so far, still over limit
            if retentionBytes >= 0 && (pd.totalBytes - bytesToDrop) > retentionBytes:
                shouldTrim = true

            if shouldTrim:
                newLogStart = lastOffset + 1  // advance past this batch
                bytesToDrop += len(batch.RawBytes)
            else:
                break  // batches are ordered; once we stop trimming, we're done

        pd.mu.RUnlock()
        // ── End of read-locked scan ──

        // Compare against origLogStart (captured under the read lock above),
        // NOT pd.logStart (which would be a data race — reading a field
        // protected by pd.mu without holding the lock).
        if newLogStart > origLogStart:
            // Always acquire compactionMu, even for non-compact topics.
            // For non-compact topics, compactionMu is uncontended (~15ns).
            // This eliminates a conditional code path and makes the locking
            // protocol uniform. See "Lock Ordering" section.
            pd.compactionMu.Lock()
            pd.advanceLogStartOffset(newLogStart, metaLog)
            pd.compactionMu.Unlock()
```

**Why a single pass:** Both time-based and size-based policies scan batches
from oldest to newest. Combining them into one loop avoids a second scan and
makes it clear that the entire computation happens under a single consistent
read-lock snapshot. The `bytesToDrop` accumulator estimates the size reduction
without modifying `totalBytes` — the actual decrement happens inside
`trimBatchesLocked` (called by `advanceLogStartOffset` under the write lock).

**Why unconditional `compactionMu`:** For topics with `cleanup.policy`
containing `compact`, `compactionMu` prevents retention from racing with the
compaction goroutine. For `delete`-only topics, `compactionMu` is never
contended (no compaction goroutine touches them), so acquiring it costs ~15ns
— negligible. Unconditional acquisition eliminates a conditional code path
and removes a class of bugs where the wrong branch is taken.

### WAL Segment Cleanup

After `advanceLogStartOffset` prunes the WAL index, it calls
`walWriter.TryCleanupSegments()` to delete WAL segments whose offset ranges
are entirely below `logStartOffset` for ALL partitions in that segment.

Two cleanup triggers exist:
1. **Segment rotation** (during produce): the WAL writer checks for deletable
   segments when rotating to a new file (see `15-wal-format-and-writer.md`).
2. **After retention/DeleteRecords**: `advanceLogStartOffset` calls
   `TryCleanupSegments()` explicitly. This handles the idle-system case
   where no new data is produced, segment rotation never fires, and stale
   segments would otherwise linger on disk indefinitely.

`TryCleanupSegments()` is cheap (scans the in-memory segment list, compares
against per-partition `logStartOffset` values — no disk I/O) and idempotent.

Note: A WAL segment is only deletable when ALL partitions' entries in that
segment are below their respective `logStartOffset` values. Since the WAL
is interleaved across partitions, a single partition's retention cannot
delete a segment that still contains live data for another partition.

### S3 Object Cleanup

For partitions with S3 offload, S3 objects whose offset ranges are entirely
below the new log start offset are deleted during the next S3 flush cycle.
The S3 flusher checks `logStartOffset` before retaining objects (see
`18-s3-storage.md`).

**Note on cleanup delay:** S3 objects are not deleted immediately when
retention advances `logStartOffset`. They persist until the next S3 flush
cycle (default: 10 minutes). During this window, the data still exists in
S3, but the Fetch handler checks `logStartOffset` and returns
`OFFSET_OUT_OF_RANGE` for offsets below it — so stale S3 data is never
served to clients. The only cost is temporary excess S3 storage.

---

## Interaction with Log Compaction (`compact,delete`)

When a topic has `cleanup.policy=compact,delete`, both the compaction
goroutine and the retention goroutine may operate on the same partition.
A per-partition `compactionMu sync.Mutex` (defined in `22-log-compaction.md`)
prevents concurrent access:

- **Retention** always acquires `pd.compactionMu` before calling
  `advanceLogStartOffset` (unconditionally, for all topics — see
  "Per-Partition Enforcement" above). For `compact,delete` topics, this
  prevents retention from deleting S3 objects that compaction is currently
  reading. For `delete`-only topics, the lock is uncontended.
- **Compaction** acquires `pd.compactionMu` for the duration of its
  window processing (GET, filter, PUT, DELETE cycle).

The two goroutines don't need explicit ordering — `compactionMu` prevents
overlap, and each is idempotent. See `22-log-compaction.md` "Interaction
with Retention" for the full coordination protocol.

---

## Configuration

### New Config Options

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--retention-check-interval` | `KLITE_RETENTION_CHECK_INTERVAL` | `5m` | How often the retention goroutine runs |

### Existing Topic Configs (Already Defined)

These are already in the supported topic configs table (`00-overview.md`):
- `retention.ms` — default 604800000 (7 days)
- `retention.bytes` — default -1 (infinite)

### Broker-Level Defaults

When a topic doesn't override retention, the broker-level defaults apply:
- `log.retention.ms` — same as `retention.ms` default
- `log.retention.bytes` — same as `retention.bytes` default

These are already accepted as broker configs in DescribeConfigs/AlterConfigs.

---

## Edge Cases

### Empty Partitions

Skip — no batches to evaluate.

### All Batches Expired

Advance `logStartOffset` to HW. The partition becomes empty but retains its
HW and next offset. New produces continue from where they left off.

### Active Consumers Below Cutoff

If a consumer is reading at an offset that gets deleted by retention, its
next Fetch will receive `OFFSET_OUT_OF_RANGE`. The consumer must reset to
`EARLIEST` (which now returns the new `logStartOffset`). This is standard
Kafka behavior.

### Concurrent Produce During Enforcement

Retention calls `advanceLogStartOffset`, which trims all eligible batches
in a single pass under `pd.mu.Lock()`. Even trimming 100K batches at ~100ns
each takes only ~10ms — comparable to a Go GC pause. Since this happens at
most once per partition per retention cycle (every 5 minutes), the produce
stall is negligible. A concurrent produce blocks for the duration of the
trim but proceeds normally afterward.

### Future Timestamps with CreateTime

When `message.timestamp.type=CreateTime` (the default), clients set the
batch timestamp. A misbehaving client can set timestamps far in the future,
making those batches effectively immune to time-based retention until the
wall clock catches up. This matches Kafka's behavior — Kafka has the same
issue. With `LogAppendTime`, the broker sets `MaxTimestamp` at produce time
using the server wall clock, which avoids this edge case. No special
handling is needed — matching Kafka semantics is the correct choice.

---

## When Stuck

1. Search `.cache/repos/kafka/` for `LogCleaner` and `LogCleanerManager` —
   Kafka's retention enforcement runs in the log cleaner thread pool.
2. Search `.cache/repos/kafka/` for `deleteOldSegments` — the core retention
   loop that evaluates time and size policies per partition.
3. See `00b-testing.md` "When Stuck: Escalation Path" for additional tools.

---

## Acceptance Criteria

- Unit tests:
  - `TestRetentionByTime` — produce with old timestamps, run enforcement,
    verify batches deleted
  - `TestRetentionBySize` — produce until over limit, run enforcement,
    verify size reduced
  - `TestRetentionInfinite` — retention.ms=-1, nothing deleted
  - `TestRetentionBothPolicies` — both time and size set, more aggressive wins
  - `TestRetentionBytesPerPartition` — produce to 3 partitions, verify each
    partition independently enforces the size limit (per-partition, not
    per-topic)
  - `TestAdvanceLogStartOffset` — verify shared method persists to
    metadata.log, trims batches, prunes WAL index, sets logStart to
    exactly newOffset (not to oldest surviving batch's base offset)
  - `TestAdvanceLogStartOffsetStraddlingBatch` — advance logStartOffset
    to the middle of a batch (batch base offset < newOffset, last offset
    >= newOffset). Verify: batch is retained, logStart set to newOffset
    (not batch base), Fetch below logStart returns OFFSET_OUT_OF_RANGE
  - `TestAdvanceLogStartOffsetConcurrent` — call advanceLogStartOffset
    concurrently from two goroutines (simulating retention + DeleteRecords
    on the same partition). Verify: logStartOffset ends at the higher
    value, exactly one LOG_START_OFFSET entry per distinct advancement is
    written to metadata.log, no data corruption, no deadlock.

- Integration tests:
  - `TestRetentionEnforcement` — create topic with short retention, produce,
    wait, verify records deleted and logStartOffset advanced
  - `TestRetentionConsumerReset` — consumer reading old data gets
    OFFSET_OUT_OF_RANGE after retention, resets to earliest
  - `TestRetentionSurvivesRestart` — advance logStartOffset via retention,
    restart broker, verify logStartOffset is restored from metadata.log
    and deleted data does not reappear

### Verify

```bash
# This work unit:
go test ./internal/cluster/ -run 'TestRetention|TestAdvanceLogStartOffset' -v -race -count=5
go test ./test/integration/ -run 'TestRetention' -v -race -count=5

# Regression check:
go test ./... -race -count=5
```

### Commit

After tests pass, stage and commit all changes:

```bash
git add -A
git commit -m "21: retention enforcement — time-based, size-based, advanceLogStartOffset, WAL cleanup"
```
