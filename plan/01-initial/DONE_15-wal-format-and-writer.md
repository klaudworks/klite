# WAL, Ring Buffer, and partData Evolution

Write-ahead log for durable local storage plus per-partition ring buffer for
zero-disk-I/O tail reads. Data survives broker restarts. This is Phase 3.
Prerequisite: Phase 2 complete.

Read before starting: `06-partition-data-model.md` (partData struct, pushBatch,
storedBatch — you'll be replacing the unbounded slice with ring buffer + WAL),
`01-foundations.md` (concurrency model — per-partition RWMutex, the lock
pattern that commitBatch relies on), `research/automq.md` (WAL design context).

This file covers the WAL entry format, segment files, writer goroutine, fsync
batching, ring buffer design, memory budget, the three-tier read cascade, and
how `partData` evolves across phases. For the metadata log, see
`16-metadata-log.md`. For crash recovery, see `17-crash-recovery.md`.

---

## Design Overview

The WAL is a single append-only log that interleaves all partitions. This is
simpler than Kafka's per-partition directories. AutoMQ's WAL is conceptually
similar (interleaved multi-stream batches) but writes to S3 instead of local
disk (see `research/automq.md`). Our local-disk approach gives us ~2ms P50
latency vs AutoMQ's ~250ms.

Each partition also has a fixed-size ring buffer that holds the most recent
batches in memory. Tail consumers (the common case) are served entirely from
the ring buffer with no disk I/O.

```
Write path:
  Produce request -> assign offsets -> append to WAL -> fsync -> ring buffer push -> ACK

Read path (recent data):
  Fetch request -> check ring buffer -> hit -> serve from memory

Read path (slightly old data):
  Fetch request -> ring buffer miss -> check WAL index -> pread from WAL segment

Read path (old data, Phase 4):
  Fetch request -> not in ring buffer or WAL -> read from S3
```

### Why a Single Interleaved WAL

- **Fewer fsyncs**: One fsync covers writes to all partitions in the batch.
  Kafka fsyncs per-partition (or relies on replication instead of fsync).
- **Simpler**: One directory, sequential segment files, one writer goroutine.
- **Better for S3 flush**: WAL segments naturally produce multi-partition S3
  objects (which is what we want for amortizing S3 PUT costs).
- **Trade-off**: Reading a single partition requires scanning past other
  partitions' data. Mitigated by the in-memory index.

---

## WAL Format

### Segment Files

```
<data-dir>/wal/
  00000000000000000000.wal    # First segment (named by first entry's sequence number)
  00000000000000001024.wal    # Second segment
  ...
```

Segment rotation when: size >= `wal.segment.bytes` (default 64 MiB).
Segment names are 20-digit zero-padded sequence numbers (matching Kafka's
convention for log segments, but these are WAL sequence numbers, not offsets).

### Entry Format

Each WAL entry is one RecordBatch for one partition:

```
[4 bytes]  entry_length (uint32, big-endian, excludes these 4 bytes)
[4 bytes]  crc32c (over remaining bytes)
[8 bytes]  wal_sequence (uint64, monotonic across all entries)
[16 bytes] topic_id (UUID, same [16]byte generated at topic creation)
[4 bytes]  partition_index (int32)
[8 bytes]  base_offset (int64, the assigned Kafka offset)
[M bytes]  record_batch (raw RecordBatch bytes, already offset-assigned)
```

Total overhead per entry: 4 + 4 + 8 + 16 + 4 + 8 = 44 bytes (fixed).

Using the topic UUID instead of the topic name keeps every field fixed-size,
which simplifies both the writer (no length-prefix encoding) and the recovery
scanner (entries can be validated with a single `io.ReadFull` of the fixed
header). The UUID-to-topic-name mapping is already maintained in memory and
rebuilt from `metadata.log` on startup (see `17-crash-recovery.md`).

### Why CRC per Entry

Crash recovery: after an unclean shutdown, the last segment may have a partially
written entry. On startup, scan forward entry by entry. If CRC fails or entry
is truncated, stop recovery at that point. All entries before the bad one are
valid.

### WAL Sequence Numbers

Global monotonic counter across all entries. Purpose:
- Segment file naming
- Efficient WAL trimming (trim all entries with sequence < X)
- Ordering guarantee during S3 flush

Not exposed to Kafka clients. Clients see Kafka offsets (per-partition).

---

## In-Memory WAL Index

The WAL alone isn't enough for efficient reads. We maintain an in-memory index
that maps `(topic, partition, offset)` to WAL position:

```go
type WALIndex struct {
    // For each partition, sorted list of (base_offset -> WAL position)
    partitions map[TopicPartition][]WALEntry
}

type WALEntry struct {
    BaseOffset   int64
    LastOffset   int64   // BaseOffset + LastOffsetDelta
    SegmentFile  string  // Which segment file
    FileOffset   int64   // Byte position within segment
    BatchSize    int32   // Size of the RecordBatch (for direct read)
}
```

This index is rebuilt from WAL on startup (by scanning all segments). Kept
in memory during operation. Updated on every WAL append.

### Index Pruning

When `logStartOffset` advances (via DeleteRecords or retention enforcement),
WAL index entries for the affected partition whose `LastOffset` is below the
new `logStartOffset` must be removed. Without pruning, a fetch that falls
through the ring buffer to the WAL tier would find a stale index entry and
attempt to `pread` a deleted segment file.

```go
// PruneBefore removes all index entries for (topic, partition) whose
// LastOffset < newLogStart.
func (idx *WALIndex) PruneBefore(topic string, partition int32, newLogStart int64) {
    key := TopicPartition{topic, partition}
    entries := idx.partitions[key]
    // entries are sorted by BaseOffset; find the first entry with
    // LastOffset >= newLogStart and slice
    cutoff := sort.Search(len(entries), func(i int) bool {
        return entries[i].LastOffset >= newLogStart
    })
    idx.partitions[key] = entries[cutoff:]
}
```

This is called by `advanceLogStartOffset` (see `21-retention.md`).

### Index Size Estimate

Each entry is ~50 bytes. At 100 bytes/record average and 64MiB WAL segments:
- ~670K records per segment
- ~670K index entries per segment (worst case: one record per batch)
- Realistic: ~67K entries (10 records per batch average)
- ~3.3 MiB of index per segment
- With 10 segments (640 MiB WAL): ~33 MiB index

Acceptable for a single-broker deployment.

---

## Ring Buffer

In Phase 3, the unbounded `[]storedBatch` slice from Phase 1 is replaced by a
fixed-size ring buffer per partition. The ring buffer is the first tier in the
read cascade -- tail consumers are served entirely from memory.

### Data Structure

The ring buffer is indexed by **push sequence number** (monotonic count of
pushes), NOT by Kafka offset. Each batch covers a variable range of Kafka
offsets (BaseOffset to BaseOffset + LastOffsetDelta), so Kafka offsets cannot
be used as direct array indices.

```go
type ringBuffer struct {
    batches   []storedBatch  // fixed-size, allocated once
    head      int64          // push sequence of oldest valid entry
    tail      int64          // next push sequence (= total pushes so far)
    usedBytes int64          // total bytes of batches currently in the ring
}

func (r *ringBuffer) push(b storedBatch) {
    slot := int(r.tail % int64(len(r.batches)))
    // Subtract evicted batch's size, add new batch's size
    r.usedBytes -= int64(len(r.batches[slot].RawBytes))
    r.batches[slot] = b
    r.usedBytes += int64(len(b.RawBytes))
    r.tail++
    if r.tail - r.head > int64(len(r.batches)) {
        r.head = r.tail - int64(len(r.batches))
    }
}

// findByOffset finds the batch containing the given Kafka offset via linear
// scan of the ring. Returns (batch, true) if found. The ring is small
// (16-4096 slots) so linear scan is fast (~1us for 4096 slots).
func (r *ringBuffer) findByOffset(offset int64) (storedBatch, bool) {
    for seq := r.head; seq < r.tail; seq++ {
        slot := int(seq % int64(len(r.batches)))
        b := r.batches[slot]
        lastOffset := b.BaseOffset + int64(b.LastOffsetDelta)
        if offset >= b.BaseOffset && offset <= lastOffset {
            return b, true
        }
        if b.BaseOffset > offset {
            break  // ring is ordered by offset, no need to scan further
        }
    }
    return storedBatch{}, false
}

// collectFrom returns batches starting at offset up to maxBytes.
// KIP-74: always includes at least one complete batch even if > maxBytes.
func (r *ringBuffer) collectFrom(offset int64, maxBytes int32) []storedBatch {
    var result []storedBatch
    var totalBytes int32
    for seq := r.head; seq < r.tail; seq++ {
        slot := int(seq % int64(len(r.batches)))
        b := r.batches[slot]
        lastOffset := b.BaseOffset + int64(b.LastOffsetDelta)
        if lastOffset < offset {
            continue  // batch ends before requested offset
        }
        batchSize := int32(len(b.RawBytes))
        if len(result) > 0 && totalBytes+batchSize > maxBytes {
            break  // respect maxBytes (but first batch is always included)
        }
        result = append(result, b)
        totalBytes += batchSize
    }
    return result
}
```

**Properties:**
- O(1) push — overwrites oldest entry when full, no allocation
- O(N) lookup by Kafka offset — linear scan over ring slots (N = ring size,
  capped at 4096; ~1us worst case). Binary search is possible since batches
  are ordered, but the ring is small enough that linear scan is simpler and
  fast.
- No GC pressure — backing array allocated once, entries overwritten in-place
- Automatic eviction — oldest batches fall off as new ones wrap around
- Kafka offsets are properly handled — each batch covers a variable range

### Global Memory Budget

All ring buffers share a single global memory budget configured by
`--ring-buffer-max-memory` (default 512 MiB). The per-partition slot count
is derived from the global budget:

```
slotsPerPartition = maxMemory / (numPartitions * estimatedAvgBatchSize)
clamped to [16, 4096]
```

`estimatedAvgBatchSize` defaults to 16 KiB (reasonable for 1KB messages with
~10 records per batch).

**Sizing examples:**

| Partitions | Budget | Slots/Partition | Memory/Partition | Coverage |
|-----------|--------|-----------------|------------------|----------|
| 100 | 512 MiB | 320 | ~5 MiB | ~seconds of tail data |
| 500 | 512 MiB | 64 | ~1 MiB | sub-second |
| 1000 | 512 MiB | 32 | ~512 KiB | sub-second |
| 100 | 2 GiB | 1310 | ~20 MiB | ~10s of tail data |

Ring buffer slot counts are calculated when partitions are created. When new
partitions are added (CreateTopics), the new partitions get the current
per-partition allowance based on total partition count. Existing ring buffers
keep their size -- shrinking a ring is destructive.

---

## Write Path

### Goroutine Interaction: Two Actors

A durable Produce involves two kinds of goroutines:

1. **Handler goroutine** (one per connection) -- reads the Produce request off the
   wire, orchestrates the flow, sends back the response. Hundreds of these run
   concurrently. Each handler locks individual partitions via `pd.mu` as needed.
2. **WAL writer goroutine** (exactly one) -- owns the WAL file handle. Receives
   serialized entries, writes to disk, fsyncs every ~2ms, signals completion.

There is no cluster goroutine. Partition state is protected by per-partition
`sync.RWMutex` (see `06-partition-data-model.md`). The handler goroutine holds the
partition write lock briefly twice (reserve offset, then commit batch) and
releases it during the fsync wait. This means Fetches on the same partition
can proceed while the Produce is waiting for fsync.

### Produce Flow (Handler-Driven, With Locks)

```
Handler goroutine                              WAL writer goroutine
     |                                                |
     |  pd.mu.Lock()                                  |
     |    baseOffset = pd.reserveOffset(meta)         |
     |  pd.mu.Unlock()                                |
     |                                                |
     |  serialize WAL entry                           |
     |---[WAL entry]--------------------------------->|
     |                                       write() to segment
     |  (pd.mu is NOT held -- Fetches can proceed)    |
     |                                           fsync()
     |                                                |
     |<--[done]---------------------------------------|
     |                                                |
     |  pd.mu.Lock()                                  |
     |    pd.commitBatch(batch)  // ring buffer push  |
     |  pd.mu.Unlock()                                |
     |  pd.notifyWaiters()                            |
     |                                                |
     |---[response]---> client                        |
```

### Key Property: Lock Is Not Held During Fsync

The partition write lock is held for ~1us (reserve) and ~1us (commit). Between
those two moments -- while the WAL writer is doing the ~2ms fsync -- the lock is
released. This means:

- Fetches on the same partition proceed normally (take read lock).
- Other Produces to the same partition can reserve their offset (take write lock
  for ~1us, release, then also wait for fsync).
- Different partitions are entirely independent (different locks).

### Sequential HW Advancement (The Ordering Problem)

When multiple Produces are in flight simultaneously, their offsets are reserved
in order but their fsyncs may complete out of order:

```
Time 0ms: Produce A reserves offset 10 (pd.mu.Lock, reserve, Unlock)
Time 0ms: Produce B reserves offset 11 (pd.mu.Lock, reserve, Unlock)
Time 2ms: Both fsyncs complete in same batch
Time 2ms: Produce B's goroutine runs first, tries to commit offset 11
```

If B commits and advances HW to 12, a Fetch sees HW=12 but offset 10 is missing
-- a gap. This violates Kafka's ordering guarantee.

**Solution: Sequential commit with pending queue.** The `commitBatch` method
(called under `pd.mu.Lock()`) tracks the next expected commit offset. Commits
that arrive out of order are queued until all earlier offsets have committed:

```go
type partData struct {
    mu             sync.RWMutex
    // ... existing fields ...
    hw             int64          // published high watermark (visible to Fetch)
    nextReserve    int64          // next offset to hand out (advances on reserve)
    nextCommit     int64          // next offset expected in commitBatch
    pendingCommits []pendingBatch // out-of-order commits waiting for earlier ones
    ring           *ringBuffer    // in-memory ring buffer for recent batches
}

// Called under pd.mu.Lock(). Reserves an offset range, does NOT store data.
func (pd *partData) reserveOffset(lastOffsetDelta int32) int64 {
    base := pd.nextReserve
    pd.nextReserve = base + int64(lastOffsetDelta) + 1
    return base
}

// Called under pd.mu.Lock() after fsync completes. Stores batch in ring buffer,
// advances HW in strict offset order.
func (pd *partData) commitBatch(batch storedBatch) {
    if batch.BaseOffset == pd.nextCommit {
        // In order: apply immediately
        pd.ring.push(batch)
        pd.nextCommit = batch.BaseOffset + int64(batch.LastOffsetDelta) + 1
        pd.hw = pd.nextCommit
        // Drain any queued commits that are now in order
        for len(pd.pendingCommits) > 0 && pd.pendingCommits[0].BaseOffset == pd.nextCommit {
            next := pd.pendingCommits[0]
            pd.pendingCommits = pd.pendingCommits[1:]
            pd.ring.push(next)
            pd.nextCommit = next.BaseOffset + int64(next.LastOffsetDelta) + 1
            pd.hw = pd.nextCommit
        }
    } else {
        // Out of order: queue until earlier offsets commit
        pd.pendingCommits = insertSorted(pd.pendingCommits, batch)
    }
}
```

### WAL Writer Goroutine

Single goroutine that serializes all WAL writes. Receives entries from handler
goroutines, batches them, fsyncs once per batch.

```go
type walEntry struct {
    topicID   [16]byte
    partition int32
    offset    int64
    raw       []byte      // serialized WAL entry (header + RecordBatch)
    doneCh    chan struct{} // closed after fsync completes
}

func (w *WALWriter) run() {
    ticker := time.NewTicker(w.syncInterval) // default 2ms
    defer ticker.Stop()
    var pending []walEntry

    for {
        // Phase 1: Wait for at least one event (entry, tick, or stop)
        select {
        case e := <-w.writeCh:
            pending = append(pending, e)
        case <-ticker.C:
            // fall through to drain
        case <-w.stopCh:
            w.flushAndSync(pending)
            return
        }

        // Phase 1b: Non-blocking drain of all queued entries.
        // Always runs after ANY wakeup (entry or tick). This ensures
        // entries that arrived concurrently with a ticker fire are not
        // missed and delayed by an extra syncInterval.
    DrainLoop:
        for {
            select {
            case e := <-w.writeCh:
                pending = append(pending, e)
            default:
                break DrainLoop
            }
        }

        if len(pending) == 0 {
            continue  // ticker fired with no work
        }

        // Phase 2: Write all pending entries (sequential append)
        for _, e := range pending {
            w.appendEntry(e)
        }

        // Phase 3: Single fsync for entire batch
        w.currentSegment.Sync()

        // Phase 4: Signal all waiters (handler goroutines unblock)
        for _, e := range pending {
            close(e.doneCh)
        }
        pending = pending[:0]  // reuse slice
    }
}
```

Key details:
- `pending` slice is reused (reset to `[:0]`), avoiding allocation.
- `ticker` ensures fsync happens at least every `syncInterval` even under
  low load (prevents unbounded latency for a lone request).
- Under high load, the drain loop naturally batches hundreds of entries per
  fsync -- throughput scales with load.
- The WAL writer does NOT touch cluster state. It writes bytes and signals done.
  The handler goroutine then commits to partition state.

### Segment Rotation: Directory Fsync

When rotating to a new segment file, the WAL writer must fsync the parent
directory after creating the file. On Linux/macOS, creating a new file doesn't
guarantee the directory entry is durable until the parent directory is fsync'd.

```go
func (w *WALWriter) rotateSegment() error {
    // 1. Fsync and close current segment
    w.currentSegment.Sync()
    w.currentSegment.Close()
    // 2. Create new segment file
    f, err := os.Create(filepath.Join(w.walDir, newSegmentName))
    // 3. Fsync the directory to make the new entry durable
    dir, _ := os.Open(w.walDir)
    dir.Sync()
    dir.Close()
    w.currentSegment = f
    return err
}
```

---

## Read Path: Three-Tier Cascade

The fetch path checks tiers in order. The WAL serves dual duty: durability
(write-ahead log with fsync) and read tier (via in-memory index + pread on
segment files).

```
fetchFrom(offset, maxBytes):
    // Tier 1: ring buffer (memory, ~1us linear scan over small ring)
    if batches := pd.ring.collectFrom(offset, maxBytes); len(batches) > 0 {
        return batches
    }
    // Tier 2: WAL via index + pread (~1us page cache, ~1ms disk)
    if batches := pd.walIndex.Fetch(pd.topic, pd.index, offset, maxBytes); len(batches) > 0 {
        return batches
    }
    // Tier 3 (Phase 4): S3 range read (~50ms)
    return pd.s3Reader.Fetch(pd.topic, pd.index, offset, maxBytes)
```

The cascade handles consumers at any lag:
- **Tail consumer** (keeping up): ring buffer hit. Zero disk I/O.
- **Slightly behind** (seconds to hours): WAL hit via index + pread.
- **Far behind** (days): WAL miss, S3 GET. ~50ms latency per fetch.

### WAL Read via Index

Although the WAL interleaves all partitions in write order, the in-memory
WAL index maps `(topic, partition, offset)` to `(segment_file, byte_offset,
batch_size)`. Reading a specific batch is a single
`pread(fd, buf, offset, size)` -- no scanning required.

```go
// WAL read: single pread via the in-memory index. No scanning.
func (w *WAL) ReadBatch(entry WALEntry) ([]byte, error) {
    buf := make([]byte, entry.BatchSize)
    _, err := w.segmentFile(entry.SegmentFile).ReadAt(buf, entry.FileOffset)
    return buf, err
}
```

---

## partData Evolution

### Phase 1 (in-memory)

Handler takes `pd.mu.Lock()`, calls `pd.pushBatch()`, releases lock, calls
`pd.notifyWaiters()`. Synchronous, single step. `fetchFrom` does binary
search over `pd.batches` under `pd.mu.RLock()`.

### Phase 3 (WAL + Ring Buffer) -- this work unit

The `[]storedBatch` slice is replaced by a per-partition ring buffer. The
produce flow splits into reserve + WAL write + commit (see Write Path above).

`partData` gains:
- `nextReserve int64` -- next offset to hand out (advances on reserve)
- `nextCommit int64` -- next offset expected to commit (for sequential ordering)
- `pendingCommits []pendingBatch` -- out-of-order commits waiting for predecessors
- `ring *ringBuffer` -- in-memory ring buffer for recent batches
- `walIndex *wal.Index` -- reference to WAL index for read-path lookups
- `totalBytes int64` -- running total of raw RecordBatch bytes across all tiers
  (incremented in `commitBatch`, decremented in `trimBatchesLocked`; rebuilt
  during WAL replay on startup; used by `retention.bytes` enforcement and
  `DescribeLogDirs`)
- `reserveOffset(meta) int64` -- reserves offset range without storing data
- `commitBatch(batch)` -- stores batch in ring buffer (sync), advances HW in order,
  increments `totalBytes`
- `advanceLogStartOffset(newOffset, metaLog)` -- shared method used by both
  DeleteRecords and retention enforcement; acquires pd.mu internally (caller
  must NOT hold it); persists to metadata.log with synchronous fsync, trims
  all eligible batches in a single pass, prunes WAL index, decrements
  `totalBytes`, triggers `walWriter.TryCleanupSegments()`. Sets
  `pd.logStart = newOffset` (not to the oldest surviving batch's base offset).
  Concurrent-safe: only one caller advances per invocation (see
  `21-retention.md`).
  Lock ordering: caller must hold compactionMu (unconditionally, for all
  topics — see `21-retention.md` "Lock Ordering").

### Phase 4 (S3)

`partData` gains:
- `s3FlushWatermark int64` -- highest offset flushed to S3
- `s3Reader *s3.Reader` -- injected concrete type (not an interface)

**Why no interface:** The write path changes fundamentally (sync -> async fsync
-> not-a-write-target). The read path is a cascade, not polymorphic dispatch.
An interface would either be wrong or so broad it provides no guidance.

---

## Throughput Math

The WAL writer is the **sole bottleneck for durable produce throughput.**

| Mode | Throughput | Latency | How |
|------|-----------|---------|-----|
| fsync per request | ~50K msgs/sec | ~20us | 1M us / 20us per fsync |
| Batched fsync, 1ms window | ~100-300K msgs/sec | ~1ms | Batch, write, 1 fsync |
| Batched fsync, 2ms window | ~200-500K msgs/sec | ~2ms | Batch, write, 1 fsync |
| No fsync (acks=1) | ~500K-1M msgs/sec | <0.5ms | CPU-bound |

**Default batch window: 2ms.** Configurable via `--wal-sync-interval`.

---

## WAL Segment Deletion

The WAL is a durability buffer, not a read cache. Segments are deleted as soon
as they've been flushed to S3 (Phase 4).

### Deletion Policy

1. **After all partitions in a segment are flushed to S3**: delete immediately.
2. **Disk budget exceeded**: if unflushed segments exceed `--wal-max-disk-size`,
   log a warning. Don't delete unflushed data.
3. **Without S3 (Phase 3 only)**: delete oldest segments when total size exceeds
   `--wal-max-disk-size`. Data in deleted segments is gone -- acceptable because
   the ring buffer still has the most recent data for reads.

### Deletion Triggers

WAL segment deletion is checked in two places:

1. **During segment rotation** (in the WAL writer goroutine): after creating a
   new segment, check whether any old segments are fully below `logStartOffset`
   for all partitions. This is the common trigger during active writes.
2. **After `advanceLogStartOffset`** (retention / DeleteRecords): the method
   calls `walWriter.TryCleanupSegments()` after pruning the WAL index. This
   handles the idle-system case where no new data is produced, segment rotation
   never fires, and stale segments would linger on disk indefinitely.

### `TryCleanupSegments`

```go
// TryCleanupSegments scans the in-memory segment list and deletes any
// segments whose offset ranges are fully below logStartOffset for ALL
// partitions that have entries in that segment. This is cheap (no disk I/O
// beyond the unlink) and idempotent — safe to call from any goroutine.
//
// Synchronization: reads each partition's pd.logStart under pd.mu.RLock()
// individually (not all at once). This means a partition's logStartOffset
// could advance between reads, but that can only make the check more
// conservative (never deletes a segment prematurely). See
// `21-retention.md` "TryCleanupSegments Synchronization" for full details.
//
// Called by: WAL writer (during rotation), advanceLogStartOffset (after trim).
func (w *WALWriter) TryCleanupSegments()
```

---

## Entry Framing

The WAL and `metadata.log` use the same on-disk entry framing:

```
[4 bytes]  entry_length (uint32, big-endian, excludes these 4 bytes)
[4 bytes]  crc32c (over remaining bytes)
[N bytes]  payload (caller-defined)
```

Each writer (WAL writer goroutine, metadata log writer) implements this
framing inline — it's ~10 lines of code per writer. No shared abstraction
is needed because the WAL and metadata.log have fundamentally different
write patterns (the WAL uses segment rotation + fsync batching via a
dedicated goroutine; metadata.log is synchronous single-file writes).

A standalone `scanFramedEntries(file, fn)` function implements the recovery
scan (read forward, validate CRC, stop at corruption). Both the WAL replay
and metadata.log replay call this function.

---

## Configuration

| Config | Default | Description |
|--------|---------|-------------|
| `wal.segment.bytes` | 67108864 (64 MiB) | Segment rotation size |
| `wal.sync` | `true` | Fsync on every write batch |
| `wal.sync.interval` | 2ms | Fsync batch window |
| `--wal-max-disk-size` | 1 GiB | Max unflushed WAL on disk |
| `--ring-buffer-max-memory` | 512 MiB | Global memory budget for all partition ring buffers |

---

## When Stuck

These references are for understanding correct behavior and gathering context,
not for copying patterns. Find the optimal solution for our architecture.

1. Read `research/automq.md` for WAL design context -- AutoMQ uses a similar
   interleaved WAL with batched fsync, though writing to S3 instead of local disk.
2. Read `research/automq.md` for WAL and S3 storage design context.
3. For fsync batching patterns, check `.cache/repos/automq/s3stream/` -- search
   for WAL writer and sync logic.
4. For ring buffer edge cases (wrap-around, eviction), write targeted unit tests
   and verify with `go test ./internal/cluster/ -run TestRingBuffer -v`.
5. See `00b-testing.md` "When Stuck: Escalation Path" for additional tools.

---

## Acceptance Criteria

WAL:
- WAL writer goroutine starts and accepts entries
- Entries are written to segment files with correct format
- Fsync batching works (multiple entries per fsync)
- Segment rotation occurs at configured size
- In-memory index is updated on every append
- `ReadBatch` reads correct data via pread
- `FramedLog` scan detects and stops at corrupted entries

Ring buffer:
- Ring buffer push/get work correctly with wrap-around
- O(1) push and O(1) lookup by offset
- Global memory budget is respected across all partitions

Integration:
- `fetchFrom` cascades correctly: ring buffer -> WAL -> (S3 in Phase 4)
- `commitBatch` advances HW in strict offset order via ring buffer push
- `reserveOffset` + `commitBatch` flow handles concurrent Produces
- `TestWALProduceRestart` passes (ring buffer rebuilt from WAL on startup)
- `TestWALReadTier` -- produce data, evict from ring buffer, verify fetch reads from WAL

### Verify

```bash
# This work unit:
go test ./internal/wal/ -v -race -count=5
go test ./internal/cluster/ -run 'TestRingBuffer|TestCommitBatch|TestReserveOffset|TestReadCascade' -v -race -count=5
go test ./test/integration/ -run 'TestWALProduce|TestWALRead' -v -race -count=5

# Regression check (all prior work units):
go test ./... -race -count=5
```

### Commit

After tests pass, stage and commit all changes:

```bash
git add -A
git commit -m "15: WAL format and writer — segment files, fsync batching, ring buffer, read cascade"
```
