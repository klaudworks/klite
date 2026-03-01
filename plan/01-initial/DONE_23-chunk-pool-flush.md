# Chunk Pool and Memory-First S3 Flush

Replace the WAL-read flush pipeline with a shared chunk pool that holds all
unflushed data in memory. S3 flushes read from the chunk pool (zero disk I/O).
The WAL becomes an async crash-recovery journal — written but never read during
normal operation.

Phase 5. Prerequisite: S3 storage (Phase 4) works.

Read before starting: `DONE_15-wal-format-and-writer.md` (WAL writer, ring
buffer, read cascade), `DONE_18-s3-storage.md` (flush pipeline, S3 object
format), `internal/cluster/ringbuf.go` (current ring buffer implementation),
`internal/s3/flusher.go` (current flusher — being replaced).

---

## Problem

The current flush pipeline re-reads all unflushed data from WAL segment files
on disk before uploading to S3. This creates 2x read-write amplification on
the storage device:

```
Current: Produce writes 100 MB/s to WAL (disk write)
         Flusher reads 100 MB/s from WAL (disk read)
         Total disk I/O: 200 MB/s
         gp3 baseline on m7g.large: 78.75 MB/s
         Result: flusher can never keep up
```

Even with rate limiting, the flusher falls behind at high ingest rates.
The problem compounds: each flush cycle accumulates a larger backlog, causing
the next cycle to take longer, which accumulates an even larger backlog. This
is the root cause of the p95 latency climb under sustained load.

The data is already in memory (the ring buffer holds recent batches). Re-reading
it from disk is unnecessary.

---

## Design Overview

```
Before:
  Produce -> ring buffer (memory) + WAL (disk write)
  Flush   -> WAL (disk read) -> S3 upload

After:
  Produce -> chunk pool (memory) + WAL (disk write, async crash journal)
  Flush   -> chunk pool (memory read) -> S3 upload -> release chunks
```

Three new components:

1. **Chunk pool** — global pool of fixed-size memory chunks (~1 MiB each).
   Partitions borrow chunks as they ingest data. Chunks are released back to the
   pool after their data is flushed to S3.

2. **Memory-first flusher** — reads batch data from the chunk pool instead of
   from WAL segment files. Zero disk I/O during flush.

3. **WAL role change** — the WAL is still written on every produce (crash
   durability), but never read during normal operation. Only read on startup
   after a crash, to rebuild the chunk pool.

The existing per-partition ring buffer is **replaced** by the chunk pool. The
ring buffer and chunk pool serve the same purpose (in-memory batch storage for
reads and flush), so maintaining both would be redundant memory usage.

---

## Chunk Pool

### Data Structure

The chunk pool is a global, fixed-budget memory allocator. It manages a set of
fixed-size chunks. Each chunk is a contiguous byte buffer that holds one or more
RecordBatch entries for a single partition.

```go
// ChunkPool manages a global pool of fixed-size memory chunks.
type ChunkPool struct {
    chunkSize   int           // bytes per chunk (default 4 MiB)
    maxChunks   int           // total chunks = budget / chunkSize
    free        []*Chunk      // available chunks (stack, LIFO for cache warmth)
    mu          sync.Mutex    // protects free list
    stats       ChunkPoolStats
}

// Chunk is a single memory buffer, owned by one partition at a time.
type Chunk struct {
    data     []byte         // fixed-size backing buffer, allocated once
    used     int            // bytes written so far
    partition TopicPartition // which partition owns this chunk
    batches  []ChunkBatch   // index of batches within this chunk
    baseSeq  int64          // chunk sequence (for ordering within a partition)
}

// ChunkBatch records the position of one RecordBatch within a chunk.
type ChunkBatch struct {
    Offset    int        // byte offset within chunk.data
    Size      int        // batch size in bytes
    BaseOffset int64     // Kafka base offset of this batch
    LastOffsetDelta int32
    MaxTimestamp int64
}
```

### Lifecycle

Chunks are allocated **lazily**: a partition acquires a chunk only when it
receives a produce. Idle partitions hold zero chunks.

```
1. Pool initialized at startup with maxChunks = budget / chunkSize
   All chunks pre-allocated, placed on free list.

2. Partition receives a produce:
   a. If current chunk is nil -> acquire from pool (lazy allocation)
   b. If current chunk has room -> append to current chunk
   c. If current chunk is full -> seal it (move to flush queue),
      acquire a new chunk from pool
   d. If pool is empty -> backpressure (see Backpressure section)

3. Multiple small batches pack into the same chunk. A 1 MiB chunk
   receiving 16 KiB batches holds ~64 batches before sealing.

4. Flusher picks up sealed + current chunks from partition:
   a. Detaches sealed chunks (and current chunk if flushing all)
   b. Assembles S3 object from chunk batch data
   c. Uploads to S3
   d. Returns chunks to pool's free list
   e. Partition's current is set to nil -> holds zero chunks until
      next produce

5. On crash recovery:
   Replay WAL -> re-populate chunks (same as initial produce path)
```

### Lazy Allocation: Why It Matters

With eager allocation (one chunk per partition at startup), 500 partitions
consume 500 chunks immediately — nearly the entire 511-chunk pool at 512 MiB.
No headroom for sealed chunks or hot partitions.

With lazy allocation, only partitions that receive produces hold chunks:

```
500 total partitions, 10 hot, 490 idle:
  Hot partitions: ~10 current + ~40 sealed = ~50 chunks
  Idle partitions: 0 chunks
  Free pool: ~461 chunks
```

After the age-threshold flush (default 60s), even low-volume partitions that
received a single message get their chunk flushed and released. Steady state:
only actively-producing partitions hold chunks.

### Tail Waste

Each active partition's current chunk has unused capacity. With 1 MiB chunks
and 16 KiB average batches, a current chunk is on average 50% full. With 100
active partitions, that's ~50 MiB of unused capacity — acceptable overhead at
512 MiB budget (10%).

### Why Fixed-Size Chunks

- **No fragmentation** — all chunks are identical, allocated once at startup.
- **O(1) allocation** — pop from free list. No `malloc`, no GC pressure.
- **Naturally adaptive** — hot partitions hold many chunks, cold partitions
  hold few (possibly zero). No rebalancing timer needed.
- **Bounded memory** — total memory is `maxChunks * chunkSize`, fixed at startup.

### Chunk Size

The chunk size is always set to `max.message.bytes` (default 1048588 =
~1 MiB + 12 bytes). This is not configurable — it is derived automatically.

This is the optimal choice:
- Every RecordBatch fits in exactly one chunk (no spanning, no reassembly)
- It is the smallest possible chunk size that satisfies this guarantee,
  which maximizes the number of chunks in the pool
- If `max.message.bytes` changes (per-topic config), the chunk size is the
  maximum across all topics

```go
chunkSize = maxMessageBytesAcrossAllTopics()
maxChunks = budget / chunkSize
```

| max.message.bytes | Chunk size | Chunks at 512 MiB | Chunks at 1 GiB |
|---|---|---|---|
| ~1 MiB (default) | 1048588 | 511 | 1,023 |
| 5 MiB | 5 MiB | 102 | 204 |
| 10 MiB | 10 MiB | 51 | 102 |

Note: a large `max.message.bytes` dramatically reduces chunk count. Users
with 10 MiB max message size and 500 partitions need a proportionally
larger `--chunk-pool-memory`.

### Configuration

| Flag | Default | Description |
|---|---|---|
| `--chunk-pool-memory` | `536870912` (512 MiB) | Total memory budget for the chunk pool |
| `--s3-target-object-size` | `67108864` (64 MiB) | Target S3 object size before flush. Lower values (e.g. 32 MiB) reduce chunk hold time and memory requirements at the cost of more S3 PUT requests and objects to compact. |

Chunk size is derived from `max.message.bytes`, not configurable.

The old `--ring-buffer-max-memory` flag is removed. The chunk pool replaces
the ring buffer.

---

## Partition Integration

Each partition holds a reference to its "current" chunk (being written to) and
a queue of "sealed" chunks (full, awaiting flush). Both start as nil/empty —
chunks are only acquired on first produce.

```go
// Per-partition chunk state. Protected by pd.mu.
type partChunkState struct {
    current   *Chunk         // chunk being written to (nil until first produce)
    sealed    []*Chunk       // full chunks awaiting S3 flush, ordered by write time
    pool      *ChunkPool     // reference to global pool
}
```

### Produce Path

```
Handler goroutine                              WAL writer goroutine
     |                                                |
     |  pd.mu.Lock()                                  |
     |    baseOffset = pd.reserveOffset(meta)         |
     |    pd.appendToChunk(batch)  // <-- NEW         |
     |  pd.mu.Unlock()                                |
     |                                                |
     |  serialize WAL entry                           |
     |---[WAL entry]--------------------------------->|
     |                                       write() to segment
     |  (pd.mu is NOT held)                           |
     |                                           fsync()
     |<--[done]---------------------------------------|
     |                                                |
     |  pd.mu.Lock()                                  |
     |    pd.commitBatch(baseOffset)  // advance HW   |
     |  pd.mu.Unlock()                                |
     |  pd.notifyWaiters()                            |
```

**Key change:** `appendToChunk` happens during the first lock acquisition
(alongside `reserveOffset`), not after fsync. The batch data is in the chunk
pool immediately. If the process crashes before fsync, the chunk pool data is
lost — but so is the WAL entry, so this is consistent.

Wait — that's wrong. If we append to the chunk before WAL fsync, and the
process crashes, the chunk data is lost (memory) and the WAL entry was never
fsynced. That's fine — the produce never ACKed, so the client will retry.
But we've consumed a chunk slot. On recovery, the chunk pool is rebuilt from
WAL, which doesn't contain this entry. Consistent.

However, there's a subtlety: **the chunk data must be the offset-assigned
bytes**, not the raw client bytes. Offset assignment happens in
`reserveOffset`. So the sequence is:

```
1. pd.mu.Lock()
2. baseOffset = pd.reserveOffset(meta)
3. Assign offset into raw bytes (in-place mutation)
4. Copy into chunk (pd.appendToChunk)      // memory
5. pd.mu.Unlock()
6. WAL append (uses same offset-assigned bytes)  // disk
7. Wait for fsync
8. pd.mu.Lock()
9. pd.commitOffset(baseOffset)   // advance HW only, no data copy
10. pd.mu.Unlock()
```

`commitBatch` no longer pushes data into a ring buffer — the data is already
in the chunk. It only advances HW and nextCommit.

### Fetch Path (Read Cascade)

```
fetchFrom(offset, maxBytes):
    // Tier 1: Chunk pool (memory, scan partition's chunks)
    pd.mu.RLock()
    hw := pd.hw
    if batches := pd.fetchFromChunks(offset, maxBytes, hw); len(batches) > 0 {
        pd.mu.RUnlock()
        return batches
    }
    // Capture references for cold reads
    ...
    pd.mu.RUnlock()

    // Tier 2: WAL (only if chunks don't cover the offset — rare during
    //         normal operation, common during catch-up after restart)
    // Tier 3: S3 (same as before)
```

**HW visibility gate:** `fetchFromChunks` must only return batches where
`BaseOffset < hw`. Unlike the ring buffer (where data enters only after HW
advances in `CommitBatch`), chunks contain data before HW advances — the
batch is copied into the chunk during `reserveOffset`, before WAL fsync.
Without the HW filter, a consumer could observe a batch that was never
fsynced and never ACKed. If the process crashes before fsync, that batch
is gone, but the consumer already saw it — a correctness violation.

The HW check in `FetchFrom` (`if offset >= pd.hw { return nil }`) gates
callers who are fully caught up, but `fetchFromChunks` itself must also
stop yielding batches at the HW boundary to avoid returning uncommitted
data from the current chunk's tail.

During normal operation, the chunk pool holds ALL unflushed data, so Tier 2
(WAL reads) only happens for offsets that have already been flushed to S3 but
aren't in the chunk pool anymore. After a restart, before the WAL is fully
replayed, there may be a brief window where the WAL tier is needed.

Actually, after restart the WAL is replayed first (rebuilding chunks), so by
the time the broker accepts connections, all data is in chunks. The WAL read
tier is only needed for a degenerate case: the chunk pool is too small to hold
all data between the S3 watermark and HW. In that case, the oldest unflushed
data falls out of chunks and must be read from WAL for flush. This is the
graceful degradation path.

### Chunk Acquisition and Sealing

```go
// appendToChunk appends a batch to the partition's current chunk.
// Acquires a chunk lazily on first call. Seals and acquires a new chunk
// when the current one is full.
// Caller must hold pd.mu.Lock().
func (pd *PartData) appendToChunk(raw []byte, meta BatchMeta) {
    // Lazy acquisition: first produce after startup or after flush
    // released the current chunk.
    if pd.chunks.current == nil {
        pd.chunks.current = pd.chunks.pool.Acquire()
        // If Acquire blocks (pool empty), this blocks the produce.
        // See Backpressure section.
    }

    // Seal if this batch wouldn't fit. Since chunkSize >= max.message.bytes,
    // a batch always fits in an empty chunk.
    if pd.chunks.current.used + len(raw) > pd.chunks.pool.chunkSize {
        pd.chunks.sealed = append(pd.chunks.sealed, pd.chunks.current)
        pd.chunks.current = pd.chunks.pool.Acquire()
    }

    offset := pd.chunks.current.used
    copy(pd.chunks.current.data[offset:], raw)
    pd.chunks.current.used += len(raw)
    pd.chunks.current.batches = append(pd.chunks.current.batches, ChunkBatch{
        Offset:          offset,
        Size:            len(raw),
        BaseOffset:      meta.BaseOffset,
        LastOffsetDelta:  meta.LastOffsetDelta,
        MaxTimestamp:     meta.MaxTimestamp,
    })
}
```

---

## Memory-First Flusher

The flusher scans partitions and collects data from the chunk pool instead of
reading from WAL segment files.

### Flush Algorithm

```
Every CheckInterval (5s), or on emergency trigger:

1. Scan all partitions for flush eligibility:
   - Size threshold: sum of sealed chunk bytes >= TargetObjectSize (64 MiB)
   - Age threshold: oldest sealed chunk age >= FlushInterval (60s)
   - Emergency: flush all (triggered by chunk pool pressure)

2. For each eligible partition:
   a. pd.mu.Lock()
   b. Detach sealed chunks (move to local variable, clear pd.sealed)
   c. If current chunk is non-empty and we're flushing all, seal it too
   d. pd.mu.Unlock()

3. For each partition's detached chunks (no lock held):
   a. Walk chunk batches to build []BatchData for S3 object assembly
   b. Build S3 object (data section + footer)
   c. Upload to S3 with retry
   d. On success: advance S3 watermark, release chunks back to pool

4. After all flushes: signal WAL cleanup (WAL entries for flushed offsets
   can be pruned, segments can be deleted)
```

### No Rate Limiting Needed

The current flusher has a `rate.Limiter` to throttle WAL reads. With
memory-first flush, there are no disk reads to throttle. The `--s3-flush-read-rate`
flag and all rate limiter code are removed.

The S3 upload bandwidth is the only throughput constraint, and it's governed
by network (not disk). On m7g.large with S3 in the same region, 8 concurrent
uploads achieve ~625 MB/s effective throughput — far above any realistic
ingest rate.

### Chunk Pool Pressure (replaces WAL Disk Pressure)

The WAL writer's disk pressure mechanism (75% emergency flush, 100% pause) is
replaced by chunk pool pressure:

```
pressure = allocatedChunks / maxChunks

>= 75%:  signal emergency flush (same triggerCh mechanism)
>= 90%:  Acquire() blocks until a chunk is freed (backpressure on producers)
```

At 90% pressure, `Acquire()` blocks. Since `appendToChunk` is called under
`pd.mu.Lock()`, this blocks the produce handler for that partition. Other
partitions on different locks are unaffected (unless they also need chunks).
This is preferable to rejecting produces with `KAFKA_STORAGE_ERROR` — the
producer's request simply takes longer, and the flusher is concurrently
freeing chunks.

---

## WAL Role Change

### What Stays the Same

- WAL entry format (unchanged)
- WAL segment files (unchanged)
- WAL writer goroutine (unchanged write path)
- WAL fsync batching (unchanged)
- WAL segment rotation (unchanged)
- WAL replay on startup (unchanged, but now rebuilds chunk pool instead of
  just ring buffer + WAL index)

### What Changes

| Aspect | Before | After |
|---|---|---|
| Flush data source | WAL disk reads | Chunk pool memory reads |
| WAL read during normal operation | Yes (flush + fetch tier 2) | No (only on crash recovery) |
| Fetch tier 2 | WAL pread via index | Chunk pool scan (memory) |
| WAL read rate limiter | Yes (50 MiB/s default) | Removed |
| WAL disk pressure mechanism | Emergency flush + pause at 75%/100% | Replaced by chunk pool pressure |
| Segment cleanup trigger | After S3 flush prunes WAL index | Same (unchanged) |
| WAL max disk size purpose | Limit read I/O and disk usage | Limit crash recovery time |

### WAL Sizing

The WAL now only needs to hold enough data for crash recovery. After an S3
flush, WAL segments covering flushed offsets are pruned and deleted.

At 100 MB/s ingest with 10s flush interval:
- Unflushed WAL data: ~1 GiB
- With 3x safety margin: ~3 GiB
- Default `--wal-max-disk-size`: 4 GiB (increased from 1 GiB)

The WAL is larger than before because we no longer rely on it for reads
(so keeping more data doesn't cause I/O contention). The extra size provides
more recovery headroom if S3 flushes are delayed.

### Crash Recovery

On startup after a crash:

```
1. Replay WAL segments (same as before)
2. For each WAL entry:
   a. Rebuild partition state (offsets, HW) — same as before
   b. Append batch data to chunk pool — NEW (replaces ring buffer push)
   c. Update WAL index — same as before
3. After replay, chunk pool contains all unflushed data
4. Flush immediately to S3 (optional, configurable)
5. Start accepting connections
```

The chunk pool is rebuilt from WAL on startup. This is the only time WAL
segments are read. Recovery time is bounded by WAL size and disk read speed:

| WAL size | Disk read speed | Recovery time |
|---|---|---|
| 1 GiB | 125 MB/s (gp3 baseline) | ~8s |
| 1 GiB | 500 MB/s (NVMe) | ~2s |
| 4 GiB | 125 MB/s | ~32s |
| 4 GiB | 500 MB/s | ~8s |

---

## Deployment Recommendations

### Instance Selection

| Ingest rate | Recommended instance | WAL storage | Monthly cost |
|---|---|---|---|
| 1-10 MB/s | m7g.large + gp3 | EBS (gp3, 20 GiB) | ~$60 |
| 10-50 MB/s | m7g.large + gp3 | EBS (gp3, 50 GiB) | ~$63 |
| 50-100 MB/s | m7gd.large (NVMe) | Instance store | ~$77 |
| 100-200 MB/s | m7gd.xlarge (NVMe) | Instance store | ~$154 |

The chunk pool eliminates read I/O, so the only disk I/O is WAL writes.
gp3 baseline (125 MB/s, capped by instance EBS bandwidth) is sufficient
for up to ~50 MB/s ingest on m7g.large (78.75 MB/s EBS bandwidth).

For higher ingest rates, m7gd instances with NVMe instance store provide
500+ MB/s write throughput at lower cost than provisioned EBS.

### NVMe Instance Store Notes

- Data persists across process crashes and OS reboots
- Data is lost on instance stop/start/terminate
- Graceful shutdown flushes all data to S3 before stopping (FlushAll)
- Maximum data loss on ungraceful instance termination: one flush interval
  (default 60s, configurable down to 5s)

### Memory Sizing

| Partitions | Ingest rate | Chunk pool budget | Chunks (1 MiB) | Headroom |
|---|---|---|---|---|
| 6 | 100 MB/s | 512 MiB | 512 | ~5s of data |
| 50 | 50 MB/s | 1 GiB | 1,024 | ~20s of data |
| 500 | 10 MB/s | 512 MiB | 512 | ~50s of data |
| 500 | 100 MB/s | 2 GiB | 2,048 | ~20s of data |

Rule of thumb: chunk pool budget should hold 2-3x the data produced in
one flush check interval (default 5s).

---

## What Gets Removed

1. **Ring buffer** (`internal/cluster/ringbuf.go`) — replaced by chunk pool
2. **`--ring-buffer-max-memory` flag** — replaced by `--chunk-pool-memory`
3. **`CalcRingSlots`** — no longer needed (chunks are shared, not pre-partitioned)
4. **`CollectWALBatches`** in `internal/s3/flusher.go` — replaced by chunk reads
5. **`--s3-flush-read-rate` flag** — no disk reads to rate-limit
6. **WAL read rate limiter** in flusher — removed
7. **WAL disk pressure mechanism** in writer (75%/100% thresholds) — replaced
   by chunk pool pressure

## What Gets Added

1. **`internal/chunk/pool.go`** — ChunkPool, Chunk types, Acquire/Release
2. **`internal/chunk/pool_test.go`** — unit tests
3. **Chunk state in PartData** — current chunk, sealed queue
4. **`--chunk-pool-memory` flag** (chunk size derived from `max.message.bytes`)
5. **Chunk pool pressure tracking** — allocated/max ratio, emergency trigger
6. **Chunk-based fetch** in PartData.FetchFrom — replaces ring buffer tier 1

## What Gets Modified

1. **`internal/s3/flusher.go`** — read from chunks instead of WAL
2. **`internal/cluster/partition.go`** — chunk integration in produce/fetch paths
3. **`internal/cluster/state.go`** — chunk pool initialization instead of ring buffers
4. **`internal/broker/broker.go`** — chunk pool wiring, config plumbing
5. **`internal/broker/config.go`** — new flags, remove old flags
6. **`internal/wal/writer.go`** — remove disk pressure mechanism (keep writer
   otherwise unchanged)
7. **WAL replay** — rebuild chunk pool instead of ring buffer

---

## Concurrency Model

### Chunk Pool (global)

- `pool.mu` protects only the free list
- Acquire: lock, pop from free list, unlock. O(1).
- Release: lock, push to free list, unlock. O(1).
- No contention between partitions unless both acquiring simultaneously
  (which is brief — just a slice pop)

### Partition Chunks (per-partition)

- Protected by `pd.mu` (same lock as before)
- `appendToChunk` under `pd.mu.Lock()` — same as current `PushBatch`
- `fetchFromChunks` under `pd.mu.RLock()` — same as current ring buffer read
- `detachSealed` under `pd.mu.Lock()` — flusher takes sealed chunks

### Flusher

- Detaches chunks under `pd.mu.Lock()` (brief, no I/O)
- All I/O (S3 upload) happens without any lock held
- `Release` chunks back to pool after upload (brief pool.mu)

No new lock ordering constraints. The chunk pool lock is leaf-level (never
held while acquiring pd.mu or any other lock).

---

## Graceful Degradation

If the chunk pool is too small to hold all unflushed data (e.g., 500
partitions, 10 MB/s each, 512 MiB pool, flush takes 10s):

1. Hot partitions consume chunks faster than the flusher releases them
2. Pool hits 75% → emergency flush triggered
3. Pool hits 90% → Acquire blocks, producing slows down
4. Flusher uploads and releases chunks → Acquire unblocks
5. If flusher can't keep up → sustained backpressure on producers

This is correct behavior: the broker is signaling that it cannot ingest
faster than it can flush. The operator should either increase the chunk pool
budget or reduce the ingest rate.

**Fallback to WAL reads**: NOT implemented. If data falls out of chunks before
being flushed, it is lost (the S3 object won't contain those batches). This is
prevented by the backpressure mechanism — chunks are never released until
flushed. The sealed chunk queue grows until the flusher processes it. The pool
budget is the hard limit.

---

## Testing

### Unit Tests

- `TestChunkPoolAcquireRelease` — basic acquire/release cycle
- `TestChunkPoolExhaustion` — acquire all chunks, verify next acquire blocks
- `TestChunkPoolPressure` — verify pressure calculation and emergency trigger
- `TestPartitionAppendToChunk` — append batches, verify chunk contents
- `TestPartitionChunkSealing` — fill chunk, verify it moves to sealed queue
- `TestPartitionFetchFromChunks` — write batches, fetch by offset
- `TestFlusherMemoryPath` — flush from chunks instead of WAL

### Integration Tests

- `TestChunkPoolS3Flush` — produce, flush from memory, verify S3 objects
- `TestChunkPoolCrashRecovery` — produce, crash (kill WAL writer), restart,
  verify chunks rebuilt from WAL
- `TestChunkPoolBackpressure` — small pool, high ingest, verify produces
  slow down (not error)

### Verify

```bash
go test ./internal/chunk/ -v -race -count=5
go test ./internal/cluster/ -run 'TestPartitionChunk' -v -race -count=5
go test ./internal/s3/ -run 'TestFlusherMemory' -v -race -count=5
go test ./test/integration/ -run 'TestChunkPool' -v -race -count=5
go test ./... -race -count=5
```

---

## Migration

### From Ring Buffer to Chunk Pool

The ring buffer is removed entirely. This is a breaking change in internal
structure but transparent to Kafka clients.

1. Remove `ringbuf.go` and `ringbuf_test.go`
2. Remove ring buffer fields from `PartData`
3. Add chunk state fields to `PartData`
4. Update `CommitBatch` to only advance HW (no ring push — data already in chunk)
5. Update `FetchFrom` to read from chunks instead of ring
6. Update `PushBatch` (WAL replay) to write to chunk instead of ring
7. Update `SetWALConfig` to accept ChunkPool instead of ring buffer params
8. Update flusher to read from chunks
9. Remove WAL read rate limiter
10. Replace WAL disk pressure with chunk pool pressure

### Flag Migration

| Old flag | New flag | Notes |
|---|---|---|
| `--ring-buffer-max-memory` | `--chunk-pool-memory` | Same default (512 MiB) |
| `--s3-flush-read-rate` | (removed) | No disk reads to limit |

---

## When Stuck

1. For chunk pool allocation patterns, see Redpanda's iobuf design
   (`research/warpstream.md` or search "redpanda tpc buffers").
2. For backpressure mechanisms, check `.cache/repos/franz-go/pkg/kfake/`
   produce handler — how does kfake handle full partitions?
3. For crash recovery with chunks, the WAL replay path in
   `internal/broker/broker.go` `replayWAL()` is the starting point.
4. See `00b-testing.md` "When Stuck: Escalation Path" for additional tools.

---

## Acceptance Criteria

Chunk Pool:
- Pre-allocated at startup, fixed memory budget
- Acquire/Release are O(1) and lock-free for the happy path
- Pressure tracking triggers emergency flush at 75%
- Backpressure at 90% slows producers without errors

Partition Integration:
- Produce path appends to chunk before WAL write
- CommitBatch only advances HW (no data copy)
- FetchFrom reads from chunks (tier 1), falls through to WAL/S3

Flusher:
- Reads batch data from chunks, not from WAL segments
- Releases chunks back to pool after successful S3 upload
- No disk reads during flush (verify with I/O tracing in test)

WAL:
- Still written on every produce (crash durability)
- Never read during normal operation
- Crash recovery replays WAL to rebuild chunk pool
- Segments pruned after S3 flush (same as before)

Performance:
- Flush throughput limited only by S3 upload bandwidth, not disk
- gp3 on m7g.large can sustain 50 MB/s ingest without I/O contention
- NVMe on m7gd.large can sustain 200+ MB/s ingest
