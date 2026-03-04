# S3 Storage

S3 offload pipeline for long-term storage. Unflushed WAL data is periodically
written to per-partition S3 objects, enabling cost-effective unlimited retention.

Phase 4 (alongside transactions). Prerequisite: WAL persistence (Phase 3) works.

Read before starting: `15-wal-format-and-writer.md` (WAL segment deletion
policy, read cascade — S3 is tier 3, ring buffer and WAL index interaction),
`16-metadata-log.md` (unified S3 sync cycle uploads metadata.log),
`research/warpstream.md` (zero-disk S3 architecture for context).

---

## Design Overview

```
WAL segments (recent, fast) --> per-partition S3 objects (historical, cheap)

Write: WAL fsync -> ACK -> (async) flush partition data -> PUT -> trim WAL
Read:  ring buffer -> WAL (index + pread) -> S3 (cascade)
```

The broker serves recent data from the ring buffer and WAL. S3 is only
used for data older than what local storage retains.

**Two sources of truth:**
- `metadata.log`: topics, configs, consumer offsets, producer IDs (see `16-metadata-log.md`)
- S3 objects: record data, self-describing via key naming (no local routing table needed)

No overlap between these two. No consistency issues.

---

## S3 Object Format

Each S3 object contains data for **one partition only**. This is simpler than
a multi-partition format and enables S3 key naming that serves as its own
routing index — no local routing table, no metadata.log bloat.

### Layout

```
[M bytes]     data section: concatenated raw RecordBatch bytes (sequential offsets)
[N × 20 B]   batch index: one entry per RecordBatch
[4 bytes]     entry count (uint32, big-endian)
[4 bytes]     magic number (0x4B4C4958 = "KLIX", big-endian)
```

The data section is the raw RecordBatch bytes — identical to what was
previously the entire object. The batch index footer enables S3 range
reads: the reader can fetch just the footer to discover batch boundaries,
then issue a targeted range GET for only the needed batches. Without a
footer, the reader must download the entire object even if the consumer
only needs one batch.

**Batch index entry (20 bytes, big-endian):**

```
[8 bytes]   baseOffset (int64)       — first Kafka offset in this batch
[4 bytes]   bytePosition (uint32)    — byte offset within the data section
[4 bytes]   batchLength (uint32)     — total batch size (61-byte header + payload)
[4 bytes]   lastOffsetDelta (int32)  — compute last offset without parsing the batch
```

**Footer size examples:**

| Object size | Avg batch size | Batches | Footer size | Overhead |
|---|---|---|---|---|
| 256 MiB | 1 MiB | 256 | 5,128 B (~5 KiB) | 0.002% |
| 64 MiB | 64 KiB | 1,024 | 20,488 B (~20 KiB) | 0.03% |
| 16 MiB | 16 KiB | 1,024 | 20,488 B (~20 KiB) | 0.12% |

The footer is negligible in all cases.

**Magic number:** The trailing 4-byte magic (`0x4B4C4958`) serves as a
validity check when parsing the footer. The reader verifies the magic
before trusting the entry count and index entries — a corrupted or
truncated object won't accidentally be parsed as having a valid footer.

**Why a footer and not a header:** The footer can be read with a single
range GET of the last N bytes of the object. A header would require
knowing the header size before reading the data — either a fixed-size
header (wasteful, limits max batches) or a two-phase read (read header
size, then read header). The footer approach needs at most one speculative
read: request the last 64 KiB, which covers up to 3,272 batches. If the
object has more batches (extremely unlikely — that's 64 KiB batches in a
256 MiB object), a second read fetches the full footer.

### S3 Key Format

```
s3://<bucket>/<prefix>/<topic>/<partition>/<baseOffset>.obj

Examples:
s3://my-bucket/klite/orders/5/00000000000000000.obj
s3://my-bucket/klite/orders/5/00000000000001000.obj
s3://my-bucket/klite/clicks/0/00000000000050000.obj
```

- `<prefix>`: cluster-specific prefix (includes cluster ID for multi-tenant buckets)
- `<topic>/<partition>`: natural partitioning in S3 namespace
- `<baseOffset>`: 20-digit zero-padded offset of the first record in the object.
  Zero-padding ensures lexicographic sort matches numeric sort.

**The S3 key IS the index.** To find the object containing offset X for
partition P of topic T:

```
ListObjectsV2(
    prefix = "<prefix>/<topic>/<partition>/",
)
-> Find the last key whose base offset <= X.
   Iterate the listing, or use the fact that keys are lexicographically
   sorted: the last key that is <= zeroPad(X) contains the data.
```

**Note on `startAfter`:** S3's `startAfter` parameter is exclusive — it
returns objects whose keys come *after* the given key lexicographically. If
offset X exactly matches a base offset, `startAfter=zeroPad(X)` would skip
that object. Instead, list with `prefix` only and find the correct object
client-side, or use `startAfter=zeroPad(X-1)` and take the first result.
The result is cached in a per-partition LRU (see S3 Fetch below), so the
ListObjects cost is amortized.

No routing table to maintain, no metadata.log entries for S3 objects. The
S3 namespace is self-describing. The per-object batch index footer enables
efficient range reads (see S3 Fetch below) but is not needed for object
discovery — that's handled entirely by key naming.

---

## Flush Pipeline

### Per-Partition Flush

Each partition flushes independently. The flush goroutine scans unflushed WAL
entries per partition, assembles the data and batch index into an S3 object,
and uploads.

```
For each partition with unflushed data:
  1. Collect unflushed RecordBatch bytes for this partition from WAL
  2. Concatenate into a contiguous data section
  3. Build the batch index footer:
     - Walk RecordBatch boundaries in the data section (each batch has a
       61-byte header with BaseOffset at bytes 0-7 and Length at bytes 8-11)
     - For each batch: record (baseOffset, bytePosition, batchLength, lastOffsetDelta)
     - Append: index entries + entry count (uint32) + magic (0x4B4C4958)
  4. Upload data + footer to s3://<prefix>/<topic>/<partition>/<baseOffset>.obj
  5. On success: mark the corresponding WAL entries as flushed for this partition
  6. If all entries in a WAL segment are flushed: delete the segment
```

Building the footer adds negligible cost: the flush pipeline already walks
batch boundaries to determine the object's base offset. Recording
`(bytePosition, batchLength, lastOffsetDelta)` per batch is a single pass
over the concatenated data — no decompression needed (these fields are in
the uncompressed batch header).

### Flush Triggers

Only two triggers — keep it simple:

| Trigger | Config | Default | Description |
|---------|--------|---------|-------------|
| Unified sync | `--s3-flush-interval` | 10m | Flush ALL partitions + upload metadata.log. Also runs on graceful shutdown. |
| WAL pressure | -- | 80% of `--wal-max-disk-size` | Emergency: force flush oldest partitions to free WAL space |

No per-partition size threshold. The unified sync interval already flushes
everything periodically, and WAL pressure handles emergencies. Adding a
per-partition size trigger would complicate the flush pipeline without
meaningful benefit for a single-broker system.

### Unified S3 Sync Cycle

The primary S3 backup mechanism. Ensures both record data and metadata are
backed up to S3 in a consistent state:

```
Every --s3-flush-interval (default 10m), and on graceful shutdown:
  1. Flush all partitions with unflushed data to S3 (per-partition PUTs)
  2. Wait for all S3 PUTs to complete
  3. Delete fully-flushed WAL segments
  4. Compact metadata.log if needed (>64 MiB)
  5. Upload metadata.log to s3://<prefix>/metadata.log
```

**After step 5:** the S3 backup is a perfect snapshot. Single RPO:
`--s3-flush-interval`.

### Failure Handling

- S3 upload failure: retry with exponential backoff (1s, 2s, 4s, ... up to 60s)
- During retry: WAL continues accepting writes (no backpressure until WAL full)
- If WAL fills during S3 outage: reject produces with `KAFKA_STORAGE_ERROR`
- On success after retries: resume normal operation

---

## Read Path

### Three-Tier Read Cascade

| Tier | Medium | Latency | Budget Config | Populated By |
|------|--------|---------|--------------|--------------|
| 1. Ring buffer | RAM | ~10ns | `--ring-buffer-max-memory` (512 MiB) | Produce path only |
| 2. WAL | Local disk | ~1us (page cache) / ~1ms (disk) | `--wal-max-disk-size` (1 GiB) | Produce path (WAL write) |
| 3. S3 objects | Network | ~60ms (footer cached) / ~110ms (cold) | Kafka retention policy (`retention.ms`) | S3 flush pipeline |

S3 latency breakdown: ~50ms first-byte latency per range GET + transfer.
A cold read (footer not cached) requires two range GETs: one for the
footer (~50ms), one for the data (~50ms + transfer). A warm read (footer
cached) requires one range GET for the data only.

See `15-wal-format-and-writer.md` for the read cascade implementation.

### S3 Fetch

The S3 fetch path uses the batch index footer to issue targeted range
reads — only the needed batches are downloaded, not the entire object.

```go
func (s *S3Reader) Fetch(topic string, partition int32, offset int64, maxBytes int32) ([]byte, error) {
    // 1. Find the right S3 object via key naming
    prefix := fmt.Sprintf("%s/%s/%d/", s.prefix, topic, partition)
    key, objectSize := s.findObjectForOffset(prefix, offset)
    // findObjectForOffset: list objects with prefix, find last key whose
    // base offset <= offset. Result is cached in an in-memory LRU per partition.
    // S3 ListObjectsV2 includes Content-Length, so objectSize is free.

    // 2. Read the batch index footer (cached in memory after first access)
    footer, err := s.getFooter(key, objectSize)
    if err != nil {
        return nil, err
    }

    // 3. Binary search the footer for the first batch containing offset
    startIdx := footer.FindBatch(offset) // binary search on baseOffset

    // 4. Compute the byte range: from the target batch to maxBytes
    startByte := footer.Entries[startIdx].BytePosition
    endByte := s.computeEndByte(footer, startIdx, maxBytes)

    // 5. Range GET only the needed bytes
    data, err := s.s3RangeGet(key, startByte, endByte)
    if err != nil {
        return nil, err
    }

    // 6. Return batches from requested offset onward
    return extractFromOffset(data, offset, maxBytes), nil
}
```

**Footer caching:** The `getFooter` method maintains an in-memory LRU of
parsed footers, keyed by S3 object key. Each cached footer is ~5-20 KiB.
For 1,000 objects across all partitions, the footer cache uses ~20 MiB —
negligible.

On a cache miss, `getFooter` issues a single range GET for the last 64 KiB
of the object. This covers footers with up to 3,272 batch entries. If the
footer is larger (detected by checking `entryCount × 20 + 8 > 65536`), a
second range GET fetches the full footer. In practice, the speculative
64 KiB read is sufficient — a 256 MiB object with 64 KiB average batches
has 4,096 entries (80 KiB footer), which requires one extra read. Objects
with 1 MiB average batches have only 256 entries (5 KiB footer).

**Object key lookup caching:** The `findObjectForOffset` result is cached
in a per-partition LRU. After the first ListObjects call, subsequent
fetches for nearby offsets hit the cache.

**Footer cache invalidation:** When compaction rewrites objects for a
partition (creating new object keys and deleting old ones), the compactor
calls `s3Reader.InvalidateFooters(topic, partition)` to evict stale footer
cache entries. This is safe because the `findObjectForOffset` LRU is also
invalidated (the old object keys no longer exist in S3). The next fetch
discovers the new object key via ListObjects and loads its footer fresh.

### Read Amplification

With the batch index footer, the S3 fetch reads only the needed batches:

| Fetch size | Object size | Data read | Amplification | Latency |
|---|---|---|---|---|
| 1 MiB | 256 MiB | 1 MiB + 5 KiB footer | 1.005x | ~60ms |
| 50 MiB | 256 MiB | 50 MiB + 5 KiB footer | 1.0001x | ~560ms |
| 1 MiB | 16 MiB | 1 MiB + 20 KiB footer | 1.02x | ~60ms |

Compare to the full-object GET approach (no footer):

| Fetch size | Object size | Data read | Amplification | Latency |
|---|---|---|---|---|
| 1 MiB | 256 MiB | 256 MiB | 256x | ~2,610ms |
| 50 MiB | 256 MiB | 256 MiB | 5.12x | ~2,610ms |

The footer eliminates read amplification. Each fetch transfers only what
the consumer requested (plus a negligible footer on first access).

---

## Disaster Recovery (Disk Loss)

### Fast Path: metadata.log Backup Exists (Typical)

```
1. Download s3://<prefix>/metadata.log       -- 1 GET, ~300 KB
2. Replay metadata.log -> topics, configs, committed offsets, producer IDs
3. Rebuild per-partition totalBytes from S3 object listings:
   For each partition:
     ListObjectsV2 with prefix "<prefix>/<topic>/<partition>/"
     totalBytes = sum of each object's Size (from ListObjectsV2 response)
   ListObjectsV2 returns Content-Length for each object — no HeadObject
   calls needed. For 100 partitions, this adds ~5 seconds to startup
   (100 × ~50ms LIST latency).
4. WAL is empty -- all data served from S3 until new produces arrive
5. Start accepting connections
```

**What is lost:** up to `--s3-flush-interval` (default 10 minutes) of both
record data and metadata changes.

**Why rebuild `totalBytes`:** Without this, `totalBytes` would be 0 for
all partitions after disaster recovery, causing `retention.bytes`
enforcement to incorrectly skip all partitions (they appear empty).
`DescribeLogDirs` would also report 0 bytes for all partitions.

### Slow Path: No metadata.log Backup (Fallback)

```
1. ListObjectsV2 with prefix "<prefix>/"     -- discovers all topic/partition paths
2. Infer topics and partition counts from key structure
3. Rebuild per-partition totalBytes from the same listing
   (sum each object's Size per partition — no extra S3 calls needed)
4. Write fresh metadata.log with inferred topics (no configs, no offsets)
5. WAL is empty -- all data served from S3
6. Start accepting connections
```

---

## Future Optimization: Read-Ahead Buffer

Deliberately deferred. The batch index footer already eliminates read
amplification — each fetch reads only the needed batches via S3 range GET.
The remaining overhead is S3 round-trip latency (~50ms per fetch).

If sequential cold-read throughput becomes a bottleneck (e.g., many
consumers catching up simultaneously), a per-consumer read-ahead buffer
can be added:

```go
// Lightweight read-ahead: request more data than needed, cache the excess.
// Next sequential fetch likely hits the buffer. ~20 lines of code.
type readAhead struct {
    objectKey  string
    data       []byte  // last range-read result (e.g., 4-16 MiB ahead of consumer)
    startOff   int64
    endOff     int64
}
```

This is NOT a full file-based cache with LRU eviction and disk management.
It's a small in-memory buffer per active S3 consumer that requests more
data than the consumer asked for (e.g., 16 MiB when the consumer requests
1 MiB). The next ~15 sequential fetches hit the buffer at ~1ms instead of
~50ms. The buffer is allocated on demand when a partition falls through to
S3, and released when the consumer catches up to the WAL/ring buffer range.

Estimated memory: shared pool of 64 buffers × 4 MiB = 256 MiB. This
covers 64 concurrent S3 consumers. Buffers are LRU-evicted when the pool
is full.

**Why not a file-based fetch cache:** A full file-based cache adds disk
budget management, LRU eviction, and cache invalidation complexity (stale
entries when compaction rewrites objects). Under disaster recovery — when
all consumers start cold simultaneously — the cache churns heavily and
provides little benefit. The batch index footer makes full-object caching
unnecessary: reads are already proportional to consumption. The read-ahead
buffer captures the remaining latency optimization at a fraction of the
complexity.

---

## Configuration

| Config | Default | Description |
|--------|---------|-------------|
| `s3.bucket` | (required) | S3 bucket name |
| `s3.region` | (required) | S3 region |
| `s3.endpoint` | (empty) | Custom S3 endpoint (MinIO, LocalStack) |
| `s3.prefix` | `klite/<clusterID>` | S3 key prefix |
| `--s3-flush-interval` | 10m | Unified S3 sync interval (also runs on graceful shutdown) |
| `s3.upload.concurrency` | 4 | Parallel S3 PUT calls |
| `s3.read.concurrency` | 64 | Parallel S3 GET calls (semaphore) |

---

## Testing with LocalStack

Integration tests use LocalStack for S3.

## When Stuck

These references are for understanding correct behavior and gathering context,
not for copying patterns. Find the optimal solution for our architecture.

1. Read `research/warpstream.md` for WarpStream's zero-disk S3 architecture
   (context for key naming and read path design).
2. Read `research/automq.md` for AutoMQ's S3 flush pipeline (batched uploads,
   retry logic).
3. For S3 key naming and ListObjectsV2 patterns, search `.cache/repos/automq/s3stream/`
   for S3 client usage.
4. For LocalStack test setup, check testcontainers documentation via web search.
5. See `00b-testing.md` "When Stuck: Escalation Path" for additional tools.

---

## Acceptance Criteria

- `TestS3FlushBasic` -- produce, trigger flush, verify per-partition S3 object exists
- `TestS3FlushFooter` -- flush, download raw object, verify footer magic, entry count, and batch entries match actual batch boundaries in the data section
- `TestS3ReadAfterWALTrim` -- produce, flush to S3, trim WAL, fetch from S3
- `TestS3ReadCascade` -- recent data from ring buffer, WAL data from WAL, old data from S3
- `TestS3RangeRead` -- flush, fetch a specific offset mid-object, verify only the needed byte range was requested (mock S3 client tracks Range headers)
- `TestS3FooterCache` -- fetch twice from the same object, verify footer is only downloaded once (second fetch reuses cached footer)
- `TestS3FooterCorrupted` -- upload an object with truncated/invalid footer (bad magic), verify fetch returns an error rather than silently serving corrupt data
- `TestS3FlushRetry` -- simulate S3 failure, verify retry + eventual success
- `TestS3PerPartitionKeys` -- produce to 3 partitions, flush, verify separate objects
- `TestS3KeyLookup` -- verify ListObjects-based offset lookup finds correct object
- `TestS3Restart` -- flush to S3, restart broker, verify S3 data readable
- `TestS3UnifiedSync` -- verify --s3-flush-interval flushes all partitions + metadata.log
- `TestS3SyncConsistency` -- produce, wait for sync, delete data dir, restart, verify match
- `TestS3DisasterRecoveryWithBackup` -- flush, delete data dir, restart with backup, verify
- `TestS3DisasterRecoveryWithoutBackup` -- flush, delete data dir + backup, restart, verify inferred
- `TestS3GracefulShutdownFlush` -- produce data, SIGTERM, verify unified sync runs and all data flushed
- `TestWALReadTier` -- produce data, evict from ring buffer, verify fetch reads from WAL
- `TestWALReadAfterRestart` -- produce, restart broker, verify WAL data readable via rebuilt index

### Verify

```bash
# This work unit (requires LocalStack):
go test ./test/integration/ -run 'TestS3' -v -race -count=5
go test ./test/integration/ -run 'TestWALReadTier|TestWALReadAfterRestart' -v -race -count=5

# Regression check (all prior work units):
go test ./... -race -count=5
```

### Commit

After tests pass, stage and commit all changes:

```bash
git add -A
git commit -m "18: S3 storage — flush pipeline, per-partition objects, batch index footer, range reads, disaster recovery"
```
