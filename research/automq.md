# AutoMQ

Reference repo: `.cache/repos/automq/`

AutoMQ is a direct fork of Apache Kafka 3.9 that replaces the storage layer
with S3-backed object storage. The entire Kafka compute layer (protocol handling,
group coordination, transactions, admin APIs) is kept intact.

---

## Storage Architecture

### Overview

AutoMQ keeps Kafka's protocol stack verbatim. The change is below the log layer:
instead of local segment files, data goes through a WAL then to S3.

```
Kafka Client <-> Kafka Protocol Layer (unchanged)
                        |
                   S3Stream Module (new)
                        |
                 +------+------+
                 |             |
            WAL             S3 Objects
          (fast writes)   (long-term storage)
```

Key module: `s3stream/src/main/java/com/automq/stream/s3/`

### WAL Evolution: EBS Block Device -> S3

AutoMQ's WAL has gone through three generations:

**Gen 1 (2023-2024): EBS block-device WAL with Direct I/O.** This was a
`BlockWALService` that wrote directly to a raw EBS volume (e.g., 10GB GP3) using
Linux `O_DIRECT` to bypass page cache. Used a `SlidingWindowService` for
offset management. The DirectIO code is vendored at
`s3stream/.../thirdparty/moe/cnkirito/kdio/` (DirectIOLib, DirectChannel, etc.)
and utility functions remain at `s3stream/.../s3/wal/util/WALUtil.java`
(isBlockDevice, getBlockDeviceCapacity). EBS multi-attach enabled failover by
remounting volumes to other brokers. Write latency: ~single-digit ms (EBS
fsync). This was the architecture described in AutoMQ's "How do we run Kafka
100% on object storage" blog post.

**Gen 2 (2024): Dual WAL support.** The `DefaultWalFactory` dispatched on the
WAL URI protocol -- `file://` for local block device, `s3://` for S3-backed.
Config `s3.wal.path` accepted both: a bare path (auto-wrapped as
`file://<path>?capacity=...&iops=...&iodepth=...`) or an S3 URI.

**Gen 3 (current, 2025): S3-only WAL.** The `BlockWALService` class has been
completely removed from the codebase. The `DefaultWalFactory.build()` switch
statement now has only `case "S3":` (with a `//noinspection
SwitchStatementWithTooFewBranches` comment -- a tombstone of the removed case).
The DirectIO library is still vendored but imported by **nothing** in the
production code. The `file://` protocol handler in `ObjectStorageFactory` maps
to `LocalFileObjectStorage` (a local filesystem implementing the `ObjectStorage`
interface -- used for testing/dev, not a Direct I/O WAL). The two
`WriteAheadLog` implementations that remain are:

- **`ObjectWALService`** (production) -- S3-backed WAL
- **`MemoryWriteAheadLog`** (testing only)

Deprecated config references confirm this transition:
- `S3_WAL_CAPACITY_DOC`: "[DEPRECATED] please use s3.wal.path"
- `S3_WAL_THREAD_DOC`: "[DEPRECATED] please use s3.wal.path"
- `S3_WAL_IOPS_DOC`: "[DEPRECATED] please use s3.wal.path"
- Current `S3_WAL_PATH_DOC`: "The format is '0@s3://$bucket?region=$region...'"

The `genWALConfig()` method in `AutoMQConfig.java:388` still has backward-compat
code that wraps bare paths as `file://` URIs, but `DefaultWalFactory` would throw
`IllegalArgumentException("Unsupported WAL protocol: FILE")` if that path were
actually used.

See: `core/src/main/java/kafka/automq/AutoMQConfig.java` (lines 69-70, 388-401)
See: `core/src/main/scala/kafka/log/stream/s3/wal/DefaultWalFactory.java`

### Why They Removed the On-Disk WAL

AutoMQ moved to S3-only WAL for operational simplicity in cloud environments:
- Eliminates EBS volume management (provisioning, multi-attach, failover)
- Works identically across all cloud providers (no EBS dependency)
- S3 Express One Zone provides ~single-digit-ms latency for WAL PUTs on AWS
- Failover is trivial: any node can read any other node's WAL objects
- Trade-off: higher baseline write latency (~250ms batch interval with
  standard S3) vs ~1-5ms with EBS Direct I/O

---

## S3 Object Format (Permanent Objects)

Three sections: data blocks, index block, footer.

Each **data block** holds records from one stream. Header: magic `0x5A` (1B),
flag `0x02` (1B), recordCount (4B), dataLength (4B) = 10 bytes. Body:
concatenated `StreamRecordBatch` entries.

See: `ObjectWriter.DataBlock` inner class at `s3stream/.../s3/ObjectWriter.java:283`

**Index block**: array of 36-byte entries per `DataBlockIndex`:
- streamId (8B)
- startOffset (8B)
- endOffsetDelta (4B)
- recordCount (4B)
- blockPosition (8B) -- byte offset of data block in object
- blockSize (4B)

Ordered by `(streamId, startOffset)` for binary search.

See: `s3stream/.../s3/DataBlockIndex.java` (constant: `BLOCK_INDEX_SIZE = 36`)

**Footer**: 48 bytes.
- indexStartPosition (8B)
- indexBlockLength (4B)
- reserved zeros (28B)
- magic `0x88e241b785f4cff7` (8B)

See: `ObjectWriter.Footer` inner class at `s3stream/.../s3/ObjectWriter.java:322`

Key classes:
- `s3stream/.../s3/ObjectWriter.java` -- `DefaultObjectWriter` builds objects
- `s3stream/.../s3/ObjectReader.java` -- `DefaultObjectReader` reads with
  index-based lookup
- `s3stream/.../s3/objects/ObjectAttributes.java` -- object type encoding

### Three Object Types

| Type | Storage | Description |
|---|---|---|
| STREAM_SET | Normal S3 object | Multi-stream, output of WAL flush |
| STREAM | Normal S3 object | Single-stream, output of compaction or large-stream flush |
| COMPOSITE | Metadata-only S3 object | Soft-links to other Normal objects (zero-copy merge) |

COMPOSITE objects contain no data blocks. They store an objects block (magic
`0x52`, array of objectId+blockStartIdx+bucketId entries at 14 bytes each)
plus an index block that references positions in the linked Normal objects.
Footer magic: `0x88e241b785f4cff8`.

See:
- `s3stream/.../s3/CompositeObject.java` -- format constants
- `s3stream/.../s3/CompositeObjectWriter.java`
- `s3stream/.../s3/CompositeObjectReader.java`

### S3 Object Key Format

`{reversed_hex(objectId)}/{namespace}/{objectId}` -- reversed hex prefix for
even S3 shard distribution. Example: objectId=42 -> `a2000000/DEFAULT/42`.

See: `s3stream/.../s3/metadata/ObjectUtils.java`

---

## WAL Implementation Detail (ObjectWALService)

### Architecture

`ObjectWALService` delegates to two components:
- **`DefaultWriter`** -- batches records and uploads WAL objects to S3
- **`DefaultReader`** -- reads WAL objects for recovery

See: `s3stream/.../s3/wal/impl/object/ObjectWALService.java`

### WAL Record Format

Each record in a WAL object has a 24-byte header (`RecordHeader`):

```
[4 bytes]  magicCode      (0x87654321 for data, 0x76543210 for padding)
[4 bytes]  bodyLength      (int32)
[8 bytes]  bodyOffset      (int64, absolute offset within WAL object)
[4 bytes]  bodyCRC         (CRC32 of body)
[4 bytes]  headerCRC       (CRC32 of preceding 20 bytes)
```

See: `s3stream/.../s3/wal/common/RecordHeader.java`

### WAL Object Format

Each S3 WAL object has this layout:

```
[WALObjectHeader]    48 bytes (V1)
[Record 0]           RecordHeader (24B) + StreamRecordBatch body
[Record 1]           RecordHeader (24B) + StreamRecordBatch body
...
[Record N]           RecordHeader (24B) + StreamRecordBatch body
```

WAL Object Header V1 (48 bytes, magic `0xEDCBA987`):

```
[4 bytes]  magicCode       (0xEDCBA987)
[8 bytes]  startOffset     (first record offset in this object)
[8 bytes]  bodyLength      (total data length excluding header)
[8 bytes]  stickyRecordLen (deprecated, always 0)
[4 bytes]  nodeId          (broker node ID)
[8 bytes]  epoch           (node epoch for fencing)
[8 bytes]  trimOffset      (WAL trim watermark for GC)
```

V0 header (40 bytes, magic `0x12345678`) is identical but without trimOffset.

See: `s3stream/.../s3/wal/impl/object/WALObjectHeader.java`

### WAL Object Key Format

```
{md5hex(nodeId)}/{namespace}{clusterId}/{nodeId}[_{type}]/{epoch}/wal/{startOffset}-{endOffset}
```

Example: `A1B2.../DEFAULTmycluster/0/42/wal/0-67108864`

V0 format used just `{startOffset}` without the end offset.

See: `s3stream/.../s3/wal/impl/object/ObjectUtils.java`

### DefaultWriter: Batching and Upload

The `DefaultWriter` accumulates records into **Bulks** (batches):

1. `append()` adds a record to the active `Bulk`
2. Each Bulk has a scheduled force-upload timer (default `batchInterval` = 250ms)
3. A Bulk is sealed early if:
   - It exceeds `maxBytesInBatch` (8 MiB), or
   - Adding the record would overflow `DATA_FILE_ALIGN_SIZE` (64 MiB)
4. Sealed Bulks enter `waitingUploadBulks` queue
5. Up to `maxInflightUploadCount` (50) Bulks upload concurrently on
   `S3_WAL_UPLOAD` thread pool (sized to CPU cores)
6. Records within a Bulk are **sorted by (streamId, baseOffset)** before
   serialization to improve read locality
7. After successful S3 PUT, a **fencing check** runs via `ReservationService`
   to verify this node still holds the lease
8. Only after fencing check passes are callers' futures completed (ACK)
9. Back-pressure: `OverCapacityException` thrown if `bufferedDataBytes` exceeds
   `maxUnflushedBytes` (1 GiB)

Key detail: `nextOffset` tracks a virtual WAL offset. Each Bulk's end offset is
ceil-aligned to `DATA_FILE_ALIGN_SIZE` (64 MiB), so WAL offsets jump in 64 MiB
increments. This means WAL offsets are NOT stream offsets -- they're byte
positions within a virtual linear WAL address space.

See: `s3stream/.../s3/wal/impl/object/DefaultWriter.java`

### WAL Configuration Defaults

| Config | Default | Description |
|---|---|---|
| `batchInterval` | 250ms | Max time to accumulate records before upload |
| `maxBytesInBatch` | 8 MiB | Force upload when Bulk reaches this size |
| `maxUnflushedBytes` | 1 GiB | Back-pressure threshold |
| `maxInflightUploadCount` | 50 | Max concurrent S3 WAL PUTs |
| `DATA_FILE_ALIGN_SIZE` | 64 MiB | WAL offset alignment boundary |
| `readAheadObjectCount` | 4 | Objects to prefetch during recovery |

See: `s3stream/.../s3/wal/impl/object/ObjectWALConfig.java`

### WAL Trim

Not a circular buffer. Trim works by deleting S3 WAL objects:

1. `trim(recordOffset)` is called after permanent S3 objects are committed
2. A fake record (streamId=-1) is appended to persist the trim offset in
   the next WAL object's header (`trimOffset` field)
3. WAL objects whose last record offset <= trim offset are deleted
4. The last WAL object is never deleted (prevents offset reset to zero
   on restart)
5. A safety re-delete is scheduled 10 seconds later to handle fast-retry
   object leaks

See: `DefaultWriter.trim0()` at `s3stream/.../s3/wal/impl/object/DefaultWriter.java:468`

---

## S3Storage: The Main Storage Engine

`S3Storage` orchestrates the full write and read path. It manages the WAL,
in-memory cache, and permanent S3 object lifecycle.

See: `s3stream/.../s3/S3Storage.java`

### Write Path (Full Detail)

```
S3Stream.append(RecordBatch)
  1. Assign stream offset: nextOffset.getAndAdd(recordBatch.count())
     (S3Stream.java:218 -- per-stream AtomicLong, monotonic)
  2. Wrap as StreamRecordBatch (streamId, epoch, baseOffset, count, payload)
  3. Call S3Storage.append(context, streamRecordBatch)

S3Storage.append(streamRecordBatch)
  4. Back-pressure check: if deltaWALCache.size() >= capacity, backoff
     (request queued, retried every 100ms)
  5. deltaWAL.append(streamRecordBatch) -- S3 WAL PUT (batched)
     Throws OverCapacityException if WAL is over capacity -> force upload

  6. On WAL append complete (S3 PUT succeeded):
     a. confirmWAL.onAppend(record, recordOffset, nextOffset)
     b. handleAppendCallback(request):
        - deltaWALCache.put(record)  -- add to in-memory LogCache
        - deltaWALCache.setLastRecordOffset(request.offset)
        - If cache block is full -> trigger uploadDeltaWAL()
     c. Complete caller's future (ACK)

  Write latency = WAL batchInterval (~250ms) + S3 PUT latency
```

### Permanent S3 Object Upload (uploadDeltaWAL)

Triggered when:
- LogCache active block is full (size >= `walUploadThreshold`)
- Periodic timer (`walUploadIntervalMs`, default 60s)
- Force upload on stream close / broker shutdown

```
uploadDeltaWAL flow:
  1. Archive current LogCacheBlock from deltaWALCache
  2. Create DefaultUploadWriteAheadLogTask with records grouped by streamId
  3. prepare(): obtain objectId from ObjectManager (KRaft)
  4. upload(): For each stream:
     - If stream size >= streamSplitSizeThreshold -> separate STREAM object
     - Else -> aggregated into shared STREAM_SET object
     - ObjectWriter builds: data blocks + index + footer
     - Rate-limited upload to S3
  5. commit(): CommitStreamSetObject to ObjectManager (KRaft metadata)
  6. On commit success: trim WAL (delete old WAL S3 objects)
  7. Mark LogCacheBlock as free (stays readable until evicted)
```

Pipeline ordering: prepare and upload can overlap across tasks, but commits
are strictly ordered (FIFO queues: `walPrepareQueue`, `walCommitQueue`).

Upload concurrency: 4 threads (`s3-storage-upload-wal`).

If commit fails: `Runtime.getRuntime().halt(1)` -- hard crash. This is
intentional: a failed KRaft commit means data loss is possible if the WAL
is trimmed, so AutoMQ chooses to crash rather than risk inconsistency.

See: `s3stream/.../s3/DefaultUploadWriteAheadLogTask.java`

### Read Path (Full Detail)

```
S3Storage.read0(context, streamId, startOffset, endOffset, maxBytes)
  Tier 1: deltaWALCache (LogCache) -- in-memory
    - Binary search across LogCacheBlocks for (streamId, startOffset)
    - If data starts at or before startOffset -> return immediately
    - Also checks snapshotReadCache if in snapshot-read mode

  Tier 2: blockCache (S3BlockCache) -- LRU cache of S3 data blocks
    - objectManager.getObjects() to find which S3 objects contain data
    - objectReader.find() for index lookup (binary search on DataBlockIndex)
    - S3 range read on cache miss
    - Cached by (objectId, blockIndex)

  Merge: Results from Tier 1 and Tier 2 are concatenated
    - Continuity check: records must form unbroken offset sequence
    - Tier 2 covers [startOffset, X), Tier 1 covers [X, endOffset)

  Timeout: 2 minutes (logs [POTENTIAL_BUG] on timeout)
```

Adaptive readahead (per-stream `StreamReader`): starts at 512KB, grows by
512KB per cache miss up to 32MB. Resets when an unread block is evicted
(cache pressure). 1-minute cooldown.

See:
- `s3stream/.../s3/S3Storage.java` -- `read0()` method (line 679)
- `s3stream/.../s3/cache/StreamReader.java` -- readahead logic

### In-Memory Cache (LogCache / deltaWALCache)

`LogCache` is a list of `LogCacheBlock` objects:
- **Active block**: accepts writes via `put()`
- **Archived blocks**: sealed, being uploaded or already uploaded
- **Free blocks**: upload + commit done, still readable until evicted

Eviction triggers: total blocks > 64 OR cache size > 90% of capacity.

`deltaWALCache` capacity is configured via `s3.wal.cache.size` (default: auto
based on available memory). The `walUploadThreshold` is capped at 2/5 of
cache capacity to prevent uploads from falling behind appends.

Optional `snapshotReadCache`: a second LogCache (2/3 of total cache size) for
read-only replicas. Enabled via `s3.snapshot.read.enable`.

See: `s3stream/.../s3/cache/LogCache.java`

### Recovery

On startup, `S3Storage.recover()`:

1. `streamManager.getOpeningStreams()` -- get streams with their committed
   end offsets
2. `deltaWAL.recover()` -> `RecoverIterator` that:
   - Lists all WAL S3 objects for this node (by key prefix)
   - Sorts by (epoch, startOffset)
   - Removes overlapping objects from older epochs (fencing cleanup)
   - Reads each object, parses header, iterates records
3. `recoverContinuousRecords()` filters recovered records:
   - Drops records with offset < stream's committed endOffset (already in
     permanent storage)
   - Reorders out-of-order records (bug tolerance)
   - Discards records after first gap (discontinuous = data loss)
   - Throws `IllegalStateException` if first recovered record offset doesn't
     match stream's endOffset (data loss detected)
4. Recovered records go into a new `LogCacheBlock`
5. Upload + commit the recovered block to permanent storage
6. Close and reopen streams with updated end offsets

See: `S3Storage.recoverContinuousRecords()` at line 249

---

## Compaction

Two levels run as background tasks:

**CompactionManager** (periodic):
- Force Split: old STREAM_SET objects -> individual STREAM objects per stream
- Compact: merge fragmented streams across multiple objects

**StreamObjectCompactor** (per-stream):
- MINOR: physical merge of small objects (up to 128 MiB)
- MAJOR_V1: composite object merge (zero-copy, metadata-only)
- CLEANUP_V1: rebuild composites with >512 MiB dirty data

Key classes:
- `s3stream/.../s3/compact/CompactionManager.java`
- `s3stream/.../s3/compact/CompactionAnalyzer.java`
- `s3stream/.../s3/compact/StreamObjectCompactor.java`

---

## Metadata -- KRaft Integration

AutoMQ adds custom record types to the KRaft metadata log:

| apiKey | Record | Purpose |
|---|---|---|
| 505 | S3StreamObjectRecord | Single-stream object registration |
| 506 | RemoveS3StreamObjectRecord | Stream object deletion |
| 507 | S3StreamSetObjectRecord | Multi-stream object registration |
| 509 | S3ObjectRecord | Object lifecycle (PREPARED->COMMITTED->DESTROYED) |
| 510 | RemoveS3ObjectRecord | Object deletion marker |
| 512 | AssignedS3ObjectIdRecord | Monotonic ID counter |

These replay into `MetadataImage` -> in-memory maps for efficient lookups.

Key packages:
- KRaft records: `metadata/src/main/resources/common/metadata/`
- Image: `metadata/src/main/java/org/apache/kafka/image/`
- Object manager: `s3stream/.../s3/metadata/`

### Fencing

S3-based leader election via reservation objects (21 bytes). Checked after
every WAL batch upload to detect fencing. If fencing detected:
- `DefaultWriter.fenced = true`
- All pending appends fail with `WALFencedException`
- No more writes accepted

See: `s3stream/.../s3/wal/impl/object/ObjectReservationService.java`

---

## Streams vs Kafka Partitions

AutoMQ's storage layer uses **streams** (not Kafka partitions). Each Kafka
partition maps to one or more streams. A stream is identified by a numeric
`streamId` (int64) and has a monotonic offset counter.

`S3Stream` assigns offsets in `append0()`:
```java
long offset = nextOffset.getAndAdd(recordBatch.count());
```

This is a per-stream AtomicLong. The offset represents a logical record
count, not byte position. Each `StreamRecordBatch` carries
`(streamId, epoch, baseOffset, count, payload)`.

See: `s3stream/.../s3/S3Stream.java:218`

---

## Testing Infrastructure

### Approach: Run Kafka's Own Ducktape Tests

Since AutoMQ is a Kafka fork, they run Kafka's ducktape system tests directly.

### CI Pipeline

GitHub Actions workflow: `.github/workflows/nightly-main-e2e.yml`
Runs on schedule (5x/month) with 11 parallel E2E jobs on ARM64 Ubicloud
instances (`ubicloud-standard-8-arm-ubuntu-2204`).

| Job | Test Suite |
|---|---|
| main_e2e_{1-4} | `tests/suites/main_kos_test_suite{1-4}.yml` |
| main_e2e_5 | `tests/kafkatest/automq` (AutoMQ-specific) |
| benchmarks_e2e | `tests/kafkatest/benchmarks` |
| connect_e2e_{1-3} | `tests/suites/connect_test_suite{1-3}.yml` |
| streams_e2e | `tests/kafkatest/tests/streams` |
| automq_e2e | `tests/suites/automq_test_suite1.yml` |

### Infrastructure

- `tests/docker/ducker-ak` -- modified Kafka's ducker tool
- `tests/docker/run_tests.sh` -- orchestrator
- 14 Docker containers on `ducknet` (subnet `10.5.0.0/16`)
- LocalStack at `10.5.0.2:4566` for S3 simulation
- S3 bucket `ko3` created via `aws s3api create-bucket`
- S3 cleanup between tests: `aws s3 rm s3://ko3 --recursive`
- `--deflake 4` (retry failed tests up to 4 times)

See:
- `tests/docker/s3/docker-compose.yaml` -- LocalStack config
- `tests/kafkatest/services/kafka/templates/kafka.properties` -- S3 config
- `tests/docker/Dockerfile` -- AWS credentials (test/test)
- `tests/docker/requirements.txt` -- ducktape==0.12.0

### What They Exclude

Exclusions (from `tests/suites/main_kos_test_suite2.yml` and others):

| Category | Reason |
|---|---|
| ZooKeeper tests | KRaft only |
| Replication tests | "only one replica in KOS" |
| Upgrade/downgrade | "cannot downgrade to official kafka" |
| Log dir failure | "we do not write messages to log dir" |
| Replica lag | "no lag in KOS" |
| Message format v0/v1 | "kafka 3.x not support message format v0 and v1" |
| Unclean election/truncation | "will not happen in KOS" |
| Some network partition tests | Architectural differences |

Even as a Kafka fork with identical protocol handling, AutoMQ excludes ~30%
of ducktape tests.

### AutoMQ-Specific Protocol Extensions

11 proprietary APIs added (forwarded to KRaft controller):
CREATE_STREAMS, DELETE_STREAMS, OPEN_STREAMS, CLOSE_STREAMS, TRIM_STREAMS,
GET_OPENING_STREAMS, DESCRIBE_STREAMS, GET_KVS, PUT_KVS, DELETE_KVS,
AUTOMQ_GET_NODES.

See: `core/src/main/scala/kafka/server/KafkaApis.scala` (lines 263-273)

---

## Relevance to klite

### What We Can Learn

1. **S3 object format**: Our plan's format (`plan/01-initial/18-s3-storage.md`) is
   similar to AutoMQ's but simpler. We use topic names instead of stream IDs.
   We don't need COMPOSITE objects (single broker, no compaction initially).
   The core pattern (data blocks + index + footer) is proven.

2. **Three-tier read cascade**: deltaWALCache -> block cache -> S3 range read
   is the right pattern. We mirror this in our plan.

3. **STREAM_SET vs STREAM split decision**: AutoMQ splits large streams (>=
   `streamSplitSizeThreshold`) into their own objects. Good for read locality.
   We can adopt this.

4. **Recovery model**: Replay WAL, filter out committed records, crash on
   data loss detection. Simpler for us since we use a local filesystem WAL.

### What's Different for Us

1. **Local WAL**: We use local filesystem WAL with fsync (2ms batch window)
   vs AutoMQ's S3 WAL (250ms batch window). Our latency target is ~2ms P50,
   which is impossible with S3 WAL unless using S3 Express One Zone.

2. **No KRaft**: We use `metadata.log` (append-only FramedLog) for metadata
   instead of KRaft records. Simpler but doesn't scale beyond single broker.

3. **No fencing**: Single broker means no need for reservation/fencing.

4. **Kafka partitions directly**: We don't have the stream abstraction layer.
   Our WAL entries reference topic+partition, not stream IDs.

5. **Crash safety**: Our local WAL uses CRC per entry + truncation on
   corruption. AutoMQ's S3 WAL relies on S3 PUT atomicity (an object either
   exists completely or not at all).
