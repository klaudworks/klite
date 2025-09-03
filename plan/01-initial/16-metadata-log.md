# Metadata Log

All non-record metadata is stored in a single append-only binary log using the
same entry framing as the WAL (see `15-wal-format-and-writer.md` "Entry
Framing"). Topics, configs, consumer group committed offsets, and producer ID
counters are persisted here.

This is Phase 3. Prerequisite: `15-wal-format-and-writer.md` complete.

Read before starting: `15-wal-format-and-writer.md` (entry framing, recovery
scan function), `12-group-coordinator.md` (CommittedOffset — stored in
metadata.log via OFFSET_COMMIT entries).

---

## Design

```
<data-dir>/metadata.log
```

**Why a log instead of JSON?** Consumer group offset commits can be frequent
(20+/sec with 100 partitions committing every 5s). Rewriting a full JSON file
on every commit is wasteful. Appending a ~50-byte entry to a log is O(1).
Additionally, a single log avoids consistency windows between separate files
(a crash between updating a JSON file and WAL fsync could leave them
inconsistent).

---

## Entry Types

Each entry uses `FramedLog` framing (4-byte length + 4-byte CRC), then a
typed payload:

```
[1 byte]   entry_type
[N bytes]  type-specific payload
```

| Type | Code | Payload | When Appended |
|------|------|---------|---------------|
| CREATE_TOPIC | 0x01 | topic name, partition count, topic ID, configs | CreateTopics handler |
| DELETE_TOPIC | 0x02 | topic name | DeleteTopics handler |
| ALTER_CONFIG | 0x03 | topic name, config key, config value | AlterConfigs handler |
| OFFSET_COMMIT | 0x04 | group, topic, partition, offset, metadata | OffsetCommit handler |
| PRODUCER_ID | 0x05 | next producer ID counter | InitProducerId handler (Phase 4) |
| LOG_START_OFFSET | 0x06 | topic name, partition, logStartOffset | `advanceLogStartOffset` (DeleteRecords, retention) |

Note: S3 object routing is NOT stored in `metadata.log`. S3 objects are
self-describing via per-partition key naming (see `18-s3-storage.md`).

**LOG_START_OFFSET persistence:** Without this entry type, advancing
`logStartOffset` (via DeleteRecords or retention enforcement) is lost on
restart. The crash recovery path would infer `logStartOffset` from the
lowest offset in the WAL, but if WAL segments have been deleted (because
their offsets are below the old `logStartOffset`), and S3 objects have not
yet been cleaned up, the deleted data would reappear after restart. The
LOG_START_OFFSET entry makes the advancement durable. On startup replay,
the highest LOG_START_OFFSET seen per partition is applied before WAL
replay (WAL replay can only advance `logStartOffset` further, never reduce
it).

**Fsync policy for LOG_START_OFFSET:** LOG_START_OFFSET entries must be
written with a synchronous `fdatasync()` before returning to the caller.
This is because the caller (`advanceLogStartOffset`) proceeds to delete
in-memory batches and WAL segments after the append — if the entry is only
buffered and the broker crashes, the deleted data reappears. This is
different from OFFSET_COMMIT entries, which are frequent (20+/sec) and
tolerate buffered writes — losing a few committed offsets on crash just
means consumers re-commit. The metadata log writer exposes two methods:
`Append(entry)` (buffered) and `AppendSync(entry)` (synchronous fsync).
LOG_START_OFFSET and CREATE_TOPIC use `AppendSync`; OFFSET_COMMIT uses
`Append`. See `21-retention.md` "metadata.log Fsync Semantics" for details.

---

## Startup Replay

On startup, replay `metadata.log` from the beginning to rebuild in-memory
state (topics, configs, offsets). Then replay WAL to rebuild HW, populate
ring buffer, and rebuild the WAL index (see `17-crash-recovery.md`).

```
Startup sequence:
  1. Load meta.properties (cluster ID)
  2. Replay metadata.log -> topics, configs, committed offsets, producer ID
     counter, per-partition logStartOffset
  3. Compact metadata.log if file size > 64 MiB (live state is in memory, cheap)
  4. Replay WAL -> per-partition HW, ring buffer, WAL index (skip WAL entries
     below logStartOffset from step 2)
  5. Start accepting connections
```

---

## Compaction

The log grows with every metadata change. Compact by writing a snapshot of
the current live state as a fresh log:

1. Acquire metadata write lock (blocks incoming metadata writes briefly)
2. Write `metadata.log.tmp` -- one entry per live topic, one per committed
   offset, one per producer ID counter, one LOG_START_OFFSET per partition
   with logStartOffset > 0 (skip superseded entries)
3. fsync `metadata.log.tmp`
4. rename `metadata.log.tmp` -> `metadata.log`
5. fsync directory
6. Release metadata write lock

**Trigger: file size > 64 MiB.** Checked in two places:

- **After startup replay** (step 3 of startup sequence): live state is already
  in memory, compact before accepting connections.
- **During runtime**: a background check after every N metadata writes (e.g.,
  every 1000 appends) compares file size to the 64 MiB threshold.

No compaction on shutdown -- the process may be killed before it completes.

**Blocking duration:** The metadata lock is held for the duration of the
snapshot write (~1 MB live state at SSD speed = <1ms). Negligible in practice.

---

## Unified S3 Sync Cycle (Phase 4)

When S3 is configured, a unified sync cycle runs periodically to ensure both
record data and metadata are backed up to S3 in a consistent state:

```
Every --s3-flush-interval (default 10m), and on graceful shutdown:
  1. Flush all partitions with unflushed data to S3 (per-partition objects)
  2. Wait for all S3 PUTs to complete
  3. Delete flushed WAL segments
  4. Compact metadata.log if needed (>64 MiB)
  5. Upload metadata.log to s3://<prefix>/metadata.log
```

See `18-s3-storage.md` for the full S3 design.

---

## Size Estimate

Typical entry sizes: CREATE_TOPIC ~60 bytes, OFFSET_COMMIT ~50 bytes.
Growth rate under heavy load: 50 consumer groups x 100 partitions / 5s
commit interval = 1000 offset commits/sec x 50 bytes = ~50 KB/sec =
180 MB/hour. The 64 MiB threshold is reached in ~21 minutes.

After compaction, a deployment with 100 topics, 50 consumer groups (100
partitions each): ~100 + 5000 = ~5.1K entries, ~300 KB. Replay at 2 GB/s
takes <1ms.

---

## When Stuck

These references are for understanding correct behavior and gathering context,
not for copying patterns. Find the optimal solution for our architecture.

1. Read `research/automq.md` for WAL and metadata storage design context.
2. For framed log format and CRC validation, refer to the `FramedLog` abstraction
   in `15-wal-format-and-writer.md` -- metadata.log reuses the same framing.
3. For compaction safety (fsync + rename pattern), check how AutoMQ handles
   metadata snapshots in `.cache/repos/automq/`.
4. For offset commit frequency estimates, see the "Size Estimate" section above.
5. See `00b-testing.md` "When Stuck: Escalation Path" for additional tools.

---

## Acceptance Criteria

- `metadata.log` entries are written for CREATE_TOPIC, DELETE_TOPIC,
  ALTER_CONFIG, OFFSET_COMMIT, LOG_START_OFFSET
- Startup replay correctly rebuilds topics, configs, committed offsets,
  per-partition logStartOffset
- Compaction produces a valid snapshot with no superseded entries
- Compaction uses fsync + rename for crash safety
- `TestWALTopicConfigSurvivesRestart` -- create topic with custom config,
  restart, describe
- `TestWALCommittedOffsetsSurviveRestart` -- commit offsets, restart,
  fetch offsets

### Verify

```bash
# This work unit:
go test ./internal/metadata/ -v -race -count=5
go test ./test/integration/ -run 'TestWALTopicConfig|TestWALCommittedOffsets' -v -race -count=5

# Regression check (all prior work units):
go test ./... -race -count=5
```

### Commit

After tests pass, stage and commit all changes:

```bash
git add -A
git commit -m "16: metadata.log — topic/config/offset persistence, compaction, startup replay"
```
