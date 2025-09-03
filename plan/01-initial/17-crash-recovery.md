# Crash Recovery

Startup replay of metadata.log and WAL, authority rules, and durability
guarantees. This file describes what happens when the broker restarts after
a clean or unclean shutdown.

This is Phase 3. Prerequisite: `16-metadata-log.md` complete.

Read before starting: `15-wal-format-and-writer.md` (WAL entry format, segment
files, FramedLog.Scan — recovery uses CRC-based forward scan),
`16-metadata-log.md` (entry types, replay order — metadata.log is replayed
before WAL).

---

## Startup WAL Replay

Prerequisite: `metadata.log` has already been replayed, so all topics and
partition counts are known in memory.

1. List segment files in order
2. For each segment, scan entries sequentially:
   a. Read entry_length
   b. Read entry bytes
   c. Validate CRC32C
   d. If valid: resolve the entry's topic ID to a topic name via the in-memory
      ID-to-name map (rebuilt from `metadata.log`), then look up the partition.
      If found: push batch into ring buffer, update WAL index, update partition HW.
      If not found: log a warning and skip the entry (see "Authority Rules" below).
   e. If invalid (CRC mismatch or truncated): stop scanning this segment
3. Truncate the last segment at the last valid entry
4. Rebuild data state from WAL entries:
   - Per-partition HW (from highest offset + 1 seen)
   - Per-partition LogStartOffset: use the value from `metadata.log` replay
     (LOG_START_OFFSET entries). WAL replay can only advance it further
     (if the lowest WAL offset is higher), never reduce it. Skip WAL
     entries whose last offset is below the persisted logStartOffset.
   - Ring buffer populated with recent WAL data, WAL index rebuilt

After replay, the ring buffer contains recent data and the WAL index maps
all WAL entries. The normal read cascade (ring buffer -> WAL -> S3) works
immediately when the broker starts accepting connections.

---

## Authority Rules

`metadata.log` and the WAL have distinct, non-overlapping authority:

- **`metadata.log` is authoritative for existence and boundaries**: topic
  names, partition counts, topic configs, consumer group committed offsets,
  S3 object registry, producer ID counter, and **per-partition logStartOffset**
  (via LOG_START_OFFSET entries). The WAL cannot create or delete topics.
- **WAL is authoritative for data state**: per-partition HW, record content,
  ring buffer and WAL index population. WAL replay can only advance HW (and
  logStartOffset), never reduce either.

**Conflict scenarios:**

- `metadata.log` says topic has 4 partitions, WAL has entries for partitions
  0-2 only -> Normal. Partition 3 exists with HW=0, empty.
- WAL has entries for a topic not in `metadata.log` -> Should not happen (the
  CREATE_TOPIC entry in `metadata.log` is written and fsync'd before produce
  can succeed). Log a warning, skip the entry.
- WAL has entries for a partition index beyond the topic's partition count ->
  Same handling: log warning, skip. Indicates corruption.

There is no recovery checkpoint or marker. Both files are always replayed
from scratch on startup (both are small enough that this completes in <2s).

**Crash between WAL fsync and commitBatch:** If the broker crashes after WAL
fsync (step 2 of produce flow) but before `commitBatch()` advances HW and
pushes to the ring buffer, the data is safe in the WAL. On replay, the entry
is found and applied -- HW advances, ring buffer and WAL index are populated.
The producer never received an ACK, so it retries (idempotent produce dedupes
in Phase 4; at-least-once in Phase 3). No data loss.

---

## What State Survives Restarts

| State | Source |
|-------|--------|
| Record data | WAL entries (recent), S3 objects (historical, Phase 4) |
| Topics, configs, partition counts | `metadata.log` entries |
| Per-partition HW | WAL entries (highest offset + 1) |
| Per-partition LogStartOffset | `metadata.log` LOG_START_OFFSET entries (primary), WAL lowest offset (secondary) |
| Consumer group committed offsets | `metadata.log` entries |
| Producer ID counter | `metadata.log` entries (Phase 4) |
| S3 object routing | S3 key naming (implicit, Phase 4) -- not stored locally |
| Cluster ID | `meta.properties` (one-line file, written once) |

---

## Durability Guarantees (RPO)

Recovery Point Objective depends on the failure mode:

| Failure Mode | Data Loss | Recovery |
|-------------|-----------|----------|
| Process crash | Zero (fsync'd data intact) | Replay metadata.log + WAL (~0.5-2s) |
| Single disk fail (RAID-1) | Zero | Automatic (RAID rebuild) |
| Both disks fail simultaneously | Up to `--s3-flush-interval` (records + metadata) | Disaster recovery from S3 |
| Kernel panic / power loss | Up to 1 batch (<=`wal.sync.interval`) | Replay metadata.log + WAL, truncate last bad entry |

**Key guarantees:**
- Acknowledged writes (client received ACK) are always on disk (fsync completed).
- Unacknowledged in-flight writes (current accumulation batch) may be lost on
  crash. At most `wal.sync.interval` (2ms default) of data.
- Clients with `retries` configured (default in franz-go, librdkafka, Java
  client) reconnect automatically after a broker restart. Expected blip: 1-2s.

---

## When Stuck

These references are for understanding correct behavior and gathering context,
not for copying patterns. Find the optimal solution for our architecture.

1. Read `15-wal-format-and-writer.md` for the WAL entry format and
   `FramedLog.Scan()` recovery logic.
2. Read `16-metadata-log.md` for the metadata replay order and entry types.
3. For crash safety patterns (fsync ordering, rename atomicity), search
   `.cache/repos/automq/` for recovery and replay logic.
4. For authority rule edge cases (WAL entry for unknown topic, partition
   beyond count), write a unit test that simulates the scenario and verify
   the skip-and-warn behavior.
5. See `00b-testing.md` "When Stuck: Escalation Path" for additional tools.

---

## Acceptance Criteria

- `TestWALProduceRestart` -- produce 100 records, restart broker, consume all 100
- `TestWALMultiPartitionRestart` -- produce to 3 partitions, restart, verify all
- `TestWALCrashRecovery` -- produce, corrupt last entry, restart, verify recovery
- `TestWALSegmentRotation` -- produce enough to rotate segments, verify reads
  span segments
- `TestWALEmptyStart` -- fresh start with no data dir, verify clean initialization

### Verify

```bash
# This work unit:
go test ./test/integration/ -run 'TestWALProduceRestart|TestWALMultiPartition|TestWALCrashRecovery|TestWALSegmentRotation|TestWALEmptyStart' -v -race -count=5

# Regression check (all prior work units — full Phase 3):
go test ./... -race -count=5
```

### Commit

After tests pass, stage and commit all changes:

```bash
git add -A
git commit -m "17: crash recovery — WAL replay, metadata.log replay, authority rules, durability"
```
