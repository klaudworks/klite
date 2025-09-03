# Apache Kafka Internals

Reference repo: `.cache/repos/kafka/`

---

## Storage Architecture

### Segment Files

Each topic-partition is a directory containing a sequence of segment files.
Segments are named by their base offset as a 20-digit zero-padded decimal.

Per-segment files:
- `.log` -- concatenated RecordBatches, no framing, no segment header
- `.index` -- sparse offset-to-position index (8 bytes/entry, memory-mapped)
- `.timeindex` -- sparse timestamp-to-offset index (12 bytes/entry, memory-mapped)
- `.txnindex` -- aborted transaction index (34 bytes/entry, sequential scan)

Key packages:
- Segment files: `storage/src/main/java/org/apache/kafka/storage/log/`
  - `LogSegment.java` -- single segment (log + indexes)
  - `LogFileUtils.java` -- filename conventions, suffix handling
  - `OffsetIndex.java` -- offset index with warm-section binary search
  - `TimeIndex.java` -- timestamp index
  - `TransactionIndex.java` -- aborted txn index
- Log management: `storage/src/main/java/org/apache/kafka/storage/log/`
  - `UnifiedLog.java` -- per-partition log (append, read, truncate, compact)
  - `LocalLog.java` -- segment management, floor-segment lookup
  - `LogManager.java` -- broker-wide log lifecycle

Segment rotation triggers (any of):
- Size > `log.segment.bytes` (default 1 GB)
- Age > `log.roll.hours` (default 7 days, jittered)
- Offset index or time index full
- Relative offset overflow (>2.1B offsets beyond base)

### RecordBatch Format (v2 / Magic=2)

61-byte header, all big-endian. This is both the on-disk AND on-wire format.

Key fields: BaseOffset (8B), Length (4B), PartitionLeaderEpoch (4B),
Magic=2 (1B), CRC-32C (4B, covers bytes 21+), Attributes (2B),
LastOffsetDelta (4B), BaseTimestamp (8B), MaxTimestamp (8B),
ProducerId (8B), ProducerEpoch (2B), BaseSequence (4B), RecordsCount (4B).

Attributes bits: 0-2 compression (0=none,1=gzip,2=snappy,3=lz4,4=zstd),
3 timestamp type, 4 transactional, 5 control batch, 6 delete horizon.

Individual records use zig-zag varint encoding with deltas relative to
batch base offset/timestamp.

Key packages:
- `clients/src/main/java/org/apache/kafka/common/record/internal/`
  - `DefaultRecordBatch.java` -- batch header constants, CRC, attributes
  - `DefaultRecord.java` -- individual record encoding
  - `EndTransactionMarker.java` -- control batch (ABORT=0, COMMIT=1)
- `clients/src/main/java/org/apache/kafka/common/record/`
  - `FileRecords.java` -- file-backed records, zero-copy sendfile
  - `MemoryRecords.java` -- in-memory record builder
  - `CompressionType.java` -- compression codecs

### Write Path

Flow: ProduceRequest -> `ReplicaManager.appendToLocalLog()` ->
`Partition.appendRecordsToLeader()` (ISR check) ->
`UnifiedLog.appendAsLeader()` (validate, assign offsets, append, update state).

Key design: writes go to OS page cache, not direct to disk. Fsync effectively
disabled by default. Durability comes from replication. One `synchronized` lock
per partition protects offset assignment and append.

Key packages:
- `server/src/main/java/org/apache/kafka/server/`
  - `ReplicaManager.java` -- broker-level produce dispatch
- `storage/src/main/java/org/apache/kafka/storage/log/`
  - `UnifiedLog.java` -- `appendAsLeader()` is the main entry point
  - `LogValidator.java` -- validates batches, assigns offsets, sets epochs

### Read Path

Flow: FetchRequest -> `UnifiedLog.read()` (select upper bound by isolation) ->
`LocalLog.read()` (floor segment lookup via ConcurrentSkipListMap) ->
`LogSegment.read()` (index lookup + linear scan) ->
`FileRecords.slice()` (zero-copy view) ->
`FileChannel.transferTo()` (`sendfile(2)` syscall for non-SSL).

Offset index uses warm-section optimized binary search: last 1024 entries
(~8KB) stay hot in page cache. Tail consumers almost always hit the warm section.

Three isolation levels: LOG_END (replicas), HIGH_WATERMARK (consumers),
TXN_COMMITTED (read_committed, bounded by LSO).

### Log Compaction

`LogCleaner` runs background threads. Selects log with highest cleanable ratio.
Phase 1: build `SkimpyOffsetMap` (MD5 hash -> last offset, 24 bytes/entry,
~5.59M entries in 128MB). Phase 2: copy records to `.cleaned` file, discarding
records with newer offsets in map. Phase 3: atomic swap.

Key packages:
- `storage/src/main/java/org/apache/kafka/storage/log/`
  - `LogCleaner.java` -- cleaner threads, log selection
  - `Cleaner.java` -- actual cleaning logic
  - `SkimpyOffsetMap.java` -- space-efficient hash table for dedup

### Consumer Group Coordinator

`__consumer_offsets` internal compacted topic (default 50 partitions).
Group -> partition via `abs(groupId.hashCode()) % numPartitions`.

Record types: OFFSET_COMMIT (key=group+topic+partition, value=offset+metadata),
GROUP_METADATA (full membership snapshot), plus KIP-848 records for the new
consumer group protocol.

Classic group state machine:
EMPTY -> PREPARING_REBALANCE -> COMPLETING_REBALANCE -> STABLE -> DEAD

KIP-848 new protocol: single `ConsumerGroupHeartbeat` API, server-side
assignment, epoch-based incremental reconciliation.

Key packages:
- `group-coordinator/src/main/java/org/apache/kafka/coordinator/group/`
  - `GroupCoordinatorShard.java` -- request dispatch
  - `classic/ClassicGroup.java` -- classic group state machine
  - `modern/ConsumerGroup.java` -- KIP-848 consumer groups
  - `GroupCoordinatorRecordHelpers.java` -- record serialization
- Record schemas: `group-coordinator/src/main/resources/common/message/`

### Replication (context -- we don't implement)

ISR state machine with two views: committed ISR (from controller) and
maximal ISR (optimistic, for HW advancement). HW = min(LEO of ISR members).
Leader epoch prevents log divergence during failover. `min.insync.replicas`
checked pre-append (NOT_ENOUGH_REPLICAS) and post-append (in purgatory).

---

## Testing Infrastructure

### Three Test Layers

1. **Unit tests (JUnit):** ~15K+ tests, run via Gradle. Test individual classes.
   Not useful for third-party conformance.

2. **Integration tests (JUnit + embedded broker):** Start `KafkaServer`
   in-process, connect with Java clients. Exercise wire protocol but deeply
   tied to Java class hierarchy.
   - `core/src/test/scala/integration/kafka/api/` -- protocol-level tests
   - `clients/src/test/java/` -- client integration tests

3. **System tests (ducktape):** ~500+ tests using Docker cluster.
   - `tests/kafkatest/tests/core/` -- produce, consume, replication
   - `tests/kafkatest/tests/client/` -- client compatibility
   - `tests/kafkatest/tests/connect/` -- Kafka Connect
   - `tests/kafkatest/tests/streams/` -- Kafka Streams

### Ducktape Architecture

Framework: [confluentinc/ducktape](https://github.com/confluentinc/ducktape) v0.12.0.

How it works:
- `tests/docker/ducker-ak` -- tool to manage Docker cluster
- `tests/docker/run_tests.sh` -- orchestrator script
- Creates 14 Docker containers on `ducknet` network
- Container 1 = test driver, containers 2-14 = cluster nodes
- Kafka source mounted at `/opt/kafka-dev/` in every container
- Test driver SSHs into nodes to start/stop brokers, run scripts

Key service class: `tests/kafkatest/services/kafka/kafka.py`
- `KafkaService.start_cmd()` -- builds broker start command (line ~854)
- `KafkaService.start_node()` -- SSH + process management
- `KafkaService.pids()` -- `ps ax | grep kafka.Kafka`
- Config template: `tests/kafkatest/services/kafka/templates/kafka.properties`
- Startup detection: regex `Kafka\s*Server.*started`

Path resolution: `tests/kafkatest/directory_layout/kafka_path.py`
- Resolves binary paths like `/opt/kafka-dev/bin/kafka-server-start.sh`
- Custom resolvers possible via `--globals '{"kafka-path-resolver": "..."}'`

### Key Test Files for Scenario Extraction

These contain the protocol-level test logic we want to translate to Go:

| File (under `tests/kafkatest/tests/`) | Scenarios |
|---|---|
| `core/produce_consume_validate.py` | Basic produce/consume roundtrip |
| `core/consumer_group_command_test.py` | Group admin commands |
| `core/transactions_test.py` | Transactional produce, consume, abort |
| `client/consumer_test.py` | Consumer group rebalance, offset management |
| `client/producer_test.py` | Producer acks, idempotency, errors |
| `core/group_mode_transactions_test.py` | Transactions + consumer groups |
| `core/fetch_from_follower_test.py` | Fetch isolation, rack-aware |

### Protocol Test Gap

There is NO standalone "protocol conformance" test suite in the Kafka project.
The closest is ducktape, but it tests the full system (broker lifecycle,
replication, rolling restarts, JMX metrics), not just the wire protocol.
This gap is why every non-Java Kafka implementation builds its own test suite.

### Key Numbers

| Constant | Value |
|---|---|
| RecordBatch header | 61 bytes |
| LOG_OVERHEAD | 12 bytes (BaseOffset + Length) |
| Offset index entry | 8 bytes |
| Time index entry | 12 bytes |
| Txn index entry | 34 bytes |
| CRC algorithm | CRC-32C (Castagnoli) |
| Duplicate detection window | Last 5 batches per producer |
| Default segment size | 1 GB |
| Default index interval | 4096 bytes |
| `__consumer_offsets` partitions | 50 |
