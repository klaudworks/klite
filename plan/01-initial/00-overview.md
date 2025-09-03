# klite: Overview

## Vision

Single Go binary. Single broker. Kafka-compatible. WAL + S3 storage.

klite is a minimal Kafka broker that speaks the real Kafka wire protocol.
It targets small-to-medium deployments where running a full Kafka cluster is
overkill. Clients connect with their existing Kafka libraries and everything
works.

## Performance Goal

The broker must be **bottlenecked only by hardware**, not by software overhead.
On a machine with an NVMe SSD, klite should saturate the disk before it
saturates the CPU or GC. Concretely:

| Scenario | Target | Bottleneck |
|----------|--------|------------|
| Produce, batched fsync (2ms window) | 200K-500K msgs/sec (1KB msgs) | SSD fsync latency |
| Produce, acks=1 (no fsync) | 500K-1M msgs/sec | CPU (protocol codec) |
| Fetch, tail read (page cache) | 1M+ msgs/sec | Memory bandwidth |
| Fetch, cold read (SSD) | ~2.7M msgs/sec | SSD sequential read |
| Produce latency, P50 | <2ms | Fsync batch window |
| Produce latency, P99 | <5ms | Fsync + scheduling |
| Produce latency, P99.9 | <20ms | GC pause (Go) |

These numbers assume 1KB messages, PCIe 4.0 NVMe (~3 GB/s sequential), 4+ cores.

### Design Principles for Performance

1. **Zero-allocation hot path.** Produce and Fetch must allocate near-zero on
   the per-message path. Use buffer pools (`sync.Pool`), pre-allocated slices,
   and in-place mutations. GC pressure is the primary enemy of tail latency.
2. **Batched fsync.** Never fsync per-request. Accumulate writes over a
   configurable window (default 2ms), fsync once, ACK all waiters.
3. **Pass-through bytes.** The broker never decodes individual records.
   RecordBatches are opaque byte slices: read header, assign offset, store,
   serve. No deserialization, no re-serialization.
4. **Buffered I/O.** All TCP reads/writes go through `bufio.Reader`/`Writer`
   to minimize syscall overhead. Response bytes are assembled into a contiguous
   buffer before a single `write()` syscall.
5. **OS page cache as read cache.** No application-level cache. Recent WAL data
   stays in the OS page cache naturally. Fetch reads via `mmap` or `pread` let
   the kernel manage caching.
6. **Lock-free read path.** Immutable WAL segments can be read concurrently
   without coordination. Only the write path (offset assignment + WAL append)
   goes through the serialized cluster goroutine.

---

## Non-Goals

- Multi-broker clustering / replication
- KRaft controller quorum / ZooKeeper
- Inter-broker APIs (LeaderAndIsr, StopReplica, UpdateMetadata, etc.)
- Kafka Connect / Kafka Streams server-side
- Schema Registry
- Tiered storage APIs (ListOffsets -4/-5)
- Share groups (KIP-932)
- Delegation tokens
- Message format v0/v1 (only v2/RecordBatch)
- Quota enforcement (store but don't throttle)

## Compatibility Claim

We aim for **drop-in compatibility** with standard Kafka clients for the APIs we
support. Any franz-go, librdkafka, or Java Kafka client should be able to
connect, produce, consume, and use consumer groups without modification beyond
changing the broker address.

**Target compatibility: Kafka 4.0+.** We follow Kafka 4.0's version floors
(KIP-896), dropping pre-RecordBatch and ZooKeeper-era protocol versions. Only
message format v2 (RecordBatch) is supported.

### Supported APIs

39 API keys across 6 phases. Each plan file specifies the exact API keys and
version ranges it implements. See `03-tcp-and-wire.md` for version floor
rationale and the list of explicitly unsupported API keys.

### Supported Topic Configs

Set via CreateTopics or IncrementalAlterConfigs:

| Config | Default | Notes |
|--------|---------|-------|
| `cleanup.policy` | `delete` | `delete` only until Phase 6; `compact` and `compact,delete` added in Phase 6 |
| `compression.type` | `producer` | `producer`, `none`, `gzip`, `snappy`, `lz4`, `zstd` |
| `retention.ms` | 604800000 (7d) | -1 for infinite |
| `retention.bytes` | -1 (infinite) | Per-partition |
| `max.message.bytes` | 1048588 | Max batch size |
| `segment.bytes` | 67108864 (64M) | Accepted for compatibility but not used for segmentation; S3 flush is time-based (`--s3-flush-interval`) |
| `message.timestamp.type` | `CreateTime` | `CreateTime` or `LogAppendTime` |

---

## Execution Order

Implementation proceeds in phases. Each phase has a "done" gate: tests that
must pass before moving on. Tests come first — write them, watch them fail,
implement until they pass.

| Phase | Focus | Plan files | Gate |
|-------|-------|------------|------|
| 1 | Wire protocol + core data path | `02` through `10` | Phase 1 integration tests |
| 2 | Consumer groups | `11`, `12`, `13` | Group + offset tests |
| 3 | Admin APIs + WAL persistence | `14`, `15`, `16`, `17` | Admin + crash recovery tests |
| 4 | Transactions + S3 | `18`, `19` | Transaction + S3 read path tests |
| 5 | SASL authentication | `20` | SASL smoke tests |
| 6 | Retention + compaction | `21`, `22` | Retention + compaction tests |

General references (read by every agent): `00b-testing.md`, `01-foundations.md`.

---

## Plan Files

| File | Contents |
|------|----------|
| `00-overview.md` | This file. Vision, scope, compatibility, execution order. |
| `00b-testing.md` | General reference: test infrastructure, three-tier strategy, adapting kfake tests, debugging guidance. |
| `01-foundations.md` | General reference: project structure, config, lifecycle, logging, error handling, performance, concurrency model. |
| `02-project-setup.md` | Work unit: scaffold repo, go mod init, main.go, config, broker lifecycle, meta.properties. |
| `03-tcp-and-wire.md` | Work unit: TCP listener, connection goroutines, wire framing, request dispatch, pipelining, response ordering. |
| `04-api-versions.md` | Work unit: ApiVersions handler (first handler, connectivity milestone). |
| `05-metadata.md` | Work unit: Metadata handler, auto-create, topic IDs, cluster info. |
| `06-partition-data-model.md` | Work unit: partData struct, storedBatch, parseBatchHeader, offset assignment. |
| `07-create-topics.md` | Work unit: CreateTopics handler, name validation, configs. |
| `08-produce.md` | Work unit: Produce handler, acks, LogAppendTime, validation. |
| `09-fetch.md` | Work unit: Fetch handler, long-polling, size limits, KIP-74, fetch sessions. |
| `10-list-offsets.md` | Work unit: ListOffsets handler, timestamp lookups. |
| `11-find-coordinator.md` | Work unit: FindCoordinator handler. |
| `12-group-coordinator.md` | Work unit: Group state machine, JoinGroup, SyncGroup, Heartbeat, LeaveGroup. |
| `13-offset-management.md` | Work unit: OffsetCommit, OffsetFetch, OffsetDelete, simple/admin commits. |
| `14-admin-apis.md` | Work unit: DeleteTopics, CreatePartitions, DescribeConfigs, AlterConfigs, IncrementalAlterConfigs, DescribeGroups, ListGroups, DeleteGroups, DeleteRecords, DescribeCluster, DescribeLogDirs, OffsetForLeaderEpoch. |
| `15-wal-format-and-writer.md` | Work unit: WAL entry format, segment files, writer goroutine, fsync batching, ring buffer, memory budget, partData evolution, read cascade. |
| `16-metadata-log.md` | Work unit: metadata.log format, entry types, compaction, framed log abstraction. |
| `17-crash-recovery.md` | Work unit: startup replay, authority rules, RPO guarantees. |
| `18-s3-storage.md` | Work unit: S3 object format, key naming, flush pipeline, unified sync, read path, disaster recovery. |
| `19-transactions.md` | Work unit: InitProducerID, idempotency, transactional produce, EndTxn, DescribeProducers, DescribeTransactions, ListTransactions, control batches. |
| `20-sasl-authentication.md` | Work unit: SASLHandshake, SASLAuthenticate, PLAIN + SCRAM-SHA-256/512, AlterUserScramCredentials, DescribeUserScramCredentials. |
| `21-retention.md` | Work unit: Background retention enforcement for retention.ms and retention.bytes. |
| `22-log-compaction.md` | Work unit: Log compaction engine for cleanup.policy=compact topics. |
