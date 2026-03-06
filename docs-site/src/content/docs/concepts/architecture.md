---
title: Architecture
description: How klite works -- single-binary design, WAL + S3 storage, and performance model.
---

klite is a Kafka-compatible broker designed around simplicity: one Go binary, one process, zero external dependencies. See [Design Philosophy](/concepts/design-philosophy/) for the motivation behind these choices.

## Data flow

```
                ┌─────────────┐
                │   Clients   │
                │ (any Kafka  │
                │   library)  │
                └──────┬──────┘
                       │ Kafka wire protocol
                       │ (TCP port 9092)
                       ▼
              ┌────────────────┐
              │     klite      │
              │                │
              │  ┌──────────┐  │
              │  │ Protocol │  │  Produce/Fetch/Groups/Admin
              │  │ Handlers │  │
              │  └────┬─────┘  │
              │       │        │
              │  ┌────▼─────┐  │
              │  │ Cluster  │  │  Offset assignment, topic metadata,
              │  │  State   │  │  group coordination
              │  └────┬─────┘  │
              │       │        │
              │  ┌────▼─────┐  │
              │  │   WAL    │  │  Append-only log on local disk
              │  │  Writer  │  │  Batched fsync (2ms window)
              │  └────┬─────┘  │
              └───────┼────────┘
                      │
           ┌──────────┼──────────┐
           ▼                     ▼
    ┌─────────────┐      ┌─────────────┐
    │  Local Disk │      │     S3      │
    │   (NVMe)    │      │  (durable)  │
    │             │      │             │
    │  WAL files  │ ───► │  Flushed    │
    │  metadata   │      │  segments   │
    └─────────────┘      └─────────────┘
```

## Request lifecycle

### Produce

1. Client sends a Produce request with one or more RecordBatches
2. klite validates the batch header (CRC, magic byte, size limits)
3. The cluster goroutine assigns monotonically increasing offsets
4. Batches are appended to the WAL
5. Fsync is batched: writes accumulate over a configurable window (default 2ms), then a single fsync ACKs all waiters
6. Response is sent with the assigned base offset

### Fetch

1. Client sends a Fetch request with topic-partition-offset tuples
2. klite reads from the WAL (hot data in OS page cache) or S3 (cold data)
3. Long-polling: if no data is available, the request blocks until data arrives or the timeout expires
4. Response streams RecordBatches directly -- no per-record copying

### Consumer groups

1. Consumers join a group via JoinGroup
2. The group leader receives member subscriptions and assigns partitions
3. Assignments are distributed via SyncGroup
4. Heartbeats maintain membership; missed heartbeats trigger rebalance
5. Offsets are committed and tracked per group

## Performance characteristics

klite is designed to be bottlenecked by hardware, not software:

| Scenario | Target | Bottleneck |
|----------|--------|------------|
| Produce (batched fsync, 2ms) | 200K-500K msgs/sec | SSD fsync latency |
| Produce (acks=1, no fsync) | 500K-1M msgs/sec | CPU (protocol codec) |
| Fetch (tail, page cache) | 1M+ msgs/sec | Memory bandwidth |
| Fetch (cold, SSD read) | ~2.7M msgs/sec | SSD sequential read |
| Produce latency P50 | <2ms | Fsync batch window |
| Produce latency P99 | <5ms | Fsync + scheduling |

*Assumes 1KB messages, PCIe 4.0 NVMe (~3 GB/s sequential), 4+ cores.*

### Key design decisions for performance

1. **Zero-allocation hot path** -- Produce and Fetch allocate near-zero per message. Buffer pools (`sync.Pool`), pre-allocated slices, and in-place mutations minimize GC pressure.
2. **Batched fsync** -- Never fsync per-request. Accumulate writes, fsync once, ACK all waiters.
3. **OS page cache as read cache** -- No application-level cache. The kernel manages caching via the page cache.
4. **Lock-free read path** -- Immutable WAL segments can be read concurrently without locks. Only writes go through the serialized cluster goroutine.

## Concurrency model

klite uses a single **cluster goroutine** for all write operations:

- Offset assignment
- Topic/partition creation
- Consumer group state transitions
- WAL append

This eliminates locking on the write path. Read operations (Fetch, ListOffsets) are concurrent and lock-free against immutable WAL segments.

Each TCP connection runs in its own goroutine, handling request framing and response ordering. Requests that require cluster state are dispatched to the cluster goroutine via channels.

## When to use klite

klite is a good fit when:

- You need Kafka protocol compatibility but don't need a full cluster
- You're running in dev, staging, CI, or small production environments
- You want simple operations: one binary, one process, one config
- You want S3-backed durability without managing Kafka's tiered storage
- You're running on a single node with fast local storage

klite is **not** a fit when:

- You need multi-broker replication for HA (klite is single-broker)
- You need millions of messages/sec across many nodes
- You need Kafka Connect or server-side Kafka Streams
- You need ZooKeeper or KRaft controller quorum APIs

## Comparison

| | klite | Apache Kafka | Redpanda | WarpStream |
|---|---|---|---|---|
| **Architecture** | Single binary | Multi-broker cluster | Single binary (C++) | Zero-disk, agents + S3 |
| **Storage** | WAL + S3 | Local log segments | Local log segments | S3 only |
| **Replication** | None (S3 durability) | ISR-based | Raft-based | S3 durability |
| **Dependencies** | None | ZooKeeper/KRaft | None | S3 + metadata store |
| **Resource usage** | ~20 MB idle | 1+ GB per broker | 1+ GB | Agent is stateless |
| **Operations** | Single process | Complex | Medium | Medium |
| **Best for** | Small/medium, dev, edge | Large-scale production | Medium/large production | Cloud-native, large-scale |

