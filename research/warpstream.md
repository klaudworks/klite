# WarpStream

WarpStream is closed-source (acquired by Confluent, Sept 2024). Research is
based on their public blog posts and documentation.

Sources:
- https://www.warpstream.com/blog/kafka-is-dead-long-live-kafka
- https://www.warpstream.com/blog/minimizing-s3-api-costs-with-distributed-mmap
- https://www.warpstream.com/blog/the-case-for-shared-storage
- https://www.warpstream.com/blog/the-art-of-being-lazy-log
- https://docs.warpstream.com/warpstream

---

## Storage Architecture

### Overview

Single stateless Go binary ("Agent"). Zero local disks. All data goes directly
to S3. Metadata managed by a centralized Control Plane (replicated state machine).

```
Kafka Clients
    |
WarpStream Agents (stateless, any can serve any partition)
    |               |
    S3 (all data)   Control Plane (metadata, offset assignment)
```

Core principles:
1. Separation of storage (S3) and compute (Agents)
2. Separation of data (S3) from metadata (Control Plane)
3. Any Agent can serve any partition -- no leader-per-partition

### Write Path

```
Producer -> Agent buffers (250ms or 4 MiB, whichever first)
         -> S3 PUT (single multi-partition file)
         -> Commit metadata to Control Plane (assigns offsets)
         -> ACK to producer
```

**Key innovation:** One file per Agent per flush cycle, containing data from
ALL topic-partitions received during that window. Unlike Kafka's per-partition
segments, WarpStream creates ~4 files/sec/Agent regardless of partition count.

Batches within each file sorted by topic, then partition. File size target ~4 MiB.

Offset assignment happens at the Control Plane during commit, NOT at the Agent.
The file doesn't logically "exist" until commit succeeds.

Buffering defaults:

| Parameter | Default | Low-Latency |
|---|---|---|
| Batch timeout | 250ms | 25-50ms |
| Batch size | 4 MiB | 4 MiB |

### Read Path -- Distributed mmap

Problem: files contain many partitions. Naive per-partition GETs would be
cost-prohibitive (1024 partitions * 4 files/sec = 4096+ GETs/sec).

Solution: distributed mmap abstraction across all Agents.

How it works:
1. **Consistent hashing ring:** Each Agent caches a subset of files by file ID.
2. **4 MiB aligned pages:** Data loaded in fixed 4 MiB chunks (like OS mmap).
3. **Sub-fetch routing:** If file F is cached on Agent B, Agent A forwards
   the sub-fetch to Agent B.
4. **Scan sharing:** At the "live edge," every new file's blocks are downloaded
   once per AZ. Multiple consumers sharing the same block = single S3 GET.
5. **Thundering herd prevention:** Concurrent requests for same `(file_id, chunk)`
   deduplicated into one S3 GET.
6. **Per-AZ caching:** In 3-AZ deployment, each chunk downloaded 3x total.

### S3 Object Format

- Multi-partition files, ~4 MiB at ingestion
- Metadata section extractable separately from data
- After compaction: much larger files with better per-partition locality

### Metadata / Control Plane

Stores: file-to-offset mappings, batch ordering, topic/partition config,
consumer group state, committed offsets.

Replicated state machine with journaled operations. Each Virtual Cluster has
multiple replicas backed up to object storage.

Scalability: at 4.5 GiB/s write throughput, metadata store <10% utilized.

### Compaction

Small ingestion files (~4 MiB) compacted into larger files with better
per-partition data locality. Streaming (low memory). Orchestrated by Control
Plane assigning tasks to Agents.

Critical for lagging consumers: without compaction, reading one partition from
multi-partition files has high read amplification.

For S3 Express One Zone users, compaction also moves data from S3EOZ -> S3
Standard (cheaper storage tier).

### Lightning Topics (LazyLog Pattern)

Based on the LazyLog paper (UIUC). Key insight: durability and ordering are
separable. Durability must happen upfront. Ordering can be deferred until
consumption time.

**Fast path:**
1. Buffer records
2. Write to S3 in a "sequence" folder
3. ACK immediately (return offset 0)
4. Async commit to Control Plane

**Slow path (failure recovery):**
Control Plane dispatches scan+replay jobs. Agents list sequence folders,
download metadata, commit to Control Plane. Zero data loss guarantee.

Trade-offs: no exact offsets in Produce response, no idempotent producers,
no transactions.

**Ripcord mode:** When Control Plane unavailable, all topics become Lightning
Topics. Producers continue; consumers blocked until sequencing resumes.

### Service Discovery

Connection-level load balancing (not partition-level). Each client connects
to ~1 Agent. Metadata response claims that Agent is leader for ALL partitions.
Round-robin across clients.

Zone-aware: clients encode AZ in client ID (`warpstream_az=us-east-1a`).
Behind load balancers: case-insensitive DNS names to differentiate Agents.

---

## Latency

| Configuration | P50 | P99 | E2E |
|---|---|---|---|
| S3 Standard, 250ms batch | ~200ms | ~400ms | ~1s |
| S3 Express One Zone, 50ms batch | 105ms | 169ms | ~200-400ms |
| S3EOZ + Lightning, 25ms batch | 33ms | 50ms | similar |

S3EOZ latency breakdown (50ms batch, classic topics):
- Buffer: ~30ms, S3 write: ~20ms, Control Plane commit: ~50ms = ~100ms total

Lightning Topics breakdown:
- Buffer: ~15ms, S3 write: ~18ms, ACK (no commit): 0ms = ~33ms total

---

## Cost Model

| Component | WarpStream (S3EOZ) | Kafka (3-AZ) |
|---|---|---|
| VMs | $388/mo | $5,487/mo |
| Inter-zone networking | $0 | $14,765/mo |
| S3 PUTs + GETs | $1,096/mo | N/A |
| S3 bandwidth + storage | $1,657/mo | N/A |
| **Total** | **$2,961/mo** | **$20,252/mo** |

Key: EC2<->S3 networking is free in AWS. This eliminates Kafka's dominant cost.

---

## Testing

WarpStream is closed-source so exact testing details are limited.

What's known:
- Protocol conformance tested via popular Kafka client libraries (Java, Go, Python)
- Emphasis on drop-in replacement (change broker address, everything works)
- No evidence of running Kafka's ducktape tests (Go binary, not Kafka fork)
- Focus on integration tests against real S3

---

## Implications for kafka-light

| WarpStream Decision | Our Approach |
|---|---|
| S3 only, zero local disks | WAL + S3 (local WAL for lower latency) |
| Multi-partition files | Same -- batch across partitions for S3 |
| Centralized metadata store | Single cluster-state goroutine |
| Offset assignment at commit time | Assign offsets in cluster-state goroutine |
| Distributed mmap (multi-Agent) | Not needed (single broker) |
| LazyLog (durability != ordering) | Possible future optimization |
| Single Go binary | Same -- our design goal |
| Connection-level LB | N/A for single broker |
