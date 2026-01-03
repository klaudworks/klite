---
title: Storage
description: How klite stores data -- WAL format, S3 tiered storage, retention, and crash recovery.
---

klite uses a two-tier storage model: a local write-ahead log (WAL) for hot data and optional S3 for durable cold storage.

## Storage tiers

```
┌──────────────────────────────────┐
│           Hot tier               │
│     Local WAL (NVMe/SSD)         │
│                                  │
│  Fast writes, fsync batching     │
│  Recent data in OS page cache    │
│  Crash recovery via WAL replay   │
└──────────────┬───────────────────┘
               │ Periodic flush
               ▼
┌──────────────────────────────────┐
│           Cold tier              │
│        S3 (optional)             │
│                                  │
│  Durable, cost-efficient         │
│  Immutable segment objects       │
│  Disaster recovery source        │
└──────────────────────────────────┘
```

## Write-ahead log (WAL)

The WAL is an append-only log stored in the data directory (`--data-dir`, default `./data`). All produces are written to the WAL before being acknowledged.

### WAL structure

```
data/
  wal/
    00000000000000000000.wal    # Segment files
    00000000000000065536.wal
    ...
  metadata.log                  # Topic/partition/group metadata
  meta.properties               # Cluster ID, node ID
```

### Segment files

WAL segments are append-only files containing RecordBatches. Each segment starts at a specific offset and has a configurable maximum size. Segments are immutable once rotated -- only the active (latest) segment receives writes.

### Fsync batching

klite batches fsync calls for performance. Instead of fsyncing after every produce request:

1. Write requests are accumulated over a configurable window (default 2ms)
2. A single fsync is issued for the batch
3. All waiting produce requests are acknowledged

This trades a small amount of latency (up to 2ms) for dramatically higher throughput. With `acks=all`, the fsync window is the dominant contributor to produce latency.

### Memory budget

klite manages memory carefully:

- **Ring buffer** for in-flight writes awaiting fsync
- **OS page cache** for recently written data (zero-copy reads for tail consumers)
- **No application-level cache** -- the kernel is better at this

## Metadata log

Topic definitions, partition state, consumer group metadata, and configuration are stored in `metadata.log`. This is a separate append-only log with periodic compaction.

Entries include:
- Topic creation/deletion
- Partition count changes
- Consumer group state
- Configuration changes
- Transaction state

On startup, klite replays the metadata log to reconstruct in-memory state.

## S3 tiered storage

When an S3 bucket is configured (`--s3-bucket`), klite periodically flushes completed WAL segments to S3.

### Flush pipeline

1. WAL segment is rotated (new segment starts accepting writes)
2. The completed segment is uploaded to S3
3. After successful upload, the local segment can be deleted (subject to retention policy)

### S3 object format

Objects are keyed by topic, partition, and base offset:

```
s3://my-bucket/klite/<cluster-id>/<topic>/<partition>/<base-offset>.segment
```

Each object contains the raw WAL segment data -- RecordBatches in Kafka v2 format.

### Read path

When a Fetch request needs data that's no longer in the local WAL:

1. Check local WAL (page cache hit for recent data)
2. If not found locally, fetch from S3
3. S3 data is streamed directly to the client (no local caching)

### S3-compatible backends

klite works with any S3-compatible storage:

| Backend | Flag |
|---------|------|
| AWS S3 | `--s3-bucket my-bucket --s3-region us-east-1` |
| MinIO | `--s3-bucket klite --s3-endpoint http://minio:9000` |
| Cloudflare R2 | `--s3-bucket klite --s3-endpoint https://<account>.r2.cloudflarestorage.com` |
| Google Cloud Storage | `--s3-bucket klite --s3-endpoint https://storage.googleapis.com` |
| LocalStack | `--s3-bucket klite --s3-endpoint http://localhost:4566` |

## Retention

klite supports time-based and size-based retention, configurable per topic:

| Config | Default | Description |
|--------|---------|-------------|
| `retention.ms` | 604800000 (7 days) | Maximum age of data. `-1` for infinite. |
| `retention.bytes` | -1 (infinite) | Maximum size per partition. |

A background process periodically scans partitions and deletes segments that exceed the retention policy. With S3 enabled, retention applies to both local and S3 data.

```bash
# Set broker-wide default
./klite --retention-ms 86400000  # 24 hours

# Per-topic retention is set via topic configs
# (CreateTopics or IncrementalAlterConfigs API)
```

See [Topic configs](/reference/topic-configs/) for the full list of per-topic settings.

## Log compaction

Topics with `cleanup.policy=compact` retain only the latest value for each key. klite runs a background compaction process that:

1. Scans segments for duplicate keys
2. Removes older entries, keeping only the latest per key
3. Produces compacted segments

This is useful for changelog topics, KTable backing stores, and configuration topics.

## Crash recovery

On startup, klite:

1. Reads `meta.properties` for cluster identity
2. Replays `metadata.log` to reconstruct topic/partition/group state
3. Replays the WAL from the last fsync point to recover in-flight data
4. If S3 is configured, validates that local state is consistent with S3

The recovery point objective (RPO) depends on the fsync window:
- **acks=all with 2ms fsync window**: up to 2ms of data loss on crash
- **acks=1 (no fsync)**: up to the full unflushed buffer on crash
- **S3 enabled**: data flushed to S3 is durable regardless of local disk failure

## Data directory layout

```
./data/
├── meta.properties          # Cluster ID, node ID (created on first start)
├── metadata.log             # Topic/partition/group metadata (append-only)
├── metadata.log.snapshot    # Compacted metadata snapshot
└── wal/
    ├── 00000000000000000000.wal
    ├── 00000000000000065536.wal
    └── ...
```

## Next steps

- [Architecture](/concepts/architecture/) -- overall system design
- [Configuration](/reference/configuration/) -- storage-related flags
- [Monitoring](/guides/monitoring/) -- disk usage and S3 health
