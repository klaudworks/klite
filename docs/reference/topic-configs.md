---
title: Topic Configs
description: Supported per-topic configuration options in klite.
---

Topic-level configurations can be set when creating a topic (via CreateTopics API) or modified later (via IncrementalAlterConfigs API). These override broker-level defaults.

## Setting topic configs

### On creation

Most Kafka client admin tools support setting configs at topic creation time:

```bash
# Using kafka-topics.sh (from Kafka distribution)
kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic my-topic \
  --partitions 6 \
  --config retention.ms=86400000 \
  --config cleanup.policy=compact
```

### Modifying existing topics

```bash
# Using kafka-configs.sh
kafka-configs.sh --bootstrap-server localhost:9092 \
  --alter --entity-type topics --entity-name my-topic \
  --add-config retention.ms=172800000
```

Or use any Kafka admin client library (franz-go, confluent-kafka, etc.) with the IncrementalAlterConfigs API.

## Supported configs

| Config | Type | Default | Description |
|--------|------|---------|-------------|
| `cleanup.policy` | string | `delete` | `delete` -- remove old segments by time/size. `compact` -- retain only latest value per key. `compact,delete` -- both. |
| `compression.type` | string | `producer` | Compression for stored data: `producer` (preserve client compression), `none`, `gzip`, `snappy`, `lz4`, `zstd`. |
| `retention.ms` | long | `604800000` (7d) | Maximum age of data in milliseconds. `-1` for infinite retention. |
| `retention.bytes` | long | `-1` (infinite) | Maximum size per partition in bytes. Oldest segments are deleted when exceeded. |
| `max.message.bytes` | int | `1048588` | Maximum size of a single RecordBatch in bytes. Larger batches are rejected with `MESSAGE_TOO_LARGE`. |
| `segment.bytes` | int | `67108864` (64M) | Accepted for compatibility. klite does not use segment-based rotation; S3 flush is time-based via `--s3-flush-interval`. |
| `message.timestamp.type` | string | `CreateTime` | `CreateTime` -- use the timestamp set by the producer. `LogAppendTime` -- overwrite with the broker's wall clock time. |

## Config details

### cleanup.policy

Controls how old data is removed:

- **`delete`** (default) -- Segments are deleted when they exceed `retention.ms` or `retention.bytes`.
- **`compact`** -- The background compaction process retains only the latest record for each key. Useful for changelog and snapshot topics.
- **`compact,delete`** -- Both strategies apply. Data is compacted, and segments older than the retention window are deleted.

### compression.type

- **`producer`** (default) -- klite stores batches exactly as received from the producer, preserving whatever compression the client used.
- Explicit values (`gzip`, `snappy`, `lz4`, `zstd`, `none`) -- klite re-compresses batches in the specified format before storage. This has a CPU cost.

In practice, `producer` is almost always the right choice. Set an explicit value only if you need to enforce a specific compression format.

### retention.ms

```bash
# 24 hours
retention.ms=86400000

# 30 days
retention.ms=2592000000

# Infinite (never delete)
retention.ms=-1
```

### retention.bytes

Per-partition size limit. When a partition exceeds this size, the oldest segments are deleted.

```bash
# 1 GB per partition
retention.bytes=1073741824

# Infinite (no size limit, only time-based)
retention.bytes=-1
```

### message.timestamp.type

- **`CreateTime`** -- The producer sets the timestamp. Useful for event time processing.
- **`LogAppendTime`** -- The broker overwrites the timestamp with the time the batch was appended. Useful for ingestion time processing.

## Viewing topic configs

```bash
# Using kafka-configs.sh
kafka-configs.sh --bootstrap-server localhost:9092 \
  --describe --entity-type topics --entity-name my-topic
```

## Configs accepted but not enforced

The following Kafka topic configs are accepted (stored and returned by DescribeConfigs) but have no effect on klite's behavior:

| Config | Reason |
|--------|--------|
| `min.insync.replicas` | Single broker; no ISR concept. Always effectively 1. |
| `unclean.leader.election.enable` | Single broker; no leader election. |
| `flush.messages` | klite manages fsync via its own batching window. |
| `flush.ms` | klite manages fsync via its own batching window. |
| `index.interval.bytes` | klite uses its own indexing strategy. |
| `segment.ms` | WAL rotation is managed internally. |
| `min.compaction.lag.ms` | Accepted, behavior may differ. |
| `max.compaction.lag.ms` | Accepted, behavior may differ. |
| `min.cleanable.dirty.ratio` | Accepted, behavior may differ. |

## Next steps

- [Configuration](/reference/configuration/) -- broker-level configuration
- [Supported APIs](/reference/supported-apis/) -- API compatibility reference
- [Storage](/concepts/storage/) -- how retention and compaction work internally
