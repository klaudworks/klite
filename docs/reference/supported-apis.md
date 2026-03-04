---
title: Supported APIs
description: Complete list of Kafka protocol APIs supported by klite with version ranges.
---

klite implements 39 Kafka API keys. This page lists every supported API with its version range and notes on behavior.

## How to read this table

- **API Key**: The numeric Kafka protocol API key
- **Min/Max Version**: The range of protocol versions klite supports for this API
- **Notes**: Behavioral details or differences from Apache Kafka

## Core data path

| Key | API | Min | Max | Notes |
|-----|-----|-----|-----|-------|
| 0 | Produce | 3 | 11 | `acks=0` (fire-and-forget), `acks=1` (leader ACK), `acks=-1/all` (durable). LogAppendTime supported. Transactional produce supported. |
| 1 | Fetch | 4 | 16 | Long-polling with configurable `max_wait_ms`. Size limits (`min_bytes`, `max_bytes`, `max_partition_bytes`). KIP-74 fetch response ordering. |
| 2 | ListOffsets | 1 | 8 | Timestamp `-1` (latest), `-2` (earliest), `-3` (max timestamp). Timestamp-based offset lookup. |
| 3 | Metadata | 1 | 12 | Auto-create topics (when enabled). Topic IDs (UUIDs). Reports all topics or specific topics. |
| 18 | ApiVersions | 0 | 4 | Returns the full list of supported APIs and version ranges. Always handled first for client handshake. |

## Consumer groups

| Key | API | Min | Max | Notes |
|-----|-----|-----|-----|-------|
| 10 | FindCoordinator | 0 | 5 | Returns this broker as coordinator for both `GROUP` and `TRANSACTION` key types. Batch key lookup supported. |
| 11 | JoinGroup | 0 | 9 | Eager and cooperative rebalance protocols. Leader election. `group.instance.id` for static membership. |
| 14 | SyncGroup | 0 | 5 | Distributes partition assignments from leader to members. |
| 12 | Heartbeat | 0 | 4 | Session timeout enforcement. Returns `REBALANCE_IN_PROGRESS` when rebalance is needed. |
| 13 | LeaveGroup | 0 | 5 | Graceful departure with `reason` string. Batch member leave. |
| 8 | OffsetCommit | 0 | 9 | Per-group, per-partition offset tracking. Retention enforced. |
| 9 | OffsetFetch | 0 | 9 | Retrieve committed offsets. Batch group support in v8+. |
| 47 | OffsetDelete | 0 | 0 | Delete committed offsets for specific topic-partitions. |

## Transactions & idempotency

| Key | API | Min | Max | Notes |
|-----|-----|-----|-----|-------|
| 22 | InitProducerID | 0 | 5 | Assigns producer ID and epoch. Supports transactional and idempotent producers. |
| 24 | AddPartitionsToTxn | 0 | 5 | Registers topic-partitions in an active transaction. |
| 25 | AddOffsetsToTxn | 0 | 4 | Registers consumer group offsets in an active transaction. |
| 26 | EndTxn | 0 | 4 | Commits or aborts a transaction. Writes control batches. |
| 28 | TxnOffsetCommit | 0 | 4 | Commits offsets as part of a transaction (exactly-once consume). |
| 61 | DescribeProducers | 0 | 0 | Returns active producer state for partitions. |
| 65 | DescribeTransactions | 0 | 0 | Returns state of active transactions. |
| 66 | ListTransactions | 0 | 0 | Lists all active transaction IDs. |

## Admin APIs

| Key | API | Min | Max | Notes |
|-----|-----|-----|-----|-------|
| 19 | CreateTopics | 0 | 7 | Topic name validation, partition count, replication factor (accepted but ignored), topic configs. |
| 20 | DeleteTopics | 0 | 6 | Delete by topic name or topic ID. |
| 37 | CreatePartitions | 0 | 3 | Increase partition count for existing topics. |
| 32 | DescribeConfigs | 0 | 4 | Returns topic-level and broker-level configurations. |
| 33 | AlterConfigs | 0 | 2 | Legacy full-replacement config update. |
| 44 | IncrementalAlterConfigs | 0 | 1 | Incremental config changes (SET, DELETE, APPEND, SUBTRACT). |
| 16 | ListGroups | 0 | 5 | Lists all consumer groups with state filter. |
| 15 | DescribeGroups | 0 | 5 | Returns group metadata, members, assignments. |
| 42 | DeleteGroups | 0 | 2 | Deletes inactive (empty) consumer groups. |
| 21 | DeleteRecords | 0 | 2 | Advances log start offset (tombstones earlier data). |
| 60 | DescribeCluster | 0 | 1 | Returns cluster ID, controller ID, broker list. |
| 35 | DescribeLogDirs | 0 | 4 | Returns log directory information and sizes. |
| 23 | OffsetForLeaderEpoch | 0 | 4 | Returns end offset for a given leader epoch. |

## Authentication

| Key | API | Min | Max | Notes |
|-----|-----|-----|-----|-------|
| 17 | SASLHandshake | 0 | 1 | Negotiates SASL mechanism. |
| 36 | SASLAuthenticate | 0 | 2 | Authenticates with PLAIN, SCRAM-SHA-256, or SCRAM-SHA-512. |
| 51 | AlterUserScramCredentials | 0 | 0 | Create, update, or delete SCRAM user credentials. |
| 50 | DescribeUserScramCredentials | 0 | 0 | List SCRAM users and their mechanisms. |

## Unsupported API keys

The following API keys are intentionally not supported. Requests for these APIs return `UNSUPPORTED_VERSION`:

| Key | API | Reason |
|-----|-----|--------|
| 4 | LeaderAndIsr | Inter-broker, not applicable |
| 5 | StopReplica | Inter-broker, not applicable |
| 6 | UpdateMetadata | Inter-broker, not applicable |
| 7 | ControlledShutdown | Inter-broker, not applicable |
| 27 | WriteTxnMarkers | Inter-broker transaction coordination |
| 29-31 | Various | Controller quorum APIs |
| 34 | AlterReplicaLogDirs | Replica management, not applicable |
| 38 | ElectLeaders | Multi-broker leader election |
| 39-41 | Various | Client quota APIs (stored but not enforced) |
| 43 | ElectLeaders v2 | Multi-broker leader election |
| 45-46 | Various | Broker registration, not applicable |
| 48-49 | Various | Describe/List client quotas |
| 52-59 | Various | Feature flags, allocate producer IDs, etc. |
| 62-64 | Various | Broker heartbeat, controller APIs |
| 67+ | Various | Future/experimental APIs |

## Next steps

- [Compatibility](/concepts/compatibility/) -- client library support and known differences
- [Topic configs](/reference/topic-configs/) -- supported per-topic configuration
- [Configuration](/reference/configuration/) -- broker configuration reference
