---
title: Kafka Compatibility
description: Client library support, supported protocol APIs, and known limitations.
---

klite is a drop-in replacement for standard Kafka clients. Any franz-go, librdkafka, or Java client can connect, produce, consume, and use consumer groups without code changes beyond updating the broker address.

## Client compatibility

Any client built for **Kafka 2.3** (2019) or newer will work. This includes franz-go, librdkafka, the Java Kafka client, KafkaJS, sarama, and their wrappers (confluent-kafka-go, confluent-kafka-python, Spring Kafka, etc.).

## Supported APIs

klite implements 39 Kafka API keys. The tables below show every supported API with its version range.

<details>
<summary>Core data path (5 APIs)</summary>

| Key | API | Min | Max | Notes |
|-----|-----|-----|-----|-------|
| 0 | Produce | 3 | 11 | `acks=0`, `acks=1`, `acks=all`. LogAppendTime. Transactional produce. |
| 1 | Fetch | 4 | 16 | Long-polling, size limits, KIP-74 fetch response ordering. |
| 2 | ListOffsets | 1 | 8 | Earliest, latest, max-timestamp, timestamp-based lookup. |
| 3 | Metadata | 4 | 12 | Auto-create topics, topic IDs (UUIDs). |
| 18 | ApiVersions | 0 | 4 | Client handshake. Returns all supported APIs and version ranges. |

</details>

<details>
<summary>Consumer groups (8 APIs)</summary>

| Key | API | Min | Max | Notes |
|-----|-----|-----|-----|-------|
| 10 | FindCoordinator | 0 | 6 | Group and transaction coordinators. Batch key lookup. |
| 11 | JoinGroup | 0 | 9 | Eager and cooperative rebalance. Static membership. |
| 14 | SyncGroup | 0 | 5 | Distributes partition assignments from leader to members. |
| 12 | Heartbeat | 0 | 4 | Session timeout enforcement. |
| 13 | LeaveGroup | 0 | 5 | Graceful departure. Batch member leave. |
| 8 | OffsetCommit | 0 | 9 | Per-group, per-partition offset tracking. |
| 9 | OffsetFetch | 0 | 9 | Retrieve committed offsets. Batch group support in v8+. |
| 47 | OffsetDelete | 0 | 0 | Delete committed offsets. |

</details>

<details>
<summary>Transactions and idempotency (8 APIs)</summary>

| Key | API | Min | Max | Notes |
|-----|-----|-----|-----|-------|
| 22 | InitProducerID | 0 | 5 | Producer ID and epoch assignment. Transactional and idempotent. |
| 24 | AddPartitionsToTxn | 0 | 4 | Register partitions in a transaction. |
| 25 | AddOffsetsToTxn | 0 | 3 | Register group offsets in a transaction. |
| 26 | EndTxn | 0 | 4 | Commit or abort. Writes control batches. |
| 28 | TxnOffsetCommit | 0 | 4 | Commit offsets within a transaction (exactly-once). |
| 61 | DescribeProducers | 0 | 0 | Inspect active producer state. |
| 65 | DescribeTransactions | 0 | 0 | Inspect active transactions. |
| 66 | ListTransactions | 0 | 1 | List all active transaction IDs. |

</details>

<details>
<summary>Admin (13 APIs)</summary>

| Key | API | Min | Max | Notes |
|-----|-----|-----|-----|-------|
| 19 | CreateTopics | 2 | 7 | Name validation, partition count, topic configs. |
| 20 | DeleteTopics | 0 | 6 | By name or topic ID. |
| 37 | CreatePartitions | 0 | 3 | Increase partition count for existing topics. |
| 32 | DescribeConfigs | 0 | 4 | Topic and broker configs. |
| 33 | AlterConfigs | 0 | 2 | Legacy full-replacement config update. |
| 44 | IncrementalAlterConfigs | 0 | 1 | Incremental changes (SET, DELETE, APPEND, SUBTRACT). |
| 16 | ListGroups | 0 | 5 | List consumer groups with state filter. |
| 15 | DescribeGroups | 0 | 6 | Group metadata, members, assignments. |
| 42 | DeleteGroups | 0 | 2 | Delete inactive consumer groups. |
| 21 | DeleteRecords | 0 | 2 | Advance log start offset. |
| 60 | DescribeCluster | 0 | 2 | Cluster ID, controller, broker list. |
| 35 | DescribeLogDirs | 0 | 4 | Log directory sizes. |
| 23 | OffsetForLeaderEpoch | 3 | 4 | End offset for a given leader epoch. |

</details>

<details>
<summary>Authentication (4 APIs)</summary>

| Key | API | Min | Max | Notes |
|-----|-----|-----|-----|-------|
| 17 | SASLHandshake | 1 | 1 | Mechanism negotiation. |
| 36 | SASLAuthenticate | 0 | 2 | PLAIN, SCRAM-SHA-256, SCRAM-SHA-512. |
| 51 | AlterUserScramCredentials | 0 | 0 | Create, update, or delete SCRAM users. |
| 50 | DescribeUserScramCredentials | 0 | 0 | List SCRAM users and mechanisms. |

</details>

## Known limitations

- **Quotas are stored but not enforced.** You can set client quotas through the API and they'll be persisted, but klite won't actually throttle clients based on them.
- **No legacy protocol versions.** Clients older than Kafka 2.3 (mid-2019) may not work.
- **Single broker.** No replication, no partition reassignment, no inter-broker APIs. Durability comes from S3 instead.
