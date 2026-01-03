---
title: Compatibility
description: Kafka protocol compatibility -- supported APIs, client libraries, and known differences.
---

klite aims for **drop-in compatibility** with standard Kafka clients. Any franz-go, librdkafka, or Java Kafka client should be able to connect, produce, consume, and use consumer groups without code changes beyond updating the broker address.

## Protocol version

klite targets **Kafka 4.0+ wire protocol** compatibility (KIP-896 version floors). Only message format v2 (RecordBatch) is supported.

## Client library compatibility

| Client Library | Language | Status |
|---------------|----------|--------|
| **franz-go** | Go | Fully supported |
| **confluent-kafka-go** (librdkafka) | Go | Fully supported |
| **librdkafka** | C/C++ | Fully supported |
| **confluent-kafka-python** | Python | Fully supported |
| **kafka-python** | Python | Fully supported |
| **KafkaJS** | Node.js | Fully supported |
| **kafka-clients** (Java) | Java | Fully supported |
| **sarama** | Go | Fully supported |
| **Spring Kafka** | Java | Fully supported |

If your client speaks the standard Kafka wire protocol with message format v2, it should work with klite.

## Supported APIs

klite implements 39 Kafka API keys across the core data path, consumer groups, transactions, admin, and authentication.

### Core data path

| API Key | API | Notes |
|---------|-----|-------|
| 0 | **Produce** | Full support. acks=0, 1, all. LogAppendTime. |
| 1 | **Fetch** | Long-polling, size limits, KIP-74. |
| 2 | **ListOffsets** | Earliest, latest, timestamp lookup. |
| 3 | **Metadata** | Auto-create topics, topic IDs. |
| 18 | **ApiVersions** | Reports all supported APIs and version ranges. |

### Consumer groups

| API Key | API | Notes |
|---------|-----|-------|
| 10 | **FindCoordinator** | Group and transaction coordinators. |
| 11 | **JoinGroup** | Eager and cooperative rebalance. |
| 14 | **SyncGroup** | Partition assignment distribution. |
| 12 | **Heartbeat** | Session timeout enforcement. |
| 13 | **LeaveGroup** | Graceful consumer shutdown. |
| 8 | **OffsetCommit** | Per-group offset tracking. |
| 9 | **OffsetFetch** | Retrieve committed offsets. |
| 47 | **OffsetDelete** | Delete committed offsets. |

### Transactions & idempotency

| API Key | API | Notes |
|---------|-----|-------|
| 22 | **InitProducerID** | Producer ID assignment, epoch bumps. |
| 24 | **AddPartitionsToTxn** | Register partitions in a transaction. |
| 25 | **AddOffsetsToTxn** | Register group offsets in a transaction. |
| 26 | **EndTxn** | Commit or abort a transaction. |
| 28 | **TxnOffsetCommit** | Commit offsets within a transaction. |
| 61 | **DescribeProducers** | Inspect active producer state. |
| 65 | **DescribeTransactions** | Inspect active transactions. |
| 66 | **ListTransactions** | List all active transactions. |

### Admin APIs

| API Key | API | Notes |
|---------|-----|-------|
| 19 | **CreateTopics** | Name validation, config, partition count. |
| 20 | **DeleteTopics** | By name or topic ID. |
| 37 | **CreatePartitions** | Add partitions to existing topics. |
| 32 | **DescribeConfigs** | Topic and broker configs. |
| 33 | **AlterConfigs** | Set topic configs (legacy). |
| 44 | **IncrementalAlterConfigs** | Incremental config updates. |
| 16 | **ListGroups** | List all consumer groups. |
| 15 | **DescribeGroups** | Group membership and state. |
| 42 | **DeleteGroups** | Delete inactive consumer groups. |
| 21 | **DeleteRecords** | Advance log start offset. |
| 60 | **DescribeCluster** | Cluster metadata. |
| 35 | **DescribeLogDirs** | Log directory sizes. |
| 23 | **OffsetForLeaderEpoch** | Leader epoch offset lookup. |

### Authentication

| API Key | API | Notes |
|---------|-----|-------|
| 17 | **SASLHandshake** | SASL mechanism negotiation. |
| 36 | **SASLAuthenticate** | PLAIN, SCRAM-SHA-256, SCRAM-SHA-512. |
| 51 | **AlterUserScramCredentials** | Manage SCRAM users. |
| 50 | **DescribeUserScramCredentials** | List SCRAM users. |

## Explicitly unsupported

These Kafka features are intentionally not supported:

| Feature | Reason |
|---------|--------|
| **Multi-broker clustering** | klite is a single-broker architecture. Durability comes from S3. |
| **KRaft / ZooKeeper** | No controller quorum needed. |
| **Inter-broker APIs** | LeaderAndIsr, StopReplica, UpdateMetadata, etc. are cluster-internal. |
| **Kafka Connect** | Server-side connector framework. Use standalone connectors. |
| **Kafka Streams server-side** | Kafka Streams clients work fine -- they're just producers and consumers. |
| **Schema Registry** | Not a broker feature. Use Confluent Schema Registry or Karapace alongside klite. |
| **Share groups** (KIP-932) | Not implemented. |
| **Delegation tokens** | Not implemented. |
| **Message format v0/v1** | Only v2 (RecordBatch) is supported. Very old clients may not work. |
| **Quota enforcement** | Configs are stored but not enforced. |

## Known differences from Apache Kafka

| Behavior | Kafka | klite |
|----------|-------|-------|
| **Replication** | ISR-based, configurable replication factor | None (single broker). S3 provides durability. |
| **Partition reassignment** | Dynamic across brokers | N/A (single broker) |
| **Controller** | KRaft or ZooKeeper | N/A (embedded, no election) |
| **Log segments** | Configurable segment size, roll on time/size | WAL segments, S3 flush is time-based |
| **Tiered storage API** | ListOffsets -4/-5 | Not supported; S3 is transparent |
| **Quotas** | Enforced per client/user | Stored but not enforced |

## Testing your client

The quickest way to verify compatibility with your client:

```bash
# Start klite
./klite

# Run your application pointing to klite
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
# ... start your app
```

If you encounter a compatibility issue, check [Troubleshooting](/guides/troubleshooting/) or [open an issue](https://github.com/kliteio/klite/issues).

## Next steps

- [Quickstart](/guides/getting-started/) -- get started with your preferred client
- [Supported APIs reference](/reference/supported-apis/) -- version ranges and details
- [Topic configs](/reference/topic-configs/) -- supported per-topic configuration
