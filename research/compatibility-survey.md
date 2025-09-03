# Kafka-Compatible Broker Compatibility Survey

Survey of how non-Java Kafka-compatible brokers (Redpanda, Bufstream) define
and test their protocol compatibility. Findings collected March 2026 from
official documentation.

## 1. Redpanda (C++, Vectorized/Redpanda Data)

### 1.1 API Keys / Protocol Versions

Redpanda aims for **full Kafka API compatibility** and does not publish a
specific list of supported API keys/versions in a single matrix. Instead they
position themselves as a "drop-in replacement" for Kafka. Their docs state they
are "compatible with the Kafka API" broadly, and they implement the standard
Kafka wire protocol including all major API keys through recent Kafka versions.

Key supported API areas:
- **Core data path**: Produce, Fetch, ListOffsets, Metadata
- **Consumer groups**: JoinGroup, SyncGroup, Heartbeat, LeaveGroup, OffsetFetch,
  OffsetCommit, FindCoordinator, DescribeGroups, ListGroups, DeleteGroups
- **Transactions**: Full EOS support (InitProducerId, AddPartitionsToTxn,
  AddOffsetsToTxn, EndTxn, TxnOffsetCommit) -- enabled by default
- **Admin**: CreateTopics, DeleteTopics, CreatePartitions, AlterConfigs,
  IncrementalAlterConfigs, DescribeConfigs, DeleteRecords, DescribeLogDirs
- **ACLs**: CreateAcls, DeleteAcls, DescribeAcls -- full support
- **SASL**: SaslHandshake, SaslAuthenticate (SCRAM-SHA-256/512, OAUTHBEARER,
  GSSAPI via Enterprise)
- **Idempotent producers**: Fully supported, enabled by default
- **Schema Registry**: Built-in (separate HTTP API, not Kafka wire protocol)

### 1.2 Explicitly Unsupported Features

- **ZooKeeper**: Not used; Redpanda uses Raft internally (no ZK APIs)
- **KRaft controller APIs**: Not applicable (Redpanda has its own controller)
- **Kafka Connect**: Not built-in (users run standalone Kafka Connect against
  Redpanda, or use Redpanda Connectors)
- **Kafka Streams**: Not built-in (but works as a client library against
  Redpanda since it's just a Kafka client)
- **Delegation Tokens**: Not documented as supported
- **Quotas**: Supported (quota tracking windows, default window sizes are
  configurable via cluster properties)

### 1.3 Configuration

**Topic configs** mirror Kafka conventions:
| Cluster Property | Topic Property |
|---|---|
| `log_cleanup_policy` | `cleanup.policy` |
| `retention_bytes` | `retention.bytes` |
| `log_retention_ms` | `retention.ms` |
| `log_segment_ms` | `segment.ms` |
| `log_segment_size` | `segment.bytes` |
| `log_compression_type` | `compression.type` |
| `log_message_timestamp_type` | `message.timestamp.type` |
| `kafka_batch_max_bytes` | `max.message.bytes` |
| `write_caching_default` | `write.caching` (Redpanda-specific) |

**Broker configs** use Redpanda's own property names (not Kafka's
`server.properties` format). Hundreds of cluster-level properties are
configurable via `rpk cluster config` or the Admin API. They are organized into
categories: cluster, tunable, internal. Many map conceptually to Kafka broker
configs but use different names (e.g., `log_retention_ms` instead of
`log.retention.ms`).

### 1.4 Compression Codecs

All standard codecs supported:
- `none` / `producer` (passthrough)
- `gzip`
- `snappy`
- `lz4`
- `zstd`

Configured via `log_compression_type` (cluster) or `compression.type` (topic).
Default is `producer` (retain original compression).

### 1.5 Message Timestamps

- Supports both `CreateTime` and `LogAppendTime`
- Configured via `log_message_timestamp_type` (cluster) or
  `message.timestamp.type` (topic)
- Default is `CreateTime`
- **Redpanda-specific addition**: `broker_timestamp` -- an internal property on
  each message recording the broker's system time at receipt, used for accurate
  retention calculations regardless of producer clock skew
- Timestamp alerting: configurable skew thresholds via
  `log_message_timestamp_alert_before_ms` and
  `log_message_timestamp_alert_after_ms`

### 1.6 Compatibility Documentation

Redpanda does **not** publish a formal "compatibility matrix" or "known gaps"
document. Their positioning is "Kafka API compatible" as a drop-in replacement.
Differences are documented per-feature (e.g., `acks=all` behavior note: Redpanda
fsyncs before ack by default, Kafka does not). Migration guides exist but focus
on operational steps, not protocol gaps.

### 1.7 Client Libraries Tested

Redpanda's documentation shows examples with:
- **Java**: Apache Kafka Java client (official)
- **Go**: franz-go
- **Python**: kafka-python, confluent-kafka-python
- **Node.js**: kafkajs
- **Rust**: rdkafka bindings
- **C/C++**: librdkafka

No formal "tested clients" matrix is published, but they emphasize working with
"any Kafka client."

---

## 2. Bufstream (Go, Buf)

### 2.1 API Keys / Protocol Versions

Bufstream publishes an **explicit list of supported Kafka endpoints** (as of
Kafka 3.7):

```
AddOffsetsToTxn          ListGroups
AddPartitionsToTxn       ListOffsets
AlterConfigs             ListPartitionReassignments
AlterUserScramCredentials ListTransactions
ApiVersions              Metadata
CreatePartitions         OffsetCommit
CreateTopics             OffsetDelete
DeleteGroups             OffsetFetch
DeleteRecords            Produce
DeleteTopics             SaslAuthenticate
DescribeCluster          SaslHandshake
DescribeConfigs          SyncGroup
DescribeGroups           TxnOffsetCommit
DescribeLogDirs
DescribeProducers
DescribeTransactions
DescribeUserScramCredentials
EndTxn
Fetch
FindCoordinator
Heartbeat
IncrementalAlterConfigs
InitProducerId
JoinGroup
LeaveGroup
```

**Total: 38 endpoints** covering core data path, consumer groups, transactions,
admin, and SASL auth.

Protocol version strategy: "supports the latest version of each Kafka API (as of
Kafka 3.7), while making a best effort to support all previous endpoint
versions." Formal conformance tests run on **Kafka clients going back to version
3.2**. Older client versions not extensively tested.

### 2.2 Explicitly Unsupported Features

Bufstream clearly documents what is NOT supported:

- **ACLs**: Not implemented in wire protocol (though ACL support is now in
  preview via `CreateAcls`, `DeleteAcls`, `DescribeAcls` -- see ACLs docs)
- **Quotas**: Not implemented
- **Delegation token-based authentication**: Not implemented
- **Metrics and telemetry APIs**: Not implemented
- **KRaft/ZooKeeper broker-to-broker APIs**: Deliberately omitted (BrokerRegistration,
  BeginQuorumEpoch, Vote, etc.) -- Bufstream uses Postgres/etcd + S3 instead
- **Mixed clusters**: Cannot mix Bufstream and non-Bufstream brokers

### 2.3 Configuration

**Topic configs** -- Bufstream supports a small, explicit subset:
| Config | Values |
|---|---|
| `cleanup.policy` | `compact`, `delete`, `compact,delete` |
| `compression.type` | `producer`, `gzip`, `snappy`, `lz4`, `zstd`, `uncompressed` |
| `timestamp.type` | `CreateTime`, `LogAppendTime` (case-insensitive) |
| `retention.ms` | time-based retention |
| `retention.bytes` | size-based retention |
| `min.compaction.lag.ms` | minimum time before compaction |
| `max.compaction.lag.ms` | maximum time before compaction |

**Broker-level defaults**: `log.retention.ms` and `log.retention.bytes` can be
set as broker configs to provide defaults for new topics.

**Bufstream-specific topic configs** (not in Kafka):
- `bufstream.archive.*` -- controls archival to long-term storage (Flat,
  Parquet, Iceberg)
- `bufstream.archive.iceberg.*` -- Iceberg catalog/table configuration

**Configuration via**: Any Kafka API-compatible tool (AKHQ, Redpanda Console,
CLI), or `bufstream.yaml` configuration file, or Helm values.

### 2.4 Compression Codecs

Supported: `producer` (passthrough, default), `gzip`, `snappy`, `lz4`, `zstd`,
`uncompressed`.

Same set as Kafka. Configured per-topic via `compression.type`.

### 2.5 Message Timestamps

- Both `CreateTime` and `LogAppendTime` supported
- Configured per-topic via `timestamp.type` (case-insensitive)

### 2.6 Compatibility Documentation

Bufstream has a **formal "Supported APIs" page** at
`buf.build/docs/bufstream/kafka-compatibility/conformance/` that:
1. Lists all implemented endpoints
2. Lists feature areas NOT implemented (ACLs, quotas, delegation tokens,
   metrics)
3. Explains why broker-to-broker APIs are omitted
4. Lists supported topic configs
5. Lists supported admin APIs

This is the closest thing to a formal compatibility matrix among the brokers
surveyed.

**Jepsen verification**: Bufstream underwent independent Jepsen correctness
testing (jepsen.io/analyses/bufstream-0.1.0), which is a strong signal of
production readiness.

### 2.7 Client Libraries Tested

Bufstream's documentation shows examples with:
- **Go**: franz-go (primary)
- **Java**: Apache Kafka Java Client (official)

Their conformance test suite runs against "Kafka clients going back to version
3.2." They claim compatibility with "any Kafka client software, such as the
Apache Kafka reference clients, librdkafka, or franz-go."

---

## 3. Comparison Matrix

| Feature | Redpanda | Bufstream | kafka-light (proposed) |
|---|---|---|---|
| **Language** | C++ | Go | Go |
| **Deployment** | Multi-broker cluster | Multi-broker (leaderless, S3+Postgres) | Single broker |
| **Kafka version target** | Latest | 3.7 (latest versions, best-effort back to 3.2) | TBD |
| **Produce/Fetch/ListOffsets** | Yes | Yes | Phase 1 |
| **Metadata** | Yes | Yes | Phase 1 |
| **Consumer Groups** | Yes | Yes | Phase 2 |
| **Transactions/EOS** | Yes | Yes | Phase 6+ |
| **Idempotent Producers** | Yes | Yes (via transactions) | Phase 3+ |
| **Topic Compaction** | Yes | Yes | Phase 5+ |
| **ACLs** | Yes | Preview | Not planned initially |
| **SASL** | Yes (SCRAM, OAUTHBEARER, GSSAPI) | Yes (SASL, mTLS) | Phase 7 |
| **Quotas** | Yes | No | Not planned |
| **Delegation Tokens** | No (undocumented) | No | Not planned |
| **Compression** | All 4 codecs | All 4 codecs | All 4 codecs |
| **CreateTime** | Yes | Yes | Yes |
| **LogAppendTime** | Yes | Yes | Yes |
| **broker_timestamp** | Yes (Redpanda-specific) | No | Not planned |
| **Topic retention (time/size)** | Yes | Yes | Phase 4 |
| **Formal compatibility doc** | No (implicit "drop-in") | Yes (Supported APIs page) | Yes (planned) |
| **Jepsen tested** | No (not public) | Yes | Not planned |
| **Primary test clients** | Java, Go, Python, JS, Rust | Go (franz-go), Java | Go (franz-go) |

---

## 4. Recommendations for kafka-light

Based on this survey:

### 4.1 Publish a Supported APIs Page

Follow Bufstream's approach: maintain an explicit list of supported API keys,
supported topic configs, and known gaps. This is more honest and more useful than
Redpanda's "drop-in compatible" claim.

### 4.2 Minimum Viable API Surface

For a single-broker deployment, the minimum useful API set is:

**Phase 1 (connectivity + data path)**:
- ApiVersions, Metadata, Produce, Fetch, ListOffsets

**Phase 2 (topics + consumer groups)**:
- CreateTopics, DeleteTopics, FindCoordinator, JoinGroup, SyncGroup,
  Heartbeat, LeaveGroup, OffsetFetch, OffsetCommit, DescribeGroups, ListGroups

**Phase 3 (admin + idempotent)**:
- InitProducerId, DescribeConfigs, AlterConfigs/IncrementalAlterConfigs,
  CreatePartitions, DeleteRecords

### 4.3 Topic Config Scope

Start with Bufstream's minimal set:
- `cleanup.policy` (delete only initially, compact later)
- `compression.type` (all 4 codecs + producer + uncompressed)
- `retention.ms`, `retention.bytes`
- `message.timestamp.type` / `timestamp.type` (CreateTime + LogAppendTime)

### 4.4 Compression Strategy

Support all 4 codecs from day one. This is table stakes -- every Kafka-
compatible broker supports gzip, snappy, lz4, and zstd. Use Go's standard
libraries (compress/gzip, github.com/klauspost/compress for snappy/zstd,
github.com/pierrec/lz4).

### 4.5 Timestamp Strategy

- Default to `CreateTime` (same as both Redpanda and Bufstream)
- Support `LogAppendTime` as a topic config
- Do NOT implement Redpanda's `broker_timestamp` extension -- it's proprietary

### 4.6 Explicitly Document Non-Support

From day one, document:
- No multi-broker (by design -- single broker)
- No KRaft/ZooKeeper APIs
- No ACLs (initially)
- No SASL (initially)
- No quotas
- No delegation tokens
- No Kafka Connect (not a broker feature)
- No transactions (initially -- add later)

### 4.7 Test with franz-go

Both Redpanda and Bufstream test against franz-go as a primary Go client. Since
kafka-light is Go and uses franz-go's kmsg codec, testing with the franz-go
client is the natural choice. Bufstream's approach of testing against clients
back to Kafka 3.2 is a good minimum bar.
