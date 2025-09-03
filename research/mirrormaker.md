# Kafka MirrorMaker 2 (MM2)

Research on using MirrorMaker 2 to migrate data to/from a Kafka-compatible
broker like kafka-light.

Sources:
- https://kafka.apache.org/documentation/#georeplication
- https://github.com/AutoMQ/automq/wiki/Kafka-MirrorMaker-2(MM2):-Usages-&-Best-Practices
- https://developers.redhat.com/articles/2023/11/13/demystifying-kafka-mirrormaker-2-use-cases-and-architecture
- https://bigdataboutique.com/blog/kafka-mirrormaker-2-deployment-gotchas-disaster-recovery
- https://www.automq.com/blog/beyond-mm2-kafka-migration-with-zero-downtime

---

## What MM2 Is

MirrorMaker 2 is a data replication tool that ships with Apache Kafka. It copies
topic data and consumer group offsets between two Kafka-compatible clusters. It
is built on top of Kafka Connect and runs three specialized connectors:

| Connector | Purpose |
|---|---|
| MirrorSourceConnector | Replicates topic data (records) from source to target |
| MirrorCheckpointConnector | Syncs consumer group offsets between clusters |
| MirrorHeartbeatConnector | Emits heartbeat records for replication health monitoring |

From the broker's perspective, MM2 is just a regular Kafka client -- it consumes
from one cluster and produces to another. There is no broker-side MM2 support
needed.

---

## Deployment

### Architecture

MM2 is a separate process that sits between two clusters. It does not run inside
either broker.

```
┌──────────────┐       ┌─────────────────┐       ┌──────────────┐
│  Source       │       │      MM2        │       │  Target      │
│  Cluster     │◄──────│  (consumer +    │──────►│  Cluster     │
│              │ fetch │   producer)     │produce│              │
└──────────────┘       └─────────────────┘       └──────────────┘
```

Best practice: deploy MM2 close to the **target** cluster (same network/region).
Rationale: "remote consume, local produce" -- reads tolerate latency better than
writes, and MM2's internal state topics live on the target cluster.

### Standalone Mode (simplest)

MM2 ships as part of the standard Apache Kafka distribution. No separate Kafka
Connect cluster required. A single properties file + one command:

```properties
# mirror-maker.properties
clusters = source, target
source.bootstrap.servers = old-kafka:9092
target.bootstrap.servers = kafka-light:9092

source->target.enabled = true
source->target.topics = .*

tasks.max = 10
```

```bash
./bin/connect-mirror-maker.sh ./config/mirror-maker.properties
```

This starts a Kafka Connect worker in-process and deploys the three MM2
connectors into it. Suitable for testing and smaller migrations.

### Distributed Mode (production)

For production, run MM2 as a distributed Kafka Connect cluster -- multiple
worker processes for fault tolerance and throughput. Same script with cluster
config, or deploy MM2's connectors into an existing Kafka Connect cluster.

### Managed Services

Cloud providers offer managed MM2:
- AWS MSK Connect
- Aiven MirrorMaker
- Azure HDInsight (built-in MM2 support)

---

## Required Broker APIs

MM2 uses standard Kafka consumer and producer protocols. The APIs required
depend on whether our broker is the source (being migrated from) or target
(being migrated to).

### Common (both directions)

| API Key | Name | Purpose |
|---------|------|---------|
| 18 | ApiVersions | Negotiate supported protocol versions |
| 3 | Metadata | Discover topics, partitions, broker addresses |

### Source Side (kafka-light as source -- migrate away from us)

MM2 runs a consumer against our broker. Required APIs:

| API Key | Name | Purpose |
|---------|------|---------|
| 2 | ListOffsets | Find earliest/latest offsets per partition |
| 1 | Fetch | Read records from partitions |
| 10 | FindCoordinator | Locate the group coordinator broker |
| 11 | JoinGroup | Join a consumer group |
| 14 | SyncGroup | Receive partition assignment from group leader |
| 12 | Heartbeat | Maintain group membership |
| 9 | OffsetFetch | Read previously committed offsets |
| 8 | OffsetCommit | Commit consumed offsets |

### Target Side (kafka-light as target -- migrate into us)

MM2 runs a producer against our broker and creates internal topics. Required
APIs:

| API Key | Name | Purpose |
|---------|------|---------|
| 0 | Produce | Write records to topic partitions |
| 19 | CreateTopics | Create target topics + MM2 internal topics |
| 10 | FindCoordinator | For Connect's own consumer group coordination |
| 11 | JoinGroup | Connect worker group membership |
| 14 | SyncGroup | Connect worker task assignment |
| 12 | Heartbeat | Connect worker liveness |
| 8 | OffsetCommit | Connect offset management |
| 9 | OffsetFetch | Connect offset management |

### Internal Topics Created by MM2

When replicating `source -> target`, MM2 creates these topics on the target
cluster:

| Topic | Purpose |
|---|---|
| `mm2-offset-syncs.<target>.internal` | Maps source offsets to target offsets |
| `mm2-configs.<target>.internal` | Connector configuration storage (Connect) |
| `mm2-offsets.<target>.internal` | Connector offset storage (Connect) |
| `mm2-status.<target>.internal` | Connector status storage (Connect) |
| `<source>.<topic>` | Replicated topics (prefixed with source cluster alias) |
| `<source>.checkpoints.internal` | Translated consumer group offsets |
| `<source>.heartbeats` | Heartbeat records for monitoring |

This means our CreateTopics handler must work correctly, and we need to handle
the topic naming with dots and hyphens.

---

## Mapping to kafka-light Implementation Phases

All required APIs fall within phases 1-3 of our implementation plan:

| Phase | APIs Covered | MM2 Requirement |
|---|---|---|
| Phase 1: Connectivity | ApiVersions, Metadata | Broker discovery, version negotiation |
| Phase 2: Core Data Path | Produce, Fetch, ListOffsets, CreateTopics | Read/write records, create topics |
| Phase 3: Consumer Groups | FindCoordinator, JoinGroup, SyncGroup, Heartbeat, OffsetCommit, OffsetFetch | Consumer group coordination for MM2 + Connect |

**Conclusion:** Once phases 1-3 are complete, MM2 migration works in both
directions with no additional effort.

---

## Real-World Precedent

Other Kafka-compatible systems that support MM2 migration:

| System | Compatibility | Notes |
|---|---|---|
| Redpanda | Full MM2 support | C++ broker, wire-compatible with Kafka protocol |
| AutoMQ | Full MM2 support | Kafka fork with S3 storage; also offers custom "Kafka Linking" for zero-downtime migration |
| WarpStream | Basic MM2 support | ~35% API coverage; some edge cases may not work |

The pattern is clear: if you implement the core Kafka protocol (produce, fetch,
consumer groups), MM2 works out of the box. No special broker-side code needed.

---

## Migration Scenarios

### Migrate from Apache Kafka to kafka-light

1. Deploy kafka-light, create matching topics (or let MM2 auto-create them)
2. Configure MM2 with Kafka as source, kafka-light as target
3. MM2 replicates all topic data and consumer group offsets
4. Switch producers to point at kafka-light
5. Wait for consumers to drain remaining data from Kafka
6. Switch consumers to kafka-light (offsets already synced by MM2)
7. Shut down MM2

### Migrate from kafka-light to Apache Kafka

Same process in reverse. This is important -- users are not locked in. They can
move back to standard Kafka at any time using the same tool.

### Zero-Downtime Migration Pattern

For zero downtime, the recommended sequence is:

1. Start MM2 replication (source -> target)
2. Wait until replication lag is minimal
3. Stop producers on source
4. Wait for MM2 to drain remaining records
5. Verify offsets are synced via MirrorCheckpointConnector
6. Switch producers to target
7. Switch consumers to target (using translated offsets)
8. Stop MM2

The critical window (step 3-6) is typically seconds to minutes depending on
replication lag.

---

## Key Takeaway for kafka-light

MM2 compatibility is a **free benefit** of being Kafka wire-compatible. We don't
need to build any migration tooling -- the Kafka ecosystem provides it. Users
download the standard Apache Kafka distribution, write a config file, and run
one command. This significantly lowers the adoption barrier: users know they can
try kafka-light and migrate back if needed.
