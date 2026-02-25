---
title: Migrating from Kafka
description: How to migrate from Apache Kafka to klite using MirrorMaker 2.
---

klite speaks the standard Kafka wire protocol, so migrating from an existing Kafka cluster is straightforward. The recommended approach is to use **MirrorMaker 2** (MM2) to replicate data from your Kafka cluster to klite, then cut over your clients.

## Migration strategies

| Strategy | Downtime | Complexity | Best for |
|----------|----------|------------|----------|
| **MirrorMaker 2** | Zero | Medium | Production workloads with data |
| **Client cutover** | Brief | Low | Dev/staging, or when history isn't needed |
| **Dual-write** | Zero | High | Not recommended (split-brain risk) |

## Strategy 1: MirrorMaker 2 (recommended)

MirrorMaker 2 is Apache Kafka's built-in replication tool. It continuously mirrors topics, consumer group offsets, and topic configurations from one cluster to another.

### Prerequisites

- Apache Kafka 2.7+ (for MM2 with offset sync)
- MirrorMaker 2 (included with Kafka distribution)
- Both the source Kafka cluster and klite must be reachable from the MM2 process

### Step 1: Start klite

Start klite alongside your existing Kafka cluster:

```bash
./klite --listen :9093 --data-dir ./klite-data
```

Use a different port if running on the same machine as Kafka.

### Step 2: Configure MirrorMaker 2

Create a `mm2.properties` file:

```properties
# Cluster aliases
clusters = source, target

# Source: your existing Kafka cluster
source.bootstrap.servers = kafka-broker-1:9092,kafka-broker-2:9092

# Target: klite
target.bootstrap.servers = localhost:9093

# Replication flow: source -> target
source->target.enabled = true
source->target.topics = .*
# Exclude internal topics
source->target.topics.exclude = __consumer_offsets, __transaction_state, __.*, .*[\-\.]internal

# Sync consumer group offsets
source->target.sync.group.offsets.enabled = true
source->target.sync.group.offsets.interval.seconds = 10

# Preserve topic names (no prefix)
replication.policy.class = org.apache.kafka.connect.mirror.IdentityReplicationPolicy

# Replication settings
source->target.replication.factor = 1
refresh.topics.interval.seconds = 30
refresh.groups.interval.seconds = 30
emit.heartbeats.enabled = true
emit.checkpoints.enabled = true
```

### Step 3: Run MirrorMaker 2

```bash
# Using the Kafka distribution's connect-mirror-maker script
bin/connect-mirror-maker.sh mm2.properties
```

MM2 will:
1. Discover all topics on the source cluster
2. Create corresponding topics on klite
3. Replicate all existing data
4. Continuously replicate new messages
5. Sync consumer group offsets

### Step 4: Monitor replication lag

```bash
# Check replication lag via MM2 metrics
# MM2 emits JMX metrics; check source-target lag

# Or compare offsets manually:
# Source cluster
kcat -b kafka-broker-1:9092 -L -t my-topic

# Target (klite)
kcat -b localhost:9093 -L -t my-topic
```

Wait until replication lag is consistently zero before proceeding.

### Step 5: Cut over clients

Once replication is caught up:

1. **Update consumer group offsets** are already synced by MM2
2. **Point consumers** to klite:
   ```
   bootstrap.servers = klite-host:9092
   ```
3. **Point producers** to klite:
   ```
   bootstrap.servers = klite-host:9092
   ```
4. **Stop MirrorMaker 2** once all clients have cut over
5. **Decommission Kafka** when you've verified everything works

### Step 6: Verify

```bash
# Produce to klite
echo "post-migration test" | kcat -P -b klite-host:9092 -t my-topic

# Consume from klite
kcat -C -b klite-host:9092 -t my-topic -e
```

## Strategy 2: Client cutover (simple)

If you don't need historical data, just point your clients to klite:

1. Stop producers
2. Drain consumers (process remaining messages from Kafka)
3. Start klite
4. Update `bootstrap.servers` in all clients to point to klite
5. Restart producers and consumers

```bash
# Before: clients point to Kafka
bootstrap.servers = kafka-broker-1:9092

# After: clients point to klite
bootstrap.servers = klite-host:9092
```

Topics will be auto-created when producers send their first messages (if `--auto-create-topics` is enabled, which is the default).

## Client compatibility

klite is wire-compatible with Kafka 4.0+ protocol. These client libraries work without code changes:

| Client | Language | Tested |
|--------|----------|--------|
| franz-go | Go | Yes |
| confluent-kafka-go | Go | Yes |
| librdkafka | C/C++ | Yes |
| confluent-kafka-python | Python | Yes |
| KafkaJS | Node.js | Yes |
| kafka-clients (Java) | Java | Yes |

The only change needed is updating `bootstrap.servers` to point to klite.

## What to watch out for

### Features not supported by klite

klite intentionally does not support some Kafka features. Check [Compatibility](/concepts/compatibility/) for the full list. Key differences:

- **No multi-broker clustering** -- klite is a single broker
- **No KRaft/ZooKeeper** -- no controller quorum
- **No Kafka Connect** -- use standalone connectors or alternatives
- **No Kafka Streams server-side** -- Kafka Streams clients work fine (they're just producers/consumers)
- **Message format v2 only** -- very old clients using v0/v1 format won't work

### Consumer group offsets

When using MM2 with `sync.group.offsets.enabled = true`, consumer groups will resume from their last committed offset. Verify this after cutover:

```bash
# Check committed offsets on klite
kcat -b klite-host:9092 -G my-group my-topic
```

### Topic configuration

MM2 replicates topic configurations. Verify that klite supports the configs your topics use. See [Supported topic configs](/reference/topic-configs/) for the full list.

## Rollback plan

If you need to switch back to Kafka:

1. Set up MM2 in reverse (`klite -> kafka`)
2. Or simply update `bootstrap.servers` back to Kafka (you'll lose messages produced to klite after cutover unless you mirror them back)

## Next steps

- [Compatibility](/concepts/compatibility/) -- full API and feature compatibility matrix
- [Architecture](/concepts/architecture/) -- how klite differs from Kafka
- [Configuration](/guides/configuration/) -- tune klite for your workload
