---
title: klite
description: A Kafka-compatible broker in a single Go binary.
template: splash
hero:
  tagline: A Kafka-compatible broker in a single Go binary. No ZooKeeper. No KRaft. Just one process.
  actions:
    - text: Get Started
      link: /guides/getting-started/
      icon: right-arrow
    - text: GitHub
      link: https://github.com/kliteio/klite
      icon: external
      variant: minimal
---

## Up and running in 5 seconds

```bash
# Download
curl -L https://github.com/kliteio/klite/releases/latest/download/klite-$(uname -s | tr A-Z a-z)-$(uname -m) -o klite && chmod +x klite

# Start
./klite

# Produce & consume (in another terminal)
echo "hello klite" | kcat -P -b localhost:9092 -t my-topic
kcat -C -b localhost:9092 -t my-topic -e
```

Or with Docker:

```bash
docker run -p 9092:9092 ghcr.io/kliteio/klite
```

## Why klite?

| | klite | Apache Kafka | Redpanda |
|---|---|---|---|
| **Deployment** | Single binary | 3+ brokers + controllers | Single binary (C++) |
| **Dependencies** | None | ZooKeeper or KRaft | None |
| **Resource usage** | ~20 MB RAM idle | 1+ GB RAM per broker | 1+ GB RAM |
| **Storage** | WAL + S3 | Local disks | Local disks |
| **Wire compatible** | Kafka 4.0+ protocol | N/A | Kafka protocol |
| **Best for** | Dev, staging, small prod | Large-scale production | Medium-to-large prod |

## Features

- **Drop-in Kafka replacement** -- Any Kafka client (franz-go, librdkafka, Java, Python) works without code changes
- **Consumer groups** -- Full group coordination with JoinGroup, SyncGroup, Heartbeat, and rebalancing
- **Transactions & idempotency** -- Exactly-once semantics with transactional produce and consume
- **S3 tiered storage** -- WAL for hot data, automatic flush to S3 for durability and cost efficiency
- **SASL authentication** -- PLAIN and SCRAM-SHA-256/512 authentication
- **Zero configuration** -- Starts with sane defaults, configure only what you need
