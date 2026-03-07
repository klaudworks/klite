---
title: Design Philosophy
description: Why klite exists and the problems it solves.
---

Kafka is a fantastic piece of infrastructure. It's a message queue where nothing gets thrown away. Every event is persisted, replayable, and auditable. Once you've used it, it's hard to go back to fire-and-forget messaging.

The problem is everything around it. Setting up Kafka means running a cluster of brokers, a controller quorum, and tuning JVM heap sizes. It needs a lot of RAM just to idle. Backing up Kafka data is its own challenge. There are entire companies built around solving just that. Every datastore you run is a liability, and Kafka is one of the heavier ones to operate.

We kept running into projects where Kafka would have been the right tool, but the effort of setting it up and keeping it running just wasn't worth it. klite closes that gap. You get the Kafka protocol, the durability, the replay, without the operational baggage. And because it speaks the same wire protocol, migrating away is straightforward. If you outgrow it, point your clients at a real Kafka cluster and you're done.

## Goals

**Single binary, small footprint.** A single binary shipped in a lightweight container (currently 15 MB).

**Kafka protocol compatible.** Use any existing Kafka client library. Start small, and if you ever outgrow klite, switch to full Kafka without changing a line of code.

**Good enough performance.** Sustains 550K+ messages per second on a single machine with standard gp3 storage. That's more than enough for the vast majority of workloads. See [benchmarks](/performance/benchmarks/) for real numbers.

**Durable by default.** Every event is persisted to disk immediately and written to S3 within seconds. S3 is the single source of truth. No backup tooling, no snapshots to manage.

## What klite is not

Running Kafka on a single server has a few advantages, but it also comes with tradeoffs:

**Hardware and network.** klite gets the most out of your hardware, but it is still designed for a single server. It won't replace a full Kafka cluster doing millions of messages per second across a fleet of machines. If you need that, use Kafka. It's great at it.

**Single-server by default.** klite runs as a single process. For high availability, an optional [standby replica](/concepts/replication/) provides automatic failover with zero data loss. The standby streams the primary's WAL in real time and promotes automatically on crashes or rolling updates. Failover takes ~2 seconds on graceful shutdown, ~17 seconds on a crash.

