---
title: Durability
description: What happens to your data when things go wrong.
---

klite acknowledges a produce request only after the data is fsync'd to the local WAL. This guarantees that acknowledged writes survive process crashes, OOM kills, and graceful shutdowns with zero data loss.

On top of that, each partition is flushed to S3 when it reaches 64 MB or every 60 seconds by default (whichever comes first). S3 is the long-term source of truth. The local disk is a durability buffer.

Most deployments already have reliable storage underneath. Cloud VMs use network-attached volumes (EBS, Hetzner Cloud Volumes, GCP Persistent Disk) that are replicated by the provider. Bare metal servers typically run RAID configurations. Losing a disk is uncommon. But even if it happens, the worst case is losing up to 60 seconds of unflushed data per partition. Everything already in S3 is safe. klite recovers automatically on restart from either the local disk or S3.

## Failure scenarios

### Graceful shutdown (SIGTERM, SIGINT)

No data loss. On shutdown, klite flushes all buffered partition data to S3, uploads the current metadata log to S3, fsyncs the local metadata log, and closes the WAL. Everything -- topic definitions, consumer group offsets, producer state, and record data -- is fully persisted before the process exits.

Startup after a graceful shutdown takes 1-2 seconds (metadata log and WAL replay). Kafka clients with retries enabled (the default in franz-go, librdkafka, and the Java client) reconnect automatically, so a restart during an upgrade typically results in only a few seconds of producer back-pressure.

### Process crash (kill -9, OOM)

No data loss. All acknowledged writes are already fsync'd to the WAL. On restart, klite replays its metadata log and WAL to rebuild state. This takes 1-2 seconds. Clients with retries configured reconnect automatically.

### Kernel panic or power loss

No acknowledged data is lost. Writes that were in the current fsync batch had not been acknowledged yet, so the client will retry them after reconnecting. On restart, the WAL is replayed and the last partial entry is detected by CRC check and truncated. Everything before it is intact.

### Disk loss

At most `--s3-flush-interval` of data is lost per partition (default 60 seconds). Under load it's typically less, since partitions also flush when they reach 64 MB. On restart, klite downloads the metadata log and record data from S3 and rebuilds automatically. Topics, configs, consumer group offsets, and producer state are all recovered.

## Reducing risk

**Cloud VMs.** Your storage is already replicated by the provider. EBS volumes, Hetzner Cloud Volumes, and GCP Persistent Disks all maintain multiple copies. This is the default for most deployments and requires no action.

**Bare metal with RAID-1.** Mirror your drives. This gives you redundancy with the best latency and throughput since writes go directly to local NVMe without a network hop.

**Lower the flush interval.** Set `--s3-flush-interval` to 10s or 5s to reduce the window of data at risk during a disk loss. This increases S3 PUT requests slightly but S3 PUTs are cheap ($0.005 per 1,000 requests).
