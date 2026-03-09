---
title: Configuration
description: How to configure klite -- networking, storage, S3, authentication, and tuning.
---

klite is configured via command-line flags or environment variables. Environment variables use the `KLITE_` prefix and are overridden by explicit flags. Both work in Docker:

```bash
docker run --network host \
  -e KLITE_LOG_LEVEL=debug \
  ghcr.io/klaudworks/klite --default-partitions 3
```

## Networking

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--listen` | `KLITE_LISTEN` | `:9092` | Address and port to bind to. Use `:9092` for all interfaces, or `127.0.0.1:9092` for localhost only. |
| `--advertised-addr` | `KLITE_ADVERTISED_ADDR` | *(derived)* | Address clients use to reconnect after metadata discovery. See below. |
| `--health-addr` | `KLITE_HEALTH_ADDR` | *(disabled)* | HTTP server for `/livez` and `/readyz` health checks. Set to `:8080` to enable. |

### Advertised address

The advertised address is what klite tells Kafka clients to connect to. Kafka clients first connect to any broker, receive a metadata response containing the advertised address, then reconnect to that address for all subsequent requests. Getting this wrong is the most common source of connection issues.

**When you need to set this:**

- **Docker with `--network host`** -- not needed, `localhost` works naturally.
- **Docker with port mapping (`-p 9092:9092`)** -- the container's internal hostname differs from the host. Use `--advertised-addr localhost:9092` so host-side clients can reconnect.
- **Kubernetes** -- set to the pod's headless service address, e.g. `klite-0.klite.default.svc.cluster.local:9092`.
- **Behind a load balancer or NAT** -- set to the external address clients use.

If unset, klite derives it from `--listen`. When listening on `0.0.0.0` or `::`, it defaults to `localhost:<port>` and logs a warning.

## Storage

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--data-dir` | `KLITE_DATA_DIR` | `./data` | Directory for WAL segments, metadata log, and `meta.properties`. Created automatically on first start. |
| `--wal-max-disk-size` | `KLITE_WAL_MAX_DISK_SIZE` | `1073741824` | Maximum WAL size on disk in bytes (default 1 GiB). Oldest segments are removed when this limit is reached. |

When running in Docker without S3, the data directory lives inside the container and is lost when it stops. For durable storage, configure an S3 backend.

### S3 storage

When an S3 bucket is configured, klite periodically flushes WAL data to S3 for durable storage. The local WAL acts as a write-ahead buffer -- writes are acknowledged immediately and flushed in the background.

Any S3-compatible store works: AWS S3, SeaweedFS, MinIO, Garage, etc.

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--s3-bucket` | `KLITE_S3_BUCKET` | *(none)* | Bucket name. If unset, klite runs in local-only mode. |
| `--s3-region` | `KLITE_S3_REGION` | `us-east-1` | AWS region. |
| `--s3-endpoint` | `KLITE_S3_ENDPOINT` | *(none)* | Custom endpoint for S3-compatible stores. |
| `--s3-prefix` | `KLITE_S3_PREFIX` | `klite/<clusterID>` | Key prefix for all objects. |
| `--s3-flush-interval` | `KLITE_S3_FLUSH_INTERVAL` | `60s` | Max age of unflushed partition data before flush. A partition also flushes when it reaches 64 MB. |

**Credentials** use the standard AWS environment variables: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and optionally `AWS_SESSION_TOKEN`. IAM roles, instance profiles, and IRSA work too.

When `--s3-endpoint` is set, path-style addressing is enabled automatically (required by most S3-compatible stores).

The flush interval and size threshold together determine your recovery point objective (RPO). With the defaults (60s / 64 MB per partition), at most 60 seconds of data per partition is at risk if the disk is lost before the next flush. Lower the interval for tighter RPO at the cost of more S3 PUT requests.

### S3 compaction

Compaction rewrites overlapping S3 objects for topics using `cleanup.policy=compact`, removing superseded keys and expired tombstones.

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--compaction-check-interval` | *(flag only)* | `2m` | How often klite scans partition dirty counters for compaction eligibility. |
| `--compaction-min-dirty-objects` | *(flag only)* | `16` | Minimum dirty objects before a partition is considered for compaction. |
| `--compaction-read-rate` | *(flag only)* | `52428800` | Max S3 read throughput for compaction in bytes/sec (50 MiB/s). Set `0` for unlimited. |

Tuning guidance:

- **Low-traffic clusters**: use lower thresholds (for example, `--compaction-check-interval=30s` and `--compaction-min-dirty-objects=4`) to reclaim space sooner.
- **High-throughput clusters**: keep or raise defaults (for example, `2m`/`16` or `5m`/`32`) to reduce constant background compaction pressure.
- **Fetch latency sensitive workloads**: keep `--compaction-read-rate` capped so compaction reads do not contend with client fetches.

## Topics

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--default-partitions` | `KLITE_DEFAULT_PARTITIONS` | `1` | Partition count for auto-created topics. |
| `--auto-create-topics` | `KLITE_AUTO_CREATE_TOPICS` | `true` | Create topics on first Produce or Metadata request. Set to `false` to require explicit `CreateTopics`. |

See [Topic configuration](/reference/topic-configs/) for per-topic overrides like retention.

## Authentication

klite supports SASL for client authentication. When enabled, all connections must authenticate before sending any other request.

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--sasl-enabled` | `KLITE_SASL_ENABLED` | `false` | Require SASL authentication. |
| `--sasl-mechanism` | `KLITE_SASL_MECHANISM` | `PLAIN` | Mechanism: `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`. |
| `--sasl-user` | `KLITE_SASL_USER` | *(none)* | Bootstrap admin username. |
| `--sasl-password` | `KLITE_SASL_PASSWORD` | *(none)* | Bootstrap admin password. |

## Broker identity

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--cluster-id` | `KLITE_CLUSTER_ID` | *(auto-generated)* | Kafka cluster ID. Generated on first start and persisted in `meta.properties`. |
| `--node-id` | `KLITE_NODE_ID` | `0` | Broker node ID reported in Metadata responses. |
| `--log-level` | `KLITE_LOG_LEVEL` | `info` | Log verbosity: `debug`, `info`, `warn`, `error`. |
