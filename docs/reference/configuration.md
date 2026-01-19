---
title: Configuration
description: Complete klite configuration reference -- all CLI flags and environment variables.
---

klite can be configured via command-line flags or environment variables. Environment variables use the `KLITE_` prefix and are overridden by explicit flags.

## Usage

```bash
# Flags
./klite --listen :9092 --data-dir /var/lib/klite --log-level info

# Environment variables
export KLITE_LISTEN=:9092
export KLITE_DATA_DIR=/var/lib/klite
export KLITE_LOG_LEVEL=info
./klite

# Mixed (flags take precedence)
KLITE_LOG_LEVEL=debug ./klite --listen :9093
```

## Broker settings

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--listen` | `KLITE_LISTEN` | `:9092` | Address and port to listen on. Use `:9092` for all interfaces or `127.0.0.1:9092` for localhost only. |
| `--advertised-addr` | `KLITE_ADVERTISED_ADDR` | *(derived from --listen)* | Address clients use to connect. Important when running behind NAT, in Docker, or in Kubernetes. If unset, derived from the listen address. |
| `--data-dir` | `KLITE_DATA_DIR` | `./data` | Directory for WAL segments, metadata log, and `meta.properties`. Created automatically if it doesn't exist. |
| `--cluster-id` | `KLITE_CLUSTER_ID` | *(auto-generated UUID)* | Kafka cluster ID. Auto-generated on first start and persisted in `meta.properties`. Set this to use a deterministic ID. |
| `--node-id` | `KLITE_NODE_ID` | `0` | Broker node ID. Reported in Metadata and FindCoordinator responses. |
| `--log-level` | `KLITE_LOG_LEVEL` | `info` | Log level: `debug`, `info`, `warn`, `error`. |

## Topic defaults

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--default-partitions` | `KLITE_DEFAULT_PARTITIONS` | `1` | Default number of partitions for auto-created topics. |
| `--auto-create-topics` | `KLITE_AUTO_CREATE_TOPICS` | `true` | Auto-create topics on Produce or Metadata requests. Set to `false` to require explicit topic creation. |

## S3 storage

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--s3-bucket` | `KLITE_S3_BUCKET` | *(none)* | S3 bucket for durable storage. If unset, klite runs in WAL-only mode. |
| `--s3-region` | `KLITE_S3_REGION` | `us-east-1` | AWS region for the S3 bucket. |
| `--s3-endpoint` | `KLITE_S3_ENDPOINT` | *(none)* | Custom S3 endpoint URL. Use for MinIO, LocalStack, R2, or other S3-compatible stores. |
| `--s3-flush-interval` | `KLITE_S3_FLUSH_INTERVAL` | `60s` | How often to flush WAL segments to S3. |

<!-- TODO: Add S3 path prefix, S3 force path style flags if/when implemented -->

## SASL authentication

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--sasl-enabled` | `KLITE_SASL_ENABLED` | `false` | Enable SASL authentication. When enabled, unauthenticated connections are rejected. |
| `--sasl-mechanism` | `KLITE_SASL_MECHANISM` | `PLAIN` | SASL mechanism: `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`. |

<!-- TODO: Document SASL user management flags/API when finalized -->

## Retention

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--retention-ms` | `KLITE_RETENTION_MS` | `604800000` (7 days) | Default retention time for topics. `-1` for infinite. |
| `--retention-bytes` | `KLITE_RETENTION_BYTES` | `-1` (infinite) | Default per-partition size limit. |
| `--retention-check-interval` | `KLITE_RETENTION_CHECK_INTERVAL` | `300s` | How often to check for expired segments. |

<!-- TODO: Verify these flags match the actual implementation -->

## Examples

### Development

```bash
./klite
```

Uses all defaults: listens on `:9092`, stores data in `./data`, auto-creates topics.

### Production (local storage only)

```bash
./klite \
  --listen 0.0.0.0:9092 \
  --advertised-addr kafka.example.com:9092 \
  --data-dir /var/lib/klite \
  --default-partitions 6 \
  --log-level info \
  --retention-ms 604800000
```

### Production (with S3)

```bash
./klite \
  --listen 0.0.0.0:9092 \
  --advertised-addr kafka.example.com:9092 \
  --data-dir /var/lib/klite \
  --default-partitions 6 \
  --s3-bucket my-klite-data \
  --s3-region us-west-2 \
  --s3-flush-interval 60s \
  --retention-ms 2592000000
```

### Docker

```bash
docker run -p 9092:9092 \
  -e KLITE_DATA_DIR=/data \
  -e KLITE_DEFAULT_PARTITIONS=3 \
  -e KLITE_S3_BUCKET=my-bucket \
  -e KLITE_S3_REGION=us-east-1 \
  -v klite-data:/data \
  ghcr.io/klaudworks/klite
```

### Kubernetes (via Helm values)

```yaml
config:
  listenAddr: ":9092"
  dataDir: "/data"
  logLevel: "info"
  defaultPartitions: 6
  autoCreateTopics: true

s3:
  enabled: true
  bucket: my-klite-data
  region: us-west-2
```

## Environment variable reference

All environment variables at a glance:

| Variable | Corresponding Flag |
|----------|-------------------|
| `KLITE_LISTEN` | `--listen` |
| `KLITE_ADVERTISED_ADDR` | `--advertised-addr` |
| `KLITE_DATA_DIR` | `--data-dir` |
| `KLITE_CLUSTER_ID` | `--cluster-id` |
| `KLITE_NODE_ID` | `--node-id` |
| `KLITE_LOG_LEVEL` | `--log-level` |
| `KLITE_DEFAULT_PARTITIONS` | `--default-partitions` |
| `KLITE_AUTO_CREATE_TOPICS` | `--auto-create-topics` |
| `KLITE_S3_BUCKET` | `--s3-bucket` |
| `KLITE_S3_REGION` | `--s3-region` |
| `KLITE_S3_ENDPOINT` | `--s3-endpoint` |
| `KLITE_S3_FLUSH_INTERVAL` | `--s3-flush-interval` |
| `KLITE_SASL_ENABLED` | `--sasl-enabled` |
| `KLITE_SASL_MECHANISM` | `--sasl-mechanism` |
| `KLITE_RETENTION_MS` | `--retention-ms` |
| `KLITE_RETENTION_BYTES` | `--retention-bytes` |
| `KLITE_RETENTION_CHECK_INTERVAL` | `--retention-check-interval` |

AWS credentials for S3 use the standard `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_SESSION_TOKEN` environment variables (or IAM roles, instance profiles, IRSA, etc.).

## Next steps

- [Quickstart](/guides/getting-started/) -- get started quickly
- [Docker](/guides/docker/) -- containerized deployment
- [Kubernetes & Helm](/guides/kubernetes/) -- Helm chart configuration
- [Topic configs](/reference/topic-configs/) -- per-topic configuration
