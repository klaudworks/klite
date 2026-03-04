---
title: Configuration
description: klite configuration reference.
---

# Configuration

klite can be configured via command-line flags or environment variables.

## Broker Settings

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--listen` | `KLITE_LISTEN` | `:9092` | Broker listen address |
| `--data-dir` | `KLITE_DATA_DIR` | `./data` | Data directory for WAL and metadata |
| `--log-level` | `KLITE_LOG_LEVEL` | `info` | Log level (debug, info, warn, error) |

## S3 Storage

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--s3-bucket` | `KLITE_S3_BUCKET` | _(none)_ | S3 bucket for durable storage |
| `--s3-region` | `KLITE_S3_REGION` | `us-east-1` | AWS region |
| `--s3-endpoint` | `KLITE_S3_ENDPOINT` | _(none)_ | Custom S3 endpoint (for MinIO, LocalStack, etc.) |
