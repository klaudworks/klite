---
title: S3 Compatibility
description: S3 API operations used by klite and compatible storage backends.
---

Any S3-compatible store works -- AWS S3, MinIO, SeaweedFS, Garage, Cloudflare R2, Google Cloud Storage (S3-compatible mode), and others. Set `--s3-endpoint` to point klite at non-AWS backends; path-style addressing is enabled automatically when an endpoint is set.

## S3 API operations used

klite uses the AWS SDK for Go v2. Only these operations are needed:

- `PutObject` -- upload WAL segments and metadata snapshots
- `GetObject` -- fetch segments for cold reads (supports byte-range reads)
- `HeadObject` -- check object existence and size
- `ListObjectsV2` -- enumerate segments for a partition
- `DeleteObject` -- remove segments past retention

## Credentials

klite uses the standard AWS credential chain: `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` environment variables, IAM roles, instance profiles, and IRSA all work.

See [Configuration](/guides/configuration/#s3-storage) for all S3-related flags.
