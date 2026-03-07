---
title: Kubernetes & Helm
description: Deploy klite to Kubernetes using the official Helm chart.
---

The Helm chart deploys klite as a single-replica Deployment with `Recreate` strategy, a PVC for the WAL, and a ClusterIP Service on port 9092.

## Prerequisites

- Kubernetes 1.24+
- Helm 3.8+ (OCI registry support)

## Install

The chart is published as an OCI artifact on GitHub Container Registry:

```bash
export KLITE_VERSION=$(curl -s https://api.github.com/repos/klaudworks/klite/releases/latest | grep '"tag_name"' | cut -d'"' -f4 | sed 's/^v//')

helm install klite oci://ghcr.io/klaudworks/charts/klite --version "$KLITE_VERSION"
```

Verify:

```bash
kubectl get pods -l app.kubernetes.io/name=klite
kubectl logs -l app.kubernetes.io/name=klite -f
```

## Connect

In-cluster clients can use the service DNS name directly:

```
klite.<namespace>.svc.cluster.local:9092
```

The chart automatically sets `--advertised-addr` to the service FQDN, so Metadata responses point clients to the right address.

For local development, use [mirrord](https://mirrord.dev) to access cluster-internal services from your machine. The bootstrap address must match the broker's advertised address (the chart defaults to `<service>.<namespace>.svc.cluster.local`):

```bash
mirrord exec -a <namespace> -- kcat -b klite.<namespace>.svc.cluster.local:9092 -L
```

## Configuration

Key values (see [`charts/klite/values.yaml`](https://github.com/klaudworks/klite/blob/main/charts/klite/values.yaml) for the full reference):

```yaml
image:
  repository: klaudworks/klite
  tag: ""  # Defaults to the chart's appVersion (matches the release)

broker:
  listen: ":9092"
  dataDir: /data
  logLevel: info
  defaultPartitions: 1
  autoCreateTopics: true

s3:
  bucket: ""          # Set to enable S3 storage
  region: ""
  endpoint: ""        # For MinIO / S3-compatible stores
  existingSecret: ""  # Secret with AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY

persistence:
  enabled: true
  size: 10Gi

resources: {}
  # requests:
  #   cpu: 250m
  #   memory: 512Mi
  # limits:
  #   memory: 1Gi
```

Override values inline or with a file:

```bash
helm install klite oci://ghcr.io/klaudworks/charts/klite --version "$KLITE_VERSION" \
  --set broker.defaultPartitions=6 \
  --set persistence.size=50Gi

# Or with a values file
helm install klite oci://ghcr.io/klaudworks/charts/klite --version "$KLITE_VERSION" -f my-values.yaml
```

## S3 backend

S3 is enabled by setting `s3.bucket`. Credentials can come from an existing Secret or from pod identity (IRSA, Workload Identity).

### Static credentials

```bash
kubectl create secret generic klite-s3 \
  --from-literal=AWS_ACCESS_KEY_ID=AKIA... \
  --from-literal=AWS_SECRET_ACCESS_KEY=...
```

```yaml
s3:
  bucket: my-klite-data
  region: us-west-2
  existingSecret: klite-s3
```

### IRSA (EKS)

```yaml
serviceAccount:
  annotations:
    eks.amazonaws.com/role-arn: arn:aws:iam::123456789:role/klite-s3-role

s3:
  bucket: my-klite-data
  region: us-west-2
```

### MinIO / S3-compatible

```yaml
s3:
  bucket: klite
  endpoint: http://minio.minio.svc.cluster.local:9000
  existingSecret: minio-credentials
```

## Upgrading

```bash
helm upgrade klite oci://ghcr.io/klaudworks/charts/klite --version "$KLITE_VERSION"
```

klite replays its WAL on startup, so upgrades are safe. The pod shuts down gracefully on SIGTERM.

## Uninstalling

```bash
helm uninstall klite
```

The PVC is preserved by default. To delete data too:

```bash
kubectl delete pvc -l app.kubernetes.io/name=klite
```

For common issues, see [Troubleshooting](/guides/troubleshooting/).
