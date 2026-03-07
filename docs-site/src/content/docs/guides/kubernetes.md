---
title: Kubernetes & Helm
description: Deploy klite to Kubernetes using the official Helm chart.
---

The Helm chart supports two modes:

- **Single-broker** (default) — a single-replica Deployment with `Recreate` strategy and a PVC for the WAL.
- **Replicated** — an active-passive StatefulSet with synchronous streaming replication and automatic failover, backed by S3 for lease fencing.

Both modes expose a ClusterIP Service on port 9092.

## Prerequisites

- Kubernetes 1.24+
- Helm 3.8+ (OCI registry support)
- An S3-compatible bucket (AWS S3, MinIO, etc.)

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

The chart automatically sets `--advertised-addr` to the service FQDN, so Metadata responses point clients to the right address. In replicated mode, the Service includes a `klite.io/role: primary` selector so traffic is routed exclusively to the active broker. The primary sets this label on its own pod via the Kubernetes API; on demotion or shutdown the label is removed, and the new primary adds it to itself.

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

## Replicated mode (active-passive)

Set `replication.enabled=true` to deploy a 2-pod StatefulSet with synchronous streaming replication. This requires S3 (used for leader-lease fencing and metadata storage).

```yaml
replication:
  enabled: true        # switches from Deployment to StatefulSet
  port: 9093           # inter-broker replication port
  ackTimeout: 5s       # timeout for standby ACK
  leaseDuration: 15s   # S3 lease validity
  leaseRenewInterval: 5s
  leaseRetryInterval: 2s

s3:
  bucket: my-klite-data
  region: us-west-2
```

```bash
helm install klite oci://ghcr.io/klaudworks/charts/klite --version "$KLITE_VERSION" \
  --set replication.enabled=true \
  --set s3.bucket=my-klite-data \
  --set s3.region=us-west-2
```

### How it works

- The chart deploys a StatefulSet with `replicas: 2` and `podManagementPolicy: Parallel`.
- Each pod gets a stable network identity via a headless Service (`<release>-klite-headless`), used for replication traffic on port 9093.
- On startup, both pods race for an S3 lease. The winner becomes primary and accepts Kafka traffic; the loser becomes standby and streams WAL entries from the primary.
- The primary patches its own pod with the label `klite.io/role: primary`. The Kafka Service selects on this label, so only the primary receives client traffic. Both pods report as Ready — there is no perpetually not-ready pod.
- The primary continuously replicates writes to the standby synchronously — a produce request is only ACKed to the client after the standby confirms receipt.
- The chart creates a Role and RoleBinding that grant the ServiceAccount permission to PATCH pods (required for the label update).
- A `preferredDuringSchedulingIgnoredDuringExecution` pod anti-affinity rule spreads the two pods across different nodes by default.

### Failover behavior

| Scenario | Downtime |
|---|---|
| Graceful shutdown (SIGTERM / rolling upgrade) | ~2 seconds |
| Crash / node failure | ~`leaseDuration` (default 15s) |

On primary failure, the standby detects the expired S3 lease, acquires it, promotes itself to primary, and begins accepting Kafka traffic. Kafka clients retry automatically; the interruption manifests as brief back-pressure, not data loss.

### Persistence with replication

When replication is enabled, the chart uses `volumeClaimTemplates` in the StatefulSet instead of a standalone PVC. Each pod gets its own PVC (`data-klite-0`, `data-klite-1`). The `persistence.enabled`, `persistence.size`, and `persistence.storageClass` values still apply.

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

On SIGTERM, klite flushes all buffered data to S3, uploads the metadata log, and exits cleanly. Startup replays the metadata log and WAL in 1-2 seconds. Kafka clients retry by default, so the upgrade causes a brief interruption (a few seconds of back-pressure) but no data loss.

In replicated mode, Kubernetes performs a rolling update of the StatefulSet. The standby is restarted first, then the primary. Failover happens automatically during the primary's restart, keeping total downtime to a few seconds.

## Uninstalling

```bash
helm uninstall klite
```

The PVC is preserved by default. To delete data too:

```bash
# Single-broker mode
kubectl delete pvc -l app.kubernetes.io/name=klite

# Replicated mode (StatefulSet PVCs follow a different naming pattern)
kubectl delete pvc data-klite-0 data-klite-1
```

For common issues, see [Troubleshooting](/guides/troubleshooting/).
