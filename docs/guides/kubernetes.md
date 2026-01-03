---
title: Kubernetes & Helm
description: Deploy klite to Kubernetes using the official Helm chart.
---

Deploy klite to Kubernetes as a single-replica StatefulSet with persistent storage. The Helm chart handles PVCs, services, and configuration.

<!-- TODO: Publish the Helm chart and update the repo URL below -->

## Prerequisites

- Kubernetes 1.24+
- Helm 3.x
- `kubectl` configured for your cluster
- A StorageClass that supports `ReadWriteOnce` (default on most clusters)

## Install with Helm

```bash
# Add the klite Helm repository
helm repo add klite https://charts.klite.io
helm repo update

# Install with default settings
helm install klite klite/klite

# Or install into a specific namespace
helm install klite klite/klite --namespace kafka --create-namespace
```

## Verify the installation

```bash
# Check the pod is running
kubectl get pods -l app.kubernetes.io/name=klite

# Check the service
kubectl get svc klite

# View logs
kubectl logs -l app.kubernetes.io/name=klite -f
```

## Connect to klite

From within the cluster, use the service DNS name:

```
klite.default.svc.cluster.local:9092
```

For local development, port-forward:

```bash
kubectl port-forward svc/klite 9092:9092

# In another terminal
echo "hello k8s" | kcat -P -b localhost:9092 -t my-topic
kcat -C -b localhost:9092 -t my-topic -e
```

## Configuration

### values.yaml reference

```yaml
# values.yaml
replicaCount: 1  # klite is a single-broker; always 1

image:
  repository: ghcr.io/kliteio/klite
  tag: latest  # TODO: Pin to a specific version in production
  pullPolicy: IfNotPresent

# klite configuration (passed as flags)
config:
  listenAddr: ":9092"
  dataDir: "/data"
  logLevel: "info"
  defaultPartitions: 3
  autoCreateTopics: true
  # advertisedAddr is auto-derived from the pod's service address

# S3 storage backend (optional)
s3:
  enabled: false
  bucket: ""
  region: "us-east-1"
  endpoint: ""  # Set for MinIO / S3-compatible stores
  # Credentials via existing secret
  existingSecret: ""
  # Or inline (not recommended for production)
  accessKeyId: ""
  secretAccessKey: ""

# Persistence
persistence:
  enabled: true
  storageClass: ""  # Use cluster default
  size: 10Gi
  accessMode: ReadWriteOnce

# Service
service:
  type: ClusterIP
  port: 9092

# Resources
resources:
  requests:
    cpu: 100m
    memory: 128Mi
  limits:
    cpu: "2"
    memory: 1Gi

# Probes
# TODO: Switch to HTTP health endpoint when available
livenessProbe:
  tcpSocket:
    port: kafka
  initialDelaySeconds: 10
  periodSeconds: 10
  timeoutSeconds: 5
readinessProbe:
  tcpSocket:
    port: kafka
  initialDelaySeconds: 5
  periodSeconds: 5
  timeoutSeconds: 3

# Pod-level settings
nodeSelector: {}
tolerations: []
affinity: {}
podAnnotations: {}
podSecurityContext:
  runAsNonRoot: true
  runAsUser: 1000
  fsGroup: 1000
securityContext:
  readOnlyRootFilesystem: true
  allowPrivilegeEscalation: false

# ServiceMonitor for Prometheus (optional)
# TODO: Add metrics endpoint to klite
serviceMonitor:
  enabled: false
  interval: 30s
```

### Custom values

```bash
# Production-like deployment
helm install klite klite/klite -f my-values.yaml

# Or override individual values
helm install klite klite/klite \
  --set config.defaultPartitions=6 \
  --set persistence.size=50Gi \
  --set resources.limits.memory=2Gi
```

## S3 backend

### With AWS S3

Create a Kubernetes secret with your AWS credentials:

```bash
kubectl create secret generic klite-s3 \
  --from-literal=aws-access-key-id=AKIA... \
  --from-literal=aws-secret-access-key=...
```

Then reference it in your values:

```yaml
s3:
  enabled: true
  bucket: my-klite-data
  region: us-west-2
  existingSecret: klite-s3
```

For EKS, consider using [IAM Roles for Service Accounts (IRSA)](https://docs.aws.amazon.com/eks/latest/userguide/iam-roles-for-service-accounts.html) instead of static credentials:

```yaml
serviceAccount:
  annotations:
    eks.amazonaws.com/role-arn: arn:aws:iam::123456789:role/klite-s3-role

s3:
  enabled: true
  bucket: my-klite-data
  region: us-west-2
  # No credentials needed -- IRSA provides them via the service account
```

### With MinIO (in-cluster)

```yaml
s3:
  enabled: true
  bucket: klite
  endpoint: http://minio.minio.svc.cluster.local:9000
  existingSecret: minio-credentials
```

## StatefulSet details

The Helm chart creates a **StatefulSet** (not a Deployment) to ensure:

- Stable network identity (`klite-0`)
- Persistent volume claim bound to the pod
- Ordered, graceful shutdown

The WAL data directory is mounted at `/data` from a PVC. klite replays the WAL on startup, so data survives pod restarts and rescheduling.

## Upgrading

```bash
helm repo update
helm upgrade klite klite/klite
```

klite replays its WAL on startup, so upgrades are safe. The pod will shut down gracefully (SIGTERM), and the new version will replay the WAL to recover state.

## Uninstalling

```bash
helm uninstall klite
```

This removes the StatefulSet, Service, and ConfigMap but **preserves the PVC** by default. To delete data too:

```bash
kubectl delete pvc -l app.kubernetes.io/name=klite
```

## Example: klite with a sample application

Deploy klite alongside a simple producer/consumer:

```yaml
# app.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kafka-producer
spec:
  replicas: 1
  selector:
    matchLabels:
      app: kafka-producer
  template:
    metadata:
      labels:
        app: kafka-producer
    spec:
      containers:
        - name: producer
          image: confluentinc/cp-kafkacat:latest
          command:
            - /bin/sh
            - -c
            - |
              while true; do
                echo "message at $(date)" | kafkacat -P -b klite:9092 -t demo
                sleep 5
              done
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kafka-consumer
spec:
  replicas: 1
  selector:
    matchLabels:
      app: kafka-consumer
  template:
    metadata:
      labels:
        app: kafka-consumer
    spec:
      containers:
        - name: consumer
          image: confluentinc/cp-kafkacat:latest
          command:
            - kafkacat
            - -C
            - -b
            - klite:9092
            - -t
            - demo
            - -G
            - demo-group
```

## Troubleshooting

### Pod stuck in CrashLoopBackOff

Check logs:
```bash
kubectl logs klite-0
```

Common causes:
- PVC not bound (check `kubectl get pvc`)
- Port 9092 conflict (check `kubectl get svc`)
- Invalid S3 credentials

### Clients can't connect

Ensure the service is reachable:
```bash
kubectl run test --rm -it --image=alpine -- sh -c "apk add netcat-openbsd && nc -zv klite 9092"
```

See [Troubleshooting](/guides/troubleshooting/) for more common issues.

## Next steps

- [Configuration reference](/reference/configuration/) -- all flags and environment variables
- [Monitoring](/guides/monitoring/) -- observability in Kubernetes
- [Architecture](/concepts/architecture/) -- understand klite internals
