# Helm Chart: Streaming Replication Support

Update the existing Helm chart (`charts/klite/`) to support deploying klite
with streaming replication enabled. When `replication.enabled=true`, the chart
deploys a StatefulSet with 2 replicas (instead of a single-replica Deployment),
a headless Service for replication DNS, and the necessary CLI flags.

Prerequisite: `04-broker-lifecycle.md` (config flags, replication address
resolution, health endpoint behavior).

Read before starting: `charts/klite/values.yaml`, `charts/klite/templates/`,
`04-broker-lifecycle.md` (configuration section).

---

## Design Overview

When replication is disabled (the default), the chart behaves exactly as
today: a Deployment with 1 replica, a ClusterIP Service, an optional PVC.

When `replication.enabled=true`:

1. **StatefulSet replaces Deployment** — two pods (`klite-0`, `klite-1`) with
   stable network identities and per-pod PVCs via `volumeClaimTemplates`.
   The existing `pvc.yaml` template is unused when replication is enabled
   (StatefulSet manages its own PVCs).
2. **Headless Service added** — provides DNS records like
   `klite-0.klite-headless.ns.svc.cluster.local` for replication address
   discovery.
3. **Replication CLI flags injected** — `--replication-addr`,
   `--replication-advertised-addr` (using downward API for pod name),
   `--replication-ack-timeout`, etc.
4. **Readiness probe behavior is correct by default** — standby returns 503
   on `/readyz`, so the client-facing Service only routes to the primary.

---

## Values

New values under `replication:`:

```yaml
## @section Replication
## Enable active-passive streaming replication with automatic failover.
## Requires s3.bucket to be set (S3 is used for lease fencing and TLS cert storage).

replication:
  ## @param replication.enabled Enable streaming replication (deploys StatefulSet with 2 replicas)
  enabled: false
  ## @param replication.port Replication listen port inside the container
  port: 9093
  ## @param replication.ackTimeout Timeout for standby ACK
  ackTimeout: 5s
  ## @param replication.leaseDuration How long the S3 lease is valid without renewal
  leaseDuration: 15s
  ## @param replication.leaseRenewInterval How often the primary renews the S3 lease
  leaseRenewInterval: 5s
  ## @param replication.leaseRetryInterval How often the standby polls the S3 lease
  leaseRetryInterval: 2s
```

---

## Template Changes

### Conditional Deployment vs StatefulSet

The Deployment template (`deployment.yaml`) gains a top-level guard:

```
{{- if not .Values.replication.enabled }}
```

A new `statefulset.yaml` template is added with the inverse guard:

```
{{- if .Values.replication.enabled }}
```

### StatefulSet (`statefulset.yaml`)

- `replicas: 2` (hardcoded — active-passive, always exactly 2)
- `serviceName: {{ include "klite.fullname" . }}-headless` (headless Service)
- `podManagementPolicy: Parallel` (both pods start simultaneously)
- `volumeClaimTemplates` replaces the standalone PVC — uses the same
  `persistence.*` values (storageClass, size, accessModes)
- Container spec is identical to the Deployment, plus replication args:

```yaml
args:
  # ... existing args ...
  - --replication-addr=:{{ .Values.replication.port }}
  - --replication-advertised-addr=$(POD_NAME).{{ include "klite.fullname" . }}-headless.{{ .Release.Namespace }}.svc.cluster.local:{{ .Values.replication.port }}
  - --replication-id=$(POD_NAME)
  - --replication-ack-timeout={{ .Values.replication.ackTimeout }}
  - --lease-duration={{ .Values.replication.leaseDuration }}
  - --lease-renew-interval={{ .Values.replication.leaseRenewInterval }}
  - --lease-retry-interval={{ .Values.replication.leaseRetryInterval }}
```

Additional env var for the downward API:

```yaml
env:
  - name: POD_NAME
    valueFrom:
      fieldRef:
        fieldPath: metadata.name
```

Additional port:

```yaml
ports:
  - name: replication
    containerPort: {{ .Values.replication.port }}
    protocol: TCP
```

### Headless Service (`service-headless.yaml`)

Only rendered when `replication.enabled=true`:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: {{ include "klite.fullname" . }}-headless
spec:
  clusterIP: None
  publishNotReadyAddresses: true
  ports:
    - name: replication
      port: {{ .Values.replication.port }}
      targetPort: replication
  selector:
    {{- include "klite.selectorLabels" . | nindent 4 }}
```

`publishNotReadyAddresses: true` is critical — the standby is "not ready"
(readiness probe returns 503) but must still be DNS-resolvable for the
primary to send replication data to it. Without this, the standby's DNS
record would not exist until it passes readiness, creating a chicken-and-egg
problem.

### PVC Template Guard

`pvc.yaml` gains a guard to skip when replication is enabled (StatefulSet
manages its own PVCs):

```
{{- if and .Values.persistence.enabled (not .Values.persistence.existingClaim) (not .Values.replication.enabled) }}
```

### Client-Facing Service

The existing `service.yaml` stays unchanged. It selects all pods by label
but Kubernetes only routes to pods that pass the readiness probe. Since
the standby returns 503 on `/readyz`, traffic routes only to the primary.

### NOTES.txt

Add a section when replication is enabled:

```
{{- if .Values.replication.enabled }}

Streaming replication is enabled (synchronous).
  Two pods will start: one becomes primary (accepts Kafka traffic),
  the other runs as a standby replica.

  Failover is automatic:
    Graceful (SIGTERM): ~2s
    Crash:              ~{{ .Values.replication.leaseDuration }}

  Replication uses the headless service for DNS discovery:
    {{ include "klite.fullname" . }}-headless.{{ .Release.Namespace }}.svc.cluster.local
{{- end }}
```

---

## Startup Validation

The chart should fail `helm template` / `helm install` with a clear error
if `replication.enabled=true` but `s3.bucket` is empty:

```
{{- if and .Values.replication.enabled (not .Values.s3.bucket) }}
  {{- fail "replication.enabled requires s3.bucket to be set" }}
{{- end }}
```

Place this in `_helpers.tpl` or at the top of `statefulset.yaml`.

---

## Anti-Affinity

When replication is enabled, add a pod anti-affinity to spread the two
replicas across nodes:

```yaml
affinity:
  podAntiAffinity:
    preferredDuringSchedulingIgnoredDuringExecution:
      - weight: 100
        podAffinityTerm:
          labelSelector:
            matchLabels:
              {{- include "klite.selectorLabels" . | nindent 14 }}
          topologyKey: kubernetes.io/hostname
```

Use `preferredDuring...` (soft), not `requiredDuring...` (hard), so the
chart works on single-node dev clusters. The user can override via
`.Values.affinity` if they want hard anti-affinity.

This is only added as a default when `replication.enabled=true` AND the
user has not set a custom `.Values.affinity`. If `.Values.affinity` is
non-empty, use it verbatim (same behavior as today).

---

## When Stuck

- Read the existing chart templates in `charts/klite/templates/` to
  understand the conventions (helper functions, label patterns, value
  structure).
- Read `04-broker-lifecycle.md` for the full list of replication CLI flags
  and the Kubernetes address discovery pattern.

---

## Acceptance Criteria

Default mode (replication disabled):
- `helm template` with default values produces the same output as before
  (Deployment, not StatefulSet). No regressions.
- No headless Service rendered
- No replication CLI flags in container args
- PVC template still works

Replication mode:
- `helm template` with `replication.enabled=true` and `s3.bucket=my-bucket`
  produces a StatefulSet with 2 replicas and a headless Service
- No Deployment rendered when replication is enabled
- No standalone PVC rendered (StatefulSet uses `volumeClaimTemplates`)
- Container args include all replication flags
  (`--replication-addr`, `--replication-advertised-addr`,
  `--replication-id`, `--replication-ack-timeout`, `--lease-duration`,
  `--lease-renew-interval`, `--lease-retry-interval`)
- `--replication-advertised-addr` uses downward API `$(POD_NAME)` with
  headless Service FQDN
- `--replication-id` uses `$(POD_NAME)`
- `POD_NAME` env var is set via downward API
- Replication port (9093) is exposed in container ports
- Headless Service has `clusterIP: None` and
  `publishNotReadyAddresses: true`
- `helm template` with `replication.enabled=true` and empty `s3.bucket`
  fails with a clear error message
- Pod anti-affinity is set when replication is enabled and no custom
  affinity is provided
- NOTES.txt includes replication info when enabled

### Verify

```bash
# Default mode — no regressions
helm template test-release charts/klite/ | grep -q "kind: Deployment"
helm template test-release charts/klite/ | grep -qv "kind: StatefulSet"

# Replication mode
helm template test-release charts/klite/ \
  --set replication.enabled=true \
  --set s3.bucket=my-bucket | grep -q "kind: StatefulSet"
helm template test-release charts/klite/ \
  --set replication.enabled=true \
  --set s3.bucket=my-bucket | grep -q "kind: Service"
helm template test-release charts/klite/ \
  --set replication.enabled=true \
  --set s3.bucket=my-bucket | grep -q "replication-advertised-addr"

# Validation — should fail
helm template test-release charts/klite/ \
  --set replication.enabled=true 2>&1 | grep -q "requires s3.bucket"

# Lint
helm lint charts/klite/ --set replication.enabled=true --set s3.bucket=my-bucket
```
