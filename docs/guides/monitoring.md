---
title: Monitoring
description: Observability, logs, and health checks for klite.
---

klite is designed to be easy to operate. This guide covers logging, health checks, and monitoring options.

## Logs

klite uses structured logging via Go's `slog` package. Output is JSON by default when running in a container, or human-readable text on a terminal.

### Log levels

Set the log level with `--log-level` or `KLITE_LOG_LEVEL`:

```bash
./klite --log-level debug
```

| Level | Description |
|-------|-------------|
| `debug` | Verbose protocol-level details, request/response tracing |
| `info` | Startup, shutdown, topic creation, group events (default) |
| `warn` | Recoverable issues: client disconnects, invalid requests |
| `error` | Unrecoverable errors: disk failures, S3 errors |

### Key log messages

```
INFO  broker started          listen=:9092 cluster_id=abc123 node_id=0
INFO  topic created           topic=my-topic partitions=3
INFO  group rebalance          group=my-group members=2 generation=3
WARN  client disconnected     addr=192.168.1.5:43210 error="read: connection reset"
ERROR WAL write failed        error="disk full"
```

### Log filtering in production

With JSON output, filter logs using `jq`:

```bash
# Show only errors
./klite 2>&1 | jq 'select(.level == "ERROR")'

# Show group coordinator events
./klite 2>&1 | jq 'select(.msg | contains("group"))'
```

In Kubernetes:

```bash
kubectl logs klite-0 | jq 'select(.level == "ERROR")'
```

## Health checks

<!-- TODO: Add HTTP health endpoint (/healthz, /readyz) and update this section -->

klite currently exposes health via TCP connectivity. If the broker is accepting connections on port 9092, it's healthy.

### Docker health check

```yaml
healthcheck:
  test: ["CMD", "nc", "-z", "localhost", "9092"]
  interval: 10s
  timeout: 3s
  retries: 3
```

### Kubernetes probes

```yaml
livenessProbe:
  tcpSocket:
    port: 9092
  initialDelaySeconds: 10
  periodSeconds: 10
readinessProbe:
  tcpSocket:
    port: 9092
  initialDelaySeconds: 5
  periodSeconds: 5
```

## Metrics

<!-- TODO: Add Prometheus metrics endpoint and document available metrics -->

Prometheus metrics support is planned. Key metrics to be exposed:

| Metric | Type | Description |
|--------|------|-------------|
| `klite_messages_produced_total` | Counter | Total messages produced |
| `klite_messages_consumed_total` | Counter | Total messages fetched |
| `klite_produce_latency_seconds` | Histogram | Produce request latency |
| `klite_fetch_latency_seconds` | Histogram | Fetch request latency |
| `klite_active_connections` | Gauge | Current open connections |
| `klite_consumer_groups_active` | Gauge | Active consumer groups |
| `klite_wal_size_bytes` | Gauge | WAL data size on disk |
| `klite_s3_flush_lag_seconds` | Gauge | Time since last S3 flush |

### Prometheus + Grafana (planned)

```yaml
# ServiceMonitor for Prometheus Operator
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: klite
spec:
  selector:
    matchLabels:
      app.kubernetes.io/name: klite
  endpoints:
    - port: metrics
      interval: 30s
```

## Alerting recommendations

Even without a dedicated metrics endpoint, you can alert on the basics:

| Alert | Condition | Severity |
|-------|-----------|----------|
| **klite down** | TCP probe fails for 30s | Critical |
| **Disk usage high** | Data dir > 80% capacity | Warning |
| **S3 flush failing** | Error logs with "s3" or "flush" | Critical |
| **Consumer group stuck** | Consumer lag increasing for > 5 min | Warning |

### Example alert (Prometheus)

```yaml
groups:
  - name: klite
    rules:
      - alert: KliteDown
        expr: up{job="klite"} == 0
        for: 30s
        labels:
          severity: critical
        annotations:
          summary: "klite broker is down"

      - alert: KliteDiskUsageHigh
        expr: node_filesystem_avail_bytes{mountpoint="/data"} / node_filesystem_size_bytes{mountpoint="/data"} < 0.2
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "klite data directory disk usage above 80%"
```

## Disk usage

Monitor the data directory size:

```bash
# Check WAL size
du -sh ./data/

# Watch it over time
watch -n 5 du -sh ./data/
```

With S3 storage enabled, the WAL is periodically flushed and old segments are removed. Without S3, the WAL grows indefinitely (subject to retention policy).

Configure retention to control disk usage:

```bash
./klite --retention-ms 86400000  # 24 hours
```

See [Storage](/concepts/storage/) for details on WAL lifecycle and S3 flushing.

## Next steps

- [Configuration reference](/reference/configuration/) -- all flags and environment variables
- [Architecture](/concepts/architecture/) -- understand WAL, S3, and the data path
- [Troubleshooting](/guides/troubleshooting/) -- common operational issues
