---
title: Troubleshooting
description: Solutions for common klite issues.
---

## Connection issues

### "Connection refused" when connecting

**Symptom**: Client throws `connection refused` on `localhost:9092`.

**Causes & fixes**:

1. **klite isn't running** -- Start it with `./klite` and check for startup errors in the log output.

2. **Wrong port** -- If you started klite with `--listen :9093`, connect to port 9093:
   ```
   bootstrap.servers = localhost:9093
   ```

3. **Firewall blocking the port** -- Check that port 9092 is open:
   ```bash
   nc -zv localhost 9092
   ```

### Clients connect but can't produce or consume

**Symptom**: Connection succeeds but operations fail with "broker not available" or "leader not available".

**Cause**: The advertised address doesn't match what clients can reach.

**Fix**: Set `--advertised-addr` to the address clients use:

```bash
# Docker
./klite --advertised-addr host.docker.internal:9092

# Remote server
./klite --advertised-addr my-server.example.com:9092

# Kubernetes
./klite --advertised-addr klite.default.svc.cluster.local:9092
```

### Docker: clients outside the container can't connect

**Symptom**: `kcat` from the host machine can't connect to klite running in Docker.

**Fixes**:

1. Ensure you published the port:
   ```bash
   docker run -p 9092:9092 ghcr.io/klaudworks/klite
   ```

2. Set the advertised address:
   ```bash
   docker run -p 9092:9092 ghcr.io/klaudworks/klite --advertised-addr localhost:9092
   ```

3. On Linux with Docker bridge networking, you may need `--network host`:
   ```bash
   docker run --network host ghcr.io/klaudworks/klite
   ```

### Docker Compose: services can't reach klite

**Symptom**: Other containers in the same Compose stack get connection errors.

**Fix**: Use the service name as the advertised address:

```yaml
services:
  klite:
    image: ghcr.io/klaudworks/klite
    command: ["--advertised-addr", "klite:9092"]
    ports:
      - "9092:9092"
```

Other containers connect with `bootstrap.servers = klite:9092`.

## Topic issues

### "Topic not found" errors

**Symptom**: Consumer gets `UNKNOWN_TOPIC_OR_PARTITION` error.

**Causes & fixes**:

1. **Auto-create is disabled** -- If you started klite with `--auto-create-topics=false`, create topics explicitly before use:
   ```bash
   # TODO: Add klite admin CLI example for topic creation
   # For now, producing with kcat auto-creates:
   echo "" | kcat -P -b localhost:9092 -t my-topic
   ```

2. **Typo in topic name** -- Topic names are case-sensitive. Check the exact name.

### Need more partitions

klite defaults to 1 partition per topic. For higher throughput or more consumer parallelism:

```bash
# Set default for new topics
./klite --default-partitions 6
```

<!-- TODO: Document how to add partitions to existing topics via CreatePartitions API -->

## Consumer group issues

### Consumers not receiving messages

**Symptom**: Consumer is connected and in a group but not receiving any messages.

**Causes & fixes**:

1. **More consumers than partitions** -- Extra consumers are idle. Add more partitions.

2. **Wrong `auto.offset.reset`** -- If the consumer group has no committed offsets and `auto.offset.reset` is `latest` (default in some clients), it only sees new messages:
   ```
   auto.offset.reset = earliest
   ```

3. **Consumer group is stuck in rebalancing** -- Check klite logs for rebalance events:
   ```bash
   ./klite --log-level debug 2>&1 | grep -i "group\|rebalance"
   ```

### "Group coordinator not available"

**Symptom**: Client reports the group coordinator is not available.

**Fix**: This is usually a transient error during startup. The client will retry automatically. If it persists, check that klite is running and reachable.

## Storage issues

### "disk full" or write errors

**Symptom**: klite logs `WAL write failed` with a disk full error.

**Fixes**:

1. Free disk space or expand the volume
2. Enable S3 storage to offload old data:
   ```bash
   ./klite --s3-bucket my-bucket --s3-region us-east-1
   ```
3. Reduce retention:
   ```bash
   ./klite --retention-ms 86400000  # 24 hours
   ```

### Data directory permissions

**Symptom**: klite fails to start with "permission denied" on the data directory.

**Fix**: Ensure the klite process can read and write the data directory:

```bash
mkdir -p ./data
chmod 755 ./data

# In Docker, ensure the container user has access:
docker run -p 9092:9092 -v ./data:/data --user $(id -u):$(id -g) ghcr.io/klaudworks/klite --data-dir /data
```

## Startup issues

### klite fails to start

Check the error message in the output:

| Error | Cause | Fix |
|-------|-------|-----|
| `bind: address already in use` | Another process on port 9092 | Stop it or use `--listen :9093` |
| `permission denied` | Can't bind port or open data dir | Run as appropriate user, check permissions |
| `WAL replay failed` | Corrupted WAL | Check disk health; restore from S3 if available |

### How to reset all state

To start fresh, stop klite and delete the data directory:

```bash
rm -rf ./data
./klite
```

This deletes all topics, messages, consumer group offsets, and metadata.

## Performance issues

### High produce latency

**Possible causes**:
- Slow disk (klite fsyncs on produce with acks=all). Use an NVMe SSD for best performance.
- Large batch sizes. klite processes batches atomically; very large batches take longer.

### High memory usage

klite keeps recent WAL data in the OS page cache. This is normal and managed by the kernel. `RES` memory in `top` should stay low; `VIRT` may be high due to mmap.

## Getting help

If your issue isn't covered here:

1. Check the [klite GitHub issues](https://github.com/klaudworks/klite/issues)
2. Run klite with `--log-level debug` and include the logs in your report
3. Include your klite version, OS, and client library version

<!-- TODO: Add community links (Discord, Slack, etc.) when available -->
