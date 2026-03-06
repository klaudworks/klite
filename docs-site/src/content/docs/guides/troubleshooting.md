---
title: Troubleshooting
description: Solutions for common klite issues.
---

If your issue isn't covered here, open a [GitHub issue](https://github.com/klaudworks/klite/issues). Include `--log-level debug` logs, your klite version, OS, and client library version. We appreciate any issues found and typical turnaround is a few hours to 2 days.

## Clients connect but can't produce or consume

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
