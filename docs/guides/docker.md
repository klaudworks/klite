---
title: Docker
description: Run klite in Docker with persistent storage and custom configuration.
---

Run klite as a container with a single command. The official image is available on GitHub Container Registry.

## Quick start

```bash
docker run -p 9092:9092 ghcr.io/klaudworks/klite
```

Your Kafka clients can now connect to `localhost:9092`.

## Persistent storage

By default, data is stored inside the container and lost when it stops. Mount a volume to persist data across restarts:

```bash
docker run -p 9092:9092 \
  -v klite-data:/data \
  ghcr.io/klaudworks/klite \
  --data-dir /data
```

Or bind-mount a host directory:

```bash
docker run -p 9092:9092 \
  -v $(pwd)/klite-data:/data \
  ghcr.io/klaudworks/klite \
  --data-dir /data
```

## Configuration

Configure klite using environment variables or command-line flags. Environment variables use the `KLITE_` prefix:

```bash
docker run -p 9092:9092 \
  -e KLITE_LOG_LEVEL=debug \
  -e KLITE_DEFAULT_PARTITIONS=3 \
  -e KLITE_AUTO_CREATE_TOPICS=true \
  -v klite-data:/data \
  ghcr.io/klaudworks/klite \
  --data-dir /data
```

Or pass flags directly:

```bash
docker run -p 9092:9092 \
  ghcr.io/klaudworks/klite \
  --log-level debug \
  --default-partitions 3 \
  --data-dir /data
```

See the [Configuration reference](/reference/configuration/) for all available options.

## Advertised address

When running in Docker, clients outside the container need to reach the broker. Set `--advertised-addr` to the address clients will use:

```bash
docker run -p 9092:9092 \
  ghcr.io/klaudworks/klite \
  --advertised-addr host.docker.internal:9092
```

If your clients run on the Docker host, `host.docker.internal` works on Docker Desktop (macOS/Windows). On Linux, use the host's IP or `--network host`.

## S3 backend

Connect klite to an S3-compatible backend for durable storage:

```bash
docker run -p 9092:9092 \
  -e KLITE_S3_BUCKET=my-klite-data \
  -e KLITE_S3_REGION=us-east-1 \
  -e AWS_ACCESS_KEY_ID=your-key \
  -e AWS_SECRET_ACCESS_KEY=your-secret \
  -v klite-data:/data \
  ghcr.io/klaudworks/klite \
  --data-dir /data
```

### With MinIO

```bash
docker run -p 9092:9092 \
  -e KLITE_S3_BUCKET=klite \
  -e KLITE_S3_ENDPOINT=http://minio:9000 \
  -e AWS_ACCESS_KEY_ID=minioadmin \
  -e AWS_SECRET_ACCESS_KEY=minioadmin \
  ghcr.io/klaudworks/klite
```

## Docker Compose

A complete local setup with klite, a producer, and a consumer:

```yaml
# docker-compose.yml
services:
  klite:
    image: ghcr.io/klaudworks/klite
    ports:
      - "9092:9092"
    volumes:
      - klite-data:/data
    command:
      - --data-dir=/data
      - --advertised-addr=klite:9092
      - --default-partitions=3
    # TODO: Add healthcheck once health endpoint is available
    # healthcheck:
    #   test: ["CMD", "nc", "-z", "localhost", "9092"]
    #   interval: 5s
    #   timeout: 3s
    #   retries: 5

volumes:
  klite-data:
```

### With MinIO for S3 storage

```yaml
# docker-compose.yml
services:
  klite:
    image: ghcr.io/klaudworks/klite
    ports:
      - "9092:9092"
    environment:
      KLITE_S3_BUCKET: klite
      KLITE_S3_ENDPOINT: http://minio:9000
      AWS_ACCESS_KEY_ID: minioadmin
      AWS_SECRET_ACCESS_KEY: minioadmin
    volumes:
      - klite-data:/data
    command:
      - --data-dir=/data
      - --advertised-addr=klite:9092
    depends_on:
      minio:
        condition: service_healthy

  minio:
    image: minio/minio
    command: server /data --console-address ":9001"
    ports:
      - "9000:9000"
      - "9001:9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    volumes:
      - minio-data:/data
    healthcheck:
      test: ["CMD", "mc", "ready", "local"]
      interval: 5s
      timeout: 3s
      retries: 5

  # Create the bucket on startup
  minio-init:
    image: minio/mc
    depends_on:
      minio:
        condition: service_healthy
    entrypoint: >
      /bin/sh -c "
      mc alias set local http://minio:9000 minioadmin minioadmin;
      mc mb local/klite --ignore-existing;
      "

volumes:
  klite-data:
  minio-data:
```

## Building your own image

<!-- TODO: Add Dockerfile to repo root and reference it here -->

```dockerfile
FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /klite ./cmd/klite

FROM alpine:3.20
COPY --from=build /klite /usr/local/bin/klite
EXPOSE 9092
ENTRYPOINT ["klite"]
```

```bash
docker build -t klite:local .
docker run -p 9092:9092 klite:local
```

## Next steps

- [Kubernetes & Helm](/guides/kubernetes/) -- deploy to Kubernetes with a Helm chart
- [Configuration reference](/reference/configuration/) -- all flags and environment variables
- [Monitoring](/guides/monitoring/) -- logs and health checks
