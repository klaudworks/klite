# klite

Kafka-compatible broker. 1 binary, 1 S3 bucket, 1M events/sec.

[![Go](https://img.shields.io/github/go-mod/go-version/klaudworks/klite)](https://go.dev/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)
[![Release](https://img.shields.io/github/v/release/klaudworks/klite)](https://github.com/klaudworks/klite/releases)

klite is a Kafka-compatible broker in a single Go binary. Point your existing Kafka clients at it -- no code changes needed. Data is persisted to S3 automatically. No disks to manage, no data to lose. Ships as a 15 MB container that boots in under a second. If you outgrow it, switch to a full Kafka cluster. Same wire protocol, zero lock-in.

## Quickstart

```bash
# Start klite
docker run -p 9092:9092 ghcr.io/klaudworks/klite

# Produce a message
echo "hello klite" | kcat -P -b localhost:9092 -t my-topic

# Consume it back
kcat -C -b localhost:9092 -t my-topic -e
```

See the [full quickstart](https://klite.dev/guides/getting-started/) for Go, Python, and Node.js examples and S3 setup.

## Features

- **Performance** -- tested at 553K events/sec sustained on a single node with standard gp3 storage. [Benchmarks](https://klite.dev/performance/benchmarks/)
- **S3 storage** -- WAL + automatic flush to any S3-compatible store. [How it works](https://klite.dev/concepts/storage/)
- **Any Kafka client** -- franz-go, librdkafka, Java, Python, KafkaJS, sarama. [Compatibility](https://klite.dev/reference/kafka-compatibility/)
- **39 Kafka APIs** -- produce, fetch, consumer groups, transactions, SASL auth
- **Kubernetes** -- official Helm chart. [Guide](https://klite.dev/guides/kubernetes/)
- **Migrate to/from Kafka** -- MirrorMaker 2 support

## Documentation

Full docs at [klite.dev](https://klite.dev):

- [Getting Started](https://klite.dev/guides/getting-started/)
- [Configuration](https://klite.dev/guides/configuration/)
- [Kubernetes & Helm](https://klite.dev/guides/kubernetes/)
- [Storage Architecture](https://klite.dev/concepts/storage/)
- [Kafka Compatibility](https://klite.dev/reference/kafka-compatibility/)
- [Benchmarks](https://klite.dev/performance/benchmarks/)

## Building from source

```bash
go build -o bin/klite ./cmd/klite
go test ./...
```

## Contributing

Contributions are welcome -- whether that's code, bug reports, or just trying klite and telling us what you think. If you run into anything, [open an issue](https://github.com/klaudworks/klite/issues).
