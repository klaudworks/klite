---
title: Quickstart
description: Install klite and send your first message in under a minute.
---

klite is a Kafka-compatible broker in a single Go binary. This guide gets you from zero to producing and consuming messages in under a minute.

## Install

### Download a release binary

```bash
# macOS (Apple Silicon)
curl -L https://github.com/klaudworks/klite/releases/latest/download/klite-darwin-arm64 -o klite

# macOS (Intel)
curl -L https://github.com/klaudworks/klite/releases/latest/download/klite-darwin-amd64 -o klite

# Linux (amd64)
curl -L https://github.com/klaudworks/klite/releases/latest/download/klite-linux-amd64 -o klite

# Linux (arm64)
curl -L https://github.com/klaudworks/klite/releases/latest/download/klite-linux-arm64 -o klite

chmod +x klite
```

### Build from source

```bash
go install github.com/klaudworks/klite/cmd/klite@latest
```

### Docker

```bash
docker run -p 9092:9092 ghcr.io/kliteio/klite
```

See the [Docker guide](/guides/docker/) for persistent volumes and custom configuration.

<!-- TODO: Add Homebrew install once a tap is published -->
<!-- TODO: Add Windows install instructions -->

## Start the broker

```bash
./klite
```

That's it. klite starts listening on `localhost:9092` with sane defaults:

- Data stored in `./data`
- Topics auto-created on first produce or metadata request
- 1 partition per topic by default
- Log level: `info`

You'll see output like:

```
2025/01/15 10:30:00 INFO broker started listen=:9092 cluster_id=abc123 node_id=0
```

## Produce and consume

Use **any** Kafka client. klite speaks the standard Kafka wire protocol.

### With kcat (recommended for testing)

```bash
# Produce a message
echo "hello klite" | kcat -P -b localhost:9092 -t my-topic

# Consume all messages
kcat -C -b localhost:9092 -t my-topic -e
```

### With Go (franz-go)

```go
package main

import (
	"context"
	"fmt"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	// Producer
	client, _ := kgo.NewClient(kgo.SeedBrokers("localhost:9092"))
	defer client.Close()

	ctx := context.Background()
	client.Produce(ctx, &kgo.Record{
		Topic: "my-topic",
		Value: []byte("hello from Go"),
	}, func(r *kgo.Record, err error) {
		if err != nil {
			panic(err)
		}
		fmt.Printf("produced to partition %d offset %d\n", r.Partition, r.Offset)
	})
	client.Flush(ctx)

	// Consumer
	consumer, _ := kgo.NewClient(
		kgo.SeedBrokers("localhost:9092"),
		kgo.ConsumeTopics("my-topic"),
	)
	defer consumer.Close()

	fetches := consumer.PollFetches(ctx)
	fetches.EachRecord(func(r *kgo.Record) {
		fmt.Printf("consumed: %s\n", string(r.Value))
	})
}
```

### With Python (confluent-kafka)

```python
from confluent_kafka import Producer, Consumer

# Produce
p = Producer({'bootstrap.servers': 'localhost:9092'})
p.produce('my-topic', value='hello from Python')
p.flush()

# Consume
c = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my-group',
    'auto.offset.reset': 'earliest',
})
c.subscribe(['my-topic'])

msg = c.poll(5.0)
if msg:
    print(f"consumed: {msg.value().decode()}")
c.close()
```

### With Node.js (kafkajs)

```javascript
const { Kafka } = require('kafkajs');

const kafka = new Kafka({ brokers: ['localhost:9092'] });

// Produce
const producer = kafka.producer();
await producer.connect();
await producer.send({
  topic: 'my-topic',
  messages: [{ value: 'hello from Node.js' }],
});
await producer.disconnect();

// Consume
const consumer = kafka.consumer({ groupId: 'my-group' });
await consumer.connect();
await consumer.subscribe({ topic: 'my-topic', fromBeginning: true });
await consumer.run({
  eachMessage: async ({ message }) => {
    console.log(`consumed: ${message.value.toString()}`);
  },
});
```

## What just happened?

When you produced to `my-topic`, klite:

1. **Auto-created the topic** with 1 partition (configurable via `--default-partitions`)
2. **Assigned offsets** to each RecordBatch using the Kafka v2 format
3. **Stored the data** in the write-ahead log under `./data/`
4. **Flushed to S3** if an S3 bucket was configured (otherwise WAL-only)

The topic persists across restarts. klite replays the WAL and metadata log on startup to recover state.

## Next steps

- [Configure klite](/reference/configuration/) -- customize listen address, data directory, S3 backend
- [Run in Docker](/guides/docker/) -- containerized deployment with persistent volumes
- [Deploy to Kubernetes](/guides/kubernetes/) -- Helm chart with StatefulSet and PVCs
- [Consumer groups](/guides/consumer-groups/) -- coordinate multiple consumers
- [Architecture](/concepts/architecture/) -- understand how klite works under the hood
