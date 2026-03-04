---
title: Consumer Groups
description: How to use Kafka consumer groups with klite.
---

klite supports the full Kafka consumer group protocol. Multiple consumers can coordinate to divide partitions among themselves, with automatic rebalancing when consumers join or leave.

## How consumer groups work

1. Each consumer in a group is assigned a subset of partitions
2. Only one consumer per partition within a group
3. When consumers join or leave, partitions are rebalanced automatically
4. Committed offsets are tracked per group, so consumers resume where they left off

klite implements the standard Kafka group coordination APIs: JoinGroup, SyncGroup, Heartbeat, LeaveGroup, OffsetCommit, and OffsetFetch.

## Basic example

### Go (franz-go)

```go
client, err := kgo.NewClient(
    kgo.SeedBrokers("localhost:9092"),
    kgo.ConsumerGroup("my-group"),
    kgo.ConsumeTopics("events"),
)
if err != nil {
    panic(err)
}
defer client.Close()

for {
    fetches := client.PollFetches(context.Background())
    fetches.EachRecord(func(r *kgo.Record) {
        fmt.Printf("partition=%d offset=%d value=%s\n",
            r.Partition, r.Offset, string(r.Value))
    })
    // franz-go auto-commits by default
}
```

### Python (confluent-kafka)

```python
from confluent_kafka import Consumer

c = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True,
})
c.subscribe(['events'])

try:
    while True:
        msg = c.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Error: {msg.error()}")
            continue
        print(f"partition={msg.partition()} offset={msg.offset()} value={msg.value().decode()}")
finally:
    c.close()
```

### Java

```java
Properties props = new Properties();
props.put("bootstrap.servers", "localhost:9092");
props.put("group.id", "my-group");
props.put("auto.offset.reset", "earliest");
props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
consumer.subscribe(Arrays.asList("events"));

while (true) {
    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
    for (ConsumerRecord<String, String> record : records) {
        System.out.printf("partition=%d offset=%d value=%s%n",
            record.partition(), record.offset(), record.value());
    }
}
```

## Scaling consumers

To process a topic in parallel, run multiple consumers with the same `group.id`. klite will automatically distribute partitions:

```bash
# Create a topic with 6 partitions
kcat -b localhost:9092 -L  # triggers auto-create with default partitions

# Or explicitly:
# TODO: Add CLI tool or admin API example for creating topics with specific partition count

# Terminal 1
python consumer.py --group my-group --topic events
# Assigned: partitions 0, 1, 2

# Terminal 2
python consumer.py --group my-group --topic events
# Rebalance! Now each gets: partitions 0-2 and 3-5

# Terminal 3
python consumer.py --group my-group --topic events
# Rebalance! Each gets 2 partitions
```

The maximum parallelism equals the number of partitions. If you have more consumers than partitions, the extras will be idle.

## Offset management

### Auto-commit (default)

Most client libraries auto-commit offsets periodically (every 5 seconds by default). This is the simplest option:

```python
c = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my-group',
    'enable.auto.commit': True,         # default
    'auto.commit.interval.ms': 5000,    # default
})
```

### Manual commit

For at-least-once processing, commit after processing:

```python
c = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my-group',
    'enable.auto.commit': False,
})
c.subscribe(['events'])

while True:
    msg = c.poll(1.0)
    if msg is None:
        continue

    # Process the message
    process(msg)

    # Then commit
    c.commit(msg)
```

### Reset offsets

Start consuming from the beginning or end of a topic:

```python
# From the beginning
'auto.offset.reset': 'earliest'

# From the end (only new messages)
'auto.offset.reset': 'latest'
```

## Multiple groups

Different applications can consume the same topic independently by using different group IDs. Each group maintains its own offsets:

```
Topic: events (3 partitions)

Group "analytics":   Consumer A [P0, P1, P2]  -- reads all partitions
Group "monitoring":  Consumer B [P0, P1]       -- 2 consumers
                     Consumer C [P2]
```

## Rebalancing behavior

klite supports the standard Kafka rebalance protocol:

- **Eager rebalance**: All partitions are revoked and reassigned (default for most clients)
- **Cooperative rebalance**: Only affected partitions are migrated (supported by newer clients)

Configure your client's partition assignor to control rebalance behavior:

```python
# Cooperative rebalancing (recommended for reduced downtime)
'partition.assignment.strategy': 'cooperative-sticky'
```

## Session and heartbeat timeouts

klite enforces session timeouts. If a consumer stops sending heartbeats, it's removed from the group:

```python
c = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my-group',
    'session.timeout.ms': 45000,      # Max time without heartbeat
    'heartbeat.interval.ms': 3000,    # Heartbeat frequency
    'max.poll.interval.ms': 300000,   # Max time between poll() calls
})
```

## Next steps

- [Migrating from Kafka](/guides/migrating-from-kafka/) -- move consumer groups from Kafka to klite
- [Configuration reference](/reference/configuration/) -- broker-side settings
- [Troubleshooting](/guides/troubleshooting/) -- common consumer group issues
