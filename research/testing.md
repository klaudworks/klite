# Conformance Testing Strategy

## Goal

Validate klite against the real Kafka wire protocol using franz-go as the
sole test client. Test scenarios are sourced from two places:

1. kfake's `kafka_tests/` -- already-translated Kafka integration tests (Go)
2. Kafka's ducktape tests -- protocol logic extracted and translated to Go

For background on how others test, see:
- `research/kafka-internals.md` -- Kafka's ducktape + JUnit infrastructure
- `research/automq.md` -- AutoMQ's curated ducktape subset

---

## 1. Why franz-go Only

franz-go is sufficient for wire format conformance because:

- It speaks the full Kafka binary protocol (all 93 API keys, all versions)
- kfake's own test suite already validates protocol correctness using franz-go
  as the client -- if our broker passes the same tests, we're conformant
- kfake's `kafka_tests/` directory contains 12 test files **derived directly
  from Apache Kafka's own Java integration tests** (translated via LLM)
- Adding librdkafka or segmentio/kafka-go adds CGo complexity or a second
  Go client with different assumptions -- diminishing returns vs. effort

Multi-client testing can be added later if real-world usage reveals gaps.

---

## 2. Primary Test Source: kfake's kafka_tests

Reference: `.cache/repos/franz-go/pkg/kfake/kafka_tests/`

These tests were derived from Kafka's Java integration tests and pass against
kfake. We adapt them to run against our broker instead.

| File | Coverage |
|---|---|
| `consumer_group_848_test.go` | KIP-848 consumer group protocol |
| `consumer_fetch_test.go` | Fetch behavior, edge cases |
| `consumer_poll_test.go` | Poll semantics, timeouts |
| `consumer_assign_test.go` | Manual partition assignment |
| `consumer_commit_test.go` | Offset commit flows |
| `transactions_test.go` | Transactional produce/consume/abort |
| `static_membership_test.go` | Static group membership (KIP-345) |
| `admin_fence_test.go` | Producer fencing via admin API |
| `list_offsets_test.go` | ListOffsets edge cases |
| `producer_compression_test.go` | All compression codecs |
| `describe_producers_test.go` | DescribeProducers API |
| `helpers_test.go` | Shared test utilities |

Adaptation approach: these tests construct a `kfake.Cluster` via `newCluster(t)`.
We replace that with our broker's startup. The rest of the test (franz-go client
setup, produce/consume/assert) stays identical.

---

## 3. Secondary Test Source: Ducktape Scenario Extraction

Ducktape tests have two layers:
1. **Infrastructure** (SSH, process management, JMX) -- not reusable
2. **Protocol logic** (produce, consume, verify) -- extractable

Key ducktape files (in `.cache/repos/kafka/tests/kafkatest/tests/`):

| File | Scenarios |
|---|---|
| `core/produce_consume_validate.py` | Basic produce/consume roundtrip |
| `client/consumer_test.py` | Consumer group rebalance, offset management |
| `client/producer_test.go` | Producer acks, idempotency, errors |
| `core/transactions_test.py` | Exactly-once semantics |
| `core/group_mode_transactions_test.py` | Transactions + consumer groups |

We translate the protocol-level logic to Go tests with franz-go, tagging each
with its ducktape origin for traceability.

---

## 4. Testing Architecture

```
+---------------------+
|  kafka-light broker  |  (started in-process per test)
+----------+----------+
           |
  Kafka wire protocol (TCP, localhost)
           |
+----------+----------+
|  franz-go kgo.Client |  (test client)
+----------+----------+
           |
+----------+----------+
|  Go test (go test)   |  (assertions, lifecycle)
+---------------------+
```

Each test:
1. Starts a broker in-process on a random port
2. Creates a franz-go client pointing at that port
3. Runs the protocol scenario (produce, consume, group ops, etc.)
4. Asserts correctness
5. Broker cleaned up via `t.Cleanup()`

Tests are parallelizable -- each gets its own broker on its own port.

---

## 5. Test Phases

### Phase 1: Connectivity + Core Data Path

Tests that validate the first milestone (broker accepts connections, serves data).

- TCP connect + ApiVersions handshake
- CreateTopics + Metadata (topic discovery)
- Produce single record, verify ack
- Produce batch, verify acks
- Produce compressed (gzip, snappy, lz4, zstd)
- ListOffsets (earliest, latest)
- Fetch from offset 0
- Fetch from latest
- Produce N records, consume N records, verify ordering + content
- Produce to multiple partitions, consume from all

**Feedback loop:** These tests are the gate for Phase 1 implementation. Don't
proceed until they pass.

### Phase 2: Consumer Groups

- FindCoordinator returns self
- JoinGroup + SyncGroup basic flow
- 3-consumer rebalance (add member, verify reassignment)
- Consumer leave, verify rebalance
- Heartbeat keeps session alive
- Session timeout triggers rebalance
- OffsetCommit + OffsetFetch roundtrip
- Consume, commit, restart consumer, resume from committed offset
- Static membership (KIP-345): rejoin without rebalance

**Source:** Adapt from kfake's `kafka_tests/consumer_group_848_test.go`,
`consumer_commit_test.go`, `static_membership_test.go`.

### Phase 3: Admin + Persistence

- CreateTopics with configs
- DeleteTopics
- CreatePartitions (increase count)
- DescribeConfigs / IncrementalAlterConfigs
- DescribeGroups / ListGroups
- WAL crash recovery: produce, kill, restart, verify data survived

### Phase 4: Transactions

- InitProducerID
- Transactional produce + commit (consumer sees records)
- Transactional produce + abort (consumer doesn't see records)
- Transactional offset commit (consume-transform-produce)
- Idempotent producer (duplicate detection)

**Source:** Adapt from kfake's `kafka_tests/transactions_test.go`,
`admin_fence_test.go`.

### Phase 5: Edge Cases + Stress

- Connection drop mid-produce
- Consumer crash during rebalance
- Produce to non-existent topic (auto-create)
- Large messages (at max.message.bytes boundary)
- Many partitions (1000+)
- Many consumer group members (50+)
- Transaction timeout
- S3 read path (consume historical data after WAL flush)

---

## 6. Test Infrastructure

```go
// internal/testutil/

// StartBroker starts a kafka-light broker on a random port.
func StartBroker(t *testing.T, opts ...BrokerOption) *TestBroker

type TestBroker struct {
    Addr string
}
func (b *TestBroker) Stop()

// NewFranzClient creates a franz-go client connected to the test broker.
func NewFranzClient(t *testing.T, addr string, opts ...kgo.Opt) *kgo.Client

// ProduceN produces n records to the given topic and waits for acks.
func ProduceN(t *testing.T, client *kgo.Client, topic string, n int)

// ConsumeN consumes exactly n records, failing after timeout.
func ConsumeN(t *testing.T, client *kgo.Client, topic string, n int, timeout time.Duration) []*kgo.Record
```

Pattern matches kfake's own test helpers (see `kafka_tests/helpers_test.go`):
`newCluster(t)`, `newClient(t, c)`, `produceNStrings(t, cl, topic, n)`,
`consumeN(t, cl, n, timeout)`.

---

## 7. Conformance Coverage Matrix

| API | Key | P1 | P2 | P3 | P4 | Source |
|---|---|---|---|---|---|---|
| ApiVersions | 18 | x | | | | core |
| Metadata | 3 | x | | | | core |
| CreateTopics | 19 | x | | x | | core + admin |
| Produce | 0 | x | | | x | core + txn |
| Fetch | 1 | x | | | x | core + txn |
| ListOffsets | 2 | x | | | | kfake/kafka_tests |
| FindCoordinator | 10 | | x | | | groups |
| JoinGroup | 11 | | x | | | kfake/kafka_tests |
| SyncGroup | 14 | | x | | | kfake/kafka_tests |
| Heartbeat | 12 | | x | | | kfake/kafka_tests |
| LeaveGroup | 13 | | x | | | kfake/kafka_tests |
| OffsetCommit | 8 | | x | | | kfake/kafka_tests |
| OffsetFetch | 9 | | x | | | kfake/kafka_tests |
| DescribeGroups | 15 | | | x | | admin |
| ListGroups | 16 | | | x | | admin |
| DeleteTopics | 20 | | | x | | admin |
| CreatePartitions | 37 | | | x | | admin |
| DescribeConfigs | 32 | | | x | | admin |
| IncrAlterConfigs | 44 | | | x | | admin |
| InitProducerID | 22 | | | | x | kfake/kafka_tests |
| AddPartitionsToTxn | 24 | | | | x | kfake/kafka_tests |
| EndTxn | 26 | | | | x | kfake/kafka_tests |
| TxnOffsetCommit | 28 | | | | x | kfake/kafka_tests |
