# Testing (General Reference)

Test infrastructure, three-tier strategy, and guidance for adapting kfake tests.
Every agent should read this file before starting any work unit.
Per-phase test lists live in each work unit's acceptance criteria section --
this file covers the cross-cutting test infrastructure and debugging guidance.

---

## Strategy: Tests Before Code

The agent's workflow for each phase is:
1. **Copy/write tests first** (they all fail)
2. **Implement until tests pass**
3. **Move to next phase**

This gives immediate feedback. A green test = correct behavior. A red test =
clear signal about what's broken.

---

## Test Organization

All integration tests live in a single directory: `test/integration/`. Tests
are organized by API group (one file per group), not by tier or origin.

```
test/integration/
  helpers_test.go           # StartBroker, NewClient, ProduceSync, ConsumeN, etc.
  connect_test.go           # TCP connectivity, malformed requests
  api_versions_test.go      # ApiVersions handler
  metadata_test.go          # Metadata, auto-create
  produce_test.go           # Produce handler, acks, compression, ordering
  fetch_test.go             # Fetch, long-poll, size limits, KIP-74
  list_offsets_test.go      # ListOffsets, timestamp lookups
  consumer_group_test.go    # JoinGroup, SyncGroup, Heartbeat, LeaveGroup, static membership
  offset_management_test.go # OffsetCommit, OffsetFetch, commit-resume
  admin_test.go             # DeleteTopics, CreatePartitions, DescribeConfigs, etc.
  wal_test.go               # WAL persistence, crash recovery, restart
  s3_test.go                # S3 flush, read cascade, disaster recovery
  transactions_test.go      # Idempotency, transactions, fencing
```

**Where test scenarios come from:** Use kfake's `kafka_tests/` and Kafka's
own test suite as *inspiration* for what scenarios to cover, but write all
tests from scratch for klite. This avoids maintaining a fork of kfake's
test files and their adaptation boilerplate. When a kfake test covers a
scenario we need, re-implement its assertions in our own test file — don't
copy the file.

Reference scenarios are listed in each work unit's acceptance criteria section
and in the "Kafka Test Repo Reference" section below.

---

## Test Infrastructure

### Starting the Broker

```go
// test/integration/helpers_test.go

// StartBroker starts a klite broker in-process on a random port.
// Registers cleanup with t.Cleanup().
func StartBroker(t *testing.T, opts ...BrokerOpt) *TestBroker {
    t.Helper()
    ln, err := net.Listen("tcp", "127.0.0.1:0")
    require.NoError(t, err)

    cfg := DefaultConfig()
    for _, o := range opts {
        o(&cfg)
    }
    cfg.Listener = ln

    b := NewBroker(cfg)
    ctx, cancel := context.WithCancel(context.Background())
    t.Cleanup(func() {
        cancel()
        b.Wait() // Wait for clean shutdown
    })
    go b.Run(ctx)

    // Wait for broker to be ready (accept connections)
    waitForBroker(t, ln.Addr().String())

    return &TestBroker{Addr: ln.Addr().String(), Broker: b}
}

type TestBroker struct {
    Addr   string
    Broker *broker.Broker
}

type BrokerOpt func(*broker.Config)

func WithAutoCreateTopics(v bool) BrokerOpt {
    return func(c *broker.Config) { c.AutoCreateTopics = v }
}

func WithDefaultPartitions(n int) BrokerOpt {
    return func(c *broker.Config) { c.DefaultPartitions = n }
}
```

### Creating Clients

```go
func NewClient(t *testing.T, addr string, opts ...kgo.Opt) *kgo.Client {
    t.Helper()
    allOpts := []kgo.Opt{
        kgo.SeedBrokers(addr),
        kgo.RequestRetries(0),   // Fail fast in tests
    }
    allOpts = append(allOpts, opts...)
    cl, err := kgo.NewClient(allOpts...)
    require.NoError(t, err)
    t.Cleanup(cl.Close)
    return cl
}

func NewAdminClient(t *testing.T, addr string) *kadm.Client {
    t.Helper()
    cl := NewClient(t, addr)
    return kadm.NewClient(cl)
}
```

### Produce/Consume Helpers

```go
func ProduceSync(t *testing.T, cl *kgo.Client, records ...*kgo.Record) {
    t.Helper()
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    results := cl.ProduceSync(ctx, records...)
    for _, r := range results {
        require.NoError(t, r.Err)
    }
}

func ProduceN(t *testing.T, cl *kgo.Client, topic string, n int) []*kgo.Record {
    t.Helper()
    records := make([]*kgo.Record, n)
    for i := range records {
        records[i] = &kgo.Record{
            Topic: topic,
            Key:   []byte(fmt.Sprintf("key-%d", i)),
            Value: []byte(fmt.Sprintf("value-%d", i)),
        }
    }
    ProduceSync(t, cl, records...)
    return records
}

func ConsumeN(t *testing.T, cl *kgo.Client, n int, timeout time.Duration) []*kgo.Record {
    t.Helper()
    ctx, cancel := context.WithTimeout(context.Background(), timeout)
    defer cancel()
    var records []*kgo.Record
    for len(records) < n {
        fetches := cl.PollFetches(ctx)
        require.NoError(t, ctx.Err(), "timed out waiting for %d records, got %d", n, len(records))
        fetches.EachRecord(func(r *kgo.Record) {
            records = append(records, r)
        })
    }
    return records[:n]
}
```

These mirror kfake's `kafka_tests/helpers_test.go` patterns.

### Goroutine Leak Detection

Use `go.uber.org/goleak` to catch leaked goroutines. This is critical for
kafka-light because we spawn goroutines per-connection, per-group, for the WAL
writer, retention loop, compaction loop, and S3 flush pipeline. A leaked
goroutine means a resource leak, a potential deadlock, or a shutdown bug.

```go
// test/integration/main_test.go
func TestMain(m *testing.M) {
    goleak.VerifyTestMain(m)
}
```

What goleak catches:
- Fetch long-poll goroutines that don't wake on broker shutdown
- Group coordinator goroutines that outlive their group
- Timer goroutines from `time.After` that outlive the test
- WAL writer goroutines that don't stop on context cancellation
- Connection handler goroutines that leak on malformed requests

Add `goleak.VerifyTestMain(m)` to every test package's `TestMain`. For unit
test packages that don't have a `TestMain`, add one. The overhead is negligible
(goroutine count check at process exit).

### Deterministic Clock

For anything involving timeouts, intervals, or timestamps, inject a `Clock`
interface instead of calling `time.Now()` / `time.After()` / `time.NewTicker()`
directly. This eliminates an entire class of flaky tests — the "works on my
laptop, fails in CI" kind — and lets tests control time progression in
microseconds instead of waiting for real seconds.

```go
// internal/clock/clock.go
type Clock interface {
    Now() time.Time
    After(d time.Duration) <-chan time.Time
    NewTicker(d time.Duration) *Ticker
}

// Ticker wraps a channel so test clocks can drive it.
type Ticker struct {
    C    <-chan time.Time
    stop func()
}
func (t *Ticker) Stop() { t.stop() }

// RealClock delegates to the time package. Used in production.
type RealClock struct{}
func (RealClock) Now() time.Time                         { return time.Now() }
func (RealClock) After(d time.Duration) <-chan time.Time  { return time.After(d) }
func (RealClock) NewTicker(d time.Duration) *Ticker {
    t := time.NewTicker(d)
    return &Ticker{C: t.C, stop: t.Stop}
}
```

Test code uses a fake clock that can advance time instantly:

```go
// test clock (in test helper or test file)
type FakeClock struct {
    mu  sync.Mutex
    now time.Time
    // channels/callbacks for After and Ticker
}
func (c *FakeClock) Advance(d time.Duration) { ... }
```

**Features that MUST use the Clock interface** (anywhere `time.Now()`,
`time.After()`, `time.NewTicker()`, or `time.Since()` appears in
non-test code):

| Feature | File | What Uses Wall Time |
|---------|------|---------------------|
| Fetch long-poll | `09-fetch.md` | `time.NewTimer(MaxWaitMs)` — wait for data or timeout |
| LogAppendTime | `08-produce.md` | `time.Now().UnixMilli()` — broker-assigned timestamps |
| Heartbeat session timeout | `12-group-coordinator.md` | `time.Timer` — expire members that stop heartbeating |
| Rebalance timeout | `12-group-coordinator.md` | `time.Timer` — deadline for all members to re-join |
| WAL fsync batching | `15-wal-format-and-writer.md` | `time.NewTicker(syncInterval)` — 2ms fsync batch window |
| Retention enforcement | `21-retention.md` | `time.NewTicker(5m)` — periodic scan; `time.Now()` for cutoff comparison |
| Compaction scheduling | `22-log-compaction.md` | `time.NewTicker(30s)` — periodic check; `time.Since(lastCompacted)` for staleness |
| S3 unified sync | `18-s3-storage.md` | `time.NewTicker(flushInterval)` — periodic flush to S3 |
| Committed offset expiry | `13-offset-management.md` | Future: `offsets.retention.minutes` timestamp comparison |

**Rule:** If production code needs wall-clock time, accept a `Clock` parameter
(via config, constructor, or struct field). Never call `time.Now()` etc.
directly in `internal/` code. This is a low-effort habit that pays for itself
immediately in test reliability.

---

## Using kfake Tests as Reference

kfake's `kafka_tests/` directory contains ~48 tests derived from Kafka's own
integration tests. **Do not copy these files.** Instead, use them as reference
for what scenarios to test and what assertions to make, then write equivalent
tests from scratch in the appropriate `test/integration/*.go` file.

Key kfake test files to reference for each work unit:

| Work Unit | kfake Reference Files |
|-----------|----------------------|
| `07-create-topics.md` | `static_membership_test.go` (CreateTopics scenarios) |
| `09-fetch.md` | `producer_compression_test.go`, `consumer_fetch_test.go`, `consumer_poll_test.go`, `consumer_assign_test.go`, `list_offsets_test.go` |
| `13-offset-management.md` | `consumer_commit_test.go` |
| `19-transactions.md` | `transactions_test.go`, `admin_fence_test.go`, `describe_producers_test.go` |

Never reference `consumer_group_848_test.go` (KIP-848 — not implemented).

---

## Test Execution

### Race Detector and Stress Runs

**Always run tests with `-race`.** The race detector is mandatory -- it catches
data races, unsafe shared state, and missing synchronization that no amount of
code review can reliably find. The 2-4x slowdown is worth it.

**Always run with `-count=5` (or higher).** Many races and timing bugs only
manifest on the 3rd or 7th run because goroutine scheduling is nondeterministic.
Combined with `-race`, this is extremely effective at catching intermittent
concurrency bugs. Use `-count=10` or `-count=20` when debugging a flaky test.

**LocalStack note (Phase 4+):** S3 tests use LocalStack via testcontainers.
The testcontainer lifecycle is managed by `TestMain` -- one container per
`go test` invocation, shared across all test runs within that invocation.
`-count=5` re-runs each test 5 times within the same process, so LocalStack
starts once. This is safe and correct.

### Running All Tests for a Phase

```bash
# Phase 1
go test ./test/integration/ -run 'TestConnect|TestApiVersions|TestMetadata|TestCreateTopic|TestProduce|TestFetch|TestListOffsets|TestProduceConsume' -v -race -count=5

# Phase 2
go test ./test/integration/ -run 'TestFindCoordinator|TestJoinGroup|TestSyncGroup|TestHeartbeat|TestSession|TestLeaveGroup|TestOffsetCommit|TestOffsetFetch|TestConsume|TestStatic' -v -race -count=5

# All tests
go test ./... -v -race -count=5
```

### Test Parallelism

Each test gets its own broker on a random port. Tests are safe to run in
parallel. Use `t.Parallel()` in each test. Run with `-parallel 8` or higher.

### Timeout

Default test timeout: 60s per test. Override with `-timeout 120s` for slow CI
or when using high `-count` values with `-race`.

---

## Kafka Test Repo Reference

These are available in `.cache/repos/kafka/` for the agent to consult when a
specific edge case needs coverage:

### Produce Edge Cases

| File | Key Tests |
|------|-----------|
| `ProduceRequestTest.scala` | `testSimpleProduceRequest`, `testProduceWithInvalidTimestamp`, `testCorruptLz4ProduceRequest` |
| `PlaintextProducerSendTest.scala` | `testAutoCreateTopic`, `testSendRecordBatchWithMaxRequestSizeAndHigher` |
| `BaseProducerSendTest.scala` | `testSendOffset`, `testSendToPartition` |

### Fetch Edge Cases

| File | Key Tests |
|------|-----------|
| `FetchRequestTest.scala` | `testBrokerRespectsPartitionsOrderAndSizeLimits`, `testFetchRequestV4WithReadCommitted` |
| `FetchRequestMaxBytesTest.scala` | `testConsumeMultipleRecords` |

### Consumer Group Error Paths

| File | Key Tests |
|------|-----------|
| `JoinGroupRequestTest.scala` | MEMBER_ID_REQUIRED flow, unknown member, multi-member rebalance |
| `SyncGroupRequestTest.scala` | Unknown member, wrong generation, leader/follower ordering |
| `HeartbeatRequestTest.scala` | Heartbeat in each group state |
| `LeaveGroupRequestTest.scala` | Batch leave, static member fencing |
| `OffsetCommitRequestTest.scala` | Admin commits, unknown member, stale generation |
| `OffsetFetchRequestTest.scala` | Unknown group, fetch all offsets, multi-group batch |

### Transactions

| File | Key Tests |
|------|-----------|
| `TransactionsTest.scala` | Txn produce+commit, txn produce+abort, read_committed isolation |

---

## Debugging Failed Tests

When a test fails, the agent should:

1. **Fix compilation errors first** -- run `go build ./...` and `go vet ./...`.
   Fix all type errors, undefined identifiers, and vet warnings before anything
   else. Do not try to debug test failures with broken code.
2. **Run the single most basic test** for the current work unit. Don't run the
   full suite until the simplest case works.
3. **Read the error message carefully** -- it usually points to the specific
   behavior or response field that's wrong.
4. **Check your handler** -- does it return the expected error code? Is the
   response struct populated correctly for this API version?
5. **Check wire framing** -- is the response correctly encoded? Common early
   bugs: wrong flexible version handling, missing tag byte, wrong response
   header for ApiVersions (key 18).
6. **Add debug logging** -- temporarily set log level to debug, rerun single
   test.
7. **Fix one test at a time**, re-running after each fix. Only after all
   current-work-unit tests pass, run `go test ./... -race -count=5`.

---

## When Stuck: Escalation Path

Each work unit has a "When Stuck" section with specific file references. The
general escalation strategy is:

**Important:** These references are for *understanding correct behavior* and
*gathering useful information*, not for copying code or blindly following
patterns. kfake, Kafka, and AutoMQ each made different design tradeoffs for
different constraints. The goal is always to find the optimal solution for
klite -- use these sources as inspiration and as a way to understand
what the correct protocol behavior should be, then implement the best solution
for our architecture.

### 1. Check Reference Implementations (in `.cache/repos/`)

Three reference codebases are available locally. Use a repo explorer subagent
to search them efficiently -- don't try to read them file by file.

| Repo | Best For | Location |
|------|----------|----------|
| **franz-go/kfake** | Correct per-API behavior, error codes, version-specific fields | `.cache/repos/franz-go/pkg/kfake/` |
| **Apache Kafka** | Edge cases, protocol spec compliance, test expectations | `.cache/repos/kafka/core/src/test/scala/` |
| **AutoMQ** | WAL design, S3 storage patterns, fsync batching | `.cache/repos/automq/` |

**kfake handler files** (e.g., `00_produce.go`, `01_fetch.go`) are the fastest
way to answer "what should happen when a client does X?" They implement the
same API we do, in Go, using the same kmsg codec.

**Kafka test files** are the authoritative source for edge case behavior. When
kfake doesn't cover a scenario, check the Kafka test suite at
`.cache/repos/kafka/core/src/test/scala/unit/kafka/server/`.

**AutoMQ** is useful for WAL and S3 patterns (Phase 3-4). Check
`.cache/repos/automq/s3stream/` for WAL writer design.

### 2. Search the Web

Use the Perplexity web search tool for Kafka protocol questions that aren't
answered by the local repos. Useful for:
- KIP details (e.g., "KIP-74 fetch behavior when first batch exceeds max bytes")
- Wire protocol edge cases
- Kafka error code semantics

### 3. Check Documentation

Use a docs explorer subagent to search Kafka's official documentation at
`https://kafka.apache.org/documentation/` for protocol behavior questions.
Also useful: `https://cwiki.apache.org/confluence/display/KAFKA/` for KIP
specifications.
