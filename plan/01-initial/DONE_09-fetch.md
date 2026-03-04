# Fetch Handler (Key 1)

> **NOTE:** A basic Fetch handler was added in `08-produce.md` to support
> produce-consume integration tests. It lives in `internal/handler/fetch.go`
> and handles the simple case (return available data, no long-polling, no
> sessions, no MinBytes wait). This task should **extend** that handler with
> the full implementation below (long-polling, fetch sessions, MinBytes/MaxWaitMs,
> size limits, TopicID resolution for v13+).

The most complex Phase 1 handler. After this work unit, clients can consume
records from topics with long-polling, size limits, and KIP-74 behavior.

Prerequisite: `08-produce.md` complete (records can be produced).

Read before starting: `06-partition-data-model.md` (fetchFrom, searchOffset,
fetchWaiter, waiterMu — long-polling data structures), `01-foundations.md`
(concurrency model — partition read lock, shutdown interaction with long-poll),
`03-tcp-and-wire.md` (pipelining — why Fetch must not block the connection).

---

## Versions: 4-16

The broker advertises Fetch v4-16. Versions below 4 use the legacy MessageSet
format and are excluded (see Non-Goals in `00-overview.md`).

Key version milestones:
- **v4**: RecordBatch format, isolation level field
- **v5**: LogStartOffset in response
- **v7**: Fetch sessions (KIP-227: SessionID, SessionEpoch fields)
- **v9**: CurrentLeaderEpoch in request partitions
- **v11**: Preferred read replica (KIP-392)
- **v12**: Flexible versions (tagged fields)
- **v13**: TopicID replaces topic name in request/response (KIP-516)
- **v15**: ReplicaState replaces ReplicaID (KIP-903)
- **v16**: EpochEndOffset in diverging epoch response

Phase 1 handles v4-16 by accepting all version-specific fields via `kmsg`
codec but only acting on the common subset. Version-gated behavior:
- v4-6: no session fields -- straightforward full fetch
- v7+: session fields present -- return `FETCH_SESSION_ID_NOT_FOUND` for
  non-zero SessionID (forces fallback to full fetch, see below)
- v13+: TopicID in request -- resolve topic by UUID instead of name

## Behavior

Returns records from requested topic-partitions starting at the specified offset.

## Implementation

```
1. For each topic in request:
    c.mu.RLock()
    td := c.topics[topic]
    c.mu.RUnlock()

    For each partition in topic:
        pd := td.partitions[partition]

        pd.mu.RLock()
        a. Validate FetchOffset: if < LogStart or > HW, return OFFSET_OUT_OF_RANGE
        b. batches = pd.fetchFrom(FetchOffset, PartitionMaxBytes)
        c. hw = pd.hw
        pd.mu.RUnlock()

        d. KIP-74: fetchFrom always includes at least one complete batch,
           even if it exceeds PartitionMaxBytes
2. Apply response-level MaxBytes across all partitions
3. Set HighWatermark in each partition response

If no data available and MaxWaitMs > 0:
    Register waiter (under pd.waiterMu), block up to MaxWaitMs.
    Wake up early if new data arrives (Produce calls notifyWaiters).
    On wakeup or timeout: cleanup waiter from all registered partitions.
    Then re-fetch under pd.mu.RLock().
```

## Zero-Copy Serving

For tail consumers (reading recent data), the fetch path should serve bytes
with minimal copying:

- **In-memory (Phase 1):** Return a reference to the stored `[]byte` slice
  directly. The response encoder writes these bytes to the socket via the
  buffered writer. No intermediate copy needed -- `AppendTo` for Fetch responses
  already does `append(dst, batch.RawBytes...)` which goes straight into the
  write buffer.
- **WAL (Phase 3):** Use `pread()` (via `file.ReadAt()`) to read batch bytes
  from the WAL segment file directly into the response buffer.
- **S3 (Phase 4):** S3 range reads are inherently high-latency (~50ms).

## Fetch Sessions (Key Decision)

franz-go uses fetch sessions by default (Fetch v7+). The client sends
`SessionID` and `SessionEpoch` to maintain incremental fetch state.

**Decision: Return `FETCH_SESSION_ID_NOT_FOUND` (error code 70) for any
non-zero session ID in Phase 1.** This forces franz-go to fall back to full
fetches. Simpler to implement, and the performance difference is negligible
for single-broker.

Add fetch session support in a later phase if needed.

## Isolation Levels

- `READ_UNCOMMITTED` (0): Return up to HW (= LEO for single broker with no
  transactions). This is the default.
- `READ_COMMITTED` (1): Return up to Last Stable Offset (LSO). Without
  transactions, LSO = HW. With transactions (Phase 4), LSO = min(HW, lowest
  open transaction offset).

Phase 1: both isolation levels behave identically (no transactions).

---

## Long Polling (MaxWaitMs / MinBytes)

**MinBytes** is a Fetch request field where the client says "don't respond until
you have at least this many bytes." It avoids wasteful empty responses when
polling an idle topic. **MaxWaitMs** is the upper bound on how long the broker
will wait before responding regardless.

If the initial fetch finds fewer bytes than `MinBytes` across all requested
partitions, block for up to `MaxWaitMs`. On wakeup (data arrived or timeout),
re-fetch once and return whatever is available -- no re-check against MinBytes,
no re-registration loop. This matches Kafka's behavior and avoids spin loops
under pathological workloads (e.g., tiny records trickling in, never satisfying
MinBytes).

### Data Structures

```go
type fetchWaiter struct {
    once sync.Once       // ensures ch is closed exactly once
    ch   chan struct{}    // closed when data arrives (wake-up signal only, no data)
}

// closeOnce safely closes the wake channel. Multiple partitions may call this
// concurrently for the same shared waiter -- sync.Once prevents double-close panic.
func (w *fetchWaiter) closeOnce() {
    w.once.Do(func() { close(w.ch) })
}
```

### Full Flow: Lifecycle of a Long-Poll Fetch

```
1. Client sends Fetch(partitions=[A,B,C], MinBytes=1024, MaxWaitMs=30000)

2. Handler fetches all 3 partitions (each under pd.mu.RLock()):
   - Partition A: 500 bytes of data
   - Partition B: empty
   - Partition C: empty
   - Total: 500 bytes < MinBytes (1024) -> need to wait

3. Create ONE shared waiter: w := &fetchWaiter{ch: make(chan struct{})}
   Register w on partitions B and C (the empty ones), each under pd.waiterMu:
       pd.waiterMu.Lock()
       pd.waiters = append(pd.waiters, w)
       pd.waiterMu.Unlock()

4. Handler blocks:
       timer := time.NewTimer(time.Duration(req.MaxWaitMs) * time.Millisecond)
       defer timer.Stop()
       select {
       case <-w.ch:        // some partition got data
       case <-timer.C:     // 30s elapsed, return whatever we have
       case <-shutdownCh:  // broker shutting down
       }

5. Meanwhile, a producer writes to partition B:
   -> Produce handler calls notifyWaiters() on B after releasing pd.mu
   -> notifyWaiters calls w.closeOnce() -> handler unblocks at step 4

6. No explicit cleanup needed. `notifyWaiters()` already drains the entire
   slice (`pd.waiters = nil`), which removes all waiter references. On
   partitions that don't receive a Produce, stale waiters hold a pointer
   to an already-closed channel (~64 bytes) — negligible cost. They are
   cleaned up the next time that partition receives any Produce.

7. Handler re-fetches ALL partitions (A, B, C) under their read locks:
   - Partition A: still 500 bytes
   - Partition B: now 200 bytes (new data)
   - Partition C: still empty
   - Total: 700 bytes -> return it (NO MinBytes re-check, single wakeup only)

8. Handler sends Fetch response to client with data from A and B

9. Done. The waiter and channel are garbage collected.
   Next Fetch request from this client creates a fresh waiter at step 1.
```

The channel carries NO record data -- it's purely a wake-up signal (`chan struct{}`).
Record data is read from partitions (in-memory batches in Phase 1) in step 7.

### Shared Channel: Why sync.Once

A single `fetchWaiter` is registered on multiple partitions. If two partitions
receive Produces simultaneously, both call `notifyWaiters()` which both try to
close the same channel. Closing an already-closed channel panics in Go.
`sync.Once` ensures exactly one close, making concurrent notification safe.

### Implementation

```go
// Fetch handler -- long polling path:
if totalBytes < req.MinBytes && req.MaxWaitMs > 0 {
    w := &fetchWaiter{ch: make(chan struct{})}
    // Register on all partitions that had no data
    for _, pd := range emptyPartitions {
        pd.waiterMu.Lock()
        pd.waiters = append(pd.waiters, w)
        pd.waiterMu.Unlock()
    }

    timer := time.NewTimer(time.Duration(req.MaxWaitMs) * time.Millisecond)
    defer timer.Stop()
    select {
    case <-w.ch:
        // Woken by produce -- re-fetch all partitions below
    case <-timer.C:
        // Timeout -- return whatever we had from the initial fetch
    case <-shutdownCh:
        // Broker shutting down
    }

    // No explicit cleanup needed — notifyWaiters() drains pd.waiters to nil,
    // removing all references. Stale waiters on idle partitions are harmless
    // (~64 bytes each, cleaned up on next Produce to that partition).

    // Re-fetch ALL partitions (not just the one that woke us)
    // ... same logic as initial fetch, rebuild response ...
}

// Produce handler -- after releasing pd.mu.Lock():
func (pd *partData) notifyWaiters() {
    pd.waiterMu.Lock()
    waiters := pd.waiters
    pd.waiters = nil
    pd.waiterMu.Unlock()
    for _, w := range waiters {
        w.closeOnce()  // safe even if another partition already closed it
    }
}
```

### Design Properties

- **`waiterMu` is separate from `pd.mu`**: registering a waiter doesn't require
  a partition write lock. `notifyWaiters()` is called AFTER releasing `pd.mu`
  from the Produce path, so it doesn't extend the partition lock hold time.
- **One wake channel per Fetch request**: no dynamic `select` over N channels.
  Shared across all empty partitions via `sync.Once` close guard.
- **Single wakeup, no re-loop**: after wakeup, re-fetch once and return. No
   MinBytes re-check. Matches Kafka behavior, avoids spin under pathological
   workloads (tiny records never satisfying MinBytes).
- **No explicit cleanup**: `notifyWaiters()` drains `pd.waiters` to nil,
   removing all references. Stale waiters on idle partitions hold ~64 bytes
   each and are cleaned up on next Produce. No cleanup loop needed.
- **`time.NewTimer` + `defer timer.Stop()`**: deterministic timer cleanup.

---

## Size Limit Enforcement

Three levels of size limits:
1. **MaxBytes** (response level): Total response size across all partitions
2. **PartitionMaxBytes** (per-partition): Max data per partition
3. **KIP-74**: If the first batch in a partition exceeds PartitionMaxBytes,
   return it anyway (otherwise large records are permanently stuck)

Apply limits in order: per-partition first, then response-level.

## Error Responses

| Condition | Error Code |
|-----------|------------|
| Unknown topic/partition | `UNKNOWN_TOPIC_OR_PARTITION` |
| Offset < LogStart | `OFFSET_OUT_OF_RANGE` |
| Offset > HW | `OFFSET_OUT_OF_RANGE` |
| Invalid session ID | `FETCH_SESSION_ID_NOT_FOUND` |

## Reference

- `.cache/repos/franz-go/pkg/kfake/01_fetch.go` -- correct Fetch behavior (size limits, long-poll, isolation levels)
- `.cache/repos/kafka/core/src/test/scala/unit/kafka/server/FetchRequestTest.scala` -- edge cases
- `.cache/repos/franz-go/pkg/kfake/kafka_tests/consumer_fetch_test.go` -- test scenarios to adapt

---

## When Stuck

These references are for understanding correct behavior and gathering context,
not for copying patterns. Find the optimal solution for our architecture.

1. Read `.cache/repos/franz-go/pkg/kfake/01_fetch.go` -- see how kfake handles
   size limits, KIP-74, fetch sessions, isolation levels, and long-polling.
2. Check `.cache/repos/kafka/core/src/test/scala/unit/kafka/server/FetchRequestTest.scala`
   for edge cases (size limits, partition ordering, read committed).
3. For long-polling design questions, check how kfake manages waiters and
   wake-up in its fetch handler.
4. For fetch session behavior (FETCH_SESSION_ID_NOT_FOUND), search kfake for
   how it handles `SessionID` and `SessionEpoch`.
5. See `00b-testing.md` "When Stuck: Escalation Path" for additional tools.

---

## kfake Test Reference

Reference the following files in `.cache/repos/franz-go/pkg/kfake/kafka_tests/`
for fetch/consume/compression test scenarios:
`producer_compression_test.go`, `consumer_fetch_test.go`, `consumer_poll_test.go`,
`consumer_assign_test.go`, `list_offsets_test.go`.

Write equivalent tests in `test/integration/fetch_test.go`.

---

## Acceptance Criteria

- Integration tests pass:
  - `TestFetchFromStart` -- produce N, fetch from offset 0, verify all N
  - `TestFetchFromMiddle` -- produce 10, fetch from offset 5, verify 5 records
  - `TestFetchEmpty` -- fetch from empty partition, verify empty response
  - `TestFetchOutOfRange` -- fetch from offset beyond HW, verify error
  - `TestFetchMaxBytes` -- verify size limit enforcement
  - `TestFetchLargeRecord` -- KIP-74: large record returned even if > max bytes
  - `TestCompression` -- produce with gzip/snappy/lz4/zstd, consume, verify
  - `TestAssignAndConsume` -- direct partition assignment, consume records
  - `TestFetchRecordLargerThanFetchMaxBytes` -- KIP-74 first-batch override
  - `TestMaxPollRecords` -- polling with record limits
  - `TestThreeRecordsInOneBatch` -- multi-record batch roundtrip

### Verify

```bash
# This work unit:
go test ./test/integration/ -run 'TestFetch|TestCompression|TestAssign|TestMaxPoll|TestThree' -v -race -count=5

# Regression check (all prior work units):
go test ./... -race -count=5
```

### Commit

After tests pass, stage and commit all changes:

```bash
git add -A
git commit -m "09: Fetch handler — long-polling, size limits, KIP-74, compression pass-through"
```
