# Transactions

Idempotent producers and exactly-once semantics. Phase 4 (alongside S3).

Prerequisite: Phase 3 complete (WAL persistence, admin APIs, consumer groups).

Read before starting: `06-partition-data-model.md` (batch header — ProducerID,
ProducerEpoch, BaseSequence fields at known byte offsets),
`08-produce.md` (produce handler — you'll add idempotency checks before
pushBatch), `13-offset-management.md` (OffsetCommit — TxnOffsetCommit extends
this with pending-txn state), `16-metadata-log.md` (PRODUCER_ID entry type).

---

## Overview

Kafka transactions have two layers:
1. **Idempotent producer**: Deduplicates retried batches via (ProducerID, Epoch,
   BaseSequence). No data loss, no duplicates.
2. **Transactional producer**: Atomic writes across multiple partitions. Either
   all records are visible to consumers or none are.

Both share `InitProducerID` as the entry point.

---

## InitProducerID (Key 22)

### Behavior

Assigns a ProducerID and epoch to a producer. For transactional producers, also
registers the TransactionalID.

### Implementation

```
If TransactionalID is non-empty:
    If we've seen this TransactionalID before:
        Increment epoch, keep same ProducerID
        If there's an open transaction: abort it (fence the old producer)
    Else:
        Assign new ProducerID, epoch=0
        Register TransactionalID -> ProducerID mapping
Else (idempotent only):
    Assign new ProducerID, epoch=0
```

### ProducerID Assignment

Monotonic counter. Persisted as PRODUCER_ID entries in `metadata.log`
(see `16-metadata-log.md`). Current value recovered from replay on startup.

### State

```go
type ProducerState struct {
    ProducerID   int64
    Epoch        int16
    TxnID        *string  // nil for non-transactional
    // Per-partition sequence tracking (for dedup)
    Sequences    map[TopicPartition]SequenceState
    // Transaction state (if transactional)
    TxnState     TxnState
    TxnPartitions map[TopicPartition]bool  // Partitions in current txn
}

type SequenceState struct {
    LastSequence int32
    // Ring buffer of last 5 batches for dedup (matches Kafka's window)
    RecentBatches [5]BatchInfo
}

type TxnState int8
const (
    TxnNone TxnState = iota
    TxnOngoing
    TxnPrepareCommit
    TxnPrepareAbort
)
```

### kfake Reference

- `.cache/repos/franz-go/pkg/kfake/txns.go`
- `.cache/repos/franz-go/pkg/kfake/22_init_producer_id.go`

---

## Idempotent Producer

### Duplicate Detection

On every Produce request with a ProducerID:
1. Check `BaseSequence` against the producer's last known sequence for this partition
2. Expected: `LastSequence + 1` (or 0 for first batch)
3. If `BaseSequence == expected`: accept, update LastSequence
4. If `BaseSequence <= LastSequence`: duplicate, return success with the
   original offset (don't store again)
5. If `BaseSequence > expected + 1`: gap, return `OUT_OF_ORDER_SEQUENCE_NUMBER`

### Dedup Window

Kafka keeps the last 5 batches per (ProducerID, Partition) for dedup. We do
the same. This covers normal retry scenarios where the client resends a batch
that was actually committed but the ACK was lost.

---

## Transactional Produce

### AddPartitionsToTxn (Key 24)

Registers partitions as part of the current transaction. Must be called before
producing to a partition within a transaction.

```
1. Validate ProducerID and Epoch
2. Validate transaction is ongoing
3. Add partitions to TxnPartitions set
4. Return success
```

### AddOffsetsToTxn (Key 25)

Adds the consumer group's `__consumer_offsets` partition to the transaction.

```
1. Validate ProducerID and Epoch
2. Map GroupID -> offset storage
3. Add to TxnPartitions
4. Return FindCoordinator info (always self)
```

### TxnOffsetCommit (Key 28)

Commits offsets as part of a transaction. The offsets become visible only when
the transaction commits.

```
1. Validate ProducerID, Epoch, and GroupID
2. Store offsets in pending-txn-offsets (not yet visible)
3. Return success
```

### EndTxn (Key 26)

Commits or aborts the transaction.

```
If Committed:
    1. Write COMMIT control batch to each partition in TxnPartitions
    2. Make all pending offsets visible
    3. Clear transaction state
If Aborted:
    1. Write ABORT control batch to each partition in TxnPartitions
    2. Discard pending offsets
    3. Clear transaction state
```

### Control Batches

Transaction commit/abort markers are special RecordBatches:
- `Attributes` bit 4 set (transactional)
- `Attributes` bit 5 set (control batch)
- Single record with key = `EndTransactionMarker` (version=0, coordinatorEpoch)
- Commit marker: `ControlType=COMMIT` (1)
- Abort marker: `ControlType=ABORT` (0)

Control batches consume an offset like normal batches. They're stored in the
partition but filtered out by consumers (unless using READ_UNCOMMITTED).

### Isolation Levels on Fetch

With transactions, the Fetch handler behavior changes:

- `READ_UNCOMMITTED`: Return everything up to HW, including uncommitted txn data
- `READ_COMMITTED`: Return up to LSO (Last Stable Offset = min(HW, oldest
  open transaction's first offset)). Skip aborted transaction data.

The LSO calculation requires tracking open transactions per partition.

### Aborted Transaction Index

For `READ_COMMITTED` fetches, the response includes a list of aborted
transactions in the fetched range:

```go
// In FetchResponse per partition:
AbortedTransactions []AbortedTransaction{
    ProducerID  int64
    FirstOffset int64  // First offset of the aborted transaction in this partition
}
```

### kfake Reference

- `.cache/repos/franz-go/pkg/kfake/txns.go` -- transaction coordinator logic
- `.cache/repos/franz-go/pkg/kfake/24_add_partitions_to_txn.go`
- `.cache/repos/franz-go/pkg/kfake/25_add_offsets_to_txn.go`
- `.cache/repos/franz-go/pkg/kfake/26_end_txn.go`
- `.cache/repos/franz-go/pkg/kfake/28_txn_offset_commit.go`

### Kafka Test Reference

- `.cache/repos/kafka/core/src/test/scala/integration/kafka/api/TransactionsTest.scala`
  Key scenarios: basic txn produce+commit, txn produce+abort, txn offset commit,
  read_committed isolation, interleaved txn and non-txn data
- `.cache/repos/franz-go/pkg/kfake/kafka_tests/transactions_test.go`
  Tests: aborted txn invisible to read_committed, visible to read_uncommitted
- `.cache/repos/franz-go/pkg/kfake/kafka_tests/admin_fence_test.go`
  Tests: producer fencing after/before commit

---

## DescribeProducers (Key 61)

### Behavior

Returns active producer state for requested partitions. Used by admin tools
to inspect active transactions and debug stuck producers.

### Implementation

For each requested topic-partition:
1. Look up the partition's active producers (ProducerID, Epoch, LastSequence,
   current transaction start offset if any)
2. Return producer state list per partition

For single-broker, this is a direct read of the `ProducerState` map maintained
for idempotent/transactional produce dedup (see "Idempotent Producer" above).

### Error Responses

| Condition | Error Code |
|-----------|------------|
| Unknown topic/partition | `UNKNOWN_TOPIC_OR_PARTITION` |

### kfake Reference

- `.cache/repos/franz-go/pkg/kfake/61_describe_producers.go`

---

## DescribeTransactions (Key 65)

### Behavior

Returns the state of transactions by transactional ID. Used by admin tools to
debug stuck transactions — a common operational issue in Kafka deployments.
Part of KIP-664 (transaction observability).

### Version Notes

- v0: Supported (only version).

### Implementation

For each requested transactional ID:
1. Look up the transactional ID in the producer state map
2. If not found -> `TRANSACTIONAL_ID_NOT_FOUND`
3. Return:
   - TransactionalID
   - TransactionState: `Empty`, `Ongoing`, `PrepareCommit`, `PrepareAbort`,
     `CompleteCommit`, `CompleteAbort`, `Dead`
   - TransactionTimeoutMs
   - TransactionStartTimeMs (when the current transaction began, or -1)
   - ProducerID, ProducerEpoch
   - List of topic-partitions in the current transaction

For single-broker, this is a direct read of the `ProducerState` struct
maintained for transactional produce (see "State" section above).

### Error Responses

| Condition | Error Code |
|-----------|------------|
| Unknown transactional ID | `TRANSACTIONAL_ID_NOT_FOUND` |

### kfake Reference

- `.cache/repos/franz-go/pkg/kfake/65_describe_transactions.go` (delegates to
  `pids.doDescribeTransactions`)

---

## ListTransactions (Key 66)

### Behavior

Lists all transactions on the broker, optionally filtered by state or producer
ID. Part of KIP-664 (transaction observability).

### Version Notes

- v0-v2: Supported.
  - v0: Initial version
  - v1: DurationFilterMillis (filter by transaction duration)
  - v2: TransactionalIDPattern (regex filter)

### Implementation

```
1. Enumerate all registered transactional IDs from the producer state map
2. Apply filters:
   - StateFilters: only include transactions in the specified states
   - ProducerIDFilters: only include transactions with matching producer IDs
   - v1+: DurationFilterMillis: only include transactions older than threshold
3. Return list of: TransactionalID, ProducerID, TransactionState
```

### kfake Reference

- `.cache/repos/franz-go/pkg/kfake/66_list_transactions.go` (delegates to
  `pids.doListTransactions`)

---

## When Stuck

These references are for understanding correct behavior and gathering context,
not for copying patterns. Find the optimal solution for our architecture.

1. Read `.cache/repos/franz-go/pkg/kfake/txns.go` -- the primary reference for
   transaction coordinator logic (producer state, epoch bumping, fencing,
   commit/abort flow, and the `doDescribeTransactions`, `doListTransactions`,
   `doDescribeProducers` methods).
2. Read `.cache/repos/franz-go/pkg/kfake/22_init_producer_id.go` for
   InitProducerID behavior.
3. Read `.cache/repos/franz-go/pkg/kfake/26_end_txn.go` for EndTxn + control
   batch writing.
4. Read `.cache/repos/franz-go/pkg/kfake/65_describe_transactions.go` and
   `.cache/repos/franz-go/pkg/kfake/66_list_transactions.go` for transaction
   observability handler structure.
5. Check `.cache/repos/kafka/core/src/test/scala/integration/kafka/api/TransactionsTest.scala`
   for end-to-end transaction scenarios.
6. For idempotent dedup window behavior, search `.cache/repos/kafka/` for
   `ProducerStateManager` -- it manages the 5-batch dedup window.
7. See `00b-testing.md` "When Stuck: Escalation Path" for additional tools.

---

## kfake Test Reference

Reference the following files in `.cache/repos/franz-go/pkg/kfake/kafka_tests/`
for transaction test scenarios:
`transactions_test.go`, `admin_fence_test.go`, `describe_producers_test.go`.

Write equivalent tests in `test/integration/transactions_test.go`.

---

## Acceptance Criteria

- Integration tests pass:
  - `TestInitProducerID` -- get ProducerID, verify non-zero
  - `TestIdempotentProduce` -- produce same batch twice, verify dedup
  - `TestIdempotentOutOfOrder` -- gap in sequence, expect error
  - `TestTxnProduceCommit` -- txn produce + commit, read_committed consumer sees records
  - `TestTxnProduceAbort` -- txn produce + abort, read_committed consumer sees nothing
  - `TestTxnReadUncommitted` -- read_uncommitted sees txn data before commit
  - `TestTxnOffsetCommit` -- consume-transform-produce pattern
  - `TestProducerFencing` -- init with same txn ID, old producer fenced
  - `TestFenceAfterProducerCommit` -- fencing after commit
  - `TestFenceBeforeProducerCommit` -- fencing before commit
  - `TestDescribeProducersDefaultRoutesToLeader` -- describe active producers
  - `TestDescribeProducersAfterCommit` -- describe after txn commit
  - `TestDescribeTransactions` -- start txn, describe it, verify state + partitions
  - `TestDescribeTransactionsNotFound` -- unknown txn ID returns error
  - `TestListTransactions` -- multiple txns, list all
  - `TestListTransactionsFilterByState` -- filter by Ongoing state

### Verify

```bash
# This work unit:
go test ./test/integration/ -run 'TestInitProducerID|TestIdempotent|TestTxn|TestProducerFencing|TestFence|TestDescribeProducers|TestDescribeTransactions|TestListTransactions' -v -race -count=5

# Regression check (all prior work units — full Phase 4):
go test ./... -race -count=5
```

### Commit

After tests pass, stage and commit all changes:

```bash
git add -A
git commit -m "19: transactions — idempotent produce, transactional produce, EndTxn, exactly-once"
```
