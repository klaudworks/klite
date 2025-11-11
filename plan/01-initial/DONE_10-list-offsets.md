# ListOffsets Handler (Key 2)

> **NOTE:** A basic ListOffsets handler was added in `08-produce.md` to support
> produce-consume integration tests. It lives in `internal/handler/list_offsets.go`
> and handles the core case (resolve -1/-2/-3 and timestamp lookups). This task
> should **extend** that handler with any additional edge cases, validation,
> and integration tests listed below.

After this work unit, clients can resolve special timestamp values to concrete
offsets. Used by consumers on startup to find where to begin reading.

Prerequisite: `08-produce.md` complete (records exist to query).

Read before starting: `06-partition-data-model.md` (listOffsets method,
storedBatch.MaxTimestamp, batch header byte offsets).

---

## Behavior

Resolves special timestamp values to concrete offsets.

## Timestamp Values

| Value | Name | Returns |
|-------|------|---------|
| -1 | `LATEST` | Current HW (next offset to be written) |
| -2 | `EARLIEST` | LogStartOffset (0 initially, advances after deletion/trim) |
| -3 | `MAX_TIMESTAMP` | Offset of the batch with the highest MaxTimestamp |
| >= 0 | Timestamp lookup | First offset at or after the given timestamp |

## Implementation

```
For each topic in request:
    For each partition in topic:
        1. Look up partition state
        2. Switch on Timestamp:
            -1 (LATEST):
                return Offset=HW, Timestamp=-1
            -2 (EARLIEST):
                return Offset=LogStart, Timestamp=-1
            -3 (MAX_TIMESTAMP):
                scan all batches, find the one with highest MaxTimestamp
                return that batch's BaseOffset, Timestamp=MaxTimestamp
            >= 0 (timestamp lookup):
                scan batches, find first where MaxTimestamp >= requested
                return that batch's BaseOffset, Timestamp=MaxTimestamp
```

## MAX_TIMESTAMP with Compressed Batches

For MAX_TIMESTAMP queries, the batch-level `MaxTimestamp` field in the header
is sufficient -- we don't need to decompress. The header always contains the
correct max timestamp regardless of compression.

For timestamp lookups (>= 0), the batch-level `MaxTimestamp` gives an upper
bound. To find the exact first record at or after a timestamp, you'd need to
decompress. **Decision: Use batch-level granularity.** Return the first batch
whose `MaxTimestamp >= requested`. This matches Kafka's behavior at the segment
index level (Kafka's time index also points to batch boundaries, not individual
records).

## Timestamp Lookup Efficiency by Phase

Timestamp lookups are rare (typically once per consumer startup recovery), so
linear scans are acceptable in early phases:

- **Phase 1 (in-memory):** Linear scan over `[]storedBatch`. All in RAM, fast.
- **Phase 3 (WAL + ring buffer):** Linear scan over ring buffer. Still in RAM.
- **Phase 4 (S3):** The S3 key namespace encodes base offsets, not timestamps.
  A timestamp lookup requires listing objects for the partition and checking
  batch timestamps. Since per-partition S3 objects contain raw RecordBatch
  bytes with timestamps in the batch header, binary search over the object
  list + reading the first batch header (first ~60 bytes) of candidate objects
  narrows the search efficiently. Each check is one S3 range-read of 64 bytes.
  For 1000 objects per partition: ~10 range reads (binary search). Acceptable
  for a rare operation.

No dedicated timestamp index is needed.

## Error Responses

| Condition | Error Code |
|-----------|------------|
| Unknown topic/partition | `UNKNOWN_TOPIC_OR_PARTITION` |

## Reference

- `.cache/repos/franz-go/pkg/kfake/02_list_offsets.go` -- correct ListOffsets behavior
- `.cache/repos/franz-go/pkg/kfake/kafka_tests/list_offsets_test.go` -- test scenarios to adapt
- `.cache/repos/kafka/core/src/test/scala/unit/kafka/server/ListOffsetsRequestTest.scala` -- edge cases

---

## When Stuck

These references are for understanding correct behavior and gathering context,
not for copying patterns. Find the optimal solution for our architecture.

1. Read `.cache/repos/franz-go/pkg/kfake/02_list_offsets.go` -- see how kfake
   handles all four timestamp modes (-1, -2, -3, >=0).
2. Check `.cache/repos/kafka/core/src/test/scala/unit/kafka/server/ListOffsetsRequestTest.scala`
   for edge cases.
3. `list_offsets_test.go` was already copied in `09-fetch.md`. The
   `TestListMaxTimestampWithEmptyLog` test is the key one for this work unit.
4. See `00b-testing.md` "When Stuck: Escalation Path" for additional tools.

---

## Acceptance Criteria

- Smoke tests pass:
  - `TestListOffsetsEarliest` -- verify returns 0 on fresh partition
  - `TestListOffsetsLatest` -- produce 5, verify returns 5
  - `TestListOffsetsMaxTimestamp` -- produce with known timestamps, verify
  - `TestListOffsetsTimestampLookup` -- lookup by timestamp, verify offset
  - `TestListOffsetsUnknownTopic` -- expect error
- `TestListOffsetsMaxTimestampEmptyLog` -- max timestamp on empty partition

### Verify

```bash
# This work unit:
go test ./test/integration/ -run 'TestListOffsets' -v -race -count=5

# Regression check (all prior work units — full Phase 1):
go test ./... -race -count=5
```

### Commit

After tests pass, stage and commit all changes:

```bash
git add -A
git commit -m "10: ListOffsets handler — earliest, latest, max timestamp, timestamp lookup"
```
