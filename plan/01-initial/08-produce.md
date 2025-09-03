# Produce Handler (Key 0)

After this work unit, clients can produce records to topics and receive
offset acknowledgments.

Prerequisite: `07-create-topics.md` complete (topics can be created).

Read before starting: `06-partition-data-model.md` (pushBatch, parseBatchHeader,
storedBatch, offset assignment, CRC regions), `01-foundations.md` (concurrency
model — partition write lock), `03-tcp-and-wire.md` (acks=0 sequence number
handling, response suppression).

---

## Versions: 3-11

The broker advertises Produce v3-11. Versions below 3 use the legacy MessageSet
format and are excluded (see Non-Goals in `00-overview.md`).

Key version milestones:
- **v3**: RecordBatch format, transactional produce support
- **v5**: LogStartOffset in response
- **v8**: ErrorMessage string in response (KIP-467)
- **v9**: Flexible versions (tagged fields)
- **v12**: KIP-890 implicit partition addition for transactions
- **v13**: TopicID in request/response (KIP-516)

v12-13 are above our initial ceiling but can be added later.

## Behavior

Accepts one or more RecordBatches per topic-partition. Assigns offsets. Returns
base offset and error status per partition.

## One RecordBatch per Partition per Produce Request

The `Records []byte` field in `kmsg.ProduceRequestTopicPartition` is a single
byte blob per partition. The Kafka wire format technically allows concatenating
multiple RecordBatches in this blob, but **all major clients send exactly one**:

- **franz-go**: `map[int32]*recBatch` -- one batch per partition (sink.go:2305)
- **librdkafka**: one batch per partition
- **Java client**: one batch per partition

**Single-batch validation:** parse exactly one RecordBatch header via
`parseBatchHeader(records)` (direct byte reads), validate that `BatchLength`
covers the full byte slice (`BatchLength == len(Records) - 12`), return
`CORRUPT_MESSAGE` if there are trailing bytes. If multi-batch support is ever
needed (unlikely), it's a backward-compatible change (loop instead of reject).

Note: a single Produce **request** covers multiple topics and partitions -- one
`Records` blob per partition, each containing exactly one RecordBatch.

## Implementation

```
For each topic in request:
    c.mu.RLock() -- read lock on cluster to look up topic
    td := c.topics[topic]
    c.mu.RUnlock()

    For each partition in topic:
        pd := td.partitions[partition]

        1. Validate minimum size and parse batch header (direct byte reads):
            raw := rp.Records
            if len(raw) < 61 -> CORRUPT_MESSAGE
            meta, err := parseBatchHeader(raw)
            if err != nil -> CORRUPT_MESSAGE

        2. Validate the batch covers the full Records slice:
            if int(meta.BatchLength) != len(raw) - 12 -> CORRUPT_MESSAGE
            (12 = 8-byte BaseOffset + 4-byte BatchLength prefix)

        3. Validate batch contents (BEFORE taking partition lock):
            a. meta.Magic == 2 -> else CORRUPT_MESSAGE
            b. CRC32C check: crc32.Checksum(raw[21:], crc32cTable) == meta.CRC
               -> else CORRUPT_MESSAGE
            c. len(raw) <= max.message.bytes -> else MESSAGE_TOO_LARGE
            d. meta.LastOffsetDelta == meta.NumRecords - 1 -> else CORRUPT_MESSAGE
            e. If topic timestamp.type == LogAppendTime:
                - Overwrite MaxTimestamp (bytes 35-42) + BaseTimestamp (bytes 27-34)
                  with time.Now().UnixMilli() (in-place on raw)
                - Set timestamp type bit in Attributes (byte 21-22, in-place on raw)
                - Recalculate CRC over raw[21:], write to raw[17:21]

        4. pd.mu.Lock()
           baseOffset = pd.pushBatch(raw, meta)
           pd.mu.Unlock()
           pd.notifyWaiters()

        5. Return base offset in response
```

Note: validation (steps 1-3) happens BEFORE taking the partition lock. Only
the append (step 4) holds the lock. This minimizes lock hold time -- validation
can be expensive (CRC check, LogAppendTime rewrite) and doesn't need partition
state.

## Performance Budget

The produce path is the most latency-sensitive code in the broker. Per-request
time budget for a single 1KB batch (target: <20us CPU time, excluding fsync):

| Step | Budget | Notes |
|------|--------|-------|
| Read frame from bufio | ~1us | Already buffered, memcpy only |
| Parse request header | ~0.5us | Fixed-size fields, no alloc |
| `kmsg.ReadFrom(body)` | ~1-2us | Parses topic/partition structure |
| Extract batch header (61B) | ~0.1us | Direct byte reads |
| Assign offset (overwrite 8B) | ~0.01us | In-place mutation |
| Copy batch to WAL buffer | ~0.3us | memcpy 1KB |
| Encode response (`AppendTo`) | ~1-2us | Into pooled buffer |
| Write response to bufio | ~0.5us | memcpy into write buffer |
| **Total CPU per request** | **~5us** | |

**Allocation budget: 1 allocation per Produce request** -- the `[]byte` copy
of the RecordBatch into storage. Everything else reuses pooled/pre-allocated
buffers.

## Acks Handling

- `acks=0`: The broker **must not send a response**. The produce handler returns
  `nil, nil` (no response, no error). The dispatch system sends a skip marker
  to the write goroutine (`clientResp{skip: true}`), which advances the
  sequence counter without writing any bytes to the socket. See
  `03-tcp-and-wire.md` "Response Ordering" for details.

  **Why this matters:** With `acks=0` the client never reads responses from the
  socket. If the broker writes response bytes anyway, they accumulate in the
  kernel's TCP send buffer. Once the buffer fills, `conn.Write()` blocks, the
  write goroutine stalls, and all subsequent responses (including non-produce
  requests like Metadata or Heartbeat on the same connection) are stuck behind
  the blocked write. The connection deadlocks.

  The request is still processed normally (batches are appended, offsets
  assigned). Only the response is suppressed.

- `acks=1` and `acks=-1/all`: Identical for single broker. Both mean "leader
  acknowledged." Return immediately after appending (no replication to wait for).

## LogAppendTime

If the topic's `message.timestamp.type` is `LogAppendTime`:
1. Overwrite `BaseTimestamp` (bytes 27-34) with `time.Now().UnixMilli()`
2. Overwrite `MaxTimestamp` (bytes 35-42) with the same value
3. Set bit 3 of `Attributes` (byte 21-22) to 1

This requires mutating the raw batch bytes AND recalculating the CRC, since
timestamps are within the CRC-covered region (bytes 21+).

**CRC recalculation**: Use CRC-32C (Castagnoli). Go stdlib `hash/crc32` with
`crc32.MakeTable(crc32.Castagnoli)`. Compute over bytes 21 to end of batch,
write result to bytes 17-20.

## Error Responses

| Condition | Error Code |
|-----------|------------|
| Unknown topic (auto-create disabled) | `UNKNOWN_TOPIC_OR_PARTITION` |
| Unknown partition index | `UNKNOWN_TOPIC_OR_PARTITION` |
| Batch too large | `MESSAGE_TOO_LARGE` |
| Invalid magic byte | `UNSUPPORTED_FOR_MESSAGE_FORMAT` |
| Corrupt batch (CRC mismatch) | `CORRUPT_MESSAGE` |

## Validation Decisions

Phase 1 validation (must implement):
- Batch size check against `max.message.bytes`
- Magic byte == 2 check
- Partition exists check

Deferred validation:
- CRC validation of incoming batches (Kafka does this; we can skip initially
  since clients send valid data, but should add it for robustness)
- Timestamp range validation (Phase 2+)
- Idempotent producer duplicate detection (Phase 4)
- Transaction state checks (Phase 4)

**Important: LogAppendTime always requires CRC recalculation.** Even if CRC
validation of incoming batches is deferred, the LogAppendTime path (step 3e
above) mutates timestamps which are in the CRC-covered region (bytes 21+).
After overwriting BaseTimestamp, MaxTimestamp, and Attributes, the CRC MUST
be recalculated and written to bytes 17-20. Without this, consumers will see
CRC mismatches on fetched batches and reject them. This is independent of
whether the broker validates the incoming CRC.

## Reference

- `.cache/repos/franz-go/pkg/kfake/00_produce.go` -- correct Produce behavior (validation, error codes, acks handling)
- `.cache/repos/kafka/core/src/test/scala/unit/kafka/server/ProduceRequestTest.scala` -- edge cases
- `.cache/repos/kafka/core/src/test/scala/integration/kafka/api/PlaintextProducerSendTest.scala` -- integration scenarios

---

## When Stuck

These references are for understanding correct behavior and gathering context,
not for copying patterns. Find the optimal solution for our architecture.

1. Read `.cache/repos/franz-go/pkg/kfake/00_produce.go` -- see how kfake
   validates batches, assigns offsets, handles acks=0, and returns error codes
   per partition.
2. Check `.cache/repos/kafka/core/src/test/scala/unit/kafka/server/ProduceRequestTest.scala`
   for edge cases (invalid timestamps, corrupt batches, oversized messages).
3. Check `.cache/repos/kafka/core/src/test/scala/integration/kafka/api/PlaintextProducerSendTest.scala`
   for auto-create and send-to-partition behavior.
4. For RecordBatch byte layout questions, see `research/kafka-internals.md`.
5. See `00b-testing.md` "When Stuck: Escalation Path" for additional tools.

---

## Acceptance Criteria

- Smoke tests pass:
  - `TestProduceSingleRecord` -- produce 1 record, verify ack with offset 0
  - `TestProduceBatch` -- produce 10 records in one call, verify offsets 0-9
  - `TestProduceMultiplePartitions` -- produce to partition 0 and 1, verify
  - `TestProduceUnknownTopic` -- auto-create disabled, expect error
  - `TestProduceAutoCreate` -- auto-create enabled, produce to new topic works
  - `TestProduceConsumeOrdering` -- produce [A,B,C], consume, verify order
  - `TestProduceConsumeCompression` -- produce with gzip, consume, verify values

### Verify

```bash
# This work unit:
go test ./test/integration/ -run 'TestProduce' -v -race -count=5

# Regression check (all prior work units):
go test ./... -race -count=5
```

### Commit

After tests pass, stage and commit all changes:

```bash
git add -A
git commit -m "08: Produce handler — record batches, offset assignment, acks, LogAppendTime"
```
