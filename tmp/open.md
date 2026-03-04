5. Topic Creation Without Records
   06-persistence-wal.md:375-376 says "Topic configs and explicit topic creation (without any records) need a separate persistence mechanism since they don't produce WAL data entries." This is handled by meta.json.
   Problem: But meta.json is also the mechanism for cluster ID, topic configs, consumer offsets, and the next S3 object ID counter. That's a lot of unrelated concerns in one file. More importantly, what happens during crash recovery if meta.json says a topic has 4 partitions but the WAL only has entries for partitions 0-2? Which wins?
   Needs answering: Explicit priority ordering: WAL replay overrides meta.json for HW/offsets, but meta.json is authoritative for partition count and configs. This should be stated explicitly.
6. Fetch Long Polling Interacts Badly with Cluster Goroutine
   03-core-data-path.md:306-331 describes fetch waiters registered per-partition. The comment says "The waiter registration and notification must happen within the goroutine to avoid races." But if the fetch handler blocks on the cluster goroutine to register a waiter, and then blocks on the waiter channel for up to MaxWaitMs (which can be 30 seconds), the cluster goroutine is free during the wait — good. But:
   Problem: The flow isn't fully specified. Does the handler:
7. Send request to cluster goroutine
8. Cluster goroutine checks data, finds nothing, registers waiter, returns
9. Handler blocks on waiter channel
10. Later produce on cluster goroutine closes waiter channel
11. Handler wakes up, sends another request to cluster goroutine to fetch data
    That's 2 cluster goroutine round-trips per long-poll fetch. Is that acceptable? And what about partial wakeups (waiter is woken but still not enough data for MinBytes)?
12. Multiple RecordBatches per Partition in a Single Produce Request
    03-core-data-path.md:134-148 says "For each RecordBatch in the partition's records" but doesn't address how kmsg exposes this. The kmsg.ProduceRequest has Topics[].Partitions[].Records []byte — it's a single byte slice, not a list of batches.
    Problem: That Records field contains concatenated RecordBatches. The handler needs to iterate over the byte slice, parsing batch headers to find boundaries (BatchLength + 12 gives total batch size). This batch-boundary parsing logic isn't described anywhere. It's critical for correctness — get it wrong and you corrupt records.
    Needs answering: Document the batch iteration loop explicitly. Also: what if Records is empty (zero bytes)? What if it contains a partial batch at the end?
13. Advertised Address Resolution
    01-foundations.md:75 has --advertised-addr with default "(same as listen)". But if --listen is :9092 (no host), the advertised address can't be :9092 — clients need a hostname or IP to connect back.
    Problem: This is a classic ops footgun. Kafka has the same issue and solves it by requiring explicit advertised.listeners. We should either:

- Auto-resolve the hostname (unreliable)
- Require --advertised-addr when --listen has no host
- Default to localhost:9092 in the Metadata response (works for dev, breaks in containers)
  Needs answering: Pick one. The simplest for an autonomous agent is "default to localhost" for Phase 1, with a warning log if --advertised-addr isn't explicitly set.

9. Connection Lifecycle Missing: Read Timeout, Max Connections
   01-foundations.md:178-180 mentions "Read timeout (no data for 5+ min): Close connection" and the overview mentions per-connection goroutines, but there's no plan for:

- Max connection limit (what if 10K clients connect?)
- Connection draining on shutdown (how does the 10s timeout interact with long-polling fetches that have a 30s MaxWaitMs?)
- What headers the write goroutine needs to see (does it need api key + version for flexible header encoding, or does the read goroutine encode everything?)
  Not blocking but worth deciding: does the write goroutine receive fully-encoded bytes, or does it receive (correlationID, apiKey, version, body) and handle encoding?

10. ListOffsetsTimestampLookup Without a Timestamp Index
    03-core-data-path.md:406-408 says for timestamp lookups (>= 0), use batch-level MaxTimestamp and return the first batch whose MaxTimestamp >= requested. But the plan says nothing about how to find that batch efficiently.
    Problem: For in-memory Phase 1, a linear scan over all batches is fine. But for WAL (Phase 3) and S3 (Phase 4), this becomes a full WAL scan or multiple S3 range reads. Kafka maintains a per-segment time index for this. We have no time index in the plan.
    Not blocking for Phase 1 but the plan should acknowledge this gap and specify when a time index gets built (Phase 3 WAL index? Phase 4 S3 index entries already have offset ranges but not timestamps).
