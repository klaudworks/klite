# Backlog: Kafka Features Not Yet Planned

Features that real Kafka supports (and clients may expect) but are not covered
in any plan phase. Ordered roughly by likelihood of a user hitting them.

---

## Likely Needed Soon

### DeleteRecords (Key 21)
Kafka clients use this to advance LogStartOffset (effectively deleting old
data). kfake implements it (`21_delete_records.go`). Without it, there's no way
to reclaim space short of deleting the topic. Needed for any retention workflow
that isn't purely time/size-based.

### OffsetForLeaderEpoch (Key 23)
franz-go sends this during consumer startup to detect log truncation. kfake
implements it (`23_offset_for_leader_epoch.go`, v3-4). Without it, franz-go
falls back but may log warnings. Single-broker can return a trivial response
(epoch=0, end offset=HW).

### DeleteGroups (Key 42)
Admin operation to remove consumer groups. kfake implements it. Without it,
stale groups accumulate forever. `kadm.DeleteGroups()` calls this.

### OffsetDelete (Key 47)
Deletes committed offsets for specific partitions from a group. Used by admin
tooling. kfake implements it.

### DescribeProducers (Key 61)
Returns active producer state for partitions. Used by admin tooling and tested
in kfake's `kafka_tests/describe_producers_test.go`. The Phase 4 kfake adapted
test list includes `TestDescribeProducersDefaultRoutesToLeader` and
`TestDescribeProducersAfterCommit` but the plan doesn't describe the handler.

### DescribeTransactions (Key 65) / ListTransactions (Key 66)
Admin tooling to inspect transaction state. kfake implements both. If
transactions (Phase 4) are implemented, these are expected to exist.

---

## Eventually Needed for Broader Client Compatibility

### SASL Authentication (Keys 17, 36)
SASLHandshake + SASLAuthenticate. Required for any deployment with auth.
kfake implements PLAIN and SCRAM-SHA-256/512. The plan mentions Phase 5/7
but provides no design.

### ACLs (Keys 29, 30, 31)
CreateAcls, DeleteAcls, DescribeAcls. Required alongside SASL for access
control. kfake has a full implementation.

### AlterConfigs (Key 33)
The legacy (non-incremental) alter configs API. Some older clients use this
instead of IncrementalAlterConfigs (Key 44). kfake implements it. The plan
only covers Key 44.

### DescribeCluster (Key 60)
Returns cluster metadata. Some admin tools use this instead of Metadata.
kfake implements it.

### ElectLeaders (Key 43)
Trivial for single-broker (no-op), but some admin tools call it. kfake
implements it.

---

## Retention and Compaction

### Time-Based and Size-Based Retention Enforcement
The plan stores `retention.ms` and `retention.bytes` configs but never
describes the background process that enforces them (scanning partitions,
advancing LogStartOffset, trimming data). Without enforcement, these configs
are decorative. This needs a background goroutine with periodic scans.

### Log Compaction (cleanup.policy=compact)
The plan accepts `cleanup.policy=compact` as a config but never implements
compaction. Compaction requires decompressing batches, building a key map,
and rewriting data — significant work. The plan's "Future" note in
`07-persistence-s3.md` acknowledges this but doesn't plan it.

---

## Wire Protocol Features

### Fetch Sessions (KIP-227)
The plan returns `FETCH_SESSION_ID_NOT_FOUND` to force full fetches. This
works but wastes bandwidth for clients that use sessions by default (franz-go
does). kfake has a full fetch session implementation. Not urgent but a
meaningful optimization for steady-state consumers.

### KIP-848 Consumer Groups (Key 68)
The new server-side assignment protocol. franz-go supports it behind an opt-in
flag. Kafka 4.0 makes it the default. Not needed now but will become the
standard.

### Produce v12+ (KIP-890) Implicit Transaction Partition Addition
Produces with v12+ implicitly add partitions to transactions without a prior
AddPartitionsToTxn call. The plan's ceiling is v11 but notes v12-13 can be
added later.

### Produce v13 / Fetch v17+ TopicID-Based Request/Response
Using topic UUIDs instead of names in produce/fetch. Reduces string allocation
on the hot path.

---

## Operational Features

### Quotas (Describe/Alter Client Quotas, Keys 48-49)
kfake stores quotas but doesn't enforce throttling. kafka-light could do the
same for tooling compatibility without actual enforcement.

### SCRAM Credentials (Keys 50-51)
DescribeUserScramCredentials / AlterUserScramCredentials. Needed if SASL
SCRAM auth is implemented.

### AlterReplicaLogDirs (Key 34) / DescribeLogDirs (Key 35)
Disk management APIs. kfake implements both. Some admin tools (AKHQ, Redpanda
Console) call DescribeLogDirs. Could return a single synthetic log dir for
compatibility.

### AlterPartitionAssignments (Key 45) / ListPartitionReassignments (Key 46)
Partition reassignment APIs. Meaningless for single-broker but admin tools may
call them. Could return trivial responses.

---

## Metrics and Observability

### Prometheus Metrics Endpoint
The plan explicitly defers this ("No Metrics in Phase 1"). Any production
deployment will need request latency histograms, produce/fetch throughput
counters, partition lag gauges, and connection counts.

### GetTelemetrySubscriptions (Key 71) / PushTelemetry (Key 72)
KIP-714 client telemetry. kfake implements both. Modern Kafka clients may
send these.

---

## Edge Cases and Hardening

### Connection Idle Timeout
The plan mentions a 5-minute read timeout but doesn't specify how it interacts
with Fetch long-poll (which holds a connection open for 30s+). Need to ensure
the idle timer resets on each request, not on each byte.

### Graceful Request Draining on Topic Deletion
If a topic is deleted while Fetch long-polls are registered on its partitions,
those waiters must be woken and given an error response. The plan doesn't
cover this interaction.

### Producer ID Expiration
Kafka expires producer ID state after `transactional.id.expiration.ms` (7 days
default). Without this, producer state grows unbounded. The plan mentions
persisting the counter but not expiring entries.
