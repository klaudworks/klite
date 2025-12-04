# Admin APIs

DeleteTopics, CreatePartitions, DescribeConfigs, IncrementalAlterConfigs,
AlterConfigs, DescribeGroups, ListGroups, DeleteGroups, DeleteRecords,
DescribeCluster, DescribeLogDirs. These are Phase 3 APIs that support
operational management of the broker.

Prerequisite: Phase 2 (consumer groups) complete.

Read before starting: `00-overview.md` (Supported Topic Configs table),
`01-foundations.md` (concurrency model — cluster write lock for topic
deletion/partition creation), `12-group-coordinator.md` (Group data structures
— for DescribeGroups/ListGroups/DeleteGroups).

---

## DeleteTopics (Key 20)

### Behavior

Deletes one or more topics by name or topic ID.

### Implementation

1. For each topic: look up by name or topic ID
2. Remove topic from cluster state under `c.mu.Lock()` (all partitions, all
   stored batches)
3. Remove committed offsets for the deleted topic from all groups. Since each
   group has its own goroutine (see `12-group-coordinator.md`), send a
   `deleteTopicOffsets(topicName)` message to each group goroutine via its
   command channel. This is fire-and-forget: the DeleteTopics handler does
   not wait for groups to acknowledge. Offset cleanup is eventually consistent
   — a stale offset for a deleted topic is harmless (OffsetFetch returns -1
   for unknown topics).
4. Return per-topic error or success

### Version Notes

- v0-v5: delete by name only
- v6+: delete by name or topic ID

### Error Responses

| Condition | Error Code |
|-----------|------------|
| Topic not found | `UNKNOWN_TOPIC_OR_PARTITION` |
| Invalid topic ID | `UNKNOWN_TOPIC_ID` |

### Kafka Test Reference

- `.cache/repos/kafka/core/src/test/scala/unit/kafka/server/DeleteTopicsRequestTest.scala`
  Tests: single/multi delete, by name and topic ID, unknown topic errors

---

## CreatePartitions (Key 37)

### Behavior

Increases the partition count for existing topics. Cannot decrease.

### Implementation

1. Validate: new count > current count (else `INVALID_PARTITIONS`)
2. Create new partition state objects (empty, HW=0)
3. Existing partitions unchanged

### Error Responses

| Condition | Error Code |
|-----------|------------|
| Topic not found | `UNKNOWN_TOPIC_OR_PARTITION` |
| New count <= current | `INVALID_PARTITIONS` |
| New count > some limit | `INVALID_PARTITIONS` |

### kfake Reference

- `.cache/repos/franz-go/pkg/kfake/37_create_partitions.go`

---

## DescribeConfigs (Key 32)

### Behavior

Returns configuration for topics or the broker itself.

### Implementation

For topic resources:
1. Return the topic's config values (set at creation or altered)
2. Include defaults for configs not explicitly set
3. Mark each config as DEFAULT, STATIC_BROKER_CONFIG, or TOPIC_CONFIG

For broker resources:
1. Return broker-level configs
2. We have a small fixed set (see `01-foundations.md`)

### Config Source Types

| Source | Description |
|--------|-------------|
| `DEFAULT` (5) | Built-in default value |
| `STATIC_BROKER_CONFIG` (4) | Set in broker config file |
| `TOPIC_CONFIG` (1) | Set on the topic explicitly |

### Version Notes

- v0-v3: basic config describe
- v4+: includes documentation strings (we skip per kfake's `skipped_features`)

### kfake Reference

- `.cache/repos/franz-go/pkg/kfake/32_describe_configs.go`

---

## IncrementalAlterConfigs (Key 44)

### Behavior

Modifies topic or broker configs incrementally (set, delete, append, subtract).

### Implementation

For each resource and config entry:
1. `SET` (0): Set config to new value
2. `DELETE` (1): Remove override, revert to default
3. `APPEND` (2): Append to list-valued config (not common for our configs)
4. `SUBTRACT` (3): Remove from list-valued config

Validate config names and values. Reject unknown configs.

### kfake Reference

- `.cache/repos/franz-go/pkg/kfake/44_incremental_alter_configs.go`

---

## DescribeGroups (Key 15)

### Behavior

Returns detailed info about consumer groups: state, protocol, members,
assignments.

### Implementation

For each requested group:
1. Look up group state
2. Return: state name, protocol type, protocol name, member list with
   assignments, authorized operations
3. Unknown group: return state `Dead` with empty member list

### kfake Reference

- `.cache/repos/franz-go/pkg/kfake/15_describe_groups.go`

---

## ListGroups (Key 16)

### Behavior

Returns a list of all consumer groups on the broker.

### Implementation

Return all groups with their protocol type and state.

### Version Notes

- v4+: supports filtering by state (e.g., only `Stable` groups)
- v5+: supports filtering by group type

### kfake Reference

- `.cache/repos/franz-go/pkg/kfake/16_list_groups.go`

---

## AlterConfigs (Key 33)

### Behavior

Legacy (non-incremental) config API. Replaces all configs for a resource with
the provided set. Unlike IncrementalAlterConfigs (key 44), this is a full
replacement — any config not included in the request reverts to its default.

This API is deprecated in favor of IncrementalAlterConfigs but is still used by
older clients (kafkajs, older Confluent tools). kfake implements both.

### Version Notes

- v0-v2: Supported.
  - v1: ThrottleMillis
  - v2: Flexible versions

### Implementation

For each resource (BROKER or TOPIC):

**BROKER resource:**
1. If resource name is non-empty, validate it matches our node ID
2. Validate all config names and values
3. Unless `ValidateOnly`, replace all dynamic broker configs with the provided
   set (configs not included revert to default)

**TOPIC resource:**
1. Validate topic exists
2. Validate all config names and values
3. Unless `ValidateOnly`, clear the topic's existing config overrides and apply
   the provided set (configs not included revert to default)

### Error Responses

| Condition | Error Code |
|-----------|------------|
| Unknown topic | `UNKNOWN_TOPIC_OR_PARTITION` |
| Invalid broker ID | `INVALID_REQUEST` |
| Invalid config name/value | `INVALID_REQUEST` |
| Unsupported resource type | `INVALID_REQUEST` |

### kfake Reference

- `.cache/repos/franz-go/pkg/kfake/33_alter_configs.go`

---

## DeleteGroups (Key 42)

### Behavior

Deletes one or more consumer groups. A group can only be deleted if it is in
the `Dead` or `Empty` state (no active members). Abandoned consumer groups
accumulate forever without this API.

### Version Notes

- v0-v2: Supported.
  - v1: ThrottleMillis
  - v2: Flexible versions

### Implementation

For each requested group:
1. Look up the group in the group coordinator
2. If group doesn't exist -> `GROUP_ID_NOT_FOUND`
3. If group has active members (state is not `Dead` or `Empty`) ->
   `NON_EMPTY_GROUP`
4. Remove the group and all its committed offsets from the group coordinator
5. Return success

### Interaction with Group Coordinator

DeleteGroups sends a delete command to the group's goroutine (see
`12-group-coordinator.md`). The group goroutine validates its own state (must
be `Empty` or `Dead`) and either performs the deletion or returns an error.
This keeps all group state mutations within the group goroutine.

### Error Responses

| Condition | Error Code |
|-----------|------------|
| Group not found | `GROUP_ID_NOT_FOUND` |
| Group has active members | `NON_EMPTY_GROUP` |

### kfake Reference

- `.cache/repos/franz-go/pkg/kfake/42_delete_groups.go` (delegates to
  `groups.handleDelete`)

---

## DeleteRecords (Key 21)

### Behavior

Advances the log start offset for partitions, effectively deleting records
before that offset. Used for GDPR compliance (log truncation) and operational
cleanup. Both Bufstream and Redpanda support this.

The compatibility survey's own recommendation (section 4.2) includes
DeleteRecords in Phase 3, though it was omitted from the original plan.

### Version Notes

- v0-v2: Supported.
  - v1: ThrottleMillis
  - v2: Flexible versions

### Implementation

For each topic-partition:
1. Look up the partition
2. If the requested offset is -1, use the high watermark (delete everything)
3. Validate: `logStartOffset <= requestedOffset <= highWatermark`
   (else `OFFSET_OUT_OF_RANGE`)
4. Acquire `pd.compactionMu.Lock()` (unconditionally, for all topics —
   see `21-retention.md` "Lock Ordering")
5. Call `pd.advanceLogStartOffset(requestedOffset, metaLog)` — the shared
   method that persists to metadata.log, trims batches, and prunes the WAL
   index (see `21-retention.md` for full specification)
6. Release `pd.compactionMu.Unlock()`
7. Return the new low watermark in the response

### Shared Method: `advanceLogStartOffset`

DeleteRecords and retention enforcement both use the same
`advanceLogStartOffset` method on `partData`. This method handles:
- Persisting the new logStartOffset to `metadata.log` (LOG_START_OFFSET
  entry, with synchronous fsync) for crash safety
- Trimming all eligible batches in a single pass (sets `pd.logStart`
  to exactly `newOffset`, not to the oldest surviving batch's base offset)
- Pruning stale WAL index entries pointing to deleted segments
- Updating the `totalBytes` counter on the partition
- Triggering WAL segment cleanup via `TryCleanupSegments()`

The caller must hold `pd.compactionMu` (see `21-retention.md` "Lock
Ordering"). The method is safe for concurrent calls — the early-return
check and metadata.log append both happen under `pd.mu.Lock()`, so only
one caller advances the offset and the other is a no-op. See
`21-retention.md` "advanceLogStartOffset — Shared Method" for the full
specification.

### Interaction with WAL (Phase 3+)

After `advanceLogStartOffset` prunes the WAL index, it calls
`walWriter.TryCleanupSegments()` to delete segments whose data is fully
below `logStartOffset` for all partitions. This also handles idle systems
where segment rotation wouldn't otherwise trigger cleanup.

### Interaction with S3 (Phase 4+)

S3 objects whose offset ranges are entirely below the log start offset are
deleted during the next S3 flush cycle. The S3 flusher checks log start
offsets before retaining objects. S3 cleanup is not immediate — objects
persist until the next flush cycle (default: 10 minutes). The Fetch handler
checks `logStartOffset` and returns `OFFSET_OUT_OF_RANGE` for offsets below
it, so stale S3 data is never served to clients.

### Error Responses

| Condition | Error Code |
|-----------|------------|
| Unknown topic/partition | `UNKNOWN_TOPIC_OR_PARTITION` |
| Offset out of range | `OFFSET_OUT_OF_RANGE` |

### kfake Reference

- `.cache/repos/franz-go/pkg/kfake/21_delete_records.go`

---

## DescribeCluster (Key 60)

### Behavior

Modern cluster discovery API (KIP-700). Newer clients use this instead of or
alongside Metadata for cluster-level information. Returns broker list, cluster
ID, and controller ID.

Trivial for single-broker: return a single broker entry with our listen address.

### Version Notes

- v0-v2: Supported.
  - v1: EndpointType field
  - v2: IncludeFencedBrokers / IsFenced fields (irrelevant for single-broker)

### Implementation

```
1. Build broker list (single entry: our node ID, host, port)
2. Set ClusterID = our cluster ID
3. Set ControllerID = our node ID (we are the controller)
4. If IncludeClusterAuthorizedOperations: return authorized operations
   (for single-broker without ACLs: return all operations)
5. Return response
```

### kfake Reference

- `.cache/repos/franz-go/pkg/kfake/60_describe_cluster.go`

---

## DescribeLogDirs (Key 35)

### Behavior

Returns log directory information including per-partition sizes. Admin UIs
(AKHQ, Conduktor, Redpanda Console) call this to show disk usage. Without it,
those UIs show errors or missing data.

### Version Notes

- v0-v4: Supported.
  - v1: ThrottleMillis
  - v2: Flexible versions
  - v3: TotalBytes, UsableBytes fields
  - v4: No changes

### Implementation

For single-broker, we have one log directory (`--data-dir`).

```
1. If req.Topics is null: enumerate all partitions
   Else: enumerate only the requested topic-partitions
2. For each partition:
   - In-memory only (Phase 1-2): report batch byte count
   - With WAL (Phase 3+): report WAL segment sizes for this partition
   - With S3 (Phase 4+): include S3 object sizes if available
3. Build response with one Dir entry:
   - Dir = data directory path
   - TotalBytes = sum of all partition sizes (or disk total if available)
   - UsableBytes = disk free space (or a large constant as stub)
   - Per-topic, per-partition Size values
```

### Stub Strategy

In Phase 1-2, partition "size" is the sum of stored batch byte lengths. This
is approximate (doesn't include batch headers or index overhead) but gives UIs
a reasonable number to display. Exact sizes come in Phase 3+ when real WAL
segments exist on disk.

### Error Responses

No per-partition error codes — unknown partitions are simply omitted from
the response (matching Kafka behavior).

### kfake Reference

- `.cache/repos/franz-go/pkg/kfake/35_describe_log_dirs.go`

---

## OffsetForLeaderEpoch (Key 23)

### Behavior

Returns the end offset for a given leader epoch. Used by clients (including
franz-go) during metadata refresh to detect log truncation (KIP-320). For
single-broker, this is nearly trivial — there is only one leader and epochs
change rarely (primarily on restart).

### Version Notes

- v3-v4: Supported (v0-v2 are inter-broker only).
  - v3: CurrentLeaderEpoch for fencing
  - v4: Flexible versions

### Implementation

For single-broker, the leader epoch starts at 0 and increments on restart.
Each `partData` tracks its current epoch.

For each topic-partition:
1. Validate topic/partition exists
2. Validate `ReplicaID == -1` (only consumer requests; inter-broker not
   supported)
3. Validate `CurrentLeaderEpoch` matches our epoch (fencing check)
4. Resolve the requested `LeaderEpoch`:
   - If `LeaderEpoch == currentEpoch`: return `EndOffset = HW`
   - If `LeaderEpoch < currentEpoch`: find the first batch after that epoch
     and return its base offset as the `EndOffset`
   - If `LeaderEpoch > currentEpoch`: return `LeaderEpoch = -1, EndOffset = -1`
     (unknown epoch)

### Epoch Tracking

Each `storedBatch` records the epoch at which it was written. On restart (new
epoch), new batches get the new epoch value. Binary search over batches by
epoch yields the boundary offset efficiently.

For the initial in-memory phase (before WAL), epoch is always 0 since there
are no restarts without data loss. Epoch tracking becomes meaningful in Phase 3
when the WAL survives restarts.

### Error Responses

| Condition | Error Code |
|-----------|------------|
| Unknown topic/partition | `UNKNOWN_TOPIC_OR_PARTITION` |
| ReplicaID != -1 | `UNKNOWN_SERVER_ERROR` |
| Current epoch too old | `FENCED_LEADER_EPOCH` |
| Current epoch too new | `UNKNOWN_LEADER_EPOCH` |

### kfake Reference

- `.cache/repos/franz-go/pkg/kfake/23_offset_for_leader_epoch.go`

---

## When Stuck

These references are for understanding correct behavior and gathering context,
not for copying patterns. Find the optimal solution for our architecture.

1. For DeleteTopics, read `.cache/repos/franz-go/pkg/kfake/20_delete_topics.go`.
2. For CreatePartitions, read `.cache/repos/franz-go/pkg/kfake/37_create_partitions.go`.
3. For DescribeConfigs, read `.cache/repos/franz-go/pkg/kfake/32_describe_configs.go`.
4. For IncrementalAlterConfigs, read `.cache/repos/franz-go/pkg/kfake/44_incremental_alter_configs.go`.
5. For AlterConfigs, read `.cache/repos/franz-go/pkg/kfake/33_alter_configs.go`.
6. For DescribeGroups, read `.cache/repos/franz-go/pkg/kfake/15_describe_groups.go`.
7. For ListGroups, read `.cache/repos/franz-go/pkg/kfake/16_list_groups.go`.
8. For DeleteGroups, read `.cache/repos/franz-go/pkg/kfake/42_delete_groups.go`
   and the `handleDelete` method in `.cache/repos/franz-go/pkg/kfake/groups.go`.
9. For DeleteRecords, read `.cache/repos/franz-go/pkg/kfake/21_delete_records.go`.
10. For DescribeCluster, read `.cache/repos/franz-go/pkg/kfake/60_describe_cluster.go`.
11. For DescribeLogDirs, read `.cache/repos/franz-go/pkg/kfake/35_describe_log_dirs.go`.
12. For OffsetForLeaderEpoch, read `.cache/repos/franz-go/pkg/kfake/23_offset_for_leader_epoch.go`.
13. For Kafka test edge cases, check `.cache/repos/kafka/core/src/test/scala/unit/kafka/server/DeleteTopicsRequestTest.scala`.
14. See `00b-testing.md` "When Stuck: Escalation Path" for additional tools.

---

## Acceptance Criteria

- Smoke tests pass:
  - `TestDeleteTopicBasic` -- create topic, delete it, verify gone
  - `TestDeleteTopicUnknown` -- delete non-existent topic, expect error
  - `TestCreatePartitions` -- create topic with 1 partition, expand to 4
  - `TestCreatePartitionsDecrease` -- shrink partitions, expect error
  - `TestDescribeConfigsTopic` -- create topic with config, describe it
  - `TestDescribeConfigsBroker` -- describe broker configs
  - `TestAlterConfigsTopic` -- change retention.ms, verify via describe
  - `TestAlterConfigsLegacy` -- use AlterConfigs (key 33), verify full replacement
  - `TestDescribeGroupsStable` -- group in stable state, verify members
  - `TestDescribeGroupsUnknown` -- unknown group returns Dead
  - `TestListGroups` -- multiple groups, list all
  - `TestDeleteGroupEmpty` -- delete an empty group, succeeds
  - `TestDeleteGroupActive` -- delete group with members, expect NON_EMPTY_GROUP
  - `TestDeleteGroupNotFound` -- delete non-existent group, expect error
  - `TestDeleteRecordsBasic` -- produce, delete up to offset N, verify log start
  - `TestDeleteRecordsToHWM` -- delete with offset -1, all records gone
  - `TestDeleteRecordsOutOfRange` -- offset beyond HWM, expect error
  - `TestDeleteRecordsSurvivesRestart` -- delete records, restart broker,
    verify logStartOffset is restored and deleted data does not reappear
  - `TestDescribeCluster` -- returns broker info, cluster ID, controller ID
  - `TestDescribeLogDirs` -- produce to partition, verify non-zero size
  - `TestOffsetForLeaderEpoch` -- produce, request current epoch, verify HW
  - `TestOffsetForLeaderEpochUnknown` -- request future epoch, verify -1/-1

kfake's admin test coverage is minimal. The integration tests above are the
primary gate.

### Verify

```bash
# This work unit:
go test ./test/integration/ -run 'TestDeleteTopic|TestCreatePartitions|TestDescribeConfigs|TestAlterConfigs|TestDescribeGroups|TestListGroups|TestDeleteGroup|TestDeleteRecords|TestDescribeCluster|TestDescribeLogDirs|TestOffsetForLeaderEpoch' -v -race -count=5

# Regression check (all prior work units):
go test ./... -race -count=5
```

### Commit

After tests pass, stage and commit all changes:

```bash
git add -A
git commit -m "14: admin APIs — DeleteTopics, CreatePartitions, DescribeConfigs, AlterConfigs, groups, DeleteRecords, DescribeCluster, DescribeLogDirs, OffsetForLeaderEpoch"
```
