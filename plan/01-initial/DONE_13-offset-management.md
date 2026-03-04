# Offset Management

OffsetCommit (Key 8) and OffsetFetch (Key 9). Persists and retrieves consumed
offsets for consumer groups.

This is Phase 2. Prerequisite: `12-group-coordinator.md` complete.

Read before starting: `12-group-coordinator.md` (Group data structures —
CommittedOffset, generation validation, group state machine).

---

## OffsetCommit (Key 8)

### Behavior

Persists consumed offsets for a group. In Phase 2, stored in memory only.
Phase 3 will persist to `metadata.log` (see `16-metadata-log.md`).

### Implementation

1. Validate group ID, member ID, generation
2. For each topic-partition: store `(group, topic, partition) -> (offset, metadata)`
3. Generation check: member's generation must match group's current generation
   (prevents stale commits after rebalance)

### "Simple" (Admin) Commits

If `MemberID` is empty and `GenerationID` is -1, this is an admin/simple commit
(not associated with a group member). Allow it without group membership check.
This is used by `kadm.Client.CommitOffsets()`.

### Error Responses

| Condition | Error Code |
|-----------|------------|
| Unknown group | `COORDINATOR_NOT_AVAILABLE` |
| Unknown member | `UNKNOWN_MEMBER_ID` |
| Wrong generation | `ILLEGAL_GENERATION` |
| Group rebalancing | `REBALANCE_IN_PROGRESS` |

### Kafka Test Reference

- `.cache/repos/kafka/core/src/test/scala/unit/kafka/server/OffsetCommitRequestTest.scala`
  Tests: basic commit, unknown group, empty group ID, unknown member, stale epoch,
  admin commit

---

## OffsetFetch (Key 9)

### Behavior

Returns previously committed offsets for a group.

### Implementation

1. Look up committed offsets for the requested group
2. For each requested topic-partition: return committed offset or -1 if none
3. If topics list is null/empty (v2+): return ALL committed offsets for the group
4. Unknown group: return offset -1 for all partitions (not an error)

### Version Notes

- v0-v7: single group
- v8+: batch (multiple groups in one request)

### Kafka Test Reference

- `.cache/repos/kafka/core/src/test/scala/unit/kafka/server/OffsetFetchRequestTest.scala`
  Tests: fetch by partition, unknown group returns -1, fetch all offsets,
  multi-group batch fetch

---

## OffsetDelete (Key 47)

### Behavior

Removes committed offsets for specific topic-partitions from a consumer group.
Used when partitions are removed from a group's subscription — without this,
stale offsets for removed partitions accumulate in the group's offset storage.

### Version Notes

- v0: Supported (only version).

### Implementation

1. Look up the group in the group coordinator
2. If group doesn't exist -> `GROUP_ID_NOT_FOUND` (top-level error)
3. For each topic-partition in the request:
   - Delete the committed offset entry from the group's offset map
   - If the partition had no committed offset, silently succeed
4. Return per-topic-partition results

### Interaction with Group Coordinator

Like other group operations, OffsetDelete sends a command to the group's
goroutine. The group goroutine removes the offset entries from its internal
map. If the group is in an active state (not Empty/Dead), this is still
allowed — Kafka permits deleting offsets for partitions that are not currently
assigned.

### Error Responses

| Condition | Error Code |
|-----------|------------|
| Group not found | `GROUP_ID_NOT_FOUND` (top-level) |

### kfake Reference

- `.cache/repos/franz-go/pkg/kfake/47_offset_delete.go` (delegates to
  `groups.handleOffsetDelete`)

---

## When Stuck

These references are for understanding correct behavior and gathering context,
not for copying patterns. Find the optimal solution for our architecture.

1. Read `.cache/repos/franz-go/pkg/kfake/groups.go` -- offset commit/fetch
   handling is part of the group coordinator. Search for `OffsetCommit` and
   `OffsetFetch` in this file.
2. Check `.cache/repos/kafka/core/src/test/scala/unit/kafka/server/OffsetCommitRequestTest.scala`
   for edge cases (admin commits, unknown member, stale generation).
3. Check `.cache/repos/kafka/core/src/test/scala/unit/kafka/server/OffsetFetchRequestTest.scala`
   for edge cases (fetch all offsets, multi-group batch, unknown group).
4. For OffsetDelete, read `.cache/repos/franz-go/pkg/kfake/47_offset_delete.go`
   and the `handleOffsetDelete` method in `.cache/repos/franz-go/pkg/kfake/groups.go`.
5. See `00b-testing.md` "When Stuck: Escalation Path" for additional tools.

---

## kfake Test Reference

Reference `consumer_commit_test.go` in
`.cache/repos/franz-go/pkg/kfake/kafka_tests/` for offset commit/fetch
scenarios. Write equivalent tests in `test/integration/offset_management_test.go`.

---

## Acceptance Criteria

- Integration tests pass:
  - `TestOffsetCommitFetch` -- commit offset, fetch it back
  - `TestOffsetCommitWrongGeneration` -- stale generation rejected
  - `TestOffsetFetchUnknownGroup` -- returns -1, not error
  - `TestOffsetFetchAll` -- null topics returns all committed offsets
  - `TestConsumeCommitResume` -- produce, consume, commit, restart consumer, resume
  - `TestAutoCommitOnClose` -- auto-commit fires on consumer close
  - `TestCommitMetadata` -- commit with metadata, verify via fetch
  - `TestAsyncCommit` -- async commit succeeds
  - `TestStaticMemberClassicRejoinNoRebalance` -- static member rejoin
  - `TestGroupMaxSizeClassic` -- group max size enforcement
  - `TestOffsetDelete` -- commit offsets, delete some, verify they're gone
  - `TestOffsetDeleteUnknownGroup` -- delete from non-existent group, expect error

### Verify

```bash
# This work unit:
go test ./test/integration/ -run 'TestOffsetCommit|TestOffsetFetch|TestConsumeCommitResume|TestAutoCommit|TestCommitMetadata|TestAsyncCommit|TestStaticMember|TestGroupMax|TestOffsetDelete' -v -race -count=5

# Regression check (all prior work units — full Phase 2):
go test ./... -race -count=5
```

### Commit

After tests pass, stage and commit all changes:

```bash
git add -A
git commit -m "13: offset management — OffsetCommit, OffsetFetch, OffsetDelete"
```
