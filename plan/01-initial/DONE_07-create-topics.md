# CreateTopics Handler (Key 19)

After this work unit, clients can explicitly create topics with specified
partition counts and configs.

Prerequisite: `06-partition-data-model.md` complete (partData structures exist).

Read before starting: `00-overview.md` (Supported Topic Configs table),
`01-foundations.md` (concurrency model — cluster-level write lock for topic
creation), `03-tcp-and-wire.md` (dispatch table — register key 19).

---

## Versions: 2-7

Key version milestones:
- **v2**: `ValidateOnly` field added
- **v5**: Flexible versions (tagged fields)
- **v7**: Current ceiling

## Behavior

Creates one or more topics with specified partition count and configs.

## Implementation

For each topic in the request:
1. Validate topic name (not empty, not too long, no invalid chars)
2. Check if topic already exists -> `TOPIC_ALREADY_EXISTS`
3. Check topic name collision after dot/underscore normalization: replace all
   `.` with `_` in the candidate name and reject if the normalized form matches
   any existing topic (e.g. `foo.bar` collides with `foo_bar`).
   Return `INVALID_TOPIC_EXCEPTION` on collision.
4. Determine partition count: use `NumPartitions` if > 0, else `default-partitions`
5. Ignore `ReplicationFactor` (always 1, single broker)
6. Parse and store topic configs from `Configs` array
7. Generate topic UUID
8. Create partition state objects (empty, HW=0)
9. If `ValidateOnly` is true, don't actually create -- just validate

## Supported Configs on Create

See `00-overview.md` "Supported Topic Configs" for the full list. Reject
unknown config keys with `INVALID_CONFIG`.

## Error Responses

| Condition | Error Code |
|-----------|------------|
| Topic already exists | `TOPIC_ALREADY_EXISTS` |
| Empty topic name | `INVALID_TOPIC_EXCEPTION` |
| Topic name too long (>249) | `INVALID_TOPIC_EXCEPTION` |
| Topic name is `.` or `..` | `INVALID_TOPIC_EXCEPTION` |
| Invalid chars in name | `INVALID_TOPIC_EXCEPTION` |
| Name collides after `.`/`_` normalization | `INVALID_TOPIC_EXCEPTION` |
| NumPartitions < 1 and != -1 | `INVALID_PARTITIONS` |
| Unknown config key | `INVALID_CONFIG` |

## Reference

- `.cache/repos/franz-go/pkg/kfake/19_create_topics.go` -- correct CreateTopics behavior
- `.cache/repos/kafka/core/src/test/scala/unit/kafka/server/CreateTopicsRequestTest.scala` -- edge cases

For additional CreateTopics edge cases:
Tests: `testValidCreateTopicsRequests`, `testErrorCreateTopicsRequests`,
  `testCreateTopicsRequestVersions`

---

## When Stuck

These references are for understanding correct behavior and gathering context,
not for copying patterns. Find the optimal solution for our architecture.

1. Read `.cache/repos/franz-go/pkg/kfake/19_create_topics.go` -- see how kfake
   validates topic names, handles collisions, and returns error codes.
2. Check `.cache/repos/kafka/core/src/test/scala/unit/kafka/server/CreateTopicsRequestTest.scala`
   for edge cases (invalid names, partition validation, validate-only mode).
3. See `00b-testing.md` "When Stuck: Escalation Path" for additional tools.

---

## kfake Test Reference

Reference `static_membership_test.go` in
`.cache/repos/franz-go/pkg/kfake/kafka_tests/` for CreateTopics test scenarios.
Write equivalent tests in `test/integration/produce_test.go` or
`test/integration/admin_test.go`.

---

## Acceptance Criteria

- Integration tests pass:
  - `TestCreateTopicsBasic` -- create topic, verify via Metadata
  - `TestCreateTopicAlreadyExists` -- expect TOPIC_ALREADY_EXISTS
  - `TestCreateTopicInvalidName` -- expect INVALID_TOPIC_EXCEPTION
  - `TestCreateTopicsReplicaAssignment` -- replica assignment scenarios
  - `TestCreateTopicsReplicaAssignmentWithNumPartitions`

### Verify

```bash
# This work unit:
go test ./test/integration/ -run 'TestCreateTopic' -v -race -count=5

# Regression check (all prior work units):
go test ./... -race -count=5
```

### Commit

After tests pass, stage and commit all changes:

```bash
git add -A
git commit -m "07: CreateTopics handler — topic creation, validation, configs"
```
