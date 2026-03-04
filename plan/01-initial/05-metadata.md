# Metadata Handler (Key 3)

After this work unit, clients can discover broker info and topic metadata.
Auto-creation of topics on Metadata request is supported.

Prerequisite: `04-api-versions.md` complete (ApiVersions handshake works).

Read before starting: `01-foundations.md` (config — advertised address,
cluster ID, auto-create, default partitions), `03-tcp-and-wire.md` (dispatch
table — register key 3).

---

## Behavior

Returns broker list, topic list with partition info (leader, replicas, ISR).
For single-broker, every partition's leader/replicas/ISR is just `[nodeID]`.

## Key Decisions

1. **Auto-create topics**: When a client requests metadata for a topic that
   doesn't exist, create it if `auto-create-topics` is enabled. Use
   `default-partitions` for partition count. This is critical -- franz-go
   sends Metadata requests on startup for configured topics.

   **Concurrency: double-checked locking.** Two concurrent Metadata requests
   for the same non-existent topic can both pass the "doesn't exist" check
   under `c.mu.RLock()`. To prevent duplicate creation, the auto-create path
   must: (1) release the read lock, (2) acquire `c.mu.Lock()`, (3) re-check
   whether the topic now exists (another goroutine may have created it),
   (4) only create if still missing. This is the standard double-checked
   locking pattern for Go maps protected by `sync.RWMutex`.

2. **All-topics request**: If the request has no topics listed (or null topics
   array), return metadata for ALL existing topics.

3. **Topic IDs**: Starting from Metadata v10+, responses include topic UUIDs.
   Generate a UUID per topic at creation time.

4. **Controller ID**: Always our own node ID (single broker is always the
   controller).

5. **Cluster ID**: Return the cluster ID from config/meta.properties.

## Response Structure

```go
resp.Brokers = []kmsg.MetadataResponseBroker{{
    NodeID: cfg.NodeID,
    Host:   advertisedHost,
    Port:   advertisedPort,
}}
resp.ClusterID = &clusterID
resp.ControllerID = cfg.NodeID

for _, topic := range requestedTopics {
    topicResp := kmsg.MetadataResponseTopic{
        Topic:      kmsg.StringPtr(topic.Name),
        TopicID:    topic.ID, // [16]byte UUID, v10+
        Partitions: make([]kmsg.MetadataResponseTopicPartition, numPartitions),
    }
    for i := 0; i < numPartitions; i++ {
        topicResp.Partitions[i] = kmsg.MetadataResponseTopicPartition{
            Partition:   int32(i),
            Leader:      cfg.NodeID,
            Replicas:    []int32{cfg.NodeID},
            ISR:         []int32{cfg.NodeID},
            LeaderEpoch: 0,
        }
    }
    resp.Topics = append(resp.Topics, topicResp)
}
```

## Error Responses

| Condition | Error Code |
|-----------|------------|
| Topic doesn't exist, auto-create disabled | `UNKNOWN_TOPIC_OR_PARTITION` |
| Topic doesn't exist, auto-create enabled | Create topic, return success |
| Invalid topic name | `INVALID_TOPIC_EXCEPTION` |

## Reference

- `.cache/repos/franz-go/pkg/kfake/03_metadata.go` -- correct Metadata behavior (auto-create, topic IDs, error codes)

### Kafka Test Reference

For additional Metadata edge cases, see:
- `.cache/repos/kafka/core/src/test/scala/unit/kafka/server/MetadataRequestTest.scala`

---

## When Stuck

These references are for understanding correct behavior and gathering context,
not for copying patterns. Find the optimal solution for our architecture.

1. Read `.cache/repos/franz-go/pkg/kfake/03_metadata.go` -- see how kfake
   handles auto-create, topic IDs, all-topics requests, and error codes.
2. Check `.cache/repos/kafka/core/src/test/scala/unit/kafka/server/MetadataRequestTest.scala`
   for edge cases (null topics, invalid topic names, version-specific fields).
3. For topic ID generation (v10+), check how `kmsg.MetadataResponseTopic` is
   structured in `.cache/repos/franz-go/pkg/kmsg/`.
4. See `00b-testing.md` "When Stuck: Escalation Path" for additional tools.

---

## Acceptance Criteria

- `kcat -b localhost:9092 -L` succeeds (lists broker, no topics)
- franz-go Metadata request returns broker info
- Auto-created topic appears in subsequent Metadata requests
- Smoke tests pass:
  - `TestMetadataEmptyCluster` -- no topics, broker info correct
  - `TestMetadataAutoCreate` -- request unknown topic, topic created

### Verify

```bash
# This work unit:
go test ./test/integration/ -run 'TestMetadataEmptyCluster|TestMetadataAutoCreate' -v -race -count=5

# Regression check (all prior work units):
go test ./... -race -count=5
```

### Commit

After tests pass, stage and commit all changes:

```bash
git add -A
git commit -m "05: Metadata handler — broker discovery, topic metadata, auto-create"
```
