# Group Coordinator

Classic consumer group protocol: JoinGroup, SyncGroup, Heartbeat, LeaveGroup.
The group state machine and per-group goroutine pattern.

This is Phase 2. Prerequisite: `11-find-coordinator.md` complete.

Read before starting: `01-foundations.md` (concurrency model — per-group
goroutine is the exception to the RWMutex pattern),
`03-tcp-and-wire.md` (dispatch table — register keys 11, 12, 13, 14).

---

## Overview

Consumer groups allow multiple consumers to coordinate partition assignment for
a topic. The protocol has 4 phases: find coordinator, join group, sync
assignments, then heartbeat to stay alive.

For single-broker, we are always the coordinator for every group.

### Classic vs KIP-848

Kafka has two consumer group protocols:
1. **Classic** (JoinGroup/SyncGroup): Client-side assignment. Leader consumer
   runs the assignment algorithm, distributes via SyncGroup.
2. **KIP-848** (ConsumerGroupHeartbeat): Server-side assignment. Broker runs
   the assignment algorithm. Newer, simpler.

**Decision: Implement classic first.** Reasons:
- All existing Kafka clients use classic by default
- KIP-848 is opt-in and requires client support (franz-go supports it, but
  Java client defaults to classic)
- kfake's tests use KIP-848 by default, but also test classic
- Classic is what we need for drop-in compatibility

KIP-848 can be added later. kfake's `groups.go` implements both in ~3200 lines.

---

## Group State Machine

### States

```
EMPTY -> PREPARING_REBALANCE -> COMPLETING_REBALANCE -> STABLE -> DEAD
                    ^                                      |
                    |                                      |
                    +---------- (member change) -----------+
```

| State | Description |
|-------|-------------|
| `EMPTY` | No members. Group exists but is idle. |
| `PREPARING_REBALANCE` | Waiting for all members to join. Triggered by member join/leave. |
| `COMPLETING_REBALANCE` | All members joined, waiting for leader's SyncGroup with assignments. |
| `STABLE` | Assignments distributed. Normal operation. |
| `DEAD` | Group is being removed (all members left, offsets expired). |

### Per-Group Goroutine

Following kfake's pattern, each active group gets its own goroutine. This is
needed because JoinGroup blocks until all members have joined (or timeout).
This blocking behavior can't be handled with a simple mutex -- the handler must
wait for an external event (other members joining) before responding.

```
Group map (sync.RWMutex)
    |
    +-- Group "my-group" goroutine (blocks on JoinGroup barrier)
    +-- Group "other-group" goroutine
```

The group map is protected by a `sync.RWMutex` on the `Cluster` struct (same
lock that protects the topic map). Looking up a group takes a read lock.
Creating a new group takes a write lock. Once a `*Group` is obtained, all
operations go through the group's own goroutine via channel dispatch.

This is the one place where we use the goroutine-with-channel pattern (not
RWMutex). Group operations (JoinGroup, SyncGroup, Heartbeat, LeaveGroup) have
blocking semantics that don't fit the lock-release pattern. The per-partition
RWMutex model (see `06-partition-data-model.md`) applies only to the data path
(Produce, Fetch, ListOffsets).

Group goroutines access partition state (e.g., for OffsetFetch to check HW)
by taking `pd.mu.RLock()` on the relevant partition -- no special coordination
needed.

### Group Data Structures

```go
type Group struct {
    ID              string
    State           GroupState
    ProtocolType    string          // e.g., "consumer"
    Protocol        string          // e.g., "range" or "roundrobin"
    GenerationID    int32
    LeaderID        string          // Member ID of the leader
    Members         map[string]*Member
    PendingMembers  map[string]*Member  // Members waiting in JoinGroup
    // Timers
    RebalanceTimer  *time.Timer     // Rebalance timeout
    // Committed offsets
    Offsets         map[TopicPartition]CommittedOffset
}

type Member struct {
    ID           string
    ClientID     string
    InstanceID   *string          // Static membership (KIP-345)
    Protocols    []Protocol       // Supported assignment protocols
    Assignment   []byte           // From SyncGroup leader
    SessionTimer *time.Timer      // Session timeout
    RebalanceTimer *time.Timer    // Rebalance timeout
    JoinResponseCh chan JoinResp  // Blocked JoinGroup response
    SyncResponseCh chan SyncResp  // Blocked SyncGroup response
}

type CommittedOffset struct {
    Offset   int64
    Metadata string
    // No expiration in Phase 2. Add offsets.retention.minutes later.
}
```

---

## JoinGroup (Key 11)

### Protocol Flow

1. Member sends JoinGroup with its supported protocols and session/rebalance timeouts
2. If member is unknown (no member ID), and version >= 4:
   - Return `MEMBER_ID_REQUIRED` with a generated member ID
   - Client retries with the assigned member ID
3. Add member to group, transition to `PREPARING_REBALANCE`
4. Wait for all existing members to re-join (up to rebalance timeout)
5. Once all members joined (or timeout expired for missing ones):
   - Choose group protocol (intersection of all members' supported protocols)
   - Elect leader (first member, or existing leader if still present)
   - Increment `GenerationID`
   - Transition to `COMPLETING_REBALANCE`
   - Return responses to all blocked JoinGroup calls
6. Leader's response includes all member subscriptions. Non-leaders get empty list.

### Member ID Generation

Format: `{clientID}-{UUID}` (matches Kafka).

### Static Membership (KIP-345)

If `GroupInstanceID` is set:
- Member identity is tied to instance ID, not member ID
- Rejoin with same instance ID reuses the existing member (no rebalance)
- Session timeout still applies, but rejoin within timeout is seamless
- New member with same instance ID fences the old one

### Error Responses

| Condition | Error Code |
|-----------|------------|
| Empty group ID | `INVALID_GROUP_ID` |
| No member ID and version >= 4 | `MEMBER_ID_REQUIRED` (with assigned ID) |
| Invalid member ID | `UNKNOWN_MEMBER_ID` |
| Wrong protocol type | `INCONSISTENT_GROUP_PROTOCOL` |
| No common protocol | `INCONSISTENT_GROUP_PROTOCOL` |
| Group is DEAD | `COORDINATOR_NOT_AVAILABLE` |
| Rebalance timeout exceeded | Remove absent members, proceed |

### kfake Reference

- `.cache/repos/franz-go/pkg/kfake/groups.go` (JoinGroup handling in the group goroutine)

### Kafka Test Reference

- `.cache/repos/kafka/core/src/test/scala/unit/kafka/server/JoinGroupRequestTest.scala`
  Tests: member ID required flow, unknown member, invalid group, inconsistent protocol,
  two-member join + rebalance, static member fencing

---

## SyncGroup (Key 14)

### Protocol Flow

1. After JoinGroup completes, all members send SyncGroup
2. Leader includes assignments for all members in its SyncGroup request
3. Non-leaders send empty assignments
4. Broker stores assignments, transitions to `STABLE`
5. All members receive their assignment in the SyncGroup response

### Implementation

```
If group state is COMPLETING_REBALANCE:
    If sender is leader:
        Store all member assignments
        Transition to STABLE
        Respond to all waiting SyncGroup requests with their assignments
    Else (follower):
        Block until leader syncs (or timeout)
Else if group state is STABLE:
    Return the member's current assignment (for late joiners)
Else:
    Return REBALANCE_IN_PROGRESS
```

### Error Responses

| Condition | Error Code |
|-----------|------------|
| Unknown group | `UNKNOWN_MEMBER_ID` |
| Unknown member | `UNKNOWN_MEMBER_ID` |
| Wrong generation | `ILLEGAL_GENERATION` |
| Wrong protocol type | `INCONSISTENT_GROUP_PROTOCOL` |
| Group is rebalancing | `REBALANCE_IN_PROGRESS` |

### Kafka Test Reference

- `.cache/repos/kafka/core/src/test/scala/unit/kafka/server/SyncGroupRequestTest.scala`
  Tests: unknown member, wrong generation, wrong protocol, leader before follower

---

## Heartbeat (Key 12)

### Behavior

Keeps a member's session alive. Also signals rebalance to the member.

### Implementation

1. Validate member ID and generation
2. Reset the member's session timer
3. If group is `PREPARING_REBALANCE`, return `REBALANCE_IN_PROGRESS`
4. Otherwise return success

### Session Timeout

If a member doesn't heartbeat within its `SessionTimeoutMs`:
- Remove member from group
- If group was `STABLE`, trigger rebalance (-> `PREPARING_REBALANCE`)
- If member was the last one, transition to `EMPTY`

Default session timeout: 45s (but clients typically set 10-30s).

### Error Responses

| Condition | Error Code |
|-----------|------------|
| Unknown member/group | `UNKNOWN_MEMBER_ID` |
| Wrong generation | `ILLEGAL_GENERATION` |
| Group rebalancing | `REBALANCE_IN_PROGRESS` |

### Kafka Test Reference

- `.cache/repos/kafka/core/src/test/scala/unit/kafka/server/HeartbeatRequestTest.scala`
  Tests: unknown member, wrong generation, heartbeat in each group state

---

## LeaveGroup (Key 13)

### Behavior

Member voluntarily leaves the group. Triggers rebalance for remaining members.

### Implementation

1. Remove member(s) from group
2. If group was `STABLE` or `COMPLETING_REBALANCE`, trigger rebalance
3. If no members left, transition to `EMPTY`

### Version Notes

- v0-v2: single member leave
- v3+: batch leave (multiple members in one request, with per-member error codes)

### Kafka Test Reference

- `.cache/repos/kafka/core/src/test/scala/unit/kafka/server/LeaveGroupRequestTest.scala`
  Tests: invalid group, unknown member, batch leave, static member fencing

---

## When Stuck

These references are for understanding correct behavior and gathering context,
not for copying patterns. Find the optimal solution for our architecture.

1. Read `.cache/repos/franz-go/pkg/kfake/groups.go` -- this is the primary
   reference for the group state machine. It covers JoinGroup, SyncGroup,
   Heartbeat, LeaveGroup, and the per-group goroutine pattern in ~3200 lines.
   Focus on the classic protocol sections (search for "classic" or skip KIP-848
   blocks).
2. For JoinGroup edge cases, check
   `.cache/repos/kafka/core/src/test/scala/unit/kafka/server/JoinGroupRequestTest.scala`.
3. For SyncGroup edge cases, check
   `.cache/repos/kafka/core/src/test/scala/unit/kafka/server/SyncGroupRequestTest.scala`.
4. For Heartbeat edge cases, check
   `.cache/repos/kafka/core/src/test/scala/unit/kafka/server/HeartbeatRequestTest.scala`.
5. For LeaveGroup edge cases (especially batch leave v3+), check
   `.cache/repos/kafka/core/src/test/scala/unit/kafka/server/LeaveGroupRequestTest.scala`.
6. See `00b-testing.md` "When Stuck: Escalation Path" for additional tools.

---

## Acceptance Criteria

- Smoke tests pass:
  - `TestJoinGroupSingle` -- one consumer joins, becomes leader
  - `TestJoinGroupTwo` -- two consumers join, one leader, one follower
  - `TestJoinGroupMemberIdRequired` -- v4+, first join returns MEMBER_ID_REQUIRED
  - `TestSyncGroupLeaderFollower` -- leader assigns, all get assignments
  - `TestHeartbeatKeepsAlive` -- heartbeat resets session
  - `TestSessionTimeout` -- no heartbeat, member removed, rebalance
  - `TestLeaveGroupRebalance` -- leave triggers rebalance
  - `TestStaticMemberRejoin` -- rejoin without rebalance

### Verify

```bash
# This work unit:
go test ./test/integration/ -run 'TestJoinGroup|TestSyncGroup|TestHeartbeat|TestSessionTimeout|TestLeaveGroup|TestStaticMember' -v -race -count=5

# Regression check (all prior work units):
go test ./... -race -count=5
```

### Commit

After tests pass, stage and commit all changes:

```bash
git add -A
git commit -m "12: group coordinator — JoinGroup, SyncGroup, Heartbeat, LeaveGroup, static membership"
```
