# FindCoordinator Handler (Key 10)

Trivial handler that returns ourselves as the coordinator for any group or
transaction. Entry point for consumer group and transaction workflows.

This is Phase 2. Prerequisite: all Phase 1 tests pass.

Read before starting: `01-foundations.md` (config — advertised address, node ID),
`03-tcp-and-wire.md` (dispatch table — register key 10).

---

## Behavior

Returns the broker that coordinates a given group (or transaction). For
single-broker, always returns ourselves.

```go
resp.NodeID = cfg.NodeID
resp.Host = advertisedHost
resp.Port = advertisedPort
resp.ErrorCode = 0 // NO_ERROR
```

## Version Notes

- v0-v3: single key lookup
- v4+: batch lookup (multiple keys in one request)
- `KeyType`: 0 = group, 1 = transaction

## kfake Reference

- `.cache/repos/franz-go/pkg/kfake/10_find_coordinator.go`

---

## When Stuck

These references are for understanding correct behavior and gathering context,
not for copying patterns. Find the optimal solution for our architecture.

1. Read `.cache/repos/franz-go/pkg/kfake/10_find_coordinator.go` -- this is a
   trivial handler, the kfake implementation is <30 lines.
2. For batch lookup (v4+), check how `kmsg.FindCoordinatorRequest` structures
   the `CoordinatorKeys` field in `.cache/repos/franz-go/pkg/kmsg/`.
3. See `00b-testing.md` "When Stuck: Escalation Path" for additional tools.

---

## Acceptance Criteria

- `TestFindCoordinator` -- returns self for any group
- Returns correct host/port from advertised address
- Batch lookup (v4+) returns self for every key

### Verify

```bash
# This work unit:
go test ./test/integration/ -run 'TestFindCoordinator' -v -race -count=5

# Regression check (all prior work units):
go test ./... -race -count=5
```

### Commit

After tests pass, stage and commit all changes:

```bash
git add -A
git commit -m "11: FindCoordinator handler — returns self for all groups and transactions"
```
