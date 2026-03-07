# S3 Lease: Fencing

Split-brain prevention via a lease object in the S3 bucket. The primary holds
the lease by periodically renewing it with conditional writes. The standby
monitors the lease and claims it when the primary fails to renew.

Prerequisite: none (self-contained package).

Read before starting: `00-overview.md` (architecture context),
`research/warpstream.md` (similar lease-based coordination).

---

## Pluggable Interface

The lease subsystem uses a pluggable interface so that integration tests can
use a deterministic in-memory implementation while production uses S3.

```go
// internal/lease/lease.go

package lease

type Role int

const (
    RoleStandby Role = iota
    RolePrimary
)

// Elector manages leader election. Run blocks; Role() is safe to call
    // concurrently from any goroutine.
    type Elector interface {
        // Run blocks, calling callbacks on role transitions. Returns when
        // ctx is cancelled. The initial role is always RoleStandby; the
        // elector calls OnElected if this instance wins the lease.
        Run(ctx context.Context, cb Callbacks) error

        // Role returns the current role.
        Role() Role

        // Release performs a best-effort early lease release. Called by
        // the broker during graceful shutdown AFTER draining all writes.
        // Writes an expired renewedAt so the standby can claim the lease
        // on its next poll (~retryInterval) instead of waiting for
        // leaseDuration to elapse. Noop if not currently primary.
        // Errors are logged but not fatal — the lease expires naturally
        // if Release fails.
        Release() error
    }

type Callbacks struct {
    // OnElected is called when this instance becomes primary. The
    // provided ctx is cancelled if the lease is subsequently lost.
    OnElected func(ctx context.Context)

    // OnDemoted is called when this instance loses the primary role.
    // The broker must stop accepting writes before returning.
    OnDemoted func()
}
```

---

## S3 Lease Implementation

**Package: `internal/lease/s3lease/`**

### Lease Object

Key: `<s3-prefix>/lease`

```json
{
    "version": 1,
    "holder": "node-abc123",
    "epoch": 42,
    "renewedAt": "2026-03-07T10:30:00Z",
    "replAddr": "10.0.1.5:9093"
}
```

- `version`: schema version of the lease body. Always `1` for now. If a
  node reads a version it doesn't understand, it must refuse to claim the
  lease and log an error. This enables future schema evolution.
- `holder`: identity of the current primary (from `--replication-id`,
  defaulting to hostname)
- `epoch`: monotonically increasing counter, incremented on every new
  acquisition (not on renewal)
- `renewedAt`: RFC 3339 timestamp of last successful renewal. This is the
  primary's wall-clock time. Both nodes compare `renewedAt` against their
  **own** wall clock, so clock skew between nodes has no effect — only the
  standby's local clock matters when evaluating expiry. The primary tracks
  expiry by elapsed real time since its last successful renewal, not by
  reading `renewedAt` from S3.
- `replAddr`: the primary's replication listen address (host:port). The
  standby reads this to discover where to connect. See
  `04-broker-lifecycle.md` for details.

### Renewal (Primary)

Every `renewInterval` (default 5s):

1. `PutObject` with `If-Match: <cachedETag>` containing updated `renewedAt`
   and same `epoch`, same `holder`
2. On success (200): lease renewed, cache the returned ETag from
   `PutObjectOutput.ETag` for the next cycle
3. On 412 (Precondition Failed) or 409 (Conflict): lease lost →
   call `OnDemoted`, transition to standby

**No HeadObject on the renewal path.** The primary caches the ETag returned
from the previous successful `PutObject` (acquisition or prior renewal) and
uses it directly in `If-Match`. This halves the S3 API calls and latency for
the hot-path lease renewal. The ETag from `PutObjectOutput` is always
populated by S3 and is sufficient for CAS.

**Transient S3 error handling:** if `PutObject` fails due to transient S3
errors (timeout, 500, etc.), the primary retries on the next interval. It
tracks `lastSuccessfulRenewal time.Time`. It only gives up and calls
`OnDemoted` if `time.Since(lastSuccessfulRenewal) >= leaseDuration` (i.e.,
multiple consecutive failures spanning the full lease window). This prevents
a brief S3 blip from triggering unnecessary failover.

**Distinguishing transient vs. permanent errors:** 412/409 are permanent
(lease lost, call `OnDemoted` immediately). HTTP 5xx, timeouts, and network
errors are transient (retry on next interval, check cumulative failure
window). The implementation must classify errors at the HTTP status level
using the AWS SDK's error unwrapping (check for `smithy.APIError` and its
`HTTPStatusCode`).

**Critical safety property:** on demotion, the primary must stop accepting
produce requests **before** `OnDemoted` returns. The broker closes its TCP
listener in the callback.

### Acquisition (Standby)

Every `retryInterval` (default 2s):

1. `GetObject` on the lease key → get both the body and the ETag
   (from `GetObjectOutput.ETag`). A single `GetObject` is sufficient —
   no need for a separate `HeadObject`, since `GetObject` returns the ETag
   in the response. The lease body is small (< 200 bytes), so there is no
   bandwidth concern.
2. Parse `renewedAt`, compute `time.Since(renewedAt)`
3. If lease is not expired (`< leaseDuration`): do nothing, keep watching.
   Extract the primary's `replAddr` from the body for connection use.
4. If expired or lease object doesn't exist (`NoSuchKey` error):
   - **No object:** `PutObject` with `If-None-Match: *` (create-if-not-exists),
     epoch=1
   - **Expired object:** `PutObject` with `If-Match: <ETag>`, epoch
     incremented from the expired lease
5. On successful `PutObject`: lease claimed, cache the returned ETag →
   call `OnElected`
6. On 412/409: another node claimed first → back to step 1

### Timing

| Parameter | Default | Description |
|-----------|---------|-------------|
| `leaseDuration` | 15s | How long lease is valid without renewal |
| `renewInterval` | 5s | How often primary renews |
| `retryInterval` | 2s | How often standby checks |

**Rationale for 15s lease duration:** with 5s renewal intervals, a 15s lease
tolerates 2 missed renewals (one complete S3 blip cycle). S3 conditional
writes are strongly consistent with typical latency in the tens of
milliseconds from EC2 in the same region. A 5s interval provides >100x
headroom over typical S3 latency, and tolerating 2 misses means a transient
S3 outage lasting up to 10s does not trigger failover. 15s is aggressive
enough that crash failover (~17s) is practical, while conservative enough
to avoid false positives from S3 hiccups.

**Failover times:**
- **Graceful shutdown (SIGTERM):** ~2s worst case. The primary explicitly
  releases the lease after draining (see "Graceful Lease Release" below).
  The standby detects the released lease on its next poll.
- **Crash (SIGKILL, OOM, hardware):** ~17s worst case
  (`leaseDuration + retryInterval`). The primary stops renewing, the lease
  expires after 15s, the standby detects on next poll within 2s.

### Graceful Lease Release

On graceful shutdown (SIGTERM), the primary explicitly releases the lease to
reduce failover from ~17s to ~2s. The broker calls `Elector.Release()` after
all writes have been drained.

**Shutdown sequence (caller is the broker, not the elector):**
1. Close Kafka listener (no new connections)
2. Drain in-flight requests (`server.Wait()`)
3. Clear WAL Replicator + metadata.log replicateHook
4. Final S3 flush
5. Call `elector.Release()`
6. Stop WAL writer, close metadata.log

**No race condition:** the race described as "primary releases lease but
late-arriving produce writes to WAL after standby promoted" cannot occur
because `Release()` is called AFTER step 2 (drain). After `server.Wait()`
returns, there are zero active handler goroutines, so no more WAL writes
can happen.

**Release implementation (S3 elector):**
1. If not currently primary: return nil (noop)
2. `PutObject` with `If-Match: <cachedETag>`, body containing
   `renewedAt: "0001-01-01T00:00:00Z"` (Go zero time), same epoch, holder
3. On success: lease is effectively expired. The standby sees it on next
   poll (~2s) and claims it immediately.
4. On 412/409: another node already took the lease. Fine — the primary is
   shutting down anyway.
5. On transient error: log warning, return error. The lease expires
   naturally after `leaseDuration` (15s). This is a best-effort
   optimization.

**MemElector implementation:** `Release()` calls `Demote()`.

---

### S3 Compatibility

The lease relies on:
- `PutObject` with `If-Match` (ETag conditional write) — AWS S3 since
  August 2024, MinIO
- `PutObject` with `If-None-Match: *` (create-if-not-exists) — AWS S3,
  MinIO
- `HeadObject` returning `ETag` — universal

Other S3-compatible stores (SeaweedFS, Garage, R2) may or may not support
conditional writes. The S3 lease should test conditional write support on
startup (attempt a conditional PUT and verify 412 behavior) and fail with
a clear error message if the backend doesn't support it.

---

## In-Memory Implementation (Tests)

**Package: `internal/lease/memlease/`**

Deterministic lease control for integration tests. No S3, no network.

```go
// MemCluster coordinates multiple MemElector instances. Only one can
// be primary at a time.
type MemCluster struct { ... }

func NewCluster() *MemCluster

// NewElector creates an elector that participates in this cluster's
// leader election.
func (c *MemCluster) NewElector(id string) *MemElector

type MemElector struct { ... }

// Elect forces this elector to become primary. If another elector
    // in the same cluster is primary, it is demoted first.
    func (m *MemElector) Elect()

    // Demote forces this elector to become standby.
    func (m *MemElector) Demote()

    // Release triggers demotion (same as Demote for in-memory impl).
    func (m *MemElector) Release() error

    func (m *MemElector) Run(ctx context.Context, cb Callbacks) error
    func (m *MemElector) Role() Role
```

Two `MemElector` instances from the same `MemCluster` coordinate correctly:
electing one demotes the other, matching real S3 lease semantics.

---

## InMemoryS3 Extension

The existing `InMemoryS3` test double (`internal/s3/client.go`) needs changes
to support conditional writes:

1. **Store ETags:** every `PutObject` generates an ETag (incrementing counter
   is simplest, e.g. `"etag-1"`, `"etag-2"`). The ETag is stored alongside
   the object data and returned in `PutObjectOutput.ETag`.

2. **`If-Match` on PutObject:** if `input.IfMatch` is set, compare it with
   the stored ETag. If they don't match (or object doesn't exist), return an
   error with HTTP 412 status code. Use `&smithy.GenericAPIError{Code:
   "PreconditionFailed"}` to match the AWS SDK's error type.

3. **`If-None-Match` on PutObject:** if `input.IfNoneMatch` is `"*"` and
   the object already exists, return 412.

4. **`HeadObject` returns ETag:** already partially implemented. Ensure the
   `ETag` field is populated in the response.

5. **`GetObject` returns ETag:** the `GetObjectOutput.ETag` field must be
   populated with the stored ETag. This is needed because the acquisition
   path uses a single `GetObject` (not `HeadObject` + `GetObject`).

These changes are backward-compatible — existing tests that don't set
conditional headers are unaffected.

---

## When Stuck

Use the repo-explorer subagent to check reference implementations:

- **Litestream** (`https://github.com/benbjohnson/litestream`): examine
  `generation.go` for how Litestream uses generation IDs (conceptually
  similar to our lease epochs) to detect split-brain and ensure only one
  writer owns the replication stream.

- **PostgreSQL** (`https://github.com/postgres/postgres`): examine
  `src/backend/replication/syncrep.c` for how Postgres handles the
  synchronous commit wait and what happens when the standby disconnects.

---

## Acceptance Criteria

Lease interface:
- `Elector` interface compiles and is implementable
- `Role()` returns correct role at all times
- `Release()` is safe to call when not primary (noop)

S3 lease:
- Acquisition works (create-if-not-exists via `If-None-Match: *`)
- Renewal works (CAS via `If-Match`, using cached ETag from prior PutObject)
- Renewal path makes exactly one S3 API call per interval (no HeadObject)
- Lease body includes `version`, `replAddr`, `holder`, `epoch`, `renewedAt`
- Expiry detected by standby after `leaseDuration`
- Concurrent acquisition race: exactly one winner
- Primary calls `OnDemoted` on 412/409 renewal failure (immediate)
- Primary does NOT call `OnDemoted` on transient errors until cumulative
  failure window reaches `leaseDuration`
- Standby calls `OnElected` on successful claim
- Startup probe detects missing conditional write support
- Graceful release: `Release()` writes expired `renewedAt`, standby claims
  lease within `retryInterval`
- Graceful release: `Release()` is noop when called on standby

In-memory lease:
- `Elect()` / `Demote()` trigger correct callbacks
- `Release()` triggers demotion
- Two electors in same cluster: electing one demotes the other

InMemoryS3:
- `PutObject` with `If-Match` succeeds on ETag match, fails on mismatch
- `PutObject` with `If-None-Match: *` fails if object exists
- ETags are returned in `PutObject`, `HeadObject`, and `GetObject` responses

---

## Unit Tests

### Lease: In-Memory (`internal/lease/memlease/`)

- `TestMemElectorElect` — `Elect()` triggers `OnElected` callback
- `TestMemElectorDemote` — `Demote()` triggers `OnDemoted` callback
- `TestMemClusterSinglePrimary` — electing one elector demotes the other
- `TestMemElectorContextCancel` — `Run()` returns when ctx cancelled
- `TestMemElectorRelease` — primary calls `Release()`. Verify `OnDemoted`
  is called and `Role()` becomes `RoleStandby`.
- `TestMemElectorReleaseNotPrimary` — standby calls `Release()`. Verify
  it returns nil, no callbacks fired.

### Lease: S3 (`internal/lease/s3lease/`)

Uses `InMemoryS3` with ETag/If-Match support. No real S3.

- `TestS3LeaseAcquireEmpty` — claim lease when no object exists. Verify
  `If-None-Match: *` used. Verify `OnElected` called. Verify lease body
  contains version=1, epoch=1, holder, replAddr.
- `TestS3LeaseRenew` — primary renews with cached ETag from prior PutObject
  (not HeadObject). Verify `renewedAt` updated. Verify only one S3 API call
  per renewal (PutObject, no HeadObject).
- `TestS3LeaseExpiry` — seed lease with `renewedAt` far in the past. Start
  a standby elector. Verify it detects expiry and claims after <=
  `retryInterval`.
- `TestS3LeaseRace` — two electors compete for an expired lease. Exactly one
  wins. The loser gets 412 and retries. No `OnElected` called on the loser.
- `TestS3LeaseRenewPermanentFailure` — inject ETag mismatch (412) on renewal.
  Verify `OnDemoted` called immediately (not after leaseDuration).
- `TestS3LeaseRenewTransientError` — inject S3 500 errors. Verify no
  demotion until cumulative failure window >= leaseDuration. Then verify
  `OnDemoted` called.
- `TestS3LeaseEpochIncrement` — verify epoch increments on acquisition but
  stays the same on renewal.
- `TestS3LeaseConditionalWriteProbe` — on startup, the elector performs
  a probe write to verify the S3 backend supports conditional writes.
  Verify it fails with a clear error if the backend doesn't support 412.
- `TestS3LeaseReleasePrimary` — primary calls `Release()`. Verify lease
  body contains `renewedAt` at zero time. Start a standby elector. Verify
  it claims the lease within `retryInterval` (not `leaseDuration`).
- `TestS3LeaseReleaseNotPrimary` — standby calls `Release()`. Verify it
  returns nil (noop). Verify no S3 writes occur.

### InMemoryS3 Conditional Writes (`internal/s3/`)

- `TestInMemoryS3ConditionalPutIfMatch` — PUT with matching ETag succeeds,
  returns new ETag
- `TestInMemoryS3ConditionalPutIfMatchFail` — PUT with wrong ETag returns
  412 PreconditionFailed error
- `TestInMemoryS3ConditionalPutIfMatchNoObject` — PUT with If-Match on
  nonexistent key returns 412
- `TestInMemoryS3ConditionalPutIfNoneMatch` — PUT with `If-None-Match: *`
  succeeds when object doesn't exist
- `TestInMemoryS3ConditionalPutIfNoneMatchFail` — PUT with `If-None-Match: *`
  returns 412 when object exists
- `TestInMemoryS3ETagReturned` — PutObject, HeadObject, and GetObject all
  return ETags. Successive PutObjects return different ETags.

### Integration Tests with LocalStack (`test/integration/s3lease_test.go`)

These tests verify the S3 lease against a real S3-compatible API. They use
testcontainers to start a LocalStack container. They specifically test
behavior that InMemoryS3 cannot faithfully simulate (ETag format
differences, HTTP error codes, concurrent conditional writes over real HTTP).

- `TestS3LeaseLocalStackAcquireAndRenew` — start LocalStack, create elector,
  verify acquisition + renewal + ETag changes
- `TestS3LeaseLocalStackTwoNodes` — two electors, verify exactly one primary,
  stop primary, verify standby promotes
- `TestS3LeaseLocalStackConditionalWriteSemantics` — PutObject with
  If-None-Match / If-Match, verify success and 412 on correct conditions

### Verify

```bash
go test ./internal/lease/... -v -race
go test ./internal/s3/ -run TestInMemoryS3Conditional -v -race
go test ./test/integration/ -run TestS3Lease -v -race
```
