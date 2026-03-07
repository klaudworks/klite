# Broker Lifecycle: Roles, Promotion, Auto-Cert

How the broker starts up in standby-enabled mode, how roles are managed, how
promotion and demotion work, and how mTLS certificates are auto-generated and
distributed via S3.

Prerequisite: `01-lease.md` (Elector interface), `02-replication-protocol.md`
(Sender/Receiver), `03-wal-integration.md` (Replicator in WAL writer).

Read before starting: `internal/broker/broker.go` (`Run()` method, shutdown
sequence), `internal/broker/config.go` (Config struct, flag registration).

---

## Design Overview

When `--replication-addr` is set, the broker participates in leader election
via the S3 lease. The startup sequence changes:

1. Initialize data dir, metadata.log, WAL (same as today)
2. Ensure TLS certificates exist (generate or download from S3)
3. Start the lease elector
4. Wait for role assignment:
   - **Primary**: start Kafka TCP listener, start replication listener
   - **Standby**: connect to primary's replication addr, receive stream

The broker can transition between roles during its lifetime (primary →
standby on demotion, standby → primary on promotion).

When `--replication-addr` is NOT set, the broker runs in single-node mode
exactly as today. No lease, no replication, no TLS generation.

---

## Auto-Generated TLS Certificates

The replication channel uses mTLS. Certificates are auto-generated and stored
in S3 so both nodes use the same trust root without manual setup.

### S3 Key Layout

```
<s3-prefix>/repl/ca.crt      # CA certificate (PEM)
<s3-prefix>/repl/ca.key      # CA private key (PEM)
<s3-prefix>/repl/node.crt    # Node certificate signed by CA (PEM)
<s3-prefix>/repl/node.key    # Node private key (PEM)
```

**Single shared keypair, not separate per-node.** Both primary and standby
use the same `node.crt`/`node.key`. Separate per-node certs would not add
meaningful security — both nodes can read S3, so both can read the other's
private key anyway. The mTLS handshake verifies that both sides trust the
auto-generated CA. This is sufficient to prevent rogue clients from
connecting to the replication port. Separate certs only add value when
combined with a revocation mechanism, which is out of scope.

### Certificate Generation (First Primary)

On first start with `--replication-addr`, the primary checks S3 for existing
certs. If none exist:

1. Generate a self-signed CA (ECDSA P-256):
   - Subject: `CN=klite-repl-ca`
   - Validity: 10 years
   - Key usage: cert signing, CRL signing
2. Generate a node keypair:
   - Subject: `CN=klite-repl-node`
   - SANs: empty (both sides verify via the shared CA, not hostname)
   - Validity: 10 years
   - Signed by the CA
3. Upload all four files to S3
4. Cache locally in `<data-dir>/repl-tls/` for fast restart

### Certificate Loading (Subsequent Starts)

1. Check `<data-dir>/repl-tls/` for cached certs
2. If cached and valid: use them
3. If not cached: download from S3, cache locally
4. Build `tls.Config` with mutual TLS:
   - Both primary and standby use the same node cert+key
   - Both verify the peer against the auto-generated CA
   - `tls.RequireAndVerifyClientCert` on both sides

### Certificate Rotation

Not needed for the initial implementation. The 10-year validity covers any
realistic deployment. If rotation is needed later, a new primary can
regenerate and re-upload, then both nodes restart.

### Implementation

`internal/repl/tls.go` exports a single function `EnsureTLS` that takes a
config struct (S3API, bucket, prefix, data dir, logger) and returns a
`*tls.Config` ready for mTLS. It uses Go's `crypto/x509`, `crypto/ecdsa`,
and `encoding/pem` — no external dependencies.

---

## Configuration

New fields in `broker.Config`:

```go
// Standby replication configuration
ReplicationAddr           string        // Listen address for replication (e.g. ":9093")
ReplicationAdvertisedAddr string        // Routable address written to lease (default: derived)
ReplicationID             string        // This node's identity for lease (default: hostname)
ReplicationAckTimeout     time.Duration // How long to wait for standby ACK (default 5s)
LeaseDuration             time.Duration // How long lease is valid without renewal (default 15s)
LeaseRenewInterval        time.Duration // How often primary renews lease (default 5s)
LeaseRetryInterval        time.Duration // How often standby polls lease (default 2s)
```

New CLI flags:

```
--replication-addr            :9093    Replication listen address. Setting this enables standby mode.
--replication-advertised-addr ""       Routable replication address written to the lease. Defaults to
                                       hostname:<port> derived from --replication-addr.
--replication-id              ""       Node identity for lease. Defaults to hostname.
--replication-ack-timeout     5s       Timeout for standby ACK in sync mode.
--lease-duration              15s      How long the lease is valid without renewal.
--lease-renew-interval        5s       How often the primary renews the lease.
--lease-retry-interval        2s       How often the standby polls the lease.
```

Environment variables:

```
KLITE_REPLICATION_ADDR
KLITE_REPLICATION_ADVERTISED_ADDR
KLITE_REPLICATION_ID
KLITE_REPLICATION_ACK_TIMEOUT
KLITE_LEASE_DURATION
KLITE_LEASE_RENEW_INTERVAL
KLITE_LEASE_RETRY_INTERVAL
```

**Startup validation:**
- `--replication-addr` without `--s3-bucket`: fail fast
- `--lease-duration` < 2 * `--lease-renew-interval`: fail fast (not enough
  headroom for transient S3 errors)
- `--lease-retry-interval` > `--lease-duration`: warn (standby may miss
  the expiry window)

---

## Startup Sequence (Replication Enabled)

```
1. Parse config, validate (s3 required with replication)
2. Load or create cluster ID (meta.properties) — same as today
3. Initialize metadata.log — same as today
4. Replay metadata.log — same as today
5. Initialize WAL writer — same as today
6. Replay WAL — same as today
7. Ensure TLS certs (generate or download from S3)
8. Probe S3 conditional write support (attempt a test conditional PUT,
   verify 412 behavior). Fail fast with a clear error if the S3 backend
   doesn't support If-Match / If-None-Match.
9. Create S3 lease elector
10. Start elector.Run() in background goroutine
11. Wait for role callback:

    OnElected (this node is primary):
      a. Set WAL writer's Replicator (wire up Sender)
      b. Start replication listener on --replication-addr (mTLS)
      c. Start Kafka TCP listener on --listen
      d. Register handlers, begin serving clients
      e. Start S3 flusher, retention, compaction, S3 GC

    OnDemoted (lost primary role):
      a. Close Kafka TCP listener (stop accepting new connections)
      b. Drain in-flight requests (server.Wait())
      c. Close replication listener
      d. Clear WAL writer's Replicator
      e. Clear metadata.log replicateHook and compactHook
      f. Stop S3 flusher, retention, S3 compaction, S3 GC
      g. Connect to new primary as standby (start Receiver)
```

### Standby Initial State

When the elector does NOT call `OnElected` (this node starts as standby):

1. Metadata.log and WAL are already initialized from local disk
2. **No background tasks.** The standby runs NO timer-driven loops:
   no S3 flusher, no S3 compaction, no retention, no S3 GC, no Kafka
   listener. As a blanket rule: any background goroutine that is driven
   by a timer or ticker is primary-only. The standby has no incoming
   events that would trigger these — if one fires, it's a bug.
3. **Metadata.log compaction does not run on the standby.** The standby
   receives metadata entries via `ReplayEntry` (raw frame writes), which
   does not increment `appendCount`. The compaction threshold is never
   reached. Instead, the primary sends a SNAPSHOT after each compaction
   (see `03-wal-integration.md` "Compaction → SNAPSHOT").
4. Read the lease object to discover the primary's `replAddr`
5. Connect to primary's replication address
6. Send HELLO with last WAL sequence and epoch
7. Receive SNAPSHOT (if epoch mismatch) or begin live streaming
8. Apply entries to local WAL + metadata.log (building index + chunks)
9. Wait for promotion (OnElected callback)

### Health Endpoint on Standby

The HTTP health server (if `--health-addr` is set) runs on both primary
and standby:
- `/livez` returns 200 on both roles (for liveness probes — both nodes
  should stay alive)
- `/readyz` returns 200 on primary, 503 on standby (for readiness probes
  — load balancers should only route Kafka traffic to the primary)

---

## Promotion: Standby → Primary

When the S3 lease elector calls `OnElected`:

1. **Stop receiver**: disconnect from old primary's replication stream.
   Wait for the receiver goroutine to exit cleanly.
2. **Replay pending**: any WAL entries received but not yet committed to
   chunk pool — apply them now
3. **Update lease object**: write this node's `replAddr` into the lease
   body (via a conditional PutObject with the current ETag). This is how
   the demoted node discovers where to connect as standby.
4. **Start replication listener**: begin accepting standby connections
   on `--replication-addr` with the auto-generated mTLS config
5. **Wire up WAL writer**: set Replicator (the Sender), set metadata.log
   replicateHook and compactHook
6. **Start Kafka listener**: begin accepting client connections
7. **Start background tasks**: S3 flusher, retention, compaction. The
   chunk pool is initialized during startup (step 1 of the startup
   sequence), so it is available for the new primary.
8. **Log**: `INFO klite promoted to primary`

**Ordering matters:** the replication listener (step 4) must start before
the Kafka listener (step 6). This ensures the standby can connect before
any new writes arrive. If writes arrive before the standby connects, they
succeed with local fsync only — sync mode treats "no standby ever connected"
as async (see `Replicator.Connected()` in `03-wal-integration.md`). Once
the standby connects, sync enforcement activates.

The standby has been applying WAL entries to its chunk pool and WAL index
throughout. On promotion, it's immediately ready to serve Fetch requests
from memory. There is no replay delay.

---

## Demotion: Primary → Standby

When the S3 lease elector calls `OnDemoted`:

1. **Close Kafka listener**: stop accepting new client connections.
   This MUST happen first, before any other step, to prevent the old
   primary from accepting writes after losing the lease.
2. **Drain requests**: wait for in-flight produce/fetch to complete
   (with short timeout, e.g. 5s). After timeout, force-close remaining
   connections.
3. **Clear WAL Replicator**: WAL writer reverts to local-only mode.
   Any in-flight replication sends are abandoned.
4. **Clear metadata.log replicateHook**: set to nil so metadata writes
   don't attempt to use the defunct sender.
5. **Close replication listener**: stop accepting standby connections.
   Close the sender and its ACK reader goroutine.
6. **Stop all primary-only background tasks**: S3 flusher (final flush
   first), retention loop, S3 compaction loop, S3 GC loop. The standby
   must NOT run any of these — S3 writes from both nodes would conflict,
   and the standby has no incoming events to trigger them.
7. **Clear metadata.log compactHook**: set to nil so compaction doesn't
   attempt to use the defunct sender.
8. **Connect as standby**: start Receiver, connect to new primary
   (read primary address from the lease object's `replAddr` field).
9. **Log**: `WARN klite demoted to standby`

**Critical ordering**: step 1 (close Kafka listener) must complete before
`OnDemoted` returns. The elector does not proceed (i.e., the standby does
not learn it can promote) until after the S3 lease expires, which is
independent of `OnDemoted` returning — but closing the listener immediately
prevents split-brain writes during the transition window.

---

## Primary Address Discovery

The standby discovers the primary's replication address from the S3 lease
object. The lease body includes `replAddr` (see `01-lease.md`):

```json
{
    "version": 1,
    "holder": "node-abc123",
    "epoch": 42,
    "renewedAt": "2026-03-07T10:30:00Z",
    "replAddr": "10.0.1.5:9093"
}
```

The standby reads the lease on every poll (it already does this for expiry
detection), extracts `replAddr`, and connects. This eliminates the need for
any `--replication-peer` flag — only `--replication-addr` is needed on each
node.

**`replAddr` resolution logic** (in order of precedence):

1. If `--replication-advertised-addr` is set: use it verbatim. This is
   the explicit override for NAT, containers, or multi-homed hosts.
2. If `--replication-addr` binds a specific IP (not `0.0.0.0`, not `::`,
   not empty host): use that IP + port as `replAddr`.
3. If `--replication-addr` binds all interfaces (`:9093`, `0.0.0.0:9093`,
   `[::]:9093`): use `<hostname>:<port>` where hostname is `os.Hostname()`.
   Log a warning: "using hostname as replication address; set
   --replication-advertised-addr if hostname does not resolve to a routable
   IP from the standby."

**Implementation:** `resolveReplicationAddr()` method on `Broker`, following
the same pattern as the existing `resolveAdvertisedAddr()`. Called once when
the elector promotes this node, and the result is written to the lease body.

**Why hostname, not outbound-IP detection:** detecting the "right" IP by
opening a UDP socket to a well-known address (a common Go trick) is fragile
with multiple interfaces, VPNs, or unusual routing. `os.Hostname()` is
predictable and works on most networks where DNS or `/etc/hosts` is
configured. The user can always override with `--replication-advertised-addr`.

**Kubernetes:** `os.Hostname()` returns the pod name (e.g. `klite-0`), which
is not DNS-resolvable on its own. A StatefulSet with a headless Service gives
each pod a stable FQDN like `klite-0.klite-headless.ns.svc.cluster.local`,
but that requires the full name — the bare hostname won't resolve.
`--replication-advertised-addr` is required on Kubernetes. Example using the
downward API:

```yaml
env:
  - name: POD_NAME
    valueFrom:
      fieldRef:
        fieldPath: metadata.name
args:
  - "--replication-advertised-addr=$(POD_NAME).klite-headless.$(NAMESPACE).svc.cluster.local:9093"
```

---

## Failure Scenarios

### Primary crashes (SIGKILL, OOM, hardware)

1. Primary stops renewing lease
2. Lease expires after `leaseDuration` (15s)
3. Standby detects expiry on next poll (within `retryInterval`, 2s)
4. Standby claims lease, promotes
5. Standby starts serving clients
6. **Data loss**: zero. All ACK'd writes were confirmed by standby.
7. **Downtime**: ~17s worst case (lease expiry + poll interval)

### Primary graceful shutdown (SIGTERM)

1. SIGTERM → context cancelled
2. Close Kafka listener (no new connections)
3. Drain in-flight requests (`server.Wait()`) — at this point, zero active
   handler goroutines remain. No more WAL writes can occur.
4. Clear WAL Replicator + metadata.log replicateHook
5. Final S3 flush + metadata.log upload
6. Call `elector.Release()` — writes expired `renewedAt` to S3 lease
7. Stop WAL writer, close metadata.log, exit
8. **Data loss**: zero
9. **Downtime**: ~2s (one standby poll interval)

The race described in earlier drafts ("primary releases lease but
late-arriving produce writes to WAL after standby promoted") cannot occur:
`Release()` is called in step 6, after `server.Wait()` completes in step 3.
After Wait returns, there are zero active handler goroutines — no writes
can be in flight.

If `Release()` fails (S3 transient error), the lease expires naturally
after `leaseDuration` (15s). This is best-effort; the primary is exiting
regardless.

### Standby crashes

1. Primary's next `Replicate()` call fails (connection broken)
2. In sync mode: produce fails with `KafkaStorageError`
3. In async mode: produce continues with local fsync only
4. When standby restarts, it reconnects and catches up
5. **Data loss**: zero (primary still has all data)

### Network partition (both nodes alive, can't see each other)

1. Primary can't send to standby, standby can't see primary
2. Primary attempts lease renewal via S3:
   - If S3 reachable from primary: lease renewed, primary stays primary
     but produces fail in sync mode (standby unreachable)
   - If S3 unreachable from primary: lease eventually expires, primary
     demotes itself
3. Standby polls lease:
   - If lease still held by primary: standby waits
   - If lease expired: standby claims it
4. **Split-brain impossible**: the S3 lease is the single arbiter. Only the
   node that holds the lease accepts writes.

---

## When Stuck

Use the repo-explorer subagent to check reference implementations:

- **PostgreSQL WAL sender/receiver**: clone `https://github.com/postgres/postgres`
  and examine `src/backend/replication/walsender.c` and
  `src/backend/replication/walreceiver.c` for the streaming protocol, ACK
  handling, and standby catch-up logic.
- **Litestream S3 replication**: clone `https://github.com/benbjohnson/litestream`
  and examine the generation-based fencing, S3 object naming, and WAL
  streaming design.

---

## Acceptance Criteria

Lifecycle:
- Single-node mode (no `--replication-addr`): unchanged behavior, no lease,
  no replication, no TLS generation — verify no regressions
- Replication mode: broker starts, elects via S3 lease, serves as primary
  or standby based on role
- `--replication-addr` without `--s3-bucket` fails fast on startup
- S3 conditional write probe runs before elector creation; fails fast if
  backend doesn't support If-Match / If-None-Match
- Standby does NOT start S3 flusher, S3 compaction, retention, S3 GC, or
  Kafka listener (blanket rule: no timer-driven background tasks)
- Standby metadata.log compaction does not fire (appendCount never
  incremented by ReplayEntry)

Auto-cert:
- First primary generates CA + node cert, uploads to S3
- Second start loads certs from local cache
- Standby downloads certs from S3
- mTLS handshake succeeds between primary and standby
- mTLS handshake fails with a rogue client (no matching CA)

Promotion:
- Standby receives OnElected, starts replication listener before Kafka
  listener
- Lease object is updated with new primary's `replAddr`
- Data produced before promotion is readable after promotion
- S3 flusher, compaction, retention start on promotion

Demotion:
- Primary receives OnDemoted, closes Kafka listener FIRST
- No writes accepted after demotion
- S3 flusher does final flush, then stops. Retention, compaction, S3 GC
  also stop.
- replicateHook and compactHook are cleared before connecting as standby
- Demoted node reads new primary's `replAddr` from lease and connects

Health endpoint:
- `/livez` returns 200 on both primary and standby
- `/readyz` returns 200 on primary, 503 on standby

Graceful shutdown:
- SIGTERM triggers: close listener → drain → clear replicator → S3 flush
  → `Release()` → stop WAL → exit
- `Release()` writes expired `renewedAt`; standby claims within
  `retryInterval` (~2s failover)
- `Release()` failure does not block shutdown; lease expires naturally
- No writes occur between drain and release (verified by handler count)

Address discovery:
- Lease object contains `replAddr`
- Standby reads primary address from lease, connects
- `--replication-advertised-addr` overrides the address written to the lease
- With `--replication-addr :9093` and no advertised addr: `replAddr` is
  `<hostname>:9093`, with a log warning
- With `--replication-addr 10.0.1.5:9093`: `replAddr` is `10.0.1.5:9093`

Lease timing validation:
- `--lease-duration` < 2 * `--lease-renew-interval`: startup fails
- `--lease-retry-interval` > `--lease-duration`: startup warns

---

## Unit Tests

### Auto-Cert TLS (`internal/repl/`)

- `TestEnsureTLSGenerates` — call EnsureTLS with empty S3 and empty cache
  dir. Verify CA + node certs are generated and uploaded to S3 (4 objects).
  Verify certs are cached locally.
- `TestEnsureTLSLoadsFromS3` — pre-seed S3 with certs. Call EnsureTLS with
  empty cache dir. Verify certs are downloaded (no generation). Verify
  local cache is populated.
- `TestEnsureTLSHandshake` — generate certs, build two tls.Configs (one
  server, one client). Do a mTLS handshake over `net.Pipe()`. Verify
  success.
- `TestEnsureTLSRejectsRogue` — generate certs. Create a rogue cert signed
  by a different CA. Attempt handshake. Verify it fails.

### Lease Timing Validation (`internal/broker/`)

- `TestLeaseTimingValidation` — set `--lease-duration 5s` and
  `--lease-renew-interval 5s`. Verify startup fails (duration must be
  >= 2x renew interval).
- `TestLeaseTimingWarning` — set `--lease-retry-interval 20s` and
  `--lease-duration 15s`. Verify startup logs a warning.

---

## Integration Tests

All integration tests use `memlease` for deterministic role control. Two
in-process brokers, each with their own data dir, sharing the same
`InMemoryS3`. Connected via loopback TCP.

### Test Infrastructure (`test/integration/helpers_test.go`)

New helpers for replication tests:

- `StartReplicaPair` — starts a primary and standby broker sharing the
  same InMemoryS3 and MemCluster. Returns both TestBrokers and the
  MemCluster for controlling elections. Each broker gets its own temp
  data dir.
- `WithReplicationAddr` — sets the replication listen address
- `WithLeaseElector` — injects a lease.Elector (for memlease in tests)

LocalStack helper (`test/integration/localstack_test.go`):

- Helper to start LocalStack via testcontainers and create a test bucket.
  Returns an S3API client configured to talk to the container. Tests that
  need LocalStack call this helper and skip if Docker is not available
  (`testcontainers.SkipIfNoDocker`).

### Integration Tests (`test/integration/replication_test.go`)

**TestReplicationBasic**
1. Start primary + standby (memlease: primary elected)
2. Produce 100 records to primary via Kafka client
3. Verify standby's WAL contains all 100 records (via WAL index query or
   by promoting and consuming)
4. Verify standby's metadata.log has the topic

**TestReplicationFailover**
1. Start primary + standby
2. Produce 100 records to primary
3. Demote primary, elect standby (memlease.Elect)
4. Connect Kafka client to new primary (former standby)
5. Consume all 100 records — verify none lost
6. Produce 100 more records to new primary — verify they succeed
7. Consume the additional 100 records — verify correctness

**TestReplicationZeroLoss**
1. Start primary + standby in sync mode
2. Produce N records concurrently from multiple goroutines. Track which
   produce calls return success vs. error.
3. Kill primary mid-stream (cancel context)
4. Promote standby
5. Consume from standby — verify every record whose produce returned
   success is present. No record that was ACK'd is missing. Records whose
   produce returned error may or may not be present (that's fine).

**TestReplicationReconnect**
1. Start primary + standby in sync mode
2. Produce 50 records (standby connected, all ACK'd)
3. Disconnect standby (close replication connection, e.g. by injecting
   a fault)
4. Produce 50 more records — verify they fail with KafkaStorageError
   (sync mode, standby was previously connected)
5. Reconnect standby — no SNAPSHOT sent (same epoch), live streaming resumes
6. Produce 50 more records (standby connected again, sync works)
7. Promote standby, consume — verify at least the 50 + 50 records from
   steps 2 and 6 are present. The 50 from step 4 may or may not be present
   (they were produced while standby was disconnected).

**TestReplicationAsyncMode**
1. Start primary + standby in async mode
2. Disconnect standby
3. Produce 50 records — verify they succeed (local fsync only, no error)
4. Reconnect standby — verify catch-up (promote and consume all 50)

**TestReplicationConsumerOffsets**
1. Start primary + standby
2. Produce 100 records, consume with a consumer group, commit offsets
3. Promote standby
4. New consumer with same group ID fetches from standby
5. Verify consumer resumes near the committed offset (may reprocess a few
   records due to async metadata replication, but no more than ~5s of
   auto-commit window)

**TestReplicationSingleNodeMode**
1. Start a broker WITHOUT `--replication-addr`
2. Produce and consume normally
3. Verify no lease activity, no TLS generation, no replication listeners
4. This is a regression test — existing single-node behavior must not break

**TestReplicationGracefulFailover**
1. Start primary + standby (memlease + shared InMemoryS3)
2. Produce 100 records to primary
3. Cancel primary's context (simulating SIGTERM)
4. Wait for primary's Run() to return (graceful shutdown complete)
5. Verify standby detects released lease and promotes (within ~2s, not
   ~15s lease duration)
6. Connect Kafka client to new primary, consume all 100 records

**TestReplicationDemotionStopsWrites**
1. Start primary + standby
2. Demote primary (memlease.Demote)
3. Immediately attempt produce to demoted primary — verify it fails
   (connection refused or error, NOT a successful write)

**TestReplicationSyncModeNoStandbyBootstrap**
1. Start primary in sync mode WITHOUT a standby
2. Produce records — verify they succeed (async fallback until first
   standby connects)
3. Start standby, wait for it to connect
4. Disconnect standby
5. Produce records — verify they now FAIL (sync enforcement active after
   first connection)

**TestReplicationRoleSwap**
1. Start A (primary) + B (standby)
2. Produce 100 records to A
3. Demote A, promote B (A becomes standby, B becomes primary)
4. Produce 100 more records to B — verify A receives them as standby
5. Demote B, promote A (back to original roles)
6. Connect Kafka client to A, consume all 200 records — verify none lost
7. Produce 50 more records to A — verify they succeed

**TestReplicationDemotionInflightProduce**
1. Start primary + standby in sync mode
2. Block the standby's ACK (e.g., inject a fault so the standby never
   sends ACK for the next batch)
3. Start a produce request in a goroutine — it blocks in the WAL writer
   waiting for the standby ACK
4. While the produce is in-flight, demote the primary (memlease.Demote)
5. Verify the in-flight produce receives an error (not a hang) — the
   demotion closes the Sender, which drains all pending channels with
   errDisconnected
6. Verify no goroutine leaks after demotion completes

### Verify

```bash
go test ./internal/repl/ -v -race
go test ./internal/broker/ -run "TestReplication|TestLeaseTiming" -v -race
go test ./test/integration/ -run TestReplication -v -race
```
