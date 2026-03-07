# Standby Replica: Overview

Optional active-passive failover for klite. The primary streams WAL entries and
metadata.log entries to a standby over mTLS TCP. The standby replays them
locally but does not accept client connections until promoted. Promotion happens
when the primary loses its S3 lease (fencing).

## Zero Data Loss Guarantee

The primary does not ACK a produce request until both the local fsync **and**
the standby's fsync confirmation are complete. If the primary dies, every
acknowledged record exists on the standby's disk.

Metadata.log entries (topic creation, offset commits, config changes) are
streamed fire-and-forget in the replication stream — no ACK wait. Consumer
offsets are self-healing: clients recommit within 5 seconds via auto-commit.
Topic metadata is reconstructable from the WAL on replay.

## Architecture

```
                        ┌─────────────────┐
Kafka clients ────────> │    Primary      │
                        │  (accepts R/W)  │
                        │                 │
                        │  WAL writer     │── replication stream (mTLS TCP) ──>┐
                        │  metadata.log   │                                    │
                        │  S3 lease holder│                                    │
                        └─────────────────┘                                    │
                                                                               │
                        ┌─────────────────┐                                    │
                        │    Standby      │<───────────────────────────────────┘
                        │  (passive)      │
                        │                 │
                        │  WAL replay     │
                        │  metadata.log   │
                        │  S3 lease watch │
                        └─────────────────┘
```

## Fencing: S3 Lease

A single lease object in the S3 bucket prevents split-brain. The primary holds
the lease via conditional writes (`PutObject` with `If-Match` ETag). If the
primary can't renew, it stops accepting writes. The standby claims the lease
only after it expires.

S3 is already required for any deployment that wants replication (the standby
needs S3 for catch-up). No additional dependencies — no Kubernetes client-go,
no etcd, no DynamoDB.

See `01-lease.md` for the full design.

## Auto-Generated TLS

The replication channel uses mTLS. By default, klite generates its own CA and
node certificate on first primary election and stores them in S3:

```
<prefix>/repl/ca.crt
<prefix>/repl/ca.key
<prefix>/repl/node.crt
<prefix>/repl/node.key
```

The standby downloads these from S3 before connecting. Both nodes use the same
keypair — the mTLS handshake verifies that both sides trust the auto-generated
CA. No manual cert setup, no flags, no cert-manager.

See `04-broker-lifecycle.md` for the certificate lifecycle.

## Latency Impact

The produce ACK path becomes `max(local_fsync, network_rtt + remote_fsync)`.
On the same network (same AZ, same rack): ~2-4ms total, barely worse than
today's ~2ms. The local fsync and the standby's fsync happen in parallel.

## Standby Behavior

The standby is purely passive:
- Receives WAL and metadata.log entries from the primary
- Writes them to its own local WAL and metadata.log
- Builds WAL index and chunk pool data (ready to serve immediately on promotion)
- Does NOT accept Kafka client connections
- Does NOT serve Fetch requests

On promotion, the standby starts the TCP listener and begins serving clients.
Kafka clients reconnect automatically (retries are the default in all major
client libraries).

## Replication Behavior

Replication is always synchronous: produce fails with `KafkaStorageError` if
standby ACK is not received within the timeout. Zero data loss, but availability
depends on standby health.

**Bootstrap exception:** until the first standby connects (completes HELLO
handshake), produces succeed with local fsync only, with a warning. Once a
standby has connected, sync enforcement activates. If the standby disconnects
after that, produces fail.

## Work Units

| File | Contents |
|------|----------|
| `00-overview.md` | This file. |
| `01-lease.md` | S3 lease: fencing, CAS renewal, expiry, pluggable interface. |
| `02-replication-protocol.md` | Wire protocol, sender, receiver, initial sync, reconnection. |
| `03-wal-integration.md` | WAL writer changes, Replicator interface, ACK flow. |
| `04-broker-lifecycle.md` | Role-based startup, promotion, demotion, auto-cert, config. |
| `05-testing.md` | Test strategy, unit tests, integration tests. |

## Execution Order

| Step | Work | Dependencies |
|------|------|-------------|
| 1 | Lease interface + in-memory impl + S3 impl | None |
| 2 | Replication protocol (framing, sender, receiver) | None |
| 3 | WAL writer integration (Replicator interface) | Step 2 |
| 4 | Broker lifecycle changes (role-based startup, auto-cert) | Steps 1, 2, 3 |
| 5 | Integration tests | Steps 1-4 |
| 6 | Documentation | Step 5 |

Steps 1 and 2 can be done in parallel.

## Reference Implementations

When stuck on implementation details, protocol edge cases, or failover
semantics, use the repo-explorer subagent to search these reference
implementations:

- **PostgreSQL** (`https://github.com/postgres/postgres`) — the gold standard
  for synchronous WAL streaming replication. The most relevant code:
  - `src/backend/replication/walsender.c` — WAL sender (primary side)
  - `src/backend/replication/walreceiver.c` — WAL receiver (standby side)
  - `src/backend/access/transam/xlog.c` — WAL write + fsync + sync rep wait
  - `src/backend/replication/syncrep.c` — synchronous replication ACK logic
  - `src/test/recovery/` — TAP tests for streaming replication, promotion,
    synchronous commit. Look at `t/001_stream_rep.pl`.

- **Litestream** (`https://github.com/benbjohnson/litestream`) — Go project
  that streams SQLite WAL to S3. Relevant for S3 object layout, generation-
  based fencing, and the replication lifecycle:
  - `replica.go` / `replica_client.go` — S3 replication client
  - `db.go` — WAL monitoring and streaming
  - `generation.go` — generation concept (similar to our S3 lease epochs)

Use specific questions when querying, e.g. "how does PostgreSQL's walsender
handle standby disconnect during synchronous commit?" or "how does Litestream
detect a new generation and avoid split-brain?"
