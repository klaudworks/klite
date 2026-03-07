# Testing: Standby Replica

Test strategy for the standby replica feature. Follows the project's testing
pyramid: unit tests first, integration tests only for behavior that crosses
multiple components or requires real TCP connections.

Individual test lists are co-located with their implementation task files
(`01-lease.md` through `04-broker-lifecycle.md`).

---

## Test Organization

```
internal/lease/lease.go              # Interface
internal/lease/memlease/memlease.go  # In-memory impl + tests
internal/lease/s3lease/s3lease.go    # S3 impl + tests
internal/repl/protocol.go           # Framing encode/decode
internal/repl/sender.go             # Primary-side sender
internal/repl/receiver.go           # Standby-side receiver
internal/repl/tls.go                # Auto-cert generation
internal/repl/protocol_test.go      # Unit tests
internal/repl/sender_test.go        # Unit tests
internal/repl/receiver_test.go      # Unit tests
internal/repl/tls_test.go           # Unit tests
internal/wal/writer_repl_test.go    # WAL writer replication tests
internal/metadata/log_repl_test.go  # Metadata hook tests
test/integration/replication_test.go # Integration tests
test/integration/s3lease_test.go     # S3 lease integration tests (LocalStack)
```

---

## What NOT to Test

- **Kubernetes-specific behavior** — no K8s lease, no K8s API
- **franz-go client retry behavior** — we test that our broker handles
  failover correctly. Client retry/reconnect behavior is the client
  library's responsibility.
- **mTLS with real CAs / cert-manager** — auto-generated self-signed
  certs are sufficient. We're not testing the Go TLS stack.
- **Real AWS S3** — LocalStack is sufficient for conditional write
  verification. No real AWS credentials needed.
- **S3 flusher behavior on standby** — the standby does NOT run the S3
  flusher. No test needed for "S3 flush on standby" because it doesn't
  happen.

---

## When Stuck

Use the repo-explorer subagent to check reference implementations for test
patterns and edge cases:

- **PostgreSQL replication tests**: `src/test/recovery/` — TAP-based tests
  for streaming replication, synchronous commit, standby promotion.
  Look at `t/001_stream_rep.pl`.

- **Litestream replication tests**: `replica_client_test.go` and
  `db_test.go` for generation-based replication and WAL streaming edge
  cases.

---

## Verify (All Standby Replica Tests)

```bash
# Unit tests
go test ./internal/lease/... -v -race
go test ./internal/repl/... -v -race
go test ./internal/wal/ -v -race
go test ./internal/metadata/ -v -race
go test ./internal/s3/ -run TestInMemoryS3Conditional -v -race

# Integration tests (memlease, loopback)
go test ./test/integration/ -run TestReplication -v -race

# Integration tests (LocalStack — requires Docker)
go test ./test/integration/ -run TestS3Lease -v -race

# Full regression
go test ./... -race
```
