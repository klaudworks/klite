# Foundations

General reference for all work units. Read this before starting any
implementation task. Contains project structure, configuration, lifecycle,
logging, error handling, performance principles, and concurrency model.

---

## Project Structure

```
klite/
  cmd/
    klite/
      main.go             # Entry point: parse config, start broker, signal handling
  internal/
    broker/
      broker.go           # Broker lifecycle: Start(), Shutdown(), state
      config.go           # Configuration struct + defaults + parsing
    server/
      listener.go         # TCP listener, accept loop
      conn.go             # Per-connection read/write goroutines, wire framing
      dispatch.go         # Request routing: api key -> handler function
    handler/
      api_versions.go     # Key 18
      metadata.go         # Key 3
      create_topics.go    # Key 19
      produce.go          # Key 0
      fetch.go            # Key 1
      list_offsets.go     # Key 2
      ... (one file per API key or small group)
    cluster/
      state.go            # Cluster state: topics, partitions, offsets
      topic.go            # Topic + partition data structures
      partition.go        # partData: records, offsets, HW, batch storage, read/write methods
  test/
    integration/
      helpers_test.go     # Test infrastructure (start broker, create client, etc.)
      produce_test.go     # Produce tests (one file per API group)
      fetch_test.go       # Fetch tests
      ...
  go.mod
  go.sum
```

Phase 3+ additions:
```
  internal/
    wal/
      writer.go           # WAL writer goroutine, fsync batching (Phase 3)
      framedlog.go        # Shared framing: length + CRC + payload (Phase 3)
      segment.go          # Segment file management, rotation (Phase 3)
    metadata/
      log.go              # metadata.log: topics, configs, offsets, S3 objects (Phase 3)
      entries.go          # Entry type definitions and serialization (Phase 3)
    s3/
      flusher.go          # S3 flush pipeline (Phase 4)
      reader.go           # S3 range reads, index caching (Phase 4)
    sasl/
      scram.go            # SCRAM crypto: PBKDF2, HMAC, RFC 5802 parsing (Phase 5)
      plain.go            # PLAIN auth parsing (Phase 5)
      store.go            # Credential store (sasls struct, lookup methods) (Phase 5)
```

### Rationale

- `internal/` prevents external imports (this is a binary, not a library)
- `handler/` has one file per API key -- easy to find, easy to add
- `cluster/` owns all mutable state. `partData` is a concrete struct with
  methods, NOT an interface. See `06-partition-data-model.md` for the struct
  design and phase evolution.
- No `storage/` package. There is no unified storage interface -- the three
  storage backends (memory, WAL, S3) have fundamentally different write
  semantics (sync vs async fsync vs not-a-write-target) and read patterns
  (slice index vs pread vs range-read). Instead, `partData` grows new
  fields and methods as storage tiers are added. WAL and S3 code live in
  their own packages (`wal/`, `s3/`) as concrete types, injected into
  `partData` as fields when enabled. The WAL serves as both durability
  (write-ahead log) and a read tier (via in-memory index + pread).
- `test/integration/` is separate from unit tests -- these start a real broker

---

## Configuration

### Decisions

- **Format:** CLI flags + optional TOML config file. Flags override file.
  Use `flag` stdlib for flags, a small TOML parser for file (e.g., `BurntSushi/toml`
  or parse manually -- it's a flat config).
- **No dynamic config changes** in Phase 1. Topic configs are set at creation.
  IncrementalAlterConfigs (Phase 3) updates in-memory state only.
- **Env var override** for containerized deployments: `KLITE_<FLAG_NAME>`.

### Broker Config

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--listen` | `KLITE_LISTEN` | `:9092` | Listen address |
| `--advertised-addr` | `KLITE_ADVERTISED_ADDR` | (see below) | Address clients use to connect |
| `--data-dir` | `KLITE_DATA_DIR` | `./data` | WAL and metadata directory |
| `--cluster-id` | `KLITE_CLUSTER_ID` | (generated UUID on first start) | Kafka cluster ID |
| `--node-id` | `KLITE_NODE_ID` | `0` | Broker node ID (always 0 for single broker) |
| `--default-partitions` | `KLITE_DEFAULT_PARTITIONS` | `1` | Default partition count for auto-created topics |
| `--auto-create-topics` | `KLITE_AUTO_CREATE_TOPICS` | `true` | Auto-create topics on Metadata/Produce |
| `--log-level` | `KLITE_LOG_LEVEL` | `info` | Log level: debug, info, warn, error |
| `--ring-buffer-max-memory` | `KLITE_RING_BUFFER_MAX_MEMORY` | `512MiB` | Global memory budget for all partition ring buffers (Phase 3+) |
| `--wal-max-disk-size` | `KLITE_WAL_MAX_DISK_SIZE` | `1GiB` | Max total WAL on disk; caps unflushed segments only (Phase 3+) |
| `--s3-bucket` | `KLITE_S3_BUCKET` | (none) | S3 bucket for offload (Phase 4) |
| `--s3-region` | `KLITE_S3_REGION` | (none) | S3 region |
| `--s3-endpoint` | `KLITE_S3_ENDPOINT` | (none) | S3 endpoint (for MinIO/LocalStack) |
| `--s3-flush-interval` | `KLITE_S3_FLUSH_INTERVAL` | `10m` | How often all data is flushed to S3 (partitions + metadata.log). This is the RPO — max data loss if local disk dies. (Phase 4) |
| `--sasl-enabled` | `KLITE_SASL_ENABLED` | `false` | Enable SASL authentication (Phase 5) |
| `--sasl-mechanism` | `KLITE_SASL_MECHANISM` | (none) | Mechanism for CLI-specified user: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512 (Phase 5) |
| `--sasl-user` | `KLITE_SASL_USER` | (none) | Username for CLI-specified bootstrap user (Phase 5) |
| `--sasl-password` | `KLITE_SASL_PASSWORD` | (none) | Password for CLI-specified bootstrap user (Phase 5) |
| `--retention-check-interval` | `KLITE_RETENTION_CHECK_INTERVAL` | `5m` | How often the retention enforcement goroutine runs (Phase 6) |
| `--compaction-check-interval` | `KLITE_COMPACTION_CHECK_INTERVAL` | `30s` | How often to check for dirty partitions needing compaction (Phase 6) |

### Advertised Address

The advertised address is returned in Metadata responses so clients know where
to connect. Resolution logic at startup:

1. If `--advertised-addr` is explicitly set -> use it as-is.
2. If `--listen` has a host (e.g., `192.168.1.5:9092`) -> use that.
3. If `--listen` has no host (e.g., `:9092`) -> default to `localhost:<port>`
   and log a warning:

```
WARN: --advertised-addr not set, using "localhost:9092" in Metadata responses.
      Clients outside this host won't be able to connect. Set --advertised-addr
      for production or container deployments.
```

This matches dev-oriented defaults (Redpanda dev mode does the same). Production
and container deployments must set `--advertised-addr` explicitly.

### Cluster ID

Generated as a UUID on first start, persisted to `<data-dir>/meta.properties`.
Format matches Kafka's: base64-encoded UUID (22 chars). This file is the
single source of truth for cluster identity across restarts.

### Node ID

Always `0` for single-broker. Hardcode in responses. Configurable only for
potential future multi-broker (not a goal, but costs nothing to parameterize).

---

## Lifecycle

### Startup Sequence

```
1. Parse config (flags + file + env)
2. Create/open data directory
3. Load meta.properties (or generate cluster ID on first start)
4. Initialize in-memory cluster state (empty topic map)
5. [Phase 3+] Replay metadata.log then WAL to rebuild state
6. Start TCP listener
7. Log "klite started" with listen address and cluster ID
```

### Shutdown Sequence

```
1. Receive SIGTERM or SIGINT
2. Stop accepting new connections
3. Close TCP listener
4. Close shutdownCh (wakes all long-poll Fetch waiters, JoinGroup waiters, etc.)
5. Wait for in-flight handler goroutines to complete (with timeout, e.g., 10s)
6. Close all connections
7. [Phase 3+] Flush WAL
8. [Phase 4+] Run unified S3 sync (flush all partitions + upload metadata.log)
9. Log "klite stopped"
```

Step 4 is critical: without closing `shutdownCh`, Fetch handlers with 30s
MaxWaitMs would delay shutdown by up to 30 seconds. By closing the channel,
all blockers wake immediately, re-fetch whatever is available, send their
response, and the handler goroutine exits. The 10s drain timeout in step 5
only covers actual handler processing (milliseconds), not long-poll waits.

### Signal Handling

Use `signal.NotifyContext` for clean cancellation propagation. The broker's
`Run()` method takes a `context.Context` and returns when cancelled.

```go
ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
defer stop()
if err := broker.Run(ctx); err != nil {
    log.Fatal(err)
}
```

---

## Logging

Use `log/slog` (stdlib, Go 1.21+). Structured JSON in production, text in
development. No external logging library.

Key log points:
- Startup: config summary, listen address, cluster ID
- Connection: new connection, connection closed, connection error
- Request: API key, version, client ID, latency (debug level)
- Error: handler panics (recovered), protocol errors, storage errors
- Shutdown: drain status, final flush status

### Decision: No Metrics in Phase 1

Skip Prometheus/metrics initially. Add in Phase 3+ if needed. `slog` at debug
level provides enough observability for development. The agent should NOT spend
time on metrics infrastructure.

---

## Error Handling

### Principles

1. **Never crash on client input.** Malformed requests, invalid API keys, and
   protocol errors produce error responses or clean disconnects, never panics.
2. **Recover handler panics.** Wrap each handler dispatch in a defer/recover
   that logs the panic and closes the connection.
3. **Propagate storage errors as Kafka error codes.** Disk full = appropriate
   Kafka error. S3 timeout = appropriate Kafka error. Map Go errors to
   `kerr` error codes at the handler level.

### Connection Error Handling

| Error | Action |
|-------|--------|
| Read timeout (no data for 5+ min) | Close connection |
| Invalid frame length (>100MB or <0) | Close connection |
| Unknown API key | Close connection (matches Kafka behavior) |
| Unsupported API version | Return error response with supported versions |
| Handler panic | Log, close connection |
| Write error | Close connection |

### Storage Error Handling (Phase 3+)

| Error | Action |
|-------|--------|
| WAL disk full | Reject produces with `KAFKA_STORAGE_ERROR` |
| WAL write error | Reject produce, log error |
| WAL corruption on startup | Skip corrupted tail, log warning, continue |
| S3 upload failure | Retry with backoff, block WAL trim until success |
| S3 read failure | Return `KAFKA_STORAGE_ERROR` for that fetch partition |

---

## Performance Infrastructure

**Guiding principle: get it correct first, optimize after tests pass.** The
patterns below are low-effort wins that should be adopted from the start because
they're simple and natural in Go. Anything more aggressive (object pooling,
zero-alloc guarantees, GC tuning) is deferred until the protocol works end to
end and we have real benchmarks to guide us.

### Buffer Pool

Use a `sync.Pool` for `[]byte` slices on the read/write hot path. This avoids
the most obvious source of GC pressure and is trivial to implement:

```go
var bufPool = sync.Pool{
    New: func() any {
        b := make([]byte, 0, 64*1024) // 64KB initial capacity
        return &b
    },
}

func getBuf() *[]byte  { return bufPool.Get().(*[]byte) }
func putBuf(b *[]byte) { *b = (*b)[:0]; bufPool.Put(b) }
```

Key rules:
- Use pooled buffers for request reading and response encoding.
- RecordBatch bytes from Produce must be copied into storage (the pool buffer
  is reused). Use a separate large-buffer pool for batch storage if in-memory,
  or write directly to WAL.
- Response encoding (`AppendTo`) should append to a pooled buffer, not `nil`.

### Buffered I/O

Wrap every connection's `net.Conn` in `bufio.Reader` / `bufio.Writer`. This
coalesces syscalls and is standard practice — no reason to skip it.

### Sensible Defaults (Not Gates)

These are good habits, not Phase 1 completion criteria:

- **Append to existing slices.** `AppendTo(buf)` instead of `AppendTo(nil)`.
- **Avoid gratuitous allocations.** Don't allocate where a stack variable or
  existing buffer works, but don't contort the code to avoid every allocation.
  A clean `RequestForKey()` call per request is fine until profiling says
  otherwise.
- **Minimize string copies.** Go optimizes `map[string]` lookups with `[]byte`
  keys — use that when convenient, but don't add `unsafe` hacks.

### Benchmarks

Add basic benchmarks alongside implementation so we can measure progress:

```
BenchmarkProduceSingleRecord    -- single 1KB record produce+ack
BenchmarkProduceBatch100        -- 100 records in one batch
BenchmarkFetchSequential        -- sequential fetch of 1000 records
```

Run with `-benchmem` to track allocations. Use the numbers to guide
optimization — they are not a pass/fail gate for Phase 1.

### Future Optimization (Phase 1+)

Once correctness is proven and benchmarks exist, consider:

- `GOGC` / `GOMEMLIMIT` tuning for production deployment
- P99.9 GC pause targets
- Allocation-zero hot paths

These are worth pursuing but only after the protocol is correct and the code
structure is stable. Premature optimization that makes the code harder to
understand or contradicts the concurrency model is a net negative.

Note: `kmsg.RequestForKey()` allocates a typed request struct per call, but
this is cheap (small struct on the heap) and the simplest correct approach.
Do not pool typed request objects — the complexity isn't justified until
profiling proves it matters.

---

## Go Dependencies

| Package | Purpose | Phase |
|---------|---------|-------|
| `github.com/twmb/franz-go/pkg/kmsg` | Kafka protocol codec | 1 |
| `github.com/twmb/franz-go/pkg/kbin` | Wire primitives (Reader, etc.) | 1 |
| `github.com/twmb/franz-go/pkg/kerr` | Kafka error code constants | 1 |
| `github.com/klauspost/compress/zstd` | Zstd compression | 1 |
| `github.com/klauspost/compress/snappy` | Snappy compression | 1 (if needed for validation) |
| `github.com/pierrec/lz4/v4` | LZ4 compression | 1 (if needed for validation) |
| `github.com/google/uuid` | Cluster ID generation | 1 |
| `github.com/twmb/franz-go/pkg/kgo` | Test client | 1 (test only) |
| `github.com/twmb/franz-go/pkg/kadm` | Test admin client | 1 (test only) |
| `go.uber.org/goleak` | Goroutine leak detection in tests | 1 (test only) |
| `golang.org/x/crypto/pbkdf2` | PBKDF2 key derivation for SCRAM auth | 5 |
| `github.com/twmb/franz-go/pkg/sasl/plain` | PLAIN SASL client (test only) | 5 (test only) |
| `github.com/twmb/franz-go/pkg/sasl/scram` | SCRAM SASL client (test only) | 5 (test only) |
| `github.com/aws/aws-sdk-go-v2` | S3 client | 4 |

### Compression Decision

**Pass-through compressed batches.** The broker does NOT decompress + recompress.
Client sends compressed RecordBatch bytes; broker stores them as-is, serves
them as-is. This is simpler, faster, and avoids needing compression libraries
for the produce/fetch hot path.

Compression libraries are needed only for:
- **ListOffsets MaxTimestamp** (need to decompress to find max timestamp within
  a compressed batch)
- **Log compaction** (Phase 3+, needs to decompress to read keys)

For Phase 1, the batch header already contains `MaxTimestamp` so no
decompression is needed for ListOffsets. Compression libraries can be
deferred until log compaction requires them.

---

## Concurrency Model: Per-Partition RWMutex

We use fine-grained locking at two levels, not a single serialization point:

**Cluster-level state** (topic map, broker config): protected by a
`sync.RWMutex`. Topic creation/deletion takes a write lock. Topic lookups
(every Produce, Fetch, Metadata) take a read lock. Topic mutation is rare
(admin operations), so read contention is near-zero.

**Partition-level state** (batches, HW, offsets): each `partData` has its own
`sync.RWMutex`. Produce takes a write lock on the specific partition. Fetch
takes a read lock. Different partitions are fully concurrent — Produce to
partition 0 never blocks Fetch from partition 5.

```go
type Cluster struct {
    mu     sync.RWMutex                       // protects topics map
    topics map[string]*topicData              // topic name -> topic
    cfg    Config
}

type topicData struct {
    name       string
    id         [16]byte
    partitions []*partData    // indexed by partition number
    configs    map[string]string
}

type partData struct {
    mu       sync.RWMutex   // protects all fields below
    topic    string
    index    int32
    batches  []storedBatch
    hw       int64
    logStart int64
    // ...
}
```

Handler pattern (Produce):
```go
func handleProduce(c *Cluster, req *kmsg.ProduceRequest) {
    for _, rt := range req.Topics {
        // Read lock on cluster to look up topic
        c.mu.RLock()
        td := c.topics[rt.Topic]
        c.mu.RUnlock()

        for _, rp := range rt.Partitions {
            pd := td.partitions[rp.Partition]
            // Write lock on partition to append
            pd.mu.Lock()
            baseOffset := pd.pushBatch(rp.Records)
            pd.mu.Unlock()
        }
    }
}
```

Handler pattern (Fetch):
```go
func handleFetch(c *Cluster, req *kmsg.FetchRequest) {
    for _, rt := range req.Topics {
        c.mu.RLock()
        td := c.topics[rt.Topic]
        c.mu.RUnlock()

        for _, rp := range rt.Partitions {
            pd := td.partitions[rp.Partition]
            // Read lock — concurrent Fetches don't block each other
            pd.mu.RLock()
            batches := pd.fetchFrom(rp.FetchOffset, rp.PartitionMaxBytes)
            pd.mu.RUnlock()
        }
    }
}
```

**Consumer groups** keep the per-group goroutine pattern (see
`12-group-coordinator.md`). JoinGroup blocks until all members join — that
blocking behavior requires a dedicated goroutine per group regardless of the
partition concurrency model.

See `06-partition-data-model.md` for the full `partData` struct and method
signatures.
