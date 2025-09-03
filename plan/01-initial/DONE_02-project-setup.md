# Project Setup

Scaffold the Go project from an empty repo. After this work unit, the binary
compiles, starts, listens on `:9092`, and shuts down cleanly on SIGTERM.

No protocol handling yet — that starts in `03-tcp-and-wire.md`.

Read before starting: `00-overview.md` (vision, scope, API table),
`01-foundations.md` (project structure, config, lifecycle, concurrency model),
`00b-testing.md` (test infrastructure, three-tier strategy).

---

## Steps

### 1. Initialize Go Module

```bash
go mod init github.com/klaudworks/klite
```

### 2. Add Dependencies

```bash
go get github.com/twmb/franz-go/pkg/kmsg
go get github.com/twmb/franz-go/pkg/kbin
go get github.com/twmb/franz-go/pkg/kerr
go get github.com/google/uuid
# Test dependencies
go get github.com/twmb/franz-go/pkg/kgo
go get github.com/twmb/franz-go/pkg/kadm
```

### 3. Create Directory Structure

```
mkdir -p cmd/klite
mkdir -p internal/broker
mkdir -p internal/server
mkdir -p internal/handler
mkdir -p internal/cluster
mkdir -p test/integration
```

See `01-foundations.md` "Project Structure" for the full layout and rationale.

### 4. Implement Config (`internal/broker/config.go`)

Config struct with defaults and flag parsing. See `01-foundations.md`
"Configuration" for the full config table.

```go
type Config struct {
    Listen            string
    AdvertisedAddr    string
    DataDir           string
    ClusterID         string
    NodeID            int32
    DefaultPartitions int
    AutoCreateTopics  bool
    LogLevel          string
}

func DefaultConfig() Config {
    return Config{
        Listen:            ":9092",
        DataDir:           "./data",
        NodeID:            0,
        DefaultPartitions: 1,
        AutoCreateTopics:  true,
        LogLevel:          "info",
    }
}
```

Flag parsing uses `flag` stdlib. Env var override pattern:
`KLITE_<FLAG_NAME>` (uppercase, dashes to underscores).

### 5. Implement Broker Lifecycle (`internal/broker/broker.go`)

```go
type Broker struct {
    cfg        Config
    listener   net.Listener
    shutdownCh chan struct{}  // closed on shutdown to wake all blockers
}

func (b *Broker) Run(ctx context.Context) error {
    // 1. Create/open data directory
    // 2. Load or generate cluster ID (meta.properties)
    // 3. Resolve advertised address (see 01-foundations.md)
    // 4. Start TCP listener
    // 5. Log startup
    // 6. Accept connections (blocks until ctx cancelled)
    // 7. Shutdown sequence (see 01-foundations.md)
}
```

### 6. Implement meta.properties (`internal/broker/`)

On first start: generate UUID, base64-encode (22 chars), write to
`<data-dir>/meta.properties`. On subsequent starts: read and use.

Format (matches Kafka):
```
cluster.id=<base64-uuid>
```

### 7. Implement main.go (`cmd/klite/main.go`)

```go
func main() {
    cfg := broker.DefaultConfig()
    // Parse flags into cfg
    // Apply env var overrides

    ctx, stop := signal.NotifyContext(context.Background(),
        os.Interrupt, syscall.SIGTERM)
    defer stop()

    b := broker.New(cfg)
    if err := b.Run(ctx); err != nil {
        slog.Error("broker failed", "error", err)
        os.Exit(1)
    }
}
```

### 8. Configure Logging

Set up `slog` with text handler (development) or JSON handler (production)
based on `--log-level`. See `01-foundations.md` "Logging" for key log points.

### 9. Create Test Infrastructure (`test/integration/helpers_test.go`)

Create `test/integration/helpers_test.go` with `StartBroker`, `NewClient`,
`NewAdminClient`, `ProduceSync`, `ProduceN`, and `ConsumeN` helpers. Copy the
implementations from `00b-testing.md` "Test Infrastructure" section. These
helpers are used by every subsequent work unit's tests.

---

## Acceptance Criteria

- `go build ./cmd/klite` succeeds
- Binary starts and logs: listen address, cluster ID
- Listening on `:9092` (or configured address)
- `meta.properties` created on first start, reused on restart
- `SIGTERM` or `SIGINT` triggers clean shutdown with log message
- `--help` shows all flags with descriptions
- `test/integration/helpers_test.go` exists with `StartBroker`, `NewClient`

### Verify

```bash
go build ./cmd/klite
go vet ./...
go test ./... -race -count=5
```

### Commit

After tests pass, stage and commit all changes:

```bash
git add -A
git commit -m "02: project setup — scaffold, config, broker lifecycle, test helpers"
```
