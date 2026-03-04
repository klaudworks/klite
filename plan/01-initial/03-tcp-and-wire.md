# TCP and Wire Protocol

TCP listener, per-connection goroutines, wire framing, request dispatch, and
response ordering. After this work unit, the broker accepts TCP connections,
reads Kafka wire frames, and routes them to handler functions.

No handlers yet — those start in `04-api-versions.md`. The dispatch table
starts empty; unknown API keys close the connection.

Prerequisite: `02-project-setup.md` complete (binary compiles and starts).

Read before starting: `01-foundations.md` (concurrency model, buffered I/O,
error handling, lifecycle/shutdown), `00b-testing.md` (test infrastructure).

---

## TCP Listener

### Design

- Listen on configurable address (default `:9092`)
- Accept loop in a goroutine, spawn per-connection goroutines
- Graceful shutdown: close listener, drain active connections

```go
func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
    for {
        conn, err := ln.Accept()
        if err != nil {
            // Check if listener was closed (shutdown)
            select {
            case <-ctx.Done():
                return nil
            default:
                return err
            }
        }
        go s.handleConn(ctx, conn)
    }
}
```

### Per-Connection Goroutines

Two goroutines per connection plus per-request handler goroutines:

1. **Read goroutine**: reads frames, decodes headers, parses request via kmsg,
   spawns a handler goroutine, immediately reads the next frame. Never blocks
   on handler execution.
2. **Write goroutine**: receives responses via channel, reorders by sequence
   number (responses may arrive out of order), writes to socket in request order.
3. **Handler goroutines** (spawned per request): execute the handler logic,
   may block (e.g., Fetch long-poll for 30s, JoinGroup for rebalance timeout),
   and send response to write goroutine when done. If the handler returns
   `nil, nil` (e.g., `acks=0` produce), a skip marker is sent to the write
   goroutine so it advances the sequence counter without writing bytes.

```
TCP socket
    |
    +-- read goroutine: readFrame() -> parse() -> go handler() -> loop
    |                                                  |
    |                                         handler goroutine (may block)
    |                                                  |
    |                                                  v
    +-- write goroutine: <-respCh -> reorder by seq -> writeFrame() -> socket
```

### Why Pipelining Is Required

The Java Kafka client (`KafkaConsumer`) uses a **single TCP connection per
broker** for all request types — Fetch, Heartbeat, OffsetCommit, Metadata. It
sends requests asynchronously (NIO) without waiting for prior responses. A
long-polling Fetch (30s MaxWaitMs) must not block Heartbeat processing on the
same connection, or the consumer gets kicked from its group.

franz-go uses separate connections per purpose (`cxnProduce`, `cxnFetch`,
`cxnGroup`, `cxnSlow`, `cxnNormal` — 5 connections per broker), so it doesn't
strictly need pipelining. But we must support the Java client's single-connection
model.

### Response Ordering (Sequence Numbers)

Kafka's wire protocol requires responses in request order (clients match by
correlation ID, but expect FIFO on the socket). Since handler goroutines
complete at different speeds (Metadata: ~1us, Fetch long-poll: ~30s), responses
arrive at the write goroutine out of order.

**Sequence numbers are assigned in the read goroutine** (arrival order), not in
handler goroutines (completion order). This guarantees the write goroutine can
reorder responses back to arrival order. If sequence numbers were assigned at
completion time, a fast handler (Metadata ~1us) completing before a slow one
(Fetch ~30s) would get a lower sequence number despite arriving later — the
write goroutine would send responses in completion order, violating FIFO.

For `acks=0` produce requests (no response expected), the handler goroutine
sends a **skip marker** (`skip: true`) to the write goroutine. The write
goroutine advances `nextSeq` without writing any bytes. This ensures the
sequence number space has no gaps, so the write goroutine never stalls waiting
for a response that will never arrive.

The write goroutine maintains an out-of-order buffer keyed by sequence number:

```go
type clientResp struct {
    kresp kmsg.Response  // typed response (write goroutine encodes it)
    corr  int32          // correlation ID
    seq   uint32         // monotonic sequence assigned in READ goroutine
    skip  bool           // true = no response to write (acks=0), just advance seq
    err   error          // non-nil = close connection
}

// Write goroutine reorders responses:
var (
    nextSeq uint32
    oooresp = make(map[uint32]clientResp)  // out-of-order buffer
)
for {
    resp := oooresp[nextSeq]
    if resp not found {
        resp = <-respCh
        if resp.seq != nextSeq {
            oooresp[resp.seq] = resp  // stash, wait for earlier ones
            continue
        }
    }
    delete(oooresp, nextSeq)
    nextSeq++
    if resp.skip {
        continue  // acks=0: advance sequence, write nothing
    }
    // encode and write resp (see Response Assembly below)
}
```

The write goroutine receives typed `kmsg.Response` objects and calls
`kresp.IsFlexible()`, `kresp.Key()`, and `kresp.AppendTo()` to encode.

### Connection Limits

| Limit | Default | Description |
|-------|---------|-------------|
| Max connections | 10,000 | Total TCP connections across all clients. New connections beyond this are closed immediately with a warning log. Simple `atomic.Int64` counter. |
| Max in-flight per connection | 100 | Server-side cap on pipelined requests. If the read goroutine has dispatched 100 handler goroutines that haven't responded yet, it stops reading until some complete. Safety valve against misbehaving clients. |

### Shutdown Interaction with Long-Poll

On shutdown, a global `shutdownCh` (closed by the shutdown sequence) is selected
on by all blocking operations (Fetch long-poll, JoinGroup wait, etc.):

```go
select {
case <-w.ch:        // data arrived
case <-timer.C:     // MaxWaitMs elapsed
case <-shutdownCh:  // broker shutting down — wake up immediately
}
```

This ensures long-poll Fetches don't delay shutdown beyond the drain timeout.
The shutdown sequence (see `01-foundations.md`): close listener -> close
`shutdownCh` (wakes all blockers) -> wait for in-flight handlers (10s timeout)
-> force-close connections.

### Per-Connection Buffered I/O

**Performance-critical.** Each connection wraps its `net.Conn` in buffered
readers and writers to minimize syscall overhead:

```go
const (
    connReadBufSize  = 64 * 1024  // 64KB read buffer
    connWriteBufSize = 64 * 1024  // 64KB write buffer
)

func (s *Server) handleConn(ctx context.Context, nc net.Conn) {
    br := bufio.NewReaderSize(nc, connReadBufSize)
    bw := bufio.NewWriterSize(nc, connWriteBufSize)
    // ... use br/bw for all reads/writes
}
```

The write goroutine must call `bw.Flush()` after writing each response (or
after draining a batch of queued responses). Batching multiple responses into
a single `Flush()` reduces write syscalls when the client is pipelining.

### Per-Connection Read Buffer Reuse

**Do not allocate a new `[]byte` for every request frame.** Each connection
keeps a reusable read buffer that grows as needed but is never freed:

```go
type connReader struct {
    br  *bufio.Reader
    buf []byte  // reused across requests, grown with append()
}

func (cr *connReader) readFrame() ([]byte, error) {
    // Read 4-byte size
    var sizeBuf [4]byte
    if _, err := io.ReadFull(cr.br, sizeBuf[:]); err != nil {
        return nil, err
    }
    size := int(binary.BigEndian.Uint32(sizeBuf[:]))

    // Reuse buffer, grow if needed
    if cap(cr.buf) < size {
        cr.buf = make([]byte, size)
    } else {
        cr.buf = cr.buf[:size]
    }
    if _, err := io.ReadFull(cr.br, cr.buf); err != nil {
        return nil, err
    }
    return cr.buf, nil
}
```

This means the returned `[]byte` is only valid until the next `readFrame()`
call. Since the read goroutine spawns handler goroutines and immediately loops
to read the next frame, **all parsing must complete before the handler goroutine
is spawned**. The read goroutine calls `kmsg.ReadFrom(body)` which copies all
fields out of the frame buffer into the typed request struct. After `ReadFrom`,
the frame buffer can be safely reused.

For Produce requests specifically, `kmsg.ReadFrom` copies the `Records []byte`
into the request struct, so the handler goroutine has its own copy. The handler
then copies it again into WAL/storage. Two copies total for Produce; one copy
for all other requests (just the `ReadFrom` parse).

### Response Assembly (Write Goroutine)

The write goroutine receives typed `kmsg.Response` objects and encodes them.
The write goroutine owns encoding because it also owns response ordering
(see "Response Ordering" above).

```go
// Write goroutine encodes and writes each response:
func (cc *clientConn) writeResponse(resp clientResp) error {
    buf := cc.writeBuf[:0]  // reuse per-connection encode buffer

    buf = append(buf, 0, 0, 0, 0)  // size placeholder (4 bytes)
    buf = binary.BigEndian.AppendUint32(buf, uint32(resp.corr))
    if resp.kresp.IsFlexible() && resp.kresp.Key() != 18 {
        buf = append(buf, 0)  // empty tagged fields
    }
    buf = resp.kresp.AppendTo(buf)

    binary.BigEndian.PutUint32(buf[:4], uint32(len(buf)-4))

    cc.bw.Write(buf)
    cc.writeBuf = buf  // keep grown buffer for next response
    return cc.bw.Flush()
}
```

The encode buffer is kept on the connection struct and reused across responses
(reset to `[:0]`, grown with `append`). Combined with the per-connection read
buffer reuse, the hot-path allocations are limited to `RequestForKey()` (one
small struct per request) and `ReadFrom` parsing. This is acceptable — see
`01-foundations.md` "Sensible Defaults".

---

## Wire Framing

### Request Frame

```
[4 bytes] total size (int32, big-endian, excludes these 4 bytes)
[2 bytes] api_key (int16)
[2 bytes] api_version (int16)
[4 bytes] correlation_id (int32)
[2 bytes] client_id length (int16, -1 = null)
[N bytes] client_id (utf-8 string, if length >= 0)
[if flexible version: tagged fields (compact varint length + fields)]
[remaining] request body
```

### Response Frame

```
[4 bytes] total size (int32, big-endian, excludes these 4 bytes)
[4 bytes] correlation_id (int32)
[if flexible version AND api_key != 18: 1 byte 0x00 (empty tags)]
[remaining] response body
```

### Flexible Versions

Starting from certain API versions, Kafka uses "flexible" encoding (compact
strings, tagged fields). The header gains tagged fields. **ApiVersions (key 18)
is special**: even in flexible versions, the response header does NOT include
the empty tag byte. This is a known protocol quirk.

**Request header parsing must skip tagged fields.** After reading the client ID,
flexible-version request headers contain tagged fields that must be consumed
before the request body begins. Call `kmsg.SkipTags(&reader)` to advance past
them. Without this, `kreq.ReadFrom(reader.Src)` will try to parse the tagged
field bytes as part of the request body, causing parse failures.

### Reading a Request

Uses the per-connection `connReader` (see "Per-Connection Read Buffer Reuse"
above). The frame buffer is reused across requests. `RequestForKey` allocates a
typed request struct per call — this is cheap and acceptable (see
`01-foundations.md` "Sensible Defaults").

```go
// Header is parsed in-place from the reused frame buffer.
// Returns a typed kreq (already versioned) and body ready for ReadFrom.
func (cr *connReader) readRequest() (kreq kmsg.Request, corrID int32, clientID *string, body []byte, err error) {
    frame, err := cr.readFrame()  // reuses cr.buf, zero alloc
    if err != nil {
        return
    }
    reader := kbin.Reader{Src: frame}
    apiKey := reader.Int16()
    apiVersion := reader.Int16()
    corrID = reader.Int32()
    clientID = reader.NullableString()
    kreq = kmsg.RequestForKey(apiKey)
    kreq.SetVersion(apiVersion)
    // Flexible-version headers have tagged fields after the client ID.
    // Must skip them before passing reader.Src to kreq.ReadFrom().
    if kreq.IsFlexible() {
        kmsg.SkipTags(&reader)
    }
    body = reader.Src
    return
}
```

### Writing a Response

Handled by the write goroutine (see "Response Assembly" above). The write
goroutine receives typed responses, reorders them by sequence number, encodes
with `AppendTo`, and writes to the buffered writer. Zero allocations per
response in steady state (reuses per-connection encode buffer).

### Determining Flexible Versions

The `readRequest` function above calls `kmsg.RequestForKey(apiKey)`, sets the
version, and checks `kreq.IsFlexible()` to decide whether to skip tagged fields.
This uses kmsg's built-in version metadata — no need to hardcode a lookup table.

---

## Request Dispatch

### Pattern

The read goroutine parses the request fully, then spawns a handler goroutine.
The handler goroutine runs the handler, sends the response to the write
goroutine via `respCh`. The read goroutine never blocks on handler execution.

```go
// Read goroutine — parse and dispatch:
//
// Sequence numbers are assigned HERE in the read loop (arrival order), not in
// handler goroutines (completion order). This guarantees FIFO response ordering
// on the socket. Every request gets a sequence number — including acks=0
// produce requests. The handler goroutine sends a skip marker for acks=0 so
// the write goroutine advances the sequence without writing bytes.
var nextSeq uint32

for {
    // readRequest parses the header (api key, version, correlation ID,
    // client ID) and skips tagged fields for flexible versions.
    // Returns a typed, versioned kreq and body ready for ReadFrom.
    kreq, corrID, _, body, err := cr.readRequest()
    if err != nil { return }

    // Parse request body (copies data out of frame buffer)
    if err := kreq.ReadFrom(body); err != nil {
        cc.respCh <- clientResp{err: fmt.Errorf("parse error")}
        return
    }

    // [Phase 5] SASL gate: if SASL is enabled, check whether this request
    // is allowed given the connection's current auth stage. If not, close
    // the connection. See 20-sasl-authentication.md for the state machine.
    // When SASL is disabled, this check is a no-op.
    //
    // if saslEnabled && !cc.saslAllowed(kreq) {
    //     return // close connection
    // }

    // Assign sequence number in arrival order BEFORE spawning handler
    seq := nextSeq
    nextSeq++

    // Spawn handler goroutine (frame buffer can be reused on next iteration)
    go func(corrID int32, kreq kmsg.Request, seq uint32) {
        resp, err := dispatch(kreq.Key(), kreq)
        // nil response + nil error = no response expected (acks=0 produce).
        // Send a skip marker so the write goroutine advances its sequence
        // counter without writing any bytes to the socket.
        if resp == nil && err == nil {
            cc.respCh <- clientResp{seq: seq, skip: true}
            return
        }
        cc.respCh <- clientResp{kresp: resp, corr: corrID, seq: seq, err: err}
    }(corrID, kreq, seq)
}
```

**Key property:** `kmsg.ReadFrom(body)` copies all fields out of the frame
buffer into the typed request struct. After this call, the frame buffer is safe
to reuse. The handler goroutine works only with the parsed `kreq` — no
reference to the frame buffer.

### Handler Signature

```go
type Handler func(ctx context.Context, req kmsg.Request) kmsg.Response
```

Handlers are registered in a map:

```go
var handlers = map[int16]Handler{
    18: handleApiVersions,
    3:  handleMetadata,
    19: handleCreateTopics,
    0:  handleProduce,
    1:  handleFetch,
    2:  handleListOffsets,
}
```

Unknown API keys: close the connection (matches Kafka behavior). Unsupported
versions: return `UNSUPPORTED_VERSION` error in the response.

### Version Validation

Before dispatching, check that the requested version is within our supported
range. If not, return an error response. For ApiVersions specifically, Kafka
returns the full version list even on unsupported version (so the client can
downgrade).

Reference: `.cache/repos/kafka/core/src/test/scala/unit/kafka/server/ApiVersionsRequestTest.scala`
tests this behavior.

### Version Floor Rationale

We follow Kafka 4.0's version floors (KIP-896), which dropped protocol versions
that no modern client uses. This eliminates dead code paths for ZooKeeper-era
and pre-RecordBatch semantics:

- **Produce v3+, Fetch v4+**: First versions using RecordBatch format. Older
  versions use legacy MessageSets which the broker cannot parse (see Non-Goals
  in `00-overview.md`).
- **ListOffsets v1+**: v0 used a different response schema (multiple offsets
  per partition instead of one).
- **CreateTopics v2+**: v0-1 lack `ValidateOnly` field.
- **DeleteTopics v1+**: v0 lacks per-topic error codes in the response.
- **OffsetCommit v2+**: v0-1 stored offsets in ZooKeeper, not the broker.
- **OffsetFetch v1+**: v0 was ZooKeeper-based.
- **DescribeConfigs v1+**: v0 lacks config synonyms in the response.

**Version ceiling note:** We target Produce v11 and Fetch v16 as initial
ceilings. Higher versions can be added incrementally — the main new fields
(TopicID in Produce v13, Fetch v13+) are backward-compatible additions.

### Explicitly Not Supported

Requests for these API keys return `UNSUPPORTED_VERSION` or cause disconnect:
- Keys 4-7 (inter-broker: LeaderAndIsr, StopReplica, UpdateMetadata, ControlledShutdown)
- Keys 38-41 (delegation tokens)
- Key 27 (WriteTxnMarkers — internal transaction recovery)
- Keys 52-55, 62-64 (KRaft controller)
- Key 67 (ShareGroup)

---

## When Stuck

These references are for understanding correct behavior and gathering context,
not for copying patterns. Find the optimal solution for our architecture.

1. For wire framing details, check how `.cache/repos/franz-go/pkg/kbin/` reads
   and writes Kafka frames -- especially `Reader` and how varints work.
2. For flexible version header handling (tagged fields), search
   `.cache/repos/franz-go/pkg/kmsg/` for how `IsFlexible()` is determined and
   how tagged fields are skipped during parsing.
3. For the ApiVersions header exception (no tag byte in response), check
   `.cache/repos/franz-go/pkg/kfake/18_api_versions.go` and how the response
   is written.
4. For pipelining and response ordering behavior, check how the Java Kafka
   client uses correlation IDs -- search web for "Kafka wire protocol response
   ordering" via Perplexity.
5. See `00b-testing.md` "When Stuck: Escalation Path" for additional tools.

---

## Acceptance Criteria

- TCP connections are accepted on the configured address
- Each connection gets its own read/write goroutines
- Wire frames are correctly read (4-byte size prefix + body)
- Flexible-version request headers have tagged fields skipped
- Responses are written in request order (sequence number reordering works)
- Unknown API keys close the connection
- `SIGTERM` wakes all blocked handlers via `shutdownCh`
- Connection count is tracked; connections beyond limit are rejected
- Smoke tests pass:
  - `TestConnect` -- TCP connect succeeds
  - `TestMetadataInvalidApiKey` -- connection closed on unknown API key
  - `TestMalformedRequest` -- connection closed on truncated frame

### Verify

```bash
# This work unit:
go test ./test/integration/ -run 'TestConnect|TestMetadataInvalidApiKey|TestMalformedRequest' -v -race -count=5

# Regression check (all prior work units):
go test ./... -race -count=5
```

### Commit

After tests pass, stage and commit all changes:

```bash
git add -A
git commit -m "03: TCP listener, wire framing, request dispatch, response ordering"
```
