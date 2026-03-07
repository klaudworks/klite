# Replication Protocol

Binary protocol over mTLS TCP for streaming WAL entries and metadata.log
entries from primary to standby. Not Kafka wire protocol — this is internal.

Prerequisite: none (self-contained package, can be built in parallel with
the lease work unit).

Read before starting: `00-overview.md`, the WAL entry format in
`plan/01-initial/DONE_15-wal-format-and-writer.md`, and the metadata.log
entry format in `plan/01-initial/DONE_16-metadata-log.md`.

---

## Design Overview

The replication stream is a single TCP connection from the standby to the
primary. The standby initiates the connection (it knows the primary's
replication address). The primary accepts one standby connection at a time.

Data flows primarily one direction: primary → standby. The only standby →
primary messages are the initial handshake and fsync ACKs.

```
Standby                          Primary
   │                                │
   │──── HELLO (lastWALSeq) ──────>│
   │                                │
   │<─── SNAPSHOT (metadata.log) ──│  (if standby needs catch-up)
   │<─── WAL_BATCH ────────────────│
   │──── ACK (walSeq) ────────────>│
   │<─── WAL_BATCH ────────────────│
   │<─── META_ENTRY ───────────────│  (fire-and-forget, no ACK)
   │──── ACK (walSeq) ────────────>│
   │     ...                        │
```

---

## Message Framing

Every message on the replication stream:

```
[1 byte]   message type
[4 bytes]  payload length (uint32, big-endian, NOT including these 5 header bytes)
[N bytes]  payload
```

Total overhead per message: 5 bytes. Maximum payload size: 256 MiB (same
sanity limit as `wal.ScanFramedEntries`). The receiver must reject messages
with `payload length > 256 MiB` and close the connection.

### Message Types

| Type | Value | Direction | Description |
|------|-------|-----------|-------------|
| `HELLO` | `0x01` | standby → primary | Handshake with last WAL sequence |
| `SNAPSHOT` | `0x02` | primary → standby | Full metadata.log for initial sync |
| `WAL_BATCH` | `0x03` | primary → standby | One fsync batch of WAL entries |
| `META_ENTRY` | `0x04` | primary → standby | One metadata.log entry |
| `ACK` | `0x05` | standby → primary | Fsync confirmation with WAL sequence |

The framing layer (encode/decode) is a standalone pair of functions that
operate on an `io.Writer`/`io.Reader`. No dependency on WAL or metadata
packages — just message type byte + opaque payload bytes. The framing layer
is unit-tested independently of any message payload semantics.

---

## Message Payloads

### HELLO (standby → primary)

Sent once after the TCP connection is established and mTLS handshake completes.

```
[8 bytes]  last_wal_sequence (uint64)
    The highest WAL sequence the standby has fsync'd.
    0 = fresh standby, needs full snapshot.
[8 bytes]  epoch (uint64)
    The lease epoch the standby last operated under. The primary compares
    this with its own epoch. If they differ, the standby's WAL may contain
    entries from a different timeline — the primary forces a full SNAPSHOT
    regardless of last_wal_sequence.
    0 = unknown epoch (fresh standby), treated as epoch mismatch.
```

### SNAPSHOT (primary → standby)

Sent in two situations:
1. **Initial sync / epoch mismatch:** the standby's `last_wal_sequence` is
   0, the primary has already trimmed WAL segments beyond the standby's
   position, or the epoch doesn't match.
2. **After metadata.log compaction:** the primary compacts its metadata.log
   and sends the fresh file so the standby stays in sync (see
   `03-wal-integration.md` "Compaction → SNAPSHOT").

```
[8 bytes]  wal_sequence_after (uint64)
    The WAL sequence that live streaming will begin from after this snapshot.
    For compaction-triggered SNAPSHOTs during live streaming, this is the
    current WAL sequence (streaming continues uninterrupted).
[4 bytes]  metadata_log_length (uint32)
[N bytes]  metadata_log_contents
    Complete metadata.log file contents. The standby writes this to its own
    metadata.log, replacing any existing file.
```

After receiving SNAPSHOT, the standby replays the metadata.log to rebuild
topic state. For initial sync, it then waits for live WAL_BATCH messages
starting from `wal_sequence_after`. For compaction-triggered SNAPSHOTs,
WAL streaming is already in progress and continues normally.

### WAL_BATCH (primary → standby)

One fsync batch from the WAL writer. Contains one or more serialized WAL
entries in the exact on-disk format (length-prefixed, CRC'd).

```
[8 bytes]  first_wal_sequence (uint64)
[8 bytes]  last_wal_sequence (uint64)
[4 bytes]  entry_count (uint32)
[N bytes]  entries
    Concatenated serialized WAL entries. Each entry is already framed
    with its own [4B length][4B CRC][payload] envelope (same as on disk).
```

The entries bytes are exactly what `wal.MarshalEntry` produces, concatenated
back-to-back. No transcoding, no re-serialization.

**How the standby splits the entries:** the receiver uses the existing
`wal.ScanFramedEntries` function (or its equivalent logic: read the 4-byte
length prefix, read that many bytes) to iterate individual entries from the
concatenated `entries` blob. It wraps the entries bytes in a
`bytes.Reader` and calls `ScanFramedEntries` on it. For each entry:
1. Validate CRC via `wal.UnmarshalEntry`
2. Call `walWriter.Append` (which assigns no new sequence — the sequence is
   already embedded in the entry, see "Standby WAL Writer Mode" in
   `03-wal-integration.md`)
3. Build the WAL index entry and append to the partition's chunk pool

After all entries in the batch are written and fsync'd, the standby sends
an ACK with `last_wal_sequence`.

**Empty WAL_BATCH (keepalive):** `entry_count=0`, `first_wal_sequence=0`,
`last_wal_sequence=0`, zero entry bytes. The standby ignores it (no ACK
sent for keepalives). This is purely for TCP liveness detection.

### META_ENTRY (primary → standby)

One metadata.log entry, fire-and-forget (no ACK expected).

```
[N bytes]  framed metadata entry
    The complete on-disk frame: [4B length][4B CRC][payload].
    This is exactly the bytes that `metadata.Log.appendLocked` writes to
    the file — the `frame` variable constructed there. The primary's
    replicateHook receives these bytes and the Sender wraps them in a
    META_ENTRY message (5-byte repl header + the metadata frame).
```

The standby receives the metadata frame bytes and:
1. Writes them directly to its local metadata.log file (raw append, since
   the frame is already [4B length][4B CRC][payload])
2. Dispatches the entry via a new public `metadata.Log.ReplayEntry(frame
   []byte) error` method, which parses the frame (CRC check + type byte
   dispatch), updating in-memory state (topic creation, offset commits,
   etc.). This is the same logic as `Replay()` but for a single frame.
   `dispatchEntry` is unexported — `ReplayEntry` is the public entry point.

**Ordering:** metadata entries are interleaved with WAL_BATCH messages in
the stream. They are not ordered relative to WAL entries. This is acceptable
because metadata is self-healing (see `00-overview.md`).

### ACK (standby → primary)

Sent after the standby fsync's a WAL_BATCH to local disk.

```
[8 bytes]  wal_sequence (uint64)
    The highest WAL sequence that is now durable on the standby.
```

The primary uses this to unblock handler goroutines waiting for replication
confirmation.

---

## Primary Side: Sender

**Package: `internal/repl/`**

```go
type Sender struct {
    conn    net.Conn
    writeMu sync.Mutex             // serializes all conn.Write calls
    mu      sync.Mutex             // protects pending map
    pending map[uint64]chan<- error // WAL seq → waiting handler
    logger  *slog.Logger
}

// Send transmits a WAL batch to the standby. Returns a channel that
// receives nil when the standby ACKs, or an error on timeout/disconnect.
// This is called by the WAL writer after writing locally but before
// signaling handler goroutines.
func (s *Sender) Send(batch []byte, firstSeq, lastSeq uint64) <-chan error

// SendMeta transmits a metadata.log entry. Fire-and-forget, no ACK.
func (s *Sender) SendMeta(entry []byte)
```

The sender runs a read goroutine that processes incoming ACK messages and
resolves the corresponding channels in `pending`.

**Critical: ACK reader goroutine cleanup.** When the read goroutine exits
(whether from read error, connection close, or context cancellation), it
must drain all entries from `pending` by sending an error to each channel,
then clear the map. This prevents goroutine leaks in handler goroutines
waiting on replication ACKs. The cleanup runs under `s.mu` and looks like:
lock, iterate all entries in `pending`, send `errDisconnected` to each
channel, set `pending` to nil, unlock. This must be a deferred call at the
top of the read goroutine so it runs on every exit path.

**ACK matching:** when the standby sends `ACK(walSeq=N)`, the primary
resolves ALL pending entries with `lastSeq <= N`, not just the one matching
exactly. This handles the case where multiple batches are in flight and the
standby coalesces them into a single fsync + ACK.

### ACK Timeout

If no ACK is received within `--replication-ack-timeout` (default 5s),
the corresponding channel receives an error. The WAL writer propagates the
error to handler goroutines (produce fails).

The timeout is per-batch, started when `Send` is called. Use `time.AfterFunc`
or a separate timer goroutine — do NOT block the ACK reader goroutine with
timeouts.

### TCP Write Deadline

Both `Send` and `SendMeta` must set a TCP write deadline
(`conn.SetWriteDeadline`) before each `Write()` call. Without this, a stuck
TCP connection (standby kernel alive but process dead, or full buffer with
no reader) would block the writer goroutine indefinitely. Use the ACK timeout
value (`--replication-ack-timeout`, default 5s) as the write deadline. If the
write deadline fires, the write returns an error and the connection should be
closed (triggering ACK reader cleanup and pending channel drain).

### Backpressure

If the TCP send buffer fills up (standby is slow), `Send` blocks (up to the
write deadline). This naturally backpressures the WAL writer, which
backpressures produce handlers. The write deadline and
`--replication-ack-timeout` act as the safety valve.

### Write Serialization

Both `Send` and `SendMeta` write to the same TCP connection. They acquire
`writeMu` before writing to prevent interleaved bytes. `writeMu` is separate
from `mu` (which protects the pending map) to avoid holding two locks.
`SendMeta` acquires `writeMu`, writes the frame, releases — it never touches
`mu`. `Send` acquires `writeMu` for the write, then acquires `mu` to register
the pending channel.

### SendMeta Thread Safety

`SendMeta` is called from metadata.Log's `appendLocked` (which holds
`metadata.Log.mu`). `SendMeta` must not block — it writes to the TCP
connection but does not wait for any ACK. If the write fails, it logs
a warning and returns. It must NOT panic or close the connection — the
ACK reader goroutine owns connection lifecycle.

---

## Standby Side: Receiver

```go
type Receiver struct {
    conn       net.Conn
    walWriter  *wal.Writer
    metaLog    *metadata.Log
    logger     *slog.Logger
}

// Run connects to the primary, sends HELLO, receives the stream, and
// replays entries into the local WAL and metadata.log. Blocks until
// ctx is cancelled or the connection drops.
func (r *Receiver) Run(ctx context.Context, primaryAddr string, tlsConfig *tls.Config) error
```

The receiver loop:
1. Connect to primary with mTLS
2. Send HELLO with last WAL sequence from local WAL
3. If SNAPSHOT received (initial sync): write metadata.log, replay it,
   truncate WAL (discard all segments and clear WAL index — the standby's
   WAL may contain entries from a different timeline), set WAL watermark
   to `wal_sequence_after`
3b. If SNAPSHOT received (mid-stream compaction): replace metadata.log,
   replay it. WAL is not affected — streaming continues.
4. For each WAL_BATCH: write entries to local WAL, fsync, send ACK
5. For each META_ENTRY: append to local metadata.log
6. On disconnect: return error (broker handles reconnection)

### WAL Replay on Standby

The standby's WAL writer operates in a special "replay" mode: entries come
from the replication stream rather than from produce handlers. The standby
does NOT use `wal.Writer.Append` (which assigns new sequence numbers via
`nextSeq.Add`). Instead, it needs a new method on `wal.Writer` — something
like `AppendReplicated(serializedEntry []byte)` — that writes the
already-serialized entry directly to the segment file without reassigning
the sequence number. The sequence is already embedded in the entry by the
primary.

**Index building:** the `appendEntry` method (writer.go:335) already parses
the entry and builds the WAL index + updates `segmentInfo.maxSeq`. The
standby reuses this same code path. The only difference is that sequence
numbers come from the primary, not from a local atomic counter.

**Sequence tracking:** after processing each WAL_BATCH, the standby updates
`walWriter.SetNextSequence(lastSeq + 1)` so that on promotion, new local
writes continue from the correct sequence.

**Chunk pool building:** the standby builds chunk
pool data as entries arrive. The receiver needs access to `cluster.State`
(not just `wal.Writer`), and calls a shared `broker.applyWALEntry(entry
wal.Entry, segmentSeq uint64, fileOffset int64) error` helper that
encapsulates: topic lookup by ID, partition validation, batch header parsing,
spare chunk acquisition, chunk append, HW advance, and WAL index building.
This is the same logic currently in `broker.replayWAL`'s callback — it
should be factored out into a standalone method so both `replayWAL` (startup)
and the receiver (live replication) share the same code path. On promotion,
the standby is immediately ready to serve fetch requests from memory — no
replay delay.

---

## Initial Sync and Reconnect

The primary does NOT implement WAL catch-up replay. The handshake is simple:

1. Standby sends `HELLO` with `last_wal_sequence` and `epoch`
2. Primary decides:
   - **Epoch mismatch** (including epoch=0 from fresh standby): send
     `SNAPSHOT` with current metadata.log + current WAL sequence, then
     stream live entries from that point
   - **Epoch match**: send no SNAPSHOT, just stream live WAL_BATCH from
     current writes onward
3. Primary begins streaming live WAL_BATCH messages from current writes

**No historical WAL replay.** The primary never reads old WAL segments to
send to the standby. The standby already has its own local WAL and
metadata.log from its normal startup sequence (steps 3-6 in the broker
startup, same as single-node). On reconnect after a brief disconnect, the
standby simply resumes receiving live entries. Any entries it missed during
the disconnect are already on the primary's disk and in S3 (flushed within
~60s). After promotion, the standby serves old data from S3 and recent
data from its local WAL — just like the primary does today.

**Duplicate handling:** when the primary starts streaming live entries, the
standby may receive entries it already has (if the disconnect was brief).
`AppendReplicated` skips entries whose sequence is <= the standby's current
`nextSequence - 1`. This is cheap (one comparison per entry) and avoids
any need for precise synchronization of the start point.

**SNAPSHOT during initial handshake is only sent on epoch mismatch.** This
covers: fresh standby (first ever connection), standby connecting after a
different primary held the lease (epoch changed), or any case where the
standby's WAL timeline may be inconsistent with the current primary.
SNAPSHOT replaces the standby's metadata.log and sets the WAL watermark for
live streaming. SNAPSHOT is also sent during live streaming after the
primary compacts its metadata.log (see `03-wal-integration.md` "Compaction
→ SNAPSHOT").

This design eliminates the need for a WAL scan-from-sequence API.

**Why the lack of historical replay doesn't cause gaps.** In sync mode
(the default), the primary fails produces while the standby is disconnected
(once the standby has connected at least once). No new entries are written
during the disconnect, so there is nothing to miss — the standby has
everything up to the point of disconnection. In async mode, the primary
does continue writing, so the standby would have a gap. But async mode
already accepts reduced durability as a trade-off; the missing entries are
on the primary's disk and in S3 (flushed within ~60s), and after promotion
the new primary serves them from S3.

---

## Connection Management

- The primary listens on `--replication-addr` (default `:9093`)
- Only one standby connection at a time. If a second connection arrives,
  the primary rejects it (closes immediately)
- The standby reconnects on disconnect with exponential backoff
  (1s, 2s, 4s, 8s, capped at 30s)
- Idle keepalive: the primary sends an empty WAL_BATCH (entry_count=0)
  every 5s if there's no traffic, to detect dead connections via TCP
  keepalive / write errors

---

## When Stuck

Use the repo-explorer subagent to check reference implementations:

- **PostgreSQL WAL sender** (`https://github.com/postgres/postgres`):
  examine `src/backend/replication/walsender.c` for how Postgres streams WAL
  to standbys. Pay attention to `WalSndLoop`, `XLogSendPhysical` (how it
  reads WAL and sends), and the feedback/ACK handling. Also look at
  `walreceiver.c` for the standby side — `WalReceiverMain`, how it writes
  received WAL, and how it sends flush position feedback.

- **PostgreSQL catch-up logic** (`https://github.com/postgres/postgres`):
  examine `src/backend/replication/walsender.c` search for `startpoint` to
  see how the WAL sender determines where to start streaming based on the
  standby's reported position.

- **Litestream WAL streaming** (`https://github.com/benbjohnson/litestream`):
  examine `db.go` and `replica.go` for how WAL frames are streamed to S3
  replicas, how the shadow WAL works, and how generation boundaries are
  detected.

---

## Acceptance Criteria

Protocol:
- Message framing encode/decode round-trips correctly for all 5 types
- Framing rejects payloads > 256 MiB
- HELLO (with epoch) → SNAPSHOT → WAL_BATCH → ACK flow works over
  `net.Pipe()`
- HELLO with matching epoch → no SNAPSHOT, live WAL_BATCH → ACK works
- META_ENTRY messages interleave with WAL_BATCH correctly
- ACK(N) resolves ALL pending entries with `lastSeq <= N`
- Empty WAL_BATCH (keepalive) is ignored by receiver (no ACK sent)

Sender:
- `Send` returns channel that receives nil on ACK
- `Send` returns error on ACK timeout
- `Send` sets TCP write deadline before writing
- `SendMeta` does not block on ACK
- `SendMeta` write failure does not crash or close the connection
- `Send` and `SendMeta` serialize writes via `writeMu` (no interleaving)
- Backpressure: slow standby blocks sender (up to write deadline)
- ACK reader goroutine exit drains all pending channels with error
- No pending channels are leaked after Sender.Close()

Receiver:
- Processes SNAPSHOT: replaces local metadata.log, replays it
- Processes SNAPSHOT mid-stream (compaction): replaces metadata.log,
  replays it, continues receiving WAL_BATCH without interruption
- Processes WAL_BATCH: splits entries via length-prefix scanning, writes
  each to local WAL (preserving primary's sequence numbers), builds WAL
  index and chunk data via `applyWALEntry`, fsync's, sends ACK
- Sends ACK after fsync with the batch's `last_wal_sequence`
- Processes META_ENTRY: appends framed bytes to local metadata.log file,
  dispatches entry via `ReplayEntry` for in-memory state update
- Handles reconnection: sends correct `last_wal_sequence` and epoch in
  new HELLO
- Skips unknown message types with a warning (does not crash)

---

## Unit Tests

### Replication Protocol Framing (`internal/repl/`)

Uses `bytes.Buffer` or `net.Pipe()` for in-memory I/O. No real TCP.

- `TestFrameRoundTrip` — encode each message type, decode it back, verify
  type byte and payload match exactly
- `TestFrameMaxPayload` — encoding a payload at the 256 MiB limit succeeds;
  decoding a payload that claims >256 MiB returns an error
- `TestFrameTruncated` — decoding from a reader that ends mid-payload
  returns an error (not a panic)

### Sender (`internal/repl/`)

Uses `net.Pipe()`. Test controls the other side of the pipe.

- `TestSenderSendAndACK` — send a batch, inject ACK from the other side,
  verify the returned channel receives nil
- `TestSenderACKTimeout` — send a batch, don't inject ACK, verify the
  channel receives an error after the configured timeout
- `TestSenderACKCoalesced` — send batches with lastSeq=5 and lastSeq=10.
  Inject a single ACK with walSeq=10. Verify BOTH pending channels resolve
  with nil (coalesced ACK matching).
- `TestSenderSendMeta` — call SendMeta, verify the META_ENTRY frame is
  written to the pipe. Verify SendMeta returns immediately (no ACK wait).
- `TestSenderSendMetaWriteError` — close the pipe, call SendMeta. Verify
  it does not panic, does not close the connection, just logs.
- `TestSenderDisconnect` — close the pipe (simulating standby crash). Verify
  all pending channels receive an error. Verify no goroutine leak.
- `TestSenderConcurrentSendAndSendMeta` — call Send and SendMeta
  concurrently from multiple goroutines. Verify no interleaved bytes on
  the wire (each message frame is intact). Use `-race`.
- `TestSenderWriteDeadline` — set a short write deadline. Block the reader
  so TCP buffer fills. Verify Send returns an error (write deadline
  exceeded, not hung forever). Verify all pending channels drain with error.
- `TestSenderConnected` — verify Connected() returns false before any
  standby connects, true after HELLO handshake, false after disconnect.

### Receiver (`internal/repl/`)

Uses `net.Pipe()`. Test injects messages from the primary side.

- `TestReceiverHelloSendsSequenceAndEpoch` — start receiver. Verify it sends
  a HELLO with the correct `last_wal_sequence` and `epoch` from the WAL
  writer / lease state.
- `TestReceiverSnapshot` — inject SNAPSHOT message with metadata.log bytes.
  Verify the local metadata.log file is replaced with the snapshot contents.
  Verify the receiver replays the snapshot (e.g., topic appears in state).
- `TestReceiverWALBatch` — inject a WAL_BATCH with 3 entries. Verify all 3
  are written to the standby's WAL via `AppendReplicated`. Verify an ACK is
  sent back with the correct `last_wal_sequence`. Verify WAL index is built.
- `TestReceiverWALBatchDuplicateSkip` — inject a WAL_BATCH with entries the
  standby already has (sequence <= nextSequence-1). Verify duplicates are
  skipped (not written again). Verify ACK is still sent.
- `TestReceiverMetaEntry` — inject a META_ENTRY (e.g., CreateTopic). Verify
  it's appended to the local metadata.log file and dispatched via
  `ReplayEntry` (topic appears in state).
- `TestReceiverKeepalive` — inject an empty WAL_BATCH (entry_count=0).
  Verify no ACK is sent, no crash, no state change.
- `TestReceiverUnknownMessageType` — inject a message with type byte 0xFF.
  Verify receiver logs a warning and continues (does not crash or disconnect).
- `TestReceiverDisconnect` — close the pipe mid-stream. Verify Run()
  returns an error (broker handles reconnection).
- `TestReceiverReconnect` — start receiver, inject 50 entries, disconnect.
  Reconnect with a new pipe. Verify the new HELLO contains the correct
  last_wal_sequence and epoch from the first session.
- `TestReceiverSnapshotThenImmediateWALBatch` — inject a SNAPSHOT followed
  immediately by multiple WAL_BATCH messages on the pipe (no pause between
  writes — all queued in the TCP buffer before the receiver reads). Verify
  the receiver processes the SNAPSHOT first (metadata.log replaced, state
  rebuilt), then processes each WAL_BATCH in order (entries written, ACKs
  sent). Verify all entries are indexed correctly and nextSequence reflects
  the last batch.

### Replication Address Resolution (`internal/broker/`)

- `TestResolveReplAddrExplicit` — set `--replication-advertised-addr
  10.0.1.5:9093`. Verify `resolveReplicationAddr()` returns
  `10.0.1.5:9093` regardless of listen address.
- `TestResolveReplAddrSpecificBind` — set `--replication-addr
  10.0.1.5:9093`. Verify `resolveReplicationAddr()` returns
  `10.0.1.5:9093`.
- `TestResolveReplAddrWildcard` — set `--replication-addr :9093` (and
  `0.0.0.0:9093`). Verify `resolveReplicationAddr()` returns
  `<hostname>:9093` where hostname is `os.Hostname()`. Verify a warning
  is logged.

### Verify

```bash
go test ./internal/repl/ -v -race
go test ./internal/broker/ -run TestResolveReplAddr -v -race
```
