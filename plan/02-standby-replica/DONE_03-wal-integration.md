# WAL Writer Integration

How the WAL writer's fsync batch loop changes to support synchronous
replication. The goal is minimal disruption to the existing hot path while
adding a replication hook that blocks ACKs until the standby confirms.

Prerequisite: `02-replication-protocol.md` (Sender interface).

Read before starting: `internal/wal/writer.go` (the `run()` loop at line
256, `flushAndSync`, and `appendEntry`), `internal/handler/produce.go`
(the two-phase produce flow: `produceSubmitWAL` → `produceCommitWAL`).

---

## Design Overview

The WAL writer is the single serialization point for all durable writes. It
already batches entries and fsync's once per batch. Replication hooks into
this same batch boundary: after writing the batch to disk, the writer also
sends it to the standby and waits for both local fsync and standby ACK
before signaling handler goroutines.

```
Current flow (single node):
  write entries → fsync → signal done (handlers unblock, ACK to clients)

New flow (with standby):
  write entries → send to standby ─┐
                  fsync local ──────┤
                                    ├─ both complete → signal done
                  standby ACK ──────┘
```

The local fsync and standby send happen in parallel. Latency is
`max(local_fsync, network_rtt + remote_fsync)`.

---

## Replicator Interface

Added to `WriterConfig`. When nil, behavior is identical to today.

```go
// internal/wal/writer.go

type Replicator interface {
    // Replicate sends a batch of serialized WAL entries to the standby.
    // firstSeq and lastSeq identify the sequence range for ACK tracking.
    // Returns a channel that receives nil on standby ACK, or an error
    // on timeout/disconnect.
    Replicate(batch []byte, firstSeq, lastSeq uint64) <-chan error

    // Connected returns true if a standby is currently connected and
    // has completed the HELLO handshake. Used by the WAL writer to
    // decide whether to wait for replication ACK in sync mode.
    Connected() bool
}

type WriterConfig struct {
    // ... existing fields ...

    // Replicator, if non-nil, sends each fsync batch to a standby.
    // The writer waits for the replication ACK before signaling handlers.
    Replicator Replicator

    // Replication is always synchronous: produce fails if standby ACK is
    // not received AND the standby has previously connected (Connected()
    // returned true at least once). If no standby has ever connected,
    // produces succeed with local fsync only (bootstrap exception).
}
```

---

## Changes to `run()`

The `run()` loop (writer.go:256-333) changes at the ACK phase. The write and
fsync phases are unchanged.

### Current Code (simplified)

```go
// Phase 1: write entries to segment
for _, req := range pending {
    w.appendEntry(req.entry)
}

// Phase 2: fsync
w.current.file.Sync()

// Phase 3: signal all waiters
for _, req := range pending {
    req.errCh <- nil
}
```

### New Code (simplified)

```go
// Phase 1: write entries to segment (unchanged)
for _, req := range pending {
    w.appendEntry(req.entry)
}

// Phase 2: fsync local + replicate in parallel
var replCh <-chan error
if w.cfg.Replicator != nil && w.cfg.Replicator.Connected() {
    // Concatenate the serialized entries for the batch
    batch := w.collectBatchBytes(pending)
    firstSeq, lastSeq := w.batchSeqRange(pending)
    replCh = w.cfg.Replicator.Replicate(batch, firstSeq, lastSeq)
}

syncErr := w.current.file.Sync()

// Phase 3: wait for standby ACK (if replicating)
var replErr error
if replCh != nil {
    replErr = <-replCh
}

// Phase 4: signal waiters based on results
if syncErr != nil {
    // Local fsync failed — nothing is durable
    for _, req := range pending {
        req.errCh <- syncErr
    }
} else if replErr != nil {
    // Standby failed — reject writes
    for _, req := range pending {
        req.errCh <- replErr
    }
} else {
    if w.cfg.Replicator != nil && !w.cfg.Replicator.Connected() {
        w.logNoStandbyOnce()
    }
    for _, req := range pending {
        req.errCh <- nil
    }
}
```

### Helper: Collecting Batch Bytes

The entries are already serialized when they arrive in `writeRequest.entry`.
To send them to the standby, concatenate them:

```go
func (w *Writer) collectBatchBytes(pending []writeRequest) []byte {
    var total int
    for _, req := range pending {
        total += len(req.entry)
    }
    buf := make([]byte, 0, total)
    for _, req := range pending {
        buf = append(buf, req.entry...)
    }
    return buf
}
```

### Helper: Sequence Range

Track the first and last WAL sequence in the batch for ACK matching:

```go
func (w *Writer) batchSeqRange(pending []writeRequest) (first, last uint64) {
    // Sequences were assigned by Append/AppendAsync before enqueueing.
    // The WAL entry format is [4B len][4B CRC][8B sequence]...
    // so the sequence is at byte offset 8 in the serialized entry.
    first = parseSequence(pending[0].entry)
    last = parseSequence(pending[len(pending)-1].entry)
    return
}

func parseSequence(entry []byte) uint64 {
    return binary.BigEndian.Uint64(entry[8:16])
}
```

---

## Changes to `flushAndSync()`

The `flushAndSync()` method (called during shutdown) also needs the
replication hook. On shutdown, the primary should attempt to replicate the
final batch, but with a short timeout — if the standby is already gone,
don't block shutdown.

```go
func (w *Writer) flushAndSync(pending []writeRequest) {
    // ... existing write + fsync logic ...

    // Best-effort replicate on shutdown (short timeout)
    if w.cfg.Replicator != nil && written > 0 {
        batch := w.collectBatchBytes(pending[:written])
        firstSeq, lastSeq := w.batchSeqRange(pending[:written])
        replCh := w.cfg.Replicator.Replicate(batch, firstSeq, lastSeq)
        select {
        case <-replCh:
        case <-time.After(2 * time.Second):
            w.logger.Warn("standby replication timed out during shutdown")
        }
    }
}
```

---

## Metadata.log Replication Hook

Metadata.log entries are written by handlers (CreateTopics, OffsetCommit,
etc.) directly to `metadata.Log.Append`/`AppendSync`. These need to be
forwarded to the standby as `META_ENTRY` messages.

Add an optional `replicateHook` callback field to `metadata.Log`. The hook
is called from `appendLocked` after a successful local write (and after
fsync if `doSync=true`), passing the complete framed entry bytes (the
`frame` variable: `[4B length][4B CRC][payload]`). The hook is called
just before the method returns, after all local I/O is committed. The
broker wires this up during primary initialization by setting the hook
to call `sender.SendMeta(frame)`.

Also add a new public method `ReplayEntry(frame []byte) error` to
`metadata.Log`. This is used by the standby to replay a single metadata
frame received over the replication stream. It parses the frame (validates
CRC, extracts type byte), and calls `dispatchEntry` to update in-memory
state. It also writes the raw frame bytes to the local metadata.log file.
This method acquires `mu` internally — safe to call from any goroutine.

**Thread safety:** the hook is called while holding `metadata.Log.mu`. The
hook implementation (`sender.SendMeta`) must NOT block or acquire any lock
that could be held by a goroutine waiting on `metadata.Log.mu`, or a
deadlock results. `SendMeta` just writes to a TCP connection — it does not
wait for ACK and does not acquire `metadata.Log.mu`, so this is safe. But
this constraint must be documented on the hook field.

**SetReplicateHook must be called before any writes.** It is set during
broker initialization (before handlers are registered) and cleared during
demotion (after the Kafka listener is closed and in-flight requests are
drained). It is NOT safe to call concurrently with `Append`/`AppendSync`
— it must only be set when no writes are in flight.

**Hook failure:** if `SendMeta` fails (e.g., TCP write error), it logs a
warning and returns. The local metadata.log append is NOT rolled back.
Metadata is self-healing — the standby will get the full metadata.log on
next SNAPSHOT.

This is fire-and-forget — the metadata.log append does not wait for standby
ACK. The entry rides along in the replication stream and the standby applies
it asynchronously.

### Compaction → SNAPSHOT

Metadata.log compaction runs on the primary (triggered every N appends when
the file exceeds a size threshold). `compactLocked` rewrites the file from
`snapshotFn()` and atomically renames — it does NOT go through
`appendLocked`, so the replicateHook does not fire for compaction.

Instead, after a successful compaction, the primary sends a SNAPSHOT message
containing the freshly compacted metadata.log. The standby replaces its
local metadata.log with the SNAPSHOT contents and replays it. This keeps the
standby's metadata.log in sync without the standby running its own
compaction.

The standby's `snapshotFn` remains set (not nil) — it's just never triggered
because the standby doesn't append to metadata.log via `appendLocked`
(entries arrive via `ReplayEntry`, which writes raw frames directly). The
compaction counter (`appendCount`) is never incremented on the standby, so
`compactLocked` never fires.

**Implementation:** add a `compactHook` callback to `metadata.Log`, called
at the end of `compactLocked` after the rename succeeds. The broker wires
this up to read the new file and send it as a SNAPSHOT via the replication
stream. The hook is called while holding `mu`, so the same constraints as
`replicateHook` apply (must not block).

---

## No Changes to Produce Handler

The produce handler (`internal/handler/produce.go`) is unchanged. It calls
`walWriter.AppendAsync()` and waits on the returned `errCh`. The errCh now
blocks until both local fsync and standby ACK complete (or just local fsync
if no replicator is configured). The handler doesn't know or care about
replication — it just waits for the WAL write to be "done."

This is the key design property: replication is transparent to the handler
layer.

---

## Standby WAL Writer Mode

On the standby, the WAL writer receives entries from the replication stream
rather than from produce handlers. The standby does NOT use `Append` or
`AppendAsync` (which assign new sequence numbers via `nextSeq.Add`).
Instead, a new method is needed: `AppendReplicated(serializedEntry []byte)`.

**`AppendReplicated` behavior:**
- Takes an already-serialized WAL entry (the exact bytes from
  `wal.MarshalEntry`, including the 4-byte length prefix)
- Writes it to the current segment via the same `appendEntry` code path
  (which handles segment rotation, WAL index building, disk usage tracking)
- Does NOT assign a sequence number — the sequence is already embedded
- Does NOT use `writeCh` or the async write pipeline — the receiver calls
  this directly from its goroutine
- Thread safety: the standby's WAL writer is only accessed from the single
  receiver goroutine, so no contention. However, `AppendReplicated` must
  still be safe against concurrent reads from `ReadBatch` (which opens
  segment files independently and doesn't need the writer lock).

**Fsync on standby:** the receiver collects all entries from a WAL_BATCH,
calls `AppendReplicated` for each, then calls `Sync()` on the current
segment file. This is a single fsync per batch, matching the primary's
batching granularity. After fsync, the receiver sends an ACK.

**Sequence tracking:** after each WAL_BATCH, the receiver calls
`walWriter.SetNextSequence(lastSeq + 1)` so that on promotion, new local
writes continue from the correct sequence.

The standby's WAL writer does NOT have a Replicator set (it's a leaf node,
not replicating further).

---

## When Stuck

Use the repo-explorer subagent to check reference implementations:

- **PostgreSQL synchronous replication wait**
  (`https://github.com/postgres/postgres`): examine
  `src/backend/replication/syncrep.c` — specifically `SyncRepWaitForLSN`
  which is the function that blocks a backend (equivalent to our handler
  goroutine) until the standby confirms the WAL has been flushed. This is
  the direct analog of our "wait for both local fsync and standby ACK"
  pattern. Pay attention to how it handles timeouts and cancellation.

- **PostgreSQL WAL writer fsync batching**
  (`https://github.com/postgres/postgres`): examine
  `src/backend/access/transam/xlog.c` — search for `XLogFlush` and
  `XLogWrite` to see how Postgres batches WAL writes and fsync, and how
  synchronous replication interacts with the fsync path.

---

## Acceptance Criteria

WAL writer with replicator:
- Produce blocks until both local fsync and standby ACK
- Produce succeeds with local fsync only when replicator is nil
- Sync mode: produce fails if standby ACK times out (and standby was
  previously connected)
- Sync mode: produce succeeds if no standby has ever connected (async
  until first connection, warning logged)
- Async mode: produce succeeds if standby ACK times out (warning logged)
- Shutdown flushAndSync attempts replication with short timeout
- Batch bytes are correctly collected and sequence range extracted
- `parseSequence` reads bytes 8..16 from the serialized entry
- Local fsync and standby send happen in parallel (latency is max of both,
  not sum)

Standby WAL writer (`AppendReplicated`):
- Writes already-serialized entries to segment without reassigning sequence
- Builds WAL index entries from replicated entries
- Updates `nextSeq` after each batch so promotion continues correctly
- Handles segment rotation normally when segment fills up

Metadata.log hook:
- Metadata entries are forwarded via replicateHook after successful local
  write (and after fsync if AppendSync)
- Hook is fire-and-forget (append does not block on standby)
- Hook is not called when nil (single-node mode)
- Hook failure does not affect local append success
- After compaction, primary sends SNAPSHOT with the compacted metadata.log
  via compactHook
- compactHook is called after successful rename, while holding mu

Metadata.log ReplayEntry:
- `ReplayEntry(frame)` parses frame, validates CRC, dispatches entry
- Updates in-memory state (same as Replay but for a single frame)
- Writes frame bytes to local metadata.log file
- Thread-safe (acquires mu internally)

---

## Unit Tests

### WAL Writer with Replicator (`internal/wal/`)

Uses a mock `Replicator` that returns channels the test controls.

- `TestWriterReplicatorNil` — no replicator set, produce succeeds after
  local fsync only. Verify existing behavior is unchanged.
- `TestWriterSyncModeACK` — set replicator. Produce a record. Verify the
  errCh does NOT resolve until both local fsync and replicator channel
  resolve. Resolve replicator channel with nil. Verify errCh gets nil.
- `TestWriterSyncModeTimeout` — set replicator. Produce. Never resolve the
  replicator channel. Verify errCh receives an error (from timeout).
- `TestWriterAsyncModeTimeout` — set replicator with mode=async. Produce.
  Never resolve replicator channel. Verify errCh receives nil (local fsync
  succeeded). Verify a warning is logged.
- `TestWriterReplicatorBatchBytes` — produce 3 entries in one batch. Verify
  the bytes passed to Replicator.Replicate are the concatenation of the 3
  serialized entries. Verify firstSeq and lastSeq match.
- `TestWriterReplicatorLocalFsyncFails` — mock local fsync to fail. Verify
  errCh receives the fsync error even if replicator succeeds.
- `TestWriterSyncModeNoStandbyConnected` — set replicator with mode=sync,
  but Connected() returns false (no standby has ever connected). Produce.
  Verify errCh receives nil (async fallback). Verify warning logged.
- `TestWriterSyncModeStandbyWasConnected` — set replicator with mode=sync.
  Connected() returns true, then standby disconnects (Replicate returns
  error). Verify errCh receives error (sync enforcement active after first
  connection).
- `TestWriterShutdownReplicationTimeout` — produce entries, set replicator
  that never resolves. Stop the writer. Verify flushAndSync times out after
  2s and does not block shutdown indefinitely.

### WAL Writer: AppendReplicated (`internal/wal/`)

- `TestAppendReplicated` — call AppendReplicated with a pre-serialized
  entry. Verify it's written to the segment file. Verify the WAL index
  contains the entry with the correct sequence number (from the entry, not
  auto-assigned).
- `TestAppendReplicatedSegmentRotation` — call AppendReplicated enough times
  to exceed SegmentMaxBytes. Verify segment rotation occurs normally.
- `TestAppendReplicatedDuplicateSkip` — set nextSequence to 100. Call
  AppendReplicated with an entry at sequence=50. Verify it's skipped
  (not written). Call with sequence=100. Verify it's written.

### Metadata.log Replicate Hook (`internal/metadata/`)

- `TestMetadataLogReplicateHook` — set a hook. Append an entry. Verify
  the hook is called with the complete framed bytes (4B length + 4B CRC +
  payload).
- `TestMetadataLogReplicateHookFailure` — set a hook that returns an error
  (or panics — it shouldn't, but test that the append still succeeds).
  Verify the local append is not rolled back.
- `TestMetadataLogCompactHook` — set a compactHook. Append enough entries
  to trigger compaction. Verify compactHook is called after compaction
  completes. Verify the hook receives the compacted file contents.
- `TestMetadataLogCompactHookNotCalledOnStandby` — use ReplayEntry (not
  Append) to add entries. Verify compaction never triggers (appendCount
  is not incremented by ReplayEntry).
- `TestMetadataLogCompactHookWithReplicateHook` — set both replicateHook
  and compactHook. Append enough entries to trigger compaction. Verify
  replicateHook was called for every append (count matches). Verify
  compactHook was called exactly once. Verify replicateHook was NOT called
  during the compaction itself (compaction writes go to a tmp file and
  rename — they don't go through appendLocked).

### Metadata.log ReplayEntry (`internal/metadata/`)

- `TestMetadataLogReplayEntry` — call ReplayEntry with a valid framed
  CreateTopic entry. Verify it's written to the local file and dispatched
  (topic appears in state).
- `TestMetadataLogReplayEntryCRCMismatch` — call ReplayEntry with corrupted
  CRC. Verify it returns an error. Verify nothing is written to the file.
- `TestMetadataLogReplayEntryThreadSafe` — call ReplayEntry from multiple
  goroutines concurrently. Verify no data corruption. Use `-race`.

### Verify

```bash
go test ./internal/wal/ -v -race
go test ./internal/metadata/ -v -race
```
