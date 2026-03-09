package metadata

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/klaudworks/klite/internal/wal"
)

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

const (
	// compactionThreshold is the file size threshold for triggering compaction (64 MiB).
	compactionThreshold = 64 * 1024 * 1024

	// compactionCheckInterval is how many appends between compaction checks.
	compactionCheckInterval = 1000
)

// Log manages the metadata.log file. It provides append-only writes with
// optional synchronous fsync, startup replay, and compaction.
//
// Thread safety: Append and AppendSync are safe to call from multiple goroutines.
// The internal mutex serializes writes. Compaction acquires a write lock that
// blocks incoming writes briefly.
type Log struct {
	mu     sync.Mutex
	file   *os.File
	path   string
	dir    string
	size   int64
	logger *slog.Logger

	// Compaction state
	appendCount atomic.Int64

	// For replay callbacks
	topicCallback                 func(CreateTopicEntry)
	deleteTopicCallback           func(DeleteTopicEntry)
	alterConfigCallback           func(AlterConfigEntry)
	offsetCommitCallback          func(OffsetCommitEntry)
	producerIDCallback            func(ProducerIDEntry)
	logStartCallback              func(LogStartOffsetEntry)
	scramCredentialCallback       func(ScramCredentialEntry)
	scramCredentialDeleteCallback func(ScramCredentialDeleteEntry)
	compactionWatermarkCallback   func(CompactionWatermarkEntry)

	// Snapshot provider for compaction (set by broker during init)
	snapshotFn func() [][]byte

	// replicateHook is called from appendLocked after a successful local
	// write, passing the complete framed entry bytes ([4B length][4B CRC][payload]).
	// Must not block or acquire metadata.Log.mu. Set before any writes,
	// cleared after writes are drained. Fire-and-forget.
	replicateHook func(frame []byte)

	// compactHook is called at the end of compactLocked after the rename
	// succeeds. Receives the compacted file contents. Must not block.
	compactHook func(data []byte)
}

type LogConfig struct {
	DataDir string
	Logger  *slog.Logger
}

func NewLog(cfg LogConfig) (*Log, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	path := filepath.Join(cfg.DataDir, "metadata.log")

	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open metadata.log: %w", err)
	}

	stat, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("stat metadata.log: %w", err)
	}

	return &Log{
		file:   f,
		path:   path,
		dir:    cfg.DataDir,
		size:   stat.Size(),
		logger: cfg.Logger,
	}, nil
}

func (l *Log) SetSnapshotFn(fn func() [][]byte) {
	l.snapshotFn = fn
}

// SetReplicateHook sets the replication hook called after each successful
// local append. Safe for concurrent use.
func (l *Log) SetReplicateHook(fn func(frame []byte)) {
	l.mu.Lock()
	l.replicateHook = fn
	l.mu.Unlock()
}

// SetCompactHook sets the compaction hook called after a successful compaction
// rename. The hook receives the compacted file contents. Must not block.
func (l *Log) SetCompactHook(fn func(data []byte)) {
	l.mu.Lock()
	l.compactHook = fn
	l.mu.Unlock()
}

// Append writes a serialized entry (buffered, no fsync).
func (l *Log) Append(entryPayload []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.appendLocked(entryPayload, false)
}

// AppendSync writes a serialized entry and fsyncs.
func (l *Log) AppendSync(entryPayload []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.appendLocked(entryPayload, true)
}

// Caller must hold l.mu.
// Frame: [4B length][4B CRC32c][payload]
func (l *Log) appendLocked(entryPayload []byte, doSync bool) error {
	frameSize := 4 + 4 + len(entryPayload)
	frame := make([]byte, frameSize)

	binary.BigEndian.PutUint32(frame[0:4], uint32(4+len(entryPayload)))
	binary.BigEndian.PutUint32(frame[4:8], crc32.Checksum(entryPayload, crc32cTable))
	copy(frame[8:], entryPayload)

	n, err := l.file.Write(frame)
	if err != nil {
		return fmt.Errorf("write metadata entry: %w", err)
	}
	l.size += int64(n)

	if doSync {
		if err := l.file.Sync(); err != nil {
			return fmt.Errorf("fsync metadata.log: %w", err)
		}
	}

	if l.replicateHook != nil {
		l.replicateHook(frame)
	}

	count := l.appendCount.Add(1)
	if count%compactionCheckInterval == 0 && l.size > compactionThreshold {
		// Compaction runs with the lock held — it's brief (~1ms for typical state)
		l.compactLocked()
	}

	return nil
}

// Replay reads all entries and calls the appropriate callback for each.
func (l *Log) Replay() (int, error) {
	return l.replayLocked()
}

// replayLocked is the core replay implementation. It opens its own file
// handle for reading and does not access l.mu-protected fields, so it is
// safe to call both with and without l.mu held.
func (l *Log) replayLocked() (int, error) {
	f, err := os.Open(l.path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("open metadata.log for replay: %w", err)
	}
	defer f.Close() //nolint:errcheck // best-effort close

	count := 0
	_, scanErr := wal.ScanFramedEntries(f, func(payload []byte) bool {
		if len(payload) < 4+1 {
			return false
		}

		storedCRC := binary.BigEndian.Uint32(payload[0:4])
		actualCRC := crc32.Checksum(payload[4:], crc32cTable)
		if storedCRC != actualCRC {
			l.logger.Warn("metadata.log CRC mismatch, stopping replay", "entry", count)
			return false
		}

		entryType := payload[4]
		entryData := payload[5:]

		if err := l.dispatchEntry(entryType, entryData); err != nil {
			l.logger.Warn("metadata.log entry parse error, stopping replay",
				"entry", count, "type", entryType, "err", err)
			return false
		}
		count++
		return true
	})

	if scanErr != nil {
		return count, fmt.Errorf("scan metadata.log: %w", scanErr)
	}

	return count, nil
}

func (l *Log) dispatchEntry(entryType byte, data []byte) error {
	switch entryType {
	case EntryCreateTopic:
		e, err := UnmarshalCreateTopic(data)
		if err != nil {
			return err
		}
		if l.topicCallback != nil {
			l.topicCallback(e)
		}

	case EntryDeleteTopic:
		e, err := UnmarshalDeleteTopic(data)
		if err != nil {
			return err
		}
		if l.deleteTopicCallback != nil {
			l.deleteTopicCallback(e)
		}

	case EntryAlterConfig:
		e, err := UnmarshalAlterConfig(data)
		if err != nil {
			return err
		}
		if l.alterConfigCallback != nil {
			l.alterConfigCallback(e)
		}

	case EntryOffsetCommit:
		e, err := UnmarshalOffsetCommit(data)
		if err != nil {
			return err
		}
		if l.offsetCommitCallback != nil {
			l.offsetCommitCallback(e)
		}

	case EntryProducerID:
		e, err := UnmarshalProducerID(data)
		if err != nil {
			return err
		}
		if l.producerIDCallback != nil {
			l.producerIDCallback(e)
		}

	case EntryLogStartOffset:
		e, err := UnmarshalLogStartOffset(data)
		if err != nil {
			return err
		}
		if l.logStartCallback != nil {
			l.logStartCallback(e)
		}

	case EntryScramCredential:
		e, err := UnmarshalScramCredential(data)
		if err != nil {
			return err
		}
		if l.scramCredentialCallback != nil {
			l.scramCredentialCallback(e)
		}

	case EntryScramCredentialDelete:
		e, err := UnmarshalScramCredentialDelete(data)
		if err != nil {
			return err
		}
		if l.scramCredentialDeleteCallback != nil {
			l.scramCredentialDeleteCallback(e)
		}

	case EntryCompactionWatermark:
		e, err := UnmarshalCompactionWatermark(data)
		if err != nil {
			return err
		}
		if l.compactionWatermarkCallback != nil {
			l.compactionWatermarkCallback(e)
		}

	default:
		l.logger.Warn("unknown metadata entry type, skipping", "type", entryType)
	}

	return nil
}

func (l *Log) SetCallbacks(
	topicCb func(CreateTopicEntry),
	deleteTopicCb func(DeleteTopicEntry),
	alterConfigCb func(AlterConfigEntry),
	offsetCommitCb func(OffsetCommitEntry),
	producerIDCb func(ProducerIDEntry),
	logStartCb func(LogStartOffsetEntry),
) {
	l.topicCallback = topicCb
	l.deleteTopicCallback = deleteTopicCb
	l.alterConfigCallback = alterConfigCb
	l.offsetCommitCallback = offsetCommitCb
	l.producerIDCallback = producerIDCb
	l.logStartCallback = logStartCb
}

func (l *Log) SetScramCallbacks(
	credentialCb func(ScramCredentialEntry),
	deleteCb func(ScramCredentialDeleteEntry),
) {
	l.scramCredentialCallback = credentialCb
	l.scramCredentialDeleteCallback = deleteCb
}

func (l *Log) SetCompactionWatermarkCallback(cb func(CompactionWatermarkEntry)) {
	l.compactionWatermarkCallback = cb
}

// compactLocked performs compaction while holding l.mu.
// Writes a fresh log from the snapshot function, then atomically replaces
// the old log via fsync + rename.
func (l *Log) compactLocked() {
	if l.snapshotFn == nil {
		return
	}

	entries := l.snapshotFn()
	if len(entries) == 0 {
		return
	}

	tmpPath := l.path + ".tmp"
	tmpFile, err := os.Create(tmpPath)
	if err != nil {
		l.logger.Error("compaction: create tmp file", "err", err)
		return
	}

	var newSize int64
	for _, entryPayload := range entries {
		frameSize := 4 + 4 + len(entryPayload)
		frame := make([]byte, frameSize)
		binary.BigEndian.PutUint32(frame[0:4], uint32(4+len(entryPayload)))
		crcVal := crc32.Checksum(entryPayload, crc32cTable)
		binary.BigEndian.PutUint32(frame[4:8], crcVal)
		copy(frame[8:], entryPayload)

		n, err := tmpFile.Write(frame)
		if err != nil {
			l.logger.Error("compaction: write entry", "err", err)
			_ = tmpFile.Close()
			_ = os.Remove(tmpPath)
			return
		}
		newSize += int64(n)
	}

	if err := tmpFile.Sync(); err != nil {
		l.logger.Error("compaction: fsync tmp", "err", err)
		_ = tmpFile.Close()
		_ = os.Remove(tmpPath)
		return
	}
	_ = tmpFile.Close()

	_ = l.file.Close()

	if err := os.Rename(tmpPath, l.path); err != nil {
		l.logger.Error("compaction: rename", "err", err)
		l.file, _ = os.OpenFile(l.path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
		return
	}

	dir, err := os.Open(l.dir)
	if err == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}

	f, err := os.OpenFile(l.path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		l.logger.Error("compaction: reopen", "err", err)
		return
	}
	l.file = f
	oldSize := l.size
	l.size = newSize
	l.appendCount.Store(0)

	l.logger.Info("metadata.log compacted",
		"old_size", oldSize,
		"new_size", newSize,
		"entries", len(entries),
	)

	if l.compactHook != nil {
		data, err := os.ReadFile(l.path)
		if err != nil {
			l.logger.Error("compaction: read file for hook", "err", err)
		} else {
			l.compactHook(data)
		}
	}
}

func (l *Log) Compact() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.size > compactionThreshold {
		l.compactLocked()
	}
}

func (l *Log) Size() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.size
}

func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.file != nil {
		if err := l.file.Sync(); err != nil {
			l.logger.Warn("metadata.log sync on close", "err", err)
		}
		return l.file.Close()
	}
	return nil
}

func (l *Log) Path() string {
	return l.path
}

// ReplayEntry processes a single framed metadata entry: [4B length][4B CRC][payload].
// It validates the CRC, parses the entry type, and dispatches to the appropriate callback.
// The frame format matches what appendLocked produces and the replication hook sends.
func (l *Log) ReplayEntry(frame []byte) error {
	if len(frame) < 4+4+1 {
		return fmt.Errorf("metadata frame too short: %d bytes", len(frame))
	}

	// Frame layout: [4B length][4B CRC][payload]
	// Skip the 4-byte length prefix to get [CRC][payload].
	inner := frame[4:]

	if len(inner) < 4+1 {
		return fmt.Errorf("metadata payload too short: %d bytes", len(inner))
	}

	storedCRC := binary.BigEndian.Uint32(inner[0:4])
	actualCRC := crc32.Checksum(inner[4:], crc32cTable)
	if storedCRC != actualCRC {
		return fmt.Errorf("metadata entry CRC mismatch")
	}

	entryType := inner[4]
	entryData := inner[5:]

	return l.dispatchEntry(entryType, entryData)
}

// AppendRaw writes raw framed bytes directly to the metadata.log file.
// Used by the standby receiver to replicate metadata entries from the primary.
func (l *Log) AppendRaw(frame []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	n, err := l.file.Write(frame)
	if err != nil {
		return fmt.Errorf("write raw metadata entry: %w", err)
	}
	l.size += int64(n)
	return nil
}

// ReplaceFromSnapshot atomically replaces the metadata.log file with snapshot
// contents and replays it to rebuild in-memory state. Uses write-to-tmp +
// fsync + rename to avoid data loss on crash.
func (l *Log) ReplaceFromSnapshot(data []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Write snapshot to a temp file
	tmpPath := l.path + ".tmp"
	tmpFile, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("snapshot: create tmp file: %w", err)
	}

	if _, err := tmpFile.Write(data); err != nil {
		_ = tmpFile.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("snapshot: write tmp file: %w", err)
	}

	if err := tmpFile.Sync(); err != nil {
		_ = tmpFile.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("snapshot: fsync tmp file: %w", err)
	}
	_ = tmpFile.Close()

	// Close current file before rename
	if l.file != nil {
		_ = l.file.Close()
	}

	// Atomic rename
	if err := os.Rename(tmpPath, l.path); err != nil {
		// Try to reopen the old file for continued operation
		l.file, _ = os.OpenFile(l.path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
		return fmt.Errorf("snapshot: rename tmp to metadata.log: %w", err)
	}

	// Fsync the directory to make the rename durable
	dir, err := os.Open(l.dir)
	if err == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}

	// Reopen for appending
	f, err := os.OpenFile(l.path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("snapshot: reopen metadata.log: %w", err)
	}
	l.file = f
	l.size = int64(len(data))

	// Replay the snapshot to rebuild in-memory state.
	// replayLocked opens its own file handle for reading, safe under l.mu.
	_, replayErr := l.replayLocked()
	return replayErr
}
