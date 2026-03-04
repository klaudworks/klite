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
	topicCallback        func(CreateTopicEntry)
	deleteTopicCallback  func(DeleteTopicEntry)
	alterConfigCallback  func(AlterConfigEntry)
	offsetCommitCallback func(OffsetCommitEntry)
	producerIDCallback   func(ProducerIDEntry)
	logStartCallback     func(LogStartOffsetEntry)

	// Snapshot provider for compaction (set by broker during init)
	snapshotFn func() [][]byte
}

// LogConfig holds configuration for the metadata log.
type LogConfig struct {
	DataDir string
	Logger  *slog.Logger
}

// NewLog creates or opens a metadata.log file. Call Replay() to read existing
// entries, then use Append/AppendSync for new entries.
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
		f.Close()
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

// SetSnapshotFn sets the function used during compaction to generate a fresh
// snapshot of all live state as serialized entries.
func (l *Log) SetSnapshotFn(fn func() [][]byte) {
	l.snapshotFn = fn
}

// Append writes a serialized entry to the metadata.log (buffered, no fsync).
// Used for frequent entries like OFFSET_COMMIT.
func (l *Log) Append(entryPayload []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.appendLocked(entryPayload, false)
}

// AppendSync writes a serialized entry to the metadata.log and fsyncs.
// Used for infrequent but critical entries like CREATE_TOPIC, LOG_START_OFFSET.
func (l *Log) AppendSync(entryPayload []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.appendLocked(entryPayload, true)
}

// appendLocked writes a framed entry. Caller must hold l.mu.
// Framing: [4 bytes length][4 bytes CRC32c][payload]
func (l *Log) appendLocked(entryPayload []byte, doSync bool) error {
	// Frame: length(4) + crc(4) + payload
	frameSize := 4 + 4 + len(entryPayload)
	frame := make([]byte, frameSize)

	// Length = crc(4) + payload
	binary.BigEndian.PutUint32(frame[0:4], uint32(4+len(entryPayload)))

	// CRC over payload
	crcVal := crc32.Checksum(entryPayload, crc32cTable)
	binary.BigEndian.PutUint32(frame[4:8], crcVal)

	// Payload
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

	// Check if compaction is needed
	count := l.appendCount.Add(1)
	if count%compactionCheckInterval == 0 && l.size > compactionThreshold {
		// Compaction runs with the lock held — it's brief (~1ms for typical state)
		l.compactLocked()
	}

	return nil
}

// Replay reads all entries from the metadata.log and calls the appropriate
// callback for each valid entry. Returns the number of entries replayed.
func (l *Log) Replay() (int, error) {
	// Open the file for reading from the start
	f, err := os.Open(l.path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("open metadata.log for replay: %w", err)
	}
	defer f.Close()

	count := 0
	_, scanErr := wal.ScanFramedEntries(f, func(payload []byte) bool {
		if len(payload) < 4+1 {
			// Need at least CRC(4) + type(1)
			return false
		}

		// Validate CRC
		storedCRC := binary.BigEndian.Uint32(payload[0:4])
		actualCRC := crc32.Checksum(payload[4:], crc32cTable)
		if storedCRC != actualCRC {
			l.logger.Warn("metadata.log CRC mismatch, stopping replay", "entry", count)
			return false
		}

		// Parse entry type (first byte after CRC)
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

// dispatchEntry routes a parsed entry to the appropriate callback.
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

	default:
		l.logger.Warn("unknown metadata entry type, skipping", "type", entryType)
	}

	return nil
}

// SetCallbacks sets all replay callbacks at once.
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
			tmpFile.Close()
			os.Remove(tmpPath)
			return
		}
		newSize += int64(n)
	}

	// Fsync the tmp file
	if err := tmpFile.Sync(); err != nil {
		l.logger.Error("compaction: fsync tmp", "err", err)
		tmpFile.Close()
		os.Remove(tmpPath)
		return
	}
	tmpFile.Close()

	// Close current file before rename
	l.file.Close()

	// Atomic rename
	if err := os.Rename(tmpPath, l.path); err != nil {
		l.logger.Error("compaction: rename", "err", err)
		// Reopen old file
		l.file, _ = os.OpenFile(l.path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
		return
	}

	// Fsync directory
	dir, err := os.Open(l.dir)
	if err == nil {
		dir.Sync()
		dir.Close()
	}

	// Reopen the compacted file
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
}

// Compact triggers a compaction if the file exceeds the threshold.
// Can be called externally (e.g., during startup after replay).
func (l *Log) Compact() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.size > compactionThreshold {
		l.compactLocked()
	}
}

// Size returns the current file size.
func (l *Log) Size() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.size
}

// Close closes the metadata.log file.
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

// Path returns the file path.
func (l *Log) Path() string {
	return l.path
}
