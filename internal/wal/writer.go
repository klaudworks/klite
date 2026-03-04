package wal

import (
	"encoding/binary"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klaudworks/klite/internal/clock"
)

// WriterConfig holds configuration for the WAL writer.
type WriterConfig struct {
	Dir             string        // WAL directory
	SyncInterval    time.Duration // Fsync batch window (default 2ms)
	SegmentMaxBytes int64         // Max segment size before rotation (default 64 MiB)
	MaxDiskSize     int64         // Max total WAL on disk (default 1 GiB)
	FsyncEnabled    bool          // Whether to fsync (default true)
	Clock           clock.Clock   // Clock for ticker (default RealClock)
	Logger          *slog.Logger
}

// DefaultWriterConfig returns a WriterConfig with production defaults.
func DefaultWriterConfig() WriterConfig {
	return WriterConfig{
		SyncInterval:    2 * time.Millisecond,
		SegmentMaxBytes: 64 * 1024 * 1024,  // 64 MiB
		MaxDiskSize:     1024 * 1024 * 1024, // 1 GiB
		FsyncEnabled:    true,
		Clock:           clock.RealClock{},
		Logger:          slog.Default(),
	}
}

// writeRequest is sent from handler goroutines to the WAL writer.
type writeRequest struct {
	entry  []byte       // serialized WAL entry (MarshalEntry output)
	doneCh chan struct{} // closed after fsync completes
}

// segmentInfo tracks a single WAL segment file.
type segmentInfo struct {
	seq      uint64   // starting sequence number
	file     *os.File // open file handle
	size     int64    // current file size
	path     string   // file path
	minSeq   uint64   // minimum WAL sequence in this segment
	maxSeq   uint64   // maximum WAL sequence in this segment
}

// Writer is the WAL writer goroutine. It serializes all WAL writes,
// batches fsyncs, and manages segment rotation.
type Writer struct {
	cfg    WriterConfig
	idx    *Index
	logger *slog.Logger

	writeCh chan writeRequest
	stopCh  chan struct{}
	done    chan struct{}

	// Segment state (owned exclusively by the writer goroutine)
	segments   []*segmentInfo
	current    *segmentInfo
	nextSeq    atomic.Uint64
	walDir     string

	// For external callers to check
	mu       sync.Mutex
	stopped  bool
}

// NewWriter creates a new WAL writer. Call Start() to begin the writer goroutine.
func NewWriter(cfg WriterConfig, idx *Index) (*Writer, error) {
	if cfg.Clock == nil {
		cfg.Clock = clock.RealClock{}
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.SyncInterval == 0 {
		cfg.SyncInterval = 2 * time.Millisecond
	}
	if cfg.SegmentMaxBytes == 0 {
		cfg.SegmentMaxBytes = 64 * 1024 * 1024
	}
	if cfg.MaxDiskSize == 0 {
		cfg.MaxDiskSize = 1024 * 1024 * 1024
	}

	walDir := cfg.Dir
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		return nil, fmt.Errorf("create WAL dir: %w", err)
	}

	w := &Writer{
		cfg:     cfg,
		idx:     idx,
		logger:  cfg.Logger,
		writeCh: make(chan writeRequest, 4096),
		stopCh:  make(chan struct{}),
		done:    make(chan struct{}),
		walDir:  walDir,
	}

	return w, nil
}

// Start opens or creates the initial segment and starts the writer goroutine.
func (w *Writer) Start() error {
	// Scan existing segments to determine next sequence
	existingSeqs, err := w.scanExistingSegments()
	if err != nil {
		return fmt.Errorf("scan existing segments: %w", err)
	}

	if len(existingSeqs) > 0 {
		// Open the last segment for appending
		lastSeq := existingSeqs[len(existingSeqs)-1]
		seg, err := w.openSegment(lastSeq, true)
		if err != nil {
			return fmt.Errorf("open last segment: %w", err)
		}
		w.current = seg

		// Track all segments
		for _, seq := range existingSeqs {
			if seq == lastSeq {
				w.segments = append(w.segments, seg)
			} else {
				info, err := w.openSegmentReadOnly(seq)
				if err != nil {
					w.logger.Warn("failed to stat segment", "seq", seq, "err", err)
					continue
				}
				w.segments = append(w.segments, info)
			}
		}
	} else {
		// Create first segment
		seg, err := w.createSegment(0)
		if err != nil {
			return fmt.Errorf("create first segment: %w", err)
		}
		w.current = seg
		w.segments = []*segmentInfo{seg}
	}

	go w.run()
	return nil
}

// Append submits a WAL entry for writing. Blocks until fsync completes.
// Returns ErrClosed if the writer has been stopped.
func (w *Writer) Append(entry *Entry) error {
	w.mu.Lock()
	if w.stopped {
		w.mu.Unlock()
		return ErrClosed
	}
	w.mu.Unlock()

	seq := w.nextSeq.Add(1) - 1
	entry.Sequence = seq

	serialized := MarshalEntry(entry)
	doneCh := make(chan struct{})

	select {
	case w.writeCh <- writeRequest{entry: serialized, doneCh: doneCh}:
	case <-w.stopCh:
		return ErrClosed
	}

	select {
	case <-doneCh:
		return nil
	case <-w.stopCh:
		return ErrClosed
	}
}

// AppendAsync submits a WAL entry for writing, returning a channel that is
// closed when fsync completes. The caller can select on the channel.
func (w *Writer) AppendAsync(entry *Entry) (done <-chan struct{}, err error) {
	w.mu.Lock()
	if w.stopped {
		w.mu.Unlock()
		return nil, ErrClosed
	}
	w.mu.Unlock()

	seq := w.nextSeq.Add(1) - 1
	entry.Sequence = seq

	serialized := MarshalEntry(entry)
	doneCh := make(chan struct{})

	select {
	case w.writeCh <- writeRequest{entry: serialized, doneCh: doneCh}:
		return doneCh, nil
	case <-w.stopCh:
		return nil, ErrClosed
	}
}

// NextSequence returns the next sequence number that will be assigned.
func (w *Writer) NextSequence() uint64 {
	return w.nextSeq.Load()
}

// SetNextSequence sets the next sequence number (used during replay).
func (w *Writer) SetNextSequence(seq uint64) {
	w.nextSeq.Store(seq)
}

// Stop stops the writer goroutine and flushes pending writes.
func (w *Writer) Stop() {
	w.mu.Lock()
	if w.stopped {
		w.mu.Unlock()
		return
	}
	w.stopped = true
	w.mu.Unlock()

	close(w.stopCh)
	<-w.done
}

// Index returns the WAL index.
func (w *Writer) Index() *Index {
	return w.idx
}

// Dir returns the WAL directory.
func (w *Writer) Dir() string {
	return w.walDir
}

// run is the main writer goroutine loop.
func (w *Writer) run() {
	defer close(w.done)

	ticker := w.cfg.Clock.NewTicker(w.cfg.SyncInterval)
	defer ticker.Stop()

	var pending []writeRequest

	for {
		// Phase 1: Wait for at least one event
		select {
		case req := <-w.writeCh:
			pending = append(pending, req)
		case <-ticker.C:
			// fall through to drain
		case <-w.stopCh:
			w.flushAndSync(pending)
			w.closeSegments()
			return
		}

		// Phase 1b: Non-blocking drain of all queued entries
	drainLoop:
		for {
			select {
			case req := <-w.writeCh:
				pending = append(pending, req)
			default:
				break drainLoop
			}
		}

		if len(pending) == 0 {
			continue
		}

		// Phase 2: Write all pending entries
		for _, req := range pending {
			if err := w.appendEntry(req.entry); err != nil {
				w.logger.Error("WAL write error", "err", err)
				// Signal all waiters even on error so they don't block forever
				break
			}
		}

		// Phase 3: Fsync
		if w.cfg.FsyncEnabled && w.current != nil && w.current.file != nil {
			if err := w.current.file.Sync(); err != nil {
				w.logger.Error("WAL fsync error", "err", err)
			}
		}

		// Phase 4: Signal all waiters
		for _, req := range pending {
			close(req.doneCh)
		}
		pending = pending[:0] // reuse slice
	}
}

// appendEntry writes a single serialized entry to the current segment.
func (w *Writer) appendEntry(serialized []byte) error {
	// Check if we need to rotate
	if w.current.size+int64(len(serialized)) > w.cfg.SegmentMaxBytes && w.current.size > 0 {
		if err := w.rotateSegment(); err != nil {
			return fmt.Errorf("rotate segment: %w", err)
		}
	}

	// Record file offset before write
	fileOffset := w.current.size

	n, err := w.current.file.Write(serialized)
	if err != nil {
		return fmt.Errorf("write entry: %w", err)
	}
	w.current.size += int64(n)

	// Parse entry to update index
	if len(serialized) > 4 {
		entry, parseErr := UnmarshalEntry(serialized[4:]) // skip 4-byte length prefix
		if parseErr == nil {
			tp := TopicPartition{TopicID: entry.TopicID, Partition: entry.Partition}

			// Parse the RecordBatch to get LastOffsetDelta
			var lastOffset int64
			if len(entry.Data) >= 27 {
				lastOffsetDelta := int32(binary.BigEndian.Uint32(entry.Data[23:27]))
				lastOffset = entry.Offset + int64(lastOffsetDelta)
			} else {
				lastOffset = entry.Offset
			}

			idxEntry := IndexEntry{
				BaseOffset:  entry.Offset,
				LastOffset:  lastOffset,
				SegmentSeq:  w.current.seq,
				FileOffset:  fileOffset,
				EntrySize:   int32(len(serialized)),
				BatchSize:   int32(len(entry.Data)),
				WALSequence: entry.Sequence,
			}
			w.idx.Add(tp, idxEntry)

			// Update segment seq range
			if entry.Sequence > w.current.maxSeq {
				w.current.maxSeq = entry.Sequence
			}
		}
	}

	return nil
}

// flushAndSync writes pending entries and fsyncs.
func (w *Writer) flushAndSync(pending []writeRequest) {
	for _, req := range pending {
		if err := w.appendEntry(req.entry); err != nil {
			w.logger.Error("WAL flush write error", "err", err)
		}
	}
	if w.cfg.FsyncEnabled && w.current != nil && w.current.file != nil {
		w.current.file.Sync()
	}
	for _, req := range pending {
		close(req.doneCh)
	}
}

// rotateSegment closes the current segment and creates a new one.
func (w *Writer) rotateSegment() error {
	// Fsync and close current segment
	if w.cfg.FsyncEnabled {
		w.current.file.Sync()
	}
	w.current.file.Close()

	// Create new segment
	newSeq := w.nextSeq.Load()
	seg, err := w.createSegment(newSeq)
	if err != nil {
		return err
	}

	w.current = seg
	w.segments = append(w.segments, seg)

	// Fsync the directory to make the new entry durable
	dir, err := os.Open(w.walDir)
	if err == nil {
		dir.Sync()
		dir.Close()
	}

	return nil
}

// createSegment creates a new segment file with the given starting sequence.
func (w *Writer) createSegment(seq uint64) (*segmentInfo, error) {
	name := segmentFilename(seq)
	path := filepath.Join(w.walDir, name)

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("create segment %s: %w", name, err)
	}

	return &segmentInfo{
		seq:    seq,
		file:   f,
		size:   0,
		path:   path,
		minSeq: seq,
		maxSeq: seq,
	}, nil
}

// openSegment opens an existing segment for appending.
func (w *Writer) openSegment(seq uint64, forAppend bool) (*segmentInfo, error) {
	name := segmentFilename(seq)
	path := filepath.Join(w.walDir, name)

	flags := os.O_RDWR
	if forAppend {
		flags |= os.O_APPEND
	}

	f, err := os.OpenFile(path, flags, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open segment %s: %w", name, err)
	}

	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("stat segment %s: %w", name, err)
	}

	return &segmentInfo{
		seq:    seq,
		file:   f,
		size:   stat.Size(),
		path:   path,
		minSeq: seq,
		maxSeq: seq,
	}, nil
}

// openSegmentReadOnly opens an existing segment read-only and returns its info.
func (w *Writer) openSegmentReadOnly(seq uint64) (*segmentInfo, error) {
	name := segmentFilename(seq)
	path := filepath.Join(w.walDir, name)

	stat, err := os.Stat(path)
	if err != nil {
		return nil, err
	}

	return &segmentInfo{
		seq:    seq,
		size:   stat.Size(),
		path:   path,
		minSeq: seq,
		maxSeq: seq,
	}, nil
}

// scanExistingSegments returns sorted sequence numbers of existing segment files.
func (w *Writer) scanExistingSegments() ([]uint64, error) {
	entries, err := os.ReadDir(w.walDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var seqs []uint64
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		seq, ok := parseSegmentFilename(e.Name())
		if ok {
			seqs = append(seqs, seq)
		}
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	return seqs, nil
}

// closeSegments closes all open segment files.
func (w *Writer) closeSegments() {
	if w.current != nil && w.current.file != nil {
		w.current.file.Close()
		w.current.file = nil
	}
}

// segmentFilename returns the filename for a segment with the given sequence.
func segmentFilename(seq uint64) string {
	return fmt.Sprintf("%020d.wal", seq)
}

// parseSegmentFilename extracts the sequence number from a segment filename.
func parseSegmentFilename(name string) (uint64, bool) {
	if len(name) != 24 || name[20:] != ".wal" {
		return 0, false
	}
	var seq uint64
	_, err := fmt.Sscanf(name[:20], "%d", &seq)
	return seq, err == nil
}

// ReadBatch reads a batch from a WAL segment using the index entry information.
func (w *Writer) ReadBatch(entry IndexEntry) ([]byte, error) {
	// Find the segment file
	segPath := filepath.Join(w.walDir, segmentFilename(entry.SegmentSeq))

	f, err := os.Open(segPath)
	if err != nil {
		return nil, fmt.Errorf("open segment for read: %w", err)
	}
	defer f.Close()

	// Read the full entry from disk
	entryBuf := make([]byte, entry.EntrySize)
	_, err = f.ReadAt(entryBuf, entry.FileOffset)
	if err != nil {
		return nil, fmt.Errorf("read entry from segment: %w", err)
	}

	// Parse the entry to extract the batch data
	if len(entryBuf) <= 4 {
		return nil, fmt.Errorf("entry too small")
	}
	parsed, err := UnmarshalEntry(entryBuf[4:]) // skip length prefix
	if err != nil {
		return nil, fmt.Errorf("unmarshal entry: %w", err)
	}

	return parsed.Data, nil
}

// Replay scans all WAL segments and calls fn for each valid entry.
// Used during startup to rebuild in-memory state.
// If the last segment has a corrupted tail (CRC mismatch or truncated entry),
// it is truncated at the last valid entry boundary.
func (w *Writer) Replay(fn func(entry Entry, segmentSeq uint64, fileOffset int64) error) error {
	seqs, err := w.scanExistingSegments()
	if err != nil {
		return fmt.Errorf("scan segments for replay: %w", err)
	}

	var maxSeq uint64

	for i, seq := range seqs {
		segPath := filepath.Join(w.walDir, segmentFilename(seq))
		f, err := os.Open(segPath)
		if err != nil {
			return fmt.Errorf("open segment %d for replay: %w", seq, err)
		}

		var offset int64      // tracks current valid scan position
		var lastValid int64   // position after last valid entry

		_, scanErr := ScanFramedEntries(f, func(payload []byte) bool {
			entry, parseErr := UnmarshalEntry(payload)
			if parseErr != nil {
				w.logger.Warn("corrupted WAL entry during replay, stopping scan",
					"segment", seq, "offset", offset, "err", parseErr)
				return false // stop scanning this segment
			}

			entrySize := int64(4 + len(payload)) // length prefix + payload
			if err := fn(entry, seq, offset); err != nil {
				w.logger.Error("replay callback error",
					"segment", seq, "offset", offset, "err", err)
				return false
			}

			if entry.Sequence >= maxSeq {
				maxSeq = entry.Sequence + 1
			}

			offset += entrySize
			lastValid = offset
			return true
		})

		f.Close()

		if scanErr != nil {
			return fmt.Errorf("scan segment %d: %w", seq, scanErr)
		}

		// Truncate the last segment at the last valid entry if there's trailing
		// data (corrupted CRC, truncated entry, or garbage bytes).
		// Only truncate the last segment — earlier segments should be fully intact.
		isLast := (i == len(seqs)-1)
		if isLast {
			stat, statErr := os.Stat(segPath)
			if statErr == nil && stat.Size() > lastValid {
				w.logger.Warn("truncating corrupted tail of last WAL segment",
					"segment", seq, "valid_size", lastValid, "file_size", stat.Size())
				if truncErr := os.Truncate(segPath, lastValid); truncErr != nil {
					w.logger.Error("failed to truncate WAL segment", "err", truncErr)
				}
			}
		}
	}

	// Set next sequence to continue after the highest seen
	if maxSeq > w.nextSeq.Load() {
		w.nextSeq.Store(maxSeq)
	}

	return nil
}

// TotalDiskSize returns the total size of all WAL segments.
func (w *Writer) TotalDiskSize() int64 {
	var total int64
	for _, seg := range w.segments {
		total += seg.size
	}
	return total
}

// SegmentCount returns the number of WAL segments.
func (w *Writer) SegmentCount() int {
	return len(w.segments)
}
