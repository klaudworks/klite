package wal

import (
	"encoding/binary"
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klaudworks/klite/internal/clock"
)

// wrappedReplicator wraps a Replicator in a concrete type so atomic.Value
// always stores the same type. A nil Replicator is represented by a
// wrappedReplicator with a nil inner field.
type wrappedReplicator struct{ inner Replicator }

// Replicator sends WAL batches to a standby for synchronous replication.
type Replicator interface {
	// Replicate sends a batch of serialized WAL entries to the standby.
	// firstSeq and lastSeq identify the sequence range for ACK tracking.
	// Returns a channel that receives nil on standby ACK, or an error
	// on timeout/disconnect.
	Replicate(batch []byte, firstSeq, lastSeq uint64) <-chan error

	// Connected returns true if a standby is currently connected and
	// has completed the HELLO handshake.
	Connected() bool
}

type WriterConfig struct {
	Dir             string        // WAL directory
	SyncInterval    time.Duration // Fsync batch window (default 2ms)
	SegmentMaxBytes int64         // Max segment size before rotation (default 64 MiB)
	MaxDiskSize     int64         // Max total WAL on disk (default 1 GiB)
	FsyncEnabled    bool          // Whether to fsync (default true)
	S3Configured    bool          // Whether S3 is configured; controls unflushed-segment protection
	Clock           clock.Clock   // Clock for ticker (default RealClock)
	Logger          *slog.Logger

	// Replicator, if non-nil, sends each fsync batch to a standby.
	// The writer waits for the replication ACK before signaling handlers.
	Replicator Replicator
}

func DefaultWriterConfig() WriterConfig {
	return WriterConfig{
		SyncInterval:    2 * time.Millisecond,
		SegmentMaxBytes: 64 * 1024 * 1024,   // 64 MiB
		MaxDiskSize:     1024 * 1024 * 1024, // 1 GiB
		FsyncEnabled:    true,
		Clock:           clock.RealClock{},
		Logger:          slog.Default(),
	}
}

type writeRequest struct {
	entry []byte     // serialized WAL entry (MarshalEntry output)
	errCh chan error // receives nil on success, non-nil on write/fsync failure
}

type segmentInfo struct {
	seq    uint64   // starting sequence number
	file   *os.File // open file handle
	size   int64    // current file size
	path   string   // file path
	minSeq uint64   // minimum WAL sequence in this segment
	maxSeq uint64   // maximum WAL sequence in this segment
}

// Writer serializes all WAL writes, batches fsyncs, and manages segment rotation.
type Writer struct {
	cfg    WriterConfig
	idx    *Index
	logger *slog.Logger

	writeCh   chan writeRequest
	cleanupCh chan struct{} // signals TryCleanupSegments
	stopCh    chan struct{}
	done      chan struct{}

	// Segment state. Most access is from the writer goroutine, but
	// AppendReplicated (receiver goroutine) also calls appendEntry/rotateSegment
	// which mutates segments/current. segMu protects concurrent access.
	segMu    sync.Mutex
	segments []*segmentInfo
	current  *segmentInfo
	nextSeq  atomic.Uint64
	segSeq   uint64 // monotonic segment filename counter, owned by writer goroutine
	walDir   string

	// Async segment pre-creation. A background goroutine creates the next
	// segment file and fsyncs the directory, then delivers it via preCreateCh.
	// The writer goroutine reads from the channel and parks the result in
	// preCreated. No locks needed: only the writer goroutine touches
	// preCreated, and only the background goroutine sends on the channel.
	preCreated        *segmentInfo
	preCreateCh       chan *segmentInfo // background -> writer goroutine
	preCreateInflight bool              // owned by writer goroutine

	// Disk usage tracking (for segment cleanup)
	diskUsage atomic.Int64 // total bytes across all WAL segment files

	// S3 flush watermark (WAL-sequence-based). Any WAL entry with sequence
	// below this value is guaranteed to be in S3. 0 means S3 not configured
	// or nothing flushed yet.
	s3FlushWatermark atomic.Uint64

	// Back-pressure flag: set when WAL is over max and all remaining segments
	// are unflushed. Append/AppendAsync return ErrWALFull when true.
	walFull atomic.Bool

	// For external callers to check
	mu      sync.Mutex
	stopped bool

	// noStandbyWarned tracks whether we've logged the no-standby warning
	noStandbyWarned atomic.Bool

	// localOnlyBatches counts WAL batches written without replication because
	// the standby was not connected. Reset on standby reconnect.
	//
	// These batches exist only on the primary's local WAL and in S3 once
	// flushed. The standby does NOT receive them on reconnection (the
	// replication protocol only streams new entries forward). After a
	// standby reconnects, at least one S3 flush interval must elapse
	// before a subsequent failover is safe — otherwise the local-only
	// window data is lost because it's neither on the standby's WAL
	// nor in S3.
	localOnlyBatches atomic.Int64

	// replicator stores the current Replicator behind an atomic.Value
	// so SetReplicator (called from the broker goroutine) and run()
	// (the writer goroutine) don't race. Stores nil or a Replicator.
	replicator atomic.Value // holds Replicator (or wrappedReplicator)

	// replayedMinMax stores per-segment minSeq/maxSeq discovered during
	// Replay(). Since Replay runs before Start() populates w.segments,
	// we stash the values here and apply them in Start().
	replayedMinMax map[uint64][2]uint64 // segSeq → [minSeq, maxSeq]
}

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
		cfg:         cfg,
		idx:         idx,
		logger:      cfg.Logger,
		writeCh:     make(chan writeRequest, 4096),
		cleanupCh:   make(chan struct{}, 1),
		preCreateCh: make(chan *segmentInfo, 1),
		stopCh:      make(chan struct{}),
		done:        make(chan struct{}),
		walDir:      walDir,
	}
	if cfg.Replicator != nil {
		w.replicator.Store(wrappedReplicator{inner: cfg.Replicator})
	}

	return w, nil
}

func (w *Writer) Start() error {
	existingSeqs, err := w.scanExistingSegments()
	if err != nil {
		return fmt.Errorf("scan existing segments: %w", err)
	}

	if len(existingSeqs) > 0 {
		lastSeq := existingSeqs[len(existingSeqs)-1]
		seg, err := w.openSegment(lastSeq, true)
		if err != nil {
			return fmt.Errorf("open last segment: %w", err)
		}
		w.current = seg
		w.segSeq = lastSeq + 1

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
		seg, err := w.createSegment(0)
		if err != nil {
			return fmt.Errorf("create first segment: %w", err)
		}
		w.current = seg
		w.segments = []*segmentInfo{seg}
		w.segSeq = 1
	}

	// Apply minSeq/maxSeq discovered during Replay() (which runs before
	// Start, when w.segments was still empty).
	for _, seg := range w.segments {
		if mm, ok := w.replayedMinMax[seg.seq]; ok {
			seg.minSeq = mm[0]
			seg.maxSeq = mm[1]
		}
	}
	w.replayedMinMax = nil // free memory

	var totalSize int64
	for _, seg := range w.segments {
		totalSize += seg.size
	}
	w.diskUsage.Store(totalSize)

	go w.run()
	return nil
}

// Append blocks until the entry is written and fsync'd.
func (w *Writer) Append(entry *Entry) error {
	w.mu.Lock()
	if w.stopped {
		w.mu.Unlock()
		return ErrClosed
	}
	w.mu.Unlock()

	if w.walFull.Load() {
		return ErrWALFull
	}

	seq := w.nextSeq.Add(1) - 1
	entry.Sequence = seq

	serialized := MarshalEntry(entry)
	errCh := make(chan error, 1)

	select {
	case w.writeCh <- writeRequest{entry: serialized, errCh: errCh}:
	case <-w.stopCh:
		return ErrClosed
	}

	select {
	case err := <-errCh:
		return err
	case <-w.stopCh:
		return ErrClosed
	}
}

func (w *Writer) AppendAsync(entry *Entry) (done <-chan error, err error) {
	w.mu.Lock()
	if w.stopped {
		w.mu.Unlock()
		return nil, ErrClosed
	}
	w.mu.Unlock()

	if w.walFull.Load() {
		return nil, ErrWALFull
	}

	seq := w.nextSeq.Add(1) - 1
	entry.Sequence = seq

	serialized := MarshalEntry(entry)
	errCh := make(chan error, 1)

	select {
	case w.writeCh <- writeRequest{entry: serialized, errCh: errCh}:
		return errCh, nil
	case <-w.stopCh:
		return nil, ErrClosed
	}
}

// AppendReplicated writes an already-serialized WAL entry (the exact bytes
// from MarshalEntry, including the 4-byte length prefix) to the current segment.
// It does NOT assign a sequence number — the sequence is already embedded in
// the entry by the primary. Entries with sequence <= nextSequence-1 are
// silently skipped (duplicate from reconnect). The caller must call Sync()
// separately after the batch.
//
// Returns written=true if the entry was appended to disk, written=false if
// skipped (duplicate).
// ReplicatedEntryInfo contains parsed metadata from a replicated WAL entry,
// returned by AppendReplicated so callers can advance partition state (e.g. HW).
type ReplicatedEntryInfo struct {
	TopicID   [16]byte
	Partition int32
	EndOffset int64 // lastOffset + 1 (the new HW if this is the latest entry)
}

func (w *Writer) AppendReplicated(serializedEntry []byte) (info ReplicatedEntryInfo, written bool, err error) {
	if len(serializedEntry) <= 8+8 {
		return info, false, fmt.Errorf("replicated entry too short: %d bytes", len(serializedEntry))
	}

	seq := parseSequence(serializedEntry)
	currentNext := w.nextSeq.Load()
	if currentNext > 0 && seq < currentNext {
		return info, false, nil // duplicate, skip
	}

	if err := w.appendEntry(serializedEntry); err != nil {
		return info, false, err
	}

	w.nextSeq.Store(seq + 1)

	// Parse entry metadata for the caller (e.g. to advance HW).
	if len(serializedEntry) > 4 {
		if entry, parseErr := UnmarshalEntry(serializedEntry[4:]); parseErr == nil {
			info.TopicID = entry.TopicID
			info.Partition = entry.Partition
			info.EndOffset = entry.Offset + 1
			if len(entry.Data) >= 27 {
				lastOffsetDelta := int32(binary.BigEndian.Uint32(entry.Data[23:27]))
				info.EndOffset = entry.Offset + int64(lastOffsetDelta) + 1
			}
		}
	}

	return info, true, nil
}

// Sync fsyncs the current segment file.
func (w *Writer) Sync() error {
	if w.current != nil && w.current.file != nil {
		return w.current.file.Sync()
	}
	return nil
}

func (w *Writer) NextSequence() uint64 {
	return w.nextSeq.Load()
}

func (w *Writer) SetNextSequence(seq uint64) {
	w.nextSeq.Store(seq)
}

// SetReplicator dynamically sets or clears the replicator.
// Safe to call from any goroutine.
func (w *Writer) SetReplicator(r Replicator) {
	w.replicator.Store(wrappedReplicator{inner: r})
	w.noStandbyWarned.Store(false)
}

// getReplicator returns the current Replicator, or nil if none is set.
func (w *Writer) getReplicator() Replicator {
	v := w.replicator.Load()
	if v == nil {
		return nil
	}
	return v.(wrappedReplicator).inner
}

// SetS3FlushWatermark stores the global S3 flush watermark and triggers
// cleanup, since a watermark advance may make previously ineligible segments
// eligible for deletion.
func (w *Writer) SetS3FlushWatermark(seq uint64) {
	w.s3FlushWatermark.Store(seq)
	w.TryCleanupSegments()
}

// S3FlushWatermark returns the current S3 flush watermark.
func (w *Writer) S3FlushWatermark() uint64 {
	return w.s3FlushWatermark.Load()
}

// IsWALFull returns true when the WAL is at max capacity and all remaining
// segments contain unflushed data.
func (w *Writer) IsWALFull() bool {
	return w.walFull.Load()
}

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

func (w *Writer) Index() *Index {
	return w.idx
}

func (w *Writer) Dir() string {
	return w.walDir
}

func (w *Writer) run() {
	defer close(w.done)

	ticker := w.cfg.Clock.NewTicker(w.cfg.SyncInterval)
	defer ticker.Stop()

	var pending []writeRequest

	for {
		select {
		case req := <-w.writeCh:
			pending = append(pending, req)
		case <-ticker.C:
		case seg := <-w.preCreateCh:
			if seg != nil {
				w.preCreated = seg
			}
			w.preCreateInflight = false
			continue
		case <-w.cleanupCh:
			w.cleanSegments()
			continue
		case <-w.stopCh:
			w.flushAndSync(pending)
			w.closeSegments()
			return
		}

	drainLoop:
		for {
			select {
			case req := <-w.writeCh:
				pending = append(pending, req)
			case seg := <-w.preCreateCh:
				if seg != nil {
					w.preCreated = seg
				}
				w.preCreateInflight = false
			default:
				break drainLoop
			}
		}

		if len(pending) == 0 {
			continue
		}

		written := 0
		for i, req := range pending {
			if err := w.appendEntry(req.entry); err != nil {
				w.logger.Error("WAL write error", "err", err)
				written = i
				for _, fail := range pending[i:] {
					fail.errCh <- err
				}
				break
			}
			written = i + 1
		}

		// Phase 2: fsync local + replicate in parallel
		var replCh <-chan error
		repl := w.getReplicator()
		if written > 0 && repl != nil && repl.Connected() {
			batch := w.collectBatchBytes(pending[:written])
			firstSeq, lastSeq := w.batchSeqRange(pending[:written])
			replCh = repl.Replicate(batch, firstSeq, lastSeq)
		} else if written > 0 && repl != nil && !repl.Connected() {
			newTotal := w.localOnlyBatches.Add(int64(written))
			w.logger.Warn("WAL local-only ack: standby not connected, acking without replication",
				"batches", written, "local_only_total", newTotal)
		}

		if written > 0 && w.cfg.FsyncEnabled && w.current != nil && w.current.file != nil {
			if err := w.current.file.Sync(); err != nil {
				w.logger.Error("WAL fsync error", "err", err)
				for _, req := range pending[:written] {
					req.errCh <- err
				}
				written = 0
				// Drain replication channel if started
				if replCh != nil {
					<-replCh
				}
			}
		}

		if written > 0 {
			// Phase 3: wait for standby ACK (if replicating)
			var replErr error
			if replCh != nil {
				replErr = <-replCh
			}

			if replErr != nil {
				w.logger.Warn("WAL replication failed, returning error to producers",
					"err", replErr, "batches", written)
				for _, req := range pending[:written] {
					req.errCh <- replErr
				}
			} else {
				if repl != nil && !repl.Connected() {
					w.logNoStandbyOnce()
				}
				for _, req := range pending[:written] {
					req.errCh <- nil
				}
			}
		}
		pending = pending[:0]
		w.maybePreCreateSegment()
		w.cleanSegments()
	}
}

func (w *Writer) appendEntry(serialized []byte) error {
	if w.current.size+int64(len(serialized)) > w.cfg.SegmentMaxBytes && w.current.size > 0 {
		if err := w.rotateSegment(); err != nil {
			return fmt.Errorf("rotate segment: %w", err)
		}
	}

	fileOffset := w.current.size

	n, err := w.current.file.Write(serialized)
	if err != nil {
		return fmt.Errorf("write entry: %w", err)
	}
	w.current.size += int64(n)
	w.diskUsage.Add(int64(n))

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

			if entry.Sequence < w.current.minSeq {
				w.current.minSeq = entry.Sequence
			}
			if entry.Sequence > w.current.maxSeq {
				w.current.maxSeq = entry.Sequence
			}
		}
	}

	return nil
}

func (w *Writer) flushAndSync(pending []writeRequest) {
	written := 0
	for i, req := range pending {
		if err := w.appendEntry(req.entry); err != nil {
			w.logger.Error("WAL flush write error", "err", err)
			for _, fail := range pending[i:] {
				fail.errCh <- err
			}
			break
		}
		written = i + 1
	}
	if written > 0 && w.cfg.FsyncEnabled && w.current != nil && w.current.file != nil {
		if err := w.current.file.Sync(); err != nil {
			w.logger.Error("WAL fsync error during flush", "err", err)
			for _, req := range pending[:written] {
				req.errCh <- err
			}
			return
		}
	}

	// Best-effort replicate on shutdown (short timeout)
	repl := w.getReplicator()
	if repl != nil && written > 0 {
		batch := w.collectBatchBytes(pending[:written])
		firstSeq, lastSeq := w.batchSeqRange(pending[:written])
		replCh := repl.Replicate(batch, firstSeq, lastSeq)
		select {
		case <-replCh:
		case <-w.cfg.Clock.After(2 * time.Second):
			w.logger.Warn("standby replication timed out during shutdown")
		}
	}

	for _, req := range pending[:written] {
		req.errCh <- nil
	}
}

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

func (w *Writer) batchSeqRange(pending []writeRequest) (first, last uint64) {
	first = parseSequence(pending[0].entry)
	last = parseSequence(pending[len(pending)-1].entry)
	return first, last
}

// parseSequence extracts the WAL sequence number from a serialized entry.
// Entry format: [4B length][4B CRC][8B sequence]...
func parseSequence(entry []byte) uint64 {
	return binary.BigEndian.Uint64(entry[8:16])
}

func (w *Writer) logNoStandbyOnce() {
	if !w.noStandbyWarned.Load() {
		w.logger.Warn("replicator configured but no standby connected, proceeding with local fsync only")
		w.noStandbyWarned.Store(true)
	}
}

// LocalOnlyBatches returns the number of WAL batches acked without replication
// since the last reset.
func (w *Writer) LocalOnlyBatches() int64 {
	return w.localOnlyBatches.Load()
}

// ResetLocalOnlyBatches resets the local-only batch counter (e.g. on standby reconnect).
func (w *Writer) ResetLocalOnlyBatches() {
	w.localOnlyBatches.Store(0)
	w.noStandbyWarned.Store(false)
}

// rotateSegment closes the current segment and switches to the next one.
// If a segment was pre-created, it is used directly (fast path: just a file
// close, no file creation or directory fsync on the hot path).
func (w *Writer) rotateSegment() error {
	if w.cfg.FsyncEnabled {
		if err := w.current.file.Sync(); err != nil {
			w.logger.Warn("WAL segment rotation: fsync error on old segment", "err", err)
		}
	}
	if err := w.current.file.Close(); err != nil {
		w.logger.Warn("WAL segment rotation: close error on old segment", "err", err)
	}

	w.segMu.Lock()
	if w.preCreated != nil {
		w.current = w.preCreated
		w.segments = append(w.segments, w.preCreated)
		w.preCreated = nil
	} else {
		// Slow path: create inline (only happens if writes outpace
		// pre-creation, e.g. very large batches).
		seq := w.segSeq
		w.segSeq++
		seg, err := w.createSegment(seq)
		if err != nil {
			w.segMu.Unlock()
			return err
		}
		w.current = seg
		w.segments = append(w.segments, seg)

		dir, err := os.Open(w.walDir)
		if err == nil {
			_ = dir.Sync()
			_ = dir.Close()
		}
	}
	w.segMu.Unlock()

	return nil
}

// maybePreCreateSegment kicks off a background goroutine to create the next
// segment file if one isn't already ready or in flight. The goroutine creates
// the file, fsyncs the directory, and delivers the result on preCreateCh.
// The writer goroutine picks it up in the main select loop.
func (w *Writer) maybePreCreateSegment() {
	if w.preCreated != nil || w.preCreateInflight {
		return
	}
	w.preCreateInflight = true

	walDir := w.walDir
	newSeq := w.segSeq
	w.segSeq++
	logger := w.logger

	go func() {
		seg, err := w.createSegment(newSeq)
		if err != nil {
			logger.Warn("async pre-create segment failed", "err", err)
			// Send nil so the writer goroutine clears the inflight flag.
			w.preCreateCh <- nil
			return
		}

		dir, err := os.Open(walDir)
		if err == nil {
			_ = dir.Sync()
			_ = dir.Close()
		}

		w.preCreateCh <- seg
	}()
}

// TryCleanupSegments signals the writer goroutine to scan the segment list
// and delete segments no longer referenced by the WAL index. Safe to call
// from any goroutine. Non-blocking — if a cleanup signal is already pending,
// this is a no-op.
func (w *Writer) TryCleanupSegments() {
	select {
	case w.cleanupCh <- struct{}{}:
	default:
	}
}

// cleanSegments removes eligible segments when disk usage exceeds MaxDiskSize.
// A segment is eligible if its entire content has been flushed to S3 (determined
// by comparing maxSeq against the S3 flush watermark), or if S3 is not configured.
// After cleanup, sets the walFull flag if still over max (all remaining segments
// are unflushed).
func (w *Writer) cleanSegments() {
	w.segMu.Lock()
	defer w.segMu.Unlock()

	if len(w.segments) <= 1 {
		w.walFull.Store(false)
		return
	}

	watermark := w.s3FlushWatermark.Load()
	s3Mode := w.cfg.S3Configured

	// Step 1 (disk-pressure path): if over max, delete oldest eligible segments.
	if w.diskUsage.Load() > w.cfg.MaxDiskSize {
		for len(w.segments) > 1 && w.diskUsage.Load() > w.cfg.MaxDiskSize {
			oldest := w.segments[0]
			if oldest == w.current {
				break
			}

			// Check eligibility: segment's data must be in S3, or S3 not configured.
			// A segment is empty if minSeq == MaxUint64 (never had entries written).
			// An empty segment is always eligible for deletion.
			empty := oldest.minSeq == math.MaxUint64
			if s3Mode && !empty && oldest.maxSeq >= watermark {
				// Unflushed — cannot delete. All newer segments are also
				// unflushed (sequences are monotonic), so stop.
				break
			}

			w.idx.PruneSegment(oldest.seq)
			if oldest.file != nil {
				_ = oldest.file.Close()
				oldest.file = nil
			}
			if err := os.Remove(oldest.path); err != nil && !os.IsNotExist(err) {
				w.logger.Warn("failed to delete WAL segment", "path", oldest.path, "err", err)
				break
			}
			w.diskUsage.Add(-oldest.size)
			w.logger.Info("deleted WAL segment (disk pressure)",
				"seq", oldest.seq, "path", oldest.path)
			w.segments = w.segments[1:]
		}
	}

	// Set walFull if still over max (couldn't free enough space — remaining
	// segments are unflushed).
	w.walFull.Store(w.diskUsage.Load() > w.cfg.MaxDiskSize)
}

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
		minSeq: math.MaxUint64,
		maxSeq: 0,
	}, nil
}

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
		_ = f.Close()
		return nil, fmt.Errorf("stat segment %s: %w", name, err)
	}

	return &segmentInfo{
		seq:    seq,
		file:   f,
		size:   stat.Size(),
		path:   path,
		minSeq: math.MaxUint64,
		maxSeq: 0,
	}, nil
}

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
		minSeq: math.MaxUint64,
		maxSeq: 0,
	}, nil
}

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

func (w *Writer) closeSegments() {
	if w.current != nil && w.current.file != nil {
		_ = w.current.file.Close()
		w.current.file = nil
	}
	if w.preCreated != nil && w.preCreated.file != nil {
		_ = w.preCreated.file.Close()
		w.preCreated.file = nil
	}
	// Drain any in-flight pre-create result so the goroutine doesn't leak.
	if w.preCreateInflight {
		seg := <-w.preCreateCh
		if seg != nil && seg.file != nil {
			_ = seg.file.Close()
		}
	}
}

func segmentFilename(seq uint64) string {
	return fmt.Sprintf("%020d.wal", seq)
}

func parseSegmentFilename(name string) (uint64, bool) {
	if len(name) != 24 || name[20:] != ".wal" {
		return 0, false
	}
	var seq uint64
	_, err := fmt.Sscanf(name[:20], "%d", &seq)
	return seq, err == nil
}

func (w *Writer) ReadBatch(entry IndexEntry) ([]byte, error) {
	segPath := filepath.Join(w.walDir, segmentFilename(entry.SegmentSeq))

	f, err := os.Open(segPath)
	if err != nil {
		return nil, fmt.Errorf("open segment for read: %w", err)
	}
	defer f.Close() //nolint:errcheck // best-effort close

	entryBuf := make([]byte, entry.EntrySize)
	_, err = f.ReadAt(entryBuf, entry.FileOffset)
	if err != nil {
		return nil, fmt.Errorf("read entry from segment: %w", err)
	}

	if len(entryBuf) <= 4 {
		return nil, fmt.Errorf("entry too small")
	}
	parsed, err := UnmarshalEntry(entryBuf[4:])
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

		var offset int64    // tracks current valid scan position
		var lastValid int64 // position after last valid entry

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

			// Record segment minSeq/maxSeq for Start() to apply later
			// (w.segments is not populated yet during Replay).
			if w.replayedMinMax == nil {
				w.replayedMinMax = make(map[uint64][2]uint64)
			}
			mm, exists := w.replayedMinMax[seq]
			if !exists {
				mm = [2]uint64{entry.Sequence, entry.Sequence}
			} else {
				if entry.Sequence < mm[0] {
					mm[0] = entry.Sequence
				}
				if entry.Sequence > mm[1] {
					mm[1] = entry.Sequence
				}
			}
			w.replayedMinMax[seq] = mm

			offset += entrySize
			lastValid = offset
			return true
		})

		_ = f.Close()

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

func (w *Writer) TotalDiskSize() int64 {
	w.segMu.Lock()
	defer w.segMu.Unlock()
	var total int64
	for _, seg := range w.segments {
		total += seg.size
	}
	return total
}

func (w *Writer) SegmentCount() int {
	w.segMu.Lock()
	defer w.segMu.Unlock()
	return len(w.segments)
}
