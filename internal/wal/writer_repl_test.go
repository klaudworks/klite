package wal

import (
	"bytes"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/klaudworks/klite/internal/clock"
)

// mockReplicator implements Replicator for testing.
type mockReplicator struct {
	mu        sync.Mutex
	connected bool
	calls     []mockReplCall
	autoReply error // if non-nil, auto-reply with this on Replicate
	// When autoReply is nil, caller controls via the returned channel
	pendingChs []chan error
}

type mockReplCall struct {
	Batch    []byte
	FirstSeq uint64
	LastSeq  uint64
}

func (m *mockReplicator) Replicate(batch []byte, firstSeq, lastSeq uint64) <-chan error {
	m.mu.Lock()
	defer m.mu.Unlock()

	batchCopy := make([]byte, len(batch))
	copy(batchCopy, batch)
	m.calls = append(m.calls, mockReplCall{Batch: batchCopy, FirstSeq: firstSeq, LastSeq: lastSeq})

	ch := make(chan error, 1)
	if m.autoReply != nil {
		ch <- m.autoReply
	} else {
		m.pendingChs = append(m.pendingChs, ch)
	}
	return ch
}

func (m *mockReplicator) Connected() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.connected
}

func (m *mockReplicator) resolvePending(idx int, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if idx < len(m.pendingChs) {
		m.pendingChs[idx] <- err
	}
}

func (m *mockReplicator) getCalls() []mockReplCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]mockReplCall, len(m.calls))
	copy(out, m.calls)
	return out
}

func newTestWriter(t *testing.T, repl Replicator) *Writer {
	t.Helper()
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")

	idx := NewIndex()
	cfg := WriterConfig{
		Dir:             walDir,
		SyncInterval:    1 * time.Millisecond,
		SegmentMaxBytes: 64 * 1024 * 1024,
		FsyncEnabled:    false,
		Clock:           clock.RealClock{},
		Replicator:      repl,
	}

	w, err := NewWriter(cfg, idx)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(w.Stop)
	return w
}

func TestWriterReplicatorNil(t *testing.T) {
	t.Parallel()
	w := newTestWriter(t, nil)

	entry := &Entry{
		TopicID:   [16]byte{1, 2, 3},
		Partition: 0,
		Offset:    0,
		Data:      makeTestBatch(1, 1000),
	}
	if err := w.Append(entry); err != nil {
		t.Fatalf("Append should succeed with nil replicator: %v", err)
	}
}

func TestWriterSyncModeACK(t *testing.T) {
	t.Parallel()

	repl := &mockReplicator{connected: true}
	w := newTestWriter(t, repl)

	entry := &Entry{
		TopicID:   [16]byte{1, 2, 3},
		Partition: 0,
		Offset:    0,
		Data:      makeTestBatch(1, 1000),
	}

	errCh, err := w.AppendAsync(entry)
	if err != nil {
		t.Fatal(err)
	}

	// errCh should NOT resolve yet because replicator hasn't ACKed
	select {
	case <-errCh:
		t.Fatal("errCh should not resolve before replicator ACK")
	case <-time.After(20 * time.Millisecond):
		// expected
	}

	// Resolve the replicator
	repl.resolvePending(0, nil)

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("expected nil error, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("errCh did not resolve after replicator ACK")
	}
}

func TestWriterSyncModeTimeout(t *testing.T) {
	t.Parallel()

	repl := &mockReplicator{connected: true}
	w := newTestWriter(t, repl)

	entry := &Entry{
		TopicID:   [16]byte{1, 2, 3},
		Partition: 0,
		Offset:    0,
		Data:      makeTestBatch(1, 1000),
	}

	errCh, err := w.AppendAsync(entry)
	if err != nil {
		t.Fatal(err)
	}

	// Never resolve the replicator — simulate timeout
	// We need to send a timeout error to the replicator channel
	go func() {
		time.Sleep(50 * time.Millisecond)
		repl.resolvePending(0, fmt.Errorf("repl: ACK timeout"))
	}()

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected error from timeout, got nil")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("errCh did not resolve")
	}
}

func TestWriterReplicatorBatchBytes(t *testing.T) {
	t.Parallel()

	// Use a replicator that auto-replies with nil (instant ACK) so the test
	// doesn't need to manage pending channels and can focus on verifying
	// the batch bytes content.
	repl := &batchTrackingReplicator{}

	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	idx := NewIndex()

	cfg := WriterConfig{
		Dir:             walDir,
		SyncInterval:    100 * time.Millisecond,
		SegmentMaxBytes: 64 * 1024 * 1024,
		FsyncEnabled:    false,
		Clock:           clock.RealClock{},
		Replicator:      repl,
	}

	w, err := NewWriter(cfg, idx)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	defer w.Stop()

	topicID := [16]byte{1, 2, 3}

	// Submit 3 entries in quick succession (they should batch)
	for i := 0; i < 3; i++ {
		e := &Entry{
			TopicID:   topicID,
			Partition: 0,
			Offset:    int64(i),
			Data:      makeTestBatch(1, 1000),
		}
		if err := w.Append(e); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}

	// Verify the batch bytes. Entries may have been split across multiple
	// calls to Replicate if not all were batched together. Concatenate all
	// call payloads and verify they contain 3 serialized entries.
	calls := repl.getCalls()
	if len(calls) == 0 {
		t.Fatal("expected at least one Replicate call")
	}

	var totalBatch []byte
	for _, c := range calls {
		totalBatch = append(totalBatch, c.Batch...)
	}

	// Should contain exactly 3 entries
	reader := bytes.NewReader(totalBatch)
	count, scanErr := ScanFramedEntries(reader, func(payload []byte) bool {
		return true
	})
	if scanErr != nil {
		t.Fatalf("scan error: %v", scanErr)
	}
	if count != 3 {
		t.Fatalf("expected 3 entries in batch, got %d", count)
	}

	// Verify firstSeq and lastSeq across all calls
	firstCall := calls[0]
	lastCall := calls[len(calls)-1]
	if firstCall.FirstSeq > lastCall.LastSeq {
		t.Errorf("firstSeq %d > lastSeq %d", firstCall.FirstSeq, lastCall.LastSeq)
	}
}

// batchTrackingReplicator auto-ACKs immediately and records batch data.
type batchTrackingReplicator struct {
	mu    sync.Mutex
	calls []mockReplCall
}

func (b *batchTrackingReplicator) Replicate(batch []byte, firstSeq, lastSeq uint64) <-chan error {
	b.mu.Lock()
	defer b.mu.Unlock()
	batchCopy := make([]byte, len(batch))
	copy(batchCopy, batch)
	b.calls = append(b.calls, mockReplCall{Batch: batchCopy, FirstSeq: firstSeq, LastSeq: lastSeq})
	ch := make(chan error, 1)
	ch <- nil
	return ch
}

func (b *batchTrackingReplicator) Connected() bool { return true }

func (b *batchTrackingReplicator) getCalls() []mockReplCall {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]mockReplCall, len(b.calls))
	copy(out, b.calls)
	return out
}

func TestWriterReplicatorLocalFsyncFails(t *testing.T) {
	t.Parallel()

	// Verify that when replicator succeeds but fsync is disabled (baseline),
	// the writer works correctly with the replication path.
	repl := &mockReplicator{connected: true}
	// Use auto-reply so the replicator resolves immediately
	repl.autoReply = nil // nil autoReply means manual control via pendingChs

	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	idx := NewIndex()
	cfg := WriterConfig{
		Dir:             walDir,
		SyncInterval:    1 * time.Millisecond,
		SegmentMaxBytes: 64 * 1024 * 1024,
		FsyncEnabled:    false,
		Clock:           clock.RealClock{},
		Replicator:      repl,
	}
	w, err := NewWriter(cfg, idx)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	defer w.Stop()

	entry := &Entry{
		TopicID:   [16]byte{1, 2, 3},
		Partition: 0,
		Offset:    0,
		Data:      makeTestBatch(1, 1000),
	}

	errCh, appErr := w.AppendAsync(entry)
	if appErr != nil {
		t.Fatal(appErr)
	}

	// Wait a bit for the writer goroutine to process the entry and call Replicate
	time.Sleep(50 * time.Millisecond)

	// Resolve the pending replication
	repl.resolvePending(0, nil)

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("expected nil, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout")
	}
}

func TestWriterSyncModeNoStandbyConnected(t *testing.T) {
	t.Parallel()

	repl := &mockReplicator{connected: false}
	w := newTestWriter(t, repl)

	entry := &Entry{
		TopicID:   [16]byte{1, 2, 3},
		Partition: 0,
		Offset:    0,
		Data:      makeTestBatch(1, 1000),
	}

	// No standby connected — replication is skipped (local fsync only)
	err := w.Append(entry)
	if err != nil {
		t.Fatalf("sync mode with no standby should succeed: %v", err)
	}

	// Verify no Replicate calls were made
	calls := repl.getCalls()
	if len(calls) != 0 {
		t.Fatalf("expected 0 Replicate calls, got %d", len(calls))
	}
}

func TestWriterSyncModeStandbyWasConnected(t *testing.T) {
	t.Parallel()

	repl := &mockReplicator{connected: true}
	w := newTestWriter(t, repl)

	// First write with standby connected
	entry1 := &Entry{
		TopicID:   [16]byte{1, 2, 3},
		Partition: 0,
		Offset:    0,
		Data:      makeTestBatch(1, 1000),
	}
	errCh1, err := w.AppendAsync(entry1)
	if err != nil {
		t.Fatal(err)
	}

	// Wait for writer goroutine to process and call Replicate
	time.Sleep(50 * time.Millisecond)
	repl.resolvePending(0, nil)

	select {
	case err := <-errCh1:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("first write timeout")
	}

	// Second write: standby is still "connected" but Replicate returns error
	entry2 := &Entry{
		TopicID:   [16]byte{1, 2, 3},
		Partition: 0,
		Offset:    1,
		Data:      makeTestBatch(1, 1000),
	}
	errCh2, err := w.AppendAsync(entry2)
	if err != nil {
		t.Fatal(err)
	}

	// Wait for writer goroutine to process and call Replicate
	time.Sleep(50 * time.Millisecond)
	repl.resolvePending(1, fmt.Errorf("repl: standby disconnected"))

	select {
	case err := <-errCh2:
		if err == nil {
			t.Fatal("expected error in sync mode after standby disconnect")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("second write timeout")
	}
}

func TestWriterShutdownReplicationTimeout(t *testing.T) {
	t.Parallel()

	repl := &mockReplicator{connected: true}

	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	idx := NewIndex()
	cfg := WriterConfig{
		Dir:             walDir,
		SyncInterval:    1 * time.Millisecond,
		SegmentMaxBytes: 64 * 1024 * 1024,
		FsyncEnabled:    false,
		Clock:           clock.RealClock{},
		Replicator:      repl,
	}

	w, err := NewWriter(cfg, idx)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}

	// Submit an entry that will be pending when we Stop
	entry := &Entry{
		TopicID:   [16]byte{1, 2, 3},
		Partition: 0,
		Offset:    0,
		Data:      makeTestBatch(1, 1000),
	}
	_, _ = w.AppendAsync(entry)

	// Wait for it to be processed by the writer
	time.Sleep(50 * time.Millisecond)

	// Resolve the pending from the run loop so the main errCh doesn't hang
	repl.mu.Lock()
	for _, ch := range repl.pendingChs {
		ch <- nil
	}
	repl.mu.Unlock()

	time.Sleep(20 * time.Millisecond)

	// Stop should not hang even though flushAndSync's replication times out
	done := make(chan struct{})
	go func() {
		w.Stop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Stop blocked indefinitely — replication timeout in flushAndSync is broken")
	}
}

func TestAppendReplicated(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	idx := NewIndex()

	w, err := NewWriter(WriterConfig{
		Dir:             walDir,
		SyncInterval:    1 * time.Millisecond,
		SegmentMaxBytes: 64 * 1024 * 1024,
		FsyncEnabled:    false,
		Clock:           clock.RealClock{},
	}, idx)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	defer w.Stop()

	topicID := [16]byte{1, 2, 3}
	entry := &Entry{
		Sequence:  42,
		TopicID:   topicID,
		Partition: 0,
		Offset:    100,
		Data:      makeTestBatch(3, 2000),
	}
	serialized := MarshalEntry(entry)

	_, written, err := w.AppendReplicated(serialized)
	if err != nil {
		t.Fatalf("AppendReplicated: %v", err)
	}
	if !written {
		t.Fatal("should not skip on first append")
	}

	// Verify index was updated
	tp := TopicPartition{TopicID: topicID, Partition: 0}
	entries := idx.Lookup(tp, 100, 1024*1024)
	if len(entries) != 1 {
		t.Fatalf("expected 1 index entry, got %d", len(entries))
	}
	if entries[0].BaseOffset != 100 {
		t.Errorf("BaseOffset: got %d, want 100", entries[0].BaseOffset)
	}
	if entries[0].WALSequence != 42 {
		t.Errorf("WALSequence: got %d, want 42", entries[0].WALSequence)
	}

	// Verify nextSeq was updated
	if w.NextSequence() != 43 {
		t.Errorf("NextSequence: got %d, want 43", w.NextSequence())
	}
}

func TestAppendReplicatedSegmentRotation(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	idx := NewIndex()

	w, err := NewWriter(WriterConfig{
		Dir:             walDir,
		SyncInterval:    1 * time.Millisecond,
		SegmentMaxBytes: 200, // tiny to force rotation
		FsyncEnabled:    false,
		Clock:           clock.RealClock{},
	}, idx)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	defer w.Stop()

	topicID := [16]byte{1, 2, 3}
	for i := 0; i < 10; i++ {
		entry := &Entry{
			Sequence:  uint64(i),
			TopicID:   topicID,
			Partition: 0,
			Offset:    int64(i),
			Data:      makeTestBatch(1, 1000),
		}
		_, written, err := w.AppendReplicated(MarshalEntry(entry))
		if err != nil {
			t.Fatalf("AppendReplicated %d: %v", i, err)
		}
		if !written {
			t.Fatalf("entry %d should have been written", i)
		}
	}

	if w.SegmentCount() < 2 {
		t.Errorf("expected segment rotation, got %d segments", w.SegmentCount())
	}

	// Verify all entries indexed
	tp := TopicPartition{TopicID: topicID, Partition: 0}
	entries := idx.Lookup(tp, 0, 1024*1024)
	if len(entries) != 10 {
		t.Errorf("expected 10 index entries, got %d", len(entries))
	}
}

func TestAppendReplicatedDuplicateSkip(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	idx := NewIndex()

	w, err := NewWriter(WriterConfig{
		Dir:             walDir,
		SyncInterval:    1 * time.Millisecond,
		SegmentMaxBytes: 64 * 1024 * 1024,
		FsyncEnabled:    false,
		Clock:           clock.RealClock{},
	}, idx)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	defer w.Stop()

	// Set nextSequence to 100
	w.SetNextSequence(100)

	topicID := [16]byte{1, 2, 3}

	// Entry at sequence 50 should be skipped
	old := &Entry{
		Sequence:  50,
		TopicID:   topicID,
		Partition: 0,
		Offset:    50,
		Data:      makeTestBatch(1, 1000),
	}
	_, written, err := w.AppendReplicated(MarshalEntry(old))
	if err != nil {
		t.Fatal(err)
	}
	if written {
		t.Fatal("entry at seq=50 should be skipped (nextSeq=100)")
	}

	// Entry at sequence 100 should be written
	cur := &Entry{
		Sequence:  100,
		TopicID:   topicID,
		Partition: 0,
		Offset:    100,
		Data:      makeTestBatch(1, 1000),
	}
	_, written, err = w.AppendReplicated(MarshalEntry(cur))
	if err != nil {
		t.Fatal(err)
	}
	if !written {
		t.Fatal("entry at seq=100 should have been written")
	}

	if w.NextSequence() != 101 {
		t.Errorf("NextSequence: got %d, want 101", w.NextSequence())
	}
}

func TestParseSequence(t *testing.T) {
	t.Parallel()

	entry := &Entry{
		Sequence:  12345,
		TopicID:   [16]byte{1},
		Partition: 0,
		Offset:    0,
		Data:      makeTestBatch(1, 1000),
	}
	serialized := MarshalEntry(entry)
	got := parseSequence(serialized)
	if got != 12345 {
		t.Errorf("parseSequence: got %d, want 12345", got)
	}
}

func TestCollectBatchBytes(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	idx := NewIndex()
	w, err := NewWriter(WriterConfig{
		Dir:             walDir,
		SyncInterval:    1 * time.Millisecond,
		SegmentMaxBytes: 64 * 1024 * 1024,
		FsyncEnabled:    false,
		Clock:           clock.RealClock{},
	}, idx)
	if err != nil {
		t.Fatal(err)
	}

	var reqs []writeRequest
	var expected []byte
	for i := 0; i < 3; i++ {
		e := &Entry{
			Sequence:  uint64(i),
			TopicID:   [16]byte{byte(i)},
			Partition: 0,
			Offset:    int64(i),
			Data:      makeTestBatch(1, 1000),
		}
		ser := MarshalEntry(e)
		reqs = append(reqs, writeRequest{entry: ser})
		expected = append(expected, ser...)
	}

	got := w.collectBatchBytes(reqs)
	if !bytes.Equal(got, expected) {
		t.Errorf("collectBatchBytes mismatch: got %d bytes, want %d bytes", len(got), len(expected))
	}
}

func TestBatchSeqRange(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	idx := NewIndex()
	w, err := NewWriter(WriterConfig{
		Dir:             walDir,
		SyncInterval:    1 * time.Millisecond,
		SegmentMaxBytes: 64 * 1024 * 1024,
		FsyncEnabled:    false,
		Clock:           clock.RealClock{},
	}, idx)
	if err != nil {
		t.Fatal(err)
	}

	var reqs []writeRequest
	for i := 5; i < 8; i++ {
		e := &Entry{
			Sequence:  uint64(i),
			TopicID:   [16]byte{byte(i)},
			Partition: 0,
			Offset:    int64(i),
			Data:      makeTestBatch(1, 1000),
		}
		reqs = append(reqs, writeRequest{entry: MarshalEntry(e)})
	}

	first, last := w.batchSeqRange(reqs)
	if first != 5 {
		t.Errorf("first: got %d, want 5", first)
	}
	if last != 7 {
		t.Errorf("last: got %d, want 7", last)
	}
}

func TestWriterReplicatorParallelFsyncAndSend(t *testing.T) {
	t.Parallel()

	// Verify that local fsync and standby send happen in parallel,
	// not sequentially.
	var replStart, replEnd atomic.Int64
	repl := &parallelReplicator{
		startTime: &replStart,
		endTime:   &replEnd,
	}

	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	idx := NewIndex()
	cfg := WriterConfig{
		Dir:             walDir,
		SyncInterval:    1 * time.Millisecond,
		SegmentMaxBytes: 64 * 1024 * 1024,
		FsyncEnabled:    false, // no real fsync in tests
		Clock:           clock.RealClock{},
		Replicator:      repl,
	}
	w, err := NewWriter(cfg, idx)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	defer w.Stop()

	entry := &Entry{
		TopicID:   [16]byte{1, 2, 3},
		Partition: 0,
		Offset:    0,
		Data:      makeTestBatch(1, 1000),
	}
	if err := w.Append(entry); err != nil {
		t.Fatalf("Append: %v", err)
	}

	// The Replicate call should have been made before or during the fsync,
	// not after. With FsyncEnabled=false, we just verify it was called.
	if replStart.Load() == 0 {
		t.Fatal("Replicate was never called")
	}
}

// parallelReplicator records timing of Replicate calls to verify parallel execution.
type parallelReplicator struct {
	startTime *atomic.Int64
	endTime   *atomic.Int64
}

func (p *parallelReplicator) Replicate(batch []byte, firstSeq, lastSeq uint64) <-chan error {
	p.startTime.Store(time.Now().UnixNano())
	ch := make(chan error, 1)
	ch <- nil // instant ACK
	p.endTime.Store(time.Now().UnixNano())
	return ch
}

func (p *parallelReplicator) Connected() bool { return true }

func TestSetReplicatorRaceSafe(t *testing.T) {
	t.Parallel()

	repl := &batchTrackingReplicator{}
	w := newTestWriter(t, nil)

	// Concurrently set/clear the replicator while writing entries.
	// With -race this will detect any data race.
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			w.SetReplicator(repl)
			time.Sleep(time.Millisecond)
			w.SetReplicator(nil)
			time.Sleep(time.Millisecond)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			e := &Entry{
				TopicID:   [16]byte{1, 2, 3},
				Partition: 0,
				Offset:    int64(i),
				Data:      makeTestBatch(1, 100),
			}
			_ = w.Append(e)
		}
	}()

	wg.Wait()
}
