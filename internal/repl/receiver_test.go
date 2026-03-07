package repl

import (
	"bytes"
	"context"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/klaudworks/klite/internal/wal"
)

// mockWALAppender is a test double for WALAppender.
type mockWALAppender struct {
	mu         sync.Mutex
	entries    [][]byte
	nextSeq    uint64
	syncCalled int
	syncErr    error
}

func (m *mockWALAppender) AppendReplicated(serialized []byte) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Parse entry to get sequence
	if len(serialized) > 4 {
		entry, err := wal.UnmarshalEntry(serialized[4:])
		if err == nil && entry.Sequence < m.nextSeq {
			return false, nil // duplicate, skip
		}
	}

	cp := make([]byte, len(serialized))
	copy(cp, serialized)
	m.entries = append(m.entries, cp)
	return true, nil
}

func (m *mockWALAppender) Sync() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.syncCalled++
	return m.syncErr
}

func (m *mockWALAppender) NextSequence() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.nextSeq
}

func (m *mockWALAppender) SetNextSequence(seq uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextSeq = seq
}

func (m *mockWALAppender) EntryCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.entries)
}

// mockMetaAppender is a test double for MetaAppender.
type mockMetaAppender struct {
	mu              sync.Mutex
	rawEntries      [][]byte
	replayedEntries [][]byte
	snapshotData    []byte
	replayErr       error
}

func (m *mockMetaAppender) ReplayEntry(frame []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]byte, len(frame))
	copy(cp, frame)
	m.replayedEntries = append(m.replayedEntries, cp)
	return m.replayErr
}

func (m *mockMetaAppender) AppendRaw(frame []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]byte, len(frame))
	copy(cp, frame)
	m.rawEntries = append(m.rawEntries, cp)
	return nil
}

func (m *mockMetaAppender) ReplaceFromSnapshot(data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.snapshotData = make([]byte, len(data))
	copy(m.snapshotData, data)
	return nil
}

func (m *mockMetaAppender) SnapshotApplied() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.snapshotData != nil
}

func (m *mockMetaAppender) RawCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.rawEntries)
}

func (m *mockMetaAppender) ReplayCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.replayedEntries)
}

func makeTestWALEntry(seq uint64) []byte {
	entry := &wal.Entry{
		Sequence:  seq,
		TopicID:   [16]byte{1, 2, 3},
		Partition: 0,
		Offset:    int64(seq),
		Data:      make([]byte, 61), // minimum RecordBatch-like payload
	}
	return wal.MarshalEntry(entry)
}

func TestReceiverHelloSendsSequenceAndEpoch(t *testing.T) {
	primary, standby := net.Pipe()
	defer primary.Close() //nolint:errcheck
	defer standby.Close() //nolint:errcheck

	wa := &mockWALAppender{nextSeq: 42}
	ma := &mockMetaAppender{}
	r := NewReceiver(wa, ma, 7, slog.Default())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = r.RunOnConn(ctx, standby)
	}()

	// Read HELLO from the receiver
	msgType, payload, err := ReadFrame(primary)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if msgType != MsgHello {
		t.Fatalf("expected HELLO, got %#x", msgType)
	}

	lastSeq, epoch, err := UnmarshalHello(payload)
	if err != nil {
		t.Fatal(err)
	}
	if lastSeq != 41 { // nextSeq - 1
		t.Errorf("lastWALSeq: got %d, want 41", lastSeq)
	}
	if epoch != 7 {
		t.Errorf("epoch: got %d, want 7", epoch)
	}
}

func TestReceiverSnapshot(t *testing.T) {
	primary, standby := net.Pipe()
	defer primary.Close() //nolint:errcheck

	wa := &mockWALAppender{nextSeq: 0}
	ma := &mockMetaAppender{}
	r := NewReceiver(wa, ma, 0, slog.Default())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = r.RunOnConn(ctx, standby)
	}()

	// Read HELLO
	if _, _, err := ReadFrame(primary); err != nil {
		t.Fatal(err)
	}

	// Send SNAPSHOT
	metaData := []byte("snapshot-metadata-contents")
	snapshot := MarshalSnapshot(100, metaData)
	if err := WriteFrame(primary, MsgSnapshot, snapshot); err != nil {
		t.Fatal(err)
	}

	// Give the receiver time to process
	time.Sleep(100 * time.Millisecond)

	if !ma.SnapshotApplied() {
		t.Fatal("snapshot was not applied")
	}

	ma.mu.Lock()
	if !bytes.Equal(ma.snapshotData, metaData) {
		t.Errorf("snapshot data: got %q, want %q", ma.snapshotData, metaData)
	}
	ma.mu.Unlock()

	if wa.NextSequence() != 100 {
		t.Errorf("nextSeq: got %d, want 100", wa.NextSequence())
	}
}

func TestReceiverWALBatch(t *testing.T) {
	primary, standby := net.Pipe()
	defer primary.Close() //nolint:errcheck

	wa := &mockWALAppender{nextSeq: 0}
	ma := &mockMetaAppender{}
	r := NewReceiver(wa, ma, 0, slog.Default())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = r.RunOnConn(ctx, standby)
	}()

	// Read HELLO
	if _, _, err := ReadFrame(primary); err != nil {
		t.Fatal(err)
	}

	// Build 3 WAL entries
	var entriesBuf bytes.Buffer
	for i := uint64(0); i < 3; i++ {
		entriesBuf.Write(makeTestWALEntry(i))
	}

	batch := MarshalWALBatch(0, 2, 3, entriesBuf.Bytes())
	if err := WriteFrame(primary, MsgWALBatch, batch); err != nil {
		t.Fatal(err)
	}

	// Read ACK
	msgType, payload, err := ReadFrame(primary)
	if err != nil {
		t.Fatalf("ReadFrame ACK: %v", err)
	}
	if msgType != MsgACK {
		t.Fatalf("expected ACK, got %#x", msgType)
	}
	ackSeq, err := UnmarshalACK(payload)
	if err != nil {
		t.Fatal(err)
	}
	if ackSeq != 2 {
		t.Errorf("ACK seq: got %d, want 2", ackSeq)
	}

	if wa.EntryCount() != 3 {
		t.Errorf("entries written: got %d, want 3", wa.EntryCount())
	}
	if wa.NextSequence() != 3 {
		t.Errorf("nextSeq: got %d, want 3", wa.NextSequence())
	}
}

func TestReceiverWALBatchDuplicateSkip(t *testing.T) {
	primary, standby := net.Pipe()
	defer primary.Close() //nolint:errcheck

	wa := &mockWALAppender{nextSeq: 3} // Already has entries 0-2
	ma := &mockMetaAppender{}
	r := NewReceiver(wa, ma, 0, slog.Default())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = r.RunOnConn(ctx, standby)
	}()

	// Read HELLO
	if _, _, err := ReadFrame(primary); err != nil {
		t.Fatal(err)
	}

	// Send entries with seq 0-2 (duplicates)
	var entriesBuf bytes.Buffer
	for i := uint64(0); i < 3; i++ {
		entriesBuf.Write(makeTestWALEntry(i))
	}

	batch := MarshalWALBatch(0, 2, 3, entriesBuf.Bytes())
	if err := WriteFrame(primary, MsgWALBatch, batch); err != nil {
		t.Fatal(err)
	}

	// Still should get an ACK
	msgType, _, err := ReadFrame(primary)
	if err != nil {
		t.Fatalf("ReadFrame ACK: %v", err)
	}
	if msgType != MsgACK {
		t.Fatalf("expected ACK, got %#x", msgType)
	}

	// No entries should have been written (all duplicates)
	if wa.EntryCount() != 0 {
		t.Errorf("entries written: got %d, want 0", wa.EntryCount())
	}
}

func TestReceiverMetaEntry(t *testing.T) {
	primary, standby := net.Pipe()
	defer primary.Close() //nolint:errcheck

	wa := &mockWALAppender{nextSeq: 0}
	ma := &mockMetaAppender{}
	r := NewReceiver(wa, ma, 0, slog.Default())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = r.RunOnConn(ctx, standby)
	}()

	// Read HELLO
	if _, _, err := ReadFrame(primary); err != nil {
		t.Fatal(err)
	}

	entry := []byte("meta-entry-frame-data")
	if err := WriteFrame(primary, MsgMetaEntry, entry); err != nil {
		t.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond)

	if ma.RawCount() != 1 {
		t.Errorf("raw entries: got %d, want 1", ma.RawCount())
	}
	if ma.ReplayCount() != 1 {
		t.Errorf("replayed entries: got %d, want 1", ma.ReplayCount())
	}
}

func TestReceiverKeepalive(t *testing.T) {
	primary, standby := net.Pipe()
	defer primary.Close() //nolint:errcheck

	wa := &mockWALAppender{nextSeq: 0}
	ma := &mockMetaAppender{}
	r := NewReceiver(wa, ma, 0, slog.Default())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = r.RunOnConn(ctx, standby)
	}()

	// Read HELLO
	if _, _, err := ReadFrame(primary); err != nil {
		t.Fatal(err)
	}

	// Send keepalive (empty WAL_BATCH)
	keepalive := MarshalWALBatch(0, 0, 0, nil)
	if err := WriteFrame(primary, MsgWALBatch, keepalive); err != nil {
		t.Fatal(err)
	}

	// Send a real batch after keepalive to verify the receiver still works
	var entriesBuf bytes.Buffer
	entriesBuf.Write(makeTestWALEntry(0))
	batch := MarshalWALBatch(0, 0, 1, entriesBuf.Bytes())
	if err := WriteFrame(primary, MsgWALBatch, batch); err != nil {
		t.Fatal(err)
	}

	// Read ACK from the real batch (no ACK for keepalive)
	msgType, _, err := ReadFrame(primary)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if msgType != MsgACK {
		t.Fatalf("expected ACK, got %#x", msgType)
	}
}

func TestReceiverUnknownMessageType(t *testing.T) {
	primary, standby := net.Pipe()
	defer primary.Close() //nolint:errcheck

	wa := &mockWALAppender{nextSeq: 0}
	ma := &mockMetaAppender{}
	r := NewReceiver(wa, ma, 0, slog.Default())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = r.RunOnConn(ctx, standby)
	}()

	// Read HELLO
	if _, _, err := ReadFrame(primary); err != nil {
		t.Fatal(err)
	}

	// Send unknown message type 0xFF
	if err := WriteFrame(primary, 0xFF, []byte("unknown-data")); err != nil {
		t.Fatal(err)
	}

	// Send a real batch to verify the receiver continues
	var entriesBuf bytes.Buffer
	entriesBuf.Write(makeTestWALEntry(0))
	batch := MarshalWALBatch(0, 0, 1, entriesBuf.Bytes())
	if err := WriteFrame(primary, MsgWALBatch, batch); err != nil {
		t.Fatal(err)
	}

	// Read ACK
	msgType, _, err := ReadFrame(primary)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if msgType != MsgACK {
		t.Fatalf("expected ACK, got %#x", msgType)
	}
}

func TestReceiverDisconnect(t *testing.T) {
	primary, standby := net.Pipe()

	wa := &mockWALAppender{nextSeq: 0}
	ma := &mockMetaAppender{}
	r := NewReceiver(wa, ma, 0, slog.Default())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- r.RunOnConn(ctx, standby)
	}()

	// Read HELLO
	if _, _, err := ReadFrame(primary); err != nil {
		t.Fatal(err)
	}

	// Close primary side
	_ = primary.Close()

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected error on disconnect, got nil")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for disconnect error")
	}
}

func TestReceiverReconnect(t *testing.T) {
	wa := &mockWALAppender{nextSeq: 0}
	ma := &mockMetaAppender{}
	r := NewReceiver(wa, ma, 7, slog.Default())

	// First session: receive some entries
	primary1, standby1 := net.Pipe()
	ctx1, cancel1 := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() {
		errCh <- r.RunOnConn(ctx1, standby1)
	}()

	// Read HELLO
	if _, _, err := ReadFrame(primary1); err != nil {
		t.Fatal(err)
	}

	// Send 3 entries
	var entriesBuf bytes.Buffer
	for i := uint64(0); i < 3; i++ {
		entriesBuf.Write(makeTestWALEntry(i))
	}
	batch := MarshalWALBatch(0, 2, 3, entriesBuf.Bytes())
	if err := WriteFrame(primary1, MsgWALBatch, batch); err != nil {
		t.Fatal(err)
	}

	// Read ACK
	if _, _, err := ReadFrame(primary1); err != nil {
		t.Fatal(err)
	}

	// Disconnect
	cancel1()
	primary1.Close() //nolint:errcheck
	<-errCh

	// Second session: verify HELLO contains correct state
	primary2, standby2 := net.Pipe()
	defer primary2.Close() //nolint:errcheck

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()

	go func() {
		_ = r.RunOnConn(ctx2, standby2)
	}()

	// Read HELLO
	msgType, payload, err := ReadFrame(primary2)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if msgType != MsgHello {
		t.Fatalf("expected HELLO, got %#x", msgType)
	}

	lastSeq, epoch, err := UnmarshalHello(payload)
	if err != nil {
		t.Fatal(err)
	}
	if lastSeq != 2 { // nextSeq(3) - 1
		t.Errorf("lastWALSeq: got %d, want 2", lastSeq)
	}
	if epoch != 7 {
		t.Errorf("epoch: got %d, want 7", epoch)
	}
}

func TestReceiverSnapshotThenImmediateWALBatch(t *testing.T) {
	primary, standby := net.Pipe()
	defer primary.Close() //nolint:errcheck

	wa := &mockWALAppender{nextSeq: 0}
	ma := &mockMetaAppender{}
	r := NewReceiver(wa, ma, 0, slog.Default())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = r.RunOnConn(ctx, standby)
	}()

	// Read HELLO
	if _, _, err := ReadFrame(primary); err != nil {
		t.Fatal(err)
	}

	// Queue a SNAPSHOT followed by WAL_BATCH on the pipe
	metaData := []byte("snapshot-data")
	if err := WriteFrame(primary, MsgSnapshot, MarshalSnapshot(10, metaData)); err != nil {
		t.Fatal(err)
	}

	var entriesBuf bytes.Buffer
	for i := uint64(10); i < 13; i++ {
		entriesBuf.Write(makeTestWALEntry(i))
	}
	if err := WriteFrame(primary, MsgWALBatch, MarshalWALBatch(10, 12, 3, entriesBuf.Bytes())); err != nil {
		t.Fatal(err)
	}

	// Read ACK for the WAL_BATCH
	msgType, payload, err := ReadFrame(primary)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if msgType != MsgACK {
		t.Fatalf("expected ACK, got %#x", msgType)
	}

	ackSeq, err := UnmarshalACK(payload)
	if err != nil {
		t.Fatal(err)
	}
	if ackSeq != 12 {
		t.Errorf("ACK seq: got %d, want 12", ackSeq)
	}

	if !ma.SnapshotApplied() {
		t.Fatal("snapshot was not applied")
	}

	if wa.EntryCount() != 3 {
		t.Errorf("entries written: got %d, want 3", wa.EntryCount())
	}

	if wa.NextSequence() != 13 {
		t.Errorf("nextSeq: got %d, want 13", wa.NextSequence())
	}
}
