package wal

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/klaudworks/klite/internal/clock"
)

// makeTestBatch creates a minimal RecordBatch for testing.
func makeTestBatch(numRecords int32, maxTimestamp int64) []byte {
	lastDelta := numRecords - 1
	if lastDelta < 0 {
		lastDelta = 0
	}
	raw := make([]byte, 61)
	binary.BigEndian.PutUint32(raw[8:12], 49) // batchLength
	raw[16] = 2                                // magic
	binary.BigEndian.PutUint32(raw[23:27], uint32(lastDelta))
	binary.BigEndian.PutUint64(raw[27:35], uint64(1000))
	binary.BigEndian.PutUint64(raw[35:43], uint64(maxTimestamp))
	binary.BigEndian.PutUint64(raw[43:51], ^uint64(0)) // producerID = -1
	binary.BigEndian.PutUint16(raw[51:53], ^uint16(0))  // producerEpoch = -1
	binary.BigEndian.PutUint32(raw[53:57], ^uint32(0))  // baseSequence = -1
	binary.BigEndian.PutUint32(raw[57:61], uint32(numRecords))
	return raw
}

func TestMarshalUnmarshalEntry(t *testing.T) {
	t.Parallel()

	topicID := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	data := makeTestBatch(5, 2000)

	entry := &Entry{
		Sequence:  42,
		TopicID:   topicID,
		Partition: 3,
		Offset:    100,
		Data:      data,
	}

	serialized := MarshalEntry(entry)

	// Skip 4-byte length prefix, parse the rest
	parsed, err := UnmarshalEntry(serialized[4:])
	if err != nil {
		t.Fatalf("UnmarshalEntry failed: %v", err)
	}

	if parsed.Sequence != 42 {
		t.Errorf("Sequence: got %d, want 42", parsed.Sequence)
	}
	if parsed.TopicID != topicID {
		t.Errorf("TopicID mismatch")
	}
	if parsed.Partition != 3 {
		t.Errorf("Partition: got %d, want 3", parsed.Partition)
	}
	if parsed.Offset != 100 {
		t.Errorf("Offset: got %d, want 100", parsed.Offset)
	}
	if !bytes.Equal(parsed.Data, data) {
		t.Errorf("Data mismatch")
	}
}

func TestEntryCorruptedCRC(t *testing.T) {
	t.Parallel()

	entry := &Entry{
		Sequence:  1,
		TopicID:   [16]byte{},
		Partition: 0,
		Offset:    0,
		Data:      makeTestBatch(1, 1000),
	}

	serialized := MarshalEntry(entry)

	// Corrupt CRC (bytes 4-7)
	serialized[5] ^= 0xFF

	_, err := UnmarshalEntry(serialized[4:])
	if err != ErrCRCMismatch {
		t.Errorf("expected ErrCRCMismatch, got %v", err)
	}
}

func TestScanFramedEntries(t *testing.T) {
	t.Parallel()

	// Create a buffer with multiple entries
	var buf bytes.Buffer
	topicID := [16]byte{1, 2, 3}

	for i := 0; i < 5; i++ {
		entry := &Entry{
			Sequence:  uint64(i),
			TopicID:   topicID,
			Partition: 0,
			Offset:    int64(i * 10),
			Data:      makeTestBatch(1, int64(1000+i)),
		}
		serialized := MarshalEntry(entry)
		buf.Write(serialized)
	}

	// Scan
	var entries []Entry
	count, err := ScanFramedEntries(&buf, func(payload []byte) bool {
		entry, parseErr := UnmarshalEntry(payload)
		if parseErr != nil {
			return false
		}
		entries = append(entries, entry)
		return true
	})

	if err != nil {
		t.Fatalf("ScanFramedEntries error: %v", err)
	}
	if count != 5 {
		t.Errorf("count: got %d, want 5", count)
	}
	if len(entries) != 5 {
		t.Fatalf("entries: got %d, want 5", len(entries))
	}

	for i, e := range entries {
		if e.Offset != int64(i*10) {
			t.Errorf("entry %d offset: got %d, want %d", i, e.Offset, i*10)
		}
	}
}

func TestScanFramedEntriesCorrupted(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	topicID := [16]byte{1, 2, 3}

	// Write 3 valid entries
	for i := 0; i < 3; i++ {
		entry := &Entry{
			Sequence:  uint64(i),
			TopicID:   topicID,
			Partition: 0,
			Offset:    int64(i),
			Data:      makeTestBatch(1, 1000),
		}
		buf.Write(MarshalEntry(entry))
	}

	// Write corrupted entry (bad CRC)
	badEntry := MarshalEntry(&Entry{
		Sequence: 3, TopicID: topicID, Partition: 0, Offset: 3,
		Data: makeTestBatch(1, 1000),
	})
	badEntry[5] ^= 0xFF // corrupt CRC
	buf.Write(badEntry)

	// Write another valid entry (should NOT be reached)
	buf.Write(MarshalEntry(&Entry{
		Sequence: 4, TopicID: topicID, Partition: 0, Offset: 4,
		Data: makeTestBatch(1, 1000),
	}))

	var entries []Entry
	count, _ := ScanFramedEntries(&buf, func(payload []byte) bool {
		entry, parseErr := UnmarshalEntry(payload)
		if parseErr != nil {
			return false // stop at corruption
		}
		entries = append(entries, entry)
		return true
	})

	// Should get 3 valid entries, then stop at corrupted one
	if count != 4 { // 3 valid + 1 corrupted (fn returned false)
		t.Errorf("count: got %d, want 4", count)
	}
	if len(entries) != 3 {
		t.Errorf("valid entries: got %d, want 3", len(entries))
	}
}

func TestWriterBasic(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")

	idx := NewIndex()
	cfg := WriterConfig{
		Dir:             walDir,
		SyncInterval:    1 * time.Millisecond,
		SegmentMaxBytes: 64 * 1024 * 1024,
		FsyncEnabled:    false, // skip fsync in tests
		Clock:           clock.RealClock{},
	}

	w, err := NewWriter(cfg, idx)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	if err := w.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer w.Stop()

	topicID := [16]byte{10, 20, 30}
	data := makeTestBatch(3, 2000)

	entry := &Entry{
		TopicID:   topicID,
		Partition: 0,
		Offset:    0,
		Data:      data,
	}

	if err := w.Append(entry); err != nil {
		t.Fatalf("Append: %v", err)
	}

	// Verify index was updated
	tp := TopicPartition{TopicID: topicID, Partition: 0}
	entries := idx.Lookup(tp, 0, 1024*1024)
	if len(entries) != 1 {
		t.Fatalf("index entries: got %d, want 1", len(entries))
	}
	if entries[0].BaseOffset != 0 {
		t.Errorf("BaseOffset: got %d, want 0", entries[0].BaseOffset)
	}
	if entries[0].LastOffset != 2 { // 3 records: offsets 0,1,2
		t.Errorf("LastOffset: got %d, want 2", entries[0].LastOffset)
	}
}

func TestWriterMultipleEntries(t *testing.T) {
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
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer w.Stop()

	topicID := [16]byte{1, 2, 3}

	for i := 0; i < 10; i++ {
		entry := &Entry{
			TopicID:   topicID,
			Partition: 0,
			Offset:    int64(i * 5),
			Data:      makeTestBatch(5, int64(1000+i)),
		}
		if err := w.Append(entry); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}

	tp := TopicPartition{TopicID: topicID, Partition: 0}
	entries := idx.Lookup(tp, 0, 1024*1024)
	if len(entries) != 10 {
		t.Fatalf("index entries: got %d, want 10", len(entries))
	}
}

func TestWriterConcurrentAppends(t *testing.T) {
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
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer w.Stop()

	topicID := [16]byte{5, 6, 7}
	const numGoroutines = 10
	const entriesPerGoroutine = 20

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		go func(gIdx int) {
			defer wg.Done()
			for i := 0; i < entriesPerGoroutine; i++ {
				entry := &Entry{
					TopicID:   topicID,
					Partition: int32(gIdx),
					Offset:    int64(i),
					Data:      makeTestBatch(1, 1000),
				}
				if err := w.Append(entry); err != nil {
					t.Errorf("goroutine %d append %d: %v", gIdx, i, err)
					return
				}
			}
		}(g)
	}

	wg.Wait()

	// Verify all entries were indexed
	for g := 0; g < numGoroutines; g++ {
		tp := TopicPartition{TopicID: topicID, Partition: int32(g)}
		entries := idx.Lookup(tp, 0, 1024*1024)
		if len(entries) != entriesPerGoroutine {
			t.Errorf("goroutine %d: got %d entries, want %d", g, len(entries), entriesPerGoroutine)
		}
	}
}

func TestWriterReadBatch(t *testing.T) {
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
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer w.Stop()

	topicID := [16]byte{1, 2, 3}
	originalData := makeTestBatch(5, 3000)

	entry := &Entry{
		TopicID:   topicID,
		Partition: 0,
		Offset:    0,
		Data:      originalData,
	}
	if err := w.Append(entry); err != nil {
		t.Fatalf("Append: %v", err)
	}

	// Read back via index
	tp := TopicPartition{TopicID: topicID, Partition: 0}
	entries := idx.Lookup(tp, 0, 1024*1024)
	if len(entries) != 1 {
		t.Fatalf("entries: got %d, want 1", len(entries))
	}

	readData, err := w.ReadBatch(entries[0])
	if err != nil {
		t.Fatalf("ReadBatch: %v", err)
	}

	if !bytes.Equal(readData, originalData) {
		t.Errorf("ReadBatch data mismatch: got %d bytes, want %d bytes", len(readData), len(originalData))
	}
}

func TestWriterSegmentRotation(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")

	idx := NewIndex()
	// Use very small segment size to force rotation
	w, err := NewWriter(WriterConfig{
		Dir:             walDir,
		SyncInterval:    1 * time.Millisecond,
		SegmentMaxBytes: 200, // tiny segment
		FsyncEnabled:    false,
		Clock:           clock.RealClock{},
	}, idx)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer w.Stop()

	topicID := [16]byte{1, 2, 3}

	// Append enough entries to trigger rotation
	for i := 0; i < 10; i++ {
		entry := &Entry{
			TopicID:   topicID,
			Partition: 0,
			Offset:    int64(i),
			Data:      makeTestBatch(1, 1000),
		}
		if err := w.Append(entry); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}

	// Verify multiple segments were created
	files, err := os.ReadDir(walDir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}

	walFiles := 0
	for _, f := range files {
		if !f.IsDir() {
			walFiles++
		}
	}

	if walFiles < 2 {
		t.Errorf("expected at least 2 segment files, got %d", walFiles)
	}
}

func TestWriterReplay(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")

	topicID := [16]byte{1, 2, 3}

	// Phase 1: Write some entries
	{
		idx := NewIndex()
		w, err := NewWriter(WriterConfig{
			Dir:             walDir,
			SyncInterval:    1 * time.Millisecond,
			SegmentMaxBytes: 64 * 1024 * 1024,
			FsyncEnabled:    false,
			Clock:           clock.RealClock{},
		}, idx)
		if err != nil {
			t.Fatalf("NewWriter: %v", err)
		}
		if err := w.Start(); err != nil {
			t.Fatalf("Start: %v", err)
		}

		for i := 0; i < 5; i++ {
			entry := &Entry{
				TopicID:   topicID,
				Partition: 0,
				Offset:    int64(i * 3),
				Data:      makeTestBatch(3, int64(1000+i)),
			}
			if err := w.Append(entry); err != nil {
				t.Fatalf("Append %d: %v", i, err)
			}
		}

		w.Stop()
	}

	// Phase 2: Replay and verify
	{
		idx := NewIndex()
		w, err := NewWriter(WriterConfig{
			Dir:             walDir,
			SyncInterval:    1 * time.Millisecond,
			SegmentMaxBytes: 64 * 1024 * 1024,
			FsyncEnabled:    false,
			Clock:           clock.RealClock{},
		}, idx)
		if err != nil {
			t.Fatalf("NewWriter (replay): %v", err)
		}

		var replayedEntries []Entry
		err = w.Replay(func(entry Entry, segmentSeq uint64, fileOffset int64) error {
			replayedEntries = append(replayedEntries, entry)
			return nil
		})
		if err != nil {
			t.Fatalf("Replay: %v", err)
		}

		if len(replayedEntries) != 5 {
			t.Fatalf("replayed entries: got %d, want 5", len(replayedEntries))
		}

		for i, e := range replayedEntries {
			expectedOffset := int64(i * 3)
			if e.Offset != expectedOffset {
				t.Errorf("entry %d offset: got %d, want %d", i, e.Offset, expectedOffset)
			}
			if e.TopicID != topicID {
				t.Errorf("entry %d topicID mismatch", i)
			}
		}

		// Verify next sequence was properly set
		if w.NextSequence() != 5 {
			t.Errorf("NextSequence: got %d, want 5", w.NextSequence())
		}
	}
}

func TestSegmentFilename(t *testing.T) {
	t.Parallel()

	tests := []struct {
		seq  uint64
		want string
	}{
		{0, "00000000000000000000.wal"},
		{1, "00000000000000000001.wal"},
		{1024, "00000000000000001024.wal"},
		{99999999999999999, "00099999999999999999.wal"},
	}

	for _, tt := range tests {
		got := segmentFilename(tt.seq)
		if got != tt.want {
			t.Errorf("segmentFilename(%d): got %q, want %q", tt.seq, got, tt.want)
		}

		parsed, ok := parseSegmentFilename(got)
		if !ok {
			t.Errorf("parseSegmentFilename(%q): not ok", got)
			continue
		}
		if parsed != tt.seq {
			t.Errorf("parseSegmentFilename(%q): got %d, want %d", got, parsed, tt.seq)
		}
	}
}

func TestIndexLookup(t *testing.T) {
	t.Parallel()

	idx := NewIndex()
	tp := TopicPartition{TopicID: [16]byte{1}, Partition: 0}

	// Add entries: offsets 0-4, 5-9, 10-14
	idx.Add(tp, IndexEntry{BaseOffset: 0, LastOffset: 4, BatchSize: 100})
	idx.Add(tp, IndexEntry{BaseOffset: 5, LastOffset: 9, BatchSize: 100})
	idx.Add(tp, IndexEntry{BaseOffset: 10, LastOffset: 14, BatchSize: 100})

	t.Run("fetch from start", func(t *testing.T) {
		entries := idx.Lookup(tp, 0, 1024)
		if len(entries) != 3 {
			t.Errorf("got %d entries, want 3", len(entries))
		}
	})

	t.Run("fetch from middle", func(t *testing.T) {
		entries := idx.Lookup(tp, 5, 1024)
		if len(entries) != 2 {
			t.Errorf("got %d entries, want 2", len(entries))
		}
		if entries[0].BaseOffset != 5 {
			t.Errorf("first entry base: got %d, want 5", entries[0].BaseOffset)
		}
	})

	t.Run("fetch within batch", func(t *testing.T) {
		entries := idx.Lookup(tp, 7, 1024) // mid-batch
		if len(entries) != 2 {
			t.Errorf("got %d entries, want 2", len(entries))
		}
		if entries[0].BaseOffset != 5 {
			t.Errorf("first entry base: got %d, want 5", entries[0].BaseOffset)
		}
	})

	t.Run("maxBytes limits", func(t *testing.T) {
		entries := idx.Lookup(tp, 0, 150) // first batch 100 bytes, adding second (200 total) > 150
		if len(entries) != 1 { // KIP-74: first batch always included, but second exceeds limit
			t.Errorf("got %d entries, want 1", len(entries))
		}
	})

	t.Run("fetch past end", func(t *testing.T) {
		entries := idx.Lookup(tp, 15, 1024)
		if len(entries) != 0 {
			t.Errorf("got %d entries, want 0", len(entries))
		}
	})
}

func TestIndexPruneBefore(t *testing.T) {
	t.Parallel()

	idx := NewIndex()
	tp := TopicPartition{TopicID: [16]byte{1}, Partition: 0}

	idx.Add(tp, IndexEntry{BaseOffset: 0, LastOffset: 4})
	idx.Add(tp, IndexEntry{BaseOffset: 5, LastOffset: 9})
	idx.Add(tp, IndexEntry{BaseOffset: 10, LastOffset: 14})

	idx.PruneBefore(tp, 5) // should remove entry with LastOffset=4

	entries := idx.Lookup(tp, 0, 1024*1024)
	if len(entries) != 2 {
		t.Errorf("after prune: got %d entries, want 2", len(entries))
	}
	if entries[0].BaseOffset != 5 {
		t.Errorf("first entry after prune: got base %d, want 5", entries[0].BaseOffset)
	}
}
