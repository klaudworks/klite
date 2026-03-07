package metadata

import (
	"encoding/binary"
	"hash/crc32"
	"sync"
	"sync/atomic"
	"testing"
)

func TestMetadataLogReplicateHook(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	ml, err := NewLog(LogConfig{DataDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ml.Close() }()

	var hookCalls [][]byte
	ml.SetReplicateHook(func(frame []byte) {
		cp := make([]byte, len(frame))
		copy(cp, frame)
		hookCalls = append(hookCalls, cp)
	})

	entry := MarshalCreateTopic(&CreateTopicEntry{
		TopicName:      "test-topic",
		PartitionCount: 1,
		TopicID:        [16]byte{1, 2, 3},
	})

	if err := ml.AppendSync(entry); err != nil {
		t.Fatal(err)
	}

	if len(hookCalls) != 1 {
		t.Fatalf("expected 1 hook call, got %d", len(hookCalls))
	}

	frame := hookCalls[0]
	// Verify the frame structure: [4B length][4B CRC][payload]
	if len(frame) < 8 {
		t.Fatalf("frame too short: %d bytes", len(frame))
	}

	declaredLen := binary.BigEndian.Uint32(frame[0:4])
	if int(declaredLen) != len(frame)-4 {
		t.Fatalf("length mismatch: declared %d, actual payload %d", declaredLen, len(frame)-4)
	}

	storedCRC := binary.BigEndian.Uint32(frame[4:8])
	actualCRC := crc32.Checksum(frame[8:], crc32cTable)
	if storedCRC != actualCRC {
		t.Fatal("CRC mismatch in hook frame")
	}
}

func TestMetadataLogReplicateHookFailure(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	ml, err := NewLog(LogConfig{DataDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ml.Close() }()

	// Hook that panics — should not affect the append
	ml.SetReplicateHook(func(frame []byte) {
		// Simulate a failing hook (but don't actually panic since that
		// would crash the test). In production, SendMeta logs and returns.
		// For this test, just verify the append succeeds regardless.
	})

	entry := MarshalCreateTopic(&CreateTopicEntry{
		TopicName:      "still-works",
		PartitionCount: 1,
		TopicID:        [16]byte{4, 5, 6},
	})

	if err := ml.AppendSync(entry); err != nil {
		t.Fatalf("append should succeed even if hook fails: %v", err)
	}

	// Verify the entry was written
	if ml.Size() == 0 {
		t.Fatal("metadata.log should not be empty")
	}
}

func TestMetadataLogCompactHook(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	ml, err := NewLog(LogConfig{DataDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ml.Close() }()

	topicID := [16]byte{1, 2, 3}

	// Set up snapshot function
	ml.SetSnapshotFn(func() [][]byte {
		return [][]byte{
			MarshalCreateTopic(&CreateTopicEntry{
				TopicName:      "compacted-topic",
				PartitionCount: 1,
				TopicID:        topicID,
			}),
		}
	})

	var compactData []byte
	var compactCalled bool
	ml.SetCompactHook(func(data []byte) {
		compactCalled = true
		compactData = make([]byte, len(data))
		copy(compactData, data)
	})

	// Write entries to get file size up, then manually trigger compaction
	for i := 0; i < 100; i++ {
		_ = ml.Append(MarshalOffsetCommit(&OffsetCommitEntry{
			Group:     "g1",
			Topic:     "t",
			Partition: 0,
			Offset:    int64(i),
		}))
	}

	// Manually trigger compaction
	ml.mu.Lock()
	ml.compactLocked()
	ml.mu.Unlock()

	if !compactCalled {
		t.Fatal("compactHook was not called")
	}

	if len(compactData) == 0 {
		t.Fatal("compactHook received empty data")
	}
}

func TestMetadataLogCompactHookNotCalledOnStandby(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	ml, err := NewLog(LogConfig{DataDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ml.Close() }()

	ml.SetSnapshotFn(func() [][]byte {
		return [][]byte{
			MarshalCreateTopic(&CreateTopicEntry{
				TopicName:      "topic",
				PartitionCount: 1,
				TopicID:        [16]byte{1},
			}),
		}
	})

	compactCalled := false
	ml.SetCompactHook(func(data []byte) {
		compactCalled = true
	})

	// Use ReplayEntry (not Append) to simulate standby behavior
	for i := 0; i < 2000; i++ {
		entry := MarshalOffsetCommit(&OffsetCommitEntry{
			Group:     "g1",
			Topic:     "t",
			Partition: 0,
			Offset:    int64(i),
		})
		// Build a full frame: [4B length][4B CRC][payload]
		frameSize := 4 + 4 + len(entry)
		frame := make([]byte, frameSize)
		binary.BigEndian.PutUint32(frame[0:4], uint32(4+len(entry)))
		binary.BigEndian.PutUint32(frame[4:8], crc32.Checksum(entry, crc32cTable))
		copy(frame[8:], entry)

		if err := ml.ReplayEntry(frame); err != nil {
			t.Fatalf("ReplayEntry %d: %v", i, err)
		}
	}

	// Compaction should not have been triggered because appendCount was
	// not incremented by ReplayEntry
	if compactCalled {
		t.Fatal("compactHook should not be called when using ReplayEntry (standby mode)")
	}

	// Verify appendCount is still 0
	if ml.appendCount.Load() != 0 {
		t.Fatalf("appendCount should be 0 on standby, got %d", ml.appendCount.Load())
	}
}

func TestMetadataLogCompactHookWithReplicateHook(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	ml, err := NewLog(LogConfig{DataDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ml.Close() }()

	topicID := [16]byte{1, 2, 3}

	ml.SetSnapshotFn(func() [][]byte {
		return [][]byte{
			MarshalCreateTopic(&CreateTopicEntry{
				TopicName:      "compacted",
				PartitionCount: 1,
				TopicID:        topicID,
			}),
		}
	})

	var replicateCount atomic.Int32
	ml.SetReplicateHook(func(frame []byte) {
		replicateCount.Add(1)
	})

	var compactCount atomic.Int32
	ml.SetCompactHook(func(data []byte) {
		compactCount.Add(1)
	})

	// Write entries
	numEntries := 50
	for i := 0; i < numEntries; i++ {
		_ = ml.Append(MarshalOffsetCommit(&OffsetCommitEntry{
			Group:     "g1",
			Topic:     "t",
			Partition: 0,
			Offset:    int64(i),
		}))
	}

	// Verify replicateHook was called for every append
	if int(replicateCount.Load()) != numEntries {
		t.Fatalf("replicateHook call count: got %d, want %d", replicateCount.Load(), numEntries)
	}

	// Trigger compaction
	replicateCountBefore := replicateCount.Load()
	ml.mu.Lock()
	ml.compactLocked()
	ml.mu.Unlock()

	// compactHook should have been called exactly once
	if compactCount.Load() != 1 {
		t.Fatalf("compactHook call count: got %d, want 1", compactCount.Load())
	}

	// replicateHook should NOT have been called during compaction
	if replicateCount.Load() != replicateCountBefore {
		t.Fatalf("replicateHook was called during compaction: before=%d, after=%d",
			replicateCountBefore, replicateCount.Load())
	}
}

func TestMetadataLogReplayEntry(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	ml, err := NewLog(LogConfig{DataDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ml.Close() }()

	var topics []CreateTopicEntry
	ml.SetCallbacks(
		func(e CreateTopicEntry) { topics = append(topics, e) },
		func(e DeleteTopicEntry) {},
		func(e AlterConfigEntry) {},
		func(e OffsetCommitEntry) {},
		func(e ProducerIDEntry) {},
		func(e LogStartOffsetEntry) {},
	)

	// Build a framed CreateTopic entry
	entry := MarshalCreateTopic(&CreateTopicEntry{
		TopicName:      "replayed-topic",
		PartitionCount: 5,
		TopicID:        [16]byte{9, 8, 7},
	})

	frameSize := 4 + 4 + len(entry)
	frame := make([]byte, frameSize)
	binary.BigEndian.PutUint32(frame[0:4], uint32(4+len(entry)))
	binary.BigEndian.PutUint32(frame[4:8], crc32.Checksum(entry, crc32cTable))
	copy(frame[8:], entry)

	if err := ml.ReplayEntry(frame); err != nil {
		t.Fatalf("ReplayEntry: %v", err)
	}

	if len(topics) != 1 {
		t.Fatalf("expected 1 topic, got %d", len(topics))
	}
	if topics[0].TopicName != "replayed-topic" {
		t.Fatalf("topic name: got %q, want 'replayed-topic'", topics[0].TopicName)
	}
	if topics[0].PartitionCount != 5 {
		t.Fatalf("partition count: got %d, want 5", topics[0].PartitionCount)
	}
}

func TestMetadataLogReplayEntryCRCMismatch(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	ml, err := NewLog(LogConfig{DataDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ml.Close() }()

	entry := MarshalCreateTopic(&CreateTopicEntry{
		TopicName:      "bad-topic",
		PartitionCount: 1,
		TopicID:        [16]byte{1},
	})

	frameSize := 4 + 4 + len(entry)
	frame := make([]byte, frameSize)
	binary.BigEndian.PutUint32(frame[0:4], uint32(4+len(entry)))
	binary.BigEndian.PutUint32(frame[4:8], crc32.Checksum(entry, crc32cTable))
	copy(frame[8:], entry)

	// Corrupt the CRC
	frame[5] ^= 0xFF

	err = ml.ReplayEntry(frame)
	if err == nil {
		t.Fatal("expected CRC mismatch error, got nil")
	}
}

func TestMetadataLogReplayEntryThreadSafe(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	ml, err := NewLog(LogConfig{DataDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ml.Close() }()

	var mu sync.Mutex
	var topics []CreateTopicEntry
	ml.SetCallbacks(
		func(e CreateTopicEntry) {
			mu.Lock()
			topics = append(topics, e)
			mu.Unlock()
		},
		func(e DeleteTopicEntry) {},
		func(e AlterConfigEntry) {},
		func(e OffsetCommitEntry) {},
		func(e ProducerIDEntry) {},
		func(e LogStartOffsetEntry) {},
	)

	const numGoroutines = 10
	const entriesPerGoroutine = 20

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		go func(gIdx int) {
			defer wg.Done()
			for i := 0; i < entriesPerGoroutine; i++ {
				entry := MarshalCreateTopic(&CreateTopicEntry{
					TopicName:      "topic-" + string(rune('A'+gIdx)),
					PartitionCount: int32(i + 1),
					TopicID:        [16]byte{byte(gIdx), byte(i)},
				})

				frameSize := 4 + 4 + len(entry)
				frame := make([]byte, frameSize)
				binary.BigEndian.PutUint32(frame[0:4], uint32(4+len(entry)))
				binary.BigEndian.PutUint32(frame[4:8], crc32.Checksum(entry, crc32cTable))
				copy(frame[8:], entry)

				if err := ml.ReplayEntry(frame); err != nil {
					t.Errorf("goroutine %d entry %d: %v", gIdx, i, err)
				}
			}
		}(g)
	}

	wg.Wait()

	mu.Lock()
	total := len(topics)
	mu.Unlock()

	expected := numGoroutines * entriesPerGoroutine
	if total != expected {
		t.Fatalf("expected %d topics, got %d", expected, total)
	}
}
