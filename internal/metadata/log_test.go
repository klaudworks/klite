package metadata

import (
	"os"
	"path/filepath"
	"testing"
)

func TestEntryRoundTrip_CreateTopic(t *testing.T) {
	t.Parallel()
	e := CreateTopicEntry{
		TopicName:      "my-topic",
		PartitionCount: 3,
		TopicID:        [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		Configs:        map[string]string{"retention.ms": "86400000", "cleanup.policy": "delete"},
	}
	buf := MarshalCreateTopic(&e)

	if buf[0] != EntryCreateTopic {
		t.Fatalf("expected type %x, got %x", EntryCreateTopic, buf[0])
	}

	got, err := UnmarshalCreateTopic(buf[1:])
	if err != nil {
		t.Fatal(err)
	}
	if got.TopicName != e.TopicName {
		t.Fatalf("TopicName mismatch: %q != %q", got.TopicName, e.TopicName)
	}
	if got.PartitionCount != e.PartitionCount {
		t.Fatalf("PartitionCount mismatch: %d != %d", got.PartitionCount, e.PartitionCount)
	}
	if got.TopicID != e.TopicID {
		t.Fatal("TopicID mismatch")
	}
	if len(got.Configs) != len(e.Configs) {
		t.Fatalf("Configs len mismatch: %d != %d", len(got.Configs), len(e.Configs))
	}
	for k, v := range e.Configs {
		if got.Configs[k] != v {
			t.Fatalf("Config %q: %q != %q", k, got.Configs[k], v)
		}
	}
}

func TestEntryRoundTrip_DeleteTopic(t *testing.T) {
	t.Parallel()
	e := DeleteTopicEntry{TopicName: "dead-topic"}
	buf := MarshalDeleteTopic(&e)
	got, err := UnmarshalDeleteTopic(buf[1:])
	if err != nil {
		t.Fatal(err)
	}
	if got.TopicName != e.TopicName {
		t.Fatalf("TopicName mismatch: %q != %q", got.TopicName, e.TopicName)
	}
}

func TestEntryRoundTrip_AlterConfig(t *testing.T) {
	t.Parallel()
	e := AlterConfigEntry{TopicName: "conf-topic", Key: "retention.ms", Value: "3600000"}
	buf := MarshalAlterConfig(&e)
	got, err := UnmarshalAlterConfig(buf[1:])
	if err != nil {
		t.Fatal(err)
	}
	if got.TopicName != e.TopicName || got.Key != e.Key || got.Value != e.Value {
		t.Fatalf("mismatch: got %+v, want %+v", got, e)
	}
}

func TestEntryRoundTrip_OffsetCommit(t *testing.T) {
	t.Parallel()
	e := OffsetCommitEntry{
		Group:     "test-group",
		Topic:     "test-topic",
		Partition: 5,
		Offset:    12345,
		Metadata:  "some-meta",
	}
	buf := MarshalOffsetCommit(&e)
	got, err := UnmarshalOffsetCommit(buf[1:])
	if err != nil {
		t.Fatal(err)
	}
	if got.Group != e.Group || got.Topic != e.Topic || got.Partition != e.Partition ||
		got.Offset != e.Offset || got.Metadata != e.Metadata {
		t.Fatalf("mismatch: got %+v, want %+v", got, e)
	}
}

func TestEntryRoundTrip_ProducerID(t *testing.T) {
	t.Parallel()
	e := ProducerIDEntry{NextProducerID: 42}
	buf := MarshalProducerID(&e)
	got, err := UnmarshalProducerID(buf[1:])
	if err != nil {
		t.Fatal(err)
	}
	if got.NextProducerID != e.NextProducerID {
		t.Fatalf("NextProducerID mismatch: %d != %d", got.NextProducerID, e.NextProducerID)
	}
}

func TestEntryRoundTrip_LogStartOffset(t *testing.T) {
	t.Parallel()
	e := LogStartOffsetEntry{TopicName: "ls-topic", Partition: 2, LogStartOffset: 1000}
	buf := MarshalLogStartOffset(&e)
	got, err := UnmarshalLogStartOffset(buf[1:])
	if err != nil {
		t.Fatal(err)
	}
	if got.TopicName != e.TopicName || got.Partition != e.Partition || got.LogStartOffset != e.LogStartOffset {
		t.Fatalf("mismatch: got %+v, want %+v", got, e)
	}
}

func TestLogAppendAndReplay(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	ml, err := NewLog(LogConfig{DataDir: dir})
	if err != nil {
		t.Fatal(err)
	}

	// Write some entries
	topicID := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	err = ml.AppendSync(MarshalCreateTopic(&CreateTopicEntry{
		TopicName:      "test-topic",
		PartitionCount: 3,
		TopicID:        topicID,
		Configs:        map[string]string{"retention.ms": "86400000"},
	}))
	if err != nil {
		t.Fatal(err)
	}

	err = ml.Append(MarshalOffsetCommit(&OffsetCommitEntry{
		Group:     "g1",
		Topic:     "test-topic",
		Partition: 0,
		Offset:    42,
		Metadata:  "",
	}))
	if err != nil {
		t.Fatal(err)
	}

	err = ml.AppendSync(MarshalLogStartOffset(&LogStartOffsetEntry{
		TopicName:      "test-topic",
		Partition:      0,
		LogStartOffset: 10,
	}))
	if err != nil {
		t.Fatal(err)
	}

	ml.Close()

	// Replay
	ml2, err := NewLog(LogConfig{DataDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer ml2.Close()

	var topics []CreateTopicEntry
	var offsets []OffsetCommitEntry
	var logStarts []LogStartOffsetEntry

	ml2.SetCallbacks(
		func(e CreateTopicEntry) { topics = append(topics, e) },
		func(e DeleteTopicEntry) {},
		func(e AlterConfigEntry) {},
		func(e OffsetCommitEntry) { offsets = append(offsets, e) },
		func(e ProducerIDEntry) {},
		func(e LogStartOffsetEntry) { logStarts = append(logStarts, e) },
	)

	count, err := ml2.Replay()
	if err != nil {
		t.Fatal(err)
	}

	if count != 3 {
		t.Fatalf("expected 3 entries, got %d", count)
	}
	if len(topics) != 1 {
		t.Fatalf("expected 1 topic, got %d", len(topics))
	}
	if topics[0].TopicName != "test-topic" {
		t.Fatalf("unexpected topic name: %s", topics[0].TopicName)
	}
	if topics[0].PartitionCount != 3 {
		t.Fatalf("unexpected partition count: %d", topics[0].PartitionCount)
	}
	if topics[0].TopicID != topicID {
		t.Fatal("topic ID mismatch")
	}
	if topics[0].Configs["retention.ms"] != "86400000" {
		t.Fatalf("unexpected retention.ms: %s", topics[0].Configs["retention.ms"])
	}

	if len(offsets) != 1 {
		t.Fatalf("expected 1 offset, got %d", len(offsets))
	}
	if offsets[0].Group != "g1" || offsets[0].Offset != 42 {
		t.Fatalf("unexpected offset: %+v", offsets[0])
	}

	if len(logStarts) != 1 {
		t.Fatalf("expected 1 logStart, got %d", len(logStarts))
	}
	if logStarts[0].LogStartOffset != 10 {
		t.Fatalf("unexpected logStartOffset: %d", logStarts[0].LogStartOffset)
	}
}

func TestLogCompaction(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	ml, err := NewLog(LogConfig{DataDir: dir})
	if err != nil {
		t.Fatal(err)
	}

	topicID := [16]byte{1, 2, 3, 4, 5, 6, 7, 8}

	// Write many entries to exceed compaction threshold...
	// But we'll test the compaction logic directly instead of hitting the size threshold.
	for i := 0; i < 100; i++ {
		err = ml.Append(MarshalOffsetCommit(&OffsetCommitEntry{
			Group:     "g1",
			Topic:     "test-topic",
			Partition: 0,
			Offset:    int64(i),
		}))
		if err != nil {
			t.Fatal(err)
		}
	}

	// Create a topic
	err = ml.AppendSync(MarshalCreateTopic(&CreateTopicEntry{
		TopicName:      "test-topic",
		PartitionCount: 1,
		TopicID:        topicID,
		Configs:        map[string]string{"retention.ms": "1000"},
	}))
	if err != nil {
		t.Fatal(err)
	}

	sizeBeforeCompaction := ml.Size()

	// Set up snapshot function that returns only current state
	ml.SetSnapshotFn(func() [][]byte {
		return [][]byte{
			MarshalCreateTopic(&CreateTopicEntry{
				TopicName:      "test-topic",
				PartitionCount: 1,
				TopicID:        topicID,
				Configs:        map[string]string{"retention.ms": "1000"},
			}),
			MarshalOffsetCommit(&OffsetCommitEntry{
				Group:     "g1",
				Topic:     "test-topic",
				Partition: 0,
				Offset:    99, // latest only
			}),
		}
	})

	// Force compaction by manually calling
	ml.mu.Lock()
	ml.compactLocked()
	ml.mu.Unlock()

	sizeAfterCompaction := ml.Size()
	if sizeAfterCompaction >= sizeBeforeCompaction {
		t.Fatalf("compaction did not reduce size: before=%d after=%d", sizeBeforeCompaction, sizeAfterCompaction)
	}

	ml.Close()

	// Verify replay after compaction produces correct state
	ml2, err := NewLog(LogConfig{DataDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer ml2.Close()

	var topics []CreateTopicEntry
	var offsets []OffsetCommitEntry

	ml2.SetCallbacks(
		func(e CreateTopicEntry) { topics = append(topics, e) },
		func(e DeleteTopicEntry) {},
		func(e AlterConfigEntry) {},
		func(e OffsetCommitEntry) { offsets = append(offsets, e) },
		func(e ProducerIDEntry) {},
		func(e LogStartOffsetEntry) {},
	)

	count, err := ml2.Replay()
	if err != nil {
		t.Fatal(err)
	}

	if count != 2 {
		t.Fatalf("expected 2 entries after compaction, got %d", count)
	}
	if len(topics) != 1 {
		t.Fatalf("expected 1 topic after compaction, got %d", len(topics))
	}
	if len(offsets) != 1 {
		t.Fatalf("expected 1 offset after compaction, got %d", len(offsets))
	}
	if offsets[0].Offset != 99 {
		t.Fatalf("expected latest offset 99, got %d", offsets[0].Offset)
	}
}

func TestLogDeleteTopic(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	ml, err := NewLog(LogConfig{DataDir: dir})
	if err != nil {
		t.Fatal(err)
	}

	// Create topic then delete it
	topicID := [16]byte{1, 2, 3}
	ml.AppendSync(MarshalCreateTopic(&CreateTopicEntry{
		TopicName:      "ephemeral",
		PartitionCount: 1,
		TopicID:        topicID,
	}))
	ml.AppendSync(MarshalDeleteTopic(&DeleteTopicEntry{TopicName: "ephemeral"}))
	ml.Close()

	// Replay
	ml2, err := NewLog(LogConfig{DataDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer ml2.Close()

	liveTopics := make(map[string]bool)
	ml2.SetCallbacks(
		func(e CreateTopicEntry) { liveTopics[e.TopicName] = true },
		func(e DeleteTopicEntry) { delete(liveTopics, e.TopicName) },
		func(e AlterConfigEntry) {},
		func(e OffsetCommitEntry) {},
		func(e ProducerIDEntry) {},
		func(e LogStartOffsetEntry) {},
	)

	count, err := ml2.Replay()
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("expected 2 entries, got %d", count)
	}
	if len(liveTopics) != 0 {
		t.Fatalf("expected 0 live topics after delete, got %d", len(liveTopics))
	}
}

func TestLogAlterConfig(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	ml, err := NewLog(LogConfig{DataDir: dir})
	if err != nil {
		t.Fatal(err)
	}

	ml.AppendSync(MarshalCreateTopic(&CreateTopicEntry{
		TopicName:      "cfg-topic",
		PartitionCount: 1,
		TopicID:        [16]byte{7, 8, 9},
		Configs:        map[string]string{"retention.ms": "1000"},
	}))
	ml.Append(MarshalAlterConfig(&AlterConfigEntry{
		TopicName: "cfg-topic",
		Key:       "retention.ms",
		Value:     "9999",
	}))
	ml.Close()

	// Replay
	ml2, err := NewLog(LogConfig{DataDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer ml2.Close()

	topicConfigs := make(map[string]map[string]string)
	ml2.SetCallbacks(
		func(e CreateTopicEntry) {
			topicConfigs[e.TopicName] = make(map[string]string)
			for k, v := range e.Configs {
				topicConfigs[e.TopicName][k] = v
			}
		},
		func(e DeleteTopicEntry) { delete(topicConfigs, e.TopicName) },
		func(e AlterConfigEntry) {
			if cfgs, ok := topicConfigs[e.TopicName]; ok {
				cfgs[e.Key] = e.Value
			}
		},
		func(e OffsetCommitEntry) {},
		func(e ProducerIDEntry) {},
		func(e LogStartOffsetEntry) {},
	)

	_, err = ml2.Replay()
	if err != nil {
		t.Fatal(err)
	}

	cfg := topicConfigs["cfg-topic"]
	if cfg["retention.ms"] != "9999" {
		t.Fatalf("expected retention.ms=9999, got %s", cfg["retention.ms"])
	}
}

func TestLogCrashSafety(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	ml, err := NewLog(LogConfig{DataDir: dir})
	if err != nil {
		t.Fatal(err)
	}

	// Write a good entry
	ml.AppendSync(MarshalCreateTopic(&CreateTopicEntry{
		TopicName:      "good-topic",
		PartitionCount: 1,
		TopicID:        [16]byte{1},
	}))
	ml.Close()

	// Append garbage to simulate partial write / crash
	metaPath := filepath.Join(dir, "metadata.log")
	f, err := os.OpenFile(metaPath, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte{0x00, 0x00, 0x00, 0x10}) // length prefix saying 16 bytes
	f.Write([]byte{0xFF, 0xFF, 0xFF})        // truncated entry
	f.Close()

	// Replay should recover the good entry and stop at the corrupt one
	ml2, err := NewLog(LogConfig{DataDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer ml2.Close()

	var topics []CreateTopicEntry
	ml2.SetCallbacks(
		func(e CreateTopicEntry) { topics = append(topics, e) },
		func(e DeleteTopicEntry) {},
		func(e AlterConfigEntry) {},
		func(e OffsetCommitEntry) {},
		func(e ProducerIDEntry) {},
		func(e LogStartOffsetEntry) {},
	)

	count, err := ml2.Replay()
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("expected 1 entry after crash recovery, got %d", count)
	}
	if len(topics) != 1 {
		t.Fatalf("expected 1 topic, got %d", len(topics))
	}
	if topics[0].TopicName != "good-topic" {
		t.Fatalf("expected 'good-topic', got %q", topics[0].TopicName)
	}
}

func TestLogEmptyFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	ml, err := NewLog(LogConfig{DataDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer ml.Close()

	count, err := ml.Replay()
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("expected 0 entries for empty file, got %d", count)
	}
}
