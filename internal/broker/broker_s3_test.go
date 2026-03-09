package broker

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"hash/crc32"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/klaudworks/klite/internal/cluster"
	"github.com/klaudworks/klite/internal/metadata"
	s3store "github.com/klaudworks/klite/internal/s3"
	"github.com/klaudworks/klite/internal/wal"
)

// newInferTestBroker creates a minimal Broker suitable for inferTopicsFromS3 tests.
func newInferTestBroker(t *testing.T) (*Broker, *s3store.InMemoryS3) {
	t.Helper()
	mem := s3store.NewInMemoryS3()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	client := s3store.NewClient(s3store.ClientConfig{
		S3Client: mem,
		Bucket:   "test-bucket",
		Prefix:   "klite/test",
		Logger:   logger,
	})

	return &Broker{
		logger:   logger,
		s3Client: client,
	}, mem
}

// putFakeObject stores a zero-byte object in InMemoryS3 at the given key.
func putFakeObject(t *testing.T, mem *s3store.InMemoryS3, key string) {
	t.Helper()
	ctx := context.Background()
	if err := s3store.NewClient(s3store.ClientConfig{
		S3Client: mem,
		Bucket:   "test-bucket",
		Prefix:   "",
		Logger:   slog.Default(),
	}).PutObject(ctx, key, []byte("data")); err != nil {
		t.Fatalf("put fake object %s: %v", key, err)
	}
}

// readInferredTopics parses the metadata.log written by inferTopicsFromS3.
func readInferredTopics(t *testing.T, metaPath string) []metadata.CreateTopicEntry {
	t.Helper()
	f, err := os.Open(metaPath)
	if err != nil {
		t.Fatalf("open metadata.log: %v", err)
	}
	defer f.Close() //nolint:errcheck // test helper

	crc32cTable := crc32.MakeTable(crc32.Castagnoli)
	var entries []metadata.CreateTopicEntry

	_, scanErr := wal.ScanFramedEntries(f, func(payload []byte) bool {
		if len(payload) < 5 {
			t.Fatalf("payload too short: %d bytes", len(payload))
			return false
		}
		storedCRC := binary.BigEndian.Uint32(payload[0:4])
		actualCRC := crc32.Checksum(payload[4:], crc32cTable)
		if storedCRC != actualCRC {
			t.Fatalf("CRC mismatch: stored=%x actual=%x", storedCRC, actualCRC)
			return false
		}
		entryType := payload[4]
		if entryType != metadata.EntryCreateTopic {
			t.Fatalf("unexpected entry type: %d", entryType)
			return false
		}
		e, err := metadata.UnmarshalCreateTopic(payload[5:])
		if err != nil {
			t.Fatalf("unmarshal CreateTopic: %v", err)
			return false
		}
		entries = append(entries, e)
		return true
	})
	if scanErr != nil {
		t.Fatalf("scan metadata.log: %v", scanErr)
	}
	return entries
}

func TestInferTopicsFromS3_SingleTopic(t *testing.T) {
	b, mem := newInferTestBroker(t)
	prefix := "klite/test"

	var topicID [16]byte
	topicID[0] = 0xAB
	topicID[15] = 0xCD
	topicDir := s3store.TopicDir("orders", topicID)

	putFakeObject(t, mem, prefix+"/"+topicDir+"/0/00000000000000000000.obj")
	putFakeObject(t, mem, prefix+"/"+topicDir+"/0/00000000000000000100.obj")
	putFakeObject(t, mem, prefix+"/"+topicDir+"/1/00000000000000000000.obj")

	metaPath := filepath.Join(t.TempDir(), "metadata.log")
	ctx := context.Background()
	if err := b.inferTopicsFromS3(ctx, b.s3Client, prefix, metaPath); err != nil {
		t.Fatalf("inferTopicsFromS3: %v", err)
	}

	entries := readInferredTopics(t, metaPath)
	if len(entries) != 1 {
		t.Fatalf("expected 1 topic, got %d", len(entries))
	}
	if entries[0].TopicName != "orders" {
		t.Errorf("topic name = %q, want %q", entries[0].TopicName, "orders")
	}
	if entries[0].PartitionCount != 2 {
		t.Errorf("partition count = %d, want 2", entries[0].PartitionCount)
	}
	if entries[0].TopicID != topicID {
		t.Errorf("topic ID = %x, want %x", entries[0].TopicID, topicID)
	}
}

func TestInferTopicsFromS3_MultipleTopicsSorted(t *testing.T) {
	b, mem := newInferTestBroker(t)
	prefix := "klite/test"

	var idA, idB, idC [16]byte
	idA[0] = 0x01
	idB[0] = 0x02
	idC[0] = 0x03

	// Insert in non-alphabetical order to verify sorting.
	putFakeObject(t, mem, prefix+"/"+s3store.TopicDir("zebra", idC)+"/0/00000000000000000000.obj")
	putFakeObject(t, mem, prefix+"/"+s3store.TopicDir("apple", idA)+"/0/00000000000000000000.obj")
	putFakeObject(t, mem, prefix+"/"+s3store.TopicDir("mango", idB)+"/0/00000000000000000000.obj")

	metaPath := filepath.Join(t.TempDir(), "metadata.log")
	if err := b.inferTopicsFromS3(context.Background(), b.s3Client, prefix, metaPath); err != nil {
		t.Fatalf("inferTopicsFromS3: %v", err)
	}

	entries := readInferredTopics(t, metaPath)
	if len(entries) != 3 {
		t.Fatalf("expected 3 topics, got %d", len(entries))
	}
	want := []string{"apple", "mango", "zebra"}
	for i, name := range want {
		if entries[i].TopicName != name {
			t.Errorf("entry[%d].TopicName = %q, want %q", i, entries[i].TopicName, name)
		}
	}
}

func TestInferTopicsFromS3_SkipsNonDataKeys(t *testing.T) {
	b, mem := newInferTestBroker(t)
	prefix := "klite/test"

	var topicID [16]byte
	topicID[0] = 0x42
	topicDir := s3store.TopicDir("events", topicID)

	// Data key — should be picked up.
	putFakeObject(t, mem, prefix+"/"+topicDir+"/0/00000000000000000000.obj")
	// Non-data keys — should be skipped.
	putFakeObject(t, mem, prefix+"/metadata.log")
	putFakeObject(t, mem, prefix+"/lease")
	// Key with wrong suffix — should be skipped.
	putFakeObject(t, mem, prefix+"/"+topicDir+"/0/00000000000000000000.tmp")

	metaPath := filepath.Join(t.TempDir(), "metadata.log")
	if err := b.inferTopicsFromS3(context.Background(), b.s3Client, prefix, metaPath); err != nil {
		t.Fatalf("inferTopicsFromS3: %v", err)
	}

	entries := readInferredTopics(t, metaPath)
	if len(entries) != 1 {
		t.Fatalf("expected 1 topic, got %d", len(entries))
	}
	if entries[0].TopicName != "events" {
		t.Errorf("topic name = %q, want %q", entries[0].TopicName, "events")
	}
}

func TestInferTopicsFromS3_EmptyBucket(t *testing.T) {
	b, _ := newInferTestBroker(t)
	prefix := "klite/test"

	metaPath := filepath.Join(t.TempDir(), "metadata.log")
	if err := b.inferTopicsFromS3(context.Background(), b.s3Client, prefix, metaPath); err != nil {
		t.Fatalf("inferTopicsFromS3: %v", err)
	}

	// No file should be created when there are no objects.
	if _, err := os.Stat(metaPath); !os.IsNotExist(err) {
		t.Error("metadata.log should not exist when bucket is empty")
	}
}

func TestInferTopicsFromS3_ZeroTopicIDGetsNewUUID(t *testing.T) {
	b, mem := newInferTestBroker(t)
	prefix := "klite/test"

	// Legacy format: topic dir without a valid 32-char hex ID suffix.
	// ParseTopicDir returns the whole string as topic name and a zero ID.
	putFakeObject(t, mem, prefix+"/legacy-topic/0/00000000000000000000.obj")

	metaPath := filepath.Join(t.TempDir(), "metadata.log")
	if err := b.inferTopicsFromS3(context.Background(), b.s3Client, prefix, metaPath); err != nil {
		t.Fatalf("inferTopicsFromS3: %v", err)
	}

	entries := readInferredTopics(t, metaPath)
	if len(entries) != 1 {
		t.Fatalf("expected 1 topic, got %d", len(entries))
	}
	if entries[0].TopicName != "legacy-topic" {
		t.Errorf("topic name = %q, want %q", entries[0].TopicName, "legacy-topic")
	}
	// Zero ID should be replaced with a generated UUID.
	var zeroID [16]byte
	if entries[0].TopicID == zeroID {
		t.Error("expected non-zero topic ID for legacy topic, got zero")
	}
}

func TestInferTopicsFromS3_PartitionCountFromMaxIndex(t *testing.T) {
	b, mem := newInferTestBroker(t)
	prefix := "klite/test"

	var topicID [16]byte
	topicID[0] = 0xFF
	topicDir := s3store.TopicDir("clicks", topicID)

	// Partitions 0, 2, 5 — max index is 5, so partition count should be 6.
	putFakeObject(t, mem, prefix+"/"+topicDir+"/0/00000000000000000000.obj")
	putFakeObject(t, mem, prefix+"/"+topicDir+"/2/00000000000000000000.obj")
	putFakeObject(t, mem, prefix+"/"+topicDir+"/5/00000000000000000000.obj")

	metaPath := filepath.Join(t.TempDir(), "metadata.log")
	if err := b.inferTopicsFromS3(context.Background(), b.s3Client, prefix, metaPath); err != nil {
		t.Fatalf("inferTopicsFromS3: %v", err)
	}

	entries := readInferredTopics(t, metaPath)
	if len(entries) != 1 {
		t.Fatalf("expected 1 topic, got %d", len(entries))
	}
	if entries[0].PartitionCount != 6 {
		t.Errorf("partition count = %d, want 6 (max partition index 5 + 1)", entries[0].PartitionCount)
	}
}

func TestInferTopicsFromS3_TopicIDPreserved(t *testing.T) {
	b, mem := newInferTestBroker(t)
	prefix := "klite/test"

	var topicID [16]byte
	for i := range topicID {
		topicID[i] = byte(i * 17)
	}
	topicDir := s3store.TopicDir("test-topic", topicID)

	putFakeObject(t, mem, prefix+"/"+topicDir+"/0/00000000000000000000.obj")

	metaPath := filepath.Join(t.TempDir(), "metadata.log")
	if err := b.inferTopicsFromS3(context.Background(), b.s3Client, prefix, metaPath); err != nil {
		t.Fatalf("inferTopicsFromS3: %v", err)
	}

	entries := readInferredTopics(t, metaPath)
	if len(entries) != 1 {
		t.Fatalf("expected 1 topic, got %d", len(entries))
	}
	if entries[0].TopicID != topicID {
		t.Errorf("topic ID = %s, want %s",
			hex.EncodeToString(entries[0].TopicID[:]),
			hex.EncodeToString(topicID[:]))
	}
}

// --- probeS3Watermarks / rehydrateDirtyCounters tests ---

// newS3WatermarkTestBroker creates a minimal Broker with state, s3Client, and
// s3Reader for testing probeS3Watermarks and rehydrateDirtyCounters.
func newS3WatermarkTestBroker(t *testing.T) (*Broker, *s3store.InMemoryS3) {
	t.Helper()
	mem := s3store.NewInMemoryS3()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	client := s3store.NewClient(s3store.ClientConfig{
		S3Client: mem,
		Bucket:   "test-bucket",
		Prefix:   "klite/test",
		Logger:   logger,
	})
	reader := s3store.NewReader(client, logger)
	state := cluster.NewState(cluster.Config{NodeID: 1, DefaultPartitions: 1})

	return &Broker{
		logger:   logger,
		s3Client: client,
		s3Reader: reader,
		state:    state,
	}, mem
}

// putS3Object stores a properly formatted S3 object (with footer) at the
// canonical key for the given topic/partition/baseOffset.
func putS3Object(t *testing.T, mem *s3store.InMemoryS3, prefix string, topicID [16]byte, topic string, partition int32, baseOffset int64, lastOffsetDelta int32) {
	t.Helper()
	// Build a minimal batch with enough bytes for the footer to parse.
	// RawBytes must be at least RecordBatchHeaderSize (61 bytes) for
	// BuildObject to extract maxTs/numRec, but the footer only needs
	// BaseOffset and LastOffsetDelta. Use a short slice — BuildObject
	// handles <61 gracefully.
	raw := make([]byte, 8)
	obj := s3store.BuildObject([]s3store.BatchData{
		{RawBytes: raw, BaseOffset: baseOffset, LastOffsetDelta: lastOffsetDelta},
	})
	key := s3store.ObjectKey(prefix, topic, topicID, partition, baseOffset)
	ctx := context.Background()
	if err := s3store.NewClient(s3store.ClientConfig{
		S3Client: mem,
		Bucket:   "test-bucket",
		Prefix:   "",
		Logger:   slog.Default(),
	}).PutObject(ctx, key, obj); err != nil {
		t.Fatalf("put S3 object %s: %v", key, err)
	}
}

func TestProbeS3Watermarks_SetsHWFromS3(t *testing.T) {
	b, mem := newS3WatermarkTestBroker(t)
	prefix := "klite/test"

	td, _ := b.state.CreateTopic("orders", 1)
	// Put an object at baseOffset=0 with lastOffsetDelta=9 → HW should be 10.
	putS3Object(t, mem, prefix, td.ID, "orders", 0, 0, 9)

	b.probeS3Watermarks()

	pd := td.Partitions[0]
	pd.Lock()
	hw := pd.HW()
	s3w := pd.S3FlushWatermark()
	pd.Unlock()

	if hw != 10 {
		t.Errorf("HW = %d, want 10", hw)
	}
	if s3w != 10 {
		t.Errorf("S3FlushWatermark = %d, want 10", s3w)
	}
}

func TestProbeS3Watermarks_NoS3Data(t *testing.T) {
	b, _ := newS3WatermarkTestBroker(t)
	b.state.CreateTopic("empty-topic", 2)

	b.probeS3Watermarks()

	topics := b.state.GetAllTopics()
	for _, td := range topics {
		for _, pd := range td.Partitions {
			pd.Lock()
			hw := pd.HW()
			s3w := pd.S3FlushWatermark()
			pd.Unlock()
			if hw != 0 {
				t.Errorf("partition %d: HW = %d, want 0", pd.Index, hw)
			}
			if s3w != 0 {
				t.Errorf("partition %d: S3FlushWatermark = %d, want 0", pd.Index, s3w)
			}
		}
	}
}

func TestProbeS3Watermarks_DoesNotRegressBelowWALHW(t *testing.T) {
	b, mem := newS3WatermarkTestBroker(t)
	prefix := "klite/test"

	td, _ := b.state.CreateTopic("events", 1)

	// Simulate WAL-based HW already at 50.
	pd := td.Partitions[0]
	pd.Lock()
	pd.SetHW(50)
	pd.Unlock()

	// S3 only has data up to offset 19 (baseOffset=0, lastOffsetDelta=19 → HW=20).
	putS3Object(t, mem, prefix, td.ID, "events", 0, 0, 19)

	b.probeS3Watermarks()

	pd.Lock()
	hw := pd.HW()
	s3w := pd.S3FlushWatermark()
	pd.Unlock()

	if hw != 50 {
		t.Errorf("HW = %d, want 50 (should not regress below WAL HW)", hw)
	}
	// S3 flush watermark should still be set to what S3 reports.
	if s3w != 20 {
		t.Errorf("S3FlushWatermark = %d, want 20", s3w)
	}
}

func TestProbeS3Watermarks_MultiplePartitions(t *testing.T) {
	b, mem := newS3WatermarkTestBroker(t)
	prefix := "klite/test"

	td, _ := b.state.CreateTopic("multi", 3)
	// Partition 0: offset 0-4 → HW=5
	putS3Object(t, mem, prefix, td.ID, "multi", 0, 0, 4)
	// Partition 1: no data → HW=0
	// Partition 2: offset 0-99 → HW=100
	putS3Object(t, mem, prefix, td.ID, "multi", 2, 0, 99)

	b.probeS3Watermarks()

	want := []int64{5, 0, 100}
	for i, expected := range want {
		pd := td.Partitions[i]
		pd.Lock()
		hw := pd.HW()
		pd.Unlock()
		if hw != expected {
			t.Errorf("partition %d: HW = %d, want %d", i, hw, expected)
		}
	}
}

func TestRehydrateDirtyCounters_MatchesS3ObjectCount(t *testing.T) {
	b, mem := newS3WatermarkTestBroker(t)
	prefix := "klite/test"

	td, _ := b.state.CreateTopicWithConfigs("compact-topic", 1, map[string]string{
		"cleanup.policy": "compact",
	})

	// Put 3 .obj files for partition 0. cleanedUpTo defaults to 0, and
	// rehydrateDirtyCounters counts objects with baseOffset > cleanedUpTo.
	// So baseOffset=0 is NOT dirty, but 10 and 20 are.
	putS3Object(t, mem, prefix, td.ID, "compact-topic", 0, 0, 9)
	putS3Object(t, mem, prefix, td.ID, "compact-topic", 0, 10, 9)
	putS3Object(t, mem, prefix, td.ID, "compact-topic", 0, 20, 9)

	b.rehydrateDirtyCounters()

	dirty := td.Partitions[0].DirtyObjects()
	if dirty != 2 {
		t.Errorf("dirty objects = %d, want 2", dirty)
	}
}

func TestRehydrateDirtyCounters_ZeroForNonCompactTopic(t *testing.T) {
	b, mem := newS3WatermarkTestBroker(t)
	prefix := "klite/test"

	td, _ := b.state.CreateTopic("delete-topic", 1)
	putS3Object(t, mem, prefix, td.ID, "delete-topic", 0, 0, 9)

	b.rehydrateDirtyCounters()

	dirty := td.Partitions[0].DirtyObjects()
	if dirty != 0 {
		t.Errorf("dirty objects = %d, want 0 (non-compact topic should be skipped)", dirty)
	}
}

func TestRehydrateDirtyCounters_ZeroForEmptyPartition(t *testing.T) {
	b, _ := newS3WatermarkTestBroker(t)
	b.state.CreateTopicWithConfigs("compact-empty", 1, map[string]string{
		"cleanup.policy": "compact",
	})

	b.rehydrateDirtyCounters()

	topics := b.state.GetAllTopics()
	for _, td := range topics {
		for _, pd := range td.Partitions {
			dirty := pd.DirtyObjects()
			if dirty != 0 {
				t.Errorf("partition %d: dirty objects = %d, want 0", pd.Index, dirty)
			}
		}
	}
}

func TestRehydrateDirtyCounters_ExcludesObjectsBelowCleanedUpTo(t *testing.T) {
	b, mem := newS3WatermarkTestBroker(t)
	prefix := "klite/test"

	td, _ := b.state.CreateTopicWithConfigs("compact-partial", 1, map[string]string{
		"cleanup.policy": "compact",
	})

	// 4 objects: baseOffset 0, 10, 20, 30.
	putS3Object(t, mem, prefix, td.ID, "compact-partial", 0, 0, 9)
	putS3Object(t, mem, prefix, td.ID, "compact-partial", 0, 10, 9)
	putS3Object(t, mem, prefix, td.ID, "compact-partial", 0, 20, 9)
	putS3Object(t, mem, prefix, td.ID, "compact-partial", 0, 30, 9)

	// Mark cleanedUpTo=10 → only objects with baseOffset > 10 are dirty (20, 30).
	pd := td.Partitions[0]
	pd.Lock()
	pd.SetCleanedUpTo(10)
	pd.Unlock()

	b.rehydrateDirtyCounters()

	dirty := pd.DirtyObjects()
	if dirty != 2 {
		t.Errorf("dirty objects = %d, want 2 (only objects above cleanedUpTo=10)", dirty)
	}
}
