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
