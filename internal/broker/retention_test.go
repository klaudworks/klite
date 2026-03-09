package broker

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/klaudworks/klite/internal/clock"
	"github.com/klaudworks/klite/internal/cluster"
	s3store "github.com/klaudworks/klite/internal/s3"
)

// newRetentionTestBroker creates a minimal broker wired for retention testing.
// Uses InMemoryS3, a FakeClock, and an in-memory cluster state.
func newRetentionTestBroker(t *testing.T, clk *clock.FakeClock) (*Broker, *s3store.InMemoryS3) {
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

	state := cluster.NewState(cluster.Config{
		NodeID:            0,
		DefaultPartitions: 1,
	})

	b := &Broker{
		cfg: Config{
			Clock: clk,
		},
		state:    state,
		logger:   logger,
		s3Client: client,
		s3Reader: reader,
	}
	return b, mem
}

// putTestObject builds and uploads an S3 object with batches at the given timestamps.
func putTestObject(t *testing.T, mem *s3store.InMemoryS3, topic string, topicID [16]byte, partition int32, baseOffset int64, timestamps []int64) {
	t.Helper()
	var batches []s3store.BatchData
	for i, ts := range timestamps {
		offset := baseOffset + int64(i)
		raw, err := s3store.BuildTestBatch(offset, ts, []s3store.Record{
			{OffsetDelta: 0, TimestampDelta: 0, Key: []byte("k"), Value: []byte("v")},
		}, 0)
		if err != nil {
			t.Fatal(err)
		}
		batches = append(batches, s3store.BatchData{
			RawBytes:        raw,
			BaseOffset:      offset,
			LastOffsetDelta: 0,
		})
	}
	obj := s3store.BuildObject(batches)
	key := s3store.ObjectKey("klite/test", topic, topicID, partition, baseOffset)
	client := s3store.NewClient(s3store.ClientConfig{
		S3Client: mem,
		Bucket:   "test-bucket",
		Prefix:   "klite/test",
	})
	if err := client.PutObject(context.Background(), key, obj); err != nil {
		t.Fatal(err)
	}
}

func listObjectKeys(t *testing.T, mem *s3store.InMemoryS3, topic string, topicID [16]byte, partition int32) []string {
	t.Helper()
	client := s3store.NewClient(s3store.ClientConfig{
		S3Client: mem,
		Bucket:   "test-bucket",
		Prefix:   "klite/test",
	})
	objs, err := client.ListObjects(context.Background(), s3store.ObjectKeyPrefix("klite/test", topic, topicID, partition))
	if err != nil {
		t.Fatal(err)
	}
	var keys []string
	for _, o := range objs {
		keys = append(keys, o.Key)
	}
	return keys
}

// topicID is a helper to get the topic ID from broker state.
func topicID(b *Broker, topic string) [16]byte {
	return b.state.GetTopic(topic).ID
}

func setPartitionHW(t *testing.T, b *Broker, topic string, partition int, hw int64) {
	t.Helper()
	td := b.state.GetTopic(topic)
	td.Partitions[partition].Lock()
	td.Partitions[partition].SetHW(hw)
	td.Partitions[partition].Unlock()
}

func TestRetentionByTime(t *testing.T) {
	t.Parallel()

	// Clock at T=100s (100000ms)
	clk := clock.NewFakeClock(time.UnixMilli(100000))
	b, mem := newRetentionTestBroker(t, clk)

	topic := "time-test"
	b.state.CreateTopicWithConfigs(topic, 1, map[string]string{
		"retention.ms": "5000", // 5 seconds
	})

	tid := topicID(b, topic)

	// Object 1: timestamps at 90s (90000ms) — expired (100000 - 90000 = 10000 > 5000)
	putTestObject(t, mem, topic, tid, 0, 0, []int64{90000, 91000, 92000})

	// Object 2: timestamps at 98s (98000ms) — within retention
	putTestObject(t, mem, topic, tid, 0, 3, []int64{98000, 99000})

	// Set HW to match the uploaded data (5 records total: offsets 0-4)
	setPartitionHW(t, b, topic, 0, 5)

	// Should have 2 objects before retention
	keys := listObjectKeys(t, mem, topic, tid, 0)
	if len(keys) != 2 {
		t.Fatalf("expected 2 objects before retention, got %d", len(keys))
	}

	b.enforceRetention(context.Background())

	// Object 1 should be deleted, object 2 should survive
	keys = listObjectKeys(t, mem, topic, tid, 0)
	if len(keys) != 1 {
		t.Fatalf("expected 1 object after retention, got %d", len(keys))
	}

	// logStartOffset should advance past the deleted object
	td := b.state.GetTopic(topic)
	td.Partitions[0].RLock()
	logStart := td.Partitions[0].LogStart()
	td.Partitions[0].RUnlock()
	if logStart != 3 {
		t.Errorf("logStartOffset: got %d, want 3", logStart)
	}
}

func TestRetentionBySize(t *testing.T) {
	t.Parallel()

	clk := clock.NewFakeClock(time.UnixMilli(100000))
	b, mem := newRetentionTestBroker(t, clk)

	topic := "size-test"
	b.state.CreateTopicWithConfigs(topic, 1, map[string]string{
		"retention.ms":    "-1", // infinite time retention (set retention.bytes below)
		"retention.bytes": "-1", // placeholder, updated below
	})

	tid := topicID(b, topic)

	// Put 3 objects, each ~100 bytes
	putTestObject(t, mem, topic, tid, 0, 0, []int64{90000})
	putTestObject(t, mem, topic, tid, 0, 1, []int64{91000})
	putTestObject(t, mem, topic, tid, 0, 2, []int64{92000})

	// Set HW to match (3 records, offsets 0-2)
	setPartitionHW(t, b, topic, 0, 3)

	// Measure logical data size (sum of batch lengths, not S3 object size)
	keys := listObjectKeys(t, mem, topic, tid, 0)
	if len(keys) != 3 {
		t.Fatalf("expected 3 objects, got %d", len(keys))
	}
	client := s3store.NewClient(s3store.ClientConfig{
		S3Client: mem, Bucket: "test-bucket", Prefix: "klite/test",
	})
	objs, _ := client.ListObjects(context.Background(), s3store.ObjectKeyPrefix("klite/test", topic, tid, 0))
	reader := s3store.NewReader(client, nil)
	footer0, err := reader.GetFooter(context.Background(), objs[0].Key, objs[0].Size)
	if err != nil {
		t.Fatal(err)
	}
	oneObjDataSize := footer0.DataSize()

	// Set retention.bytes to fit ~1 object's worth of logical data
	retBytes := oneObjDataSize + 1
	td := b.state.GetTopic(topic)
	b.state.SetTopicConfig(topic, "retention.bytes", intToStr(retBytes))

	b.enforceRetention(context.Background())

	// Should have deleted 2 oldest objects, kept 1 newest
	keys = listObjectKeys(t, mem, topic, tid, 0)
	if len(keys) != 1 {
		t.Fatalf("expected 1 object after size retention, got %d", len(keys))
	}

	td.Partitions[0].RLock()
	logStart := td.Partitions[0].LogStart()
	td.Partitions[0].RUnlock()
	if logStart != 2 {
		t.Errorf("logStartOffset: got %d, want 2", logStart)
	}
}

func TestRetentionBySizeWithCorruptFooterStillProgresses(t *testing.T) {
	t.Parallel()

	clk := clock.NewFakeClock(time.UnixMilli(100000))
	b, mem := newRetentionTestBroker(t, clk)

	topic := "size-corrupt-footer-test"
	b.state.CreateTopicWithConfigs(topic, 1, map[string]string{
		"retention.ms":    "-1",
		"retention.bytes": "-1",
	})

	tid := topicID(b, topic)
	putTestObject(t, mem, topic, tid, 0, 0, []int64{90000})
	putTestObject(t, mem, topic, tid, 0, 1, []int64{91000})
	putTestObject(t, mem, topic, tid, 0, 2, []int64{92000})
	setPartitionHW(t, b, topic, 0, 3)

	client := s3store.NewClient(s3store.ClientConfig{
		S3Client: mem, Bucket: "test-bucket", Prefix: "klite/test",
	})
	reader := s3store.NewReader(client, nil)

	key0 := s3store.ObjectKey("klite/test", topic, tid, 0, 0)
	key1 := s3store.ObjectKey("klite/test", topic, tid, 0, 1)
	obj0Size, err := client.HeadObject(context.Background(), key0)
	if err != nil {
		t.Fatal(err)
	}
	footer0, err := reader.GetFooter(context.Background(), key0, obj0Size)
	if err != nil {
		t.Fatal(err)
	}

	if err := client.PutObject(context.Background(), key1, []byte("corrupt")); err != nil {
		t.Fatal(err)
	}

	b.state.SetTopicConfig(topic, "retention.bytes", intToStr(footer0.DataSize()+1))
	b.enforceRetention(context.Background())

	keys := listObjectKeys(t, mem, topic, tid, 0)
	if len(keys) != 1 {
		t.Fatalf("expected 1 object after size retention with corrupt footer, got %d", len(keys))
	}
	if !strings.HasSuffix(keys[0], "/00000000000000000002.obj") {
		t.Fatalf("expected newest object to remain, got %q", keys[0])
	}

	td := b.state.GetTopic(topic)
	td.Partitions[0].RLock()
	logStart := td.Partitions[0].LogStart()
	td.Partitions[0].RUnlock()
	if logStart != 2 {
		t.Fatalf("logStartOffset: got %d, want 2", logStart)
	}
}

func TestRetentionNeverDeletesLastObject(t *testing.T) {
	t.Parallel()

	// Clock far in the future — all objects are expired
	clk := clock.NewFakeClock(time.UnixMilli(999999999))
	b, mem := newRetentionTestBroker(t, clk)

	topic := "last-obj-test"
	b.state.CreateTopicWithConfigs(topic, 1, map[string]string{
		"retention.ms": "1000",
	})

	tid := topicID(b, topic)

	// Single object — should NOT be deleted even though expired
	putTestObject(t, mem, topic, tid, 0, 0, []int64{1000})
	setPartitionHW(t, b, topic, 0, 1)

	b.enforceRetention(context.Background())

	keys := listObjectKeys(t, mem, topic, tid, 0)
	if len(keys) != 1 {
		t.Fatalf("last object should not be deleted, got %d objects", len(keys))
	}
}

func TestRetentionInfiniteSkipped(t *testing.T) {
	t.Parallel()

	clk := clock.NewFakeClock(time.UnixMilli(999999999))
	b, mem := newRetentionTestBroker(t, clk)

	topic := "infinite-test"
	b.state.CreateTopicWithConfigs(topic, 1, map[string]string{
		"retention.ms":    "-1",
		"retention.bytes": "-1",
	})

	tid := topicID(b, topic)

	putTestObject(t, mem, topic, tid, 0, 0, []int64{1000})
	putTestObject(t, mem, topic, tid, 0, 1, []int64{2000})
	setPartitionHW(t, b, topic, 0, 2)

	b.enforceRetention(context.Background())

	// Nothing should be deleted
	keys := listObjectKeys(t, mem, topic, tid, 0)
	if len(keys) != 2 {
		t.Fatalf("infinite retention should not delete anything, got %d objects", len(keys))
	}
}

func TestRetentionNoS3IsNoop(t *testing.T) {
	t.Parallel()

	b := &Broker{
		cfg:    Config{},
		state:  cluster.NewState(cluster.Config{}),
		logger: slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})),
		// s3Client is nil
	}

	// Should not panic
	b.enforceRetention(context.Background())
}

func TestRetentionAdvancesWithClock(t *testing.T) {
	t.Parallel()

	// Start at T=50s — nothing is expired yet (retention=30s, oldest batch at T=25s)
	clk := clock.NewFakeClock(time.UnixMilli(50000))
	b, mem := newRetentionTestBroker(t, clk)

	topic := "clock-advance-test"
	b.state.CreateTopicWithConfigs(topic, 1, map[string]string{
		"retention.ms": "30000", // 30 seconds
	})

	tid := topicID(b, topic)

	// Object 1: T=25s — cutoff is 50-30=20s, so 25s > 20s → retained
	putTestObject(t, mem, topic, tid, 0, 0, []int64{25000})
	// Object 2: T=45s — well within retention
	putTestObject(t, mem, topic, tid, 0, 1, []int64{45000})
	setPartitionHW(t, b, topic, 0, 2)

	b.enforceRetention(context.Background())
	keys := listObjectKeys(t, mem, topic, tid, 0)
	if len(keys) != 2 {
		t.Fatalf("round 1: expected 2 objects, got %d", len(keys))
	}

	// Advance clock to T=60s — cutoff is 60-30=30s, object 1 (T=25s) is now expired
	clk.Set(time.UnixMilli(60000))

	// Must invalidate reader caches so it re-lists
	b.s3Reader.InvalidateAll()

	b.enforceRetention(context.Background())
	keys = listObjectKeys(t, mem, topic, tid, 0)
	if len(keys) != 1 {
		t.Fatalf("round 2: expected 1 object after clock advance, got %d", len(keys))
	}
}

func intToStr(n int64) string {
	return fmt.Sprintf("%d", n)
}
