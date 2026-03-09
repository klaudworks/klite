package broker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3svc "github.com/aws/aws-sdk-go-v2/service/s3"
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

func TestRetentionByTimeWithCorruptMiddleFooterStillDeletesLaterExpiredObject(t *testing.T) {
	t.Parallel()

	clk := clock.NewFakeClock(time.UnixMilli(100000))
	b, mem := newRetentionTestBroker(t, clk)

	topic := "time-corrupt-footer-test"
	b.state.CreateTopicWithConfigs(topic, 1, map[string]string{
		"retention.ms": "1000",
	})

	tid := topicID(b, topic)
	putTestObject(t, mem, topic, tid, 0, 0, []int64{90000})
	putTestObject(t, mem, topic, tid, 0, 1, []int64{91000})
	putTestObject(t, mem, topic, tid, 0, 2, []int64{92000})
	setPartitionHW(t, b, topic, 0, 3)

	client := s3store.NewClient(s3store.ClientConfig{
		S3Client: mem, Bucket: "test-bucket", Prefix: "klite/test",
	})
	key1 := s3store.ObjectKey("klite/test", topic, tid, 0, 1)
	if err := client.PutObject(context.Background(), key1, []byte("corrupt")); err != nil {
		t.Fatal(err)
	}

	b.enforceRetention(context.Background())

	keys := listObjectKeys(t, mem, topic, tid, 0)
	if len(keys) != 1 {
		t.Fatalf("expected 1 object after time retention with corrupt footer, got %d", len(keys))
	}
	if !strings.HasSuffix(keys[0], "/00000000000000000001.obj") {
		t.Fatalf("expected only corrupt middle object to remain, got %q", keys[0])
	}

	td := b.state.GetTopic(topic)
	td.Partitions[0].RLock()
	logStart := td.Partitions[0].LogStart()
	td.Partitions[0].RUnlock()
	if logStart != 1 {
		t.Fatalf("logStartOffset: got %d, want 1", logStart)
	}
}

func TestRetentionPartialDeleteFailureAdvancesOnlyPastDeletedObjects(t *testing.T) {
	t.Parallel()

	clk := clock.NewFakeClock(time.UnixMilli(999999999))
	b, mem := newRetentionTestBroker(t, clk)

	topic := "partial-delete-failure-test"
	b.state.CreateTopicWithConfigs(topic, 1, map[string]string{
		"retention.ms": "1000",
	})

	tid := topicID(b, topic)
	putTestObject(t, mem, topic, tid, 0, 0, []int64{1000})
	putTestObject(t, mem, topic, tid, 0, 1, []int64{2000})
	putTestObject(t, mem, topic, tid, 0, 2, []int64{3000})
	setPartitionHW(t, b, topic, 0, 3)

	b.s3Client = s3store.NewClient(s3store.ClientConfig{
		S3Client: &deleteFailAfterNS3{InMemoryS3: mem, failAfter: 1},
		Bucket:   "test-bucket",
		Prefix:   "klite/test",
		Logger:   b.logger,
	})

	b.enforceRetention(context.Background())

	keys := listObjectKeys(t, mem, topic, tid, 0)
	if len(keys) != 2 {
		t.Fatalf("expected 2 objects after partial delete failure, got %d", len(keys))
	}
	for _, key := range keys {
		if strings.HasSuffix(key, "/00000000000000000000.obj") {
			t.Fatalf("expected oldest object to be deleted, got keys %v", keys)
		}
	}

	td := b.state.GetTopic(topic)
	td.Partitions[0].RLock()
	logStart := td.Partitions[0].LogStart()
	td.Partitions[0].RUnlock()
	if logStart != 1 {
		t.Fatalf("logStartOffset: got %d, want 1", logStart)
	}

	// A subsequent run should resume from the remaining eligible objects.
	b.s3Client = s3store.NewClient(s3store.ClientConfig{
		S3Client: mem,
		Bucket:   "test-bucket",
		Prefix:   "klite/test",
		Logger:   b.logger,
	})

	b.enforceRetention(context.Background())

	keys = listObjectKeys(t, mem, topic, tid, 0)
	if len(keys) != 1 {
		t.Fatalf("expected 1 object after follow-up retention run, got %d", len(keys))
	}
	if !strings.HasSuffix(keys[0], "/00000000000000000002.obj") {
		t.Fatalf("expected newest object to remain after follow-up run, got %q", keys[0])
	}

	td.Partitions[0].RLock()
	logStart = td.Partitions[0].LogStart()
	td.Partitions[0].RUnlock()
	if logStart != 2 {
		t.Fatalf("logStartOffset after follow-up run: got %d, want 2", logStart)
	}
}

func TestRetentionByTimeAndSizeCombined(t *testing.T) {
	t.Parallel()

	t.Run("union of time and size deletions", func(t *testing.T) {
		clk := clock.NewFakeClock(time.UnixMilli(100000))
		b, mem := newRetentionTestBroker(t, clk)

		topic := "time-size-union-test"
		b.state.CreateTopicWithConfigs(topic, 1, map[string]string{
			"retention.ms":    "5000",
			"retention.bytes": "-1",
		})

		tid := topicID(b, topic)
		putTestObject(t, mem, topic, tid, 0, 0, []int64{80000})
		putTestObject(t, mem, topic, tid, 0, 1, []int64{98000})
		putTestObject(t, mem, topic, tid, 0, 2, []int64{99000})
		putTestObject(t, mem, topic, tid, 0, 3, []int64{99500})
		setPartitionHW(t, b, topic, 0, 4)

		client := s3store.NewClient(s3store.ClientConfig{S3Client: mem, Bucket: "test-bucket", Prefix: "klite/test"})
		objs, err := client.ListObjects(context.Background(), s3store.ObjectKeyPrefix("klite/test", topic, tid, 0))
		if err != nil {
			t.Fatal(err)
		}
		reader := s3store.NewReader(client, nil)
		footer0, err := reader.GetFooter(context.Background(), objs[0].Key, objs[0].Size)
		if err != nil {
			t.Fatal(err)
		}

		b.state.SetTopicConfig(topic, "retention.bytes", intToStr(2*footer0.DataSize()+1))

		b.enforceRetention(context.Background())

		keys := listObjectKeys(t, mem, topic, tid, 0)
		if len(keys) != 2 {
			t.Fatalf("expected 2 objects after combined retention, got %d", len(keys))
		}
		if !strings.HasSuffix(keys[0], "/00000000000000000002.obj") {
			t.Fatalf("expected offset 2 object to remain, got %q", keys[0])
		}
		if !strings.HasSuffix(keys[1], "/00000000000000000003.obj") {
			t.Fatalf("expected offset 3 object to remain, got %q", keys[1])
		}
	})

	t.Run("time-deleted data reduces size pressure", func(t *testing.T) {
		clk := clock.NewFakeClock(time.UnixMilli(100000))
		b, mem := newRetentionTestBroker(t, clk)

		topic := "time-size-interaction-test"
		b.state.CreateTopicWithConfigs(topic, 1, map[string]string{
			"retention.ms":    "5000",
			"retention.bytes": "-1",
		})

		tid := topicID(b, topic)
		putTestObject(t, mem, topic, tid, 0, 0, []int64{80000})
		putTestObject(t, mem, topic, tid, 0, 1, []int64{98000})
		putTestObject(t, mem, topic, tid, 0, 2, []int64{99000})
		setPartitionHW(t, b, topic, 0, 3)

		client := s3store.NewClient(s3store.ClientConfig{S3Client: mem, Bucket: "test-bucket", Prefix: "klite/test"})
		objs, err := client.ListObjects(context.Background(), s3store.ObjectKeyPrefix("klite/test", topic, tid, 0))
		if err != nil {
			t.Fatal(err)
		}
		reader := s3store.NewReader(client, nil)
		footer0, err := reader.GetFooter(context.Background(), objs[0].Key, objs[0].Size)
		if err != nil {
			t.Fatal(err)
		}

		b.state.SetTopicConfig(topic, "retention.bytes", intToStr(2*footer0.DataSize()+1))

		b.enforceRetention(context.Background())

		keys := listObjectKeys(t, mem, topic, tid, 0)
		if len(keys) != 2 {
			t.Fatalf("expected only time-expired object to be deleted, got %d remaining", len(keys))
		}
		if !strings.HasSuffix(keys[0], "/00000000000000000001.obj") {
			t.Fatalf("expected offset 1 object to remain, got %q", keys[0])
		}
		if !strings.HasSuffix(keys[1], "/00000000000000000002.obj") {
			t.Fatalf("expected offset 2 object to remain, got %q", keys[1])
		}
	})

	t.Run("time retention deletes even when size retention would keep all objects", func(t *testing.T) {
		clk := clock.NewFakeClock(time.UnixMilli(100000))
		b, mem := newRetentionTestBroker(t, clk)

		topic := "time-over-size-test"
		b.state.CreateTopicWithConfigs(topic, 1, map[string]string{
			"retention.ms":    "5000",
			"retention.bytes": "1000000",
		})

		tid := topicID(b, topic)
		putTestObject(t, mem, topic, tid, 0, 0, []int64{80000})
		putTestObject(t, mem, topic, tid, 0, 1, []int64{98000})
		putTestObject(t, mem, topic, tid, 0, 2, []int64{99000})
		setPartitionHW(t, b, topic, 0, 3)

		b.enforceRetention(context.Background())

		keys := listObjectKeys(t, mem, topic, tid, 0)
		if len(keys) != 2 {
			t.Fatalf("expected only time-expired object to be deleted, got %d remaining", len(keys))
		}
		if !strings.HasSuffix(keys[0], "/00000000000000000001.obj") {
			t.Fatalf("expected offset 1 object to remain, got %q", keys[0])
		}
		if !strings.HasSuffix(keys[1], "/00000000000000000002.obj") {
			t.Fatalf("expected offset 2 object to remain, got %q", keys[1])
		}
	})
}

func TestRetentionBytesZeroKeepsLastObject(t *testing.T) {
	t.Parallel()

	clk := clock.NewFakeClock(time.UnixMilli(100000))
	b, mem := newRetentionTestBroker(t, clk)

	topic := "retention-bytes-zero-test"
	b.state.CreateTopicWithConfigs(topic, 1, map[string]string{
		"retention.ms":    "-1",
		"retention.bytes": "0",
	})

	tid := topicID(b, topic)
	putTestObject(t, mem, topic, tid, 0, 0, []int64{90000})
	putTestObject(t, mem, topic, tid, 0, 1, []int64{91000})
	putTestObject(t, mem, topic, tid, 0, 2, []int64{92000})
	setPartitionHW(t, b, topic, 0, 3)

	b.enforceRetention(context.Background())

	keys := listObjectKeys(t, mem, topic, tid, 0)
	if len(keys) != 1 {
		t.Fatalf("expected only last object to remain, got %d", len(keys))
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

func TestRetentionBytesZeroSingleObjectStillSurvives(t *testing.T) {
	t.Parallel()

	clk := clock.NewFakeClock(time.UnixMilli(100000))
	b, mem := newRetentionTestBroker(t, clk)

	topic := "retention-bytes-zero-single-test"
	b.state.CreateTopicWithConfigs(topic, 1, map[string]string{
		"retention.ms":    "-1",
		"retention.bytes": "0",
	})

	tid := topicID(b, topic)
	putTestObject(t, mem, topic, tid, 0, 0, []int64{90000})
	setPartitionHW(t, b, topic, 0, 1)

	b.enforceRetention(context.Background())

	keys := listObjectKeys(t, mem, topic, tid, 0)
	if len(keys) != 1 {
		t.Fatalf("expected single object to survive, got %d", len(keys))
	}
	if !strings.HasSuffix(keys[0], "/00000000000000000000.obj") {
		t.Fatalf("expected original object to remain, got %q", keys[0])
	}

	td := b.state.GetTopic(topic)
	td.Partitions[0].RLock()
	logStart := td.Partitions[0].LogStart()
	td.Partitions[0].RUnlock()
	if logStart != 0 {
		t.Fatalf("logStartOffset: got %d, want 0", logStart)
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

func TestScanOrphanedS3TopicsDetectsOrphansAndIgnoresMetadata(t *testing.T) {
	t.Parallel()

	b, mem := newRetentionTestBroker(t, clock.NewFakeClock(time.UnixMilli(100000)))

	_, _ = b.state.CreateTopicWithConfigs("live", 1, nil)
	liveID := topicID(b, "live")
	putTestObject(t, mem, "live", liveID, 0, 0, []int64{90000})

	var orphanID [16]byte
	orphanID[0] = 42
	putTestObject(t, mem, "orphan", orphanID, 0, 0, []int64{90000})
	putTestObject(t, mem, "orphan", orphanID, 0, 1, []int64{91000})

	client := s3store.NewClient(s3store.ClientConfig{S3Client: mem, Bucket: "test-bucket", Prefix: "klite/test"})
	if err := client.PutObject(context.Background(), "klite/test/metadata.log", []byte("meta")); err != nil {
		t.Fatal(err)
	}

	orphans := b.scanOrphanedS3Topics()
	if len(orphans) != 1 {
		t.Fatalf("expected 1 orphaned topic, got %d", len(orphans))
	}
	if orphans[0].Name != "orphan" {
		t.Fatalf("unexpected orphan name: got %q, want %q", orphans[0].Name, "orphan")
	}
	if orphans[0].TopicID != orphanID {
		t.Fatalf("unexpected orphan topic ID: got %v, want %v", orphans[0].TopicID, orphanID)
	}
}

func TestScanOrphanedS3TopicsEmptyPrefix(t *testing.T) {
	t.Parallel()

	b, _ := newRetentionTestBroker(t, clock.NewFakeClock(time.UnixMilli(100000)))
	orphans := b.scanOrphanedS3Topics()
	if len(orphans) != 0 {
		t.Fatalf("expected no orphaned topics, got %d", len(orphans))
	}
}

func TestGCDeletedTopicReenqueuesOnListFailure(t *testing.T) {
	t.Parallel()

	b, mem := newRetentionTestBroker(t, clock.NewFakeClock(time.UnixMilli(100000)))
	var retryID [16]byte
	retryID[0] = 7
	dt := cluster.DeletedTopic{Name: "retry", TopicID: retryID}
	failedPrefix := "klite/test/" + s3store.TopicDir(dt.Name, dt.TopicID) + "/"

	b.s3Client = s3store.NewClient(s3store.ClientConfig{
		S3Client: &listFailingS3{
			InMemoryS3: mem,
			failPrefix: failedPrefix,
		},
		Bucket: "test-bucket",
		Prefix: "klite/test",
		Logger: b.logger,
	})

	b.gcDeletedTopic(context.Background(), dt)

	drained := b.state.DrainDeletedTopics()
	if len(drained) != 1 {
		t.Fatalf("expected 1 deleted topic re-enqueued, got %d", len(drained))
	}
	if drained[0] != dt {
		t.Fatalf("unexpected re-enqueued topic: got %+v, want %+v", drained[0], dt)
	}
}

func TestGCDeletedTopicDeletesObjectsAndHandlesEmptyList(t *testing.T) {
	t.Parallel()

	b, mem := newRetentionTestBroker(t, clock.NewFakeClock(time.UnixMilli(100000)))
	recording := &deleteRecordingS3{InMemoryS3: mem}
	b.s3Client = s3store.NewClient(s3store.ClientConfig{
		S3Client: recording,
		Bucket:   "test-bucket",
		Prefix:   "klite/test",
		Logger:   b.logger,
	})

	var goneID [16]byte
	goneID[0] = 9
	dt := cluster.DeletedTopic{Name: "gone", TopicID: goneID}
	putTestObject(t, mem, dt.Name, dt.TopicID, 0, 0, []int64{90000})
	putTestObject(t, mem, dt.Name, dt.TopicID, 1, 0, []int64{90000})

	b.gcDeletedTopic(context.Background(), dt)
	if len(recording.deleted) != 2 {
		t.Fatalf("expected 2 deleted keys, got %d", len(recording.deleted))
	}

	objs, err := b.s3Client.ListObjects(context.Background(), "klite/test/"+s3store.TopicDir(dt.Name, dt.TopicID)+"/")
	if err != nil {
		t.Fatal(err)
	}
	if len(objs) != 0 {
		t.Fatalf("expected no remaining objects for deleted topic, got %d", len(objs))
	}

	b.gcDeletedTopic(context.Background(), dt)
	if len(recording.deleted) != 2 {
		t.Fatalf("expected empty-list GC to keep delete count unchanged, got %d", len(recording.deleted))
	}
}

func TestGCDeletedTopicReenqueuesOnPartialDeleteFailure(t *testing.T) {
	t.Parallel()

	b, mem := newRetentionTestBroker(t, clock.NewFakeClock(time.UnixMilli(100000)))

	var flakyID [16]byte
	flakyID[0] = 10
	dt := cluster.DeletedTopic{Name: "flaky", TopicID: flakyID}
	putTestObject(t, mem, dt.Name, dt.TopicID, 0, 0, []int64{90000})
	putTestObject(t, mem, dt.Name, dt.TopicID, 0, 1, []int64{90000})

	b.s3Client = s3store.NewClient(s3store.ClientConfig{
		S3Client: &deleteFailAfterNS3{InMemoryS3: mem, failAfter: 1},
		Bucket:   "test-bucket",
		Prefix:   "klite/test",
		Logger:   b.logger,
	})

	b.gcDeletedTopic(context.Background(), dt)

	remaining, err := b.s3Client.ListObjects(context.Background(), "klite/test/"+s3store.TopicDir(dt.Name, dt.TopicID)+"/")
	if err != nil {
		t.Fatal(err)
	}
	if len(remaining) != 1 {
		t.Fatalf("expected 1 remaining object after partial delete failure, got %d", len(remaining))
	}

	drained := b.state.DrainDeletedTopics()
	if len(drained) != 1 {
		t.Fatalf("expected topic re-enqueued after partial delete failure, got %d", len(drained))
	}
	if drained[0] != dt {
		t.Fatalf("unexpected re-enqueued topic: got %+v, want %+v", drained[0], dt)
	}
}

func TestDeleteTopicObjectsRespectsContextCancellationBetweenTopics(t *testing.T) {
	t.Parallel()

	b, mem := newRetentionTestBroker(t, clock.NewFakeClock(time.UnixMilli(100000)))

	var firstID [16]byte
	firstID[0] = 1
	first := cluster.DeletedTopic{Name: "first", TopicID: firstID}
	var secondID [16]byte
	secondID[0] = 2
	second := cluster.DeletedTopic{Name: "second", TopicID: secondID}

	putTestObject(t, mem, first.Name, first.TopicID, 0, 0, []int64{90000})
	putTestObject(t, mem, second.Name, second.TopicID, 0, 0, []int64{90000})

	ctx, cancel := context.WithCancel(context.Background())
	b.s3Client = s3store.NewClient(s3store.ClientConfig{
		S3Client: &cancelOnListS3{
			InMemoryS3: mem,
			cancel:     cancel,
			prefix:     "klite/test/" + s3store.TopicDir(first.Name, first.TopicID) + "/",
		},
		Bucket: "test-bucket",
		Prefix: "klite/test",
		Logger: b.logger,
	})

	b.deleteTopicObjects(ctx, []cluster.DeletedTopic{first, second})

	firstObjs, err := b.s3Client.ListObjects(context.Background(), "klite/test/"+s3store.TopicDir(first.Name, first.TopicID)+"/")
	if err != nil {
		t.Fatal(err)
	}
	if len(firstObjs) != 0 {
		t.Fatalf("expected first topic objects to be deleted, got %d", len(firstObjs))
	}

	secondObjs, err := b.s3Client.ListObjects(context.Background(), "klite/test/"+s3store.TopicDir(second.Name, second.TopicID)+"/")
	if err != nil {
		t.Fatal(err)
	}
	if len(secondObjs) != 1 {
		t.Fatalf("expected second topic objects to remain due to cancellation, got %d", len(secondObjs))
	}
}

func TestS3GCLoopStartupDeletesPendingAndOrphans(t *testing.T) {
	t.Parallel()

	b, mem := newRetentionTestBroker(t, clock.NewFakeClock(time.UnixMilli(100000)))

	_, _ = b.state.CreateTopicWithConfigs("live", 1, nil)
	liveID := topicID(b, "live")
	putTestObject(t, mem, "live", liveID, 0, 0, []int64{90000})

	pendingTD, _ := b.state.CreateTopicWithConfigs("pending", 1, nil)
	pendingID := pendingTD.ID
	putTestObject(t, mem, "pending", pendingID, 0, 0, []int64{90000})
	b.state.DeleteTopic("pending")

	var orphanID [16]byte
	orphanID[0] = 3
	putTestObject(t, mem, "orphan-loop", orphanID, 0, 0, []int64{90000})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		b.s3GCLoop(ctx)
		close(done)
	}()

	deadline := time.Now().Add(time.Second)
	for {
		pendingObjs, err := b.s3Client.ListObjects(context.Background(), "klite/test/"+s3store.TopicDir("pending", pendingID)+"/")
		if err != nil {
			t.Fatal(err)
		}
		orphanObjs, err := b.s3Client.ListObjects(context.Background(), "klite/test/"+s3store.TopicDir("orphan-loop", orphanID)+"/")
		if err != nil {
			t.Fatal(err)
		}
		if len(pendingObjs) == 0 && len(orphanObjs) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for startup S3 GC to complete: pending=%d orphan=%d", len(pendingObjs), len(orphanObjs))
		}
		time.Sleep(time.Millisecond)
	}

	cancel()
	<-done

	pendingObjs, err := b.s3Client.ListObjects(context.Background(), "klite/test/"+s3store.TopicDir("pending", pendingID)+"/")
	if err != nil {
		t.Fatal(err)
	}
	if len(pendingObjs) != 0 {
		t.Fatalf("expected pending topic objects to be deleted, got %d", len(pendingObjs))
	}

	orphanObjs, err := b.s3Client.ListObjects(context.Background(), "klite/test/"+s3store.TopicDir("orphan-loop", orphanID)+"/")
	if err != nil {
		t.Fatal(err)
	}
	if len(orphanObjs) != 0 {
		t.Fatalf("expected orphan topic objects to be deleted, got %d", len(orphanObjs))
	}

	liveObjs := listObjectKeys(t, mem, "live", liveID, 0)
	if len(liveObjs) != 1 {
		t.Fatalf("expected live topic object to remain, got %d", len(liveObjs))
	}
}

type listFailingS3 struct {
	*s3store.InMemoryS3
	failPrefix string
}

func (l *listFailingS3) ListObjectsV2(ctx context.Context, input *s3svc.ListObjectsV2Input, opts ...func(*s3svc.Options)) (*s3svc.ListObjectsV2Output, error) {
	if strings.HasPrefix(aws.ToString(input.Prefix), l.failPrefix) {
		return nil, errors.New("injected list failure")
	}
	return l.InMemoryS3.ListObjectsV2(ctx, input, opts...)
}

type deleteRecordingS3 struct {
	*s3store.InMemoryS3
	deleted []string
}

func (d *deleteRecordingS3) DeleteObject(ctx context.Context, input *s3svc.DeleteObjectInput, opts ...func(*s3svc.Options)) (*s3svc.DeleteObjectOutput, error) {
	d.deleted = append(d.deleted, aws.ToString(input.Key))
	return d.InMemoryS3.DeleteObject(ctx, input, opts...)
}

type cancelOnListS3 struct {
	*s3store.InMemoryS3
	cancel func()
	prefix string
	once   bool
}

func (c *cancelOnListS3) ListObjectsV2(ctx context.Context, input *s3svc.ListObjectsV2Input, opts ...func(*s3svc.Options)) (*s3svc.ListObjectsV2Output, error) {
	if !c.once && strings.HasPrefix(aws.ToString(input.Prefix), c.prefix) {
		c.once = true
		c.cancel()
	}
	return c.InMemoryS3.ListObjectsV2(ctx, input, opts...)
}

type deleteFailAfterNS3 struct {
	*s3store.InMemoryS3
	failAfter int
	deletes   int
}

func (d *deleteFailAfterNS3) DeleteObject(ctx context.Context, input *s3svc.DeleteObjectInput, opts ...func(*s3svc.Options)) (*s3svc.DeleteObjectOutput, error) {
	if d.deletes >= d.failAfter {
		return nil, errors.New("injected delete failure")
	}
	d.deletes++
	return d.InMemoryS3.DeleteObject(ctx, input, opts...)
}

func intToStr(n int64) string {
	return fmt.Sprintf("%d", n)
}
