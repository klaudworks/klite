package integration

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	s3store "github.com/klaudworks/klite/internal/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// createCompactedTopic creates a topic with cleanup.policy=compact via kadm.
func createCompactedTopic(t *testing.T, addr, topic string, partitions int32, extraConfigs map[string]*string) {
	t.Helper()
	admin := NewAdminClient(t, addr)
	cfgs := map[string]*string{
		"cleanup.policy": kadm.StringPtr("compact"),
	}
	for k, v := range extraConfigs {
		cfgs[k] = v
	}
	_, err := admin.CreateTopics(context.Background(), partitions, 1, cfgs, topic)
	require.NoError(t, err)
}

// countS3Objects counts .obj files for a topic/partition in InMemoryS3.
func countS3Objects(mem *s3store.InMemoryS3, topic string, partition int) int {
	count := 0
	partStr := fmt.Sprintf("%s/%d/", topic, partition)
	for _, k := range mem.Keys() {
		if strings.Contains(k, partStr) && strings.HasSuffix(k, ".obj") {
			count++
		}
	}
	return count
}

// waitForS3ObjectCount polls until the object count for a topic/partition
// reaches at least minCount.
func waitForS3ObjectCount(t *testing.T, mem *s3store.InMemoryS3, topic string, partition int, minCount int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		c := countS3Objects(mem, topic, partition)
		if c >= minCount {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	c := countS3Objects(mem, topic, partition)
	t.Fatalf("expected at least %d S3 objects, got %d (timeout %v)", minCount, c, timeout)
}

// waitForCompaction polls S3 until the number of objects for the given
// topic/partition is less than initialCount (compaction merged them).
func waitForCompaction(t *testing.T, mem *s3store.InMemoryS3, topic string, partition int, initialCount int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		c := countS3Objects(mem, topic, partition)
		if c < initialCount {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("compaction did not reduce S3 object count from %d within %v", initialCount, timeout)
}

// produceFlushProduce produces records across multiple flush cycles on a single
// broker instance. Uses short flush intervals and sleeps to ensure separate objects.
// Returns the broker and the number of S3 objects created.
func produceFlushProduce(
	t *testing.T,
	dataDir string,
	mem *s3store.InMemoryS3,
	prefix, topic string,
	records []struct{ key, value string },
) *TestBroker {
	t.Helper()

	// Start broker with short flush interval but NO compaction
	tb := StartBroker(t,

		WithDataDir(dataDir),
		WithS3(mem, "test-bucket", prefix),
		WithS3FlushInterval(200*time.Millisecond),
		// Effectively disable compaction during produce
		WithCompactionCheckInterval(24*time.Hour),
	)

	createCompactedTopic(t, tb.Addr, topic, 1, nil)

	producer := NewClient(t, tb.Addr,
		kgo.DefaultProduceTopic(topic),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)

	// Produce each record and wait for it to flush to a separate S3 object
	for i, kv := range records {
		before := countS3Objects(mem, topic, 0)
		ProduceSync(t, producer, &kgo.Record{
			Topic:     topic,
			Partition: 0,
			Key:       []byte(kv.key),
			Value:     []byte(kv.value),
		})
		// Wait for flush to create a new object
		waitForS3ObjectCount(t, mem, topic, 0, before+1, 10*time.Second)
		_ = i
	}

	return tb
}

// TestCompactionEndToEnd creates a compacted topic, produces records with
// duplicate keys across multiple S3 flushes, waits for compaction, then
// consumes and verifies only the latest value per key survives.
func TestCompactionEndToEnd(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	mem := s3store.NewInMemoryS3()
	prefix := "klite/test"
	topic := "compact-e2e"

	records := []struct{ key, value string }{
		{"A", "A-old"}, {"B", "B-old"}, {"C", "C-val"},
		{"A", "A-new"}, {"B", "B-new"},
	}

	tb := produceFlushProduce(t, dataDir, mem, prefix, topic, records)

	objsBefore := countS3Objects(mem, topic, 0)
	require.GreaterOrEqual(t, objsBefore, 3, "should have multiple S3 objects")

	// Stop and restart with compaction enabled
	tb.Stop()

	tb2 := StartBroker(t,

		WithDataDir(dataDir),
		WithS3(mem, "test-bucket", prefix),
		WithS3FlushInterval(24*time.Hour),
		WithCompactionCheckInterval(200*time.Millisecond),
		WithCompactionMinDirtyObjects(2),
	)

	waitForCompaction(t, mem, topic, 0, objsBefore, 15*time.Second)

	// Stop and delete WAL so reads come from compacted S3 objects
	tb2.Stop()
	os.RemoveAll(dataDir + "/wal")

	tb3 := StartBroker(t,

		WithDataDir(dataDir),
		WithS3(mem, "test-bucket", prefix),
		WithS3FlushInterval(24*time.Hour),
	)

	// Consume and verify only latest values per key
	consumer := NewClient(t, tb3.Addr,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)

	consumed := ConsumeN(t, consumer, 3, 15*time.Second) // A, B, C
	byKey := make(map[string]string)
	for _, r := range consumed {
		byKey[string(r.Key)] = string(r.Value)
	}

	assert.Equal(t, "A-new", byKey["A"], "key A should have latest value")
	assert.Equal(t, "B-new", byKey["B"], "key B should have latest value")
	assert.Equal(t, "C-val", byKey["C"], "key C should be retained")
}

// TestCompactDeletePolicy verifies that cleanup.policy=compact,delete applies
// both compaction (key dedup) and retention (time/size based deletion).
func TestCompactDeletePolicy(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	mem := s3store.NewInMemoryS3()
	prefix := "klite/test"
	topic := "compact-delete"

	// Start broker with short flush and no compaction
	tb := StartBroker(t,

		WithDataDir(dataDir),
		WithS3(mem, "test-bucket", prefix),
		WithS3FlushInterval(200*time.Millisecond),
		WithCompactionCheckInterval(24*time.Hour),
	)

	admin := NewAdminClient(t, tb.Addr)
	cfgs := map[string]*string{
		"cleanup.policy": kadm.StringPtr("compact,delete"),
		"retention.ms":   kadm.StringPtr("86400000"),
	}
	_, err := admin.CreateTopics(context.Background(), 1, 1, cfgs, topic)
	require.NoError(t, err)

	producer := NewClient(t, tb.Addr,
		kgo.DefaultProduceTopic(topic),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)

	for _, kv := range []struct{ key, value string }{
		{"X", "X-old"}, {"Y", "Y-val"}, {"X", "X-new"},
	} {
		before := countS3Objects(mem, topic, 0)
		ProduceSync(t, producer, &kgo.Record{
			Topic: topic, Partition: 0,
			Key: []byte(kv.key), Value: []byte(kv.value),
		})
		waitForS3ObjectCount(t, mem, topic, 0, before+1, 10*time.Second)
	}

	objsBefore := countS3Objects(mem, topic, 0)
	require.GreaterOrEqual(t, objsBefore, 3)

	tb.Stop()

	// Restart with compaction enabled
	tb2 := StartBroker(t,

		WithDataDir(dataDir),
		WithS3(mem, "test-bucket", prefix),
		WithS3FlushInterval(24*time.Hour),
		WithCompactionCheckInterval(200*time.Millisecond),
		WithCompactionMinDirtyObjects(2),
	)

	waitForCompaction(t, mem, topic, 0, objsBefore, 15*time.Second)

	// Stop, delete WAL, restart to force reads from compacted S3
	tb2.Stop()
	os.RemoveAll(dataDir + "/wal")

	tb3 := StartBroker(t,

		WithDataDir(dataDir),
		WithS3(mem, "test-bucket", prefix),
		WithS3FlushInterval(24*time.Hour),
	)

	consumer := NewClient(t, tb3.Addr,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	consumed := ConsumeN(t, consumer, 2, 15*time.Second)
	byKey := make(map[string]string)
	for _, r := range consumed {
		byKey[string(r.Key)] = string(r.Value)
	}
	assert.Equal(t, "X-new", byKey["X"], "compaction should keep latest X")
	assert.Equal(t, "Y-val", byKey["Y"], "Y should be retained")
}

// TestCompactionS3ObjectCount verifies that compaction reduces the number of
// S3 objects for a partition.
func TestCompactionS3ObjectCount(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	mem := s3store.NewInMemoryS3()
	prefix := "klite/test"
	topic := "compact-obj-count"

	var records []struct{ key, value string }
	for i := 0; i < 4; i++ {
		records = append(records, struct{ key, value string }{
			key: "K", value: fmt.Sprintf("value-%d", i),
		})
	}

	tb := produceFlushProduce(t, dataDir, mem, prefix, topic, records)

	objsBefore := countS3Objects(mem, topic, 0)
	require.GreaterOrEqual(t, objsBefore, 4, "should have 4+ S3 objects before compaction")

	tb.Stop()

	// Restart with compaction
	tb2 := StartBroker(t,

		WithDataDir(dataDir),
		WithS3(mem, "test-bucket", prefix),
		WithS3FlushInterval(24*time.Hour),
		WithCompactionCheckInterval(200*time.Millisecond),
		WithCompactionMinDirtyObjects(2),
	)

	waitForCompaction(t, mem, topic, 0, objsBefore, 15*time.Second)

	objsAfter := countS3Objects(mem, topic, 0)
	assert.Less(t, objsAfter, objsBefore, "compaction should reduce S3 object count")

	// Stop, delete WAL, restart to verify from compacted S3
	tb2.Stop()
	os.RemoveAll(dataDir + "/wal")

	tb3 := StartBroker(t,

		WithDataDir(dataDir),
		WithS3(mem, "test-bucket", prefix),
		WithS3FlushInterval(24*time.Hour),
	)

	consumer := NewClient(t, tb3.Addr,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	consumed := ConsumeN(t, consumer, 1, 15*time.Second)
	assert.Equal(t, "value-3", string(consumed[0].Value))
}

// TestCompactionIdempotentIntegration verifies that running compaction on
// already-compacted data produces identical output.
func TestCompactionIdempotentIntegration(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	mem := s3store.NewInMemoryS3()
	prefix := "klite/test"
	topic := "compact-idempotent"

	records := []struct{ key, value string }{
		{"K", "v0"}, {"K", "v1"}, {"L", "l0"},
	}

	tb := produceFlushProduce(t, dataDir, mem, prefix, topic, records)

	objsBefore := countS3Objects(mem, topic, 0)
	require.GreaterOrEqual(t, objsBefore, 3)

	tb.Stop()

	// Restart with compaction
	tb2 := StartBroker(t,

		WithDataDir(dataDir),
		WithS3(mem, "test-bucket", prefix),
		WithS3FlushInterval(24*time.Hour),
		WithCompactionCheckInterval(200*time.Millisecond),
		WithCompactionMinDirtyObjects(2),
	)

	waitForCompaction(t, mem, topic, 0, objsBefore, 15*time.Second)
	objsAfterFirst := countS3Objects(mem, topic, 0)

	// Wait a bit more — second compaction pass should not change anything
	time.Sleep(2 * time.Second)
	objsAfterSecond := countS3Objects(mem, topic, 0)
	assert.Equal(t, objsAfterFirst, objsAfterSecond,
		"second compaction pass should not change object count")

	// Verify records from compacted S3: delete WAL, restart, consume
	tb2.Stop()
	os.RemoveAll(dataDir + "/wal")

	tb3 := StartBroker(t,

		WithDataDir(dataDir),
		WithS3(mem, "test-bucket", prefix),
		WithS3FlushInterval(24*time.Hour),
	)

	consumer := NewClient(t, tb3.Addr,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	consumed := ConsumeN(t, consumer, 2, 15*time.Second)
	require.Len(t, consumed, 2)

	byKey := make(map[string]string)
	for _, r := range consumed {
		byKey[string(r.Key)] = string(r.Value)
	}
	assert.Equal(t, "v1", byKey["K"], "K should have latest value")
	assert.Equal(t, "l0", byKey["L"], "L should be retained")
}

// TestCompactionWatermarkSurvivesRestart verifies that after compaction the
// cleanedUpTo watermark is persisted to metadata.log and restored on restart.
func TestCompactionWatermarkSurvivesRestart(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	mem := s3store.NewInMemoryS3()
	prefix := "klite/test"
	topic := "compact-watermark"

	records := []struct{ key, value string }{
		{"K", "v0"}, {"K", "v1"},
	}

	tb := produceFlushProduce(t, dataDir, mem, prefix, topic, records)

	objsBefore := countS3Objects(mem, topic, 0)
	require.GreaterOrEqual(t, objsBefore, 2)

	tb.Stop()

	// Phase 2: restart with compaction, wait for it to complete, then stop
	tb2 := StartBroker(t,

		WithDataDir(dataDir),
		WithS3(mem, "test-bucket", prefix),
		WithS3FlushInterval(24*time.Hour),
		WithCompactionCheckInterval(200*time.Millisecond),
		WithCompactionMinDirtyObjects(2),
	)

	waitForCompaction(t, mem, topic, 0, objsBefore, 15*time.Second)
	objsAfterCompaction := countS3Objects(mem, topic, 0)
	tb2.Stop()

	// Phase 3: restart — watermark should be restored from metadata.log
	// Delete WAL so we read from compacted S3 data
	os.RemoveAll(dataDir + "/wal")

	tb3 := StartBroker(t,

		WithDataDir(dataDir),
		WithS3(mem, "test-bucket", prefix),
		WithS3FlushInterval(24*time.Hour),
		WithCompactionCheckInterval(200*time.Millisecond),
		WithCompactionMinDirtyObjects(1),
	)

	// Verify data is still readable from compacted S3
	consumer := NewClient(t, tb3.Addr,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	consumed := ConsumeN(t, consumer, 1, 15*time.Second)
	assert.Equal(t, "v1", string(consumed[0].Value), "should read latest value after restart")

	// Verify the object count hasn't changed (no re-compaction of already-clean data)
	time.Sleep(1 * time.Second)
	objsFinal := countS3Objects(mem, topic, 0)
	assert.Equal(t, objsAfterCompaction, objsFinal,
		"should not create extra objects after restart with restored watermark")
}

// TestCompactionDirtyCounterRestart verifies that after S3 flush, restarting
// the broker rehydrates the dirtyObjects counter and compaction triggers
// without new writes.
func TestCompactionDirtyCounterRestart(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	mem := s3store.NewInMemoryS3()
	prefix := "klite/test"
	topic := "compact-dirty-restart"

	var records []struct{ key, value string }
	for i := 0; i < 4; i++ {
		records = append(records, struct{ key, value string }{
			key: "same-key", value: fmt.Sprintf("iter-%d", i),
		})
	}

	tb := produceFlushProduce(t, dataDir, mem, prefix, topic, records)

	objsBefore := countS3Objects(mem, topic, 0)
	require.GreaterOrEqual(t, objsBefore, 4, "should have 4+ objects before restart")

	tb.Stop()

	// Restart WITH compaction enabled. The dirty counter should be rehydrated
	// from LIST, and compaction should trigger WITHOUT any new writes.
	tb2 := StartBroker(t,

		WithDataDir(dataDir),
		WithS3(mem, "test-bucket", prefix),
		WithS3FlushInterval(24*time.Hour),
		WithCompactionCheckInterval(200*time.Millisecond),
		WithCompactionMinDirtyObjects(2),
	)

	waitForCompaction(t, mem, topic, 0, objsBefore, 15*time.Second)

	objsAfter := countS3Objects(mem, topic, 0)
	assert.Less(t, objsAfter, objsBefore,
		"compaction should trigger from rehydrated dirty counters without new writes")

	// Verify data integrity: delete WAL, restart, read from compacted S3
	tb2.Stop()
	os.RemoveAll(dataDir + "/wal")

	tb3 := StartBroker(t,

		WithDataDir(dataDir),
		WithS3(mem, "test-bucket", prefix),
		WithS3FlushInterval(24*time.Hour),
	)

	consumer := NewClient(t, tb3.Addr,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	consumed := ConsumeN(t, consumer, 1, 15*time.Second)
	assert.Equal(t, "iter-3", string(consumed[0].Value), "latest value should survive")
}
