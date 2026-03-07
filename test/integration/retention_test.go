package integration

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	s3store "github.com/klaudworks/klite/internal/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// lookupTopicID resolves a topic name to its TopicID via Metadata.
func lookupTopicID(t *testing.T, cl *kgo.Client, topic string) [16]byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	adm := kadm.NewClient(cl)
	details, err := adm.ListTopics(ctx, topic)
	require.NoError(t, err)
	td, ok := details[topic]
	require.True(t, ok, "topic %q not found via Metadata", topic)
	return td.ID
}

// buildFetchAtOffset builds a Fetch request for a single partition.
func buildFetchAtOffset(t *testing.T, cl *kgo.Client, topic string, partition int32, offset int64) *kmsg.FetchRequest {
	t.Helper()
	topicID := lookupTopicID(t, cl, topic)

	fetchReq := kmsg.NewFetchRequest()
	fetchReq.MaxWaitMillis = 100
	ft := kmsg.NewFetchRequestTopic()
	ft.Topic = topic
	ft.TopicID = topicID
	fp := kmsg.NewFetchRequestTopicPartition()
	fp.Partition = partition
	fp.FetchOffset = offset
	fp.PartitionMaxBytes = 1024 * 1024
	ft.Partitions = append(ft.Partitions, fp)
	fetchReq.Topics = append(fetchReq.Topics, ft)
	return &fetchReq
}

// createTopicWithRetention creates a topic with retention.ms set.
func createTopicWithRetention(t *testing.T, cl *kgo.Client, topic string, partitions int, retentionMs string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	createReq := kmsg.NewCreateTopicsRequest()
	createReq.Version = 7
	rt := kmsg.NewCreateTopicsRequestTopic()
	rt.Topic = topic
	rt.NumPartitions = int32(partitions)
	rt.ReplicationFactor = 1
	c := kmsg.NewCreateTopicsRequestTopicConfig()
	c.Name = "retention.ms"
	c.Value = &retentionMs
	rt.Configs = append(rt.Configs, c)
	createReq.Topics = append(createReq.Topics, rt)

	resp, err := createReq.RequestWith(ctx, cl)
	require.NoError(t, err)
	require.Len(t, resp.Topics, 1)
	require.Equal(t, int16(0), resp.Topics[0].ErrorCode, "create topic %s: %s", topic, kerr.ErrorForCode(resp.Topics[0].ErrorCode))
}

// produceRecordWithTimestamp produces a single record with a specific timestamp.
func produceRecordWithTimestamp(t *testing.T, cl *kgo.Client, topic string, partition int32, key string, ts time.Time) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r := &kgo.Record{
		Topic:     topic,
		Partition: partition,
		Key:       []byte(key),
		Value:     []byte("v"),
		Timestamp: ts,
	}
	results := cl.ProduceSync(ctx, r)
	require.NoError(t, results.FirstErr(), "produce record %s", key)
}

// waitForS3Objects polls until at least minCount .obj files exist for a
// topic/partition in the given InMemoryS3.
func waitForS3Objects(t *testing.T, mem *s3store.InMemoryS3, topic string, partition, minCount int, timeout time.Duration) {
	t.Helper()
	topicPrefix := fmt.Sprintf("%s-", topic)
	partSuffix := fmt.Sprintf("/%d/", partition)
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		count := 0
		for _, k := range mem.Keys() {
			if strings.Contains(k, topicPrefix) && strings.Contains(k, partSuffix) && strings.HasSuffix(k, ".obj") {
				count++
			}
		}
		if count >= minCount {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d S3 objects for %s/%d (timeout %v)", minCount, topic, partition, timeout)
}

// waitForLogStartAdvance polls ListOffsets(Earliest) until logStartOffset > minOffset.
func waitForLogStartAdvance(t *testing.T, cl *kgo.Client, topic string, partition int32, minOffset int64, timeout time.Duration) int64 {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		listReq := kmsg.NewListOffsetsRequest()
		listReq.Version = 7
		lt := kmsg.NewListOffsetsRequestTopic()
		lt.Topic = topic
		lp := kmsg.NewListOffsetsRequestTopicPartition()
		lp.Partition = partition
		lp.Timestamp = -2 // Earliest
		lt.Partitions = append(lt.Partitions, lp)
		listReq.Topics = append(listReq.Topics, lt)

		listResp, err := listReq.RequestWith(ctx, cl)
		cancel()
		if err == nil && len(listResp.Topics) > 0 && len(listResp.Topics[0].Partitions) > 0 {
			offset := listResp.Topics[0].Partitions[0].Offset
			if offset > minOffset {
				return offset
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for logStartOffset to advance past %d for %s/%d", minOffset, topic, partition)
	return 0
}

// produceOldAndNew produces old (expired) and recent records, waits for both
// batches to flush to S3, and waits for retention to advance logStartOffset.
// Returns the client used for producing.
func produceOldAndNew(t *testing.T, tb *TestBroker, mem *s3store.InMemoryS3, topic string) *kgo.Client {
	t.Helper()
	cl := NewClient(t, tb.Addr)
	createTopicWithRetention(t, cl, topic, 1, "5000")

	// Produce old records (10s ago — clearly expired with 5s retention)
	oldTs := time.Now().Add(-10 * time.Second)
	for i := 0; i < 5; i++ {
		produceRecordWithTimestamp(t, cl, topic, 0, fmt.Sprintf("old-%d", i), oldTs)
	}

	// Wait for old records to land in S3
	waitForS3Objects(t, mem, topic, 0, 1, 10*time.Second)

	// Produce recent records (well within 5s retention)
	recentTs := time.Now()
	for i := 0; i < 3; i++ {
		produceRecordWithTimestamp(t, cl, topic, 0, fmt.Sprintf("new-%d", i), recentTs)
	}

	// Wait for recent records to flush (second S3 object)
	waitForS3Objects(t, mem, topic, 0, 2, 10*time.Second)

	// Wait for retention to advance logStartOffset past 0
	waitForLogStartAdvance(t, cl, topic, 0, 0, 10*time.Second)

	return cl
}

// TestRetentionConsumerReset verifies that a consumer reading old data gets
// OFFSET_OUT_OF_RANGE after retention, and can reset to earliest.
func TestRetentionConsumerReset(t *testing.T) {
	t.Parallel()

	mem := s3store.NewInMemoryS3()
	tb := StartBroker(t,
		WithAutoCreateTopics(false),
		WithS3(mem, "test-bucket", "klite/ret-reset"),
		WithS3FlushInterval(200*time.Millisecond),
		WithRetentionCheckInterval(500*time.Millisecond),
	)

	topic := "retention-consumer-reset"
	cl := produceOldAndNew(t, tb, mem, topic)

	// Fetch from offset 0 — should get OFFSET_OUT_OF_RANGE
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	fetchReq := buildFetchAtOffset(t, cl, topic, 0, 0)
	fetchResp, err := fetchReq.RequestWith(ctx, cl)
	require.NoError(t, err)
	require.Len(t, fetchResp.Topics, 1)
	require.Len(t, fetchResp.Topics[0].Partitions, 1)

	fetchPart := fetchResp.Topics[0].Partitions[0]
	assert.Equal(t, kerr.OffsetOutOfRange.Code, fetchPart.ErrorCode,
		"fetching deleted offset should return OFFSET_OUT_OF_RANGE")

	// Consume from earliest — should get only the recent records
	consumer := NewClient(t, tb.Addr,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)

	consumed := ConsumeN(t, consumer, 3, 5*time.Second)
	require.Len(t, consumed, 3)
}

// TestRetentionSurvivesRestart verifies that logStartOffset is restored after restart.
func TestRetentionSurvivesRestart(t *testing.T) {
	t.Parallel()

	mem := s3store.NewInMemoryS3()
	dataDir := t.TempDir()
	topic := "retention-restart"

	// Phase 1: Start broker, produce old data, flush to S3, trigger retention
	func() {
		tb := StartBroker(t,
			WithAutoCreateTopics(false),
			WithDataDir(dataDir),
			WithS3(mem, "test-bucket", "klite/ret-restart"),
			WithS3FlushInterval(200*time.Millisecond),
			WithRetentionCheckInterval(500*time.Millisecond),
		)

		cl := produceOldAndNew(t, tb, mem, topic)

		// Verify logStartOffset advanced before stopping
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		adm := kadm.NewClient(cl)
		offsets, err := adm.ListStartOffsets(ctx, topic)
		require.NoError(t, err)
		o, ok := offsets.Lookup(topic, 0)
		require.True(t, ok)
		require.Greater(t, o.Offset, int64(0), "logStartOffset should advance before restart")
	}()

	// Phase 2: Restart broker with same data dir + same S3
	tb2 := StartBroker(t,
		WithAutoCreateTopics(false),
		WithDataDir(dataDir),
		WithS3(mem, "test-bucket", "klite/ret-restart"),
		WithS3FlushInterval(24*time.Hour),
		WithRetentionCheckInterval(5*time.Minute),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cl2 := NewClient(t, tb2.Addr)
	listReq := kmsg.NewListOffsetsRequest()
	listReq.Version = 7
	lt := kmsg.NewListOffsetsRequestTopic()
	lt.Topic = topic
	lp := kmsg.NewListOffsetsRequestTopicPartition()
	lp.Partition = 0
	lp.Timestamp = -2
	lt.Partitions = append(lt.Partitions, lp)
	listReq.Topics = append(listReq.Topics, lt)

	listResp, err := listReq.RequestWith(ctx, cl2)
	require.NoError(t, err)
	require.Len(t, listResp.Topics, 1)
	require.Len(t, listResp.Topics[0].Partitions, 1)

	restoredLogStart := listResp.Topics[0].Partitions[0].Offset
	assert.Greater(t, restoredLogStart, int64(0),
		"logStartOffset should be restored from metadata.log after restart")

	fetchReq := buildFetchAtOffset(t, cl2, topic, 0, 0)
	fetchResp, err := fetchReq.RequestWith(ctx, cl2)
	require.NoError(t, err)
	require.Len(t, fetchResp.Topics, 1)
	require.Len(t, fetchResp.Topics[0].Partitions, 1)
	fetchPart := fetchResp.Topics[0].Partitions[0]
	assert.Equal(t, kerr.OffsetOutOfRange.Code, fetchPart.ErrorCode,
		"fetching deleted offset after restart should return OFFSET_OUT_OF_RANGE")
}
