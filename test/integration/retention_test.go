package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

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
// Uses TopicID (for v13+) resolved from the broker's Metadata.
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

// produceRecordWithTimestamp produces a single record with a specific timestamp
// using the kgo client, which builds proper RecordBatch encoding.
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

// TestRetentionEnforcement creates a topic with short retention, produces
// records with old timestamps, triggers retention, and verifies deletion.
func TestRetentionEnforcement(t *testing.T) {
	t.Parallel()

	// Use a short retention check interval so we don't wait 5 minutes
	tb := StartBroker(t,
		WithAutoCreateTopics(false),
		WithRetentionCheckInterval(500*time.Millisecond),
	)
	cl := NewClient(t, tb.Addr)

	topic := "retention-test"

	// Create topic with 5 second retention
	createTopicWithRetention(t, cl, topic, 1, "5000")

	// Produce records with old timestamps (10 seconds ago — clearly expired)
	oldTs := time.Now().Add(-10 * time.Second)
	for i := 0; i < 5; i++ {
		produceRecordWithTimestamp(t, cl, topic, 0, fmt.Sprintf("old-%d", i), oldTs.Add(time.Duration(i)*time.Millisecond))
	}

	// Produce records with current timestamps (well within 5s retention)
	recentTs := time.Now()
	for i := 0; i < 3; i++ {
		produceRecordWithTimestamp(t, cl, topic, 0, fmt.Sprintf("new-%d", i), recentTs.Add(time.Duration(i)*time.Millisecond))
	}

	// Wait for retention to run (check interval is 500ms, wait a few cycles)
	time.Sleep(2 * time.Second)

	// Check ListOffsets earliest - should have advanced past the old records
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	listReq := kmsg.NewListOffsetsRequest()
	listReq.Version = 7
	lt := kmsg.NewListOffsetsRequestTopic()
	lt.Topic = topic
	lp := kmsg.NewListOffsetsRequestTopicPartition()
	lp.Partition = 0
	lp.Timestamp = -2 // Earliest
	lt.Partitions = append(lt.Partitions, lp)
	listReq.Topics = append(listReq.Topics, lt)

	listResp, err := listReq.RequestWith(ctx, cl)
	require.NoError(t, err)
	require.Len(t, listResp.Topics, 1)
	require.Len(t, listResp.Topics[0].Partitions, 1)

	lsPartition := listResp.Topics[0].Partitions[0]
	assert.Equal(t, int16(0), lsPartition.ErrorCode)
	// Earliest offset should have advanced past 0 (old records deleted)
	assert.Greater(t, lsPartition.Offset, int64(0), "logStartOffset should have advanced past 0")
}

// TestRetentionConsumerReset verifies that a consumer reading old data gets
// OFFSET_OUT_OF_RANGE after retention, and can reset to earliest.
func TestRetentionConsumerReset(t *testing.T) {
	t.Parallel()

	tb := StartBroker(t,
		WithAutoCreateTopics(false),
		WithRetentionCheckInterval(500*time.Millisecond),
	)
	cl := NewClient(t, tb.Addr)

	topic := "retention-consumer-reset"

	// Create topic with 5 second retention
	createTopicWithRetention(t, cl, topic, 1, "5000")

	// Produce old records (10 seconds ago — clearly expired)
	oldTs := time.Now().Add(-10 * time.Second)
	for i := 0; i < 5; i++ {
		produceRecordWithTimestamp(t, cl, topic, 0, fmt.Sprintf("old-%d", i), oldTs)
	}

	// Produce recent records (well within 5s retention)
	recentTs := time.Now()
	for i := 0; i < 3; i++ {
		produceRecordWithTimestamp(t, cl, topic, 0, fmt.Sprintf("new-%d", i), recentTs)
	}

	// Wait for retention
	time.Sleep(2 * time.Second)

	// Try to fetch from offset 0 - should get OFFSET_OUT_OF_RANGE
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

	// Now consume from earliest - should get only the recent records
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

	dataDir := t.TempDir()
	topic := "retention-restart"

	// Phase 1: Start broker, produce with old timestamps, trigger retention
	func() {
		tb := StartBroker(t,
			WithAutoCreateTopics(false),
			WithWALEnabled(true),
			WithDataDir(dataDir),
			WithRetentionCheckInterval(500*time.Millisecond),
		)
		cl := NewClient(t, tb.Addr)

		createTopicWithRetention(t, cl, topic, 1, "5000")

		// Produce old records (10 seconds ago — clearly expired)
		oldTs := time.Now().Add(-10 * time.Second)
		for i := 0; i < 5; i++ {
			produceRecordWithTimestamp(t, cl, topic, 0, fmt.Sprintf("old-%d", i), oldTs)
		}

		// Produce recent records (well within 5s retention)
		recentTs := time.Now()
		for i := 0; i < 3; i++ {
			produceRecordWithTimestamp(t, cl, topic, 0, fmt.Sprintf("new-%d", i), recentTs)
		}

		// Wait for retention to run
		time.Sleep(2 * time.Second)

		// Verify logStartOffset advanced
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		listReq := kmsg.NewListOffsetsRequest()
		listReq.Version = 7
		lt := kmsg.NewListOffsetsRequestTopic()
		lt.Topic = topic
		lp := kmsg.NewListOffsetsRequestTopicPartition()
		lp.Partition = 0
		lp.Timestamp = -2
		lt.Partitions = append(lt.Partitions, lp)
		listReq.Topics = append(listReq.Topics, lt)

		listResp, err := listReq.RequestWith(ctx, cl)
		require.NoError(t, err)
		require.Greater(t, listResp.Topics[0].Partitions[0].Offset, int64(0),
			"logStartOffset should advance before restart")
	}()

	// Phase 2: Restart broker with same data dir
	tb2 := StartBroker(t,
		WithAutoCreateTopics(false),
		WithWALEnabled(true),
		WithDataDir(dataDir),
		WithRetentionCheckInterval(5*time.Minute), // long interval to avoid re-triggering
	)

	// Verify logStartOffset is restored from metadata.log
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

	// Verify deleted data does not reappear
	fetchReq := buildFetchAtOffset(t, cl2, topic, 0, 0)
	fetchResp, err := fetchReq.RequestWith(ctx, cl2)
	require.NoError(t, err)
	require.Len(t, fetchResp.Topics, 1)
	require.Len(t, fetchResp.Topics[0].Partitions, 1)
	fetchPart := fetchResp.Topics[0].Partitions[0]
	assert.Equal(t, kerr.OffsetOutOfRange.Code, fetchPart.ErrorCode,
		"fetching deleted offset after restart should return OFFSET_OUT_OF_RANGE")
}
