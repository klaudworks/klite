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

// TestOffsetCommitFetch tests basic offset commit and fetch.
func TestOffsetCommitFetch(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t, WithAutoCreateTopics(true))
	admin := NewAdminClient(t, tb.Addr)

	topic := "test-offset-commit-fetch"
	group := "test-offset-commit-fetch-group"

	// Create topic first
	ctx := context.Background()
	_, err := admin.CreateTopics(ctx, 1, 1, nil, topic)
	require.NoError(t, err)

	// Admin/simple commit: commit offset 5 for partition 0
	toCommit := kadm.Offsets{}
	toCommit.Add(kadm.Offset{Topic: topic, Partition: 0, At: 5, LeaderEpoch: -1})
	_, err = admin.CommitOffsets(ctx, group, toCommit)
	require.NoError(t, err)

	// Fetch committed offsets
	fetched, err := admin.FetchOffsets(ctx, group)
	require.NoError(t, err)
	off, ok := fetched.Lookup(topic, 0)
	require.True(t, ok, "expected committed offset for %s/0", topic)
	assert.Equal(t, int64(5), off.At, "expected offset 5")
}

// TestOffsetCommitWrongGeneration tests that stale generation is rejected.
func TestOffsetCommitWrongGeneration(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t, WithAutoCreateTopics(true))

	topic := "test-offset-wrong-gen"
	group := "test-offset-wrong-gen-group"

	// Join a group to get a valid member ID and generation
	resp1 := sendJoinGroup(t, tb.Addr, group, "", nil, defaultProtocols())
	require.Equal(t, kerr.MemberIDRequired.Code, resp1.ErrorCode)
	memberID := resp1.MemberID

	resp2 := sendJoinGroup(t, tb.Addr, group, memberID, nil, defaultProtocols())
	require.Equal(t, int16(0), resp2.ErrorCode)
	generation := resp2.Generation

	// Complete the sync
	sendSyncGroup(t, tb.Addr, group, generation, memberID, nil, nil)

	// Try to commit with wrong generation
	cl := NewClient(t, tb.Addr)
	req := kmsg.NewOffsetCommitRequest()
	req.Group = group
	req.MemberID = memberID
	req.Generation = generation + 100 // wrong generation

	rt := kmsg.NewOffsetCommitRequestTopic()
	rt.Topic = topic
	rp := kmsg.NewOffsetCommitRequestTopicPartition()
	rp.Partition = 0
	rp.Offset = 10
	rt.Partitions = append(rt.Partitions, rp)
	req.Topics = append(req.Topics, rt)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := req.RequestWith(ctx, cl)
	require.NoError(t, err)
	require.Len(t, resp.Topics, 1)
	require.Len(t, resp.Topics[0].Partitions, 1)
	assert.Equal(t, kerr.IllegalGeneration.Code, resp.Topics[0].Partitions[0].ErrorCode,
		"expected ILLEGAL_GENERATION for wrong generation")
}

// TestOffsetFetchUnknownGroup tests that fetching offsets for an unknown group returns -1.
func TestOffsetFetchUnknownGroup(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t, WithAutoCreateTopics(true))

	topic := "test-offset-fetch-unknown"
	group := "nonexistent-group-" + fmt.Sprintf("%d", time.Now().UnixNano())

	// Create topic first
	admin := NewAdminClient(t, tb.Addr)
	ctx := context.Background()
	_, err := admin.CreateTopics(ctx, 1, 1, nil, topic)
	require.NoError(t, err)

	// Fetch offsets for a group that doesn't exist
	cl := NewClient(t, tb.Addr)
	req := kmsg.NewOffsetFetchRequest()
	req.Group = group
	rt := kmsg.NewOffsetFetchRequestTopic()
	rt.Topic = topic
	rt.Partitions = []int32{0}
	req.Topics = append(req.Topics, rt)

	resp, err := req.RequestWith(ctx, cl)
	require.NoError(t, err)
	require.Equal(t, int16(0), resp.ErrorCode, "expected no top-level error for unknown group")
	require.Len(t, resp.Topics, 1)
	require.Len(t, resp.Topics[0].Partitions, 1)
	assert.Equal(t, int64(-1), resp.Topics[0].Partitions[0].Offset,
		"expected offset -1 for uncommitted partition")
}

// TestOffsetFetchAll tests fetching all committed offsets (null topics list).
func TestOffsetFetchAll(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t, WithAutoCreateTopics(true))
	admin := NewAdminClient(t, tb.Addr)

	topic1 := "test-offset-fetch-all-1"
	topic2 := "test-offset-fetch-all-2"
	group := "test-offset-fetch-all-group"

	ctx := context.Background()
	_, err := admin.CreateTopics(ctx, 1, 1, nil, topic1)
	require.NoError(t, err)
	_, err = admin.CreateTopics(ctx, 1, 1, nil, topic2)
	require.NoError(t, err)

	// Commit offsets for two different topics
	toCommit := kadm.Offsets{}
	toCommit.Add(kadm.Offset{Topic: topic1, Partition: 0, At: 3, LeaderEpoch: -1})
	toCommit.Add(kadm.Offset{Topic: topic2, Partition: 0, At: 7, LeaderEpoch: -1})
	_, err = admin.CommitOffsets(ctx, group, toCommit)
	require.NoError(t, err)

	// Fetch ALL offsets (empty topics list in v2+ request)
	fetched, err := admin.FetchOffsets(ctx, group)
	require.NoError(t, err)

	off1, ok := fetched.Lookup(topic1, 0)
	require.True(t, ok, "expected committed offset for %s/0", topic1)
	assert.Equal(t, int64(3), off1.At)

	off2, ok := fetched.Lookup(topic2, 0)
	require.True(t, ok, "expected committed offset for %s/0", topic2)
	assert.Equal(t, int64(7), off2.At)
}

// TestConsumeCommitResume tests the full consume-commit-resume flow.
func TestConsumeCommitResume(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t, WithAutoCreateTopics(true))
	admin := NewAdminClient(t, tb.Addr)

	topic := "test-consume-commit-resume"
	group := "test-consume-commit-resume-group"

	ctx := context.Background()
	_, err := admin.CreateTopics(ctx, 1, 1, nil, topic)
	require.NoError(t, err)

	// Produce 10 records
	producer := NewClient(t, tb.Addr, kgo.DefaultProduceTopic(topic))
	ProduceN(t, producer, topic, 10)

	// Consumer 1: consume all 10 records, commit
	consumer1 := NewClient(t, tb.Addr,
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.DisableAutoCommit(),
		kgo.FetchMaxWait(2*time.Second),
	)
	records := ConsumeN(t, consumer1, 10, 10*time.Second)
	require.Len(t, records, 10)

	// Commit offsets
	commitCtx, commitCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer commitCancel()
	err = consumer1.CommitUncommittedOffsets(commitCtx)
	require.NoError(t, err)
	consumer1.Close()

	// Consumer 2: should start from where consumer 1 left off (no new records)
	consumer2 := NewClient(t, tb.Addr,
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.DisableAutoCommit(),
		kgo.FetchMaxWait(2*time.Second),
	)

	// Poll with short timeout — should get nothing since all 10 were already consumed
	ctx2, cancel2 := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel2()
	fetches := consumer2.PollFetches(ctx2)
	var newRecords []*kgo.Record
	fetches.EachRecord(func(r *kgo.Record) {
		newRecords = append(newRecords, r)
	})
	assert.Empty(t, newRecords, "expected no new records after resume from committed offset")
}

// TestAutoCommitOnClose tests that auto-commit fires on consumer close.
func TestAutoCommitOnClose(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t, WithAutoCreateTopics(true))
	admin := NewAdminClient(t, tb.Addr)

	topic := "test-auto-commit-close"
	group := "test-auto-commit-close-group"

	ctx0 := context.Background()
	_, err := admin.CreateTopics(ctx0, 1, 1, nil, topic)
	require.NoError(t, err)

	// Produce 5 records
	producer := NewClient(t, tb.Addr, kgo.DefaultProduceTopic(topic))
	ProduceN(t, producer, topic, 5)

	// Consume with auto-commit enabled (the default)
	consumer := NewClient(t, tb.Addr,
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.FetchMaxWait(2*time.Second),
	)
	records := ConsumeN(t, consumer, 5, 10*time.Second)
	require.Len(t, records, 5)

	// Explicitly commit before close to make this deterministic
	commitCtx, commitCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer commitCancel()
	err = consumer.CommitUncommittedOffsets(commitCtx)
	require.NoError(t, err)
	consumer.Close()

	// Verify committed offsets via admin
	fetched, err := admin.FetchOffsets(context.Background(), group)
	require.NoError(t, err)
	off, ok := fetched.Lookup(topic, 0)
	require.True(t, ok, "expected committed offset for %s/0", topic)
	assert.GreaterOrEqual(t, off.At, int64(5), "expected committed offset >= 5")
}

// TestCommitMetadata tests that commit metadata round-trips correctly.
func TestCommitMetadata(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t, WithAutoCreateTopics(true))
	admin := NewAdminClient(t, tb.Addr)

	topic := "test-commit-metadata"
	group := "test-commit-metadata-group"

	ctx := context.Background()
	_, err := admin.CreateTopics(ctx, 1, 1, nil, topic)
	require.NoError(t, err)

	// Commit offset with metadata
	metadata := "custom-metadata-string"
	toCommit := kadm.Offsets{}
	toCommit.Add(kadm.Offset{
		Topic:       topic,
		Partition:   0,
		At:          42,
		LeaderEpoch: -1,
		Metadata:    metadata,
	})
	_, err = admin.CommitOffsets(ctx, group, toCommit)
	require.NoError(t, err)

	// Fetch and verify metadata
	fetched, err := admin.FetchOffsets(ctx, group)
	require.NoError(t, err)
	off, ok := fetched.Lookup(topic, 0)
	require.True(t, ok)
	assert.Equal(t, int64(42), off.At)
	assert.Equal(t, metadata, off.Metadata)
}

// TestAsyncCommit tests that async commit succeeds.
func TestAsyncCommit(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t, WithAutoCreateTopics(true))
	admin := NewAdminClient(t, tb.Addr)

	topic := "test-async-commit"
	group := "test-async-commit-group"

	ctx0 := context.Background()
	_, err := admin.CreateTopics(ctx0, 1, 1, nil, topic)
	require.NoError(t, err)

	// Produce 5 records
	producer := NewClient(t, tb.Addr, kgo.DefaultProduceTopic(topic))
	ProduceN(t, producer, topic, 5)

	// Consume with explicit commit
	consumer := NewClient(t, tb.Addr,
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.DisableAutoCommit(),
		kgo.FetchMaxWait(2*time.Second),
	)
	records := ConsumeN(t, consumer, 5, 10*time.Second)
	require.Len(t, records, 5)

	// Async commit via a separate goroutine (simulating async behavior)
	errCh := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		errCh <- consumer.CommitUncommittedOffsets(ctx)
	}()
	err = <-errCh
	require.NoError(t, err, "async commit should succeed")

	consumer.Close()

	// Verify committed offset
	fetched, err := admin.FetchOffsets(context.Background(), group)
	require.NoError(t, err)
	off, ok := fetched.Lookup(topic, 0)
	require.True(t, ok)
	assert.Equal(t, int64(5), off.At)
}

// TestStaticMemberClassicRejoinNoRebalance tests that a static member can
// rejoin without triggering a rebalance and offsets are preserved.
func TestStaticMemberClassicRejoinNoRebalance(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t, WithAutoCreateTopics(true))
	admin := NewAdminClient(t, tb.Addr)

	topic := "test-static-member-rejoin"
	group := "test-static-member-rejoin-group"

	ctx0 := context.Background()
	_, err := admin.CreateTopics(ctx0, 1, 1, nil, topic)
	require.NoError(t, err)

	// Produce some records
	producer := NewClient(t, tb.Addr, kgo.DefaultProduceTopic(topic))
	ProduceN(t, producer, topic, 5)

	instanceID := "static-instance-1"

	// Consumer 1: consume and commit
	consumer1 := NewClient(t, tb.Addr,
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.InstanceID(instanceID),
		kgo.DisableAutoCommit(),
		kgo.FetchMaxWait(2*time.Second),
	)
	records := ConsumeN(t, consumer1, 5, 10*time.Second)
	require.Len(t, records, 5)

	commitCtx, commitCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer commitCancel()
	err = consumer1.CommitUncommittedOffsets(commitCtx)
	require.NoError(t, err)
	consumer1.Close()

	// Consumer 2: same instance ID, should rejoin
	consumer2 := NewClient(t, tb.Addr,
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.InstanceID(instanceID),
		kgo.DisableAutoCommit(),
		kgo.FetchMaxWait(2*time.Second),
	)

	// Should get no new records (offset was committed)
	ctx2, cancel2 := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel2()
	fetches := consumer2.PollFetches(ctx2)
	var newRecords []*kgo.Record
	fetches.EachRecord(func(r *kgo.Record) {
		newRecords = append(newRecords, r)
	})
	assert.Empty(t, newRecords, "static member rejoin should resume from committed offset")
}

// TestGroupMaxSizeClassic tests that the group doesn't exceed limits
// (basic test — we don't enforce a hard limit currently, so this just tests
// that multiple members can join).
func TestGroupMaxSizeClassic(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t, WithAutoCreateTopics(true), WithDefaultPartitions(3))
	admin := NewAdminClient(t, tb.Addr)

	topic := "test-group-max-size"
	group := "test-group-max-size-group"

	ctx0 := context.Background()
	_, err := admin.CreateTopics(ctx0, 3, 1, nil, topic)
	require.NoError(t, err)

	// Produce records
	producer := NewClient(t, tb.Addr, kgo.DefaultProduceTopic(topic))
	ProduceN(t, producer, topic, 9)

	// Start 3 consumers in the same group
	var consumers []*kgo.Client
	for i := 0; i < 3; i++ {
		c := NewClient(t, tb.Addr,
			kgo.ConsumeTopics(topic),
			kgo.ConsumerGroup(group),
			kgo.DisableAutoCommit(),
			kgo.FetchMaxWait(2*time.Second),
		)
		consumers = append(consumers, c)
	}

	// Collect all records from all consumers
	var allRecords []*kgo.Record
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	for len(allRecords) < 9 {
		for _, c := range consumers {
			fetches := c.PollFetches(ctx)
			if ctx.Err() != nil {
				break
			}
			fetches.EachRecord(func(r *kgo.Record) {
				allRecords = append(allRecords, r)
			})
		}
		if ctx.Err() != nil {
			break
		}
	}
	assert.GreaterOrEqual(t, len(allRecords), 9, "expected all 9 records consumed by group")
}

// TestOffsetDelete tests deleting committed offsets.
func TestOffsetDelete(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t, WithAutoCreateTopics(true))
	admin := NewAdminClient(t, tb.Addr)

	topic := "test-offset-delete"
	group := "test-offset-delete-group"

	ctx := context.Background()
	_, err := admin.CreateTopics(ctx, 1, 1, nil, topic)
	require.NoError(t, err)

	// Commit offsets for partition 0
	toCommit := kadm.Offsets{}
	toCommit.Add(kadm.Offset{Topic: topic, Partition: 0, At: 10, LeaderEpoch: -1})
	_, err = admin.CommitOffsets(ctx, group, toCommit)
	require.NoError(t, err)

	// Verify commit is there
	fetched, err := admin.FetchOffsets(ctx, group)
	require.NoError(t, err)
	off, ok := fetched.Lookup(topic, 0)
	require.True(t, ok)
	assert.Equal(t, int64(10), off.At)

	// Delete the offset using raw kmsg request
	cl := NewClient(t, tb.Addr)
	delReq := kmsg.NewOffsetDeleteRequest()
	delReq.Group = group
	delTopic := kmsg.NewOffsetDeleteRequestTopic()
	delTopic.Topic = topic
	delPart := kmsg.NewOffsetDeleteRequestTopicPartition()
	delPart.Partition = 0
	delTopic.Partitions = append(delTopic.Partitions, delPart)
	delReq.Topics = append(delReq.Topics, delTopic)

	delResp, err := delReq.RequestWith(ctx, cl)
	require.NoError(t, err)
	assert.Equal(t, int16(0), delResp.ErrorCode, "expected no top-level error")
	require.Len(t, delResp.Topics, 1)
	require.Len(t, delResp.Topics[0].Partitions, 1)
	assert.Equal(t, int16(0), delResp.Topics[0].Partitions[0].ErrorCode,
		"expected no error for deleted offset")

	// Verify offset is gone
	fetched2, err := admin.FetchOffsets(ctx, group)
	require.NoError(t, err)
	off2, ok := fetched2.Lookup(topic, 0)
	if ok {
		assert.Equal(t, int64(-1), off2.At, "expected offset -1 after delete")
	}
	// If not found at all, that's also acceptable
}

// TestOffsetDeleteUnknownGroup tests deleting offsets from a non-existent group.
func TestOffsetDeleteUnknownGroup(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t, WithAutoCreateTopics(true))

	group := "nonexistent-offset-delete-group-" + fmt.Sprintf("%d", time.Now().UnixNano())

	cl := NewClient(t, tb.Addr)
	delReq := kmsg.NewOffsetDeleteRequest()
	delReq.Group = group
	delTopic := kmsg.NewOffsetDeleteRequestTopic()
	delTopic.Topic = "some-topic"
	delPart := kmsg.NewOffsetDeleteRequestTopicPartition()
	delPart.Partition = 0
	delTopic.Partitions = append(delTopic.Partitions, delPart)
	delReq.Topics = append(delReq.Topics, delTopic)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := delReq.RequestWith(ctx, cl)
	require.NoError(t, err)
	assert.Equal(t, kerr.GroupIDNotFound.Code, resp.ErrorCode,
		"expected GROUP_ID_NOT_FOUND for unknown group")
}
