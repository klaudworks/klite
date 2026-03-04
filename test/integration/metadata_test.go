package integration

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// TestMetadataEmptyCluster verifies that a Metadata request to a fresh broker
// returns broker info correctly with no topics.
func TestMetadataEmptyCluster(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t, WithAutoCreateTopics(false))
	cl := NewClient(t, tb.Addr)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Request metadata with no topics (empty list, not nil — empty means "give me nothing")
	req := kmsg.NewMetadataRequest()
	req.Version = 9 // flexible version
	req.Topics = []kmsg.MetadataRequestTopic{} // empty list = no topics

	resp, err := req.RequestWith(ctx, cl)
	require.NoError(t, err)

	// Verify broker info
	require.Len(t, resp.Brokers, 1, "expected exactly 1 broker")
	assert.Equal(t, int32(0), resp.Brokers[0].NodeID)
	assert.NotEmpty(t, resp.Brokers[0].Host)
	assert.Greater(t, resp.Brokers[0].Port, int32(0))

	// Verify cluster metadata
	require.NotNil(t, resp.ClusterID)
	assert.NotEmpty(t, *resp.ClusterID, "cluster ID should not be empty")
	assert.Equal(t, int32(0), resp.ControllerID, "controller should be node 0")

	// Verify no topics returned (we sent empty list)
	assert.Empty(t, resp.Topics, "expected no topics in empty cluster with empty request")
}

// TestMetadataAutoCreate verifies that requesting metadata for a non-existent
// topic auto-creates it when auto-create is enabled.
func TestMetadataAutoCreate(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t, WithAutoCreateTopics(true), WithDefaultPartitions(3))
	cl := NewClient(t, tb.Addr)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	topicName := "auto-created-topic"

	// Request metadata for a non-existent topic with auto-create allowed
	req := kmsg.NewMetadataRequest()
	req.Version = 9
	req.AllowAutoTopicCreation = true
	rt := kmsg.NewMetadataRequestTopic()
	rt.Topic = kmsg.StringPtr(topicName)
	req.Topics = append(req.Topics, rt)

	resp, err := req.RequestWith(ctx, cl)
	require.NoError(t, err)

	// Topic should be auto-created
	require.Len(t, resp.Topics, 1, "expected 1 topic")
	require.NotNil(t, resp.Topics[0].Topic)
	assert.Equal(t, topicName, *resp.Topics[0].Topic)
	assert.Equal(t, int16(0), resp.Topics[0].ErrorCode, "expected no error (topic auto-created)")

	// Verify partitions
	require.Len(t, resp.Topics[0].Partitions, 3, "expected 3 partitions")
	for i, p := range resp.Topics[0].Partitions {
		assert.Equal(t, int32(i), p.Partition)
		assert.Equal(t, int32(0), p.Leader)
		assert.Equal(t, []int32{0}, p.Replicas)
		assert.Equal(t, []int32{0}, p.ISR)
	}

	// Request again — topic should already exist now
	resp2, err := req.RequestWith(ctx, cl)
	require.NoError(t, err)
	require.Len(t, resp2.Topics, 1)
	require.NotNil(t, resp2.Topics[0].Topic)
	assert.Equal(t, topicName, *resp2.Topics[0].Topic)
	assert.Equal(t, int16(0), resp2.Topics[0].ErrorCode)
	assert.Len(t, resp2.Topics[0].Partitions, 3)
}

// TestMetadataAutoCreateDisabled verifies that requesting metadata for a
// non-existent topic returns UNKNOWN_TOPIC_OR_PARTITION when auto-create
// is disabled.
func TestMetadataAutoCreateDisabled(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t, WithAutoCreateTopics(false))
	cl := NewClient(t, tb.Addr)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := kmsg.NewMetadataRequest()
	req.Version = 9
	rt := kmsg.NewMetadataRequestTopic()
	rt.Topic = kmsg.StringPtr("nonexistent-topic")
	req.Topics = append(req.Topics, rt)

	resp, err := req.RequestWith(ctx, cl)
	require.NoError(t, err)

	require.Len(t, resp.Topics, 1)
	assert.Equal(t, kerr.UnknownTopicOrPartition.Code, resp.Topics[0].ErrorCode,
		"expected UNKNOWN_TOPIC_OR_PARTITION error")
}

// TestMetadataAllTopics verifies that a null topics request returns all topics.
func TestMetadataAllTopics(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t, WithAutoCreateTopics(true), WithDefaultPartitions(2))
	cl := NewClient(t, tb.Addr)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Create two topics via auto-create
	for _, name := range []string{"topic-a", "topic-b"} {
		req := kmsg.NewMetadataRequest()
		req.Version = 9
		req.AllowAutoTopicCreation = true
		rt := kmsg.NewMetadataRequestTopic()
		rt.Topic = kmsg.StringPtr(name)
		req.Topics = append(req.Topics, rt)
		_, err := req.RequestWith(ctx, cl)
		require.NoError(t, err)
	}

	// Now request all topics (nil Topics field)
	req := kmsg.NewMetadataRequest()
	req.Version = 9
	req.Topics = nil // null = all topics

	resp, err := req.RequestWith(ctx, cl)
	require.NoError(t, err)

	// Should return both topics
	require.Len(t, resp.Topics, 2, "expected 2 topics")
	names := make(map[string]bool)
	for _, st := range resp.Topics {
		names[*st.Topic] = true
		assert.Equal(t, int16(0), st.ErrorCode)
		assert.Len(t, st.Partitions, 2)
	}
	assert.True(t, names["topic-a"], "expected topic-a")
	assert.True(t, names["topic-b"], "expected topic-b")
}

// TestMetadataInvalidTopicName verifies that requesting metadata for a topic
// with an invalid name returns INVALID_TOPIC_EXCEPTION.
func TestMetadataInvalidTopicName(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t, WithAutoCreateTopics(true))
	cl := NewClient(t, tb.Addr)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := kmsg.NewMetadataRequest()
	req.Version = 9
	rt := kmsg.NewMetadataRequestTopic()
	badName := "topic with spaces"
	rt.Topic = &badName
	req.Topics = append(req.Topics, rt)

	resp, err := req.RequestWith(ctx, cl)
	require.NoError(t, err)

	require.Len(t, resp.Topics, 1)
	assert.Equal(t, kerr.InvalidTopicException.Code, resp.Topics[0].ErrorCode,
		"expected INVALID_TOPIC_EXCEPTION for invalid topic name")
}
