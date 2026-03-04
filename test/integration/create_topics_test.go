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

// TestCreateTopicsBasic creates a topic via CreateTopics and verifies
// it is visible via Metadata.
func TestCreateTopicsBasic(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t, WithAutoCreateTopics(false), WithDefaultPartitions(1))
	cl := NewClient(t, tb.Addr)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Create a topic with 3 partitions
	createReq := kmsg.NewCreateTopicsRequest()
	createReq.Version = 7
	rt := kmsg.NewCreateTopicsRequestTopic()
	rt.Topic = "test-create-basic"
	rt.NumPartitions = 3
	rt.ReplicationFactor = 1
	createReq.Topics = append(createReq.Topics, rt)

	createResp, err := createReq.RequestWith(ctx, cl)
	require.NoError(t, err)
	require.Len(t, createResp.Topics, 1)
	assert.Equal(t, "test-create-basic", createResp.Topics[0].Topic)
	assert.Equal(t, int16(0), createResp.Topics[0].ErrorCode, "expected no error")
	assert.Equal(t, int32(3), createResp.Topics[0].NumPartitions)
	assert.Equal(t, int16(1), createResp.Topics[0].ReplicationFactor)
	// v7 should return a non-zero TopicID
	assert.NotEqual(t, [16]byte{}, createResp.Topics[0].TopicID, "expected non-zero topic ID")

	// Verify via Metadata
	metaReq := kmsg.NewMetadataRequest()
	metaReq.Version = 9
	mrt := kmsg.NewMetadataRequestTopic()
	mrt.Topic = kmsg.StringPtr("test-create-basic")
	metaReq.Topics = append(metaReq.Topics, mrt)

	metaResp, err := metaReq.RequestWith(ctx, cl)
	require.NoError(t, err)
	require.Len(t, metaResp.Topics, 1)
	assert.Equal(t, int16(0), metaResp.Topics[0].ErrorCode)
	assert.Len(t, metaResp.Topics[0].Partitions, 3)
}

// TestCreateTopicsDefaultPartitions creates a topic with NumPartitions=-1
// and verifies the broker's default partition count is used.
func TestCreateTopicsDefaultPartitions(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t, WithAutoCreateTopics(false), WithDefaultPartitions(5))
	cl := NewClient(t, tb.Addr)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	createReq := kmsg.NewCreateTopicsRequest()
	createReq.Version = 7
	rt := kmsg.NewCreateTopicsRequestTopic()
	rt.Topic = "test-default-parts"
	rt.NumPartitions = -1
	rt.ReplicationFactor = -1
	createReq.Topics = append(createReq.Topics, rt)

	createResp, err := createReq.RequestWith(ctx, cl)
	require.NoError(t, err)
	require.Len(t, createResp.Topics, 1)
	assert.Equal(t, int16(0), createResp.Topics[0].ErrorCode)
	assert.Equal(t, int32(5), createResp.Topics[0].NumPartitions)
}

// TestCreateTopicAlreadyExists verifies that creating an already-existing
// topic returns TOPIC_ALREADY_EXISTS.
func TestCreateTopicAlreadyExists(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t, WithAutoCreateTopics(false))
	cl := NewClient(t, tb.Addr)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	topic := "test-already-exists"

	// Create it first
	req1 := kmsg.NewCreateTopicsRequest()
	req1.Version = 7
	rt1 := kmsg.NewCreateTopicsRequestTopic()
	rt1.Topic = topic
	rt1.NumPartitions = 1
	rt1.ReplicationFactor = 1
	req1.Topics = append(req1.Topics, rt1)

	resp1, err := req1.RequestWith(ctx, cl)
	require.NoError(t, err)
	require.Len(t, resp1.Topics, 1)
	assert.Equal(t, int16(0), resp1.Topics[0].ErrorCode)

	// Create again — should get TOPIC_ALREADY_EXISTS
	req2 := kmsg.NewCreateTopicsRequest()
	req2.Version = 7
	rt2 := kmsg.NewCreateTopicsRequestTopic()
	rt2.Topic = topic
	rt2.NumPartitions = 1
	rt2.ReplicationFactor = 1
	req2.Topics = append(req2.Topics, rt2)

	resp2, err := req2.RequestWith(ctx, cl)
	require.NoError(t, err)
	require.Len(t, resp2.Topics, 1)
	assert.Equal(t, kerr.TopicAlreadyExists.Code, resp2.Topics[0].ErrorCode,
		"expected TOPIC_ALREADY_EXISTS")
}

// TestCreateTopicInvalidName verifies that invalid topic names return
// INVALID_TOPIC_EXCEPTION.
func TestCreateTopicInvalidName(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t, WithAutoCreateTopics(false))

	cases := []struct {
		name string
		desc string
	}{
		{"", "empty name"},
		{".", "dot"},
		{"..", "dot dot"},
		{"topic with spaces", "spaces"},
		{"topic/slash", "slash"},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()
			cl := NewClient(t, tb.Addr)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			req := kmsg.NewCreateTopicsRequest()
			req.Version = 7
			rt := kmsg.NewCreateTopicsRequestTopic()
			rt.Topic = tc.name
			rt.NumPartitions = 1
			rt.ReplicationFactor = 1
			req.Topics = append(req.Topics, rt)

			resp, err := req.RequestWith(ctx, cl)
			require.NoError(t, err)
			require.Len(t, resp.Topics, 1)
			assert.Equal(t, kerr.InvalidTopicException.Code, resp.Topics[0].ErrorCode,
				"expected INVALID_TOPIC_EXCEPTION for %q", tc.name)
		})
	}
}

// TestCreateTopicNameCollision verifies that topics differing only in
// dot vs underscore are rejected with INVALID_TOPIC_EXCEPTION.
func TestCreateTopicNameCollision(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t, WithAutoCreateTopics(false))
	cl := NewClient(t, tb.Addr)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Create foo.bar
	req1 := kmsg.NewCreateTopicsRequest()
	req1.Version = 7
	rt1 := kmsg.NewCreateTopicsRequestTopic()
	rt1.Topic = "foo.bar"
	rt1.NumPartitions = 1
	rt1.ReplicationFactor = 1
	req1.Topics = append(req1.Topics, rt1)

	resp1, err := req1.RequestWith(ctx, cl)
	require.NoError(t, err)
	require.Len(t, resp1.Topics, 1)
	assert.Equal(t, int16(0), resp1.Topics[0].ErrorCode)

	// Try to create foo_bar — should collide
	req2 := kmsg.NewCreateTopicsRequest()
	req2.Version = 7
	rt2 := kmsg.NewCreateTopicsRequestTopic()
	rt2.Topic = "foo_bar"
	rt2.NumPartitions = 1
	rt2.ReplicationFactor = 1
	req2.Topics = append(req2.Topics, rt2)

	resp2, err := req2.RequestWith(ctx, cl)
	require.NoError(t, err)
	require.Len(t, resp2.Topics, 1)
	assert.Equal(t, kerr.InvalidTopicException.Code, resp2.Topics[0].ErrorCode,
		"expected INVALID_TOPIC_EXCEPTION for dot/underscore collision")
}

// TestCreateTopicsReplicaAssignment verifies that topics can be created with
// explicit replica assignments.
func TestCreateTopicsReplicaAssignment(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t, WithAutoCreateTopics(false))
	cl := NewClient(t, tb.Addr)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	createReq := kmsg.NewCreateTopicsRequest()
	createReq.Version = 7
	rt := kmsg.NewCreateTopicsRequestTopic()
	rt.Topic = "test-replica-assign"
	rt.NumPartitions = -1
	rt.ReplicationFactor = -1
	// 2 partitions, each assigned to broker 0
	ra0 := kmsg.NewCreateTopicsRequestTopicReplicaAssignment()
	ra0.Partition = 0
	ra0.Replicas = []int32{0}
	ra1 := kmsg.NewCreateTopicsRequestTopicReplicaAssignment()
	ra1.Partition = 1
	ra1.Replicas = []int32{0}
	rt.ReplicaAssignment = append(rt.ReplicaAssignment, ra0, ra1)
	createReq.Topics = append(createReq.Topics, rt)

	resp, err := createReq.RequestWith(ctx, cl)
	require.NoError(t, err)
	require.Len(t, resp.Topics, 1)
	assert.Equal(t, int16(0), resp.Topics[0].ErrorCode)
	assert.Equal(t, int32(2), resp.Topics[0].NumPartitions)
}

// TestCreateTopicsReplicaAssignmentWithNumPartitions verifies that providing
// both ReplicaAssignment and NumPartitions (not -1) returns INVALID_REQUEST.
func TestCreateTopicsReplicaAssignmentWithNumPartitions(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t, WithAutoCreateTopics(false))
	cl := NewClient(t, tb.Addr)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	createReq := kmsg.NewCreateTopicsRequest()
	createReq.Version = 7
	rt := kmsg.NewCreateTopicsRequestTopic()
	rt.Topic = "test-ra-with-numparts"
	rt.NumPartitions = 3 // Should be -1 when ReplicaAssignment is used
	rt.ReplicationFactor = 1
	ra := kmsg.NewCreateTopicsRequestTopicReplicaAssignment()
	ra.Partition = 0
	ra.Replicas = []int32{0}
	rt.ReplicaAssignment = append(rt.ReplicaAssignment, ra)
	createReq.Topics = append(createReq.Topics, rt)

	resp, err := createReq.RequestWith(ctx, cl)
	require.NoError(t, err)
	require.Len(t, resp.Topics, 1)
	assert.Equal(t, kerr.InvalidRequest.Code, resp.Topics[0].ErrorCode,
		"expected INVALID_REQUEST when ReplicaAssignment and NumPartitions are both set")
}

// TestCreateTopicsValidateOnly verifies that ValidateOnly=true validates
// without actually creating the topic.
func TestCreateTopicsValidateOnly(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t, WithAutoCreateTopics(false))
	cl := NewClient(t, tb.Addr)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// ValidateOnly create
	createReq := kmsg.NewCreateTopicsRequest()
	createReq.Version = 7
	createReq.ValidateOnly = true
	rt := kmsg.NewCreateTopicsRequestTopic()
	rt.Topic = "test-validate-only"
	rt.NumPartitions = 2
	rt.ReplicationFactor = 1
	createReq.Topics = append(createReq.Topics, rt)

	createResp, err := createReq.RequestWith(ctx, cl)
	require.NoError(t, err)
	require.Len(t, createResp.Topics, 1)
	assert.Equal(t, int16(0), createResp.Topics[0].ErrorCode, "validate-only should succeed")

	// Verify topic was NOT created
	metaReq := kmsg.NewMetadataRequest()
	metaReq.Version = 9
	mrt := kmsg.NewMetadataRequestTopic()
	mrt.Topic = kmsg.StringPtr("test-validate-only")
	metaReq.Topics = append(metaReq.Topics, mrt)

	metaResp, err := metaReq.RequestWith(ctx, cl)
	require.NoError(t, err)
	require.Len(t, metaResp.Topics, 1)
	assert.Equal(t, kerr.UnknownTopicOrPartition.Code, metaResp.Topics[0].ErrorCode,
		"topic should not exist after validate-only create")
}

// TestCreateTopicsWithConfigs verifies that topic configs are stored
// and can be passed in the create request.
func TestCreateTopicsWithConfigs(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t, WithAutoCreateTopics(false))
	cl := NewClient(t, tb.Addr)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	createReq := kmsg.NewCreateTopicsRequest()
	createReq.Version = 7
	rt := kmsg.NewCreateTopicsRequestTopic()
	rt.Topic = "test-with-configs"
	rt.NumPartitions = 1
	rt.ReplicationFactor = 1
	c1 := kmsg.NewCreateTopicsRequestTopicConfig()
	c1.Name = "retention.ms"
	val1 := "86400000"
	c1.Value = &val1
	c2 := kmsg.NewCreateTopicsRequestTopicConfig()
	c2.Name = "cleanup.policy"
	val2 := "delete"
	c2.Value = &val2
	rt.Configs = append(rt.Configs, c1, c2)
	createReq.Topics = append(createReq.Topics, rt)

	resp, err := createReq.RequestWith(ctx, cl)
	require.NoError(t, err)
	require.Len(t, resp.Topics, 1)
	assert.Equal(t, int16(0), resp.Topics[0].ErrorCode)

	// v5+ should return configs in response
	configMap := make(map[string]string)
	for _, rc := range resp.Topics[0].Configs {
		if rc.Value != nil {
			configMap[rc.Name] = *rc.Value
		}
	}
	assert.Equal(t, "86400000", configMap["retention.ms"])
	assert.Equal(t, "delete", configMap["cleanup.policy"])
}

// TestCreateTopicsInvalidConfig verifies that unknown config keys
// are rejected with INVALID_CONFIG.
func TestCreateTopicsInvalidConfig(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t, WithAutoCreateTopics(false))
	cl := NewClient(t, tb.Addr)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	createReq := kmsg.NewCreateTopicsRequest()
	createReq.Version = 7
	rt := kmsg.NewCreateTopicsRequestTopic()
	rt.Topic = "test-invalid-config"
	rt.NumPartitions = 1
	rt.ReplicationFactor = 1
	c := kmsg.NewCreateTopicsRequestTopicConfig()
	c.Name = "nonexistent.config.key"
	val := "something"
	c.Value = &val
	rt.Configs = append(rt.Configs, c)
	createReq.Topics = append(createReq.Topics, rt)

	resp, err := createReq.RequestWith(ctx, cl)
	require.NoError(t, err)
	require.Len(t, resp.Topics, 1)
	assert.Equal(t, kerr.InvalidConfig.Code, resp.Topics[0].ErrorCode,
		"expected INVALID_CONFIG for unknown config key")
}

// TestCreateTopicsMultiple verifies creating multiple topics in a single request.
func TestCreateTopicsMultiple(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t, WithAutoCreateTopics(false))
	cl := NewClient(t, tb.Addr)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	createReq := kmsg.NewCreateTopicsRequest()
	createReq.Version = 7
	for _, name := range []string{"multi-a", "multi-b", "multi-c"} {
		rt := kmsg.NewCreateTopicsRequestTopic()
		rt.Topic = name
		rt.NumPartitions = 2
		rt.ReplicationFactor = 1
		createReq.Topics = append(createReq.Topics, rt)
	}

	resp, err := createReq.RequestWith(ctx, cl)
	require.NoError(t, err)
	require.Len(t, resp.Topics, 3)
	for _, st := range resp.Topics {
		assert.Equal(t, int16(0), st.ErrorCode, "expected no error for topic %s", st.Topic)
	}
}

// TestCreateTopicsDuplicateInRequest verifies that having the same topic name
// twice in one request returns INVALID_REQUEST for all topics.
func TestCreateTopicsDuplicateInRequest(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t, WithAutoCreateTopics(false))
	cl := NewClient(t, tb.Addr)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	createReq := kmsg.NewCreateTopicsRequest()
	createReq.Version = 7
	for i := 0; i < 2; i++ {
		rt := kmsg.NewCreateTopicsRequestTopic()
		rt.Topic = "duplicate-topic"
		rt.NumPartitions = 1
		rt.ReplicationFactor = 1
		createReq.Topics = append(createReq.Topics, rt)
	}

	resp, err := createReq.RequestWith(ctx, cl)
	require.NoError(t, err)
	require.Len(t, resp.Topics, 2)
	for _, st := range resp.Topics {
		assert.Equal(t, kerr.InvalidRequest.Code, st.ErrorCode,
			"expected INVALID_REQUEST for duplicate topic name")
	}
}

// TestCreateTopicsZeroPartitions verifies that NumPartitions=0 returns
// INVALID_PARTITIONS.
func TestCreateTopicsZeroPartitions(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t, WithAutoCreateTopics(false))
	cl := NewClient(t, tb.Addr)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	createReq := kmsg.NewCreateTopicsRequest()
	createReq.Version = 7
	rt := kmsg.NewCreateTopicsRequestTopic()
	rt.Topic = "test-zero-parts"
	rt.NumPartitions = 0
	rt.ReplicationFactor = 1
	createReq.Topics = append(createReq.Topics, rt)

	resp, err := createReq.RequestWith(ctx, cl)
	require.NoError(t, err)
	require.Len(t, resp.Topics, 1)
	assert.Equal(t, kerr.InvalidPartitions.Code, resp.Topics[0].ErrorCode,
		"expected INVALID_PARTITIONS for NumPartitions=0")
}
