package integration

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// ---- DeleteTopics ----

func TestDeleteTopicBasic(t *testing.T) {
	tb := StartBroker(t)
	admin := NewAdminClient(t, tb.Addr)

	ctx := context.Background()
	_, err := admin.CreateTopics(ctx, 1, -1, nil, "del-me")
	require.NoError(t, err)

	// Verify it exists
	topics, err := admin.ListTopics(ctx)
	require.NoError(t, err)
	_, ok := topics["del-me"]
	require.True(t, ok, "topic should exist")

	// Delete it
	resps, err := admin.DeleteTopics(ctx, "del-me")
	require.NoError(t, err)
	require.NoError(t, resps.Error())

	// Verify it's gone
	topics, err = admin.ListTopics(ctx)
	require.NoError(t, err)
	_, ok = topics["del-me"]
	require.False(t, ok, "topic should be deleted")
}

func TestDeleteTopicUnknown(t *testing.T) {
	tb := StartBroker(t)
	admin := NewAdminClient(t, tb.Addr)

	ctx := context.Background()
	resps, err := admin.DeleteTopics(ctx, "no-such-topic")
	require.NoError(t, err)

	resp, ok := resps["no-such-topic"]
	require.True(t, ok)
	require.Error(t, resp.Err)
	require.ErrorIs(t, resp.Err, kerr.UnknownTopicOrPartition)
}

// ---- CreatePartitions ----

func TestCreatePartitions(t *testing.T) {
	tb := StartBroker(t)
	admin := NewAdminClient(t, tb.Addr)

	ctx := context.Background()
	_, err := admin.CreateTopics(ctx, 1, -1, nil, "expand-me")
	require.NoError(t, err)

	// Expand from 1 to 4
	resps, err := admin.UpdatePartitions(ctx, 4, "expand-me")
	require.NoError(t, err)
	require.NoError(t, resps.Error())

	// Verify partition count
	topics, err := admin.ListTopics(ctx)
	require.NoError(t, err)
	td, ok := topics["expand-me"]
	require.True(t, ok)
	require.Equal(t, 4, len(td.Partitions))
}

func TestCreatePartitionsDecrease(t *testing.T) {
	tb := StartBroker(t)
	admin := NewAdminClient(t, tb.Addr)

	ctx := context.Background()
	_, err := admin.CreateTopics(ctx, 4, -1, nil, "shrink-me")
	require.NoError(t, err)

	// Try to shrink from 4 to 2 — should fail
	resps, err := admin.UpdatePartitions(ctx, 2, "shrink-me")
	require.NoError(t, err)

	resp, ok := resps["shrink-me"]
	require.True(t, ok)
	require.Error(t, resp.Err)
	require.ErrorIs(t, resp.Err, kerr.InvalidPartitions)
}

// ---- DescribeConfigs ----

func TestDescribeConfigsTopic(t *testing.T) {
	tb := StartBroker(t)
	admin := NewAdminClient(t, tb.Addr)

	ctx := context.Background()
	cfgs := map[string]*string{
		"retention.ms": kadm.StringPtr("3600000"),
	}
	_, err := admin.CreateTopics(ctx, 1, -1, cfgs, "cfg-topic")
	require.NoError(t, err)

	// Describe the topic configs
	resources, err := admin.DescribeTopicConfigs(ctx, "cfg-topic")
	require.NoError(t, err)
	require.Len(t, resources, 1)
	require.NoError(t, resources[0].Err)

	// Find retention.ms — should show as TOPIC_CONFIG (not DEFAULT)
	var found bool
	for _, c := range resources[0].Configs {
		if c.Key == "retention.ms" {
			found = true
			require.NotNil(t, c.Value)
			require.Equal(t, "3600000", *c.Value)
			require.Equal(t, kmsg.ConfigSourceDynamicTopicConfig, c.Source)
			break
		}
	}
	require.True(t, found, "retention.ms config should be present")
}

func TestDescribeConfigsBroker(t *testing.T) {
	tb := StartBroker(t)

	// Use raw kmsg for broker describe
	cl := NewClient(t, tb.Addr)
	ctx := context.Background()

	req := kmsg.NewDescribeConfigsRequest()
	res := kmsg.NewDescribeConfigsRequestResource()
	res.ResourceType = kmsg.ConfigResourceTypeBroker
	res.ResourceName = "0"
	req.Resources = append(req.Resources, res)

	kresp, err := cl.Request(ctx, &req)
	require.NoError(t, err)

	resp := kresp.(*kmsg.DescribeConfigsResponse)
	require.Len(t, resp.Resources, 1)
	require.Equal(t, int16(0), resp.Resources[0].ErrorCode)

	// Should have at least broker.id config
	var foundBrokerID bool
	for _, c := range resp.Resources[0].Configs {
		if c.Name == "broker.id" {
			foundBrokerID = true
			require.NotNil(t, c.Value)
			require.Equal(t, "0", *c.Value)
		}
	}
	require.True(t, foundBrokerID, "broker.id should be in describe response")
}

// ---- AlterConfigs ----

func TestAlterConfigsTopic(t *testing.T) {
	tb := StartBroker(t)
	admin := NewAdminClient(t, tb.Addr)

	ctx := context.Background()
	_, err := admin.CreateTopics(ctx, 1, -1, nil, "alter-topic")
	require.NoError(t, err)

	// Set retention.ms using IncrementalAlterConfigs
	resps, err := admin.AlterTopicConfigs(ctx, []kadm.AlterConfig{
		{Op: kadm.SetConfig, Name: "retention.ms", Value: kadm.StringPtr("7200000")},
	}, "alter-topic")
	require.NoError(t, err)
	require.Len(t, resps, 1)
	require.NoError(t, resps[0].Err)

	// Verify via describe
	resources, err := admin.DescribeTopicConfigs(ctx, "alter-topic")
	require.NoError(t, err)
	for _, c := range resources[0].Configs {
		if c.Key == "retention.ms" {
			require.NotNil(t, c.Value)
			require.Equal(t, "7200000", *c.Value)
			return
		}
	}
	t.Fatal("retention.ms should be present after alter")
}

func TestAlterConfigsLegacy(t *testing.T) {
	tb := StartBroker(t)
	admin := NewAdminClient(t, tb.Addr)

	ctx := context.Background()
	cfgs := map[string]*string{
		"retention.ms":   kadm.StringPtr("1000"),
		"cleanup.policy": kadm.StringPtr("compact"),
	}
	_, err := admin.CreateTopics(ctx, 1, -1, cfgs, "legacy-alter")
	require.NoError(t, err)

	// Use legacy AlterConfigs (full replacement) — only set retention.ms
	resps, err := admin.AlterTopicConfigsState(ctx, []kadm.AlterConfig{
		{Name: "retention.ms", Value: kadm.StringPtr("5000")},
	}, "legacy-alter")
	require.NoError(t, err)
	require.Len(t, resps, 1)
	require.NoError(t, resps[0].Err)

	// Verify: retention.ms should be 5000, cleanup.policy should revert to default
	resources, err := admin.DescribeTopicConfigs(ctx, "legacy-alter")
	require.NoError(t, err)
	var foundRetention, foundCleanup bool
	for _, c := range resources[0].Configs {
		if c.Key == "retention.ms" {
			foundRetention = true
			require.NotNil(t, c.Value)
			require.Equal(t, "5000", *c.Value)
			require.Equal(t, kmsg.ConfigSourceDynamicTopicConfig, c.Source,
				"retention.ms should be a topic config override")
		}
		if c.Key == "cleanup.policy" {
			foundCleanup = true
			require.NotNil(t, c.Value)
			require.Equal(t, "delete", *c.Value,
				"cleanup.policy should revert to default after full replacement")
			require.Equal(t, kmsg.ConfigSourceDefaultConfig, c.Source,
				"cleanup.policy should be DEFAULT source after full replacement")
		}
	}
	require.True(t, foundRetention, "retention.ms config should be present")
	require.True(t, foundCleanup, "cleanup.policy config should be present")
}

// ---- DescribeGroups / ListGroups / DeleteGroups ----

func TestDescribeGroupsStable(t *testing.T) {
	tb := StartBroker(t)
	admin := NewAdminClient(t, tb.Addr)
	ctx := context.Background()

	// Create topic and consumer
	_, err := admin.CreateTopics(ctx, 1, -1, nil, "desc-grp-topic")
	require.NoError(t, err)

	cl := NewClient(t, tb.Addr,
		kgo.ConsumeTopics("desc-grp-topic"),
		kgo.ConsumerGroup("desc-grp"),
	)

	// Poll to trigger group join
	pctx, pcancel := context.WithTimeout(ctx, 3*time.Second)
	defer pcancel()
	cl.PollFetches(pctx)

	waitGroupState(t, tb.Addr, "desc-grp", "Stable", 5*time.Second)

	// Describe the group
	groups, err := admin.DescribeGroups(ctx, "desc-grp")
	require.NoError(t, err)

	g, ok := groups["desc-grp"]
	require.True(t, ok)
	require.NoError(t, g.Err)
	require.Equal(t, "Stable", g.State)
	require.Equal(t, "consumer", g.ProtocolType)
	require.GreaterOrEqual(t, len(g.Members), 1)
}

func TestDescribeGroupsUnknown(t *testing.T) {
	tb := StartBroker(t)
	admin := NewAdminClient(t, tb.Addr)
	ctx := context.Background()

	groups, err := admin.DescribeGroups(ctx, "ghost-group")
	require.NoError(t, err)

	g, ok := groups["ghost-group"]
	require.True(t, ok)
	require.Equal(t, "Dead", g.State)
}

func TestListGroups(t *testing.T) {
	tb := StartBroker(t)
	admin := NewAdminClient(t, tb.Addr)
	ctx := context.Background()

	// Create two consumer groups
	_, err := admin.CreateTopics(ctx, 1, -1, nil, "list-grp-topic")
	require.NoError(t, err)

	cl1 := NewClient(t, tb.Addr,
		kgo.ConsumeTopics("list-grp-topic"),
		kgo.ConsumerGroup("list-grp-1"),
	)
	cl2 := NewClient(t, tb.Addr,
		kgo.ConsumeTopics("list-grp-topic"),
		kgo.ConsumerGroup("list-grp-2"),
	)

	// Poll both to trigger group join
	pctx, pcancel := context.WithTimeout(ctx, 3*time.Second)
	defer pcancel()
	cl1.PollFetches(pctx)
	cl2.PollFetches(pctx)
	waitGroupState(t, tb.Addr, "list-grp-1", "Stable", 5*time.Second)
	waitGroupState(t, tb.Addr, "list-grp-2", "Stable", 5*time.Second)

	listed, err := admin.ListGroups(ctx)
	require.NoError(t, err)

	_, ok1 := listed["list-grp-1"]
	_, ok2 := listed["list-grp-2"]
	require.True(t, ok1, "list-grp-1 should be listed")
	require.True(t, ok2, "list-grp-2 should be listed")
}

func TestDeleteGroupEmpty(t *testing.T) {
	tb := StartBroker(t)
	admin := NewAdminClient(t, tb.Addr)
	ctx := context.Background()

	// Create topic and consumer group
	_, err := admin.CreateTopics(ctx, 1, -1, nil, "del-grp-topic")
	require.NoError(t, err)

	cl := NewClient(t, tb.Addr,
		kgo.ConsumeTopics("del-grp-topic"),
		kgo.ConsumerGroup("del-grp"),
	)

	// Join the group
	pctx, pcancel := context.WithTimeout(ctx, 3*time.Second)
	defer pcancel()
	cl.PollFetches(pctx)
	waitGroupState(t, tb.Addr, "del-grp", "Stable", 5*time.Second)

	// Leave the group
	cl.LeaveGroup()
	waitGroupState(t, tb.Addr, "del-grp", "Empty", 5*time.Second)

	// Delete the now-empty group
	resps, err := admin.DeleteGroups(ctx, "del-grp")
	require.NoError(t, err)
	require.NoError(t, resps.Error())
}

func TestDeleteGroupActive(t *testing.T) {
	tb := StartBroker(t)
	admin := NewAdminClient(t, tb.Addr)
	ctx := context.Background()

	_, err := admin.CreateTopics(ctx, 1, -1, nil, "active-grp-topic")
	require.NoError(t, err)

	cl := NewClient(t, tb.Addr,
		kgo.ConsumeTopics("active-grp-topic"),
		kgo.ConsumerGroup("active-grp"),
	)

	// Join group
	pctx, pcancel := context.WithTimeout(ctx, 3*time.Second)
	defer pcancel()
	cl.PollFetches(pctx)
	waitGroupState(t, tb.Addr, "active-grp", "Stable", 5*time.Second)

	// Try to delete while active
	resps, err := admin.DeleteGroups(ctx, "active-grp")
	require.NoError(t, err)

	resp, ok := resps["active-grp"]
	require.True(t, ok)
	require.Error(t, resp.Err)
	require.ErrorIs(t, resp.Err, kerr.NonEmptyGroup)
}

func TestDeleteGroupNotFound(t *testing.T) {
	tb := StartBroker(t)
	admin := NewAdminClient(t, tb.Addr)
	ctx := context.Background()

	resps, err := admin.DeleteGroups(ctx, "nonexistent-grp")
	require.NoError(t, err)

	resp, ok := resps["nonexistent-grp"]
	require.True(t, ok)
	require.Error(t, resp.Err)
	require.ErrorIs(t, resp.Err, kerr.GroupIDNotFound)
}

// ---- DeleteRecords ----

func TestDeleteRecordsBasic(t *testing.T) {
	tb := StartBroker(t)
	admin := NewAdminClient(t, tb.Addr)
	ctx := context.Background()

	_, err := admin.CreateTopics(ctx, 1, -1, nil, "del-recs")
	require.NoError(t, err)

	cl := NewClient(t, tb.Addr)
	ProduceN(t, cl, "del-recs", 10)

	// Delete up to offset 5
	offsets := make(kadm.Offsets)
	offsets.Add(kadm.Offset{Topic: "del-recs", Partition: 0, At: 5})
	resps, err := admin.DeleteRecords(ctx, offsets)
	require.NoError(t, err)
	require.NoError(t, resps.Error())

	// Check that list offsets returns logStart=5
	lo, err := admin.ListStartOffsets(ctx, "del-recs")
	require.NoError(t, err)
	o, ok := lo.Lookup("del-recs", 0)
	require.True(t, ok)
	require.Equal(t, int64(5), o.Offset, "log start offset should be 5")
}

func TestDeleteRecordsToHWM(t *testing.T) {
	tb := StartBroker(t)
	admin := NewAdminClient(t, tb.Addr)
	ctx := context.Background()

	_, err := admin.CreateTopics(ctx, 1, -1, nil, "del-hwm")
	require.NoError(t, err)

	cl := NewClient(t, tb.Addr)
	ProduceN(t, cl, "del-hwm", 10)

	// Delete with offset -1 (= to HWM)
	offsets := make(kadm.Offsets)
	offsets.Add(kadm.Offset{Topic: "del-hwm", Partition: 0, At: -1})
	resps, err := admin.DeleteRecords(ctx, offsets)
	require.NoError(t, err)
	require.NoError(t, resps.Error())

	// Log start should now equal HWM (10)
	lo, err := admin.ListStartOffsets(ctx, "del-hwm")
	require.NoError(t, err)
	o, ok := lo.Lookup("del-hwm", 0)
	require.True(t, ok)
	require.Equal(t, int64(10), o.Offset, "log start should equal HWM after delete-to-HWM")
}

func TestDeleteRecordsOutOfRange(t *testing.T) {
	tb := StartBroker(t)
	admin := NewAdminClient(t, tb.Addr)
	ctx := context.Background()

	_, err := admin.CreateTopics(ctx, 1, -1, nil, "del-oor")
	require.NoError(t, err)

	cl := NewClient(t, tb.Addr)
	ProduceN(t, cl, "del-oor", 5)

	// Try to delete beyond HWM
	offsets := make(kadm.Offsets)
	offsets.Add(kadm.Offset{Topic: "del-oor", Partition: 0, At: 100})
	resps, err := admin.DeleteRecords(ctx, offsets)
	require.NoError(t, err)

	resp, ok := resps.Lookup("del-oor", 0)
	require.True(t, ok)
	require.Error(t, resp.Err)
	require.ErrorIs(t, resp.Err, kerr.OffsetOutOfRange)
}

// TestDeleteRecordsSurvivesRestart is a placeholder for Phase 3 (WAL).
// Pre-WAL, data is in-memory and does not survive restarts.
// This test will be meaningful once the WAL is implemented.
func TestDeleteRecordsSurvivesRestart(t *testing.T) {
	t.Skip("Pre-WAL: in-memory data does not survive restarts; WAL required for this test")
}

// ---- DescribeCluster ----

func TestDescribeCluster(t *testing.T) {
	tb := StartBroker(t)
	admin := NewAdminClient(t, tb.Addr)
	ctx := context.Background()

	meta, err := admin.BrokerMetadata(ctx)
	require.NoError(t, err)

	require.NotEmpty(t, meta.Cluster, "cluster ID should not be empty")
	require.Equal(t, int32(0), meta.Controller, "controller should be node 0")
	require.GreaterOrEqual(t, len(meta.Brokers), 1, "should have at least one broker")
	require.Equal(t, int32(0), meta.Brokers[0].NodeID)
}

// ---- DescribeLogDirs ----

func TestDescribeLogDirs(t *testing.T) {
	tb := StartBroker(t)
	admin := NewAdminClient(t, tb.Addr)
	ctx := context.Background()

	_, err := admin.CreateTopics(ctx, 1, -1, nil, "logdir-topic")
	require.NoError(t, err)

	cl := NewClient(t, tb.Addr)
	ProduceN(t, cl, "logdir-topic", 10)

	// DescribeAllLogDirs for all topics
	allDirs, err := admin.DescribeAllLogDirs(ctx, nil)
	require.NoError(t, err)
	require.NotEmpty(t, allDirs, "should have at least one broker's log dirs")

	// Find our broker's dirs
	dirs, ok := allDirs[0]
	require.True(t, ok, "should have dirs for broker 0")

	// Find our topic in any dir
	var totalSize int64
	for _, dir := range dirs {
		require.NoError(t, dir.Err)
		for topicName, partitions := range dir.Topics {
			if topicName == "logdir-topic" {
				for _, p := range partitions {
					totalSize += p.Size
				}
			}
		}
	}
	require.Greater(t, totalSize, int64(0), "partition should have non-zero size after producing")
}

// ---- OffsetForLeaderEpoch ----

func TestOffsetForLeaderEpoch(t *testing.T) {
	tb := StartBroker(t)
	ctx := context.Background()

	admin := NewAdminClient(t, tb.Addr)
	_, err := admin.CreateTopics(ctx, 1, -1, nil, "epoch-topic")
	require.NoError(t, err)

	cl := NewClient(t, tb.Addr)
	ProduceN(t, cl, "epoch-topic", 5)

	// Request epoch 0 (current) — should return HW
	epochReq := make(kadm.OffsetForLeaderEpochRequest)
	epochReq.Add("epoch-topic", 0, 0)
	resps, err := admin.OffsetForLeaderEpoch(ctx, epochReq)
	require.NoError(t, err)

	topicResps, ok := resps["epoch-topic"]
	require.True(t, ok)
	partResp, ok := topicResps[0]
	require.True(t, ok)
	require.NoError(t, partResp.Err)
	require.Equal(t, int32(0), partResp.LeaderEpoch, "leader epoch should be 0")
	require.Equal(t, int64(5), partResp.EndOffset, "end offset should be HW=5")
}

func TestOffsetForLeaderEpochUnknown(t *testing.T) {
	tb := StartBroker(t)
	ctx := context.Background()

	admin := NewAdminClient(t, tb.Addr)
	_, err := admin.CreateTopics(ctx, 1, -1, nil, "epoch-unknown")
	require.NoError(t, err)

	cl := NewClient(t, tb.Addr)
	ProduceN(t, cl, "epoch-unknown", 5)
	cl.Close()

	// Request a future epoch — should return -1/-1
	rawCl := NewClient(t, tb.Addr)
	req := kmsg.NewOffsetForLeaderEpochRequest()
	req.ReplicaID = -1
	rt := kmsg.NewOffsetForLeaderEpochRequestTopic()
	rt.Topic = "epoch-unknown"
	rp := kmsg.NewOffsetForLeaderEpochRequestTopicPartition()
	rp.Partition = 0
	rp.CurrentLeaderEpoch = 0
	rp.LeaderEpoch = 99 // Future epoch
	rt.Partitions = append(rt.Partitions, rp)
	req.Topics = append(req.Topics, rt)

	kresp, err := rawCl.Request(ctx, &req)
	require.NoError(t, err)

	resp := kresp.(*kmsg.OffsetForLeaderEpochResponse)
	require.Len(t, resp.Topics, 1)
	require.Len(t, resp.Topics[0].Partitions, 1)
	p := resp.Topics[0].Partitions[0]
	// For future epoch > current: LeaderEpoch = -1, EndOffset = -1
	require.Equal(t, int32(-1), p.LeaderEpoch, "future epoch should return -1")
	require.Equal(t, int64(-1), p.EndOffset, "future epoch should return -1 offset")
}
