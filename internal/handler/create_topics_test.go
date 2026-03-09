package handler

import (
	"log/slog"
	"testing"

	"github.com/klaudworks/klite/internal/cluster"
	"github.com/klaudworks/klite/internal/metadata"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func newTestState(defaultPartitions int) *cluster.State {
	return cluster.NewState(cluster.Config{
		NodeID:            0,
		DefaultPartitions: defaultPartitions,
	})
}

func makeCreateReq(version int16, topics ...kmsg.CreateTopicsRequestTopic) *kmsg.CreateTopicsRequest {
	r := kmsg.NewPtrCreateTopicsRequest()
	r.Version = version
	r.Topics = topics
	return r
}

func simpleTopic(name string, numPartitions int32) kmsg.CreateTopicsRequestTopic {
	rt := kmsg.NewCreateTopicsRequestTopic()
	rt.Topic = name
	rt.NumPartitions = numPartitions
	rt.ReplicationFactor = 1
	return rt
}

func callHandler(t *testing.T, state *cluster.State, req *kmsg.CreateTopicsRequest) *kmsg.CreateTopicsResponse {
	t.Helper()
	h := HandleCreateTopics(state)
	resp, err := h(req)
	if err != nil {
		t.Fatalf("handler returned error: %v", err)
	}
	return resp.(*kmsg.CreateTopicsResponse)
}

func topicErrorCode(resp *kmsg.CreateTopicsResponse, topic string) int16 {
	for _, st := range resp.Topics {
		if st.Topic == topic {
			return st.ErrorCode
		}
	}
	return -1 // sentinel: topic not found in response
}

func TestCreateTopics_DuplicateInRequest(t *testing.T) {
	state := newTestState(1)
	req := makeCreateReq(7,
		simpleTopic("dup-topic", 1),
		simpleTopic("dup-topic", 1),
	)

	resp := callHandler(t, state, req)

	if len(resp.Topics) != 2 {
		t.Fatalf("expected 2 response topics, got %d", len(resp.Topics))
	}
	for _, st := range resp.Topics {
		if st.ErrorCode != kerr.InvalidRequest.Code {
			t.Errorf("topic %q: expected InvalidRequest (%d), got %d",
				st.Topic, kerr.InvalidRequest.Code, st.ErrorCode)
		}
	}
	// Topic should NOT have been created
	if state.TopicExists("dup-topic") {
		t.Error("duplicate topic should not have been created")
	}
}

func TestCreateTopics_InRequestCollision(t *testing.T) {
	state := newTestState(1)
	// normalizedInReq uses last-writer-wins: both normalize to "foo_bar",
	// so "foo_bar" (last) is the winner. When "foo.bar" is processed, its
	// normalized form maps to "foo_bar" which != "foo.bar" → collision.
	req := makeCreateReq(7,
		simpleTopic("foo.bar", 1),
		simpleTopic("foo_bar", 1),
	)

	resp := callHandler(t, state, req)

	if len(resp.Topics) != 2 {
		t.Fatalf("expected 2 response topics, got %d", len(resp.Topics))
	}

	// foo.bar is detected as colliding (last-writer-wins in normalizedInReq)
	if code := topicErrorCode(resp, "foo.bar"); code != kerr.InvalidTopicException.Code {
		t.Errorf("foo.bar: expected InvalidTopicException (%d), got %d",
			kerr.InvalidTopicException.Code, code)
	}
	// foo_bar is the last writer for normalized key, so it passes the in-request check
	if code := topicErrorCode(resp, "foo_bar"); code != 0 {
		t.Errorf("foo_bar: expected success (0), got %d", code)
	}
}

func TestCreateTopics_ReplicaAssignment_Valid(t *testing.T) {
	state := newTestState(1)

	rt := kmsg.NewCreateTopicsRequestTopic()
	rt.Topic = "ra-valid"
	rt.NumPartitions = -1
	rt.ReplicationFactor = -1
	for i := int32(0); i < 3; i++ {
		ra := kmsg.NewCreateTopicsRequestTopicReplicaAssignment()
		ra.Partition = i
		ra.Replicas = []int32{0}
		rt.ReplicaAssignment = append(rt.ReplicaAssignment, ra)
	}

	resp := callHandler(t, state, makeCreateReq(7, rt))

	if len(resp.Topics) != 1 {
		t.Fatalf("expected 1 topic, got %d", len(resp.Topics))
	}
	if resp.Topics[0].ErrorCode != 0 {
		t.Errorf("expected success, got error %d", resp.Topics[0].ErrorCode)
	}
	if resp.Topics[0].NumPartitions != 3 {
		t.Errorf("expected 3 partitions, got %d", resp.Topics[0].NumPartitions)
	}
}

func TestCreateTopics_ReplicaAssignment_NonConsecutiveIDs(t *testing.T) {
	state := newTestState(1)

	rt := kmsg.NewCreateTopicsRequestTopic()
	rt.Topic = "ra-gap"
	rt.NumPartitions = -1
	rt.ReplicationFactor = -1
	// Partition IDs 0 and 2 (gap at 1)
	for _, id := range []int32{0, 2} {
		ra := kmsg.NewCreateTopicsRequestTopicReplicaAssignment()
		ra.Partition = id
		ra.Replicas = []int32{0}
		rt.ReplicaAssignment = append(rt.ReplicaAssignment, ra)
	}

	resp := callHandler(t, state, makeCreateReq(7, rt))

	if resp.Topics[0].ErrorCode != kerr.InvalidReplicaAssignment.Code {
		t.Errorf("expected InvalidReplicaAssignment (%d), got %d",
			kerr.InvalidReplicaAssignment.Code, resp.Topics[0].ErrorCode)
	}
}

func TestCreateTopics_ReplicaAssignment_EmptyReplicas(t *testing.T) {
	state := newTestState(1)

	rt := kmsg.NewCreateTopicsRequestTopic()
	rt.Topic = "ra-empty"
	rt.NumPartitions = -1
	rt.ReplicationFactor = -1
	ra := kmsg.NewCreateTopicsRequestTopicReplicaAssignment()
	ra.Partition = 0
	ra.Replicas = nil // empty
	rt.ReplicaAssignment = append(rt.ReplicaAssignment, ra)

	resp := callHandler(t, state, makeCreateReq(7, rt))

	if resp.Topics[0].ErrorCode != kerr.InvalidReplicaAssignment.Code {
		t.Errorf("expected InvalidReplicaAssignment (%d), got %d",
			kerr.InvalidReplicaAssignment.Code, resp.Topics[0].ErrorCode)
	}
}

func TestCreateTopics_ReplicaAssignment_DuplicatePartitionIDs(t *testing.T) {
	state := newTestState(1)

	rt := kmsg.NewCreateTopicsRequestTopic()
	rt.Topic = "ra-dup-id"
	rt.NumPartitions = -1
	rt.ReplicationFactor = -1
	for i := 0; i < 2; i++ {
		ra := kmsg.NewCreateTopicsRequestTopicReplicaAssignment()
		ra.Partition = 0 // same ID twice
		ra.Replicas = []int32{0}
		rt.ReplicaAssignment = append(rt.ReplicaAssignment, ra)
	}

	resp := callHandler(t, state, makeCreateReq(7, rt))

	if resp.Topics[0].ErrorCode != kerr.InvalidReplicaAssignment.Code {
		t.Errorf("expected InvalidReplicaAssignment (%d), got %d",
			kerr.InvalidReplicaAssignment.Code, resp.Topics[0].ErrorCode)
	}
}

func TestCreateTopics_ReplicaAssignment_WithNumPartitions(t *testing.T) {
	state := newTestState(1)

	rt := kmsg.NewCreateTopicsRequestTopic()
	rt.Topic = "ra-with-numparts"
	rt.NumPartitions = 3 // must be -1 when ReplicaAssignment is set
	rt.ReplicationFactor = -1
	ra := kmsg.NewCreateTopicsRequestTopicReplicaAssignment()
	ra.Partition = 0
	ra.Replicas = []int32{0}
	rt.ReplicaAssignment = append(rt.ReplicaAssignment, ra)

	resp := callHandler(t, state, makeCreateReq(7, rt))

	if resp.Topics[0].ErrorCode != kerr.InvalidRequest.Code {
		t.Errorf("expected InvalidRequest (%d), got %d",
			kerr.InvalidRequest.Code, resp.Topics[0].ErrorCode)
	}
}

func TestCreateTopics_ReplicaAssignment_WithReplicationFactor(t *testing.T) {
	state := newTestState(1)

	rt := kmsg.NewCreateTopicsRequestTopic()
	rt.Topic = "ra-with-rf"
	rt.NumPartitions = -1
	rt.ReplicationFactor = 1 // must be -1 when ReplicaAssignment is set
	ra := kmsg.NewCreateTopicsRequestTopicReplicaAssignment()
	ra.Partition = 0
	ra.Replicas = []int32{0}
	rt.ReplicaAssignment = append(rt.ReplicaAssignment, ra)

	resp := callHandler(t, state, makeCreateReq(7, rt))

	if resp.Topics[0].ErrorCode != kerr.InvalidRequest.Code {
		t.Errorf("expected InvalidRequest (%d), got %d",
			kerr.InvalidRequest.Code, resp.Topics[0].ErrorCode)
	}
}

func TestCreateTopics_ValidateOnly(t *testing.T) {
	state := newTestState(1)

	req := makeCreateReq(7, simpleTopic("validate-me", 3))
	req.ValidateOnly = true

	// Add a config to verify it's returned
	c := kmsg.NewCreateTopicsRequestTopicConfig()
	c.Name = "retention.ms"
	val := "86400000"
	c.Value = &val
	req.Topics[0].Configs = append(req.Topics[0].Configs, c)

	resp := callHandler(t, state, req)

	if len(resp.Topics) != 1 {
		t.Fatalf("expected 1 topic, got %d", len(resp.Topics))
	}
	st := resp.Topics[0]
	if st.ErrorCode != 0 {
		t.Errorf("expected success, got error %d", st.ErrorCode)
	}
	if st.NumPartitions != 3 {
		t.Errorf("expected 3 partitions, got %d", st.NumPartitions)
	}
	if st.ReplicationFactor != 1 {
		t.Errorf("expected RF 1, got %d", st.ReplicationFactor)
	}

	// Config should be in response
	found := false
	for _, rc := range st.Configs {
		if rc.Name == "retention.ms" && rc.Value != nil && *rc.Value == "86400000" {
			found = true
		}
	}
	if !found {
		t.Error("expected retention.ms config in validate-only response")
	}

	// Topic must NOT exist
	if state.TopicExists("validate-me") {
		t.Error("topic should not have been created in validate-only mode")
	}
}

func TestCreateTopics_TopicAlreadyExists(t *testing.T) {
	state := newTestState(1)
	state.CreateTopic("existing", 1)

	resp := callHandler(t, state, makeCreateReq(7, simpleTopic("existing", 1)))

	if resp.Topics[0].ErrorCode != kerr.TopicAlreadyExists.Code {
		t.Errorf("expected TopicAlreadyExists (%d), got %d",
			kerr.TopicAlreadyExists.Code, resp.Topics[0].ErrorCode)
	}
}

func TestCreateTopics_CollisionWithExisting(t *testing.T) {
	state := newTestState(1)
	state.CreateTopic("foo.bar", 1)

	resp := callHandler(t, state, makeCreateReq(7, simpleTopic("foo_bar", 1)))

	if resp.Topics[0].ErrorCode != kerr.InvalidTopicException.Code {
		t.Errorf("expected InvalidTopicException (%d), got %d",
			kerr.InvalidTopicException.Code, resp.Topics[0].ErrorCode)
	}
}

func TestCreateTopics_InvalidTopicName(t *testing.T) {
	cases := []struct {
		name string
		desc string
	}{
		{"", "empty"},
		{".", "dot"},
		{"..", "dotdot"},
		{"has space", "space"},
		{"has/slash", "slash"},
		{"has@symbol", "at"},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			state := newTestState(1)
			resp := callHandler(t, state, makeCreateReq(7, simpleTopic(tc.name, 1)))

			if resp.Topics[0].ErrorCode != kerr.InvalidTopicException.Code {
				t.Errorf("topic %q: expected InvalidTopicException (%d), got %d",
					tc.name, kerr.InvalidTopicException.Code, resp.Topics[0].ErrorCode)
			}
		})
	}
}

func TestCreateTopics_InvalidConfigKey(t *testing.T) {
	state := newTestState(1)

	rt := simpleTopic("bad-config", 1)
	c := kmsg.NewCreateTopicsRequestTopicConfig()
	c.Name = "unknown.config"
	val := "value"
	c.Value = &val
	rt.Configs = append(rt.Configs, c)

	resp := callHandler(t, state, makeCreateReq(7, rt))

	if resp.Topics[0].ErrorCode != kerr.InvalidConfig.Code {
		t.Errorf("expected InvalidConfig (%d), got %d",
			kerr.InvalidConfig.Code, resp.Topics[0].ErrorCode)
	}
}

func TestCreateTopics_ZeroPartitions(t *testing.T) {
	state := newTestState(1)
	resp := callHandler(t, state, makeCreateReq(7, simpleTopic("zero-parts", 0)))

	if resp.Topics[0].ErrorCode != kerr.InvalidPartitions.Code {
		t.Errorf("expected InvalidPartitions (%d), got %d",
			kerr.InvalidPartitions.Code, resp.Topics[0].ErrorCode)
	}
}

func TestCreateTopics_DefaultPartitions(t *testing.T) {
	state := newTestState(5)
	resp := callHandler(t, state, makeCreateReq(7, simpleTopic("default-parts", -1)))

	if resp.Topics[0].ErrorCode != 0 {
		t.Fatalf("expected success, got error %d", resp.Topics[0].ErrorCode)
	}
	if resp.Topics[0].NumPartitions != 5 {
		t.Errorf("expected 5 partitions (default), got %d", resp.Topics[0].NumPartitions)
	}
	td := state.GetTopic("default-parts")
	if td == nil {
		t.Fatal("topic should exist")
	}
	if len(td.Partitions) != 5 {
		t.Errorf("expected 5 partitions in state, got %d", len(td.Partitions))
	}
}

func TestCreateTopics_SuccessfulCreate(t *testing.T) {
	state := newTestState(1)

	rt := simpleTopic("new-topic", 3)
	c := kmsg.NewCreateTopicsRequestTopicConfig()
	c.Name = "retention.ms"
	val := "3600000"
	c.Value = &val
	rt.Configs = append(rt.Configs, c)

	resp := callHandler(t, state, makeCreateReq(7, rt))

	st := resp.Topics[0]
	if st.ErrorCode != 0 {
		t.Fatalf("expected success, got error %d", st.ErrorCode)
	}
	if st.NumPartitions != 3 {
		t.Errorf("expected 3 partitions, got %d", st.NumPartitions)
	}
	if st.ReplicationFactor != 1 {
		t.Errorf("expected RF 1, got %d", st.ReplicationFactor)
	}
	var zeroID [16]byte
	if st.TopicID == zeroID {
		t.Error("expected non-zero topic ID")
	}

	td := state.GetTopic("new-topic")
	if td == nil {
		t.Fatal("topic should exist in state")
	}
	if len(td.Partitions) != 3 {
		t.Errorf("expected 3 partitions in state, got %d", len(td.Partitions))
	}
	if v, ok := td.GetConfig("retention.ms"); !ok || v != "3600000" {
		t.Errorf("expected retention.ms=3600000, got %q (exists=%v)", v, ok)
	}
}

func TestCreateTopics_PersistFailure(t *testing.T) {
	state := newTestState(1)

	ml, err := metadata.NewLog(metadata.LogConfig{DataDir: t.TempDir(), Logger: slog.Default()})
	if err != nil {
		t.Fatalf("new metadata log: %v", err)
	}
	state.SetMetadataLog(ml)
	if err := ml.Close(); err != nil {
		t.Fatalf("close metadata log: %v", err)
	}

	resp := callHandler(t, state, makeCreateReq(7, simpleTopic("persist-fail", 1)))

	if resp.Topics[0].ErrorCode != kerr.KafkaStorageError.Code {
		t.Fatalf("expected KafkaStorageError (%d), got %d", kerr.KafkaStorageError.Code, resp.Topics[0].ErrorCode)
	}
	if state.TopicExists("persist-fail") {
		t.Fatal("topic should not exist after metadata persistence failure")
	}
}
