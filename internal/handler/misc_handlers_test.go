package handler

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// --- HandleFindCoordinator ---

func TestHandleFindCoordinator_ValidKey(t *testing.T) {
	h := HandleFindCoordinator(FindCoordinatorConfig{
		NodeID:         1,
		AdvertisedAddr: "broker1:9092",
	})

	r := kmsg.NewPtrFindCoordinatorRequest()
	r.Version = 4
	r.CoordinatorType = 0 // group
	r.CoordinatorKeys = []string{"my-group"}

	resp, err := h(r)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	fcResp := resp.(*kmsg.FindCoordinatorResponse)
	if len(fcResp.Coordinators) != 1 {
		t.Fatalf("expected 1 coordinator, got %d", len(fcResp.Coordinators))
	}
	c := fcResp.Coordinators[0]
	if c.Key != "my-group" {
		t.Errorf("expected key my-group, got %q", c.Key)
	}
	if c.ErrorCode != 0 {
		t.Errorf("expected no error, got %d", c.ErrorCode)
	}
	if c.NodeID != 1 {
		t.Errorf("expected NodeID 1, got %d", c.NodeID)
	}
	if c.Host != "broker1" {
		t.Errorf("expected host broker1, got %q", c.Host)
	}
	if c.Port != 9092 {
		t.Errorf("expected port 9092, got %d", c.Port)
	}
}

func TestHandleFindCoordinator_UnknownType(t *testing.T) {
	h := HandleFindCoordinator(FindCoordinatorConfig{
		NodeID:         0,
		AdvertisedAddr: "localhost:9092",
	})

	r := kmsg.NewPtrFindCoordinatorRequest()
	r.Version = 4
	r.CoordinatorType = 99 // invalid
	r.CoordinatorKeys = []string{"key1"}

	resp, err := h(r)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	fcResp := resp.(*kmsg.FindCoordinatorResponse)
	if len(fcResp.Coordinators) != 1 {
		t.Fatalf("expected 1 coordinator, got %d", len(fcResp.Coordinators))
	}
	c := fcResp.Coordinators[0]
	if c.ErrorCode != kerr.InvalidRequest.Code {
		t.Errorf("expected InvalidRequest (%d), got %d", kerr.InvalidRequest.Code, c.ErrorCode)
	}
	if c.ErrorMessage == nil || *c.ErrorMessage != "invalid coordinator type" {
		t.Errorf("expected error message 'invalid coordinator type', got %v", c.ErrorMessage)
	}
}

func TestHandleFindCoordinator_LegacyV0(t *testing.T) {
	h := HandleFindCoordinator(FindCoordinatorConfig{
		NodeID:         2,
		AdvertisedAddr: "broker2:9093",
	})

	r := kmsg.NewPtrFindCoordinatorRequest()
	r.Version = 0
	r.CoordinatorType = 0
	r.CoordinatorKey = "legacy-group"

	resp, err := h(r)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	fcResp := resp.(*kmsg.FindCoordinatorResponse)
	// v0-v3: top-level fields should be populated
	if fcResp.NodeID != 2 {
		t.Errorf("expected top-level NodeID 2, got %d", fcResp.NodeID)
	}
	if fcResp.Host != "broker2" {
		t.Errorf("expected top-level host broker2, got %q", fcResp.Host)
	}
	if fcResp.Port != 9093 {
		t.Errorf("expected top-level port 9093, got %d", fcResp.Port)
	}
	if fcResp.ErrorCode != 0 {
		t.Errorf("expected no top-level error, got %d", fcResp.ErrorCode)
	}
}

func TestHandleFindCoordinator_MultipleKeys(t *testing.T) {
	h := HandleFindCoordinator(FindCoordinatorConfig{
		NodeID:         0,
		AdvertisedAddr: "host:9092",
	})

	r := kmsg.NewPtrFindCoordinatorRequest()
	r.Version = 4
	r.CoordinatorType = 0
	r.CoordinatorKeys = []string{"g1", "g2", "g3"}

	resp, err := h(r)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	fcResp := resp.(*kmsg.FindCoordinatorResponse)
	if len(fcResp.Coordinators) != 3 {
		t.Fatalf("expected 3 coordinators, got %d", len(fcResp.Coordinators))
	}
	for i, c := range fcResp.Coordinators {
		if c.ErrorCode != 0 {
			t.Errorf("coordinator %d: expected no error, got %d", i, c.ErrorCode)
		}
	}
}

func TestHandleFindCoordinator_TransactionCoordinator(t *testing.T) {
	h := HandleFindCoordinator(FindCoordinatorConfig{
		NodeID:         0,
		AdvertisedAddr: "host:9092",
	})

	r := kmsg.NewPtrFindCoordinatorRequest()
	r.Version = 4
	r.CoordinatorType = 1 // transaction
	r.CoordinatorKeys = []string{"txn-1"}

	resp, err := h(r)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	fcResp := resp.(*kmsg.FindCoordinatorResponse)
	if len(fcResp.Coordinators) != 1 {
		t.Fatalf("expected 1 coordinator, got %d", len(fcResp.Coordinators))
	}
	if fcResp.Coordinators[0].ErrorCode != 0 {
		t.Errorf("expected no error for transaction coordinator, got %d",
			fcResp.Coordinators[0].ErrorCode)
	}
}

// --- HandleDescribeCluster ---

func TestHandleDescribeCluster_BasicResponse(t *testing.T) {
	h := HandleDescribeCluster(DescribeClusterConfig{
		NodeID:         5,
		AdvertisedAddr: "myhost:9094",
		ClusterID:      "test-cluster-id",
	})

	r := kmsg.NewPtrDescribeClusterRequest()
	resp, err := h(r)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	dcResp := resp.(*kmsg.DescribeClusterResponse)

	if dcResp.ClusterID != "test-cluster-id" {
		t.Errorf("expected ClusterID test-cluster-id, got %q", dcResp.ClusterID)
	}
	if dcResp.ControllerID != 5 {
		t.Errorf("expected ControllerID 5, got %d", dcResp.ControllerID)
	}
	if len(dcResp.Brokers) != 1 {
		t.Fatalf("expected 1 broker, got %d", len(dcResp.Brokers))
	}
	b := dcResp.Brokers[0]
	if b.NodeID != 5 {
		t.Errorf("expected broker NodeID 5, got %d", b.NodeID)
	}
	if b.Host != "myhost" {
		t.Errorf("expected broker host myhost, got %q", b.Host)
	}
	if b.Port != 9094 {
		t.Errorf("expected broker port 9094, got %d", b.Port)
	}
}

// --- HandleDescribeLogDirs ---

func TestHandleDescribeLogDirs_AllTopics(t *testing.T) {
	state := newTestState(1)
	state.CreateTopic("topic-a", 2)
	state.CreateTopic("topic-b", 1)

	h := HandleDescribeLogDirs(state, "/data/klite")
	r := kmsg.NewPtrDescribeLogDirsRequest()
	r.Topics = nil // nil means all topics

	resp, err := h(r)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	dlResp := resp.(*kmsg.DescribeLogDirsResponse)
	if len(dlResp.Dirs) != 1 {
		t.Fatalf("expected 1 dir, got %d", len(dlResp.Dirs))
	}
	dir := dlResp.Dirs[0]
	if dir.Dir != "/data/klite" {
		t.Errorf("expected dir /data/klite, got %q", dir.Dir)
	}
	if len(dir.Topics) != 2 {
		t.Fatalf("expected 2 topics, got %d", len(dir.Topics))
	}

	topicParts := make(map[string]int)
	for _, rt := range dir.Topics {
		topicParts[rt.Topic] = len(rt.Partitions)
	}
	if topicParts["topic-a"] != 2 {
		t.Errorf("expected topic-a to have 2 partitions, got %d", topicParts["topic-a"])
	}
	if topicParts["topic-b"] != 1 {
		t.Errorf("expected topic-b to have 1 partition, got %d", topicParts["topic-b"])
	}
}

func TestHandleDescribeLogDirs_SpecificTopics(t *testing.T) {
	state := newTestState(1)
	state.CreateTopic("topic-a", 3)
	state.CreateTopic("topic-b", 1)

	h := HandleDescribeLogDirs(state, "/data/klite")
	r := kmsg.NewPtrDescribeLogDirsRequest()
	rt := kmsg.NewDescribeLogDirsRequestTopic()
	rt.Topic = "topic-a"
	rt.Partitions = []int32{0, 2}
	r.Topics = append(r.Topics, rt)

	resp, err := h(r)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	dlResp := resp.(*kmsg.DescribeLogDirsResponse)
	dir := dlResp.Dirs[0]
	if len(dir.Topics) != 1 {
		t.Fatalf("expected 1 topic, got %d", len(dir.Topics))
	}
	if dir.Topics[0].Topic != "topic-a" {
		t.Errorf("expected topic-a, got %q", dir.Topics[0].Topic)
	}
	if len(dir.Topics[0].Partitions) != 2 {
		t.Fatalf("expected 2 partitions, got %d", len(dir.Topics[0].Partitions))
	}
}

func TestHandleDescribeLogDirs_UnknownTopic(t *testing.T) {
	state := newTestState(1)

	h := HandleDescribeLogDirs(state, "/data/klite")
	r := kmsg.NewPtrDescribeLogDirsRequest()
	rt := kmsg.NewDescribeLogDirsRequestTopic()
	rt.Topic = "nonexistent"
	rt.Partitions = []int32{0}
	r.Topics = append(r.Topics, rt)

	resp, err := h(r)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	dlResp := resp.(*kmsg.DescribeLogDirsResponse)
	dir := dlResp.Dirs[0]
	// Unknown topics are omitted
	if len(dir.Topics) != 0 {
		t.Errorf("expected 0 topics for unknown topic, got %d", len(dir.Topics))
	}
}

func TestHandleDescribeLogDirs_OutOfRangePartition(t *testing.T) {
	state := newTestState(1)
	state.CreateTopic("topic-a", 2) // partitions 0,1

	h := HandleDescribeLogDirs(state, "/data/klite")
	r := kmsg.NewPtrDescribeLogDirsRequest()
	rt := kmsg.NewDescribeLogDirsRequestTopic()
	rt.Topic = "topic-a"
	rt.Partitions = []int32{0, 5} // 5 is out of range
	r.Topics = append(r.Topics, rt)

	resp, err := h(r)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	dlResp := resp.(*kmsg.DescribeLogDirsResponse)
	dir := dlResp.Dirs[0]
	if len(dir.Topics) != 1 {
		t.Fatalf("expected 1 topic, got %d", len(dir.Topics))
	}
	// Only partition 0 should be returned; 5 is out of range and skipped
	if len(dir.Topics[0].Partitions) != 1 {
		t.Fatalf("expected 1 partition (out-of-range skipped), got %d", len(dir.Topics[0].Partitions))
	}
	if dir.Topics[0].Partitions[0].Partition != 0 {
		t.Errorf("expected partition 0, got %d", dir.Topics[0].Partitions[0].Partition)
	}
}

// --- HandleDeleteTopics ---

func TestHandleDeleteTopics_ByName(t *testing.T) {
	state := newTestState(1)
	state.CreateTopic("to-delete", 1)

	h := HandleDeleteTopics(state)
	r := kmsg.NewPtrDeleteTopicsRequest()
	r.Version = 6
	rt := kmsg.NewDeleteTopicsRequestTopic()
	name := "to-delete"
	rt.Topic = &name
	r.Topics = append(r.Topics, rt)

	resp, err := h(r)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	dtResp := resp.(*kmsg.DeleteTopicsResponse)
	if len(dtResp.Topics) != 1 {
		t.Fatalf("expected 1 topic, got %d", len(dtResp.Topics))
	}
	if dtResp.Topics[0].ErrorCode != 0 {
		t.Errorf("expected success, got error %d", dtResp.Topics[0].ErrorCode)
	}
	if state.TopicExists("to-delete") {
		t.Error("topic should have been deleted")
	}
}

func TestHandleDeleteTopics_ByTopicID(t *testing.T) {
	state := newTestState(1)
	state.CreateTopic("by-id", 1)
	td := state.GetTopic("by-id")
	topicID := td.ID

	h := HandleDeleteTopics(state)
	r := kmsg.NewPtrDeleteTopicsRequest()
	r.Version = 6
	rt := kmsg.NewDeleteTopicsRequestTopic()
	rt.TopicID = topicID
	r.Topics = append(r.Topics, rt)

	resp, err := h(r)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	dtResp := resp.(*kmsg.DeleteTopicsResponse)
	if len(dtResp.Topics) != 1 {
		t.Fatalf("expected 1 topic, got %d", len(dtResp.Topics))
	}
	if dtResp.Topics[0].ErrorCode != 0 {
		t.Errorf("expected success, got error %d", dtResp.Topics[0].ErrorCode)
	}
	if state.TopicExists("by-id") {
		t.Error("topic should have been deleted")
	}
}

func TestHandleDeleteTopics_UnknownTopicID(t *testing.T) {
	state := newTestState(1)

	h := HandleDeleteTopics(state)
	r := kmsg.NewPtrDeleteTopicsRequest()
	r.Version = 6
	rt := kmsg.NewDeleteTopicsRequestTopic()
	rt.TopicID = [16]byte{1, 2, 3, 4} // doesn't exist
	r.Topics = append(r.Topics, rt)

	resp, err := h(r)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	dtResp := resp.(*kmsg.DeleteTopicsResponse)
	if dtResp.Topics[0].ErrorCode != kerr.UnknownTopicID.Code {
		t.Errorf("expected UnknownTopicID (%d), got %d",
			kerr.UnknownTopicID.Code, dtResp.Topics[0].ErrorCode)
	}
}

func TestHandleDeleteTopics_EmptyTopicName(t *testing.T) {
	state := newTestState(1)

	h := HandleDeleteTopics(state)
	r := kmsg.NewPtrDeleteTopicsRequest()
	r.Version = 6
	rt := kmsg.NewDeleteTopicsRequestTopic()
	name := ""
	rt.Topic = &name
	r.Topics = append(r.Topics, rt)

	resp, err := h(r)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	dtResp := resp.(*kmsg.DeleteTopicsResponse)
	if dtResp.Topics[0].ErrorCode != kerr.UnknownTopicOrPartition.Code {
		t.Errorf("expected UnknownTopicOrPartition (%d), got %d",
			kerr.UnknownTopicOrPartition.Code, dtResp.Topics[0].ErrorCode)
	}
}

func TestHandleDeleteTopics_LegacyTopicNames(t *testing.T) {
	state := newTestState(1)
	state.CreateTopic("legacy-del", 1)

	h := HandleDeleteTopics(state)
	r := kmsg.NewPtrDeleteTopicsRequest()
	r.Version = 5 // v0-v5 uses TopicNames
	r.TopicNames = []string{"legacy-del"}

	resp, err := h(r)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	dtResp := resp.(*kmsg.DeleteTopicsResponse)
	if len(dtResp.Topics) != 1 {
		t.Fatalf("expected 1 topic, got %d", len(dtResp.Topics))
	}
	if dtResp.Topics[0].ErrorCode != 0 {
		t.Errorf("expected success, got error %d", dtResp.Topics[0].ErrorCode)
	}
	if state.TopicExists("legacy-del") {
		t.Error("topic should have been deleted")
	}
}

func TestHandleDeleteTopics_NonexistentByName(t *testing.T) {
	state := newTestState(1)

	h := HandleDeleteTopics(state)
	r := kmsg.NewPtrDeleteTopicsRequest()
	r.Version = 6
	rt := kmsg.NewDeleteTopicsRequestTopic()
	name := "no-such-topic"
	rt.Topic = &name
	r.Topics = append(r.Topics, rt)

	resp, err := h(r)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	dtResp := resp.(*kmsg.DeleteTopicsResponse)
	if dtResp.Topics[0].ErrorCode != kerr.UnknownTopicOrPartition.Code {
		t.Errorf("expected UnknownTopicOrPartition (%d), got %d",
			kerr.UnknownTopicOrPartition.Code, dtResp.Topics[0].ErrorCode)
	}
}
