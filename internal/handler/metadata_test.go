package handler

import (
	"testing"

	"github.com/klaudworks/klite/internal/cluster"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func newMetadataCfg(state *cluster.State) MetadataConfig {
	return MetadataConfig{
		NodeID:         0,
		AdvertisedAddr: "localhost:9092",
		ClusterID:      "test-cluster",
		State:          state,
	}
}

func callMetadata(t *testing.T, cfg MetadataConfig, req *kmsg.MetadataRequest) *kmsg.MetadataResponse {
	t.Helper()
	h := HandleMetadata(cfg)
	resp, err := h(req)
	if err != nil {
		t.Fatalf("handler returned error: %v", err)
	}
	return resp.(*kmsg.MetadataResponse)
}

func TestMetadata_NullTopicsReturnsAll(t *testing.T) {
	state := newTestState(1)
	state.CreateTopic("topic-a", 2)
	state.CreateTopic("topic-b", 3)
	cfg := newMetadataCfg(state)

	req := kmsg.NewPtrMetadataRequest()
	req.Version = 9
	req.Topics = nil // null = return all topics

	resp := callMetadata(t, cfg, req)

	if len(resp.Topics) != 2 {
		t.Fatalf("expected 2 topics, got %d", len(resp.Topics))
	}
	names := map[string]int{}
	for _, st := range resp.Topics {
		if st.Topic == nil {
			t.Fatal("topic name should not be nil")
		}
		names[*st.Topic] = len(st.Partitions)
	}
	if names["topic-a"] != 2 {
		t.Errorf("topic-a: expected 2 partitions, got %d", names["topic-a"])
	}
	if names["topic-b"] != 3 {
		t.Errorf("topic-b: expected 3 partitions, got %d", names["topic-b"])
	}
}

func TestMetadata_TopicByIDReturnsUnknownTopicID(t *testing.T) {
	state := newTestState(1)
	cfg := newMetadataCfg(state)

	req := kmsg.NewPtrMetadataRequest()
	req.Version = 10
	rt := kmsg.NewMetadataRequestTopic()
	rt.TopicID = [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	req.Topics = append(req.Topics, rt)

	resp := callMetadata(t, cfg, req)

	if len(resp.Topics) != 1 {
		t.Fatalf("expected 1 topic, got %d", len(resp.Topics))
	}
	if resp.Topics[0].ErrorCode != kerr.UnknownTopicID.Code {
		t.Errorf("expected UnknownTopicID (%d), got %d",
			kerr.UnknownTopicID.Code, resp.Topics[0].ErrorCode)
	}
}

func TestMetadata_InvalidTopicName(t *testing.T) {
	state := newTestState(1)
	cfg := newMetadataCfg(state)

	req := kmsg.NewPtrMetadataRequest()
	req.Version = 9
	rt := kmsg.NewMetadataRequestTopic()
	bad := "topic with spaces"
	rt.Topic = &bad
	req.Topics = append(req.Topics, rt)

	resp := callMetadata(t, cfg, req)

	if len(resp.Topics) != 1 {
		t.Fatalf("expected 1 topic, got %d", len(resp.Topics))
	}
	if resp.Topics[0].ErrorCode != kerr.InvalidTopicException.Code {
		t.Errorf("expected InvalidTopicException (%d), got %d",
			kerr.InvalidTopicException.Code, resp.Topics[0].ErrorCode)
	}
}

func TestMetadata_UnknownTopicWithoutAutoCreate(t *testing.T) {
	state := cluster.NewState(cluster.Config{
		NodeID:            0,
		DefaultPartitions: 1,
		AutoCreateTopics:  false,
	})
	cfg := newMetadataCfg(state)

	req := kmsg.NewPtrMetadataRequest()
	req.Version = 9
	rt := kmsg.NewMetadataRequestTopic()
	name := "nonexistent"
	rt.Topic = &name
	req.Topics = append(req.Topics, rt)

	resp := callMetadata(t, cfg, req)

	if len(resp.Topics) != 1 {
		t.Fatalf("expected 1 topic, got %d", len(resp.Topics))
	}
	if resp.Topics[0].ErrorCode != kerr.UnknownTopicOrPartition.Code {
		t.Errorf("expected UnknownTopicOrPartition (%d), got %d",
			kerr.UnknownTopicOrPartition.Code, resp.Topics[0].ErrorCode)
	}
}

func TestMetadata_AutoCreateV4AllowField(t *testing.T) {
	state := cluster.NewState(cluster.Config{
		NodeID:            0,
		DefaultPartitions: 1,
		AutoCreateTopics:  true,
	})
	cfg := newMetadataCfg(state)

	// v4+ with AllowAutoTopicCreation=false should not auto-create
	req := kmsg.NewPtrMetadataRequest()
	req.Version = 9
	req.AllowAutoTopicCreation = false
	rt := kmsg.NewMetadataRequestTopic()
	name := "no-auto"
	rt.Topic = &name
	req.Topics = append(req.Topics, rt)

	resp := callMetadata(t, cfg, req)

	if len(resp.Topics) != 1 {
		t.Fatalf("expected 1 topic, got %d", len(resp.Topics))
	}
	if resp.Topics[0].ErrorCode != kerr.UnknownTopicOrPartition.Code {
		t.Errorf("expected UnknownTopicOrPartition when AllowAutoTopicCreation=false, got %d",
			resp.Topics[0].ErrorCode)
	}
	if state.TopicExists("no-auto") {
		t.Error("topic should not have been auto-created")
	}

	// v4+ with AllowAutoTopicCreation=true should auto-create
	req2 := kmsg.NewPtrMetadataRequest()
	req2.Version = 9
	req2.AllowAutoTopicCreation = true
	rt2 := kmsg.NewMetadataRequestTopic()
	name2 := "yes-auto"
	rt2.Topic = &name2
	req2.Topics = append(req2.Topics, rt2)

	resp2 := callMetadata(t, cfg, req2)

	if len(resp2.Topics) != 1 {
		t.Fatalf("expected 1 topic, got %d", len(resp2.Topics))
	}
	if resp2.Topics[0].ErrorCode != 0 {
		t.Errorf("expected success (0) when AllowAutoTopicCreation=true, got %d",
			resp2.Topics[0].ErrorCode)
	}
	if !state.TopicExists("yes-auto") {
		t.Error("topic should have been auto-created")
	}
}

func TestMetadata_BuildTopicMetadataIncludesTopicID(t *testing.T) {
	state := newTestState(1)
	td, _ := state.CreateTopic("id-topic", 1)
	cfg := newMetadataCfg(state)
	var zeroID [16]byte

	// Version < 10: TopicID should be zero
	req := kmsg.NewPtrMetadataRequest()
	req.Version = 9
	rt := kmsg.NewMetadataRequestTopic()
	name := "id-topic"
	rt.Topic = &name
	req.Topics = append(req.Topics, rt)

	resp := callMetadata(t, cfg, req)

	if len(resp.Topics) != 1 {
		t.Fatalf("expected 1 topic, got %d", len(resp.Topics))
	}
	if resp.Topics[0].TopicID != zeroID {
		t.Error("expected zero TopicID for version < 10")
	}

	// Version >= 10: TopicID should be populated
	req10 := kmsg.NewPtrMetadataRequest()
	req10.Version = 10
	rt10 := kmsg.NewMetadataRequestTopic()
	rt10.Topic = &name
	req10.Topics = append(req10.Topics, rt10)

	resp10 := callMetadata(t, cfg, req10)

	if len(resp10.Topics) != 1 {
		t.Fatalf("expected 1 topic, got %d", len(resp10.Topics))
	}
	if resp10.Topics[0].TopicID != td.ID {
		t.Errorf("expected TopicID %v for version >= 10, got %v",
			td.ID, resp10.Topics[0].TopicID)
	}
}

func TestMetadata_BrokerInfo(t *testing.T) {
	state := newTestState(1)
	cfg := MetadataConfig{
		NodeID:         7,
		AdvertisedAddr: "my-host:19092",
		ClusterID:      "my-cluster",
		State:          state,
	}

	req := kmsg.NewPtrMetadataRequest()
	req.Version = 9
	req.Topics = []kmsg.MetadataRequestTopic{} // empty list

	resp := callMetadata(t, cfg, req)

	if len(resp.Brokers) != 1 {
		t.Fatalf("expected 1 broker, got %d", len(resp.Brokers))
	}
	if resp.Brokers[0].NodeID != 7 {
		t.Errorf("expected NodeID 7, got %d", resp.Brokers[0].NodeID)
	}
	if resp.Brokers[0].Host != "my-host" {
		t.Errorf("expected host 'my-host', got %q", resp.Brokers[0].Host)
	}
	if resp.Brokers[0].Port != 19092 {
		t.Errorf("expected port 19092, got %d", resp.Brokers[0].Port)
	}
	if resp.ClusterID == nil || *resp.ClusterID != "my-cluster" {
		t.Errorf("expected ClusterID 'my-cluster', got %v", resp.ClusterID)
	}
	if resp.ControllerID != 7 {
		t.Errorf("expected ControllerID 7, got %d", resp.ControllerID)
	}
}

func TestMetadata_NilTopicPointerSkipped(t *testing.T) {
	state := newTestState(1)
	cfg := newMetadataCfg(state)

	req := kmsg.NewPtrMetadataRequest()
	req.Version = 9
	rt := kmsg.NewMetadataRequestTopic()
	rt.Topic = nil // nil topic name, zero TopicID
	req.Topics = append(req.Topics, rt)

	resp := callMetadata(t, cfg, req)

	if len(resp.Topics) != 0 {
		t.Errorf("expected 0 topics (nil topic pointer should be skipped), got %d", len(resp.Topics))
	}
}

func TestMetadata_PartitionLeaderAndReplicas(t *testing.T) {
	state := newTestState(1)
	state.CreateTopic("partitioned", 3)
	cfg := MetadataConfig{
		NodeID:         5,
		AdvertisedAddr: "localhost:9092",
		ClusterID:      "test",
		State:          state,
	}

	req := kmsg.NewPtrMetadataRequest()
	req.Version = 9
	rt := kmsg.NewMetadataRequestTopic()
	name := "partitioned"
	rt.Topic = &name
	req.Topics = append(req.Topics, rt)

	resp := callMetadata(t, cfg, req)

	if len(resp.Topics) != 1 {
		t.Fatalf("expected 1 topic, got %d", len(resp.Topics))
	}
	if len(resp.Topics[0].Partitions) != 3 {
		t.Fatalf("expected 3 partitions, got %d", len(resp.Topics[0].Partitions))
	}
	for i, p := range resp.Topics[0].Partitions {
		if p.Partition != int32(i) {
			t.Errorf("partition %d: expected index %d, got %d", i, i, p.Partition)
		}
		if p.Leader != 5 {
			t.Errorf("partition %d: expected leader 5, got %d", i, p.Leader)
		}
		if len(p.Replicas) != 1 || p.Replicas[0] != 5 {
			t.Errorf("partition %d: expected replicas [5], got %v", i, p.Replicas)
		}
		if len(p.ISR) != 1 || p.ISR[0] != 5 {
			t.Errorf("partition %d: expected ISR [5], got %v", i, p.ISR)
		}
	}
}
