package handler

import (
	"testing"

	"github.com/klaudworks/klite/internal/cluster"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// newTestStateWithShutdown creates a State with a controllable shutdown channel.
// Closing the returned channel causes group.Send to fail, which is used to test
// the CoordinatorNotAvailable error path in handlers.
func newTestStateWithShutdown(t *testing.T) (*cluster.State, chan struct{}) {
	t.Helper()
	shutdownCh := make(chan struct{})
	s := cluster.NewState(cluster.Config{NodeID: 0, DefaultPartitions: 1})
	s.SetShutdownCh(shutdownCh)
	t.Cleanup(func() {
		select {
		case <-shutdownCh:
		default:
			close(shutdownCh)
		}
	})
	return s, shutdownCh
}

// --- HandleHeartbeat ---

func TestHandleHeartbeat_EmptyGroupID(t *testing.T) {
	state := newTestState(1)
	h := HandleHeartbeat(state)

	r := kmsg.NewPtrHeartbeatRequest()
	r.Group = ""
	resp, err := h(r)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	hResp := resp.(*kmsg.HeartbeatResponse)
	if hResp.ErrorCode != kerr.InvalidGroupID.Code {
		t.Errorf("expected InvalidGroupID (%d), got %d", kerr.InvalidGroupID.Code, hResp.ErrorCode)
	}
}

func TestHandleHeartbeat_GroupNotFound(t *testing.T) {
	state := newTestState(1)
	h := HandleHeartbeat(state)

	r := kmsg.NewPtrHeartbeatRequest()
	r.Group = "nonexistent"
	resp, err := h(r)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	hResp := resp.(*kmsg.HeartbeatResponse)
	if hResp.ErrorCode != kerr.UnknownMemberID.Code {
		t.Errorf("expected UnknownMemberID (%d), got %d", kerr.UnknownMemberID.Code, hResp.ErrorCode)
	}
}

func TestHandleHeartbeat_SendError(t *testing.T) {
	state, shutdownCh := newTestStateWithShutdown(t)
	// Create a group so GetGroup returns non-nil
	state.GetOrCreateGroup("test-group")
	// Close shutdown to make Send fail
	close(shutdownCh)

	h := HandleHeartbeat(state)
	r := kmsg.NewPtrHeartbeatRequest()
	r.Group = "test-group"
	resp, err := h(r)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	hResp := resp.(*kmsg.HeartbeatResponse)
	if hResp.ErrorCode != kerr.CoordinatorNotAvailable.Code {
		t.Errorf("expected CoordinatorNotAvailable (%d), got %d",
			kerr.CoordinatorNotAvailable.Code, hResp.ErrorCode)
	}
}

// --- HandleSyncGroup ---

func TestHandleSyncGroup_EmptyGroupID(t *testing.T) {
	state := newTestState(1)
	h := HandleSyncGroup(state)

	r := kmsg.NewPtrSyncGroupRequest()
	r.Group = ""
	resp, err := h(r)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	sResp := resp.(*kmsg.SyncGroupResponse)
	if sResp.ErrorCode != kerr.InvalidGroupID.Code {
		t.Errorf("expected InvalidGroupID (%d), got %d", kerr.InvalidGroupID.Code, sResp.ErrorCode)
	}
}

func TestHandleSyncGroup_GroupNotFound(t *testing.T) {
	state := newTestState(1)
	h := HandleSyncGroup(state)

	r := kmsg.NewPtrSyncGroupRequest()
	r.Group = "nonexistent"
	resp, err := h(r)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	sResp := resp.(*kmsg.SyncGroupResponse)
	if sResp.ErrorCode != kerr.UnknownMemberID.Code {
		t.Errorf("expected UnknownMemberID (%d), got %d", kerr.UnknownMemberID.Code, sResp.ErrorCode)
	}
}

func TestHandleSyncGroup_SendError(t *testing.T) {
	state, shutdownCh := newTestStateWithShutdown(t)
	state.GetOrCreateGroup("test-group")
	close(shutdownCh)

	h := HandleSyncGroup(state)
	r := kmsg.NewPtrSyncGroupRequest()
	r.Group = "test-group"
	resp, err := h(r)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	sResp := resp.(*kmsg.SyncGroupResponse)
	if sResp.ErrorCode != kerr.CoordinatorNotAvailable.Code {
		t.Errorf("expected CoordinatorNotAvailable (%d), got %d",
			kerr.CoordinatorNotAvailable.Code, sResp.ErrorCode)
	}
}

// --- HandleLeaveGroup ---

func TestHandleLeaveGroup_EmptyGroupID(t *testing.T) {
	state := newTestState(1)
	h := HandleLeaveGroup(state)

	r := kmsg.NewPtrLeaveGroupRequest()
	r.Group = ""
	resp, err := h(r)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	lResp := resp.(*kmsg.LeaveGroupResponse)
	if lResp.ErrorCode != kerr.InvalidGroupID.Code {
		t.Errorf("expected InvalidGroupID (%d), got %d", kerr.InvalidGroupID.Code, lResp.ErrorCode)
	}
}

func TestHandleLeaveGroup_GroupNotFound(t *testing.T) {
	state := newTestState(1)
	h := HandleLeaveGroup(state)

	r := kmsg.NewPtrLeaveGroupRequest()
	r.Group = "nonexistent"
	resp, err := h(r)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	lResp := resp.(*kmsg.LeaveGroupResponse)
	if lResp.ErrorCode != kerr.UnknownMemberID.Code {
		t.Errorf("expected UnknownMemberID (%d), got %d", kerr.UnknownMemberID.Code, lResp.ErrorCode)
	}
}

func TestHandleLeaveGroup_SendError(t *testing.T) {
	state, shutdownCh := newTestStateWithShutdown(t)
	state.GetOrCreateGroup("test-group")
	close(shutdownCh)

	h := HandleLeaveGroup(state)
	r := kmsg.NewPtrLeaveGroupRequest()
	r.Group = "test-group"
	resp, err := h(r)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	lResp := resp.(*kmsg.LeaveGroupResponse)
	if lResp.ErrorCode != kerr.CoordinatorNotAvailable.Code {
		t.Errorf("expected CoordinatorNotAvailable (%d), got %d",
			kerr.CoordinatorNotAvailable.Code, lResp.ErrorCode)
	}
}

// --- HandleOffsetCommit ---

func TestHandleOffsetCommit_EmptyGroupID(t *testing.T) {
	state := newTestState(1)
	h := HandleOffsetCommit(state)

	r := kmsg.NewPtrOffsetCommitRequest()
	r.Group = ""
	rt := kmsg.NewOffsetCommitRequestTopic()
	rt.Topic = "t1"
	rp := kmsg.NewOffsetCommitRequestTopicPartition()
	rp.Partition = 0
	rp.Offset = 10
	rt.Partitions = append(rt.Partitions, rp)
	r.Topics = append(r.Topics, rt)

	resp, err := h(r)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	ocResp := resp.(*kmsg.OffsetCommitResponse)
	if len(ocResp.Topics) != 1 {
		t.Fatalf("expected 1 topic in response, got %d", len(ocResp.Topics))
	}
	if ocResp.Topics[0].Topic != "t1" {
		t.Errorf("expected topic t1, got %q", ocResp.Topics[0].Topic)
	}
	if len(ocResp.Topics[0].Partitions) != 1 {
		t.Fatalf("expected 1 partition, got %d", len(ocResp.Topics[0].Partitions))
	}
	if ocResp.Topics[0].Partitions[0].ErrorCode != kerr.InvalidGroupID.Code {
		t.Errorf("expected InvalidGroupID (%d), got %d",
			kerr.InvalidGroupID.Code, ocResp.Topics[0].Partitions[0].ErrorCode)
	}
}

func TestHandleOffsetCommit_SendError(t *testing.T) {
	state, shutdownCh := newTestStateWithShutdown(t)
	// GetOrCreateGroup will create the group; then close shutdown to make Send fail
	state.GetOrCreateGroup("test-group")
	close(shutdownCh)

	h := HandleOffsetCommit(state)
	r := kmsg.NewPtrOffsetCommitRequest()
	r.Group = "test-group"
	rt := kmsg.NewOffsetCommitRequestTopic()
	rt.Topic = "t1"
	rp := kmsg.NewOffsetCommitRequestTopicPartition()
	rp.Partition = 0
	rp.Offset = 10
	rt.Partitions = append(rt.Partitions, rp)
	r.Topics = append(r.Topics, rt)

	resp, err := h(r)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	ocResp := resp.(*kmsg.OffsetCommitResponse)
	if len(ocResp.Topics) != 1 {
		t.Fatalf("expected 1 topic, got %d", len(ocResp.Topics))
	}
	if ocResp.Topics[0].Partitions[0].ErrorCode != kerr.CoordinatorNotAvailable.Code {
		t.Errorf("expected CoordinatorNotAvailable (%d), got %d",
			kerr.CoordinatorNotAvailable.Code, ocResp.Topics[0].Partitions[0].ErrorCode)
	}
}

// --- HandleOffsetFetch (legacy, v0-v7) ---

func TestHandleOffsetFetch_Legacy_EmptyGroupID(t *testing.T) {
	state := newTestState(1)
	h := HandleOffsetFetch(state)

	r := kmsg.NewPtrOffsetFetchRequest()
	r.Version = 7
	r.Group = ""

	resp, err := h(r)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	ofResp := resp.(*kmsg.OffsetFetchResponse)
	if ofResp.ErrorCode != kerr.InvalidGroupID.Code {
		t.Errorf("expected InvalidGroupID (%d), got %d",
			kerr.InvalidGroupID.Code, ofResp.ErrorCode)
	}
}

func TestHandleOffsetFetch_Legacy_GroupNotFound(t *testing.T) {
	state := newTestState(1)
	h := HandleOffsetFetch(state)

	r := kmsg.NewPtrOffsetFetchRequest()
	r.Version = 7
	r.Group = "nonexistent"
	rt := kmsg.NewOffsetFetchRequestTopic()
	rt.Topic = "t1"
	rt.Partitions = []int32{0, 1}
	r.Topics = append(r.Topics, rt)

	resp, err := h(r)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	ofResp := resp.(*kmsg.OffsetFetchResponse)
	// fillOffsetFetchNotFound populates topics with offset=-1
	if len(ofResp.Topics) != 1 {
		t.Fatalf("expected 1 topic, got %d", len(ofResp.Topics))
	}
	if ofResp.Topics[0].Topic != "t1" {
		t.Errorf("expected topic t1, got %q", ofResp.Topics[0].Topic)
	}
	if len(ofResp.Topics[0].Partitions) != 2 {
		t.Fatalf("expected 2 partitions, got %d", len(ofResp.Topics[0].Partitions))
	}
	for _, p := range ofResp.Topics[0].Partitions {
		if p.Offset != -1 {
			t.Errorf("partition %d: expected offset -1, got %d", p.Partition, p.Offset)
		}
		if p.LeaderEpoch != -1 {
			t.Errorf("partition %d: expected leader epoch -1, got %d", p.Partition, p.LeaderEpoch)
		}
	}
}

func TestHandleOffsetFetch_Legacy_SendError(t *testing.T) {
	state, shutdownCh := newTestStateWithShutdown(t)
	state.GetOrCreateGroup("test-group")
	close(shutdownCh)

	h := HandleOffsetFetch(state)
	r := kmsg.NewPtrOffsetFetchRequest()
	r.Version = 7
	r.Group = "test-group"

	resp, err := h(r)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	ofResp := resp.(*kmsg.OffsetFetchResponse)
	if ofResp.ErrorCode != kerr.CoordinatorNotAvailable.Code {
		t.Errorf("expected CoordinatorNotAvailable (%d), got %d",
			kerr.CoordinatorNotAvailable.Code, ofResp.ErrorCode)
	}
}

// --- HandleOffsetFetch (batched, v8+) ---

func TestHandleOffsetFetch_Batched_EmptyGroupID(t *testing.T) {
	state := newTestState(1)
	h := HandleOffsetFetch(state)

	r := kmsg.NewPtrOffsetFetchRequest()
	r.Version = 8
	rg := kmsg.NewOffsetFetchRequestGroup()
	rg.Group = ""
	r.Groups = append(r.Groups, rg)

	resp, err := h(r)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	ofResp := resp.(*kmsg.OffsetFetchResponse)
	if len(ofResp.Groups) != 1 {
		t.Fatalf("expected 1 group, got %d", len(ofResp.Groups))
	}
	if ofResp.Groups[0].ErrorCode != kerr.InvalidGroupID.Code {
		t.Errorf("expected InvalidGroupID (%d), got %d",
			kerr.InvalidGroupID.Code, ofResp.Groups[0].ErrorCode)
	}
}

func TestHandleOffsetFetch_Batched_GroupNotFound(t *testing.T) {
	state := newTestState(1)
	h := HandleOffsetFetch(state)

	r := kmsg.NewPtrOffsetFetchRequest()
	r.Version = 8
	rg := kmsg.NewOffsetFetchRequestGroup()
	rg.Group = "nonexistent"
	rt := kmsg.NewOffsetFetchRequestGroupTopic()
	rt.Topic = "t1"
	rt.Partitions = []int32{0, 2}
	rg.Topics = append(rg.Topics, rt)
	r.Groups = append(r.Groups, rg)

	resp, err := h(r)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	ofResp := resp.(*kmsg.OffsetFetchResponse)
	if len(ofResp.Groups) != 1 {
		t.Fatalf("expected 1 group, got %d", len(ofResp.Groups))
	}
	gResp := ofResp.Groups[0]
	if gResp.Group != "nonexistent" {
		t.Errorf("expected group nonexistent, got %q", gResp.Group)
	}
	// fillOffsetFetchGroupNotFound populates topics with offset=-1
	if len(gResp.Topics) != 1 {
		t.Fatalf("expected 1 topic, got %d", len(gResp.Topics))
	}
	if gResp.Topics[0].Topic != "t1" {
		t.Errorf("expected topic t1, got %q", gResp.Topics[0].Topic)
	}
	for _, p := range gResp.Topics[0].Partitions {
		if p.Offset != -1 {
			t.Errorf("partition %d: expected offset -1, got %d", p.Partition, p.Offset)
		}
		if p.LeaderEpoch != -1 {
			t.Errorf("partition %d: expected leader epoch -1, got %d", p.Partition, p.LeaderEpoch)
		}
	}
}

func TestHandleOffsetFetch_Batched_SendError(t *testing.T) {
	state, shutdownCh := newTestStateWithShutdown(t)
	state.GetOrCreateGroup("test-group")
	close(shutdownCh)

	h := HandleOffsetFetch(state)
	r := kmsg.NewPtrOffsetFetchRequest()
	r.Version = 8
	rg := kmsg.NewOffsetFetchRequestGroup()
	rg.Group = "test-group"
	r.Groups = append(r.Groups, rg)

	resp, err := h(r)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	ofResp := resp.(*kmsg.OffsetFetchResponse)
	if len(ofResp.Groups) != 1 {
		t.Fatalf("expected 1 group, got %d", len(ofResp.Groups))
	}
	if ofResp.Groups[0].ErrorCode != kerr.CoordinatorNotAvailable.Code {
		t.Errorf("expected CoordinatorNotAvailable (%d), got %d",
			kerr.CoordinatorNotAvailable.Code, ofResp.Groups[0].ErrorCode)
	}
}

func TestHandleOffsetFetch_Batched_MultipleGroups(t *testing.T) {
	state, shutdownCh := newTestStateWithShutdown(t)
	state.GetOrCreateGroup("group-a")
	close(shutdownCh)

	h := HandleOffsetFetch(state)
	r := kmsg.NewPtrOffsetFetchRequest()
	r.Version = 8

	// Group with empty ID
	rg1 := kmsg.NewOffsetFetchRequestGroup()
	rg1.Group = ""
	r.Groups = append(r.Groups, rg1)

	// Group that doesn't exist
	rg2 := kmsg.NewOffsetFetchRequestGroup()
	rg2.Group = "nonexistent"
	rt := kmsg.NewOffsetFetchRequestGroupTopic()
	rt.Topic = "t1"
	rt.Partitions = []int32{0}
	rg2.Topics = append(rg2.Topics, rt)
	r.Groups = append(r.Groups, rg2)

	// Group that exists but Send fails
	rg3 := kmsg.NewOffsetFetchRequestGroup()
	rg3.Group = "group-a"
	r.Groups = append(r.Groups, rg3)

	resp, err := h(r)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	ofResp := resp.(*kmsg.OffsetFetchResponse)
	if len(ofResp.Groups) != 3 {
		t.Fatalf("expected 3 groups, got %d", len(ofResp.Groups))
	}

	// First: empty group → InvalidGroupID
	if ofResp.Groups[0].ErrorCode != kerr.InvalidGroupID.Code {
		t.Errorf("group 0: expected InvalidGroupID (%d), got %d",
			kerr.InvalidGroupID.Code, ofResp.Groups[0].ErrorCode)
	}
	// Second: nonexistent → fills not-found offsets
	if ofResp.Groups[1].ErrorCode != 0 {
		t.Errorf("group 1: expected no error code, got %d", ofResp.Groups[1].ErrorCode)
	}
	if len(ofResp.Groups[1].Topics) != 1 {
		t.Fatalf("group 1: expected 1 topic, got %d", len(ofResp.Groups[1].Topics))
	}
	if ofResp.Groups[1].Topics[0].Partitions[0].Offset != -1 {
		t.Errorf("group 1: expected offset -1, got %d", ofResp.Groups[1].Topics[0].Partitions[0].Offset)
	}
	// Third: Send error → CoordinatorNotAvailable
	if ofResp.Groups[2].ErrorCode != kerr.CoordinatorNotAvailable.Code {
		t.Errorf("group 2: expected CoordinatorNotAvailable (%d), got %d",
			kerr.CoordinatorNotAvailable.Code, ofResp.Groups[2].ErrorCode)
	}
}
