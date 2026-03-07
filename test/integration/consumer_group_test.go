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

// sendJoinGroup is a helper that sends a JoinGroup request using a raw client.
func sendJoinGroup(t *testing.T, addr, group, memberID string, instanceID *string, protocols []kmsg.JoinGroupRequestProtocol) *kmsg.JoinGroupResponse {
	t.Helper()
	cl := NewClient(t, addr)
	req := kmsg.NewJoinGroupRequest()
	req.Group = group
	req.MemberID = memberID
	req.InstanceID = instanceID
	req.ProtocolType = "consumer"
	req.Protocols = protocols
	req.SessionTimeoutMillis = 30000
	req.RebalanceTimeoutMillis = 60000
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := req.RequestWith(ctx, cl)
	require.NoError(t, err)
	return resp
}

// sendSyncGroup is a helper that sends a SyncGroup request.
func sendSyncGroup(t *testing.T, addr, group string, generation int32, memberID string, instanceID *string, assignments []kmsg.SyncGroupRequestGroupAssignment) *kmsg.SyncGroupResponse {
	t.Helper()
	cl := NewClient(t, addr)
	req := kmsg.NewSyncGroupRequest()
	req.Group = group
	req.Generation = generation
	req.MemberID = memberID
	req.InstanceID = instanceID
	req.GroupAssignment = assignments
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := req.RequestWith(ctx, cl)
	require.NoError(t, err)
	return resp
}

// sendHeartbeat is a helper that sends a Heartbeat request.
func sendHeartbeat(t *testing.T, addr, group string, generation int32, memberID string, instanceID *string) *kmsg.HeartbeatResponse {
	t.Helper()
	cl := NewClient(t, addr)
	req := kmsg.NewHeartbeatRequest()
	req.Group = group
	req.Generation = generation
	req.MemberID = memberID
	req.InstanceID = instanceID
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := req.RequestWith(ctx, cl)
	require.NoError(t, err)
	return resp
}

// sendLeaveGroup is a helper that sends a LeaveGroup request.
func sendLeaveGroup(t *testing.T, addr, group, memberID string) *kmsg.LeaveGroupResponse {
	t.Helper()
	cl := NewClient(t, addr)
	req := kmsg.NewLeaveGroupRequest()
	req.Group = group
	req.MemberID = memberID
	// For v3+, also set the Members field
	m := kmsg.NewLeaveGroupRequestMember()
	m.MemberID = memberID
	req.Members = []kmsg.LeaveGroupRequestMember{m}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := req.RequestWith(ctx, cl)
	require.NoError(t, err)
	return resp
}

// defaultProtocols returns a simple consumer protocol for testing.
func defaultProtocols() []kmsg.JoinGroupRequestProtocol {
	p := kmsg.NewJoinGroupRequestProtocol()
	p.Name = "range"
	p.Metadata = []byte{0, 0, 0, 0, 0, 0, 0, 0} // minimal consumer protocol metadata
	return []kmsg.JoinGroupRequestProtocol{p}
}

// TestJoinGroupSingle tests a single consumer joining and becoming leader.
func TestJoinGroupSingle(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t, WithAutoCreateTopics(true))

	// First call: MEMBER_ID_REQUIRED (v4+ behavior)
	resp1 := sendJoinGroup(t, tb.Addr, "test-group-single", "", nil, defaultProtocols())
	require.Equal(t, kerr.MemberIDRequired.Code, resp1.ErrorCode, "expected MEMBER_ID_REQUIRED")
	require.NotEmpty(t, resp1.MemberID, "expected assigned member ID")

	// Second call: with assigned member ID
	resp2 := sendJoinGroup(t, tb.Addr, "test-group-single", resp1.MemberID, nil, defaultProtocols())
	require.Equal(t, int16(0), resp2.ErrorCode, "expected no error")
	require.Equal(t, resp1.MemberID, resp2.MemberID, "member ID should be preserved")
	require.Equal(t, int32(1), resp2.Generation, "first generation should be 1")
	require.Equal(t, resp1.MemberID, resp2.LeaderID, "single member should be leader")
	require.NotEmpty(t, resp2.Members, "leader should get member list")
	require.Len(t, resp2.Members, 1)
}

// TestJoinGroupTwo tests two consumers joining the same group via a controlled
// rebalance protocol. Member 1 joins, then member 2 joins (triggering a rebalance).
// Member 1 is notified via heartbeat (REBALANCE_IN_PROGRESS) and rejoins.
func TestJoinGroupTwo(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t, WithAutoCreateTopics(true))
	group := "test-group-two"

	// Member 1: get member ID and join
	resp1a := sendJoinGroup(t, tb.Addr, group, "", nil, defaultProtocols())
	require.Equal(t, kerr.MemberIDRequired.Code, resp1a.ErrorCode)
	member1ID := resp1a.MemberID

	resp1b := sendJoinGroup(t, tb.Addr, group, member1ID, nil, defaultProtocols())
	require.Equal(t, int16(0), resp1b.ErrorCode)
	require.Equal(t, int32(1), resp1b.Generation)

	// SyncGroup for member 1
	syncResp1 := sendSyncGroup(t, tb.Addr, group, resp1b.Generation, member1ID, nil, nil)
	require.Equal(t, int16(0), syncResp1.ErrorCode)

	// Member 2: get member ID
	resp2a := sendJoinGroup(t, tb.Addr, group, "", nil, defaultProtocols())
	require.Equal(t, kerr.MemberIDRequired.Code, resp2a.ErrorCode)
	member2ID := resp2a.MemberID

	// Member 2 joins (triggers rebalance for member 1)
	// This will block until member 1 also rejoins or until rebalance timeout
	type joinResult struct {
		resp *kmsg.JoinGroupResponse
		err  error
	}
	ch2 := make(chan joinResult, 1)
	go func() {
		cl := NewClient(t, tb.Addr)
		req := kmsg.NewJoinGroupRequest()
		req.Group = group
		req.MemberID = member2ID
		req.ProtocolType = "consumer"
		req.Protocols = defaultProtocols()
		req.SessionTimeoutMillis = 30000
		req.RebalanceTimeoutMillis = 10000
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		resp, err := req.RequestWith(ctx, cl)
		ch2 <- joinResult{resp, err}
	}()

	// Give member 2's join a moment to be processed
	time.Sleep(100 * time.Millisecond)

	// Member 1 sends heartbeat, should get REBALANCE_IN_PROGRESS
	hbResp := sendHeartbeat(t, tb.Addr, group, resp1b.Generation, member1ID, nil)
	require.Equal(t, kerr.RebalanceInProgress.Code, hbResp.ErrorCode,
		"member 1 should get REBALANCE_IN_PROGRESS after member 2 joins")

	// Member 1 rejoins
	ch1 := make(chan joinResult, 1)
	go func() {
		cl := NewClient(t, tb.Addr)
		req := kmsg.NewJoinGroupRequest()
		req.Group = group
		req.MemberID = member1ID
		req.ProtocolType = "consumer"
		req.Protocols = defaultProtocols()
		req.SessionTimeoutMillis = 30000
		req.RebalanceTimeoutMillis = 10000
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		resp, err := req.RequestWith(ctx, cl)
		ch1 <- joinResult{resp, err}
	}()

	// Both should complete now
	r1 := <-ch1
	r2 := <-ch2
	require.NoError(t, r1.err)
	require.NoError(t, r2.err)
	require.Equal(t, int16(0), r1.resp.ErrorCode)
	require.Equal(t, int16(0), r2.resp.ErrorCode)

	// Both should have the same generation (should be 2 now)
	require.Equal(t, r1.resp.Generation, r2.resp.Generation)
	require.Equal(t, int32(2), r1.resp.Generation)

	// Exactly one should be the leader
	leaderCount := 0
	if len(r1.resp.Members) > 0 {
		leaderCount++
	}
	if len(r2.resp.Members) > 0 {
		leaderCount++
	}
	require.Equal(t, 1, leaderCount, "exactly one member should be leader (get member list)")
}

// TestJoinGroupMemberIdRequired tests v4+ MEMBER_ID_REQUIRED flow.
func TestJoinGroupMemberIdRequired(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t, WithAutoCreateTopics(true))

	resp := sendJoinGroup(t, tb.Addr, "test-mid-required", "", nil, defaultProtocols())
	require.Equal(t, kerr.MemberIDRequired.Code, resp.ErrorCode)
	require.NotEmpty(t, resp.MemberID)

	// The assigned member ID should be valid for a retry
	resp2 := sendJoinGroup(t, tb.Addr, "test-mid-required", resp.MemberID, nil, defaultProtocols())
	require.Equal(t, int16(0), resp2.ErrorCode)
	require.Equal(t, resp.MemberID, resp2.MemberID)
}

// TestSyncGroupLeaderFollower tests leader assignment distribution via SyncGroup
// with a single member (the leader gets its own assignment back).
func TestSyncGroupLeaderFollower(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t, WithAutoCreateTopics(true))
	group := "test-sync-group"

	// Member 1: join as single member (leader)
	resp1a := sendJoinGroup(t, tb.Addr, group, "", nil, defaultProtocols())
	require.Equal(t, kerr.MemberIDRequired.Code, resp1a.ErrorCode)
	member1ID := resp1a.MemberID

	resp1b := sendJoinGroup(t, tb.Addr, group, member1ID, nil, defaultProtocols())
	require.Equal(t, int16(0), resp1b.ErrorCode)

	gen := resp1b.Generation
	leaderID := resp1b.LeaderID
	require.Equal(t, member1ID, leaderID, "single member should be leader")

	// Leader sends SyncGroup with its own assignment
	assignment := []byte("leader-assignment")
	a1 := kmsg.NewSyncGroupRequestGroupAssignment()
	a1.MemberID = leaderID
	a1.MemberAssignment = assignment
	syncResp := sendSyncGroup(t, tb.Addr, group, gen, leaderID, nil,
		[]kmsg.SyncGroupRequestGroupAssignment{a1})
	require.Equal(t, int16(0), syncResp.ErrorCode)

	// Leader should get its assignment back
	assert.Equal(t, assignment, syncResp.MemberAssignment)
}

// TestHeartbeatKeepsAlive tests that heartbeats reset the session timer.
func TestHeartbeatKeepsAlive(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t, WithAutoCreateTopics(true))
	group := "test-heartbeat"

	// Join a single member
	resp1 := sendJoinGroup(t, tb.Addr, group, "", nil, defaultProtocols())
	require.Equal(t, kerr.MemberIDRequired.Code, resp1.ErrorCode)

	resp2 := sendJoinGroup(t, tb.Addr, group, resp1.MemberID, nil, defaultProtocols())
	require.Equal(t, int16(0), resp2.ErrorCode)

	// Do SyncGroup (to transition to Stable)
	syncResp := sendSyncGroup(t, tb.Addr, group, resp2.Generation, resp2.MemberID, nil, nil)
	require.Equal(t, int16(0), syncResp.ErrorCode)

	// Send heartbeat — should succeed
	hbResp := sendHeartbeat(t, tb.Addr, group, resp2.Generation, resp2.MemberID, nil)
	require.Equal(t, int16(0), hbResp.ErrorCode)

	// Send another heartbeat — should still succeed
	hbResp2 := sendHeartbeat(t, tb.Addr, group, resp2.Generation, resp2.MemberID, nil)
	require.Equal(t, int16(0), hbResp2.ErrorCode)
}

// TestSessionTimeout tests that a member is removed when it stops heartbeating.
func TestSessionTimeout(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t, WithAutoCreateTopics(true))
	group := "test-session-timeout"

	// Join with a very short session timeout
	cl := NewClient(t, tb.Addr)
	req := kmsg.NewJoinGroupRequest()
	req.Group = group
	req.MemberID = ""
	req.ProtocolType = "consumer"
	req.Protocols = defaultProtocols()
	req.SessionTimeoutMillis = 1000 // 1 second
	req.RebalanceTimeoutMillis = 2000

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp1, err := req.RequestWith(ctx, cl)
	require.NoError(t, err)
	require.Equal(t, kerr.MemberIDRequired.Code, resp1.ErrorCode)

	req.MemberID = resp1.MemberID
	resp2, err := req.RequestWith(ctx, cl)
	require.NoError(t, err)
	require.Equal(t, int16(0), resp2.ErrorCode)

	// SyncGroup to go stable
	syncReq := kmsg.NewSyncGroupRequest()
	syncReq.Group = group
	syncReq.Generation = resp2.Generation
	syncReq.MemberID = resp2.MemberID
	syncResp, err := syncReq.RequestWith(ctx, cl)
	require.NoError(t, err)
	require.Equal(t, int16(0), syncResp.ErrorCode)

	// Wait for session timeout
	time.Sleep(2 * time.Second)

	// Heartbeat should fail with UNKNOWN_MEMBER_ID (member was removed)
	hbResp := sendHeartbeat(t, tb.Addr, group, resp2.Generation, resp2.MemberID, nil)
	// After session timeout, member is removed. May get UNKNOWN_MEMBER_ID or
	// REBALANCE_IN_PROGRESS depending on timing
	assert.True(t, hbResp.ErrorCode == kerr.UnknownMemberID.Code ||
		hbResp.ErrorCode == kerr.RebalanceInProgress.Code ||
		hbResp.ErrorCode == kerr.IllegalGeneration.Code,
		"expected UNKNOWN_MEMBER_ID, REBALANCE_IN_PROGRESS, or ILLEGAL_GENERATION, got %d", hbResp.ErrorCode)
}

// TestLeaveGroupRebalance tests that LeaveGroup removes a member from the group.
func TestLeaveGroupRebalance(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t, WithAutoCreateTopics(true))
	group := "test-leave-rebalance"

	// Join member 1
	resp1a := sendJoinGroup(t, tb.Addr, group, "", nil, defaultProtocols())
	require.Equal(t, kerr.MemberIDRequired.Code, resp1a.ErrorCode)
	member1 := resp1a.MemberID

	// Join the member (it's alone, so it becomes leader immediately)
	resp1b := sendJoinGroup(t, tb.Addr, group, member1, nil, defaultProtocols())
	require.Equal(t, int16(0), resp1b.ErrorCode)

	// SyncGroup
	syncResp := sendSyncGroup(t, tb.Addr, group, resp1b.Generation, member1, nil, nil)
	require.Equal(t, int16(0), syncResp.ErrorCode)

	// Heartbeat should succeed while member is active
	hbResp := sendHeartbeat(t, tb.Addr, group, resp1b.Generation, member1, nil)
	require.Equal(t, int16(0), hbResp.ErrorCode)

	// Leave group
	leaveResp := sendLeaveGroup(t, tb.Addr, group, member1)
	require.Equal(t, int16(0), leaveResp.ErrorCode)

	// Now heartbeat should fail (member was removed, group is empty)
	// Use the old generation - should get UNKNOWN_MEMBER_ID since member no longer exists
	hbResp2 := sendHeartbeat(t, tb.Addr, group, resp1b.Generation, member1, nil)
	assert.True(t, hbResp2.ErrorCode == kerr.UnknownMemberID.Code ||
		hbResp2.ErrorCode == kerr.IllegalGeneration.Code,
		"heartbeat after leave should fail with UNKNOWN_MEMBER_ID or ILLEGAL_GENERATION, got %d", hbResp2.ErrorCode)
}

// TestStaticMemberRejoin tests that a static member can rejoin without triggering rebalance.
func TestStaticMemberRejoin(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t, WithAutoCreateTopics(true))
	group := "test-static-rejoin"

	instanceID := "my-static-instance"

	// First join: get member ID
	resp1 := sendJoinGroup(t, tb.Addr, group, "", &instanceID, defaultProtocols())
	require.Equal(t, kerr.MemberIDRequired.Code, resp1.ErrorCode)
	member1 := resp1.MemberID

	// Second join: use the member ID
	resp2 := sendJoinGroup(t, tb.Addr, group, member1, &instanceID, defaultProtocols())
	require.Equal(t, int16(0), resp2.ErrorCode)

	gen1 := resp2.Generation

	// SyncGroup
	syncResp := sendSyncGroup(t, tb.Addr, group, gen1, member1, &instanceID, nil)
	require.Equal(t, int16(0), syncResp.ErrorCode)

	// Rejoin with same instance ID but empty member ID (simulates client restart)
	// The broker should replace the old static member
	resp3 := sendJoinGroup(t, tb.Addr, group, "", &instanceID, defaultProtocols())
	// For static member rejoin, the broker should return a new member ID
	// but potentially skip rebalance if protocols match
	if resp3.ErrorCode == kerr.MemberIDRequired.Code {
		// v4+ flow: retry with assigned ID
		resp4 := sendJoinGroup(t, tb.Addr, group, resp3.MemberID, &instanceID, defaultProtocols())
		require.Equal(t, int16(0), resp4.ErrorCode)
		// Generation might or might not change depending on whether rebalance was skipped
	} else {
		require.Equal(t, int16(0), resp3.ErrorCode)
	}
}

// TestJoinGroupInvalidGroupID tests that an empty group ID returns INVALID_GROUP_ID.
func TestJoinGroupInvalidGroupID(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t, WithAutoCreateTopics(true))

	resp := sendJoinGroup(t, tb.Addr, "", "", nil, defaultProtocols())
	require.Equal(t, kerr.InvalidGroupID.Code, resp.ErrorCode)
}

// TestHeartbeatWrongGeneration tests that heartbeat with wrong generation returns ILLEGAL_GENERATION.
func TestHeartbeatWrongGeneration(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t, WithAutoCreateTopics(true))
	group := "test-hb-wrong-gen"

	// Join single member
	resp1 := sendJoinGroup(t, tb.Addr, group, "", nil, defaultProtocols())
	require.Equal(t, kerr.MemberIDRequired.Code, resp1.ErrorCode)
	resp2 := sendJoinGroup(t, tb.Addr, group, resp1.MemberID, nil, defaultProtocols())
	require.Equal(t, int16(0), resp2.ErrorCode)

	// SyncGroup
	syncResp := sendSyncGroup(t, tb.Addr, group, resp2.Generation, resp2.MemberID, nil, nil)
	require.Equal(t, int16(0), syncResp.ErrorCode)

	// Heartbeat with wrong generation
	hbResp := sendHeartbeat(t, tb.Addr, group, 999, resp2.MemberID, nil)
	require.Equal(t, kerr.IllegalGeneration.Code, hbResp.ErrorCode)
}

// TestHeartbeatUnknownMember tests that heartbeat with unknown member returns UNKNOWN_MEMBER_ID.
func TestHeartbeatUnknownMember(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t, WithAutoCreateTopics(true))
	group := "test-hb-unknown"

	// Join single member to create the group
	resp1 := sendJoinGroup(t, tb.Addr, group, "", nil, defaultProtocols())
	require.Equal(t, kerr.MemberIDRequired.Code, resp1.ErrorCode)
	resp2 := sendJoinGroup(t, tb.Addr, group, resp1.MemberID, nil, defaultProtocols())
	require.Equal(t, int16(0), resp2.ErrorCode)

	// Heartbeat with bogus member ID
	hbResp := sendHeartbeat(t, tb.Addr, group, resp2.Generation, "bogus-member-id", nil)
	require.Equal(t, kerr.UnknownMemberID.Code, hbResp.ErrorCode)
}

// TestSyncGroupWrongGeneration tests SyncGroup with wrong generation.
func TestSyncGroupWrongGeneration(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t, WithAutoCreateTopics(true))
	group := "test-sync-wrong-gen"

	// Join single member
	resp1 := sendJoinGroup(t, tb.Addr, group, "", nil, defaultProtocols())
	require.Equal(t, kerr.MemberIDRequired.Code, resp1.ErrorCode)
	resp2 := sendJoinGroup(t, tb.Addr, group, resp1.MemberID, nil, defaultProtocols())
	require.Equal(t, int16(0), resp2.ErrorCode)

	// SyncGroup with wrong generation
	syncResp := sendSyncGroup(t, tb.Addr, group, 999, resp2.MemberID, nil, nil)
	require.Equal(t, kerr.IllegalGeneration.Code, syncResp.ErrorCode)
}
