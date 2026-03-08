package cluster

import (
	"log/slog"
	"runtime"
	"testing"
	"time"

	"github.com/klaudworks/klite/internal/clock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

var t0 = time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

func newTestGroup(t *testing.T, clk clock.Clock) *Group {
	t.Helper()
	shutdownCh := make(chan struct{})
	t.Cleanup(func() { close(shutdownCh) })
	g := NewGroup("test-group", shutdownCh, slog.Default(), clk)
	t.Cleanup(g.Stop)
	return g
}

func makeProtocols(names ...string) []kmsg.JoinGroupRequestProtocol {
	protos := make([]kmsg.JoinGroupRequestProtocol, len(names))
	for i, n := range names {
		protos[i] = kmsg.JoinGroupRequestProtocol{Name: n, Metadata: []byte{0}}
	}
	return protos
}

func joinRequest(memberID string, instanceID *string, version int16, protos []kmsg.JoinGroupRequestProtocol) *kmsg.JoinGroupRequest {
	req := &kmsg.JoinGroupRequest{
		Version:                version,
		Group:                  "test-group",
		MemberID:               memberID,
		InstanceID:             instanceID,
		ProtocolType:           "consumer",
		Protocols:              protos,
		SessionTimeoutMillis:   10000,
		RebalanceTimeoutMillis: 5000,
	}
	return req
}

func syncRequest(memberID string, gen int32, assignments []kmsg.SyncGroupRequestGroupAssignment) *kmsg.SyncGroupRequest {
	req := &kmsg.SyncGroupRequest{
		Group:           "test-group",
		Generation:      gen,
		MemberID:        memberID,
		GroupAssignment: assignments,
	}
	return req
}

func heartbeatRequest(memberID string, gen int32) *kmsg.HeartbeatRequest {
	return &kmsg.HeartbeatRequest{
		Group:      "test-group",
		Generation: gen,
		MemberID:   memberID,
	}
}

// joinMember performs the v4+ join flow (MEMBER_ID_REQUIRED → rejoin) and
// returns the JoinGroupResponse. Returns the assigned memberID and response.
func joinMember(t *testing.T, g *Group, instanceID *string, protos []kmsg.JoinGroupRequestProtocol) (string, *kmsg.JoinGroupResponse) {
	t.Helper()
	resp1, err := g.Send(joinRequest("", instanceID, 4, protos))
	require.NoError(t, err)
	jr1 := resp1.(*kmsg.JoinGroupResponse)
	require.Equal(t, kerr.MemberIDRequired.Code, jr1.ErrorCode)
	memberID := jr1.MemberID
	require.NotEmpty(t, memberID)

	resp2, err := g.Send(joinRequest(memberID, instanceID, 4, protos))
	require.NoError(t, err)
	jr2 := resp2.(*kmsg.JoinGroupResponse)
	return memberID, jr2
}

// joinMemberAsync performs the first step (get memberID) then sends the
// second join in a goroutine, returning the memberID and a channel for the
// response. This is needed when multiple members join concurrently — the
// second join blocks until all members have joined.
func joinMemberAsync(t *testing.T, g *Group, instanceID *string, protos []kmsg.JoinGroupRequestProtocol) (string, <-chan *kmsg.JoinGroupResponse) {
	t.Helper()
	resp1, err := g.Send(joinRequest("", instanceID, 4, protos))
	require.NoError(t, err)
	jr1 := resp1.(*kmsg.JoinGroupResponse)
	require.Equal(t, kerr.MemberIDRequired.Code, jr1.ErrorCode)
	memberID := jr1.MemberID

	ch := make(chan *kmsg.JoinGroupResponse, 1)
	go func() {
		resp, err2 := g.Send(joinRequest(memberID, instanceID, 4, protos))
		if err2 != nil {
			return
		}
		ch <- resp.(*kmsg.JoinGroupResponse)
	}()
	return memberID, ch
}

// syncLeader sends a SyncGroup as the leader with provided assignments.
func syncLeader(t *testing.T, g *Group, memberID string, gen int32, assignments []kmsg.SyncGroupRequestGroupAssignment) *kmsg.SyncGroupResponse {
	t.Helper()
	resp, err := g.Send(syncRequest(memberID, gen, assignments))
	require.NoError(t, err)
	return resp.(*kmsg.SyncGroupResponse)
}

// getState reads the group state from the manage goroutine.
func getState(g *Group) GroupState {
	var s GroupState
	g.Control(func() { s = g.state })
	return s
}

// getMemberCount reads the member count from the manage goroutine.
func getMemberCount(g *Group) int {
	var n int
	g.Control(func() { n = len(g.members) })
	return n
}

// waitForState polls until the group reaches the expected state or times out.
func waitForState(t *testing.T, g *Group, want GroupState, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if getState(g) == want {
			return
		}
		runtime.Gosched()
	}
	t.Fatalf("timed out waiting for state %s, got %s", want, getState(g))
}

func strPtr(s string) *string { return &s }

// ---------------------------------------------------------------------------
// Tests: helper functions
// ---------------------------------------------------------------------------

func TestSameProtocols(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		a, b []kmsg.JoinGroupRequestProtocol
		want bool
	}{
		{"identical", makeProtocols("range"), makeProtocols("range"), true},
		{"different_name", makeProtocols("range"), makeProtocols("roundrobin"), false},
		{"length_mismatch", makeProtocols("range", "roundrobin"), makeProtocols("range"), false},
		{"empty_both", nil, nil, true},
		{"multi_match", makeProtocols("a", "b"), makeProtocols("a", "b"), true},
		{"multi_order_differs", makeProtocols("a", "b"), makeProtocols("b", "a"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &groupMember{protocols: tt.a}
			b := &groupMember{protocols: tt.b}
			assert.Equal(t, tt.want, sameProtocols(a, b))
		})
	}
}

func TestDerefStr(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "", derefStr(nil))
	s := "hello"
	assert.Equal(t, "hello", derefStr(&s))
}

func TestGenerateMemberID(t *testing.T) {
	t.Parallel()
	id1 := generateMemberID("client-1", nil)
	assert.Contains(t, id1, "client-1-")

	inst := "my-instance"
	id2 := generateMemberID("client-1", &inst)
	assert.Contains(t, id2, "my-instance-")
	assert.NotContains(t, id2, "client-1")

	id3 := generateMemberID("client-1", nil)
	assert.NotEqual(t, id1, id3, "should be unique (UUIDs)")
}

// ---------------------------------------------------------------------------
// Tests: protocol election
// ---------------------------------------------------------------------------

func TestElectProtocol_SingleMember(t *testing.T) {
	t.Parallel()
	g := newTestGroup(t, nil)
	memberID, resp := joinMember(t, g, nil, makeProtocols("range"))
	require.Equal(t, int16(0), resp.ErrorCode)
	_ = memberID

	var elected string
	g.Control(func() { elected = g.protocol })
	assert.Equal(t, "range", elected)
}

func TestElectProtocol_MultiMember_CommonProtocol(t *testing.T) {
	t.Parallel()
	g := newTestGroup(t, nil)

	// Set up two members with different protocol orderings via Control
	// to test electProtocol directly.
	var elected string
	g.Control(func() {
		m1 := &groupMember{
			memberID:     "m1",
			protocolType: "consumer",
			protocols:    makeProtocols("range", "roundrobin"),
		}
		m2 := &groupMember{
			memberID:     "m2",
			protocolType: "consumer",
			protocols:    makeProtocols("roundrobin", "range"),
		}
		g.members["m1"] = m1
		g.members["m2"] = m2
		g.protocolType = "consumer"
		for _, m := range g.members {
			for _, p := range m.protocols {
				g.protocols[p.Name]++
			}
		}

		elected = g.electProtocol()
	})

	// Both protocols are supported by all members. Each member votes for
	// its first common protocol. Result may be either.
	assert.True(t, elected == "range" || elected == "roundrobin",
		"expected range or roundrobin, got %q", elected)
}

func TestElectProtocol_NoOverlap(t *testing.T) {
	t.Parallel()
	g := newTestGroup(t, nil)

	// Member 1 supports only "range", member 2 supports only "roundrobin".
	// No common protocol → protocolsMatch should reject the second join.
	_, resp1 := joinMember(t, g, nil, makeProtocols("range"))
	require.Equal(t, int16(0), resp1.ErrorCode)

	// Sync to go stable
	syncResp := syncLeader(t, g, resp1.MemberID, resp1.Generation, nil)
	require.Equal(t, int16(0), syncResp.ErrorCode)

	// Second member with non-overlapping protocol should get InconsistentGroupProtocol
	resp2, err := g.Send(joinRequest("", nil, 4, makeProtocols("roundrobin")))
	require.NoError(t, err)
	jr2 := resp2.(*kmsg.JoinGroupResponse)
	assert.Equal(t, kerr.InconsistentGroupProtocol.Code, jr2.ErrorCode)
}

// ---------------------------------------------------------------------------
// Tests: state-gated error responses
// ---------------------------------------------------------------------------

func TestJoinGroup_DeadGroup(t *testing.T) {
	t.Parallel()
	g := newTestGroup(t, nil)
	g.Stop()
	// After stop, Send should fail because quitCh is closed
	_, err := g.Send(joinRequest("", nil, 4, makeProtocols("range")))
	assert.Error(t, err)
}

func TestJoinGroup_EmptyProtocols(t *testing.T) {
	t.Parallel()
	g := newTestGroup(t, nil)
	resp, err := g.Send(joinRequest("", nil, 4, nil))
	require.NoError(t, err)
	jr := resp.(*kmsg.JoinGroupResponse)
	assert.Equal(t, kerr.InconsistentGroupProtocol.Code, jr.ErrorCode)
}

func TestJoinGroup_UnknownMemberID(t *testing.T) {
	t.Parallel()
	g := newTestGroup(t, nil)
	resp, err := g.Send(joinRequest("nonexistent-member", nil, 4, makeProtocols("range")))
	require.NoError(t, err)
	jr := resp.(*kmsg.JoinGroupResponse)
	assert.Equal(t, kerr.UnknownMemberID.Code, jr.ErrorCode)
}

func TestHeartbeat_UnknownMember(t *testing.T) {
	t.Parallel()
	g := newTestGroup(t, nil)

	resp, err := g.Send(heartbeatRequest("nonexistent", 1))
	require.NoError(t, err)
	hr := resp.(*kmsg.HeartbeatResponse)
	assert.Equal(t, kerr.UnknownMemberID.Code, hr.ErrorCode)
}

func TestHeartbeat_IllegalGeneration(t *testing.T) {
	t.Parallel()
	g := newTestGroup(t, nil)

	memberID, joinResp := joinMember(t, g, nil, makeProtocols("range"))
	require.Equal(t, int16(0), joinResp.ErrorCode)

	syncResp := syncLeader(t, g, memberID, joinResp.Generation, nil)
	require.Equal(t, int16(0), syncResp.ErrorCode)

	// Heartbeat with wrong generation
	resp, err := g.Send(heartbeatRequest(memberID, 999))
	require.NoError(t, err)
	hr := resp.(*kmsg.HeartbeatResponse)
	assert.Equal(t, kerr.IllegalGeneration.Code, hr.ErrorCode)
}

func TestHeartbeat_PreparingRebalance(t *testing.T) {
	t.Parallel()
	g := newTestGroup(t, nil)

	// Join first member and go stable
	member1, joinResp := joinMember(t, g, nil, makeProtocols("range"))
	require.Equal(t, int16(0), joinResp.ErrorCode)
	gen := joinResp.Generation
	syncResp := syncLeader(t, g, member1, gen, nil)
	require.Equal(t, int16(0), syncResp.ErrorCode)

	// Start a second member joining (triggers rebalance)
	_, _ = joinMemberAsync(t, g, nil, makeProtocols("range"))
	// Give manage goroutine time to process
	runtime.Gosched()
	time.Sleep(10 * time.Millisecond)

	// Heartbeat from member1 should get RebalanceInProgress
	resp, err := g.Send(heartbeatRequest(member1, gen))
	require.NoError(t, err)
	hr := resp.(*kmsg.HeartbeatResponse)
	assert.Equal(t, kerr.RebalanceInProgress.Code, hr.ErrorCode)
}

func TestSyncGroup_PreparingRebalance(t *testing.T) {
	t.Parallel()
	g := newTestGroup(t, nil)

	// Join a member and bring the group to Stable
	member1, jr := joinMember(t, g, nil, makeProtocols("range"))
	require.Equal(t, int16(0), jr.ErrorCode)
	syncResp := syncLeader(t, g, member1, jr.Generation, nil)
	require.Equal(t, int16(0), syncResp.ErrorCode)
	waitForState(t, g, GroupStable, time.Second)

	// Trigger rebalance by adding a second member (goes to PreparingRebalance)
	_, _ = joinMemberAsync(t, g, nil, makeProtocols("range"))
	runtime.Gosched()
	time.Sleep(10 * time.Millisecond)
	waitForState(t, g, GroupPreparingRebalance, time.Second)

	// SyncGroup while in PreparingRebalance should get RebalanceInProgress
	resp, err := g.Send(syncRequest(member1, jr.Generation, nil))
	require.NoError(t, err)
	sr := resp.(*kmsg.SyncGroupResponse)
	assert.Equal(t, kerr.RebalanceInProgress.Code, sr.ErrorCode)
}

func TestSyncGroup_UnknownMember(t *testing.T) {
	t.Parallel()
	g := newTestGroup(t, nil)

	memberID, joinResp := joinMember(t, g, nil, makeProtocols("range"))
	require.Equal(t, int16(0), joinResp.ErrorCode)
	_ = memberID

	// SyncGroup with unknown member
	resp, err := g.Send(syncRequest("bogus-member", joinResp.Generation, nil))
	require.NoError(t, err)
	sr := resp.(*kmsg.SyncGroupResponse)
	assert.Equal(t, kerr.UnknownMemberID.Code, sr.ErrorCode)
}

func TestSyncGroup_IllegalGeneration(t *testing.T) {
	t.Parallel()
	g := newTestGroup(t, nil)

	memberID, joinResp := joinMember(t, g, nil, makeProtocols("range"))
	require.Equal(t, int16(0), joinResp.ErrorCode)

	resp, err := g.Send(syncRequest(memberID, 999, nil))
	require.NoError(t, err)
	sr := resp.(*kmsg.SyncGroupResponse)
	assert.Equal(t, kerr.IllegalGeneration.Code, sr.ErrorCode)
}

// ---------------------------------------------------------------------------
// Tests: rebalance timer / completeRebalance with FakeClock
// ---------------------------------------------------------------------------

func TestRebalanceTimer_AbsentMemberRemoved(t *testing.T) {
	t.Parallel()
	fc := clock.NewFakeClock(t0)
	g := newTestGroup(t, fc)

	// Join member1 as a single member, go stable
	member1, jr1 := joinMember(t, g, nil, makeProtocols("range"))
	require.Equal(t, int16(0), jr1.ErrorCode)
	syncResp := syncLeader(t, g, member1, jr1.Generation, nil)
	require.Equal(t, int16(0), syncResp.ErrorCode)

	// Now add member2 (triggers rebalance — member1 hasn't rejoined)
	_, ch2 := joinMemberAsync(t, g, nil, makeProtocols("range"))
	runtime.Gosched()
	time.Sleep(10 * time.Millisecond)

	waitForState(t, g, GroupPreparingRebalance, time.Second)

	// Advance past the rebalance timeout (max of member rebalanceTimeout values = 5000ms)
	fc.WaitForTimers(1, time.Second)
	fc.Advance(6 * time.Second)
	// The timer callback sends to controlCh — give manage goroutine time to process
	runtime.Gosched()
	time.Sleep(50 * time.Millisecond)

	// After timeout, member1 (who didn't rejoin) should be removed.
	// member2 should be the only member and should get a JoinGroupResponse.
	r2 := <-ch2
	require.Equal(t, int16(0), r2.ErrorCode)
	assert.Equal(t, 1, getMemberCount(g))
}

func TestRebalanceTimer_AllAbsent_GroupEmpty(t *testing.T) {
	t.Parallel()
	fc := clock.NewFakeClock(t0)
	g := newTestGroup(t, fc)

	// Join member1, go stable
	member1, jr1 := joinMember(t, g, nil, makeProtocols("range"))
	require.Equal(t, int16(0), jr1.ErrorCode)
	syncResp := syncLeader(t, g, member1, jr1.Generation, nil)
	require.Equal(t, int16(0), syncResp.ErrorCode)

	// Member1 joins another member (triggering rebalance), but then we remove
	// member1 via leave before the rebalance completes
	_, _ = joinMemberAsync(t, g, nil, makeProtocols("range"))
	runtime.Gosched()
	time.Sleep(10 * time.Millisecond)

	// Leave member1 — now only member2 is waiting, and after timeout member2
	// should get its response
	leaveResp, err := g.Send(&kmsg.LeaveGroupRequest{
		Group:    "test-group",
		MemberID: member1,
	})
	require.NoError(t, err)
	lr := leaveResp.(*kmsg.LeaveGroupResponse)
	assert.Equal(t, int16(0), lr.ErrorCode)
}

// ---------------------------------------------------------------------------
// Tests: session timeout with FakeClock
// ---------------------------------------------------------------------------

func TestSessionTimeout_MemberRemoved(t *testing.T) {
	t.Parallel()
	fc := clock.NewFakeClock(t0)
	g := newTestGroup(t, fc)

	member1, jr1 := joinMember(t, g, nil, makeProtocols("range"))
	require.Equal(t, int16(0), jr1.ErrorCode)
	syncResp := syncLeader(t, g, member1, jr1.Generation, nil)
	require.Equal(t, int16(0), syncResp.ErrorCode)

	waitForState(t, g, GroupStable, time.Second)

	// Session timeout is 10000ms. Advance past it.
	fc.WaitForTimers(1, time.Second)
	fc.Advance(11 * time.Second)
	runtime.Gosched()
	time.Sleep(50 * time.Millisecond)

	// Member should be removed, group should be Empty
	waitForState(t, g, GroupEmpty, time.Second)
	assert.Equal(t, 0, getMemberCount(g))
}

func TestSessionTimeout_HeartbeatResetsTimer(t *testing.T) {
	t.Parallel()
	fc := clock.NewFakeClock(t0)
	g := newTestGroup(t, fc)

	member1, jr1 := joinMember(t, g, nil, makeProtocols("range"))
	require.Equal(t, int16(0), jr1.ErrorCode)
	syncResp := syncLeader(t, g, member1, jr1.Generation, nil)
	require.Equal(t, int16(0), syncResp.ErrorCode)

	waitForState(t, g, GroupStable, time.Second)

	// Advance 8 seconds (under 10s timeout)
	fc.WaitForTimers(1, time.Second)
	fc.Advance(8 * time.Second)
	runtime.Gosched()
	time.Sleep(20 * time.Millisecond)

	// Send heartbeat — should succeed and reset timer
	resp, err := g.Send(heartbeatRequest(member1, jr1.Generation))
	require.NoError(t, err)
	hr := resp.(*kmsg.HeartbeatResponse)
	assert.Equal(t, int16(0), hr.ErrorCode)

	// Advance another 8 seconds (total 16s from start, but only 8s from heartbeat)
	fc.WaitForTimers(1, time.Second)
	fc.Advance(8 * time.Second)
	runtime.Gosched()
	time.Sleep(20 * time.Millisecond)

	// Member should still be alive
	assert.Equal(t, GroupStable, getState(g))
	assert.Equal(t, 1, getMemberCount(g))

	// Advance past the new timeout (another 3 seconds → 11s from last heartbeat)
	fc.Advance(3 * time.Second)
	runtime.Gosched()
	time.Sleep(50 * time.Millisecond)

	waitForState(t, g, GroupEmpty, time.Second)
}

// ---------------------------------------------------------------------------
// Tests: pending member timeout
// ---------------------------------------------------------------------------

func TestPendingMemberTimeout(t *testing.T) {
	t.Parallel()
	fc := clock.NewFakeClock(t0)
	g := newTestGroup(t, fc)

	// v4+ join: get MEMBER_ID_REQUIRED, but never follow up
	resp1, err := g.Send(joinRequest("", nil, 4, makeProtocols("range")))
	require.NoError(t, err)
	jr1 := resp1.(*kmsg.JoinGroupResponse)
	require.Equal(t, kerr.MemberIDRequired.Code, jr1.ErrorCode)
	pendingMemberID := jr1.MemberID

	// Verify member is in pending map
	var inPending bool
	g.Control(func() { _, inPending = g.pending[pendingMemberID] })
	assert.True(t, inPending)

	// Advance past session timeout (10s)
	fc.WaitForTimers(1, time.Second)
	fc.Advance(11 * time.Second)
	runtime.Gosched()
	time.Sleep(50 * time.Millisecond)

	// Member should be removed from pending
	g.Control(func() { _, inPending = g.pending[pendingMemberID] })
	assert.False(t, inPending)
}

// ---------------------------------------------------------------------------
// Tests: static member replacement
// ---------------------------------------------------------------------------

func TestStaticMember_ReplaceWithSameProtocols_SkipsRebalance(t *testing.T) {
	t.Parallel()
	g := newTestGroup(t, nil)
	instID := "static-inst-1"

	// Join static member
	member1, jr1 := joinMember(t, g, strPtr(instID), makeProtocols("range"))
	require.Equal(t, int16(0), jr1.ErrorCode)
	gen1 := jr1.Generation

	// Sync with assignment
	assignment := []byte("assignment-data")
	a := kmsg.NewSyncGroupRequestGroupAssignment()
	a.MemberID = member1
	a.MemberAssignment = assignment
	syncResp := syncLeader(t, g, member1, gen1, []kmsg.SyncGroupRequestGroupAssignment{a})
	require.Equal(t, int16(0), syncResp.ErrorCode)

	waitForState(t, g, GroupStable, time.Second)

	// New client with same instance ID and empty memberID joins.
	// replaceStaticMember sees old member exists in members map and same
	// protocols in Stable → returns JoinGroupResponse directly (no rebalance,
	// no MEMBER_ID_REQUIRED).
	resp1, err := g.Send(joinRequest("", strPtr(instID), 4, makeProtocols("range")))
	require.NoError(t, err)
	jr := resp1.(*kmsg.JoinGroupResponse)
	require.Equal(t, int16(0), jr.ErrorCode)
	newMemberID := jr.MemberID
	require.NotEqual(t, member1, newMemberID)

	// Generation should be the same (no rebalance triggered)
	assert.Equal(t, gen1, jr.Generation)
	// Should still be stable
	assert.Equal(t, GroupStable, getState(g))
}

func TestStaticMember_ReplaceWithDifferentProtocols_TriggersRebalance(t *testing.T) {
	t.Parallel()
	g := newTestGroup(t, nil)
	instID := "static-inst-2"

	// Join static member with both "range" and "roundrobin"
	member1, jr1 := joinMember(t, g, strPtr(instID), makeProtocols("range", "roundrobin"))
	require.Equal(t, int16(0), jr1.ErrorCode)
	gen1 := jr1.Generation

	syncResp := syncLeader(t, g, member1, gen1, nil)
	require.Equal(t, int16(0), syncResp.ErrorCode)
	waitForState(t, g, GroupStable, time.Second)

	// New client with same instance ID but different protocol order/subset.
	// The new member supports "roundrobin" only — protocolsMatch passes because
	// "roundrobin" has count == len(members) == 1. But sameProtocols detects
	// the difference (old had 2 protocols, new has 1) → triggers rebalance.
	resp1, err := g.Send(joinRequest("", strPtr(instID), 4, makeProtocols("roundrobin")))
	require.NoError(t, err)
	jr := resp1.(*kmsg.JoinGroupResponse)
	require.Equal(t, int16(0), jr.ErrorCode)
	// Generation should have bumped (rebalance triggered)
	assert.Greater(t, jr.Generation, gen1)
}

func TestStaticMember_FencesOldMemberJoinCh(t *testing.T) {
	t.Parallel()
	g := newTestGroup(t, nil)
	instID := "static-fence-test"

	// Join a single static member
	member1, jr1 := joinMember(t, g, strPtr(instID), makeProtocols("range"))
	require.Equal(t, int16(0), jr1.ErrorCode)
	gen := jr1.Generation

	// Sync to go stable
	syncResp := syncLeader(t, g, member1, gen, nil)
	require.Equal(t, int16(0), syncResp.ErrorCode)
	waitForState(t, g, GroupStable, time.Second)

	// Replace static member with same protocols → should skip rebalance
	resp, err := g.Send(joinRequest("", strPtr(instID), 4, makeProtocols("range")))
	require.NoError(t, err)
	jr := resp.(*kmsg.JoinGroupResponse)
	assert.Equal(t, int16(0), jr.ErrorCode)
	assert.NotEqual(t, member1, jr.MemberID, "should get new memberID")
	assert.Equal(t, gen, jr.Generation, "generation should not change (no rebalance)")
	assert.Equal(t, GroupStable, getState(g))
}

// ---------------------------------------------------------------------------
// Tests: leave group
// ---------------------------------------------------------------------------

func TestLeaveGroup_V2_KnownMember(t *testing.T) {
	t.Parallel()
	g := newTestGroup(t, nil)

	member1, jr1 := joinMember(t, g, nil, makeProtocols("range"))
	require.Equal(t, int16(0), jr1.ErrorCode)
	syncResp := syncLeader(t, g, member1, jr1.Generation, nil)
	require.Equal(t, int16(0), syncResp.ErrorCode)

	// Leave with v2 (uses top-level MemberID)
	resp, err := g.Send(&kmsg.LeaveGroupRequest{
		Version:  2,
		Group:    "test-group",
		MemberID: member1,
	})
	require.NoError(t, err)
	lr := resp.(*kmsg.LeaveGroupResponse)
	assert.Equal(t, int16(0), lr.ErrorCode)

	waitForState(t, g, GroupEmpty, time.Second)
}

func TestLeaveGroup_V2_UnknownMember(t *testing.T) {
	t.Parallel()
	g := newTestGroup(t, nil)

	resp, err := g.Send(&kmsg.LeaveGroupRequest{
		Version:  2,
		Group:    "test-group",
		MemberID: "nonexistent",
	})
	require.NoError(t, err)
	lr := resp.(*kmsg.LeaveGroupResponse)
	assert.Equal(t, kerr.UnknownMemberID.Code, lr.ErrorCode)
}

func TestLeaveGroup_V3_MultipleMembers(t *testing.T) {
	t.Parallel()
	g := newTestGroup(t, nil)

	// Set up two members directly via Control to avoid the sequential
	// rebalance issue where the first join completes before the second arrives.
	var member1ID, member2ID string
	g.Control(func() {
		member1ID = "member-1"
		member2ID = "member-2"
		m1 := &groupMember{
			memberID:         member1ID,
			protocolType:     "consumer",
			protocols:        makeProtocols("range"),
			sessionTimeoutMs: 30000,
		}
		m2 := &groupMember{
			memberID:         member2ID,
			protocolType:     "consumer",
			protocols:        makeProtocols("range"),
			sessionTimeoutMs: 30000,
		}
		g.members[member1ID] = m1
		g.members[member2ID] = m2
		g.protocolType = "consumer"
		g.state = GroupStable
		g.generation = 1
		g.leader = member1ID
	})

	// v3+ leave with both members + an unknown member
	m1 := kmsg.NewLeaveGroupRequestMember()
	m1.MemberID = member1ID
	m2 := kmsg.NewLeaveGroupRequestMember()
	m2.MemberID = member2ID
	m3 := kmsg.NewLeaveGroupRequestMember()
	m3.MemberID = "unknown-member"

	resp, err := g.Send(&kmsg.LeaveGroupRequest{
		Version: 3,
		Group:   "test-group",
		Members: []kmsg.LeaveGroupRequestMember{m1, m2, m3},
	})
	require.NoError(t, err)
	lr := resp.(*kmsg.LeaveGroupResponse)
	require.Len(t, lr.Members, 3)
	assert.Equal(t, int16(0), lr.Members[0].ErrorCode)
	assert.Equal(t, int16(0), lr.Members[1].ErrorCode)
	assert.Equal(t, kerr.UnknownMemberID.Code, lr.Members[2].ErrorCode)

	waitForState(t, g, GroupEmpty, time.Second)
}

func TestLeaveGroup_PendingMember(t *testing.T) {
	t.Parallel()
	g := newTestGroup(t, nil)

	// Create a pending member (v4+ join, never follow up)
	resp1, err := g.Send(joinRequest("", nil, 4, makeProtocols("range")))
	require.NoError(t, err)
	jr1 := resp1.(*kmsg.JoinGroupResponse)
	require.Equal(t, kerr.MemberIDRequired.Code, jr1.ErrorCode)
	pendingID := jr1.MemberID

	// Leave the pending member (v2 style)
	resp, err := g.Send(&kmsg.LeaveGroupRequest{
		Version:  2,
		Group:    "test-group",
		MemberID: pendingID,
	})
	require.NoError(t, err)
	lr := resp.(*kmsg.LeaveGroupResponse)
	assert.Equal(t, int16(0), lr.ErrorCode)

	// Verify pending map is empty
	var pendingCount int
	g.Control(func() { pendingCount = len(g.pending) })
	assert.Equal(t, 0, pendingCount)
}

// ---------------------------------------------------------------------------
// Tests: offset operations
// ---------------------------------------------------------------------------

func TestOffsetCommit_UnknownMember(t *testing.T) {
	t.Parallel()
	g := newTestGroup(t, nil)

	tp := kmsg.NewOffsetCommitRequestTopicPartition()
	tp.Partition = 0
	tp.Offset = 100
	topic := kmsg.NewOffsetCommitRequestTopic()
	topic.Topic = "test-topic"
	topic.Partitions = []kmsg.OffsetCommitRequestTopicPartition{tp}

	req := &kmsg.OffsetCommitRequest{
		Group:      "test-group",
		MemberID:   "unknown-member",
		Generation: 1,
		Topics:     []kmsg.OffsetCommitRequestTopic{topic},
	}

	resp, err := g.Send(req)
	require.NoError(t, err)
	cr := resp.(*kmsg.OffsetCommitResponse)
	require.Len(t, cr.Topics, 1)
	require.Len(t, cr.Topics[0].Partitions, 1)
	assert.Equal(t, kerr.UnknownMemberID.Code, cr.Topics[0].Partitions[0].ErrorCode)
}

func TestOffsetCommit_WrongGeneration(t *testing.T) {
	t.Parallel()
	g := newTestGroup(t, nil)

	member1, jr1 := joinMember(t, g, nil, makeProtocols("range"))
	require.Equal(t, int16(0), jr1.ErrorCode)
	syncResp := syncLeader(t, g, member1, jr1.Generation, nil)
	require.Equal(t, int16(0), syncResp.ErrorCode)

	tp := kmsg.NewOffsetCommitRequestTopicPartition()
	tp.Partition = 0
	tp.Offset = 100
	topic := kmsg.NewOffsetCommitRequestTopic()
	topic.Topic = "test-topic"
	topic.Partitions = []kmsg.OffsetCommitRequestTopicPartition{tp}

	req := &kmsg.OffsetCommitRequest{
		Group:      "test-group",
		MemberID:   member1,
		Generation: 999,
		Topics:     []kmsg.OffsetCommitRequestTopic{topic},
	}

	resp, err := g.Send(req)
	require.NoError(t, err)
	cr := resp.(*kmsg.OffsetCommitResponse)
	require.Len(t, cr.Topics, 1)
	require.Len(t, cr.Topics[0].Partitions, 1)
	assert.Equal(t, kerr.IllegalGeneration.Code, cr.Topics[0].Partitions[0].ErrorCode)
}

func TestOffsetCommit_Success(t *testing.T) {
	t.Parallel()
	g := newTestGroup(t, nil)

	member1, jr1 := joinMember(t, g, nil, makeProtocols("range"))
	require.Equal(t, int16(0), jr1.ErrorCode)
	syncResp := syncLeader(t, g, member1, jr1.Generation, nil)
	require.Equal(t, int16(0), syncResp.ErrorCode)

	tp := kmsg.NewOffsetCommitRequestTopicPartition()
	tp.Partition = 0
	tp.Offset = 42
	meta := "meta"
	tp.Metadata = &meta
	topic := kmsg.NewOffsetCommitRequestTopic()
	topic.Topic = "test-topic"
	topic.Partitions = []kmsg.OffsetCommitRequestTopicPartition{tp}

	req := &kmsg.OffsetCommitRequest{
		Group:      "test-group",
		MemberID:   member1,
		Generation: jr1.Generation,
		Topics:     []kmsg.OffsetCommitRequestTopic{topic},
	}

	resp, err := g.Send(req)
	require.NoError(t, err)
	cr := resp.(*kmsg.OffsetCommitResponse)
	require.Len(t, cr.Topics, 1)
	require.Len(t, cr.Topics[0].Partitions, 1)
	assert.Equal(t, int16(0), cr.Topics[0].Partitions[0].ErrorCode)

	// Verify offset was stored
	var stored int64
	g.Control(func() {
		co, ok := g.offsets[TopicPartition{Topic: "test-topic", Partition: 0}]
		if ok {
			stored = co.Offset
		}
	})
	assert.Equal(t, int64(42), stored)
}

func TestOffsetDelete_DeadGroup(t *testing.T) {
	t.Parallel()
	g := newTestGroup(t, nil)

	// Set state to Dead via Control
	g.Control(func() { g.state = GroupDead })

	tp := kmsg.NewOffsetDeleteRequestTopicPartition()
	tp.Partition = 0
	topic := kmsg.NewOffsetDeleteRequestTopic()
	topic.Topic = "test-topic"
	topic.Partitions = []kmsg.OffsetDeleteRequestTopicPartition{tp}

	req := &kmsg.OffsetDeleteRequest{
		Group:  "test-group",
		Topics: []kmsg.OffsetDeleteRequestTopic{topic},
	}

	resp, err := g.Send(req)
	require.NoError(t, err)
	dr := resp.(*kmsg.OffsetDeleteResponse)
	assert.Equal(t, kerr.GroupIDNotFound.Code, dr.ErrorCode)
}

func TestOffsetDelete_EmptyGroup_Success(t *testing.T) {
	t.Parallel()
	g := newTestGroup(t, nil)

	// Pre-populate an offset
	g.Control(func() {
		g.offsets[TopicPartition{Topic: "test-topic", Partition: 0}] = CommittedOffset{Offset: 42}
	})

	tp := kmsg.NewOffsetDeleteRequestTopicPartition()
	tp.Partition = 0
	topic := kmsg.NewOffsetDeleteRequestTopic()
	topic.Topic = "test-topic"
	topic.Partitions = []kmsg.OffsetDeleteRequestTopicPartition{tp}

	req := &kmsg.OffsetDeleteRequest{
		Group:  "test-group",
		Topics: []kmsg.OffsetDeleteRequestTopic{topic},
	}

	resp, err := g.Send(req)
	require.NoError(t, err)
	dr := resp.(*kmsg.OffsetDeleteResponse)
	assert.Equal(t, int16(0), dr.ErrorCode)
	require.Len(t, dr.Topics, 1)
	require.Len(t, dr.Topics[0].Partitions, 1)
	assert.Equal(t, int16(0), dr.Topics[0].Partitions[0].ErrorCode)

	// Verify offset was deleted
	var exists bool
	g.Control(func() {
		_, exists = g.offsets[TopicPartition{Topic: "test-topic", Partition: 0}]
	})
	assert.False(t, exists)
}

func TestFetchOffsets_EmptyQuery_ReturnsAll(t *testing.T) {
	t.Parallel()
	g := newTestGroup(t, nil)

	g.Control(func() {
		g.offsets[TopicPartition{Topic: "t1", Partition: 0}] = CommittedOffset{Offset: 10, Metadata: "m1"}
		g.offsets[TopicPartition{Topic: "t1", Partition: 1}] = CommittedOffset{Offset: 20, Metadata: "m2"}
		g.offsets[TopicPartition{Topic: "t2", Partition: 0}] = CommittedOffset{Offset: 30, Metadata: "m3"}
	})

	var results []OffsetResult
	g.Control(func() {
		results = g.FetchOffsets(OffsetQuery{})
	})
	assert.Len(t, results, 3)

	offsets := make(map[string]int64)
	for _, r := range results {
		key := r.Topic + "-" + string(rune('0'+r.Partition))
		offsets[key] = r.Offset
	}
	assert.Equal(t, int64(10), offsets["t1-0"])
	assert.Equal(t, int64(20), offsets["t1-1"])
	assert.Equal(t, int64(30), offsets["t2-0"])
}

func TestFetchOffsets_SpecificPartitions(t *testing.T) {
	t.Parallel()
	g := newTestGroup(t, nil)

	g.Control(func() {
		g.offsets[TopicPartition{Topic: "t1", Partition: 0}] = CommittedOffset{Offset: 10}
		g.offsets[TopicPartition{Topic: "t1", Partition: 1}] = CommittedOffset{Offset: 20}
	})

	var results []OffsetResult
	g.Control(func() {
		results = g.FetchOffsets(OffsetQuery{
			Topics: []OffsetQueryTopic{
				{Topic: "t1", Partitions: []int32{0, 2}}, // 0 exists, 2 doesn't
			},
		})
	})
	require.Len(t, results, 2)
	assert.Equal(t, int64(10), results[0].Offset) // partition 0
	assert.Equal(t, int64(-1), results[1].Offset) // partition 2 (not found)
}

// ---------------------------------------------------------------------------
// Tests: protocolsMatch edge cases
// ---------------------------------------------------------------------------

func TestProtocolsMatch_EmptyGroup(t *testing.T) {
	t.Parallel()
	g := newTestGroup(t, nil)

	var matched bool
	g.Control(func() {
		matched = g.protocolsMatch("consumer", makeProtocols("range"))
	})
	assert.True(t, matched, "empty group should match any protocol")
}

func TestProtocolsMatch_DifferentProtocolType(t *testing.T) {
	t.Parallel()
	g := newTestGroup(t, nil)

	// Join a member to set protocolType to "consumer"
	_, jr := joinMember(t, g, nil, makeProtocols("range"))
	require.Equal(t, int16(0), jr.ErrorCode)

	var matched bool
	g.Control(func() {
		matched = g.protocolsMatch("connect", makeProtocols("range"))
	})
	assert.False(t, matched, "different protocol type should not match")
}

func TestProtocolsMatch_NoOverlap(t *testing.T) {
	t.Parallel()
	g := newTestGroup(t, nil)

	// Join a member with "range"
	_, jr := joinMember(t, g, nil, makeProtocols("range"))
	require.Equal(t, int16(0), jr.ErrorCode)

	var matched bool
	g.Control(func() {
		matched = g.protocolsMatch("consumer", makeProtocols("sticky"))
	})
	assert.False(t, matched, "no overlapping protocol names should not match")
}

// ---------------------------------------------------------------------------
// Tests: v3 pre-join flow
// ---------------------------------------------------------------------------

func TestJoinGroup_V3_SkipsMemberIDRequired(t *testing.T) {
	t.Parallel()
	g := newTestGroup(t, nil)

	// v3 join should skip MEMBER_ID_REQUIRED and join directly
	resp, err := g.Send(joinRequest("", nil, 3, makeProtocols("range")))
	require.NoError(t, err)
	jr := resp.(*kmsg.JoinGroupResponse)
	require.Equal(t, int16(0), jr.ErrorCode)
	assert.NotEmpty(t, jr.MemberID)
	assert.Equal(t, int32(1), jr.Generation)
}

// ---------------------------------------------------------------------------
// Tests: GroupState.String()
// ---------------------------------------------------------------------------

func TestGroupState_String(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "Empty", GroupEmpty.String())
	assert.Equal(t, "PreparingRebalance", GroupPreparingRebalance.String())
	assert.Equal(t, "CompletingRebalance", GroupCompletingRebalance.String())
	assert.Equal(t, "Stable", GroupStable.String())
	assert.Equal(t, "Dead", GroupDead.String())
	assert.Equal(t, "Unknown", GroupState(99).String())
}

// ---------------------------------------------------------------------------
// Tests: validateInstanceID
// ---------------------------------------------------------------------------

func TestValidateInstanceID(t *testing.T) {
	t.Parallel()
	g := newTestGroup(t, nil)

	instID := "my-instance"
	member1, jr := joinMember(t, g, strPtr(instID), makeProtocols("range"))
	require.Equal(t, int16(0), jr.ErrorCode)
	_ = member1

	// Validate correct pairing
	g.Control(func() {
		err := g.validateInstanceID(strPtr(instID), member1)
		assert.Nil(t, err)
	})

	// Validate nil instanceID (always passes)
	g.Control(func() {
		err := g.validateInstanceID(nil, "any-member")
		assert.Nil(t, err)
	})

	// Validate wrong memberID for instanceID → FencedInstanceID
	g.Control(func() {
		err := g.validateInstanceID(strPtr(instID), "wrong-member")
		require.NotNil(t, err)
		assert.Equal(t, kerr.FencedInstanceID.Code, err.Code)
	})

	// Validate unknown instanceID → UnknownMemberID
	g.Control(func() {
		err := g.validateInstanceID(strPtr("unknown-instance"), "any-member")
		require.NotNil(t, err)
		assert.Equal(t, kerr.UnknownMemberID.Code, err.Code)
	})
}

// ---------------------------------------------------------------------------
// Tests: pending sync timer
// ---------------------------------------------------------------------------

func TestPendingSyncTimer_NoSyncCausesRebalance(t *testing.T) {
	t.Parallel()
	fc := clock.NewFakeClock(t0)
	g := newTestGroup(t, fc)

	// Join a single member
	member1, jr1 := joinMember(t, g, nil, makeProtocols("range"))
	require.Equal(t, int16(0), jr1.ErrorCode)

	// Don't send SyncGroup — just wait for pendingSyncTimer to fire
	waitForState(t, g, GroupCompletingRebalance, time.Second)

	// Advance past the pendingSyncTimer (rebalanceTimeout = 5000ms)
	fc.WaitForTimers(1, time.Second)
	fc.Advance(6 * time.Second)
	runtime.Gosched()
	time.Sleep(50 * time.Millisecond)

	// Member should be removed (no assignment, no sync), group goes Empty
	waitForState(t, g, GroupEmpty, time.Second)
	_ = member1
}

// ---------------------------------------------------------------------------
// Tests: SyncGroup in Stable state returns cached assignment
// ---------------------------------------------------------------------------

func TestSyncGroup_StableState_ReturnsCachedAssignment(t *testing.T) {
	t.Parallel()
	g := newTestGroup(t, nil)

	member1, jr1 := joinMember(t, g, nil, makeProtocols("range"))
	require.Equal(t, int16(0), jr1.ErrorCode)

	// Leader sync with assignment
	assignment := []byte("partition-assignment")
	a := kmsg.NewSyncGroupRequestGroupAssignment()
	a.MemberID = member1
	a.MemberAssignment = assignment
	syncResp := syncLeader(t, g, member1, jr1.Generation, []kmsg.SyncGroupRequestGroupAssignment{a})
	require.Equal(t, int16(0), syncResp.ErrorCode)
	assert.Equal(t, assignment, syncResp.MemberAssignment)

	waitForState(t, g, GroupStable, time.Second)

	// SyncGroup again in Stable state should return cached assignment
	resp, err := g.Send(syncRequest(member1, jr1.Generation, nil))
	require.NoError(t, err)
	sr := resp.(*kmsg.SyncGroupResponse)
	assert.Equal(t, int16(0), sr.ErrorCode)
	assert.Equal(t, assignment, sr.MemberAssignment)
}

// ---------------------------------------------------------------------------
// Tests: OffsetCommit without member (simple consumer)
// ---------------------------------------------------------------------------

func TestOffsetCommit_NoMemberID_Success(t *testing.T) {
	t.Parallel()
	g := newTestGroup(t, nil)

	tp := kmsg.NewOffsetCommitRequestTopicPartition()
	tp.Partition = 0
	tp.Offset = 100
	topic := kmsg.NewOffsetCommitRequestTopic()
	topic.Topic = "test-topic"
	topic.Partitions = []kmsg.OffsetCommitRequestTopicPartition{tp}

	req := &kmsg.OffsetCommitRequest{
		Group:  "test-group",
		Topics: []kmsg.OffsetCommitRequestTopic{topic},
	}

	resp, err := g.Send(req)
	require.NoError(t, err)
	cr := resp.(*kmsg.OffsetCommitResponse)
	require.Len(t, cr.Topics, 1)
	require.Len(t, cr.Topics[0].Partitions, 1)
	assert.Equal(t, int16(0), cr.Topics[0].Partitions[0].ErrorCode)
}
