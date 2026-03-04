package cluster

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/klaudworks/klite/internal/metadata"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// GroupState represents the state of a consumer group.
type GroupState int

const (
	GroupEmpty               GroupState = iota
	GroupPreparingRebalance             // waiting for members to join
	GroupCompletingRebalance            // waiting for leader's SyncGroup
	GroupStable                         // normal operation
	GroupDead                           // being removed
)

// String returns the Kafka-standard state name.
func (s GroupState) String() string {
	switch s {
	case GroupEmpty:
		return "Empty"
	case GroupPreparingRebalance:
		return "PreparingRebalance"
	case GroupCompletingRebalance:
		return "CompletingRebalance"
	case GroupStable:
		return "Stable"
	case GroupDead:
		return "Dead"
	default:
		return "Unknown"
	}
}

// groupRequest is sent from handler goroutines to the group's manage goroutine.
type groupRequest struct {
	req    kmsg.Request
	respCh chan groupResponse
}

// groupResponse is sent back from the group goroutine to the handler.
type groupResponse struct {
	resp kmsg.Response
	err  error
}

// groupMember represents a member of a consumer group.
type groupMember struct {
	memberID   string
	instanceID *string // static membership (KIP-345)

	// From the member's JoinGroup request
	protocolType     string
	protocols        []kmsg.JoinGroupRequestProtocol
	sessionTimeoutMs int32
	rebalanceTimeout int32
	joinVersion      int16 // version of the JoinGroup request for response encoding

	// Assignment from SyncGroup leader
	assignment []byte

	// Parked request waiting for response (JoinGroup or SyncGroup)
	waitJoinCh  chan groupResponse
	waitSyncCh  chan groupResponse
	syncVersion int16 // version of the SyncGroup request for response encoding

	// Session timeout timer
	sessionTimer *time.Timer
	lastSeen     time.Time
}

// Group represents a consumer group with its state machine.
// All state is mutated exclusively in the manage() goroutine.
type Group struct {
	id    string
	state GroupState

	protocolType string            // e.g., "consumer"
	protocol     string            // elected protocol, e.g., "range"
	protocols    map[string]int    // protocol name -> count of members supporting it
	generation   int32
	leader       string            // member ID of the leader

	members       map[string]*groupMember // memberID -> member
	pending       map[string]*groupMember // memberID -> pending member (MEMBER_ID_REQUIRED)
	staticMembers map[string]string       // instanceID -> memberID

	// Committed offsets (topic-partition -> offset)
	offsets map[TopicPartition]CommittedOffset

	// Number of members with a parked JoinGroup request
	nJoining int

	// Timers
	rebalanceTimer *time.Timer // rebalance timeout
	pendingSyncTimer *time.Timer // pending sync timeout

	// Channel-based dispatch
	reqCh     chan groupRequest
	controlCh chan func()
	quitCh    chan struct{}

	shutdownCh <-chan struct{}
	logger     *slog.Logger
	metaLog    *metadata.Log // optional: persistence for offset commits
}

// TopicPartition is a (topic, partition) pair used as a map key.
type TopicPartition struct {
	Topic     string
	Partition int32
}

// CommittedOffset stores a committed offset for a topic-partition.
type CommittedOffset struct {
	Offset        int64
	LeaderEpoch   int32
	Metadata      string
	CommitTime    time.Time
}

// NewGroup creates a new consumer group and starts its manage goroutine.
func NewGroup(id string, shutdownCh <-chan struct{}, logger *slog.Logger) *Group {
	g := &Group{
		id:            id,
		state:         GroupEmpty,
		protocols:     make(map[string]int),
		members:       make(map[string]*groupMember),
		pending:       make(map[string]*groupMember),
		staticMembers: make(map[string]string),
		offsets:       make(map[TopicPartition]CommittedOffset),
		reqCh:         make(chan groupRequest, 16),
		controlCh:     make(chan func(), 1),
		quitCh:        make(chan struct{}),
		shutdownCh:    shutdownCh,
		logger:        logger.With("group", id),
	}
	go g.manage()
	return g
}

// SetMetadataLog sets the metadata log for persisting offset commits.
func (g *Group) SetMetadataLog(ml *metadata.Log) {
	g.metaLog = ml
}

// Send sends a request to the group goroutine and waits for a response.
func (g *Group) Send(req kmsg.Request) (kmsg.Response, error) {
	respCh := make(chan groupResponse, 1)
	select {
	case g.reqCh <- groupRequest{req: req, respCh: respCh}:
	case <-g.quitCh:
		return nil, fmt.Errorf("group is dead")
	case <-g.shutdownCh:
		return nil, fmt.Errorf("broker shutting down")
	}
	select {
	case resp := <-respCh:
		return resp.resp, resp.err
	case <-g.quitCh:
		return nil, fmt.Errorf("group is dead")
	case <-g.shutdownCh:
		return nil, fmt.Errorf("broker shutting down")
	}
}

// Control executes a function in the group's manage goroutine context.
// Used for cross-goroutine reads (DescribeGroups, ListGroups, etc.)
func (g *Group) Control(fn func()) {
	done := make(chan struct{})
	wrapped := func() {
		fn()
		close(done)
	}
	select {
	case g.controlCh <- wrapped:
	case <-g.quitCh:
		return
	case <-g.shutdownCh:
		return
	}
	select {
	case <-done:
	case <-g.quitCh:
	case <-g.shutdownCh:
	}
}

// Stop stops the group goroutine.
func (g *Group) Stop() {
	select {
	case <-g.quitCh:
	default:
		close(g.quitCh)
	}
}

// manage is the group's event loop. All state mutation happens here.
func (g *Group) manage() {
	defer g.cleanup()

	for {
		select {
		case <-g.quitCh:
			return
		case <-g.shutdownCh:
			return
		case greq := <-g.reqCh:
			g.handleRequest(greq)
		case fn := <-g.controlCh:
			fn()
		}
	}
}

// cleanup stops all timers and responds to any parked requests.
func (g *Group) cleanup() {
	if g.rebalanceTimer != nil {
		g.rebalanceTimer.Stop()
	}
	if g.pendingSyncTimer != nil {
		g.pendingSyncTimer.Stop()
	}
	for _, m := range g.members {
		if m.sessionTimer != nil {
			m.sessionTimer.Stop()
		}
		// Unblock any parked JoinGroup or SyncGroup requests
		if m.waitJoinCh != nil {
			close(m.waitJoinCh)
		}
		if m.waitSyncCh != nil {
			close(m.waitSyncCh)
		}
	}
	for _, m := range g.pending {
		if m.sessionTimer != nil {
			m.sessionTimer.Stop()
		}
	}
	g.state = GroupDead
}

// handleRequest dispatches a request to the appropriate handler.
func (g *Group) handleRequest(greq groupRequest) {
	var resp kmsg.Response

	switch req := greq.req.(type) {
	case *kmsg.JoinGroupRequest:
		resp = g.handleJoinGroup(req, greq.respCh)
	case *kmsg.SyncGroupRequest:
		resp = g.handleSyncGroup(req, greq.respCh)
	case *kmsg.HeartbeatRequest:
		resp = g.handleHeartbeat(req)
	case *kmsg.LeaveGroupRequest:
		resp = g.handleLeaveGroup(req)
	case *kmsg.OffsetCommitRequest:
		resp = g.handleOffsetCommit(req)
	case *kmsg.OffsetFetchRequest:
		resp = g.handleOffsetFetch(req)
	case *kmsg.OffsetDeleteRequest:
		resp = g.handleOffsetDelete(req)
	default:
		g.logger.Error("unexpected request type in group goroutine", "type", fmt.Sprintf("%T", greq.req))
		return
	}

	// If resp is nil, the handler parked the request and will respond later
	if resp != nil {
		greq.respCh <- groupResponse{resp: resp}
	}
}

// generateMemberID generates a member ID in the format {clientID}-{UUID}.
func generateMemberID(clientID string, instanceID *string) string {
	prefix := clientID
	if instanceID != nil {
		prefix = *instanceID
	}
	return prefix + "-" + uuid.New().String()
}

// ---- JoinGroup ----

func (g *Group) handleJoinGroup(req *kmsg.JoinGroupRequest, respCh chan groupResponse) kmsg.Response {
	resp := req.ResponseKind().(*kmsg.JoinGroupResponse)

	if g.state == GroupDead {
		resp.ErrorCode = kerr.CoordinatorNotAvailable.Code
		return resp
	}

	// Validate protocol type
	if req.ProtocolType != "" && !g.protocolsMatch(req.ProtocolType, req.Protocols) {
		resp.ErrorCode = kerr.InconsistentGroupProtocol.Code
		return resp
	}

	if len(req.Protocols) == 0 {
		resp.ErrorCode = kerr.InconsistentGroupProtocol.Code
		return resp
	}

	// New member (empty member ID)
	if req.MemberID == "" {
		return g.handleJoinNewMember(req, resp, respCh)
	}

	// Known member: check pending first
	if m, ok := g.pending[req.MemberID]; ok {
		return g.handleJoinPendingMember(m, req, resp, respCh)
	}

	// Known member: check active members
	if m, ok := g.members[req.MemberID]; ok {
		return g.handleJoinExistingMember(m, req, resp, respCh)
	}

	// Unknown member
	resp.ErrorCode = kerr.UnknownMemberID.Code
	return resp
}

func (g *Group) handleJoinNewMember(req *kmsg.JoinGroupRequest, resp *kmsg.JoinGroupResponse, respCh chan groupResponse) kmsg.Response {
	// Static membership: check if instance ID is already registered
	if req.InstanceID != nil && *req.InstanceID != "" {
		if oldMemberID, ok := g.staticMembers[*req.InstanceID]; ok {
			return g.replaceStaticMember(oldMemberID, req, resp, respCh)
		}
	}

	memberID := generateMemberID("", req.InstanceID)

	// v4+: MEMBER_ID_REQUIRED flow — return ID, client retries
	if req.Version >= 4 {
		m := &groupMember{
			memberID:         memberID,
			instanceID:       req.InstanceID,
			protocolType:     req.ProtocolType,
			protocols:        req.Protocols,
			sessionTimeoutMs: req.SessionTimeoutMillis,
			rebalanceTimeout: req.RebalanceTimeoutMillis,
			joinVersion:      req.Version,
		}
		g.addPending(m)
		resp.MemberID = memberID
		resp.ErrorCode = kerr.MemberIDRequired.Code
		return resp
	}

	// v0-v3: add member directly and trigger rebalance
	m := &groupMember{
		memberID:         memberID,
		instanceID:       req.InstanceID,
		protocolType:     req.ProtocolType,
		protocols:        req.Protocols,
		sessionTimeoutMs: req.SessionTimeoutMillis,
		rebalanceTimeout: req.RebalanceTimeoutMillis,
		joinVersion:      req.Version,
	}
	g.addMemberAndRebalance(m, respCh)
	return nil // response sent later
}

func (g *Group) handleJoinPendingMember(m *groupMember, req *kmsg.JoinGroupRequest, resp *kmsg.JoinGroupResponse, respCh chan groupResponse) kmsg.Response {
	// Update member info from the retry request
	m.protocolType = req.ProtocolType
	m.protocols = req.Protocols
	m.sessionTimeoutMs = req.SessionTimeoutMillis
	m.rebalanceTimeout = req.RebalanceTimeoutMillis
	m.joinVersion = req.Version

	// Remove from pending
	delete(g.pending, m.memberID)
	if m.sessionTimer != nil {
		m.sessionTimer.Stop()
		m.sessionTimer = nil
	}

	// Add to active members and rebalance
	g.addMemberAndRebalance(m, respCh)
	return nil // response sent later
}

func (g *Group) handleJoinExistingMember(m *groupMember, req *kmsg.JoinGroupRequest, resp *kmsg.JoinGroupResponse, respCh chan groupResponse) kmsg.Response {
	// Static member validation
	if req.InstanceID != nil && *req.InstanceID != "" {
		if err := g.validateInstanceID(req.InstanceID, req.MemberID); err != nil {
			resp.ErrorCode = err.Code
			return resp
		}
	}

	// Update member protocols
	g.updateMemberProtocols(m, req)

	switch g.state {
	case GroupPreparingRebalance:
		// Member is rejoining during rebalance
		g.updateMemberAndRebalance(m, respCh)
		return nil

	case GroupCompletingRebalance, GroupStable:
		// New join triggers a rebalance
		g.updateMemberAndRebalance(m, respCh)
		return nil

	case GroupEmpty:
		// Should not happen (member exists but group is empty)
		resp.ErrorCode = kerr.UnknownMemberID.Code
		return resp

	default:
		resp.ErrorCode = kerr.UnknownMemberID.Code
		return resp
	}
}

// replaceStaticMember handles a new member replacing an existing static member.
func (g *Group) replaceStaticMember(oldMemberID string, req *kmsg.JoinGroupRequest, resp *kmsg.JoinGroupResponse, respCh chan groupResponse) kmsg.Response {
	oldMember, ok := g.members[oldMemberID]
	if !ok {
		// Old member not found, treat as new
		memberID := generateMemberID("", req.InstanceID)
		if req.Version >= 4 {
			m := &groupMember{
				memberID:         memberID,
				instanceID:       req.InstanceID,
				protocolType:     req.ProtocolType,
				protocols:        req.Protocols,
				sessionTimeoutMs: req.SessionTimeoutMillis,
				rebalanceTimeout: req.RebalanceTimeoutMillis,
				joinVersion:      req.Version,
			}
			g.addPending(m)
			g.staticMembers[*req.InstanceID] = memberID
			resp.MemberID = memberID
			resp.ErrorCode = kerr.MemberIDRequired.Code
			return resp
		}
		m := &groupMember{
			memberID:         memberID,
			instanceID:       req.InstanceID,
			protocolType:     req.ProtocolType,
			protocols:        req.Protocols,
			sessionTimeoutMs: req.SessionTimeoutMillis,
			rebalanceTimeout: req.RebalanceTimeoutMillis,
			joinVersion:      req.Version,
		}
		g.addMemberAndRebalance(m, respCh)
		return nil
	}

	// Generate new member ID for the replacement
	newMemberID := generateMemberID("", req.InstanceID)
	savedAssignment := oldMember.assignment

	// Fence the old member: send FENCED_INSTANCE_ID to any parked requests
	if oldMember.waitJoinCh != nil {
		fencedResp := &kmsg.JoinGroupResponse{Version: oldMember.joinVersion, ErrorCode: kerr.FencedInstanceID.Code}
		oldMember.waitJoinCh <- groupResponse{resp: fencedResp}
		oldMember.waitJoinCh = nil
	}
	if oldMember.waitSyncCh != nil {
		fencedResp := &kmsg.SyncGroupResponse{Version: oldMember.syncVersion, ErrorCode: kerr.FencedInstanceID.Code}
		oldMember.waitSyncCh <- groupResponse{resp: fencedResp}
		oldMember.waitSyncCh = nil
	}

	// Remove old member (without triggering rebalance)
	g.removeMemberDirect(oldMember)

	// Create new member with preserved assignment
	newMember := &groupMember{
		memberID:         newMemberID,
		instanceID:       req.InstanceID,
		protocolType:     req.ProtocolType,
		protocols:        req.Protocols,
		sessionTimeoutMs: req.SessionTimeoutMillis,
		rebalanceTimeout: req.RebalanceTimeoutMillis,
		assignment:       savedAssignment,
		joinVersion:      req.Version,
	}

	// Check if we can skip rebalance (KIP-345: same protocols, group is stable)
	if g.state == GroupStable && sameProtocols(oldMember, newMember) {
		// Fast path: skip rebalance
		g.addMemberDirect(newMember)
		if g.leader == oldMemberID {
			g.leader = newMemberID
		}
		g.startSessionTimer(newMember)

		g.fillJoinResponse(newMemberID, resp)
		resp.MemberID = newMemberID
		return resp
	}

	// Trigger rebalance
	g.addMemberAndRebalance(newMember, respCh)
	return nil
}

// sameProtocols checks if two members have the same protocol set.
func sameProtocols(old, new *groupMember) bool {
	if len(old.protocols) != len(new.protocols) {
		return false
	}
	for i, p := range old.protocols {
		if p.Name != new.protocols[i].Name {
			return false
		}
	}
	return true
}

// addPending adds a member to the pending map with a session timeout.
func (g *Group) addPending(m *groupMember) {
	g.pending[m.memberID] = m
	if m.instanceID != nil {
		g.staticMembers[*m.instanceID] = m.memberID
	}
	timeout := time.Duration(m.sessionTimeoutMs) * time.Millisecond
	if timeout <= 0 {
		timeout = 45 * time.Second
	}
	m.sessionTimer = time.AfterFunc(timeout, func() {
		g.timerControl(func() {
			if _, ok := g.pending[m.memberID]; ok {
				delete(g.pending, m.memberID)
				if m.instanceID != nil {
					if g.staticMembers[*m.instanceID] == m.memberID {
						delete(g.staticMembers, *m.instanceID)
					}
				}
			}
		})
	})
}

// addMemberAndRebalance adds a member to the group and triggers a rebalance.
// The respCh is stored in the member so the response can be sent later when
// the rebalance completes.
func (g *Group) addMemberAndRebalance(m *groupMember, respCh chan groupResponse) {
	g.addMemberDirect(m)
	m.waitJoinCh = respCh
	g.nJoining++
	g.rebalance()
}

// updateMemberAndRebalance updates an existing member's join state and triggers rebalance.
func (g *Group) updateMemberAndRebalance(m *groupMember, respCh chan groupResponse) {
	// If member had an existing parked join, uncount it
	if m.waitJoinCh != nil {
		g.nJoining--
	}
	m.waitJoinCh = respCh
	g.nJoining++
	g.rebalance()
}

// addMemberDirect adds a member to the group without triggering rebalance.
func (g *Group) addMemberDirect(m *groupMember) {
	g.members[m.memberID] = m
	if m.instanceID != nil {
		g.staticMembers[*m.instanceID] = m.memberID
	}
	for _, p := range m.protocols {
		g.protocols[p.Name]++
	}
	// First member sets the protocol type
	if g.protocolType == "" {
		g.protocolType = m.protocolType
	}
}

// removeMemberDirect removes a member without triggering rebalance.
func (g *Group) removeMemberDirect(m *groupMember) {
	if m.sessionTimer != nil {
		m.sessionTimer.Stop()
		m.sessionTimer = nil
	}
	for _, p := range m.protocols {
		g.protocols[p.Name]--
		if g.protocols[p.Name] <= 0 {
			delete(g.protocols, p.Name)
		}
	}
	if m.instanceID != nil {
		if g.staticMembers[*m.instanceID] == m.memberID {
			delete(g.staticMembers, *m.instanceID)
		}
	}
	delete(g.members, m.memberID)
}

// removeMemberAndRebalance removes a member and triggers a rebalance.
func (g *Group) removeMemberAndRebalance(m *groupMember) {
	if m.waitJoinCh != nil {
		g.nJoining--
		m.waitJoinCh = nil
	}
	if m.waitSyncCh != nil {
		m.waitSyncCh = nil
	}
	g.removeMemberDirect(m)

	if len(g.members) == 0 {
		g.state = GroupEmpty
		g.generation++
		g.protocolType = ""
		g.protocol = ""
		return
	}
	g.rebalance()
}

// updateMemberProtocols updates a member's protocol info from a new JoinGroup request.
func (g *Group) updateMemberProtocols(m *groupMember, req *kmsg.JoinGroupRequest) {
	// Remove old protocol counts
	for _, p := range m.protocols {
		g.protocols[p.Name]--
		if g.protocols[p.Name] <= 0 {
			delete(g.protocols, p.Name)
		}
	}
	// Update member info
	m.protocols = req.Protocols
	m.sessionTimeoutMs = req.SessionTimeoutMillis
	m.rebalanceTimeout = req.RebalanceTimeoutMillis
	m.joinVersion = req.Version
	// Add new protocol counts
	for _, p := range m.protocols {
		g.protocols[p.Name]++
	}
}

// ---- Rebalance ----

func (g *Group) rebalance() {
	// If currently in CompletingRebalance, kick out syncing members
	if g.state == GroupCompletingRebalance {
		for _, m := range g.members {
			if m.waitSyncCh != nil {
				resp := &kmsg.SyncGroupResponse{
					Version:   m.syncVersion,
					ErrorCode: kerr.RebalanceInProgress.Code,
				}
				m.waitSyncCh <- groupResponse{resp: resp}
				m.waitSyncCh = nil
			}
		}
	}

	if g.pendingSyncTimer != nil {
		g.pendingSyncTimer.Stop()
		g.pendingSyncTimer = nil
	}

	g.state = GroupPreparingRebalance
	g.logger.Debug("rebalance started", "nMembers", len(g.members), "nJoining", g.nJoining)

	// Fast path: all members have joined
	if g.nJoining >= len(g.members) {
		g.completeRebalance()
		return
	}

	// Slow path: wait for remaining members
	maxTimeout := g.maxRebalanceTimeout()
	if g.rebalanceTimer != nil {
		g.rebalanceTimer.Stop()
	}
	g.rebalanceTimer = time.AfterFunc(maxTimeout, func() {
		g.timerControl(g.completeRebalance)
	})
}

// maxRebalanceTimeout returns the maximum rebalance timeout across all members.
func (g *Group) maxRebalanceTimeout() time.Duration {
	var max int32
	for _, m := range g.members {
		if m.rebalanceTimeout > max {
			max = m.rebalanceTimeout
		}
	}
	if max <= 0 {
		max = 300000 // 5 minutes default
	}
	return time.Duration(max) * time.Millisecond
}

// completeRebalance finishes the rebalance, removes absent members, elects protocol.
func (g *Group) completeRebalance() {
	if g.rebalanceTimer != nil {
		g.rebalanceTimer.Stop()
		g.rebalanceTimer = nil
	}

	g.nJoining = 0

	// Remove members that didn't rejoin (no parked JoinGroup request)
	for id, m := range g.members {
		if m.waitJoinCh == nil {
			g.logger.Debug("removing absent member", "member", id)
			g.removeMemberDirect(m)
		}
	}

	if len(g.members) == 0 {
		g.state = GroupEmpty
		g.generation++
		g.protocolType = ""
		g.protocol = ""
		g.logger.Debug("group now empty after rebalance")
		return
	}

	g.generation++
	g.state = GroupCompletingRebalance

	// Elect protocol
	g.protocol = g.electProtocol()

	// Elect leader (first member, or keep existing if still present)
	if _, ok := g.members[g.leader]; !ok {
		// Pick the first member as leader
		for id := range g.members {
			g.leader = id
			break
		}
	}

	g.logger.Debug("rebalance complete",
		"generation", g.generation,
		"protocol", g.protocol,
		"leader", g.leader,
		"nMembers", len(g.members))

	// Send JoinGroup responses to all waiting members
	for _, m := range g.members {
		if m.waitJoinCh == nil {
			continue
		}
		resp := &kmsg.JoinGroupResponse{Version: m.joinVersion}
		g.fillJoinResponse(m.memberID, resp)
		m.waitJoinCh <- groupResponse{resp: resp}
		m.waitJoinCh = nil
		g.startSessionTimer(m)
	}

	// Start pending sync timeout
	maxTimeout := g.maxRebalanceTimeout()
	g.pendingSyncTimer = time.AfterFunc(maxTimeout, func() {
		g.timerControl(func() {
			// Remove members that haven't synced
			for id, m := range g.members {
				if m.waitSyncCh == nil && m.assignment == nil {
					// Member hasn't synced and has no assignment
					g.logger.Debug("removing member that didn't sync", "member", id)
					g.removeMemberDirect(m)
				}
			}
			if len(g.members) == 0 {
				g.state = GroupEmpty
				g.generation++
			} else {
				g.rebalance()
			}
		})
	})
}

// electProtocol uses Kafka's voting algorithm to elect the group protocol.
func (g *Group) electProtocol() string {
	// Find candidate protocols (supported by ALL members)
	candidates := make(map[string]struct{})
	for proto, count := range g.protocols {
		if count == len(g.members) {
			candidates[proto] = struct{}{}
		}
	}

	// Each member votes for their most-preferred candidate
	votes := make(map[string]int)
	for _, m := range g.members {
		for _, p := range m.protocols {
			if _, ok := candidates[p.Name]; ok {
				votes[p.Name]++
				break // first preferred = vote
			}
		}
	}

	// Pick the protocol with most votes
	var best string
	var bestVotes int
	for proto, v := range votes {
		if v > bestVotes {
			best = proto
			bestVotes = v
		}
	}
	return best
}

// fillJoinResponse populates a JoinGroup response for a member.
func (g *Group) fillJoinResponse(memberID string, resp *kmsg.JoinGroupResponse) {
	resp.Generation = g.generation
	pt := g.protocolType
	resp.ProtocolType = &pt
	prot := g.protocol
	resp.Protocol = &prot
	resp.LeaderID = g.leader
	resp.MemberID = memberID

	// Only the leader gets the full member list
	if memberID == g.leader {
		for _, m := range g.members {
			member := kmsg.JoinGroupResponseMember{
				MemberID: m.memberID,
			}
			if m.instanceID != nil {
				member.InstanceID = m.instanceID
			}
			// Include the member's metadata for the elected protocol
			for _, p := range m.protocols {
				if p.Name == g.protocol {
					member.ProtocolMetadata = p.Metadata
					break
				}
			}
			resp.Members = append(resp.Members, member)
		}
	}
}

// protocolsMatch checks if a member's protocols are compatible with the group.
func (g *Group) protocolsMatch(protocolType string, protocols []kmsg.JoinGroupRequestProtocol) bool {
	// Empty group: any protocol type is fine
	if len(g.members) == 0 && len(g.pending) == 0 {
		return true
	}
	// Protocol type must match
	if g.protocolType != "" && protocolType != g.protocolType {
		return false
	}
	// No existing protocols: anything goes
	if len(g.protocols) == 0 {
		return true
	}
	// Must support at least one protocol supported by ALL current members
	for _, p := range protocols {
		if g.protocols[p.Name] == len(g.members) {
			return true
		}
	}
	return false
}

// ---- SyncGroup ----

func (g *Group) handleSyncGroup(req *kmsg.SyncGroupRequest, respCh chan groupResponse) kmsg.Response {
	resp := req.ResponseKind().(*kmsg.SyncGroupResponse)

	m, ok := g.members[req.MemberID]
	if !ok {
		resp.ErrorCode = kerr.UnknownMemberID.Code
		return resp
	}

	// Validate instance ID for static members
	if req.InstanceID != nil && *req.InstanceID != "" {
		if err := g.validateInstanceID(req.InstanceID, req.MemberID); err != nil {
			resp.ErrorCode = err.Code
			return resp
		}
	}

	if req.Generation != g.generation {
		resp.ErrorCode = kerr.IllegalGeneration.Code
		return resp
	}

	switch g.state {
	case GroupCompletingRebalance:
		m.waitSyncCh = respCh
		m.syncVersion = req.Version

		if req.MemberID == g.leader {
			g.completeLeaderSync(req)
		}
		return nil // response sent later

	case GroupStable:
		// Late sync: return current assignment
		resp.MemberAssignment = m.assignment
		pt := g.protocolType
		resp.ProtocolType = &pt
		prot := g.protocol
		resp.Protocol = &prot
		g.startSessionTimer(m)
		return resp

	case GroupPreparingRebalance:
		resp.ErrorCode = kerr.RebalanceInProgress.Code
		return resp

	default:
		resp.ErrorCode = kerr.UnknownMemberID.Code
		return resp
	}
}

// completeLeaderSync processes the leader's SyncGroup and unblocks all followers.
func (g *Group) completeLeaderSync(req *kmsg.SyncGroupRequest) {
	// Clear all assignments
	for _, m := range g.members {
		m.assignment = nil
	}

	// Apply leader's assignments
	for _, a := range req.GroupAssignment {
		m, ok := g.members[a.MemberID]
		if !ok {
			continue
		}
		m.assignment = a.MemberAssignment
	}

	if g.pendingSyncTimer != nil {
		g.pendingSyncTimer.Stop()
		g.pendingSyncTimer = nil
	}

	g.state = GroupStable

	// Send SyncGroup responses to all waiting members
	for _, m := range g.members {
		if m.waitSyncCh == nil {
			continue
		}
		resp := &kmsg.SyncGroupResponse{
			Version:          m.syncVersion,
			MemberAssignment: m.assignment,
		}
		pt := g.protocolType
		resp.ProtocolType = &pt
		prot := g.protocol
		resp.Protocol = &prot
		m.waitSyncCh <- groupResponse{resp: resp}
		m.waitSyncCh = nil
		g.startSessionTimer(m)
	}

	g.logger.Debug("leader sync complete", "generation", g.generation)
}

// ---- Heartbeat ----

func (g *Group) handleHeartbeat(req *kmsg.HeartbeatRequest) kmsg.Response {
	resp := req.ResponseKind().(*kmsg.HeartbeatResponse)

	m, ok := g.members[req.MemberID]
	if !ok {
		resp.ErrorCode = kerr.UnknownMemberID.Code
		return resp
	}

	// Validate instance ID for static members
	if req.InstanceID != nil && *req.InstanceID != "" {
		if err := g.validateInstanceID(req.InstanceID, req.MemberID); err != nil {
			resp.ErrorCode = err.Code
			return resp
		}
	}

	if req.Generation != g.generation {
		resp.ErrorCode = kerr.IllegalGeneration.Code
		return resp
	}

	// Reset session timer
	g.startSessionTimer(m)

	switch g.state {
	case GroupPreparingRebalance:
		resp.ErrorCode = kerr.RebalanceInProgress.Code
	case GroupCompletingRebalance, GroupStable:
		// Success
	case GroupEmpty, GroupDead:
		resp.ErrorCode = kerr.UnknownMemberID.Code
	}

	return resp
}

// ---- LeaveGroup ----

func (g *Group) handleLeaveGroup(req *kmsg.LeaveGroupRequest) kmsg.Response {
	resp := req.ResponseKind().(*kmsg.LeaveGroupResponse)

	// v0-v2: single member leave (use MemberID field)
	if req.Version < 3 {
		if m, ok := g.members[req.MemberID]; ok {
			g.removeMemberAndRebalance(m)
		} else if _, ok := g.pending[req.MemberID]; ok {
			g.removePending(req.MemberID)
		} else {
			resp.ErrorCode = kerr.UnknownMemberID.Code
		}
		return resp
	}

	// v3+: batch leave
	for _, rm := range req.Members {
		mresp := kmsg.NewLeaveGroupResponseMember()
		mresp.MemberID = rm.MemberID
		if rm.InstanceID != nil {
			mresp.InstanceID = rm.InstanceID
		}

		memberID := rm.MemberID
		// Resolve memberID from instanceID for static members
		if rm.InstanceID != nil && *rm.InstanceID != "" {
			resolvedID, ok := g.staticMembers[*rm.InstanceID]
			if !ok {
				mresp.ErrorCode = kerr.UnknownMemberID.Code
				resp.Members = append(resp.Members, mresp)
				continue
			}
			memberID = resolvedID
			mresp.MemberID = memberID
		}

		if m, ok := g.members[memberID]; ok {
			g.removeMemberAndRebalance(m)
		} else if _, ok := g.pending[memberID]; ok {
			g.removePending(memberID)
		} else {
			mresp.ErrorCode = kerr.UnknownMemberID.Code
		}

		resp.Members = append(resp.Members, mresp)
	}

	return resp
}

// removePending removes a pending member.
func (g *Group) removePending(memberID string) {
	m, ok := g.pending[memberID]
	if !ok {
		return
	}
	if m.sessionTimer != nil {
		m.sessionTimer.Stop()
	}
	if m.instanceID != nil {
		if g.staticMembers[*m.instanceID] == m.memberID {
			delete(g.staticMembers, *m.instanceID)
		}
	}
	delete(g.pending, memberID)
}

// ---- OffsetCommit (handled in group goroutine) ----

func (g *Group) handleOffsetCommit(req *kmsg.OffsetCommitRequest) kmsg.Response {
	resp := req.ResponseKind().(*kmsg.OffsetCommitResponse)

	// Check if the group has the member
	var errCode int16
	if req.MemberID != "" {
		if _, ok := g.members[req.MemberID]; !ok {
			errCode = kerr.UnknownMemberID.Code
		} else if req.Generation != g.generation {
			errCode = kerr.IllegalGeneration.Code
		}
	}
	// Simple/admin commit (empty member ID) is always allowed

	for _, rt := range req.Topics {
		rtResp := kmsg.NewOffsetCommitResponseTopic()
		rtResp.Topic = rt.Topic
		for _, rp := range rt.Partitions {
			rpResp := kmsg.NewOffsetCommitResponseTopicPartition()
			rpResp.Partition = rp.Partition

			if errCode != 0 {
				rpResp.ErrorCode = errCode
			} else {
				g.offsets[TopicPartition{Topic: rt.Topic, Partition: rp.Partition}] = CommittedOffset{
					Offset:      rp.Offset,
					LeaderEpoch: rp.LeaderEpoch,
					Metadata:    derefStr(rp.Metadata),
					CommitTime:  time.Now(),
				}
				// Persist to metadata.log (buffered, no fsync — losing a few offsets on crash is OK)
				if g.metaLog != nil {
					entry := metadata.MarshalOffsetCommit(&metadata.OffsetCommitEntry{
						Group:     g.id,
						Topic:     rt.Topic,
						Partition: rp.Partition,
						Offset:    rp.Offset,
						Metadata:  derefStr(rp.Metadata),
					})
					g.metaLog.Append(entry) //nolint:errcheck
				}
			}
			rtResp.Partitions = append(rtResp.Partitions, rpResp)
		}
		resp.Topics = append(resp.Topics, rtResp)
	}
	return resp
}

// ---- OffsetFetch (handled in group goroutine) ----

func (g *Group) handleOffsetFetch(req *kmsg.OffsetFetchRequest) kmsg.Response {
	resp := req.ResponseKind().(*kmsg.OffsetFetchResponse)

	// v8+ uses the Groups array format
	if req.Version >= 8 {
		for _, rg := range req.Groups {
			gResp := kmsg.NewOffsetFetchResponseGroup()
			gResp.Group = rg.Group

			if rg.Group != g.id {
				gResp.ErrorCode = kerr.GroupIDNotFound.Code
				resp.Groups = append(resp.Groups, gResp)
				continue
			}

			if len(rg.Topics) == 0 {
				// Fetch all committed offsets
				topicMap := make(map[string]*kmsg.OffsetFetchResponseGroupTopic)
				for tp, co := range g.offsets {
					tResp, ok := topicMap[tp.Topic]
					if !ok {
						t := kmsg.NewOffsetFetchResponseGroupTopic()
						t.Topic = tp.Topic
						tResp = &t
						topicMap[tp.Topic] = tResp
					}
					pResp := kmsg.NewOffsetFetchResponseGroupTopicPartition()
					pResp.Partition = tp.Partition
					pResp.Offset = co.Offset
					pResp.LeaderEpoch = co.LeaderEpoch
					pResp.Metadata = &co.Metadata
					tResp.Partitions = append(tResp.Partitions, pResp)
				}
				for _, t := range topicMap {
					gResp.Topics = append(gResp.Topics, *t)
				}
			} else {
				for _, rt := range rg.Topics {
					tResp := kmsg.NewOffsetFetchResponseGroupTopic()
					tResp.Topic = rt.Topic
					for _, p := range rt.Partitions {
						pResp := kmsg.NewOffsetFetchResponseGroupTopicPartition()
						pResp.Partition = p
						co, ok := g.offsets[TopicPartition{Topic: rt.Topic, Partition: p}]
						if ok {
							pResp.Offset = co.Offset
							pResp.LeaderEpoch = co.LeaderEpoch
							pResp.Metadata = &co.Metadata
						} else {
							pResp.Offset = -1
							pResp.LeaderEpoch = -1
						}
						tResp.Partitions = append(tResp.Partitions, pResp)
					}
					gResp.Topics = append(gResp.Topics, tResp)
				}
			}
			resp.Groups = append(resp.Groups, gResp)
		}
		return resp
	}

	// v0-v7: single group format
	if len(req.Topics) == 0 {
		// Fetch all committed offsets
		topicMap := make(map[string]*kmsg.OffsetFetchResponseTopic)
		for tp, co := range g.offsets {
			tResp, ok := topicMap[tp.Topic]
			if !ok {
				t := kmsg.NewOffsetFetchResponseTopic()
				t.Topic = tp.Topic
				tResp = &t
				topicMap[tp.Topic] = tResp
			}
			pResp := kmsg.NewOffsetFetchResponseTopicPartition()
			pResp.Partition = tp.Partition
			pResp.Offset = co.Offset
			pResp.LeaderEpoch = co.LeaderEpoch
			pResp.Metadata = &co.Metadata
			tResp.Partitions = append(tResp.Partitions, pResp)
		}
		for _, t := range topicMap {
			resp.Topics = append(resp.Topics, *t)
		}
	} else {
		for _, rt := range req.Topics {
			tResp := kmsg.NewOffsetFetchResponseTopic()
			tResp.Topic = rt.Topic
			for _, p := range rt.Partitions {
				pResp := kmsg.NewOffsetFetchResponseTopicPartition()
				pResp.Partition = p
				co, ok := g.offsets[TopicPartition{Topic: rt.Topic, Partition: p}]
				if ok {
					pResp.Offset = co.Offset
					pResp.LeaderEpoch = co.LeaderEpoch
					pResp.Metadata = &co.Metadata
				} else {
					pResp.Offset = -1
					pResp.LeaderEpoch = -1
				}
				tResp.Partitions = append(tResp.Partitions, pResp)
			}
			resp.Topics = append(resp.Topics, tResp)
		}
	}

	return resp
}

// ---- OffsetDelete (handled in group goroutine) ----

func (g *Group) handleOffsetDelete(req *kmsg.OffsetDeleteRequest) kmsg.Response {
	resp := req.ResponseKind().(*kmsg.OffsetDeleteResponse)

	// Check group state
	switch g.state {
	case GroupDead:
		resp.ErrorCode = kerr.GroupIDNotFound.Code
		return resp
	case GroupEmpty:
		// Empty group: delete is allowed for all partitions
	case GroupPreparingRebalance, GroupCompletingRebalance, GroupStable:
		// Non-empty group with non-consumer protocol type: reject
		if g.protocolType != "consumer" {
			resp.ErrorCode = kerr.NonEmptyGroup.Code
			return resp
		}
		// For consumer groups: check subscribed topics
	}

	// Collect subscribed topics from all active members
	subTopics := make(map[string]struct{})
	if g.state != GroupEmpty {
		for _, m := range g.members {
			for _, proto := range m.protocols {
				var meta kmsg.ConsumerMemberMetadata
				if err := meta.ReadFrom(proto.Metadata); err == nil {
					for _, topic := range meta.Topics {
						subTopics[topic] = struct{}{}
					}
				}
			}
		}
	}

	for _, rt := range req.Topics {
		rtResp := kmsg.NewOffsetDeleteResponseTopic()
		rtResp.Topic = rt.Topic
		for _, rp := range rt.Partitions {
			rpResp := kmsg.NewOffsetDeleteResponseTopicPartition()
			rpResp.Partition = rp.Partition

			if _, ok := subTopics[rt.Topic]; ok {
				rpResp.ErrorCode = kerr.GroupSubscribedToTopic.Code
			} else {
				delete(g.offsets, TopicPartition{Topic: rt.Topic, Partition: rp.Partition})
			}
			rtResp.Partitions = append(rtResp.Partitions, rpResp)
		}
		resp.Topics = append(resp.Topics, rtResp)
	}
	return resp
}

// ---- Session Timeout ----

func (g *Group) startSessionTimer(m *groupMember) {
	if m.sessionTimer != nil {
		m.sessionTimer.Stop()
	}
	timeout := time.Duration(m.sessionTimeoutMs) * time.Millisecond
	if timeout <= 0 {
		timeout = 45 * time.Second
	}
	m.lastSeen = time.Now()
	m.sessionTimer = time.AfterFunc(timeout, func() {
		g.timerControl(func() {
			if m2, ok := g.members[m.memberID]; ok && m2 == m {
				if time.Since(m.lastSeen) >= timeout {
					g.logger.Debug("session timeout", "member", m.memberID)
					g.removeMemberAndRebalance(m)
				}
			}
		})
	})
}

// timerControl sends a function to the group's controlCh to be executed in the
// manage goroutine. Timer callbacks must use this to mutate group state safely.
func (g *Group) timerControl(fn func()) {
	select {
	case <-g.quitCh:
	case <-g.shutdownCh:
	case g.controlCh <- fn:
	}
}

// validateInstanceID checks that the instanceID maps to the expected memberID.
func (g *Group) validateInstanceID(instanceID *string, memberID string) *kerr.Error {
	if instanceID == nil {
		return nil
	}
	knownMID, ok := g.staticMembers[*instanceID]
	if !ok {
		return kerr.UnknownMemberID
	}
	if knownMID != memberID {
		return kerr.FencedInstanceID
	}
	return nil
}

// ApplyTxnOffset applies a pending transactional offset commit.
// Must be called within the group goroutine (via Control).
func (g *Group) ApplyTxnOffset(tp TopicPartition, po PendingTxnOffset) {
	g.offsets[tp] = CommittedOffset{
		Offset:      po.Offset,
		LeaderEpoch: po.LeaderEpoch,
		Metadata:    po.Metadata,
		CommitTime:  time.Now(),
	}
	// Persist to metadata.log
	if g.metaLog != nil {
		entry := metadata.MarshalOffsetCommit(&metadata.OffsetCommitEntry{
			Group:     g.id,
			Topic:     tp.Topic,
			Partition: tp.Partition,
			Offset:    po.Offset,
			Metadata:  po.Metadata,
		})
		g.metaLog.Append(entry) //nolint:errcheck
	}
}

// derefStr dereferences a *string, returning empty string if nil.
func derefStr(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// ---- State accessors (safe to call from any goroutine via Control()) ----

// ID returns the group ID.
func (g *Group) ID() string { return g.id }

// State returns the current group state.
func (g *Group) GetState() GroupState { return g.state }

// GroupInfo holds a snapshot of group state for DescribeGroups/ListGroups.
type GroupInfo struct {
	State        string
	ProtocolType string
	Protocol     string
	Members      []GroupMemberInfo
}

// GroupMemberInfo holds member info for DescribeGroups.
type GroupMemberInfo struct {
	MemberID         string
	InstanceID       *string
	ProtocolMetadata []byte
	Assignment       []byte
}

// GetCommittedOffsets returns a copy of all committed offsets.
// Safe to call from any goroutine (uses Control to access group state).
func (g *Group) GetCommittedOffsets() map[TopicPartition]CommittedOffset {
	var result map[TopicPartition]CommittedOffset
	g.Control(func() {
		result = make(map[TopicPartition]CommittedOffset, len(g.offsets))
		for k, v := range g.offsets {
			result[k] = v
		}
	})
	return result
}

// Describe returns a snapshot of the group state. Must be called from the
// group goroutine (via Control() or within handleRequest).
func (g *Group) Describe() GroupInfo {
	info := GroupInfo{
		State:        g.state.String(),
		ProtocolType: g.protocolType,
		Protocol:     g.protocol,
	}
	for _, m := range g.members {
		mi := GroupMemberInfo{
			MemberID:   m.memberID,
			InstanceID: m.instanceID,
			Assignment: m.assignment,
		}
		// Include the member's metadata for the elected protocol
		for _, p := range m.protocols {
			if p.Name == g.protocol {
				mi.ProtocolMetadata = p.Metadata
				break
			}
		}
		info.Members = append(info.Members, mi)
	}
	return info
}
