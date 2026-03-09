package cluster

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/klaudworks/klite/internal/clock"
	"github.com/klaudworks/klite/internal/metadata"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type GroupState int

const (
	GroupEmpty               GroupState = iota
	GroupPreparingRebalance             // waiting for members to join
	GroupCompletingRebalance            // waiting for leader's SyncGroup
	GroupStable                         // normal operation
	GroupDead                           // being removed
)

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

type groupRequest struct {
	req    kmsg.Request
	respCh chan groupResponse
}

type groupResponse struct {
	resp kmsg.Response
	err  error
}

type groupMember struct {
	memberID   string
	instanceID *string

	protocolType     string
	protocols        []kmsg.JoinGroupRequestProtocol
	sessionTimeoutMs int32
	rebalanceTimeout int32
	joinVersion      int16 // version of the JoinGroup request for response encoding

	assignment []byte

	waitJoinCh  chan groupResponse
	waitSyncCh  chan groupResponse
	syncVersion int16 // version of the SyncGroup request for response encoding

	sessionTimer *clock.Timer
	lastSeen     time.Time
}

// Group is a consumer group. All state is mutated in the manage() goroutine.
type Group struct {
	id    string
	state GroupState

	protocolType string // e.g., "consumer"
	protocol     string
	protocols    map[string]int
	generation   int32
	leader       string // member ID of the leader

	members       map[string]*groupMember
	pending       map[string]*groupMember
	staticMembers map[string]string

	offsets  map[TopicPartition]CommittedOffset
	nJoining int

	rebalanceTimer   *clock.Timer
	pendingSyncTimer *clock.Timer

	reqCh     chan groupRequest
	controlCh chan func()
	quitCh    chan struct{}

	shutdownCh <-chan struct{}
	logger     *slog.Logger
	metaLog    *metadata.Log
	clk        clock.Clock
}

type TopicPartition struct {
	Topic     string
	Partition int32
}

type CommittedOffset struct {
	Offset      int64
	LeaderEpoch int32
	Metadata    string
	CommitTime  time.Time
}

func NewGroup(id string, shutdownCh <-chan struct{}, logger *slog.Logger, clk clock.Clock) *Group {
	if clk == nil {
		clk = clock.RealClock{}
	}
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
		clk:           clk,
	}
	go g.manage()
	return g
}

func (g *Group) SetMetadataLog(ml *metadata.Log) {
	g.metaLog = ml
}

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

func (g *Group) Stop() {
	select {
	case <-g.quitCh:
	default:
		close(g.quitCh)
	}
}

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

	if resp != nil {
		greq.respCh <- groupResponse{resp: resp}
	}
}

func generateMemberID(clientID string, instanceID *string) string {
	prefix := clientID
	if instanceID != nil {
		prefix = *instanceID
	}
	return prefix + "-" + uuid.New().String()
}

func (g *Group) handleJoinGroup(req *kmsg.JoinGroupRequest, respCh chan groupResponse) kmsg.Response {
	resp := req.ResponseKind().(*kmsg.JoinGroupResponse)

	if g.state == GroupDead {
		resp.ErrorCode = kerr.CoordinatorNotAvailable.Code
		return resp
	}

	if req.ProtocolType != "" && !g.protocolsMatch(req.ProtocolType, req.Protocols) {
		resp.ErrorCode = kerr.InconsistentGroupProtocol.Code
		return resp
	}

	if len(req.Protocols) == 0 {
		resp.ErrorCode = kerr.InconsistentGroupProtocol.Code
		return resp
	}

	if req.MemberID == "" {
		return g.handleJoinNewMember(req, resp, respCh)
	}

	if m, ok := g.pending[req.MemberID]; ok {
		return g.handleJoinPendingMember(m, req, resp, respCh)
	}

	if m, ok := g.members[req.MemberID]; ok {
		return g.handleJoinExistingMember(m, req, resp, respCh)
	}

	resp.ErrorCode = kerr.UnknownMemberID.Code
	return resp
}

func (g *Group) handleJoinNewMember(req *kmsg.JoinGroupRequest, resp *kmsg.JoinGroupResponse, respCh chan groupResponse) kmsg.Response {
	if req.InstanceID != nil && *req.InstanceID != "" {
		if oldMemberID, ok := g.staticMembers[*req.InstanceID]; ok {
			return g.replaceStaticMember(oldMemberID, req, resp, respCh)
		}
	}

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
	return nil // response sent later
}

func (g *Group) handleJoinPendingMember(m *groupMember, req *kmsg.JoinGroupRequest, resp *kmsg.JoinGroupResponse, respCh chan groupResponse) kmsg.Response {
	m.protocolType = req.ProtocolType
	m.protocols = req.Protocols
	m.sessionTimeoutMs = req.SessionTimeoutMillis
	m.rebalanceTimeout = req.RebalanceTimeoutMillis
	m.joinVersion = req.Version

	delete(g.pending, m.memberID)
	if m.sessionTimer != nil {
		m.sessionTimer.Stop()
		m.sessionTimer = nil
	}

	g.addMemberAndRebalance(m, respCh)
	return nil // response sent later
}

func (g *Group) handleJoinExistingMember(m *groupMember, req *kmsg.JoinGroupRequest, resp *kmsg.JoinGroupResponse, respCh chan groupResponse) kmsg.Response {
	if req.InstanceID != nil && *req.InstanceID != "" {
		if err := g.validateInstanceID(req.InstanceID, req.MemberID); err != nil {
			resp.ErrorCode = err.Code
			return resp
		}
	}

	g.updateMemberProtocols(m, req)

	switch g.state {
	case GroupPreparingRebalance:
		g.updateMemberAndRebalance(m, respCh)
		return nil

	case GroupCompletingRebalance, GroupStable:
		g.updateMemberAndRebalance(m, respCh)
		return nil

	case GroupEmpty:
		resp.ErrorCode = kerr.UnknownMemberID.Code
		return resp

	default:
		resp.ErrorCode = kerr.UnknownMemberID.Code
		return resp
	}
}

func (g *Group) replaceStaticMember(oldMemberID string, req *kmsg.JoinGroupRequest, resp *kmsg.JoinGroupResponse, respCh chan groupResponse) kmsg.Response {
	oldMember, ok := g.members[oldMemberID]
	if !ok {
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

	newMemberID := generateMemberID("", req.InstanceID)
	savedAssignment := oldMember.assignment

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

	g.removeMemberDirect(oldMember)

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

	if g.state == GroupStable && sameProtocols(oldMember, newMember) {
		g.addMemberDirect(newMember)
		if g.leader == oldMemberID {
			g.leader = newMemberID
		}
		g.startSessionTimer(newMember)

		g.fillJoinResponse(newMemberID, resp)
		resp.MemberID = newMemberID
		return resp
	}

	g.addMemberAndRebalance(newMember, respCh)
	return nil
}

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

func (g *Group) addPending(m *groupMember) {
	g.pending[m.memberID] = m
	if m.instanceID != nil {
		g.staticMembers[*m.instanceID] = m.memberID
	}
	timeout := time.Duration(m.sessionTimeoutMs) * time.Millisecond
	if timeout <= 0 {
		timeout = 45 * time.Second
	}
	m.sessionTimer = g.clk.AfterFunc(timeout, func() {
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

func (g *Group) addMemberAndRebalance(m *groupMember, respCh chan groupResponse) {
	g.addMemberDirect(m)
	m.waitJoinCh = respCh
	g.nJoining++
	g.rebalance()
}

func (g *Group) updateMemberAndRebalance(m *groupMember, respCh chan groupResponse) {
	if m.waitJoinCh != nil {
		g.nJoining--
	}
	m.waitJoinCh = respCh
	g.nJoining++
	g.rebalance()
}

func (g *Group) addMemberDirect(m *groupMember) {
	g.members[m.memberID] = m
	if m.instanceID != nil {
		g.staticMembers[*m.instanceID] = m.memberID
	}
	for _, p := range m.protocols {
		g.protocols[p.Name]++
	}
	if g.protocolType == "" {
		g.protocolType = m.protocolType
	}
}

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

func (g *Group) updateMemberProtocols(m *groupMember, req *kmsg.JoinGroupRequest) {
	for _, p := range m.protocols {
		g.protocols[p.Name]--
		if g.protocols[p.Name] <= 0 {
			delete(g.protocols, p.Name)
		}
	}
	m.protocols = req.Protocols
	m.sessionTimeoutMs = req.SessionTimeoutMillis
	m.rebalanceTimeout = req.RebalanceTimeoutMillis
	m.joinVersion = req.Version
	for _, p := range m.protocols {
		g.protocols[p.Name]++
	}
}

func (g *Group) rebalance() {
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

	if g.nJoining >= len(g.members) {
		g.completeRebalance()
		return
	}

	maxTimeout := g.maxRebalanceTimeout()
	if g.rebalanceTimer != nil {
		g.rebalanceTimer.Stop()
	}
	g.rebalanceTimer = g.clk.AfterFunc(maxTimeout, func() {
		g.timerControl(g.completeRebalance)
	})
}

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

func (g *Group) completeRebalance() {
	if g.rebalanceTimer != nil {
		g.rebalanceTimer.Stop()
		g.rebalanceTimer = nil
	}

	g.nJoining = 0

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

	g.protocol = g.electProtocol()

	if _, ok := g.members[g.leader]; !ok {
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

	maxTimeout := g.maxRebalanceTimeout()
	g.pendingSyncTimer = g.clk.AfterFunc(maxTimeout, func() {
		g.timerControl(func() {
			for id, m := range g.members {
				if m.waitSyncCh == nil && m.assignment == nil {
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

func (g *Group) electProtocol() string {
	candidates := make(map[string]struct{})
	for proto, count := range g.protocols {
		if count == len(g.members) {
			candidates[proto] = struct{}{}
		}
	}

	votes := make(map[string]int)
	for _, m := range g.members {
		for _, p := range m.protocols {
			if _, ok := candidates[p.Name]; ok {
				votes[p.Name]++
				break
			}
		}
	}

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

func (g *Group) fillJoinResponse(memberID string, resp *kmsg.JoinGroupResponse) {
	resp.Generation = g.generation
	pt := g.protocolType
	resp.ProtocolType = &pt
	prot := g.protocol
	resp.Protocol = &prot
	resp.LeaderID = g.leader
	resp.MemberID = memberID

	if memberID == g.leader {
		for _, m := range g.members {
			member := kmsg.JoinGroupResponseMember{
				MemberID: m.memberID,
			}
			if m.instanceID != nil {
				member.InstanceID = m.instanceID
			}
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

func (g *Group) protocolsMatch(protocolType string, protocols []kmsg.JoinGroupRequestProtocol) bool {
	if len(g.members) == 0 && len(g.pending) == 0 {
		return true
	}
	if g.protocolType != "" && protocolType != g.protocolType {
		return false
	}
	if len(g.protocols) == 0 {
		return true
	}
	for _, p := range protocols {
		if g.protocols[p.Name] == len(g.members) {
			return true
		}
	}
	return false
}

func (g *Group) handleSyncGroup(req *kmsg.SyncGroupRequest, respCh chan groupResponse) kmsg.Response {
	resp := req.ResponseKind().(*kmsg.SyncGroupResponse)

	m, ok := g.members[req.MemberID]
	if !ok {
		resp.ErrorCode = kerr.UnknownMemberID.Code
		return resp
	}

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

func (g *Group) completeLeaderSync(req *kmsg.SyncGroupRequest) {
	for _, m := range g.members {
		m.assignment = nil
	}

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

func (g *Group) handleHeartbeat(req *kmsg.HeartbeatRequest) kmsg.Response {
	resp := req.ResponseKind().(*kmsg.HeartbeatResponse)

	m, ok := g.members[req.MemberID]
	if !ok {
		resp.ErrorCode = kerr.UnknownMemberID.Code
		return resp
	}

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

	g.startSessionTimer(m)

	switch g.state {
	case GroupPreparingRebalance:
		resp.ErrorCode = kerr.RebalanceInProgress.Code
	case GroupCompletingRebalance, GroupStable:
	case GroupEmpty, GroupDead:
		resp.ErrorCode = kerr.UnknownMemberID.Code
	}

	return resp
}

func (g *Group) handleLeaveGroup(req *kmsg.LeaveGroupRequest) kmsg.Response {
	resp := req.ResponseKind().(*kmsg.LeaveGroupResponse)

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

	for _, rm := range req.Members {
		mresp := kmsg.NewLeaveGroupResponseMember()
		mresp.MemberID = rm.MemberID
		if rm.InstanceID != nil {
			mresp.InstanceID = rm.InstanceID
		}

		memberID := rm.MemberID
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

func (g *Group) handleOffsetCommit(req *kmsg.OffsetCommitRequest) kmsg.Response {
	resp := req.ResponseKind().(*kmsg.OffsetCommitResponse)

	var errCode int16
	if req.MemberID != "" {
		if _, ok := g.members[req.MemberID]; !ok {
			errCode = kerr.UnknownMemberID.Code
		} else if req.Generation != g.generation {
			errCode = kerr.IllegalGeneration.Code
		}
	}

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
					CommitTime:  g.clk.Now(),
				}
				if g.metaLog != nil {
					entry := metadata.MarshalOffsetCommit(&metadata.OffsetCommitEntry{
						Group:     g.id,
						Topic:     rt.Topic,
						Partition: rp.Partition,
						Offset:    rp.Offset,
						Metadata:  derefStr(rp.Metadata),
					})
					if err := g.metaLog.Append(entry); err != nil {
						g.logger.Warn("metadata.log: failed to persist OffsetCommit",
							"group", g.id, "topic", rt.Topic, "partition", rp.Partition, "err", err)
					}
				}
			}
			rtResp.Partitions = append(rtResp.Partitions, rpResp)
		}
		resp.Topics = append(resp.Topics, rtResp)
	}
	return resp
}

type OffsetQuery struct {
	Topics []OffsetQueryTopic
}

type OffsetQueryTopic struct {
	Topic      string
	Partitions []int32
}

type OffsetResult struct {
	Topic       string
	Partition   int32
	Offset      int64
	LeaderEpoch int32
	Metadata    string
}

func (g *Group) FetchOffsets(q OffsetQuery) []OffsetResult {
	if len(q.Topics) == 0 {
		results := make([]OffsetResult, 0, len(g.offsets))
		for tp, co := range g.offsets {
			results = append(results, OffsetResult{
				Topic:       tp.Topic,
				Partition:   tp.Partition,
				Offset:      co.Offset,
				LeaderEpoch: co.LeaderEpoch,
				Metadata:    co.Metadata,
			})
		}
		return results
	}

	var results []OffsetResult
	for _, qt := range q.Topics {
		for _, p := range qt.Partitions {
			co, ok := g.offsets[TopicPartition{Topic: qt.Topic, Partition: p}]
			if ok {
				results = append(results, OffsetResult{
					Topic:       qt.Topic,
					Partition:   p,
					Offset:      co.Offset,
					LeaderEpoch: co.LeaderEpoch,
					Metadata:    co.Metadata,
				})
			} else {
				results = append(results, OffsetResult{
					Topic:       qt.Topic,
					Partition:   p,
					Offset:      -1,
					LeaderEpoch: -1,
				})
			}
		}
	}
	return results
}

func (g *Group) handleOffsetFetch(req *kmsg.OffsetFetchRequest) kmsg.Response {
	resp := req.ResponseKind().(*kmsg.OffsetFetchResponse)

	if req.Version >= 8 {
		for _, rg := range req.Groups {
			gResp := kmsg.NewOffsetFetchResponseGroup()
			gResp.Group = rg.Group

			if rg.Group != g.id {
				gResp.ErrorCode = kerr.GroupIDNotFound.Code
				resp.Groups = append(resp.Groups, gResp)
				continue
			}

			q := offsetQueryFromGroupTopics(rg.Topics)
			results := g.FetchOffsets(q)
			gResp.Topics = groupTopicsFromResults(results)
			resp.Groups = append(resp.Groups, gResp)
		}
		return resp
	}

	q := offsetQueryFromV7Topics(req.Topics)
	results := g.FetchOffsets(q)
	resp.Topics = v7TopicsFromResults(results)
	return resp
}

func offsetQueryFromGroupTopics(topics []kmsg.OffsetFetchRequestGroupTopic) OffsetQuery {
	if len(topics) == 0 {
		return OffsetQuery{}
	}
	q := OffsetQuery{Topics: make([]OffsetQueryTopic, len(topics))}
	for i, t := range topics {
		q.Topics[i] = OffsetQueryTopic{Topic: t.Topic, Partitions: t.Partitions}
	}
	return q
}

func offsetQueryFromV7Topics(topics []kmsg.OffsetFetchRequestTopic) OffsetQuery {
	if len(topics) == 0 {
		return OffsetQuery{}
	}
	q := OffsetQuery{Topics: make([]OffsetQueryTopic, len(topics))}
	for i, t := range topics {
		q.Topics[i] = OffsetQueryTopic{Topic: t.Topic, Partitions: t.Partitions}
	}
	return q
}

func groupTopicsFromResults(results []OffsetResult) []kmsg.OffsetFetchResponseGroupTopic {
	topicMap := make(map[string][]kmsg.OffsetFetchResponseGroupTopicPartition)
	topicOrder := make([]string, 0)
	for _, r := range results {
		if _, seen := topicMap[r.Topic]; !seen {
			topicOrder = append(topicOrder, r.Topic)
		}
		pResp := kmsg.NewOffsetFetchResponseGroupTopicPartition()
		pResp.Partition = r.Partition
		pResp.Offset = r.Offset
		pResp.LeaderEpoch = r.LeaderEpoch
		pResp.Metadata = &r.Metadata
		topicMap[r.Topic] = append(topicMap[r.Topic], pResp)
	}
	out := make([]kmsg.OffsetFetchResponseGroupTopic, 0, len(topicOrder))
	for _, topic := range topicOrder {
		t := kmsg.NewOffsetFetchResponseGroupTopic()
		t.Topic = topic
		t.Partitions = topicMap[topic]
		out = append(out, t)
	}
	return out
}

func v7TopicsFromResults(results []OffsetResult) []kmsg.OffsetFetchResponseTopic {
	topicMap := make(map[string][]kmsg.OffsetFetchResponseTopicPartition)
	topicOrder := make([]string, 0)
	for _, r := range results {
		if _, seen := topicMap[r.Topic]; !seen {
			topicOrder = append(topicOrder, r.Topic)
		}
		pResp := kmsg.NewOffsetFetchResponseTopicPartition()
		pResp.Partition = r.Partition
		pResp.Offset = r.Offset
		pResp.LeaderEpoch = r.LeaderEpoch
		pResp.Metadata = &r.Metadata
		topicMap[r.Topic] = append(topicMap[r.Topic], pResp)
	}
	out := make([]kmsg.OffsetFetchResponseTopic, 0, len(topicOrder))
	for _, topic := range topicOrder {
		t := kmsg.NewOffsetFetchResponseTopic()
		t.Topic = topic
		t.Partitions = topicMap[topic]
		out = append(out, t)
	}
	return out
}

func (g *Group) handleOffsetDelete(req *kmsg.OffsetDeleteRequest) kmsg.Response {
	resp := req.ResponseKind().(*kmsg.OffsetDeleteResponse)

	switch g.state {
	case GroupDead:
		resp.ErrorCode = kerr.GroupIDNotFound.Code
		return resp
	case GroupEmpty:
	case GroupPreparingRebalance, GroupCompletingRebalance, GroupStable:
		if g.protocolType != "consumer" {
			resp.ErrorCode = kerr.NonEmptyGroup.Code
			return resp
		}
	}

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

func (g *Group) startSessionTimer(m *groupMember) {
	if m.sessionTimer != nil {
		m.sessionTimer.Stop()
	}
	timeout := time.Duration(m.sessionTimeoutMs) * time.Millisecond
	if timeout <= 0 {
		timeout = 45 * time.Second
	}
	m.lastSeen = g.clk.Now()
	m.sessionTimer = g.clk.AfterFunc(timeout, func() {
		g.timerControl(func() {
			if m2, ok := g.members[m.memberID]; ok && m2 == m {
				if g.clk.Since(m.lastSeen) >= timeout {
					g.logger.Debug("session timeout", "member", m.memberID)
					g.removeMemberAndRebalance(m)
				}
			}
		})
	})
}

func (g *Group) timerControl(fn func()) {
	select {
	case <-g.quitCh:
	case <-g.shutdownCh:
	case g.controlCh <- fn:
	}
}

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

func (g *Group) ApplyTxnOffset(tp TopicPartition, po PendingTxnOffset) {
	g.offsets[tp] = CommittedOffset{
		Offset:      po.Offset,
		LeaderEpoch: po.LeaderEpoch,
		Metadata:    po.Metadata,
		CommitTime:  g.clk.Now(),
	}
	if g.metaLog != nil {
		entry := metadata.MarshalOffsetCommit(&metadata.OffsetCommitEntry{
			Group:     g.id,
			Topic:     tp.Topic,
			Partition: tp.Partition,
			Offset:    po.Offset,
			Metadata:  po.Metadata,
		})
		if err := g.metaLog.Append(entry); err != nil {
			g.logger.Warn("metadata.log: failed to persist TxnOffsetCommit",
				"group", g.id, "topic", tp.Topic, "partition", tp.Partition, "err", err)
		}
	}
}

func derefStr(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func (g *Group) ID() string { return g.id }

type GroupInfo struct {
	State        string
	ProtocolType string
	Protocol     string
	Members      []GroupMemberInfo
}

type GroupMemberInfo struct {
	MemberID         string
	InstanceID       *string
	ProtocolMetadata []byte
	Assignment       []byte
}

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
