package cluster

import (
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/klaudworks/klite/internal/metadata"
	"github.com/klaudworks/klite/internal/wal"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// State holds all in-memory cluster state: topics, partitions, offsets, groups.
// Protected by a RWMutex for concurrent access.
type State struct {
	mu     sync.RWMutex
	topics map[string]*TopicData // topic name -> topic
	tnorms map[string]string     // normalized name -> actual topic name (collision detection)
	groups map[string]*Group     // group ID -> group
	cfg    Config

	shutdownCh <-chan struct{}
	logger     *slog.Logger

	// WAL-related state (Phase 3+)
	walWriter      *wal.Writer
	walIndex       *wal.Index
	ringBufferMem  int64 // global memory budget for ring buffers

	// Metadata log (Phase 3+)
	metaLog *metadata.Log
}

// Config holds cluster-level configuration relevant to state management.
type Config struct {
	NodeID            int32
	DefaultPartitions int
	AutoCreateTopics  bool
}

// NewState creates a new empty cluster state.
func NewState(cfg Config) *State {
	return &State{
		topics: make(map[string]*TopicData),
		tnorms: make(map[string]string),
		groups: make(map[string]*Group),
		cfg:    cfg,
		logger: slog.Default(),
	}
}

// SetShutdownCh sets the shutdown channel used by group goroutines.
func (s *State) SetShutdownCh(ch <-chan struct{}) {
	s.shutdownCh = ch
}

// SetLogger sets the logger used by group goroutines.
func (s *State) SetLogger(l *slog.Logger) {
	s.logger = l
}

// SetWALConfig configures the cluster state for WAL mode.
// All existing partitions get ring buffers initialized.
func (s *State) SetWALConfig(w *wal.Writer, idx *wal.Index, ringBufferMem int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.walWriter = w
	s.walIndex = idx
	s.ringBufferMem = ringBufferMem

	// Initialize ring buffers for all existing partitions
	totalPartitions := 0
	for _, td := range s.topics {
		totalPartitions += len(td.Partitions)
	}

	slots := CalcRingSlots(ringBufferMem, totalPartitions, 16*1024)

	for _, td := range s.topics {
		for _, pd := range td.Partitions {
			ring := NewRingBuffer(slots)
			pd.InitWAL(ring, w, idx)
		}
	}
}

// HasWAL returns whether WAL is configured.
func (s *State) HasWAL() bool {
	return s.walWriter != nil
}

// NormalizeTopicName normalizes a topic name for collision detection.
// Kafka considers topics that differ only in '.' vs '_' as colliding.
func NormalizeTopicName(name string) string {
	return strings.ReplaceAll(name, ".", "_")
}

// CheckCollision checks whether the given topic name collides with any existing
// topic after dot/underscore normalization. Returns the colliding existing topic
// name if a collision exists, or empty string if no collision.
// Caller must hold s.mu.RLock() or s.mu.Lock().
func (s *State) checkCollision(name string) string {
	normalized := NormalizeTopicName(name)
	if existing, ok := s.tnorms[normalized]; ok && existing != name {
		return existing
	}
	return ""
}

// GetTopic returns the topic data for the given name, or nil if not found.
// Caller must not modify the returned TopicData without holding appropriate locks.
func (s *State) GetTopic(name string) *TopicData {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.topics[name]
}

// GetAllTopics returns a snapshot of all current topics.
// The returned slice is safe to iterate without holding locks.
func (s *State) GetAllTopics() []*TopicData {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]*TopicData, 0, len(s.topics))
	for _, td := range s.topics {
		result = append(result, td)
	}
	return result
}

// GetTopicByID returns the topic data matching the given UUID, or nil if not found.
func (s *State) GetTopicByID(id [16]byte) *TopicData {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, td := range s.topics {
		if td.ID == id {
			return td
		}
	}
	return nil
}

// CreateTopic creates a new topic with the given name and partition count.
// Returns the created topic, or the existing topic if it already exists.
// The second return value is true if the topic was newly created.
func (s *State) CreateTopic(name string, numPartitions int) (*TopicData, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Double-checked: topic may have been created between RUnlock and Lock
	if td, ok := s.topics[name]; ok {
		return td, false
	}

	td := newTopicData(name, numPartitions, s.cfg.NodeID)
	s.topics[name] = td
	s.tnorms[NormalizeTopicName(name)] = name

	// Initialize WAL on new partitions if WAL is enabled
	if s.walWriter != nil {
		s.initPartitionsWAL(td)
	}

	return td, true
}

// CreateTopicWithConfigs creates a new topic with the given name, partition count,
// and configuration. Returns the created topic data and true if newly created.
// Returns (existing, false) if the topic already exists.
func (s *State) CreateTopicWithConfigs(name string, numPartitions int, configs map[string]string) (*TopicData, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if td, ok := s.topics[name]; ok {
		return td, false
	}

	td := newTopicData(name, numPartitions, s.cfg.NodeID)
	for k, v := range configs {
		td.Configs[k] = v
	}
	s.topics[name] = td
	s.tnorms[NormalizeTopicName(name)] = name

	// Initialize WAL on new partitions if WAL is enabled
	if s.walWriter != nil {
		s.initPartitionsWAL(td)
	}

	return td, true
}

// TopicExists returns whether a topic with the given name exists.
func (s *State) TopicExists(name string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.topics[name]
	return ok
}

// CheckTopicCollision checks whether the given topic name collides with any
// existing topic after dot/underscore normalization (e.g., foo.bar vs foo_bar).
// Returns the colliding topic name, or empty string if no collision.
func (s *State) CheckTopicCollision(name string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.checkCollision(name)
}

// DefaultPartitions returns the configured default partition count.
func (s *State) DefaultPartitions() int {
	return s.cfg.DefaultPartitions
}

// GetOrCreateTopic returns the existing topic, or creates it if auto-create
// is enabled. Returns (topic, created, error). Error is non-nil if the topic
// doesn't exist and auto-create is disabled.
func (s *State) GetOrCreateTopic(name string) (*TopicData, bool, error) {
	// Fast path: read lock
	s.mu.RLock()
	td, ok := s.topics[name]
	s.mu.RUnlock()
	if ok {
		return td, false, nil
	}

	if !s.cfg.AutoCreateTopics {
		return nil, false, ErrTopicNotFound
	}

	// Slow path: write lock with double-check
	td, created := s.CreateTopic(name, s.cfg.DefaultPartitions)
	return td, created, nil
}

// AutoCreateEnabled returns whether auto-create topics is enabled.
func (s *State) AutoCreateEnabled() bool {
	return s.cfg.AutoCreateTopics
}

// DeleteTopic removes a topic by name. Returns true if the topic existed.
func (s *State) DeleteTopic(name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.topics[name]
	if ok {
		delete(s.topics, name)
		delete(s.tnorms, NormalizeTopicName(name))
	}
	return ok
}

// TopicData holds metadata and partitions for a single topic.
type TopicData struct {
	Name       string
	ID         [16]byte // UUID
	Partitions []*PartData
	Configs    map[string]string
}

// newTopicData creates a new TopicData with the given partition count.
func newTopicData(name string, numPartitions int, nodeID int32) *TopicData {
	id := uuid.New()
	var topicID [16]byte
	copy(topicID[:], id[:])

	partitions := make([]*PartData, numPartitions)
	for i := range partitions {
		partitions[i] = &PartData{
			Topic:                name,
			Index:                int32(i),
			TopicID:              topicID,
			maxTimestampBatchIdx: -1,
		}
	}

	return &TopicData{
		Name:       name,
		ID:         topicID,
		Partitions: partitions,
		Configs:    make(map[string]string),
	}
}

// PartData and StoredBatch are defined in partition.go.

// GetOrCreateGroup returns an existing group, or creates a new one.
// The group's manage goroutine is started on creation.
func (s *State) GetOrCreateGroup(groupID string) *Group {
	s.mu.RLock()
	g, ok := s.groups[groupID]
	s.mu.RUnlock()
	if ok {
		return g
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	// Double-check after acquiring write lock
	if g, ok := s.groups[groupID]; ok {
		return g
	}
	g = NewGroup(groupID, s.shutdownCh, s.logger)
	if s.metaLog != nil {
		g.SetMetadataLog(s.metaLog)
	}
	s.groups[groupID] = g
	return g
}

// GetGroup returns the group with the given ID, or nil if not found.
func (s *State) GetGroup(groupID string) *Group {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.groups[groupID]
}

// GetAllGroups returns a snapshot of all current groups.
func (s *State) GetAllGroups() []*Group {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]*Group, 0, len(s.groups))
	for _, g := range s.groups {
		result = append(result, g)
	}
	return result
}

// DeleteGroup removes a group by ID and stops its goroutine.
func (s *State) DeleteGroup(groupID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, ok := s.groups[groupID]
	if ok {
		g.Stop()
		delete(s.groups, groupID)
	}
	return ok
}

// AddPartitions increases the partition count for a topic to newCount.
// If newCount <= current count, this is a no-op.
func (s *State) AddPartitions(topicName string, newCount int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	td, ok := s.topics[topicName]
	if !ok {
		return
	}
	current := len(td.Partitions)
	if newCount <= current {
		return
	}
	for i := current; i < newCount; i++ {
		pd := &PartData{
			Topic:                topicName,
			Index:                int32(i),
			TopicID:              td.ID,
			maxTimestampBatchIdx: -1,
		}
		if s.walWriter != nil {
			totalPartitions := s.countPartitions() + (newCount - current)
			slots := CalcRingSlots(s.ringBufferMem, totalPartitions, 16*1024)
			ring := NewRingBuffer(slots)
			pd.InitWAL(ring, s.walWriter, s.walIndex)
		}
		td.Partitions = append(td.Partitions, pd)
	}
}

// SetTopicConfig sets a single config key on a topic.
func (s *State) SetTopicConfig(topicName, key, value string) {
	s.mu.RLock()
	td, ok := s.topics[topicName]
	s.mu.RUnlock()
	if !ok {
		return
	}
	td.Configs[key] = value
}

// DeleteTopicConfig removes a single config override from a topic.
func (s *State) DeleteTopicConfig(topicName, key string) {
	s.mu.RLock()
	td, ok := s.topics[topicName]
	s.mu.RUnlock()
	if !ok {
		return
	}
	delete(td.Configs, key)
}

// ReplaceTopicConfigs replaces all topic configs with the provided set.
// Configs not in the provided set revert to defaults (removed from overrides).
func (s *State) ReplaceTopicConfigs(topicName string, configs []kmsg.AlterConfigsRequestResourceConfig) {
	s.mu.RLock()
	td, ok := s.topics[topicName]
	s.mu.RUnlock()
	if !ok {
		return
	}
	// Clear existing overrides
	for k := range td.Configs {
		delete(td.Configs, k)
	}
	// Apply new ones
	for _, c := range configs {
		if c.Value != nil {
			td.Configs[c.Name] = *c.Value
		}
	}
}

// SetMetadataLog sets the metadata log reference.
func (s *State) SetMetadataLog(ml *metadata.Log) {
	s.metaLog = ml
}

// MetadataLog returns the metadata log, or nil if not set.
func (s *State) MetadataLog() *metadata.Log {
	return s.metaLog
}

// SnapshotEntries generates serialized metadata entries for all live state.
// Used by compaction. Caller should acquire cluster state read lock first
// (or this is called from within the metadata log's compaction lock).
func (s *State) SnapshotEntries() [][]byte {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var entries [][]byte

	// 1. CREATE_TOPIC entries for all topics
	for _, td := range s.topics {
		e := metadata.MarshalCreateTopic(&metadata.CreateTopicEntry{
			TopicName:      td.Name,
			PartitionCount: int32(len(td.Partitions)),
			TopicID:        td.ID,
			Configs:        td.Configs,
		})
		entries = append(entries, e)
	}

	// 2. OFFSET_COMMIT entries for all groups
	for _, g := range s.groups {
		offsets := g.GetCommittedOffsets()
		for tp, co := range offsets {
			e := metadata.MarshalOffsetCommit(&metadata.OffsetCommitEntry{
				Group:     g.ID(),
				Topic:     tp.Topic,
				Partition: tp.Partition,
				Offset:    co.Offset,
				Metadata:  co.Metadata,
			})
			entries = append(entries, e)
		}
	}

	// 3. LOG_START_OFFSET entries for partitions with logStart > 0
	for _, td := range s.topics {
		for _, pd := range td.Partitions {
			pd.mu.RLock()
			ls := pd.logStart
			pd.mu.RUnlock()
			if ls > 0 {
				e := metadata.MarshalLogStartOffset(&metadata.LogStartOffsetEntry{
					TopicName:      td.Name,
					Partition:      pd.Index,
					LogStartOffset: ls,
				})
				entries = append(entries, e)
			}
		}
	}

	return entries
}

// CreateTopicFromReplay creates a topic during metadata.log replay.
// Unlike CreateTopic, this accepts a specific topic ID.
func (s *State) CreateTopicFromReplay(name string, numPartitions int, topicID [16]byte, configs map[string]string) *TopicData {
	s.mu.Lock()
	defer s.mu.Unlock()

	// If topic already exists, skip (handles duplicate entries in log)
	if td, ok := s.topics[name]; ok {
		return td
	}

	partitions := make([]*PartData, numPartitions)
	for i := range partitions {
		partitions[i] = &PartData{
			Topic:                name,
			Index:                int32(i),
			TopicID:              topicID,
			maxTimestampBatchIdx: -1,
		}
	}

	td := &TopicData{
		Name:       name,
		ID:         topicID,
		Partitions: partitions,
		Configs:    make(map[string]string),
	}
	for k, v := range configs {
		td.Configs[k] = v
	}

	s.topics[name] = td
	s.tnorms[NormalizeTopicName(name)] = name

	return td
}

// SetCommittedOffsetFromReplay sets a committed offset during replay.
// This bypasses the group goroutine since it's used during startup.
func (s *State) SetCommittedOffsetFromReplay(groupID, topic string, partition int32, offset int64, metadataStr string) {
	s.mu.Lock()
	g, ok := s.groups[groupID]
	if !ok {
		g = NewGroup(groupID, s.shutdownCh, s.logger)
		if s.metaLog != nil {
			g.SetMetadataLog(s.metaLog)
		}
		s.groups[groupID] = g
	}
	s.mu.Unlock()

	g.Control(func() {
		g.offsets[TopicPartition{Topic: topic, Partition: partition}] = CommittedOffset{
			Offset:     offset,
			Metadata:   metadataStr,
			CommitTime: time.Now(),
		}
	})
}

// SetLogStartOffsetFromReplay sets the logStartOffset for a partition during replay.
func (s *State) SetLogStartOffsetFromReplay(topicName string, partition int32, logStart int64) {
	s.mu.RLock()
	td, ok := s.topics[topicName]
	s.mu.RUnlock()
	if !ok || int(partition) >= len(td.Partitions) {
		return
	}

	pd := td.Partitions[partition]
	pd.mu.Lock()
	if logStart > pd.logStart {
		pd.logStart = logStart
		if pd.hw < logStart {
			pd.hw = logStart
			if pd.nextReserve < logStart {
				pd.nextReserve = logStart
			}
			if pd.nextCommit < logStart {
				pd.nextCommit = logStart
			}
		}
	}
	pd.mu.Unlock()
}

// StopAllGroups stops all group goroutines. Called during broker shutdown.
func (s *State) StopAllGroups() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, g := range s.groups {
		g.Stop()
	}
}

// initPartitionsWAL initializes ring buffers and WAL references on all partitions of a topic.
// Caller must hold s.mu.Lock().
func (s *State) initPartitionsWAL(td *TopicData) {
	totalPartitions := s.countPartitions()
	slots := CalcRingSlots(s.ringBufferMem, totalPartitions, 16*1024)

	for _, pd := range td.Partitions {
		ring := NewRingBuffer(slots)
		pd.InitWAL(ring, s.walWriter, s.walIndex)
	}
}

// countPartitions returns the total number of partitions across all topics.
// Caller must hold s.mu.RLock() or s.mu.Lock().
func (s *State) countPartitions() int {
	total := 0
	for _, td := range s.topics {
		total += len(td.Partitions)
	}
	return total
}
