package cluster

import (
	"sync"

	"github.com/google/uuid"
)

// State holds all in-memory cluster state: topics, partitions, offsets.
// Protected by a RWMutex for concurrent access.
type State struct {
	mu     sync.RWMutex
	topics map[string]*TopicData // topic name -> topic
	cfg    Config
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
		cfg:    cfg,
	}
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
	return td, true
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
