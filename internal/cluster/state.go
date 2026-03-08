package cluster

import (
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/klaudworks/klite/internal/chunk"
	"github.com/klaudworks/klite/internal/metadata"
	"github.com/klaudworks/klite/internal/wal"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type DeletedTopic struct {
	Name    string
	TopicID [16]byte
}

// State holds all in-memory cluster state: topics, partitions, offsets, groups.
// Protected by a RWMutex for concurrent access.
type State struct {
	mu            sync.RWMutex
	topics        map[string]*TopicData // topic name -> topic
	tnorms        map[string]string     // normalized name -> actual topic name (collision detection)
	groups        map[string]*Group     // group ID -> group
	deletedTopics []DeletedTopic        // topics pending S3 GC
	cfg           Config

	shutdownCh <-chan struct{}
	logger     *slog.Logger

	// WAL-related state (Phase 3+)
	walWriter *wal.Writer
	walIndex  *wal.Index
	chunkPool *chunk.Pool // global chunk pool (replaces ring buffers)

	// S3-related state (Phase 4)
	s3Fetcher S3Fetcher // S3 reader adapter (nil if S3 not configured)

	// Metadata log (Phase 3+)
	metaLog *metadata.Log

	// Producer ID / transaction management (Phase 4)
	pidManager *ProducerIDManager
}

type Config struct {
	NodeID            int32
	DefaultPartitions int
	AutoCreateTopics  bool
}

func NewState(cfg Config) *State {
	return &State{
		topics:     make(map[string]*TopicData),
		tnorms:     make(map[string]string),
		groups:     make(map[string]*Group),
		cfg:        cfg,
		logger:     slog.Default(),
		pidManager: NewProducerIDManager(),
	}
}

func (s *State) PIDManager() *ProducerIDManager {
	return s.pidManager
}

func (s *State) SetShutdownCh(ch <-chan struct{}) {
	s.shutdownCh = ch
}

func (s *State) SetLogger(l *slog.Logger) {
	s.logger = l
}

func (s *State) SetWALConfig(w *wal.Writer, idx *wal.Index, pool *chunk.Pool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.walWriter = w
	s.walIndex = idx
	s.chunkPool = pool

	for _, td := range s.topics {
		for _, pd := range td.Partitions {
			pd.InitWAL(pool, w, idx)
		}
	}
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

func (s *State) GetTopic(name string) *TopicData {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.topics[name]
}

func (s *State) GetAllTopics() []*TopicData {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]*TopicData, 0, len(s.topics))
	for _, td := range s.topics {
		result = append(result, td)
	}
	return result
}

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

	s.initPartitionsWAL(td)

	return td, true
}

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

	s.initPartitionsWAL(td)

	return td, true
}

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

func (s *State) DefaultPartitions() int {
	return s.cfg.DefaultPartitions
}

// GetOrCreateTopic returns the existing topic, or creates it if auto-create
// is enabled. Returns (topic, created, error). Error is non-nil if the topic
// doesn't exist and auto-create is disabled.
// When a topic is auto-created, it is persisted to the metadata log.
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
	if created {
		s.PersistTopicCreation(td)
	}
	return td, created, nil
}

func (s *State) AutoCreateEnabled() bool {
	return s.cfg.AutoCreateTopics
}

func (s *State) DeleteTopic(name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	td, ok := s.topics[name]
	if ok {
		s.deletedTopics = append(s.deletedTopics, DeletedTopic{
			Name:    name,
			TopicID: td.ID,
		})
		delete(s.topics, name)
		delete(s.tnorms, NormalizeTopicName(name))
	}
	return ok
}

func (s *State) DrainDeletedTopics() []DeletedTopic {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := s.deletedTopics
	s.deletedTopics = nil
	return result
}

func (s *State) AddDeletedTopic(dt DeletedTopic) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deletedTopics = append(s.deletedTopics, dt)
}

type TopicData struct {
	Name       string
	ID         [16]byte // UUID
	Partitions []*PartData
	Configs    map[string]string // protected by configMu
	configMu   sync.RWMutex
}

// GetConfig returns the value of a topic config key. Safe for concurrent use.
func (td *TopicData) GetConfig(key string) (string, bool) {
	td.configMu.RLock()
	v, ok := td.Configs[key]
	td.configMu.RUnlock()
	return v, ok
}

// CopyConfigs returns a snapshot of the topic configs. Safe for concurrent use.
func (td *TopicData) CopyConfigs() map[string]string {
	td.configMu.RLock()
	cp := make(map[string]string, len(td.Configs))
	for k, v := range td.Configs {
		cp[k] = v
	}
	td.configMu.RUnlock()
	return cp
}

func newTopicData(name string, numPartitions int, nodeID int32) *TopicData {
	id := uuid.New()
	var topicID [16]byte
	copy(topicID[:], id[:])

	partitions := make([]*PartData, numPartitions)
	for i := range partitions {
		partitions[i] = &PartData{
			Topic:   name,
			Index:   int32(i),
			TopicID: topicID,
		}
	}

	return &TopicData{
		Name:       name,
		ID:         topicID,
		Partitions: partitions,
		Configs:    make(map[string]string),
	}
}

func (s *State) GetOrCreateGroup(groupID string) *Group {
	s.mu.RLock()
	g, ok := s.groups[groupID]
	s.mu.RUnlock()
	if ok {
		return g
	}

	s.mu.Lock()
	defer s.mu.Unlock()
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

func (s *State) GetGroup(groupID string) *Group {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.groups[groupID]
}

func (s *State) GetAllGroups() []*Group {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]*Group, 0, len(s.groups))
	for _, g := range s.groups {
		result = append(result, g)
	}
	return result
}

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
			Topic:   topicName,
			Index:   int32(i),
			TopicID: td.ID,
		}
		pd.InitWAL(s.chunkPool, s.walWriter, s.walIndex)
		td.Partitions = append(td.Partitions, pd)
	}
}

func (s *State) SetTopicConfig(topicName, key, value string) {
	s.mu.RLock()
	td, ok := s.topics[topicName]
	s.mu.RUnlock()
	if !ok {
		return
	}
	td.configMu.Lock()
	td.Configs[key] = value
	td.configMu.Unlock()
}

func (s *State) DeleteTopicConfig(topicName, key string) {
	s.mu.RLock()
	td, ok := s.topics[topicName]
	s.mu.RUnlock()
	if !ok {
		return
	}
	td.configMu.Lock()
	delete(td.Configs, key)
	td.configMu.Unlock()
}

func (s *State) ReplaceTopicConfigs(topicName string, configs []kmsg.AlterConfigsRequestResourceConfig) {
	s.mu.RLock()
	td, ok := s.topics[topicName]
	s.mu.RUnlock()
	if !ok {
		return
	}
	td.configMu.Lock()
	for k := range td.Configs {
		delete(td.Configs, k)
	}
	for _, c := range configs {
		if c.Value != nil {
			td.Configs[c.Name] = *c.Value
		}
	}
	td.configMu.Unlock()
}

func (s *State) SetMetadataLog(ml *metadata.Log) {
	s.metaLog = ml
}

func (s *State) MetadataLog() *metadata.Log {
	return s.metaLog
}

// PersistTopicCreation writes a CreateTopic entry to the metadata log.
// Must be called outside s.mu. No-op if the metadata log is nil.
func (s *State) PersistTopicCreation(td *TopicData) {
	ml := s.metaLog
	if ml == nil {
		return
	}
	entry := metadata.MarshalCreateTopic(&metadata.CreateTopicEntry{
		TopicName:      td.Name,
		PartitionCount: int32(len(td.Partitions)),
		TopicID:        td.ID,
		Configs:        td.CopyConfigs(),
	})
	if err := ml.AppendSync(entry); err != nil {
		slog.Warn("metadata.log: failed to persist topic creation",
			"topic", td.Name, "err", err)
	}
}

// SnapshotEntries generates serialized metadata entries for all live state.
// Used by compaction. Caller should acquire cluster state read lock first
// (or this is called from within the metadata log's compaction lock).
func (s *State) SnapshotEntries() [][]byte {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var entries [][]byte

	for _, td := range s.topics {
		e := metadata.MarshalCreateTopic(&metadata.CreateTopicEntry{
			TopicName:      td.Name,
			PartitionCount: int32(len(td.Partitions)),
			TopicID:        td.ID,
			Configs:        td.CopyConfigs(),
		})
		entries = append(entries, e)
	}

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

	nextPID := s.pidManager.NextPID()
	if nextPID > 1 {
		e := metadata.MarshalProducerID(&metadata.ProducerIDEntry{
			NextProducerID: nextPID,
		})
		entries = append(entries, e)
	}

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

	for _, td := range s.topics {
		for _, pd := range td.Partitions {
			pd.mu.RLock()
			cu := pd.cleanedUpTo
			pd.mu.RUnlock()
			if cu > 0 {
				e := metadata.MarshalCompactionWatermark(&metadata.CompactionWatermarkEntry{
					TopicName:   td.Name,
					Partition:   pd.Index,
					CleanedUpTo: cu,
				})
				entries = append(entries, e)
			}
		}
	}

	return entries
}

// CreateTopicFromReplay creates a topic during metadata.log replay.
// Unlike CreateTopic, it accepts a specific topic ID rather than generating one.
func (s *State) CreateTopicFromReplay(name string, numPartitions int, topicID [16]byte, configs map[string]string) *TopicData {
	s.mu.Lock()
	defer s.mu.Unlock()

	if td, ok := s.topics[name]; ok {
		return td
	}

	partitions := make([]*PartData, numPartitions)
	for i := range partitions {
		partitions[i] = &PartData{
			Topic:   name,
			Index:   int32(i),
			TopicID: topicID,
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

	s.initPartitionsWAL(td)

	return td
}

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

func (s *State) SetCompactionWatermarkFromReplay(topicName string, partition int32, cleanedUpTo int64) {
	s.mu.RLock()
	td, ok := s.topics[topicName]
	s.mu.RUnlock()
	if !ok || int(partition) >= len(td.Partitions) {
		return
	}

	pd := td.Partitions[partition]
	pd.mu.Lock()
	pd.SetCleanedUpTo(cleanedUpTo)
	pd.mu.Unlock()
}

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

func (s *State) StopAllGroups() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, g := range s.groups {
		g.Stop()
	}
}

// Caller must hold s.mu.Lock().
func (s *State) initPartitionsWAL(td *TopicData) {
	for _, pd := range td.Partitions {
		pd.InitWAL(s.chunkPool, s.walWriter, s.walIndex)
		if s.s3Fetcher != nil {
			pd.SetS3Fetcher(s.s3Fetcher)
		}
	}
}

func (s *State) SetS3Fetcher(fetcher S3Fetcher) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, td := range s.topics {
		for _, pd := range td.Partitions {
			pd.SetS3Fetcher(fetcher)
		}
	}
	s.s3Fetcher = fetcher
}

func (s *State) FlushablePartitions() []FlushablePartition {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []FlushablePartition
	for _, td := range s.topics {
		for _, pd := range td.Partitions {
			pd.mu.RLock()
			hw := pd.hw
			watermark := pd.s3FlushWatermark
			topicID := pd.TopicID
			pd.mu.RUnlock()

			if hw <= watermark {
				continue // nothing to flush
			}

			capturedPd := pd
			result = append(result, FlushablePartition{
				Topic:       td.Name,
				Partition:   pd.Index,
				TopicID:     topicID,
				S3Watermark: watermark,
				HW:          hw,
				Partition_:  capturedPd,
			})
		}
	}

	return result
}

type FlushablePartition struct {
	Topic       string
	Partition   int32
	TopicID     [16]byte
	S3Watermark int64
	HW          int64
	Partition_  *PartData // reference to the partition for watermark update
}
