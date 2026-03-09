package cluster

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/klaudworks/klite/internal/metadata"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// TestSetTopicConfigConcurrentRead verifies that SetTopicConfig and concurrent
// reads of td.Configs do not race. Before the fix, SetTopicConfig mutated the
// map without holding the State write lock, causing a data race with any
// goroutine reading td.Configs (produce handlers, retention, compaction, etc.).
//
// Run with: go test -race -count=1 ./internal/cluster/ -run TestSetTopicConfigConcurrentRead
func TestSetTopicConfigConcurrentRead(t *testing.T) {
	t.Parallel()

	s := NewState(Config{
		NodeID:            0,
		DefaultPartitions: 1,
		AutoCreateTopics:  true,
	})

	td, created := s.CreateTopic("race-topic", 1)
	if !created {
		t.Fatal("expected topic to be created")
	}

	const goroutines = 10
	const iterations = 500

	var wg sync.WaitGroup

	// Writers: SetTopicConfig
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				s.SetTopicConfig("race-topic", "retention.ms", "86400000")
			}
		}()
	}

	// Readers: read td.Configs directly (simulates produce handler, retention, etc.)
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				_, _ = td.GetConfig("retention.ms")
			}
		}()
	}

	// Readers via SnapshotEntries (iterates the map for serialization)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				_ = s.SnapshotEntries()
			}
		}()
	}

	wg.Wait()
}

// TestReplaceTopicConfigsConcurrentRead verifies that ReplaceTopicConfigs and
// concurrent reads do not race.
func TestReplaceTopicConfigsConcurrentRead(t *testing.T) {
	t.Parallel()

	s := NewState(Config{
		NodeID:            0,
		DefaultPartitions: 1,
		AutoCreateTopics:  true,
	})

	td, _ := s.CreateTopic("replace-race", 1)

	const goroutines = 10
	const iterations = 200

	var wg sync.WaitGroup

	// Writer: ReplaceTopicConfigs
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				s.DeleteTopicConfig("replace-race", "retention.ms")
			}
		}()
	}

	// Reader
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				_, _ = td.GetConfig("retention.ms")
			}
		}()
	}

	wg.Wait()
}

func TestGetOrCreateTopic_AutoCreateDisabled(t *testing.T) {
	t.Parallel()

	s := NewState(Config{
		NodeID:            0,
		DefaultPartitions: 3,
		AutoCreateTopics:  false,
	})

	td, created, err := s.GetOrCreateTopic("no-auto")
	if td != nil {
		t.Fatalf("expected nil topic, got %v", td)
	}
	if created {
		t.Fatal("expected created=false")
	}
	if !errors.Is(err, ErrTopicNotFound) {
		t.Fatalf("expected ErrTopicNotFound, got %v", err)
	}
}

func TestGetOrCreateTopic_AutoCreateEnabled(t *testing.T) {
	t.Parallel()

	s := NewState(Config{
		NodeID:            0,
		DefaultPartitions: 3,
		AutoCreateTopics:  true,
	})

	td, created, err := s.GetOrCreateTopic("auto-topic")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !created {
		t.Fatal("expected created=true for first call")
	}
	if td == nil {
		t.Fatal("expected non-nil topic")
	}
	if td.Name != "auto-topic" {
		t.Fatalf("expected name auto-topic, got %s", td.Name)
	}
	if len(td.Partitions) != 3 {
		t.Fatalf("expected 3 partitions, got %d", len(td.Partitions))
	}

	// Second call returns existing, not a new one
	td2, created2, err2 := s.GetOrCreateTopic("auto-topic")
	if err2 != nil {
		t.Fatalf("unexpected error on second call: %v", err2)
	}
	if created2 {
		t.Fatal("expected created=false on second call")
	}
	if td2 != td {
		t.Fatal("expected same TopicData pointer on second call")
	}
}

func TestGetOrCreateTopic_AutoCreatePersistFailure(t *testing.T) {
	t.Parallel()

	s := NewState(Config{
		NodeID:            0,
		DefaultPartitions: 3,
		AutoCreateTopics:  true,
	})

	ml, err := metadata.NewLog(metadata.LogConfig{DataDir: t.TempDir(), Logger: slog.Default()})
	if err != nil {
		t.Fatalf("new metadata log: %v", err)
	}
	s.SetMetadataLog(ml)
	if err := ml.Close(); err != nil {
		t.Fatalf("close metadata log: %v", err)
	}

	td, created, err := s.GetOrCreateTopic("auto-topic")
	if td != nil {
		t.Fatalf("expected nil topic, got %v", td)
	}
	if created {
		t.Fatal("expected created=false")
	}
	if !errors.Is(err, ErrTopicPersistenceFailed) {
		t.Fatalf("expected ErrTopicPersistenceFailed, got %v", err)
	}
	if s.TopicExists("auto-topic") {
		t.Fatal("topic should not exist after metadata persistence failure")
	}
}

func TestGetOrCreateTopic_ConcurrentAutoCreate(t *testing.T) {
	t.Parallel()

	s := NewState(Config{
		NodeID:            0,
		DefaultPartitions: 1,
		AutoCreateTopics:  true,
	})

	const goroutines = 20
	results := make([]*TopicData, goroutines)
	createdCount := make([]bool, goroutines)
	var wg sync.WaitGroup

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			td, created, err := s.GetOrCreateTopic("concurrent-topic")
			if err != nil {
				t.Errorf("goroutine %d: unexpected error: %v", idx, err)
				return
			}
			results[idx] = td
			createdCount[idx] = created
		}(i)
	}

	wg.Wait()

	// All goroutines must get the same TopicData pointer
	first := results[0]
	if first == nil {
		t.Fatal("first result is nil")
	}
	numCreated := 0
	for i, td := range results {
		if td != first {
			t.Fatalf("goroutine %d got different TopicData pointer", i)
		}
		if createdCount[i] {
			numCreated++
		}
	}

	// Exactly one goroutine should have created=true
	if numCreated != 1 {
		t.Fatalf("expected exactly 1 creator, got %d", numCreated)
	}
}

func TestReplaceTopicConfigs(t *testing.T) {
	t.Parallel()

	s := NewState(Config{
		NodeID:            0,
		DefaultPartitions: 1,
		AutoCreateTopics:  true,
	})

	td, _ := s.CreateTopic("config-topic", 1)
	s.SetTopicConfig("config-topic", "retention.ms", "86400000")
	s.SetTopicConfig("config-topic", "cleanup.policy", "delete")

	// Verify initial configs
	v, ok := td.GetConfig("retention.ms")
	if !ok || v != "86400000" {
		t.Fatalf("expected retention.ms=86400000, got %s (ok=%v)", v, ok)
	}

	// Replace with new configs — old keys should be gone
	newVal := "compact"
	s.ReplaceTopicConfigs("config-topic", []kmsg.AlterConfigsRequestResourceConfig{
		{Name: "cleanup.policy", Value: &newVal},
	})

	// retention.ms should be gone
	if _, ok := td.GetConfig("retention.ms"); ok {
		t.Fatal("expected retention.ms to be deleted after replace")
	}

	// cleanup.policy should have the new value
	v, ok = td.GetConfig("cleanup.policy")
	if !ok || v != "compact" {
		t.Fatalf("expected cleanup.policy=compact, got %s (ok=%v)", v, ok)
	}
}

func TestReplaceTopicConfigs_ConcurrentReadWrite(t *testing.T) {
	t.Parallel()

	s := NewState(Config{
		NodeID:            0,
		DefaultPartitions: 1,
		AutoCreateTopics:  true,
	})

	td, _ := s.CreateTopic("replace-race-2", 1)

	const goroutines = 10
	const iterations = 200

	var wg sync.WaitGroup

	// Writer: ReplaceTopicConfigs
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			val := fmt.Sprintf("%d", idx)
			for j := 0; j < iterations; j++ {
				s.ReplaceTopicConfigs("replace-race-2", []kmsg.AlterConfigsRequestResourceConfig{
					{Name: "retention.ms", Value: &val},
				})
			}
		}(i)
	}

	// Reader: CopyConfigs
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				_ = td.CopyConfigs()
			}
		}()
	}

	wg.Wait()
}

func TestCreateTopicFromReplay(t *testing.T) {
	t.Parallel()

	s := NewState(Config{
		NodeID:            0,
		DefaultPartitions: 1,
		AutoCreateTopics:  false,
	})

	topicID := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	configs := map[string]string{
		"retention.ms":   "3600000",
		"cleanup.policy": "compact",
	}

	td := s.CreateTopicFromReplay("replayed-topic", 4, topicID, configs)

	if td.Name != "replayed-topic" {
		t.Fatalf("expected name replayed-topic, got %s", td.Name)
	}
	if td.ID != topicID {
		t.Fatalf("expected topicID %v, got %v", topicID, td.ID)
	}
	if len(td.Partitions) != 4 {
		t.Fatalf("expected 4 partitions, got %d", len(td.Partitions))
	}

	// Verify partition fields
	for i, pd := range td.Partitions {
		if pd.Topic != "replayed-topic" {
			t.Errorf("partition %d: expected topic replayed-topic, got %s", i, pd.Topic)
		}
		if pd.Index != int32(i) {
			t.Errorf("partition %d: expected index %d, got %d", i, i, pd.Index)
		}
		if pd.TopicID != topicID {
			t.Errorf("partition %d: expected topicID %v, got %v", i, topicID, pd.TopicID)
		}
	}

	// Verify configs
	v, ok := td.GetConfig("retention.ms")
	if !ok || v != "3600000" {
		t.Fatalf("expected retention.ms=3600000, got %s (ok=%v)", v, ok)
	}
	v, ok = td.GetConfig("cleanup.policy")
	if !ok || v != "compact" {
		t.Fatalf("expected cleanup.policy=compact, got %s (ok=%v)", v, ok)
	}

	// Verify topic is accessible through State
	got := s.GetTopic("replayed-topic")
	if got != td {
		t.Fatal("GetTopic did not return the replayed topic")
	}

	// Verify normalization entry was created (dots and underscores collide)
	collision := s.CheckTopicCollision("replayed.topic")
	if collision != "" {
		t.Fatalf("expected no collision with replayed.topic (hyphens don't collide), got %q", collision)
	}

	// Create a topic with dot to test actual collision detection
	s.CreateTopicFromReplay("foo.bar", 1, [16]byte{99}, nil)
	collision = s.CheckTopicCollision("foo_bar")
	if collision != "foo.bar" {
		t.Fatalf("expected collision with foo.bar, got %q", collision)
	}
}

func TestCreateTopicFromReplay_Idempotent(t *testing.T) {
	t.Parallel()

	s := NewState(Config{
		NodeID:            0,
		DefaultPartitions: 1,
		AutoCreateTopics:  false,
	})

	topicID := [16]byte{1, 2, 3, 4, 5, 6, 7, 8}
	td1 := s.CreateTopicFromReplay("dup-topic", 2, topicID, nil)
	td2 := s.CreateTopicFromReplay("dup-topic", 4, [16]byte{9, 9, 9}, nil)

	// Second call returns the existing topic, does not overwrite
	if td1 != td2 {
		t.Fatal("expected same pointer for duplicate replay")
	}
	if len(td1.Partitions) != 2 {
		t.Fatalf("partition count should not change, got %d", len(td1.Partitions))
	}
}

func TestSetCommittedOffsetFromReplay(t *testing.T) {
	t.Parallel()

	shutdownCh := make(chan struct{})
	defer close(shutdownCh)

	s := NewState(Config{
		NodeID:            0,
		DefaultPartitions: 1,
		AutoCreateTopics:  false,
	})
	s.SetShutdownCh(shutdownCh)
	s.SetLogger(slog.Default())

	s.SetCommittedOffsetFromReplay("test-group", "topic-a", 0, 42, "meta-1")
	s.SetCommittedOffsetFromReplay("test-group", "topic-a", 1, 100, "meta-2")
	s.SetCommittedOffsetFromReplay("test-group", "topic-b", 0, 7, "")

	g := s.GetGroup("test-group")
	if g == nil {
		t.Fatal("expected group to exist after replay")
	}
	defer g.Stop()

	offsets := g.GetCommittedOffsets()

	check := func(topic string, partition int32, wantOffset int64, wantMeta string) {
		t.Helper()
		tp := TopicPartition{Topic: topic, Partition: partition}
		co, ok := offsets[tp]
		if !ok {
			t.Fatalf("missing offset for %s/%d", topic, partition)
		}
		if co.Offset != wantOffset {
			t.Fatalf("%s/%d: expected offset %d, got %d", topic, partition, wantOffset, co.Offset)
		}
		if co.Metadata != wantMeta {
			t.Fatalf("%s/%d: expected metadata %q, got %q", topic, partition, wantMeta, co.Metadata)
		}
	}

	check("topic-a", 0, 42, "meta-1")
	check("topic-a", 1, 100, "meta-2")
	check("topic-b", 0, 7, "")
}

func TestSetCommittedOffsetFromReplay_CreatesGroup(t *testing.T) {
	t.Parallel()

	shutdownCh := make(chan struct{})
	defer close(shutdownCh)

	s := NewState(Config{
		NodeID:            0,
		DefaultPartitions: 1,
		AutoCreateTopics:  false,
	})
	s.SetShutdownCh(shutdownCh)
	s.SetLogger(slog.Default())

	// Group doesn't exist yet
	if g := s.GetGroup("new-group"); g != nil {
		t.Fatal("expected no group before replay")
	}

	s.SetCommittedOffsetFromReplay("new-group", "t", 0, 5, "")

	g := s.GetGroup("new-group")
	if g == nil {
		t.Fatal("expected group to be created by replay")
	}
	defer g.Stop()
}

func TestSetMetadataLog_BackfillsReplayGroups(t *testing.T) {
	t.Parallel()

	shutdownCh := make(chan struct{})
	defer close(shutdownCh)

	s := NewState(Config{
		NodeID:            0,
		DefaultPartitions: 1,
		AutoCreateTopics:  false,
	})
	s.SetShutdownCh(shutdownCh)
	s.SetLogger(slog.Default())

	// Simulate metadata replay creating a group before State has a metadata log.
	s.SetCommittedOffsetFromReplay("restored-group", "topic-a", 0, 42, "from-replay")

	g := s.GetGroup("restored-group")
	if g == nil {
		t.Fatal("expected replay to create group")
	}
	defer g.Stop()

	ml, err := metadata.NewLog(metadata.LogConfig{DataDir: t.TempDir(), Logger: slog.Default()})
	if err != nil {
		t.Fatalf("create metadata log: %v", err)
	}
	defer func() {
		if err := ml.Close(); err != nil {
			t.Fatalf("close metadata log: %v", err)
		}
	}()

	s.SetMetadataLog(ml)

	beforeOffsetCommit := ml.Size()

	req := kmsg.NewPtrOffsetCommitRequest()
	req.Group = "restored-group"
	topic := kmsg.NewOffsetCommitRequestTopic()
	topic.Topic = "topic-a"
	partition := kmsg.NewOffsetCommitRequestTopicPartition()
	partition.Partition = 0
	partition.Offset = 43
	topic.Partitions = append(topic.Partitions, partition)
	req.Topics = append(req.Topics, topic)

	respRaw, err := g.Send(req)
	if err != nil {
		t.Fatalf("send OffsetCommit request: %v", err)
	}
	resp, ok := respRaw.(*kmsg.OffsetCommitResponse)
	if !ok {
		t.Fatalf("expected *kmsg.OffsetCommitResponse, got %T", respRaw)
	}
	if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
		t.Fatalf("unexpected offset commit response shape: %+v", resp.Topics)
	}
	if got := resp.Topics[0].Partitions[0].ErrorCode; got != 0 {
		t.Fatalf("expected successful OffsetCommit, got error code %d", got)
	}
	if got := ml.Size(); got <= beforeOffsetCommit {
		t.Fatalf("expected OffsetCommit to append to metadata log, size before=%d after=%d", beforeOffsetCommit, got)
	}

	beforeTxnApply := ml.Size()
	g.Control(func() {
		g.ApplyTxnOffset(TopicPartition{Topic: "topic-a", Partition: 0}, PendingTxnOffset{
			Offset:      44,
			LeaderEpoch: 0,
			Metadata:    "txn-apply",
		})
	})
	if got := ml.Size(); got <= beforeTxnApply {
		t.Fatalf("expected transactional offset apply to append to metadata log, size before=%d after=%d", beforeTxnApply, got)
	}
}

func TestSetCompactionWatermarkFromReplay(t *testing.T) {
	t.Parallel()

	s := NewState(Config{
		NodeID:            0,
		DefaultPartitions: 1,
		AutoCreateTopics:  false,
	})

	s.CreateTopic("compact-topic", 3)

	s.SetCompactionWatermarkFromReplay("compact-topic", 1, 50)

	td := s.GetTopic("compact-topic")
	// Partition 0 should be untouched
	td.Partitions[0].mu.RLock()
	if td.Partitions[0].CleanedUpTo() != 0 {
		t.Fatalf("partition 0: expected cleanedUpTo=0, got %d", td.Partitions[0].CleanedUpTo())
	}
	td.Partitions[0].mu.RUnlock()

	// Partition 1 should have watermark set
	td.Partitions[1].mu.RLock()
	if td.Partitions[1].CleanedUpTo() != 50 {
		t.Fatalf("partition 1: expected cleanedUpTo=50, got %d", td.Partitions[1].CleanedUpTo())
	}
	td.Partitions[1].mu.RUnlock()
}

func TestSetCompactionWatermarkFromReplay_MissingTopic(t *testing.T) {
	t.Parallel()

	s := NewState(Config{
		NodeID:            0,
		DefaultPartitions: 1,
		AutoCreateTopics:  false,
	})

	// Should not panic on missing topic
	s.SetCompactionWatermarkFromReplay("missing", 0, 100)
}

func TestSetCompactionWatermarkFromReplay_InvalidPartition(t *testing.T) {
	t.Parallel()

	s := NewState(Config{
		NodeID:            0,
		DefaultPartitions: 1,
		AutoCreateTopics:  false,
	})

	s.CreateTopic("small-topic", 2)

	// Partition 5 doesn't exist — should not panic
	s.SetCompactionWatermarkFromReplay("small-topic", 5, 100)
}

func TestFlushablePartitions(t *testing.T) {
	t.Parallel()

	s := NewState(Config{
		NodeID:            0,
		DefaultPartitions: 1,
		AutoCreateTopics:  false,
	})

	s.CreateTopic("flush-topic", 3)
	td := s.GetTopic("flush-topic")

	// Partition 0: hw=10, s3Watermark=0 → flushable
	td.Partitions[0].mu.Lock()
	td.Partitions[0].SetHW(10)
	td.Partitions[0].mu.Unlock()

	// Partition 1: hw=5, s3Watermark=5 → NOT flushable (equal)
	td.Partitions[1].mu.Lock()
	td.Partitions[1].SetHW(5)
	td.Partitions[1].SetS3FlushWatermark(5)
	td.Partitions[1].mu.Unlock()

	// Partition 2: hw=20, s3Watermark=10 → flushable
	td.Partitions[2].mu.Lock()
	td.Partitions[2].SetHW(20)
	td.Partitions[2].SetS3FlushWatermark(10)
	td.Partitions[2].mu.Unlock()

	result := s.FlushablePartitions()

	if len(result) != 2 {
		t.Fatalf("expected 2 flushable partitions, got %d", len(result))
	}

	// Build index by partition for easier checking
	byPartition := make(map[int32]FlushablePartition)
	for _, fp := range result {
		byPartition[fp.Partition] = fp
	}

	fp0, ok := byPartition[0]
	if !ok {
		t.Fatal("expected partition 0 to be flushable")
	}
	if fp0.HW != 10 || fp0.S3Watermark != 0 {
		t.Fatalf("partition 0: expected HW=10, S3Watermark=0, got HW=%d, S3Watermark=%d", fp0.HW, fp0.S3Watermark)
	}
	if fp0.Topic != "flush-topic" {
		t.Fatalf("expected topic flush-topic, got %s", fp0.Topic)
	}

	fp2, ok := byPartition[2]
	if !ok {
		t.Fatal("expected partition 2 to be flushable")
	}
	if fp2.HW != 20 || fp2.S3Watermark != 10 {
		t.Fatalf("partition 2: expected HW=20, S3Watermark=10, got HW=%d, S3Watermark=%d", fp2.HW, fp2.S3Watermark)
	}

	if _, ok := byPartition[1]; ok {
		t.Fatal("partition 1 should NOT be flushable")
	}
}

func TestFlushablePartitions_Empty(t *testing.T) {
	t.Parallel()

	s := NewState(Config{
		NodeID:            0,
		DefaultPartitions: 1,
		AutoCreateTopics:  false,
	})

	// No topics at all
	result := s.FlushablePartitions()
	if len(result) != 0 {
		t.Fatalf("expected 0 flushable partitions, got %d", len(result))
	}
}

func TestAllPartitionWatermarks(t *testing.T) {
	t.Parallel()

	s := NewState(Config{
		NodeID:            0,
		DefaultPartitions: 1,
		AutoCreateTopics:  false,
	})

	s.CreateTopic("wm-topic-a", 2)
	s.CreateTopic("wm-topic-b", 1)

	tdA := s.GetTopic("wm-topic-a")
	tdB := s.GetTopic("wm-topic-b")

	// Set watermarks
	tdA.Partitions[0].mu.Lock()
	tdA.Partitions[0].SetS3FlushWatermark(100)
	tdA.Partitions[0].mu.Unlock()

	tdA.Partitions[1].mu.Lock()
	tdA.Partitions[1].SetS3FlushWatermark(200)
	tdA.Partitions[1].mu.Unlock()

	tdB.Partitions[0].mu.Lock()
	tdB.Partitions[0].SetS3FlushWatermark(50)
	tdB.Partitions[0].mu.Unlock()

	result := s.AllPartitionWatermarks()

	if len(result) != 3 {
		t.Fatalf("expected 3 watermarks, got %d", len(result))
	}

	// Build index by TopicID+Partition for checking
	type key struct {
		topicID   [16]byte
		partition int32
	}
	byKey := make(map[key]PartitionWatermark)
	for _, wm := range result {
		byKey[key{wm.TopicID, wm.Partition}] = wm
	}

	wmA0 := byKey[key{tdA.ID, 0}]
	if wmA0.S3Watermark != 100 {
		t.Fatalf("wm-topic-a/0: expected watermark=100, got %d", wmA0.S3Watermark)
	}

	wmA1 := byKey[key{tdA.ID, 1}]
	if wmA1.S3Watermark != 200 {
		t.Fatalf("wm-topic-a/1: expected watermark=200, got %d", wmA1.S3Watermark)
	}

	wmB0 := byKey[key{tdB.ID, 0}]
	if wmB0.S3Watermark != 50 {
		t.Fatalf("wm-topic-b/0: expected watermark=50, got %d", wmB0.S3Watermark)
	}
}

func TestAllPartitionWatermarks_Empty(t *testing.T) {
	t.Parallel()

	s := NewState(Config{
		NodeID:            0,
		DefaultPartitions: 1,
		AutoCreateTopics:  false,
	})

	result := s.AllPartitionWatermarks()
	if result != nil {
		t.Fatalf("expected nil for empty state, got %v", result)
	}
}

func TestWakeAllFetchWaiters(t *testing.T) {
	t.Parallel()

	s := NewState(Config{
		NodeID:            0,
		DefaultPartitions: 1,
		AutoCreateTopics:  false,
	})

	s.CreateTopic("wake-topic", 2)
	td := s.GetTopic("wake-topic")

	// Register waiters on both partitions
	w0 := NewFetchWaiter()
	w1 := NewFetchWaiter()
	td.Partitions[0].RegisterWaiter(w0)
	td.Partitions[1].RegisterWaiter(w1)

	// Verify waiters are not yet woken
	select {
	case <-w0.Ch():
		t.Fatal("waiter 0 should not be woken yet")
	default:
	}
	select {
	case <-w1.Ch():
		t.Fatal("waiter 1 should not be woken yet")
	default:
	}

	s.WakeAllFetchWaiters()

	// Both should now be closed
	select {
	case <-w0.Ch():
		// good
	case <-time.After(100 * time.Millisecond):
		t.Fatal("waiter 0 was not woken")
	}

	select {
	case <-w1.Ch():
		// good
	case <-time.After(100 * time.Millisecond):
		t.Fatal("waiter 1 was not woken")
	}
}

func TestWakeAllFetchWaiters_NoWaiters(t *testing.T) {
	t.Parallel()

	s := NewState(Config{
		NodeID:            0,
		DefaultPartitions: 1,
		AutoCreateTopics:  false,
	})

	s.CreateTopic("no-waiters", 1)

	// Should not panic
	s.WakeAllFetchWaiters()
}

func TestWakeAllFetchWaiters_MultipleTopics(t *testing.T) {
	t.Parallel()

	s := NewState(Config{
		NodeID:            0,
		DefaultPartitions: 1,
		AutoCreateTopics:  false,
	})

	s.CreateTopic("topic-x", 1)
	s.CreateTopic("topic-y", 1)

	tdX := s.GetTopic("topic-x")
	tdY := s.GetTopic("topic-y")

	wX := NewFetchWaiter()
	wY := NewFetchWaiter()
	tdX.Partitions[0].RegisterWaiter(wX)
	tdY.Partitions[0].RegisterWaiter(wY)

	s.WakeAllFetchWaiters()

	select {
	case <-wX.Ch():
	case <-time.After(100 * time.Millisecond):
		t.Fatal("waiter on topic-x was not woken")
	}
	select {
	case <-wY.Ch():
	case <-time.After(100 * time.Millisecond):
		t.Fatal("waiter on topic-y was not woken")
	}
}

func TestSetLogStartOffsetFromReplay(t *testing.T) {
	t.Parallel()

	s := NewState(Config{
		NodeID:            0,
		DefaultPartitions: 1,
		AutoCreateTopics:  false,
	})

	s.CreateTopic("ls-topic", 2)
	td := s.GetTopic("ls-topic")

	s.SetLogStartOffsetFromReplay("ls-topic", 0, 50)

	td.Partitions[0].mu.RLock()
	ls := td.Partitions[0].LogStart()
	hw := td.Partitions[0].HW()
	td.Partitions[0].mu.RUnlock()

	if ls != 50 {
		t.Fatalf("expected logStart=50, got %d", ls)
	}
	// HW should be bumped to logStart when logStart > hw
	if hw != 50 {
		t.Fatalf("expected hw=50, got %d", hw)
	}

	// Partition 1 should be unaffected
	td.Partitions[1].mu.RLock()
	if td.Partitions[1].LogStart() != 0 {
		t.Fatalf("partition 1 logStart should be 0, got %d", td.Partitions[1].LogStart())
	}
	td.Partitions[1].mu.RUnlock()
}

func TestSetLogStartOffsetFromReplay_DoesNotDecrease(t *testing.T) {
	t.Parallel()

	s := NewState(Config{
		NodeID:            0,
		DefaultPartitions: 1,
		AutoCreateTopics:  false,
	})

	s.CreateTopic("ls-nodecrease", 1)
	td := s.GetTopic("ls-nodecrease")

	s.SetLogStartOffsetFromReplay("ls-nodecrease", 0, 100)
	s.SetLogStartOffsetFromReplay("ls-nodecrease", 0, 50) // should be ignored

	td.Partitions[0].mu.RLock()
	ls := td.Partitions[0].LogStart()
	td.Partitions[0].mu.RUnlock()

	if ls != 100 {
		t.Fatalf("logStart should stay at 100, got %d", ls)
	}
}

func TestSetLogStartOffsetFromReplay_CascadesNextReserveAndCommit(t *testing.T) {
	t.Parallel()

	s := NewState(Config{
		NodeID:            0,
		DefaultPartitions: 1,
		AutoCreateTopics:  false,
	})

	s.CreateTopic("cascade-topic", 1)
	td := s.GetTopic("cascade-topic")

	// Set logStart higher than all zero-initialized offsets.
	s.SetLogStartOffsetFromReplay("cascade-topic", 0, 200)

	pd := td.Partitions[0]
	pd.mu.RLock()
	ls := pd.LogStart()
	hw := pd.HW()
	nr := pd.nextReserve
	nc := pd.nextCommit
	pd.mu.RUnlock()

	if ls != 200 {
		t.Fatalf("expected logStart=200, got %d", ls)
	}
	if hw != 200 {
		t.Fatalf("expected hw=200 (cascaded from logStart), got %d", hw)
	}
	if nr != 200 {
		t.Fatalf("expected nextReserve=200 (cascaded from logStart>hw), got %d", nr)
	}
	if nc != 200 {
		t.Fatalf("expected nextCommit=200 (cascaded from logStart>hw), got %d", nc)
	}
}

func TestDeleteTopic(t *testing.T) {
	t.Parallel()

	s := NewState(Config{
		NodeID:            0,
		DefaultPartitions: 1,
		AutoCreateTopics:  false,
	})

	s.CreateTopic("del-topic", 2)
	if !s.TopicExists("del-topic") {
		t.Fatal("topic should exist after creation")
	}

	ok := s.DeleteTopic("del-topic")
	if !ok {
		t.Fatal("DeleteTopic should return true for existing topic")
	}
	if s.TopicExists("del-topic") {
		t.Fatal("topic should not exist after deletion")
	}
	if s.GetTopic("del-topic") != nil {
		t.Fatal("GetTopic should return nil after deletion")
	}

	// Normalized name should be removed (no collision with new topic using same normalized name)
	if col := s.CheckTopicCollision("del-topic"); col != "" {
		t.Fatalf("expected no collision after deletion, got %q", col)
	}

	// Deleting non-existent topic returns false
	ok = s.DeleteTopic("del-topic")
	if ok {
		t.Fatal("DeleteTopic should return false for non-existent topic")
	}
}

func TestDrainDeletedTopics(t *testing.T) {
	t.Parallel()

	s := NewState(Config{
		NodeID:            0,
		DefaultPartitions: 1,
		AutoCreateTopics:  false,
	})

	// No deleted topics yet
	drained := s.DrainDeletedTopics()
	if len(drained) != 0 {
		t.Fatalf("expected 0 deleted topics initially, got %d", len(drained))
	}

	// Create and delete two topics
	td1, _ := s.CreateTopic("drain-a", 1)
	td2, _ := s.CreateTopic("drain-b", 1)
	id1 := td1.ID
	id2 := td2.ID

	s.DeleteTopic("drain-a")
	s.DeleteTopic("drain-b")

	drained = s.DrainDeletedTopics()
	if len(drained) != 2 {
		t.Fatalf("expected 2 deleted topics, got %d", len(drained))
	}

	// Verify contents
	names := map[string][16]byte{}
	for _, dt := range drained {
		names[dt.Name] = dt.TopicID
	}
	if names["drain-a"] != id1 {
		t.Fatalf("drain-a: expected ID %v, got %v", id1, names["drain-a"])
	}
	if names["drain-b"] != id2 {
		t.Fatalf("drain-b: expected ID %v, got %v", id2, names["drain-b"])
	}

	// Second drain should return empty (already drained)
	drained2 := s.DrainDeletedTopics()
	if len(drained2) != 0 {
		t.Fatalf("expected 0 after second drain, got %d", len(drained2))
	}
}

func TestAddPartitions(t *testing.T) {
	t.Parallel()

	s := NewState(Config{
		NodeID:            0,
		DefaultPartitions: 1,
		AutoCreateTopics:  false,
	})

	td, _ := s.CreateTopic("grow-topic", 2)
	if len(td.Partitions) != 2 {
		t.Fatalf("expected 2 partitions initially, got %d", len(td.Partitions))
	}

	// Grow from 2 to 5
	s.AddPartitions("grow-topic", 5)
	td = s.GetTopic("grow-topic")
	if len(td.Partitions) != 5 {
		t.Fatalf("expected 5 partitions after AddPartitions, got %d", len(td.Partitions))
	}

	// Verify new partition fields
	for i := 2; i < 5; i++ {
		pd := td.Partitions[i]
		if pd.Topic != "grow-topic" {
			t.Errorf("partition %d: expected topic grow-topic, got %s", i, pd.Topic)
		}
		if pd.Index != int32(i) {
			t.Errorf("partition %d: expected index %d, got %d", i, i, pd.Index)
		}
		if pd.TopicID != td.ID {
			t.Errorf("partition %d: expected topicID %v, got %v", i, td.ID, pd.TopicID)
		}
	}

	// AddPartitions with count <= current should be no-op
	s.AddPartitions("grow-topic", 3)
	td = s.GetTopic("grow-topic")
	if len(td.Partitions) != 5 {
		t.Fatalf("expected 5 partitions (no shrink), got %d", len(td.Partitions))
	}

	// AddPartitions on non-existent topic should not panic
	s.AddPartitions("nonexistent", 10)
}

func TestAddPartitionsFromReplay(t *testing.T) {
	t.Parallel()

	s := NewState(Config{
		NodeID:            0,
		DefaultPartitions: 1,
		AutoCreateTopics:  false,
	})

	td, _ := s.CreateTopic("grow-replay-topic", 2)
	if len(td.Partitions) != 2 {
		t.Fatalf("expected 2 partitions initially, got %d", len(td.Partitions))
	}

	s.AddPartitionsFromReplay("grow-replay-topic", td.ID, 5)
	td = s.GetTopic("grow-replay-topic")
	if len(td.Partitions) != 5 {
		t.Fatalf("expected 5 partitions after replay growth, got %d", len(td.Partitions))
	}

	// Re-applying the same replay entry should be idempotent.
	s.AddPartitionsFromReplay("grow-replay-topic", td.ID, 5)
	td = s.GetTopic("grow-replay-topic")
	if len(td.Partitions) != 5 {
		t.Fatalf("expected idempotent replay to keep 5 partitions, got %d", len(td.Partitions))
	}

	// Stale replay entry with smaller count should be ignored.
	s.AddPartitionsFromReplay("grow-replay-topic", td.ID, 3)
	td = s.GetTopic("grow-replay-topic")
	if len(td.Partitions) != 5 {
		t.Fatalf("expected stale replay entry to be ignored, got %d partitions", len(td.Partitions))
	}

	// Unknown topic ID should be ignored.
	s.AddPartitionsFromReplay("grow-replay-topic", [16]byte{9, 9, 9}, 8)
	td = s.GetTopic("grow-replay-topic")
	if len(td.Partitions) != 5 {
		t.Fatalf("expected unknown topic ID to be ignored, got %d partitions", len(td.Partitions))
	}
}

func TestSnapshotEntries(t *testing.T) {
	t.Parallel()

	shutdownCh := make(chan struct{})
	defer close(shutdownCh)

	s := NewState(Config{
		NodeID:            0,
		DefaultPartitions: 1,
		AutoCreateTopics:  false,
	})
	s.SetShutdownCh(shutdownCh)
	s.SetLogger(slog.Default())

	// 1. Create a topic with configs
	s.CreateTopicWithConfigs("snap-topic", 2, map[string]string{
		"retention.ms": "3600000",
	})

	// 2. Set logStart on partition 0
	s.SetLogStartOffsetFromReplay("snap-topic", 0, 10)

	// 3. Set compaction watermark on partition 1
	s.SetCompactionWatermarkFromReplay("snap-topic", 1, 25)

	// 4. Commit an offset (creates a group)
	s.SetCommittedOffsetFromReplay("snap-group", "snap-topic", 0, 42, "m1")

	// 5. Set a producer ID
	s.pidManager.SetNextPID(100)

	entries := s.SnapshotEntries()

	// Count entry types
	typeCounts := map[byte]int{}
	for _, e := range entries {
		if len(e) == 0 {
			t.Fatal("empty entry in snapshot")
		}
		typeCounts[e[0]]++
	}

	// Should have: 1 CreateTopic, 1 OffsetCommit, 1 ProducerID, 1 LogStartOffset, 1 CompactionWatermark
	if typeCounts[0x01] != 1 {
		t.Fatalf("expected 1 CreateTopic entry, got %d", typeCounts[0x01])
	}
	if typeCounts[0x04] != 1 {
		t.Fatalf("expected 1 OffsetCommit entry, got %d", typeCounts[0x04])
	}
	if typeCounts[0x05] != 1 {
		t.Fatalf("expected 1 ProducerID entry, got %d", typeCounts[0x05])
	}
	if typeCounts[0x06] != 1 {
		t.Fatalf("expected 1 LogStartOffset entry, got %d", typeCounts[0x06])
	}
	if typeCounts[0x09] != 1 {
		t.Fatalf("expected 1 CompactionWatermark entry, got %d", typeCounts[0x09])
	}

	// Verify total count
	if len(entries) != 5 {
		t.Fatalf("expected 5 total entries, got %d", len(entries))
	}
}

func TestSnapshotEntries_Empty(t *testing.T) {
	t.Parallel()

	s := NewState(Config{
		NodeID:            0,
		DefaultPartitions: 1,
		AutoCreateTopics:  false,
	})

	entries := s.SnapshotEntries()
	if len(entries) != 0 {
		t.Fatalf("expected 0 entries for empty state, got %d", len(entries))
	}
}

func TestNormalizeTopicName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input string
		want  string
	}{
		{"foo.bar", "foo_bar"},
		{"foo_bar", "foo_bar"},
		{"foo.bar.baz", "foo_bar_baz"},
		{"no-dots", "no-dots"},
		{"", ""},
		{"....", "____"},
		{"a.b_c.d", "a_b_c_d"},
	}

	for _, tt := range tests {
		got := NormalizeTopicName(tt.input)
		if got != tt.want {
			t.Errorf("NormalizeTopicName(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestCheckTopicCollision(t *testing.T) {
	t.Parallel()

	s := NewState(Config{
		NodeID:            0,
		DefaultPartitions: 1,
		AutoCreateTopics:  false,
	})

	s.CreateTopic("foo.bar", 1)

	// foo_bar collides with foo.bar (dots and underscores are equivalent)
	if col := s.CheckTopicCollision("foo_bar"); col != "foo.bar" {
		t.Fatalf("expected collision with foo.bar, got %q", col)
	}

	// Same exact name does not count as a collision
	if col := s.CheckTopicCollision("foo.bar"); col != "" {
		t.Fatalf("expected no collision for same name, got %q", col)
	}

	// Unrelated name does not collide
	if col := s.CheckTopicCollision("baz.qux"); col != "" {
		t.Fatalf("expected no collision for unrelated name, got %q", col)
	}

	// After deletion, collision should be gone
	s.DeleteTopic("foo.bar")
	if col := s.CheckTopicCollision("foo_bar"); col != "" {
		t.Fatalf("expected no collision after deletion, got %q", col)
	}
}
