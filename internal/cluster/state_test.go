package cluster

import (
	"sync"
	"testing"
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
