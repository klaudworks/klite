package cluster

import (
	"sync"
	"testing"
	"time"

	"github.com/klaudworks/klite/internal/chunk"
)

// testPool is a shared chunk pool for tests. 64 MiB budget with default chunk size.
var testPool = chunk.NewPool(64*1024*1024, DefaultMaxMessageBytes)

// newTestPartition creates a PartData with a chunk pool for testing.
func newTestPartition() *PartData {
	return &PartData{
		Topic:     "test-topic",
		Index:     0,
		chunkPool: testPool,
	}
}

// pushTestBatch is a helper that creates a batch with the given parameters,
// parses it, and pushes it to the partition.
func pushTestBatch(t *testing.T, pd *PartData, numRecords int32, maxTimestamp int64) int64 {
	t.Helper()
	raw := makeSimpleBatch(numRecords, maxTimestamp)
	meta, err := ParseBatchHeader(raw)
	if err != nil {
		t.Fatalf("ParseBatchHeader failed: %v", err)
	}
	base, _ := pd.PushBatch(raw, meta, nil)
	return base
}

func TestPushBatch(t *testing.T) {
	t.Parallel()

	t.Run("single batch", func(t *testing.T) {
		t.Parallel()
		pd := newTestPartition()

		pd.Lock()
		base := pushTestBatch(t, pd, 5, 2000)
		pd.Unlock()

		if base != 0 {
			t.Errorf("first batch base offset: got %d, want 0", base)
		}

		pd.RLock()
		if pd.HW() != 5 {
			t.Errorf("HW after 5 records: got %d, want 5", pd.HW())
		}
		if pd.BatchCount() != 1 {
			t.Errorf("batch count: got %d, want 1", pd.BatchCount())
		}
		pd.RUnlock()
	})

	t.Run("multiple batches", func(t *testing.T) {
		t.Parallel()
		pd := newTestPartition()

		pd.Lock()
		base0 := pushTestBatch(t, pd, 3, 1000) // offsets 0,1,2
		base1 := pushTestBatch(t, pd, 5, 2000) // offsets 3,4,5,6,7
		base2 := pushTestBatch(t, pd, 1, 3000) // offset 8
		pd.Unlock()

		if base0 != 0 {
			t.Errorf("batch 0 base: got %d, want 0", base0)
		}
		if base1 != 3 {
			t.Errorf("batch 1 base: got %d, want 3", base1)
		}
		if base2 != 8 {
			t.Errorf("batch 2 base: got %d, want 8", base2)
		}

		pd.RLock()
		if pd.HW() != 9 {
			t.Errorf("HW: got %d, want 9", pd.HW())
		}
		pd.RUnlock()
	})

	t.Run("single record batch", func(t *testing.T) {
		t.Parallel()
		pd := newTestPartition()

		pd.Lock()
		base := pushTestBatch(t, pd, 1, 5000)
		pd.Unlock()

		if base != 0 {
			t.Errorf("base: got %d, want 0", base)
		}

		pd.RLock()
		if pd.HW() != 1 {
			t.Errorf("HW: got %d, want 1", pd.HW())
		}
		pd.RUnlock()
	})

	t.Run("raw bytes are copied", func(t *testing.T) {
		t.Parallel()
		pd := newTestPartition()

		raw := makeSimpleBatch(1, 1000)
		meta, err := ParseBatchHeader(raw)
		if err != nil {
			t.Fatalf("ParseBatchHeader failed: %v", err)
		}

		pd.Lock()
		pd.PushBatch(raw, meta, nil)
		pd.Unlock()

		// Mutate the original raw bytes
		raw[0] = 0xFF

		// The stored bytes should not be affected
		fetched := pd.FetchFrom(0, 1024*1024)
		if len(fetched) == 0 {
			t.Fatal("expected at least one batch")
		}
		if fetched[0].RawBytes[0] == 0xFF {
			t.Error("raw bytes were not copied - mutation affected stored batch")
		}
	})
}

func TestFetchFrom(t *testing.T) {
	t.Parallel()

	t.Run("empty partition", func(t *testing.T) {
		t.Parallel()
		pd := newTestPartition()

		result := pd.FetchFrom(0, 1024*1024)

		if result != nil {
			t.Errorf("expected nil for empty partition, got %d batches", len(result))
		}
	})

	t.Run("fetch from start", func(t *testing.T) {
		t.Parallel()
		pd := newTestPartition()

		pd.Lock()
		pushTestBatch(t, pd, 3, 1000)
		pushTestBatch(t, pd, 2, 2000)
		pd.Unlock()

		result := pd.FetchFrom(0, 1024*1024)

		if len(result) != 2 {
			t.Fatalf("expected 2 batches, got %d", len(result))
		}
		if result[0].BaseOffset != 0 {
			t.Errorf("batch 0 base: got %d, want 0", result[0].BaseOffset)
		}
		if result[1].BaseOffset != 3 {
			t.Errorf("batch 1 base: got %d, want 3", result[1].BaseOffset)
		}
	})

	t.Run("fetch from middle", func(t *testing.T) {
		t.Parallel()
		pd := newTestPartition()

		pd.Lock()
		pushTestBatch(t, pd, 3, 1000) // offsets 0,1,2
		pushTestBatch(t, pd, 2, 2000) // offsets 3,4
		pushTestBatch(t, pd, 1, 3000) // offset 5
		pd.Unlock()

		result := pd.FetchFrom(3, 1024*1024)

		if len(result) != 2 {
			t.Fatalf("expected 2 batches, got %d", len(result))
		}
		if result[0].BaseOffset != 3 {
			t.Errorf("first batch base: got %d, want 3", result[0].BaseOffset)
		}
	})

	t.Run("fetch within a batch", func(t *testing.T) {
		t.Parallel()
		pd := newTestPartition()

		pd.Lock()
		pushTestBatch(t, pd, 5, 1000) // offsets 0-4
		pushTestBatch(t, pd, 3, 2000) // offsets 5-7
		pd.Unlock()

		result := pd.FetchFrom(2, 1024*1024) // mid-batch

		if len(result) != 2 {
			t.Fatalf("expected 2 batches, got %d", len(result))
		}
		if result[0].BaseOffset != 0 {
			t.Errorf("first batch: got base %d, want 0 (batch containing offset 2)", result[0].BaseOffset)
		}
	})

	t.Run("KIP-74: at least one batch even if exceeds maxBytes", func(t *testing.T) {
		t.Parallel()
		pd := newTestPartition()

		pd.Lock()
		pushTestBatch(t, pd, 3, 1000) // 61-byte batch
		pushTestBatch(t, pd, 2, 2000) // 61-byte batch
		pd.Unlock()

		result := pd.FetchFrom(0, 1) // maxBytes=1, way smaller than any batch

		if len(result) != 1 {
			t.Fatalf("KIP-74: expected at least 1 batch, got %d", len(result))
		}
		if result[0].BaseOffset != 0 {
			t.Errorf("first batch base: got %d, want 0", result[0].BaseOffset)
		}
	})

	t.Run("maxBytes limits after first batch", func(t *testing.T) {
		t.Parallel()
		pd := newTestPartition()

		pd.Lock()
		pushTestBatch(t, pd, 1, 1000) // 61 bytes
		pushTestBatch(t, pd, 1, 2000) // 61 bytes
		pushTestBatch(t, pd, 1, 3000) // 61 bytes
		pd.Unlock()

		result := pd.FetchFrom(0, 62) // room for 1 batch (61 bytes) + 1 byte

		// Should get 1 batch (61 bytes), second batch (61+61=122) exceeds 62
		if len(result) != 1 {
			t.Fatalf("expected 1 batch with maxBytes=62, got %d", len(result))
		}
	})

	t.Run("fetch at HW returns nil", func(t *testing.T) {
		t.Parallel()
		pd := newTestPartition()

		pd.Lock()
		pushTestBatch(t, pd, 3, 1000) // HW=3
		pd.Unlock()

		result := pd.FetchFrom(3, 1024*1024)

		if result != nil {
			t.Errorf("expected nil for fetch at HW, got %d batches", len(result))
		}
	})

	t.Run("fetch past HW returns nil", func(t *testing.T) {
		t.Parallel()
		pd := newTestPartition()

		pd.Lock()
		pushTestBatch(t, pd, 3, 1000)
		pd.Unlock()

		result := pd.FetchFrom(100, 1024*1024)

		if result != nil {
			t.Errorf("expected nil for fetch past HW, got %d batches", len(result))
		}
	})
}

func TestListOffsets(t *testing.T) {
	t.Parallel()

	t.Run("empty partition", func(t *testing.T) {
		t.Parallel()
		pd := newTestPartition()

		pd.RLock()
		defer pd.RUnlock()

		// Latest (-1)
		off, ts := pd.ListOffsets(-1, 0)
		if off != 0 || ts != -1 {
			t.Errorf("Latest on empty: got (%d, %d), want (0, -1)", off, ts)
		}

		// Earliest (-2)
		off, ts = pd.ListOffsets(-2, 0)
		if off != 0 || ts != -1 {
			t.Errorf("Earliest on empty: got (%d, %d), want (0, -1)", off, ts)
		}

		// MaxTimestamp (-3) — empty partition returns (-1, -1) per kfake/Kafka behavior
		off, ts = pd.ListOffsets(-3, 0)
		if off != -1 || ts != -1 {
			t.Errorf("MaxTimestamp on empty: got (%d, %d), want (-1, -1)", off, ts)
		}
	})

	t.Run("latest", func(t *testing.T) {
		t.Parallel()
		pd := newTestPartition()

		pd.Lock()
		pushTestBatch(t, pd, 3, 1000) // HW=3
		pushTestBatch(t, pd, 2, 2000) // HW=5
		pd.Unlock()

		pd.RLock()
		off, ts := pd.ListOffsets(-1, 0)
		pd.RUnlock()

		if off != 5 {
			t.Errorf("Latest: got offset %d, want 5", off)
		}
		if ts != -1 {
			t.Errorf("Latest: got ts %d, want -1", ts)
		}
	})

	t.Run("earliest", func(t *testing.T) {
		t.Parallel()
		pd := newTestPartition()

		pd.Lock()
		pushTestBatch(t, pd, 3, 1000)
		pd.Unlock()

		pd.RLock()
		off, ts := pd.ListOffsets(-2, 0)
		pd.RUnlock()

		if off != 0 {
			t.Errorf("Earliest: got offset %d, want 0", off)
		}
		if ts != -1 {
			t.Errorf("Earliest: got ts %d, want -1", ts)
		}
	})

	t.Run("max timestamp", func(t *testing.T) {
		t.Parallel()
		pd := newTestPartition()

		pd.Lock()
		pushTestBatch(t, pd, 3, 1000) // batch 0: offsets 0-2, ts=1000
		pushTestBatch(t, pd, 2, 5000) // batch 1: offsets 3-4, ts=5000
		pushTestBatch(t, pd, 1, 3000) // batch 2: offset 5, ts=3000
		pd.Unlock()

		pd.RLock()
		off, ts := pd.ListOffsets(-3, 0)
		pd.RUnlock()

		// Max timestamp is in batch 1 (ts=5000), last offset in that batch = 3+1 = 4
		if off != 4 {
			t.Errorf("MaxTimestamp: got offset %d, want 4", off)
		}
		if ts != 5000 {
			t.Errorf("MaxTimestamp: got ts %d, want 5000", ts)
		}
	})

	t.Run("timestamp lookup found", func(t *testing.T) {
		t.Parallel()
		pd := newTestPartition()

		pd.Lock()
		pushTestBatch(t, pd, 2, 1000) // batch 0: offsets 0-1, maxTS=1000
		pushTestBatch(t, pd, 2, 2000) // batch 1: offsets 2-3, maxTS=2000
		pushTestBatch(t, pd, 2, 3000) // batch 2: offsets 4-5, maxTS=3000
		pd.Unlock()

		pd.RLock()
		defer pd.RUnlock()

		// Find first batch with maxTS >= 1500 -> batch 1 (maxTS=2000)
		off, ts := pd.ListOffsets(1500, 0)
		if off != 2 {
			t.Errorf("Timestamp 1500: got offset %d, want 2", off)
		}
		if ts != 2000 {
			t.Errorf("Timestamp 1500: got ts %d, want 2000", ts)
		}
	})

	t.Run("timestamp lookup exact match", func(t *testing.T) {
		t.Parallel()
		pd := newTestPartition()

		pd.Lock()
		pushTestBatch(t, pd, 2, 1000)
		pushTestBatch(t, pd, 2, 2000)
		pd.Unlock()

		pd.RLock()
		off, ts := pd.ListOffsets(2000, 0)
		pd.RUnlock()

		if off != 2 {
			t.Errorf("Timestamp 2000: got offset %d, want 2", off)
		}
		if ts != 2000 {
			t.Errorf("Timestamp 2000: got ts %d, want 2000", ts)
		}
	})

	t.Run("timestamp lookup not found", func(t *testing.T) {
		t.Parallel()
		pd := newTestPartition()

		pd.Lock()
		pushTestBatch(t, pd, 2, 1000)
		pushTestBatch(t, pd, 2, 2000)
		pd.Unlock()

		pd.RLock()
		off, ts := pd.ListOffsets(3000, 0) // beyond all batches
		pd.RUnlock()

		// No match returns (-1, -1) per Kafka/kfake behavior
		if off != -1 {
			t.Errorf("Timestamp 3000: got offset %d, want -1", off)
		}
		if ts != -1 {
			t.Errorf("Timestamp 3000: got ts %d, want -1", ts)
		}
	})

	t.Run("timestamp 0 returns first batch", func(t *testing.T) {
		t.Parallel()
		pd := newTestPartition()

		pd.Lock()
		pushTestBatch(t, pd, 2, 1000)
		pd.Unlock()

		pd.RLock()
		off, ts := pd.ListOffsets(0, 0)
		pd.RUnlock()

		if off != 0 {
			t.Errorf("Timestamp 0: got offset %d, want 0", off)
		}
		if ts != 1000 {
			t.Errorf("Timestamp 0: got ts %d, want 1000", ts)
		}
	})

	t.Run("latest read_committed returns LSO", func(t *testing.T) {
		t.Parallel()
		pd := newTestPartition()

		pd.Lock()
		pushTestBatch(t, pd, 3, 1000) // HW=3
		pushTestBatch(t, pd, 2, 2000) // HW=5
		// Simulate an open transaction starting at offset 2
		pd.AddOpenTxn(42, 2)
		pd.Unlock()

		pd.RLock()
		defer pd.RUnlock()

		// read_uncommitted: should return HW (5)
		off, ts := pd.ListOffsets(-1, 0)
		if off != 5 {
			t.Errorf("read_uncommitted Latest: got offset %d, want 5", off)
		}
		if ts != -1 {
			t.Errorf("read_uncommitted Latest: got ts %d, want -1", ts)
		}

		// read_committed: should return LSO = min(HW, oldest open txn) = 2
		off, ts = pd.ListOffsets(-1, 1)
		if off != 2 {
			t.Errorf("read_committed Latest: got offset %d, want 2 (LSO)", off)
		}
		if ts != -1 {
			t.Errorf("read_committed Latest: got ts %d, want -1", ts)
		}
	})
}

func TestNotifyWaiters(t *testing.T) {
	t.Parallel()

	t.Run("wake single waiter", func(t *testing.T) {
		t.Parallel()
		pd := newTestPartition()

		w := NewFetchWaiter()
		pd.RegisterWaiter(w)

		// NotifyWaiters should close the channel
		pd.NotifyWaiters()

		select {
		case <-w.Ch():
			// OK - channel was closed
		default:
			t.Error("waiter channel was not closed")
		}
	})

	t.Run("wake multiple waiters", func(t *testing.T) {
		t.Parallel()
		pd := newTestPartition()

		w1 := NewFetchWaiter()
		w2 := NewFetchWaiter()
		w3 := NewFetchWaiter()
		pd.RegisterWaiter(w1)
		pd.RegisterWaiter(w2)
		pd.RegisterWaiter(w3)

		pd.NotifyWaiters()

		for i, w := range []*FetchWaiter{w1, w2, w3} {
			select {
			case <-w.Ch():
				// OK
			default:
				t.Errorf("waiter %d channel was not closed", i)
			}
		}
	})

	t.Run("notify with no waiters is safe", func(t *testing.T) {
		t.Parallel()
		pd := newTestPartition()
		// Should not panic
		pd.NotifyWaiters()
	})

	t.Run("new waiter after notify not woken", func(t *testing.T) {
		t.Parallel()
		pd := newTestPartition()

		pd.NotifyWaiters() // notify with no waiters

		w := NewFetchWaiter()
		pd.RegisterWaiter(w)

		select {
		case <-w.Ch():
			t.Error("new waiter should not be immediately woken")
		default:
			// OK - channel is still open
		}
	})

	t.Run("shared waiter across partitions", func(t *testing.T) {
		t.Parallel()
		pd1 := newTestPartition()
		pd2 := newTestPartition()

		// Register the same waiter on two partitions
		w := NewFetchWaiter()
		pd1.RegisterWaiter(w)
		pd2.RegisterWaiter(w)

		// Notifying one partition should wake the shared waiter
		pd1.NotifyWaiters()
		select {
		case <-w.Ch():
			// OK
		default:
			t.Error("shared waiter should be woken by first partition")
		}

		// Notifying the second partition should not panic (sync.Once protects double-close)
		pd2.NotifyWaiters()
	})
}

func TestMaxTimestampTracking(t *testing.T) {
	t.Parallel()

	t.Run("first batch sets max", func(t *testing.T) {
		t.Parallel()
		pd := newTestPartition()

		pd.Lock()
		pushTestBatch(t, pd, 1, 5000) // 1 record: offsets 0-0
		pd.Unlock()

		pd.RLock()
		off, ts := pd.ListOffsets(-3, 0)
		pd.RUnlock()

		if ts != 5000 {
			t.Errorf("MaxTimestamp: got %d, want 5000", ts)
		}
		// Last offset in batch = BaseOffset(0) + LastOffsetDelta(0) = 0
		if off != 0 {
			t.Errorf("offset: got %d, want 0", off)
		}
	})

	t.Run("newer timestamp updates max", func(t *testing.T) {
		t.Parallel()
		pd := newTestPartition()

		pd.Lock()
		pushTestBatch(t, pd, 1, 1000)
		pushTestBatch(t, pd, 1, 5000)
		pushTestBatch(t, pd, 1, 3000)
		pd.Unlock()

		pd.RLock()
		_, ts := pd.ListOffsets(-3, 0)
		pd.RUnlock()

		if ts != 5000 {
			t.Errorf("MaxTimestamp: got %d, want 5000", ts)
		}
	})

	t.Run("equal timestamp updates to latest batch", func(t *testing.T) {
		t.Parallel()
		pd := newTestPartition()

		pd.Lock()
		pushTestBatch(t, pd, 1, 1000) // batch 0: offset 0
		pushTestBatch(t, pd, 1, 1000) // batch 1: offset 1 (same ts)
		pd.Unlock()

		pd.RLock()
		off, ts := pd.ListOffsets(-3, 0)
		pd.RUnlock()

		if ts != 1000 {
			t.Errorf("MaxTimestamp: got %d, want 1000", ts)
		}
		// Should point to the second batch (index 1), last offset = 1+0 = 1
		if off != 1 {
			t.Errorf("offset: got %d, want 1", off)
		}
	})
}

// --- Phase 3 tests: ChunkPool, ReserveOffset, CommitBatch ---

func newTestPartitionWithChunks() *PartData {
	pd := &PartData{
		Topic:     "test-topic",
		Index:     0,
		TopicID:   [16]byte{1, 2, 3},
		chunkPool: testPool,
	}
	return pd
}

func TestReserveOffset(t *testing.T) {
	t.Parallel()

	pd := newTestPartitionWithChunks()

	pd.Lock()
	// Reserve 3 records: offsets 0, 1, 2
	base0 := pd.ReserveOffset(BatchMeta{LastOffsetDelta: 2})
	// Reserve 2 records: offsets 3, 4
	base1 := pd.ReserveOffset(BatchMeta{LastOffsetDelta: 1})
	pd.Unlock()

	if base0 != 0 {
		t.Errorf("first reserve base: got %d, want 0", base0)
	}
	if base1 != 3 {
		t.Errorf("second reserve base: got %d, want 3", base1)
	}
}

func TestCommitBatchInOrder(t *testing.T) {
	t.Parallel()

	pd := newTestPartitionWithChunks()

	raw0 := makeSimpleBatch(3, 1000)
	raw1 := makeSimpleBatch(2, 2000)

	pd.Lock()
	base0 := pd.ReserveOffset(BatchMeta{LastOffsetDelta: 2})
	base1 := pd.ReserveOffset(BatchMeta{LastOffsetDelta: 1})
	pd.Unlock()

	// Commit in order
	pd.Lock()
	pd.CommitBatch(StoredBatch{
		BaseOffset: base0, LastOffsetDelta: 2,
		RawBytes: raw0, MaxTimestamp: 1000, NumRecords: 3,
	})
	pd.Unlock()

	pd.RLock()
	if pd.HW() != 3 {
		t.Errorf("HW after first commit: got %d, want 3", pd.HW())
	}
	pd.RUnlock()

	pd.Lock()
	pd.CommitBatch(StoredBatch{
		BaseOffset: base1, LastOffsetDelta: 1,
		RawBytes: raw1, MaxTimestamp: 2000, NumRecords: 2,
	})
	pd.Unlock()

	pd.RLock()
	if pd.HW() != 5 {
		t.Errorf("HW after second commit: got %d, want 5", pd.HW())
	}
	pd.RUnlock()
}

func TestCommitBatchOutOfOrder(t *testing.T) {
	t.Parallel()

	pd := newTestPartitionWithChunks()

	pd.Lock()
	base0 := pd.ReserveOffset(BatchMeta{LastOffsetDelta: 2}) // offsets 0-2
	base1 := pd.ReserveOffset(BatchMeta{LastOffsetDelta: 1}) // offsets 3-4
	base2 := pd.ReserveOffset(BatchMeta{LastOffsetDelta: 0}) // offset 5
	pd.Unlock()

	// Commit OUT of order: 2, 0, 1
	pd.Lock()
	pd.CommitBatch(StoredBatch{
		BaseOffset: base2, LastOffsetDelta: 0,
		RawBytes: makeSimpleBatch(1, 3000), MaxTimestamp: 3000, NumRecords: 1,
	})
	pd.Unlock()

	pd.RLock()
	// HW should still be 0 (waiting for base0)
	if pd.HW() != 0 {
		t.Errorf("HW after out-of-order commit: got %d, want 0", pd.HW())
	}
	pd.RUnlock()

	pd.Lock()
	pd.CommitBatch(StoredBatch{
		BaseOffset: base0, LastOffsetDelta: 2,
		RawBytes: makeSimpleBatch(3, 1000), MaxTimestamp: 1000, NumRecords: 3,
	})
	pd.Unlock()

	pd.RLock()
	// HW should be 3 (base0 committed, but base1 still missing)
	if pd.HW() != 3 {
		t.Errorf("HW after base0 commit: got %d, want 3", pd.HW())
	}
	pd.RUnlock()

	pd.Lock()
	pd.CommitBatch(StoredBatch{
		BaseOffset: base1, LastOffsetDelta: 1,
		RawBytes: makeSimpleBatch(2, 2000), MaxTimestamp: 2000, NumRecords: 2,
	})
	pd.Unlock()

	pd.RLock()
	// Now all 3 are committed, HW should be 6
	if pd.HW() != 6 {
		t.Errorf("HW after all commits: got %d, want 6", pd.HW())
	}
	pd.RUnlock()

	_ = base0
	_ = base1
	_ = base2
}

func TestReadCascadeRingBuffer(t *testing.T) {
	t.Parallel()

	pd := newTestPartitionWithChunks()

	// Push some batches
	pd.Lock()
	pd.PushBatch(makeSimpleBatch(3, 1000), BatchMeta{LastOffsetDelta: 2, MaxTimestamp: 1000, NumRecords: 3}, nil)
	pd.PushBatch(makeSimpleBatch(2, 2000), BatchMeta{LastOffsetDelta: 1, MaxTimestamp: 2000, NumRecords: 2}, nil)
	pd.Unlock()

	result := pd.FetchFrom(0, 1024*1024)

	if len(result) != 2 {
		t.Fatalf("expected 2 batches, got %d", len(result))
	}
	if result[0].BaseOffset != 0 {
		t.Errorf("batch 0 base: got %d, want 0", result[0].BaseOffset)
	}
	if result[1].BaseOffset != 3 {
		t.Errorf("batch 1 base: got %d, want 3", result[1].BaseOffset)
	}
}

func TestReadCascadeFetchFromMiddle(t *testing.T) {
	t.Parallel()

	pd := newTestPartitionWithChunks()

	pd.Lock()
	pd.PushBatch(makeSimpleBatch(3, 1000), BatchMeta{LastOffsetDelta: 2, MaxTimestamp: 1000, NumRecords: 3}, nil)
	pd.PushBatch(makeSimpleBatch(2, 2000), BatchMeta{LastOffsetDelta: 1, MaxTimestamp: 2000, NumRecords: 2}, nil)
	pd.PushBatch(makeSimpleBatch(1, 3000), BatchMeta{LastOffsetDelta: 0, MaxTimestamp: 3000, NumRecords: 1}, nil)
	pd.Unlock()

	result := pd.FetchFrom(3, 1024*1024) // Start at offset 3

	if len(result) != 2 {
		t.Fatalf("expected 2 batches, got %d", len(result))
	}
	if result[0].BaseOffset != 3 {
		t.Errorf("first batch base: got %d, want 3", result[0].BaseOffset)
	}
}

// --- Phase 6 tests: Retention ---

func TestAdvanceLogStartOffset(t *testing.T) {
	t.Parallel()

	pd := newTestPartition()

	pd.Lock()
	pushTestBatch(t, pd, 1, 1000) // offset 0
	pushTestBatch(t, pd, 1, 2000) // offset 1
	pushTestBatch(t, pd, 1, 3000) // offset 2
	pd.Unlock()

	// Advance without metadata.log (pass nil)
	pd.CompactionMu.Lock()
	err := pd.AdvanceLogStartOffset(2, nil)
	pd.CompactionMu.Unlock()
	if err != nil {
		t.Fatalf("AdvanceLogStartOffset failed: %v", err)
	}

	pd.RLock()
	if pd.LogStart() != 2 {
		t.Errorf("logStart: got %d, want 2", pd.LogStart())
	}
	if pd.BatchCount() != 1 {
		t.Errorf("batch count: got %d, want 1", pd.BatchCount())
	}
	pd.RUnlock()

	// Remaining batch should be offset 2
	fetched := pd.FetchFrom(2, 1024*1024)
	if len(fetched) == 0 || fetched[0].BaseOffset != 2 {
		t.Errorf("remaining batch base: expected 2")
	}
}

func TestAdvanceLogStartOffsetStraddlingBatch(t *testing.T) {
	t.Parallel()

	pd := newTestPartition()

	pd.Lock()
	pushTestBatch(t, pd, 3, 1000) // offsets 0,1,2
	pushTestBatch(t, pd, 3, 2000) // offsets 3,4,5
	pd.Unlock()

	// Advance to middle of first batch (offset 1)
	pd.CompactionMu.Lock()
	err := pd.AdvanceLogStartOffset(1, nil)
	pd.CompactionMu.Unlock()
	if err != nil {
		t.Fatalf("AdvanceLogStartOffset failed: %v", err)
	}

	pd.RLock()
	// logStart should be exactly 1, not rounded to batch boundary
	if pd.LogStart() != 1 {
		t.Errorf("logStart: got %d, want 1", pd.LogStart())
	}
	// Both batches should still be present (first batch straddles logStart)
	if pd.BatchCount() != 2 {
		t.Errorf("batch count: got %d, want 2", pd.BatchCount())
	}
	pd.RUnlock()
}

func TestAdvanceLogStartOffsetConcurrent(t *testing.T) {
	t.Parallel()

	pd := newTestPartition()

	pd.Lock()
	for i := 0; i < 10; i++ {
		pushTestBatch(t, pd, 1, int64(1000+i*100)) // offsets 0..9
	}
	pd.Unlock()

	// Two goroutines concurrently advancing logStartOffset
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		pd.CompactionMu.Lock()
		pd.AdvanceLogStartOffset(5, nil)
		pd.CompactionMu.Unlock()
	}()

	go func() {
		defer wg.Done()
		pd.CompactionMu.Lock()
		pd.AdvanceLogStartOffset(7, nil)
		pd.CompactionMu.Unlock()
	}()

	wg.Wait()

	pd.RLock()
	// logStart should end up at 7 (the higher value)
	if pd.LogStart() != 7 {
		t.Errorf("logStart: got %d, want 7", pd.LogStart())
	}
	// Should have 3 remaining batches (offsets 7, 8, 9)
	if pd.BatchCount() != 3 {
		t.Errorf("batch count: got %d, want 3", pd.BatchCount())
	}
	pd.RUnlock()
}

// --- WAL error recovery tests: SkipOffsets and FetchFrom gap ---

// TestSkipOffsetsUnblocksPendingCommits verifies that SkipOffsets advances
// nextCommit past a gap, allowing subsequent out-of-order commits that were
// queued behind the gap to drain and advance HW.
//
// Scenario: Reserve offsets for batches A(0-2), B(3-4), C(5). Commit C first
// (out of order — queued). Skip B (WAL error). Commit A. After A commits,
// SkipOffsets(B) should drain the gap and then C should also drain, advancing
// HW to 6.
func TestSkipOffsetsUnblocksPendingCommits(t *testing.T) {
	t.Parallel()

	pd := newTestPartitionWithChunks()

	pd.Lock()
	base0 := pd.ReserveOffset(BatchMeta{LastOffsetDelta: 2}) // offsets 0-2
	base1 := pd.ReserveOffset(BatchMeta{LastOffsetDelta: 1}) // offsets 3-4
	base2 := pd.ReserveOffset(BatchMeta{LastOffsetDelta: 0}) // offset 5
	pd.Unlock()

	_ = base1

	// Commit C (out of order) — queued as pending
	pd.Lock()
	pd.CommitBatch(StoredBatch{
		BaseOffset: base2, LastOffsetDelta: 0,
		RawBytes: makeSimpleBatch(1, 3000), MaxTimestamp: 3000, NumRecords: 1,
	})
	pd.Unlock()

	pd.RLock()
	if pd.HW() != 0 {
		t.Errorf("HW after out-of-order C commit: got %d, want 0", pd.HW())
	}
	pd.RUnlock()

	// Commit A (in order) — HW should advance to 3
	pd.Lock()
	pd.CommitBatch(StoredBatch{
		BaseOffset: base0, LastOffsetDelta: 2,
		RawBytes: makeSimpleBatch(3, 1000), MaxTimestamp: 1000, NumRecords: 3,
	})
	pd.Unlock()

	pd.RLock()
	if pd.HW() != 3 {
		t.Errorf("HW after A commit: got %d, want 3", pd.HW())
	}
	pd.RUnlock()

	// Skip B (offsets 3-4) — simulates WAL write failure for batch B.
	// After skip, nextCommit should jump past 3-4, then drain C from
	// pendingCommits, advancing HW to 6.
	pd.Lock()
	pd.SkipOffsets(base1, 2) // skip 2 offsets starting at base1=3
	pd.Unlock()

	pd.RLock()
	hw := pd.HW()
	pd.RUnlock()

	if hw != 6 {
		t.Errorf("HW after SkipOffsets: got %d, want 6 (gap should be skipped, C should drain)", hw)
	}
}

// TestSkipOffsetsOutOfOrder verifies that SkipOffsets works when the skip
// arrives before the preceding batch has committed (out-of-order skip).
func TestSkipOffsetsOutOfOrder(t *testing.T) {
	t.Parallel()

	pd := newTestPartitionWithChunks()

	pd.Lock()
	base0 := pd.ReserveOffset(BatchMeta{LastOffsetDelta: 2}) // offsets 0-2
	base1 := pd.ReserveOffset(BatchMeta{LastOffsetDelta: 1}) // offsets 3-4
	base2 := pd.ReserveOffset(BatchMeta{LastOffsetDelta: 0}) // offset 5
	pd.Unlock()

	// Skip B FIRST (before A commits) — out-of-order skip
	pd.Lock()
	pd.SkipOffsets(base1, 2)
	pd.Unlock()

	// HW should still be 0 — A hasn't committed yet
	pd.RLock()
	if pd.HW() != 0 {
		t.Errorf("HW after out-of-order skip: got %d, want 0", pd.HW())
	}
	pd.RUnlock()

	// Commit C (out of order)
	pd.Lock()
	pd.CommitBatch(StoredBatch{
		BaseOffset: base2, LastOffsetDelta: 0,
		RawBytes: makeSimpleBatch(1, 3000), MaxTimestamp: 3000, NumRecords: 1,
	})
	pd.Unlock()

	// HW still 0 — waiting for A
	pd.RLock()
	if pd.HW() != 0 {
		t.Errorf("HW after out-of-order C commit: got %d, want 0", pd.HW())
	}
	pd.RUnlock()

	// Commit A — should drain: A(0-2) → skip B(3-4) → C(5) → HW=6
	pd.Lock()
	pd.CommitBatch(StoredBatch{
		BaseOffset: base0, LastOffsetDelta: 2,
		RawBytes: makeSimpleBatch(3, 1000), MaxTimestamp: 1000, NumRecords: 3,
	})
	pd.Unlock()

	pd.RLock()
	hw := pd.HW()
	pd.RUnlock()

	if hw != 6 {
		t.Errorf("HW after all commits + skip: got %d, want 6", hw)
	}
}

// TestFetchFromGapReturnsNextAvailableBatch verifies that when the chunk pool
// has no data at the requested offset but has data at a higher offset, and
// cold storage (WAL/S3) also has nothing, FetchFrom returns the higher chunk
// data instead of nil. This prevents consumers from getting stuck on offset
// gaps caused by WAL write failures.
//
// This test uses SetHW to directly set the high watermark, bypassing
// SkipOffsets/CommitBatch, so it isolates the FetchFrom behavior.
func TestFetchFromGapReturnsNextAvailableBatch(t *testing.T) {
	t.Parallel()

	pd := newTestPartitionWithChunks()

	// Simulate a gap: push batch at offsets 0-2, then push batch at offsets
	// 5-7 (skipping 3-4). Use PushBatch for 0-2, then manually place
	// batch 5-7 at the right offset in the chunk pool.
	raw0 := makeSimpleBatch(3, 1000)
	raw1 := makeSimpleBatch(3, 2000)

	pd.Lock()
	// Batch 0: offsets 0-2
	pd.PushBatch(raw0, BatchMeta{LastOffsetDelta: 2, MaxTimestamp: 1000, NumRecords: 3}, nil)

	// Reserve and skip offsets 3-4 (just advance nextOffset without data)
	pd.ReserveOffset(BatchMeta{LastOffsetDelta: 1})

	// Batch 2: offsets 5-7 — assign offset and push to chunk
	base2 := pd.ReserveOffset(BatchMeta{LastOffsetDelta: 2})
	AssignOffset(raw1, base2)
	pd.AppendToChunk(raw1, chunk.ChunkBatch{
		BaseOffset: base2, LastOffsetDelta: 2, MaxTimestamp: 2000, NumRecords: 3,
	}, nil)

	// Directly set HW past the gap. In real code this would be done via
	// CommitBatch + SkipOffsets, but we're isolating the FetchFrom test.
	pd.SetHW(8)
	pd.Unlock()

	// Fetch from offset 3 (in the gap). No WAL or S3 configured, so cold
	// storage returns nothing. Should return batch at offset 5 instead of nil.
	result := pd.FetchFrom(3, 1024*1024)

	if len(result) == 0 {
		t.Fatal("FetchFrom(3) returned no batches; expected batch at offset 5 (skip gap)")
	}
	if result[0].BaseOffset != 5 {
		t.Errorf("FetchFrom(3) first batch base: got %d, want 5", result[0].BaseOffset)
	}
}

// TestAcquireSpareChunkDoesNotHoldPartitionLock verifies that when
// AcquireSpareChunk blocks on an exhausted pool, pd.mu is NOT held.
// This is the core invariant that prevents deadlock between the produce
// path and the S3 flusher (which needs pd.mu to detach chunks).
func TestAcquireSpareChunkDoesNotHoldPartitionLock(t *testing.T) {
	t.Parallel()

	// Small pool: 2 chunks with tiny chunk size so we can fill them easily.
	chunkSize := 128
	pool := chunk.NewPool(int64(2*chunkSize), chunkSize)
	pd := &PartData{
		Topic:     "test-topic",
		Index:     0,
		chunkPool: pool,
	}

	// Push a batch into the partition so chunkCurrent is non-nil.
	batchSize := 61
	spare := pd.AcquireSpareChunk(batchSize)
	pd.Lock()
	pd.PushBatch(makeSimpleBatch(1, 1000), BatchMeta{
		LastOffsetDelta: 0, MaxTimestamp: 1000, NumRecords: 1,
	}, spare)
	pd.Unlock()
	// chunkCurrent now has 61 bytes used out of 128.
	// A second 61-byte batch won't fit (61+61=122 > 128... actually it fits).
	// Push another to ensure the chunk is nearly full.
	spare = pd.AcquireSpareChunk(batchSize)
	pd.Lock()
	pd.PushBatch(makeSimpleBatch(1, 2000), BatchMeta{
		LastOffsetDelta: 0, MaxTimestamp: 2000, NumRecords: 1,
	}, spare)
	pd.Unlock()
	// chunkCurrent now has 122 bytes used. Next 61-byte batch won't fit (122+61=183 > 128).
	// AcquireSpareChunk will see needsNew=true.

	// Exhaust the pool: one chunk is in chunkCurrent, grab the other.
	held := pool.Acquire()

	// Pool is now empty. AcquireSpareChunk should block.
	blocked := make(chan struct{})
	acquired := make(chan *chunk.Chunk, 1)
	go func() {
		close(blocked)
		c := pd.AcquireSpareChunk(batchSize)
		acquired <- c
	}()

	<-blocked
	time.Sleep(20 * time.Millisecond) // give goroutine time to enter Acquire()

	// The key assertion: pd.Lock() must succeed immediately, proving the
	// blocked goroutine is NOT holding the partition lock.
	lockAcquired := make(chan struct{})
	go func() {
		pd.Lock()
		close(lockAcquired)
		// Simulate flusher: detach sealed chunks
		pd.DetachSealedChunks(false)
		pd.Unlock()
	}()

	select {
	case <-lockAcquired:
		// pd.mu was free — no deadlock.
	case <-time.After(time.Second):
		t.Fatal("pd.Lock() blocked — AcquireSpareChunk is holding the partition lock (deadlock)")
	}

	// Release the held chunk to unblock the AcquireSpareChunk goroutine.
	pool.Release(held)

	select {
	case c := <-acquired:
		if c == nil {
			t.Error("AcquireSpareChunk returned nil after pool was freed")
		} else {
			pool.Release(c)
		}
	case <-time.After(time.Second):
		t.Fatal("AcquireSpareChunk did not unblock after pool release")
	}
}
