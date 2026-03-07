package cluster

import (
	"sync"
	"testing"
	"time"

	"github.com/klaudworks/klite/internal/chunk"
)

var testPool = chunk.NewPool(64*1024*1024, DefaultMaxMessageBytes)

func newTestPartition() *PartData {
	return &PartData{
		Topic:     "test-topic",
		Index:     0,
		chunkPool: testPool,
	}
}

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

		raw[0] = 0xFF

		fr := pd.Fetch(0, 1024*1024)
		if len(fr.Batches) == 0 {
			t.Fatal("expected at least one batch")
		}
		if fr.Batches[0].RawBytes[0] == 0xFF {
			t.Error("raw bytes were not copied - mutation affected stored batch")
		}
	})
}

func TestFetch(t *testing.T) {
	t.Parallel()

	t.Run("empty partition", func(t *testing.T) {
		t.Parallel()
		pd := newTestPartition()

		fr := pd.Fetch(0, 1024*1024)

		if len(fr.Batches) != 0 {
			t.Errorf("expected no batches for empty partition, got %d", len(fr.Batches))
		}
	})

	t.Run("fetch from start", func(t *testing.T) {
		t.Parallel()
		pd := newTestPartition()

		pd.Lock()
		pushTestBatch(t, pd, 3, 1000)
		pushTestBatch(t, pd, 2, 2000)
		pd.Unlock()

		fr := pd.Fetch(0, 1024*1024)

		if len(fr.Batches) != 2 {
			t.Fatalf("expected 2 batches, got %d", len(fr.Batches))
		}
		if fr.Batches[0].BaseOffset != 0 {
			t.Errorf("batch 0 base: got %d, want 0", fr.Batches[0].BaseOffset)
		}
		if fr.Batches[1].BaseOffset != 3 {
			t.Errorf("batch 1 base: got %d, want 3", fr.Batches[1].BaseOffset)
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

		fr := pd.Fetch(3, 1024*1024)

		if len(fr.Batches) != 2 {
			t.Fatalf("expected 2 batches, got %d", len(fr.Batches))
		}
		if fr.Batches[0].BaseOffset != 3 {
			t.Errorf("first batch base: got %d, want 3", fr.Batches[0].BaseOffset)
		}
	})

	t.Run("fetch within a batch", func(t *testing.T) {
		t.Parallel()
		pd := newTestPartition()

		pd.Lock()
		pushTestBatch(t, pd, 5, 1000)
		pushTestBatch(t, pd, 3, 2000)
		pd.Unlock()

		fr := pd.Fetch(2, 1024*1024)

		if len(fr.Batches) != 2 {
			t.Fatalf("expected 2 batches, got %d", len(fr.Batches))
		}
		if fr.Batches[0].BaseOffset != 0 {
			t.Errorf("first batch: got base %d, want 0", fr.Batches[0].BaseOffset)
		}
	})

	t.Run("KIP-74: at least one batch even if exceeds maxBytes", func(t *testing.T) {
		t.Parallel()
		pd := newTestPartition()

		pd.Lock()
		pushTestBatch(t, pd, 3, 1000)
		pushTestBatch(t, pd, 2, 2000)
		pd.Unlock()

		fr := pd.Fetch(0, 1)

		if len(fr.Batches) != 1 {
			t.Fatalf("KIP-74: expected at least 1 batch, got %d", len(fr.Batches))
		}
		if fr.Batches[0].BaseOffset != 0 {
			t.Errorf("first batch base: got %d, want 0", fr.Batches[0].BaseOffset)
		}
	})

	t.Run("maxBytes limits after first batch", func(t *testing.T) {
		t.Parallel()
		pd := newTestPartition()

		pd.Lock()
		pushTestBatch(t, pd, 1, 1000)
		pushTestBatch(t, pd, 1, 2000)
		pushTestBatch(t, pd, 1, 3000)
		pd.Unlock()

		fr := pd.Fetch(0, 62)

		if len(fr.Batches) != 1 {
			t.Fatalf("expected 1 batch with maxBytes=62, got %d", len(fr.Batches))
		}
	})

	t.Run("fetch at HW returns no batches", func(t *testing.T) {
		t.Parallel()
		pd := newTestPartition()

		pd.Lock()
		pushTestBatch(t, pd, 3, 1000)
		pd.Unlock()

		fr := pd.Fetch(3, 1024*1024)

		if fr.Err != 0 {
			t.Errorf("fetch at HW should not error, got err=%d", fr.Err)
		}
		if len(fr.Batches) != 0 {
			t.Errorf("expected no batches for fetch at HW, got %d", len(fr.Batches))
		}
	})

	t.Run("fetch past HW returns error", func(t *testing.T) {
		t.Parallel()
		pd := newTestPartition()

		pd.Lock()
		pushTestBatch(t, pd, 3, 1000)
		pd.Unlock()

		fr := pd.Fetch(100, 1024*1024)

		if fr.Err != ErrCodeOffsetOutOfRange {
			t.Errorf("fetch past HW: want err=%d, got err=%d", ErrCodeOffsetOutOfRange, fr.Err)
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

		off, ts := pd.ListOffsets(-1, 0)
		if off != 0 || ts != -1 {
			t.Errorf("Latest on empty: got (%d, %d), want (0, -1)", off, ts)
		}

		off, ts = pd.ListOffsets(-2, 0)
		if off != 0 || ts != -1 {
			t.Errorf("Earliest on empty: got (%d, %d), want (0, -1)", off, ts)
		}

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
		pushTestBatch(t, pd, 3, 1000)
		pushTestBatch(t, pd, 2, 5000)
		pushTestBatch(t, pd, 1, 3000)
		pd.Unlock()

		pd.RLock()
		off, ts := pd.ListOffsets(-3, 0)
		pd.RUnlock()

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
		pushTestBatch(t, pd, 2, 1000)
		pushTestBatch(t, pd, 2, 2000)
		pushTestBatch(t, pd, 2, 3000)
		pd.Unlock()

		pd.RLock()
		defer pd.RUnlock()

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
		off, ts := pd.ListOffsets(3000, 0)
		pd.RUnlock()

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
		pushTestBatch(t, pd, 3, 1000)
		pushTestBatch(t, pd, 2, 2000)
		pd.AddOpenTxn(42, 2)
		pd.Unlock()

		pd.RLock()
		defer pd.RUnlock()

		off, ts := pd.ListOffsets(-1, 0)
		if off != 5 {
			t.Errorf("read_uncommitted Latest: got offset %d, want 5", off)
		}
		if ts != -1 {
			t.Errorf("read_uncommitted Latest: got ts %d, want -1", ts)
		}

		// LSO = min(HW, oldest open txn) = 2
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

		pd.NotifyWaiters()

		select {
		case <-w.Ch():
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
			default:
				t.Errorf("waiter %d channel was not closed", i)
			}
		}
	})

	t.Run("notify with no waiters is safe", func(t *testing.T) {
		t.Parallel()
		pd := newTestPartition()
		pd.NotifyWaiters()
	})

	t.Run("new waiter after notify not woken", func(t *testing.T) {
		t.Parallel()
		pd := newTestPartition()

		pd.NotifyWaiters()

		w := NewFetchWaiter()
		pd.RegisterWaiter(w)

		select {
		case <-w.Ch():
			t.Error("new waiter should not be immediately woken")
		default:
		}
	})

	t.Run("shared waiter across partitions", func(t *testing.T) {
		t.Parallel()
		pd1 := newTestPartition()
		pd2 := newTestPartition()

		w := NewFetchWaiter()
		pd1.RegisterWaiter(w)
		pd2.RegisterWaiter(w)

		pd1.NotifyWaiters()
		select {
		case <-w.Ch():
		default:
			t.Error("shared waiter should be woken by first partition")
		}

		pd2.NotifyWaiters() // double-close safe via sync.Once
	})
}

func TestMaxTimestampTracking(t *testing.T) {
	t.Parallel()

	t.Run("first batch sets max", func(t *testing.T) {
		t.Parallel()
		pd := newTestPartition()

		pd.Lock()
		pushTestBatch(t, pd, 1, 5000)
		pd.Unlock()

		pd.RLock()
		off, ts := pd.ListOffsets(-3, 0)
		pd.RUnlock()

		if ts != 5000 {
			t.Errorf("MaxTimestamp: got %d, want 5000", ts)
		}
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
		pushTestBatch(t, pd, 1, 1000)
		pushTestBatch(t, pd, 1, 1000)
		pd.Unlock()

		pd.RLock()
		off, ts := pd.ListOffsets(-3, 0)
		pd.RUnlock()

		if ts != 1000 {
			t.Errorf("MaxTimestamp: got %d, want 1000", ts)
		}
		if off != 1 {
			t.Errorf("offset: got %d, want 1", off)
		}
	})
}

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
	base0 := pd.ReserveOffset(BatchMeta{LastOffsetDelta: 2})
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

	pd.Lock()
	pd.CommitBatch(StoredBatch{
		BaseOffset: base2, LastOffsetDelta: 0,
		RawBytes: makeSimpleBatch(1, 3000), MaxTimestamp: 3000, NumRecords: 1,
	})
	pd.Unlock()

	pd.RLock()
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

	pd.Lock()
	pd.PushBatch(makeSimpleBatch(3, 1000), BatchMeta{LastOffsetDelta: 2, MaxTimestamp: 1000, NumRecords: 3}, nil)
	pd.PushBatch(makeSimpleBatch(2, 2000), BatchMeta{LastOffsetDelta: 1, MaxTimestamp: 2000, NumRecords: 2}, nil)
	pd.Unlock()

	fr := pd.Fetch(0, 1024*1024)

	if len(fr.Batches) != 2 {
		t.Fatalf("expected 2 batches, got %d", len(fr.Batches))
	}
	if fr.Batches[0].BaseOffset != 0 {
		t.Errorf("batch 0 base: got %d, want 0", fr.Batches[0].BaseOffset)
	}
	if fr.Batches[1].BaseOffset != 3 {
		t.Errorf("batch 1 base: got %d, want 3", fr.Batches[1].BaseOffset)
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

	fr := pd.Fetch(3, 1024*1024)

	if len(fr.Batches) != 2 {
		t.Fatalf("expected 2 batches, got %d", len(fr.Batches))
	}
	if fr.Batches[0].BaseOffset != 3 {
		t.Errorf("first batch base: got %d, want 3", fr.Batches[0].BaseOffset)
	}
}

func TestAdvanceLogStartOffset(t *testing.T) {
	t.Parallel()

	pd := newTestPartition()

	pd.Lock()
	pushTestBatch(t, pd, 1, 1000) // offset 0
	pushTestBatch(t, pd, 1, 2000) // offset 1
	pushTestBatch(t, pd, 1, 3000) // offset 2
	pd.Unlock()

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

	fr := pd.Fetch(2, 1024*1024)
	if len(fr.Batches) == 0 || fr.Batches[0].BaseOffset != 2 {
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

	pd.CompactionMu.Lock()
	err := pd.AdvanceLogStartOffset(1, nil)
	pd.CompactionMu.Unlock()
	if err != nil {
		t.Fatalf("AdvanceLogStartOffset failed: %v", err)
	}

	pd.RLock()
	if pd.LogStart() != 1 {
		t.Errorf("logStart: got %d, want 1", pd.LogStart())
	}
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

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		pd.CompactionMu.Lock()
		_ = pd.AdvanceLogStartOffset(5, nil)
		pd.CompactionMu.Unlock()
	}()

	go func() {
		defer wg.Done()
		pd.CompactionMu.Lock()
		_ = pd.AdvanceLogStartOffset(7, nil)
		pd.CompactionMu.Unlock()
	}()

	wg.Wait()

	pd.RLock()
	if pd.LogStart() != 7 {
		t.Errorf("logStart: got %d, want 7", pd.LogStart())
	}
	if pd.BatchCount() != 3 {
		t.Errorf("batch count: got %d, want 3", pd.BatchCount())
	}
	pd.RUnlock()
}

// TestSkipOffsetsUnblocksPendingCommits verifies that SkipOffsets drains
// pending commits queued behind a gap. Scenario: A(0-2), B(3-4), C(5).
// Commit C, skip B, commit A => HW should reach 6.
func TestSkipOffsetsUnblocksPendingCommits(t *testing.T) {
	t.Parallel()

	pd := newTestPartitionWithChunks()

	pd.Lock()
	base0 := pd.ReserveOffset(BatchMeta{LastOffsetDelta: 2}) // offsets 0-2
	base1 := pd.ReserveOffset(BatchMeta{LastOffsetDelta: 1}) // offsets 3-4
	base2 := pd.ReserveOffset(BatchMeta{LastOffsetDelta: 0}) // offset 5
	pd.Unlock()

	_ = base1

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

	pd.Lock()
	pd.SkipOffsets(base1, 2)
	pd.Unlock()

	pd.RLock()
	hw := pd.HW()
	pd.RUnlock()

	if hw != 6 {
		t.Errorf("HW after SkipOffsets: got %d, want 6 (gap should be skipped, C should drain)", hw)
	}
}

func TestSkipOffsetsOutOfOrder(t *testing.T) {
	t.Parallel()

	pd := newTestPartitionWithChunks()

	pd.Lock()
	base0 := pd.ReserveOffset(BatchMeta{LastOffsetDelta: 2}) // offsets 0-2
	base1 := pd.ReserveOffset(BatchMeta{LastOffsetDelta: 1}) // offsets 3-4
	base2 := pd.ReserveOffset(BatchMeta{LastOffsetDelta: 0}) // offset 5
	pd.Unlock()

	pd.Lock()
	pd.SkipOffsets(base1, 2)
	pd.Unlock()

	pd.RLock()
	if pd.HW() != 0 {
		t.Errorf("HW after out-of-order skip: got %d, want 0", pd.HW())
	}
	pd.RUnlock()

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

	// Commit A drains: A(0-2) -> skip B(3-4) -> C(5) -> HW=6
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

// TestFetchGapReturnsNextAvailableBatch verifies that Fetch skips past offset
// gaps (from WAL failures) by returning the next available chunk data.
func TestFetchGapReturnsNextAvailableBatch(t *testing.T) {
	t.Parallel()

	pd := newTestPartitionWithChunks()

	raw0 := makeSimpleBatch(3, 1000)
	raw1 := makeSimpleBatch(3, 2000)

	pd.Lock()
	pd.PushBatch(raw0, BatchMeta{LastOffsetDelta: 2, MaxTimestamp: 1000, NumRecords: 3}, nil)
	pd.ReserveOffset(BatchMeta{LastOffsetDelta: 1}) // gap: offsets 3-4
	base2 := pd.ReserveOffset(BatchMeta{LastOffsetDelta: 2})
	AssignOffset(raw1, base2)
	pd.AppendToChunk(raw1, chunk.ChunkBatch{
		BaseOffset: base2, LastOffsetDelta: 2, MaxTimestamp: 2000, NumRecords: 3,
	}, nil)

	pd.SetHW(8)
	pd.Unlock()

	fr := pd.Fetch(3, 1024*1024)

	if len(fr.Batches) == 0 {
		t.Fatal("Fetch(3) returned no batches; expected batch at offset 5 (skip gap)")
	}
	if fr.Batches[0].BaseOffset != 5 {
		t.Errorf("Fetch(3) first batch base: got %d, want 5", fr.Batches[0].BaseOffset)
	}
}

// TestAcquireSpareChunkDoesNotHoldPartitionLock verifies that AcquireSpareChunk
// does not hold pd.mu while blocking on an exhausted pool (deadlock prevention).
func TestAcquireSpareChunkDoesNotHoldPartitionLock(t *testing.T) {
	t.Parallel()

	chunkSize := 128
	pool := chunk.NewPool(int64(2*chunkSize), chunkSize)
	pd := &PartData{
		Topic:     "test-topic",
		Index:     0,
		chunkPool: pool,
	}

	batchSize := 61
	spare := pd.AcquireSpareChunk(batchSize)
	pd.Lock()
	pd.PushBatch(makeSimpleBatch(1, 1000), BatchMeta{
		LastOffsetDelta: 0, MaxTimestamp: 1000, NumRecords: 1,
	}, spare)
	pd.Unlock()
	spare = pd.AcquireSpareChunk(batchSize)
	pd.Lock()
	pd.PushBatch(makeSimpleBatch(1, 2000), BatchMeta{
		LastOffsetDelta: 0, MaxTimestamp: 2000, NumRecords: 1,
	}, spare)
	pd.Unlock()

	// Exhaust the pool
	held := pool.Acquire()

	blocked := make(chan struct{})
	acquired := make(chan *chunk.Chunk, 1)
	go func() {
		close(blocked)
		c := pd.AcquireSpareChunk(batchSize)
		acquired <- c
	}()

	<-blocked
	time.Sleep(20 * time.Millisecond)

	// pd.Lock() must succeed, proving blocked goroutine doesn't hold pd.mu
	lockAcquired := make(chan struct{})
	go func() {
		pd.Lock()
		close(lockAcquired)
		pd.DetachSealedChunks(false)
		pd.Unlock()
	}()

	select {
	case <-lockAcquired:
	case <-time.After(time.Second):
		t.Fatal("pd.Lock() blocked — AcquireSpareChunk is holding the partition lock (deadlock)")
	}

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

// TestFetchHWConsistency verifies that Fetch never returns batches whose
// offsets extend beyond the HW reported in the same response.
func TestFetchHWConsistency(t *testing.T) {
	t.Parallel()

	pd := newTestPartition()

	const batches = 5000
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < batches; i++ {
			raw := makeSimpleBatch(10, int64(i))
			meta, err := ParseBatchHeader(raw)
			if err != nil {
				return
			}
			spare := pd.AcquireSpareChunk(len(raw))
			pd.Lock()
			_, spare = pd.PushBatch(raw, meta, spare)
			pd.Unlock()
			pd.ReleaseSpareChunk(spare)
		}
	}()

	wg.Add(1)
	var violations int
	go func() {
		defer wg.Done()
		fetchOffset := int64(0)
		for fetchOffset < batches*10 {
			fr := pd.Fetch(fetchOffset, 1024*1024)
			if fr.Err != 0 {
				if fr.Err == ErrCodeOffsetOutOfRange {
					t.Errorf("unexpected OffsetOutOfRange at fetchOffset=%d hw=%d logStart=%d",
						fetchOffset, fr.HW, fr.LogStart)
					return
				}
				continue
			}

			for _, b := range fr.Batches {
				lastOffset := b.BaseOffset + int64(b.LastOffsetDelta)
				if lastOffset >= fr.HW {
					violations++
					if violations <= 5 {
						t.Errorf("batch [%d..%d] extends to offset %d but response HW=%d",
							b.BaseOffset, lastOffset, lastOffset, fr.HW)
					}
				}
			}

			if len(fr.Batches) > 0 {
				last := fr.Batches[len(fr.Batches)-1]
				fetchOffset = last.BaseOffset + int64(last.LastOffsetDelta) + 1
			}
		}
	}()

	wg.Wait()
	if violations > 0 {
		t.Errorf("total HW consistency violations: %d", violations)
	}
}

// TestFetchHWConsistencyTwoPhase verifies the HW invariant under the two-phase
// produce path (ReserveOffset -> AppendToChunk -> CommitBatch). Batch data is
// visible in the chunk pool before HW advances; Fetch must not return it.
func TestFetchHWConsistencyTwoPhase(t *testing.T) {
	t.Parallel()

	pd := newTestPartition()

	const iterations = 5000
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			raw := makeSimpleBatch(10, int64(i))
			meta, err := ParseBatchHeader(raw)
			if err != nil {
				return
			}
			stored := make([]byte, len(raw))
			copy(stored, raw)

			spare := pd.AcquireSpareChunk(len(stored))

			pd.Lock()
			baseOffset := pd.ReserveOffset(meta)
			AssignOffset(stored, baseOffset)
			spare = pd.AppendToChunk(stored, chunk.ChunkBatch{
				BaseOffset:      baseOffset,
				LastOffsetDelta: meta.LastOffsetDelta,
				MaxTimestamp:    meta.MaxTimestamp,
				NumRecords:      meta.NumRecords,
			}, spare)
			pd.Unlock()
			pd.ReleaseSpareChunk(spare)

			pd.Lock()
			pd.CommitBatch(StoredBatch{
				BaseOffset:      baseOffset,
				LastOffsetDelta: meta.LastOffsetDelta,
				RawBytes:        stored,
				MaxTimestamp:    meta.MaxTimestamp,
				NumRecords:      meta.NumRecords,
			})
			pd.Unlock()
			pd.NotifyWaiters()
		}
	}()

	wg.Add(1)
	var violations int
	go func() {
		defer wg.Done()
		fetchOffset := int64(0)
		for fetchOffset < iterations*10 {
			fr := pd.Fetch(fetchOffset, 1024*1024)
			if fr.Err != 0 {
				if fr.Err == ErrCodeOffsetOutOfRange {
					t.Errorf("unexpected OffsetOutOfRange at fetchOffset=%d hw=%d logStart=%d",
						fetchOffset, fr.HW, fr.LogStart)
					return
				}
				continue
			}

			for _, b := range fr.Batches {
				lastOffset := b.BaseOffset + int64(b.LastOffsetDelta)
				if lastOffset >= fr.HW {
					violations++
					if violations <= 5 {
						t.Errorf("batch [%d..%d] extends to offset %d but response HW=%d",
							b.BaseOffset, lastOffset, lastOffset, fr.HW)
					}
				}
			}

			if len(fr.Batches) > 0 {
				last := fr.Batches[len(fr.Batches)-1]
				fetchOffset = last.BaseOffset + int64(last.LastOffsetDelta) + 1
			}
		}
	}()

	wg.Wait()
	if violations > 0 {
		t.Errorf("total HW consistency violations: %d", violations)
	}
}

func TestFetchCaughtUp(t *testing.T) {
	t.Parallel()
	pd := newTestPartition()

	pd.Lock()
	pushTestBatch(t, pd, 5, 1000)
	pd.Unlock()

	fr := pd.Fetch(5, 1024*1024)
	if fr.Err != 0 {
		t.Errorf("Fetch at HW should not error, got err=%d", fr.Err)
	}
	if len(fr.Batches) != 0 {
		t.Errorf("Fetch at HW should return no batches, got %d", len(fr.Batches))
	}
	if fr.HW != 5 {
		t.Errorf("HW: got %d, want 5", fr.HW)
	}
}

func TestFetchOutOfRangeBounds(t *testing.T) {
	t.Parallel()
	pd := newTestPartition()

	pd.Lock()
	pushTestBatch(t, pd, 5, 1000)
	pd.Unlock()

	pd.CompactionMu.Lock()
	_ = pd.AdvanceLogStartOffset(2, nil)
	pd.CompactionMu.Unlock()

	fr := pd.Fetch(1, 1024*1024)
	if fr.Err != ErrCodeOffsetOutOfRange {
		t.Errorf("fetch below logStart: want err=%d, got err=%d", ErrCodeOffsetOutOfRange, fr.Err)
	}

	fr = pd.Fetch(6, 1024*1024)
	if fr.Err != ErrCodeOffsetOutOfRange {
		t.Errorf("fetch above HW: want err=%d, got err=%d", ErrCodeOffsetOutOfRange, fr.Err)
	}

	fr = pd.Fetch(2, 1024*1024)
	if fr.Err != 0 {
		t.Errorf("fetch at logStart: want err=0, got err=%d", fr.Err)
	}
}

func TestFilterBatchesByHW(t *testing.T) {
	t.Parallel()

	t.Run("all below HW", func(t *testing.T) {
		t.Parallel()
		batches := []StoredBatch{
			{BaseOffset: 0, LastOffsetDelta: 2}, // offsets 0-2
			{BaseOffset: 3, LastOffsetDelta: 1}, // offsets 3-4
		}
		result := filterBatchesByHW(batches, 10)
		if len(result) != 2 {
			t.Errorf("expected 2 batches, got %d", len(result))
		}
	})

	t.Run("last batch straddles HW", func(t *testing.T) {
		t.Parallel()
		batches := []StoredBatch{
			{BaseOffset: 0, LastOffsetDelta: 2}, // offsets 0-2, end=3 (ok if hw>=3)
			{BaseOffset: 3, LastOffsetDelta: 4}, // offsets 3-7, end=8 (dropped if hw<8)
		}
		result := filterBatchesByHW(batches, 5)
		if len(result) != 1 {
			t.Errorf("expected 1 batch (second should be filtered), got %d", len(result))
		}
	})

	t.Run("all above HW", func(t *testing.T) {
		t.Parallel()
		batches := []StoredBatch{
			{BaseOffset: 10, LastOffsetDelta: 2}, // offsets 10-12, end=13
		}
		result := filterBatchesByHW(batches, 5)
		if len(result) != 0 {
			t.Errorf("expected 0 batches, got %d", len(result))
		}
	})

	t.Run("empty input", func(t *testing.T) {
		t.Parallel()
		result := filterBatchesByHW(nil, 10)
		if len(result) != 0 {
			t.Errorf("expected 0 batches, got %d", len(result))
		}
	})

	t.Run("exact HW boundary", func(t *testing.T) {
		t.Parallel()
		batches := []StoredBatch{
			{BaseOffset: 0, LastOffsetDelta: 4}, // offsets 0-4, end=5
		}
		// HW=5 means offsets 0-4 are committed, so end(5) <= hw(5) is ok
		result := filterBatchesByHW(batches, 5)
		if len(result) != 1 {
			t.Errorf("batch ending exactly at HW should be included, got %d", len(result))
		}
		// HW=4 means offsets 0-3 are committed, end(5) > hw(4)
		result = filterBatchesByHW(batches, 4)
		if len(result) != 0 {
			t.Errorf("batch ending past HW should be excluded, got %d", len(result))
		}
	})
}

// TestAdvanceLogStartOffsetBeyondHWCapsAtHW verifies that AdvanceLogStartOffset
// caps the new logStart at HW to prevent the logStart > HW invariant violation.
func TestAdvanceLogStartOffsetBeyondHWCapsAtHW(t *testing.T) {
	t.Parallel()

	pd := newTestPartition()

	pd.Lock()
	pushTestBatch(t, pd, 3, 1000) // offsets 0-2, HW=3
	pd.Unlock()

	// Try to advance logStart past HW
	pd.CompactionMu.Lock()
	err := pd.AdvanceLogStartOffset(100, nil)
	pd.CompactionMu.Unlock()
	if err != nil {
		t.Fatalf("AdvanceLogStartOffset failed: %v", err)
	}

	pd.RLock()
	logStart := pd.LogStart()
	hw := pd.HW()
	pd.RUnlock()

	if logStart != hw {
		t.Errorf("logStart should be capped at HW: got logStart=%d, hw=%d", logStart, hw)
	}
	if logStart != 3 {
		t.Errorf("logStart: got %d, want 3", logStart)
	}
}

// TestConcurrentProduceAndRetention verifies that concurrent produce and
// AdvanceLogStartOffset don't corrupt state. This tests the lock ordering
// (CompactionMu -> pd.mu).
func TestConcurrentProduceAndRetention(t *testing.T) {
	t.Parallel()

	pd := newTestPartition()

	const iterations = 1000
	var wg sync.WaitGroup

	// Writer goroutine: push batches continuously
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			raw := makeSimpleBatch(1, int64(i))
			meta, err := ParseBatchHeader(raw)
			if err != nil {
				return
			}
			spare := pd.AcquireSpareChunk(len(raw))
			pd.Lock()
			_, spare = pd.PushBatch(raw, meta, spare)
			pd.Unlock()
			pd.ReleaseSpareChunk(spare)
		}
	}()

	// Retention goroutine: advance logStart periodically
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			pd.RLock()
			hw := pd.HW()
			pd.RUnlock()

			if hw > 2 {
				pd.CompactionMu.Lock()
				_ = pd.AdvanceLogStartOffset(hw-1, nil)
				pd.CompactionMu.Unlock()
			}
		}
	}()

	wg.Wait()

	pd.RLock()
	hw := pd.HW()
	logStart := pd.LogStart()
	pd.RUnlock()

	if logStart > hw {
		t.Errorf("invariant violated: logStart(%d) > hw(%d)", logStart, hw)
	}
}

// TestSkipOffsetsEdgeCases verifies edge cases for SkipOffsets.
func TestSkipOffsetsEdgeCases(t *testing.T) {
	t.Parallel()

	t.Run("skip count=1", func(t *testing.T) {
		t.Parallel()
		pd := newTestPartitionWithChunks()

		pd.Lock()
		base0 := pd.ReserveOffset(BatchMeta{LastOffsetDelta: 0}) // offset 0
		base1 := pd.ReserveOffset(BatchMeta{LastOffsetDelta: 0}) // offset 1
		pd.Unlock()

		// Commit first, skip second (single offset)
		pd.Lock()
		pd.CommitBatch(StoredBatch{
			BaseOffset: base0, LastOffsetDelta: 0,
			RawBytes: makeSimpleBatch(1, 1000), MaxTimestamp: 1000, NumRecords: 1,
		})
		pd.Unlock()

		pd.RLock()
		if pd.HW() != 1 {
			t.Errorf("HW after commit: got %d, want 1", pd.HW())
		}
		pd.RUnlock()

		pd.Lock()
		pd.SkipOffsets(base1, 1) // skip single offset
		pd.Unlock()

		pd.RLock()
		hw := pd.HW()
		pd.RUnlock()
		if hw != 2 {
			t.Errorf("HW after skip(count=1): got %d, want 2", hw)
		}
	})

	t.Run("skip already-past range is no-op", func(t *testing.T) {
		t.Parallel()
		pd := newTestPartitionWithChunks()

		pd.Lock()
		pd.ReserveOffset(BatchMeta{LastOffsetDelta: 2}) // offsets 0-2
		pd.CommitBatch(StoredBatch{
			BaseOffset: 0, LastOffsetDelta: 2,
			RawBytes: makeSimpleBatch(3, 1000), MaxTimestamp: 1000, NumRecords: 3,
		})
		pd.Unlock()

		pd.RLock()
		if pd.HW() != 3 {
			t.Errorf("HW after commit: got %d, want 3", pd.HW())
		}
		pd.RUnlock()

		// Skip range 0-1 which is already past nextCommit (3)
		pd.Lock()
		pd.SkipOffsets(0, 2)
		pd.Unlock()

		pd.RLock()
		hw := pd.HW()
		pd.RUnlock()
		if hw != 3 {
			t.Errorf("HW after no-op skip: got %d, want 3 (unchanged)", hw)
		}
	})
}

// TestConcurrentDetachSealedChunksAndFetch verifies that Fetch reads are
// safe when DetachSealedChunks runs concurrently (simulating the S3 flusher).
func TestConcurrentDetachSealedChunksAndFetch(t *testing.T) {
	t.Parallel()

	chunkSize := 256
	pool := chunk.NewPool(int64(8*chunkSize), chunkSize)
	pd := &PartData{
		Topic:     "test-topic",
		Index:     0,
		chunkPool: pool,
	}

	const batches = 200
	var wg sync.WaitGroup

	// Writer goroutine: produce batches
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < batches; i++ {
			raw := makeSimpleBatch(1, int64(i))
			meta, err := ParseBatchHeader(raw)
			if err != nil {
				return
			}
			spare := pd.AcquireSpareChunk(len(raw))
			pd.Lock()
			_, spare = pd.PushBatch(raw, meta, spare)
			pd.Unlock()
			pd.ReleaseSpareChunk(spare)
		}
	}()

	// Flusher goroutine: detach sealed chunks and release them
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < batches; i++ {
			pd.Lock()
			chunks := pd.DetachSealedChunks(false)
			pd.Unlock()
			for _, c := range chunks {
				pool.Release(c)
			}
			time.Sleep(100 * time.Microsecond)
		}
	}()

	// Reader goroutine: Fetch repeatedly
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < batches*2; i++ {
			pd.RLock()
			hw := pd.HW()
			pd.RUnlock()
			if hw > 0 {
				fr := pd.Fetch(0, 1024*1024)
				// We just verify no panic; data may or may not be in chunks
				_ = fr
			}
		}
	}()

	wg.Wait()
}

// TestFetchAtLogStartBoundary verifies Fetch at exactly logStart when
// chunk data starts at a higher offset (requiring cold-path fallthrough).
func TestFetchAtLogStartBoundary(t *testing.T) {
	t.Parallel()

	pd := newTestPartition()

	pd.Lock()
	pushTestBatch(t, pd, 3, 1000) // offsets 0-2
	pushTestBatch(t, pd, 3, 2000) // offsets 3-5
	pushTestBatch(t, pd, 3, 3000) // offsets 6-8
	pd.Unlock()

	// Advance logStart to 3 (first batch is below logStart)
	pd.CompactionMu.Lock()
	_ = pd.AdvanceLogStartOffset(3, nil)
	pd.CompactionMu.Unlock()

	// Fetch at exactly logStart
	fr := pd.Fetch(3, 1024*1024)
	if fr.Err != 0 {
		t.Errorf("Fetch at logStart: got err=%d, want 0", fr.Err)
	}
	if len(fr.Batches) == 0 {
		t.Fatal("Fetch at logStart should return data")
	}
	if fr.Batches[0].BaseOffset != 3 {
		t.Errorf("first batch base: got %d, want 3", fr.Batches[0].BaseOffset)
	}
	if fr.LogStart != 3 {
		t.Errorf("LogStart in response: got %d, want 3", fr.LogStart)
	}
}
