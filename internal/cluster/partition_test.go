package cluster

import (
	"testing"
)

// newTestPartition creates a PartData with sensible defaults for testing.
func newTestPartition() *PartData {
	return &PartData{
		Topic:                "test-topic",
		Index:                0,
		maxTimestampBatchIdx: -1,
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
	return pd.PushBatch(raw, meta)
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
		pd.PushBatch(raw, meta)
		pd.Unlock()

		// Mutate the original raw bytes
		raw[0] = 0xFF

		pd.RLock()
		// The stored bytes should not be affected
		if pd.batches[0].RawBytes[0] == 0xFF {
			t.Error("raw bytes were not copied - mutation affected stored batch")
		}
		pd.RUnlock()
	})
}

func TestSearchOffset(t *testing.T) {
	t.Parallel()

	t.Run("empty partition", func(t *testing.T) {
		t.Parallel()
		pd := newTestPartition()

		pd.RLock()
		_, found, atEnd := pd.SearchOffset(0)
		pd.RUnlock()

		if found {
			t.Error("expected not found for empty partition")
		}
		if !atEnd {
			t.Error("expected atEnd for empty partition")
		}
	})

	t.Run("offset past end", func(t *testing.T) {
		t.Parallel()
		pd := newTestPartition()

		pd.Lock()
		pushTestBatch(t, pd, 3, 1000) // offsets 0,1,2, HW=3
		pd.Unlock()

		pd.RLock()
		_, found, atEnd := pd.SearchOffset(3)
		pd.RUnlock()

		if found {
			t.Error("expected not found for offset at HW")
		}
		if !atEnd {
			t.Error("expected atEnd for offset at HW")
		}
	})

	t.Run("offset in single batch", func(t *testing.T) {
		t.Parallel()
		pd := newTestPartition()

		pd.Lock()
		pushTestBatch(t, pd, 5, 1000) // offsets 0-4
		pd.Unlock()

		pd.RLock()
		defer pd.RUnlock()

		for _, offset := range []int64{0, 1, 2, 3, 4} {
			idx, found, atEnd := pd.SearchOffset(offset)
			if !found {
				t.Errorf("offset %d: expected found", offset)
			}
			if atEnd {
				t.Errorf("offset %d: expected not atEnd", offset)
			}
			if idx != 0 {
				t.Errorf("offset %d: expected index 0, got %d", offset, idx)
			}
		}
	})

	t.Run("offset across multiple batches", func(t *testing.T) {
		t.Parallel()
		pd := newTestPartition()

		pd.Lock()
		pushTestBatch(t, pd, 3, 1000) // batch 0: offsets 0,1,2
		pushTestBatch(t, pd, 2, 2000) // batch 1: offsets 3,4
		pushTestBatch(t, pd, 4, 3000) // batch 2: offsets 5,6,7,8
		pd.Unlock()

		pd.RLock()
		defer pd.RUnlock()

		tests := []struct {
			offset    int64
			wantIdx   int
			wantFound bool
		}{
			{0, 0, true},
			{1, 0, true},
			{2, 0, true},
			{3, 1, true},
			{4, 1, true},
			{5, 2, true},
			{8, 2, true},
		}

		for _, tt := range tests {
			idx, found, _ := pd.SearchOffset(tt.offset)
			if found != tt.wantFound {
				t.Errorf("offset %d: found=%v, want %v", tt.offset, found, tt.wantFound)
			}
			if found && idx != tt.wantIdx {
				t.Errorf("offset %d: idx=%d, want %d", tt.offset, idx, tt.wantIdx)
			}
		}
	})

	t.Run("first offset of each batch", func(t *testing.T) {
		t.Parallel()
		pd := newTestPartition()

		pd.Lock()
		pushTestBatch(t, pd, 1, 1000) // batch 0: offset 0
		pushTestBatch(t, pd, 1, 2000) // batch 1: offset 1
		pushTestBatch(t, pd, 1, 3000) // batch 2: offset 2
		pd.Unlock()

		pd.RLock()
		defer pd.RUnlock()

		for i := 0; i < 3; i++ {
			idx, found, _ := pd.SearchOffset(int64(i))
			if !found {
				t.Errorf("offset %d: expected found", i)
			}
			if idx != i {
				t.Errorf("offset %d: idx=%d, want %d", i, idx, i)
			}
		}
	})
}

func TestFetchFrom(t *testing.T) {
	t.Parallel()

	t.Run("empty partition", func(t *testing.T) {
		t.Parallel()
		pd := newTestPartition()

		pd.RLock()
		result := pd.FetchFrom(0, 1024*1024)
		pd.RUnlock()

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

		pd.RLock()
		result := pd.FetchFrom(0, 1024*1024)
		pd.RUnlock()

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

		pd.RLock()
		result := pd.FetchFrom(3, 1024*1024)
		pd.RUnlock()

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

		pd.RLock()
		result := pd.FetchFrom(2, 1024*1024) // mid-batch
		pd.RUnlock()

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

		pd.RLock()
		result := pd.FetchFrom(0, 1) // maxBytes=1, way smaller than any batch
		pd.RUnlock()

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

		pd.RLock()
		result := pd.FetchFrom(0, 62) // room for 1 batch (61 bytes) + 1 byte
		pd.RUnlock()

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

		pd.RLock()
		result := pd.FetchFrom(3, 1024*1024)
		pd.RUnlock()

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

		pd.RLock()
		result := pd.FetchFrom(100, 1024*1024)
		pd.RUnlock()

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
		off, ts := pd.ListOffsets(-1)
		if off != 0 || ts != -1 {
			t.Errorf("Latest on empty: got (%d, %d), want (0, -1)", off, ts)
		}

		// Earliest (-2)
		off, ts = pd.ListOffsets(-2)
		if off != 0 || ts != -1 {
			t.Errorf("Earliest on empty: got (%d, %d), want (0, -1)", off, ts)
		}

		// MaxTimestamp (-3)
		off, ts = pd.ListOffsets(-3)
		if off != 0 || ts != -1 {
			t.Errorf("MaxTimestamp on empty: got (%d, %d), want (0, -1)", off, ts)
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
		off, ts := pd.ListOffsets(-1)
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
		off, ts := pd.ListOffsets(-2)
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
		off, ts := pd.ListOffsets(-3)
		pd.RUnlock()

		// Max timestamp is in batch 1 (ts=5000), last offset in that batch = 3+1 = 4
		// Return offset after batch = 4+1 = 5
		if off != 5 {
			t.Errorf("MaxTimestamp: got offset %d, want 5", off)
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
		off, ts := pd.ListOffsets(1500)
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
		off, ts := pd.ListOffsets(2000)
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
		off, ts := pd.ListOffsets(3000) // beyond all batches
		pd.RUnlock()

		if off != 4 {
			t.Errorf("Timestamp 3000: got offset %d, want 4 (HW)", off)
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
		off, ts := pd.ListOffsets(0)
		pd.RUnlock()

		if off != 0 {
			t.Errorf("Timestamp 0: got offset %d, want 0", off)
		}
		if ts != 1000 {
			t.Errorf("Timestamp 0: got ts %d, want 1000", ts)
		}
	})
}

func TestNotifyWaiters(t *testing.T) {
	t.Parallel()

	t.Run("wake single waiter", func(t *testing.T) {
		t.Parallel()
		pd := newTestPartition()

		ch := pd.RegisterWaiter()

		// NotifyWaiters should close the channel
		pd.NotifyWaiters()

		select {
		case <-ch:
			// OK - channel was closed
		default:
			t.Error("waiter channel was not closed")
		}
	})

	t.Run("wake multiple waiters", func(t *testing.T) {
		t.Parallel()
		pd := newTestPartition()

		ch1 := pd.RegisterWaiter()
		ch2 := pd.RegisterWaiter()
		ch3 := pd.RegisterWaiter()

		pd.NotifyWaiters()

		for i, ch := range []<-chan struct{}{ch1, ch2, ch3} {
			select {
			case <-ch:
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

		ch := pd.RegisterWaiter()

		select {
		case <-ch:
			t.Error("new waiter should not be immediately woken")
		default:
			// OK - channel is still open
		}
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
		off, ts := pd.ListOffsets(-3)
		pd.RUnlock()

		if ts != 5000 {
			t.Errorf("MaxTimestamp: got %d, want 5000", ts)
		}
		if off != 1 {
			t.Errorf("offset: got %d, want 1", off)
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
		_, ts := pd.ListOffsets(-3)
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
		off, ts := pd.ListOffsets(-3)
		pd.RUnlock()

		if ts != 1000 {
			t.Errorf("MaxTimestamp: got %d, want 1000", ts)
		}
		// Should point to the second batch (index 1), which has offset 1
		// Return offset after batch = 1+0+1 = 2
		if off != 2 {
			t.Errorf("offset: got %d, want 2", off)
		}
	})
}
