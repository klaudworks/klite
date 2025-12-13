package cluster

import (
	"testing"
)

func TestRingBufferPush(t *testing.T) {
	t.Parallel()

	t.Run("basic push", func(t *testing.T) {
		t.Parallel()
		r := NewRingBuffer(16)

		r.Push(StoredBatch{BaseOffset: 0, LastOffsetDelta: 2, RawBytes: make([]byte, 100)})
		r.Push(StoredBatch{BaseOffset: 3, LastOffsetDelta: 1, RawBytes: make([]byte, 50)})

		if r.Len() != 2 {
			t.Errorf("Len: got %d, want 2", r.Len())
		}
		if r.UsedBytes() != 150 {
			t.Errorf("UsedBytes: got %d, want 150", r.UsedBytes())
		}
	})

	t.Run("wrap around", func(t *testing.T) {
		t.Parallel()
		r := NewRingBuffer(16) // min clamped

		// Push more than capacity
		for i := 0; i < 20; i++ {
			r.Push(StoredBatch{
				BaseOffset:      int64(i * 5),
				LastOffsetDelta: 4,
				RawBytes:        make([]byte, 10),
			})
		}

		// Should have exactly capacity entries
		if r.Len() != 16 {
			t.Errorf("Len after wrap: got %d, want 16", r.Len())
		}

		// Oldest should be batch 4 (i=4, offset=20)
		if r.OldestOffset() != 20 {
			t.Errorf("OldestOffset: got %d, want 20", r.OldestOffset())
		}

		// Newest should be batch 19 (i=19, offset=99)
		if r.NewestOffset() != 99 {
			t.Errorf("NewestOffset: got %d, want 99", r.NewestOffset())
		}
	})

	t.Run("used bytes tracks eviction", func(t *testing.T) {
		t.Parallel()
		r := NewRingBuffer(16) // min clamped

		// Fill completely with 100-byte batches
		for i := 0; i < 16; i++ {
			r.Push(StoredBatch{BaseOffset: int64(i), RawBytes: make([]byte, 100)})
		}
		if r.UsedBytes() != 1600 {
			t.Errorf("UsedBytes full: got %d, want 1600", r.UsedBytes())
		}

		// Push one more (200 bytes), evicting one 100-byte batch
		r.Push(StoredBatch{BaseOffset: 16, RawBytes: make([]byte, 200)})
		if r.UsedBytes() != 1700 {
			t.Errorf("UsedBytes after evict: got %d, want 1700", r.UsedBytes())
		}
	})
}

func TestRingBufferFindByOffset(t *testing.T) {
	t.Parallel()

	r := NewRingBuffer(16)
	r.Push(StoredBatch{BaseOffset: 0, LastOffsetDelta: 2, RawBytes: make([]byte, 10)})
	r.Push(StoredBatch{BaseOffset: 3, LastOffsetDelta: 1, RawBytes: make([]byte, 10)})
	r.Push(StoredBatch{BaseOffset: 5, LastOffsetDelta: 4, RawBytes: make([]byte, 10)})

	tests := []struct {
		offset int64
		found  bool
		base   int64
	}{
		{0, true, 0},
		{1, true, 0},
		{2, true, 0},
		{3, true, 3},
		{4, true, 3},
		{5, true, 5},
		{9, true, 5},
		{10, false, 0},
	}

	for _, tt := range tests {
		b, found := r.FindByOffset(tt.offset)
		if found != tt.found {
			t.Errorf("offset %d: found=%v, want %v", tt.offset, found, tt.found)
		}
		if found && b.BaseOffset != tt.base {
			t.Errorf("offset %d: base=%d, want %d", tt.offset, b.BaseOffset, tt.base)
		}
	}
}

func TestRingBufferCollectFrom(t *testing.T) {
	t.Parallel()

	t.Run("collect all", func(t *testing.T) {
		t.Parallel()
		r := NewRingBuffer(16)
		r.Push(StoredBatch{BaseOffset: 0, LastOffsetDelta: 2, RawBytes: make([]byte, 100)})
		r.Push(StoredBatch{BaseOffset: 3, LastOffsetDelta: 1, RawBytes: make([]byte, 100)})
		r.Push(StoredBatch{BaseOffset: 5, LastOffsetDelta: 4, RawBytes: make([]byte, 100)})

		result := r.CollectFrom(0, 1024*1024)
		if len(result) != 3 {
			t.Errorf("got %d batches, want 3", len(result))
		}
	})

	t.Run("collect from middle", func(t *testing.T) {
		t.Parallel()
		r := NewRingBuffer(16)
		r.Push(StoredBatch{BaseOffset: 0, LastOffsetDelta: 2, RawBytes: make([]byte, 100)})
		r.Push(StoredBatch{BaseOffset: 3, LastOffsetDelta: 1, RawBytes: make([]byte, 100)})
		r.Push(StoredBatch{BaseOffset: 5, LastOffsetDelta: 4, RawBytes: make([]byte, 100)})

		result := r.CollectFrom(3, 1024*1024)
		if len(result) != 2 {
			t.Errorf("got %d batches, want 2", len(result))
		}
		if result[0].BaseOffset != 3 {
			t.Errorf("first batch base: got %d, want 3", result[0].BaseOffset)
		}
	})

	t.Run("KIP-74 at least one batch", func(t *testing.T) {
		t.Parallel()
		r := NewRingBuffer(16)
		r.Push(StoredBatch{BaseOffset: 0, LastOffsetDelta: 2, RawBytes: make([]byte, 100)})

		result := r.CollectFrom(0, 1) // maxBytes=1, way smaller than batch
		if len(result) != 1 {
			t.Errorf("got %d batches, want 1 (KIP-74)", len(result))
		}
	})

	t.Run("maxBytes limits after first", func(t *testing.T) {
		t.Parallel()
		r := NewRingBuffer(16)
		r.Push(StoredBatch{BaseOffset: 0, LastOffsetDelta: 0, RawBytes: make([]byte, 100)})
		r.Push(StoredBatch{BaseOffset: 1, LastOffsetDelta: 0, RawBytes: make([]byte, 100)})
		r.Push(StoredBatch{BaseOffset: 2, LastOffsetDelta: 0, RawBytes: make([]byte, 100)})

		result := r.CollectFrom(0, 150) // first batch (100) + second (200) > 150
		if len(result) != 1 {
			t.Errorf("got %d batches, want 1", len(result))
		}
	})

	t.Run("empty ring", func(t *testing.T) {
		t.Parallel()
		r := NewRingBuffer(16)

		result := r.CollectFrom(0, 1024*1024)
		if len(result) != 0 {
			t.Errorf("got %d batches from empty ring, want 0", len(result))
		}
	})
}

func TestRingBufferSlotClamping(t *testing.T) {
	t.Parallel()

	r := NewRingBuffer(5) // below minimum
	if r.capacity != 16 {
		t.Errorf("capacity: got %d, want 16 (min clamp)", r.capacity)
	}

	r = NewRingBuffer(10000) // above maximum
	if r.capacity != 4096 {
		t.Errorf("capacity: got %d, want 4096 (max clamp)", r.capacity)
	}
}

func TestCalcRingSlots(t *testing.T) {
	t.Parallel()

	t.Run("clamped to min", func(t *testing.T) {
		t.Parallel()
		got := CalcRingSlots(512*1024*1024, 100000, 16*1024)
		if got != 16 {
			t.Errorf("got %d, want 16", got)
		}
	})

	t.Run("clamped to max", func(t *testing.T) {
		t.Parallel()
		got := CalcRingSlots(10*1024*1024*1024, 10, 16*1024)
		if got != 4096 {
			t.Errorf("got %d, want 4096", got)
		}
	})

	t.Run("within range", func(t *testing.T) {
		t.Parallel()
		got := CalcRingSlots(512*1024*1024, 100, 16*1024)
		if got < 16 || got > 4096 {
			t.Errorf("got %d, want [16, 4096]", got)
		}
		// Should be approximately 320 (512M / (100 * 16K))
		if got < 300 || got > 340 {
			t.Errorf("got %d, want ~320", got)
		}
	})

	t.Run("many partitions", func(t *testing.T) {
		t.Parallel()
		got := CalcRingSlots(512*1024*1024, 1000, 16*1024)
		if got < 16 || got > 4096 {
			t.Errorf("got %d, want [16, 4096]", got)
		}
		if got < 30 || got > 36 {
			t.Errorf("got %d, want ~32", got)
		}
	})
}
