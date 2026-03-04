package cluster

// RingBuffer is a fixed-size circular buffer for StoredBatch entries.
// It's indexed by push sequence number (monotonic count of pushes),
// NOT by Kafka offset.
type RingBuffer struct {
	batches   []StoredBatch // fixed-size, allocated once
	head      int64         // push sequence of oldest valid entry
	tail      int64         // next push sequence (= total pushes so far)
	usedBytes int64         // total bytes of batches currently in the ring
	capacity  int           // number of slots
}

// NewRingBuffer creates a ring buffer with the given number of slots.
// Slots are clamped to [16, 4096].
func NewRingBuffer(slots int) *RingBuffer {
	if slots < 16 {
		slots = 16
	}
	if slots > 4096 {
		slots = 4096
	}
	return &RingBuffer{
		batches:  make([]StoredBatch, slots),
		capacity: slots,
	}
}

// Push adds a batch to the ring buffer. Overwrites the oldest entry
// when the buffer is full. O(1) operation.
func (r *RingBuffer) Push(b StoredBatch) {
	slot := int(r.tail % int64(r.capacity))
	// Subtract evicted batch's size
	r.usedBytes -= int64(len(r.batches[slot].RawBytes))
	r.batches[slot] = b
	r.usedBytes += int64(len(b.RawBytes))
	r.tail++
	if r.tail-r.head > int64(r.capacity) {
		r.head = r.tail - int64(r.capacity)
	}
}

// FindByOffset finds the batch containing the given Kafka offset.
// Returns (batch, true) if found. Linear scan over the ring.
func (r *RingBuffer) FindByOffset(offset int64) (StoredBatch, bool) {
	for seq := r.head; seq < r.tail; seq++ {
		slot := int(seq % int64(r.capacity))
		b := r.batches[slot]
		lastOffset := b.BaseOffset + int64(b.LastOffsetDelta)
		if offset >= b.BaseOffset && offset <= lastOffset {
			return b, true
		}
		if b.BaseOffset > offset {
			break // ring is ordered by offset, no need to scan further
		}
	}
	return StoredBatch{}, false
}

// CollectFrom returns batches starting at offset up to maxBytes.
// KIP-74: always includes at least one complete batch even if > maxBytes.
func (r *RingBuffer) CollectFrom(offset int64, maxBytes int32) []StoredBatch {
	var result []StoredBatch
	var totalBytes int32

	for seq := r.head; seq < r.tail; seq++ {
		slot := int(seq % int64(r.capacity))
		b := r.batches[slot]
		lastOffset := b.BaseOffset + int64(b.LastOffsetDelta)

		if lastOffset < offset {
			continue // batch ends before requested offset
		}

		batchSize := int32(len(b.RawBytes))
		if len(result) > 0 && totalBytes+batchSize > maxBytes {
			break // respect maxBytes (but first batch is always included)
		}

		result = append(result, b)
		totalBytes += batchSize
	}

	return result
}

// Len returns the number of batches currently in the ring.
func (r *RingBuffer) Len() int {
	n := int(r.tail - r.head)
	if n > r.capacity {
		n = r.capacity
	}
	return n
}

// UsedBytes returns the total bytes of batches currently in the ring.
func (r *RingBuffer) UsedBytes() int64 {
	return r.usedBytes
}

// OldestOffset returns the base offset of the oldest batch in the ring,
// or -1 if the ring is empty.
func (r *RingBuffer) OldestOffset() int64 {
	if r.tail == r.head {
		return -1
	}
	slot := int(r.head % int64(r.capacity))
	return r.batches[slot].BaseOffset
}

// NewestOffset returns the last offset of the newest batch in the ring,
// or -1 if the ring is empty.
func (r *RingBuffer) NewestOffset() int64 {
	if r.tail == r.head {
		return -1
	}
	slot := int((r.tail - 1) % int64(r.capacity))
	b := r.batches[slot]
	return b.BaseOffset + int64(b.LastOffsetDelta)
}

// CalcRingSlots calculates per-partition ring buffer slot count from global budget.
func CalcRingSlots(maxMemory int64, numPartitions int, estimatedAvgBatchSize int64) int {
	if numPartitions <= 0 {
		numPartitions = 1
	}
	if estimatedAvgBatchSize <= 0 {
		estimatedAvgBatchSize = 16 * 1024 // 16 KiB default
	}

	slotsPerPartition := int(maxMemory / (int64(numPartitions) * estimatedAvgBatchSize))

	// Clamp to [16, 4096]
	if slotsPerPartition < 16 {
		slotsPerPartition = 16
	}
	if slotsPerPartition > 4096 {
		slotsPerPartition = 4096
	}

	return slotsPerPartition
}
