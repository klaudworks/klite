package wal

import (
	"sort"
	"sync"
)

type TopicPartition struct {
	TopicID   [16]byte
	Partition int32
}

type IndexEntry struct {
	BaseOffset  int64  // First Kafka offset in the batch
	LastOffset  int64  // BaseOffset + LastOffsetDelta
	SegmentSeq  uint64 // Segment file's starting sequence number
	FileOffset  int64  // Byte position within segment file
	EntrySize   int32  // Total entry size on disk (for direct read)
	BatchSize   int32  // Size of the RecordBatch payload
	WALSequence uint64 // WAL sequence number of this entry
}

// Index is thread-safe via RWMutex.
type Index struct {
	mu         sync.RWMutex
	partitions map[TopicPartition][]IndexEntry
}

func NewIndex() *Index {
	return &Index{
		partitions: make(map[TopicPartition][]IndexEntry),
	}
}

// Add inserts a new index entry. Entries must be added in offset order.
func (idx *Index) Add(tp TopicPartition, entry IndexEntry) {
	idx.mu.Lock()
	idx.partitions[tp] = append(idx.partitions[tp], entry)
	idx.mu.Unlock()
}

func (idx *Index) Lookup(tp TopicPartition, fetchOffset int64, maxBytes int32) []IndexEntry {
	idx.mu.RLock()
	entries := idx.partitions[tp]
	idx.mu.RUnlock()

	if len(entries) == 0 {
		return nil
	}

	startIdx := sort.Search(len(entries), func(i int) bool {
		return entries[i].LastOffset >= fetchOffset
	})

	if startIdx >= len(entries) {
		return nil
	}

	var result []IndexEntry
	var totalBytes int32
	for i := startIdx; i < len(entries); i++ {
		e := entries[i]
		batchSize := e.BatchSize
		// KIP-74: always include at least one batch
		if len(result) > 0 && totalBytes+batchSize > maxBytes {
			break
		}
		result = append(result, e)
		totalBytes += batchSize
	}

	return result
}

func (idx *Index) PruneSegment(segmentSeq uint64) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	for tp, entries := range idx.partitions {
		var kept []IndexEntry
		for _, e := range entries {
			if e.SegmentSeq != segmentSeq {
				kept = append(kept, e)
			}
		}
		if len(kept) == 0 {
			delete(idx.partitions, tp)
		} else {
			idx.partitions[tp] = kept
		}
	}
}

func (idx *Index) PartitionEntries(tp TopicPartition) []IndexEntry {
	idx.mu.RLock()
	entries := idx.partitions[tp]
	idx.mu.RUnlock()
	result := make([]IndexEntry, len(entries))
	copy(result, entries)
	return result
}

func (idx *Index) SegmentReferenced(segmentSeq uint64) bool {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	for _, entries := range idx.partitions {
		for _, e := range entries {
			if e.SegmentSeq == segmentSeq {
				return true
			}
		}
	}
	return false
}

func (idx *Index) UnflushedBytes(tp TopicPartition, s3Watermark int64) int64 {
	idx.mu.RLock()
	entries := idx.partitions[tp]
	idx.mu.RUnlock()

	var total int64
	for _, e := range entries {
		if e.LastOffset >= s3Watermark {
			total += int64(e.BatchSize)
		}
	}
	return total
}

// OldestUnflushedSequence returns the WAL sequence of the oldest unflushed entry.
// WAL sequence serves as a monotonic proxy for age (assigned at write time).
func (idx *Index) OldestUnflushedSequence(tp TopicPartition, s3Watermark int64) uint64 {
	idx.mu.RLock()
	entries := idx.partitions[tp]
	idx.mu.RUnlock()

	for _, e := range entries {
		if e.LastOffset >= s3Watermark {
			return e.WALSequence
		}
	}
	return 0
}

func (idx *Index) AllPartitions() []TopicPartition {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	result := make([]TopicPartition, 0, len(idx.partitions))
	for tp := range idx.partitions {
		result = append(result, tp)
	}
	return result
}

// MaxOffset returns the highest (LastOffset+1) across all entries for the
// given partition, i.e. the high watermark implied by the WAL index.
// Returns 0 if the partition has no entries.
func (idx *Index) MaxOffset(tp TopicPartition) int64 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	entries := idx.partitions[tp]
	if len(entries) == 0 {
		return 0
	}
	return entries[len(entries)-1].LastOffset + 1
}
