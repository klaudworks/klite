package wal

import (
	"sort"
	"sync"
)

// TopicPartition is a key for the WAL index.
type TopicPartition struct {
	TopicID   [16]byte
	Partition int32
}

// IndexEntry maps a batch's offset range to its location in a WAL segment.
type IndexEntry struct {
	BaseOffset  int64  // First Kafka offset in the batch
	LastOffset  int64  // BaseOffset + LastOffsetDelta
	SegmentSeq  uint64 // Segment file's starting sequence number
	FileOffset  int64  // Byte position within segment file
	EntrySize   int32  // Total entry size on disk (for direct read)
	BatchSize   int32  // Size of the RecordBatch payload
	WALSequence uint64 // WAL sequence number of this entry
}

// Index is an in-memory index mapping (topic, partition, offset) to WAL positions.
// Thread-safe via RWMutex.
type Index struct {
	mu         sync.RWMutex
	partitions map[TopicPartition][]IndexEntry
}

// NewIndex creates a new empty WAL index.
func NewIndex() *Index {
	return &Index{
		partitions: make(map[TopicPartition][]IndexEntry),
	}
}

// Add inserts a new index entry. Entries must be added in offset order per partition.
func (idx *Index) Add(tp TopicPartition, entry IndexEntry) {
	idx.mu.Lock()
	idx.partitions[tp] = append(idx.partitions[tp], entry)
	idx.mu.Unlock()
}

// Lookup finds index entries for the given partition starting at fetchOffset,
// collecting up to maxBytes of batch data.
func (idx *Index) Lookup(tp TopicPartition, fetchOffset int64, maxBytes int32) []IndexEntry {
	idx.mu.RLock()
	entries := idx.partitions[tp]
	idx.mu.RUnlock()

	if len(entries) == 0 {
		return nil
	}

	// Binary search for the first entry whose LastOffset >= fetchOffset
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

// PruneBefore removes all index entries for a partition whose LastOffset < newLogStart.
func (idx *Index) PruneBefore(tp TopicPartition, newLogStart int64) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	entries := idx.partitions[tp]
	if len(entries) == 0 {
		return
	}

	cutoff := sort.Search(len(entries), func(i int) bool {
		return entries[i].LastOffset >= newLogStart
	})

	if cutoff > 0 {
		idx.partitions[tp] = entries[cutoff:]
	}
}

// PruneSegment removes all index entries referencing the given segment.
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

// PartitionEntries returns a copy of all index entries for a partition.
// Used during segment cleanup to check if entries still reference a segment.
func (idx *Index) PartitionEntries(tp TopicPartition) []IndexEntry {
	idx.mu.RLock()
	entries := idx.partitions[tp]
	idx.mu.RUnlock()
	result := make([]IndexEntry, len(entries))
	copy(result, entries)
	return result
}

// SegmentReferenced returns true if any index entry references the given segment.
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

// AllPartitions returns all tracked topic-partitions.
func (idx *Index) AllPartitions() []TopicPartition {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	result := make([]TopicPartition, 0, len(idx.partitions))
	for tp := range idx.partitions {
		result = append(result, tp)
	}
	return result
}
