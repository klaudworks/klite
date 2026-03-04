// Package s3 implements the S3 offload pipeline for long-term storage.
// Per-partition S3 objects contain concatenated RecordBatch bytes with
// a batch index footer for efficient range reads.
package s3

import (
	"encoding/binary"
	"fmt"
	"sort"
)

// S3 object format:
//
//	[M bytes]     data section: concatenated raw RecordBatch bytes
//	[N × 20 B]   batch index: one entry per RecordBatch
//	[4 bytes]     entry count (uint32, big-endian)
//	[4 bytes]     magic number (0x4B4C4958 = "KLIX", big-endian)
//
// Batch index entry (20 bytes, big-endian):
//
//	[8 bytes]   baseOffset (int64)
//	[4 bytes]   bytePosition (uint32)
//	[4 bytes]   batchLength (uint32)
//	[4 bytes]   lastOffsetDelta (int32)

const (
	// FooterMagic is the 4-byte magic number at the end of each S3 object.
	FooterMagic uint32 = 0x4B4C4958 // "KLIX"

	// FooterTrailerSize is the size of the entry count + magic at the end.
	FooterTrailerSize = 4 + 4 // uint32 entryCount + uint32 magic

	// IndexEntrySize is the size of one batch index entry.
	IndexEntrySize = 20 // 8 + 4 + 4 + 4

	// DefaultFooterReadSize is the speculative read size for the footer.
	// Covers up to 3272 batch entries.
	DefaultFooterReadSize = 64 * 1024

	// RecordBatchHeaderSize is the minimum size of a RecordBatch header.
	RecordBatchHeaderSize = 61
)

// BatchIndexEntry represents one entry in the S3 object footer.
type BatchIndexEntry struct {
	BaseOffset      int64
	BytePosition    uint32
	BatchLength     uint32
	LastOffsetDelta int32
}

// LastOffset returns the last Kafka offset in this batch.
func (e BatchIndexEntry) LastOffset() int64 {
	return e.BaseOffset + int64(e.LastOffsetDelta)
}

// Footer represents the parsed batch index footer of an S3 object.
type Footer struct {
	Entries []BatchIndexEntry
}

// FindBatch returns the index of the first batch whose offset range
// contains the given offset. Uses binary search on BaseOffset.
// Returns -1 if the offset is before all batches.
func (f *Footer) FindBatch(offset int64) int {
	// Find the last batch whose BaseOffset <= offset
	idx := sort.Search(len(f.Entries), func(i int) bool {
		return f.Entries[i].BaseOffset > offset
	})
	idx-- // step back to the batch whose BaseOffset <= offset

	if idx < 0 {
		return 0 // offset before all batches, start from first
	}

	// Verify offset is within this batch's range
	e := &f.Entries[idx]
	if offset <= e.LastOffset() {
		return idx
	}

	// Offset is between batches (gap) — return next batch
	if idx+1 < len(f.Entries) {
		return idx + 1
	}
	return idx
}

// FirstOffset returns the first offset in the footer, or -1 if empty.
func (f *Footer) FirstOffset() int64 {
	if len(f.Entries) == 0 {
		return -1
	}
	return f.Entries[0].BaseOffset
}

// LastOffset returns the last offset in the footer, or -1 if empty.
func (f *Footer) LastOffset() int64 {
	if len(f.Entries) == 0 {
		return -1
	}
	last := &f.Entries[len(f.Entries)-1]
	return last.BaseOffset + int64(last.LastOffsetDelta)
}

// BuildObject assembles an S3 object from raw RecordBatch bytes.
// Returns the complete object bytes (data + footer).
//
// Each batch in the data slice must have a valid 61-byte RecordBatch header.
// The batchData slice contains (rawBytes, baseOffset, lastOffsetDelta) triples.
func BuildObject(batches []BatchData) []byte {
	if len(batches) == 0 {
		return nil
	}

	// Calculate total data size
	var dataSize int
	for _, b := range batches {
		dataSize += len(b.RawBytes)
	}

	// Footer size
	footerSize := len(batches)*IndexEntrySize + FooterTrailerSize
	total := dataSize + footerSize

	buf := make([]byte, total)

	// Write data section
	var pos uint32
	entries := make([]BatchIndexEntry, len(batches))
	for i, b := range batches {
		copy(buf[pos:], b.RawBytes)
		entries[i] = BatchIndexEntry{
			BaseOffset:      b.BaseOffset,
			BytePosition:    pos,
			BatchLength:     uint32(len(b.RawBytes)),
			LastOffsetDelta: b.LastOffsetDelta,
		}
		pos += uint32(len(b.RawBytes))
	}

	// Write batch index entries
	off := int(pos)
	for _, e := range entries {
		binary.BigEndian.PutUint64(buf[off:off+8], uint64(e.BaseOffset))
		binary.BigEndian.PutUint32(buf[off+8:off+12], e.BytePosition)
		binary.BigEndian.PutUint32(buf[off+12:off+16], e.BatchLength)
		binary.BigEndian.PutUint32(buf[off+16:off+20], uint32(e.LastOffsetDelta))
		off += IndexEntrySize
	}

	// Write entry count and magic
	binary.BigEndian.PutUint32(buf[off:off+4], uint32(len(entries)))
	binary.BigEndian.PutUint32(buf[off+4:off+8], FooterMagic)

	return buf
}

// BatchData holds the data needed to build one batch in an S3 object.
type BatchData struct {
	RawBytes        []byte
	BaseOffset      int64
	LastOffsetDelta int32
}

// ParseFooter parses the batch index footer from the tail of an S3 object.
// tailData should be the last N bytes of the object.
// objectSize is the total size of the object.
func ParseFooter(tailData []byte, objectSize int64) (*Footer, error) {
	if len(tailData) < FooterTrailerSize {
		return nil, fmt.Errorf("tail data too small: %d bytes", len(tailData))
	}

	// Read magic and entry count from the very end
	tailLen := len(tailData)
	magic := binary.BigEndian.Uint32(tailData[tailLen-4 : tailLen])
	if magic != FooterMagic {
		return nil, fmt.Errorf("invalid footer magic: 0x%08X (expected 0x%08X)", magic, FooterMagic)
	}

	entryCount := binary.BigEndian.Uint32(tailData[tailLen-8 : tailLen-4])
	if entryCount == 0 {
		return &Footer{}, nil
	}

	// Calculate required footer size
	footerSize := int(entryCount)*IndexEntrySize + FooterTrailerSize
	if footerSize > len(tailData) {
		return nil, fmt.Errorf("footer requires %d bytes but tail is only %d bytes (need second read)", footerSize, len(tailData))
	}

	// Parse entries from the tail
	entriesStart := tailLen - footerSize
	entries := make([]BatchIndexEntry, entryCount)
	for i := range entries {
		off := entriesStart + i*IndexEntrySize
		entries[i] = BatchIndexEntry{
			BaseOffset:      int64(binary.BigEndian.Uint64(tailData[off : off+8])),
			BytePosition:    binary.BigEndian.Uint32(tailData[off+8 : off+12]),
			BatchLength:     binary.BigEndian.Uint32(tailData[off+12 : off+16]),
			LastOffsetDelta: int32(binary.BigEndian.Uint32(tailData[off+16 : off+20])),
		}
	}

	return &Footer{Entries: entries}, nil
}

// ObjectKeyPrefix returns the S3 key prefix for a topic/partition.
func ObjectKeyPrefix(prefix, topic string, partition int32) string {
	return fmt.Sprintf("%s/%s/%d/", prefix, topic, partition)
}

// ObjectKey returns the full S3 key for a partition object at the given base offset.
func ObjectKey(prefix, topic string, partition int32, baseOffset int64) string {
	return fmt.Sprintf("%s/%s/%d/%020d.obj", prefix, topic, partition, baseOffset)
}

// ZeroPadOffset returns a 20-digit zero-padded string for an offset.
func ZeroPadOffset(offset int64) string {
	return fmt.Sprintf("%020d", offset)
}
