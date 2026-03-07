// Package s3 implements the S3 offload pipeline for long-term storage.
// Per-partition S3 objects contain concatenated RecordBatch bytes with
// a batch index footer for efficient range reads.
package s3

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
)

// S3 object format:
//
//	[M bytes]     data section: concatenated raw RecordBatch bytes
//	[N × 32 B]   batch index: one entry per RecordBatch
//	[4 bytes]     entry count (uint32, big-endian)
//	[4 bytes]     magic number (0x4B4C4958 = "KLIX", big-endian)
//
// Batch index entry (32 bytes, big-endian):
//
//	[8 bytes]   baseOffset (int64)
//	[4 bytes]   bytePosition (uint32)
//	[4 bytes]   batchLength (uint32)
//	[4 bytes]   lastOffsetDelta (int32)
//	[8 bytes]   maxTimestamp (int64)
//	[4 bytes]   recordCount (int32)

const (
	// FooterMagic is the 4-byte magic number at the end of each S3 object.
	FooterMagic uint32 = 0x4B4C4958 // "KLIX"

	// FooterTrailerSize is the size of the entry count + magic at the end.
	FooterTrailerSize = 4 + 4 // uint32 entryCount + uint32 magic

	// IndexEntrySize is the size of one batch index entry.
	IndexEntrySize = 32 // 8 + 4 + 4 + 4 + 8 + 4

	// DefaultFooterReadSize is the speculative read size for the footer.
	// Covers up to 3272 batch entries.
	DefaultFooterReadSize = 64 * 1024

	// RecordBatchHeaderSize is the minimum size of a RecordBatch header.
	RecordBatchHeaderSize = 61
)

type BatchIndexEntry struct {
	BaseOffset      int64
	BytePosition    uint32
	BatchLength     uint32
	LastOffsetDelta int32
	MaxTimestamp    int64
	RecordCount     int32
}

func (e BatchIndexEntry) LastOffset() int64 {
	return e.BaseOffset + int64(e.LastOffsetDelta)
}

type Footer struct {
	Entries []BatchIndexEntry
}

// FindBatch returns the index of the first batch whose offset range contains
// the given offset. Returns len(Entries) if offset is past all batches.
func (f *Footer) FindBatch(offset int64) int {
	idx := sort.Search(len(f.Entries), func(i int) bool {
		return f.Entries[i].BaseOffset > offset
	})
	idx--

	if idx < 0 {
		return 0
	}

	e := &f.Entries[idx]
	if offset <= e.LastOffset() {
		return idx
	}

	if idx+1 < len(f.Entries) {
		return idx + 1
	}

	return len(f.Entries)
}

func (f *Footer) FirstOffset() int64 {
	if len(f.Entries) == 0 {
		return -1
	}
	return f.Entries[0].BaseOffset
}

func (f *Footer) LastOffset() int64 {
	if len(f.Entries) == 0 {
		return -1
	}
	last := &f.Entries[len(f.Entries)-1]
	return last.BaseOffset + int64(last.LastOffsetDelta)
}

func (f *Footer) MaxTimestamp() int64 {
	if len(f.Entries) == 0 {
		return -1
	}
	max := f.Entries[0].MaxTimestamp
	for i := 1; i < len(f.Entries); i++ {
		if f.Entries[i].MaxTimestamp > max {
			max = f.Entries[i].MaxTimestamp
		}
	}
	return max
}

func (f *Footer) DataSize() int64 {
	var total int64
	for i := range f.Entries {
		total += int64(f.Entries[i].BatchLength)
	}
	return total
}

func (f *Footer) TotalRecordCount() int64 {
	var total int64
	for i := range f.Entries {
		total += int64(f.Entries[i].RecordCount)
	}
	return total
}

func BuildObject(batches []BatchData) []byte {
	if len(batches) == 0 {
		return nil
	}

	var dataSize int
	for _, b := range batches {
		dataSize += len(b.RawBytes)
	}

	footerSize := len(batches)*IndexEntrySize + FooterTrailerSize
	total := dataSize + footerSize

	buf := make([]byte, total)

	var pos uint32
	entries := make([]BatchIndexEntry, len(batches))
	for i, b := range batches {
		copy(buf[pos:], b.RawBytes)
		var maxTs int64
		var numRec int32
		if len(b.RawBytes) >= RecordBatchHeaderSize {
			maxTs = int64(binary.BigEndian.Uint64(b.RawBytes[35:43]))
			numRec = int32(binary.BigEndian.Uint32(b.RawBytes[57:61]))
		}
		entries[i] = BatchIndexEntry{
			BaseOffset:      b.BaseOffset,
			BytePosition:    pos,
			BatchLength:     uint32(len(b.RawBytes)),
			LastOffsetDelta: b.LastOffsetDelta,
			MaxTimestamp:    maxTs,
			RecordCount:     numRec,
		}
		pos += uint32(len(b.RawBytes))
	}

	off := int(pos)
	for _, e := range entries {
		binary.BigEndian.PutUint64(buf[off:off+8], uint64(e.BaseOffset))
		binary.BigEndian.PutUint32(buf[off+8:off+12], e.BytePosition)
		binary.BigEndian.PutUint32(buf[off+12:off+16], e.BatchLength)
		binary.BigEndian.PutUint32(buf[off+16:off+20], uint32(e.LastOffsetDelta))
		binary.BigEndian.PutUint64(buf[off+20:off+28], uint64(e.MaxTimestamp))
		binary.BigEndian.PutUint32(buf[off+28:off+32], uint32(e.RecordCount))
		off += IndexEntrySize
	}

	binary.BigEndian.PutUint32(buf[off:off+4], uint32(len(entries)))
	binary.BigEndian.PutUint32(buf[off+4:off+8], FooterMagic)

	return buf
}

type BatchData struct {
	RawBytes        []byte
	BaseOffset      int64
	LastOffsetDelta int32
}

func ParseFooter(tailData []byte, objectSize int64) (*Footer, error) {
	if len(tailData) < FooterTrailerSize {
		return nil, fmt.Errorf("tail data too small: %d bytes", len(tailData))
	}

	tailLen := len(tailData)
	magic := binary.BigEndian.Uint32(tailData[tailLen-4 : tailLen])
	if magic != FooterMagic {
		return nil, fmt.Errorf("invalid footer magic: 0x%08X (expected 0x%08X)", magic, FooterMagic)
	}

	entryCount := binary.BigEndian.Uint32(tailData[tailLen-8 : tailLen-4])
	if entryCount == 0 {
		return &Footer{}, nil
	}

	footerSize := int(entryCount)*IndexEntrySize + FooterTrailerSize
	if footerSize > len(tailData) {
		return nil, fmt.Errorf("footer requires %d bytes but tail is only %d bytes (need second read)", footerSize, len(tailData))
	}

	entriesStart := tailLen - footerSize
	entries := make([]BatchIndexEntry, entryCount)
	for i := range entries {
		off := entriesStart + i*IndexEntrySize
		entries[i] = BatchIndexEntry{
			BaseOffset:      int64(binary.BigEndian.Uint64(tailData[off : off+8])),
			BytePosition:    binary.BigEndian.Uint32(tailData[off+8 : off+12]),
			BatchLength:     binary.BigEndian.Uint32(tailData[off+12 : off+16]),
			LastOffsetDelta: int32(binary.BigEndian.Uint32(tailData[off+16 : off+20])),
			MaxTimestamp:    int64(binary.BigEndian.Uint64(tailData[off+20 : off+28])),
			RecordCount:     int32(binary.BigEndian.Uint32(tailData[off+28 : off+32])),
		}
	}

	return &Footer{Entries: entries}, nil
}

// TopicDir returns the directory component for a topic, encoding both the
// name and UUID so that delete-then-recreate produces a distinct prefix.
// Format: "topicName-hexTopicID"
func TopicDir(topic string, topicID [16]byte) string {
	return topic + "-" + hex.EncodeToString(topicID[:])
}

// ParseTopicDir extracts the topic name and topic ID from a topic directory
// component of the format "topicName-hexTopicID". Returns ("", zero) if
// the format is invalid.
func ParseTopicDir(dir string) (string, [16]byte) {
	var zeroID [16]byte
	// The hex-encoded topic ID is always 32 chars at the end, preceded by "-"
	if len(dir) < 34 { // at least 1 char name + "-" + 32 hex chars
		return dir, zeroID // legacy format without topic ID
	}
	sep := len(dir) - 33 // position of the "-" before the 32-char hex ID
	if dir[sep] != '-' {
		return dir, zeroID
	}
	hexStr := dir[sep+1:]
	idBytes, err := hex.DecodeString(hexStr)
	if err != nil || len(idBytes) != 16 {
		return dir, zeroID
	}
	var id [16]byte
	copy(id[:], idBytes)
	return dir[:sep], id
}

func ObjectKeyPrefix(prefix, topic string, topicID [16]byte, partition int32) string {
	return fmt.Sprintf("%s/%s/%d/", prefix, TopicDir(topic, topicID), partition)
}

func ObjectKey(prefix, topic string, topicID [16]byte, partition int32, baseOffset int64) string {
	return fmt.Sprintf("%s/%s/%d/%020d.obj", prefix, TopicDir(topic, topicID), partition, baseOffset)
}

func ZeroPadOffset(offset int64) string {
	return fmt.Sprintf("%020d", offset)
}

func ParseBaseOffsetFromKey(key string) int64 {
	idx := strings.LastIndex(key, "/")
	if idx < 0 {
		return -1
	}
	name := key[idx+1:]
	if !strings.HasSuffix(name, ".obj") {
		return -1
	}
	name = strings.TrimSuffix(name, ".obj")
	var offset int64
	if _, err := fmt.Sscanf(name, "%d", &offset); err != nil {
		return -1
	}
	return offset
}
