package s3

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildObjectParseFooterRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		batches []BatchData
	}{
		{
			name: "single batch",
			batches: []BatchData{
				{RawBytes: makeMinimalBatch(0, 4), BaseOffset: 0, LastOffsetDelta: 4},
			},
		},
		{
			name: "multiple batches",
			batches: []BatchData{
				{RawBytes: makeMinimalBatch(0, 9), BaseOffset: 0, LastOffsetDelta: 9},
				{RawBytes: makeMinimalBatch(10, 4), BaseOffset: 10, LastOffsetDelta: 4},
				{RawBytes: makeMinimalBatch(15, 2), BaseOffset: 15, LastOffsetDelta: 2},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			obj := BuildObject(tc.batches)
			require.NotNil(t, obj)

			footer, err := ParseFooter(obj, int64(len(obj)))
			require.NoError(t, err)
			require.Len(t, footer.Entries, len(tc.batches))

			var expectedPos uint32
			for i, b := range tc.batches {
				e := footer.Entries[i]
				assert.Equal(t, b.BaseOffset, e.BaseOffset, "batch %d: BaseOffset", i)
				assert.Equal(t, expectedPos, e.BytePosition, "batch %d: BytePosition", i)
				assert.Equal(t, uint32(len(b.RawBytes)), e.BatchLength, "batch %d: BatchLength", i)
				assert.Equal(t, b.LastOffsetDelta, e.LastOffsetDelta, "batch %d: LastOffsetDelta", i)
				expectedPos += uint32(len(b.RawBytes))
			}
		})
	}
}

func TestBuildObjectNil(t *testing.T) {
	t.Parallel()
	assert.Nil(t, BuildObject(nil))
	assert.Nil(t, BuildObject([]BatchData{}))
}

func TestParseFooterMagicValidation(t *testing.T) {
	t.Parallel()

	// Construct a minimal valid footer then corrupt the magic.
	obj := BuildObject([]BatchData{
		{RawBytes: makeMinimalBatch(0, 0), BaseOffset: 0, LastOffsetDelta: 0},
	})
	require.NotNil(t, obj)

	// Corrupt the last 4 bytes (magic).
	copy(obj[len(obj)-4:], []byte{0x00, 0x00, 0x00, 0x00})

	_, err := ParseFooter(obj, int64(len(obj)))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid footer magic")
}

func TestParseFooterTooSmall(t *testing.T) {
	t.Parallel()

	_, err := ParseFooter([]byte{0x01, 0x02}, int64(2))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "too small")
}

func TestParseFooterZeroEntries(t *testing.T) {
	t.Parallel()

	// 8 bytes: 4-byte entry count (0) + 4-byte magic.
	buf := make([]byte, FooterTrailerSize)
	binary.BigEndian.PutUint32(buf[0:4], 0)
	binary.BigEndian.PutUint32(buf[4:8], FooterMagic)

	footer, err := ParseFooter(buf, int64(len(buf)))
	require.NoError(t, err)
	assert.Empty(t, footer.Entries)
}

func TestParseFooterInsufficientIndexData(t *testing.T) {
	t.Parallel()

	// Claim 1 entry but only provide the trailer (8 bytes).
	buf := make([]byte, FooterTrailerSize)
	binary.BigEndian.PutUint32(buf[0:4], 1)          // entryCount = 1
	binary.BigEndian.PutUint32(buf[4:8], FooterMagic) // magic

	_, err := ParseFooter(buf, int64(len(buf)))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "need second read")
}

func TestFooterFindBatch(t *testing.T) {
	t.Parallel()

	footer := &Footer{
		Entries: []BatchIndexEntry{
			{BaseOffset: 0, LastOffsetDelta: 9},   // offsets 0-9
			{BaseOffset: 10, LastOffsetDelta: 4},  // offsets 10-14
			{BaseOffset: 15, LastOffsetDelta: 2},  // offsets 15-17
		},
	}

	tests := []struct {
		name     string
		offset   int64
		expected int
	}{
		{"before all batches", -1, 0},
		{"first offset of first batch", 0, 0},
		{"middle of first batch", 5, 0},
		{"last offset of first batch", 9, 0},
		{"first offset of second batch", 10, 1},
		{"middle of second batch", 12, 1},
		{"last offset of second batch", 14, 1},
		{"first offset of third batch", 15, 2},
		{"last offset of third batch", 17, 2},
		{"past all batches", 20, 3},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.expected, footer.FindBatch(tc.offset))
		})
	}
}

func TestFooterFindBatchEmpty(t *testing.T) {
	t.Parallel()
	footer := &Footer{}
	assert.Equal(t, 0, footer.FindBatch(0))
}

func TestFooterFirstOffset(t *testing.T) {
	t.Parallel()

	assert.Equal(t, int64(-1), (&Footer{}).FirstOffset())
	assert.Equal(t, int64(5), (&Footer{
		Entries: []BatchIndexEntry{{BaseOffset: 5}},
	}).FirstOffset())
}

func TestFooterLastOffset(t *testing.T) {
	t.Parallel()

	assert.Equal(t, int64(-1), (&Footer{}).LastOffset())
	assert.Equal(t, int64(14), (&Footer{
		Entries: []BatchIndexEntry{
			{BaseOffset: 0, LastOffsetDelta: 9},
			{BaseOffset: 10, LastOffsetDelta: 4},
		},
	}).LastOffset())
}

func TestFooterMaxTimestamp(t *testing.T) {
	t.Parallel()

	assert.Equal(t, int64(-1), (&Footer{}).MaxTimestamp())
	assert.Equal(t, int64(3000), (&Footer{
		Entries: []BatchIndexEntry{
			{MaxTimestamp: 1000},
			{MaxTimestamp: 3000},
			{MaxTimestamp: 2000},
		},
	}).MaxTimestamp())
}

func TestFooterDataSize(t *testing.T) {
	t.Parallel()

	assert.Equal(t, int64(0), (&Footer{}).DataSize())
	assert.Equal(t, int64(300), (&Footer{
		Entries: []BatchIndexEntry{
			{BatchLength: 100},
			{BatchLength: 200},
		},
	}).DataSize())
}

func TestFooterTotalRecordCount(t *testing.T) {
	t.Parallel()

	assert.Equal(t, int64(0), (&Footer{}).TotalRecordCount())
	assert.Equal(t, int64(15), (&Footer{
		Entries: []BatchIndexEntry{
			{RecordCount: 5},
			{RecordCount: 10},
		},
	}).TotalRecordCount())
}

func TestBatchIndexEntryLastOffset(t *testing.T) {
	t.Parallel()

	e := BatchIndexEntry{BaseOffset: 10, LastOffsetDelta: 4}
	assert.Equal(t, int64(14), e.LastOffset())
}

func TestTopicDirRoundTrip(t *testing.T) {
	t.Parallel()

	id := [16]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10}
	dir := TopicDir("my-topic", id)
	name, parsedID := ParseTopicDir(dir)
	assert.Equal(t, "my-topic", name)
	assert.Equal(t, id, parsedID)
}

func TestParseTopicDirLegacy(t *testing.T) {
	t.Parallel()

	// Short string without hex ID should return the full string as name.
	name, id := ParseTopicDir("short")
	assert.Equal(t, "short", name)
	assert.Equal(t, [16]byte{}, id)
}

func TestParseTopicDirInvalidHex(t *testing.T) {
	t.Parallel()

	// 33 chars with dash at correct position but invalid hex.
	name, id := ParseTopicDir("t-zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz")
	assert.Equal(t, "t-zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz", name)
	assert.Equal(t, [16]byte{}, id)
}

func TestTopicDirHyphenatedName(t *testing.T) {
	t.Parallel()

	id := [16]byte{0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x00, 0x11,
		0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99}
	dir := TopicDir("my-cool-topic", id)
	name, parsedID := ParseTopicDir(dir)
	assert.Equal(t, "my-cool-topic", name)
	assert.Equal(t, id, parsedID)
}

func TestObjectKeyAndPrefix(t *testing.T) {
	t.Parallel()

	id := [16]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10}

	prefix := ObjectKeyPrefix("pfx", "mytopic", id, 3)
	assert.Equal(t, "pfx/mytopic-0102030405060708090a0b0c0d0e0f10/3/", prefix)

	key := ObjectKey("pfx", "mytopic", id, 3, 42)
	assert.Equal(t, "pfx/mytopic-0102030405060708090a0b0c0d0e0f10/3/00000000000000000042.obj", key)
}

func TestZeroPadOffset(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "00000000000000000000", ZeroPadOffset(0))
	assert.Equal(t, "00000000000000000042", ZeroPadOffset(42))
	assert.Equal(t, "00000000000123456789", ZeroPadOffset(123456789))
}

func TestParseBaseOffsetFromKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		key      string
		expected int64
	}{
		{"valid key", "pfx/topic-id/0/00000000000000000042.obj", 42},
		{"zero offset", "pfx/topic-id/0/00000000000000000000.obj", 0},
		{"no slash", "00000000000000000042.obj", -1},
		{"no .obj suffix", "pfx/topic/0/00000000000000000042.dat", -1},
		{"empty", "", -1},
		{"just slash", "/", -1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.expected, ParseBaseOffsetFromKey(tc.key))
		})
	}
}

func TestBuildObjectFooterLayout(t *testing.T) {
	t.Parallel()

	// Verify the exact byte layout: data || index entries || entry count || magic.
	batch := makeMinimalBatch(100, 3)
	obj := BuildObject([]BatchData{
		{RawBytes: batch, BaseOffset: 100, LastOffsetDelta: 3},
	})

	dataLen := len(batch)
	expectedTotal := dataLen + IndexEntrySize + FooterTrailerSize
	require.Len(t, obj, expectedTotal)

	// Data section: first dataLen bytes should be the raw batch.
	assert.Equal(t, batch, obj[:dataLen])

	// Trailer at the end: entry count and magic.
	trailerStart := len(obj) - FooterTrailerSize
	entryCount := binary.BigEndian.Uint32(obj[trailerStart : trailerStart+4])
	magic := binary.BigEndian.Uint32(obj[trailerStart+4 : trailerStart+8])
	assert.Equal(t, uint32(1), entryCount)
	assert.Equal(t, FooterMagic, magic)

	// Index entry: 32 bytes before the trailer.
	entryStart := trailerStart - IndexEntrySize
	baseOffset := int64(binary.BigEndian.Uint64(obj[entryStart : entryStart+8]))
	bytePos := binary.BigEndian.Uint32(obj[entryStart+8 : entryStart+12])
	batchLen := binary.BigEndian.Uint32(obj[entryStart+12 : entryStart+16])
	lastOD := int32(binary.BigEndian.Uint32(obj[entryStart+16 : entryStart+20]))

	assert.Equal(t, int64(100), baseOffset)
	assert.Equal(t, uint32(0), bytePos)
	assert.Equal(t, uint32(len(batch)), batchLen)
	assert.Equal(t, int32(3), lastOD)
}

func TestBuildObjectTimestampAndRecordCount(t *testing.T) {
	t.Parallel()

	// makeMinimalBatch sets maxTimestamp at bytes 35-42 and numRecords at bytes 57-60.
	// Verify BuildObject extracts them correctly into the index entry.
	batch := makeMinimalBatch(0, 4)

	// Set a specific maxTimestamp in the batch header (bytes 35-42).
	binary.BigEndian.PutUint64(batch[35:43], uint64(1234567890))

	obj := BuildObject([]BatchData{
		{RawBytes: batch, BaseOffset: 0, LastOffsetDelta: 4},
	})

	footer, err := ParseFooter(obj, int64(len(obj)))
	require.NoError(t, err)
	require.Len(t, footer.Entries, 1)

	assert.Equal(t, int64(1234567890), footer.Entries[0].MaxTimestamp)
	assert.Equal(t, int32(5), footer.Entries[0].RecordCount) // lastOffsetDelta=4, so numRecords=5
}

func TestBuildObjectShortBatch(t *testing.T) {
	t.Parallel()

	// A batch shorter than RecordBatchHeaderSize should produce zero
	// maxTimestamp and recordCount (no panic).
	shortBatch := make([]byte, 20)
	obj := BuildObject([]BatchData{
		{RawBytes: shortBatch, BaseOffset: 0, LastOffsetDelta: 0},
	})

	footer, err := ParseFooter(obj, int64(len(obj)))
	require.NoError(t, err)
	require.Len(t, footer.Entries, 1)
	assert.Equal(t, int64(0), footer.Entries[0].MaxTimestamp)
	assert.Equal(t, int32(0), footer.Entries[0].RecordCount)
}
