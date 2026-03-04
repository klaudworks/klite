package cluster

import (
	"encoding/binary"
	"testing"
)

// makeBatchBytes builds a minimal 61-byte RecordBatch header + optional padding.
// Fields are written in big-endian at their standard byte offsets.
func makeBatchBytes(baseOffset int64, batchLength int32, leaderEpoch int32, magic int8, crc uint32, attrs int16, lastOffsetDelta int32, baseTS, maxTS int64, producerID int64, producerEpoch int16, baseSeq int32, numRecords int32) []byte {
	raw := make([]byte, 61)
	binary.BigEndian.PutUint64(raw[0:8], uint64(baseOffset))
	binary.BigEndian.PutUint32(raw[8:12], uint32(batchLength))
	binary.BigEndian.PutUint32(raw[12:16], uint32(leaderEpoch))
	raw[16] = byte(magic)
	binary.BigEndian.PutUint32(raw[17:21], crc)
	binary.BigEndian.PutUint16(raw[21:23], uint16(attrs))
	binary.BigEndian.PutUint32(raw[23:27], uint32(lastOffsetDelta))
	binary.BigEndian.PutUint64(raw[27:35], uint64(baseTS))
	binary.BigEndian.PutUint64(raw[35:43], uint64(maxTS))
	binary.BigEndian.PutUint64(raw[43:51], uint64(producerID))
	binary.BigEndian.PutUint16(raw[51:53], uint16(producerEpoch))
	binary.BigEndian.PutUint32(raw[53:57], uint32(baseSeq))
	binary.BigEndian.PutUint32(raw[57:61], uint32(numRecords))
	return raw
}

// makeSimpleBatch creates a batch with sensible defaults for testing.
// baseOffset=0, magic=2, numRecords records with lastOffsetDelta=numRecords-1.
func makeSimpleBatch(numRecords int32, maxTimestamp int64) []byte {
	lastDelta := numRecords - 1
	if lastDelta < 0 {
		lastDelta = 0
	}
	return makeBatchBytes(
		0,          // baseOffset (client sends 0, broker overwrites)
		49,         // batchLength (61-12=49 for a minimal batch)
		0,          // leaderEpoch
		2,          // magic
		0xDEADBEEF, // crc (not validated)
		0,          // attributes (no compression)
		lastDelta,  // lastOffsetDelta
		1000,       // baseTimestamp
		maxTimestamp,
		-1, // producerID (no idempotency)
		-1, // producerEpoch
		-1, // baseSequence
		numRecords,
	)
}

func TestParseBatchHeader(t *testing.T) {
	t.Parallel()

	t.Run("valid header", func(t *testing.T) {
		t.Parallel()
		raw := makeBatchBytes(
			100,        // baseOffset
			49,         // batchLength
			5,          // leaderEpoch
			2,          // magic
			0xAABBCCDD, // crc
			4,          // attributes (zstd)
			9,          // lastOffsetDelta
			1000,       // baseTimestamp
			2000,       // maxTimestamp
			42,         // producerID
			3,          // producerEpoch
			7,          // baseSequence
			10,         // numRecords
		)

		meta, err := ParseBatchHeader(raw)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if meta.BatchLength != 49 {
			t.Errorf("BatchLength: got %d, want 49", meta.BatchLength)
		}
		if meta.Magic != 2 {
			t.Errorf("Magic: got %d, want 2", meta.Magic)
		}
		if meta.CRC != 0xAABBCCDD {
			t.Errorf("CRC: got %x, want 0xAABBCCDD", meta.CRC)
		}
		if meta.Attributes != 4 {
			t.Errorf("Attributes: got %d, want 4", meta.Attributes)
		}
		if meta.LastOffsetDelta != 9 {
			t.Errorf("LastOffsetDelta: got %d, want 9", meta.LastOffsetDelta)
		}
		if meta.BaseTimestamp != 1000 {
			t.Errorf("BaseTimestamp: got %d, want 1000", meta.BaseTimestamp)
		}
		if meta.MaxTimestamp != 2000 {
			t.Errorf("MaxTimestamp: got %d, want 2000", meta.MaxTimestamp)
		}
		if meta.ProducerID != 42 {
			t.Errorf("ProducerID: got %d, want 42", meta.ProducerID)
		}
		if meta.ProducerEpoch != 3 {
			t.Errorf("ProducerEpoch: got %d, want 3", meta.ProducerEpoch)
		}
		if meta.BaseSequence != 7 {
			t.Errorf("BaseSequence: got %d, want 7", meta.BaseSequence)
		}
		if meta.NumRecords != 10 {
			t.Errorf("NumRecords: got %d, want 10", meta.NumRecords)
		}
	})

	t.Run("too short", func(t *testing.T) {
		t.Parallel()
		raw := make([]byte, 60) // 1 byte short
		_, err := ParseBatchHeader(raw)
		if err != ErrShortBatch {
			t.Errorf("expected ErrShortBatch, got %v", err)
		}
	})

	t.Run("empty slice", func(t *testing.T) {
		t.Parallel()
		_, err := ParseBatchHeader(nil)
		if err != ErrShortBatch {
			t.Errorf("expected ErrShortBatch, got %v", err)
		}
	})

	t.Run("negative values", func(t *testing.T) {
		t.Parallel()
		raw := makeBatchBytes(
			-1, -1, -1, 2, 0, -1, -1, -1, -1, -1, -1, -1, -1,
		)
		meta, err := ParseBatchHeader(raw)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if meta.ProducerID != -1 {
			t.Errorf("ProducerID: got %d, want -1", meta.ProducerID)
		}
		if meta.ProducerEpoch != -1 {
			t.Errorf("ProducerEpoch: got %d, want -1", meta.ProducerEpoch)
		}
		if meta.BaseSequence != -1 {
			t.Errorf("BaseSequence: got %d, want -1", meta.BaseSequence)
		}
	})

	t.Run("longer than 61 bytes is fine", func(t *testing.T) {
		t.Parallel()
		raw := make([]byte, 200)
		raw[16] = 2 // magic
		binary.BigEndian.PutUint32(raw[57:61], 5)
		meta, err := ParseBatchHeader(raw)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if meta.NumRecords != 5 {
			t.Errorf("NumRecords: got %d, want 5", meta.NumRecords)
		}
	})
}

func TestAssignOffset(t *testing.T) {
	t.Parallel()

	raw := makeBatchBytes(0, 49, 99, 2, 0, 0, 0, 0, 0, 0, 0, 0, 1)

	AssignOffset(raw, 42)

	gotBase := int64(binary.BigEndian.Uint64(raw[0:8]))
	if gotBase != 42 {
		t.Errorf("BaseOffset: got %d, want 42", gotBase)
	}

	gotEpoch := int32(binary.BigEndian.Uint32(raw[12:16]))
	if gotEpoch != 0 {
		t.Errorf("PartitionLeaderEpoch: got %d, want 0", gotEpoch)
	}

	// Verify CRC and other fields are untouched
	gotMagic := int8(raw[16])
	if gotMagic != 2 {
		t.Errorf("Magic: got %d, want 2", gotMagic)
	}
}
