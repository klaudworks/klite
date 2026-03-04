package cluster

import (
	"encoding/binary"
	"errors"
)

// errShortBatch is returned when the raw batch is shorter than the 61-byte header.
var errShortBatch = errors.New("batch too short: need at least 61 bytes")

// batchMeta holds fields extracted from the 61-byte RecordBatch header.
// Extracted via direct byte reads — no kmsg.RecordBatch.ReadFrom().
type batchMeta struct {
	BatchLength     int32
	Magic           int8
	CRC             uint32
	Attributes      int16
	LastOffsetDelta int32
	BaseTimestamp   int64
	MaxTimestamp    int64
	ProducerID      int64
	ProducerEpoch   int16
	BaseSequence    int32
	NumRecords      int32
}

// parseBatchHeader reads the 61-byte RecordBatch header from raw bytes.
// Returns an error if raw is shorter than 61 bytes.
// Does NOT validate CRC, Magic, or any field values — caller does that.
//
// RecordBatch header layout (big-endian, 61 bytes):
//
//	Offset  Size  Field
//	0       8     BaseOffset (overwritten by broker)
//	8       4     BatchLength
//	12      4     PartitionLeaderEpoch (overwritten by broker)
//	16      1     Magic (must be 2)
//	17      4     CRC32C (covers bytes 21+)
//	21      2     Attributes
//	23      4     LastOffsetDelta
//	27      8     BaseTimestamp
//	35      8     MaxTimestamp
//	43      8     ProducerID
//	51      2     ProducerEpoch
//	53      4     BaseSequence
//	57      4     NumRecords
//	61+     var   Records (possibly compressed)
func parseBatchHeader(raw []byte) (batchMeta, error) {
	if len(raw) < 61 {
		return batchMeta{}, errShortBatch
	}
	return batchMeta{
		BatchLength:     int32(binary.BigEndian.Uint32(raw[8:12])),
		Magic:           int8(raw[16]),
		CRC:             binary.BigEndian.Uint32(raw[17:21]),
		Attributes:      int16(binary.BigEndian.Uint16(raw[21:23])),
		LastOffsetDelta: int32(binary.BigEndian.Uint32(raw[23:27])),
		BaseTimestamp:   int64(binary.BigEndian.Uint64(raw[27:35])),
		MaxTimestamp:    int64(binary.BigEndian.Uint64(raw[35:43])),
		ProducerID:      int64(binary.BigEndian.Uint64(raw[43:51])),
		ProducerEpoch:   int16(binary.BigEndian.Uint16(raw[51:53])),
		BaseSequence:    int32(binary.BigEndian.Uint32(raw[53:57])),
		NumRecords:      int32(binary.BigEndian.Uint32(raw[57:61])),
	}, nil
}

// assignOffset overwrites the BaseOffset (bytes 0-7) and
// PartitionLeaderEpoch (bytes 12-15) in the raw batch bytes.
// BaseOffset is set to the given value; PartitionLeaderEpoch is set to 0.
// Both fields are outside the CRC-covered region (CRC covers bytes 21+),
// so no CRC recalculation is needed.
func assignOffset(raw []byte, baseOffset int64) {
	binary.BigEndian.PutUint64(raw[0:8], uint64(baseOffset))
	binary.BigEndian.PutUint32(raw[12:16], 0) // PartitionLeaderEpoch = 0
}
