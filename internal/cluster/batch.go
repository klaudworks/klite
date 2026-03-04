package cluster

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

// ErrShortBatch is returned when the raw batch is shorter than the 61-byte header.
var ErrShortBatch = errors.New("batch too short: need at least 61 bytes")

// BatchMeta holds fields extracted from the 61-byte RecordBatch header.
// Extracted via direct byte reads — no kmsg.RecordBatch.ReadFrom().
type BatchMeta struct {
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

// ParseBatchHeader reads the 61-byte RecordBatch header from raw bytes.
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
func ParseBatchHeader(raw []byte) (BatchMeta, error) {
	if len(raw) < 61 {
		return BatchMeta{}, ErrShortBatch
	}
	return BatchMeta{
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

// DefaultMaxMessageBytes is the default value for the max.message.bytes topic config.
const DefaultMaxMessageBytes = 1048588

// crc32cTable is the CRC-32C (Castagnoli) table used for RecordBatch CRC.
var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// SetLogAppendTime overwrites timestamps and attributes in the raw batch bytes
// for LogAppendTime semantics. It sets BaseTimestamp and MaxTimestamp to the
// given value, sets the timestamp type bit in Attributes, and recalculates
// the CRC. The raw slice is mutated in-place. The returned BatchMeta reflects
// the updated values.
func SetLogAppendTime(raw []byte, nowMillis int64, meta *BatchMeta) {
	// Set BaseTimestamp (bytes 27-34)
	binary.BigEndian.PutUint64(raw[27:35], uint64(nowMillis))
	// Set MaxTimestamp (bytes 35-42)
	binary.BigEndian.PutUint64(raw[35:43], uint64(nowMillis))

	// Set timestamp type bit (bit 3 of Attributes, bytes 21-22)
	attrs := binary.BigEndian.Uint16(raw[21:23])
	attrs |= 0x0008 // bit 3 = LogAppendTime
	binary.BigEndian.PutUint16(raw[21:23], attrs)

	// Recalculate CRC over bytes 21+ and write to bytes 17-20
	newCRC := crc32.Checksum(raw[21:], crc32cTable)
	binary.BigEndian.PutUint32(raw[17:21], newCRC)

	// Update meta to reflect changes
	meta.BaseTimestamp = nowMillis
	meta.MaxTimestamp = nowMillis
	meta.Attributes = int16(attrs)
	meta.CRC = newCRC
}

// ValidateBatchCRC checks the CRC32C of a raw RecordBatch.
// CRC covers bytes 21 to end. Returns true if valid.
func ValidateBatchCRC(raw []byte, expectedCRC uint32) bool {
	if len(raw) < 22 {
		return false
	}
	actual := crc32.Checksum(raw[21:], crc32cTable)
	return actual == expectedCRC
}

// AssignOffset overwrites the BaseOffset (bytes 0-7) and
// PartitionLeaderEpoch (bytes 12-15) in the raw batch bytes.
// BaseOffset is set to the given value; PartitionLeaderEpoch is set to 0.
// Both fields are outside the CRC-covered region (CRC covers bytes 21+),
// so no CRC recalculation is needed.
func AssignOffset(raw []byte, baseOffset int64) {
	binary.BigEndian.PutUint64(raw[0:8], uint64(baseOffset))
	binary.BigEndian.PutUint32(raw[12:16], 0) // PartitionLeaderEpoch = 0
}
