package wal

import (
	"encoding/binary"
	"hash/crc32"
	"io"
)

// WAL entry format (on disk):
//
//	[4 bytes]  entry_length (uint32, big-endian, excludes these 4 bytes)
//	[4 bytes]  crc32c (over remaining bytes after CRC)
//	[8 bytes]  wal_sequence (uint64, monotonic across all entries)
//	[16 bytes] topic_id (UUID)
//	[4 bytes]  partition_index (int32)
//	[8 bytes]  base_offset (int64, assigned Kafka offset)
//	[M bytes]  record_batch (raw RecordBatch bytes)
//
// Fixed header overhead: 4 + 4 + 8 + 16 + 4 + 8 = 44 bytes (before record batch)

const (
	// entryFixedPayload is the fixed payload portion after CRC (seq + topicID + partition + baseOffset).
	entryFixedPayload = 8 + 16 + 4 + 8 // 36 bytes
)

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// Entry represents a single WAL entry for a partition's RecordBatch.
type Entry struct {
	Sequence  uint64   // WAL sequence number (monotonic)
	TopicID   [16]byte // Topic UUID
	Partition int32    // Partition index
	Offset    int64    // Assigned Kafka base offset
	Data      []byte   // Raw RecordBatch bytes (already offset-assigned)
}

// MarshalEntry serializes a WAL entry into the on-disk format.
// Returns the complete entry bytes including length prefix and CRC.
func MarshalEntry(e *Entry) []byte {
	payloadLen := entryFixedPayload + len(e.Data)
	total := 4 + 4 + payloadLen
	buf := make([]byte, total)

	// entry_length = everything after the length field
	binary.BigEndian.PutUint32(buf[0:4], uint32(4+payloadLen)) // crc + payload

	// Payload starts at offset 8 (after length + crc)
	off := 8
	binary.BigEndian.PutUint64(buf[off:off+8], e.Sequence)
	off += 8
	copy(buf[off:off+16], e.TopicID[:])
	off += 16
	binary.BigEndian.PutUint32(buf[off:off+4], uint32(e.Partition))
	off += 4
	binary.BigEndian.PutUint64(buf[off:off+8], uint64(e.Offset))
	off += 8
	copy(buf[off:], e.Data)

	// CRC covers everything after the CRC field (offset 8 to end)
	crcVal := crc32.Checksum(buf[8:], crc32cTable)
	binary.BigEndian.PutUint32(buf[4:8], crcVal)

	return buf
}

// UnmarshalEntry parses a WAL entry from the payload bytes (after length prefix).
// The payload should start at the CRC field.
func UnmarshalEntry(payload []byte) (Entry, error) {
	if len(payload) < 4+entryFixedPayload {
		return Entry{}, io.ErrUnexpectedEOF
	}

	// Validate CRC
	storedCRC := binary.BigEndian.Uint32(payload[0:4])
	actualCRC := crc32.Checksum(payload[4:], crc32cTable)
	if storedCRC != actualCRC {
		return Entry{}, ErrCRCMismatch
	}

	off := 4 // skip CRC
	seq := binary.BigEndian.Uint64(payload[off : off+8])
	off += 8

	var topicID [16]byte
	copy(topicID[:], payload[off:off+16])
	off += 16

	partition := int32(binary.BigEndian.Uint32(payload[off : off+4]))
	off += 4

	baseOffset := int64(binary.BigEndian.Uint64(payload[off : off+8]))
	off += 8

	data := make([]byte, len(payload)-off)
	copy(data, payload[off:])

	return Entry{
		Sequence:  seq,
		TopicID:   topicID,
		Partition: partition,
		Offset:    baseOffset,
		Data:      data,
	}, nil
}
