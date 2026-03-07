package metadata

import (
	"encoding/binary"
	"fmt"
)

// Entry types for metadata.log.
const (
	EntryCreateTopic           byte = 0x01
	EntryDeleteTopic           byte = 0x02
	EntryAlterConfig           byte = 0x03
	EntryOffsetCommit          byte = 0x04
	EntryProducerID            byte = 0x05
	EntryLogStartOffset        byte = 0x06
	EntryScramCredential       byte = 0x07
	EntryScramCredentialDelete byte = 0x08
	EntryCompactionWatermark   byte = 0x09
)

type CreateTopicEntry struct {
	TopicName      string
	PartitionCount int32
	TopicID        [16]byte
	Configs        map[string]string
}

type DeleteTopicEntry struct {
	TopicName string
	TopicID   [16]byte
}

type AlterConfigEntry struct {
	TopicName string
	Key       string
	Value     string
}

type OffsetCommitEntry struct {
	Group     string
	Topic     string
	Partition int32
	Offset    int64
	Metadata  string
}

type ProducerIDEntry struct {
	NextProducerID int64
}

type LogStartOffsetEntry struct {
	TopicName      string
	Partition      int32
	LogStartOffset int64
}

// Serialization format:
//   Each entry: [1B type][type-specific payload]
//   Strings: [2B uint16 len][bytes]
//   Config maps: [2B uint16 count] then (key, value) string pairs.

func putString(buf []byte, off int, s string) int {
	binary.BigEndian.PutUint16(buf[off:off+2], uint16(len(s)))
	off += 2
	copy(buf[off:off+len(s)], s)
	return off + len(s)
}

func getString(buf []byte, off int) (string, int, error) {
	if off+2 > len(buf) {
		return "", off, fmt.Errorf("short read for string length at offset %d", off)
	}
	n := int(binary.BigEndian.Uint16(buf[off : off+2]))
	off += 2
	if off+n > len(buf) {
		return "", off, fmt.Errorf("short read for string data at offset %d, need %d bytes", off, n)
	}
	s := string(buf[off : off+n])
	return s, off + n, nil
}

func MarshalCreateTopic(e *CreateTopicEntry) []byte {
	size := 1 + 2 + len(e.TopicName) + 4 + 16 + 2
	for k, v := range e.Configs {
		size += 2 + len(k) + 2 + len(v)
	}

	buf := make([]byte, size)
	buf[0] = EntryCreateTopic
	off := 1
	off = putString(buf, off, e.TopicName)
	binary.BigEndian.PutUint32(buf[off:off+4], uint32(e.PartitionCount))
	off += 4
	copy(buf[off:off+16], e.TopicID[:])
	off += 16
	binary.BigEndian.PutUint16(buf[off:off+2], uint16(len(e.Configs)))
	off += 2
	for k, v := range e.Configs {
		off = putString(buf, off, k)
		off = putString(buf, off, v)
	}
	return buf
}

func UnmarshalCreateTopic(payload []byte) (CreateTopicEntry, error) {
	var e CreateTopicEntry
	off := 0
	var err error

	e.TopicName, off, err = getString(payload, off)
	if err != nil {
		return e, err
	}

	if off+4 > len(payload) {
		return e, fmt.Errorf("short read for partition count")
	}
	e.PartitionCount = int32(binary.BigEndian.Uint32(payload[off : off+4]))
	off += 4

	if off+16 > len(payload) {
		return e, fmt.Errorf("short read for topic ID")
	}
	copy(e.TopicID[:], payload[off:off+16])
	off += 16

	if off+2 > len(payload) {
		return e, fmt.Errorf("short read for config count")
	}
	configCount := int(binary.BigEndian.Uint16(payload[off : off+2]))
	off += 2

	e.Configs = make(map[string]string, configCount)
	for i := 0; i < configCount; i++ {
		var k, v string
		k, off, err = getString(payload, off)
		if err != nil {
			return e, err
		}
		v, off, err = getString(payload, off)
		if err != nil {
			return e, err
		}
		e.Configs[k] = v
	}

	return e, nil
}

func MarshalDeleteTopic(e *DeleteTopicEntry) []byte {
	size := 1 + 2 + len(e.TopicName) + 16
	buf := make([]byte, size)
	buf[0] = EntryDeleteTopic
	off := putString(buf, 1, e.TopicName)
	copy(buf[off:off+16], e.TopicID[:])
	return buf
}

func UnmarshalDeleteTopic(payload []byte) (DeleteTopicEntry, error) {
	var e DeleteTopicEntry
	var err error
	var off int
	e.TopicName, off, err = getString(payload, 0)
	if err != nil {
		return e, err
	}
	if off+16 <= len(payload) {
		copy(e.TopicID[:], payload[off:off+16])
	}
	return e, nil
}

func MarshalAlterConfig(e *AlterConfigEntry) []byte {
	size := 1 + 2 + len(e.TopicName) + 2 + len(e.Key) + 2 + len(e.Value)
	buf := make([]byte, size)
	buf[0] = EntryAlterConfig
	off := 1
	off = putString(buf, off, e.TopicName)
	off = putString(buf, off, e.Key)
	putString(buf, off, e.Value)
	return buf
}

func UnmarshalAlterConfig(payload []byte) (AlterConfigEntry, error) {
	var e AlterConfigEntry
	off := 0
	var err error

	e.TopicName, off, err = getString(payload, off)
	if err != nil {
		return e, err
	}
	e.Key, off, err = getString(payload, off)
	if err != nil {
		return e, err
	}
	e.Value, _, err = getString(payload, off)
	return e, err
}

func MarshalOffsetCommit(e *OffsetCommitEntry) []byte {
	size := 1 + 2 + len(e.Group) + 2 + len(e.Topic) + 4 + 8 + 2 + len(e.Metadata)
	buf := make([]byte, size)
	buf[0] = EntryOffsetCommit
	off := 1
	off = putString(buf, off, e.Group)
	off = putString(buf, off, e.Topic)
	binary.BigEndian.PutUint32(buf[off:off+4], uint32(e.Partition))
	off += 4
	binary.BigEndian.PutUint64(buf[off:off+8], uint64(e.Offset))
	off += 8
	putString(buf, off, e.Metadata)
	return buf
}

func UnmarshalOffsetCommit(payload []byte) (OffsetCommitEntry, error) {
	var e OffsetCommitEntry
	off := 0
	var err error

	e.Group, off, err = getString(payload, off)
	if err != nil {
		return e, err
	}
	e.Topic, off, err = getString(payload, off)
	if err != nil {
		return e, err
	}

	if off+4 > len(payload) {
		return e, fmt.Errorf("short read for partition")
	}
	e.Partition = int32(binary.BigEndian.Uint32(payload[off : off+4]))
	off += 4

	if off+8 > len(payload) {
		return e, fmt.Errorf("short read for offset")
	}
	e.Offset = int64(binary.BigEndian.Uint64(payload[off : off+8]))
	off += 8

	e.Metadata, _, err = getString(payload, off)
	return e, err
}

func MarshalProducerID(e *ProducerIDEntry) []byte {
	buf := make([]byte, 1+8)
	buf[0] = EntryProducerID
	binary.BigEndian.PutUint64(buf[1:9], uint64(e.NextProducerID))
	return buf
}

func UnmarshalProducerID(payload []byte) (ProducerIDEntry, error) {
	if len(payload) < 8 {
		return ProducerIDEntry{}, fmt.Errorf("short read for producer ID")
	}
	return ProducerIDEntry{
		NextProducerID: int64(binary.BigEndian.Uint64(payload[0:8])),
	}, nil
}

func MarshalLogStartOffset(e *LogStartOffsetEntry) []byte {
	size := 1 + 2 + len(e.TopicName) + 4 + 8
	buf := make([]byte, size)
	buf[0] = EntryLogStartOffset
	off := 1
	off = putString(buf, off, e.TopicName)
	binary.BigEndian.PutUint32(buf[off:off+4], uint32(e.Partition))
	off += 4
	binary.BigEndian.PutUint64(buf[off:off+8], uint64(e.LogStartOffset))
	return buf
}

func UnmarshalLogStartOffset(payload []byte) (LogStartOffsetEntry, error) {
	var e LogStartOffsetEntry
	off := 0
	var err error

	e.TopicName, off, err = getString(payload, off)
	if err != nil {
		return e, err
	}

	if off+4 > len(payload) {
		return e, fmt.Errorf("short read for partition")
	}
	e.Partition = int32(binary.BigEndian.Uint32(payload[off : off+4]))
	off += 4

	if off+8 > len(payload) {
		return e, fmt.Errorf("short read for logStartOffset")
	}
	e.LogStartOffset = int64(binary.BigEndian.Uint64(payload[off : off+8]))
	return e, nil
}

type CompactionWatermarkEntry struct {
	TopicName   string
	Partition   int32
	CleanedUpTo int64
}

func MarshalCompactionWatermark(e *CompactionWatermarkEntry) []byte {
	size := 1 + 2 + len(e.TopicName) + 4 + 8
	buf := make([]byte, size)
	buf[0] = EntryCompactionWatermark
	off := 1
	off = putString(buf, off, e.TopicName)
	binary.BigEndian.PutUint32(buf[off:off+4], uint32(e.Partition))
	off += 4
	binary.BigEndian.PutUint64(buf[off:off+8], uint64(e.CleanedUpTo))
	return buf
}

func UnmarshalCompactionWatermark(payload []byte) (CompactionWatermarkEntry, error) {
	var e CompactionWatermarkEntry
	off := 0
	var err error

	e.TopicName, off, err = getString(payload, off)
	if err != nil {
		return e, err
	}

	if off+4 > len(payload) {
		return e, fmt.Errorf("short read for partition")
	}
	e.Partition = int32(binary.BigEndian.Uint32(payload[off : off+4]))
	off += 4

	if off+8 > len(payload) {
		return e, fmt.Errorf("short read for cleanedUpTo")
	}
	e.CleanedUpTo = int64(binary.BigEndian.Uint64(payload[off : off+8]))
	return e, nil
}

type ScramCredentialEntry struct {
	Username   string
	Mechanism  int8 // 1=SHA-256, 2=SHA-512
	Iterations int32
	Salt       []byte
	SaltedPass []byte
	MechName   string // "SCRAM-SHA-256" or "SCRAM-SHA-512" (not persisted, derived from Mechanism)
}

type ScramCredentialDeleteEntry struct {
	Username  string
	Mechanism int8
	MechName  string // not persisted, derived from Mechanism
}

func putBytes(buf []byte, off int, b []byte) int {
	binary.BigEndian.PutUint16(buf[off:off+2], uint16(len(b)))
	off += 2
	copy(buf[off:off+len(b)], b)
	return off + len(b)
}

func getBytes(buf []byte, off int) ([]byte, int, error) {
	if off+2 > len(buf) {
		return nil, off, fmt.Errorf("short read for bytes length at offset %d", off)
	}
	n := int(binary.BigEndian.Uint16(buf[off : off+2]))
	off += 2
	if off+n > len(buf) {
		return nil, off, fmt.Errorf("short read for bytes data at offset %d, need %d bytes", off, n)
	}
	b := make([]byte, n)
	copy(b, buf[off:off+n])
	return b, off + n, nil
}

func MarshalScramCredential(e *ScramCredentialEntry) []byte {
	size := 1 + 2 + len(e.Username) + 1 + 4 + 2 + len(e.Salt) + 2 + len(e.SaltedPass)
	buf := make([]byte, size)
	buf[0] = EntryScramCredential
	off := 1
	off = putString(buf, off, e.Username)
	buf[off] = byte(e.Mechanism)
	off++
	binary.BigEndian.PutUint32(buf[off:off+4], uint32(e.Iterations))
	off += 4
	off = putBytes(buf, off, e.Salt)
	putBytes(buf, off, e.SaltedPass)
	return buf
}

func UnmarshalScramCredential(payload []byte) (ScramCredentialEntry, error) {
	var e ScramCredentialEntry
	off := 0
	var err error

	e.Username, off, err = getString(payload, off)
	if err != nil {
		return e, err
	}
	if off >= len(payload) {
		return e, fmt.Errorf("short read for mechanism")
	}
	e.Mechanism = int8(payload[off])
	off++
	if off+4 > len(payload) {
		return e, fmt.Errorf("short read for iterations")
	}
	e.Iterations = int32(binary.BigEndian.Uint32(payload[off : off+4]))
	off += 4
	e.Salt, off, err = getBytes(payload, off)
	if err != nil {
		return e, err
	}
	e.SaltedPass, _, err = getBytes(payload, off)
	if err != nil {
		return e, err
	}
	return e, nil
}

func MarshalScramCredentialDelete(e *ScramCredentialDeleteEntry) []byte {
	size := 1 + 2 + len(e.Username) + 1
	buf := make([]byte, size)
	buf[0] = EntryScramCredentialDelete
	off := 1
	off = putString(buf, off, e.Username)
	buf[off] = byte(e.Mechanism)
	return buf
}

func UnmarshalScramCredentialDelete(payload []byte) (ScramCredentialDeleteEntry, error) {
	var e ScramCredentialDeleteEntry
	off := 0
	var err error

	e.Username, off, err = getString(payload, off)
	if err != nil {
		return e, err
	}
	if off >= len(payload) {
		return e, fmt.Errorf("short read for mechanism")
	}
	e.Mechanism = int8(payload[off])
	return e, nil
}
