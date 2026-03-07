package s3

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

// Compression codec constants from RecordBatch Attributes bits 0-2.
const (
	CompressionNone   = 0
	CompressionGZIP   = 1
	CompressionSnappy = 2
	CompressionLZ4    = 3
	CompressionZSTD   = 4
)

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

type Record struct {
	Length         int
	Attributes     int8
	TimestampDelta int64
	OffsetDelta    int32
	Key            []byte // nil means null key
	Value          []byte // nil means null value (tombstone)
	Headers        []RecordHeader
}

func (r *Record) AbsoluteOffset(baseOffset int64) int64 {
	return baseOffset + int64(r.OffsetDelta)
}

func (r *Record) AbsoluteTimestamp(baseTimestamp int64) int64 {
	return baseTimestamp + r.TimestampDelta
}

type RecordHeader struct {
	Key   string
	Value []byte
}

// BatchHeader holds the parsed 61-byte RecordBatch header fields.
type BatchHeader struct {
	BaseOffset      int64
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

func (h *BatchHeader) CompressionCodec() int {
	return int(h.Attributes & 0x07)
}

func (h *BatchHeader) IsTransactional() bool {
	return h.Attributes&0x10 != 0
}

func (h *BatchHeader) IsControlBatch() bool {
	return h.Attributes&0x20 != 0
}

// TimestampType returns 0 for CreateTime, 1 for LogAppendTime.
func (h *BatchHeader) TimestampType() int {
	return int((h.Attributes >> 3) & 0x01)
}

func ParseBatchHeaderFromRaw(raw []byte) (BatchHeader, error) {
	if len(raw) < 61 {
		return BatchHeader{}, errors.New("batch too short: need at least 61 bytes")
	}
	return BatchHeader{
		BaseOffset:      int64(binary.BigEndian.Uint64(raw[0:8])),
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

func DecompressRecords(raw []byte, codec int) ([]byte, error) {
	recordsData := raw[61:]
	if len(recordsData) == 0 {
		return nil, nil
	}

	switch codec {
	case CompressionNone:
		return recordsData, nil
	case CompressionGZIP:
		return decompressGzip(recordsData)
	case CompressionSnappy:
		return decompressSnappy(recordsData)
	case CompressionLZ4:
		return decompressLZ4(recordsData)
	case CompressionZSTD:
		return decompressZSTD(recordsData)
	default:
		return nil, fmt.Errorf("unknown compression codec: %d", codec)
	}
}

func CompressRecords(data []byte, codec int) ([]byte, error) {
	switch codec {
	case CompressionNone:
		return data, nil
	case CompressionGZIP:
		return compressGzip(data)
	case CompressionSnappy:
		return compressSnappy(data)
	case CompressionLZ4:
		return compressLZ4(data)
	case CompressionZSTD:
		return compressZSTD(data)
	default:
		return nil, fmt.Errorf("unknown compression codec: %d", codec)
	}
}

func IterateRecords(decompressed []byte, fn func(Record) bool) error {
	off := 0
	for off < len(decompressed) {
		rec, bytesRead, err := parseRecord(decompressed, off)
		if err != nil {
			return fmt.Errorf("parse record at offset %d: %w", off, err)
		}
		if !fn(rec) {
			return nil
		}
		off += bytesRead
	}
	return nil
}

// BuildRecordBatch creates a new RecordBatch from a set of records using
// the original batch header as a template. Records are serialized,
// compressed with the given codec, and a new CRC is computed.
func BuildRecordBatch(header BatchHeader, records []Record, codec int) ([]byte, error) {
	if len(records) == 0 {
		return nil, nil
	}

	var recBuf bytes.Buffer
	for _, r := range records {
		appendRecord(&recBuf, r)
	}

	compressed, err := CompressRecords(recBuf.Bytes(), codec)
	if err != nil {
		return nil, fmt.Errorf("compress records: %w", err)
	}

	batchLen := 49 + len(compressed) // everything after BaseOffset(8) + BatchLength(4)
	totalSize := 12 + batchLen
	buf := make([]byte, totalSize)

	binary.BigEndian.PutUint64(buf[0:8], uint64(header.BaseOffset))
	binary.BigEndian.PutUint32(buf[8:12], uint32(batchLen))
	binary.BigEndian.PutUint32(buf[12:16], 0) // PartitionLeaderEpoch
	buf[16] = 2                               // Magic
	// CRC at buf[17:21] computed below
	binary.BigEndian.PutUint16(buf[21:23], uint16(header.Attributes))
	binary.BigEndian.PutUint32(buf[23:27], uint32(records[len(records)-1].OffsetDelta))
	binary.BigEndian.PutUint64(buf[27:35], uint64(header.BaseTimestamp))

	maxTS := header.BaseTimestamp
	for _, r := range records {
		ts := header.BaseTimestamp + r.TimestampDelta
		if ts > maxTS {
			maxTS = ts
		}
	}
	binary.BigEndian.PutUint64(buf[35:43], uint64(maxTS))
	binary.BigEndian.PutUint64(buf[43:51], uint64(header.ProducerID))
	binary.BigEndian.PutUint16(buf[51:53], uint16(header.ProducerEpoch))
	binary.BigEndian.PutUint32(buf[53:57], uint32(header.BaseSequence))
	binary.BigEndian.PutUint32(buf[57:61], uint32(len(records)))

	copy(buf[61:], compressed)

	crc := crc32.Checksum(buf[21:], crc32cTable)
	binary.BigEndian.PutUint32(buf[17:21], crc)

	return buf, nil
}

// parseRecord parses one record from decompressed bytes at the given offset.
// Returns the record, number of bytes consumed, and any error.
//
// Record format (all varints are zigzag-encoded):
//
//	length: varint          (length of the rest of the record)
//	attributes: int8        (currently unused, reserved)
//	timestampDelta: varint  (delta from BaseTimestamp)
//	offsetDelta: varint     (delta from BaseOffset)
//	keyLength: varint       (-1 for null key)
//	key: bytes
//	valueLength: varint     (-1 for null value / tombstone)
//	value: bytes
//	headerCount: varint
//	headers: [headerCount × (headerKeyLen:varint, headerKey:bytes, headerValLen:varint, headerVal:bytes)]
func parseRecord(data []byte, off int) (Record, int, error) {
	startOff := off
	if off >= len(data) {
		return Record{}, 0, errors.New("no data for record")
	}

	length, n := decodeVarint(data[off:])
	if n <= 0 {
		return Record{}, 0, errors.New("invalid record length varint")
	}
	off += n

	recEnd := off + int(length)
	if recEnd > len(data) {
		return Record{}, 0, fmt.Errorf("record length %d exceeds available data %d", length, len(data)-off)
	}

	var rec Record
	rec.Length = int(length)

	if off >= recEnd {
		return Record{}, 0, errors.New("truncated record: no attributes")
	}
	rec.Attributes = int8(data[off])
	off++

	tsDelta, n := decodeVarint(data[off:])
	if n <= 0 {
		return Record{}, 0, errors.New("invalid timestamp delta varint")
	}
	rec.TimestampDelta = tsDelta
	off += n

	offDelta, n := decodeVarint(data[off:])
	if n <= 0 {
		return Record{}, 0, errors.New("invalid offset delta varint")
	}
	rec.OffsetDelta = int32(offDelta)
	off += n

	keyLen, n := decodeVarint(data[off:])
	if n <= 0 {
		return Record{}, 0, errors.New("invalid key length varint")
	}
	off += n
	if keyLen >= 0 {
		if off+int(keyLen) > recEnd {
			return Record{}, 0, errors.New("key extends past record end")
		}
		rec.Key = make([]byte, keyLen)
		copy(rec.Key, data[off:off+int(keyLen)])
		off += int(keyLen)
	}
	valLen, n := decodeVarint(data[off:])
	if n <= 0 {
		return Record{}, 0, errors.New("invalid value length varint")
	}
	off += n
	if valLen >= 0 {
		if off+int(valLen) > recEnd {
			return Record{}, 0, errors.New("value extends past record end")
		}
		rec.Value = make([]byte, valLen)
		copy(rec.Value, data[off:off+int(valLen)])
		off += int(valLen)
	}
	headerCount, n := decodeVarint(data[off:])
	if n <= 0 {
		return Record{}, 0, errors.New("invalid header count varint")
	}
	off += n

	if headerCount > 0 {
		rec.Headers = make([]RecordHeader, 0, headerCount)
		for i := int64(0); i < headerCount; i++ {
			hKeyLen, n := decodeVarint(data[off:])
			if n <= 0 {
				return Record{}, 0, errors.New("invalid header key length varint")
			}
			off += n
			hKey := ""
			if hKeyLen >= 0 {
				if off+int(hKeyLen) > recEnd {
					return Record{}, 0, errors.New("header key extends past record end")
				}
				hKey = string(data[off : off+int(hKeyLen)])
				off += int(hKeyLen)
			}

			hValLen, n := decodeVarint(data[off:])
			if n <= 0 {
				return Record{}, 0, errors.New("invalid header value length varint")
			}
			off += n
			var hVal []byte
			if hValLen >= 0 {
				if off+int(hValLen) > recEnd {
					return Record{}, 0, errors.New("header value extends past record end")
				}
				hVal = make([]byte, hValLen)
				copy(hVal, data[off:off+int(hValLen)])
				off += int(hValLen)
			}

			rec.Headers = append(rec.Headers, RecordHeader{Key: hKey, Value: hVal})
		}
	}

	return rec, recEnd - startOff, nil
}

func appendRecord(buf *bytes.Buffer, rec Record) {
	var tmp bytes.Buffer

	tmp.WriteByte(byte(rec.Attributes))
	appendVarint(&tmp, rec.TimestampDelta)
	appendVarint(&tmp, int64(rec.OffsetDelta))

	if rec.Key == nil {
		appendVarint(&tmp, -1)
	} else {
		appendVarint(&tmp, int64(len(rec.Key)))
		tmp.Write(rec.Key)
	}

	if rec.Value == nil {
		appendVarint(&tmp, -1)
	} else {
		appendVarint(&tmp, int64(len(rec.Value)))
		tmp.Write(rec.Value)
	}

	appendVarint(&tmp, int64(len(rec.Headers)))
	for _, h := range rec.Headers {
		appendVarint(&tmp, int64(len(h.Key)))
		tmp.WriteString(h.Key)
		if h.Value == nil {
			appendVarint(&tmp, -1)
		} else {
			appendVarint(&tmp, int64(len(h.Value)))
			tmp.Write(h.Value)
		}
	}

	appendVarint(buf, int64(tmp.Len()))
	buf.Write(tmp.Bytes())
}

func decodeVarint(data []byte) (int64, int) {
	var x uint64
	var s uint
	for i, b := range data {
		if i >= 10 {
			return 0, -1 // overflow
		}
		if b < 0x80 {
			x |= uint64(b) << s
			// Zigzag decode
			return int64(x>>1) ^ -(int64(x) & 1), i + 1
		}
		x |= uint64(b&0x7f) << s
		s += 7
	}
	return 0, -1 // short buffer
}

func appendVarint(buf *bytes.Buffer, v int64) {
	ux := uint64(v<<1) ^ uint64(v>>63) // zigzag encode
	for ux >= 0x80 {
		buf.WriteByte(byte(ux) | 0x80)
		ux >>= 7
	}
	buf.WriteByte(byte(ux))
}

func decompressGzip(data []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("gzip reader: %w", err)
	}
	defer r.Close() //nolint:errcheck // best-effort close
	return io.ReadAll(r)
}

func compressGzip(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	if _, err := w.Write(data); err != nil {
		return nil, fmt.Errorf("gzip write: %w", err)
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("gzip close: %w", err)
	}
	return buf.Bytes(), nil
}

func decompressSnappy(data []byte) ([]byte, error) {
	return snappy.Decode(nil, data)
}

func compressSnappy(data []byte) ([]byte, error) {
	return snappy.Encode(nil, data), nil
}

func decompressLZ4(data []byte) ([]byte, error) {
	r := lz4.NewReader(bytes.NewReader(data))
	return io.ReadAll(r)
}

func compressLZ4(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	w := lz4.NewWriter(&buf)
	if _, err := w.Write(data); err != nil {
		return nil, fmt.Errorf("lz4 write: %w", err)
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("lz4 close: %w", err)
	}
	return buf.Bytes(), nil
}

var (
	zstdEncoder *zstd.Encoder
	zstdDecoder *zstd.Decoder
)

func init() {
	var err error
	zstdEncoder, err = zstd.NewWriter(nil)
	if err != nil {
		panic("zstd encoder init: " + err.Error())
	}
	zstdDecoder, err = zstd.NewReader(nil)
	if err != nil {
		panic("zstd decoder init: " + err.Error())
	}
}

func decompressZSTD(data []byte) ([]byte, error) {
	return zstdDecoder.DecodeAll(data, nil)
}

func compressZSTD(data []byte) ([]byte, error) {
	return zstdEncoder.EncodeAll(data, nil), nil
}

func BuildTestBatch(baseOffset, baseTimestamp int64, records []Record, codec int) ([]byte, error) {
	header := BatchHeader{
		BaseOffset:    baseOffset,
		Attributes:    int16(codec), // bits 0-2 = codec
		BaseTimestamp: baseTimestamp,
		MaxTimestamp:  baseTimestamp,
		ProducerID:    -1,
		ProducerEpoch: -1,
		BaseSequence:  -1,
	}

	for _, r := range records {
		ts := baseTimestamp + r.TimestampDelta
		if ts > header.MaxTimestamp {
			header.MaxTimestamp = ts
		}
	}

	if len(records) > 0 {
		header.LastOffsetDelta = records[len(records)-1].OffsetDelta
	}

	return BuildRecordBatch(header, records, codec)
}
