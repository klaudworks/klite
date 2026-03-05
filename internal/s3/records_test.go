package s3

import (
	"hash/crc32"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRecordIteration(t *testing.T) {
	// Build a batch with known records using no compression
	records := []Record{
		{OffsetDelta: 0, TimestampDelta: 0, Key: []byte("key-0"), Value: []byte("val-0")},
		{OffsetDelta: 1, TimestampDelta: 100, Key: []byte("key-1"), Value: []byte("val-1")},
		{OffsetDelta: 2, TimestampDelta: 200, Key: []byte("key-2"), Value: []byte("val-2")},
	}

	batchBytes, err := BuildTestBatch(0, 1000, records, CompressionNone)
	require.NoError(t, err)
	require.NotNil(t, batchBytes)

	// Parse header
	header, err := ParseBatchHeaderFromRaw(batchBytes)
	require.NoError(t, err)
	require.Equal(t, int32(3), header.NumRecords)
	require.Equal(t, int32(2), header.LastOffsetDelta)
	require.Equal(t, int64(0), header.BaseOffset)

	// Decompress
	decompressed, err := DecompressRecords(batchBytes, header.CompressionCodec())
	require.NoError(t, err)

	// Iterate
	var parsed []Record
	err = IterateRecords(decompressed, func(r Record) bool {
		parsed = append(parsed, r)
		return true
	})
	require.NoError(t, err)
	require.Len(t, parsed, 3)

	require.Equal(t, "key-0", string(parsed[0].Key))
	require.Equal(t, "val-0", string(parsed[0].Value))
	require.Equal(t, int32(0), parsed[0].OffsetDelta)

	require.Equal(t, "key-1", string(parsed[1].Key))
	require.Equal(t, int32(1), parsed[1].OffsetDelta)
	require.Equal(t, int64(100), parsed[1].TimestampDelta)

	require.Equal(t, "key-2", string(parsed[2].Key))
	require.Equal(t, int32(2), parsed[2].OffsetDelta)
}

func TestRecordIterationNullKey(t *testing.T) {
	records := []Record{
		{OffsetDelta: 0, TimestampDelta: 0, Key: nil, Value: []byte("val-0")},
		{OffsetDelta: 1, TimestampDelta: 0, Key: []byte("key-1"), Value: nil},
	}

	batchBytes, err := BuildTestBatch(0, 1000, records, CompressionNone)
	require.NoError(t, err)

	decompressed, err := DecompressRecords(batchBytes, CompressionNone)
	require.NoError(t, err)

	var parsed []Record
	err = IterateRecords(decompressed, func(r Record) bool {
		parsed = append(parsed, r)
		return true
	})
	require.NoError(t, err)
	require.Len(t, parsed, 2)

	require.Nil(t, parsed[0].Key)
	require.Equal(t, "val-0", string(parsed[0].Value))

	require.Equal(t, "key-1", string(parsed[1].Key))
	require.Nil(t, parsed[1].Value) // tombstone
}

func TestRecordIterationWithHeaders(t *testing.T) {
	records := []Record{
		{
			OffsetDelta:    0,
			TimestampDelta: 0,
			Key:            []byte("key"),
			Value:          []byte("val"),
			Headers: []RecordHeader{
				{Key: "h1", Value: []byte("v1")},
				{Key: "h2", Value: []byte("v2")},
			},
		},
	}

	batchBytes, err := BuildTestBatch(0, 1000, records, CompressionNone)
	require.NoError(t, err)

	decompressed, err := DecompressRecords(batchBytes, CompressionNone)
	require.NoError(t, err)

	var parsed []Record
	err = IterateRecords(decompressed, func(r Record) bool {
		parsed = append(parsed, r)
		return true
	})
	require.NoError(t, err)
	require.Len(t, parsed, 1)
	require.Len(t, parsed[0].Headers, 2)
	require.Equal(t, "h1", parsed[0].Headers[0].Key)
	require.Equal(t, "v1", string(parsed[0].Headers[0].Value))
}

func TestCompactionCompression(t *testing.T) {
	codecs := []struct {
		name  string
		codec int
	}{
		{"none", CompressionNone},
		{"gzip", CompressionGZIP},
		{"snappy", CompressionSnappy},
		{"lz4", CompressionLZ4},
		{"zstd", CompressionZSTD},
	}

	for _, tc := range codecs {
		t.Run(tc.name, func(t *testing.T) {
			records := []Record{
				{OffsetDelta: 0, TimestampDelta: 0, Key: []byte("key-0"), Value: []byte("value-0")},
				{OffsetDelta: 1, TimestampDelta: 100, Key: []byte("key-1"), Value: []byte("value-1")},
				{OffsetDelta: 2, TimestampDelta: 200, Key: []byte("key-2"), Value: []byte("value-2")},
			}

			batchBytes, err := BuildTestBatch(10, 5000, records, tc.codec)
			require.NoError(t, err)
			require.NotNil(t, batchBytes)

			// Parse and verify header
			header, err := ParseBatchHeaderFromRaw(batchBytes)
			require.NoError(t, err)
			require.Equal(t, int32(3), header.NumRecords)
			require.Equal(t, int32(2), header.LastOffsetDelta)
			require.Equal(t, int64(10), header.BaseOffset)
			require.Equal(t, tc.codec, header.CompressionCodec())

			// Decompress and iterate
			decompressed, err := DecompressRecords(batchBytes, tc.codec)
			require.NoError(t, err)

			var parsed []Record
			err = IterateRecords(decompressed, func(r Record) bool {
				parsed = append(parsed, r)
				return true
			})
			require.NoError(t, err)
			require.Len(t, parsed, 3)
			require.Equal(t, "key-0", string(parsed[0].Key))
			require.Equal(t, "value-2", string(parsed[2].Value))
		})
	}
}

func TestCompactionCRCValid(t *testing.T) {
	records := []Record{
		{OffsetDelta: 0, TimestampDelta: 0, Key: []byte("key"), Value: []byte("val")},
	}

	batchBytes, err := BuildTestBatch(0, 1000, records, CompressionNone)
	require.NoError(t, err)

	// Verify CRC
	header, err := ParseBatchHeaderFromRaw(batchBytes)
	require.NoError(t, err)

	computedCRC := crc32.Checksum(batchBytes[21:], crc32.MakeTable(crc32.Castagnoli))
	require.Equal(t, header.CRC, computedCRC, "CRC should match")
}

func TestRecordEmptyKey(t *testing.T) {
	// Empty key (length 0, not null)
	records := []Record{
		{OffsetDelta: 0, TimestampDelta: 0, Key: []byte{}, Value: []byte("val")},
	}

	batchBytes, err := BuildTestBatch(0, 1000, records, CompressionNone)
	require.NoError(t, err)

	decompressed, err := DecompressRecords(batchBytes, CompressionNone)
	require.NoError(t, err)

	var parsed []Record
	err = IterateRecords(decompressed, func(r Record) bool {
		parsed = append(parsed, r)
		return true
	})
	require.NoError(t, err)
	require.Len(t, parsed, 1)
	require.NotNil(t, parsed[0].Key)
	require.Len(t, parsed[0].Key, 0)
}
