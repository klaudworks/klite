package s3

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// makeMinimalBatch creates a minimal valid RecordBatch header for testing.
// The resulting bytes have valid header fields for the S3 object format tests.
// These are NOT actual Kafka-decodable batches.
func makeMinimalBatch(baseOffset int64, lastOffsetDelta int32) []byte {
	batch := make([]byte, 80)

	// Offset 8-11: BatchLength (total length minus 12)
	batchLength := uint32(len(batch) - 12)
	batch[8] = byte(batchLength >> 24)
	batch[9] = byte(batchLength >> 16)
	batch[10] = byte(batchLength >> 8)
	batch[11] = byte(batchLength)

	// Offset 16: Magic byte (must be 2)
	batch[16] = 2

	// Offset 23-26: LastOffsetDelta
	batch[23] = byte(lastOffsetDelta >> 24)
	batch[24] = byte(lastOffsetDelta >> 16)
	batch[25] = byte(lastOffsetDelta >> 8)
	batch[26] = byte(lastOffsetDelta)

	// Offset 57-60: NumRecords
	numRecords := lastOffsetDelta + 1
	batch[57] = byte(numRecords >> 24)
	batch[58] = byte(numRecords >> 16)
	batch[59] = byte(numRecords >> 8)
	batch[60] = byte(numRecords)

	return batch
}

func TestKeyLookup(t *testing.T) {
	t.Parallel()

	mem := NewInMemoryS3()
	client := NewClient(ClientConfig{
		S3Client: mem,
		Bucket:   "test-bucket",
		Prefix:   "klite/test",
	})
	tid := testTopicID

	reader := NewReader(client, nil)

	batches1 := []BatchData{
		{RawBytes: makeMinimalBatch(0, 9), BaseOffset: 0, LastOffsetDelta: 9},
	}
	obj1 := BuildObject(batches1)
	key1 := ObjectKey("klite/test", "lookup-test", tid, 0, 0)

	batches2 := []BatchData{
		{RawBytes: makeMinimalBatch(10, 9), BaseOffset: 10, LastOffsetDelta: 9},
	}
	obj2 := BuildObject(batches2)
	key2 := ObjectKey("klite/test", "lookup-test", tid, 0, 10)

	ctx := context.Background()
	require.NoError(t, client.PutObject(ctx, key1, obj1))
	require.NoError(t, client.PutObject(ctx, key2, obj2))

	// Fetch offset 5 -- should come from first object
	data, err := reader.Fetch(ctx, "lookup-test", tid, 0, 5, 1024*1024)
	require.NoError(t, err)
	require.NotNil(t, data, "should find data for offset 5")

	// Fetch offset 15 -- should come from second object
	data, err = reader.Fetch(ctx, "lookup-test", tid, 0, 15, 1024*1024)
	require.NoError(t, err)
	require.NotNil(t, data, "should find data for offset 15")
}

func TestFooterCache(t *testing.T) {
	t.Parallel()

	mem := NewInMemoryS3()
	client := NewClient(ClientConfig{
		S3Client: mem,
		Bucket:   "test-bucket",
		Prefix:   "klite/test",
	})

	reader := NewReader(client, nil)

	batches := []BatchData{
		{RawBytes: makeMinimalBatch(0, 4), BaseOffset: 0, LastOffsetDelta: 4},
		{RawBytes: makeMinimalBatch(5, 4), BaseOffset: 5, LastOffsetDelta: 4},
	}
	tid := testTopicID
	obj := BuildObject(batches)
	key := ObjectKey("klite/test", "cache-test", tid, 0, 0)

	ctx := context.Background()
	require.NoError(t, client.PutObject(ctx, key, obj))

	// First fetch -- footer cache miss
	require.Equal(t, 0, reader.FooterCacheSize())
	_, err := reader.Fetch(ctx, "cache-test", tid, 0, 0, 1024*1024)
	require.NoError(t, err)
	require.Equal(t, 1, reader.FooterCacheSize(), "footer should be cached after first fetch")

	// Second fetch -- footer cache hit (no new S3 GET for footer)
	rangesBefore := len(mem.RangeRequests)
	_, err = reader.Fetch(ctx, "cache-test", tid, 0, 5, 1024*1024)
	require.NoError(t, err)
	require.Equal(t, 1, reader.FooterCacheSize(), "still 1 cached footer")
	rangesAfter := len(mem.RangeRequests)
	require.Equal(t, rangesBefore+1, rangesAfter, "should only make 1 range request (data), not 2 (data+footer)")
}

func TestFooterCorrupted(t *testing.T) {
	t.Parallel()

	mem := NewInMemoryS3()
	client := NewClient(ClientConfig{
		S3Client: mem,
		Bucket:   "test-bucket",
		Prefix:   "klite/test",
	})

	reader := NewReader(client, nil)

	badObj := make([]byte, 100)
	for i := range badObj {
		badObj[i] = 0xAA
	}
	tid := testTopicID
	key := ObjectKey("klite/test", "corrupt-test", tid, 0, 0)

	ctx := context.Background()
	require.NoError(t, client.PutObject(ctx, key, badObj))

	_, err := reader.Fetch(ctx, "corrupt-test", tid, 0, 0, 1024*1024)
	require.Error(t, err, "should return error for corrupted footer")
	require.Contains(t, err.Error(), "footer magic", "error should mention footer magic")
}

func TestRangeRead(t *testing.T) {
	t.Parallel()

	mem := NewInMemoryS3()
	client := NewClient(ClientConfig{
		S3Client: mem,
		Bucket:   "test-bucket",
		Prefix:   "klite/test",
	})
	reader := NewReader(client, nil)

	batches := []BatchData{
		{RawBytes: makeMinimalBatch(0, 4), BaseOffset: 0, LastOffsetDelta: 4},
		{RawBytes: makeMinimalBatch(5, 4), BaseOffset: 5, LastOffsetDelta: 4},
		{RawBytes: makeMinimalBatch(10, 4), BaseOffset: 10, LastOffsetDelta: 4},
	}
	tid := testTopicID
	obj := BuildObject(batches)
	key := ObjectKey("klite/test", "range-test", tid, 0, 0)

	ctx := context.Background()
	require.NoError(t, client.PutObject(ctx, key, obj))

	// Fetch offset 5 -- should only read the second batch, not the whole object
	data, err := reader.Fetch(ctx, "range-test", tid, 0, 5, int32(len(batches[1].RawBytes)))
	require.NoError(t, err)
	require.NotNil(t, data)

	requests := reader.RangeRequests()
	require.NotEmpty(t, requests, "should have made range request(s)")

	for _, rr := range requests {
		rangeSize := rr.EndByte - rr.StartByte
		require.Less(t, rangeSize, int64(len(obj)),
			"range read should be smaller than full object")
	}
}

// TestFetchBatchesGapBetweenObjects verifies that when the requested offset
// falls in a gap between two S3 objects, the reader tries the next object
// instead of returning nil.
//
// Setup: Object 1 has offsets 0-9, Object 2 has offsets 20-29. Fetch offset 15,
// which is between the two objects. findObjectForOffset picks Object 1, but
// Object 1 doesn't contain offset 15. The reader should try Object 2 and
// return data starting at offset 20.
func TestFetchBatchesGapBetweenObjects(t *testing.T) {
	t.Parallel()

	mem := NewInMemoryS3()
	client := NewClient(ClientConfig{
		S3Client: mem,
		Bucket:   "test-bucket",
		Prefix:   "klite/test",
	})
	tid := testTopicID
	reader := NewReader(client, nil)

	// Object 1: offsets 0-9
	batches1 := []BatchData{
		{RawBytes: makeMinimalBatch(0, 9), BaseOffset: 0, LastOffsetDelta: 9},
	}
	obj1 := BuildObject(batches1)
	key1 := ObjectKey("klite/test", "gap-test", tid, 0, 0)

	// Object 2: offsets 20-29 (gap at 10-19)
	batches2 := []BatchData{
		{RawBytes: makeMinimalBatch(20, 9), BaseOffset: 20, LastOffsetDelta: 9},
	}
	obj2 := BuildObject(batches2)
	key2 := ObjectKey("klite/test", "gap-test", tid, 0, 20)

	ctx := context.Background()
	require.NoError(t, client.PutObject(ctx, key1, obj1))
	require.NoError(t, client.PutObject(ctx, key2, obj2))

	// Fetch offset 15 — in the gap between objects.
	// findObjectForOffset should find Object 1 (base offset 0 <= 15).
	// Object 1 doesn't have offset 15. Reader should try Object 2.
	result, err := reader.FetchBatches(ctx, "gap-test", tid, 0, 15, 1024*1024)
	require.NoError(t, err)
	require.NotEmpty(t, result, "should find data from next object when offset is in gap")
	require.Equal(t, int64(20), result[0].BaseOffset,
		"should return batch from Object 2 (next available after gap)")
}
