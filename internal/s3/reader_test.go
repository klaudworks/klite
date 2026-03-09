package s3

import (
	"context"
	"sync/atomic"
	"testing"

	s3svc "github.com/aws/aws-sdk-go-v2/service/s3"
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

// --- AppendToListing tests ---

func TestAppendToListing_Basic(t *testing.T) {
	t.Parallel()

	mem := newListCountingS3()
	client := NewClient(ClientConfig{
		S3Client: mem,
		Bucket:   "test-bucket",
		Prefix:   "klite/test",
	})
	tid := testTopicID
	reader := NewReader(client, nil)
	ctx := context.Background()

	// Create two objects and populate the listing cache via a fetch.
	obj1 := BuildObject([]BatchData{
		{RawBytes: makeMinimalBatch(0, 9), BaseOffset: 0, LastOffsetDelta: 9},
	})
	obj2 := BuildObject([]BatchData{
		{RawBytes: makeMinimalBatch(10, 9), BaseOffset: 10, LastOffsetDelta: 9},
	})
	require.NoError(t, client.PutObject(ctx, ObjectKey("klite/test", "append-test", tid, 0, 0), obj1))
	require.NoError(t, client.PutObject(ctx, ObjectKey("klite/test", "append-test", tid, 0, 10), obj2))

	// Fetch offset 0 to populate the listing cache.
	_, err := reader.Fetch(ctx, "append-test", tid, 0, 0, 1024*1024)
	require.NoError(t, err)

	// Now PUT a third object directly to S3 and append to the listing cache.
	obj3 := BuildObject([]BatchData{
		{RawBytes: makeMinimalBatch(20, 9), BaseOffset: 20, LastOffsetDelta: 9},
	})
	key3 := ObjectKey("klite/test", "append-test", tid, 0, 20)
	require.NoError(t, client.PutObject(ctx, key3, obj3))

	reader.AppendToListing("append-test", tid, 0, ObjectInfo{
		Key:  key3,
		Size: int64(len(obj3)),
	})

	// Count LIST calls before the fetch.
	listsBefore := int(mem.listCalls.Load())

	// Fetch offset 20 — should succeed without a new LIST call.
	data, err := reader.Fetch(ctx, "append-test", tid, 0, 20, 1024*1024)
	require.NoError(t, err)
	require.NotNil(t, data, "should find data for offset 20 via appended listing")

	listsAfter := int(mem.listCalls.Load())
	require.Equal(t, listsBefore, listsAfter, "no new LIST call should be made after append")
}

func TestAppendToListing_AfterInvalidation(t *testing.T) {
	t.Parallel()

	mem := newListCountingS3()
	client := NewClient(ClientConfig{
		S3Client: mem,
		Bucket:   "test-bucket",
		Prefix:   "klite/test",
	})
	tid := testTopicID
	reader := NewReader(client, nil)
	ctx := context.Background()

	// Create an object and populate cache.
	obj := BuildObject([]BatchData{
		{RawBytes: makeMinimalBatch(0, 9), BaseOffset: 0, LastOffsetDelta: 9},
	})
	require.NoError(t, client.PutObject(ctx, ObjectKey("klite/test", "inv-test", tid, 0, 0), obj))
	_, err := reader.Fetch(ctx, "inv-test", tid, 0, 0, 1024*1024)
	require.NoError(t, err)

	// Simulate compaction: invalidate the listing.
	reader.InvalidateFooters("inv-test", tid, 0)

	// Now try to append — should be a no-op.
	reader.AppendToListing("inv-test", tid, 0, ObjectInfo{
		Key:  ObjectKey("klite/test", "inv-test", tid, 0, 10),
		Size: 100,
	})

	// Verify cache is still empty: next fetch must trigger a LIST.
	listsBefore := int(mem.listCalls.Load())
	_, err = reader.Fetch(ctx, "inv-test", tid, 0, 0, 1024*1024)
	require.NoError(t, err)
	listsAfter := int(mem.listCalls.Load())
	require.Greater(t, listsAfter, listsBefore,
		"fetch after invalidation+append should trigger a LIST (cache should be empty)")
}

func TestAppendToListing_ColdCache(t *testing.T) {
	t.Parallel()

	mem := newListCountingS3()
	client := NewClient(ClientConfig{
		S3Client: mem,
		Bucket:   "test-bucket",
		Prefix:   "klite/test",
	})
	tid := testTopicID
	reader := NewReader(client, nil)
	ctx := context.Background()

	// Put an object into S3 but never fetch (cache is cold).
	obj := BuildObject([]BatchData{
		{RawBytes: makeMinimalBatch(0, 9), BaseOffset: 0, LastOffsetDelta: 9},
	})
	key := ObjectKey("klite/test", "cold-test", tid, 0, 0)
	require.NoError(t, client.PutObject(ctx, key, obj))

	// Append on a cold cache — should be a no-op.
	reader.AppendToListing("cold-test", tid, 0, ObjectInfo{
		Key:  ObjectKey("klite/test", "cold-test", tid, 0, 10),
		Size: 100,
	})

	// Verify: next fetch triggers a LIST (cache was not created by append).
	listsBefore := int(mem.listCalls.Load())
	_, err := reader.Fetch(ctx, "cold-test", tid, 0, 0, 1024*1024)
	require.NoError(t, err)
	listsAfter := int(mem.listCalls.Load())
	require.Greater(t, listsAfter, listsBefore,
		"fetch on cold cache should trigger a LIST, even after append attempt")
}

func TestAppendToListing_SortOrder(t *testing.T) {
	t.Parallel()

	mem := NewInMemoryS3()
	client := NewClient(ClientConfig{
		S3Client: mem,
		Bucket:   "test-bucket",
		Prefix:   "klite/test",
	})
	tid := testTopicID
	reader := NewReader(client, nil)
	ctx := context.Background()

	// Create objects at offsets 0, 100, 200 and populate cache.
	for _, baseOff := range []int64{0, 100, 200} {
		obj := BuildObject([]BatchData{
			{RawBytes: makeMinimalBatch(baseOff, 9), BaseOffset: baseOff, LastOffsetDelta: 9},
		})
		require.NoError(t, client.PutObject(ctx, ObjectKey("klite/test", "sort-test", tid, 0, baseOff), obj))
	}
	_, err := reader.Fetch(ctx, "sort-test", tid, 0, 0, 1024*1024)
	require.NoError(t, err)

	// Append objects at offsets 300, 400, 500.
	for _, baseOff := range []int64{300, 400, 500} {
		obj := BuildObject([]BatchData{
			{RawBytes: makeMinimalBatch(baseOff, 9), BaseOffset: baseOff, LastOffsetDelta: 9},
		})
		key := ObjectKey("klite/test", "sort-test", tid, 0, baseOff)
		require.NoError(t, client.PutObject(ctx, key, obj))
		reader.AppendToListing("sort-test", tid, 0, ObjectInfo{
			Key:  key,
			Size: int64(len(obj)),
		})
	}

	// Verify binary search resolves all offsets correctly.
	tests := []struct {
		offset      int64
		wantBaseOff int64
	}{
		{0, 0},
		{50, 0}, // within first object's range
		{100, 100},
		{150, 100}, // within second object
		{300, 300}, // first appended object
		{350, 300}, // within appended object
		{450, 400}, // within second appended object
		{500, 500}, // last appended object
	}

	for _, tt := range tests {
		data, err := reader.FetchBatches(ctx, "sort-test", tid, 0, tt.offset, 1024*1024)
		require.NoError(t, err, "offset %d", tt.offset)
		require.NotEmpty(t, data, "offset %d should return data", tt.offset)
		require.Equal(t, tt.wantBaseOff, data[0].BaseOffset,
			"offset %d should resolve to object at base %d", tt.offset, tt.wantBaseOff)
	}
}

func TestAppendToListing_SizeAccuracy(t *testing.T) {
	t.Parallel()

	mem := NewInMemoryS3()
	client := NewClient(ClientConfig{
		S3Client: mem,
		Bucket:   "test-bucket",
		Prefix:   "klite/test",
	})
	tid := testTopicID
	reader := NewReader(client, nil)
	ctx := context.Background()

	// Create first object and populate cache.
	obj1 := BuildObject([]BatchData{
		{RawBytes: makeMinimalBatch(0, 4), BaseOffset: 0, LastOffsetDelta: 4},
	})
	require.NoError(t, client.PutObject(ctx, ObjectKey("klite/test", "size-test", tid, 0, 0), obj1))
	_, err := reader.Fetch(ctx, "size-test", tid, 0, 0, 1024*1024)
	require.NoError(t, err)

	// Create a second object with multiple batches (bigger footer).
	obj2 := BuildObject([]BatchData{
		{RawBytes: makeMinimalBatch(10, 4), BaseOffset: 10, LastOffsetDelta: 4},
		{RawBytes: makeMinimalBatch(15, 4), BaseOffset: 15, LastOffsetDelta: 4},
		{RawBytes: makeMinimalBatch(20, 4), BaseOffset: 20, LastOffsetDelta: 4},
	})
	key2 := ObjectKey("klite/test", "size-test", tid, 0, 10)
	require.NoError(t, client.PutObject(ctx, key2, obj2))

	// Append with correct size.
	reader.AppendToListing("size-test", tid, 0, ObjectInfo{
		Key:  key2,
		Size: int64(len(obj2)),
	})

	// Fetch from the appended object — footer parsing must succeed because
	// TailGet uses the Size from the listing to compute the byte range.
	data, err := reader.FetchBatches(ctx, "size-test", tid, 0, 10, 1024*1024)
	require.NoError(t, err)
	require.NotEmpty(t, data)
	require.Equal(t, int64(10), data[0].BaseOffset)
	// Should get all 3 batches.
	require.Len(t, data, 3)
	require.Equal(t, int64(15), data[1].BaseOffset)
	require.Equal(t, int64(20), data[2].BaseOffset)
}

// TestFlushThenConsume_NoListCall verifies the end-to-end behavioral property:
// after the initial listing cache population, repeated flush+consume cycles
// never trigger additional LIST calls.
func TestFlushThenConsume_NoListCall(t *testing.T) {
	t.Parallel()

	mem := newListCountingS3()
	client := NewClient(ClientConfig{
		S3Client: mem,
		Bucket:   "test-bucket",
		Prefix:   "klite/test",
	})
	tid := testTopicID
	reader := NewReader(client, nil)
	ctx := context.Background()

	// Seed an initial object and populate the cache.
	obj0 := BuildObject([]BatchData{
		{RawBytes: makeMinimalBatch(0, 9), BaseOffset: 0, LastOffsetDelta: 9},
	})
	require.NoError(t, client.PutObject(ctx, ObjectKey("klite/test", "nolist-test", tid, 0, 0), obj0))
	_, err := reader.Fetch(ctx, "nolist-test", tid, 0, 0, 1024*1024)
	require.NoError(t, err)

	listsBefore := int(mem.listCalls.Load())

	// Simulate 10 flush cycles: build object, upload, append to listing, then consume.
	for i := 1; i <= 10; i++ {
		baseOff := int64(i * 10)
		obj := BuildObject([]BatchData{
			{RawBytes: makeMinimalBatch(baseOff, 9), BaseOffset: baseOff, LastOffsetDelta: 9},
		})
		key := ObjectKey("klite/test", "nolist-test", tid, 0, baseOff)
		require.NoError(t, client.PutObject(ctx, key, obj))
		reader.AppendToListing("nolist-test", tid, 0, ObjectInfo{
			Key:  key,
			Size: int64(len(obj)),
		})

		// Consume the just-flushed offset.
		data, err := reader.Fetch(ctx, "nolist-test", tid, 0, baseOff, 1024*1024)
		require.NoError(t, err)
		require.NotNil(t, data, "flush cycle %d: should find data", i)
	}

	listsAfter := int(mem.listCalls.Load())
	require.Equal(t, listsBefore, listsAfter,
		"no LIST calls should be made during 10 flush+consume cycles")
}

// TestCompactionInvalidatesThenFlushAppend verifies that when compaction
// invalidates the listing and a flush follows, the append is a no-op and
// the next consumer fetch re-LISTs from S3 and sees all objects.
func TestCompactionInvalidatesThenFlushAppend(t *testing.T) {
	t.Parallel()

	mem := newListCountingS3()
	client := NewClient(ClientConfig{
		S3Client: mem,
		Bucket:   "test-bucket",
		Prefix:   "klite/test",
	})
	tid := testTopicID
	reader := NewReader(client, nil)
	ctx := context.Background()

	// Create initial objects and populate cache.
	obj1 := BuildObject([]BatchData{
		{RawBytes: makeMinimalBatch(0, 9), BaseOffset: 0, LastOffsetDelta: 9},
	})
	obj2 := BuildObject([]BatchData{
		{RawBytes: makeMinimalBatch(10, 9), BaseOffset: 10, LastOffsetDelta: 9},
	})
	require.NoError(t, client.PutObject(ctx, ObjectKey("klite/test", "ci-test", tid, 0, 0), obj1))
	require.NoError(t, client.PutObject(ctx, ObjectKey("klite/test", "ci-test", tid, 0, 10), obj2))
	_, err := reader.Fetch(ctx, "ci-test", tid, 0, 0, 1024*1024)
	require.NoError(t, err)

	// Simulate compaction: invalidate footer+listing cache.
	reader.InvalidateFooters("ci-test", tid, 0)

	// Simulate flush: upload new object and try to append.
	obj3 := BuildObject([]BatchData{
		{RawBytes: makeMinimalBatch(20, 9), BaseOffset: 20, LastOffsetDelta: 9},
	})
	key3 := ObjectKey("klite/test", "ci-test", tid, 0, 20)
	require.NoError(t, client.PutObject(ctx, key3, obj3))
	reader.AppendToListing("ci-test", tid, 0, ObjectInfo{
		Key:  key3,
		Size: int64(len(obj3)),
	})

	// Consumer fetch should trigger a fresh LIST and see all 3 objects.
	listsBefore := int(mem.listCalls.Load())
	data, err := reader.FetchBatches(ctx, "ci-test", tid, 0, 0, 1024*1024)
	require.NoError(t, err)
	require.NotEmpty(t, data)
	require.Equal(t, int64(0), data[0].BaseOffset)

	listsAfter := int(mem.listCalls.Load())
	require.Greater(t, listsAfter, listsBefore,
		"should have triggered a fresh LIST after compaction invalidation")

	// Verify we can also reach the flushed object at offset 20.
	data, err = reader.FetchBatches(ctx, "ci-test", tid, 0, 20, 1024*1024)
	require.NoError(t, err)
	require.NotEmpty(t, data)
	require.Equal(t, int64(20), data[0].BaseOffset)
}

// listCountingS3 wraps InMemoryS3 to count ListObjectsV2 calls.
type listCountingS3 struct {
	*InMemoryS3
	listCalls atomic.Int32
}

func newListCountingS3() *listCountingS3 {
	return &listCountingS3{InMemoryS3: NewInMemoryS3()}
}

func (l *listCountingS3) ListObjectsV2(ctx context.Context, input *s3svc.ListObjectsV2Input, opts ...func(*s3svc.Options)) (*s3svc.ListObjectsV2Output, error) {
	l.listCalls.Add(1)
	return l.InMemoryS3.ListObjectsV2(ctx, input, opts...)
}
