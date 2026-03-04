package integration

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestListOffsetsEarliest(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t)

	admin := NewAdminClient(t, tb.Addr)
	ctx := context.Background()

	_, err := admin.CreateTopic(ctx, 1, 1, nil, "test-lo-earliest")
	require.NoError(t, err)

	// On a fresh partition, earliest should be 0
	offsets, err := admin.ListStartOffsets(ctx, "test-lo-earliest")
	require.NoError(t, err)

	lo, ok := offsets.Lookup("test-lo-earliest", 0)
	require.True(t, ok)
	require.NoError(t, lo.Err)
	require.Equal(t, int64(0), lo.Offset)
}

func TestListOffsetsLatest(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t)

	admin := NewAdminClient(t, tb.Addr)
	ctx := context.Background()

	_, err := admin.CreateTopic(ctx, 1, 1, nil, "test-lo-latest")
	require.NoError(t, err)

	// Produce 5 records
	producer := NewClient(t, tb.Addr,
		kgo.DefaultProduceTopic("test-lo-latest"),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	ProduceN(t, producer, "test-lo-latest", 5)

	// Latest should be 5 (HW = next offset to be written)
	offsets, err := admin.ListEndOffsets(ctx, "test-lo-latest")
	require.NoError(t, err)

	lo, ok := offsets.Lookup("test-lo-latest", 0)
	require.True(t, ok)
	require.NoError(t, lo.Err)
	require.Equal(t, int64(5), lo.Offset)
}

func TestListOffsetsMaxTimestamp(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t)

	admin := NewAdminClient(t, tb.Addr)
	ctx := context.Background()

	_, err := admin.CreateTopic(ctx, 1, 1, nil, "test-lo-maxts")
	require.NoError(t, err)

	// Produce records with known timestamps. Since we use CreateTime, the
	// client-set timestamp is used. Produce each record individually so
	// each becomes its own batch.
	producer := NewClient(t, tb.Addr,
		kgo.DefaultProduceTopic("test-lo-maxts"),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.ProducerLinger(0), // no batching — each record in its own batch
	)

	records := []*kgo.Record{
		{Topic: "test-lo-maxts", Partition: 0, Value: []byte("v1"), Timestamp: time.UnixMilli(1000)},
		{Topic: "test-lo-maxts", Partition: 0, Value: []byte("v2"), Timestamp: time.UnixMilli(5000)},
		{Topic: "test-lo-maxts", Partition: 0, Value: []byte("v3"), Timestamp: time.UnixMilli(3000)},
	}
	for _, r := range records {
		ProduceSync(t, producer, r)
	}

	// MaxTimestamp should return offset of the batch with ts=5000
	// That's the second batch (offset 1).
	offsets, err := admin.ListMaxTimestampOffsets(ctx, "test-lo-maxts")
	require.NoError(t, err)

	lo, ok := offsets.Lookup("test-lo-maxts", 0)
	require.True(t, ok)
	require.NoError(t, lo.Err)
	require.Equal(t, int64(1), lo.Offset, "should return offset of batch with max timestamp")
	require.Equal(t, int64(5000), lo.Timestamp, "should return the max timestamp")
}

func TestListOffsetsTimestampLookup(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t)

	admin := NewAdminClient(t, tb.Addr)
	ctx := context.Background()

	_, err := admin.CreateTopic(ctx, 1, 1, nil, "test-lo-tslookup")
	require.NoError(t, err)

	// Produce records with ascending timestamps, each in its own batch
	producer := NewClient(t, tb.Addr,
		kgo.DefaultProduceTopic("test-lo-tslookup"),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.ProducerLinger(0),
	)

	for i := 0; i < 5; i++ {
		r := &kgo.Record{
			Topic:     "test-lo-tslookup",
			Partition: 0,
			Value:     []byte("v"),
			Timestamp: time.UnixMilli(int64(1000 + i*1000)), // 1000, 2000, 3000, 4000, 5000
		}
		ProduceSync(t, producer, r)
	}

	// Look up timestamp 2500 — should find the first batch with MaxTimestamp >= 2500
	// That's the batch at offset 2 with ts=3000.
	offsets, err := admin.ListOffsetsAfterMilli(ctx, 2500, "test-lo-tslookup")
	require.NoError(t, err)

	lo, ok := offsets.Lookup("test-lo-tslookup", 0)
	require.True(t, ok)
	require.NoError(t, lo.Err)
	require.Equal(t, int64(2), lo.Offset, "should return offset of first batch at or after ts 2500")
	require.Equal(t, int64(3000), lo.Timestamp)
}

func TestListOffsetsUnknownTopic(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t, WithAutoCreateTopics(false))

	admin := NewAdminClient(t, tb.Addr)
	ctx := context.Background()

	// List offsets for a topic that doesn't exist
	offsets, err := admin.ListEndOffsets(ctx, "nonexistent-topic")
	// kadm wraps unknown topics as an error in the response map
	// with a special -1 partition entry
	if err != nil {
		// If there's a top-level error, that's ok too
		return
	}

	// The topic should either not be present or have an error
	lo, ok := offsets.Lookup("nonexistent-topic", -1)
	if ok {
		require.ErrorIs(t, lo.Err, kerr.UnknownTopicOrPartition)
	}
}

func TestListOffsetsMaxTimestampEmptyLog(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t)

	admin := NewAdminClient(t, tb.Addr)
	ctx := context.Background()

	_, err := admin.CreateTopic(ctx, 1, 1, nil, "test-lo-maxts-empty")
	require.NoError(t, err)

	// MaxTimestamp on empty partition: the broker returns offset=-1 (no batches).
	// kadm sees offset=-1 with no error and re-requests with timestamp=-1 (Latest),
	// which returns HW=0. So the final kadm result is offset=0.
	offsets, err := admin.ListMaxTimestampOffsets(ctx, "test-lo-maxts-empty")
	require.NoError(t, err)

	lo, ok := offsets.Lookup("test-lo-maxts-empty", 0)
	require.True(t, ok)
	require.NoError(t, lo.Err)
	require.Equal(t, int64(0), lo.Offset, "empty partition: kadm falls back to Latest (HW=0)")
}

func TestListOffsetsTimestampLookupNotFound(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t)

	admin := NewAdminClient(t, tb.Addr)
	ctx := context.Background()

	_, err := admin.CreateTopic(ctx, 1, 1, nil, "test-lo-ts-notfound")
	require.NoError(t, err)

	// Produce records with timestamps 1000, 2000
	producer := NewClient(t, tb.Addr,
		kgo.DefaultProduceTopic("test-lo-ts-notfound"),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.ProducerLinger(0),
	)
	for i := 0; i < 2; i++ {
		r := &kgo.Record{
			Topic:     "test-lo-ts-notfound",
			Partition: 0,
			Value:     []byte("v"),
			Timestamp: time.UnixMilli(int64(1000 + i*1000)),
		}
		ProduceSync(t, producer, r)
	}

	// Look up timestamp 5000 — beyond all records. Broker returns offset=-1.
	// kadm sees offset=-1 with no error and re-requests with timestamp=-1
	// (Latest), which returns HW=2 (two records produced). This matches kadm's
	// documented behavior: "If a partition has no offsets after the requested
	// millisecond, the offset will be the current end offset."
	offsets, err := admin.ListOffsetsAfterMilli(ctx, 5000, "test-lo-ts-notfound")
	require.NoError(t, err)

	lo, ok := offsets.Lookup("test-lo-ts-notfound", 0)
	require.True(t, ok)
	require.NoError(t, lo.Err)
	require.Equal(t, int64(2), lo.Offset, "kadm falls back to Latest (HW=2) when no match")
}

func TestListOffsetsEarliestAfterProduce(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t)

	admin := NewAdminClient(t, tb.Addr)
	ctx := context.Background()

	_, err := admin.CreateTopic(ctx, 1, 1, nil, "test-lo-earliest-produce")
	require.NoError(t, err)

	// Produce some records
	producer := NewClient(t, tb.Addr,
		kgo.DefaultProduceTopic("test-lo-earliest-produce"),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	ProduceN(t, producer, "test-lo-earliest-produce", 10)

	// Earliest should still be 0 (no truncation/deletion)
	offsets, err := admin.ListStartOffsets(ctx, "test-lo-earliest-produce")
	require.NoError(t, err)

	lo, ok := offsets.Lookup("test-lo-earliest-produce", 0)
	require.True(t, ok)
	require.NoError(t, lo.Err)
	require.Equal(t, int64(0), lo.Offset, "earliest should still be 0")
}
