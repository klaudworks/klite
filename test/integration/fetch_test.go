package integration

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestFetchFromStart(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t)

	admin := NewAdminClient(t, tb.Addr)
	_, err := admin.CreateTopic(context.Background(), 1, 1, nil, "test-fetch-from-start")
	require.NoError(t, err)

	// Produce N records
	producer := NewClient(t, tb.Addr,
		kgo.DefaultProduceTopic("test-fetch-from-start"),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	n := 10
	ProduceN(t, producer, "test-fetch-from-start", n)

	// Consume from start, verify all N
	consumer := NewClient(t, tb.Addr,
		kgo.ConsumeTopics("test-fetch-from-start"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)

	consumed := ConsumeN(t, consumer, n, 10*time.Second)
	require.Len(t, consumed, n)

	for i, r := range consumed {
		require.Equal(t, int64(i), r.Offset, "record %d offset", i)
	}
}

func TestFetchFromMiddle(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t)

	admin := NewAdminClient(t, tb.Addr)
	_, err := admin.CreateTopic(context.Background(), 1, 1, nil, "test-fetch-from-middle")
	require.NoError(t, err)

	// Produce 10 records
	producer := NewClient(t, tb.Addr,
		kgo.DefaultProduceTopic("test-fetch-from-middle"),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	ProduceN(t, producer, "test-fetch-from-middle", 10)

	// Consume from offset 5
	consumer := NewClient(t, tb.Addr,
		kgo.ConsumeTopics("test-fetch-from-middle"),
		kgo.ConsumeResetOffset(kgo.NewOffset().At(5)),
	)

	consumed := ConsumeN(t, consumer, 5, 10*time.Second)
	require.Len(t, consumed, 5)

	// Offsets should be 5,6,7,8,9
	for i, r := range consumed {
		require.Equal(t, int64(5+i), r.Offset, "record %d offset", i)
	}
}

func TestFetchEmpty(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t)

	admin := NewAdminClient(t, tb.Addr)
	_, err := admin.CreateTopic(context.Background(), 1, 1, nil, "test-fetch-empty")
	require.NoError(t, err)

	// Consume from empty partition — should return quickly with no records
	// Use MaxWaitMs=0 effectively by setting short timeout
	consumer := NewClient(t, tb.Addr,
		kgo.ConsumeTopics("test-fetch-empty"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(500*time.Millisecond),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	fetches := consumer.PollFetches(ctx)
	// Should get an empty fetch (no error, no records) or context timeout
	var records []*kgo.Record
	fetches.EachRecord(func(r *kgo.Record) {
		records = append(records, r)
	})
	require.Empty(t, records, "empty partition should return no records")
}

func TestFetchOutOfRange(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t)

	admin := NewAdminClient(t, tb.Addr)
	_, err := admin.CreateTopic(context.Background(), 1, 1, nil, "test-fetch-oor")
	require.NoError(t, err)

	// Produce 5 records (offsets 0-4, HW=5)
	producer := NewClient(t, tb.Addr,
		kgo.DefaultProduceTopic("test-fetch-oor"),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	ProduceN(t, producer, "test-fetch-oor", 5)

	// Try to consume starting at offset 100 (beyond HW).
	// franz-go will receive OFFSET_OUT_OF_RANGE and reset to ConsumeResetOffset.
	// We set ConsumeResetOffset to AtStart, so after the error the consumer
	// should read from offset 0.
	consumer := NewClient(t, tb.Addr,
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			"test-fetch-oor": {
				0: kgo.NewOffset().At(100),
			},
		}),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)

	// After the reset, we should get all 5 records from start
	consumed := ConsumeN(t, consumer, 5, 10*time.Second)
	require.Len(t, consumed, 5)
	require.Equal(t, int64(0), consumed[0].Offset)
}

func TestFetchMaxBytes(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t)

	admin := NewAdminClient(t, tb.Addr)
	_, err := admin.CreateTopic(context.Background(), 1, 1, nil, "test-fetch-maxbytes")
	require.NoError(t, err)

	// Produce records with known size
	producer := NewClient(t, tb.Addr,
		kgo.DefaultProduceTopic("test-fetch-maxbytes"),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	for i := 0; i < 20; i++ {
		rec := &kgo.Record{
			Topic:     "test-fetch-maxbytes",
			Partition: 0,
			Value:     []byte(strings.Repeat("x", 1000)),
		}
		ProduceSync(t, producer, rec)
	}

	// Consume with small max bytes to test size limit enforcement.
	// Even with small FetchMaxBytes, we should still be able to consume all records
	// (KIP-74: first batch always returned).
	consumer := NewClient(t, tb.Addr,
		kgo.ConsumeTopics("test-fetch-maxbytes"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxBytes(1024), // small limit
	)

	consumed := ConsumeN(t, consumer, 20, 15*time.Second)
	require.Len(t, consumed, 20)
}

func TestFetchLargeRecord(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t)

	admin := NewAdminClient(t, tb.Addr)
	_, err := admin.CreateTopic(context.Background(), 1, 1, nil, "test-fetch-large")
	require.NoError(t, err)

	// Produce a single large record
	producer := NewClient(t, tb.Addr,
		kgo.DefaultProduceTopic("test-fetch-large"),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.ProducerBatchMaxBytes(2*1024*1024), // allow large batches
	)
	largeValue := []byte(strings.Repeat("A", 100*1024)) // 100KB
	rec := &kgo.Record{
		Topic:     "test-fetch-large",
		Partition: 0,
		Value:     largeValue,
	}
	ProduceSync(t, producer, rec)

	// Consume with FetchMaxPartitionBytes smaller than the record.
	// KIP-74: the first batch should still be returned even if it exceeds the limit.
	consumer := NewClient(t, tb.Addr,
		kgo.ConsumeTopics("test-fetch-large"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxPartitionBytes(1024), // much smaller than the record
	)

	consumed := ConsumeN(t, consumer, 1, 10*time.Second)
	require.Len(t, consumed, 1)
	require.Equal(t, largeValue, consumed[0].Value)
}

func TestFetchRecordLargerThanFetchMaxBytes(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t)

	admin := NewAdminClient(t, tb.Addr)
	_, err := admin.CreateTopic(context.Background(), 1, 1, nil, "test-fetch-larger-than-max")
	require.NoError(t, err)

	// Produce a large record
	producer := NewClient(t, tb.Addr,
		kgo.DefaultProduceTopic("test-fetch-larger-than-max"),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.ProducerBatchMaxBytes(2*1024*1024),
	)
	bigValue := []byte(strings.Repeat("B", 50*1024)) // 50KB
	rec := &kgo.Record{
		Topic:     "test-fetch-larger-than-max",
		Partition: 0,
		Value:     bigValue,
	}
	ProduceSync(t, producer, rec)

	// Consume with response-level FetchMaxBytes smaller than the batch.
	// KIP-74: the first batch in the first non-empty partition is always returned.
	consumer := NewClient(t, tb.Addr,
		kgo.ConsumeTopics("test-fetch-larger-than-max"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxBytes(1024),
		kgo.FetchMaxPartitionBytes(1024),
	)

	consumed := ConsumeN(t, consumer, 1, 10*time.Second)
	require.Len(t, consumed, 1)
	require.Equal(t, bigValue, consumed[0].Value)
}

func TestCompression(t *testing.T) {
	t.Parallel()

	compressions := []struct {
		name string
		codec kgo.CompressionCodec
	}{
		{"gzip", kgo.GzipCompression()},
		{"snappy", kgo.SnappyCompression()},
		{"lz4", kgo.Lz4Compression()},
		{"zstd", kgo.ZstdCompression()},
	}

	for _, tc := range compressions {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tb := StartBroker(t)

			topicName := "test-compression-" + tc.name
			admin := NewAdminClient(t, tb.Addr)
			_, err := admin.CreateTopic(context.Background(), 1, 1, nil, topicName)
			require.NoError(t, err)

			// Produce with compression
			producer := NewClient(t, tb.Addr,
				kgo.DefaultProduceTopic(topicName),
				kgo.RecordPartitioner(kgo.ManualPartitioner()),
				kgo.ProducerBatchCompression(tc.codec),
			)

			records := []*kgo.Record{
				{Topic: topicName, Partition: 0, Key: []byte("k1"), Value: []byte("compressed-value-1")},
				{Topic: topicName, Partition: 0, Key: []byte("k2"), Value: []byte("compressed-value-2")},
				{Topic: topicName, Partition: 0, Key: []byte("k3"), Value: []byte("compressed-value-3")},
			}
			ProduceSync(t, producer, records...)

			// Consume and verify values are decompressed correctly
			consumer := NewClient(t, tb.Addr,
				kgo.ConsumeTopics(topicName),
				kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
			)

			consumed := ConsumeN(t, consumer, 3, 10*time.Second)
			require.Len(t, consumed, 3)
			require.Equal(t, "compressed-value-1", string(consumed[0].Value))
			require.Equal(t, "compressed-value-2", string(consumed[1].Value))
			require.Equal(t, "compressed-value-3", string(consumed[2].Value))
		})
	}
}

func TestAssignAndConsume(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t)

	admin := NewAdminClient(t, tb.Addr)
	_, err := admin.CreateTopic(context.Background(), 3, 1, nil, "test-assign-consume")
	require.NoError(t, err)

	// Produce to specific partitions
	producer := NewClient(t, tb.Addr,
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)

	for p := int32(0); p < 3; p++ {
		for i := 0; i < 5; i++ {
			rec := &kgo.Record{
				Topic:     "test-assign-consume",
				Partition: p,
				Value:     []byte("value"),
			}
			ProduceSync(t, producer, rec)
		}
	}

	// Consume from only partition 1 using direct assignment
	consumer := NewClient(t, tb.Addr,
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			"test-assign-consume": {
				1: kgo.NewOffset().AtStart(),
			},
		}),
	)

	consumed := ConsumeN(t, consumer, 5, 10*time.Second)
	require.Len(t, consumed, 5)
	for _, r := range consumed {
		require.Equal(t, int32(1), r.Partition, "should only consume from partition 1")
	}
}

func TestMaxPollRecords(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t)

	admin := NewAdminClient(t, tb.Addr)
	_, err := admin.CreateTopic(context.Background(), 1, 1, nil, "test-max-poll")
	require.NoError(t, err)

	// Produce 20 records
	producer := NewClient(t, tb.Addr,
		kgo.DefaultProduceTopic("test-max-poll"),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	ProduceN(t, producer, "test-max-poll", 20)

	// Consume with PollRecords limiting to 5 per poll
	consumer := NewClient(t, tb.Addr,
		kgo.ConsumeTopics("test-max-poll"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// PollRecords returns at most N records
	fetches := consumer.PollRecords(ctx, 5)
	var records []*kgo.Record
	fetches.EachRecord(func(r *kgo.Record) {
		records = append(records, r)
	})
	require.LessOrEqual(t, len(records), 5, "PollRecords(5) should return at most 5 records")
	require.Greater(t, len(records), 0, "should get some records")
}

func TestThreeRecordsInOneBatch(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t)

	admin := NewAdminClient(t, tb.Addr)
	_, err := admin.CreateTopic(context.Background(), 1, 1, nil, "test-three-in-batch")
	require.NoError(t, err)

	// Produce 3 records that will be batched together
	producer := NewClient(t, tb.Addr,
		kgo.DefaultProduceTopic("test-three-in-batch"),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.MaxBufferedRecords(100),
		kgo.ProducerLinger(100*time.Millisecond), // allow batching
	)

	records := []*kgo.Record{
		{Topic: "test-three-in-batch", Partition: 0, Key: []byte("a"), Value: []byte("val-a")},
		{Topic: "test-three-in-batch", Partition: 0, Key: []byte("b"), Value: []byte("val-b")},
		{Topic: "test-three-in-batch", Partition: 0, Key: []byte("c"), Value: []byte("val-c")},
	}
	ProduceSync(t, producer, records...)

	// Consume and verify all 3
	consumer := NewClient(t, tb.Addr,
		kgo.ConsumeTopics("test-three-in-batch"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)

	consumed := ConsumeN(t, consumer, 3, 10*time.Second)
	require.Len(t, consumed, 3)
	require.Equal(t, "val-a", string(consumed[0].Value))
	require.Equal(t, "val-b", string(consumed[1].Value))
	require.Equal(t, "val-c", string(consumed[2].Value))

	// Verify sequential offsets
	require.Equal(t, int64(0), consumed[0].Offset)
	require.Equal(t, int64(1), consumed[1].Offset)
	require.Equal(t, int64(2), consumed[2].Offset)
}
