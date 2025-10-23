package integration

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestProduceSingleRecord(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t)

	// Create topic first
	admin := NewAdminClient(t, tb.Addr)
	_, err := admin.CreateTopic(context.Background(), 1, 1, nil, "test-produce-single")
	require.NoError(t, err)

	cl := NewClient(t, tb.Addr,
		kgo.DefaultProduceTopic("test-produce-single"),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)

	rec := &kgo.Record{
		Topic:     "test-produce-single",
		Partition: 0,
		Key:       []byte("key-0"),
		Value:     []byte("value-0"),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	results := cl.ProduceSync(ctx, rec)
	require.Len(t, results, 1)
	require.NoError(t, results[0].Err)
	require.Equal(t, int64(0), results[0].Record.Offset, "first record should have offset 0")
	require.Equal(t, int32(0), results[0].Record.Partition)
}

func TestProduceBatch(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t)

	admin := NewAdminClient(t, tb.Addr)
	_, err := admin.CreateTopic(context.Background(), 1, 1, nil, "test-produce-batch")
	require.NoError(t, err)

	cl := NewClient(t, tb.Addr,
		kgo.DefaultProduceTopic("test-produce-batch"),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)

	records := make([]*kgo.Record, 10)
	for i := range records {
		records[i] = &kgo.Record{
			Topic:     "test-produce-batch",
			Partition: 0,
			Key:       []byte("key"),
			Value:     []byte("value"),
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	results := cl.ProduceSync(ctx, records...)
	require.Len(t, results, 10)

	for i, r := range results {
		require.NoError(t, r.Err, "record %d", i)
		// All records in a single batch get the same base offset,
		// but the per-record offsets should be sequential 0-9.
		require.Equal(t, int64(i), r.Record.Offset, "record %d offset", i)
	}
}

func TestProduceMultiplePartitions(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t)

	admin := NewAdminClient(t, tb.Addr)
	_, err := admin.CreateTopic(context.Background(), 2, 1, nil, "test-produce-multi-part")
	require.NoError(t, err)

	cl := NewClient(t, tb.Addr,
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)

	// Produce to partition 0
	rec0 := &kgo.Record{
		Topic:     "test-produce-multi-part",
		Partition: 0,
		Value:     []byte("p0-value"),
	}
	// Produce to partition 1
	rec1 := &kgo.Record{
		Topic:     "test-produce-multi-part",
		Partition: 1,
		Value:     []byte("p1-value"),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	results := cl.ProduceSync(ctx, rec0, rec1)
	require.Len(t, results, 2)

	for _, r := range results {
		require.NoError(t, r.Err)
		require.Equal(t, int64(0), r.Record.Offset, "first record in each partition should be offset 0")
	}

	// Verify we got records on both partitions
	partitions := map[int32]bool{}
	for _, r := range results {
		partitions[r.Record.Partition] = true
	}
	require.True(t, partitions[0], "should have produced to partition 0")
	require.True(t, partitions[1], "should have produced to partition 1")
}

func TestProduceUnknownTopic(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t, WithAutoCreateTopics(false))

	cl := NewClient(t, tb.Addr,
		kgo.DefaultProduceTopic("nonexistent-topic"),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.MaxBufferedRecords(1),
		kgo.ProducerBatchMaxBytes(1024),
	)

	rec := &kgo.Record{
		Topic:     "nonexistent-topic",
		Partition: 0,
		Value:     []byte("value"),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	results := cl.ProduceSync(ctx, rec)
	require.Len(t, results, 1)
	require.Error(t, results[0].Err, "should fail for unknown topic with auto-create disabled")
}

func TestProduceAutoCreate(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t, WithAutoCreateTopics(true))

	cl := NewClient(t, tb.Addr,
		kgo.DefaultProduceTopic("auto-created-topic"),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.AllowAutoTopicCreation(),
	)

	rec := &kgo.Record{
		Topic:     "auto-created-topic",
		Partition: 0,
		Value:     []byte("value"),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	results := cl.ProduceSync(ctx, rec)
	require.Len(t, results, 1)
	require.NoError(t, results[0].Err, "auto-create should allow produce to new topic")
	require.Equal(t, int64(0), results[0].Record.Offset)
}

func TestProduceConsumeOrdering(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t)

	admin := NewAdminClient(t, tb.Addr)
	_, err := admin.CreateTopic(context.Background(), 1, 1, nil, "test-ordering")
	require.NoError(t, err)

	// Produce records A, B, C
	producer := NewClient(t, tb.Addr,
		kgo.DefaultProduceTopic("test-ordering"),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)

	records := []*kgo.Record{
		{Topic: "test-ordering", Partition: 0, Value: []byte("A")},
		{Topic: "test-ordering", Partition: 0, Value: []byte("B")},
		{Topic: "test-ordering", Partition: 0, Value: []byte("C")},
	}
	ProduceSync(t, producer, records...)

	// Consume and verify order
	consumer := NewClient(t, tb.Addr,
		kgo.ConsumeTopics("test-ordering"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)

	consumed := ConsumeN(t, consumer, 3, 10*time.Second)
	require.Len(t, consumed, 3)
	require.Equal(t, "A", string(consumed[0].Value))
	require.Equal(t, "B", string(consumed[1].Value))
	require.Equal(t, "C", string(consumed[2].Value))

	// Verify offsets are sequential
	require.Equal(t, int64(0), consumed[0].Offset)
	require.Equal(t, int64(1), consumed[1].Offset)
	require.Equal(t, int64(2), consumed[2].Offset)
}

func TestProduceConsumeCompression(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t)

	admin := NewAdminClient(t, tb.Addr)
	_, err := admin.CreateTopic(context.Background(), 1, 1, nil, "test-compression")
	require.NoError(t, err)

	// Produce with gzip compression
	producer := NewClient(t, tb.Addr,
		kgo.DefaultProduceTopic("test-compression"),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.ProducerBatchCompression(kgo.GzipCompression()),
	)

	records := []*kgo.Record{
		{Topic: "test-compression", Partition: 0, Key: []byte("k1"), Value: []byte("compressed-value-1")},
		{Topic: "test-compression", Partition: 0, Key: []byte("k2"), Value: []byte("compressed-value-2")},
		{Topic: "test-compression", Partition: 0, Key: []byte("k3"), Value: []byte("compressed-value-3")},
	}
	ProduceSync(t, producer, records...)

	// Consume and verify values are decompressed correctly
	consumer := NewClient(t, tb.Addr,
		kgo.ConsumeTopics("test-compression"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)

	consumed := ConsumeN(t, consumer, 3, 10*time.Second)
	require.Len(t, consumed, 3)
	require.Equal(t, "compressed-value-1", string(consumed[0].Value))
	require.Equal(t, "compressed-value-2", string(consumed[1].Value))
	require.Equal(t, "compressed-value-3", string(consumed[2].Value))
}
