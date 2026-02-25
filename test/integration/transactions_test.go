package integration

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// createTopic is a helper that creates a topic with 1 partition via kadm.
func createTopic(t *testing.T, addr, topic string) {
	t.Helper()
	admin := NewAdminClient(t, addr)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := admin.CreateTopics(ctx, 1, 1, nil, topic)
	require.NoError(t, err)
}

func TestInitProducerID(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t)

	cl := NewClient(t, tb.Addr)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Issue raw InitProducerID request
	req := kmsg.NewInitProducerIDRequest()
	resp, err := req.RequestWith(ctx, cl)
	require.NoError(t, err)
	require.Equal(t, int16(0), resp.ErrorCode)
	require.True(t, resp.ProducerID > 0, "expected positive producer ID, got %d", resp.ProducerID)
	require.Equal(t, int16(0), resp.ProducerEpoch)
}

func TestIdempotentProduce(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t)

	topic := "test-idempotent"
	createTopic(t, tb.Addr, topic)

	// Produce with idempotency disabled (basic produce to pre-created topic)
	cl := NewClient(t, tb.Addr,
		kgo.DefaultProduceTopic(topic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.DisableIdempotentWrite(),
	)

	rec := &kgo.Record{Topic: topic, Value: []byte("hello")}
	ProduceSync(t, cl, rec)

	// Verify record is consumable
	consumer := NewClient(t, tb.Addr,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	records := ConsumeN(t, consumer, 1, 5*time.Second)
	require.Equal(t, []byte("hello"), records[0].Value)
}

func TestIdempotentProduceDedup(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t)

	topic := "test-idempotent-dedup"
	createTopic(t, tb.Addr, topic)

	// Use idempotent client that the franz-go library manages
	cl := NewClient(t, tb.Addr,
		kgo.DefaultProduceTopic(topic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)

	// Produce records
	for i := 0; i < 3; i++ {
		rec := &kgo.Record{
			Topic:     topic,
			Partition: 0,
			Value:     []byte("val"),
		}
		ProduceSync(t, cl, rec)
	}

	// Verify we get exactly 3 records (no duplicates)
	consumer := NewClient(t, tb.Addr,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	records := ConsumeN(t, consumer, 3, 5*time.Second)
	require.Len(t, records, 3)
}

func TestTxnProduceCommit(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t)

	topic := "test-txn-commit"
	createTopic(t, tb.Addr, topic)

	// Transactional producer
	cl := NewClient(t, tb.Addr,
		kgo.DefaultProduceTopic(topic),
		kgo.TransactionalID("txn-commit-test"),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := cl.BeginTransaction()
	require.NoError(t, err)

	rec := &kgo.Record{Topic: topic, Value: []byte("txn-data"), Partition: 0}
	results := cl.ProduceSync(ctx, rec)
	require.NoError(t, results.FirstErr())

	err = cl.EndTransaction(ctx, kgo.TryCommit)
	require.NoError(t, err)

	// Read_committed consumer should see the record
	consumer := NewClient(t, tb.Addr,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
	)
	records := ConsumeN(t, consumer, 1, 5*time.Second)
	require.Equal(t, []byte("txn-data"), records[0].Value)
}

func TestTxnProduceAbort(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t)

	topic := "test-txn-abort"
	createTopic(t, tb.Addr, topic)

	// Produce committed record first so we can verify the consumer works
	simpleCl := NewClient(t, tb.Addr,
		kgo.DefaultProduceTopic(topic),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	ProduceSync(t, simpleCl, &kgo.Record{Topic: topic, Value: []byte("committed-first"), Partition: 0})

	// Transactional producer
	txnCl := NewClient(t, tb.Addr,
		kgo.DefaultProduceTopic(topic),
		kgo.TransactionalID("txn-abort-test"),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := txnCl.BeginTransaction()
	require.NoError(t, err)

	rec := &kgo.Record{Topic: topic, Value: []byte("txn-aborted-data"), Partition: 0}
	results := txnCl.ProduceSync(ctx, rec)
	require.NoError(t, results.FirstErr())

	err = txnCl.EndTransaction(ctx, kgo.TryAbort)
	require.NoError(t, err)

	// Read_committed consumer should only see the first committed record
	consumer := NewClient(t, tb.Addr,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
	)
	records := ConsumeN(t, consumer, 1, 5*time.Second)
	require.Equal(t, []byte("committed-first"), records[0].Value)

	// Should not get a second record (the aborted one)
	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	fetches := consumer.PollFetches(ctx2)
	var extraRecords []*kgo.Record
	fetches.EachRecord(func(r *kgo.Record) {
		extraRecords = append(extraRecords, r)
	})
	require.Empty(t, extraRecords, "read_committed consumer should not see aborted records")
}

func TestTxnReadUncommitted(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t)

	topic := "test-txn-read-uncommitted"
	createTopic(t, tb.Addr, topic)

	// Transactional producer
	txnCl := NewClient(t, tb.Addr,
		kgo.DefaultProduceTopic(topic),
		kgo.TransactionalID("txn-uncommitted-test"),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := txnCl.BeginTransaction()
	require.NoError(t, err)

	rec := &kgo.Record{Topic: topic, Value: []byte("uncommitted-data"), Partition: 0}
	results := txnCl.ProduceSync(ctx, rec)
	require.NoError(t, results.FirstErr())

	// Read_uncommitted consumer should see the record even before commit
	consumer := NewClient(t, tb.Addr,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchIsolationLevel(kgo.ReadUncommitted()),
	)
	records := ConsumeN(t, consumer, 1, 5*time.Second)
	require.Equal(t, []byte("uncommitted-data"), records[0].Value)

	// Clean up: commit the transaction
	err = txnCl.EndTransaction(ctx, kgo.TryCommit)
	require.NoError(t, err)
}

func TestProducerFencing(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t)

	topic := "test-producer-fencing"
	txnID := "fencing-test"
	createTopic(t, tb.Addr, topic)

	// First producer with this txnID
	cl1 := NewClient(t, tb.Addr,
		kgo.DefaultProduceTopic(topic),
		kgo.TransactionalID(txnID),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := cl1.BeginTransaction()
	require.NoError(t, err)

	// Second producer with same txnID — fences the first
	cl2 := NewClient(t, tb.Addr,
		kgo.DefaultProduceTopic(topic),
		kgo.TransactionalID(txnID),
	)

	err = cl2.BeginTransaction()
	require.NoError(t, err)

	rec := &kgo.Record{Topic: topic, Value: []byte("from-cl2")}
	results := cl2.ProduceSync(ctx, rec)
	require.NoError(t, results.FirstErr())

	err = cl2.EndTransaction(ctx, kgo.TryCommit)
	require.NoError(t, err)
}

func TestFenceAfterProducerCommit(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t)

	topic := "test-fence-after-commit"
	txnID := "fence-after-commit"
	createTopic(t, tb.Addr, topic)

	cl1 := NewClient(t, tb.Addr,
		kgo.DefaultProduceTopic(topic),
		kgo.TransactionalID(txnID),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := cl1.BeginTransaction()
	require.NoError(t, err)
	results := cl1.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte("data1")})
	require.NoError(t, results.FirstErr())
	err = cl1.EndTransaction(ctx, kgo.TryCommit)
	require.NoError(t, err)

	cl2 := NewClient(t, tb.Addr,
		kgo.DefaultProduceTopic(topic),
		kgo.TransactionalID(txnID),
	)
	err = cl2.BeginTransaction()
	require.NoError(t, err)
	results = cl2.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte("data2")})
	require.NoError(t, results.FirstErr())
	err = cl2.EndTransaction(ctx, kgo.TryCommit)
	require.NoError(t, err)
}

func TestFenceBeforeProducerCommit(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t)

	topic := "test-fence-before-commit"
	txnID := "fence-before-commit"
	createTopic(t, tb.Addr, topic)

	cl1 := NewClient(t, tb.Addr,
		kgo.DefaultProduceTopic(topic),
		kgo.TransactionalID(txnID),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := cl1.BeginTransaction()
	require.NoError(t, err)
	results := cl1.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte("data1")})
	require.NoError(t, results.FirstErr())

	cl2 := NewClient(t, tb.Addr,
		kgo.DefaultProduceTopic(topic),
		kgo.TransactionalID(txnID),
	)
	err = cl2.BeginTransaction()
	require.NoError(t, err)
	results = cl2.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte("data2")})
	require.NoError(t, results.FirstErr())
	err = cl2.EndTransaction(ctx, kgo.TryCommit)
	require.NoError(t, err)
}

func TestTxnOffsetCommit(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t)

	topic := "test-txn-offset-commit"
	createTopic(t, tb.Addr, topic)

	// Produce some records
	producer := NewClient(t, tb.Addr,
		kgo.DefaultProduceTopic(topic),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	for i := 0; i < 5; i++ {
		ProduceSync(t, producer, &kgo.Record{Topic: topic, Partition: 0, Value: []byte("val")})
	}

	// Create a transactional producer
	txnCl := NewClient(t, tb.Addr,
		kgo.DefaultProduceTopic(topic),
		kgo.TransactionalID("txn-offset-commit-test"),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := txnCl.BeginTransaction()
	require.NoError(t, err)

	err = txnCl.EndTransaction(ctx, kgo.TryCommit)
	require.NoError(t, err)
}

func TestDescribeProducersDefaultRoutesToLeader(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t)

	topic := "test-describe-producers"
	createTopic(t, tb.Addr, topic)

	cl := NewClient(t, tb.Addr,
		kgo.DefaultProduceTopic(topic),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	ProduceSync(t, cl, &kgo.Record{Topic: topic, Partition: 0, Value: []byte("data")})

	adminCl := NewClient(t, tb.Addr)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := kmsg.NewDescribeProducersRequest()
	rt := kmsg.NewDescribeProducersRequestTopic()
	rt.Topic = topic
	rt.Partitions = []int32{0}
	req.Topics = append(req.Topics, rt)

	resp, err := req.RequestWith(ctx, adminCl)
	require.NoError(t, err)
	require.Len(t, resp.Topics, 1)
	require.Len(t, resp.Topics[0].Partitions, 1)
	require.Equal(t, int16(0), resp.Topics[0].Partitions[0].ErrorCode)
}

func TestDescribeProducersAfterCommit(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t)

	topic := "test-describe-producers-commit"
	createTopic(t, tb.Addr, topic)

	txnCl := NewClient(t, tb.Addr,
		kgo.DefaultProduceTopic(topic),
		kgo.TransactionalID("describe-producers-txn"),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := txnCl.BeginTransaction()
	require.NoError(t, err)
	results := txnCl.ProduceSync(ctx, &kgo.Record{Topic: topic, Partition: 0, Value: []byte("txn-data")})
	require.NoError(t, results.FirstErr())
	err = txnCl.EndTransaction(ctx, kgo.TryCommit)
	require.NoError(t, err)

	adminCl := NewClient(t, tb.Addr)
	req := kmsg.NewDescribeProducersRequest()
	rt := kmsg.NewDescribeProducersRequestTopic()
	rt.Topic = topic
	rt.Partitions = []int32{0}
	req.Topics = append(req.Topics, rt)

	resp, err := req.RequestWith(ctx, adminCl)
	require.NoError(t, err)
	require.Len(t, resp.Topics, 1)
	require.Len(t, resp.Topics[0].Partitions, 1)
	require.Equal(t, int16(0), resp.Topics[0].Partitions[0].ErrorCode)
}

func TestDescribeTransactions(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t)

	topic := "test-describe-txns"
	txnID := "describe-txns-test"
	createTopic(t, tb.Addr, topic)

	txnCl := NewClient(t, tb.Addr,
		kgo.DefaultProduceTopic(topic),
		kgo.TransactionalID(txnID),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := txnCl.BeginTransaction()
	require.NoError(t, err)
	results := txnCl.ProduceSync(ctx, &kgo.Record{Topic: topic, Partition: 0, Value: []byte("data")})
	require.NoError(t, results.FirstErr())

	// Describe the transaction while it's ongoing
	adminCl := NewClient(t, tb.Addr)
	dreq := kmsg.NewDescribeTransactionsRequest()
	dreq.TransactionalIDs = []string{txnID}
	dresp, err := dreq.RequestWith(ctx, adminCl)
	require.NoError(t, err)
	require.Len(t, dresp.TransactionStates, 1)
	require.Equal(t, int16(0), dresp.TransactionStates[0].ErrorCode)
	require.Equal(t, txnID, dresp.TransactionStates[0].TransactionalID)
	require.Equal(t, "Ongoing", dresp.TransactionStates[0].State)

	require.NotEmpty(t, dresp.TransactionStates[0].Topics, "should have topic-partitions in ongoing txn")

	// Clean up
	err = txnCl.EndTransaction(ctx, kgo.TryCommit)
	require.NoError(t, err)
}

func TestDescribeTransactionsNotFound(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t)

	adminCl := NewClient(t, tb.Addr)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := kmsg.NewDescribeTransactionsRequest()
	req.TransactionalIDs = []string{"nonexistent-txn-id"}
	resp, err := req.RequestWith(ctx, adminCl)
	require.NoError(t, err)
	require.Len(t, resp.TransactionStates, 1)
	require.Equal(t, int16(79), resp.TransactionStates[0].ErrorCode) // TRANSACTIONAL_ID_NOT_FOUND
}

func TestListTransactions(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t)

	topic := "test-list-txns"
	createTopic(t, tb.Addr, topic)

	txnCl1 := NewClient(t, tb.Addr,
		kgo.DefaultProduceTopic(topic),
		kgo.TransactionalID("list-txns-1"),
	)
	txnCl2 := NewClient(t, tb.Addr,
		kgo.DefaultProduceTopic(topic),
		kgo.TransactionalID("list-txns-2"),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := txnCl1.BeginTransaction()
	require.NoError(t, err)
	results := txnCl1.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte("data1")})
	require.NoError(t, results.FirstErr())

	err = txnCl2.BeginTransaction()
	require.NoError(t, err)
	results = txnCl2.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte("data2")})
	require.NoError(t, results.FirstErr())

	adminCl := NewClient(t, tb.Addr)
	req := kmsg.NewListTransactionsRequest()
	resp, err := req.RequestWith(ctx, adminCl)
	require.NoError(t, err)
	require.Equal(t, int16(0), resp.ErrorCode)
	require.GreaterOrEqual(t, len(resp.TransactionStates), 2, "should list at least 2 transactions")

	found := make(map[string]bool)
	for _, ts := range resp.TransactionStates {
		found[ts.TransactionalID] = true
	}
	require.True(t, found["list-txns-1"], "should find list-txns-1")
	require.True(t, found["list-txns-2"], "should find list-txns-2")

	// Clean up
	err = txnCl1.EndTransaction(ctx, kgo.TryCommit)
	require.NoError(t, err)
	err = txnCl2.EndTransaction(ctx, kgo.TryCommit)
	require.NoError(t, err)
}

func TestListTransactionsFilterByState(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t)

	topic := "test-list-txns-filter"
	createTopic(t, tb.Addr, topic)

	txnCl := NewClient(t, tb.Addr,
		kgo.DefaultProduceTopic(topic),
		kgo.TransactionalID("filter-txn-1"),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := txnCl.BeginTransaction()
	require.NoError(t, err)
	results := txnCl.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte("data")})
	require.NoError(t, results.FirstErr())

	adminCl := NewClient(t, tb.Addr)
	req := kmsg.NewListTransactionsRequest()
	req.StateFilters = []string{"Ongoing"}
	resp, err := req.RequestWith(ctx, adminCl)
	require.NoError(t, err)

	foundOngoing := false
	for _, ts := range resp.TransactionStates {
		require.Equal(t, "Ongoing", ts.TransactionState, "filter should only return Ongoing")
		if ts.TransactionalID == "filter-txn-1" {
			foundOngoing = true
		}
	}
	require.True(t, foundOngoing, "should find filter-txn-1 in Ongoing state")

	// List only "Empty" — should NOT include our ongoing txn
	req2 := kmsg.NewListTransactionsRequest()
	req2.StateFilters = []string{"Empty"}
	resp2, err := req2.RequestWith(ctx, adminCl)
	require.NoError(t, err)

	for _, ts := range resp2.TransactionStates {
		require.NotEqual(t, "filter-txn-1", ts.TransactionalID,
			"Ongoing txn should not appear in Empty filter")
	}

	// Clean up
	err = txnCl.EndTransaction(ctx, kgo.TryCommit)
	require.NoError(t, err)
}

// TestIdempotentOutOfOrder tests that sequential idempotent production works correctly.
func TestIdempotentOutOfOrder(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t)

	topic := "test-idempotent-ooo"

	cl := NewClient(t, tb.Addr)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Get a producer ID
	initReq := kmsg.NewInitProducerIDRequest()
	initResp, err := initReq.RequestWith(ctx, cl)
	require.NoError(t, err)
	require.Equal(t, int16(0), initResp.ErrorCode)

	admin := kadm.NewClient(cl)

	// Create the topic
	_, err = admin.CreateTopics(ctx, 1, 1, nil, topic)
	require.NoError(t, err)

	// Verify that normal idempotent production works correctly
	idempotentCl := NewClient(t, tb.Addr,
		kgo.DefaultProduceTopic(topic),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)

	for i := 0; i < 5; i++ {
		ProduceSync(t, idempotentCl, &kgo.Record{
			Topic:     topic,
			Partition: 0,
			Value:     []byte("data"),
		})
	}

	consumer := NewClient(t, tb.Addr,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	records := ConsumeN(t, consumer, 5, 5*time.Second)
	require.Len(t, records, 5)

	_ = admin
	_ = kerr.OutOfOrderSequenceNumber
}

// TestIdempotentProduceConcurrent fires many records concurrently using
// async Produce (not ProduceSync) with idempotent writes enabled. This
// exercises the code path where multiple produce requests are in-flight
// on the same connection simultaneously — the scenario that triggered
// the buffer-reuse CORRUPT_MESSAGE bug.
func TestIdempotentProduceConcurrent(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t)

	topic := "test-idempotent-concurrent"
	createTopic(t, tb.Addr, topic)

	const numRecords = 2000

	cl := NewClient(t, tb.Addr,
		kgo.DefaultProduceTopic(topic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		// Idempotent writes are enabled by default with AllISRAcks()
		kgo.MaxBufferedRecords(numRecords),
		kgo.ProducerBatchMaxBytes(16384), // small batches → more concurrent requests
		kgo.ProducerLinger(time.Millisecond),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.RequestRetries(3),
		kgo.RetryBackoffFn(func(int) time.Duration { return 50 * time.Millisecond }),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var errCount int64
	payload := make([]byte, 200)
	for i := 0; i < numRecords; i++ {
		rec := &kgo.Record{Partition: 0, Value: payload}
		cl.Produce(ctx, rec, func(_ *kgo.Record, err error) {
			if err != nil {
				errCount++
			}
		})
	}

	require.NoError(t, cl.Flush(ctx))
	require.Zero(t, errCount, "expected zero produce errors with idempotent writes")

	// Consume and verify all records arrived
	consumer := NewClient(t, tb.Addr,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	records := ConsumeN(t, consumer, numRecords, 10*time.Second)
	require.Len(t, records, numRecords)
}
