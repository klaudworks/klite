package integration

import (
	"context"
	"encoding/binary"
	"hash/crc32"
	"sync/atomic"
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

	cl := NewClient(t, tb.Addr)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Allocate a producer ID.
	initReq := kmsg.NewInitProducerIDRequest()
	initResp, err := initReq.RequestWith(ctx, cl)
	require.NoError(t, err)
	require.Equal(t, int16(0), initResp.ErrorCode)
	pid := initResp.ProducerID
	epoch := initResp.ProducerEpoch

	// Send a batch: PID, epoch, baseSeq=0, 3 records → offsets 0-2.
	resp1 := rawProduce(t, cl, topic, 0, pid, epoch, 0, 3)
	require.Equal(t, int16(0), resp1.ErrorCode)
	require.Equal(t, int64(0), resp1.BaseOffset)

	// Replay the exact same batch (same PID/epoch/baseSeq) — should be deduped.
	resp2 := rawProduce(t, cl, topic, 0, pid, epoch, 0, 3)
	require.Equal(t, int16(0), resp2.ErrorCode, "duplicate batch should not error")
	require.Equal(t, int64(0), resp2.BaseOffset, "duplicate batch should return original base offset")

	// Consume: only 3 records should exist, not 6.
	consumer := NewClient(t, tb.Addr,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	records := ConsumeN(t, consumer, 3, 5*time.Second)
	require.Len(t, records, 3)

	// Verify no extra records beyond the 3.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel2()
	fetches := consumer.PollFetches(ctx2)
	var extra []*kgo.Record
	fetches.EachRecord(func(r *kgo.Record) { extra = append(extra, r) })
	require.Empty(t, extra, "dedup should prevent duplicate records")
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

	cl := NewClient(t, tb.Addr)

	// First producer: allocate PID with epoch 0.
	pid, epoch1 := initProducerIDRaw(t, cl, txnID)

	// Produce a batch with the first epoch to establish sequence state.
	resp1 := rawProduce(t, cl, topic, 0, pid, epoch1, 0, 1)
	require.Equal(t, int16(0), resp1.ErrorCode, "first producer should succeed")

	// Second producer with same txnID: bumps epoch, fencing the first.
	_, epoch2 := initProducerIDRaw(t, cl, txnID)
	require.Greater(t, epoch2, epoch1, "second init should bump epoch")

	// Produce with the new epoch to prove it works.
	resp2 := rawProduce(t, cl, topic, 0, pid, epoch2, 0, 1)
	require.Equal(t, int16(0), resp2.ErrorCode, "new epoch should succeed")

	// Attempt to produce with the OLD (fenced) epoch — should be rejected.
	resp3 := rawProduce(t, cl, topic, 0, pid, epoch1, 1, 1)
	require.Equal(t, int16(35), resp3.ErrorCode,
		"old epoch should be fenced")
}

func TestFenceAfterProducerCommit(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t)

	topic := "test-fence-after-commit"
	txnID := "fence-after-commit"
	createTopic(t, tb.Addr, topic)

	cl := NewClient(t, tb.Addr)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// First producer: allocate PID, produce within a transaction, and commit.
	pid, epoch1 := initProducerIDRaw(t, cl, txnID)
	addPartitionsToTxnRaw(t, cl, ctx, txnID, pid, epoch1, topic, 0)
	resp := rawProduce(t, cl, topic, 0, pid, epoch1, 0, 1)
	require.Equal(t, int16(0), resp.ErrorCode)
	endTxnRaw(t, cl, ctx, txnID, pid, epoch1, true)

	// Second producer with same txnID: bumps epoch, fencing epoch1.
	_, epoch2 := initProducerIDRaw(t, cl, txnID)
	require.Greater(t, epoch2, epoch1)

	// Produce with stale epoch1 after the fence — should be rejected.
	fencedResp := rawProduce(t, cl, topic, 0, pid, epoch1, 1, 1)
	require.Equal(t, int16(35), fencedResp.ErrorCode,
		"stale epoch after commit should be fenced")

	// New epoch works fine.
	resp2 := rawProduce(t, cl, topic, 0, pid, epoch2, 0, 1)
	require.Equal(t, int16(0), resp2.ErrorCode)
}

func TestFenceBeforeProducerCommit(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t)

	topic := "test-fence-before-commit"
	txnID := "fence-before-commit"
	createTopic(t, tb.Addr, topic)

	cl := NewClient(t, tb.Addr)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// First producer: allocate PID, begin transaction, produce (but don't commit).
	pid, epoch1 := initProducerIDRaw(t, cl, txnID)
	addPartitionsToTxnRaw(t, cl, ctx, txnID, pid, epoch1, topic, 0)
	resp := rawProduce(t, cl, topic, 0, pid, epoch1, 0, 1)
	require.Equal(t, int16(0), resp.ErrorCode)

	// Second producer with same txnID while first txn is still open: bumps epoch.
	_, epoch2 := initProducerIDRaw(t, cl, txnID)
	require.Greater(t, epoch2, epoch1)

	// Try to produce with old epoch — should be fenced.
	fencedResp := rawProduce(t, cl, topic, 0, pid, epoch1, 1, 1)
	require.Equal(t, int16(35), fencedResp.ErrorCode,
		"stale epoch during open txn should be fenced")

	// Try to EndTxn with the old epoch — should also be fenced.
	endReq := kmsg.NewEndTxnRequest()
	endReq.TransactionalID = txnID
	endReq.ProducerID = pid
	endReq.ProducerEpoch = epoch1
	endReq.Commit = true
	endResp, err := endReq.RequestWith(ctx, cl)
	require.NoError(t, err)
	require.Equal(t, int16(35), endResp.ErrorCode,
		"EndTxn with stale epoch should be fenced")

	// New epoch works.
	resp2 := rawProduce(t, cl, topic, 0, pid, epoch2, 0, 1)
	require.Equal(t, int16(0), resp2.ErrorCode)
}

func TestTxnOffsetCommit(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t)

	topic := "test-txn-offset-commit"
	groupID := "txn-offset-commit-group"
	txnID := "txn-offset-commit-test"
	createTopic(t, tb.Addr, topic)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Produce 5 records to the topic.
	producer := NewClient(t, tb.Addr,
		kgo.DefaultProduceTopic(topic),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	for i := 0; i < 5; i++ {
		ProduceSync(t, producer, &kgo.Record{Topic: topic, Partition: 0, Value: []byte("val")})
	}

	// Allocate a transactional PID via raw InitProducerID.
	rawCl := NewClient(t, tb.Addr)
	initReq := kmsg.NewInitProducerIDRequest()
	initReq.TransactionalID = &txnID
	initReq.TransactionTimeoutMillis = 30000
	initResp, err := initReq.RequestWith(ctx, rawCl)
	require.NoError(t, err)
	require.Equal(t, int16(0), initResp.ErrorCode, "InitProducerID should succeed")
	pid := initResp.ProducerID
	epoch := initResp.ProducerEpoch

	// AddOffsetsToTxn: register the consumer group in the transaction.
	addOffsetsReq := kmsg.NewAddOffsetsToTxnRequest()
	addOffsetsReq.TransactionalID = txnID
	addOffsetsReq.ProducerID = pid
	addOffsetsReq.ProducerEpoch = epoch
	addOffsetsReq.Group = groupID
	addOffsetsResp, err := addOffsetsReq.RequestWith(ctx, rawCl)
	require.NoError(t, err)
	require.Equal(t, int16(0), addOffsetsResp.ErrorCode, "AddOffsetsToTxn should succeed")

	// TxnOffsetCommit: commit offset 3 for partition 0 within the transaction.
	txnOffsetReq := kmsg.NewTxnOffsetCommitRequest()
	txnOffsetReq.TransactionalID = txnID
	txnOffsetReq.Group = groupID
	txnOffsetReq.ProducerID = pid
	txnOffsetReq.ProducerEpoch = epoch
	tp := kmsg.NewTxnOffsetCommitRequestTopic()
	tp.Topic = topic
	pp := kmsg.NewTxnOffsetCommitRequestTopicPartition()
	pp.Partition = 0
	pp.Offset = 3
	tp.Partitions = append(tp.Partitions, pp)
	txnOffsetReq.Topics = append(txnOffsetReq.Topics, tp)
	txnOffsetResp, err := txnOffsetReq.RequestWith(ctx, rawCl)
	require.NoError(t, err)
	require.Len(t, txnOffsetResp.Topics, 1)
	require.Len(t, txnOffsetResp.Topics[0].Partitions, 1)
	require.Equal(t, int16(0), txnOffsetResp.Topics[0].Partitions[0].ErrorCode,
		"TxnOffsetCommit should succeed")

	// EndTxn: commit the transaction so offsets become visible.
	endReq := kmsg.NewEndTxnRequest()
	endReq.TransactionalID = txnID
	endReq.ProducerID = pid
	endReq.ProducerEpoch = epoch
	endReq.Commit = true
	endResp, err := endReq.RequestWith(ctx, rawCl)
	require.NoError(t, err)
	require.Equal(t, int16(0), endResp.ErrorCode, "EndTxn commit should succeed")

	// OffsetFetch: verify the committed offset is 3.
	admin := kadm.NewClient(NewClient(t, tb.Addr))
	offsets, err := admin.FetchOffsets(ctx, groupID)
	require.NoError(t, err)
	o, ok := offsets.Lookup(topic, 0)
	require.True(t, ok, "should find committed offset for %s/0", topic)
	require.Equal(t, int64(3), o.At, "transactionally committed offset should be 3")
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
	require.Equal(t, int16(105), resp.TransactionStates[0].ErrorCode) // TRANSACTIONAL_ID_NOT_FOUND
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

// TestIdempotentOutOfOrder verifies that a produce request with a wrong
// sequence number is rejected with OUT_OF_ORDER_SEQUENCE_NUMBER (error 45).
func TestIdempotentOutOfOrder(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t)

	topic := "test-idempotent-ooo"
	createTopic(t, tb.Addr, topic)

	cl := NewClient(t, tb.Addr)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Allocate a producer ID.
	initReq := kmsg.NewInitProducerIDRequest()
	initResp, err := initReq.RequestWith(ctx, cl)
	require.NoError(t, err)
	require.Equal(t, int16(0), initResp.ErrorCode)
	pid := initResp.ProducerID
	epoch := initResp.ProducerEpoch

	// First batch with baseSeq=0 should succeed.
	resp1 := rawProduce(t, cl, topic, 0, pid, epoch, 0, 1)
	require.Equal(t, int16(0), resp1.ErrorCode, "first batch should succeed")

	// Send batch with wrong baseSeq=5 (expected 1) — should get error 45.
	resp2 := rawProduce(t, cl, topic, 0, pid, epoch, 5, 1)
	require.Equal(t, kerr.OutOfOrderSequenceNumber.Code, resp2.ErrorCode,
		"wrong sequence number should return OUT_OF_ORDER_SEQUENCE_NUMBER")
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

	var errCount atomic.Int64
	payload := make([]byte, 200)
	for i := 0; i < numRecords; i++ {
		rec := &kgo.Record{Partition: 0, Value: payload}
		cl.Produce(ctx, rec, func(_ *kgo.Record, err error) {
			if err != nil {
				errCount.Add(1)
			}
		})
	}

	require.NoError(t, cl.Flush(ctx))
	require.Zero(t, errCount.Load(), "expected zero produce errors with idempotent writes")

	// Consume and verify all records arrived
	consumer := NewClient(t, tb.Addr,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	records := ConsumeN(t, consumer, numRecords, 10*time.Second)
	require.Len(t, records, numRecords)
}

// TestIdempotentDedupAfterRestart verifies that producer dedup state survives
// a broker restart via WAL replay. This is the integration-level counterpart
// to TestRebuildChunksFromWAL_PIDState (unit test in broker_test.go).
func TestIdempotentDedupAfterRestart(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	topic := "test-idempotent-restart-dedup"

	var pid int64
	var epoch int16

	// Phase 1: Start broker, get a PID, produce 2 batches with known sequences.
	func() {
		tb := StartBroker(t, WithDataDir(dataDir))
		defer tb.Stop()
		createTopic(t, tb.Addr, topic)

		cl := NewClient(t, tb.Addr)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Allocate a producer ID.
		initReq := kmsg.NewInitProducerIDRequest()
		initResp, err := initReq.RequestWith(ctx, cl)
		require.NoError(t, err)
		require.Equal(t, int16(0), initResp.ErrorCode,
			"InitProducerID should succeed")
		pid = initResp.ProducerID
		epoch = initResp.ProducerEpoch

		// Batch 1: baseSequence=0, 5 records → offsets 0-4.
		resp1 := rawProduce(t, cl, topic, 0, pid, epoch, 0, 5)
		require.Equal(t, int16(0), resp1.ErrorCode,
			"batch 1 should succeed")
		require.Equal(t, int64(0), resp1.BaseOffset,
			"batch 1 base offset")

		// Batch 2: baseSequence=5, 5 records → offsets 5-9.
		resp2 := rawProduce(t, cl, topic, 0, pid, epoch, 5, 5)
		require.Equal(t, int16(0), resp2.ErrorCode,
			"batch 2 should succeed")
		require.Equal(t, int64(5), resp2.BaseOffset,
			"batch 2 base offset")
	}()

	// Phase 2: Restart broker, verify dedup state was rebuilt from WAL.
	tb2 := StartBroker(t, WithDataDir(dataDir))
	cl2 := NewClient(t, tb2.Addr)

	// Retry batch 2 (same PID/epoch/sequence) → should be deduped.
	dupResp := rawProduce(t, cl2, topic, 0, pid, epoch, 5, 5)
	require.Equal(t, int16(0), dupResp.ErrorCode,
		"duplicate batch should not return an error")
	require.Equal(t, int64(5), dupResp.BaseOffset,
		"duplicate batch should return the original base offset")

	// New batch: baseSequence=10, 1 record → should succeed with offset 10.
	newResp := rawProduce(t, cl2, topic, 0, pid, epoch, 10, 1)
	require.Equal(t, int16(0), newResp.ErrorCode,
		"new batch after restart should succeed")
	require.Equal(t, int64(10), newResp.BaseOffset,
		"new batch should get next offset")

	// Verify all 11 records are consumable and offsets are contiguous.
	consumer := NewClient(t, tb2.Addr,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	consumed := ConsumeN(t, consumer, 11, 10*time.Second)
	require.Len(t, consumed, 11)
	for i, r := range consumed {
		require.Equal(t, int64(i), r.Offset,
			"offset continuity at record %d", i)
	}
}

// initProducerIDRaw sends a raw InitProducerID request with the given
// transactional ID and returns the allocated PID and epoch.
func initProducerIDRaw(t *testing.T, cl *kgo.Client, txnID string) (int64, int16) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req := kmsg.NewInitProducerIDRequest()
	req.TransactionalID = &txnID
	req.TransactionTimeoutMillis = 30000
	resp, err := req.RequestWith(ctx, cl)
	require.NoError(t, err)
	require.Equal(t, int16(0), resp.ErrorCode, "InitProducerID should succeed")
	return resp.ProducerID, resp.ProducerEpoch
}

// addPartitionsToTxnRaw sends AddPartitionsToTxn to begin a transaction
// and register a topic-partition.
func addPartitionsToTxnRaw(t *testing.T, cl *kgo.Client, ctx context.Context, txnID string, pid int64, epoch int16, topic string, partition int32) {
	t.Helper()
	req := kmsg.NewAddPartitionsToTxnRequest()
	req.TransactionalID = txnID
	req.ProducerID = pid
	req.ProducerEpoch = epoch
	rt := kmsg.NewAddPartitionsToTxnRequestTopic()
	rt.Topic = topic
	rt.Partitions = []int32{partition}
	req.Topics = append(req.Topics, rt)
	resp, err := req.RequestWith(ctx, cl)
	require.NoError(t, err)
	require.Len(t, resp.Topics, 1)
	require.Len(t, resp.Topics[0].Partitions, 1)
	require.Equal(t, int16(0), resp.Topics[0].Partitions[0].ErrorCode,
		"AddPartitionsToTxn should succeed")
}

// endTxnRaw sends a raw EndTxn request and asserts success.
func endTxnRaw(t *testing.T, cl *kgo.Client, ctx context.Context, txnID string, pid int64, epoch int16, commit bool) {
	t.Helper()
	req := kmsg.NewEndTxnRequest()
	req.TransactionalID = txnID
	req.ProducerID = pid
	req.ProducerEpoch = epoch
	req.Commit = commit
	resp, err := req.RequestWith(ctx, cl)
	require.NoError(t, err)
	require.Equal(t, int16(0), resp.ErrorCode, "EndTxn should succeed")
}

// rawProduce sends a raw ProduceRequest with a hand-crafted RecordBatch.
// Returns the partition response for assertions.
func rawProduce(t *testing.T, cl *kgo.Client, topic string, partition int32,
	pid int64, epoch int16, baseSeq, numRecords int32,
) kmsg.ProduceResponseTopicPartition {
	t.Helper()

	batch := buildIdempotentBatch(pid, epoch, baseSeq, numRecords)

	req := kmsg.NewProduceRequest()
	req.Acks = -1
	req.TimeoutMillis = 5000
	rt := kmsg.NewProduceRequestTopic()
	rt.Topic = topic
	rp := kmsg.NewProduceRequestTopicPartition()
	rp.Partition = partition
	rp.Records = batch
	rt.Partitions = append(rt.Partitions, rp)
	req.Topics = append(req.Topics, rt)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := req.RequestWith(ctx, cl)
	require.NoError(t, err)
	require.Len(t, resp.Topics, 1)
	require.Len(t, resp.Topics[0].Partitions, 1)
	return resp.Topics[0].Partitions[0]
}

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// buildIdempotentBatch builds a minimal valid Kafka RecordBatch with the
// given idempotent producer fields. Each record has an empty key and a
// single-byte value, encoded with proper varints.
func buildIdempotentBatch(pid int64, epoch int16, baseSeq, numRecords int32) []byte {
	// Build the records section first (each record is minimal).
	var records []byte
	for i := int32(0); i < numRecords; i++ {
		records = append(records, buildMinimalRecord(i)...)
	}

	headerSize := 61
	totalSize := headerSize + len(records)
	raw := make([]byte, totalSize)

	// BaseOffset (0:8) — client sends 0, broker overwrites.
	binary.BigEndian.PutUint64(raw[0:8], 0)
	// BatchLength (8:12) = totalSize - 12 (excludes baseOffset + batchLength fields).
	binary.BigEndian.PutUint32(raw[8:12], uint32(totalSize-12))
	// PartitionLeaderEpoch (12:16).
	binary.BigEndian.PutUint32(raw[12:16], 0)
	// Magic (16).
	raw[16] = 2
	// CRC (17:21) — placeholder, computed below.
	// Attributes (21:23) — 0 = no compression, CreateTime.
	binary.BigEndian.PutUint16(raw[21:23], 0)
	// LastOffsetDelta (23:27).
	lastDelta := numRecords - 1
	if lastDelta < 0 {
		lastDelta = 0
	}
	binary.BigEndian.PutUint32(raw[23:27], uint32(lastDelta))
	// BaseTimestamp (27:35).
	now := time.Now().UnixMilli()
	binary.BigEndian.PutUint64(raw[27:35], uint64(now))
	// MaxTimestamp (35:43).
	binary.BigEndian.PutUint64(raw[35:43], uint64(now))
	// ProducerID (43:51).
	binary.BigEndian.PutUint64(raw[43:51], uint64(pid))
	// ProducerEpoch (51:53).
	binary.BigEndian.PutUint16(raw[51:53], uint16(epoch))
	// BaseSequence (53:57).
	binary.BigEndian.PutUint32(raw[53:57], uint32(baseSeq))
	// NumRecords (57:61).
	binary.BigEndian.PutUint32(raw[57:61], uint32(numRecords))

	// Copy records after the header.
	copy(raw[61:], records)

	// Compute CRC32C over bytes 21+ (attributes through end of records).
	crc := crc32.Checksum(raw[21:], crc32cTable)
	binary.BigEndian.PutUint32(raw[17:21], crc)

	return raw
}

// buildMinimalRecord builds a single Kafka record with an empty key and
// a one-byte value ('v'). Uses varint encoding per the Kafka wire protocol.
func buildMinimalRecord(offsetDelta int32) []byte {
	// Record format:
	//   length: varint (record body size)
	//   attributes: int8 (0)
	//   timestampDelta: varint (0)
	//   offsetDelta: varint
	//   keyLength: varint (-1 = null)
	//   key: (empty)
	//   valueLength: varint (1)
	//   value: 1 byte
	//   headersCount: varint (0)

	var body []byte
	body = append(body, 0)                        // attributes
	body = appendVarint(body, 0)                  // timestampDelta
	body = appendVarint(body, int64(offsetDelta)) // offsetDelta
	body = appendVarint(body, -1)                 // keyLength (null)
	body = appendVarint(body, 1)                  // valueLength
	body = append(body, 'v')                      // value
	body = appendVarint(body, 0)                  // headers count

	var rec []byte
	rec = appendVarint(rec, int64(len(body))) // record length
	rec = append(rec, body...)
	return rec
}

// appendVarint appends a zig-zag varint-encoded int64 to buf.
func appendVarint(buf []byte, v int64) []byte {
	uv := uint64((v << 1) ^ (v >> 63)) // zig-zag encode
	for uv >= 0x80 {
		buf = append(buf, byte(uv)|0x80)
		uv >>= 7
	}
	buf = append(buf, byte(uv))
	return buf
}
