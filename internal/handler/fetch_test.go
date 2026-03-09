package handler

import (
	"testing"
	"time"

	"github.com/klaudworks/klite/internal/chunk"
	"github.com/klaudworks/klite/internal/clock"
	"github.com/klaudworks/klite/internal/cluster"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

var fetchTestPool = chunk.NewPool(64*1024*1024, cluster.DefaultMaxMessageBytes)

// newFetchTestState creates a State with a chunk pool so PushBatch stores data
// in memory and Fetch can retrieve it.
func newFetchTestState() *cluster.State {
	state := cluster.NewState(cluster.Config{
		NodeID:            0,
		DefaultPartitions: 1,
	})
	state.SetWALConfig(nil, nil, fetchTestPool)
	return state
}

// pushBatch pushes a valid batch into a partition using PushBatch (requires lock).
func pushBatch(t *testing.T, pd *cluster.PartData, numRecords int32) int64 {
	t.Helper()
	raw := makeValidBatch(numRecords)
	meta, err := cluster.ParseBatchHeader(raw)
	if err != nil {
		t.Fatalf("ParseBatchHeader: %v", err)
	}
	base, _ := pd.PushBatch(raw, meta, nil)
	return base
}

func makeFetchReq(version int16, topic string, partition int32, offset int64) *kmsg.FetchRequest {
	r := kmsg.NewPtrFetchRequest()
	r.Version = version
	r.MaxBytes = 1024 * 1024
	r.MaxWaitMillis = 0

	rt := kmsg.NewFetchRequestTopic()
	rt.Topic = topic
	rp := kmsg.NewFetchRequestTopicPartition()
	rp.Partition = partition
	rp.FetchOffset = offset
	rp.PartitionMaxBytes = 1024 * 1024
	rt.Partitions = append(rt.Partitions, rp)
	r.Topics = append(r.Topics, rt)
	return r
}

func callFetch(t *testing.T, state *cluster.State, clk clock.Clock, req *kmsg.FetchRequest) *kmsg.FetchResponse {
	t.Helper()
	h := HandleFetch(state, nil, clk)
	resp, err := h(req)
	if err != nil {
		t.Fatalf("handler returned error: %v", err)
	}
	return resp.(*kmsg.FetchResponse)
}

func fetchPartResp(resp *kmsg.FetchResponse, topic string, partition int32) *kmsg.FetchResponseTopicPartition {
	for _, st := range resp.Topics {
		if st.Topic == topic {
			for i := range st.Partitions {
				if st.Partitions[i].Partition == partition {
					return &st.Partitions[i]
				}
			}
		}
	}
	return nil
}

func TestFetch_SessionIDNotZero(t *testing.T) {
	state := newFetchTestState()
	clk := clock.NewFakeClock(time.Unix(1000, 0))

	req := makeFetchReq(12, "test-topic", 0, 0)
	req.SessionID = 42

	resp := callFetch(t, state, clk, req)
	if resp.ErrorCode != kerr.FetchSessionIDNotFound.Code {
		t.Errorf("expected FetchSessionIDNotFound (%d), got %d",
			kerr.FetchSessionIDNotFound.Code, resp.ErrorCode)
	}
}

func TestFetch_UnknownTopic(t *testing.T) {
	state := newFetchTestState()
	clk := clock.NewFakeClock(time.Unix(1000, 0))

	req := makeFetchReq(12, "nonexistent", 0, 0)
	resp := callFetch(t, state, clk, req)

	pr := fetchPartResp(resp, "nonexistent", 0)
	if pr == nil {
		t.Fatal("expected partition response")
	}
	if pr.ErrorCode != kerr.UnknownTopicOrPartition.Code {
		t.Errorf("expected UnknownTopicOrPartition (%d), got %d",
			kerr.UnknownTopicOrPartition.Code, pr.ErrorCode)
	}
	if pr.HighWatermark != -1 {
		t.Errorf("expected HW -1, got %d", pr.HighWatermark)
	}
}

func TestFetch_UnknownPartition(t *testing.T) {
	state := newFetchTestState()
	state.CreateTopic("test-topic", 1) // partition 0 only
	clk := clock.NewFakeClock(time.Unix(1000, 0))

	req := makeFetchReq(12, "test-topic", 5, 0)
	resp := callFetch(t, state, clk, req)

	pr := fetchPartResp(resp, "test-topic", 5)
	if pr == nil {
		t.Fatal("expected partition response")
	}
	if pr.ErrorCode != kerr.UnknownTopicOrPartition.Code {
		t.Errorf("expected UnknownTopicOrPartition (%d), got %d",
			kerr.UnknownTopicOrPartition.Code, pr.ErrorCode)
	}
}

func TestFetch_HappyPath(t *testing.T) {
	state := newFetchTestState()
	td, _ := state.CreateTopic("test-topic", 1)
	clk := clock.NewFakeClock(time.Unix(1000, 0))

	pd := td.Partitions[0]
	pd.Lock()
	pushBatch(t, pd, 3) // offsets 0,1,2
	pushBatch(t, pd, 2) // offsets 3,4
	pd.Unlock()

	req := makeFetchReq(12, "test-topic", 0, 0)
	resp := callFetch(t, state, clk, req)

	pr := fetchPartResp(resp, "test-topic", 0)
	if pr == nil {
		t.Fatal("expected partition response")
	}
	if pr.ErrorCode != 0 {
		t.Errorf("expected no error, got %d", pr.ErrorCode)
	}
	if pr.HighWatermark != 5 {
		t.Errorf("expected HW 5, got %d", pr.HighWatermark)
	}
	if pr.LastStableOffset != 5 {
		t.Errorf("expected LSO 5, got %d", pr.LastStableOffset)
	}
	if pr.LogStartOffset != 0 {
		t.Errorf("expected LogStartOffset 0, got %d", pr.LogStartOffset)
	}
	if len(pr.RecordBatches) == 0 {
		t.Error("expected non-empty RecordBatches")
	}
}

func TestFetch_EmptyPartition(t *testing.T) {
	state := newFetchTestState()
	state.CreateTopic("test-topic", 1)
	clk := clock.NewFakeClock(time.Unix(1000, 0))

	req := makeFetchReq(12, "test-topic", 0, 0)
	resp := callFetch(t, state, clk, req)

	pr := fetchPartResp(resp, "test-topic", 0)
	if pr == nil {
		t.Fatal("expected partition response")
	}
	if pr.ErrorCode != 0 {
		t.Errorf("expected no error, got %d", pr.ErrorCode)
	}
	if pr.HighWatermark != 0 {
		t.Errorf("expected HW 0, got %d", pr.HighWatermark)
	}
}

func TestFetch_KIP74_MaxBytesTrimming(t *testing.T) {
	state := newFetchTestState()
	td, _ := state.CreateTopic("test-topic", 3)
	clk := clock.NewFakeClock(time.Unix(1000, 0))

	// Push data into partitions 0, 1, 2
	for i := 0; i < 3; i++ {
		pd := td.Partitions[i]
		pd.Lock()
		pushBatch(t, pd, 1)
		pd.Unlock()
	}

	// Build a request fetching all 3 partitions with a very small MaxBytes.
	// KIP-74: first partition with data is always included even if oversized,
	// subsequent partitions should be trimmed.
	r := kmsg.NewPtrFetchRequest()
	r.Version = 12
	r.MaxBytes = 1 // very small — only first partition should have data
	r.MaxWaitMillis = 0

	rt := kmsg.NewFetchRequestTopic()
	rt.Topic = "test-topic"
	for i := int32(0); i < 3; i++ {
		rp := kmsg.NewFetchRequestTopicPartition()
		rp.Partition = i
		rp.FetchOffset = 0
		rp.PartitionMaxBytes = 1024 * 1024
		rt.Partitions = append(rt.Partitions, rp)
	}
	r.Topics = append(r.Topics, rt)

	resp := callFetch(t, state, clk, r)

	// Count how many partitions have data
	dataCount := 0
	for _, st := range resp.Topics {
		for _, sp := range st.Partitions {
			if len(sp.RecordBatches) > 0 {
				dataCount++
			}
		}
	}

	if dataCount != 1 {
		t.Errorf("KIP-74: expected exactly 1 partition with data (first always included), got %d", dataCount)
	}
}

func TestFetch_ReadCommitted_LSOCap(t *testing.T) {
	state := newFetchTestState()
	td, _ := state.CreateTopic("test-topic", 1)
	clk := clock.NewFakeClock(time.Unix(1000, 0))

	pd := td.Partitions[0]
	pd.Lock()
	pushBatch(t, pd, 3) // offsets 0,1,2
	pushBatch(t, pd, 2) // offsets 3,4 — HW=5

	// Open transaction starting at offset 2 → LSO should be 2
	pd.AddOpenTxn(100, 2)
	pd.Unlock()

	req := makeFetchReq(12, "test-topic", 0, 0)
	req.IsolationLevel = 1 // READ_COMMITTED

	resp := callFetch(t, state, clk, req)

	pr := fetchPartResp(resp, "test-topic", 0)
	if pr == nil {
		t.Fatal("expected partition response")
	}
	if pr.ErrorCode != 0 {
		t.Errorf("expected no error, got %d", pr.ErrorCode)
	}
	if pr.HighWatermark != 5 {
		t.Errorf("expected HW 5, got %d", pr.HighWatermark)
	}
	if pr.LastStableOffset != 2 {
		t.Errorf("expected LSO 2, got %d", pr.LastStableOffset)
	}

	// With READ_COMMITTED, batches at or beyond LSO=2 should be filtered.
	// Only the first batch (offsets 0-2, baseOffset=0) has baseOffset < LSO=2,
	// so it should be included. The second batch (baseOffset=3) has baseOffset >= LSO
	// and should be filtered out.
	// Actually, the filter checks b.BaseOffset >= fr.LSO. Batch 0 has baseOffset=0 < 2, included.
	// Batch 1 has baseOffset=3 >= 2, filtered.
	if len(pr.RecordBatches) == 0 {
		t.Error("expected some record batches for offsets below LSO")
	}
}

func TestFetch_ReadCommitted_AbortedTxns(t *testing.T) {
	state := newFetchTestState()
	td, _ := state.CreateTopic("test-topic", 1)
	clk := clock.NewFakeClock(time.Unix(1000, 0))

	pd := td.Partitions[0]
	pd.Lock()
	pushBatch(t, pd, 3) // offsets 0,1,2
	pushBatch(t, pd, 2) // offsets 3,4

	// Add an aborted transaction: producerID=42, first data at offset 1, abort at offset 4
	pd.AddAbortedTxn(cluster.AbortedTxnEntry{
		ProducerID:  42,
		FirstOffset: 1,
		LastOffset:  4,
	})
	pd.Unlock()

	req := makeFetchReq(12, "test-topic", 0, 0)
	req.IsolationLevel = 1

	resp := callFetch(t, state, clk, req)

	pr := fetchPartResp(resp, "test-topic", 0)
	if pr == nil {
		t.Fatal("expected partition response")
	}
	if pr.ErrorCode != 0 {
		t.Errorf("expected no error, got %d", pr.ErrorCode)
	}

	if len(pr.AbortedTransactions) != 1 {
		t.Fatalf("expected 1 aborted transaction, got %d", len(pr.AbortedTransactions))
	}
	at := pr.AbortedTransactions[0]
	if at.ProducerID != 42 {
		t.Errorf("expected ProducerID 42, got %d", at.ProducerID)
	}
	if at.FirstOffset != 1 {
		t.Errorf("expected FirstOffset 1, got %d", at.FirstOffset)
	}
}

func TestFetch_TopicIDResolution(t *testing.T) {
	state := newFetchTestState()
	td, _ := state.CreateTopic("test-topic", 1)
	clk := clock.NewFakeClock(time.Unix(1000, 0))

	pd := td.Partitions[0]
	pd.Lock()
	pushBatch(t, pd, 2)
	pd.Unlock()

	// Use v16 (>= 13) and set TopicID instead of topic name
	r := kmsg.NewPtrFetchRequest()
	r.Version = 16
	r.MaxBytes = 1024 * 1024
	r.MaxWaitMillis = 0

	rt := kmsg.NewFetchRequestTopic()
	rt.TopicID = td.ID
	// Leave rt.Topic empty — resolution should happen by ID
	rp := kmsg.NewFetchRequestTopicPartition()
	rp.Partition = 0
	rp.FetchOffset = 0
	rp.PartitionMaxBytes = 1024 * 1024
	rt.Partitions = append(rt.Partitions, rp)
	r.Topics = append(r.Topics, rt)

	resp := callFetch(t, state, clk, r)

	if len(resp.Topics) != 1 {
		t.Fatalf("expected 1 topic, got %d", len(resp.Topics))
	}
	if len(resp.Topics[0].Partitions) != 1 {
		t.Fatalf("expected 1 partition, got %d", len(resp.Topics[0].Partitions))
	}

	sp := resp.Topics[0].Partitions[0]
	if sp.ErrorCode != 0 {
		t.Errorf("expected no error, got %d", sp.ErrorCode)
	}
	if sp.HighWatermark != 2 {
		t.Errorf("expected HW 2, got %d", sp.HighWatermark)
	}
	if len(sp.RecordBatches) == 0 {
		t.Error("expected non-empty RecordBatches via TopicID resolution")
	}
}

func TestFetch_TopicIDResolution_UnknownID(t *testing.T) {
	state := newFetchTestState()
	clk := clock.NewFakeClock(time.Unix(1000, 0))

	// Fetch with a random TopicID that doesn't exist
	r := kmsg.NewPtrFetchRequest()
	r.Version = 16
	r.MaxBytes = 1024 * 1024
	r.MaxWaitMillis = 0

	rt := kmsg.NewFetchRequestTopic()
	rt.TopicID = [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	rp := kmsg.NewFetchRequestTopicPartition()
	rp.Partition = 0
	rp.FetchOffset = 0
	rp.PartitionMaxBytes = 1024 * 1024
	rt.Partitions = append(rt.Partitions, rp)
	r.Topics = append(r.Topics, rt)

	resp := callFetch(t, state, clk, r)

	if len(resp.Topics) != 1 {
		t.Fatalf("expected 1 topic, got %d", len(resp.Topics))
	}
	sp := resp.Topics[0].Partitions[0]
	if sp.ErrorCode != kerr.UnknownTopicOrPartition.Code {
		t.Errorf("expected UnknownTopicOrPartition (%d), got %d",
			kerr.UnknownTopicOrPartition.Code, sp.ErrorCode)
	}
}

func TestFetch_LongPollWait(t *testing.T) {
	state := newFetchTestState()
	td, _ := state.CreateTopic("test-topic", 1)
	clk := clock.NewFakeClock(time.Unix(1000, 0))

	// Empty partition — fetch will long-poll.
	req := makeFetchReq(12, "test-topic", 0, 0)
	req.MinBytes = 1
	req.MaxWaitMillis = 5000 // 5 seconds

	done := make(chan *kmsg.FetchResponse, 1)
	go func() {
		resp := callFetch(t, state, clk, req)
		done <- resp
	}()

	// Wait for the timer to be registered
	if !clk.WaitForTimers(1, 2*time.Second) {
		t.Fatal("timed out waiting for fetch timer registration")
	}

	// Push data so the waiter wakes up
	pd := td.Partitions[0]
	pd.Lock()
	pushBatch(t, pd, 1)
	pd.Unlock()
	pd.NotifyWaiters()

	select {
	case resp := <-done:
		pr := fetchPartResp(resp, "test-topic", 0)
		if pr == nil {
			t.Fatal("expected partition response")
		}
		// After wake-up, the re-fetch should find the data
		if pr.ErrorCode != 0 {
			t.Errorf("expected no error, got %d", pr.ErrorCode)
		}
		if pr.HighWatermark != 1 {
			t.Errorf("expected HW 1 after push, got %d", pr.HighWatermark)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("fetch handler did not return after waiter notification")
	}
}

func TestFetch_LongPollTimeout(t *testing.T) {
	state := newFetchTestState()
	state.CreateTopic("test-topic", 1)
	clk := clock.NewFakeClock(time.Unix(1000, 0))

	req := makeFetchReq(12, "test-topic", 0, 0)
	req.MinBytes = 1
	req.MaxWaitMillis = 500

	done := make(chan *kmsg.FetchResponse, 1)
	go func() {
		resp := callFetch(t, state, clk, req)
		done <- resp
	}()

	// Wait for the timer to be registered
	if !clk.WaitForTimers(1, 2*time.Second) {
		t.Fatal("timed out waiting for fetch timer registration")
	}

	// Advance clock past MaxWaitMillis to fire the timer
	clk.Advance(600 * time.Millisecond)

	select {
	case resp := <-done:
		pr := fetchPartResp(resp, "test-topic", 0)
		if pr == nil {
			t.Fatal("expected partition response")
		}
		// Empty partition, should return with no error and HW 0
		if pr.ErrorCode != 0 {
			t.Errorf("expected no error after timeout, got %d", pr.ErrorCode)
		}
		if pr.HighWatermark != 0 {
			t.Errorf("expected HW 0, got %d", pr.HighWatermark)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("fetch handler did not return after clock advance")
	}
}

func TestFetch_ResponseTopicIDSetForV13Plus(t *testing.T) {
	state := newFetchTestState()
	td, _ := state.CreateTopic("test-topic", 1)
	clk := clock.NewFakeClock(time.Unix(1000, 0))

	// v13+: response should include TopicID
	r := kmsg.NewPtrFetchRequest()
	r.Version = 16
	r.MaxBytes = 1024 * 1024

	rt := kmsg.NewFetchRequestTopic()
	rt.TopicID = td.ID
	rp := kmsg.NewFetchRequestTopicPartition()
	rp.Partition = 0
	rp.FetchOffset = 0
	rp.PartitionMaxBytes = 1024 * 1024
	rt.Partitions = append(rt.Partitions, rp)
	r.Topics = append(r.Topics, rt)

	resp := callFetch(t, state, clk, r)
	if len(resp.Topics) != 1 {
		t.Fatalf("expected 1 topic, got %d", len(resp.Topics))
	}
	if resp.Topics[0].TopicID != td.ID {
		t.Errorf("expected TopicID %v in response, got %v", td.ID, resp.Topics[0].TopicID)
	}

	// v12 (< 13): response should NOT include TopicID
	r2 := makeFetchReq(12, "test-topic", 0, 0)
	resp2 := callFetch(t, state, clk, r2)
	if len(resp2.Topics) != 1 {
		t.Fatalf("expected 1 topic, got %d", len(resp2.Topics))
	}
	if resp2.Topics[0].TopicID != [16]byte{} {
		t.Errorf("expected zero TopicID for version < 13, got %v", resp2.Topics[0].TopicID)
	}
}
