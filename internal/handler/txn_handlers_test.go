package handler

import (
	"testing"
	"time"

	"github.com/klaudworks/klite/internal/clock"
	"github.com/klaudworks/klite/internal/cluster"
	"github.com/klaudworks/klite/internal/metadata"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// --- InitProducerID handler tests ---

func callInitProducerID(t *testing.T, state *cluster.State, req *kmsg.InitProducerIDRequest) *kmsg.InitProducerIDResponse {
	t.Helper()
	h := HandleInitProducerID(state)
	resp, err := h(req)
	if err != nil {
		t.Fatalf("handler returned error: %v", err)
	}
	return resp.(*kmsg.InitProducerIDResponse)
}

func TestInitProducerID_UnsupportedVersion(t *testing.T) {
	state := cluster.NewState(cluster.Config{NodeID: 0, DefaultPartitions: 1})

	r := kmsg.NewPtrInitProducerIDRequest()
	r.Version = 999

	resp := callInitProducerID(t, state, r)
	if resp.ErrorCode != kerr.UnsupportedVersion.Code {
		t.Errorf("expected UnsupportedVersion (%d), got %d",
			kerr.UnsupportedVersion.Code, resp.ErrorCode)
	}
}

func TestInitProducerID_Success_NonTransactional(t *testing.T) {
	state := cluster.NewState(cluster.Config{NodeID: 0, DefaultPartitions: 1})

	r := kmsg.NewPtrInitProducerIDRequest()
	r.Version = 4

	resp := callInitProducerID(t, state, r)
	if resp.ErrorCode != 0 {
		t.Fatalf("expected success, got error %d", resp.ErrorCode)
	}
	if resp.ProducerID < 0 {
		t.Errorf("expected non-negative ProducerID, got %d", resp.ProducerID)
	}
	if resp.ProducerEpoch < 0 {
		t.Errorf("expected non-negative ProducerEpoch, got %d", resp.ProducerEpoch)
	}
}

func TestInitProducerID_Success_Transactional(t *testing.T) {
	state := cluster.NewState(cluster.Config{NodeID: 0, DefaultPartitions: 1})

	txnID := "my-txn"
	r := kmsg.NewPtrInitProducerIDRequest()
	r.Version = 4
	r.TransactionalID = &txnID
	r.TransactionTimeoutMillis = 5000

	resp := callInitProducerID(t, state, r)
	if resp.ErrorCode != 0 {
		t.Fatalf("expected success, got error %d", resp.ErrorCode)
	}
	if resp.ProducerID < 0 {
		t.Errorf("expected non-negative ProducerID, got %d", resp.ProducerID)
	}

	// Calling again with same txnID bumps epoch
	resp2 := callInitProducerID(t, state, r)
	if resp2.ErrorCode != 0 {
		t.Fatalf("expected success on second call, got error %d", resp2.ErrorCode)
	}
	if resp2.ProducerID != resp.ProducerID {
		t.Errorf("expected same ProducerID %d, got %d", resp.ProducerID, resp2.ProducerID)
	}
	if resp2.ProducerEpoch != resp.ProducerEpoch+1 {
		t.Errorf("expected epoch %d, got %d", resp.ProducerEpoch+1, resp2.ProducerEpoch)
	}
}

func TestInitProducerID_MetadataLogPersistence(t *testing.T) {
	state := cluster.NewState(cluster.Config{NodeID: 0, DefaultPartitions: 1})

	ml, err := metadata.NewLog(metadata.LogConfig{
		DataDir: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("failed to create metadata log: %v", err)
	}
	state.SetMetadataLog(ml)

	r := kmsg.NewPtrInitProducerIDRequest()
	r.Version = 4

	resp := callInitProducerID(t, state, r)
	if resp.ErrorCode != 0 {
		t.Fatalf("expected success, got error %d", resp.ErrorCode)
	}

	// Verify NextPID was incremented (meaning the allocation happened)
	nextPID := state.PIDManager().NextPID()
	if nextPID <= resp.ProducerID {
		t.Errorf("NextPID (%d) should be > allocated ProducerID (%d)", nextPID, resp.ProducerID)
	}
}

func TestInitProducerID_MultipleAllocations(t *testing.T) {
	state := cluster.NewState(cluster.Config{NodeID: 0, DefaultPartitions: 1})

	r := kmsg.NewPtrInitProducerIDRequest()
	r.Version = 4

	resp1 := callInitProducerID(t, state, r)
	resp2 := callInitProducerID(t, state, r)

	if resp1.ErrorCode != 0 || resp2.ErrorCode != 0 {
		t.Fatalf("expected success, got errors %d, %d", resp1.ErrorCode, resp2.ErrorCode)
	}
	if resp1.ProducerID == resp2.ProducerID {
		t.Errorf("expected different PIDs for non-transactional requests, both got %d", resp1.ProducerID)
	}
}

// --- EndTxn handler tests ---

func callEndTxn(t *testing.T, state *cluster.State, clk clock.Clock, req *kmsg.EndTxnRequest) *kmsg.EndTxnResponse {
	t.Helper()
	h := HandleEndTxn(state, nil, clk) // nil walWriter — tests exercise pre-WAL logic
	resp, err := h(req)
	if err != nil {
		t.Fatalf("handler returned error: %v", err)
	}
	return resp.(*kmsg.EndTxnResponse)
}

func TestEndTxn_UnsupportedVersion(t *testing.T) {
	state := cluster.NewState(cluster.Config{NodeID: 0, DefaultPartitions: 1})
	clk := clock.NewFakeClock(time.Unix(1000, 0))

	r := kmsg.NewPtrEndTxnRequest()
	r.Version = 999

	resp := callEndTxn(t, state, clk, r)
	if resp.ErrorCode != 0 {
		t.Errorf("expected no error for bad version (handler returns empty response), got %d", resp.ErrorCode)
	}
}

func TestEndTxn_UnknownProducerID(t *testing.T) {
	state := cluster.NewState(cluster.Config{NodeID: 0, DefaultPartitions: 1})
	clk := clock.NewFakeClock(time.Unix(1000, 0))

	r := kmsg.NewPtrEndTxnRequest()
	r.Version = 3
	r.ProducerID = 999
	r.ProducerEpoch = 0
	r.Commit = true

	resp := callEndTxn(t, state, clk, r)
	// Error code 3 = UNKNOWN_MEMBER_ID (used for unknown PID)
	if resp.ErrorCode != 3 {
		t.Errorf("expected error code 3, got %d", resp.ErrorCode)
	}
}

func TestEndTxn_FencedEpoch(t *testing.T) {
	state := cluster.NewState(cluster.Config{NodeID: 0, DefaultPartitions: 1})
	clk := clock.NewFakeClock(time.Unix(1000, 0))

	txnID := "txn-fence-test"
	pid, _, _ := state.PIDManager().InitProducerID(txnID, 5000)
	// Bump the epoch
	_, newEpoch, _ := state.PIDManager().InitProducerID(txnID, 5000)

	r := kmsg.NewPtrEndTxnRequest()
	r.Version = 3
	r.ProducerID = pid
	r.ProducerEpoch = newEpoch - 1 // old epoch
	r.Commit = true

	resp := callEndTxn(t, state, clk, r)
	// Error code 35 = PRODUCER_FENCED
	if resp.ErrorCode != 35 {
		t.Errorf("expected PRODUCER_FENCED (35), got %d", resp.ErrorCode)
	}
}

func TestEndTxn_NilTxnPartitions_NoWork(t *testing.T) {
	state := cluster.NewState(cluster.Config{NodeID: 0, DefaultPartitions: 1})
	clk := clock.NewFakeClock(time.Unix(1000, 0))

	txnID := "txn-empty"
	pid, epoch, _ := state.PIDManager().InitProducerID(txnID, 5000)

	// Start a transaction but add no partitions
	tp := cluster.TopicPartition{Topic: "t", Partition: 0}
	state.PIDManager().AddPartitionsToTxn(pid, epoch, []cluster.TopicPartition{tp})

	// First EndTxn to commit
	state.PIDManager().PrepareEndTxn(pid, epoch, true)

	// Now retry — TxnPartitions will be nil (idempotent retry with empty state)
	r := kmsg.NewPtrEndTxnRequest()
	r.Version = 3
	r.ProducerID = pid
	r.ProducerEpoch = epoch
	r.Commit = true

	resp := callEndTxn(t, state, clk, r)
	if resp.ErrorCode != 0 {
		t.Errorf("expected success for idempotent retry, got error %d", resp.ErrorCode)
	}
}

func TestEndTxn_InvalidTxnState(t *testing.T) {
	state := cluster.NewState(cluster.Config{NodeID: 0, DefaultPartitions: 1})
	clk := clock.NewFakeClock(time.Unix(1000, 0))

	txnID := "txn-invalid-state"
	pid, epoch, _ := state.PIDManager().InitProducerID(txnID, 5000)

	// Start and commit a transaction
	tp := cluster.TopicPartition{Topic: "t", Partition: 0}
	state.PIDManager().AddPartitionsToTxn(pid, epoch, []cluster.TopicPartition{tp})
	state.PIDManager().PrepareEndTxn(pid, epoch, true)

	// Try to abort after committing — should get INVALID_TXN_STATE (53)
	r := kmsg.NewPtrEndTxnRequest()
	r.Version = 3
	r.ProducerID = pid
	r.ProducerEpoch = epoch
	r.Commit = false

	resp := callEndTxn(t, state, clk, r)
	// Error code 53 = INVALID_TXN_STATE
	if resp.ErrorCode != 53 {
		t.Errorf("expected INVALID_TXN_STATE (53), got %d", resp.ErrorCode)
	}
}
