package cluster

import (
	"testing"
	"time"

	"github.com/klaudworks/klite/internal/clock"
)

func TestSequenceWindowReplay(t *testing.T) {
	t.Parallel()
	var w SequenceWindow

	// Replay 3 batches: seq 0(5 recs), 5(3 recs), 8(2 recs)
	w.Replay(0, 0, 5, 100)
	w.Replay(0, 5, 3, 200)
	w.Replay(0, 8, 2, 300)

	if got := w.LastSequence(); got != 9 {
		t.Fatalf("LastSequence = %d, want 9", got)
	}

	// PushAndValidate should detect a duplicate for seq=8 (last replayed batch).
	ok, isDup, dupOff := w.PushAndValidate(0, 8, 2, 999)
	if !ok || !isDup {
		t.Fatalf("expected duplicate detection for seq=8, got ok=%v isDup=%v", ok, isDup)
	}
	if dupOff != 300 {
		t.Fatalf("dupOffset = %d, want 300", dupOff)
	}

	// PushAndValidate should accept seq=10 as the next expected sequence.
	ok, isDup, _ = w.PushAndValidate(0, 10, 1, 400)
	if !ok || isDup {
		t.Fatalf("expected seq=10 accepted, got ok=%v isDup=%v", ok, isDup)
	}
}

func TestSequenceWindowReplayEpochChange(t *testing.T) {
	t.Parallel()
	var w SequenceWindow

	// Replay under epoch 0.
	w.Replay(0, 0, 5, 100)
	w.Replay(0, 5, 3, 200)

	if got := w.LastSequence(); got != 7 {
		t.Fatalf("LastSequence after epoch 0 = %d, want 7", got)
	}

	// Epoch change to 1 — window resets. First seq can be anything.
	w.Replay(1, 0, 4, 300)

	if got := w.LastSequence(); got != 3 {
		t.Fatalf("LastSequence after epoch 1 = %d, want 3", got)
	}

	// Verify dedup works in new epoch.
	ok, isDup, dupOff := w.PushAndValidate(1, 0, 4, 999)
	if !ok || !isDup {
		t.Fatalf("expected duplicate for seq=0 epoch=1, got ok=%v isDup=%v", ok, isDup)
	}
	if dupOff != 300 {
		t.Fatalf("dupOffset = %d, want 300", dupOff)
	}
}

func TestSequenceWindowReplayIdempotent(t *testing.T) {
	t.Parallel()
	var w SequenceWindow

	// Replay the same sequence of batches twice — should be idempotent.
	for range 2 {
		w.Replay(0, 0, 5, 100)
		w.Replay(0, 5, 3, 200)
	}

	if got := w.LastSequence(); got != 7 {
		t.Fatalf("LastSequence = %d, want 7", got)
	}
}

func TestReplayBatchSkipsNonIdempotent(t *testing.T) {
	t.Parallel()
	m := NewProducerIDManager()

	m.ReplayBatch(TopicPartition{Topic: "t", Partition: 0}, BatchMeta{
		ProducerID:    -1,
		ProducerEpoch: 0,
		BaseSequence:  0,
		NumRecords:    1,
	}, 0)

	if p := m.GetProducer(-1); p != nil {
		t.Fatal("expected no producer for PID -1")
	}
}

func TestReplayBatchSkipsControlBatch(t *testing.T) {
	t.Parallel()
	m := NewProducerIDManager()

	m.ReplayBatch(TopicPartition{Topic: "t", Partition: 0}, BatchMeta{
		ProducerID:    42,
		ProducerEpoch: 0,
		BaseSequence:  -1, // control batch
		NumRecords:    1,
	}, 0)

	if p := m.GetProducer(42); p != nil {
		t.Fatal("expected no producer state for control batch")
	}
}

func TestReplayBatchCreatesState(t *testing.T) {
	t.Parallel()
	m := NewProducerIDManager()
	tp := TopicPartition{Topic: "test-topic", Partition: 0}

	m.ReplayBatch(tp, BatchMeta{
		ProducerID:    10,
		ProducerEpoch: 3,
		BaseSequence:  0,
		NumRecords:    5,
	}, 100)

	ps := m.GetProducer(10)
	if ps == nil {
		t.Fatal("expected producer state for PID 10")
	}
	if ps.Epoch != 3 {
		t.Fatalf("Epoch = %d, want 3", ps.Epoch)
	}
	w, ok := ps.Sequences[tp]
	if !ok {
		t.Fatal("expected sequence window for topic-partition")
	}
	if got := w.LastSequence(); got != 4 {
		t.Fatalf("LastSequence = %d, want 4", got)
	}
}

func TestReplayBatchThenDedup(t *testing.T) {
	t.Parallel()
	m := NewProducerIDManager()
	tp := TopicPartition{Topic: "test-topic", Partition: 0}

	// Replay 3 batches for PID 7.
	m.ReplayBatch(tp, BatchMeta{ProducerID: 7, ProducerEpoch: 0, BaseSequence: 0, NumRecords: 10}, 0)
	m.ReplayBatch(tp, BatchMeta{ProducerID: 7, ProducerEpoch: 0, BaseSequence: 10, NumRecords: 10}, 10)
	m.ReplayBatch(tp, BatchMeta{ProducerID: 7, ProducerEpoch: 0, BaseSequence: 20, NumRecords: 5}, 20)

	// ValidateAndDedup should detect seq=20 as duplicate.
	errCode, isDup, dupOff := m.ValidateAndDedup(7, 0, tp, 20, 5, 999)
	if errCode != 0 {
		t.Fatalf("errCode = %d, want 0", errCode)
	}
	if !isDup {
		t.Fatal("expected duplicate detection for seq=20")
	}
	if dupOff != 20 {
		t.Fatalf("dupOffset = %d, want 20", dupOff)
	}

	// seq=25 should be accepted as new.
	errCode, isDup, _ = m.ValidateAndDedup(7, 0, tp, 25, 5, 25)
	if errCode != 0 {
		t.Fatalf("errCode = %d, want 0", errCode)
	}
	if isDup {
		t.Fatal("seq=25 should not be a duplicate")
	}
}

func TestReplayBatchAdvancesEpoch(t *testing.T) {
	t.Parallel()
	m := NewProducerIDManager()
	tp := TopicPartition{Topic: "t", Partition: 0}

	// Replay with epoch 1, then epoch 3.
	m.ReplayBatch(tp, BatchMeta{ProducerID: 5, ProducerEpoch: 1, BaseSequence: 0, NumRecords: 1}, 0)
	m.ReplayBatch(tp, BatchMeta{ProducerID: 5, ProducerEpoch: 3, BaseSequence: 0, NumRecords: 1}, 10)

	ps := m.GetProducer(5)
	if ps.Epoch != 3 {
		t.Fatalf("Epoch = %d, want 3", ps.Epoch)
	}
}

func TestValidateAndDedupAcceptsHigherEpoch(t *testing.T) {
	t.Parallel()
	m := NewProducerIDManager()
	tp := TopicPartition{Topic: "t", Partition: 0}

	// Produce a few batches at epoch 0.
	m.InitProducerID("", 0)
	errCode, _, _ := m.ValidateAndDedup(1, 0, tp, 0, 5, 0)
	if errCode != 0 {
		t.Fatalf("initial produce: errCode=%d", errCode)
	}
	errCode, _, _ = m.ValidateAndDedup(1, 0, tp, 5, 5, 5)
	if errCode != 0 {
		t.Fatalf("second produce: errCode=%d", errCode)
	}

	// Epoch bump to 1 with seq=0 (KIP-360 client-side epoch bump).
	errCode, isDup, _ := m.ValidateAndDedup(1, 1, tp, 0, 3, 100)
	if errCode != 0 {
		t.Fatalf("epoch bump: errCode=%d, want 0", errCode)
	}
	if isDup {
		t.Fatal("epoch bump should not be a duplicate")
	}

	// Verify epoch was updated.
	ps := m.GetProducer(1)
	if ps.Epoch != 1 {
		t.Fatalf("Epoch = %d, want 1", ps.Epoch)
	}

	// Next produce at new epoch should work.
	errCode, _, _ = m.ValidateAndDedup(1, 1, tp, 3, 2, 103)
	if errCode != 0 {
		t.Fatalf("post-bump produce: errCode=%d", errCode)
	}
}

func TestValidateAndDedupRejectsHigherEpochNonZeroSeq(t *testing.T) {
	t.Parallel()
	m := NewProducerIDManager()
	tp := TopicPartition{Topic: "t", Partition: 0}

	m.InitProducerID("", 0)
	if errCode, _, _ := m.ValidateAndDedup(1, 0, tp, 0, 5, 0); errCode != 0 {
		t.Fatalf("setup ValidateAndDedup: errCode=%d", errCode)
	}

	// Epoch bump with non-zero seq must be rejected.
	errCode, _, _ := m.ValidateAndDedup(1, 1, tp, 5, 3, 100)
	if errCode != 45 {
		t.Fatalf("errCode = %d, want 45 (OUT_OF_ORDER_SEQUENCE_NUMBER)", errCode)
	}
}

func TestValidateAndDedupEpochBumpClearsAllPartitions(t *testing.T) {
	t.Parallel()
	m := NewProducerIDManager()
	tp0 := TopicPartition{Topic: "t", Partition: 0}
	tp1 := TopicPartition{Topic: "t", Partition: 1}

	m.InitProducerID("", 0)
	if errCode, _, _ := m.ValidateAndDedup(1, 0, tp0, 0, 5, 0); errCode != 0 {
		t.Fatalf("setup ValidateAndDedup tp0: errCode=%d", errCode)
	}
	if errCode, _, _ := m.ValidateAndDedup(1, 0, tp1, 0, 3, 100); errCode != 0 {
		t.Fatalf("setup ValidateAndDedup tp1: errCode=%d", errCode)
	}

	// Epoch bump on tp0 should clear all partition windows.
	errCode, _, _ := m.ValidateAndDedup(1, 1, tp0, 0, 2, 200)
	if errCode != 0 {
		t.Fatalf("epoch bump on tp0: errCode=%d", errCode)
	}

	// tp1 sequence window was cleared — seq=0 should work at new epoch.
	errCode, _, _ = m.ValidateAndDedup(1, 1, tp1, 0, 1, 300)
	if errCode != 0 {
		t.Fatalf("tp1 post-bump: errCode=%d", errCode)
	}
}

func TestValidateAndDedupReplayThenEpochBump(t *testing.T) {
	t.Parallel()
	m := NewProducerIDManager()
	tp := TopicPartition{Topic: "t", Partition: 0}

	// Simulate WAL replay: PID 2 at epoch 0 with sequences up to 10000.
	m.ReplayBatch(tp, BatchMeta{ProducerID: 2, ProducerEpoch: 0, BaseSequence: 0, NumRecords: 5000}, 0)
	m.ReplayBatch(tp, BatchMeta{ProducerID: 2, ProducerEpoch: 0, BaseSequence: 5000, NumRecords: 5000}, 5000)

	// After failover, client sends epoch=1 seq=0 (KIP-360).
	errCode, isDup, _ := m.ValidateAndDedup(2, 1, tp, 0, 5, 50000)
	if errCode != 0 {
		t.Fatalf("post-failover epoch bump: errCode=%d, want 0", errCode)
	}
	if isDup {
		t.Fatal("should not be a duplicate")
	}

	// Continue producing at new epoch.
	errCode, _, _ = m.ValidateAndDedup(2, 1, tp, 5, 5, 50005)
	if errCode != 0 {
		t.Fatalf("continued produce: errCode=%d", errCode)
	}
}

func TestValidateAndDedupFencesLowerEpoch(t *testing.T) {
	t.Parallel()
	m := NewProducerIDManager()
	tp := TopicPartition{Topic: "t", Partition: 0}

	m.ReplayBatch(tp, BatchMeta{ProducerID: 1, ProducerEpoch: 3, BaseSequence: 0, NumRecords: 5}, 0)

	// Lower epoch should be fenced.
	errCode, _, _ := m.ValidateAndDedup(1, 2, tp, 0, 1, 100)
	if errCode != 90 {
		t.Fatalf("errCode = %d, want 90 (PRODUCER_FENCED)", errCode)
	}
}

func TestReplayBatchMultiplePartitions(t *testing.T) {
	t.Parallel()
	m := NewProducerIDManager()
	tp0 := TopicPartition{Topic: "t", Partition: 0}
	tp1 := TopicPartition{Topic: "t", Partition: 1}

	m.ReplayBatch(tp0, BatchMeta{ProducerID: 1, ProducerEpoch: 0, BaseSequence: 0, NumRecords: 5}, 0)
	m.ReplayBatch(tp1, BatchMeta{ProducerID: 1, ProducerEpoch: 0, BaseSequence: 0, NumRecords: 3}, 100)

	// Dedup on each partition independently.
	_, isDup, _ := m.ValidateAndDedup(1, 0, tp0, 0, 5, 999)
	if !isDup {
		t.Fatal("expected dup on tp0")
	}
	_, isDup, _ = m.ValidateAndDedup(1, 0, tp1, 0, 3, 999)
	if !isDup {
		t.Fatal("expected dup on tp1")
	}

	// Next sequence on each partition accepted.
	errCode, isDup, _ := m.ValidateAndDedup(1, 0, tp0, 5, 1, 5)
	if errCode != 0 || isDup {
		t.Fatalf("tp0 seq=5: errCode=%d isDup=%v", errCode, isDup)
	}
	errCode, isDup, _ = m.ValidateAndDedup(1, 0, tp1, 3, 1, 103)
	if errCode != 0 || isDup {
		t.Fatalf("tp1 seq=3: errCode=%d isDup=%v", errCode, isDup)
	}
}

func TestIsCommitControlBatch(t *testing.T) {
	t.Parallel()

	commitBatch := BuildControlBatch(42, 0, true, 1000)
	if !IsCommitControlBatch(commitBatch) {
		t.Fatal("expected commit control batch to return true")
	}

	abortBatch := BuildControlBatch(42, 0, false, 1000)
	if IsCommitControlBatch(abortBatch) {
		t.Fatal("expected abort control batch to return false")
	}
}

func TestIsCommitControlBatchNonControl(t *testing.T) {
	t.Parallel()

	// A regular (non-control) batch should return false.
	raw := make([]byte, 65)
	raw[16] = 2 // magic
	// attributes = 0 (no control bit)
	if IsCommitControlBatch(raw) {
		t.Fatal("expected non-control batch to return false")
	}
}

func TestIsCommitControlBatchTooShort(t *testing.T) {
	t.Parallel()

	if IsCommitControlBatch(nil) {
		t.Fatal("expected nil to return false")
	}
	if IsCommitControlBatch(make([]byte, 30)) {
		t.Fatal("expected short batch to return false")
	}
}

// TestValidateAndDedupRegistersUnknownPID verifies that when a produce arrives
// for an unknown PID (e.g., after failover), the PID is registered and
// subsequent duplicate batches are detected.
func TestValidateAndDedupRegistersUnknownPID(t *testing.T) {
	t.Parallel()
	m := NewProducerIDManager()
	tp := TopicPartition{Topic: "t", Partition: 0}

	// PID 42 was never registered via InitProducerID or ReplayBatch.
	// First batch should be accepted.
	errCode, isDup, _ := m.ValidateAndDedup(42, 0, tp, 100, 5, 500)
	if errCode != 0 {
		t.Fatalf("first batch: errCode=%d, want 0", errCode)
	}
	if isDup {
		t.Fatal("first batch should not be a duplicate")
	}

	// Verify PID was registered.
	ps := m.GetProducer(42)
	if ps == nil {
		t.Fatal("PID 42 should be registered after first ValidateAndDedup")
	}
	if ps.Epoch != 0 {
		t.Fatalf("Epoch = %d, want 0", ps.Epoch)
	}

	// Retry of the same batch should be detected as duplicate.
	errCode, isDup, dupOff := m.ValidateAndDedup(42, 0, tp, 100, 5, 999)
	if errCode != 0 {
		t.Fatalf("retry batch: errCode=%d, want 0", errCode)
	}
	if !isDup {
		t.Fatal("retry batch should be detected as duplicate")
	}
	if dupOff != 500 {
		t.Fatalf("dupOffset = %d, want 500", dupOff)
	}

	// Next sequence should be accepted.
	errCode, isDup, _ = m.ValidateAndDedup(42, 0, tp, 105, 5, 505)
	if errCode != 0 {
		t.Fatalf("next batch: errCode=%d, want 0", errCode)
	}
	if isDup {
		t.Fatal("next batch should not be a duplicate")
	}
}

// TestValidateAndDedupUnknownPIDDifferentPartitions verifies that an unknown
// PID producing to a second partition gets a new SequenceWindow for that
// partition without interfering with the first.
func TestValidateAndDedupUnknownPIDDifferentPartitions(t *testing.T) {
	t.Parallel()
	m := NewProducerIDManager()
	tp0 := TopicPartition{Topic: "t", Partition: 0}
	tp1 := TopicPartition{Topic: "t", Partition: 1}

	// First produce on tp0 — registers PID.
	errCode, isDup, _ := m.ValidateAndDedup(10, 0, tp0, 50, 5, 0)
	if errCode != 0 || isDup {
		t.Fatalf("tp0 first: errCode=%d isDup=%v", errCode, isDup)
	}

	// First produce on tp1 — PID exists now but no window for tp1.
	// Should be accepted and create a new window.
	errCode, isDup, _ = m.ValidateAndDedup(10, 0, tp1, 0, 3, 100)
	if errCode != 0 || isDup {
		t.Fatalf("tp1 first: errCode=%d isDup=%v", errCode, isDup)
	}

	// Retry on tp0 should be deduped.
	errCode, isDup, _ = m.ValidateAndDedup(10, 0, tp0, 50, 5, 999)
	if errCode != 0 {
		t.Fatalf("tp0 retry: errCode=%d", errCode)
	}
	if !isDup {
		t.Fatal("tp0 retry should be detected as duplicate")
	}
}

func TestExpiredTransactionsReturnsExpired(t *testing.T) {
	t.Parallel()

	fc := clock.NewFakeClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	m := NewProducerIDManager()
	m.SetClock(fc)

	// Create a transactional producer with 5s timeout.
	pid, epoch, _ := m.InitProducerID("txn-1", 5000)
	tp := TopicPartition{Topic: "t", Partition: 0}
	if ec := m.AddPartitionsToTxn(pid, epoch, []TopicPartition{tp}); ec != 0 {
		t.Fatalf("setup AddPartitionsToTxn: errCode=%d", ec)
	}

	// Not expired yet.
	expired := m.ExpiredTransactions()
	if len(expired) != 0 {
		t.Fatalf("expected 0 expired, got %d", len(expired))
	}

	// Advance past the timeout.
	fc.Advance(6 * time.Second)

	expired = m.ExpiredTransactions()
	if len(expired) != 1 {
		t.Fatalf("expected 1 expired, got %d", len(expired))
	}
	if expired[0].ProducerID != pid {
		t.Fatalf("expired producer ID = %d, want %d", expired[0].ProducerID, pid)
	}
}

func TestExpiredTransactionsIgnoresCompletedTxns(t *testing.T) {
	t.Parallel()

	fc := clock.NewFakeClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	m := NewProducerIDManager()
	m.SetClock(fc)

	pid, epoch, _ := m.InitProducerID("txn-2", 5000)
	tp := TopicPartition{Topic: "t", Partition: 0}
	if ec := m.AddPartitionsToTxn(pid, epoch, []TopicPartition{tp}); ec != 0 {
		t.Fatalf("setup AddPartitionsToTxn: errCode=%d", ec)
	}

	// Complete the transaction.
	if _, ec := m.PrepareEndTxn(pid, epoch, true); ec != 0 {
		t.Fatalf("setup PrepareEndTxn: errCode=%d", ec)
	}
	if ec := m.FinalizeEndTxn(pid, epoch, true, true); ec != 0 {
		t.Fatalf("setup FinalizeEndTxn: errCode=%d", ec)
	}

	// Advance past timeout.
	fc.Advance(6 * time.Second)

	expired := m.ExpiredTransactions()
	if len(expired) != 0 {
		t.Fatalf("expected 0 expired after commit, got %d", len(expired))
	}
}

func TestExpiredTransactionsIgnoresZeroTimeout(t *testing.T) {
	t.Parallel()

	fc := clock.NewFakeClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	m := NewProducerIDManager()
	m.SetClock(fc)

	// Timeout of 0 should not be treated as expired.
	pid, epoch, _ := m.InitProducerID("txn-3", 0)
	tp := TopicPartition{Topic: "t", Partition: 0}
	if ec := m.AddPartitionsToTxn(pid, epoch, []TopicPartition{tp}); ec != 0 {
		t.Fatalf("setup AddPartitionsToTxn: errCode=%d", ec)
	}

	fc.Advance(time.Hour)

	expired := m.ExpiredTransactions()
	if len(expired) != 0 {
		t.Fatalf("expected 0 expired with zero timeout, got %d", len(expired))
	}
}

// --- StoreTxnOffset tests ---

func TestStoreTxnOffsetValid(t *testing.T) {
	t.Parallel()
	m := NewProducerIDManager()

	pid, epoch, _ := m.InitProducerID("txn-store", 5000)
	tp := TopicPartition{Topic: "t", Partition: 0}
	if ec := m.AddPartitionsToTxn(pid, epoch, []TopicPartition{tp}); ec != 0 {
		t.Fatalf("setup AddPartitionsToTxn: errCode=%d", ec)
	}

	errCode := m.StoreTxnOffset(pid, epoch, "group-1", "t", 0, 42, 0, "meta")
	if errCode != 0 {
		t.Fatalf("StoreTxnOffset: errCode=%d, want 0", errCode)
	}

	ps := m.GetProducer(pid)
	gOffsets := ps.TxnOffsets["group-1"]
	pending, ok := gOffsets[TopicPartition{Topic: "t", Partition: 0}]
	if !ok {
		t.Fatal("expected pending offset for group-1, t-0")
	}
	if pending.Offset != 42 || pending.Metadata != "meta" {
		t.Fatalf("pending = %+v, want offset=42 metadata=meta", pending)
	}
}

func TestStoreTxnOffsetUnknownPID(t *testing.T) {
	t.Parallel()
	m := NewProducerIDManager()

	errCode := m.StoreTxnOffset(999, 0, "group-1", "t", 0, 42, 0, "")
	if errCode != 3 {
		t.Fatalf("StoreTxnOffset unknown PID: errCode=%d, want 3", errCode)
	}
}

func TestStoreTxnOffsetFencedEpoch(t *testing.T) {
	t.Parallel()
	m := NewProducerIDManager()

	pid, _, _ := m.InitProducerID("txn-fence", 5000)
	// Re-init to bump epoch to 1.
	_, epoch, _ := m.InitProducerID("txn-fence", 5000)
	tp := TopicPartition{Topic: "t", Partition: 0}
	if ec := m.AddPartitionsToTxn(pid, epoch, []TopicPartition{tp}); ec != 0 {
		t.Fatalf("setup AddPartitionsToTxn: errCode=%d", ec)
	}

	// Old epoch (0) should be fenced.
	errCode := m.StoreTxnOffset(pid, 0, "group-1", "t", 0, 42, 0, "")
	if errCode != 90 {
		t.Fatalf("StoreTxnOffset fenced: errCode=%d, want 90 (PRODUCER_FENCED)", errCode)
	}
}

func TestStoreTxnOffsetInvalidEpoch(t *testing.T) {
	t.Parallel()
	m := NewProducerIDManager()

	pid, epoch, _ := m.InitProducerID("txn-inv-epoch", 5000)
	tp := TopicPartition{Topic: "t", Partition: 0}
	if ec := m.AddPartitionsToTxn(pid, epoch, []TopicPartition{tp}); ec != 0 {
		t.Fatalf("setup AddPartitionsToTxn: errCode=%d", ec)
	}

	// Higher epoch than current should return INVALID_PRODUCER_EPOCH.
	errCode := m.StoreTxnOffset(pid, epoch+1, "group-1", "t", 0, 42, 0, "")
	if errCode != 47 {
		t.Fatalf("StoreTxnOffset invalid epoch: errCode=%d, want 47 (INVALID_PRODUCER_EPOCH)", errCode)
	}
}

func TestStoreTxnOffsetNotOngoing(t *testing.T) {
	t.Parallel()
	m := NewProducerIDManager()

	pid, epoch, _ := m.InitProducerID("txn-not-ongoing", 5000)

	// No AddPartitionsToTxn → TxnState is TxnNone.
	errCode := m.StoreTxnOffset(pid, epoch, "group-1", "t", 0, 42, 0, "")
	if errCode != 53 {
		t.Fatalf("StoreTxnOffset not ongoing: errCode=%d, want 53 (INVALID_TXN_STATE)", errCode)
	}
}

func TestStoreTxnOffsetMultipleGroups(t *testing.T) {
	t.Parallel()
	m := NewProducerIDManager()

	pid, epoch, _ := m.InitProducerID("txn-multi-grp", 5000)
	tp := TopicPartition{Topic: "t", Partition: 0}
	if ec := m.AddPartitionsToTxn(pid, epoch, []TopicPartition{tp}); ec != 0 {
		t.Fatalf("setup AddPartitionsToTxn: errCode=%d", ec)
	}

	if errCode := m.StoreTxnOffset(pid, epoch, "group-a", "t", 0, 10, 0, ""); errCode != 0 {
		t.Fatalf("StoreTxnOffset group-a: errCode=%d", errCode)
	}
	if errCode := m.StoreTxnOffset(pid, epoch, "group-b", "t", 0, 20, 0, ""); errCode != 0 {
		t.Fatalf("StoreTxnOffset group-b: errCode=%d", errCode)
	}

	ps := m.GetProducer(pid)
	if len(ps.TxnOffsets) != 2 {
		t.Fatalf("expected 2 groups in TxnOffsets, got %d", len(ps.TxnOffsets))
	}
	if ps.TxnOffsets["group-a"][TopicPartition{Topic: "t", Partition: 0}].Offset != 10 {
		t.Fatal("group-a offset mismatch")
	}
	if ps.TxnOffsets["group-b"][TopicPartition{Topic: "t", Partition: 0}].Offset != 20 {
		t.Fatal("group-b offset mismatch")
	}
}

// --- AddOffsetsToTxn tests ---

func TestAddOffsetsToTxnValid(t *testing.T) {
	t.Parallel()

	fc := clock.NewFakeClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	m := NewProducerIDManager()
	m.SetClock(fc)

	pid, epoch, _ := m.InitProducerID("txn-offsets", 5000)
	tp := TopicPartition{Topic: "t", Partition: 0}
	if ec := m.AddPartitionsToTxn(pid, epoch, []TopicPartition{tp}); ec != 0 {
		t.Fatalf("setup AddPartitionsToTxn: errCode=%d", ec)
	}

	errCode := m.AddOffsetsToTxn(pid, epoch, "consumer-group")
	if errCode != 0 {
		t.Fatalf("AddOffsetsToTxn: errCode=%d, want 0", errCode)
	}

	ps := m.GetProducer(pid)
	if len(ps.TxnGroups) != 1 || ps.TxnGroups[0] != "consumer-group" {
		t.Fatalf("TxnGroups = %v, want [consumer-group]", ps.TxnGroups)
	}
}

func TestAddOffsetsToTxnStartsTxn(t *testing.T) {
	t.Parallel()

	fc := clock.NewFakeClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	m := NewProducerIDManager()
	m.SetClock(fc)

	pid, epoch, _ := m.InitProducerID("txn-auto-start", 5000)

	// Call AddOffsetsToTxn without prior AddPartitionsToTxn — should start txn.
	errCode := m.AddOffsetsToTxn(pid, epoch, "group-1")
	if errCode != 0 {
		t.Fatalf("AddOffsetsToTxn: errCode=%d, want 0", errCode)
	}

	ps := m.GetProducer(pid)
	if ps.TxnState != TxnOngoing {
		t.Fatalf("TxnState = %d, want TxnOngoing (%d)", ps.TxnState, TxnOngoing)
	}
}

func TestAddOffsetsToTxnUnknownPID(t *testing.T) {
	t.Parallel()
	m := NewProducerIDManager()

	errCode := m.AddOffsetsToTxn(999, 0, "group-1")
	if errCode != 3 {
		t.Fatalf("AddOffsetsToTxn unknown PID: errCode=%d, want 3", errCode)
	}
}

func TestAddOffsetsToTxnFencedEpoch(t *testing.T) {
	t.Parallel()
	m := NewProducerIDManager()

	pid, _, _ := m.InitProducerID("txn-aot-fence", 5000)
	_, epoch, _ := m.InitProducerID("txn-aot-fence", 5000)
	_ = epoch

	errCode := m.AddOffsetsToTxn(pid, 0, "group-1")
	if errCode != 90 {
		t.Fatalf("AddOffsetsToTxn fenced: errCode=%d, want 90", errCode)
	}
}

func TestAddOffsetsToTxnInvalidEpoch(t *testing.T) {
	t.Parallel()
	m := NewProducerIDManager()

	pid, epoch, _ := m.InitProducerID("txn-aot-inv", 5000)

	errCode := m.AddOffsetsToTxn(pid, epoch+1, "group-1")
	if errCode != 47 {
		t.Fatalf("AddOffsetsToTxn invalid epoch: errCode=%d, want 47", errCode)
	}
}

func TestAddOffsetsToTxnNonTransactional(t *testing.T) {
	t.Parallel()
	m := NewProducerIDManager()

	pid, epoch, _ := m.InitProducerID("", 0)

	errCode := m.AddOffsetsToTxn(pid, epoch, "group-1")
	if errCode != 49 {
		t.Fatalf("AddOffsetsToTxn non-transactional: errCode=%d, want 49", errCode)
	}
}

// --- PrepareEndTxn tests ---

func TestPrepareEndTxnCommit(t *testing.T) {
	t.Parallel()
	m := NewProducerIDManager()

	pid, epoch, _ := m.InitProducerID("txn-end", 5000)
	tp := TopicPartition{Topic: "t", Partition: 0}
	if ec := m.AddPartitionsToTxn(pid, epoch, []TopicPartition{tp}); ec != 0 {
		t.Fatalf("setup AddPartitionsToTxn: errCode=%d", ec)
	}
	if ec := m.AddOffsetsToTxn(pid, epoch, "group-1"); ec != 0 {
		t.Fatalf("setup AddOffsetsToTxn: errCode=%d", ec)
	}
	if ec := m.StoreTxnOffset(pid, epoch, "group-1", "t", 0, 50, 0, ""); ec != 0 {
		t.Fatalf("setup StoreTxnOffset: errCode=%d", ec)
	}
	m.RecordTxnBatch(pid, "t", 0, 100)

	state, errCode := m.PrepareEndTxn(pid, epoch, true)
	if errCode != 0 {
		t.Fatalf("PrepareEndTxn: errCode=%d, want 0", errCode)
	}
	if !state.Commit {
		t.Fatal("expected commit=true")
	}
	if len(state.TxnPartitions) != 1 {
		t.Fatalf("TxnPartitions count=%d, want 1", len(state.TxnPartitions))
	}
	if len(state.TxnGroups) != 1 || state.TxnGroups[0] != "group-1" {
		t.Fatalf("TxnGroups = %v, want [group-1]", state.TxnGroups)
	}
	if state.TxnOffsets["group-1"][TopicPartition{Topic: "t", Partition: 0}].Offset != 50 {
		t.Fatal("expected offset 50 in end state")
	}
	if state.TxnFirstOffsets[tp] != 100 {
		t.Fatalf("TxnFirstOffsets[tp] = %d, want 100", state.TxnFirstOffsets[tp])
	}

	// Producer is now in the ending phase until finalize succeeds.
	ps := m.GetProducer(pid)
	if ps.TxnState != TxnEnding {
		t.Fatalf("TxnState after prepare = %d, want TxnEnding", ps.TxnState)
	}

	if ec := m.FinalizeEndTxn(pid, epoch, true, true); ec != 0 {
		t.Fatalf("FinalizeEndTxn: errCode=%d", ec)
	}
	ps = m.GetProducer(pid)
	if ps.TxnState != TxnNone {
		t.Fatalf("TxnState after finalize = %d, want TxnNone", ps.TxnState)
	}
	if ps.LastWasCommit != true {
		t.Fatal("LastWasCommit should be true")
	}
}

func TestPrepareEndTxnRetryIdempotent(t *testing.T) {
	t.Parallel()
	m := NewProducerIDManager()

	pid, epoch, _ := m.InitProducerID("txn-retry", 5000)
	tp := TopicPartition{Topic: "t", Partition: 0}
	if ec := m.AddPartitionsToTxn(pid, epoch, []TopicPartition{tp}); ec != 0 {
		t.Fatalf("setup AddPartitionsToTxn: errCode=%d", ec)
	}

	// First EndTxn commit enters TxnEnding.
	_, errCode := m.PrepareEndTxn(pid, epoch, true)
	if errCode != 0 {
		t.Fatalf("first PrepareEndTxn: errCode=%d", errCode)
	}

	// Retry with same commit flag while ending should return the pending txn state.
	state, errCode := m.PrepareEndTxn(pid, epoch, true)
	if errCode != 0 {
		t.Fatalf("retry PrepareEndTxn (same flag): errCode=%d, want 0", errCode)
	}
	if state == nil {
		t.Fatal("expected non-nil state on idempotent retry")
	}
	if !state.Commit {
		t.Fatal("expected commit=true on idempotent retry")
	}
	if len(state.TxnPartitions) != 1 {
		t.Fatalf("expected 1 TxnPartition on retry before finalize, got %d", len(state.TxnPartitions))
	}

	if ec := m.FinalizeEndTxn(pid, epoch, true, true); ec != 0 {
		t.Fatalf("FinalizeEndTxn: errCode=%d", ec)
	}

	// Retry after successful finalize is idempotent and empty.
	state, errCode = m.PrepareEndTxn(pid, epoch, true)
	if errCode != 0 {
		t.Fatalf("post-finalize retry PrepareEndTxn: errCode=%d, want 0", errCode)
	}
	if len(state.TxnPartitions) != 0 {
		t.Fatalf("expected empty TxnPartitions after finalize, got %d", len(state.TxnPartitions))
	}
}

func TestPrepareEndTxnRetryDifferentFlag(t *testing.T) {
	t.Parallel()
	m := NewProducerIDManager()

	pid, epoch, _ := m.InitProducerID("txn-diff", 5000)
	tp := TopicPartition{Topic: "t", Partition: 0}
	if ec := m.AddPartitionsToTxn(pid, epoch, []TopicPartition{tp}); ec != 0 {
		t.Fatalf("setup AddPartitionsToTxn: errCode=%d", ec)
	}

	// Enter ending with commit.
	_, errCode := m.PrepareEndTxn(pid, epoch, true)
	if errCode != 0 {
		t.Fatalf("first commit: errCode=%d", errCode)
	}

	// Retry with different flag (abort) — should return INVALID_TXN_STATE.
	state, errCode := m.PrepareEndTxn(pid, epoch, false)
	if errCode != 53 {
		t.Fatalf("retry with different flag: errCode=%d, want 53 (INVALID_TXN_STATE)", errCode)
	}
	if state != nil {
		t.Fatal("expected nil state on INVALID_TXN_STATE")
	}
}

func TestPrepareEndTxnAbortRetryIdempotent(t *testing.T) {
	t.Parallel()
	m := NewProducerIDManager()

	pid, epoch, _ := m.InitProducerID("txn-abort-retry", 5000)
	tp := TopicPartition{Topic: "t", Partition: 0}
	if ec := m.AddPartitionsToTxn(pid, epoch, []TopicPartition{tp}); ec != 0 {
		t.Fatalf("setup AddPartitionsToTxn: errCode=%d", ec)
	}

	// First EndTxn abort enters TxnEnding.
	_, errCode := m.PrepareEndTxn(pid, epoch, false)
	if errCode != 0 {
		t.Fatalf("first abort: errCode=%d", errCode)
	}

	// Retry abort — should be idempotent.
	state, errCode := m.PrepareEndTxn(pid, epoch, false)
	if errCode != 0 {
		t.Fatalf("retry abort: errCode=%d, want 0", errCode)
	}
	if state.Commit {
		t.Fatal("expected commit=false on abort retry")
	}
}

func TestFinalizeEndTxnFailureRestoresOngoing(t *testing.T) {
	t.Parallel()
	m := NewProducerIDManager()

	pid, epoch, _ := m.InitProducerID("txn-finalize-fail", 5000)
	tp := TopicPartition{Topic: "t", Partition: 0}
	if ec := m.AddPartitionsToTxn(pid, epoch, []TopicPartition{tp}); ec != 0 {
		t.Fatalf("setup AddPartitionsToTxn: errCode=%d", ec)
	}

	_, ec := m.PrepareEndTxn(pid, epoch, true)
	if ec != 0 {
		t.Fatalf("PrepareEndTxn: errCode=%d", ec)
	}

	if ec := m.FinalizeEndTxn(pid, epoch, true, false); ec != 0 {
		t.Fatalf("FinalizeEndTxn failure: errCode=%d", ec)
	}

	ps := m.GetProducer(pid)
	if ps.TxnState != TxnOngoing {
		t.Fatalf("TxnState after failed finalize = %d, want TxnOngoing", ps.TxnState)
	}
	if !ps.TxnPartitions[tp] {
		t.Fatalf("TxnPartitions missing %v after failed finalize", tp)
	}

	state, ec := m.PrepareEndTxn(pid, epoch, true)
	if ec != 0 {
		t.Fatalf("retry PrepareEndTxn after failed finalize: errCode=%d", ec)
	}
	if len(state.TxnPartitions) != 1 {
		t.Fatalf("retry TxnPartitions = %d, want 1", len(state.TxnPartitions))
	}
}

func TestAddPartitionsToTxnRejectedWhileEnding(t *testing.T) {
	t.Parallel()
	m := NewProducerIDManager()

	pid, epoch, _ := m.InitProducerID("txn-ending-add", 5000)
	tp0 := TopicPartition{Topic: "t", Partition: 0}
	if ec := m.AddPartitionsToTxn(pid, epoch, []TopicPartition{tp0}); ec != 0 {
		t.Fatalf("setup AddPartitionsToTxn: errCode=%d", ec)
	}

	if _, ec := m.PrepareEndTxn(pid, epoch, true); ec != 0 {
		t.Fatalf("PrepareEndTxn: errCode=%d", ec)
	}

	tp1 := TopicPartition{Topic: "t", Partition: 1}
	if ec := m.AddPartitionsToTxn(pid, epoch, []TopicPartition{tp1}); ec != 53 {
		t.Fatalf("AddPartitionsToTxn while ending: errCode=%d, want 53", ec)
	}
}

func TestPrepareEndTxnUnknownPID(t *testing.T) {
	t.Parallel()
	m := NewProducerIDManager()

	state, errCode := m.PrepareEndTxn(999, 0, true)
	if errCode != 3 {
		t.Fatalf("PrepareEndTxn unknown PID: errCode=%d, want 3", errCode)
	}
	if state != nil {
		t.Fatal("expected nil state for unknown PID")
	}
}

func TestPrepareEndTxnFencedEpoch(t *testing.T) {
	t.Parallel()
	m := NewProducerIDManager()

	pid, _, _ := m.InitProducerID("txn-end-fence", 5000)
	_, epoch, _ := m.InitProducerID("txn-end-fence", 5000)
	tp := TopicPartition{Topic: "t", Partition: 0}
	if ec := m.AddPartitionsToTxn(pid, epoch, []TopicPartition{tp}); ec != 0 {
		t.Fatalf("setup AddPartitionsToTxn: errCode=%d", ec)
	}

	_, errCode := m.PrepareEndTxn(pid, 0, true)
	if errCode != 90 {
		t.Fatalf("PrepareEndTxn fenced: errCode=%d, want 90", errCode)
	}
}

func TestPrepareEndTxnInvalidEpoch(t *testing.T) {
	t.Parallel()
	m := NewProducerIDManager()

	pid, epoch, _ := m.InitProducerID("txn-end-inv", 5000)
	tp := TopicPartition{Topic: "t", Partition: 0}
	if ec := m.AddPartitionsToTxn(pid, epoch, []TopicPartition{tp}); ec != 0 {
		t.Fatalf("setup AddPartitionsToTxn: errCode=%d", ec)
	}

	_, errCode := m.PrepareEndTxn(pid, epoch+1, true)
	if errCode != 47 {
		t.Fatalf("PrepareEndTxn invalid epoch: errCode=%d, want 47", errCode)
	}
}

// --- RecordTxnBatch tests ---

func TestRecordTxnBatchTracksFirstOffset(t *testing.T) {
	t.Parallel()
	m := NewProducerIDManager()

	pid, epoch, _ := m.InitProducerID("txn-batch", 5000)
	tp := TopicPartition{Topic: "t", Partition: 0}
	if ec := m.AddPartitionsToTxn(pid, epoch, []TopicPartition{tp}); ec != 0 {
		t.Fatalf("setup AddPartitionsToTxn: errCode=%d", ec)
	}

	m.RecordTxnBatch(pid, "t", 0, 100)
	m.RecordTxnBatch(pid, "t", 0, 200)
	m.RecordTxnBatch(pid, "t", 0, 300)

	ps := m.GetProducer(pid)

	// TxnFirstOffsets should only record the first offset per partition.
	if first := ps.TxnFirstOffsets[tp]; first != 100 {
		t.Fatalf("TxnFirstOffsets[t-0] = %d, want 100", first)
	}

	// All batches should be tracked in TxnBatches.
	if len(ps.TxnBatches) != 3 {
		t.Fatalf("TxnBatches count = %d, want 3", len(ps.TxnBatches))
	}
}

func TestRecordTxnBatchMultiplePartitions(t *testing.T) {
	t.Parallel()
	m := NewProducerIDManager()

	pid, epoch, _ := m.InitProducerID("txn-batch-mp", 5000)
	tp0 := TopicPartition{Topic: "t", Partition: 0}
	tp1 := TopicPartition{Topic: "t", Partition: 1}
	if ec := m.AddPartitionsToTxn(pid, epoch, []TopicPartition{tp0, tp1}); ec != 0 {
		t.Fatalf("setup AddPartitionsToTxn: errCode=%d", ec)
	}

	m.RecordTxnBatch(pid, "t", 0, 10)
	m.RecordTxnBatch(pid, "t", 1, 20)
	m.RecordTxnBatch(pid, "t", 0, 50)

	ps := m.GetProducer(pid)
	if ps.TxnFirstOffsets[tp0] != 10 {
		t.Fatalf("TxnFirstOffsets[t-0] = %d, want 10", ps.TxnFirstOffsets[tp0])
	}
	if ps.TxnFirstOffsets[tp1] != 20 {
		t.Fatalf("TxnFirstOffsets[t-1] = %d, want 20", ps.TxnFirstOffsets[tp1])
	}
}

func TestRecordTxnBatchUnknownPID(t *testing.T) {
	t.Parallel()
	m := NewProducerIDManager()

	// Should be a no-op, not panic.
	m.RecordTxnBatch(999, "t", 0, 100)
}

// --- InitProducerID epoch overflow tests ---

func TestInitProducerIDEpochOverflow(t *testing.T) {
	t.Parallel()
	m := NewProducerIDManager()

	pid, _, _ := m.InitProducerID("txn-overflow", 5000)
	originalPID := pid

	// Manually set epoch to max int16 so next bump overflows.
	ps := m.GetProducer(pid)
	m.mu.Lock()
	ps.Epoch = 32766 // next InitProducerID will bump to 32767, then one more will overflow
	m.mu.Unlock()

	// Bump to 32767 (max int16).
	pid2, epoch2, errCode := m.InitProducerID("txn-overflow", 5000)
	if errCode != 0 {
		t.Fatalf("bump to max: errCode=%d", errCode)
	}
	if pid2 != originalPID {
		t.Fatalf("PID should still be %d, got %d", originalPID, pid2)
	}
	if epoch2 != 32767 {
		t.Fatalf("epoch = %d, want 32767", epoch2)
	}

	// Next bump: 32767+1 overflows int16 to negative → triggers new PID allocation.
	pid3, epoch3, errCode := m.InitProducerID("txn-overflow", 5000)
	if errCode != 0 {
		t.Fatalf("overflow bump: errCode=%d", errCode)
	}
	if pid3 == originalPID {
		t.Fatalf("expected new PID after overflow, still got %d", pid3)
	}
	if epoch3 != 0 {
		t.Fatalf("epoch after overflow = %d, want 0", epoch3)
	}

	// Old PID should be removed.
	if old := m.GetProducer(originalPID); old != nil {
		t.Fatal("old PID should be removed after overflow")
	}

	// New PID should be active.
	newPS := m.GetProducer(pid3)
	if newPS == nil {
		t.Fatal("new PID should be registered")
	}
	if newPS.TxnID != "txn-overflow" {
		t.Fatalf("new producer TxnID = %q, want txn-overflow", newPS.TxnID)
	}
}

func TestInitProducerIDReuseTxnIDResetsOngoingTxn(t *testing.T) {
	t.Parallel()

	fc := clock.NewFakeClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	m := NewProducerIDManager()
	m.SetClock(fc)

	pid, epoch, _ := m.InitProducerID("txn-reuse", 5000)
	tp := TopicPartition{Topic: "t", Partition: 0}
	if ec := m.AddPartitionsToTxn(pid, epoch, []TopicPartition{tp}); ec != 0 {
		t.Fatalf("setup AddPartitionsToTxn: errCode=%d", ec)
	}

	ps := m.GetProducer(pid)
	if ps.TxnState != TxnOngoing {
		t.Fatalf("TxnState = %d, want TxnOngoing", ps.TxnState)
	}

	// Re-init same txnID while txn is ongoing — should reset txn state and bump epoch.
	pid2, epoch2, _ := m.InitProducerID("txn-reuse", 5000)
	if pid2 != pid {
		t.Fatalf("PID changed on re-init: %d != %d", pid2, pid)
	}
	if epoch2 != epoch+1 {
		t.Fatalf("epoch = %d, want %d", epoch2, epoch+1)
	}

	ps = m.GetProducer(pid)
	if ps.TxnState != TxnNone {
		t.Fatalf("TxnState after re-init = %d, want TxnNone", ps.TxnState)
	}
	if len(ps.TxnPartitions) != 0 {
		t.Fatalf("TxnPartitions should be empty after re-init, got %d", len(ps.TxnPartitions))
	}
}

// --- AddPartitionsToTxn tests ---

func TestAddPartitionsToTxnHappyPath(t *testing.T) {
	t.Parallel()

	fc := clock.NewFakeClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	m := NewProducerIDManager()
	m.SetClock(fc)

	pid, epoch, _ := m.InitProducerID("txn-apt", 5000)
	tp0 := TopicPartition{Topic: "t", Partition: 0}
	tp1 := TopicPartition{Topic: "t", Partition: 1}

	errCode := m.AddPartitionsToTxn(pid, epoch, []TopicPartition{tp0, tp1})
	if errCode != 0 {
		t.Fatalf("AddPartitionsToTxn: errCode=%d, want 0", errCode)
	}

	ps := m.GetProducer(pid)
	if ps.TxnState != TxnOngoing {
		t.Fatalf("TxnState = %d, want TxnOngoing", ps.TxnState)
	}
	if !ps.TxnPartitions[tp0] || !ps.TxnPartitions[tp1] {
		t.Fatalf("expected both partitions in TxnPartitions, got %v", ps.TxnPartitions)
	}
}

func TestAddPartitionsToTxnUnknownPID(t *testing.T) {
	t.Parallel()
	m := NewProducerIDManager()

	errCode := m.AddPartitionsToTxn(999, 0, []TopicPartition{{Topic: "t", Partition: 0}})
	if errCode != 3 {
		t.Fatalf("AddPartitionsToTxn unknown PID: errCode=%d, want 3", errCode)
	}
}

func TestAddPartitionsToTxnFencedEpoch(t *testing.T) {
	t.Parallel()
	m := NewProducerIDManager()

	pid, _, _ := m.InitProducerID("txn-apt-fence", 5000)
	// Re-init bumps epoch to 1.
	_, epoch, _ := m.InitProducerID("txn-apt-fence", 5000)
	_ = epoch

	// Old epoch (0) should be fenced.
	errCode := m.AddPartitionsToTxn(pid, 0, []TopicPartition{{Topic: "t", Partition: 0}})
	if errCode != 90 {
		t.Fatalf("AddPartitionsToTxn fenced: errCode=%d, want 90 (PRODUCER_FENCED)", errCode)
	}
}

func TestAddPartitionsToTxnFutureEpoch(t *testing.T) {
	t.Parallel()
	m := NewProducerIDManager()

	pid, epoch, _ := m.InitProducerID("txn-apt-future", 5000)

	errCode := m.AddPartitionsToTxn(pid, epoch+1, []TopicPartition{{Topic: "t", Partition: 0}})
	if errCode != 47 {
		t.Fatalf("AddPartitionsToTxn future epoch: errCode=%d, want 47 (INVALID_PRODUCER_EPOCH)", errCode)
	}
}

func TestAddPartitionsToTxnNonTransactional(t *testing.T) {
	t.Parallel()
	m := NewProducerIDManager()

	// Non-transactional producer (empty txnID).
	pid, epoch, _ := m.InitProducerID("", 0)

	errCode := m.AddPartitionsToTxn(pid, epoch, []TopicPartition{{Topic: "t", Partition: 0}})
	if errCode != 49 {
		t.Fatalf("AddPartitionsToTxn non-transactional: errCode=%d, want 49", errCode)
	}
}

func TestAddPartitionsToTxnAddsToExisting(t *testing.T) {
	t.Parallel()
	m := NewProducerIDManager()

	pid, epoch, _ := m.InitProducerID("txn-apt-add", 5000)
	tp0 := TopicPartition{Topic: "t", Partition: 0}
	tp1 := TopicPartition{Topic: "t", Partition: 1}

	if ec := m.AddPartitionsToTxn(pid, epoch, []TopicPartition{tp0}); ec != 0 {
		t.Fatalf("AddPartitionsToTxn tp0: errCode=%d", ec)
	}
	if ec := m.AddPartitionsToTxn(pid, epoch, []TopicPartition{tp1}); ec != 0 {
		t.Fatalf("AddPartitionsToTxn tp1: errCode=%d", ec)
	}

	ps := m.GetProducer(pid)
	if len(ps.TxnPartitions) != 2 {
		t.Fatalf("expected 2 partitions, got %d", len(ps.TxnPartitions))
	}
	if !ps.TxnPartitions[tp0] || !ps.TxnPartitions[tp1] {
		t.Fatalf("expected both partitions, got %v", ps.TxnPartitions)
	}
}

// --- UpdateDedupOffset tests ---

func TestUpdateDedupOffset(t *testing.T) {
	t.Parallel()
	m := NewProducerIDManager()
	tp := TopicPartition{Topic: "t", Partition: 0}

	pid, _, _ := m.InitProducerID("", 0)

	// Produce a batch with a tentative offset of 0.
	errCode, isDup, _ := m.ValidateAndDedup(pid, 0, tp, 0, 5, 0)
	if errCode != 0 || isDup {
		t.Fatalf("initial produce: errCode=%d isDup=%v", errCode, isDup)
	}

	// Update the offset to the actual assigned value.
	m.UpdateDedupOffset(pid, tp, 0, 42)

	// Retry should return the updated offset.
	errCode, isDup, dupOff := m.ValidateAndDedup(pid, 0, tp, 0, 5, 999)
	if errCode != 0 {
		t.Fatalf("retry: errCode=%d", errCode)
	}
	if !isDup {
		t.Fatal("retry should be detected as duplicate")
	}
	if dupOff != 42 {
		t.Fatalf("dupOffset = %d, want 42", dupOff)
	}
}

func TestUpdateDedupOffsetUnknownPID(t *testing.T) {
	t.Parallel()
	m := NewProducerIDManager()

	// Should not panic.
	m.UpdateDedupOffset(999, TopicPartition{Topic: "t", Partition: 0}, 0, 100)
}

func TestUpdateDedupOffsetUnknownPartition(t *testing.T) {
	t.Parallel()
	m := NewProducerIDManager()

	pid, _, _ := m.InitProducerID("", 0)

	// Should not panic — PID exists but partition has no window.
	m.UpdateDedupOffset(pid, TopicPartition{Topic: "t", Partition: 99}, 0, 100)
}

// --- GetProducersForPartition tests ---

func TestGetProducersForPartition(t *testing.T) {
	t.Parallel()
	m := NewProducerIDManager()
	tp := TopicPartition{Topic: "t", Partition: 0}

	// Create two producers that have written to the same partition.
	pid1, _, _ := m.InitProducerID("", 0)
	if errCode, _, _ := m.ValidateAndDedup(pid1, 0, tp, 0, 5, 0); errCode != 0 {
		t.Fatalf("setup pid1 ValidateAndDedup: errCode=%d", errCode)
	}

	pid2, _, _ := m.InitProducerID("", 0)
	if errCode, _, _ := m.ValidateAndDedup(pid2, 0, tp, 0, 3, 100); errCode != 0 {
		t.Fatalf("setup pid2 ValidateAndDedup: errCode=%d", errCode)
	}

	// A third producer writing to a different partition.
	pid3, _, _ := m.InitProducerID("", 0)
	if errCode, _, _ := m.ValidateAndDedup(pid3, 0, TopicPartition{Topic: "t", Partition: 1}, 0, 1, 200); errCode != 0 {
		t.Fatalf("setup pid3 ValidateAndDedup: errCode=%d", errCode)
	}

	snaps := m.GetProducersForPartition("t", 0)
	if len(snaps) != 2 {
		t.Fatalf("expected 2 producers for t-0, got %d", len(snaps))
	}

	found := map[int64]ProducerSnapshot{}
	for _, s := range snaps {
		found[s.ProducerID] = s
	}

	s1, ok := found[pid1]
	if !ok {
		t.Fatalf("producer %d not found in results", pid1)
	}
	if s1.LastSequence != 4 {
		t.Fatalf("pid1 LastSequence = %d, want 4", s1.LastSequence)
	}

	s2, ok := found[pid2]
	if !ok {
		t.Fatalf("producer %d not found in results", pid2)
	}
	if s2.LastSequence != 2 {
		t.Fatalf("pid2 LastSequence = %d, want 2", s2.LastSequence)
	}
}

func TestGetProducersForPartitionWithTxn(t *testing.T) {
	t.Parallel()

	fc := clock.NewFakeClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	m := NewProducerIDManager()
	m.SetClock(fc)

	tp := TopicPartition{Topic: "t", Partition: 0}

	pid, epoch, _ := m.InitProducerID("txn-gpp", 5000)
	if ec := m.AddPartitionsToTxn(pid, epoch, []TopicPartition{tp}); ec != 0 {
		t.Fatalf("setup AddPartitionsToTxn: errCode=%d", ec)
	}
	if errCode, _, _ := m.ValidateAndDedup(pid, epoch, tp, 0, 5, 0); errCode != 0 {
		t.Fatalf("setup ValidateAndDedup: errCode=%d", errCode)
	}
	m.RecordTxnBatch(pid, "t", 0, 50)

	snaps := m.GetProducersForPartition("t", 0)
	if len(snaps) != 1 {
		t.Fatalf("expected 1 producer, got %d", len(snaps))
	}

	s := snaps[0]
	if s.ProducerID != pid {
		t.Fatalf("ProducerID = %d, want %d", s.ProducerID, pid)
	}
	if s.LastSequence != 4 {
		t.Fatalf("LastSequence = %d, want 4", s.LastSequence)
	}
	if s.TxnStartOffset != 50 {
		t.Fatalf("TxnStartOffset = %d, want 50", s.TxnStartOffset)
	}
	if s.TxnState != TxnOngoing {
		t.Fatalf("TxnState = %d, want TxnOngoing", s.TxnState)
	}
}

func TestGetProducersForPartitionEmpty(t *testing.T) {
	t.Parallel()
	m := NewProducerIDManager()

	snaps := m.GetProducersForPartition("nonexistent", 0)
	if len(snaps) != 0 {
		t.Fatalf("expected 0 producers, got %d", len(snaps))
	}
}

// --- Concurrent access race test ---

func TestProducerIDManagerConcurrentAccess(t *testing.T) {
	t.Parallel()
	m := NewProducerIDManager()

	const goroutines = 10
	const opsPerGoroutine = 50

	// Pre-create some transactional producers.
	var pids [goroutines]int64
	var epochs [goroutines]int16
	for i := range goroutines {
		txnID := ""
		if i%2 == 0 {
			txnID = "txn-" + string(rune('a'+i))
		}
		pids[i], epochs[i], _ = m.InitProducerID(txnID, 5000)
	}

	done := make(chan struct{})
	for i := range goroutines {
		go func(idx int) {
			defer func() { done <- struct{}{} }()
			pid := pids[idx]
			epoch := epochs[idx]
			tp := TopicPartition{Topic: "t", Partition: int32(idx % 3)}

			for j := range opsPerGoroutine {
				switch j % 5 {
				case 0:
					m.ValidateAndDedup(pid, epoch, tp, int32(j*5), 5, int64(j*100))
				case 1:
					m.GetProducersForPartition("t", int32(idx%3))
				case 2:
					if idx%2 == 0 {
						m.AddPartitionsToTxn(pid, epoch, []TopicPartition{tp})
					}
				case 3:
					if idx%2 == 0 {
						m.AddOffsetsToTxn(pid, epoch, "group-race")
					}
				case 4:
					m.UpdateDedupOffset(pid, tp, int32(j*5), int64(j*100+1))
				}
			}
		}(i)
	}

	for range goroutines {
		<-done
	}
}

// TestFinalizeEndTxnFailureKeepsTxnRetryable verifies that a failed EndTxn
// finalize keeps the txn in-progress so timeout sweeps can retry it.
func TestFinalizeEndTxnFailureKeepsTxnRetryable(t *testing.T) {
	t.Parallel()

	fc := clock.NewFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	m := NewProducerIDManager()
	m.SetClock(fc)

	pid, epoch, errCode := m.InitProducerID("txn-stall", 5000)
	if errCode != 0 {
		t.Fatalf("InitProducerID errCode=%d", errCode)
	}

	tp := TopicPartition{Topic: "test-topic", Partition: 0}
	if ec := m.AddPartitionsToTxn(pid, epoch, []TopicPartition{tp}); ec != 0 {
		t.Fatalf("AddPartitionsToTxn errCode=%d", ec)
	}
	m.RecordTxnBatch(pid, "test-topic", 0, 0)

	// Set up partition: HW at 10, open txn from offset 0.
	pd := &PartData{}
	pd.Lock()
	pd.SetHW(10)
	pd.AddOpenTxn(pid, 0)
	pd.Unlock()

	// LSO should be pinned at 0 (open txn blocks it).
	pd.RLock()
	if lso := pd.LSO(); lso != 0 {
		t.Fatalf("LSO before PrepareEndTxn = %d, want 0", lso)
	}
	pd.RUnlock()

	// Advance clock past timeout so the txn is expired.
	fc.Advance(6 * time.Second)

	expired := m.ExpiredTransactions()
	if len(expired) != 1 {
		t.Fatalf("expected 1 expired txn, got %d", len(expired))
	}

	// Prepare then fail finalize, simulating WAL failure before durable marker.
	_, ec := m.PrepareEndTxn(pid, epoch, false)
	if ec != 0 {
		t.Fatalf("PrepareEndTxn errCode=%d", ec)
	}
	if ec := m.FinalizeEndTxn(pid, epoch, false, false); ec != 0 {
		t.Fatalf("FinalizeEndTxn failure errCode=%d", ec)
	}

	// Simulate partition-side failure path: offsets can be skipped while open
	// transaction tracking remains until a later successful abort.
	pd.Lock()
	pd.SkipOffsets(10, 1) // skip the control batch offset
	pd.Unlock()

	// Txn is still retryable.
	expired = m.ExpiredTransactions()
	if len(expired) != 1 {
		t.Fatalf("expected 1 expired txn after failed finalize, got %d", len(expired))
	}

	// LSO remains pinned until RemoveOpenTxn is called by a successful abort.
	pd.RLock()
	lso := pd.LSO()
	openPIDs := pd.OpenTxnPIDs()
	pd.RUnlock()

	if lso != 0 {
		t.Fatalf("LSO = %d, want 0 (still stuck without RemoveOpenTxn)", lso)
	}
	if len(openPIDs) != 1 {
		t.Fatalf("expected 1 open txn PID still present, got %d", len(openPIDs))
	}

	// Fix: calling RemoveOpenTxn should unstick the LSO.
	pd.Lock()
	pd.RemoveOpenTxn(pid)
	pd.Unlock()

	pd.RLock()
	lsoAfter := pd.LSO()
	hwAfter := pd.HW()
	pd.RUnlock()

	// After RemoveOpenTxn, LSO should equal HW (11 due to SkipOffsets advancing past 10).
	if lsoAfter != hwAfter {
		t.Fatalf("LSO after RemoveOpenTxn = %d, want HW = %d", lsoAfter, hwAfter)
	}
}
