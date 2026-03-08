package cluster

import (
	"testing"
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
	m.ValidateAndDedup(1, 0, tp, 0, 5, 0)

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
	m.ValidateAndDedup(1, 0, tp0, 0, 5, 0)
	m.ValidateAndDedup(1, 0, tp1, 0, 3, 100)

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
	if errCode != 35 {
		t.Fatalf("errCode = %d, want 35 (PRODUCER_FENCED)", errCode)
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
