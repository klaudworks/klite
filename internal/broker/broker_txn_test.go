package broker

import (
	"log/slog"
	"testing"
	"time"

	"github.com/klaudworks/klite/internal/chunk"
	"github.com/klaudworks/klite/internal/clock"
	"github.com/klaudworks/klite/internal/cluster"
	"github.com/klaudworks/klite/internal/wal"
)

// setupTxnTestBroker creates a Broker with a WAL, PIDManager, and a single
// topic+partition ready for transaction testing. The returned FakeClock
// controls both the broker's clock and the PIDManager's clock.
func setupTxnTestBroker(t *testing.T, topic string) (*Broker, *cluster.PartData, *clock.FakeClock) {
	t.Helper()

	walDir := t.TempDir()
	topicID := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	logger := slog.Default()

	idx := wal.NewIndex()
	walCfg := wal.DefaultWriterConfig()
	walCfg.Dir = walDir
	walCfg.S3Configured = true
	walCfg.Logger = logger
	w, err := wal.NewWriter(walCfg, idx)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { w.Stop() })

	pool := chunk.NewPool(64*1024*1024, cluster.DefaultMaxMessageBytes)

	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	fc := clock.NewFakeClock(now)

	state := cluster.NewState(cluster.Config{
		NodeID:            0,
		DefaultPartitions: 1,
		AutoCreateTopics:  true,
	})
	state.SetClock(fc)

	td, _, err := state.GetOrCreateTopic(topic)
	if err != nil {
		t.Fatal(err)
	}
	td.ID = topicID
	pd := td.Partitions[0]
	pd.Lock()
	pd.TopicID = topicID
	pd.InitWAL(pool, w, idx)
	pd.Unlock()

	b := &Broker{
		walWriter: w,
		walIndex:  idx,
		chunkPool: pool,
		state:     state,
		logger:    logger,
		cfg:       Config{Clock: fc},
	}

	return b, pd, fc
}

// startTxn initializes a transactional producer, adds a partition to its
// transaction, and records a batch. Returns (pid, epoch).
func startTxn(t *testing.T, b *Broker, txnID, topic string, partition, timeoutMs int32, firstOffset int64) (int64, int16) {
	t.Helper()

	pid, epoch, errCode := b.state.PIDManager().InitProducerID(txnID, timeoutMs)
	if errCode != 0 {
		t.Fatalf("InitProducerID errCode=%d", errCode)
	}

	tp := cluster.TopicPartition{Topic: topic, Partition: partition}
	if ec := b.state.PIDManager().AddPartitionsToTxn(pid, epoch, []cluster.TopicPartition{tp}); ec != 0 {
		t.Fatalf("AddPartitionsToTxn errCode=%d", ec)
	}

	b.state.PIDManager().RecordTxnBatch(pid, topic, partition, firstOffset)

	return pid, epoch
}

func TestAbortExpiredTransactions_SingleExpired(t *testing.T) {
	t.Parallel()
	b, pd, fc := setupTxnTestBroker(t, "test-topic")

	pid, _ := startTxn(t, b, "txn-1", "test-topic", 0, 5000, 0)

	// Set up partition state: HW at 5, open txn from offset 0.
	pd.Lock()
	pd.SetHW(5)
	pd.AddOpenTxn(pid, 0)
	pd.Unlock()

	// LSO should be blocked at the open txn's first offset.
	pd.RLock()
	lso := pd.LSO()
	pd.RUnlock()
	if lso != 0 {
		t.Fatalf("LSO before abort = %d, want 0", lso)
	}

	// Advance clock past the 5s timeout.
	fc.Advance(6 * time.Second)

	b.abortExpiredTransactions()

	// After abort: open txn should be cleared, HW and LSO should advance.
	pd.RLock()
	openPIDs := pd.OpenTxnPIDs()
	hw := pd.HW()
	lsoAfter := pd.LSO()
	pd.RUnlock()

	if len(openPIDs) != 0 {
		t.Fatalf("expected no open txns after abort, got %d", len(openPIDs))
	}
	if hw <= 5 {
		t.Fatalf("HW = %d, should have advanced past 5 after abort control batch", hw)
	}
	if lsoAfter != hw {
		t.Fatalf("LSO = %d, want HW = %d (no more open txns)", lsoAfter, hw)
	}
}

func TestAbortExpiredTransactions_AbortedTxnIndex(t *testing.T) {
	t.Parallel()
	b, pd, fc := setupTxnTestBroker(t, "test-topic")

	pid, _ := startTxn(t, b, "txn-1", "test-topic", 0, 5000, 10)

	pd.Lock()
	pd.SetHW(20)
	pd.AddOpenTxn(pid, 10)
	pd.Unlock()

	fc.Advance(6 * time.Second)
	b.abortExpiredTransactions()

	// Verify aborted txn index was updated with correct FirstOffset.
	pd.RLock()
	hw := pd.HW()
	aborted := pd.AbortedTxnsInRange(10, hw)
	pd.RUnlock()

	if len(aborted) != 1 {
		t.Fatalf("expected 1 aborted txn entry, got %d", len(aborted))
	}
	if aborted[0].ProducerID != pid {
		t.Fatalf("aborted txn PID = %d, want %d", aborted[0].ProducerID, pid)
	}
	if aborted[0].FirstOffset != 10 {
		t.Fatalf("aborted txn FirstOffset = %d, want 10", aborted[0].FirstOffset)
	}
}

func TestAbortExpiredTransactions_MultipleExpired(t *testing.T) {
	t.Parallel()
	b, pd, fc := setupTxnTestBroker(t, "test-topic")

	pid1, _ := startTxn(t, b, "txn-1", "test-topic", 0, 5000, 0)
	pid2, _ := startTxn(t, b, "txn-2", "test-topic", 0, 5000, 3)

	pd.Lock()
	pd.SetHW(10)
	pd.AddOpenTxn(pid1, 0)
	pd.AddOpenTxn(pid2, 3)
	pd.Unlock()

	fc.Advance(6 * time.Second)
	b.abortExpiredTransactions()

	pd.RLock()
	openPIDs := pd.OpenTxnPIDs()
	hw := pd.HW()
	lso := pd.LSO()
	aborted := pd.AbortedTxnsInRange(0, hw)
	pd.RUnlock()

	if len(openPIDs) != 0 {
		t.Fatalf("expected no open txns, got %d", len(openPIDs))
	}
	// Both abort control batches should advance HW past 10.
	if hw <= 10 {
		t.Fatalf("HW = %d, want > 10 after two abort control batches", hw)
	}
	if lso != hw {
		t.Fatalf("LSO = %d, want HW = %d", lso, hw)
	}
	if len(aborted) != 2 {
		t.Fatalf("expected 2 aborted txn entries, got %d", len(aborted))
	}
}

func TestAbortExpiredTransactions_PrepareEndTxnError(t *testing.T) {
	t.Parallel()
	b, pd, fc := setupTxnTestBroker(t, "test-topic")

	// Start a transaction, then fence it by re-initializing (bumps epoch),
	// so PrepareEndTxn with the old epoch will fail.
	pid, epoch := startTxn(t, b, "txn-fence", "test-topic", 0, 5000, 0)

	pd.Lock()
	pd.SetHW(5)
	pd.AddOpenTxn(pid, 0)
	pd.Unlock()

	// Bump the epoch by re-initializing the same txnID.
	_, newEpoch, errCode := b.state.PIDManager().InitProducerID("txn-fence", 5000)
	if errCode != 0 {
		t.Fatalf("re-init errCode=%d", errCode)
	}
	if newEpoch <= epoch {
		t.Fatalf("expected epoch bump: old=%d new=%d", epoch, newEpoch)
	}

	fc.Advance(6 * time.Second)
	b.abortExpiredTransactions()

	// The expired transaction was skipped because PrepareEndTxn fails
	// (the txn was already reset by re-init). The open txn on the partition
	// may still be there (the sweep doesn't touch partition state on error).
	// The key point: no crash, no abort control batch written.
	pd.RLock()
	hw := pd.HW()
	pd.RUnlock()

	if hw != 5 {
		t.Fatalf("HW = %d, want 5 (no abort should have been written)", hw)
	}
}

func TestAbortExpiredTransactions_NonExistentPartition(t *testing.T) {
	t.Parallel()
	b, _, fc := setupTxnTestBroker(t, "test-topic")

	// Register a txn that references a topic that doesn't exist in state.
	pid, epoch, errCode := b.state.PIDManager().InitProducerID("txn-ghost", 5000)
	if errCode != 0 {
		t.Fatalf("InitProducerID errCode=%d", errCode)
	}

	ghostTP := cluster.TopicPartition{Topic: "nonexistent-topic", Partition: 0}
	if ec := b.state.PIDManager().AddPartitionsToTxn(pid, epoch, []cluster.TopicPartition{ghostTP}); ec != 0 {
		t.Fatalf("AddPartitionsToTxn errCode=%d", ec)
	}
	b.state.PIDManager().RecordTxnBatch(pid, "nonexistent-topic", 0, 0)

	fc.Advance(6 * time.Second)

	// Should not crash even though the topic/partition doesn't exist.
	b.abortExpiredTransactions()
}

func TestAbortExpiredTransactions_NoExpired(t *testing.T) {
	t.Parallel()
	b, pd, _ := setupTxnTestBroker(t, "test-topic")

	// Start a transaction but DON'T advance the clock past timeout.
	pid, _ := startTxn(t, b, "txn-active", "test-topic", 0, 5000, 0)

	pd.Lock()
	pd.SetHW(5)
	pd.AddOpenTxn(pid, 0)
	pd.Unlock()

	// Clock is still at t=0, timeout is 5s — not expired.
	b.abortExpiredTransactions()

	pd.RLock()
	openPIDs := pd.OpenTxnPIDs()
	hw := pd.HW()
	pd.RUnlock()

	if len(openPIDs) != 1 {
		t.Fatalf("expected 1 open txn (not expired), got %d", len(openPIDs))
	}
	if hw != 5 {
		t.Fatalf("HW = %d, want 5 (no abort written)", hw)
	}
}

func TestAbortExpiredTransactions_WALSubmitFailureKeepsTxnRetryable(t *testing.T) {
	t.Parallel()
	b, pd, fc := setupTxnTestBroker(t, "test-topic")

	pid, _ := startTxn(t, b, "txn-submit-fail", "test-topic", 0, 5000, 0)

	pd.Lock()
	pd.SetHW(5)
	pd.AddOpenTxn(pid, 0)
	pd.Unlock()

	fc.Advance(6 * time.Second)

	// Force AppendAsync submit failure.
	b.walWriter.Stop()
	b.abortExpiredTransactions()

	ps := b.state.PIDManager().GetProducer(pid)
	if ps.TxnState != cluster.TxnOngoing {
		t.Fatalf("TxnState after failed abort = %d, want TxnOngoing", ps.TxnState)
	}

	pd.RLock()
	openAfterFail := pd.OpenTxnPIDs()
	pd.RUnlock()
	if len(openAfterFail) != 1 {
		t.Fatalf("expected 1 open txn after failed abort, got %d", len(openAfterFail))
	}

	idx2 := wal.NewIndex()
	walCfg := wal.DefaultWriterConfig()
	walCfg.Dir = t.TempDir()
	walCfg.S3Configured = true
	walCfg.Logger = slog.Default()
	w2, err := wal.NewWriter(walCfg, idx2)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w2.Start(); err != nil {
		t.Fatalf("Start writer: %v", err)
	}
	t.Cleanup(func() { w2.Stop() })

	pd.Lock()
	pd.InitWAL(b.chunkPool, w2, idx2)
	pd.Unlock()
	b.walWriter = w2
	b.walIndex = idx2

	b.abortExpiredTransactions()

	pd.RLock()
	openAfterRetry := pd.OpenTxnPIDs()
	pd.RUnlock()
	if len(openAfterRetry) != 0 {
		t.Fatalf("expected no open txns after retry, got %d", len(openAfterRetry))
	}
}

func TestAbortExpiredTransactions_MultiPartitionTxn(t *testing.T) {
	t.Parallel()

	walDir := t.TempDir()
	logger := slog.Default()

	idx := wal.NewIndex()
	walCfg := wal.DefaultWriterConfig()
	walCfg.Dir = walDir
	walCfg.S3Configured = true
	walCfg.Logger = logger
	w, err := wal.NewWriter(walCfg, idx)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { w.Stop() })

	pool := chunk.NewPool(64*1024*1024, cluster.DefaultMaxMessageBytes)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	fc := clock.NewFakeClock(now)

	state := cluster.NewState(cluster.Config{
		NodeID:            0,
		DefaultPartitions: 3,
		AutoCreateTopics:  true,
	})
	state.SetClock(fc)

	td, _, err := state.GetOrCreateTopic("multi-part")
	if err != nil {
		t.Fatal(err)
	}
	topicID := [16]byte{2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17}
	td.ID = topicID
	for i := range td.Partitions {
		pd := td.Partitions[i]
		pd.Lock()
		pd.TopicID = topicID
		pd.InitWAL(pool, w, idx)
		pd.SetHW(10)
		pd.Unlock()
	}

	// Start a txn that spans partitions 0 and 2.
	pid, epoch, errCode := state.PIDManager().InitProducerID("txn-multi", 5000)
	if errCode != 0 {
		t.Fatalf("InitProducerID errCode=%d", errCode)
	}

	tps := []cluster.TopicPartition{
		{Topic: "multi-part", Partition: 0},
		{Topic: "multi-part", Partition: 2},
	}
	if ec := state.PIDManager().AddPartitionsToTxn(pid, epoch, tps); ec != 0 {
		t.Fatalf("AddPartitionsToTxn errCode=%d", ec)
	}
	state.PIDManager().RecordTxnBatch(pid, "multi-part", 0, 5)
	state.PIDManager().RecordTxnBatch(pid, "multi-part", 2, 7)

	// Track open txns on each partition.
	td.Partitions[0].Lock()
	td.Partitions[0].AddOpenTxn(pid, 5)
	td.Partitions[0].Unlock()
	td.Partitions[2].Lock()
	td.Partitions[2].AddOpenTxn(pid, 7)
	td.Partitions[2].Unlock()

	b := &Broker{
		walWriter: w,
		walIndex:  idx,
		chunkPool: pool,
		state:     state,
		logger:    logger,
		cfg:       Config{Clock: fc},
	}

	fc.Advance(6 * time.Second)
	b.abortExpiredTransactions()

	// Both partition 0 and 2 should have abort control batches.
	for _, pi := range []int{0, 2} {
		pd := td.Partitions[pi]
		pd.RLock()
		open := pd.OpenTxnPIDs()
		hw := pd.HW()
		pd.RUnlock()
		if len(open) != 0 {
			t.Fatalf("partition %d: expected no open txns, got %d", pi, len(open))
		}
		if hw <= 10 {
			t.Fatalf("partition %d: HW = %d, want > 10 after abort", pi, hw)
		}
	}

	// Partition 1 should be unaffected.
	pd1 := td.Partitions[1]
	pd1.RLock()
	hw1 := pd1.HW()
	pd1.RUnlock()
	if hw1 != 10 {
		t.Fatalf("partition 1: HW = %d, want 10 (unaffected)", hw1)
	}
}
