package broker

import (
	"context"
	"encoding/binary"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/klaudworks/klite/internal/chunk"
	"github.com/klaudworks/klite/internal/clock"
	"github.com/klaudworks/klite/internal/cluster"
	"github.com/klaudworks/klite/internal/handler"
	"github.com/klaudworks/klite/internal/wal"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func testHealthListener(t *testing.T) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return ln
}

func TestBrokerStartStop(t *testing.T) {
	t.Parallel()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	cfg := DefaultConfig()
	cfg.Listener = ln
	cfg.HealthListener = testHealthListener(t)
	cfg.DataDir = t.TempDir()
	cfg.LogLevel = "debug"

	b := New(cfg)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- b.Run(ctx)
	}()

	// Wait for broker to be ready
	select {
	case <-b.Ready():
	case <-time.After(5 * time.Second):
		t.Fatal("broker did not become ready within 5s")
	}

	// Verify meta.properties was created
	metaPath := filepath.Join(cfg.DataDir, "meta.properties")
	data, err := os.ReadFile(metaPath)
	if err != nil {
		t.Fatal("meta.properties not created:", err)
	}
	cid := parseClusterID(string(data))
	if cid == "" {
		t.Fatal("meta.properties has no cluster.id")
	}
	if len(cid) != 22 {
		t.Fatalf("cluster ID should be 22 chars (base64 UUID), got %d: %q", len(cid), cid)
	}

	// Verify broker returns the same cluster ID
	if b.ClusterID() != cid {
		t.Fatalf("broker ClusterID() = %q, meta.properties has %q", b.ClusterID(), cid)
	}

	// Verify we can connect
	conn, dialErr := net.DialTimeout("tcp", ln.Addr().String(), 2*time.Second)
	if dialErr != nil {
		t.Fatal("failed to connect:", dialErr)
	}
	_ = conn.Close()

	// Shutdown
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal("Run returned error:", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("broker did not stop within 5s")
	}
}

func TestBrokerReusesClusterID(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()

	// First start
	ln1, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	cfg := DefaultConfig()
	cfg.Listener = ln1
	cfg.HealthListener = testHealthListener(t)
	cfg.DataDir = dataDir
	cfg.LogLevel = "error"

	b1 := New(cfg)
	ctx1, cancel1 := context.WithCancel(context.Background())
	done1 := make(chan error, 1)
	go func() { done1 <- b1.Run(ctx1) }()

	cid1 := b1.ClusterID() // blocks until ready
	cancel1()
	<-done1

	// Second start with same data dir
	ln2, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	cfg.Listener = ln2
	cfg.HealthListener = testHealthListener(t)
	b2 := New(cfg)
	ctx2, cancel2 := context.WithCancel(context.Background())
	done2 := make(chan error, 1)
	go func() { done2 <- b2.Run(ctx2) }()

	cid2 := b2.ClusterID() // blocks until ready
	cancel2()
	<-done2

	if cid1 != cid2 {
		t.Fatalf("cluster ID changed across restarts: %q -> %q", cid1, cid2)
	}
}

func TestBrokerConfiguredClusterID(t *testing.T) {
	t.Parallel()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	cfg := DefaultConfig()
	cfg.Listener = ln
	cfg.HealthListener = testHealthListener(t)
	cfg.DataDir = t.TempDir()
	cfg.ClusterID = "my-custom-cluster-id-22"
	cfg.LogLevel = "error"

	b := New(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- b.Run(ctx) }()

	if b.ClusterID() != "my-custom-cluster-id-22" {
		t.Fatalf("expected custom cluster ID, got %q", b.ClusterID())
	}

	cancel()
	<-done
}

func TestResolveAdvertisedAddr(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		listen     string
		advertised string
		wantAddr   string
		wantWarn   bool
	}{
		{"explicit", ":9092", "myhost:9092", "myhost:9092", false},
		{"from listen host", "192.168.1.5:9092", "", "192.168.1.5:9092", false},
		{"wildcard", ":9092", "", "localhost:9092", true},
		{"0.0.0.0", "0.0.0.0:9092", "", "localhost:9092", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Config{Listen: tt.listen, AdvertisedAddr: tt.advertised}
			addr, warn := cfg.ResolveAdvertisedAddr()
			if addr != tt.wantAddr {
				t.Errorf("addr = %q, want %q", addr, tt.wantAddr)
			}
			if warn != tt.wantWarn {
				t.Errorf("warn = %v, want %v", warn, tt.wantWarn)
			}
		})
	}
}

func TestDefaultConfig(t *testing.T) {
	t.Parallel()
	cfg := DefaultConfig()
	if cfg.Listen != ":9092" {
		t.Errorf("Listen = %q, want %q", cfg.Listen, ":9092")
	}
	if cfg.DataDir != "./data" {
		t.Errorf("DataDir = %q, want %q", cfg.DataDir, "./data")
	}
	if cfg.NodeID != 0 {
		t.Errorf("NodeID = %d, want 0", cfg.NodeID)
	}
	if cfg.DefaultPartitions != 1 {
		t.Errorf("DefaultPartitions = %d, want 1", cfg.DefaultPartitions)
	}
	if !cfg.AutoCreateTopics {
		t.Error("AutoCreateTopics should be true by default")
	}
	if cfg.LogLevel != "info" {
		t.Errorf("LogLevel = %q, want %q", cfg.LogLevel, "info")
	}
	if cfg.HealthAddr != "" {
		t.Errorf("HealthAddr = %q, want %q (disabled by default)", cfg.HealthAddr, "")
	}
}

func TestLeaseTimingValidation(t *testing.T) {
	t.Parallel()

	// lease-duration < 2 * lease-renew-interval should fail
	cfg := Config{
		ReplicationAddr:    ":9093",
		S3Bucket:           "test-bucket",
		LeaseDuration:      5 * time.Second,
		LeaseRenewInterval: 5 * time.Second,
	}
	_, err := cfg.ValidateReplication()
	if err == nil {
		t.Fatal("expected error when lease-duration < 2 * lease-renew-interval")
	}

	// Exactly 2x should pass
	cfg.LeaseDuration = 10 * time.Second
	cfg.LeaseRenewInterval = 5 * time.Second
	_, err = cfg.ValidateReplication()
	if err != nil {
		t.Fatal("expected no error when lease-duration == 2 * lease-renew-interval:", err)
	}

	// Default values should pass
	cfg.LeaseDuration = 0
	cfg.LeaseRenewInterval = 0
	_, err = cfg.ValidateReplication()
	if err != nil {
		t.Fatal("expected no error with default values:", err)
	}
}

func TestLeaseTimingWarning(t *testing.T) {
	t.Parallel()

	cfg := Config{
		ReplicationAddr:    ":9093",
		S3Bucket:           "test-bucket",
		LeaseDuration:      15 * time.Second,
		LeaseRetryInterval: 20 * time.Second,
	}
	warnings, err := cfg.ValidateReplication()
	if err != nil {
		t.Fatal("unexpected error:", err)
	}
	if len(warnings) == 0 {
		t.Fatal("expected warning when lease-retry-interval > lease-duration")
	}
}

func TestReplicationRequiresS3(t *testing.T) {
	t.Parallel()

	cfg := Config{
		ReplicationAddr: ":9093",
	}
	_, err := cfg.ValidateReplication()
	if err == nil {
		t.Fatal("expected error when replication-addr set without s3-bucket")
	}
}

func TestReplicationNoReplicationAddr(t *testing.T) {
	t.Parallel()

	// No replication addr = no validation needed
	cfg := Config{}
	warnings, err := cfg.ValidateReplication()
	if err != nil {
		t.Fatal("unexpected error:", err)
	}
	if len(warnings) != 0 {
		t.Fatal("unexpected warnings:", warnings)
	}
}

// makeTestBatch builds a minimal 61-byte RecordBatch with the given offset
// and record count. The offset is embedded at bytes 0:8 (Kafka header format).
// Uses ProducerID=-1 (non-idempotent).
func makeTestBatch(baseOffset int64, numRecords int32) []byte {
	return makeTestBatchPID(baseOffset, numRecords, -1, -1, -1)
}

// makeTestBatchPID builds a 61-byte RecordBatch with explicit PID fields.
func makeTestBatchPID(baseOffset int64, numRecords int32, pid int64, epoch int16, baseSeq int32) []byte {
	raw := make([]byte, 61)
	binary.BigEndian.PutUint64(raw[0:8], uint64(baseOffset))
	binary.BigEndian.PutUint32(raw[8:12], 49) // batchLength
	raw[16] = 2                               // magic
	binary.BigEndian.PutUint32(raw[17:21], 0xDEADBEEF)
	lastDelta := numRecords - 1
	if lastDelta < 0 {
		lastDelta = 0
	}
	binary.BigEndian.PutUint32(raw[23:27], uint32(lastDelta))
	binary.BigEndian.PutUint64(raw[27:35], 1000)            // baseTimestamp
	binary.BigEndian.PutUint64(raw[35:43], 2000)            // maxTimestamp
	binary.BigEndian.PutUint64(raw[43:51], uint64(pid))     // producerID
	binary.BigEndian.PutUint16(raw[51:53], uint16(epoch))   // producerEpoch
	binary.BigEndian.PutUint32(raw[53:57], uint32(baseSeq)) // baseSequence
	binary.BigEndian.PutUint32(raw[57:61], uint32(numRecords))
	return raw
}

// TestRebuildChunksFromWAL verifies that on promotion, WAL entries received
// during the standby phase (which are WAL-only, not in chunks) get loaded
// into chunks so the S3 flusher can eventually flush them.
func TestRebuildChunksFromWAL(t *testing.T) {
	t.Parallel()
	walDir := t.TempDir()

	topicID := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	logger := slog.Default()

	// Create WAL and index.
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

	// Create chunk pool and state.
	pool := chunk.NewPool(64*1024*1024, cluster.DefaultMaxMessageBytes)
	state := cluster.NewState(cluster.Config{
		NodeID:            0,
		DefaultPartitions: 1,
		AutoCreateTopics:  true,
	})

	// Create topic with the known topicID.
	td, _, err := state.GetOrCreateTopic("test-topic")
	if err != nil {
		t.Fatal(err)
	}
	td.ID = topicID

	pd := td.Partitions[0]
	pd.Lock()
	pd.TopicID = topicID
	pd.InitWAL(pool, w, idx)
	pd.Unlock()

	// Simulate the standby phase: write batches via AppendReplicated.
	// These go to WAL + WAL index but NOT to chunks (matching real behavior).
	const numBatches = 10
	const recsPerBatch = 5
	for i := 0; i < numBatches; i++ {
		baseOffset := int64(i * recsPerBatch)
		batch := makeTestBatch(baseOffset, recsPerBatch)

		entry := &wal.Entry{
			Sequence:  uint64(i + 1), // monotonically increasing, starting at 1
			TopicID:   topicID,
			Partition: 0,
			Offset:    baseOffset,
			Data:      batch,
		}
		serialized := wal.MarshalEntry(entry)
		info, written, err := w.AppendReplicated(serialized)
		if err != nil {
			t.Fatalf("AppendReplicated failed: %v", err)
		}
		if !written {
			t.Fatalf("entry %d was not written (duplicate?)", i)
		}

		// Advance HW as the receiver would.
		pd.Lock()
		if info.EndOffset > pd.HW() {
			pd.SetHW(info.EndOffset)
		}
		pd.Unlock()
	}

	expectedHW := int64(numBatches * recsPerBatch) // 50

	// Set S3 flush watermark to simulate that some data was already in S3
	// from the old primary. Say offsets 0-19 are in S3 (watermark=20).
	pd.Lock()
	pd.SetS3FlushWatermark(20)
	pd.Unlock()

	// Verify: chunks should be EMPTY (standby doesn't populate chunks).
	pd.RLock()
	hasData := pd.HasChunkData()
	pd.RUnlock()
	if hasData {
		t.Fatal("expected no chunk data before rebuildChunksFromWAL")
	}

	// Construct a minimal broker with just the fields needed.
	b := &Broker{
		walWriter: w,
		walIndex:  idx,
		chunkPool: pool,
		state:     state,
		logger:    logger,
	}

	// Call rebuildChunksFromWAL (simulating promotion).
	b.rebuildChunksFromWAL()

	// Verify: chunks should now contain data for offsets >= 20 (above S3 watermark).
	pd.RLock()
	hasData = pd.HasChunkData()
	hw := pd.HW()
	pd.RUnlock()

	if !hasData {
		t.Fatal("expected chunk data after rebuildChunksFromWAL")
	}
	if hw != expectedHW {
		t.Fatalf("HW = %d, want %d", hw, expectedHW)
	}

	// Fetch at offset 20 should return data from chunks.
	fr := pd.Fetch(20, 1024*1024)
	if fr.Err != 0 {
		t.Fatalf("Fetch at offset 20 returned error code %d", fr.Err)
	}
	if len(fr.Batches) == 0 {
		t.Fatal("Fetch at offset 20 returned no batches")
	}
	if fr.Batches[0].BaseOffset != 20 {
		t.Fatalf("first batch BaseOffset = %d, want 20", fr.Batches[0].BaseOffset)
	}

	// Fetch at offset 0 should also work (offsets 0-19 are below S3 watermark
	// but are still in WAL, so fetchFromCold should find them).
	fr0 := pd.Fetch(0, 1024*1024)
	if fr0.Err != 0 {
		t.Fatalf("Fetch at offset 0 returned error code %d", fr0.Err)
	}
	// Offsets 0-19 are below s3FlushWatermark and were NOT loaded into chunks,
	// but they are still in the WAL, so fetchFromCold via WAL index should work.
	if len(fr0.Batches) == 0 {
		t.Fatal("Fetch at offset 0 returned no batches (expected WAL read)")
	}

	// Verify all offsets from 20 to 49 are fetchable from chunks.
	for off := int64(20); off < expectedHW; off += recsPerBatch {
		fr := pd.Fetch(off, 1024*1024)
		if fr.Err != 0 {
			t.Fatalf("Fetch at offset %d returned error code %d", off, fr.Err)
		}
		if len(fr.Batches) == 0 {
			t.Fatalf("Fetch at offset %d returned no batches", off)
		}
	}
}

// TestAbortOrphanedTransactions verifies that after WAL replay, open
// transactions on partitions are aborted and LSO advances.
func TestAbortOrphanedTransactions(t *testing.T) {
	t.Parallel()
	logger := slog.Default()

	state := cluster.NewState(cluster.Config{
		NodeID:            0,
		DefaultPartitions: 1,
		AutoCreateTopics:  true,
	})

	td, _, err := state.GetOrCreateTopic("test-topic")
	if err != nil {
		t.Fatal(err)
	}

	pd := td.Partitions[0]

	// Simulate post-replay state: partition has data and open transactions.
	pd.Lock()
	pd.SetHW(100)
	pd.AddOpenTxn(42, 50) // PID 42 has an open txn starting at offset 50
	pd.AddOpenTxn(99, 70) // PID 99 has an open txn starting at offset 70
	pd.Unlock()

	// Verify LSO is blocked by the open transactions.
	pd.RLock()
	lso := pd.LSO()
	pd.RUnlock()
	if lso != 50 {
		t.Fatalf("LSO before sweep = %d, want 50", lso)
	}

	b := &Broker{
		state:  state,
		cfg:    Config{Clock: clock.RealClock{}},
		logger: logger,
	}

	b.abortOrphanedTransactions()

	// After sweep: no open transactions, LSO should equal HW.
	pd.RLock()
	openPIDs := pd.OpenTxnPIDs()
	lso = pd.LSO()
	hw := pd.HW()
	pd.RUnlock()

	if len(openPIDs) != 0 {
		t.Fatalf("expected no open txns after sweep, got %d", len(openPIDs))
	}
	// LSO should now advance — the abort control batches advance HW beyond
	// 100, so LSO = HW (since no more open txns).
	if lso != hw {
		t.Fatalf("LSO = %d, want HW = %d", lso, hw)
	}
	// HW should have advanced by 2 abort control batches (one per PID).
	if hw <= 100 {
		t.Fatalf("HW = %d, should be > 100 after abort control batches", hw)
	}

	// Verify aborted txn entries were recorded.
	pd.RLock()
	aborted50 := pd.AbortedTxnsInRange(50, hw)
	pd.RUnlock()
	if len(aborted50) != 2 {
		t.Fatalf("expected 2 aborted txn entries, got %d", len(aborted50))
	}
}

// TestAbortOrphanedTransactions_NoOrphans verifies the sweep is a no-op when
// there are no open transactions.
func TestAbortOrphanedTransactions_NoOrphans(t *testing.T) {
	t.Parallel()
	logger := slog.Default()

	state := cluster.NewState(cluster.Config{
		NodeID:            0,
		DefaultPartitions: 1,
		AutoCreateTopics:  true,
	})

	td, _, err := state.GetOrCreateTopic("test-topic")
	if err != nil {
		t.Fatal(err)
	}

	pd := td.Partitions[0]
	pd.Lock()
	pd.SetHW(100)
	pd.Unlock()

	b := &Broker{
		state:  state,
		cfg:    Config{Clock: clock.RealClock{}},
		logger: logger,
	}

	b.abortOrphanedTransactions()

	// HW should not change.
	pd.RLock()
	hw := pd.HW()
	lso := pd.LSO()
	pd.RUnlock()
	if hw != 100 {
		t.Fatalf("HW = %d, want 100 (no change)", hw)
	}
	if lso != 100 {
		t.Fatalf("LSO = %d, want 100", lso)
	}
}

// TestEndTxnControlBatchDurability verifies that EndTxn commit/abort control
// batches are written to the WAL and survive replay. This is the regression
// test for the bug where HandleEndTxn used PushBatch (in-memory only) instead
// of routing through the WAL, causing committed transactions to be silently
// re-aborted on restart.
func TestEndTxnControlBatchDurability(t *testing.T) {
	t.Parallel()
	walDir := t.TempDir()
	topicID := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	logger := slog.Default()

	// Create WAL writer.
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
	state := cluster.NewState(cluster.Config{
		NodeID:            0,
		DefaultPartitions: 1,
		AutoCreateTopics:  true,
	})

	td, _, err := state.GetOrCreateTopic("test-topic")
	if err != nil {
		t.Fatal(err)
	}
	td.ID = topicID

	pd := td.Partitions[0]
	pd.Lock()
	pd.TopicID = topicID
	pd.InitWAL(pool, w, idx)
	pd.Unlock()

	// Set up a transactional producer with an open transaction.
	const pid int64 = 42
	const txnTimeoutMs int32 = 30000
	allocPID, allocEpoch, errCode := state.PIDManager().InitProducerID("test-txn", txnTimeoutMs)
	if errCode != 0 {
		t.Fatalf("InitProducerID errCode=%d", errCode)
	}

	// Produce a transactional batch to set up HW and open txn state.
	tp := cluster.TopicPartition{Topic: "test-topic", Partition: 0}
	if ec := state.PIDManager().AddPartitionsToTxn(allocPID, allocEpoch, []cluster.TopicPartition{tp}); ec != 0 {
		t.Fatalf("AddPartitionsToTxn errCode=%d", ec)
	}

	// Write a data batch through the WAL (simulating a produce).
	batch := makeTestBatchPID(0, 5, pid, allocEpoch, 0)
	// Set transactional attribute (bit 4)
	binary.BigEndian.PutUint16(batch[17:19], binary.BigEndian.Uint16(batch[17:19])|0x0010)
	entry := &wal.Entry{
		TopicID:   topicID,
		Partition: 0,
		Offset:    0,
		Data:      batch,
	}
	errCh, walErr := w.AppendAsync(entry)
	if walErr != nil {
		t.Fatal(walErr)
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}

	// Advance HW and record the txn batch.
	pd.Lock()
	pd.SetHW(5)
	pd.AddOpenTxn(allocPID, 0)
	pd.Unlock()
	state.PIDManager().RecordTxnBatch(allocPID, "test-topic", 0, 0)

	// Now commit the transaction via HandleEndTxn.
	h := handler.HandleEndTxn(state, w, clock.RealClock{})
	endReq := &kmsg.EndTxnRequest{
		TransactionalID: "test-txn",
		ProducerID:      allocPID,
		ProducerEpoch:   allocEpoch,
		Commit:          true,
	}
	resp, err := h(endReq)
	if err != nil {
		t.Fatalf("HandleEndTxn error: %v", err)
	}
	endResp := resp.(*kmsg.EndTxnResponse)
	if endResp.ErrorCode != 0 {
		t.Fatalf("HandleEndTxn errCode=%d", endResp.ErrorCode)
	}

	// Verify the partition state: no open txns, HW advanced.
	pd.RLock()
	openPIDs := pd.OpenTxnPIDs()
	hwAfter := pd.HW()
	pd.RUnlock()
	if len(openPIDs) != 0 {
		t.Fatalf("expected no open txns after commit, got %d", len(openPIDs))
	}
	if hwAfter <= 5 {
		t.Fatalf("HW should have advanced past 5 after control batch, got %d", hwAfter)
	}

	// KEY ASSERTION: Stop WAL, create a fresh state, replay the WAL, and
	// verify the commit control batch survived (i.e. no orphaned open txns).
	w.Stop()

	// Create fresh state for replay.
	idx2 := wal.NewIndex()
	walCfg2 := wal.DefaultWriterConfig()
	walCfg2.Dir = walDir
	walCfg2.S3Configured = true
	walCfg2.Logger = logger
	w2, err := wal.NewWriter(walCfg2, idx2)
	if err != nil {
		t.Fatal(err)
	}
	if err := w2.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { w2.Stop() })

	state2 := cluster.NewState(cluster.Config{
		NodeID:            0,
		DefaultPartitions: 1,
		AutoCreateTopics:  true,
	})
	td2, _, err := state2.GetOrCreateTopic("test-topic")
	if err != nil {
		t.Fatal(err)
	}
	td2.ID = topicID
	pd2 := td2.Partitions[0]
	pd2.Lock()
	pd2.TopicID = topicID
	pd2.InitWAL(pool, w2, idx2)
	pd2.Unlock()

	b2 := &Broker{
		walWriter: w2,
		walIndex:  idx2,
		chunkPool: pool,
		state:     state2,
		logger:    logger,
		cfg:       Config{Clock: clock.RealClock{}},
	}

	if err := b2.replayWAL(w2); err != nil {
		t.Fatalf("replayWAL: %v", err)
	}

	// After replay: the commit control batch should have been replayed,
	// so no open transactions should exist.
	pd2.RLock()
	openPIDs2 := pd2.OpenTxnPIDs()
	hw2 := pd2.HW()
	pd2.RUnlock()

	if len(openPIDs2) != 0 {
		t.Fatalf("after WAL replay: expected no open txns (commit was durable), got %d open PIDs: %v", len(openPIDs2), openPIDs2)
	}
	if hw2 != hwAfter {
		t.Fatalf("after WAL replay: HW = %d, want %d", hw2, hwAfter)
	}
}

// TestRebuildChunksFromWAL_PIDState verifies that rebuildChunksFromWAL
// reconstructs producer dedup state so duplicates are detected after promotion.
func TestRebuildChunksFromWAL_PIDState(t *testing.T) {
	t.Parallel()
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
	state := cluster.NewState(cluster.Config{
		NodeID:            0,
		DefaultPartitions: 1,
		AutoCreateTopics:  true,
	})

	td, _, err := state.GetOrCreateTopic("test-topic")
	if err != nil {
		t.Fatal(err)
	}
	td.ID = topicID

	pd := td.Partitions[0]
	pd.Lock()
	pd.TopicID = topicID
	pd.InitWAL(pool, w, idx)
	pd.Unlock()

	// Write idempotent batches via AppendReplicated (simulating standby).
	// PID=42, epoch=0, sequences: 0(5 recs), 5(5 recs), 10(5 recs)
	const pid int64 = 42
	const epoch int16 = 0
	const recsPerBatch int32 = 5
	for i := 0; i < 3; i++ {
		baseOffset := int64(i) * int64(recsPerBatch)
		baseSeq := int32(i) * recsPerBatch
		batch := makeTestBatchPID(baseOffset, recsPerBatch, pid, epoch, baseSeq)

		entry := &wal.Entry{
			Sequence:  uint64(i + 1),
			TopicID:   topicID,
			Partition: 0,
			Offset:    baseOffset,
			Data:      batch,
		}
		serialized := wal.MarshalEntry(entry)
		info, written, err := w.AppendReplicated(serialized)
		if err != nil {
			t.Fatalf("AppendReplicated failed: %v", err)
		}
		if !written {
			t.Fatalf("entry %d was not written", i)
		}
		pd.Lock()
		if info.EndOffset > pd.HW() {
			pd.SetHW(info.EndOffset)
		}
		pd.Unlock()
	}

	// Before rebuild, PID manager should have no state (standby doesn't populate it).
	if ps := state.PIDManager().GetProducer(pid); ps != nil {
		t.Fatal("expected no producer state before rebuild")
	}

	b := &Broker{
		walWriter: w,
		walIndex:  idx,
		chunkPool: pool,
		state:     state,
		logger:    logger,
	}

	b.rebuildChunksFromWAL()

	// After rebuild, PID state should be reconstructed.
	ps := state.PIDManager().GetProducer(pid)
	if ps == nil {
		t.Fatal("expected producer state for PID 42 after rebuild")
	}
	if ps.Epoch != epoch {
		t.Fatalf("Epoch = %d, want %d", ps.Epoch, epoch)
	}

	// ValidateAndDedup should detect the last batch (seq=10) as duplicate.
	tp := cluster.TopicPartition{Topic: "test-topic", Partition: 0}
	errCode, isDup, dupOff := state.PIDManager().ValidateAndDedup(pid, epoch, tp, 10, 5, 999)
	if errCode != 0 {
		t.Fatalf("errCode = %d, want 0", errCode)
	}
	if !isDup {
		t.Fatal("expected duplicate detection for seq=10")
	}
	if dupOff != 10 {
		t.Fatalf("dupOffset = %d, want 10", dupOff)
	}

	// Next sequence (15) should be accepted.
	errCode, isDup, _ = state.PIDManager().ValidateAndDedup(pid, epoch, tp, 15, 5, 15)
	if errCode != 0 || isDup {
		t.Fatalf("seq=15: errCode=%d isDup=%v, expected accept", errCode, isDup)
	}
}

// TestRebuildHWFromWAL verifies that rebuildHWFromWAL advances the HW
// for partitions present in the WAL index and leaves others unchanged.
func TestRebuildHWFromWAL(t *testing.T) {
	t.Parallel()
	logger := slog.Default()

	topicID := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}

	idx := wal.NewIndex()
	state := cluster.NewState(cluster.Config{
		NodeID:            0,
		DefaultPartitions: 2,
		AutoCreateTopics:  true,
	})

	td, _, err := state.GetOrCreateTopic("test-topic")
	if err != nil {
		t.Fatal(err)
	}
	td.ID = topicID

	// Set initial HW for both partitions.
	td.Partitions[0].Lock()
	td.Partitions[0].TopicID = topicID
	td.Partitions[0].SetHW(5)
	td.Partitions[0].Unlock()

	td.Partitions[1].Lock()
	td.Partitions[1].TopicID = topicID
	td.Partitions[1].SetHW(10)
	td.Partitions[1].Unlock()

	// Add WAL index entries only for partition 0 with offsets up to 49.
	tp0 := wal.TopicPartition{TopicID: topicID, Partition: 0}
	for i := 0; i < 10; i++ {
		baseOff := int64(i * 5)
		idx.Add(tp0, wal.IndexEntry{
			BaseOffset: baseOff,
			LastOffset: baseOff + 4,
		})
	}
	// MaxOffset for tp0 = 49+1 = 50

	b := &Broker{
		walIndex: idx,
		state:    state,
		logger:   logger,
	}

	b.rebuildHWFromWAL()

	// Partition 0: HW should advance from 5 to 50 (WAL max offset).
	td.Partitions[0].RLock()
	hw0 := td.Partitions[0].HW()
	td.Partitions[0].RUnlock()
	if hw0 != 50 {
		t.Fatalf("partition 0 HW = %d, want 50", hw0)
	}

	// Partition 1: HW should remain at 10 (not in WAL index).
	td.Partitions[1].RLock()
	hw1 := td.Partitions[1].HW()
	td.Partitions[1].RUnlock()
	if hw1 != 10 {
		t.Fatalf("partition 1 HW = %d, want 10 (unchanged)", hw1)
	}
}

// TestRebuildHWFromWAL_NoRegression verifies that rebuildHWFromWAL does not
// lower the HW when WAL max offset is below the current HW.
func TestRebuildHWFromWAL_NoRegression(t *testing.T) {
	t.Parallel()
	logger := slog.Default()

	topicID := [16]byte{2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17}

	idx := wal.NewIndex()
	state := cluster.NewState(cluster.Config{
		NodeID:            0,
		DefaultPartitions: 1,
		AutoCreateTopics:  true,
	})

	td, _, err := state.GetOrCreateTopic("test-topic")
	if err != nil {
		t.Fatal(err)
	}
	td.ID = topicID

	// Set HW to 100, higher than WAL data.
	td.Partitions[0].Lock()
	td.Partitions[0].TopicID = topicID
	td.Partitions[0].SetHW(100)
	td.Partitions[0].Unlock()

	// WAL index has data up to offset 49 → MaxOffset = 50.
	tp := wal.TopicPartition{TopicID: topicID, Partition: 0}
	for i := 0; i < 10; i++ {
		baseOff := int64(i * 5)
		idx.Add(tp, wal.IndexEntry{
			BaseOffset: baseOff,
			LastOffset: baseOff + 4,
		})
	}

	b := &Broker{
		walIndex: idx,
		state:    state,
		logger:   logger,
	}

	b.rebuildHWFromWAL()

	td.Partitions[0].RLock()
	hw := td.Partitions[0].HW()
	td.Partitions[0].RUnlock()
	if hw != 100 {
		t.Fatalf("HW = %d, want 100 (should not regress)", hw)
	}
}

// TestRebuildHWFromWAL_NilIndex verifies that rebuildHWFromWAL is a no-op
// when walIndex is nil.
func TestRebuildHWFromWAL_NilIndex(t *testing.T) {
	t.Parallel()

	b := &Broker{
		walIndex: nil,
		logger:   slog.Default(),
	}

	// Should not panic.
	b.rebuildHWFromWAL()
}

// makeTestBatchTxn builds a transactional data batch (not a control batch)
// with the transactional attribute bit set.
func makeTestBatchTxn(baseOffset int64, numRecords int32, pid int64, epoch int16, baseSeq int32) []byte {
	raw := makeTestBatchPID(baseOffset, numRecords, pid, epoch, baseSeq)
	// Set transactional attribute bit (bit 4 = 0x0010).
	attrs := binary.BigEndian.Uint16(raw[21:23])
	attrs |= 0x0010
	binary.BigEndian.PutUint16(raw[21:23], attrs)
	return raw
}

// TestRebuildChunksFromWAL_TxnState verifies that rebuildChunksFromWAL
// reconstructs transaction state: open transactions are tracked and
// abort control records produce aborted txn entries.
func TestRebuildChunksFromWAL_TxnState(t *testing.T) {
	t.Parallel()
	walDir := t.TempDir()

	topicID := [16]byte{3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18}
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
	state := cluster.NewState(cluster.Config{
		NodeID:            0,
		DefaultPartitions: 1,
		AutoCreateTopics:  true,
	})

	td, _, err := state.GetOrCreateTopic("test-topic")
	if err != nil {
		t.Fatal(err)
	}
	td.ID = topicID

	pd := td.Partitions[0]
	pd.Lock()
	pd.TopicID = topicID
	pd.InitWAL(pool, w, idx)
	pd.Unlock()

	const pid1 int64 = 100
	const pid2 int64 = 200
	const epoch int16 = 0

	// Write txn data batches for PID 100 at offsets 0-4 and 5-9.
	appendReplicated := func(batch []byte, baseOffset, lastOffset int64, seq uint64) {
		t.Helper()
		entry := &wal.Entry{
			Sequence:  seq,
			TopicID:   topicID,
			Partition: 0,
			Offset:    baseOffset,
			Data:      batch,
		}
		serialized := wal.MarshalEntry(entry)
		info, written, err := w.AppendReplicated(serialized)
		if err != nil {
			t.Fatalf("AppendReplicated failed: %v", err)
		}
		if !written {
			t.Fatalf("entry seq=%d was not written", seq)
		}
		pd.Lock()
		if info.EndOffset > pd.HW() {
			pd.SetHW(info.EndOffset)
		}
		pd.Unlock()
	}

	// PID 100: two data batches then abort.
	txnBatch1 := makeTestBatchTxn(0, 5, pid1, epoch, 0)
	appendReplicated(txnBatch1, 0, 4, 1)

	txnBatch2 := makeTestBatchTxn(5, 5, pid1, epoch, 5)
	appendReplicated(txnBatch2, 5, 9, 2)

	// PID 200: one data batch (will remain open — no control record).
	txnBatch3 := makeTestBatchTxn(10, 5, pid2, epoch, 0)
	appendReplicated(txnBatch3, 10, 14, 3)

	// Abort control record for PID 100.
	abortCtrl := cluster.BuildControlBatch(pid1, epoch, false, 1000)
	appendReplicated(abortCtrl, 15, 15, 4)

	b := &Broker{
		walWriter: w,
		walIndex:  idx,
		chunkPool: pool,
		state:     state,
		logger:    logger,
	}

	b.rebuildChunksFromWAL()

	// Verify: PID 200 should have an open txn.
	pd.RLock()
	openPIDs := pd.OpenTxnPIDs()
	pd.RUnlock()
	if _, ok := openPIDs[pid2]; !ok {
		t.Fatalf("expected PID %d to have open txn, got openPIDs=%v", pid2, openPIDs)
	}

	// Verify: PID 100 should NOT have an open txn (was aborted).
	if _, ok := openPIDs[pid1]; ok {
		t.Fatalf("expected PID %d to NOT have open txn after abort", pid1)
	}

	// Verify: aborted txn entry exists for PID 100.
	pd.RLock()
	aborted := pd.AbortedTxnsInRange(0, 20)
	pd.RUnlock()
	if len(aborted) != 1 {
		t.Fatalf("expected 1 aborted txn entry, got %d", len(aborted))
	}
	if aborted[0].ProducerID != pid1 {
		t.Fatalf("aborted txn PID = %d, want %d", aborted[0].ProducerID, pid1)
	}
	if aborted[0].FirstOffset != 0 {
		t.Fatalf("aborted txn FirstOffset = %d, want 0", aborted[0].FirstOffset)
	}
}

// TestRebuildChunksFromWAL_DetachOldChunks verifies that rebuildChunksFromWAL
// detaches existing chunks before rebuilding from WAL data.
func TestRebuildChunksFromWAL_DetachOldChunks(t *testing.T) {
	t.Parallel()
	walDir := t.TempDir()

	topicID := [16]byte{4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19}
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
	state := cluster.NewState(cluster.Config{
		NodeID:            0,
		DefaultPartitions: 1,
		AutoCreateTopics:  true,
	})

	td, _, err := state.GetOrCreateTopic("test-topic")
	if err != nil {
		t.Fatal(err)
	}
	td.ID = topicID

	pd := td.Partitions[0]
	pd.Lock()
	pd.TopicID = topicID
	pd.InitWAL(pool, w, idx)
	pd.Unlock()

	// Pre-populate chunks with stale data via PushBatch (simulating
	// startup WAL replay that populated chunks from partial data).
	staleBatch := makeTestBatch(0, 3)
	meta, err := cluster.ParseBatchHeader(staleBatch)
	if err != nil {
		t.Fatal(err)
	}
	spare := pd.AcquireSpareChunk(len(staleBatch))
	pd.Lock()
	pd.PushBatch(staleBatch, meta, spare)
	pd.Unlock()

	// Verify stale data is present.
	pd.RLock()
	hasDataBefore := pd.HasChunkData()
	pd.RUnlock()
	if !hasDataBefore {
		t.Fatal("expected chunk data before rebuild")
	}

	// Now write different WAL data (offsets 0-9 with 2 batches of 5 records).
	for i := 0; i < 2; i++ {
		baseOffset := int64(i * 5)
		batch := makeTestBatch(baseOffset, 5)
		entry := &wal.Entry{
			Sequence:  uint64(i + 1),
			TopicID:   topicID,
			Partition: 0,
			Offset:    baseOffset,
			Data:      batch,
		}
		serialized := wal.MarshalEntry(entry)
		info, written, err := w.AppendReplicated(serialized)
		if err != nil {
			t.Fatalf("AppendReplicated failed: %v", err)
		}
		if !written {
			t.Fatalf("entry %d was not written", i)
		}
		pd.Lock()
		if info.EndOffset > pd.HW() {
			pd.SetHW(info.EndOffset)
		}
		pd.Unlock()
	}

	b := &Broker{
		walWriter: w,
		walIndex:  idx,
		chunkPool: pool,
		state:     state,
		logger:    logger,
	}

	b.rebuildChunksFromWAL()

	// Fetch at offset 0: should return WAL data, not stale chunk data.
	// The stale batch had 3 records (offsets 0-2), the WAL batch has 5 records (offsets 0-4).
	fr := pd.Fetch(0, 1024*1024)
	if fr.Err != 0 {
		t.Fatalf("Fetch returned error code %d", fr.Err)
	}
	if len(fr.Batches) == 0 {
		t.Fatal("Fetch returned no batches")
	}

	// The first batch should have 5 records (from WAL), not 3 (from stale chunks).
	if fr.Batches[0].NumRecords != 5 {
		t.Fatalf("first batch NumRecords = %d, want 5 (WAL data, not stale)", fr.Batches[0].NumRecords)
	}
}
