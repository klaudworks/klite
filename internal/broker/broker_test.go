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
	"github.com/klaudworks/klite/internal/cluster"
	"github.com/klaudworks/klite/internal/wal"
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
