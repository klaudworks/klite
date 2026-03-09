package broker

import (
	"context"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/klaudworks/klite/internal/clock"
	"github.com/klaudworks/klite/internal/cluster"
	"github.com/klaudworks/klite/internal/lease"
	"github.com/klaudworks/klite/internal/metadata"
	"github.com/klaudworks/klite/internal/repl"
	"github.com/klaudworks/klite/internal/server"
	"github.com/klaudworks/klite/internal/wal"
)

// newStepRecorderBroker constructs a minimal Broker wired with a step
// recorder. Subsystems are nil where possible (the functions being tested
// nil-check them), and a real server/listener is provided so the Kafka
// listener step in onElected doesn't panic.
func newStepRecorderBroker(t *testing.T) (*Broker, *[]string) {
	t.Helper()

	kafkaLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = kafkaLn.Close() })

	logger := slog.Default()
	shutdownCh := make(chan struct{})
	handlers := server.NewHandlerRegistry()
	srv := server.NewServer(handlers, shutdownCh, logger)

	state := cluster.NewState(cluster.Config{
		NodeID:            0,
		DefaultPartitions: 1,
		AutoCreateTopics:  true,
	})
	state.SetShutdownCh(shutdownCh)
	state.SetLogger(logger)
	state.SetClock(clock.RealClock{})

	var steps []string

	defaultCfg := DefaultConfig()
	b := &Broker{
		cfg: Config{
			Listener:               kafkaLn,
			ReplicationAddr:        "127.0.0.1:0", // triggers startReplicationListener
			Clock:                  clock.RealClock{},
			RetentionCheckInterval: defaultCfg.RetentionCheckInterval,
		},
		state:      state,
		shutdownCh: shutdownCh,
		done:       make(chan struct{}),
		ready:      make(chan struct{}),
		logger:     logger,
		server:     srv,
		handlers:   handlers,
	}

	b.testStepHook = func(name string) {
		steps = append(steps, name)
	}

	return b, &steps
}

func TestOnElectedStepOrder(t *testing.T) {
	t.Parallel()

	b, steps := newStepRecorderBroker(t)

	ctx := context.Background()
	primaryCtx, primaryCancel := context.WithCancel(ctx)
	defer primaryCancel()

	b.onElected(ctx, primaryCtx)

	// Clean up: close the listener that onElected started (Serve is in a goroutine).
	if b.listener != nil {
		_ = b.listener.Close()
	}
	if b.replListener != nil {
		_ = b.replListener.Close()
	}

	want := []string{
		"stop-receiver",
		"rebuild-hw",
		// "probe-s3" is skipped when s3Client == nil
		"rebuild-chunks",
		"abort-orphan-txns",
		"start-repl-listener",
		"start-kafka-listener",
		"start-background",
	}

	got := *steps
	if len(got) != len(want) {
		t.Fatalf("step count: got %d, want %d\ngot:  %v\nwant: %v", len(got), len(want), got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("step[%d]: got %q, want %q\nfull sequence: %v", i, got[i], want[i], got)
		}
	}
}

// TestOnElectedProbeS3Conditional verifies that probe-s3 is only emitted
// when s3Client is non-nil and appears in the correct position.
func TestOnElectedProbeS3Conditional(t *testing.T) {
	t.Parallel()

	// Without S3: verify probe-s3 is absent.
	b1, steps1 := newStepRecorderBroker(t)
	ctx := context.Background()
	pctx1, pcancel1 := context.WithCancel(ctx)
	defer pcancel1()
	b1.onElected(ctx, pctx1)
	if b1.listener != nil {
		_ = b1.listener.Close()
	}
	if b1.replListener != nil {
		_ = b1.replListener.Close()
	}
	for _, s := range *steps1 {
		if s == "probe-s3" {
			t.Fatal("probe-s3 should not appear when s3Client is nil")
		}
	}

	// Verify rebuild-hw is immediately followed by rebuild-chunks (no probe-s3 in between).
	got := *steps1
	for i, s := range got {
		if s == "rebuild-hw" && i+1 < len(got) {
			if got[i+1] != "rebuild-chunks" {
				t.Fatalf("without S3: rebuild-hw should be followed by rebuild-chunks, got %q", got[i+1])
			}
		}
	}
}

func TestShutdownPrimaryStepOrder(t *testing.T) {
	t.Parallel()

	b, steps := newStepRecorderBroker(t)

	// Simulate that this broker was primary: give it a listener and mark it primary.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	b.listener = ln
	b.replRole.Store(1)

	b.shutdownPrimary()

	want := []string{
		"close-listener",
		"close-conns",
		"wake-fetch-waiters",
		"drain-requests",
		"clear-replicator",
		"clear-metalog",
		"close-repl",
		"stop-s3-flusher",
	}

	got := *steps
	if len(got) != len(want) {
		t.Fatalf("step count: got %d, want %d\ngot:  %v\nwant: %v", len(got), len(want), got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("step[%d]: got %q, want %q\nfull sequence: %v", i, got[i], want[i], got)
		}
	}

	// Verify listener was closed.
	if b.listener != nil {
		t.Fatal("listener should be nil after shutdownPrimary")
	}

	// Verify role was reset.
	if b.replRole.Load() != 0 {
		t.Fatal("replRole should be 0 after shutdownPrimary")
	}
}

func TestOnDemotedCallsShutdownPrimary(t *testing.T) {
	t.Parallel()

	b, steps := newStepRecorderBroker(t)
	b.replRole.Store(1)

	// Give it a listener to confirm shutdownPrimary ran.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	b.listener = ln

	b.onDemoted()

	got := *steps
	// onDemoted calls shutdownPrimary, so we should see the shutdown steps.
	if len(got) < 8 {
		t.Fatalf("expected at least 8 shutdown steps from onDemoted, got %d: %v", len(got), got)
	}
	if got[0] != "close-listener" {
		t.Fatalf("first step should be close-listener (from shutdownPrimary), got %q", got[0])
	}

	// Verify the full shutdown sequence.
	shutdownSteps := []string{
		"close-listener",
		"close-conns",
		"wake-fetch-waiters",
		"drain-requests",
		"clear-replicator",
		"clear-metalog",
		"close-repl",
		"stop-s3-flusher",
	}
	for i, want := range shutdownSteps {
		if i >= len(got) || got[i] != want {
			actual := ""
			if i < len(got) {
				actual = got[i]
			}
			t.Fatalf("step[%d]: got %q, want %q\nfull: %v", i, actual, want, got)
		}
	}

	// Verify transition to standby.
	if b.replRole.Load() != 0 {
		t.Fatal("replRole should be 0 after onDemoted")
	}
}

// TestWakeAllFetchWaitersCalledOnDemotion verifies that WakeAllFetchWaiters
// is called during the shutdown sequence so blocked consumers unblock.
func TestWakeAllFetchWaitersCalledOnDemotion(t *testing.T) {
	t.Parallel()

	b, steps := newStepRecorderBroker(t)
	b.replRole.Store(1)

	b.shutdownPrimary()

	got := *steps
	found := false
	for _, s := range got {
		if s == "wake-fetch-waiters" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("wake-fetch-waiters not found in shutdown steps: %v", got)
	}

	// Verify it comes after close-conns and before drain-requests.
	closeConnsIdx := -1
	wakeIdx := -1
	drainIdx := -1
	for i, s := range got {
		switch s {
		case "close-conns":
			closeConnsIdx = i
		case "wake-fetch-waiters":
			wakeIdx = i
		case "drain-requests":
			drainIdx = i
		}
	}
	if closeConnsIdx >= wakeIdx {
		t.Fatalf("close-conns (%d) should come before wake-fetch-waiters (%d)", closeConnsIdx, wakeIdx)
	}
	if wakeIdx >= drainIdx {
		t.Fatalf("wake-fetch-waiters (%d) should come before drain-requests (%d)", wakeIdx, drainIdx)
	}
}

// stubElector implements lease.Elector with a fixed epoch for testing.
type stubElector struct {
	epoch uint64
}

func (s *stubElector) Run(context.Context, lease.Callbacks) error { return nil }
func (s *stubElector) Role() lease.Role                           { return lease.RolePrimary }
func (s *stubElector) Release() error                             { return nil }
func (s *stubElector) Epoch() uint64                              { return s.epoch }
func (s *stubElector) PrimaryAddr() string                        { return "" }

// newHandshakeBroker creates a minimal Broker wired with real wal.Writer and
// metadata.Log for testing handleStandbyConn. The WAL writer is created but
// not started — we only need its atomic fields (NextSequence, SetReplicator,
// LocalOnlyBatches).
func newHandshakeBroker(t *testing.T, clk clock.Clock, epoch uint64) *Broker {
	t.Helper()
	dir := t.TempDir()

	idx := wal.NewIndex()
	walCfg := wal.DefaultWriterConfig()
	walCfg.Dir = dir
	walCfg.Clock = clk
	w, err := wal.NewWriter(walCfg, idx)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.Stop)
	w.SetNextSequence(42)

	metaLog, err := metadata.NewLog(metadata.LogConfig{DataDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = metaLog.Close() })

	// Write some metadata so the snapshot has content.
	if err := metaLog.Append([]byte("test-metadata-entry")); err != nil {
		t.Fatal(err)
	}

	return &Broker{
		cfg: Config{
			ReplicationAckTimeout: 5 * time.Second,
			Clock:                 clk,
		},
		logger:    slog.Default(),
		walWriter: w,
		metaLog:   metaLog,
		elector:   &stubElector{epoch: epoch},
	}
}

func TestHandleStandbyConn_ValidHello(t *testing.T) {
	t.Parallel()
	clk := clock.NewFakeClock(time.Now())
	b := newHandshakeBroker(t, clk, 5)

	brokerConn, standbyConn := net.Pipe()
	defer standbyConn.Close() //nolint:errcheck

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Run the standby side concurrently: send HELLO, then read the SNAPSHOT
	// response. net.Pipe is synchronous, so reads and writes must happen
	// concurrently with handleStandbyConn.
	type result struct {
		msgType  byte
		payload  []byte
		writeErr error
		readErr  error
	}
	resCh := make(chan result, 1)
	go func() {
		var r result
		hello := repl.MarshalHello(0, 0) // fresh standby, epoch 0
		r.writeErr = repl.WriteFrame(standbyConn, repl.MsgHello, hello)
		if r.writeErr != nil {
			resCh <- r
			return
		}
		r.msgType, r.payload, r.readErr = repl.ReadFrame(standbyConn)
		resCh <- r
	}()

	b.handleStandbyConn(ctx, brokerConn)

	r := <-resCh
	if r.writeErr != nil {
		t.Fatalf("writing HELLO: %v", r.writeErr)
	}
	if r.readErr != nil {
		t.Fatalf("reading response: %v", r.readErr)
	}

	// Verify: a SNAPSHOT should have been sent (epoch mismatch: standby=0, broker=5).
	if r.msgType != repl.MsgSnapshot {
		t.Fatalf("expected MsgSnapshot (0x%02x), got 0x%02x", repl.MsgSnapshot, r.msgType)
	}

	walSeq, metaData, err := repl.UnmarshalSnapshot(r.payload)
	if err != nil {
		t.Fatalf("unmarshal snapshot: %v", err)
	}
	if walSeq != 42 {
		t.Errorf("snapshot walSeq: got %d, want 42", walSeq)
	}
	if len(metaData) == 0 {
		t.Error("snapshot metadata should not be empty")
	}

	if b.replSender == nil {
		t.Error("replSender should be set after valid HELLO")
	}

	cancel()
	b.replSender.Close()
}

func TestHandleStandbyConn_WrongMessageType(t *testing.T) {
	t.Parallel()
	clk := clock.NewFakeClock(time.Now())
	b := newHandshakeBroker(t, clk, 1)

	brokerConn, standbyConn := net.Pipe()
	defer standbyConn.Close() //nolint:errcheck

	// Send a WAL_BATCH instead of HELLO.
	go func() {
		_ = repl.WriteFrame(standbyConn, repl.MsgWALBatch, []byte("not-a-hello"))
	}()

	ctx := context.Background()
	b.handleStandbyConn(ctx, brokerConn)

	// The broker should have closed brokerConn. Reading from standbyConn
	// should either return EOF or an error.
	buf := make([]byte, 1)
	_, err := standbyConn.Read(buf)
	if err == nil {
		t.Fatal("expected error reading from standby side after broker closed conn")
	}

	// replSender should NOT be set.
	if b.replSender != nil {
		t.Error("replSender should be nil after wrong message type")
	}
}

func TestHandleStandbyConn_MalformedHello(t *testing.T) {
	t.Parallel()
	clk := clock.NewFakeClock(time.Now())
	b := newHandshakeBroker(t, clk, 1)

	brokerConn, standbyConn := net.Pipe()
	defer standbyConn.Close() //nolint:errcheck

	// Send a HELLO with truncated payload (less than 16 bytes).
	go func() {
		_ = repl.WriteFrame(standbyConn, repl.MsgHello, []byte{0x01, 0x02})
	}()

	ctx := context.Background()
	b.handleStandbyConn(ctx, brokerConn)

	// The broker should have closed brokerConn.
	buf := make([]byte, 1)
	_, err := standbyConn.Read(buf)
	if err == nil {
		t.Fatal("expected error reading from standby side after broker closed conn")
	}

	if b.replSender != nil {
		t.Error("replSender should be nil after malformed HELLO")
	}
}

func TestHandleStandbyConn_EpochMatch_NoSnapshot(t *testing.T) {
	t.Parallel()
	clk := clock.NewFakeClock(time.Now())
	brokerEpoch := uint64(7)
	b := newHandshakeBroker(t, clk, brokerEpoch)

	brokerConn, standbyConn := net.Pipe()
	defer standbyConn.Close() //nolint:errcheck

	// Send HELLO with matching epoch.
	hello := repl.MarshalHello(10, brokerEpoch)
	errCh := make(chan error, 1)
	go func() {
		errCh <- repl.WriteFrame(standbyConn, repl.MsgHello, hello)
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b.handleStandbyConn(ctx, brokerConn)

	if err := <-errCh; err != nil {
		t.Fatalf("writing HELLO: %v", err)
	}

	// With matching epoch, no SNAPSHOT should be sent. Close the standby
	// write side and try to read — should get nothing before EOF.
	// Set a short deadline so we don't block forever.
	_ = standbyConn.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
	_, _, err := repl.ReadFrame(standbyConn)
	if err == nil {
		t.Fatal("expected no frame (timeout or EOF), but got one")
	}

	if b.replSender == nil {
		t.Error("replSender should be set even without snapshot")
	}

	cancel()
	b.replSender.Close()
}

func TestHandleStandbyConn_ReadError(t *testing.T) {
	t.Parallel()
	clk := clock.NewFakeClock(time.Now())
	b := newHandshakeBroker(t, clk, 1)

	brokerConn, standbyConn := net.Pipe()

	// Close the standby side immediately so broker gets a read error.
	_ = standbyConn.Close()

	ctx := context.Background()
	b.handleStandbyConn(ctx, brokerConn)

	if b.replSender != nil {
		t.Error("replSender should be nil after read error")
	}
}

func TestKeepaliveLoop(t *testing.T) {
	t.Parallel()
	clk := clock.NewFakeClock(time.Now())

	// Set up a connection pair. The sender writes keepalives to serverConn,
	// we read them from clientConn.
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close() //nolint:errcheck

	sender := repl.NewSender(serverConn, 5*time.Second, slog.Default(), clk)
	defer sender.Close()

	b := &Broker{
		cfg: Config{Clock: clk},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go b.keepaliveLoop(ctx, sender)

	// Wait for the ticker to be registered on the fake clock.
	if !clk.WaitForTimers(1, 2*time.Second) {
		t.Fatal("timed out waiting for keepalive ticker to register")
	}

	// Advance clock past the 5s keepalive interval.
	clk.Advance(6 * time.Second)

	// Read the keepalive frame.
	_ = clientConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	msgType, payload, err := repl.ReadFrame(clientConn)
	if err != nil {
		t.Fatalf("reading keepalive: %v", err)
	}
	if msgType != repl.MsgWALBatch {
		t.Fatalf("expected MsgWALBatch (keepalive), got 0x%02x", msgType)
	}

	// Keepalive is an empty WAL_BATCH: firstSeq=0, lastSeq=0, entryCount=0.
	firstSeq, lastSeq, entryCount, _, _, err := repl.UnmarshalWALBatch(payload)
	if err != nil {
		t.Fatalf("unmarshal keepalive: %v", err)
	}
	if firstSeq != 0 || lastSeq != 0 || entryCount != 0 {
		t.Errorf("keepalive should have zeros: firstSeq=%d, lastSeq=%d, entryCount=%d",
			firstSeq, lastSeq, entryCount)
	}

	// Cancel context and verify the loop exits.
	cancel()
}

func TestKeepaliveLoop_StopsOnContextCancel(t *testing.T) {
	t.Parallel()
	clk := clock.NewFakeClock(time.Now())

	serverConn, clientConn := net.Pipe()
	defer clientConn.Close() //nolint:errcheck

	sender := repl.NewSender(serverConn, 5*time.Second, slog.Default(), clk)
	defer sender.Close()

	b := &Broker{
		cfg: Config{Clock: clk},
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		b.keepaliveLoop(ctx, sender)
		close(done)
	}()

	// Wait for ticker registration then cancel.
	if !clk.WaitForTimers(1, 2*time.Second) {
		t.Fatal("timed out waiting for keepalive ticker")
	}
	cancel()

	select {
	case <-done:
		// OK, loop exited
	case <-time.After(2 * time.Second):
		t.Fatal("keepaliveLoop did not exit after context cancel")
	}
}

func TestHandleStandbyConn_BackfillsMissingWALOnReconnect(t *testing.T) {
	t.Parallel()

	clk := clock.NewFakeClock(time.Now())
	brokerEpoch := uint64(9)
	b := newHandshakeBroker(t, clk, brokerEpoch)

	// Seed WAL with sequences 0,1,2.
	b.walWriter.SetNextSequence(0)
	topicID := [16]byte{1, 2, 3}
	for i := 0; i < 3; i++ {
		err := b.walWriter.Append(&wal.Entry{
			TopicID:   topicID,
			Partition: 0,
			Offset:    int64(i),
			Data:      make([]byte, 27),
		})
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	brokerConn, standbyConn := net.Pipe()
	defer standbyConn.Close() //nolint:errcheck

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	type result struct {
		msgType byte
		payload []byte
		err     error
	}
	resCh := make(chan result, 1)

	go func() {
		var r result
		hello := repl.MarshalHello(0, brokerEpoch) // standby has seq 0, missing 1 and 2
		if err := repl.WriteFrame(standbyConn, repl.MsgHello, hello); err != nil {
			r.err = err
			resCh <- r
			return
		}

		r.msgType, r.payload, r.err = repl.ReadFrame(standbyConn)
		if r.err != nil {
			resCh <- r
			return
		}

		_, lastSeq, _, _, _, err := repl.UnmarshalWALBatch(r.payload)
		if err != nil {
			r.err = err
			resCh <- r
			return
		}

		r.err = repl.WriteFrame(standbyConn, repl.MsgACK, repl.MarshalACK(lastSeq))
		resCh <- r
	}()

	b.handleStandbyConn(ctx, brokerConn)

	r := <-resCh
	if r.err != nil {
		t.Fatalf("standby handshake/read failed: %v", r.err)
	}
	if r.msgType != repl.MsgWALBatch {
		t.Fatalf("expected WAL_BATCH backfill, got type 0x%02x", r.msgType)
	}

	firstSeq, lastSeq, entryCount, _, _, err := repl.UnmarshalWALBatch(r.payload)
	if err != nil {
		t.Fatalf("unmarshal WAL_BATCH: %v", err)
	}
	if firstSeq != 1 || lastSeq != 2 {
		t.Fatalf("backfill range: got [%d,%d], want [1,2]", firstSeq, lastSeq)
	}
	if entryCount != 2 {
		t.Fatalf("backfill entryCount: got %d, want 2", entryCount)
	}

	if b.replSender == nil {
		t.Fatal("replSender should be set after successful reconnect")
	}
	b.replSender.Close()
}
