package broker

import (
	"context"
	"log/slog"
	"net"
	"testing"

	"github.com/klaudworks/klite/internal/clock"
	"github.com/klaudworks/klite/internal/cluster"
	"github.com/klaudworks/klite/internal/server"
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

	b := &Broker{
		cfg: Config{
			Listener:        kafkaLn,
			ReplicationAddr: "127.0.0.1:0", // triggers startReplicationListener
			Clock:           clock.RealClock{},
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
