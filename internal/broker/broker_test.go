package broker

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
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
