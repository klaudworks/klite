package broker

import (
	"context"
	"io"
	"net"
	"net/http"
	"testing"
	"time"
)

func TestHealthEndpoints(t *testing.T) {
	t.Parallel()

	kafkaLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	healthLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	cfg := DefaultConfig()
	cfg.Listener = kafkaLn
	cfg.HealthAddr = healthLn.Addr().String()
	cfg.HealthListener = healthLn
	cfg.DataDir = t.TempDir()
	cfg.LogLevel = "error"

	b := New(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- b.Run(ctx) }()

	select {
	case <-b.Ready():
	case <-time.After(5 * time.Second):
		t.Fatal("broker did not become ready")
	}

	base := "http://" + healthLn.Addr().String()
	client := &http.Client{Timeout: 2 * time.Second}

	// /livez should return 200
	resp, err := client.Get(base + "/livez")
	if err != nil {
		t.Fatal("GET /livez:", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("/livez: got status %d, want 200", resp.StatusCode)
	}
	if string(body) != "ok" {
		t.Fatalf("/livez: got body %q, want %q", body, "ok")
	}

	// /readyz should return 200 (broker is ready)
	resp, err = client.Get(base + "/readyz")
	if err != nil {
		t.Fatal("GET /readyz:", err)
	}
	body, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("/readyz: got status %d, want 200", resp.StatusCode)
	}
	if string(body) != "ok" {
		t.Fatalf("/readyz: got body %q, want %q", body, "ok")
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal("Run:", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("broker did not stop")
	}
}

func TestPrimaryz(t *testing.T) {
	t.Parallel()

	kafkaLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	healthLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	cfg := DefaultConfig()
	cfg.Listener = kafkaLn
	cfg.HealthAddr = healthLn.Addr().String()
	cfg.HealthListener = healthLn
	cfg.DataDir = t.TempDir()
	cfg.LogLevel = "error"

	b := New(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- b.Run(ctx) }()

	select {
	case <-b.Ready():
	case <-time.After(5 * time.Second):
		t.Fatal("broker did not become ready")
	}

	base := "http://" + healthLn.Addr().String()
	client := &http.Client{Timeout: 2 * time.Second}

	// Single-broker (no replication) — always primary
	resp, err := client.Get(base + "/primaryz")
	if err != nil {
		t.Fatal("GET /primaryz:", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("/primaryz (single): got status %d, want 200", resp.StatusCode)
	}
	if string(body) != "primary" {
		t.Fatalf("/primaryz (single): got body %q, want %q", body, "primary")
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal("Run:", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("broker did not stop")
	}
}

func TestPrimaryzStandby(t *testing.T) {
	t.Parallel()

	// Simulate a standby: replication configured, role = 0 (standby), broker ready.
	b := &Broker{
		ready: make(chan struct{}),
		cfg:   Config{ReplicationAddr: ":9093"},
	}
	close(b.ready)
	// replRole defaults to 0 (standby)

	mux := http.NewServeMux()
	mux.HandleFunc("/primaryz", b.handlePrimaryz)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: mux}
	go func() { _ = srv.Serve(ln) }()
	defer func() { _ = srv.Close() }()

	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get("http://" + ln.Addr().String() + "/primaryz")
	if err != nil {
		t.Fatal("GET /primaryz:", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("/primaryz (standby): got status %d, want %d", resp.StatusCode, http.StatusServiceUnavailable)
	}
	if string(body) != "standby" {
		t.Fatalf("/primaryz (standby): got body %q, want %q", body, "standby")
	}
}

func TestPrimaryzBeforeReady(t *testing.T) {
	t.Parallel()

	b := &Broker{
		ready: make(chan struct{}), // not closed = not ready
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/primaryz", b.handlePrimaryz)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: mux}
	go func() { _ = srv.Serve(ln) }()
	defer func() { _ = srv.Close() }()

	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get("http://" + ln.Addr().String() + "/primaryz")
	if err != nil {
		t.Fatal("GET /primaryz:", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("/primaryz before ready: got status %d, want %d", resp.StatusCode, http.StatusServiceUnavailable)
	}
	if string(body) != "not ready" {
		t.Fatalf("/primaryz before ready: got body %q, want %q", body, "not ready")
	}
}

func TestReadyzBeforeReady(t *testing.T) {
	t.Parallel()

	// Test that /readyz returns 503 before the broker is ready by calling
	// the handler directly on an un-readied broker.
	b := &Broker{
		ready: make(chan struct{}), // not closed = not ready
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/readyz", b.handleReadyz)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: mux}
	go func() { _ = srv.Serve(ln) }()
	defer func() { _ = srv.Close() }()

	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get("http://" + ln.Addr().String() + "/readyz")
	if err != nil {
		t.Fatal("GET /readyz:", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("/readyz before ready: got status %d, want %d", resp.StatusCode, http.StatusServiceUnavailable)
	}
	if string(body) != "not ready" {
		t.Fatalf("/readyz before ready: got body %q, want %q", body, "not ready")
	}
}
