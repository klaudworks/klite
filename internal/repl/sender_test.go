package repl

import (
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"
)

func TestSenderSendAndACK(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close() //nolint:errcheck

	s := NewSender(client, 5*time.Second, slog.Default())
	defer s.Close()

	// Read WAL_BATCH in background (net.Pipe is synchronous)
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		msgType, _, err := ReadFrame(server)
		if err != nil {
			t.Errorf("ReadFrame: %v", err)
			return
		}
		if msgType != MsgWALBatch {
			t.Errorf("expected MsgWALBatch, got %#x", msgType)
			return
		}
		// Send ACK back
		if err := WriteFrame(server, MsgACK, MarshalACK(5)); err != nil {
			t.Errorf("WriteFrame ACK: %v", err)
		}
	}()

	ch := s.Send([]byte("batch-data"), 1, 5)

	select {
	case err := <-ch:
		if err != nil {
			t.Fatalf("expected nil error, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for ACK resolution")
	}
	<-readDone
}

func TestSenderACKTimeout(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close() //nolint:errcheck

	s := NewSender(client, 100*time.Millisecond, slog.Default())
	defer s.Close()

	// Read the frame in background but don't ACK
	go func() {
		_, _, _ = ReadFrame(server)
	}()

	ch := s.Send([]byte("batch-data"), 1, 5)

	select {
	case err := <-ch:
		if err == nil {
			t.Fatal("expected timeout error, got nil")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for error")
	}
}

func TestSenderACKCoalesced(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close() //nolint:errcheck

	s := NewSender(client, 5*time.Second, slog.Default())
	defer s.Close()

	// Read all frames in background
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		// Read first WAL_BATCH
		if _, _, err := ReadFrame(server); err != nil {
			t.Errorf("read batch 1: %v", err)
			return
		}
		// Read second WAL_BATCH
		if _, _, err := ReadFrame(server); err != nil {
			t.Errorf("read batch 2: %v", err)
			return
		}
		// Send a single coalesced ACK for seq=10
		if err := WriteFrame(server, MsgACK, MarshalACK(10)); err != nil {
			t.Errorf("write ACK: %v", err)
		}
	}()

	ch1 := s.Send([]byte("batch-1"), 1, 5)
	ch2 := s.Send([]byte("batch-2"), 6, 10)

	// Both channels should resolve with nil
	for i, ch := range []<-chan error{ch1, ch2} {
		select {
		case err := <-ch:
			if err != nil {
				t.Fatalf("ch%d: expected nil, got %v", i+1, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("ch%d: timeout", i+1)
		}
	}
	<-readDone
}

func TestSenderSendMeta(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close() //nolint:errcheck

	s := NewSender(client, 5*time.Second, slog.Default())
	defer s.Close()

	entry := []byte("meta-entry-data")

	// Read in background (net.Pipe is synchronous)
	readDone := make(chan struct{})
	var gotType byte
	var gotPayload []byte
	go func() {
		defer close(readDone)
		var err error
		gotType, gotPayload, err = ReadFrame(server)
		if err != nil {
			t.Errorf("ReadFrame: %v", err)
		}
	}()

	s.SendMeta(entry)
	<-readDone

	if gotType != MsgMetaEntry {
		t.Fatalf("expected MsgMetaEntry, got %#x", gotType)
	}
	if string(gotPayload) != string(entry) {
		t.Fatalf("payload: got %q, want %q", gotPayload, entry)
	}
}

func TestSenderSendMetaWriteError(t *testing.T) {
	server, client := net.Pipe()

	s := NewSender(client, 100*time.Millisecond, slog.Default())

	// Close the server side to cause write errors
	_ = server.Close()

	// Wait for ackReader to detect the close
	time.Sleep(50 * time.Millisecond)

	// Should not panic
	s.SendMeta([]byte("meta-entry"))

	s.Close()
}

func TestSenderDisconnect(t *testing.T) {
	server, client := net.Pipe()

	s := NewSender(client, 5*time.Second, slog.Default())

	// Read the WAL_BATCH in background
	go func() {
		_, _, _ = ReadFrame(server)
		// Close server side to simulate disconnect
		_ = server.Close()
	}()

	ch := s.Send([]byte("batch-data"), 1, 5)

	select {
	case err := <-ch:
		if err == nil {
			t.Fatal("expected error on disconnect, got nil")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for disconnect error")
	}

	s.Close()
}

func TestSenderConcurrentSendAndSendMeta(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close() //nolint:errcheck

	s := NewSender(client, 5*time.Second, slog.Default())
	defer s.Close()

	// Read all frames in background
	go func() {
		for {
			_, _, err := ReadFrame(server)
			if err != nil {
				return
			}
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			s.Send([]byte("batch"), 1, 1)
		}()
		go func() {
			defer wg.Done()
			s.SendMeta([]byte("meta"))
		}()
	}
	wg.Wait()
}

func TestSenderWriteDeadline(t *testing.T) {
	server, client := net.Pipe()

	s := NewSender(client, 100*time.Millisecond, slog.Default())

	// net.Pipe() is synchronous: writes block until the other side reads.
	// Don't read the server side, so the write blocks and hits the deadline.
	go func() {
		time.Sleep(500 * time.Millisecond)
		_ = server.Close()
	}()

	ch := s.Send(make([]byte, 64), 1, 1)

	select {
	case err := <-ch:
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for write deadline error")
	}

	s.Close()
}

func TestSenderConnected(t *testing.T) {
	server, client := net.Pipe()

	s := NewSender(client, 5*time.Second, slog.Default())

	if !s.Connected() {
		t.Fatal("expected Connected() to be true after creation")
	}

	_ = server.Close()
	s.Close()

	if s.Connected() {
		t.Fatal("expected Connected() to be false after close")
	}
}
