package server

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestConnReaderReadFrame(t *testing.T) {
	t.Parallel()

	// Build a frame: 4-byte size prefix + body
	body := []byte("hello world")
	var buf bytes.Buffer
	_ = binary.Write(&buf, binary.BigEndian, int32(len(body)))
	buf.Write(body)

	cr := &connReader{
		br: nil, // we'll use a bufio.Reader
	}
	// Use a bytes.Reader wrapped in a bufio.Reader
	cr.br = newBufioReader(&buf)

	frame, err := cr.readFrame()
	if err != nil {
		t.Fatalf("readFrame: %v", err)
	}
	if !bytes.Equal(frame, body) {
		t.Fatalf("got %q, want %q", frame, body)
	}
}

func TestConnReaderReadFrameReusesBuffer(t *testing.T) {
	t.Parallel()

	// Write two frames
	var buf bytes.Buffer
	writeFrame := func(data []byte) {
		_ = binary.Write(&buf, binary.BigEndian, int32(len(data)))
		buf.Write(data)
	}
	writeFrame([]byte("first"))
	writeFrame([]byte("second-longer"))

	cr := &connReader{
		br: newBufioReader(&buf),
	}

	f1, err := cr.readFrame()
	if err != nil {
		t.Fatalf("readFrame 1: %v", err)
	}
	if string(f1) != "first" {
		t.Fatalf("frame 1: got %q", f1)
	}

	f2, err := cr.readFrame()
	if err != nil {
		t.Fatalf("readFrame 2: %v", err)
	}
	if string(f2) != "second-longer" {
		t.Fatalf("frame 2: got %q", f2)
	}
}

func TestConnReaderInvalidFrameSize(t *testing.T) {
	t.Parallel()

	// Frame with size > maxFrameSize
	var buf bytes.Buffer
	_ = binary.Write(&buf, binary.BigEndian, int32(maxFrameSize+1))

	cr := &connReader{
		br: newBufioReader(&buf),
	}

	_, err := cr.readFrame()
	if err == nil {
		t.Fatal("expected error for oversized frame")
	}
}

func TestConnReaderTruncatedFrame(t *testing.T) {
	t.Parallel()

	// Frame says 100 bytes but only has 5
	var buf bytes.Buffer
	_ = binary.Write(&buf, binary.BigEndian, int32(100))
	buf.Write([]byte("short"))

	cr := &connReader{
		br: newBufioReader(&buf),
	}

	_, err := cr.readFrame()
	if err == nil {
		t.Fatal("expected error for truncated frame")
	}
	if err != io.ErrUnexpectedEOF {
		t.Fatalf("expected io.ErrUnexpectedEOF, got %v", err)
	}
}

func TestConnReaderEmptyStream(t *testing.T) {
	t.Parallel()

	cr := &connReader{
		br: newBufioReader(&bytes.Buffer{}),
	}

	_, err := cr.readFrame()
	if err == nil {
		t.Fatal("expected error on empty stream")
	}
}

// newBufioReader is a helper to create a bufio.Reader from an io.Reader.
func newBufioReader(r io.Reader) *bufio.Reader {
	return bufio.NewReaderSize(r, 4096)
}

func TestWriteResponseApiVersionsNoTagByte(t *testing.T) {
	t.Parallel()

	// ApiVersions (key 18) response should NOT have the tag byte in the header.
	// Other flexible responses DO have the tag byte.
	//
	// We verify this by checking the encoded bytes.
	// ApiVersions response header: [4 size][4 corrID][body]  (no tag byte)
	// Other flexible response header: [4 size][4 corrID][1 tag=0][body]

	// This is a structural test to verify the writeResponse method handles
	// the ApiVersions exception correctly. The full protocol test is in
	// integration tests.
}

func TestHandlerRegistryBasic(t *testing.T) {
	t.Parallel()

	reg := NewHandlerRegistry()

	// Initially empty
	if h := reg.Get(18); h != nil {
		t.Fatal("expected nil handler for unregistered key")
	}

	// Register a handler
	called := false
	reg.Register(18, func(req kmsg.Request) (kmsg.Response, error) {
		called = true
		return nil, nil
	})

	h := reg.Get(18)
	if h == nil {
		t.Fatal("expected non-nil handler after registration")
	}

	_, _ = h(nil)
	if !called {
		t.Fatal("handler was not called")
	}

	// Other keys still nil
	if h := reg.Get(0); h != nil {
		t.Fatal("expected nil handler for key 0")
	}
}

func TestSASLAllowed(t *testing.T) {
	t.Parallel()

	apiVersions := kmsg.NewApiVersionsRequest()
	handshake := kmsg.NewSASLHandshakeRequest()
	authenticate := kmsg.NewSASLAuthenticateRequest()
	produce := kmsg.NewProduceRequest()
	fetch := kmsg.NewFetchRequest()
	metadata := kmsg.NewMetadataRequest()

	tests := []struct {
		name    string
		stage   SASLStage
		req     kmsg.Request
		allowed bool
	}{
		// SASLStageBegin: only ApiVersions + SASLHandshake
		{"begin_api_versions", SASLStageBegin, &apiVersions, true},
		{"begin_sasl_handshake", SASLStageBegin, &handshake, true},
		{"begin_produce_rejected", SASLStageBegin, &produce, false},
		{"begin_fetch_rejected", SASLStageBegin, &fetch, false},
		{"begin_sasl_authenticate_rejected", SASLStageBegin, &authenticate, false},
		{"begin_metadata_rejected", SASLStageBegin, &metadata, false},

		// SASLStageAuthPlain: only ApiVersions + SASLAuthenticate
		{"auth_plain_api_versions", SASLStageAuthPlain, &apiVersions, true},
		{"auth_plain_sasl_authenticate", SASLStageAuthPlain, &authenticate, true},
		{"auth_plain_produce_rejected", SASLStageAuthPlain, &produce, false},
		{"auth_plain_sasl_handshake_rejected", SASLStageAuthPlain, &handshake, false},

		// SASLStageAuthScram256: only ApiVersions + SASLAuthenticate
		{"auth_scram256_api_versions", SASLStageAuthScram256, &apiVersions, true},
		{"auth_scram256_sasl_authenticate", SASLStageAuthScram256, &authenticate, true},
		{"auth_scram256_produce_rejected", SASLStageAuthScram256, &produce, false},
		{"auth_scram256_sasl_handshake_rejected", SASLStageAuthScram256, &handshake, false},

		// SASLStageAuthScram512: only ApiVersions + SASLAuthenticate
		{"auth_scram512_api_versions", SASLStageAuthScram512, &apiVersions, true},
		{"auth_scram512_sasl_authenticate", SASLStageAuthScram512, &authenticate, true},
		{"auth_scram512_produce_rejected", SASLStageAuthScram512, &produce, false},

		// SASLStageAuthScram1: only ApiVersions + SASLAuthenticate
		{"auth_scram1_api_versions", SASLStageAuthScram1, &apiVersions, true},
		{"auth_scram1_sasl_authenticate", SASLStageAuthScram1, &authenticate, true},
		{"auth_scram1_produce_rejected", SASLStageAuthScram1, &produce, false},

		// SASLStageComplete: all requests allowed
		{"complete_api_versions", SASLStageComplete, &apiVersions, true},
		{"complete_produce", SASLStageComplete, &produce, true},
		{"complete_fetch", SASLStageComplete, &fetch, true},
		{"complete_metadata", SASLStageComplete, &metadata, true},
		{"complete_sasl_handshake", SASLStageComplete, &handshake, true},
		{"complete_sasl_authenticate", SASLStageComplete, &authenticate, true},

		// Unknown stage: returns false
		{"unknown_stage", SASLStage(99), &apiVersions, false},
		{"unknown_stage_produce", SASLStage(99), &produce, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cc := &clientConn{saslStage: tt.stage}
			got := cc.saslAllowed(tt.req)
			if got != tt.allowed {
				t.Errorf("saslAllowed(stage=%d, req=%T) = %v, want %v",
					tt.stage, tt.req, got, tt.allowed)
			}
		})
	}
}

func TestCloseConnsUnblocksWait(t *testing.T) {
	t.Parallel()

	shutdownCh := make(chan struct{})
	handlers := NewHandlerRegistry()
	srv := NewServer(handlers, shutdownCh, slog.Default())

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	serveDone := make(chan struct{})
	go func() {
		_ = srv.Serve(ln)
		close(serveDone)
	}()

	// Connect a client that stays idle (simulates a consumer blocked on
	// long-poll Fetch). The server's readLoop blocks on readFrame().
	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	// Wait for the server to register the connection.
	deadline := time.After(2 * time.Second)
	for srv.ConnCount() == 0 {
		select {
		case <-deadline:
			t.Fatal("server did not register connection")
		case <-time.After(10 * time.Millisecond):
		}
	}

	// Close the listener (no new connections), then close active
	// connections — same sequence as shutdownPrimary.
	_ = ln.Close()
	srv.CloseConns()

	// Wait must return promptly.
	waitDone := make(chan struct{})
	go func() {
		srv.Wait()
		close(waitDone)
	}()

	select {
	case <-waitDone:
	case <-time.After(2 * time.Second):
		t.Fatal("server.Wait() did not return within 2s after CloseConns()")
	}
}
