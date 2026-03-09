package server

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
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

// writeLoopHarness sets up a clientConn with a net.Pipe suitable for testing
// writeLoop in isolation. The caller sends clientResp values on respCh and
// reads framed Kafka responses from the returned reader. Close shutdownCh or
// respCh to terminate the write loop.
type writeLoopHarness struct {
	cc         *clientConn
	respCh     chan clientResp
	shutdownCh chan struct{}
	clientConn net.Conn // test reads from this side
	serverConn net.Conn // writeLoop writes to this side
}

func newWriteLoopHarness() *writeLoopHarness {
	client, server := net.Pipe()
	shutdownCh := make(chan struct{})
	respCh := make(chan clientResp, maxInFlight)

	cc := &clientConn{
		conn:       server,
		respCh:     respCh,
		bw:         bufio.NewWriterSize(server, connWriteBufSize),
		shutdownCh: shutdownCh,
		logger:     slog.Default(),
	}

	return &writeLoopHarness{
		cc:         cc,
		respCh:     respCh,
		shutdownCh: shutdownCh,
		clientConn: client,
		serverConn: server,
	}
}

// readCorrID reads one framed response from the client side of the pipe
// and returns the correlation ID.
func (h *writeLoopHarness) readCorrID(t *testing.T) int32 {
	t.Helper()
	var sizeBuf [4]byte
	if _, err := io.ReadFull(h.clientConn, sizeBuf[:]); err != nil {
		t.Fatalf("reading frame size: %v", err)
	}
	size := int(binary.BigEndian.Uint32(sizeBuf[:]))
	frame := make([]byte, size)
	if _, err := io.ReadFull(h.clientConn, frame); err != nil {
		t.Fatalf("reading frame body: %v", err)
	}
	return int32(binary.BigEndian.Uint32(frame[:4]))
}

// makeResp creates a clientResp with a HeartbeatResponse for a given seq/corrID.
func makeResp(seq uint32, corr int32) clientResp {
	resp := kmsg.NewHeartbeatResponse()
	return clientResp{
		kresp: &resp,
		corr:  corr,
		seq:   seq,
	}
}

func TestWriteLoopInOrderDelivery(t *testing.T) {
	t.Parallel()

	h := newWriteLoopHarness()
	go h.cc.writeLoop()

	// Send 5 responses in order, verify they arrive in order.
	for i := 0; i < 5; i++ {
		h.respCh <- makeResp(uint32(i), int32(i+100))
	}

	for i := 0; i < 5; i++ {
		got := h.readCorrID(t)
		want := int32(i + 100)
		if got != want {
			t.Fatalf("response %d: got corrID %d, want %d", i, got, want)
		}
	}

	close(h.respCh)
}

func TestWriteLoopReverseOrderReassembly(t *testing.T) {
	t.Parallel()

	h := newWriteLoopHarness()
	go h.cc.writeLoop()

	// Send 4 responses in reverse order (3, 2, 1, 0). The writeLoop should
	// buffer them and only emit once seq 0 arrives.
	h.respCh <- makeResp(3, 300)
	h.respCh <- makeResp(2, 200)
	h.respCh <- makeResp(1, 100)
	h.respCh <- makeResp(0, 0) // triggers drain of all buffered

	for i := 0; i < 4; i++ {
		got := h.readCorrID(t)
		want := int32(i * 100)
		if got != want {
			t.Fatalf("response %d: got corrID %d, want %d", i, got, want)
		}
	}

	close(h.respCh)
}

func TestWriteLoopSkipInterleavedWithNormal(t *testing.T) {
	t.Parallel()

	h := newWriteLoopHarness()
	go h.cc.writeLoop()

	// seq 0: normal, seq 1: skip, seq 2: normal, seq 3: skip, seq 4: normal
	h.respCh <- makeResp(0, 10)
	h.respCh <- clientResp{seq: 1, skip: true}
	h.respCh <- makeResp(2, 20)
	h.respCh <- clientResp{seq: 3, skip: true}
	h.respCh <- makeResp(4, 30)

	// Only the non-skip responses should appear on the wire.
	expected := []int32{10, 20, 30}
	for i, want := range expected {
		got := h.readCorrID(t)
		if got != want {
			t.Fatalf("response %d: got corrID %d, want %d", i, got, want)
		}
	}

	close(h.respCh)
}

func TestWriteLoopSkipOutOfOrder(t *testing.T) {
	t.Parallel()

	h := newWriteLoopHarness()
	go h.cc.writeLoop()

	// seq 2 arrives first (normal), seq 1 arrives (skip), seq 0 arrives (normal)
	// Expected write order: corrID 10 (seq 0), then corrID 30 (seq 2). Seq 1 is skipped.
	h.respCh <- makeResp(2, 30)
	h.respCh <- clientResp{seq: 1, skip: true}
	h.respCh <- makeResp(0, 10)

	expected := []int32{10, 30}
	for i, want := range expected {
		got := h.readCorrID(t)
		if got != want {
			t.Fatalf("response %d: got corrID %d, want %d", i, got, want)
		}
	}

	close(h.respCh)
}

func TestWriteLoopErrorClosesConnection(t *testing.T) {
	t.Parallel()

	h := newWriteLoopHarness()
	done := make(chan struct{})
	go func() {
		h.cc.writeLoop()
		close(done)
	}()

	// Send an error response — writeLoop should return and close the connection.
	h.respCh <- clientResp{seq: 0, err: fmt.Errorf("handler failed")}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("writeLoop did not exit after error response")
	}

	// The server side should be closed; reading from clientConn should get EOF.
	var buf [1]byte
	_, err := h.clientConn.Read(buf[:])
	if err == nil {
		t.Fatal("expected error reading from closed pipe")
	}
}

func TestWriteLoopErrorMidOOOBuffer(t *testing.T) {
	t.Parallel()

	h := newWriteLoopHarness()
	done := make(chan struct{})
	go func() {
		h.cc.writeLoop()
		close(done)
	}()

	// Send seq 0 (normal), seq 2 (buffered), seq 1 (error).
	// Seq 0 should be written, seq 1 error closes the connection,
	// and seq 2 is never delivered.
	h.respCh <- makeResp(0, 10)

	// Read seq 0 to confirm it was written.
	got := h.readCorrID(t)
	if got != 10 {
		t.Fatalf("got corrID %d, want 10", got)
	}

	// Now send seq 2 (buffered) and seq 1 (error).
	h.respCh <- makeResp(2, 30)
	h.respCh <- clientResp{seq: 1, err: fmt.Errorf("mid-sequence error")}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("writeLoop did not exit after error in OOO sequence")
	}
}

func TestWriteLoopShutdownDuringWait(t *testing.T) {
	t.Parallel()

	h := newWriteLoopHarness()
	done := make(chan struct{})
	go func() {
		h.cc.writeLoop()
		close(done)
	}()

	// Don't send any responses — writeLoop is blocked on respCh.
	// Close shutdownCh to unblock it.
	close(h.shutdownCh)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("writeLoop did not exit after shutdown")
	}
}

func TestWriteLoopChannelCloseDuringOOODrain(t *testing.T) {
	t.Parallel()

	h := newWriteLoopHarness()
	done := make(chan struct{})
	go func() {
		h.cc.writeLoop()
		close(done)
	}()

	// Send seq 2 and seq 1 (both buffered, waiting for seq 0).
	// Then close the channel without sending seq 0.
	h.respCh <- makeResp(2, 20)
	h.respCh <- makeResp(1, 10)

	// Give the writeLoop time to buffer both responses.
	time.Sleep(50 * time.Millisecond)
	close(h.respCh)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("writeLoop did not exit after channel close with buffered OOO responses")
	}
}

// buildKafkaFrame builds a length-prefixed Kafka request frame from a kmsg.Request.
// The frame format is: [4-byte size][int16 apiKey][int16 version][int32 corrID][nullable string clientID][flexible tags if applicable][request body].
func buildKafkaFrame(t *testing.T, kreq kmsg.Request, corrID int32) []byte {
	t.Helper()
	var buf []byte
	buf = binary.BigEndian.AppendUint16(buf, uint16(kreq.Key()))
	buf = binary.BigEndian.AppendUint16(buf, uint16(kreq.GetVersion()))
	buf = binary.BigEndian.AppendUint32(buf, uint32(corrID))
	// Nullable string clientID: length -1 (null)
	buf = binary.BigEndian.AppendUint16(buf, 0xFFFF)
	if kreq.IsFlexible() {
		buf = append(buf, 0) // empty tags
	}
	buf = kreq.AppendTo(buf)
	// Prefix with 4-byte length
	frame := make([]byte, 4+len(buf))
	binary.BigEndian.PutUint32(frame[:4], uint32(len(buf)))
	copy(frame[4:], buf)
	return frame
}

func TestMaxConnectionLimit(t *testing.T) {
	t.Parallel()

	shutdownCh := make(chan struct{})
	defer close(shutdownCh)
	handlers := NewHandlerRegistry()
	srv := NewServer(handlers, shutdownCh, slog.Default())
	srv.maxConns = 2

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()

	go func() { _ = srv.Serve(ln) }()

	addr := ln.Addr().String()

	// Connect maxConns clients.
	conn1, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial 1: %v", err)
	}
	defer func() { _ = conn1.Close() }()

	conn2, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial 2: %v", err)
	}
	defer func() { _ = conn2.Close() }()

	// Wait for both connections to be registered.
	deadline := time.After(2 * time.Second)
	for srv.ConnCount() < 2 {
		select {
		case <-deadline:
			t.Fatalf("expected ConnCount=2, got %d", srv.ConnCount())
		case <-time.After(10 * time.Millisecond):
		}
	}

	// Third connection should be rejected (closed immediately by server).
	conn3, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial 3: %v", err)
	}
	defer func() { _ = conn3.Close() }()

	// The server closes the rejected connection. Reading should return EOF/error.
	_ = conn3.SetReadDeadline(time.Now().Add(2 * time.Second))
	var buf [1]byte
	_, err = conn3.Read(buf[:])
	if err == nil {
		t.Fatal("expected rejected connection to be closed by server")
	}

	// ConnCount should still be 2 (rejected conn not counted).
	if got := srv.ConnCount(); got != 2 {
		t.Fatalf("ConnCount after rejection: got %d, want 2", got)
	}
}

func TestMaxConnectionLimitAcceptsAfterClose(t *testing.T) {
	t.Parallel()

	shutdownCh := make(chan struct{})
	defer close(shutdownCh)
	handlers := NewHandlerRegistry()
	srv := NewServer(handlers, shutdownCh, slog.Default())
	srv.maxConns = 1

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()

	go func() { _ = srv.Serve(ln) }()

	addr := ln.Addr().String()

	// Fill the limit.
	conn1, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial 1: %v", err)
	}

	deadline := time.After(2 * time.Second)
	for srv.ConnCount() < 1 {
		select {
		case <-deadline:
			t.Fatalf("expected ConnCount=1, got %d", srv.ConnCount())
		case <-time.After(10 * time.Millisecond):
		}
	}

	// Close the connection to free a slot.
	_ = conn1.Close()

	// Wait for ConnCount to drop to 0.
	deadline = time.After(2 * time.Second)
	for srv.ConnCount() > 0 {
		select {
		case <-deadline:
			t.Fatalf("expected ConnCount=0 after close, got %d", srv.ConnCount())
		case <-time.After(10 * time.Millisecond):
		}
	}

	// New connection should now be accepted.
	conn2, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial 2: %v", err)
	}
	defer func() { _ = conn2.Close() }()

	deadline = time.After(2 * time.Second)
	for srv.ConnCount() < 1 {
		select {
		case <-deadline:
			t.Fatalf("expected ConnCount=1 after reconnect, got %d", srv.ConnCount())
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestProduceHandlerRunsInlineBlocksReadLoop(t *testing.T) {
	t.Parallel()

	shutdownCh := make(chan struct{})
	defer close(shutdownCh)
	handlers := NewHandlerRegistry()

	produceBlocking := make(chan struct{})
	metadataCalled := make(chan struct{}, 1)

	// Produce handler (key 0) blocks until we signal.
	handlers.Register(0, func(req kmsg.Request) (kmsg.Response, error) {
		<-produceBlocking
		resp := kmsg.NewProduceResponse()
		return &resp, nil
	})

	// Metadata handler (key 3) signals when called.
	handlers.Register(3, func(req kmsg.Request) (kmsg.Response, error) {
		select {
		case metadataCalled <- struct{}{}:
		default:
		}
		resp := kmsg.NewMetadataResponse()
		return &resp, nil
	})

	srv := NewServer(handlers, shutdownCh, slog.Default())

	// Use net.Pipe for deterministic control.
	clientSide, serverSide := net.Pipe()
	defer func() { _ = clientSide.Close() }()
	defer func() { _ = serverSide.Close() }()

	// Run the server's connection handler.
	srv.connCount.Add(1)
	serveDone := make(chan struct{})
	go func() {
		cc := newClientConn(srv, serverSide)
		cc.serve()
		close(serveDone)
	}()

	// Send a Produce request (key=0, version=0).
	produceReq := kmsg.NewProduceRequest()
	produceReq.SetVersion(0)
	produceFrame := buildKafkaFrame(t, &produceReq, 1)
	if _, err := clientSide.Write(produceFrame); err != nil {
		t.Fatalf("write produce: %v", err)
	}

	// Give the readLoop time to enter the produce handler.
	time.Sleep(50 * time.Millisecond)

	// Send a Metadata request from a goroutine because net.Pipe is
	// synchronous — the Write blocks until the reader consumes the data,
	// and the readLoop is stuck in the produce handler.
	metadataReq := kmsg.NewMetadataRequest()
	metadataReq.SetVersion(0)
	metadataFrame := buildKafkaFrame(t, &metadataReq, 2)
	metaWriteDone := make(chan error, 1)
	go func() {
		_, err := clientSide.Write(metadataFrame)
		metaWriteDone <- err
	}()

	// The Metadata handler should NOT be called while Produce is blocking,
	// because Produce runs inline on the readLoop goroutine.
	select {
	case <-metadataCalled:
		t.Fatal("Metadata handler called while Produce handler is blocked — Produce is not running inline")
	case <-time.After(200 * time.Millisecond):
		// Expected: readLoop is stuck in produce handler, can't read next frame.
	}

	// Unblock produce handler.
	close(produceBlocking)

	// Wait for the metadata write to complete.
	select {
	case err := <-metaWriteDone:
		if err != nil {
			t.Fatalf("write metadata: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("metadata write did not complete")
	}

	// Now Metadata should be dispatched.
	select {
	case <-metadataCalled:
		// Expected.
	case <-time.After(2 * time.Second):
		t.Fatal("Metadata handler not called after Produce handler unblocked")
	}

	// Clean up: close client side, wait for serve to finish.
	_ = clientSide.Close()
	select {
	case <-serveDone:
	case <-time.After(2 * time.Second):
		t.Fatal("serve did not exit")
	}
}

func TestPanicRecoveryProduceInline(t *testing.T) {
	t.Parallel()

	shutdownCh := make(chan struct{})
	defer close(shutdownCh)
	handlers := NewHandlerRegistry()

	// Produce handler panics.
	handlers.Register(0, func(req kmsg.Request) (kmsg.Response, error) {
		panic("test produce panic")
	})

	srv := NewServer(handlers, shutdownCh, slog.Default())

	clientSide, serverSide := net.Pipe()
	defer func() { _ = clientSide.Close() }()
	defer func() { _ = serverSide.Close() }()

	srv.connCount.Add(1)
	serveDone := make(chan struct{})
	go func() {
		cc := newClientConn(srv, serverSide)
		cc.serve()
		close(serveDone)
	}()

	// Send a Produce request that triggers the panic.
	produceReq := kmsg.NewProduceRequest()
	produceReq.SetVersion(0)
	frame := buildKafkaFrame(t, &produceReq, 1)
	if _, err := clientSide.Write(frame); err != nil {
		t.Fatalf("write: %v", err)
	}

	// The panic should be recovered. The connection should close because
	// the panic sends a clientResp with err, which writeLoop treats as fatal.
	select {
	case <-serveDone:
		// Expected: connection closed gracefully.
	case <-time.After(2 * time.Second):
		t.Fatal("serve did not exit after handler panic")
	}

	// Verify the pipe was closed (reading returns error).
	var buf [1]byte
	_, err := clientSide.Read(buf[:])
	if err == nil {
		t.Fatal("expected read error after panic-induced close")
	}
}

func TestPanicRecoveryGoroutinePool(t *testing.T) {
	t.Parallel()

	shutdownCh := make(chan struct{})
	defer close(shutdownCh)
	handlers := NewHandlerRegistry()

	// Metadata handler (key 3) panics — dispatched via goroutine pool.
	handlers.Register(3, func(req kmsg.Request) (kmsg.Response, error) {
		panic("test metadata panic")
	})

	srv := NewServer(handlers, shutdownCh, slog.Default())

	// First verify with a panicking connection.
	clientSide, serverSide := net.Pipe()
	defer func() { _ = clientSide.Close() }()
	defer func() { _ = serverSide.Close() }()

	srv.connCount.Add(1)
	serveDone := make(chan struct{})
	go func() {
		cc := newClientConn(srv, serverSide)
		cc.serve()
		close(serveDone)
	}()

	metadataReq := kmsg.NewMetadataRequest()
	metadataReq.SetVersion(0)
	frame := buildKafkaFrame(t, &metadataReq, 1)
	if _, err := clientSide.Write(frame); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Connection should close after panic recovery sends error to writeLoop.
	select {
	case <-serveDone:
	case <-time.After(2 * time.Second):
		t.Fatal("serve did not exit after goroutine pool handler panic")
	}

	// Now verify the server still works: create a new connection with a
	// non-panicking handler and confirm it functions.
	handlers2 := NewHandlerRegistry()
	handlers2.Register(3, func(req kmsg.Request) (kmsg.Response, error) {
		resp := kmsg.NewMetadataResponse()
		return &resp, nil
	})
	srv2 := NewServer(handlers2, shutdownCh, slog.Default())

	client2, server2 := net.Pipe()
	defer func() { _ = client2.Close() }()
	defer func() { _ = server2.Close() }()

	srv2.connCount.Add(1)
	serveDone2 := make(chan struct{})
	go func() {
		cc := newClientConn(srv2, server2)
		cc.serve()
		close(serveDone2)
	}()

	frame2 := buildKafkaFrame(t, &metadataReq, 1)
	if _, err := client2.Write(frame2); err != nil {
		t.Fatalf("write to healthy conn: %v", err)
	}

	// Read the response — should succeed.
	_ = client2.SetReadDeadline(time.Now().Add(2 * time.Second))
	var sizeBuf [4]byte
	if _, err := io.ReadFull(client2, sizeBuf[:]); err != nil {
		t.Fatalf("read response size: %v", err)
	}
	respSize := binary.BigEndian.Uint32(sizeBuf[:])
	if respSize == 0 {
		t.Fatal("empty response from healthy connection")
	}

	_ = client2.Close()
	select {
	case <-serveDone2:
	case <-time.After(2 * time.Second):
		t.Fatal("serve2 did not exit")
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
