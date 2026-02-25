package server

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"

	"github.com/klaudworks/klite/internal/sasl"
	"github.com/twmb/franz-go/pkg/kbin"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// SASLStage tracks the SASL authentication state for a connection.
type SASLStage uint8

const (
	SASLStageBegin        SASLStage = iota // Only ApiVersions + SASLHandshake allowed
	SASLStageAuthPlain                     // SASLAuthenticate: 1 round (PLAIN)
	SASLStageAuthScram256                  // SASLAuthenticate: round 1 of 2 (SCRAM-SHA-256)
	SASLStageAuthScram512                  // SASLAuthenticate: round 1 of 2 (SCRAM-SHA-512)
	SASLStageAuthScram1                    // SASLAuthenticate: round 2 of 2 (SCRAM)
	SASLStageComplete                      // Authenticated -- all requests allowed
)

const (
	connReadBufSize  = 64 * 1024 // 64KB read buffer
	connWriteBufSize = 64 * 1024 // 64KB write buffer
	maxFrameSize     = 100 << 20 // 100MB max frame size
	maxInFlight      = 100       // max pipelined requests per connection
)

// clientResp is a response to be written to the client, sent from handler
// goroutines to the write goroutine via respCh.
type clientResp struct {
	kresp kmsg.Response // typed response (write goroutine encodes it)
	corr  int32         // correlation ID
	seq   uint32        // monotonic sequence assigned in READ goroutine
	skip  bool          // true = no response to write (acks=0), just advance seq
	err   error         // non-nil = close connection
}

// connReader wraps a bufio.Reader with a reusable frame buffer.
type connReader struct {
	br  *bufio.Reader
	buf []byte // reused across requests, grown with append()
}

// readFrame reads a single Kafka wire frame (4-byte size prefix + body).
// The returned slice is valid only until the next readFrame() call.
func (cr *connReader) readFrame() ([]byte, error) {
	var sizeBuf [4]byte
	if _, err := io.ReadFull(cr.br, sizeBuf[:]); err != nil {
		return nil, err
	}
	size := int(binary.BigEndian.Uint32(sizeBuf[:]))

	// Validate frame size
	if size < 0 || size > maxFrameSize {
		return nil, fmt.Errorf("invalid frame size: %d", size)
	}

	// Reuse buffer, grow if needed
	if cap(cr.buf) < size {
		cr.buf = make([]byte, size)
	} else {
		cr.buf = cr.buf[:size]
	}
	if _, err := io.ReadFull(cr.br, cr.buf); err != nil {
		return nil, err
	}
	return cr.buf, nil
}

// clientConn manages a single client TCP connection.
type clientConn struct {
	server     *Server
	conn       net.Conn
	respCh     chan clientResp
	reader     connReader
	bw         *bufio.Writer
	writeBuf   []byte // reused encode buffer for response assembly
	shutdownCh <-chan struct{}
	logger     *slog.Logger

	// inflightSem limits the number of in-flight handler goroutines.
	// The read goroutine acquires before spawning; the handler releases.
	inflightSem chan struct{}

	// wg tracks handler goroutines for clean shutdown.
	wg sync.WaitGroup

	// SASL authentication state
	saslStage SASLStage
	scramS0   *sasl.ScramServer0 // SCRAM server-first state (between rounds 1 and 2)
	user      string             // authenticated username (set after auth completes)
}

// newClientConn creates a new clientConn wrapping the given net.Conn.
func newClientConn(s *Server, nc net.Conn) *clientConn {
	stage := SASLStageComplete // no auth required by default
	if s.saslEnabled {
		stage = SASLStageBegin
	}
	return &clientConn{
		server: s,
		conn:   nc,
		respCh: make(chan clientResp, maxInFlight),
		reader: connReader{
			br: bufio.NewReaderSize(nc, connReadBufSize),
		},
		bw:          bufio.NewWriterSize(nc, connWriteBufSize),
		shutdownCh:  s.shutdownCh,
		logger:      s.logger,
		inflightSem: make(chan struct{}, maxInFlight),
		saslStage:   stage,
	}
}

// serve runs the read and write goroutines for this connection.
// It blocks until the connection is closed.
func (cc *clientConn) serve() {
	go cc.writeLoop()
	cc.readLoop()
	// readLoop returned: wait for all handlers to finish,
	// then close respCh so writeLoop drains and exits.
	cc.wg.Wait()
	close(cc.respCh)
}

// readLoop reads frames from the connection, parses them, and spawns
// handler goroutines. Sequence numbers are assigned here in arrival order.
func (cc *clientConn) readLoop() {
	who := cc.conn.RemoteAddr()
	var nextSeq uint32

	for {
		frame, err := cc.reader.readFrame()
		if err != nil {
			// Check if this is a shutdown
			select {
			case <-cc.shutdownCh:
			default:
				if err != io.EOF {
					cc.logger.Debug("read error", "remote", who, "error", err)
				}
			}
			return
		}

		// Minimum header: apiKey(2) + apiVersion(2) + corrID(4) + clientIDLen(2) = 10 bytes
		if len(frame) < 10 {
			cc.logger.Debug("frame too short", "remote", who, "size", len(frame))
			return
		}

		reader := kbin.Reader{Src: frame}
		apiKey := reader.Int16()
		apiVersion := reader.Int16()
		corrID := reader.Int32()
		_ = reader.NullableString() // client ID (unused for now)

		// Get typed request struct for this API key
		kreq := kmsg.RequestForKey(apiKey)
		if kreq == nil {
			// Unknown API key: close connection (matches Kafka behavior)
			cc.logger.Debug("unknown API key, closing connection", "remote", who, "api_key", apiKey)
			return
		}
		kreq.SetVersion(apiVersion)

		// Flexible-version headers have tagged fields after the client ID.
		// Must skip them before passing reader.Src to kreq.ReadFrom().
		if kreq.IsFlexible() {
			kmsg.SkipTags(&reader)
		}

		// Copy the remaining body bytes so the parsed request owns its
		// memory. ReadFrom returns sub-slices (e.g. Records []byte) that
		// alias the input — without this copy, the next readFrame() call
		// would overwrite the reusable frame buffer and corrupt in-flight
		// handler data.
		bodyCopy := make([]byte, len(reader.Src))
		copy(bodyCopy, reader.Src)

		// ApiVersions (key 18) is special: if parsing fails due to an
		// unsupported version, we still dispatch to the handler so it can
		// return the version list with UNSUPPORTED_VERSION error code.
		// This lets the client negotiate down to a supported version.
		if err := kreq.ReadFrom(bodyCopy); err != nil {
			if apiKey == 18 {
				// For ApiVersions, ignore parse errors from unsupported versions.
				// The handler will detect the version mismatch and respond accordingly.
				cc.logger.Debug("ApiVersions parse error (will still dispatch)",
					"remote", who, "version", apiVersion, "error", err)
			} else {
				cc.logger.Debug("parse error", "remote", who, "api_key", apiKey, "version", apiVersion, "error", err)
				return
			}
		}

		// SASL gate: if SASL is enabled, check whether this request
		// is allowed given the connection's current auth stage.
		if cc.saslStage != SASLStageComplete {
			if !cc.saslAllowed(kreq) {
				cc.logger.Debug("SASL gate: request not allowed before auth, closing",
					"remote", who, "api_key", apiKey, "sasl_stage", cc.saslStage)
				return
			}
		}

		// Assign sequence number in arrival order BEFORE dispatching
		seq := nextSeq
		nextSeq++

		// Produce requests (API key 0) are handled inline in the
		// read loop to preserve arrival order. Kafka does the same:
		// it mutes the socket after reading a request and only
		// unmutes after the response is sent, ensuring at most one
		// in-flight request per connection. Handling produce inline
		// is critical for idempotent writes where the sequence
		// window must see batches in wire order.
		//
		// All other requests are dispatched to goroutines so that
		// long-running operations (e.g. fetch long-polling) don't
		// block the connection.
		if apiKey == 0 {
			func() {
				defer func() {
					if r := recover(); r != nil {
						cc.logger.Error("handler panic", "remote", who, "api_key", apiKey, "panic", r)
						cc.sendResp(clientResp{seq: seq, err: fmt.Errorf("handler panic: %v", r)})
					}
				}()
				resp, err := cc.dispatchReq(kreq)
				if resp == nil && err == nil {
					cc.sendResp(clientResp{seq: seq, skip: true})
					return
				}
				cc.sendResp(clientResp{kresp: resp, corr: corrID, seq: seq, err: err})
			}()
			continue
		}

		// Acquire in-flight semaphore (backpressure on misbehaving clients)
		select {
		case cc.inflightSem <- struct{}{}:
		case <-cc.shutdownCh:
			return
		}

		// Spawn handler goroutine for non-produce requests
		cc.wg.Add(1)
		go func(corrID int32, kreq kmsg.Request, seq uint32) {
			defer cc.wg.Done()
			defer func() { <-cc.inflightSem }()

			// Recover from handler panics
			defer func() {
				if r := recover(); r != nil {
					cc.logger.Error("handler panic", "remote", who, "api_key", kreq.Key(), "panic", r)
					cc.sendResp(clientResp{seq: seq, err: fmt.Errorf("handler panic: %v", r)})
				}
			}()

			resp, err := cc.dispatchReq(kreq)

			// nil response + nil error = no response expected (acks=0 produce)
			if resp == nil && err == nil {
				cc.sendResp(clientResp{seq: seq, skip: true})
				return
			}
			cc.sendResp(clientResp{kresp: resp, corr: corrID, seq: seq, err: err})
		}(corrID, kreq, seq)
	}
}

// saslAllowed checks if the given request is allowed in the current SASL stage.
func (cc *clientConn) saslAllowed(kreq kmsg.Request) bool {
	switch cc.saslStage {
	case SASLStageBegin:
		switch kreq.(type) {
		case *kmsg.ApiVersionsRequest, *kmsg.SASLHandshakeRequest:
			return true
		default:
			return false
		}
	case SASLStageAuthPlain, SASLStageAuthScram256,
		SASLStageAuthScram512, SASLStageAuthScram1:
		switch kreq.(type) {
		case *kmsg.ApiVersionsRequest, *kmsg.SASLAuthenticateRequest:
			return true
		default:
			return false
		}
	case SASLStageComplete:
		return true
	default:
		return false
	}
}

// sendResp sends a response to the write goroutine, respecting shutdown.
func (cc *clientConn) sendResp(resp clientResp) {
	select {
	case cc.respCh <- resp:
	case <-cc.shutdownCh:
	}
}

// writeLoop receives responses from handler goroutines, reorders them by
// sequence number, and writes them to the connection in request order.
func (cc *clientConn) writeLoop() {
	defer cc.conn.Close()

	var (
		nextSeq uint32
		oooresp = make(map[uint32]clientResp) // out-of-order buffer
	)

	for {
		// Check if we already have the next response buffered
		resp, ok := oooresp[nextSeq]
		if ok {
			delete(oooresp, nextSeq)
			nextSeq++
		} else {
			// Wait for a response from the handler
			var resp2 clientResp
			var open bool
			select {
			case resp2, open = <-cc.respCh:
				if !open {
					return // readLoop finished and all handlers done
				}
			case <-cc.shutdownCh:
				return
			}

			if resp2.seq != nextSeq {
				// Out of order: stash and continue waiting
				oooresp[resp2.seq] = resp2
				continue
			}
			resp = resp2
			nextSeq++
		}

		// Check for errors
		if resp.err != nil {
			cc.logger.Debug("handler error, closing connection",
				"remote", cc.conn.RemoteAddr(), "error", resp.err)
			return
		}

		// Skip marker (acks=0): advance sequence, write nothing
		if resp.skip {
			continue
		}

		// Encode and write the response
		if err := cc.writeResponse(resp); err != nil {
			cc.logger.Debug("write error", "remote", cc.conn.RemoteAddr(), "error", err)
			return
		}
	}
}

// writeResponse encodes a Kafka response and writes it to the connection.
func (cc *clientConn) writeResponse(resp clientResp) error {
	buf := cc.writeBuf[:0]

	// Size placeholder (4 bytes)
	buf = append(buf, 0, 0, 0, 0)
	// Correlation ID (4 bytes)
	buf = binary.BigEndian.AppendUint32(buf, uint32(resp.corr))
	// Flexible version response header tag byte (except ApiVersions key 18)
	if resp.kresp.IsFlexible() && resp.kresp.Key() != 18 {
		buf = append(buf, 0) // empty tagged fields
	}
	// Response body
	buf = resp.kresp.AppendTo(buf)

	// Fill in size
	binary.BigEndian.PutUint32(buf[:4], uint32(len(buf)-4))

	if _, err := cc.bw.Write(buf); err != nil {
		return err
	}
	cc.writeBuf = buf // keep grown buffer for next response
	return cc.bw.Flush()
}
