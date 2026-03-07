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
	connReadBufSize  = 64 * 1024
	connWriteBufSize = 64 * 1024
	maxFrameSize     = 100 << 20
	maxInFlight      = 100
)

type clientResp struct {
	kresp kmsg.Response
	corr  int32
	seq   uint32
	skip  bool
	err   error
}

type connReader struct {
	br  *bufio.Reader
	buf []byte
}

func (cr *connReader) readFrame() ([]byte, error) {
	var sizeBuf [4]byte
	if _, err := io.ReadFull(cr.br, sizeBuf[:]); err != nil {
		return nil, err
	}
	size := int(binary.BigEndian.Uint32(sizeBuf[:]))

	if size < 0 || size > maxFrameSize {
		return nil, fmt.Errorf("invalid frame size: %d", size)
	}

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

type clientConn struct {
	server     *Server
	conn       net.Conn
	respCh     chan clientResp
	reader     connReader
	bw         *bufio.Writer
	writeBuf   []byte
	shutdownCh <-chan struct{}
	logger     *slog.Logger

	inflightSem chan struct{}
	wg          sync.WaitGroup

	saslStage SASLStage
	scramS0   *sasl.ScramServer0
	user      string
}

func newClientConn(s *Server, nc net.Conn) *clientConn {
	stage := SASLStageComplete
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

func (cc *clientConn) serve() {
	go cc.writeLoop()
	cc.readLoop()
	cc.wg.Wait()
	close(cc.respCh)
}

func (cc *clientConn) readLoop() {
	who := cc.conn.RemoteAddr()
	var nextSeq uint32

	for {
		frame, err := cc.reader.readFrame()
		if err != nil {
			select {
			case <-cc.shutdownCh:
			default:
				if err != io.EOF {
					cc.logger.Debug("read error", "remote", who, "error", err)
				}
			}
			return
		}

		if len(frame) < 10 {
			cc.logger.Debug("frame too short", "remote", who, "size", len(frame))
			return
		}

		reader := kbin.Reader{Src: frame}
		apiKey := reader.Int16()
		apiVersion := reader.Int16()
		corrID := reader.Int32()
		_ = reader.NullableString()

		kreq := kmsg.RequestForKey(apiKey)
		if kreq == nil {
			cc.logger.Debug("unknown API key, closing connection", "remote", who, "api_key", apiKey)
			return
		}
		kreq.SetVersion(apiVersion)

		if kreq.IsFlexible() {
			kmsg.SkipTags(&reader)
		}

		// Copy body so parsed request owns its memory (ReadFrom aliases input).
		bodyCopy := make([]byte, len(reader.Src))
		copy(bodyCopy, reader.Src)

		if err := kreq.ReadFrom(bodyCopy); err != nil {
			if apiKey == 18 {
				cc.logger.Debug("ApiVersions parse error (will still dispatch)",
					"remote", who, "version", apiVersion, "error", err)
			} else {
				cc.logger.Debug("parse error", "remote", who, "api_key", apiKey, "version", apiVersion, "error", err)
				return
			}
		}

		if cc.saslStage != SASLStageComplete {
			if !cc.saslAllowed(kreq) {
				cc.logger.Debug("SASL gate: request not allowed before auth, closing",
					"remote", who, "api_key", apiKey, "sasl_stage", cc.saslStage)
				return
			}
		}

		seq := nextSeq
		nextSeq++

		// Produce handled inline to preserve arrival order (required for idempotency).
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

		select {
		case cc.inflightSem <- struct{}{}:
		case <-cc.shutdownCh:
			return
		}

		cc.wg.Add(1)
		go func(corrID int32, kreq kmsg.Request, seq uint32) {
			defer cc.wg.Done()
			defer func() { <-cc.inflightSem }()

			defer func() {
				if r := recover(); r != nil {
					cc.logger.Error("handler panic", "remote", who, "api_key", kreq.Key(), "panic", r)
					cc.sendResp(clientResp{seq: seq, err: fmt.Errorf("handler panic: %v", r)})
				}
			}()

			resp, err := cc.dispatchReq(kreq)

			if resp == nil && err == nil {
				cc.sendResp(clientResp{seq: seq, skip: true})
				return
			}
			cc.sendResp(clientResp{kresp: resp, corr: corrID, seq: seq, err: err})
		}(corrID, kreq, seq)
	}
}

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

func (cc *clientConn) sendResp(resp clientResp) {
	select {
	case cc.respCh <- resp:
	case <-cc.shutdownCh:
	}
}

func (cc *clientConn) writeLoop() {
	defer cc.conn.Close() //nolint:errcheck // best-effort close

	var (
		nextSeq uint32
		oooresp = make(map[uint32]clientResp)
	)

	for {
		resp, ok := oooresp[nextSeq]
		if ok {
			delete(oooresp, nextSeq)
			nextSeq++
		} else {
			var resp2 clientResp
			var open bool
			select {
			case resp2, open = <-cc.respCh:
				if !open {
					return
				}
			case <-cc.shutdownCh:
				return
			}

			if resp2.seq != nextSeq {
				oooresp[resp2.seq] = resp2
				continue
			}
			resp = resp2
			nextSeq++
		}

		if resp.err != nil {
			cc.logger.Debug("handler error, closing connection",
				"remote", cc.conn.RemoteAddr(), "error", resp.err)
			return
		}

		if resp.skip {
			continue
		}

		if err := cc.writeResponse(resp); err != nil {
			cc.logger.Debug("write error", "remote", cc.conn.RemoteAddr(), "error", err)
			return
		}
	}
}

func (cc *clientConn) writeResponse(resp clientResp) error {
	buf := cc.writeBuf[:0]

	buf = append(buf, 0, 0, 0, 0)
	buf = binary.BigEndian.AppendUint32(buf, uint32(resp.corr))
	if resp.kresp.IsFlexible() && resp.kresp.Key() != 18 {
		buf = append(buf, 0)
	}
	buf = resp.kresp.AppendTo(buf)
	binary.BigEndian.PutUint32(buf[:4], uint32(len(buf)-4))

	if _, err := cc.bw.Write(buf); err != nil {
		return err
	}
	cc.writeBuf = buf
	return cc.bw.Flush()
}
