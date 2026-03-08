package repl

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/klaudworks/klite/internal/clock"
)

var errDisconnected = errors.New("repl: standby disconnected")

// countEntries counts length-prefixed WAL entries in a batch.
func countEntries(batch []byte) uint32 {
	var n uint32
	offset := 0
	for offset+4 <= len(batch) {
		entryLen := int(batch[offset])<<24 | int(batch[offset+1])<<16 |
			int(batch[offset+2])<<8 | int(batch[offset+3])
		offset += 4 + entryLen
		n++
	}
	return n
}

// Sender streams WAL batches and metadata entries to a standby over a TCP
// connection. It is used by the primary side.
type Sender struct {
	writeMu    sync.Mutex // serializes all conn.Write calls
	mu         sync.Mutex // protects pending, conn, connected
	conn       net.Conn
	pending    map[uint64]chan<- error // lastSeq → waiting handler
	logger     *slog.Logger
	ackTimeout time.Duration
	connected  bool
	clk        clock.Clock

	// S3FlushWatermarkFn returns the current S3 flush watermark. If nil,
	// the watermark is always 0. Set by the caller who owns the WAL writer.
	S3FlushWatermarkFn func() uint64

	closeOnce sync.Once
	done      chan struct{} // closed when ackReader exits
}

// NewSender creates a Sender for the given connection. It starts the ACK
// reader goroutine. The caller must call Close() when done.
func NewSender(conn net.Conn, ackTimeout time.Duration, logger *slog.Logger, clk clock.Clock) *Sender {
	if logger == nil {
		logger = slog.Default()
	}
	if ackTimeout == 0 {
		ackTimeout = 5 * time.Second
	}
	if clk == nil {
		clk = clock.RealClock{}
	}
	s := &Sender{
		conn:       conn,
		pending:    make(map[uint64]chan<- error),
		logger:     logger,
		ackTimeout: ackTimeout,
		connected:  true,
		clk:        clk,
		done:       make(chan struct{}),
	}
	go s.ackReader()
	return s
}

// Send transmits a WAL batch to the standby. Returns a channel that receives
// nil when the standby ACKs, or an error on timeout/disconnect.
func (s *Sender) Send(batch []byte, firstSeq, lastSeq uint64) <-chan error {
	ch := make(chan error, 1)

	var watermark uint64
	if s.S3FlushWatermarkFn != nil {
		watermark = s.S3FlushWatermarkFn()
	}
	payload := MarshalWALBatch(firstSeq, lastSeq, countEntries(batch), watermark, batch)

	// Register the pending ACK entry BEFORE writing to the wire.
	// If the write succeeds, the ackReader may receive the ACK
	// immediately (especially on net.Pipe); registering after the
	// write would race and lose the ACK.
	s.mu.Lock()
	if s.pending == nil {
		s.mu.Unlock()
		ch <- errDisconnected
		return ch
	}
	s.pending[lastSeq] = ch
	s.mu.Unlock()

	s.writeMu.Lock()
	if s.conn != nil {
		_ = s.conn.SetWriteDeadline(s.clk.Now().Add(s.ackTimeout))
	}
	writeErr := WriteFrame(s.conn, MsgWALBatch, payload)
	s.writeMu.Unlock()

	if writeErr != nil {
		s.logger.Warn("repl sender: WAL_BATCH write failed", "err", writeErr)
		// Clean up the pending entry we just registered.
		s.mu.Lock()
		if s.pending != nil {
			delete(s.pending, lastSeq)
		}
		s.mu.Unlock()
		ch <- fmt.Errorf("repl: send WAL_BATCH: %w", writeErr)
		return ch
	}

	// Start ACK timeout timer
	s.clk.AfterFunc(s.ackTimeout, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.pending == nil {
			return
		}
		if pendCh, ok := s.pending[lastSeq]; ok {
			delete(s.pending, lastSeq)
			pendCh <- fmt.Errorf("repl: ACK timeout after %s for seq %d", s.ackTimeout, lastSeq)
		}
	})

	return ch
}

// SendMeta transmits a metadata.log entry. Fire-and-forget, no ACK expected.
// Must not block. If the write fails, it logs a warning and returns.
func (s *Sender) SendMeta(entry []byte) {
	s.writeMu.Lock()
	if s.conn != nil {
		_ = s.conn.SetWriteDeadline(s.clk.Now().Add(s.ackTimeout))
	}
	err := WriteFrame(s.conn, MsgMetaEntry, entry)
	s.writeMu.Unlock()

	if err != nil {
		s.logger.Warn("repl sender: META_ENTRY write failed", "err", err)
	}
}

// SendSnapshot transmits a SNAPSHOT message (metadata.log contents).
func (s *Sender) SendSnapshot(walSeqAfter uint64, metadataLog []byte) error {
	payload := MarshalSnapshot(walSeqAfter, metadataLog)

	s.writeMu.Lock()
	if s.conn != nil {
		_ = s.conn.SetWriteDeadline(s.clk.Now().Add(s.ackTimeout))
	}
	err := WriteFrame(s.conn, MsgSnapshot, payload)
	s.writeMu.Unlock()

	return err
}

// SendKeepalive writes an empty WAL_BATCH frame (keepalive) to the standby.
// Unlike Send, it does not register a pending entry or start a timeout timer.
func (s *Sender) SendKeepalive() {
	var watermark uint64
	if s.S3FlushWatermarkFn != nil {
		watermark = s.S3FlushWatermarkFn()
	}
	payload := MarshalWALBatch(0, 0, 0, watermark, nil)

	s.writeMu.Lock()
	if s.conn != nil {
		_ = s.conn.SetWriteDeadline(s.clk.Now().Add(s.ackTimeout))
	}
	err := WriteFrame(s.conn, MsgWALBatch, payload)
	s.writeMu.Unlock()

	if err != nil {
		s.logger.Warn("repl sender: keepalive write failed", "err", err)
	}
}

// Replicate implements wal.Replicator. It is an alias for Send.
func (s *Sender) Replicate(batch []byte, firstSeq, lastSeq uint64) <-chan error {
	return s.Send(batch, firstSeq, lastSeq)
}

// Connected returns true if the standby is currently connected.
func (s *Sender) Connected() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.connected
}

// Close closes the sender and drains all pending channels.
func (s *Sender) Close() {
	s.closeOnce.Do(func() {
		if s.conn != nil {
			_ = s.conn.Close()
		}
	})
	<-s.done // wait for ackReader to exit
}

// ackReader reads ACK messages from the standby and resolves pending channels.
func (s *Sender) ackReader() {
	defer func() {
		s.drainPending()
		close(s.done)
	}()

	for {
		msgType, payload, err := ReadFrame(s.conn)
		if err != nil {
			s.logger.Info("repl sender: ACK reader stopped", "err", err)
			return
		}

		if msgType != MsgACK {
			s.logger.Warn("repl sender: unexpected message type from standby", "type", msgType)
			continue
		}

		ackSeq, err := UnmarshalACK(payload)
		if err != nil {
			s.logger.Warn("repl sender: invalid ACK payload", "err", err)
			continue
		}

		s.resolveACK(ackSeq)
	}
}

// resolveACK resolves ALL pending entries with lastSeq <= ackSeq.
func (s *Sender) resolveACK(ackSeq uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.pending == nil {
		return
	}

	for seq, ch := range s.pending {
		if seq <= ackSeq {
			ch <- nil
			delete(s.pending, seq)
		}
	}
}

// drainPending sends errDisconnected to all pending channels and clears the map.
func (s *Sender) drainPending() {
	s.mu.Lock()
	defer s.mu.Unlock()

	n := len(s.pending)
	for _, ch := range s.pending {
		ch <- errDisconnected
	}
	s.pending = nil
	s.connected = false
	s.logger.Info("repl sender: drainPending complete, connected=false",
		"drained_entries", n)
}
