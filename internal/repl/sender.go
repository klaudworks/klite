package repl

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"
)

var errDisconnected = errors.New("repl: standby disconnected")

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

	closeOnce sync.Once
	done      chan struct{} // closed when ackReader exits
}

// NewSender creates a Sender for the given connection. It starts the ACK
// reader goroutine. The caller must call Close() when done.
func NewSender(conn net.Conn, ackTimeout time.Duration, logger *slog.Logger) *Sender {
	if logger == nil {
		logger = slog.Default()
	}
	if ackTimeout == 0 {
		ackTimeout = 5 * time.Second
	}
	s := &Sender{
		conn:       conn,
		pending:    make(map[uint64]chan<- error),
		logger:     logger,
		ackTimeout: ackTimeout,
		connected:  true,
		done:       make(chan struct{}),
	}
	go s.ackReader()
	return s
}

// Send transmits a WAL batch to the standby. Returns a channel that receives
// nil when the standby ACKs, or an error on timeout/disconnect.
func (s *Sender) Send(batch []byte, firstSeq, lastSeq uint64) <-chan error {
	ch := make(chan error, 1)

	payload := MarshalWALBatch(firstSeq, lastSeq, 0, batch)

	s.writeMu.Lock()
	if s.conn != nil {
		_ = s.conn.SetWriteDeadline(time.Now().Add(s.ackTimeout))
	}
	writeErr := WriteFrame(s.conn, MsgWALBatch, payload)
	s.writeMu.Unlock()

	if writeErr != nil {
		s.logger.Warn("repl sender: WAL_BATCH write failed", "err", writeErr)
		ch <- fmt.Errorf("repl: send WAL_BATCH: %w", writeErr)
		return ch
	}

	s.mu.Lock()
	if s.pending == nil {
		s.mu.Unlock()
		ch <- errDisconnected
		return ch
	}
	s.pending[lastSeq] = ch
	s.mu.Unlock()

	// Start ACK timeout timer
	time.AfterFunc(s.ackTimeout, func() {
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
		_ = s.conn.SetWriteDeadline(time.Now().Add(s.ackTimeout))
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
		_ = s.conn.SetWriteDeadline(time.Now().Add(s.ackTimeout))
	}
	err := WriteFrame(s.conn, MsgSnapshot, payload)
	s.writeMu.Unlock()

	return err
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

	for _, ch := range s.pending {
		ch <- errDisconnected
	}
	s.pending = nil
	s.connected = false
}
