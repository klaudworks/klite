package server

import (
	"errors"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
)

const (
	defaultMaxConnections = 10000
)

// Server manages TCP connections and dispatches Kafka requests.
type Server struct {
	handlers   *HandlerRegistry
	shutdownCh <-chan struct{}
	logger     *slog.Logger

	maxConns  int64
	connCount atomic.Int64

	// wg tracks active connection goroutines for clean shutdown.
	wg sync.WaitGroup
}

// NewServer creates a new Server.
func NewServer(handlers *HandlerRegistry, shutdownCh <-chan struct{}, logger *slog.Logger) *Server {
	return &Server{
		handlers:   handlers,
		shutdownCh: shutdownCh,
		logger:     logger,
		maxConns:   defaultMaxConnections,
	}
}

// Serve accepts connections on ln and handles them until the listener is closed.
// It returns nil on clean shutdown.
func (s *Server) Serve(ln net.Listener) error {
	for {
		conn, err := ln.Accept()
		if err != nil {
			// Check if we're shutting down
			select {
			case <-s.shutdownCh:
				return nil
			default:
			}
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			s.logger.Error("accept error", "error", err)
			continue
		}

		// Check connection limit
		if s.connCount.Load() >= s.maxConns {
			s.logger.Warn("connection limit reached, rejecting",
				"remote", conn.RemoteAddr(), "limit", s.maxConns)
			conn.Close()
			continue
		}

		s.connCount.Add(1)
		s.logger.Debug("new connection", "remote", conn.RemoteAddr())

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer s.connCount.Add(-1)
			defer s.logger.Debug("connection closed", "remote", conn.RemoteAddr())

			cc := newClientConn(s, conn)
			cc.serve()
		}()
	}
}

// Wait blocks until all active connections have been closed.
func (s *Server) Wait() {
	s.wg.Wait()
}

// ConnCount returns the current number of active connections.
func (s *Server) ConnCount() int64 {
	return s.connCount.Load()
}
