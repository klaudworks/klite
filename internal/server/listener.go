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

type Server struct {
	handlers   *HandlerRegistry
	shutdownCh <-chan struct{}
	logger     *slog.Logger

	maxConns    int64
	connCount   atomic.Int64
	saslEnabled bool // when true, new connections start at SASLStageBegin

	// wg tracks active connection goroutines for clean shutdown.
	wg sync.WaitGroup
}

func NewServer(handlers *HandlerRegistry, shutdownCh <-chan struct{}, logger *slog.Logger) *Server {
	return &Server{
		handlers:   handlers,
		shutdownCh: shutdownCh,
		logger:     logger,
		maxConns:   defaultMaxConnections,
	}
}

func (s *Server) SetSASLEnabled(enabled bool) {
	s.saslEnabled = enabled
}

func (s *Server) Serve(ln net.Listener) error {
	for {
		conn, err := ln.Accept()
		if err != nil {
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

		if s.connCount.Load() >= s.maxConns {
			s.logger.Warn("connection limit reached, rejecting",
				"remote", conn.RemoteAddr(), "limit", s.maxConns)
			_ = conn.Close()
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

func (s *Server) Wait() {
	s.wg.Wait()
}

func (s *Server) ConnCount() int64 {
	return s.connCount.Load()
}
