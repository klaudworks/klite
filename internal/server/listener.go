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

	// connsMu protects activeConns.
	connsMu     sync.Mutex
	activeConns map[net.Conn]struct{}
}

func NewServer(handlers *HandlerRegistry, shutdownCh <-chan struct{}, logger *slog.Logger) *Server {
	return &Server{
		handlers:    handlers,
		shutdownCh:  shutdownCh,
		logger:      logger,
		maxConns:    defaultMaxConnections,
		activeConns: make(map[net.Conn]struct{}),
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

		s.connsMu.Lock()
		s.activeConns[conn] = struct{}{}
		s.connsMu.Unlock()

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer s.connCount.Add(-1)
			defer func() {
				s.connsMu.Lock()
				delete(s.activeConns, conn)
				s.connsMu.Unlock()
			}()
			defer s.logger.Debug("connection closed", "remote", conn.RemoteAddr())

			cc := newClientConn(s, conn)
			cc.serve()
		}()
	}
}

func (s *Server) Wait() {
	s.wg.Wait()
}

// CloseConns forcefully closes all active client connections. Used during
// demotion to unblock server.Wait() without waiting for clients to
// disconnect on their own.
func (s *Server) CloseConns() {
	s.connsMu.Lock()
	conns := make([]net.Conn, 0, len(s.activeConns))
	for c := range s.activeConns {
		conns = append(conns, c)
	}
	s.connsMu.Unlock()

	for _, c := range conns {
		_ = c.Close()
	}
}

func (s *Server) ConnCount() int64 {
	return s.connCount.Load()
}
