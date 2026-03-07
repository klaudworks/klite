package broker

import (
	"context"
	"net"
	"net/http"
	"time"
)

func (b *Broker) startHealthServer() (shutdown func(), err error) {
	mux := http.NewServeMux()
	mux.HandleFunc("/livez", b.handleLivez)
	mux.HandleFunc("/readyz", b.handleReadyz)

	srv := &http.Server{
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}

	ln := b.cfg.HealthListener
	if ln == nil {
		ln, err = net.Listen("tcp", b.cfg.HealthAddr)
		if err != nil {
			return nil, err
		}
	}

	go func() { _ = srv.Serve(ln) }()

	b.logger.Info("health server started", "addr", ln.Addr().String())

	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}, nil
}

func (b *Broker) handleLivez(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (b *Broker) handleReadyz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	select {
	case <-b.ready:
		// In replication mode, standby returns 503.
		if b.isStandby() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("standby"))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	default:
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("not ready"))
	}
}
