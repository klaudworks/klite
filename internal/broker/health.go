package broker

import (
	"context"
	"net"
	"net/http"
	"time"
)

// startHealthServer starts the HTTP health server.
// Returns a cleanup function that shuts down the server gracefully.
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

	go srv.Serve(ln)

	b.logger.Info("health server started", "addr", ln.Addr().String())

	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		srv.Shutdown(ctx)
	}, nil
}

func (b *Broker) handleLivez(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

func (b *Broker) handleReadyz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	select {
	case <-b.ready:
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	default:
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte("not ready"))
	}
}
