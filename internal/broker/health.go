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
	mux.HandleFunc("/primaryz", b.handlePrimaryz)

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
		w.WriteHeader(http.StatusOK)
		if b.isStandby() {
			_, _ = w.Write([]byte("standby"))
		} else {
			_, _ = w.Write([]byte("ok"))
		}
	default:
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("not ready"))
	}
}

// handlePrimaryz returns 200 only when this broker is the active primary.
// Load balancers on any platform (Traefik, ECS, HAProxy) can use this
// endpoint to route Kafka traffic to the primary without platform-specific
// hooks like Kubernetes pod labels.
func (b *Broker) handlePrimaryz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	if b.isStandby() {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("standby"))
		return
	}
	select {
	case <-b.ready:
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("primary"))
	default:
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("not ready"))
	}
}
