package broker

import (
	"context"
	"encoding/base64"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/klaudworks/klite/internal/cluster"
	"github.com/klaudworks/klite/internal/handler"
	"github.com/klaudworks/klite/internal/server"
)

// Broker is the top-level klite broker.
type Broker struct {
	cfg        Config
	listener   net.Listener
	clusterID  string
	state      *cluster.State
	shutdownCh chan struct{} // closed on shutdown to wake all blockers
	done       chan struct{} // closed when Run() returns
	ready      chan struct{} // closed when initialization is complete
	logger     *slog.Logger
	server     *server.Server
	handlers   *server.HandlerRegistry

	mu   sync.Mutex
	quit bool
}

// New creates a new Broker with the given config.
func New(cfg Config) *Broker {
	logger := setupLogger(cfg.LogLevel)
	handlers := server.NewHandlerRegistry()
	shutdownCh := make(chan struct{})
	srv := server.NewServer(handlers, shutdownCh, logger)

	state := cluster.NewState(cluster.Config{
		NodeID:            cfg.NodeID,
		DefaultPartitions: cfg.DefaultPartitions,
		AutoCreateTopics:  cfg.AutoCreateTopics,
	})

	b := &Broker{
		cfg:        cfg,
		state:      state,
		shutdownCh: shutdownCh,
		done:       make(chan struct{}),
		ready:      make(chan struct{}),
		logger:     logger,
		server:     srv,
		handlers:   handlers,
	}
	b.registerBaseHandlers()
	return b
}

// registerBaseHandlers wires up handlers that don't depend on runtime state.
func (b *Broker) registerBaseHandlers() {
	b.handlers.Register(18, handler.HandleApiVersions())
}

// registerRuntimeHandlers wires up handlers that depend on runtime state
// (cluster ID, advertised address, etc.). Must be called after initialization.
func (b *Broker) registerRuntimeHandlers(advAddr string) {
	b.handlers.Register(0, handler.HandleProduce(b.state))
	b.handlers.Register(1, handler.HandleFetch(b.state, b.shutdownCh))
	b.handlers.Register(2, handler.HandleListOffsets(b.state))
	b.handlers.Register(3, handler.HandleMetadata(handler.MetadataConfig{
		NodeID:         b.cfg.NodeID,
		AdvertisedAddr: advAddr,
		ClusterID:      b.clusterID,
		State:          b.state,
	}))
	b.handlers.Register(19, handler.HandleCreateTopics(b.state))
	b.handlers.Register(10, handler.HandleFindCoordinator(handler.FindCoordinatorConfig{
		NodeID:         b.cfg.NodeID,
		AdvertisedAddr: advAddr,
	}))
}

// Run starts the broker and blocks until ctx is cancelled.
func (b *Broker) Run(ctx context.Context) error {
	defer close(b.done)

	// 1. Create/open data directory
	if err := os.MkdirAll(b.cfg.DataDir, 0o755); err != nil {
		return fmt.Errorf("create data dir: %w", err)
	}

	// 2. Load or generate cluster ID
	cid, err := b.loadOrCreateClusterID()
	if err != nil {
		return fmt.Errorf("cluster ID: %w", err)
	}
	b.clusterID = cid

	// 3. Start TCP listener
	ln := b.cfg.Listener
	if ln == nil {
		ln, err = net.Listen("tcp", b.cfg.Listen)
		if err != nil {
			return fmt.Errorf("listen: %w", err)
		}
	}
	b.listener = ln

	// 4. Resolve advertised address (after listener is bound so we know the real port)
	advAddr, warn := b.resolveAdvertisedAddr()
	if warn {
		b.logger.Warn("--advertised-addr not set, using derived address in Metadata responses; clients outside this host won't be able to connect",
			"advertised_addr", advAddr)
	}

	// 5. Register handlers that depend on runtime state
	b.registerRuntimeHandlers(advAddr)

	// 6. Log startup
	b.logger.Info("klite started",
		"listen", b.listener.Addr().String(),
		"advertised_addr", advAddr,
		"cluster_id", b.clusterID,
		"node_id", b.cfg.NodeID,
		"data_dir", b.cfg.DataDir,
	)

	// Signal that initialization is complete
	close(b.ready)

	// 7. Serve connections (blocks until listener closed)
	errCh := make(chan error, 1)
	go func() {
		errCh <- b.server.Serve(b.listener)
	}()

	// Wait for context cancellation or serve error
	var serveReturned bool
	select {
	case <-ctx.Done():
	case err := <-errCh:
		serveReturned = true
		if err != nil {
			return err
		}
	}

	// 8. Shutdown sequence
	b.shutdown()

	// 9. Wait for Serve() to return so no new connections are accepted
	// (no more wg.Add calls after this point).
	if !serveReturned {
		<-errCh
	}

	// 10. Wait for all connections to drain
	b.server.Wait()

	b.logger.Info("klite stopped")
	return nil
}

// Wait blocks until Run() returns.
func (b *Broker) Wait() {
	<-b.done
}

// ShutdownCh returns a channel that is closed when the broker is shutting down.
func (b *Broker) ShutdownCh() <-chan struct{} {
	return b.shutdownCh
}

// ClusterID returns the broker's cluster ID. Blocks until initialization is complete.
func (b *Broker) ClusterID() string {
	<-b.ready
	return b.clusterID
}

// Addr returns the listener address, or empty string if not listening.
// Blocks until initialization is complete.
func (b *Broker) Addr() string {
	<-b.ready
	if b.listener != nil {
		return b.listener.Addr().String()
	}
	return ""
}

// Ready returns a channel that is closed when the broker has finished initialization.
func (b *Broker) Ready() <-chan struct{} {
	return b.ready
}

// Handlers returns the handler registry for registering API handlers.
func (b *Broker) Handlers() *server.HandlerRegistry {
	return b.handlers
}

// resolveAdvertisedAddr determines the advertised address from config,
// falling back to the actual listener address if not explicitly configured.
func (b *Broker) resolveAdvertisedAddr() (addr string, warn bool) {
	if b.cfg.AdvertisedAddr != "" {
		return b.cfg.AdvertisedAddr, false
	}
	// Use the actual listener address (important for tests with random ports)
	if b.listener != nil {
		laddr := b.listener.Addr().String()
		host, port, err := net.SplitHostPort(laddr)
		if err == nil {
			if host == "" || host == "0.0.0.0" || host == "::" {
				return net.JoinHostPort("localhost", port), true
			}
			return laddr, false
		}
	}
	// Fallback to config
	return b.cfg.ResolveAdvertisedAddr()
}

func (b *Broker) shutdown() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.quit {
		return
	}
	b.quit = true

	// 1. Close shutdownCh to wake all blockers
	close(b.shutdownCh)

	// 2. Close listener to stop accepting
	if b.listener != nil {
		b.listener.Close()
	}
}

// loadOrCreateClusterID loads the cluster ID from meta.properties or generates
// a new one. If ClusterID is set in config, that value is used and persisted.
func (b *Broker) loadOrCreateClusterID() (string, error) {
	metaPath := filepath.Join(b.cfg.DataDir, "meta.properties")

	// Try to read existing
	data, err := os.ReadFile(metaPath)
	if err == nil {
		cid := parseClusterID(string(data))
		if cid != "" {
			if b.cfg.ClusterID != "" && b.cfg.ClusterID != cid {
				return "", fmt.Errorf("configured cluster ID %q does not match existing %q in %s",
					b.cfg.ClusterID, cid, metaPath)
			}
			b.logger.Info("loaded cluster ID from meta.properties", "cluster_id", cid)
			return cid, nil
		}
	}

	// Generate or use configured
	cid := b.cfg.ClusterID
	if cid == "" {
		cid = generateClusterID()
	}

	// Persist
	content := fmt.Sprintf("cluster.id=%s\n", cid)
	if err := os.WriteFile(metaPath, []byte(content), 0o644); err != nil {
		return "", fmt.Errorf("write meta.properties: %w", err)
	}
	b.logger.Info("created meta.properties", "cluster_id", cid, "path", metaPath)
	return cid, nil
}

// parseClusterID extracts the cluster.id value from meta.properties content.
func parseClusterID(content string) string {
	for _, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "cluster.id=") {
			return strings.TrimPrefix(line, "cluster.id=")
		}
	}
	return ""
}

// generateClusterID generates a Kafka-compatible cluster ID (base64-encoded UUID, 22 chars).
func generateClusterID() string {
	id := uuid.New()
	return base64.RawURLEncoding.EncodeToString(id[:])
}

// setupLogger creates an slog.Logger with the given level.
func setupLogger(level string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	h := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: lvl})
	return slog.New(h)
}
