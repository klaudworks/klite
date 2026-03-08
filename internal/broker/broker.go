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
	"sync/atomic"

	"github.com/google/uuid"
	"github.com/klaudworks/klite/internal/handler"
	"github.com/klaudworks/klite/internal/sasl"
	"github.com/klaudworks/klite/internal/server"

	crypto_tls "crypto/tls"

	"github.com/klaudworks/klite/internal/chunk"
	"github.com/klaudworks/klite/internal/clock"
	"github.com/klaudworks/klite/internal/cluster"
	"github.com/klaudworks/klite/internal/lease"
	"github.com/klaudworks/klite/internal/metadata"
	"github.com/klaudworks/klite/internal/repl"
	s3store "github.com/klaudworks/klite/internal/s3"
	"github.com/klaudworks/klite/internal/wal"
)

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
	walWriter  *wal.Writer
	walIndex   *wal.Index
	chunkPool  *chunk.Pool
	metaLog    *metadata.Log
	s3Client   *s3store.Client
	s3Reader   *s3store.Reader
	s3Flusher  *s3store.Flusher
	saslStore  *sasl.Store

	mu   sync.Mutex
	quit bool

	kafkaListenAddr string // actual Kafka listen address (set on first promotion)

	// Replication state
	elector       lease.Elector
	replTLSConfig *crypto_tls.Config
	replListener  net.Listener
	replSender    *repl.Sender
	replRole      atomic.Int32       // 0 = single-node/standby, 1 = primary
	replCancel    context.CancelFunc // protected by mu

	// testStepHook, when non-nil, is called at each major step boundary in
	// onElected and shutdownPrimary. Test-only; nil in production.
	testStepHook func(string)
}

func (b *Broker) stepHook(name string) {
	if b.testStepHook != nil {
		b.testStepHook(name)
	}
}

func New(cfg Config) *Broker {
	if cfg.Clock == nil {
		cfg.Clock = clock.RealClock{}
	}
	logger := setupLogger(cfg.LogLevel)
	slog.SetDefault(logger)
	handlers := server.NewHandlerRegistry()
	shutdownCh := make(chan struct{})
	srv := server.NewServer(handlers, shutdownCh, logger)

	state := cluster.NewState(cluster.Config{
		NodeID:            cfg.NodeID,
		DefaultPartitions: cfg.DefaultPartitions,
		AutoCreateTopics:  cfg.AutoCreateTopics,
	})
	state.SetShutdownCh(shutdownCh)
	state.SetLogger(logger)
	state.SetClock(cfg.Clock)

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

	if cfg.SASLEnabled {
		b.initSASLStore()
		srv.SetSASLEnabled(true)
	}

	b.registerBaseHandlers()
	return b
}

func (b *Broker) initSASLStore() {
	if b.cfg.SASLStore != nil {
		b.saslStore = b.cfg.SASLStore
		return
	}
	b.saslStore = sasl.NewStore()

	if b.cfg.SASLUser != "" && b.cfg.SASLPassword != "" {
		switch b.cfg.SASLMechanism {
		case sasl.MechanismPlain:
			b.saslStore.AddPlain(b.cfg.SASLUser, b.cfg.SASLPassword)
		case sasl.MechanismScram256:
			auth := sasl.NewScramAuth(sasl.MechanismScram256, b.cfg.SASLPassword)
			b.saslStore.AddScram256(b.cfg.SASLUser, auth)
		case sasl.MechanismScram512:
			auth := sasl.NewScramAuth(sasl.MechanismScram512, b.cfg.SASLPassword)
			b.saslStore.AddScram512(b.cfg.SASLUser, auth)
		default:
			b.logger.Warn("unknown SASL mechanism for CLI user, skipping", "mechanism", b.cfg.SASLMechanism)
		}
	}
}

func (b *Broker) registerBaseHandlers() {
	b.handlers.Register(handler.APIKeyApiVersions, handler.HandleApiVersions())

	if b.saslStore != nil {
		b.handlers.RegisterConn(handler.APIKeySASLHandshake, handler.HandleSASLHandshake())
		b.handlers.RegisterConn(handler.APIKeySASLAuthenticate, handler.HandleSASLAuthenticate(b.saslStore))
	}
}

func (b *Broker) registerRuntimeHandlers(advAddr string) {
	b.handlers.Register(handler.APIKeyProduce, handler.HandleProduce(b.state, b.walWriter, b.cfg.Clock))
	b.handlers.Register(handler.APIKeyFetch, handler.HandleFetch(b.state, b.shutdownCh, b.cfg.Clock))
	b.handlers.Register(handler.APIKeyListOffsets, handler.HandleListOffsets(b.state))
	b.handlers.Register(handler.APIKeyMetadata, handler.HandleMetadata(handler.MetadataConfig{
		NodeID:         b.cfg.NodeID,
		AdvertisedAddr: advAddr,
		ClusterID:      b.clusterID,
		State:          b.state,
	}))
	b.handlers.Register(handler.APIKeyCreateTopics, handler.HandleCreateTopics(b.state))
	b.handlers.Register(handler.APIKeyFindCoordinator, handler.HandleFindCoordinator(handler.FindCoordinatorConfig{
		NodeID:         b.cfg.NodeID,
		AdvertisedAddr: advAddr,
	}))
	b.handlers.Register(handler.APIKeyOffsetCommit, handler.HandleOffsetCommit(b.state))
	b.handlers.Register(handler.APIKeyOffsetFetch, handler.HandleOffsetFetch(b.state))
	b.handlers.Register(handler.APIKeyJoinGroup, handler.HandleJoinGroup(b.state))
	b.handlers.Register(handler.APIKeyHeartbeat, handler.HandleHeartbeat(b.state))
	b.handlers.Register(handler.APIKeyLeaveGroup, handler.HandleLeaveGroup(b.state))
	b.handlers.Register(handler.APIKeySyncGroup, handler.HandleSyncGroup(b.state))
	b.handlers.Register(handler.APIKeyOffsetDelete, handler.HandleOffsetDelete(b.state))

	b.handlers.Register(handler.APIKeyDescribeGroups, handler.HandleDescribeGroups(b.state))
	b.handlers.Register(handler.APIKeyListGroups, handler.HandleListGroups(b.state))
	b.handlers.Register(handler.APIKeyDeleteTopics, handler.HandleDeleteTopics(b.state))
	b.handlers.Register(handler.APIKeyDeleteRecords, handler.HandleDeleteRecords(b.state))
	b.handlers.Register(handler.APIKeyOffsetForLeaderEpoch, handler.HandleOffsetForLeaderEpoch(b.state))
	b.handlers.Register(handler.APIKeyDescribeConfigs, handler.HandleDescribeConfigs(handler.DescribeConfigsConfig{
		NodeID: b.cfg.NodeID,
		State:  b.state,
	}))
	b.handlers.Register(handler.APIKeyAlterConfigs, handler.HandleAlterConfigs(b.state))
	b.handlers.Register(handler.APIKeyDescribeLogDirs, handler.HandleDescribeLogDirs(b.state, b.cfg.DataDir))
	b.handlers.Register(handler.APIKeyCreatePartitions, handler.HandleCreatePartitions(b.state))
	b.handlers.Register(handler.APIKeyDeleteGroups, handler.HandleDeleteGroups(b.state))
	b.handlers.Register(handler.APIKeyIncrementalAlterConfigs, handler.HandleIncrementalAlterConfigs(b.state))
	b.handlers.Register(handler.APIKeyDescribeCluster, handler.HandleDescribeCluster(handler.DescribeClusterConfig{
		NodeID:         b.cfg.NodeID,
		AdvertisedAddr: advAddr,
		ClusterID:      b.clusterID,
	}))

	if b.saslStore != nil {
		b.handlers.Register(handler.APIKeyDescribeUserScramCreds, handler.HandleDescribeUserScramCredentials(b.saslStore))
		b.handlers.Register(handler.APIKeyAlterUserScramCreds, handler.HandleAlterUserScramCredentials(b.saslStore, b.metaLog))
	}

	b.handlers.Register(handler.APIKeyInitProducerID, handler.HandleInitProducerID(b.state))
	b.handlers.Register(handler.APIKeyAddPartitionsToTxn, handler.HandleAddPartitionsToTxn(b.state))
	b.handlers.Register(handler.APIKeyAddOffsetsToTxn, handler.HandleAddOffsetsToTxn(b.state))
	b.handlers.Register(handler.APIKeyEndTxn, handler.HandleEndTxn(b.state, b.walWriter, b.cfg.Clock))
	b.handlers.Register(handler.APIKeyTxnOffsetCommit, handler.HandleTxnOffsetCommit(b.state))
	b.handlers.Register(handler.APIKeyDescribeProducers, handler.HandleDescribeProducers(b.state))
	b.handlers.Register(handler.APIKeyDescribeTransactions, handler.HandleDescribeTransactions(b.state))
	b.handlers.Register(handler.APIKeyListTransactions, handler.HandleListTransactions(b.state))
}

// Run starts the broker and blocks until ctx is cancelled.
func (b *Broker) Run(ctx context.Context) error {
	defer close(b.done)

	// Validate replication config upfront
	if warnings, err := b.cfg.ValidateReplication(); err != nil {
		return err
	} else {
		for _, w := range warnings {
			b.logger.Warn(w)
		}
	}

	if err := os.MkdirAll(b.cfg.DataDir, 0o755); err != nil {
		return fmt.Errorf("create data dir: %w", err)
	}

	cid, err := b.loadOrCreateClusterID()
	if err != nil {
		return fmt.Errorf("cluster ID: %w", err)
	}
	b.clusterID = cid

	if b.cfg.S3Bucket != "" {
		if err := b.maybeRecoverFromS3(); err != nil {
			b.logger.Warn("S3 disaster recovery failed, continuing without", "err", err)
		}
	}

	if err := b.initMetadataLog(); err != nil {
		return fmt.Errorf("init metadata.log: %w", err)
	}
	// After metadata.log replay (which may have loaded persisted SCRAM
	// credentials), re-apply CLI-flag user so config always wins.
	if b.saslStore != nil && b.cfg.SASLUser != "" && b.cfg.SASLPassword != "" {
		b.applyCLISASLUser()
	}

	if b.cfg.SASLEnabled && b.saslStore != nil && b.saslStore.Empty() {
		return fmt.Errorf("SASL is enabled but no credentials are configured; add users via --sasl-user/--sasl-password or config file")
	}

	if err := b.initWAL(); err != nil {
		return fmt.Errorf("init WAL: %w", err)
	}

	b.abortOrphanedTransactions()

	if b.cfg.ReplicationAddr != "" {
		return b.runReplicationMode(ctx)
	}

	return b.runSingleNodeMode(ctx)
}

// runSingleNodeMode runs the broker in single-node mode (no replication).
func (b *Broker) runSingleNodeMode(ctx context.Context) error {
	if b.cfg.S3Bucket != "" {
		if err := b.initS3(); err != nil {
			return fmt.Errorf("init S3: %w", err)
		}
	} else {
		b.logger.Warn("no S3 bucket configured; data is stored only in the local WAL — this is suitable for demos but data loss will occur when the WAL exceeds --wal-max-disk-size and old segments are deleted, or if the disk is lost")
	}

	var err error
	ln := b.cfg.Listener
	if ln == nil {
		ln, err = net.Listen("tcp", b.cfg.Listen)
		if err != nil {
			return fmt.Errorf("listen: %w", err)
		}
	}
	b.listener = ln

	advAddr, warn := b.resolveAdvertisedAddr()
	if warn {
		b.logger.Warn("--advertised-addr not set, using derived address in Metadata responses; clients outside this host won't be able to connect",
			"advertised_addr", advAddr)
	}

	b.registerRuntimeHandlers(advAddr)

	b.logger.Info("klite started",
		"listen", b.listener.Addr().String(),
		"advertised_addr", advAddr,
		"cluster_id", b.clusterID,
		"node_id", b.cfg.NodeID,
		"data_dir", b.cfg.DataDir,
	)

	// In single-node mode, mark as primary for health checks
	b.replRole.Store(1)
	close(b.ready)

	var healthShutdown func()
	if b.cfg.HealthAddr != "" || b.cfg.HealthListener != nil {
		healthShutdown, err = b.startHealthServer()
		if err != nil {
			return fmt.Errorf("health server: %w", err)
		}
	}

	go b.retentionLoop(ctx)
	go b.txnTimeoutLoop(ctx)

	if b.s3Client != nil {
		go b.compactionLoop(ctx)
		go b.s3GCLoop(ctx)
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- b.server.Serve(b.listener)
	}()

	var serveReturned bool
	select {
	case <-ctx.Done():
	case err := <-errCh:
		serveReturned = true
		if err != nil {
			return err
		}
	}

	b.shutdown()
	if healthShutdown != nil {
		healthShutdown()
	}

	if !serveReturned {
		<-errCh
	}

	if b.chunkPool != nil {
		b.chunkPool.Close()
	}

	b.server.Wait()
	b.state.StopAllGroups()

	if b.s3Flusher != nil {
		b.s3Flusher.Stop()
	}

	if b.walWriter != nil {
		b.walWriter.Stop()
	}

	if b.metaLog != nil {
		_ = b.metaLog.Close()
	}

	b.logger.Info("klite stopped")
	return nil
}

func (b *Broker) Wait() {
	<-b.done
}

func (b *Broker) ShutdownCh() <-chan struct{} {
	return b.shutdownCh
}

func (b *Broker) ClusterID() string {
	<-b.ready
	return b.clusterID
}

func (b *Broker) Addr() string {
	<-b.ready
	if b.listener != nil {
		return b.listener.Addr().String()
	}
	return ""
}

func (b *Broker) Ready() <-chan struct{} {
	return b.ready
}

func (b *Broker) Handlers() *server.HandlerRegistry {
	return b.handlers
}

func (b *Broker) resolveAdvertisedAddr() (addr string, warn bool) {
	if b.cfg.AdvertisedAddr != "" {
		return b.cfg.AdvertisedAddr, false
	}
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
	return b.cfg.ResolveAdvertisedAddr()
}

func (b *Broker) shutdown() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.quit {
		return
	}
	b.quit = true

	close(b.shutdownCh)

	if b.listener != nil {
		_ = b.listener.Close()
	}
}

// loadOrCreateClusterID loads the cluster ID from meta.properties or generates
// a new one. If ClusterID is set in config, that value is used and persisted.
func (b *Broker) loadOrCreateClusterID() (string, error) {
	metaPath := filepath.Join(b.cfg.DataDir, "meta.properties")

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

	cid := b.cfg.ClusterID
	if cid == "" {
		cid = generateClusterID()
	}

	content := fmt.Sprintf("cluster.id=%s\n", cid)
	if err := os.WriteFile(metaPath, []byte(content), 0o644); err != nil {
		return "", fmt.Errorf("write meta.properties: %w", err)
	}
	b.logger.Info("created meta.properties", "cluster_id", cid, "path", metaPath)
	return cid, nil
}

func parseClusterID(content string) string {
	for _, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "cluster.id=") {
			return strings.TrimPrefix(line, "cluster.id=")
		}
	}
	return ""
}

func generateClusterID() string {
	id := uuid.New()
	return base64.RawURLEncoding.EncodeToString(id[:])
}

func (b *Broker) applyCLISASLUser() {
	switch b.cfg.SASLMechanism {
	case sasl.MechanismPlain:
		b.saslStore.AddPlain(b.cfg.SASLUser, b.cfg.SASLPassword)
	case sasl.MechanismScram256:
		auth := sasl.NewScramAuth(sasl.MechanismScram256, b.cfg.SASLPassword)
		b.saslStore.AddScram256(b.cfg.SASLUser, auth)
	case sasl.MechanismScram512:
		auth := sasl.NewScramAuth(sasl.MechanismScram512, b.cfg.SASLPassword)
		b.saslStore.AddScram512(b.cfg.SASLUser, auth)
	}
}

func scramMechName(mech int8) string {
	switch mech {
	case 1:
		return sasl.MechanismScram256
	case 2:
		return sasl.MechanismScram512
	default:
		return ""
	}
}

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
