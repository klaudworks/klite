package broker

import (
	"context"
	crypto_tls "crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/klaudworks/klite/internal/chunk"
	"github.com/klaudworks/klite/internal/clock"
	"github.com/klaudworks/klite/internal/cluster"
	"github.com/klaudworks/klite/internal/handler"
	"github.com/klaudworks/klite/internal/lease"
	"github.com/klaudworks/klite/internal/lease/s3lease"
	"github.com/klaudworks/klite/internal/metadata"
	"github.com/klaudworks/klite/internal/repl"
	s3store "github.com/klaudworks/klite/internal/s3"
	"github.com/klaudworks/klite/internal/sasl"
	"github.com/klaudworks/klite/internal/server"
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
		store, ok := b.cfg.SASLStore.(*sasl.Store)
		if !ok {
			b.logger.Error("SASLStore has unexpected type, using empty store", "type", fmt.Sprintf("%T", b.cfg.SASLStore))
		} else {
			b.saslStore = store
			return
		}
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
	b.handlers.Register(18, handler.HandleApiVersions())

	if b.saslStore != nil {
		b.handlers.RegisterConn(17, handler.HandleSASLHandshake())
		b.handlers.RegisterConn(36, handler.HandleSASLAuthenticate(b.saslStore))
	}
}

func (b *Broker) registerRuntimeHandlers(advAddr string) {
	b.handlers.Register(0, handler.HandleProduce(b.state, b.walWriter, b.cfg.Clock))
	b.handlers.Register(1, handler.HandleFetch(b.state, b.shutdownCh, b.cfg.Clock))
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
	b.handlers.Register(8, handler.HandleOffsetCommit(b.state))
	b.handlers.Register(9, handler.HandleOffsetFetch(b.state))
	b.handlers.Register(11, handler.HandleJoinGroup(b.state))
	b.handlers.Register(12, handler.HandleHeartbeat(b.state))
	b.handlers.Register(13, handler.HandleLeaveGroup(b.state))
	b.handlers.Register(14, handler.HandleSyncGroup(b.state))
	b.handlers.Register(47, handler.HandleOffsetDelete(b.state))

	b.handlers.Register(15, handler.HandleDescribeGroups(b.state))
	b.handlers.Register(16, handler.HandleListGroups(b.state))
	b.handlers.Register(20, handler.HandleDeleteTopics(b.state))
	b.handlers.Register(21, handler.HandleDeleteRecords(b.state))
	b.handlers.Register(23, handler.HandleOffsetForLeaderEpoch(b.state))
	b.handlers.Register(32, handler.HandleDescribeConfigs(handler.DescribeConfigsConfig{
		NodeID: b.cfg.NodeID,
		State:  b.state,
	}))
	b.handlers.Register(33, handler.HandleAlterConfigs(b.state))
	b.handlers.Register(35, handler.HandleDescribeLogDirs(b.state, b.cfg.DataDir))
	b.handlers.Register(37, handler.HandleCreatePartitions(b.state))
	b.handlers.Register(42, handler.HandleDeleteGroups(b.state))
	b.handlers.Register(44, handler.HandleIncrementalAlterConfigs(b.state))
	b.handlers.Register(60, handler.HandleDescribeCluster(handler.DescribeClusterConfig{
		NodeID:         b.cfg.NodeID,
		AdvertisedAddr: advAddr,
		ClusterID:      b.clusterID,
	}))

	if b.saslStore != nil {
		b.handlers.Register(50, handler.HandleDescribeUserScramCredentials(b.saslStore))
		b.handlers.Register(51, handler.HandleAlterUserScramCredentials(b.saslStore, b.metaLog))
	}

	b.handlers.Register(22, handler.HandleInitProducerID(b.state))
	b.handlers.Register(24, handler.HandleAddPartitionsToTxn(b.state))
	b.handlers.Register(25, handler.HandleAddOffsetsToTxn(b.state))
	b.handlers.Register(26, handler.HandleEndTxn(b.state, b.walWriter, b.cfg.Clock))
	b.handlers.Register(28, handler.HandleTxnOffsetCommit(b.state))
	b.handlers.Register(61, handler.HandleDescribeProducers(b.state))
	b.handlers.Register(65, handler.HandleDescribeTransactions(b.state))
	b.handlers.Register(66, handler.HandleListTransactions(b.state))
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

// runReplicationMode runs the broker with replication enabled.
func (b *Broker) runReplicationMode(ctx context.Context) error {
	// Initialize S3 (required for replication)
	if b.cfg.S3Bucket != "" {
		if err := b.initS3(); err != nil {
			return fmt.Errorf("init S3: %w", err)
		}
	}

	// Ensure TLS certificates exist (requires S3 for cert storage)
	if b.s3Client != nil {
		prefix := "klite-" + b.clusterID
		if b.cfg.S3Prefix != "" {
			prefix = b.cfg.S3Prefix + "/" + prefix
		}

		tlsCfg, err := repl.EnsureTLS(ctx, repl.TLSConfig{
			Store:     &s3TLSStore{s3Client: b.s3Client},
			Prefix:    prefix,
			CacheDir:  filepath.Join(b.cfg.DataDir, "repl-tls"),
			ExtraSANs: b.cfg.ReplicationTLSSANs,
			Logger:    b.logger,
			Clock:     b.cfg.Clock,
		})
		if err != nil {
			return fmt.Errorf("ensure TLS: %w", err)
		}
		b.replTLSConfig = tlsCfg
	}

	// Create the lease elector
	if b.cfg.LeaseElector != nil {
		elector, ok := b.cfg.LeaseElector.(lease.Elector)
		if !ok {
			return fmt.Errorf("LeaseElector has unexpected type %T", b.cfg.LeaseElector)
		}
		b.elector = elector
	} else {
		elector, err := b.createS3Elector()
		if err != nil {
			return fmt.Errorf("create S3 lease elector: %w", err)
		}
		b.elector = elector
	}

	b.logger.Info("klite started (replication mode)",
		"replication_addr", b.cfg.ReplicationAddr,
		"cluster_id", b.clusterID,
		"node_id", b.cfg.NodeID,
		"data_dir", b.cfg.DataDir,
	)

	close(b.ready)

	var err error
	var healthShutdown func()
	if b.cfg.HealthAddr != "" || b.cfg.HealthListener != nil {
		healthShutdown, err = b.startHealthServer()
		if err != nil {
			return fmt.Errorf("health server: %w", err)
		}
	}

	// Create a child context for the elector
	electorCtx, electorCancel := context.WithCancel(ctx)
	defer electorCancel()

	// Primary-only context for background tasks
	var primaryCtx context.Context
	var primaryCancel context.CancelFunc

	// Start receiver eagerly so a born-standby connects to the primary as
	// soon as one is elected. onElected will call stopReceiver before
	// becoming primary. The retry loop inside startReceiverFromLease
	// handles the case where no primary exists yet.
	b.startReceiverFromLease()

	electorErrCh := make(chan error, 1)
	go func() {
		electorErrCh <- b.elector.Run(electorCtx, lease.Callbacks{
			OnElected: func(leaseCtx context.Context) {
				primaryCtx, primaryCancel = context.WithCancel(leaseCtx)
				b.onElected(ctx, primaryCtx)
			},
			OnDemoted: func() {
				if primaryCancel != nil {
					primaryCancel()
					primaryCancel = nil
				}
				b.onDemoted()
			},
		})
	}()

	select {
	case <-ctx.Done():
	case err := <-electorErrCh:
		if err != nil && err != context.Canceled {
			return err
		}
	}

	// Shutdown
	electorCancel()

	// If we're primary, do graceful shutdown
	if b.replRole.Load() == 1 {
		if b.cfg.RoleChangeHook != nil {
			b.cfg.RoleChangeHook.ClearPrimary()
		}
		b.shutdownPrimary()

		// Release lease for fast failover
		if releaseErr := b.elector.Release(); releaseErr != nil {
			b.logger.Warn("lease release failed, will expire naturally", "err", releaseErr)
		}
	} else {
		// Stop receiver if running
		b.stopReceiver()
	}

	if primaryCancel != nil {
		primaryCancel()
	}

	if healthShutdown != nil {
		healthShutdown()
	}

	if b.chunkPool != nil {
		b.chunkPool.Close()
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

// onElected is called when this node becomes primary.
func (b *Broker) onElected(outerCtx, primaryCtx context.Context) {
	b.logger.Info("klite promoted to primary")
	b.replRole.Store(1)

	if b.cfg.RoleChangeHook != nil {
		b.cfg.RoleChangeHook.SetPrimary()
	}

	// Stop any running receiver (was standby before)
	b.stopReceiver()

	// Rebuild high watermarks from WAL index. The receiver advances HW as
	// entries arrive, but WAL replay at startup does not set HW for entries
	// that predate the receiver (e.g. local WAL from a previous primary tenure).
	b.rebuildHWFromWAL()

	// Probe S3 for the actual flush watermark BEFORE rebuilding chunks.
	// The standby's s3FlushWatermark lags the actual S3 state because the
	// old primary may have flushed more data after the last watermark update
	// was replicated. Without this probe, rebuildChunksFromWAL may skip WAL
	// entries that it assumes are in S3 (based on the stale watermark) but
	// whose offsets fall in a gap between the stale watermark and the first
	// WAL entry — data the old primary had in chunks/WAL and flushed to S3
	// but never replicated the watermark for.
	if b.s3Client != nil {
		b.probeS3Watermarks()
	}

	// Log partition state at promotion time for diagnostics.
	if b.walIndex != nil {
		for _, tp := range b.walIndex.AllPartitions() {
			td := b.state.GetTopicByID(tp.TopicID)
			if td == nil || int(tp.Partition) >= len(td.Partitions) {
				continue
			}
			pd := td.Partitions[tp.Partition]
			pd.RLock()
			hw := pd.HW()
			s3wm := pd.S3FlushWatermark()
			pd.RUnlock()
			walMax := b.walIndex.MaxOffset(tp)
			b.logger.Info("promotion: partition state",
				"topic", td.Name, "partition", tp.Partition,
				"hw", hw, "s3_watermark", s3wm, "wal_max_offset", walMax,
				"wal_entries", len(b.walIndex.PartitionEntries(tp)))
		}
	}

	// Rebuild chunks from WAL entries received during the standby phase.
	// The replication receiver writes WAL entries to disk but does NOT
	// populate in-memory chunks. Without this step, the S3 flusher would
	// only flush chunk data (from startup replay + new produces), advancing
	// the S3 watermark past replicated-but-unchunked data. When WAL segments
	// are then cleaned, that data is permanently lost.
	b.rebuildChunksFromWAL()

	// Log partition state after rebuild for diagnostics.
	if b.walIndex != nil {
		for _, tp := range b.walIndex.AllPartitions() {
			td := b.state.GetTopicByID(tp.TopicID)
			if td == nil || int(tp.Partition) >= len(td.Partitions) {
				continue
			}
			pd := td.Partitions[tp.Partition]
			pd.RLock()
			hw := pd.HW()
			s3wm := pd.S3FlushWatermark()
			pd.RUnlock()
			b.logger.Info("promotion: partition state after rebuild",
				"topic", td.Name, "partition", tp.Partition,
				"hw", hw, "s3_watermark", s3wm)
		}
	}

	// Abort orphaned transactions that survived failover. rebuildChunksFromWAL
	// reconstructs partition-level openTxnPIDs but not PIDManager txn state,
	// so the timeout sweep cannot find them. Sweep them now before accepting
	// connections.
	b.abortOrphanedTransactions()

	// Set up replication sender: start replication listener
	b.startReplicationListener(primaryCtx)

	// Start Kafka listener
	var err error
	ln := b.cfg.Listener
	if ln == nil {
		listenAddr := b.kafkaListenAddr
		if listenAddr == "" {
			listenAddr = b.cfg.Listen
		}
		ln, err = net.Listen("tcp", listenAddr)
		if err != nil {
			b.logger.Error("failed to start Kafka listener, reverting to standby", "err", err)
			if b.cfg.RoleChangeHook != nil {
				b.cfg.RoleChangeHook.ClearPrimary()
			}
			b.shutdownPrimary()
			b.startReceiverFromLease()
			return
		}
	}
	b.cfg.Listener = nil // consume the injected listener so re-promotion creates a new one
	b.kafkaListenAddr = ln.Addr().String()
	b.listener = ln

	// Resolve advertised address now that the listener is open
	advAddr, warn := b.resolveAdvertisedAddr()
	if warn {
		b.logger.Warn("--advertised-addr not set, using derived address in Metadata responses; clients outside this host won't be able to connect",
			"advertised_addr", advAddr)
	}
	b.registerRuntimeHandlers(advAddr)

	go func() {
		if serveErr := b.server.Serve(b.listener); serveErr != nil {
			b.logger.Debug("kafka server stopped", "err", serveErr)
		}
	}()

	b.logger.Info("kafka listener started", "addr", b.listener.Addr().String())

	// Start primary-only background tasks
	go b.retentionLoop(primaryCtx)
	go b.txnTimeoutLoop(primaryCtx)
	if b.s3Client != nil {
		if b.s3Flusher != nil {
			b.s3Flusher.Start()
		}
		go b.compactionLoop(primaryCtx)
		go b.s3GCLoop(primaryCtx)
	}
}

// onDemoted is called when this node loses the primary role.
func (b *Broker) onDemoted() {
	b.logger.Warn("klite demoted to standby")
	b.shutdownPrimary()

	if b.cfg.RoleChangeHook != nil {
		b.cfg.RoleChangeHook.ClearPrimary()
	}

	// Start receiver to connect to new primary
	b.startReceiverFromLease()
}

// shutdownPrimary cleanly shuts down primary-specific resources.
func (b *Broker) shutdownPrimary() {
	// Log final partition state for diagnostics.
	for _, td := range b.state.GetAllTopics() {
		for _, pd := range td.Partitions {
			pd.RLock()
			hw := pd.HW()
			s3wm := pd.S3FlushWatermark()
			pd.RUnlock()
			b.logger.Info("shutdownPrimary: final partition state",
				"topic", td.Name, "partition", pd.Index,
				"hw", hw, "s3_watermark", s3wm)
		}
	}
	if b.walWriter != nil {
		b.logger.Info("shutdownPrimary: local-only batches since last standby",
			"count", b.walWriter.LocalOnlyBatches())
	}

	// 1. Close Kafka listener (MUST be first — no new connections)
	if b.listener != nil {
		_ = b.listener.Close()
		b.listener = nil
	}

	// 2. Close existing client connections. klite is a single-primary-per-cluster
	// broker, so on demotion zero partitions remain on this node — there's
	// nothing useful a client can do on this connection. This also stops
	// readLoops from dispatching new Fetch handlers.
	b.server.CloseConns()

	// 3. Wake all Fetch long-poll waiters so in-flight Fetch handlers return
	// immediately. This MUST happen after CloseConns — otherwise the Fetch
	// handler returns, the client sends a new Fetch, and the readLoop
	// dispatches a new handler with a fresh waiter that missed the wake.
	b.state.WakeAllFetchWaiters()

	// 4. Drain in-flight requests
	b.server.Wait()
	b.state.StopAllGroups()

	// 5. Clear WAL Replicator
	if b.walWriter != nil {
		b.walWriter.SetReplicator(nil)
	}

	// 6. Clear metadata.log hooks
	if b.metaLog != nil {
		b.metaLog.SetReplicateHook(nil)
		b.metaLog.SetCompactHook(nil)
	}

	// 7. Close replication listener and sender
	if b.replListener != nil {
		_ = b.replListener.Close()
		b.replListener = nil
	}
	if b.replSender != nil {
		b.replSender.Close()
		b.replSender = nil
	}

	// 8. Stop S3 flusher (final flush)
	if b.s3Flusher != nil {
		b.s3Flusher.Stop()
	}

	b.replRole.Store(0)
}

// startReplicationListener starts the mTLS replication listener.
func (b *Broker) startReplicationListener(ctx context.Context) {
	addr := b.cfg.ReplicationAddr
	if addr == "" {
		addr = ":9093"
	}

	var ln net.Listener
	var err error
	if b.replTLSConfig != nil {
		ln, err = crypto_tls.Listen("tcp", addr, b.replTLSConfig)
	} else {
		ln, err = net.Listen("tcp", addr)
	}
	if err != nil {
		b.logger.Error("failed to start replication listener", "err", err)
		return
	}

	b.replListener = ln
	b.logger.Info("replication listener started", "addr", ln.Addr().String())

	go b.acceptReplicationConns(ctx, ln)
}

// acceptReplicationConns accepts one standby connection at a time.
func (b *Broker) acceptReplicationConns(ctx context.Context, ln net.Listener) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
			}
			b.logger.Debug("replication accept error", "err", err)
			return
		}

		// Only one standby at a time — if we already have a sender with
		// a connected standby, reject this connection.
		if b.replSender != nil && b.replSender.Connected() {
			b.logger.Warn("rejecting second standby connection")
			_ = conn.Close()
			continue
		}

		// Handle inline (not in a goroutine) to avoid races on broker
		// state (replSender, walWriter.SetReplicator, metaLog hooks).
		b.handleStandbyConn(ctx, conn)
	}
}

// handleStandbyConn handles a single standby connection (reads HELLO, sends
// SNAPSHOT if needed, and wires the new Sender into the WAL writer).
//
// IMPORTANT: reconnection only sets up forward-streaming of new WAL entries.
// The primary does NOT backfill WAL entries that were written while the standby
// was disconnected (the "local-only" window tracked by localOnlyBatches).
// Those entries reach the standby only via S3: the primary's S3 flusher must
// persist them before the next failover, or they will be lost. Callers that
// need the standby to be fully caught up must wait at least one S3 flush
// interval after reconnection.
func (b *Broker) handleStandbyConn(ctx context.Context, conn net.Conn) {
	// Read HELLO
	msgType, payload, err := repl.ReadFrame(conn)
	if err != nil {
		b.logger.Warn("replication: failed to read HELLO", "err", err)
		_ = conn.Close()
		return
	}

	if msgType != repl.MsgHello {
		b.logger.Warn("replication: expected HELLO, got", "type", msgType)
		_ = conn.Close()
		return
	}

	lastWALSeq, epoch, err := repl.UnmarshalHello(payload)
	if err != nil {
		b.logger.Warn("replication: invalid HELLO", "err", err)
		_ = conn.Close()
		return
	}

	localOnly := b.walWriter.LocalOnlyBatches()
	b.logger.Info("standby connected",
		"last_wal_seq", lastWALSeq,
		"epoch", epoch,
		"local_only_batches_since_last_standby", localOnly)
	b.walWriter.ResetLocalOnlyBatches()

	ackTimeout := b.cfg.ReplicationAckTimeout
	if ackTimeout == 0 {
		ackTimeout = 5 * time.Second
	}

	// Close existing sender if any
	if b.replSender != nil {
		b.replSender.Close()
	}

	sender := repl.NewSender(conn, ackTimeout, b.logger, b.cfg.Clock)
	sender.S3FlushWatermarkFn = b.walWriter.S3FlushWatermark
	b.replSender = sender

	b.walWriter.SetReplicator(sender)

	// Re-wire metadata hooks
	b.metaLog.SetReplicateHook(func(frame []byte) {
		sender.SendMeta(frame)
	})
	b.metaLog.SetCompactHook(func(data []byte) {
		walSeq := b.walWriter.NextSequence()
		if snapErr := sender.SendSnapshot(walSeq, data); snapErr != nil {
			b.logger.Warn("failed to send compaction snapshot", "err", snapErr)
		}
	})

	// Send SNAPSHOT on epoch mismatch or fresh standby
	// (For simplicity, always send SNAPSHOT on new connection to ensure consistency)
	if epoch == 0 || epoch != b.currentEpoch() {
		metaData, readErr := os.ReadFile(b.metaLog.Path())
		if readErr != nil {
			b.logger.Warn("replication: failed to read metadata.log for snapshot", "err", readErr)
			return
		}
		walSeq := b.walWriter.NextSequence()
		if snapErr := sender.SendSnapshot(walSeq, metaData); snapErr != nil {
			b.logger.Warn("replication: failed to send snapshot", "err", snapErr)
		}
	}

	// Start keepalive goroutine
	go b.keepaliveLoop(ctx, sender)
}

// keepaliveLoop sends empty WAL_BATCH (keepalive) every 5s while the sender is connected.
func (b *Broker) keepaliveLoop(ctx context.Context, sender *repl.Sender) {
	ticker := b.cfg.Clock.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !sender.Connected() {
				return
			}
			sender.SendKeepalive()
		}
	}
}

// currentEpoch returns the current lease epoch.
func (b *Broker) currentEpoch() uint64 {
	if b.elector == nil {
		return 0
	}
	return b.elector.Epoch()
}

// startReceiverFromLease reads the primary address from the lease and starts a receiver.
// If the primary address is not yet known (new primary not elected), it polls
// until the address appears or the context is cancelled.
func (b *Broker) startReceiverFromLease() {
	if b.elector == nil {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	b.mu.Lock()
	b.replCancel = cancel
	b.mu.Unlock()

	go func() {
		// Wait for primary address to become available
		primaryAddr := b.elector.PrimaryAddr()
		for primaryAddr == "" {
			b.logger.Debug("repl receiver: no primary address in lease, polling...")
			select {
			case <-ctx.Done():
				return
			case <-b.cfg.Clock.After(100 * time.Millisecond):
			}
			primaryAddr = b.elector.PrimaryAddr()
		}

		epoch := b.elector.Epoch()
		hwAdvancer := func(topicID [16]byte, partition int32, endOffset int64) {
			td := b.state.GetTopicByID(topicID)
			if td == nil || int(partition) >= len(td.Partitions) {
				return
			}
			pd := td.Partitions[partition]
			pd.Lock()
			if endOffset > pd.HW() {
				pd.SetHW(endOffset)
			}
			pd.Unlock()
		}
		receiver := repl.NewReceiver(b.walWriter, b.metaLog, hwAdvancer, epoch, b.logger)

		for {
			b.logger.Info("repl receiver: connecting to primary", "addr", primaryAddr)
			err := receiver.Run(ctx, primaryAddr, b.replTLSConfig)
			if ctx.Err() != nil {
				return
			}
			b.logger.Warn("repl receiver: disconnected from primary, retrying in 2s", "err", err)

			select {
			case <-ctx.Done():
				return
			case <-b.cfg.Clock.After(2 * time.Second):
			}

			// Re-read primary address in case it changed
			if addr := b.elector.PrimaryAddr(); addr != "" {
				primaryAddr = addr
			}
			receiver.SetEpoch(b.elector.Epoch())
		}
	}()
}

// stopReceiver stops the replication receiver if it's running.
func (b *Broker) stopReceiver() {
	b.mu.Lock()
	cancel := b.replCancel
	b.replCancel = nil
	b.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// rebuildHWFromWAL scans the WAL index and advances the high watermark
// for every partition that has WAL data. This catches entries from a
// previous primary tenure that were written before the receiver was active.
func (b *Broker) rebuildHWFromWAL() {
	if b.walIndex == nil {
		return
	}
	allTP := b.walIndex.AllPartitions()
	rebuilt := 0
	for _, tp := range allTP {
		maxOff := b.walIndex.MaxOffset(tp)
		if maxOff == 0 {
			continue
		}
		td := b.state.GetTopicByID(tp.TopicID)
		if td == nil {
			b.logger.Warn("rebuildHW: topic not found in state (may indicate missed metadata entry)", "topic_id", tp.TopicID, "partition", tp.Partition)
			continue
		}
		if int(tp.Partition) >= len(td.Partitions) {
			continue
		}
		pd := td.Partitions[tp.Partition]
		pd.Lock()
		if maxOff > pd.HW() {
			b.logger.Debug("rebuildHW: advancing HW", "topic", td.Name, "partition", tp.Partition, "old_hw", pd.HW(), "new_hw", maxOff)
			pd.SetHW(maxOff)
			rebuilt++
		}
		pd.Unlock()
	}
	if rebuilt > 0 {
		b.logger.Info("rebuilt high watermarks from WAL index", "partitions", rebuilt)
	} else if len(allTP) > 0 {
		b.logger.Debug("rebuildHW: no HW changes needed", "wal_partitions", len(allTP))
	}
}

// rebuildChunksFromWAL repopulates in-memory chunks from WAL entries that are
// above the partition's S3 flush watermark. This is necessary on promotion
// because the replication receiver writes WAL entries to disk without adding
// them to chunks. Without this, the S3 flusher skips WAL-only data, creating
// a gap between what S3 has and what chunks cover.
//
// Called during onElected, before the Kafka listener or S3 flusher starts,
// so no concurrent produce or flush activity exists.
func (b *Broker) rebuildChunksFromWAL() {
	if b.walIndex == nil || b.chunkPool == nil {
		return
	}

	allTP := b.walIndex.AllPartitions()
	var rebuilt int
	for _, tp := range allTP {
		td := b.state.GetTopicByID(tp.TopicID)
		if td == nil {
			continue
		}
		if int(tp.Partition) >= len(td.Partitions) {
			continue
		}
		pd := td.Partitions[tp.Partition]

		pd.Lock()
		s3WM := pd.S3FlushWatermark()

		// Detach all existing chunks — they were populated from WAL replay
		// at startup and may cover a subset of what's now in the WAL. We
		// rebuild from scratch using the full WAL index.
		oldChunks := pd.DetachSealedChunks(true)
		pd.Unlock()
		if len(oldChunks) > 0 {
			b.chunkPool.ReleaseMany(oldChunks)
		}

		entries := b.walIndex.PartitionEntries(tp)
		var loaded int
		var skippedS3, skippedRead int
		var firstLoadedOffset, lastLoadedOffset int64
		firstLoadedOffset = -1
		// Track open txns per-producer to reconstruct abortedTxns index.
		openTxnFirstOffset := make(map[int64]int64) // producerID -> first data offset
		for _, e := range entries {
			data, err := b.walWriter.ReadBatch(e)
			if err != nil {
				skippedRead++
				b.logger.Warn("rebuildChunks: skipping unreadable WAL entry",
					"topic_id", tp.TopicID, "partition", tp.Partition,
					"base_offset", e.BaseOffset, "err", err)
				continue
			}

			meta, err := cluster.ParseBatchHeader(data)
			if err != nil {
				b.logger.Warn("rebuildChunks: skipping corrupt batch",
					"topic_id", tp.TopicID, "partition", tp.Partition,
					"base_offset", e.BaseOffset, "err", err)
				continue
			}

			// Replay dedup state for ALL entries so the PIDManager knows
			// about producers whose batches were already flushed to S3.
			if meta.ProducerID >= 0 && meta.BaseSequence >= 0 {
				ctp := cluster.TopicPartition{Topic: td.Name, Partition: int32(tp.Partition)}
				b.state.PIDManager().ReplayBatch(ctp, meta, e.BaseOffset)
			}

			// Skip chunk rebuild for entries already flushed to S3.
			if e.LastOffset < s3WM {
				skippedS3++
				continue
			}

			spare := pd.AcquireSpareChunk(len(data))
			pd.Lock()

			// Reconstruct transaction state.
			if cluster.IsTransactionalBatch(data) && meta.ProducerID >= 0 {
				if cluster.IsControlBatch(data) {
					pd.RemoveOpenTxn(meta.ProducerID)
					if !cluster.IsCommitControlBatch(data) {
						if firstOff, ok := openTxnFirstOffset[meta.ProducerID]; ok {
							pd.AddAbortedTxn(cluster.AbortedTxnEntry{
								ProducerID:  meta.ProducerID,
								FirstOffset: firstOff,
								LastOffset:  e.BaseOffset,
							})
						}
					}
					delete(openTxnFirstOffset, meta.ProducerID)
				} else {
					pd.AddOpenTxn(meta.ProducerID, e.BaseOffset)
					if _, exists := openTxnFirstOffset[meta.ProducerID]; !exists {
						openTxnFirstOffset[meta.ProducerID] = e.BaseOffset
					}
				}
			}

			spare = pd.AppendToChunk(data, chunk.ChunkBatch{
				BaseOffset:      e.BaseOffset,
				LastOffsetDelta: meta.LastOffsetDelta,
				MaxTimestamp:    meta.MaxTimestamp,
				NumRecords:      meta.NumRecords,
			}, spare)
			pd.Unlock()
			pd.ReleaseSpareChunk(spare)
			if firstLoadedOffset < 0 {
				firstLoadedOffset = e.BaseOffset
			}
			lastLoadedOffset = e.LastOffset
			loaded++
		}

		if loaded > 0 {
			rebuilt++
			b.logger.Debug("rebuildChunks: loaded WAL entries into chunks",
				"topic", td.Name, "partition", tp.Partition,
				"entries", loaded, "s3_watermark", s3WM,
				"first_offset", firstLoadedOffset, "last_offset", lastLoadedOffset,
				"skipped_s3", skippedS3, "skipped_read", skippedRead,
				"total_wal_entries", len(entries))
		}
	}

	if rebuilt > 0 {
		b.logger.Info("rebuilt chunks from WAL entries", "partitions", rebuilt)
	}
}

// s3TLSStore adapts the S3 client to the TLSStore interface.
type s3TLSStore struct {
	s3Client *s3store.Client
}

func (s *s3TLSStore) Put(ctx context.Context, key string, data []byte) error {
	return s.s3Client.PutObject(ctx, key, data)
}

func (s *s3TLSStore) PutIfAbsent(ctx context.Context, key string, data []byte) error {
	err := s.s3Client.PutObjectIfAbsent(ctx, key, data)
	if errors.Is(err, s3store.ErrPreconditionFailed) {
		return repl.ErrAlreadyExists
	}
	return err
}

func (s *s3TLSStore) Get(ctx context.Context, key string) ([]byte, error) {
	return s.s3Client.GetObject(ctx, key)
}

// createS3Elector creates an S3 lease elector from broker config.
func (b *Broker) createS3Elector() (lease.Elector, error) {
	var s3api s3store.S3API
	if b.cfg.S3API != nil {
		var ok bool
		s3api, ok = b.cfg.S3API.(s3store.S3API)
		if !ok {
			return nil, fmt.Errorf("S3API has unexpected type %T", b.cfg.S3API)
		}
	} else {
		var err error
		s3api, err = createAWSS3Client(b.cfg)
		if err != nil {
			return nil, fmt.Errorf("create AWS S3 client for lease: %w", err)
		}
	}

	prefix := "klite-" + b.clusterID
	if b.cfg.S3Prefix != "" {
		prefix = b.cfg.S3Prefix + "/" + prefix
	}

	replAddr, warn := resolveReplicationAddr(b.cfg.ReplicationAddr, b.cfg.ReplicationAdvertisedAddr)
	if warn {
		b.logger.Warn("using hostname as replication address; set --replication-advertised-addr if hostname does not resolve to a routable IP from the standby",
			"repl_addr", replAddr)
	}

	holder := b.cfg.ReplicationID
	if holder == "" {
		hostname, err := os.Hostname()
		if err != nil {
			hostname = "unknown"
		}
		holder = hostname
	}

	leaseDur := b.cfg.LeaseDuration
	if leaseDur == 0 {
		leaseDur = 15 * time.Second
	}
	renewInt := b.cfg.LeaseRenewInterval
	if renewInt == 0 {
		renewInt = 5 * time.Second
	}
	retryInt := b.cfg.LeaseRetryInterval
	if retryInt == 0 {
		retryInt = 2 * time.Second
	}

	return s3lease.New(s3lease.Config{
		S3:            s3api,
		Bucket:        b.cfg.S3Bucket,
		Key:           prefix + "/lease",
		Holder:        holder,
		ReplAddr:      replAddr,
		LeaseDuration: leaseDur,
		RenewInterval: renewInt,
		RetryInterval: retryInt,
		Logger:        b.logger,
	}), nil
}

func (b *Broker) initMetadataLog() error {
	ml, err := metadata.NewLog(metadata.LogConfig{
		DataDir: b.cfg.DataDir,
		Logger:  b.logger,
	})
	if err != nil {
		return err
	}

	ml.SetCallbacks(
		func(e metadata.CreateTopicEntry) {
			b.state.CreateTopicFromReplay(e.TopicName, int(e.PartitionCount), e.TopicID, e.Configs)
		},
		func(e metadata.DeleteTopicEntry) {
			b.state.DeleteTopic(e.TopicName)
		},
		func(e metadata.AlterConfigEntry) {
			b.state.SetTopicConfig(e.TopicName, e.Key, e.Value)
		},
		func(e metadata.OffsetCommitEntry) {
			b.state.SetCommittedOffsetFromReplay(e.Group, e.Topic, e.Partition, e.Offset, e.Metadata)
		},
		func(e metadata.ProducerIDEntry) {
			b.state.PIDManager().SetNextPID(e.NextProducerID)
		},
		func(e metadata.LogStartOffsetEntry) {
			b.state.SetLogStartOffsetFromReplay(e.TopicName, e.Partition, e.LogStartOffset)
		},
	)

	ml.SetCompactionWatermarkCallback(func(e metadata.CompactionWatermarkEntry) {
		b.state.SetCompactionWatermarkFromReplay(e.TopicName, e.Partition, e.CleanedUpTo)
	})

	if b.saslStore != nil {
		ml.SetScramCallbacks(
			func(e metadata.ScramCredentialEntry) {
				auth := sasl.ScramAuthFromPreHashed(
					scramMechName(e.Mechanism),
					int(e.Iterations),
					e.SaltedPass,
					e.Salt,
				)
				switch e.Mechanism {
				case 1:
					b.saslStore.AddScram256(e.Username, auth)
				case 2:
					b.saslStore.AddScram512(e.Username, auth)
				}
			},
			func(e metadata.ScramCredentialDeleteEntry) {
				switch e.Mechanism {
				case 1:
					b.saslStore.DeleteScram256(e.Username)
				case 2:
					b.saslStore.DeleteScram512(e.Username)
				}
			},
		)
	}

	count, err := ml.Replay()
	if err != nil {
		_ = ml.Close()
		return fmt.Errorf("replay metadata.log: %w", err)
	}

	if count > 0 {
		b.logger.Info("metadata.log replay complete", "entries", count)
	}

	ml.SetSnapshotFn(b.state.SnapshotEntries)
	ml.Compact()

	b.metaLog = ml
	b.state.SetMetadataLog(ml)

	return nil
}

func (b *Broker) initWAL() error {
	walDir := filepath.Join(b.cfg.DataDir, "wal")

	idx := wal.NewIndex()
	b.walIndex = idx

	syncInterval := time.Duration(b.cfg.WALSyncIntervalMs) * time.Millisecond
	if syncInterval == 0 {
		syncInterval = 2 * time.Millisecond
	}
	segMaxBytes := b.cfg.WALSegmentMaxBytes
	if segMaxBytes == 0 {
		segMaxBytes = 64 * 1024 * 1024
	}
	maxDiskSize := b.cfg.WALMaxDiskSize
	if maxDiskSize == 0 {
		maxDiskSize = 1024 * 1024 * 1024
	}

	cfg := wal.WriterConfig{
		Dir:             walDir,
		SyncInterval:    syncInterval,
		SegmentMaxBytes: segMaxBytes,
		MaxDiskSize:     maxDiskSize,
		FsyncEnabled:    true,
		S3Configured:    b.cfg.S3Bucket != "",
		Logger:          b.logger,
	}

	w, err := wal.NewWriter(cfg, idx)
	if err != nil {
		return fmt.Errorf("create WAL writer: %w", err)
	}

	b.walWriter = w

	// Initialize chunk pool only when S3 is configured. The pool exists
	// solely for the S3 flush pipeline — without a flusher, chunks are
	// never released and producers would deadlock on Acquire().
	// When S3 is disabled, all reads are served from the WAL.
	var pool *chunk.Pool
	if b.cfg.S3Bucket != "" {
		poolMem := b.cfg.ChunkPoolMemory
		if poolMem == 0 {
			poolMem = 512 * 1024 * 1024 // 512 MiB
		}
		pool = chunk.NewPool(poolMem, cluster.DefaultMaxMessageBytes)
		b.chunkPool = pool
	}
	b.state.SetWALConfig(w, idx, pool)

	if err := b.replayWAL(w); err != nil {
		return fmt.Errorf("replay WAL: %w", err)
	}

	if err := w.Start(); err != nil {
		return fmt.Errorf("start WAL writer: %w", err)
	}

	b.logger.Info("WAL initialized",
		"dir", walDir,
		"sync_interval", syncInterval,
		"segment_max_bytes", segMaxBytes,
	)

	return nil
}

// replayWAL scans existing WAL segments and rebuilds in-memory state.
// Authority rules:
//   - metadata.log is authoritative for topic existence, partition counts, and logStartOffset.
//   - WAL entries for unknown topics or out-of-range partitions are skipped with a warning.
//   - WAL entries whose last offset is below the partition's logStartOffset (from metadata.log) are skipped.
//   - WAL replay can only advance HW and logStartOffset, never reduce them.
func (b *Broker) replayWAL(w *wal.Writer) error {
	entryCount := 0
	skippedUnknown := 0
	skippedPartition := 0
	skippedBelowLogStart := 0
	skippedCorrupt := 0

	// Track open transactions across replay to reconstruct abortedTxns index.
	type replayTxnKey struct {
		partition  int32
		producerID int64
	}
	replayOpenTxns := make(map[replayTxnKey]int64) // key -> first data offset

	err := w.Replay(func(entry wal.Entry, segmentSeq uint64, fileOffset int64) error {
		// Look up the topic by ID — metadata.log has already been replayed
		td := b.state.GetTopicByID(entry.TopicID)
		if td == nil {
			// WAL entry for topic not in metadata.log. Should not happen
			// (CREATE_TOPIC is written and fsync'd before produce can succeed).
			// Log a warning and skip.
			if skippedUnknown == 0 {
				b.logger.Warn("WAL replay: skipping entry for unknown topic ID",
					"topic_id", fmt.Sprintf("%x", entry.TopicID),
					"segment", segmentSeq, "offset", fileOffset)
			}
			skippedUnknown++
			return nil
		}

		if int(entry.Partition) >= len(td.Partitions) {
			// Partition index beyond the topic's partition count — indicates corruption.
			if skippedPartition == 0 {
				b.logger.Warn("WAL replay: skipping entry for out-of-range partition",
					"topic", td.Name, "partition", entry.Partition,
					"partition_count", len(td.Partitions),
					"segment", segmentSeq, "offset", fileOffset)
			}
			skippedPartition++
			return nil
		}

		pd := td.Partitions[entry.Partition]

		// Parse batch header to get metadata
		meta, err := cluster.ParseBatchHeader(entry.Data)
		if err != nil {
			if skippedCorrupt == 0 {
				b.logger.Warn("WAL replay: skipping entry with corrupted batch header",
					"topic", td.Name, "partition", entry.Partition,
					"segment", segmentSeq, "offset", fileOffset, "err", err)
			}
			skippedCorrupt++
			return nil
		}

		// Reconstruct producer dedup state from committed batches so that
		// duplicate detection works after restart or failover.
		if meta.ProducerID >= 0 && meta.BaseSequence >= 0 {
			ctp := cluster.TopicPartition{Topic: td.Name, Partition: entry.Partition}
			b.state.PIDManager().ReplayBatch(ctp, meta, entry.Offset)
		}

		// Skip WAL entries whose last offset is below the persisted logStartOffset
		lastOffset := entry.Offset + int64(meta.LastOffsetDelta)
		pd.RLock()
		logStart := pd.LogStart()
		pd.RUnlock()
		if lastOffset < logStart {
			skippedBelowLogStart++
			return nil
		}

		// Pre-acquire spare chunk before taking the partition lock.
		spare := pd.AcquireSpareChunk(len(entry.Data))

		// Rebuild partition state: append to chunk pool and advance HW.
		pd.Lock()

		// Advance HW if needed (WAL can only advance, never reduce)
		endOffset := lastOffset + 1
		if endOffset > pd.HW() {
			pd.SetHW(endOffset)
		}

		// Reconstruct transaction state from WAL entries.
		if cluster.IsTransactionalBatch(entry.Data) && meta.ProducerID >= 0 {
			if cluster.IsControlBatch(entry.Data) {
				pd.RemoveOpenTxn(meta.ProducerID)
				if !cluster.IsCommitControlBatch(entry.Data) {
					if firstOff, ok := replayOpenTxns[replayTxnKey{entry.Partition, meta.ProducerID}]; ok {
						pd.AddAbortedTxn(cluster.AbortedTxnEntry{
							ProducerID:  meta.ProducerID,
							FirstOffset: firstOff,
							LastOffset:  entry.Offset,
						})
					}
				}
				delete(replayOpenTxns, replayTxnKey{entry.Partition, meta.ProducerID})
			} else {
				pd.AddOpenTxn(meta.ProducerID, entry.Offset)
				key := replayTxnKey{entry.Partition, meta.ProducerID}
				if _, exists := replayOpenTxns[key]; !exists {
					replayOpenTxns[key] = entry.Offset
				}
			}
		}

		// Append batch data to chunk pool (replaces ring buffer push).
		// Data is already offset-assigned in the WAL.
		spare = pd.AppendToChunk(entry.Data, chunk.ChunkBatch{
			BaseOffset:      entry.Offset,
			LastOffsetDelta: meta.LastOffsetDelta,
			MaxTimestamp:    meta.MaxTimestamp,
			NumRecords:      meta.NumRecords,
		}, spare)
		pd.Unlock()
		pd.ReleaseSpareChunk(spare)

		// Add to WAL index
		tp := wal.TopicPartition{TopicID: entry.TopicID, Partition: entry.Partition}
		idxEntry := wal.IndexEntry{
			BaseOffset:  entry.Offset,
			LastOffset:  lastOffset,
			SegmentSeq:  segmentSeq,
			FileOffset:  fileOffset,
			EntrySize:   int32(4 + 4 + 8 + 16 + 4 + 8 + len(entry.Data)), // length + crc + fixed payload + data
			BatchSize:   int32(len(entry.Data)),
			WALSequence: entry.Sequence,
		}
		b.walIndex.Add(tp, idxEntry)

		entryCount++
		return nil
	})
	if err != nil {
		return err
	}

	if entryCount > 0 || skippedUnknown > 0 || skippedPartition > 0 || skippedBelowLogStart > 0 || skippedCorrupt > 0 {
		b.logger.Info("WAL replay complete",
			"entries", entryCount,
			"skipped_unknown_topic", skippedUnknown,
			"skipped_invalid_partition", skippedPartition,
			"skipped_below_log_start", skippedBelowLogStart,
			"skipped_corrupt_batch", skippedCorrupt,
		)
	}
	return nil
}

// abortOrphanedTransactions sweeps all partitions for open transactions that
// survived a restart. After WAL replay, any transaction still in openTxnPIDs
// is definitionally orphaned — the producer either committed/aborted (and we'd
// have a control record) or crashed. We write abort control batches for each
// orphan so LSO can advance.
func (b *Broker) abortOrphanedTransactions() {
	topics := b.state.GetAllTopics()
	var aborted int
	for _, td := range topics {
		for _, pd := range td.Partitions {
			pd.RLock()
			openPIDs := pd.OpenTxnPIDs()
			pd.RUnlock()
			if len(openPIDs) == 0 {
				continue
			}

			for pid, firstOffset := range openPIDs {
				raw := cluster.BuildControlBatch(pid, 0, false, b.cfg.Clock.Now().UnixMilli())
				meta, err := cluster.ParseBatchHeader(raw)
				if err != nil {
					b.logger.Warn("abortOrphanedTransactions: failed to parse control batch", "err", err)
					continue
				}

				spare := pd.AcquireSpareChunk(len(raw))
				pd.Lock()
				baseOffset, spare := pd.PushBatch(raw, meta, spare)
				pd.RemoveOpenTxn(pid)
				pd.AddAbortedTxn(cluster.AbortedTxnEntry{
					ProducerID:  pid,
					FirstOffset: firstOffset,
					LastOffset:  baseOffset,
				})
				pd.Unlock()
				pd.ReleaseSpareChunk(spare)

				aborted++
			}
		}
	}
	if aborted > 0 {
		b.logger.Info("aborted orphaned transactions after WAL replay", "count", aborted)
	}
}

func (b *Broker) initS3() error {
	var s3api s3store.S3API
	if b.cfg.S3API != nil {
		var ok bool
		s3api, ok = b.cfg.S3API.(s3store.S3API)
		if !ok {
			return fmt.Errorf("S3API config field must implement s3.S3API interface")
		}
	} else {
		var err error
		s3api, err = createAWSS3Client(b.cfg)
		if err != nil {
			return fmt.Errorf("create AWS S3 client: %w", err)
		}
	}

	prefix := "klite-" + b.clusterID
	if b.cfg.S3Prefix != "" {
		prefix = b.cfg.S3Prefix + "/" + prefix
	}

	b.s3Client = s3store.NewClient(s3store.ClientConfig{
		S3Client: s3api,
		Bucket:   b.cfg.S3Bucket,
		Prefix:   prefix,
		Logger:   b.logger,
	})

	b.s3Reader = s3store.NewReader(b.s3Client, b.logger)

	fetchAdapter := &s3store.ReaderAdapter{Reader: b.s3Reader}
	b.state.SetS3Fetcher(fetchAdapter)

	flushInterval := b.cfg.S3FlushInterval
	if flushInterval == 0 {
		flushInterval = 60 * time.Second
	}
	checkInterval := b.cfg.S3FlushCheckInterval
	if checkInterval == 0 {
		checkInterval = 5 * time.Second
	}
	targetObjSize := b.cfg.S3TargetObjectSize
	if targetObjSize == 0 {
		targetObjSize = 64 * 1024 * 1024 // 64 MiB
	}

	triggerCh := make(chan struct{}, 1)
	if b.chunkPool != nil {
		b.chunkPool.SetTriggerCh(triggerCh)
	}

	partAdapter := &s3PartitionAdapter{
		state: b.state,
	}

	flusherCfg := s3store.FlusherConfig{
		Client:            b.s3Client,
		WALWriter:         b.walWriter,
		WALIndex:          b.walIndex,
		FlushInterval:     flushInterval,
		CheckInterval:     checkInterval,
		TargetObjectSize:  targetObjSize,
		UploadConcurrency: 16,
		Logger:            b.logger,
		TriggerCh:         triggerCh,
		MetadataUploader:  b.uploadMetadataLog,
		Reader:            b.s3Reader,
		WatermarkProvider: partAdapter,
	}

	b.s3Flusher = s3store.NewFlusher(flusherCfg, partAdapter)

	// In replication mode, the flusher is started/stopped by onElected/shutdownPrimary.
	// In single-node mode, start it immediately.
	if b.cfg.ReplicationAddr == "" {
		b.s3Flusher.Start()
	}

	// Probe S3 to discover HW for partitions that have data in S3
	// but no local WAL data (disaster recovery scenario)
	b.probeS3Watermarks()

	// Rehydrate dirty object counters for compacted partitions so
	// compaction resumes after restart without new writes.
	b.rehydrateDirtyCounters()

	b.logger.Info("S3 storage initialized",
		"bucket", b.cfg.S3Bucket,
		"prefix", prefix,
		"flush_interval", flushInterval,
		"check_interval", checkInterval,
		"target_object_size", targetObjSize,
	)

	return nil
}

func (b *Broker) uploadMetadataLog(ctx context.Context) error {
	if b.metaLog == nil || b.s3Client == nil {
		return nil
	}

	data, err := os.ReadFile(b.metaLog.Path())
	if err != nil {
		return fmt.Errorf("read metadata.log: %w", err)
	}

	key := b.s3Client.Prefix() + "/metadata.log"
	return b.s3Client.PutObject(ctx, key, data)
}

// maybeRecoverFromS3 checks if metadata.log is missing locally and attempts
// to download it from S3 (disaster recovery scenario).
func (b *Broker) maybeRecoverFromS3() error {
	metaPath := filepath.Join(b.cfg.DataDir, "metadata.log")
	if _, err := os.Stat(metaPath); err == nil {
		return nil // metadata.log exists locally, no recovery needed
	}

	// metadata.log is missing. Try to download from S3.
	var s3api s3store.S3API
	if b.cfg.S3API != nil {
		var ok bool
		s3api, ok = b.cfg.S3API.(s3store.S3API)
		if !ok {
			return fmt.Errorf("S3API must implement s3.S3API")
		}
	} else {
		var err error
		s3api, err = createAWSS3Client(b.cfg)
		if err != nil {
			return fmt.Errorf("create AWS S3 client for recovery: %w", err)
		}
	}

	prefix := "klite-" + b.clusterID
	if b.cfg.S3Prefix != "" {
		prefix = b.cfg.S3Prefix + "/" + prefix
	}

	client := s3store.NewClient(s3store.ClientConfig{
		S3Client: s3api,
		Bucket:   b.cfg.S3Bucket,
		Prefix:   prefix,
		Logger:   b.logger,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	metaKey := prefix + "/metadata.log"
	data, err := client.GetObject(ctx, metaKey)
	if err != nil {
		b.logger.Info("no metadata.log backup found in S3, attempting topic inference from S3 keys", "key", metaKey, "err", err)
		return b.inferTopicsFromS3(ctx, client, prefix, metaPath)
	}

	if err := os.WriteFile(metaPath, data, 0o644); err != nil {
		return fmt.Errorf("write recovered metadata.log: %w", err)
	}

	b.logger.Info("recovered metadata.log from S3",
		"key", metaKey, "size", len(data))

	return nil
}

// inferTopicsFromS3 reconstructs a metadata.log from S3 object keys when both
// the local metadata.log and the S3 backup are missing (full disaster recovery).
// It lists objects under prefix, parses topic/partition from key structure
// (prefix/topic/partition/offset.obj), generates new topic IDs, and writes
// a fresh metadata.log with CREATE_TOPIC entries.
func (b *Broker) inferTopicsFromS3(ctx context.Context, client *s3store.Client, prefix, metaPath string) error {
	objects, err := client.ListObjects(ctx, prefix+"/")
	if err != nil {
		return fmt.Errorf("list S3 objects for inference: %w", err)
	}

	if len(objects) == 0 {
		b.logger.Info("no S3 objects found, nothing to infer")
		return nil
	}

	// Parse topic/partition from keys. Key format: prefix/topicName-topicID/partition/offset.obj
	// Skip non-data keys like prefix/metadata.log.
	type topicInfo struct {
		maxPartition int32
		topicID      [16]byte // extracted from the key's hex-encoded topic ID
	}
	topics := make(map[string]*topicInfo)

	trimPrefix := prefix + "/"
	for _, obj := range objects {
		rel := strings.TrimPrefix(obj.Key, trimPrefix)
		parts := strings.Split(rel, "/")
		if len(parts) != 3 {
			continue // not a data object (e.g. metadata.log)
		}
		if !strings.HasSuffix(parts[2], ".obj") {
			continue
		}

		// parts[0] is "topicName-hexTopicID" (32-char hex suffix)
		topicDir := parts[0]
		topicName, parsedID := s3store.ParseTopicDir(topicDir)
		if topicName == "" {
			continue
		}

		partIdx, err := strconv.Atoi(parts[1])
		if err != nil {
			continue
		}

		ti, ok := topics[topicName]
		if !ok {
			ti = &topicInfo{maxPartition: -1, topicID: parsedID}
			topics[topicName] = ti
		}
		if int32(partIdx) > ti.maxPartition {
			ti.maxPartition = int32(partIdx)
		}
	}

	if len(topics) == 0 {
		b.logger.Info("no topic data found in S3 objects")
		return nil
	}

	// Sort topic names for deterministic metadata.log output.
	topicNames := make([]string, 0, len(topics))
	for name := range topics {
		topicNames = append(topicNames, name)
	}
	sort.Strings(topicNames)

	// Build metadata.log entries with generated topic IDs.
	crc32cTable := crc32.MakeTable(crc32.Castagnoli)
	f, err := os.Create(metaPath)
	if err != nil {
		return fmt.Errorf("create inferred metadata.log: %w", err)
	}
	defer f.Close() //nolint:errcheck // best-effort close

	for _, name := range topicNames {
		ti := topics[name]
		idBytes := ti.topicID
		var zeroID [16]byte
		if idBytes == zeroID {
			topicUUID := uuid.New()
			copy(idBytes[:], topicUUID[:])
		}

		entry := metadata.MarshalCreateTopic(&metadata.CreateTopicEntry{
			TopicName:      name,
			PartitionCount: ti.maxPartition + 1,
			TopicID:        idBytes,
		})

		// Frame: [4 bytes length][4 bytes CRC32c][payload]
		frameSize := 4 + 4 + len(entry)
		frame := make([]byte, frameSize)
		binary.BigEndian.PutUint32(frame[0:4], uint32(4+len(entry)))
		binary.BigEndian.PutUint32(frame[4:8], crc32.Checksum(entry, crc32cTable))
		copy(frame[8:], entry)

		if _, err := f.Write(frame); err != nil {
			return fmt.Errorf("write inferred metadata entry for %s: %w", name, err)
		}

		b.logger.Info("inferred topic from S3",
			"topic", name,
			"partitions", ti.maxPartition+1,
			"topic_id", hex.EncodeToString(idBytes[:]))
	}

	if err := f.Sync(); err != nil {
		return fmt.Errorf("fsync inferred metadata.log: %w", err)
	}

	b.logger.Info("inferred metadata.log from S3 key structure",
		"topics", len(topics))
	return nil
}

// rehydrateDirtyCounters counts S3 objects per compacted partition and sets
// the dirty object counter so compaction can trigger after restart without
// new writes. For each compacted partition, objects above cleanedUpTo are
// counted as dirty.
func (b *Broker) rehydrateDirtyCounters() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	topics := b.state.GetAllTopics()
	for _, td := range topics {
		policy, ok := td.GetConfig("cleanup.policy")
		if !ok {
			policy = "delete"
		}
		if !strings.Contains(policy, "compact") {
			continue
		}

		for _, pd := range td.Partitions {
			prefix := s3store.ObjectKeyPrefix(b.s3Client.Prefix(), td.Name, td.ID, pd.Index)
			objects, err := b.s3Client.ListObjects(ctx, prefix)
			if err != nil {
				b.logger.Debug("dirty counter rehydration failed",
					"topic", td.Name, "partition", pd.Index, "err", err)
				continue
			}

			pd.Lock()
			cleanedUpTo := pd.CleanedUpTo()
			dirty := int32(0)
			for _, obj := range objects {
				if !strings.HasSuffix(obj.Key, ".obj") {
					continue
				}
				baseOff := s3store.ParseBaseOffsetFromKey(obj.Key)
				if baseOff > cleanedUpTo {
					dirty++
				}
			}
			if dirty > 0 {
				pd.SetDirtyObjects(dirty)
				b.logger.Debug("dirty counter rehydrated",
					"topic", td.Name, "partition", pd.Index, "dirty", dirty)
			}
			pd.Unlock()
		}
	}
}

// probeS3Watermarks discovers high-water marks from S3 for each partition
// and updates the cluster state. Always probes, even when a watermark already
// exists, because the standby's watermark may lag the actual S3 state — the
// old primary may have flushed more data after the last watermark was
// replicated. For partitions with no WAL data (disaster recovery), both HW
// and S3 flush watermark are set from S3.
func (b *Broker) probeS3Watermarks() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	topics := b.state.GetAllTopics()
	updated := 0
	for _, td := range topics {
		for _, pd := range td.Partitions {
			s3HW, err := b.s3Reader.DiscoverHW(ctx, td.Name, td.ID, pd.Index)
			if err != nil {
				b.logger.Debug("S3 HW probe failed", "topic", td.Name, "partition", pd.Index, "err", err)
				continue
			}
			if s3HW > 0 {
				pd.Lock()
				if s3HW > pd.HW() {
					pd.SetHW(s3HW)
				}
				pd.SetS3FlushWatermark(s3HW)
				pd.Unlock()
				updated++
			}
		}
	}

	if updated > 0 {
		b.logger.Info("S3 watermark probe complete", "partitions_updated", updated)
	}
}

func createAWSS3Client(cfg Config) (s3store.S3API, error) {
	region := cfg.S3Region
	if region == "" {
		region = "us-east-1"
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion(region),
	)
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}

	var s3Opts []func(*s3sdk.Options)
	if cfg.S3Endpoint != "" {
		s3Opts = append(s3Opts, func(o *s3sdk.Options) {
			o.BaseEndpoint = &cfg.S3Endpoint
			o.UsePathStyle = true
		})
	}

	return s3sdk.NewFromConfig(awsCfg, s3Opts...), nil
}

type s3PartitionAdapter struct {
	state *cluster.State
}

func (a *s3PartitionAdapter) FlushablePartitions() []s3store.FlushPartition {
	parts := a.state.FlushablePartitions()
	var result []s3store.FlushPartition

	for _, fp := range parts {
		pd := fp.Partition_ // captured partition reference

		pd.RLock()
		hasData := pd.HasChunkData()
		sealedBytes := pd.SealedChunkBytes()
		oldestTime := pd.OldestSealedChunkTime()
		pd.RUnlock()

		if !hasData {
			continue
		}

		result = append(result, s3store.FlushPartition{
			Topic:           fp.Topic,
			Partition:       fp.Partition,
			TopicID:         fp.TopicID,
			S3Watermark:     fp.S3Watermark,
			HW:              fp.HW,
			SealedBytes:     sealedBytes,
			OldestChunkTime: oldestTime,
			DetachChunks: func(flushAll bool) ([]*chunk.Chunk, *chunk.Pool) {
				pd.Lock()
				chunks := pd.DetachSealedChunks(flushAll)
				pool := pd.ChunkPool()
				pd.Unlock()
				return chunks, pool
			},
			ReattachChunks: func(chunks []*chunk.Chunk) {
				pd.Lock()
				pd.ReattachSealedChunks(chunks)
				pd.Unlock()
			},
			AdvanceWatermark: func(newWatermark int64) {
				pd.Lock()
				pd.SetS3FlushWatermark(newWatermark)
				pd.IncrementDirtyObjects()
				pd.Unlock()
			},
		})
	}

	return result
}

func (a *s3PartitionAdapter) AllPartitionWatermarks() []s3store.PartitionWatermarkInfo {
	wms := a.state.AllPartitionWatermarks()
	result := make([]s3store.PartitionWatermarkInfo, len(wms))
	for i, w := range wms {
		result[i] = s3store.PartitionWatermarkInfo{
			TopicID:     w.TopicID,
			Partition:   w.Partition,
			S3Watermark: w.S3Watermark,
		}
	}
	return result
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

// resolveReplicationAddr returns the advertised replication address for lease
// body and standby discovery. Priority:
// 1. --replication-advertised-addr (explicit)
// 2. --replication-addr if it specifies a concrete host (not wildcard)
// 3. <hostname>:port from --replication-addr when using wildcard bind
func resolveReplicationAddr(replAddr, replAdvertisedAddr string) (addr string, warn bool) {
	if replAdvertisedAddr != "" {
		return replAdvertisedAddr, false
	}

	if replAddr == "" {
		replAddr = ":9093"
	}

	host, port, err := net.SplitHostPort(replAddr)
	if err != nil {
		return replAddr, false
	}

	if host != "" && host != "0.0.0.0" && host != "::" {
		return replAddr, false
	}

	hostname, err := os.Hostname()
	if err != nil {
		hostname = "localhost"
	}
	return net.JoinHostPort(hostname, port), true
}

// isStandby returns true if replication is enabled and this node is a standby.
func (b *Broker) isStandby() bool {
	return b.cfg.ReplicationAddr != "" && b.replRole.Load() == 0
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

// txnTimeoutLoop periodically checks for expired transactions and writes abort
// control batches for them. This handles the case where a transactional producer
// crashes mid-transaction — without this sweep, the open transaction blocks LSO
// advancement indefinitely.
func (b *Broker) txnTimeoutLoop(ctx context.Context) {
	ticker := b.cfg.Clock.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			b.abortExpiredTransactions()
		}
	}
}

func (b *Broker) abortExpiredTransactions() {
	expired := b.state.PIDManager().ExpiredTransactions()
	for _, snap := range expired {
		endState, errCode := b.state.PIDManager().PrepareEndTxn(snap.ProducerID, snap.Epoch, false)
		if errCode != 0 {
			continue
		}
		if endState.TxnPartitions == nil {
			continue
		}

		controlBatch := cluster.BuildControlBatch(endState.ProducerID, endState.Epoch, false, b.cfg.Clock.Now().UnixMilli())

		// Two-phase WAL write: submit all entries first, then commit after fsync.
		type pendingAbort struct {
			pd         *cluster.PartData
			baseOffset int64
			meta       cluster.BatchMeta
			stored     []byte
			errCh      <-chan error
			tp         cluster.TopicPartition
		}
		var pending []pendingAbort

		for tp := range endState.TxnPartitions {
			td := b.state.GetTopic(tp.Topic)
			if td == nil || int(tp.Partition) >= len(td.Partitions) {
				continue
			}
			pd := td.Partitions[tp.Partition]

			meta, err := cluster.ParseBatchHeader(controlBatch)
			if err != nil {
				continue
			}

			raw := make([]byte, len(controlBatch))
			copy(raw, controlBatch)

			pd.Lock()
			baseOffset := pd.ReserveOffset(meta)
			cluster.AssignOffset(raw, baseOffset)

			walEntry := &wal.Entry{
				TopicID:   pd.TopicID,
				Partition: pd.Index,
				Offset:    baseOffset,
				Data:      raw,
			}
			errCh, walErr := b.walWriter.AppendAsync(walEntry)
			if walErr != nil {
				pd.RollbackReserve(baseOffset)
				pd.Unlock()
				b.logger.Error("abortExpiredTransactions: WAL submit failed",
					"topic", tp.Topic, "partition", tp.Partition, "err", walErr)
				continue
			}
			pd.Unlock()

			pending = append(pending, pendingAbort{
				pd:         pd,
				baseOffset: baseOffset,
				meta:       meta,
				stored:     raw,
				errCh:      errCh,
				tp:         tp,
			})
		}

		for i := range pending {
			pc := &pending[i]

			walErr := <-pc.errCh
			if walErr != nil {
				b.logger.Error("abortExpiredTransactions: WAL write failed",
					"topic", pc.tp.Topic, "partition", pc.tp.Partition, "err", walErr)
				pc.pd.Lock()
				pc.pd.SkipOffsets(pc.baseOffset, int64(pc.meta.LastOffsetDelta)+1)
				pc.pd.Unlock()
				continue
			}

			spare := pc.pd.AcquireSpareChunk(len(pc.stored))
			pc.pd.Lock()
			spare = pc.pd.AppendToChunk(pc.stored, chunk.ChunkBatch{
				BaseOffset:      pc.baseOffset,
				LastOffsetDelta: pc.meta.LastOffsetDelta,
				MaxTimestamp:    pc.meta.MaxTimestamp,
				NumRecords:      pc.meta.NumRecords,
			}, spare)
			pc.pd.CommitBatch(cluster.StoredBatch{
				BaseOffset:      pc.baseOffset,
				LastOffsetDelta: pc.meta.LastOffsetDelta,
				RawBytes:        pc.stored,
				MaxTimestamp:    pc.meta.MaxTimestamp,
				NumRecords:      pc.meta.NumRecords,
			})
			pc.pd.RemoveOpenTxn(endState.ProducerID)
			if firstOffset, ok := endState.TxnFirstOffsets[pc.tp]; ok {
				pc.pd.AddAbortedTxn(cluster.AbortedTxnEntry{
					ProducerID:  endState.ProducerID,
					FirstOffset: firstOffset,
					LastOffset:  pc.baseOffset,
				})
			}
			pc.pd.Unlock()
			pc.pd.ReleaseSpareChunk(spare)
			pc.pd.NotifyWaiters()
		}

		b.logger.Info("aborted expired transaction",
			"producer_id", snap.ProducerID,
			"txn_id", snap.TxnID,
			"partitions", len(endState.TxnPartitions))
	}
}

func (b *Broker) compactionLoop(ctx context.Context) {
	interval := b.cfg.CompactionCheckInterval
	if interval == 0 {
		interval = 30 * time.Second
	}
	minDirty := b.cfg.CompactionMinDirtyObjects
	if minDirty == 0 {
		minDirty = 4
	}

	compactor := s3store.NewCompactor(s3store.CompactorConfig{
		Client: b.s3Client,
		Reader: b.s3Reader,
		Logger: b.logger,
		PersistWatermark: func(topic string, partition int32, cleanedUpTo int64) error {
			if b.metaLog == nil {
				return nil
			}
			entry := metadata.MarshalCompactionWatermark(&metadata.CompactionWatermarkEntry{
				TopicName:   topic,
				Partition:   partition,
				CleanedUpTo: cleanedUpTo,
			})
			return b.metaLog.AppendSync(entry)
		},
		WindowBytes:   b.cfg.CompactionWindowBytes,
		S3Concurrency: b.cfg.CompactionS3Concurrency,
		ReadRateLimit: b.cfg.CompactionReadRate,
	})

	ticker := b.cfg.Clock.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			b.compactOneDirtyPartition(ctx, compactor, int32(minDirty))
		}
	}
}

func (b *Broker) compactOneDirtyPartition(ctx context.Context, compactor *s3store.Compactor, minDirty int32) {
	topics := b.state.GetAllTopics()

	var bestPD *cluster.PartData
	var bestTD *cluster.TopicData
	var bestDirty int32

	for _, td := range topics {
		policy, ok := td.GetConfig("cleanup.policy")
		if !ok {
			policy = "delete"
		}
		if !strings.Contains(policy, "compact") {
			continue
		}

		for _, pd := range td.Partitions {
			dirty := pd.DirtyObjects()

			// Eligibility check
			if dirty < minDirty {
				// Check staleness guarantee
				lc := pd.LastCompacted()
				switch {
				case lc.IsZero() && dirty > 0:
					// Never compacted and has dirty objects — eligible
				case dirty <= 0:
					continue
				default:
					// Check max.compaction.lag.ms
					maxLagMs := int64(9223372036854775807) // math.MaxInt64
					if v, ok := td.GetConfig("max.compaction.lag.ms"); ok {
						if parsed, err := strconv.ParseInt(v, 10, 64); err == nil {
							maxLagMs = parsed
						}
					}
					if maxLagMs < 9223372036854775807 && b.cfg.Clock.Since(lc).Milliseconds() > maxLagMs && dirty > 0 {
						// Stale — eligible
					} else {
						continue
					}
				}
			}

			if dirty > bestDirty || bestPD == nil {
				bestPD = pd
				bestTD = td
				bestDirty = dirty
			}
		}
	}

	if bestPD == nil {
		return
	}

	minCompactionLagMs := int64(0)
	if v, ok := bestTD.GetConfig("min.compaction.lag.ms"); ok {
		if parsed, err := strconv.ParseInt(v, 10, 64); err == nil {
			minCompactionLagMs = parsed
		}
	}

	deleteRetentionMs := int64(86400000) // 24h default
	if v, ok := bestTD.GetConfig("delete.retention.ms"); ok {
		if parsed, err := strconv.ParseInt(v, 10, 64); err == nil {
			deleteRetentionMs = parsed
		}
	}
	compactor.SetDeleteRetentionMs(deleteRetentionMs)

	bestPD.RLock()
	cleanedUpTo := bestPD.CleanedUpTo()
	bestPD.RUnlock()

	newWatermark, err := compactor.CompactPartition(
		ctx,
		bestTD.Name,
		bestTD.ID,
		bestPD.Index,
		cleanedUpTo,
		minCompactionLagMs,
		func() { bestPD.CompactionMu.Lock() },
		func() { bestPD.CompactionMu.Unlock() },
	)
	if err != nil {
		b.logger.Warn("compaction failed",
			"topic", bestTD.Name, "partition", bestPD.Index, "err", err)
		return
	}

	if newWatermark > cleanedUpTo {
		bestPD.Lock()
		bestPD.SetCleanedUpTo(newWatermark)
		bestPD.ResetDirtyObjects(b.cfg.Clock.Now())
		bestPD.Unlock()
		b.logger.Info("compaction complete",
			"topic", bestTD.Name, "partition", bestPD.Index,
			"cleaned_up_to", newWatermark)
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
