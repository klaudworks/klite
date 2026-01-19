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
	"time"

	"github.com/google/uuid"
	"github.com/klaudworks/klite/internal/cluster"
	"github.com/klaudworks/klite/internal/handler"
	"github.com/klaudworks/klite/internal/metadata"
	"github.com/klaudworks/klite/internal/server"
	"github.com/klaudworks/klite/internal/wal"
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
	walWriter  *wal.Writer    // nil if WAL disabled
	walIndex   *wal.Index     // nil if WAL disabled
	metaLog    *metadata.Log  // nil if metadata persistence disabled

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
	state.SetShutdownCh(shutdownCh)
	state.SetLogger(logger)

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
	b.handlers.Register(0, handler.HandleProduce(b.state, b.walWriter))
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
	b.handlers.Register(8, handler.HandleOffsetCommit(b.state))
	b.handlers.Register(9, handler.HandleOffsetFetch(b.state))
	b.handlers.Register(11, handler.HandleJoinGroup(b.state))
	b.handlers.Register(12, handler.HandleHeartbeat(b.state))
	b.handlers.Register(13, handler.HandleLeaveGroup(b.state))
	b.handlers.Register(14, handler.HandleSyncGroup(b.state))
	b.handlers.Register(47, handler.HandleOffsetDelete(b.state))

	// Phase 3: Admin APIs
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

	// 3. Initialize metadata.log if WAL is enabled (replay metadata first)
	if b.cfg.WALEnabled {
		if err := b.initMetadataLog(); err != nil {
			return fmt.Errorf("init metadata.log: %w", err)
		}
	}

	// 4. Initialize WAL if enabled (replay WAL after metadata)
	if b.cfg.WALEnabled {
		if err := b.initWAL(); err != nil {
			return fmt.Errorf("init WAL: %w", err)
		}
	}

	// 5. Start TCP listener
	ln := b.cfg.Listener
	if ln == nil {
		ln, err = net.Listen("tcp", b.cfg.Listen)
		if err != nil {
			return fmt.Errorf("listen: %w", err)
		}
	}
	b.listener = ln

	// 6. Resolve advertised address (after listener is bound so we know the real port)
	advAddr, warn := b.resolveAdvertisedAddr()
	if warn {
		b.logger.Warn("--advertised-addr not set, using derived address in Metadata responses; clients outside this host won't be able to connect",
			"advertised_addr", advAddr)
	}

	// 7. Register handlers that depend on runtime state
	b.registerRuntimeHandlers(advAddr)

	// 8. Log startup
	b.logger.Info("klite started",
		"listen", b.listener.Addr().String(),
		"advertised_addr", advAddr,
		"cluster_id", b.clusterID,
		"node_id", b.cfg.NodeID,
		"data_dir", b.cfg.DataDir,
		"wal_enabled", b.cfg.WALEnabled,
	)

	// Signal that initialization is complete
	close(b.ready)

	// 9. Serve connections (blocks until listener closed)
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

	// 10. Shutdown sequence
	b.shutdown()

	// 11. Wait for Serve() to return so no new connections are accepted
	if !serveReturned {
		<-errCh
	}

	// 12. Wait for all connections to drain
	b.server.Wait()

	// 13. Stop all group goroutines
	b.state.StopAllGroups()

	// 14. Stop WAL writer (flush pending writes)
	if b.walWriter != nil {
		b.walWriter.Stop()
	}

	// 15. Close metadata log
	if b.metaLog != nil {
		b.metaLog.Close()
	}

	b.logger.Info("klite stopped")
	return nil
}

// initMetadataLog initializes the metadata.log, sets up replay callbacks,
// replays existing entries to rebuild state, then compacts if needed.
func (b *Broker) initMetadataLog() error {
	ml, err := metadata.NewLog(metadata.LogConfig{
		DataDir: b.cfg.DataDir,
		Logger:  b.logger,
	})
	if err != nil {
		return err
	}

	// Set replay callbacks to rebuild cluster state
	ml.SetCallbacks(
		// CREATE_TOPIC
		func(e metadata.CreateTopicEntry) {
			b.state.CreateTopicFromReplay(e.TopicName, int(e.PartitionCount), e.TopicID, e.Configs)
		},
		// DELETE_TOPIC
		func(e metadata.DeleteTopicEntry) {
			b.state.DeleteTopic(e.TopicName)
		},
		// ALTER_CONFIG
		func(e metadata.AlterConfigEntry) {
			b.state.SetTopicConfig(e.TopicName, e.Key, e.Value)
		},
		// OFFSET_COMMIT
		func(e metadata.OffsetCommitEntry) {
			b.state.SetCommittedOffsetFromReplay(e.Group, e.Topic, e.Partition, e.Offset, e.Metadata)
		},
		// PRODUCER_ID (Phase 4)
		func(e metadata.ProducerIDEntry) {
			// TODO: Phase 4 — set next producer ID counter
		},
		// LOG_START_OFFSET
		func(e metadata.LogStartOffsetEntry) {
			b.state.SetLogStartOffsetFromReplay(e.TopicName, e.Partition, e.LogStartOffset)
		},
	)

	// Replay existing entries
	count, err := ml.Replay()
	if err != nil {
		ml.Close()
		return fmt.Errorf("replay metadata.log: %w", err)
	}

	if count > 0 {
		b.logger.Info("metadata.log replay complete", "entries", count)
	}

	// Set up snapshot function for compaction
	ml.SetSnapshotFn(b.state.SnapshotEntries)

	// Compact if needed (after replay, live state is in memory)
	ml.Compact()

	b.metaLog = ml
	b.state.SetMetadataLog(ml)

	return nil
}

// initWAL initializes the WAL writer and replays existing WAL segments.
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
		Logger:          b.logger,
	}

	w, err := wal.NewWriter(cfg, idx)
	if err != nil {
		return fmt.Errorf("create WAL writer: %w", err)
	}

	// Replay existing WAL entries to rebuild state
	if err := b.replayWAL(w); err != nil {
		return fmt.Errorf("replay WAL: %w", err)
	}

	// Start the writer goroutine
	if err := w.Start(); err != nil {
		return fmt.Errorf("start WAL writer: %w", err)
	}

	b.walWriter = w

	// Configure cluster state for WAL mode
	ringMem := b.cfg.RingBufferMaxMem
	if ringMem == 0 {
		ringMem = 512 * 1024 * 1024 // 512 MiB
	}
	b.state.SetWALConfig(w, idx, ringMem)

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

		// Skip WAL entries whose last offset is below the persisted logStartOffset
		lastOffset := entry.Offset + int64(meta.LastOffsetDelta)
		pd.RLock()
		logStart := pd.LogStart()
		pd.RUnlock()
		if lastOffset < logStart {
			skippedBelowLogStart++
			return nil
		}

		// Rebuild partition state
		pd.Lock()
		batch := cluster.StoredBatch{
			BaseOffset:      entry.Offset,
			LastOffsetDelta: meta.LastOffsetDelta,
			RawBytes:        entry.Data,
			MaxTimestamp:    meta.MaxTimestamp,
			NumRecords:      meta.NumRecords,
		}

		// Advance HW if needed (WAL can only advance, never reduce)
		endOffset := lastOffset + 1
		if endOffset > pd.HW() {
			pd.SetHW(endOffset)
		}

		// Advance logStartOffset if the lowest WAL offset is higher
		// than the metadata.log value (secondary authority)
		if entry.Offset > pd.LogStart() {
			// Only if no earlier entries exist in this partition
			// This is handled naturally: first WAL entry for a partition
			// sets the floor. We don't need explicit logic — the metadata.log
			// value takes precedence and WAL data below it was already skipped.
		}

		// If ring buffer is initialized, push to it
		if pd.HasWAL() {
			pd.CommitBatch(batch)
		}
		pd.Unlock()

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
