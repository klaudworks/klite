package broker

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
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
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/klaudworks/klite/internal/chunk"
	"github.com/klaudworks/klite/internal/cluster"
	"github.com/klaudworks/klite/internal/handler"
	"github.com/klaudworks/klite/internal/metadata"
	s3store "github.com/klaudworks/klite/internal/s3"
	"github.com/klaudworks/klite/internal/sasl"
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
	walWriter  *wal.Writer      // nil if WAL disabled
	walIndex   *wal.Index       // nil if WAL disabled
	chunkPool  *chunk.Pool      // nil if WAL disabled
	metaLog    *metadata.Log    // nil if metadata persistence disabled
	s3Client   *s3store.Client  // nil if S3 disabled
	s3Reader   *s3store.Reader  // nil if S3 disabled
	s3Flusher  *s3store.Flusher // nil if S3 disabled
	saslStore  *sasl.Store      // nil if SASL disabled

	mu   sync.Mutex
	quit bool
}

// New creates a new Broker with the given config.
func New(cfg Config) *Broker {
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

	// Initialize SASL if enabled
	if cfg.SASLEnabled {
		b.initSASLStore()
		srv.SetSASLEnabled(true)
	}

	b.registerBaseHandlers()
	return b
}

// initSASLStore creates and populates the SASL credential store.
func (b *Broker) initSASLStore() {
	if b.cfg.SASLStore != nil {
		b.saslStore = b.cfg.SASLStore.(*sasl.Store)
		return
	}
	b.saslStore = sasl.NewStore()

	// Add CLI-flag user
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

// registerBaseHandlers wires up handlers that don't depend on runtime state.
func (b *Broker) registerBaseHandlers() {
	b.handlers.Register(18, handler.HandleApiVersions())

	// SASL handlers are registered early (before runtime handlers)
	// because they only depend on the SASL store, not on cluster state.
	if b.saslStore != nil {
		b.handlers.RegisterConn(17, handler.HandleSASLHandshake())
		b.handlers.RegisterConn(36, handler.HandleSASLAuthenticate(b.saslStore))
	}
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

	// Phase 5: SASL credential management
	if b.saslStore != nil {
		b.handlers.Register(50, handler.HandleDescribeUserScramCredentials(b.saslStore))
		b.handlers.Register(51, handler.HandleAlterUserScramCredentials(b.saslStore, b.metaLog))
	}

	// Phase 4: Transactions
	b.handlers.Register(22, handler.HandleInitProducerID(b.state))
	b.handlers.Register(24, handler.HandleAddPartitionsToTxn(b.state))
	b.handlers.Register(25, handler.HandleAddOffsetsToTxn(b.state))
	b.handlers.Register(26, handler.HandleEndTxn(b.state))
	b.handlers.Register(28, handler.HandleTxnOffsetCommit(b.state))
	b.handlers.Register(61, handler.HandleDescribeProducers(b.state))
	b.handlers.Register(65, handler.HandleDescribeTransactions(b.state))
	b.handlers.Register(66, handler.HandleListTransactions(b.state))
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

	// 2b. If S3 is configured, attempt disaster recovery if needed
	if b.cfg.S3Bucket != "" {
		if err := b.maybeRecoverFromS3(); err != nil {
			b.logger.Warn("S3 disaster recovery failed, continuing without", "err", err)
		}
	}

	// 3. Initialize metadata.log (replay metadata first)
	if err := b.initMetadataLog(); err != nil {
		return fmt.Errorf("init metadata.log: %w", err)
	}
	// After metadata.log replay (which may have loaded persisted SCRAM
	// credentials), re-apply CLI-flag user so config always wins.
	if b.saslStore != nil && b.cfg.SASLUser != "" && b.cfg.SASLPassword != "" {
		b.applyCLISASLUser()
	}

	// Validate SASL: if enabled but no credentials configured, refuse to start
	if b.cfg.SASLEnabled && b.saslStore != nil && b.saslStore.Empty() {
		return fmt.Errorf("SASL is enabled but no credentials are configured; add users via --sasl-user/--sasl-password or config file")
	}

	// 4. Initialize WAL (replay WAL after metadata)
	if err := b.initWAL(); err != nil {
		return fmt.Errorf("init WAL: %w", err)
	}

	// 4b. Initialize S3 if configured
	if b.cfg.S3Bucket != "" {
		if err := b.initS3(); err != nil {
			return fmt.Errorf("init S3: %w", err)
		}
	} else {
		b.logger.Warn("no S3 bucket configured; data is stored only in the local WAL — this is suitable for demos but data loss will occur when the WAL exceeds --wal-max-disk-size and old segments are deleted, or if the disk is lost")
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
	)

	// Signal that initialization is complete
	close(b.ready)

	// 8a. Start HTTP health server if configured (opt-in via --health-addr)
	var healthShutdown func()
	if b.cfg.HealthAddr != "" || b.cfg.HealthListener != nil {
		healthShutdown, err = b.startHealthServer()
		if err != nil {
			return fmt.Errorf("health server: %w", err)
		}
	}

	// 8b. Start retention loop
	go b.retentionLoop(ctx)

	// 8c. Start compaction loop (only if S3 is configured)
	if b.s3Client != nil {
		go b.compactionLoop(ctx)
		go b.s3GCLoop(ctx)
	}

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
	if healthShutdown != nil {
		healthShutdown()
	}

	// 11. Wait for Serve() to return so no new connections are accepted
	if !serveReturned {
		<-errCh
	}

	// 12. Close chunk pool to unblock any producers stuck in Acquire
	if b.chunkPool != nil {
		b.chunkPool.Close()
	}

	// 13. Wait for all connections to drain
	b.server.Wait()

	// 14. Stop all group goroutines
	b.state.StopAllGroups()

	// 15. Run unified S3 sync (flush all partitions + upload metadata.log)
	if b.s3Flusher != nil {
		b.s3Flusher.Stop() // Stop triggers a final flush
	}

	// 16. Stop WAL writer (flush pending writes)
	if b.walWriter != nil {
		b.walWriter.Stop()
	}

	// 17. Close metadata log
	if b.metaLog != nil {
		_ = b.metaLog.Close()
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
			b.state.PIDManager().SetNextPID(e.NextProducerID)
		},
		// LOG_START_OFFSET
		func(e metadata.LogStartOffsetEntry) {
			b.state.SetLogStartOffsetFromReplay(e.TopicName, e.Partition, e.LogStartOffset)
		},
	)

	// Compaction watermark replay callback
	ml.SetCompactionWatermarkCallback(func(e metadata.CompactionWatermarkEntry) {
		b.state.SetCompactionWatermarkFromReplay(e.TopicName, e.Partition, e.CleanedUpTo)
	})

	// Set up SCRAM credential replay callbacks if SASL is enabled
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

	// Replay existing entries
	count, err := ml.Replay()
	if err != nil {
		_ = ml.Close()
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

	// Replay existing WAL entries to rebuild state
	if err := b.replayWAL(w); err != nil {
		return fmt.Errorf("replay WAL: %w", err)
	}

	// Start the writer goroutine (after replay, so no concurrent writes during rebuild)
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

		// Pre-acquire spare chunk before taking the partition lock.
		spare := pd.AcquireSpareChunk(len(entry.Data))

		// Rebuild partition state: append to chunk pool and advance HW.
		pd.Lock()

		// Advance HW if needed (WAL can only advance, never reduce)
		endOffset := lastOffset + 1
		if endOffset > pd.HW() {
			pd.SetHW(endOffset)
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

// initS3 initializes the S3 storage layer.
func (b *Broker) initS3() error {
	var s3api s3store.S3API
	if b.cfg.S3API != nil {
		var ok bool
		s3api, ok = b.cfg.S3API.(s3store.S3API)
		if !ok {
			return fmt.Errorf("S3API config field must implement s3.S3API interface")
		}
	} else {
		// Create a real AWS S3 client
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

	// Set S3 fetcher on all existing partitions
	fetchAdapter := &s3store.ReaderAdapter{Reader: b.s3Reader}
	b.state.SetS3Fetcher(fetchAdapter)

	// Set up the flusher with per-partition thresholds
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

	// Create triggerCh for emergency flush signaling from chunk pool pressure
	triggerCh := make(chan struct{}, 1)
	if b.chunkPool != nil {
		b.chunkPool.SetTriggerCh(triggerCh)
	}

	flusherCfg := s3store.FlusherConfig{
		Client:            b.s3Client,
		WALWriter:         b.walWriter,
		WALIndex:          b.walIndex,
		FlushInterval:     flushInterval,
		CheckInterval:     checkInterval,
		TargetObjectSize:  targetObjSize,
		UploadConcurrency: 8,
		Logger:            b.logger,
		TriggerCh:         triggerCh,
		MetadataUploader:  b.uploadMetadataLog,
		Reader:            b.s3Reader,
	}

	partAdapter := &s3PartitionAdapter{
		state: b.state,
	}
	b.s3Flusher = s3store.NewFlusher(flusherCfg, partAdapter)
	b.s3Flusher.Start()

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

// uploadMetadataLog uploads the metadata.log file to S3.
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

	// Write metadata.log to disk
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
		policy, ok := td.Configs["cleanup.policy"]
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
// and updates the cluster state. For partitions with no WAL data (disaster
// recovery), both HW and S3 flush watermark are set from S3. For partitions
// with WAL data, only the S3 flush watermark is set to avoid re-flushing
// objects that already exist in S3.
func (b *Broker) probeS3Watermarks() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	topics := b.state.GetAllTopics()
	updated := 0
	for _, td := range topics {
		for _, pd := range td.Partitions {
			pd.RLock()
			hw := pd.HW()
			flushWM := pd.S3FlushWatermark()
			pd.RUnlock()

			if flushWM > 0 {
				continue // already has S3 flush watermark
			}

			s3HW, err := b.s3Reader.DiscoverHW(ctx, td.Name, td.ID, pd.Index)
			if err != nil {
				b.logger.Debug("S3 HW probe failed", "topic", td.Name, "partition", pd.Index, "err", err)
				continue
			}
			if s3HW > 0 {
				pd.Lock()
				if hw == 0 {
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

// createAWSS3Client creates an AWS S3 client from broker config.
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

// s3PartitionAdapter adapts the cluster state to the s3.PartitionProvider interface.
type s3PartitionAdapter struct {
	state *cluster.State
}

func (a *s3PartitionAdapter) FlushablePartitions() []s3store.FlushPartition {
	parts := a.state.FlushablePartitions()
	var result []s3store.FlushPartition

	for _, fp := range parts {
		pd := fp.Partition_ // captured partition reference

		// Read chunk state under RLock
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
			AdvanceWatermark: func(newWatermark int64) {
				pd.Lock()
				pd.SetS3FlushWatermark(newWatermark)
				pd.IncrementDirtyObjects()
				pd.Unlock()

				// Prune WAL index entries for flushed data
				if walIdx := pd.WalIndex(); walIdx != nil {
					tp := wal.TopicPartition{TopicID: fp.TopicID, Partition: fp.Partition}
					walIdx.PruneBefore(tp, newWatermark)
				}
			},
		})
	}

	return result
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
		_ = b.listener.Close()
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

// applyCLISASLUser re-applies the CLI-specified SASL user to override
// any persisted credentials for the same user.
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

// scramMechName converts a mechanism number to a name string.
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

// compactionLoop runs the background compaction goroutine.
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

	ticker := time.NewTicker(interval)
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

// compactOneDirtyPartition finds the most eligible partition and compacts it.
func (b *Broker) compactOneDirtyPartition(ctx context.Context, compactor *s3store.Compactor, minDirty int32) {
	topics := b.state.GetAllTopics()

	var bestPD *cluster.PartData
	var bestTD *cluster.TopicData
	var bestDirty int32

	for _, td := range topics {
		policy, ok := td.Configs["cleanup.policy"]
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
					if v, ok := td.Configs["max.compaction.lag.ms"]; ok {
						if parsed, err := strconv.ParseInt(v, 10, 64); err == nil {
							maxLagMs = parsed
						}
					}
					if maxLagMs < 9223372036854775807 && time.Since(lc).Milliseconds() > maxLagMs && dirty > 0 {
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

	// Determine topic-level configs
	minCompactionLagMs := int64(0)
	if v, ok := bestTD.Configs["min.compaction.lag.ms"]; ok {
		if parsed, err := strconv.ParseInt(v, 10, 64); err == nil {
			minCompactionLagMs = parsed
		}
	}

	deleteRetentionMs := int64(86400000) // 24h default
	if v, ok := bestTD.Configs["delete.retention.ms"]; ok {
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
		bestPD.ResetDirtyObjects(time.Now())
		bestPD.Unlock()
		b.logger.Info("compaction complete",
			"topic", bestTD.Name, "partition", bestPD.Index,
			"cleaned_up_to", newWatermark)
	}
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
