package broker

import (
	"context"
	crypto_tls "crypto/tls"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/klaudworks/klite/internal/lease"
	"github.com/klaudworks/klite/internal/lease/s3lease"
	"github.com/klaudworks/klite/internal/repl"
	s3store "github.com/klaudworks/klite/internal/s3"
)

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
		b.elector = b.cfg.LeaseElector
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
	b.stepHook("stop-receiver")
	b.stopReceiver()

	// Rebuild high watermarks from WAL index. The receiver advances HW as
	// entries arrive, but WAL replay at startup does not set HW for entries
	// that predate the receiver (e.g. local WAL from a previous primary tenure).
	b.stepHook("rebuild-hw")
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
		b.stepHook("probe-s3")
		// The listing cache was populated at startup and is stale — the old
		// primary flushed additional objects while this node was a standby.
		b.s3Reader.InvalidateAll()
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
			entries := b.walIndex.PartitionEntries(tp)
			walMax := b.walIndex.MaxOffset(tp)
			b.logger.Info("promotion: partition state",
				"topic", td.Name, "partition", tp.Partition,
				"hw", hw, "s3_watermark", s3wm, "wal_max_offset", walMax,
				"wal_entries", len(entries))

			// Check for overlapping/duplicate entries (different segments
			// covering the same offset range).
			var overlaps int
			var lastSeg uint64
			var segTransitions int
			for i := 1; i < len(entries); i++ {
				if entries[i].SegmentSeq != entries[i-1].SegmentSeq {
					segTransitions++
					// Log if we go backwards in segment seq (old tenure interleaved).
					if entries[i].SegmentSeq < entries[i-1].SegmentSeq {
						if overlaps == 0 {
							b.logger.Warn("promotion: WAL index segment regression",
								"topic", td.Name, "partition", tp.Partition,
								"idx", i,
								"prev_offset", entries[i-1].BaseOffset,
								"prev_seg", entries[i-1].SegmentSeq,
								"curr_offset", entries[i].BaseOffset,
								"curr_seg", entries[i].SegmentSeq)
						}
						overlaps++
					}
				}
				if entries[i].BaseOffset <= entries[i-1].BaseOffset+int64(entries[i-1].LastOffset-entries[i-1].BaseOffset) {
					if overlaps == 0 {
						b.logger.Warn("promotion: WAL index offset overlap",
							"topic", td.Name, "partition", tp.Partition,
							"idx", i,
							"prev_base", entries[i-1].BaseOffset,
							"prev_last", entries[i-1].LastOffset,
							"prev_seg", entries[i-1].SegmentSeq,
							"curr_base", entries[i].BaseOffset,
							"curr_seg", entries[i].SegmentSeq)
					}
					overlaps++
				}
				lastSeg = entries[i].SegmentSeq
			}
			if overlaps > 0 || segTransitions > 0 {
				b.logger.Warn("promotion: WAL index analysis",
					"topic", td.Name, "partition", tp.Partition,
					"overlaps", overlaps, "seg_transitions", segTransitions,
					"first_seg", entries[0].SegmentSeq, "last_seg", lastSeg,
					"entries", len(entries))
			}
		}
	}

	// Rebuild chunks from WAL entries received during the standby phase.
	// The replication receiver writes WAL entries to disk but does NOT
	// populate in-memory chunks. Without this step, the S3 flusher would
	// only flush chunk data (from startup replay + new produces), advancing
	// the S3 watermark past replicated-but-unchunked data. When WAL segments
	// are then cleaned, that data is permanently lost.
	b.stepHook("rebuild-chunks")
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
	b.stepHook("abort-orphan-txns")
	b.abortOrphanedTransactions()

	// Set up replication sender: start replication listener
	b.stepHook("start-repl-listener")
	b.startReplicationListener(primaryCtx)

	// Start Kafka listener
	b.stepHook("start-kafka-listener")
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
	b.stepHook("start-background")
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
	b.stepHook("close-listener")
	if b.listener != nil {
		_ = b.listener.Close()
		b.listener = nil
	}

	// 2. Close existing client connections. klite is a single-primary-per-cluster
	// broker, so on demotion zero partitions remain on this node — there's
	// nothing useful a client can do on this connection. This also stops
	// readLoops from dispatching new Fetch handlers.
	b.stepHook("close-conns")
	b.server.CloseConns()

	// 3. Wake all Fetch long-poll waiters so in-flight Fetch handlers return
	// immediately. This MUST happen after CloseConns — otherwise the Fetch
	// handler returns, the client sends a new Fetch, and the readLoop
	// dispatches a new handler with a fresh waiter that missed the wake.
	b.stepHook("wake-fetch-waiters")
	b.state.WakeAllFetchWaiters()

	// 4. Drain in-flight requests
	b.stepHook("drain-requests")
	b.server.Wait()
	b.state.StopAllGroups()

	// 5. Clear WAL Replicator
	b.stepHook("clear-replicator")
	if b.walWriter != nil {
		b.walWriter.SetReplicator(nil)
	}

	// 6. Clear metadata.log hooks
	b.stepHook("clear-metalog")
	if b.metaLog != nil {
		b.metaLog.SetReplicateHook(nil)
		b.metaLog.SetCompactHook(nil)
	}

	// 7. Close replication listener and sender
	b.stepHook("close-repl")
	if b.replListener != nil {
		_ = b.replListener.Close()
		b.replListener = nil
	}
	if b.replSender != nil {
		b.replSender.Close()
		b.replSender = nil
	}

	// 8. Stop S3 flusher (final flush)
	b.stepHook("stop-s3-flusher")
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
	s3api := b.cfg.S3API
	if s3api == nil {
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
