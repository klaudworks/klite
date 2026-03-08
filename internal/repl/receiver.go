package repl

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"

	"github.com/klaudworks/klite/internal/wal"
)

// WALAppender is the interface used by the receiver to write replicated WAL entries.
type WALAppender interface {
	// AppendReplicated writes an already-serialized WAL entry (from the primary)
	// without assigning a new sequence number. Returns entry metadata and true
	// if written, or zero info and false if skipped (duplicate).
	AppendReplicated(serialized []byte) (info wal.ReplicatedEntryInfo, written bool, err error)

	// Sync fsyncs the current WAL segment.
	Sync() error

	// NextSequence returns the next WAL sequence number.
	NextSequence() uint64

	// SetNextSequence updates the next sequence counter.
	SetNextSequence(seq uint64)

	// SetS3FlushWatermark sets the S3 flush watermark on the WAL writer.
	SetS3FlushWatermark(seq uint64)
}

// MetaAppender is the interface used by the receiver to handle metadata entries.
type MetaAppender interface {
	// ReplayEntry processes a single framed metadata entry (CRC + type + data).
	ReplayEntry(frame []byte) error

	// AppendRaw writes raw framed bytes to the metadata.log file.
	AppendRaw(frame []byte) error

	// ReplaceFromSnapshot replaces the metadata.log file with snapshot contents
	// and replays it to rebuild in-memory state.
	ReplaceFromSnapshot(data []byte) error
}

// HWAdvancer is called by the receiver to advance the high watermark on a
// partition as replicated WAL entries arrive. This keeps the standby's HW
// up to date so that promotion does not require an S3 probe.
type HWAdvancer func(topicID [16]byte, partition int32, endOffset int64)

// Receiver connects to the primary, receives WAL and metadata entries,
// and replays them locally. Used by the standby side.
type Receiver struct {
	walAppender  WALAppender
	metaAppender MetaAppender
	hwAdvancer   HWAdvancer
	logger       *slog.Logger
	epoch        uint64 // current lease epoch
}

// NewReceiver creates a Receiver.
func NewReceiver(walAppender WALAppender, metaAppender MetaAppender, hwAdvancer HWAdvancer, epoch uint64, logger *slog.Logger) *Receiver {
	if logger == nil {
		logger = slog.Default()
	}
	return &Receiver{
		walAppender:  walAppender,
		metaAppender: metaAppender,
		hwAdvancer:   hwAdvancer,
		logger:       logger,
		epoch:        epoch,
	}
}

// SetEpoch updates the epoch used in HELLO messages.
func (r *Receiver) SetEpoch(epoch uint64) {
	r.epoch = epoch
}

// Run connects to the primary, sends HELLO, and processes the replication
// stream. Blocks until ctx is cancelled or the connection drops.
func (r *Receiver) Run(ctx context.Context, primaryAddr string, tlsConfig *tls.Config) error {
	var conn net.Conn
	var err error

	if tlsConfig != nil {
		dialer := &tls.Dialer{Config: tlsConfig}
		conn, err = dialer.DialContext(ctx, "tcp", primaryAddr)
	} else {
		var d net.Dialer
		conn, err = d.DialContext(ctx, "tcp", primaryAddr)
	}
	if err != nil {
		return fmt.Errorf("repl receiver: dial %s: %w", primaryAddr, err)
	}
	defer conn.Close() //nolint:errcheck

	return r.RunOnConn(ctx, conn)
}

// RunOnConn processes the replication stream on an already-established
// connection. Sends HELLO and then reads messages in a loop.
func (r *Receiver) RunOnConn(ctx context.Context, conn net.Conn) error {
	lastWALSeq := r.walAppender.NextSequence()
	if lastWALSeq > 0 {
		lastWALSeq--
	}

	hello := MarshalHello(lastWALSeq, r.epoch)
	if err := WriteFrame(conn, MsgHello, hello); err != nil {
		return fmt.Errorf("repl receiver: send HELLO: %w", err)
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- r.readLoop(conn)
	}()

	select {
	case <-ctx.Done():
		_ = conn.Close()
		<-errCh
		return ctx.Err()
	case err := <-errCh:
		return err
	}
}

func (r *Receiver) readLoop(conn net.Conn) error {
	for {
		msgType, payload, err := ReadFrame(conn)
		if err != nil {
			return fmt.Errorf("repl receiver: read frame: %w", err)
		}

		switch msgType {
		case MsgSnapshot:
			if err := r.handleSnapshot(payload); err != nil {
				return fmt.Errorf("repl receiver: handle snapshot: %w", err)
			}

		case MsgWALBatch:
			if err := r.handleWALBatch(payload, conn); err != nil {
				return fmt.Errorf("repl receiver: handle WAL batch: %w", err)
			}

		case MsgMetaEntry:
			if err := r.handleMetaEntry(payload); err != nil {
				r.logger.Warn("repl receiver: meta entry error", "err", err)
			}

		default:
			r.logger.Warn("repl receiver: unknown message type, skipping", "type", msgType)
		}
	}
}

func (r *Receiver) handleSnapshot(payload []byte) error {
	walSeqAfter, metadataLog, err := UnmarshalSnapshot(payload)
	if err != nil {
		return err
	}

	if err := r.metaAppender.ReplaceFromSnapshot(metadataLog); err != nil {
		return fmt.Errorf("replace metadata from snapshot: %w", err)
	}

	r.walAppender.SetNextSequence(walSeqAfter)

	r.logger.Info("repl receiver: snapshot applied",
		"wal_seq_after", walSeqAfter,
		"metadata_size", len(metadataLog))

	return nil
}

func (r *Receiver) handleWALBatch(payload []byte, conn net.Conn) error {
	firstSeq, lastSeq, entryCount, s3FlushWatermark, entries, err := UnmarshalWALBatch(payload)
	if err != nil {
		return err
	}

	// Apply the S3 flush watermark from the primary.
	if s3FlushWatermark > 0 {
		r.walAppender.SetS3FlushWatermark(s3FlushWatermark)
	}

	// Keepalive: empty batch, no ACK
	if entryCount == 0 && firstSeq == 0 && lastSeq == 0 {
		return nil
	}

	// Split and write entries using length-prefix scanning
	written := 0
	offset := 0
	for offset < len(entries) {
		if offset+4 > len(entries) {
			return fmt.Errorf("truncated entry at offset %d", offset)
		}
		entryLen := int(entries[offset])<<24 | int(entries[offset+1])<<16 |
			int(entries[offset+2])<<8 | int(entries[offset+3])
		totalLen := 4 + entryLen // length prefix + payload

		if offset+totalLen > len(entries) {
			return fmt.Errorf("entry extends past batch at offset %d", offset)
		}

		serialized := entries[offset : offset+totalLen]
		info, ok, err := r.walAppender.AppendReplicated(serialized)
		if err != nil {
			return fmt.Errorf("append replicated entry: %w", err)
		}
		if ok {
			written++
			if r.hwAdvancer != nil && info.EndOffset > 0 {
				r.hwAdvancer(info.TopicID, info.Partition, info.EndOffset)
			}
		}

		offset += totalLen
	}

	// Fsync
	if written > 0 {
		if err := r.walAppender.Sync(); err != nil {
			return fmt.Errorf("fsync replicated entries: %w", err)
		}
	}

	// Update next sequence
	if lastSeq > 0 {
		nextSeq := lastSeq + 1
		if nextSeq > r.walAppender.NextSequence() {
			r.walAppender.SetNextSequence(nextSeq)
		}
	}

	// Send ACK
	ack := MarshalACK(lastSeq)
	if err := WriteFrame(conn, MsgACK, ack); err != nil {
		return fmt.Errorf("send ACK: %w", err)
	}

	r.logger.Debug("repl receiver: WAL batch processed",
		"first_seq", firstSeq, "last_seq", lastSeq,
		"entries", entryCount, "written", written)

	return nil
}

func (r *Receiver) handleMetaEntry(payload []byte) error {
	if err := r.metaAppender.AppendRaw(payload); err != nil {
		return fmt.Errorf("append raw meta: %w", err)
	}

	if err := r.metaAppender.ReplayEntry(payload); err != nil {
		return fmt.Errorf("replay meta entry: %w", err)
	}

	return nil
}
