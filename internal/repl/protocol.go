package repl

import (
	"encoding/binary"
	"fmt"
	"io"
)

// Message types for the replication protocol.
const (
	MsgHello     byte = 0x01 // standby → primary: handshake with last WAL sequence
	MsgSnapshot  byte = 0x02 // primary → standby: full metadata.log for initial sync
	MsgWALBatch  byte = 0x03 // primary → standby: one fsync batch of WAL entries
	MsgMetaEntry byte = 0x04 // primary → standby: one metadata.log entry (fire-and-forget)
	MsgACK       byte = 0x05 // standby → primary: fsync confirmation with WAL sequence
)

const (
	frameHeaderSize = 5                 // 1 byte type + 4 bytes payload length
	maxPayloadSize  = 256 * 1024 * 1024 // 256 MiB
)

// WriteFrame writes a framed message: [1B type][4B payload len][payload].
func WriteFrame(w io.Writer, msgType byte, payload []byte) error {
	if len(payload) > maxPayloadSize {
		return fmt.Errorf("repl: payload size %d exceeds maximum %d", len(payload), maxPayloadSize)
	}

	var hdr [frameHeaderSize]byte
	hdr[0] = msgType
	binary.BigEndian.PutUint32(hdr[1:5], uint32(len(payload)))

	if _, err := w.Write(hdr[:]); err != nil {
		return fmt.Errorf("repl: write header: %w", err)
	}
	if len(payload) > 0 {
		if _, err := w.Write(payload); err != nil {
			return fmt.Errorf("repl: write payload: %w", err)
		}
	}
	return nil
}

// ReadFrame reads a framed message. Returns message type and payload.
func ReadFrame(r io.Reader) (msgType byte, payload []byte, err error) {
	var hdr [frameHeaderSize]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return 0, nil, err
	}

	msgType = hdr[0]
	payloadLen := binary.BigEndian.Uint32(hdr[1:5])

	if payloadLen > maxPayloadSize {
		return 0, nil, fmt.Errorf("repl: payload length %d exceeds maximum %d", payloadLen, maxPayloadSize)
	}

	payload = make([]byte, payloadLen)
	if payloadLen > 0 {
		if _, err := io.ReadFull(r, payload); err != nil {
			return 0, nil, fmt.Errorf("repl: read payload: %w", err)
		}
	}

	return msgType, payload, nil
}

// --- HELLO message ---

// MarshalHello encodes a HELLO payload: [8B lastWALSeq][8B epoch].
func MarshalHello(lastWALSeq, epoch uint64) []byte {
	buf := make([]byte, 16)
	binary.BigEndian.PutUint64(buf[0:8], lastWALSeq)
	binary.BigEndian.PutUint64(buf[8:16], epoch)
	return buf
}

// UnmarshalHello decodes a HELLO payload.
func UnmarshalHello(payload []byte) (lastWALSeq, epoch uint64, err error) {
	if len(payload) < 16 {
		return 0, 0, fmt.Errorf("repl: HELLO payload too short: %d", len(payload))
	}
	lastWALSeq = binary.BigEndian.Uint64(payload[0:8])
	epoch = binary.BigEndian.Uint64(payload[8:16])
	return lastWALSeq, epoch, nil
}

// --- SNAPSHOT message ---

// MarshalSnapshot encodes a SNAPSHOT payload.
func MarshalSnapshot(walSeqAfter uint64, metadataLog []byte) []byte {
	buf := make([]byte, 8+4+len(metadataLog))
	binary.BigEndian.PutUint64(buf[0:8], walSeqAfter)
	binary.BigEndian.PutUint32(buf[8:12], uint32(len(metadataLog)))
	copy(buf[12:], metadataLog)
	return buf
}

// UnmarshalSnapshot decodes a SNAPSHOT payload.
func UnmarshalSnapshot(payload []byte) (walSeqAfter uint64, metadataLog []byte, err error) {
	if len(payload) < 12 {
		return 0, nil, fmt.Errorf("repl: SNAPSHOT payload too short: %d", len(payload))
	}
	walSeqAfter = binary.BigEndian.Uint64(payload[0:8])
	metaLen := binary.BigEndian.Uint32(payload[8:12])
	if int(metaLen) > len(payload)-12 {
		return 0, nil, fmt.Errorf("repl: SNAPSHOT metadata length %d exceeds payload", metaLen)
	}
	metadataLog = make([]byte, metaLen)
	copy(metadataLog, payload[12:12+metaLen])
	return walSeqAfter, metadataLog, nil
}

// --- WAL_BATCH message ---

// MarshalWALBatch encodes a WAL_BATCH payload.
func MarshalWALBatch(firstSeq, lastSeq uint64, entryCount uint32, entries []byte) []byte {
	buf := make([]byte, 8+8+4+len(entries))
	binary.BigEndian.PutUint64(buf[0:8], firstSeq)
	binary.BigEndian.PutUint64(buf[8:16], lastSeq)
	binary.BigEndian.PutUint32(buf[16:20], entryCount)
	copy(buf[20:], entries)
	return buf
}

// UnmarshalWALBatch decodes a WAL_BATCH payload.
func UnmarshalWALBatch(payload []byte) (firstSeq, lastSeq uint64, entryCount uint32, entries []byte, err error) {
	if len(payload) < 20 {
		return 0, 0, 0, nil, fmt.Errorf("repl: WAL_BATCH payload too short: %d", len(payload))
	}
	firstSeq = binary.BigEndian.Uint64(payload[0:8])
	lastSeq = binary.BigEndian.Uint64(payload[8:16])
	entryCount = binary.BigEndian.Uint32(payload[16:20])
	if len(payload) > 20 {
		entries = make([]byte, len(payload)-20)
		copy(entries, payload[20:])
	}
	return firstSeq, lastSeq, entryCount, entries, nil
}

// --- ACK message ---

// MarshalACK encodes an ACK payload: [8B walSequence].
func MarshalACK(walSequence uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf[0:8], walSequence)
	return buf
}

// UnmarshalACK decodes an ACK payload.
func UnmarshalACK(payload []byte) (walSequence uint64, err error) {
	if len(payload) < 8 {
		return 0, fmt.Errorf("repl: ACK payload too short: %d", len(payload))
	}
	walSequence = binary.BigEndian.Uint64(payload[0:8])
	return walSequence, nil
}
