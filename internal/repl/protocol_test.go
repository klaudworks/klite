package repl

import (
	"bytes"
	"io"
	"testing"
)

func TestFrameRoundTrip(t *testing.T) {
	tests := []struct {
		name    string
		msgType byte
		payload []byte
	}{
		{"HELLO", MsgHello, MarshalHello(42, 7)},
		{"SNAPSHOT", MsgSnapshot, MarshalSnapshot(100, []byte("metadata-contents"))},
		{"WAL_BATCH", MsgWALBatch, MarshalWALBatch(1, 5, 3, []byte("wal-entries"))},
		{"META_ENTRY", MsgMetaEntry, []byte("meta-entry-frame")},
		{"ACK", MsgACK, MarshalACK(99)},
		{"empty payload", MsgHello, []byte{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer

			if err := WriteFrame(&buf, tt.msgType, tt.payload); err != nil {
				t.Fatalf("WriteFrame: %v", err)
			}

			gotType, gotPayload, err := ReadFrame(&buf)
			if err != nil {
				t.Fatalf("ReadFrame: %v", err)
			}

			if gotType != tt.msgType {
				t.Errorf("type: got %#x, want %#x", gotType, tt.msgType)
			}

			if !bytes.Equal(gotPayload, tt.payload) {
				t.Errorf("payload: got %q, want %q", gotPayload, tt.payload)
			}
		})
	}
}

func TestFrameMaxPayload(t *testing.T) {
	// Encoding at the limit should succeed
	atLimit := make([]byte, maxPayloadSize)
	var buf bytes.Buffer
	if err := WriteFrame(&buf, MsgWALBatch, atLimit); err != nil {
		t.Fatalf("WriteFrame at limit: %v", err)
	}

	// Encoding above the limit should fail
	overLimit := make([]byte, maxPayloadSize+1)
	if err := WriteFrame(&buf, MsgWALBatch, overLimit); err == nil {
		t.Fatal("WriteFrame over limit: expected error")
	}

	// Decoding a frame that claims > 256 MiB should fail
	var fakeBuf bytes.Buffer
	fakeBuf.WriteByte(MsgWALBatch)
	// Write a length that's too big
	lenBytes := []byte{0x10, 0x00, 0x00, 0x01} // 256 MiB + 1
	fakeBuf.Write(lenBytes)
	_, _, err := ReadFrame(&fakeBuf)
	if err == nil {
		t.Fatal("ReadFrame with oversized length: expected error")
	}
}

func TestFrameTruncated(t *testing.T) {
	// Write a valid frame
	var buf bytes.Buffer
	payload := []byte("hello world")
	if err := WriteFrame(&buf, MsgHello, payload); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}

	full := buf.Bytes()

	// Truncate mid-header
	_, _, err := ReadFrame(bytes.NewReader(full[:3]))
	if err == nil {
		t.Fatal("expected error for truncated header")
	}

	// Truncate mid-payload
	_, _, err = ReadFrame(bytes.NewReader(full[:8]))
	if err == nil {
		t.Fatal("expected error for truncated payload")
	}

	// Empty reader
	_, _, err = ReadFrame(bytes.NewReader(nil))
	if err != io.EOF {
		t.Fatalf("expected io.EOF for empty reader, got %v", err)
	}
}

func TestHelloRoundTrip(t *testing.T) {
	payload := MarshalHello(42, 7)
	seq, epoch, err := UnmarshalHello(payload)
	if err != nil {
		t.Fatal(err)
	}
	if seq != 42 {
		t.Errorf("lastWALSeq: got %d, want 42", seq)
	}
	if epoch != 7 {
		t.Errorf("epoch: got %d, want 7", epoch)
	}
}

func TestSnapshotRoundTrip(t *testing.T) {
	meta := []byte("snapshot metadata contents")
	payload := MarshalSnapshot(100, meta)
	seq, gotMeta, err := UnmarshalSnapshot(payload)
	if err != nil {
		t.Fatal(err)
	}
	if seq != 100 {
		t.Errorf("walSeqAfter: got %d, want 100", seq)
	}
	if !bytes.Equal(gotMeta, meta) {
		t.Errorf("metadata: got %q, want %q", gotMeta, meta)
	}
}

func TestWALBatchRoundTrip(t *testing.T) {
	entries := []byte("concatenated-wal-entries")
	payload := MarshalWALBatch(5, 10, 3, entries)
	first, last, count, gotEntries, err := UnmarshalWALBatch(payload)
	if err != nil {
		t.Fatal(err)
	}
	if first != 5 || last != 10 || count != 3 {
		t.Errorf("got first=%d last=%d count=%d, want 5/10/3", first, last, count)
	}
	if !bytes.Equal(gotEntries, entries) {
		t.Errorf("entries: got %q, want %q", gotEntries, entries)
	}
}

func TestACKRoundTrip(t *testing.T) {
	payload := MarshalACK(99)
	seq, err := UnmarshalACK(payload)
	if err != nil {
		t.Fatal(err)
	}
	if seq != 99 {
		t.Errorf("walSequence: got %d, want 99", seq)
	}
}

func TestUnmarshalShortPayloads(t *testing.T) {
	_, _, err := UnmarshalHello([]byte{1, 2, 3})
	if err == nil {
		t.Error("expected error for short HELLO")
	}

	_, _, err = UnmarshalSnapshot([]byte{1, 2, 3})
	if err == nil {
		t.Error("expected error for short SNAPSHOT")
	}

	_, _, _, _, err = UnmarshalWALBatch([]byte{1, 2, 3})
	if err == nil {
		t.Error("expected error for short WAL_BATCH")
	}

	_, err = UnmarshalACK([]byte{1, 2, 3})
	if err == nil {
		t.Error("expected error for short ACK")
	}
}

func TestMultipleFramesOnWire(t *testing.T) {
	var buf bytes.Buffer

	// Write multiple frames
	if err := WriteFrame(&buf, MsgHello, MarshalHello(1, 1)); err != nil {
		t.Fatal(err)
	}
	if err := WriteFrame(&buf, MsgWALBatch, MarshalWALBatch(1, 3, 2, []byte("data"))); err != nil {
		t.Fatal(err)
	}
	if err := WriteFrame(&buf, MsgACK, MarshalACK(3)); err != nil {
		t.Fatal(err)
	}

	// Read them back in order
	msgType, _, err := ReadFrame(&buf)
	if err != nil || msgType != MsgHello {
		t.Fatalf("first: type=%#x err=%v", msgType, err)
	}

	msgType, _, err = ReadFrame(&buf)
	if err != nil || msgType != MsgWALBatch {
		t.Fatalf("second: type=%#x err=%v", msgType, err)
	}

	msgType, _, err = ReadFrame(&buf)
	if err != nil || msgType != MsgACK {
		t.Fatalf("third: type=%#x err=%v", msgType, err)
	}
}
