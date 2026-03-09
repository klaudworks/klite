package handler

import (
	"encoding/binary"
	"hash/crc32"
	"testing"
	"time"

	"github.com/klaudworks/klite/internal/clock"
	"github.com/klaudworks/klite/internal/cluster"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// makeValidBatch builds a minimal valid RecordBatch (61 bytes, magic=2,
// correct BatchLength, valid CRC32C). numRecords sets the NumRecords field
// and LastOffsetDelta = numRecords-1.
func makeValidBatch(numRecords int32) []byte {
	raw := make([]byte, 61)

	// BaseOffset (0:8) — left as zero, broker overwrites
	// BatchLength (8:12) — len(raw) - 12
	binary.BigEndian.PutUint32(raw[8:12], uint32(len(raw)-12))
	// PartitionLeaderEpoch (12:16) — left as zero
	// Magic (16) — must be 2
	raw[16] = 2
	// Attributes (21:23) — 0 (CreateTime, no compression)
	binary.BigEndian.PutUint16(raw[21:23], 0)
	// LastOffsetDelta (23:27)
	if numRecords > 0 {
		binary.BigEndian.PutUint32(raw[23:27], uint32(numRecords-1))
	}
	// BaseTimestamp (27:35)
	binary.BigEndian.PutUint64(raw[27:35], uint64(time.Now().UnixMilli()))
	// MaxTimestamp (35:43)
	binary.BigEndian.PutUint64(raw[35:43], uint64(time.Now().UnixMilli()))
	// ProducerID (43:51) — -1 means non-idempotent
	binary.BigEndian.PutUint64(raw[43:51], uint64(0xFFFFFFFFFFFFFFFF))
	// ProducerEpoch (51:53)
	binary.BigEndian.PutUint16(raw[51:53], 0)
	// BaseSequence (53:57) — -1
	binary.BigEndian.PutUint32(raw[53:57], 0xFFFFFFFF)
	// NumRecords (57:61)
	binary.BigEndian.PutUint32(raw[57:61], uint32(numRecords))

	// CRC32C covers bytes 21+ (attributes through end)
	crc := crc32.Checksum(raw[21:], crc32.MakeTable(crc32.Castagnoli))
	binary.BigEndian.PutUint32(raw[17:21], crc)

	return raw
}

// makeProduceReq creates a ProduceRequest with a single topic/partition.
func makeProduceReq(version, acks int16, topic string, partition int32, records []byte) *kmsg.ProduceRequest {
	r := kmsg.NewPtrProduceRequest()
	r.Version = version
	r.Acks = acks

	rt := kmsg.NewProduceRequestTopic()
	rt.Topic = topic
	rp := kmsg.NewProduceRequestTopicPartition()
	rp.Partition = partition
	rp.Records = records
	rt.Partitions = append(rt.Partitions, rp)
	r.Topics = append(r.Topics, rt)
	return r
}

// callProduce invokes the produce handler and returns the response (may be nil for acks=0).
func callProduce(t *testing.T, state *cluster.State, clk clock.Clock, req *kmsg.ProduceRequest) *kmsg.ProduceResponse {
	t.Helper()
	h := HandleProduce(state, nil, clk) // nil walWriter — all validation tests exit before WAL
	resp, err := h(req)
	if err != nil {
		t.Fatalf("handler returned error: %v", err)
	}
	if resp == nil {
		return nil
	}
	return resp.(*kmsg.ProduceResponse)
}

// partitionErrorCode extracts the error code for a given topic/partition from the response.
func partitionErrorCode(resp *kmsg.ProduceResponse, topic string, partition int32) int16 {
	for _, st := range resp.Topics {
		if st.Topic == topic {
			for _, sp := range st.Partitions {
				if sp.Partition == partition {
					return sp.ErrorCode
				}
			}
		}
	}
	return -1
}

func TestProduce_TooShortMessage(t *testing.T) {
	state := cluster.NewState(cluster.Config{
		NodeID:            0,
		DefaultPartitions: 1,
		AutoCreateTopics:  true,
	})
	clk := clock.NewFakeClock(time.Unix(1000, 0))

	// Less than 61 bytes
	for _, size := range []int{0, 1, 30, 60} {
		req := makeProduceReq(9, 1, "test-topic", 0, make([]byte, size))
		resp := callProduce(t, state, clk, req)

		code := partitionErrorCode(resp, "test-topic", 0)
		if code != kerr.CorruptMessage.Code {
			t.Errorf("size=%d: expected CorruptMessage (%d), got %d",
				size, kerr.CorruptMessage.Code, code)
		}
	}
}

func TestProduce_BatchLengthMismatch(t *testing.T) {
	state := cluster.NewState(cluster.Config{
		NodeID:            0,
		DefaultPartitions: 1,
		AutoCreateTopics:  true,
	})
	clk := clock.NewFakeClock(time.Unix(1000, 0))

	raw := makeValidBatch(1)
	// Corrupt BatchLength: set it to a wrong value
	binary.BigEndian.PutUint32(raw[8:12], 999)

	req := makeProduceReq(9, 1, "test-topic", 0, raw)
	resp := callProduce(t, state, clk, req)

	code := partitionErrorCode(resp, "test-topic", 0)
	if code != kerr.CorruptMessage.Code {
		t.Errorf("expected CorruptMessage (%d), got %d",
			kerr.CorruptMessage.Code, code)
	}
}

func TestProduce_ParseBatchHeaderError(t *testing.T) {
	state := cluster.NewState(cluster.Config{
		NodeID:            0,
		DefaultPartitions: 1,
		AutoCreateTopics:  true,
	})
	clk := clock.NewFakeClock(time.Unix(1000, 0))

	// 60 bytes — exactly below the 61-byte minimum for ParseBatchHeader,
	// but above the < 61 check because the code checks len(raw) < 61.
	// With 60 bytes, the first check (len < 61) catches it.
	// Use exactly 61 bytes but with a BatchLength that is correct (49)
	// but make the content un-parseable... Actually ParseBatchHeader
	// only checks length >= 61, so we need something that passes the
	// len(raw) < 61 check but fails ParseBatchHeader. Since
	// ParseBatchHeader also checks >= 61, these two checks are identical.
	// The parse error path is actually triggered when the first check passes
	// (len >= 61) but ParseBatchHeader still fails — which can't happen
	// because ParseBatchHeader's only error condition is len < 61.
	//
	// So the parse error path in produce.go is only reached if
	// ParseBatchHeader is changed to return other errors. For now,
	// the too-short check at len < 61 covers this.
	// Let's verify that exactly 60 bytes triggers CorruptMessage.
	raw := make([]byte, 60)
	req := makeProduceReq(9, 1, "test-topic", 0, raw)
	resp := callProduce(t, state, clk, req)

	code := partitionErrorCode(resp, "test-topic", 0)
	if code != kerr.CorruptMessage.Code {
		t.Errorf("expected CorruptMessage (%d), got %d",
			kerr.CorruptMessage.Code, code)
	}
}

func TestProduce_MagicNot2(t *testing.T) {
	state := cluster.NewState(cluster.Config{
		NodeID:            0,
		DefaultPartitions: 1,
		AutoCreateTopics:  true,
	})
	clk := clock.NewFakeClock(time.Unix(1000, 0))

	for _, magic := range []byte{0, 1, 3, 255} {
		raw := makeValidBatch(1)
		raw[16] = magic
		// Re-compute CRC since we changed bytes
		crc := crc32.Checksum(raw[21:], crc32.MakeTable(crc32.Castagnoli))
		binary.BigEndian.PutUint32(raw[17:21], crc)

		req := makeProduceReq(9, 1, "test-topic", 0, raw)
		resp := callProduce(t, state, clk, req)

		code := partitionErrorCode(resp, "test-topic", 0)
		if code != kerr.UnsupportedForMessageFormat.Code {
			t.Errorf("magic=%d: expected UnsupportedForMessageFormat (%d), got %d",
				magic, kerr.UnsupportedForMessageFormat.Code, code)
		}
	}
}

func TestProduce_MessageTooLarge(t *testing.T) {
	state := cluster.NewState(cluster.Config{
		NodeID:            0,
		DefaultPartitions: 1,
		AutoCreateTopics:  true,
	})
	clk := clock.NewFakeClock(time.Unix(1000, 0))

	// Create a batch that exceeds DefaultMaxMessageBytes (1048588)
	oversized := make([]byte, cluster.DefaultMaxMessageBytes+1)
	// Set the minimal header fields for it to pass earlier checks
	binary.BigEndian.PutUint32(oversized[8:12], uint32(len(oversized)-12))
	oversized[16] = 2
	binary.BigEndian.PutUint64(oversized[43:51], uint64(0xFFFFFFFFFFFFFFFF)) // ProducerID = -1
	binary.BigEndian.PutUint32(oversized[53:57], 0xFFFFFFFF)                 // BaseSequence = -1
	binary.BigEndian.PutUint32(oversized[57:61], 1)                          // NumRecords = 1
	crc := crc32.Checksum(oversized[21:], crc32.MakeTable(crc32.Castagnoli))
	binary.BigEndian.PutUint32(oversized[17:21], crc)

	req := makeProduceReq(9, 1, "test-topic", 0, oversized)
	resp := callProduce(t, state, clk, req)

	code := partitionErrorCode(resp, "test-topic", 0)
	if code != kerr.MessageTooLarge.Code {
		t.Errorf("expected MessageTooLarge (%d), got %d",
			kerr.MessageTooLarge.Code, code)
	}
}

func TestProduce_MessageTooLarge_CustomConfig(t *testing.T) {
	state := cluster.NewState(cluster.Config{
		NodeID:            0,
		DefaultPartitions: 1,
		AutoCreateTopics:  true,
	})
	clk := clock.NewFakeClock(time.Unix(1000, 0))

	// Create the topic with a custom max.message.bytes = 100
	state.CreateTopicWithConfigs("small-topic", 1, map[string]string{
		"max.message.bytes": "100",
	})

	// 101 bytes: valid header but exceeds the custom limit
	raw := make([]byte, 101)
	binary.BigEndian.PutUint32(raw[8:12], uint32(len(raw)-12))
	raw[16] = 2
	binary.BigEndian.PutUint64(raw[43:51], uint64(0xFFFFFFFFFFFFFFFF))
	binary.BigEndian.PutUint32(raw[53:57], 0xFFFFFFFF)
	binary.BigEndian.PutUint32(raw[57:61], 1)
	crc := crc32.Checksum(raw[21:], crc32.MakeTable(crc32.Castagnoli))
	binary.BigEndian.PutUint32(raw[17:21], crc)

	req := makeProduceReq(9, 1, "small-topic", 0, raw)
	resp := callProduce(t, state, clk, req)

	code := partitionErrorCode(resp, "small-topic", 0)
	if code != kerr.MessageTooLarge.Code {
		t.Errorf("expected MessageTooLarge (%d), got %d",
			kerr.MessageTooLarge.Code, code)
	}
}

func TestProduce_UnknownPartition(t *testing.T) {
	state := cluster.NewState(cluster.Config{
		NodeID:            0,
		DefaultPartitions: 1,
		AutoCreateTopics:  true,
	})
	clk := clock.NewFakeClock(time.Unix(1000, 0))

	// Topic auto-created with 1 partition (index 0). Partition 1 is out of range.
	raw := makeValidBatch(1)
	req := makeProduceReq(9, 1, "test-topic", 1, raw)
	resp := callProduce(t, state, clk, req)

	code := partitionErrorCode(resp, "test-topic", 1)
	if code != kerr.UnknownTopicOrPartition.Code {
		t.Errorf("expected UnknownTopicOrPartition (%d), got %d",
			kerr.UnknownTopicOrPartition.Code, code)
	}

	// Negative partition
	req2 := makeProduceReq(9, 1, "test-topic", -1, raw)
	resp2 := callProduce(t, state, clk, req2)

	code2 := partitionErrorCode(resp2, "test-topic", -1)
	if code2 != kerr.UnknownTopicOrPartition.Code {
		t.Errorf("negative partition: expected UnknownTopicOrPartition (%d), got %d",
			kerr.UnknownTopicOrPartition.Code, code2)
	}
}

func TestProduce_UnknownTopic(t *testing.T) {
	state := cluster.NewState(cluster.Config{
		NodeID:            0,
		DefaultPartitions: 1,
		AutoCreateTopics:  false, // disable auto-create
	})
	clk := clock.NewFakeClock(time.Unix(1000, 0))

	raw := makeValidBatch(1)
	req := makeProduceReq(9, 1, "nonexistent", 0, raw)
	resp := callProduce(t, state, clk, req)

	code := partitionErrorCode(resp, "nonexistent", 0)
	if code != kerr.UnknownTopicOrPartition.Code {
		t.Errorf("expected UnknownTopicOrPartition (%d), got %d",
			kerr.UnknownTopicOrPartition.Code, code)
	}
}

func TestProduce_Acks0_ReturnsNil(t *testing.T) {
	state := cluster.NewState(cluster.Config{
		NodeID:            0,
		DefaultPartitions: 1,
		AutoCreateTopics:  true,
	})
	clk := clock.NewFakeClock(time.Unix(1000, 0))

	// Use a corrupt (too short) message so the handler exits before reaching WAL.
	// acks=0 should still return nil regardless of validation errors.
	req := makeProduceReq(9, 0, "test-topic", 0, make([]byte, 10))
	resp := callProduce(t, state, clk, req)
	if resp != nil {
		t.Errorf("expected nil response for acks=0 with corrupt data, got %+v", resp)
	}

	// Also test the version-check early exit: use an unsupported version
	req2 := makeProduceReq(999, 0, "test-topic", 0, makeValidBatch(1))
	resp2 := callProduce(t, state, clk, req2)
	if resp2 != nil {
		t.Errorf("expected nil response for acks=0 with bad version, got %+v", resp2)
	}
}

func TestProduce_GetMaxMessageBytes(t *testing.T) {
	tests := []struct {
		name     string
		configs  map[string]string
		expected int
	}{
		{
			name:     "default",
			configs:  nil,
			expected: cluster.DefaultMaxMessageBytes,
		},
		{
			name:     "custom valid",
			configs:  map[string]string{"max.message.bytes": "512000"},
			expected: 512000,
		},
		{
			name:     "invalid non-numeric",
			configs:  map[string]string{"max.message.bytes": "not-a-number"},
			expected: cluster.DefaultMaxMessageBytes,
		},
		{
			name:     "invalid zero",
			configs:  map[string]string{"max.message.bytes": "0"},
			expected: cluster.DefaultMaxMessageBytes,
		},
		{
			name:     "invalid negative",
			configs:  map[string]string{"max.message.bytes": "-1"},
			expected: cluster.DefaultMaxMessageBytes,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			state := cluster.NewState(cluster.Config{
				NodeID:            0,
				DefaultPartitions: 1,
			})
			var td *cluster.TopicData
			if tc.configs != nil {
				td, _ = state.CreateTopicWithConfigs("t", 1, tc.configs)
			} else {
				td, _ = state.CreateTopic("t", 1)
			}
			got := getMaxMessageBytes(td)
			if got != tc.expected {
				t.Errorf("getMaxMessageBytes() = %d, want %d", got, tc.expected)
			}
		})
	}
}

func TestProduce_GetTimestampType(t *testing.T) {
	tests := []struct {
		name     string
		configs  map[string]string
		expected string
	}{
		{
			name:     "default",
			configs:  nil,
			expected: "CreateTime",
		},
		{
			name:     "LogAppendTime",
			configs:  map[string]string{"message.timestamp.type": "LogAppendTime"},
			expected: "LogAppendTime",
		},
		{
			name:     "explicit CreateTime",
			configs:  map[string]string{"message.timestamp.type": "CreateTime"},
			expected: "CreateTime",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			state := cluster.NewState(cluster.Config{
				NodeID:            0,
				DefaultPartitions: 1,
			})
			var td *cluster.TopicData
			if tc.configs != nil {
				td, _ = state.CreateTopicWithConfigs("t", 1, tc.configs)
			} else {
				td, _ = state.CreateTopic("t", 1)
			}
			got := getTimestampType(td)
			if got != tc.expected {
				t.Errorf("getTimestampType() = %q, want %q", got, tc.expected)
			}
		})
	}
}

func TestProduce_LogAppendTimeRewriting(t *testing.T) {
	state := cluster.NewState(cluster.Config{
		NodeID:            0,
		DefaultPartitions: 1,
		AutoCreateTopics:  true,
	})
	// Pre-create with LogAppendTime
	state.CreateTopicWithConfigs("lat-topic", 1, map[string]string{
		"message.timestamp.type": "LogAppendTime",
	})

	fixedTime := time.Unix(5000, 0)

	raw := makeValidBatch(1)

	// Verify timestamps before — should NOT be the clock time
	origBaseTs := int64(binary.BigEndian.Uint64(raw[27:35]))
	if origBaseTs == fixedTime.UnixMilli() {
		t.Fatal("test setup: original timestamp should differ from fake clock")
	}

	// With LogAppendTime, the handler calls SetLogAppendTime on the raw bytes.
	// Since acks=1 and walWriter=nil, this will panic when trying to submit to WAL.
	// We use acks=0 to suppress the response (and the handler returns nil before WAL).
	// Actually, acks=0 still processes the request — it just doesn't send a response.
	// With a nil walWriter, the code will panic at produceSubmitWAL.
	//
	// Instead, we test LogAppendTime rewriting more directly by verifying
	// the behavior through SetLogAppendTime at a lower level, or we accept
	// that the WAL path will be reached. For a proper integration, we'd
	// need a walWriter.
	//
	// However, since the handler applies SetLogAppendTime before reaching
	// the WAL code, we can verify it by checking the error path: the
	// handler will reach produceSubmitWAL with nil walWriter, which will
	// panic. For unit tests, we test the helper directly.

	// Direct test of SetLogAppendTime behavior
	meta, err := cluster.ParseBatchHeader(raw)
	if err != nil {
		t.Fatalf("ParseBatchHeader: %v", err)
	}

	cluster.SetLogAppendTime(raw, fixedTime.UnixMilli(), &meta)

	newBaseTs := int64(binary.BigEndian.Uint64(raw[27:35]))
	newMaxTs := int64(binary.BigEndian.Uint64(raw[35:43]))

	if newBaseTs != fixedTime.UnixMilli() {
		t.Errorf("BaseTimestamp: got %d, want %d", newBaseTs, fixedTime.UnixMilli())
	}
	if newMaxTs != fixedTime.UnixMilli() {
		t.Errorf("MaxTimestamp: got %d, want %d", newMaxTs, fixedTime.UnixMilli())
	}

	// Verify attributes bit 3 is set (LogAppendTime)
	attrs := binary.BigEndian.Uint16(raw[21:23])
	if attrs&0x0008 == 0 {
		t.Error("LogAppendTime attribute bit (0x0008) should be set")
	}

	// Verify CRC was updated and is valid
	if !cluster.ValidateBatchCRC(raw, meta.CRC) {
		t.Error("CRC should be valid after SetLogAppendTime")
	}
}

func TestProduce_MultiplePartitionsMultipleErrors(t *testing.T) {
	state := cluster.NewState(cluster.Config{
		NodeID:            0,
		DefaultPartitions: 3,
		AutoCreateTopics:  true,
	})
	clk := clock.NewFakeClock(time.Unix(1000, 0))

	r := kmsg.NewPtrProduceRequest()
	r.Version = 9
	r.Acks = 1

	rt := kmsg.NewProduceRequestTopic()
	rt.Topic = "multi-topic"

	// Partition 0: valid batch (will fail at WAL since walWriter is nil,
	// but we check that earlier errors are reported correctly)
	// Partition 5: out of range (topic has 3 partitions)
	// Partition 0 with too-short data
	rp0 := kmsg.NewProduceRequestTopicPartition()
	rp0.Partition = 5
	rp0.Records = makeValidBatch(1)
	rt.Partitions = append(rt.Partitions, rp0)

	rp1 := kmsg.NewProduceRequestTopicPartition()
	rp1.Partition = 0
	rp1.Records = make([]byte, 10) // too short
	rt.Partitions = append(rt.Partitions, rp1)

	r.Topics = append(r.Topics, rt)
	resp := callProduce(t, state, clk, r)

	// Partition 5: out of range
	code0 := partitionErrorCode(resp, "multi-topic", 5)
	if code0 != kerr.UnknownTopicOrPartition.Code {
		t.Errorf("partition 5: expected UnknownTopicOrPartition (%d), got %d",
			kerr.UnknownTopicOrPartition.Code, code0)
	}

	// Partition 0 with short data: corrupt
	code1 := partitionErrorCode(resp, "multi-topic", 0)
	if code1 != kerr.CorruptMessage.Code {
		t.Errorf("partition 0 (short): expected CorruptMessage (%d), got %d",
			kerr.CorruptMessage.Code, code1)
	}
}

func TestProduce_UnknownTopicAllPartitionsError(t *testing.T) {
	state := cluster.NewState(cluster.Config{
		NodeID:            0,
		DefaultPartitions: 1,
		AutoCreateTopics:  false,
	})

	// Request with multiple partitions for an unknown topic
	r := kmsg.NewPtrProduceRequest()
	r.Version = 9
	r.Acks = 1

	rt := kmsg.NewProduceRequestTopic()
	rt.Topic = "unknown"

	for _, pid := range []int32{0, 1, 2} {
		rp := kmsg.NewProduceRequestTopicPartition()
		rp.Partition = pid
		rp.Records = makeValidBatch(1)
		rt.Partitions = append(rt.Partitions, rp)
	}
	r.Topics = append(r.Topics, rt)
	resp := callProduce(t, state, clock.NewFakeClock(time.Unix(1000, 0)), r)

	for _, pid := range []int32{0, 1, 2} {
		code := partitionErrorCode(resp, "unknown", pid)
		if code != kerr.UnknownTopicOrPartition.Code {
			t.Errorf("partition %d: expected UnknownTopicOrPartition (%d), got %d",
				pid, kerr.UnknownTopicOrPartition.Code, code)
		}
	}
}
