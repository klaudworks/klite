package integration

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	s3store "github.com/klaudworks/klite/internal/s3"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// TestWALProduceRestart verifies that data written through the WAL path
// survives a broker restart. Produces 100 records, shuts down the broker,
// restarts with the same data directory, and verifies all 100 records are
// readable from offset 0.
func TestWALProduceRestart(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	topic := "wal-restart-test"
	numRecords := 100

	// Phase 1: Start broker with WAL, produce data
	func() {
		tb := StartBroker(t,

			WithDataDir(dataDir),
		)

		admin := NewAdminClient(t, tb.Addr)
		_, err := admin.CreateTopic(context.Background(), 1, 1, nil, topic)
		require.NoError(t, err)

		producer := NewClient(t, tb.Addr,
			kgo.DefaultProduceTopic(topic),
			kgo.RecordPartitioner(kgo.ManualPartitioner()),
		)

		// Produce 100 records
		for i := 0; i < numRecords; i++ {
			rec := &kgo.Record{
				Topic:     topic,
				Partition: 0,
				Key:       []byte(fmt.Sprintf("key-%d", i)),
				Value:     []byte(fmt.Sprintf("value-%d", i)),
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			results := producer.ProduceSync(ctx, rec)
			cancel()
			require.Len(t, results, 1)
			require.NoError(t, results[0].Err, "record %d", i)
			require.Equal(t, int64(i), results[0].Record.Offset)
		}

		// Verify data is readable before shutdown
		consumer := NewClient(t, tb.Addr,
			kgo.ConsumeTopics(topic),
			kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		)
		consumed := ConsumeN(t, consumer, numRecords, 15*time.Second)
		require.Len(t, consumed, numRecords)
		for i, r := range consumed {
			require.Equal(t, fmt.Sprintf("value-%d", i), string(r.Value))
		}
	}()

	// Phase 2: Restart broker with same data dir and WAL enabled.
	// With metadata.log, the topic metadata is persisted, so WAL replay
	// can find the topic and rebuild partition state from WAL entries.
	tb2 := StartBroker(t,

		WithDataDir(dataDir),
	)

	// The topic should already exist (restored from metadata.log)
	// Verify all 100 records are still accessible after restart
	consumer2 := NewClient(t, tb2.Addr,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	consumed2 := ConsumeN(t, consumer2, numRecords, 15*time.Second)
	require.Len(t, consumed2, numRecords)
	for i, r := range consumed2 {
		require.Equal(t, fmt.Sprintf("value-%d", i), string(r.Value))
		require.Equal(t, int64(i), r.Offset)
	}

	// Produce more records — offsets should continue from 100
	producer2 := NewClient(t, tb2.Addr,
		kgo.DefaultProduceTopic(topic),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)

	for i := 0; i < 5; i++ {
		rec := &kgo.Record{
			Topic:     topic,
			Partition: 0,
			Key:       []byte(fmt.Sprintf("restart-key-%d", i)),
			Value:     []byte(fmt.Sprintf("restart-value-%d", i)),
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		results := producer2.ProduceSync(ctx, rec)
		cancel()
		require.Len(t, results, 1)
		require.NoError(t, results[0].Err, "restart record %d", i)
		require.Equal(t, int64(numRecords+i), results[0].Record.Offset, "restart record %d offset", i)
	}
}

// TestWALReadTier verifies the three-tier read cascade: when data is evicted
// from the ring buffer, it can still be read from the WAL via the index.
// Uses a tiny ring buffer (16 slots) and produces enough data to force eviction.
func TestWALReadTier(t *testing.T) {
	t.Parallel()

	topic := "wal-read-tier-test"
	tb := StartBroker(t,

		// Use a tiny ring buffer: 16 slots * 1 partition * ~16 KiB est = very small
		WithChunkPoolMemory(16*16*1024),
	)

	admin := NewAdminClient(t, tb.Addr)
	_, err := admin.CreateTopic(context.Background(), 1, 1, nil, topic)
	require.NoError(t, err)

	producer := NewClient(t, tb.Addr,
		kgo.DefaultProduceTopic(topic),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.MaxBufferedRecords(1), // Force immediate flush per record
		kgo.ProducerLinger(0),     // No linger — send immediately
	)

	// Produce 50 records (each as a separate batch via ProduceSync with MaxBufferedRecords=1).
	// More than the 16-slot ring buffer, so older entries are evicted but should still be in WAL.
	totalRecords := 50
	for i := 0; i < totalRecords; i++ {
		rec := &kgo.Record{
			Topic:     topic,
			Partition: 0,
			Key:       []byte(fmt.Sprintf("key-%d", i)),
			Value:     []byte(fmt.Sprintf("value-%d", i)),
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		results := producer.ProduceSync(ctx, rec)
		cancel()
		require.Len(t, results, 1)
		require.NoError(t, results[0].Err, "record %d", i)
	}

	// Read from the beginning — offset 0 should be served from WAL (evicted from ring)
	consumer := NewClient(t, tb.Addr,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	consumed := ConsumeN(t, consumer, totalRecords, 15*time.Second)
	require.Len(t, consumed, totalRecords)

	// Verify all records are present and in order
	for i, r := range consumed {
		require.Equal(t, int64(i), r.Offset, "record %d offset", i)
		require.Equal(t, fmt.Sprintf("value-%d", i), string(r.Value), "record %d value", i)
	}
}

// TestWALTopicConfigSurvivesRestart verifies that topics created with custom configs
// survive a broker restart. Creates a topic with custom config, restarts the broker,
// and uses DescribeConfigs to verify the config is still present.
func TestWALTopicConfigSurvivesRestart(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	topic := "wal-config-restart-test"

	// Phase 1: Start broker, create topic with custom config
	func() {
		tb := StartBroker(t,

			WithDataDir(dataDir),
			WithAutoCreateTopics(false),
		)

		admin := NewAdminClient(t, tb.Addr)

		// Create topic with custom retention.ms
		retentionMs := "3600000"
		_, err := admin.CreateTopic(context.Background(), 1, 1,
			map[string]*string{
				"retention.ms": &retentionMs,
			},
			topic,
		)
		require.NoError(t, err)

		// Verify the config via describe
		configs, err := admin.DescribeTopicConfigs(context.Background(), topic)
		require.NoError(t, err)
		require.Len(t, configs, 1)
		require.NoError(t, configs[0].Err)

		found := false
		for _, c := range configs[0].Configs {
			if c.Key == "retention.ms" && *c.Value == "3600000" {
				found = true
				break
			}
		}
		require.True(t, found, "retention.ms not found in initial describe")
	}()

	// Phase 2: Restart, verify config persists
	tb2 := StartBroker(t,

		WithDataDir(dataDir),
		WithAutoCreateTopics(false),
	)

	admin2 := NewAdminClient(t, tb2.Addr)

	// The topic should exist after restart (from metadata.log)
	configs, err := admin2.DescribeTopicConfigs(context.Background(), topic)
	require.NoError(t, err)
	require.Len(t, configs, 1)
	require.NoError(t, configs[0].Err)

	found := false
	for _, c := range configs[0].Configs {
		if c.Key == "retention.ms" && *c.Value == "3600000" {
			found = true
			break
		}
	}
	require.True(t, found, "retention.ms not found after restart")
}

// TestWALCommittedOffsetsSurviveRestart verifies that consumer group committed
// offsets survive a broker restart. Commits offsets, restarts the broker,
// and fetches offsets to verify they're still present.
func TestWALCommittedOffsetsSurviveRestart(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	topic := "wal-offset-restart-test"
	group := "wal-offset-restart-group"

	// Phase 1: Start broker, produce data, commit offsets
	func() {
		tb := StartBroker(t,

			WithDataDir(dataDir),
		)

		admin := NewAdminClient(t, tb.Addr)
		_, err := admin.CreateTopic(context.Background(), 1, 1, nil, topic)
		require.NoError(t, err)

		// Produce 5 records
		producer := NewClient(t, tb.Addr,
			kgo.DefaultProduceTopic(topic),
			kgo.RecordPartitioner(kgo.ManualPartitioner()),
		)
		for i := 0; i < 5; i++ {
			rec := &kgo.Record{
				Topic:     topic,
				Partition: 0,
				Value:     []byte(fmt.Sprintf("value-%d", i)),
			}
			ProduceSync(t, producer, rec)
		}

		// Commit offset 3 for the group (simple/admin commit)
		offsets := make(map[string]map[int32]kgo.EpochOffset)
		offsets[topic] = map[int32]kgo.EpochOffset{
			0: {Offset: 3, Epoch: -1},
		}
		commitReq := kmsg.NewOffsetCommitRequest()
		commitReq.Group = group
		commitReq.Topics = []kmsg.OffsetCommitRequestTopic{{
			Topic: topic,
			Partitions: []kmsg.OffsetCommitRequestTopicPartition{{
				Partition: 0,
				Offset:    3,
			}},
		}}
		cl := NewClient(t, tb.Addr)
		commitResp, err := commitReq.RequestWith(context.Background(), cl)
		require.NoError(t, err)
		require.Len(t, commitResp.Topics, 1)
		require.Len(t, commitResp.Topics[0].Partitions, 1)
		require.Equal(t, int16(0), commitResp.Topics[0].Partitions[0].ErrorCode, "commit should succeed")
	}()

	// Phase 2: Restart, verify offsets persist
	tb2 := StartBroker(t,

		WithDataDir(dataDir),
	)

	// Fetch committed offsets
	fetchReq := kmsg.NewOffsetFetchRequest()
	fetchReq.Group = group
	fetchReq.Topics = []kmsg.OffsetFetchRequestTopic{{
		Topic:      topic,
		Partitions: []int32{0},
	}}

	cl2 := NewClient(t, tb2.Addr)
	fetchResp, err := fetchReq.RequestWith(context.Background(), cl2)
	require.NoError(t, err)
	require.Len(t, fetchResp.Topics, 1)
	require.Len(t, fetchResp.Topics[0].Partitions, 1)
	require.Equal(t, int64(3), fetchResp.Topics[0].Partitions[0].Offset,
		"committed offset should be 3 after restart")
}

// TestWALMultiPartitionRestart verifies that data across multiple partitions
// survives a broker restart. Produces to 3 partitions, restarts, and verifies
// all data is present.
func TestWALMultiPartitionRestart(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	topic := "wal-multipart-restart-test"
	numPartitions := 3
	recordsPerPartition := 20

	// Phase 1: Start broker, create multi-partition topic, produce data
	func() {
		tb := StartBroker(t,

			WithDataDir(dataDir),
		)

		admin := NewAdminClient(t, tb.Addr)
		_, err := admin.CreateTopic(context.Background(), int32(numPartitions), 1, nil, topic)
		require.NoError(t, err)

		producer := NewClient(t, tb.Addr,
			kgo.DefaultProduceTopic(topic),
			kgo.RecordPartitioner(kgo.ManualPartitioner()),
		)

		// Produce records to each partition
		for p := 0; p < numPartitions; p++ {
			for i := 0; i < recordsPerPartition; i++ {
				rec := &kgo.Record{
					Topic:     topic,
					Partition: int32(p),
					Key:       []byte(fmt.Sprintf("p%d-key-%d", p, i)),
					Value:     []byte(fmt.Sprintf("p%d-value-%d", p, i)),
				}
				ProduceSync(t, producer, rec)
			}
		}

		// Verify each partition has data before shutdown
		for p := 0; p < numPartitions; p++ {
			consumer := NewClient(t, tb.Addr,
				kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
					topic: {int32(p): kgo.NewOffset().AtStart()},
				}),
			)
			consumed := ConsumeN(t, consumer, recordsPerPartition, 10*time.Second)
			require.Len(t, consumed, recordsPerPartition, "partition %d", p)
		}
	}()

	// Phase 2: Restart and verify all partitions
	tb2 := StartBroker(t,

		WithDataDir(dataDir),
	)

	for p := 0; p < numPartitions; p++ {
		consumer := NewClient(t, tb2.Addr,
			kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
				topic: {int32(p): kgo.NewOffset().AtStart()},
			}),
		)
		consumed := ConsumeN(t, consumer, recordsPerPartition, 10*time.Second)
		require.Len(t, consumed, recordsPerPartition, "partition %d after restart", p)

		for i, r := range consumed {
			require.Equal(t, fmt.Sprintf("p%d-value-%d", p, i), string(r.Value),
				"partition %d record %d", p, i)
			require.Equal(t, int64(i), r.Offset, "partition %d record %d offset", p, i)
		}
	}

	// Verify offsets continue correctly after restart
	producer2 := NewClient(t, tb2.Addr,
		kgo.DefaultProduceTopic(topic),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	for p := 0; p < numPartitions; p++ {
		rec := &kgo.Record{
			Topic:     topic,
			Partition: int32(p),
			Value:     []byte("after-restart"),
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		results := producer2.ProduceSync(ctx, rec)
		cancel()
		require.Len(t, results, 1)
		require.NoError(t, results[0].Err)
		require.Equal(t, int64(recordsPerPartition), results[0].Record.Offset,
			"partition %d offset after restart", p)
	}
}

// TestWALCrashRecovery verifies that if the last WAL entry is corrupted
// (simulating a crash mid-write), the broker recovers by truncating the
// corrupted tail and serves all valid data.
func TestWALCrashRecovery(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	topic := "wal-crash-recovery-test"
	numRecords := 20

	// Phase 1: Start broker, produce data, stop cleanly
	func() {
		tb := StartBroker(t,

			WithDataDir(dataDir),
		)

		admin := NewAdminClient(t, tb.Addr)
		_, err := admin.CreateTopic(context.Background(), 1, 1, nil, topic)
		require.NoError(t, err)

		producer := NewClient(t, tb.Addr,
			kgo.DefaultProduceTopic(topic),
			kgo.RecordPartitioner(kgo.ManualPartitioner()),
		)

		for i := 0; i < numRecords; i++ {
			rec := &kgo.Record{
				Topic:     topic,
				Partition: 0,
				Key:       []byte(fmt.Sprintf("key-%d", i)),
				Value:     []byte(fmt.Sprintf("value-%d", i)),
			}
			ProduceSync(t, producer, rec)
		}
	}()

	// Corrupt the last entry in the WAL by appending garbage bytes
	// to the last segment file. This simulates a crash mid-write.
	walDir := dataDir + "/wal"
	entries, err := os.ReadDir(walDir)
	require.NoError(t, err)

	var lastSeg string
	for _, e := range entries {
		if !e.IsDir() && len(e.Name()) == 24 {
			lastSeg = e.Name()
		}
	}
	require.NotEmpty(t, lastSeg, "should have at least one WAL segment")

	segPath := walDir + "/" + lastSeg
	origStat, err := os.Stat(segPath)
	require.NoError(t, err)
	origSize := origStat.Size()

	// Append partial/corrupted bytes (simulate partial write + crash)
	f, err := os.OpenFile(segPath, os.O_WRONLY|os.O_APPEND, 0o644)
	require.NoError(t, err)
	// Write a valid-looking length prefix (100 bytes) but then garbage data
	garbage := make([]byte, 50) // partial entry: claims 100 bytes but only has 46
	garbage[0] = 0
	garbage[1] = 0
	garbage[2] = 0
	garbage[3] = 100 // length = 100
	for i := 4; i < len(garbage); i++ {
		garbage[i] = 0xFF // junk
	}
	_, err = f.Write(garbage)
	require.NoError(t, err)
	_ = f.Close()

	// Verify file is now larger
	corruptStat, err := os.Stat(segPath)
	require.NoError(t, err)
	require.Greater(t, corruptStat.Size(), origSize, "file should be larger after corruption")

	// Phase 2: Restart — broker should truncate the corrupted tail and recover
	tb2 := StartBroker(t,

		WithDataDir(dataDir),
	)

	// Verify the segment was truncated back to the valid size
	truncStat, err := os.Stat(segPath)
	require.NoError(t, err)
	require.Equal(t, origSize, truncStat.Size(), "segment should be truncated to valid size")

	// All valid records should be readable
	consumer := NewClient(t, tb2.Addr,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	consumed := ConsumeN(t, consumer, numRecords, 10*time.Second)
	require.Len(t, consumed, numRecords)
	for i, r := range consumed {
		require.Equal(t, fmt.Sprintf("value-%d", i), string(r.Value), "record %d", i)
		require.Equal(t, int64(i), r.Offset)
	}

	// New produces should work and continue from the right offset
	producer2 := NewClient(t, tb2.Addr,
		kgo.DefaultProduceTopic(topic),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	rec := &kgo.Record{
		Topic:     topic,
		Partition: 0,
		Value:     []byte("after-crash"),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	results := producer2.ProduceSync(ctx, rec)
	cancel()
	require.Len(t, results, 1)
	require.NoError(t, results[0].Err)
	require.Equal(t, int64(numRecords), results[0].Record.Offset, "offset should continue after crash recovery")
}

// TestWALSegmentRotation verifies that when enough data is produced to rotate
// WAL segments, reads correctly span across segment boundaries.
func TestWALSegmentRotation(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	topic := "wal-segment-rotation-test"

	// Use a tiny segment size (4 KiB) to force rotation with few records
	segMaxBytes := int64(4 * 1024)

	// Phase 1: Produce enough data to cause multiple segment rotations
	func() {
		tb := StartBroker(t,

			WithDataDir(dataDir),
			WithWALSegmentMaxBytes(segMaxBytes),
		)

		admin := NewAdminClient(t, tb.Addr)
		_, err := admin.CreateTopic(context.Background(), 1, 1, nil, topic)
		require.NoError(t, err)

		producer := NewClient(t, tb.Addr,
			kgo.DefaultProduceTopic(topic),
			kgo.RecordPartitioner(kgo.ManualPartitioner()),
			kgo.MaxBufferedRecords(1),
			kgo.ProducerLinger(0),
		)

		// Produce 100 records — each ~100-200 bytes, so with 4 KiB segments
		// we should get many segment rotations.
		numRecords := 100
		for i := 0; i < numRecords; i++ {
			rec := &kgo.Record{
				Topic:     topic,
				Partition: 0,
				Key:       []byte(fmt.Sprintf("key-%03d", i)),
				Value:     []byte(fmt.Sprintf("value-data-payload-%03d", i)),
			}
			ProduceSync(t, producer, rec)
		}

		// Verify we have multiple segments
		walDir := dataDir + "/wal"
		entries, dirErr := os.ReadDir(walDir)
		require.NoError(t, dirErr)
		segCount := 0
		for _, e := range entries {
			if !e.IsDir() && len(e.Name()) == 24 {
				segCount++
			}
		}
		require.Greater(t, segCount, 1, "should have rotated into multiple segments")
		t.Logf("produced %d records across %d segments", numRecords, segCount)

		// Verify all records readable before shutdown
		consumer := NewClient(t, tb.Addr,
			kgo.ConsumeTopics(topic),
			kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		)
		consumed := ConsumeN(t, consumer, numRecords, 15*time.Second)
		require.Len(t, consumed, numRecords)
	}()

	// Phase 2: Restart and verify all records across segments
	tb2 := StartBroker(t,

		WithDataDir(dataDir),
		WithWALSegmentMaxBytes(segMaxBytes),
	)

	consumer2 := NewClient(t, tb2.Addr,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	consumed2 := ConsumeN(t, consumer2, 100, 15*time.Second)
	require.Len(t, consumed2, 100)

	for i, r := range consumed2 {
		require.Equal(t, fmt.Sprintf("value-data-payload-%03d", i), string(r.Value), "record %d", i)
		require.Equal(t, int64(i), r.Offset, "record %d offset", i)
	}

	// Offsets should continue correctly
	producer2 := NewClient(t, tb2.Addr,
		kgo.DefaultProduceTopic(topic),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	rec := &kgo.Record{
		Topic:     topic,
		Partition: 0,
		Value:     []byte("after-rotation-restart"),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	results := producer2.ProduceSync(ctx, rec)
	cancel()
	require.Len(t, results, 1)
	require.NoError(t, results[0].Err)
	require.Equal(t, int64(100), results[0].Record.Offset)
}

// TestWALEmptyStart verifies that a fresh broker start with no existing data
// directory works correctly — no errors, no replay, broker accepts connections
// and can produce/consume normally.
func TestWALEmptyStart(t *testing.T) {
	t.Parallel()

	// Use a fresh temp dir (no pre-existing data)
	dataDir := t.TempDir()

	tb := StartBroker(t,

		WithDataDir(dataDir),
	)

	// Broker should be ready and functional
	topic := "wal-empty-start-test"
	admin := NewAdminClient(t, tb.Addr)
	_, err := admin.CreateTopic(context.Background(), 1, 1, nil, topic)
	require.NoError(t, err)

	// Produce and consume to verify basic functionality
	producer := NewClient(t, tb.Addr,
		kgo.DefaultProduceTopic(topic),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)

	for i := 0; i < 5; i++ {
		rec := &kgo.Record{
			Topic:     topic,
			Partition: 0,
			Value:     []byte(fmt.Sprintf("empty-start-%d", i)),
		}
		ProduceSync(t, producer, rec)
	}

	consumer := NewClient(t, tb.Addr,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	consumed := ConsumeN(t, consumer, 5, 10*time.Second)
	require.Len(t, consumed, 5)
	for i, r := range consumed {
		require.Equal(t, fmt.Sprintf("empty-start-%d", i), string(r.Value))
		require.Equal(t, int64(i), r.Offset)
	}

	// Verify WAL directory was created
	_, err = os.Stat(dataDir + "/wal")
	require.NoError(t, err, "WAL directory should exist")

	// Verify metadata.log was created
	_, err = os.Stat(dataDir + "/metadata.log")
	require.NoError(t, err, "metadata.log should exist")
}

// TestWALProduceConsume verifies basic produce/consume through the WAL path.
func TestWALProduceConsume(t *testing.T) {
	t.Parallel()

	topic := "wal-produce-consume-test"
	tb := StartBroker(t)

	admin := NewAdminClient(t, tb.Addr)
	_, err := admin.CreateTopic(context.Background(), 1, 1, nil, topic)
	require.NoError(t, err)

	producer := NewClient(t, tb.Addr,
		kgo.DefaultProduceTopic(topic),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)

	// Produce records
	records := []*kgo.Record{
		{Topic: topic, Partition: 0, Value: []byte("A")},
		{Topic: topic, Partition: 0, Value: []byte("B")},
		{Topic: topic, Partition: 0, Value: []byte("C")},
	}
	ProduceSync(t, producer, records...)

	// Consume and verify
	consumer := NewClient(t, tb.Addr,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)

	consumed := ConsumeN(t, consumer, 3, 10*time.Second)
	require.Len(t, consumed, 3)
	require.Equal(t, "A", string(consumed[0].Value))
	require.Equal(t, "B", string(consumed[1].Value))
	require.Equal(t, "C", string(consumed[2].Value))
	require.Equal(t, int64(0), consumed[0].Offset)
	require.Equal(t, int64(1), consumed[1].Offset)
	require.Equal(t, int64(2), consumed[2].Offset)
}

// TestWALBackPressureS3Recovery verifies the end-to-end back-pressure path:
// when the WAL fills up (all segments unflushed to S3), produce fails or
// times out. After the S3 flusher runs and advances the watermark, produce
// succeeds again and all data remains consumable.
func TestWALBackPressureS3Recovery(t *testing.T) {
	t.Parallel()

	const (
		walSegmentSize = 4 * 1024  // 4 KiB segments — rotate frequently
		walMaxDisk     = 16 * 1024 // 16 KiB total WAL — fills after a few segments
		s3ObjSize      = 4 * 1024  // 4 KiB S3 objects — flush small batches
		valuePadding   = 512       // pad each record to fill WAL faster
	)

	mem := s3store.NewInMemoryS3()
	topic := "wal-backpressure-s3"
	pad := strings.Repeat("x", valuePadding)

	tb := StartBroker(t,
		WithS3(mem, "test-bucket", "klite/test"),
		WithWALSegmentMaxBytes(walSegmentSize),
		WithWALMaxDiskSize(walMaxDisk),
		WithS3FlushInterval(1*time.Second),
		WithS3FlushCheckInterval(200*time.Millisecond),
		WithS3TargetObjectSize(s3ObjSize),
	)

	admin := NewAdminClient(t, tb.Addr)
	_, err := admin.CreateTopic(context.Background(), 1, 1, nil, topic)
	require.NoError(t, err)

	// Phase 1: Produce until WAL back-pressure causes a failure.
	// A short timeout ensures we detect the back-pressure quickly — the
	// error may be KafkaStorageError (WAL full on submit) or a context
	// timeout (WAL full causes fsync to stall).
	producer := NewClient(t, tb.Addr,
		kgo.DefaultProduceTopic(topic),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.MaxBufferedRecords(1),
		kgo.ProducerLinger(0),
		kgo.RequestRetries(0),
	)

	var ackedCount int
	var hitBackPressure bool

	for i := 0; i < 500; i++ {
		rec := &kgo.Record{
			Topic:     topic,
			Partition: 0,
			Key:       []byte(fmt.Sprintf("key-%d", i)),
			Value:     []byte(fmt.Sprintf("v-%d-%s", i, pad)),
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		results := producer.ProduceSync(ctx, rec)
		cancel()

		if len(results) == 1 && results[0].Err != nil {
			t.Logf("produce failed at record %d: %v", i, results[0].Err)
			hitBackPressure = true
			break
		}
		require.Len(t, results, 1)
		require.NoError(t, results[0].Err, "record %d", i)
		ackedCount++
	}
	require.True(t, hitBackPressure, "expected produce to fail from WAL back-pressure after %d records", ackedCount)
	require.Greater(t, ackedCount, 0, "should have produced at least some records before back-pressure")
	t.Logf("produced %d acked records before WAL back-pressure", ackedCount)

	// Phase 2: Wait for S3 flusher to flush data and clear back-pressure.
	require.Eventually(t, func() bool {
		for _, k := range mem.Keys() {
			if s3KeyMatchesPartition(k, topic, 0) && strings.HasSuffix(k, ".obj") {
				return true
			}
		}
		return false
	}, 15*time.Second, 200*time.Millisecond, "S3 flush did not occur")

	// Phase 3: Produce should succeed again after S3 flush clears WAL pressure.
	// Use a fresh producer to avoid idempotent dedup complications from the
	// timed-out produce (which may have been durably written).
	producer2 := NewClient(t, tb.Addr,
		kgo.DefaultProduceTopic(topic),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.MaxBufferedRecords(1),
		kgo.ProducerLinger(0),
		kgo.RequestRetries(0),
	)

	require.Eventually(t, func() bool {
		rec := &kgo.Record{
			Topic:     topic,
			Partition: 0,
			Key:       []byte("recovery"),
			Value:     []byte("recovered"),
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		results := producer2.ProduceSync(ctx, rec)
		cancel()
		return len(results) == 1 && results[0].Err == nil
	}, 15*time.Second, 500*time.Millisecond, "produce did not recover after S3 flush")
	t.Logf("produce recovered after S3 flush")

	// Phase 4: Verify all data is consumable from offset 0.
	// The timed-out record may or may not have been durably written, so the
	// total consumable count is at least ackedCount + 1 (the recovery record)
	// but may be ackedCount + 2 if the timed-out record was also persisted.
	consumer := NewClient(t, tb.Addr,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)

	minExpected := ackedCount + 1 // acked + recovery
	consumed := ConsumeN(t, consumer, minExpected, 30*time.Second)
	require.GreaterOrEqual(t, len(consumed), minExpected)

	// Verify ordering: offsets should be monotonically increasing.
	for i := 1; i < len(consumed); i++ {
		require.Greater(t, consumed[i].Offset, consumed[i-1].Offset,
			"offsets should be monotonically increasing at position %d", i)
	}
	t.Logf("consumed %d records total (acked %d + extras)", len(consumed), ackedCount)
}
