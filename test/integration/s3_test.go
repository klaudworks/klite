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
)

// TestS3FlushBasic produces data, triggers flush, verifies S3 object exists.
func TestS3FlushBasic(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	mem := s3store.NewInMemoryS3()
	prefix := "klite/test"
	topic := "s3-flush-basic"

	// Start broker with S3, produce data, then stop (stop triggers flush)
	tb := StartBroker(t,

		WithDataDir(dataDir),
		WithS3(mem, "test-bucket", prefix),
		WithS3FlushInterval(24*time.Hour),
	)

	admin := NewAdminClient(t, tb.Addr)
	_, err := admin.CreateTopic(context.Background(), 1, 1, nil, topic)
	require.NoError(t, err)

	producer := NewClient(t, tb.Addr,
		kgo.DefaultProduceTopic(topic),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)

	for i := 0; i < 10; i++ {
		rec := &kgo.Record{
			Topic:     topic,
			Partition: 0,
			Value:     []byte(fmt.Sprintf("s3-basic-%d", i)),
		}
		ProduceSync(t, producer, rec)
	}

	// Explicit stop triggers final S3 flush
	tb.Stop()

	// Verify per-partition S3 object exists
	keys := mem.Keys()
	found := false
	for _, key := range keys {
		if strings.Contains(key, topic+"/0/") && strings.HasSuffix(key, ".obj") {
			found = true
			break
		}
	}
	require.True(t, found, "should have S3 object for topic/partition, keys: %v", keys)
}

// TestS3FlushFooter verifies the footer format of flushed S3 objects.
func TestS3FlushFooter(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	mem := s3store.NewInMemoryS3()
	prefix := "klite/test"
	topic := "s3-flush-footer"

	tb := StartBroker(t,

		WithDataDir(dataDir),
		WithS3(mem, "test-bucket", prefix),
		WithS3FlushInterval(24*time.Hour),
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

	for i := 0; i < 5; i++ {
		rec := &kgo.Record{
			Topic:     topic,
			Partition: 0,
			Value:     []byte(fmt.Sprintf("footer-%d", i)),
		}
		ProduceSync(t, producer, rec)
	}

	tb.Stop()

	// Find the object and parse its footer
	keys := mem.Keys()
	var objKey string
	for _, key := range keys {
		if strings.Contains(key, topic+"/0/") && strings.HasSuffix(key, ".obj") {
			objKey = key
			break
		}
	}
	require.NotEmpty(t, objKey, "should have S3 object")

	raw, ok := mem.GetRaw(objKey)
	require.True(t, ok)
	require.True(t, len(raw) > s3store.FooterTrailerSize)

	footer, err := s3store.ParseFooter(raw, int64(len(raw)))
	require.NoError(t, err)
	require.NotEmpty(t, footer.Entries, "footer should have batch entries")

	// Verify each entry has valid data
	for i, entry := range footer.Entries {
		require.True(t, entry.BatchLength > 0, "entry %d batch length should be > 0", i)
		require.True(t, entry.BytePosition+entry.BatchLength <= uint32(len(raw)),
			"entry %d should reference valid data within object", i)
	}

	// Verify the first entry starts at offset 0
	require.Equal(t, int64(0), footer.Entries[0].BaseOffset, "first batch should start at offset 0")
}

// TestS3ReadAfterWALTrim produces data, flushes to S3, trims WAL (by restart with
// small WAL), and verifies data can be read from S3.
func TestS3ReadAfterWALTrim(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	mem := s3store.NewInMemoryS3()
	prefix := "klite/test"
	topic := "s3-read-after-trim"
	numRecords := 20

	// Phase 1: produce + flush to S3
	tb := StartBroker(t,

		WithDataDir(dataDir),
		WithS3(mem, "test-bucket", prefix),
		WithS3FlushInterval(24*time.Hour),
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
			Value:     []byte(fmt.Sprintf("s3-read-%d", i)),
		}
		ProduceSync(t, producer, rec)
	}

	tb.Stop()

	// Delete WAL data to simulate trim
	os.RemoveAll(dataDir + "/wal")

	// Phase 2: restart with S3 — data should be readable from S3
	tb2 := StartBroker(t,

		WithDataDir(dataDir),
		WithS3(mem, "test-bucket", prefix),
		WithS3FlushInterval(24*time.Hour),
	)

	consumer := NewClient(t, tb2.Addr,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	consumed := ConsumeN(t, consumer, numRecords, 15*time.Second)
	require.Len(t, consumed, numRecords)
	for i, r := range consumed {
		require.Equal(t, fmt.Sprintf("s3-read-%d", i), string(r.Value), "record %d", i)
	}
}

// TestS3ReadCascade verifies the three-tier read cascade:
// recent data from ring buffer, WAL data from WAL, old data from S3.
func TestS3ReadCascade(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	mem := s3store.NewInMemoryS3()
	prefix := "klite/test"
	topic := "s3-read-cascade"

	// Phase 1: produce 20 records + flush to S3
	tb := StartBroker(t,

		WithDataDir(dataDir),
		WithS3(mem, "test-bucket", prefix),
		WithS3FlushInterval(24*time.Hour),
		WithRingBufferMaxMem(16*16*1024), // tiny ring buffer
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

	for i := 0; i < 20; i++ {
		rec := &kgo.Record{
			Topic:     topic,
			Partition: 0,
			Value:     []byte(fmt.Sprintf("cascade-%d", i)),
		}
		ProduceSync(t, producer, rec)
	}

	tb.Stop()

	// Delete WAL so only S3 has the old data
	os.RemoveAll(dataDir + "/wal")

	// Phase 2: restart, produce more (these go to ring buffer/WAL)
	tb2 := StartBroker(t,

		WithDataDir(dataDir),
		WithS3(mem, "test-bucket", prefix),
		WithS3FlushInterval(24*time.Hour),
		WithRingBufferMaxMem(16*16*1024),
	)

	producer2 := NewClient(t, tb2.Addr,
		kgo.DefaultProduceTopic(topic),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)

	for i := 0; i < 5; i++ {
		rec := &kgo.Record{
			Topic:     topic,
			Partition: 0,
			Value:     []byte(fmt.Sprintf("new-%d", i)),
		}
		ProduceSync(t, producer2, rec)
	}

	// Consume all 25 records — first 20 from S3, last 5 from ring/WAL
	consumer := NewClient(t, tb2.Addr,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	consumed := ConsumeN(t, consumer, 25, 15*time.Second)
	require.Len(t, consumed, 25)

	// Verify all records
	for i := 0; i < 20; i++ {
		require.Equal(t, fmt.Sprintf("cascade-%d", i), string(consumed[i].Value), "record %d", i)
	}
	for i := 0; i < 5; i++ {
		require.Equal(t, fmt.Sprintf("new-%d", i), string(consumed[20+i].Value), "record %d", 20+i)
	}
}

// TestS3PerPartitionKeys verifies that flushing produces separate objects per partition.
func TestS3PerPartitionKeys(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	mem := s3store.NewInMemoryS3()
	prefix := "klite/test"
	topic := "s3-per-part-keys"

	tb := StartBroker(t,

		WithDataDir(dataDir),
		WithS3(mem, "test-bucket", prefix),
		WithS3FlushInterval(24*time.Hour),
	)

	admin := NewAdminClient(t, tb.Addr)
	_, err := admin.CreateTopic(context.Background(), 3, 1, nil, topic)
	require.NoError(t, err)

	producer := NewClient(t, tb.Addr,
		kgo.DefaultProduceTopic(topic),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)

	for p := 0; p < 3; p++ {
		for i := 0; i < 5; i++ {
			rec := &kgo.Record{
				Topic:     topic,
				Partition: int32(p),
				Value:     []byte(fmt.Sprintf("p%d-%d", p, i)),
			}
			ProduceSync(t, producer, rec)
		}
	}

	tb.Stop()

	// Check that we have separate objects per partition
	keys := mem.Keys()
	partitionObjects := make(map[string]bool)
	for _, key := range keys {
		if strings.Contains(key, topic) && strings.HasSuffix(key, ".obj") {
			// Extract partition from key
			parts := strings.Split(key, "/")
			for i, p := range parts {
				if p == topic && i+1 < len(parts) {
					partitionObjects[parts[i+1]] = true
					break
				}
			}
		}
	}

	require.Len(t, partitionObjects, 3, "should have objects for all 3 partitions, got: %v", partitionObjects)
	require.True(t, partitionObjects["0"])
	require.True(t, partitionObjects["1"])
	require.True(t, partitionObjects["2"])
}

// TestS3Restart produces data, flushes to S3, restarts broker, verifies S3 data readable.
func TestS3Restart(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	mem := s3store.NewInMemoryS3()
	prefix := "klite/test"
	topic := "s3-restart"
	numRecords := 15

	// Phase 1: produce + stop (flush)
	tb := StartBroker(t,

		WithDataDir(dataDir),
		WithS3(mem, "test-bucket", prefix),
		WithS3FlushInterval(24*time.Hour),
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
			Value:     []byte(fmt.Sprintf("restart-%d", i)),
		}
		ProduceSync(t, producer, rec)
	}

	tb.Stop()

	// Phase 2: restart and verify data readable
	tb2 := StartBroker(t,

		WithDataDir(dataDir),
		WithS3(mem, "test-bucket", prefix),
		WithS3FlushInterval(24*time.Hour),
	)

	consumer := NewClient(t, tb2.Addr,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	consumed := ConsumeN(t, consumer, numRecords, 15*time.Second)
	require.Len(t, consumed, numRecords)
	for i, r := range consumed {
		require.Equal(t, fmt.Sprintf("restart-%d", i), string(r.Value), "record %d", i)
	}
}

// TestS3UnifiedSync verifies that the flush interval triggers a full sync of
// all partitions plus metadata.log.
func TestS3UnifiedSync(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	mem := s3store.NewInMemoryS3()
	prefix := "klite/test"
	topic := "s3-unified-sync"

	tb := StartBroker(t,

		WithDataDir(dataDir),
		WithS3(mem, "test-bucket", prefix),
		WithS3FlushInterval(24*time.Hour),
	)

	admin := NewAdminClient(t, tb.Addr)
	_, err := admin.CreateTopic(context.Background(), 2, 1, nil, topic)
	require.NoError(t, err)

	producer := NewClient(t, tb.Addr,
		kgo.DefaultProduceTopic(topic),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)

	for p := 0; p < 2; p++ {
		for i := 0; i < 5; i++ {
			rec := &kgo.Record{
				Topic:     topic,
				Partition: int32(p),
				Value:     []byte(fmt.Sprintf("sync-p%d-%d", p, i)),
			}
			ProduceSync(t, producer, rec)
		}
	}

	tb.Stop()

	// Verify S3 has objects for both partitions
	keys := mem.Keys()
	hasP0 := false
	hasP1 := false
	hasMetadata := false
	for _, key := range keys {
		if strings.Contains(key, topic+"/0/") {
			hasP0 = true
		}
		if strings.Contains(key, topic+"/1/") {
			hasP1 = true
		}
		if strings.Contains(key, "metadata.log") {
			hasMetadata = true
		}
	}
	require.True(t, hasP0, "should have partition 0 object")
	require.True(t, hasP1, "should have partition 1 object")
	require.True(t, hasMetadata, "should have metadata.log backup")
}

// TestS3DisasterRecoveryWithBackup tests disaster recovery when metadata.log is
// backed up in S3.
func TestS3DisasterRecoveryWithBackup(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	mem := s3store.NewInMemoryS3()
	prefix := "klite/test"
	topic := "s3-dr-backup"
	numRecords := 10

	// Phase 1: produce + flush
	tb := StartBroker(t,

		WithDataDir(dataDir),
		WithS3(mem, "test-bucket", prefix),
		WithS3FlushInterval(24*time.Hour),
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
			Value:     []byte(fmt.Sprintf("dr-%d", i)),
		}
		ProduceSync(t, producer, rec)
	}

	tb.Stop()

	// Delete the entire data directory (simulate disk loss)
	os.RemoveAll(dataDir)
	os.MkdirAll(dataDir, 0o755)

	// Phase 2: restart — should recover from S3 backup
	tb2 := StartBroker(t,

		WithDataDir(dataDir),
		WithS3(mem, "test-bucket", prefix),
		WithS3FlushInterval(24*time.Hour),
	)

	// Data should be readable from S3
	consumer := NewClient(t, tb2.Addr,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	consumed := ConsumeN(t, consumer, numRecords, 15*time.Second)
	require.Len(t, consumed, numRecords)
	for i, r := range consumed {
		require.Equal(t, fmt.Sprintf("dr-%d", i), string(r.Value), "record %d", i)
	}
}

// TestWALReadAfterRestart verifies that after restart, WAL data is readable via rebuilt index.
func TestWALReadAfterRestart(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	topic := "wal-read-after-restart"
	numRecords := 30

	// Phase 1: produce with tiny ring buffer (so data is mainly in WAL)
	tb := StartBroker(t,

		WithDataDir(dataDir),
		WithRingBufferMaxMem(16*16*1024), // tiny
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

	for i := 0; i < numRecords; i++ {
		rec := &kgo.Record{
			Topic:     topic,
			Partition: 0,
			Value:     []byte(fmt.Sprintf("wal-restart-%d", i)),
		}
		ProduceSync(t, producer, rec)
	}

	tb.Stop()

	// Phase 2: restart with same data dir, read from rebuilt WAL index
	tb2 := StartBroker(t,

		WithDataDir(dataDir),
		WithRingBufferMaxMem(16*16*1024),
	)

	consumer := NewClient(t, tb2.Addr,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	consumed := ConsumeN(t, consumer, numRecords, 15*time.Second)
	require.Len(t, consumed, numRecords)
	for i, r := range consumed {
		require.Equal(t, fmt.Sprintf("wal-restart-%d", i), string(r.Value), "record %d", i)
		require.Equal(t, int64(i), r.Offset)
	}
}

// TestS3DisasterRecoveryWithoutBackup tests the slow path: metadata.log backup
// is also missing from S3, so topics must be inferred from S3 key structure.
func TestS3DisasterRecoveryWithoutBackup(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	mem := s3store.NewInMemoryS3()
	prefix := "klite/test"
	topic := "s3-dr-no-backup"
	numRecords := 10

	// Phase 1: produce + flush to S3
	tb := StartBroker(t,

		WithDataDir(dataDir),
		WithS3(mem, "test-bucket", prefix),
		WithS3FlushInterval(24*time.Hour),
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
			Value:     []byte(fmt.Sprintf("dr-no-backup-%d", i)),
		}
		ProduceSync(t, producer, rec)
	}

	tb.Stop() // triggers S3 flush + metadata.log upload

	// Verify data objects exist in S3
	keys := mem.Keys()
	var dataKeys []string
	for _, k := range keys {
		if strings.HasSuffix(k, ".obj") {
			dataKeys = append(dataKeys, k)
		}
	}
	require.NotEmpty(t, dataKeys, "expected S3 data objects after flush")

	// Delete the entire data directory (simulate disk loss)
	os.RemoveAll(dataDir)
	os.MkdirAll(dataDir, 0o755)

	// Also delete the metadata.log backup from S3 (simulate backup loss)
	metaKey := prefix + "/metadata.log"
	s3Client := s3store.NewClient(s3store.ClientConfig{
		S3Client: mem,
		Bucket:   "test-bucket",
		Prefix:   prefix,
	})
	err = s3Client.DeleteObject(context.Background(), metaKey)
	require.NoError(t, err)

	// Verify metadata.log is gone from S3
	_, ok := mem.GetRaw(metaKey)
	require.False(t, ok, "metadata.log should be deleted from S3")

	// Phase 2: restart — should infer topics from S3 key structure
	tb2 := StartBroker(t,

		WithDataDir(dataDir),
		WithS3(mem, "test-bucket", prefix),
		WithS3FlushInterval(24*time.Hour),
	)

	// Data should be readable from S3 via the inferred topic
	consumer := NewClient(t, tb2.Addr,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	consumed := ConsumeN(t, consumer, numRecords, 15*time.Second)
	require.Len(t, consumed, numRecords)
	for i, r := range consumed {
		require.Equal(t, fmt.Sprintf("dr-no-backup-%d", i), string(r.Value), "record %d", i)
	}
}
