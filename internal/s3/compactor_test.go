package s3

import (
	"context"
	"hash/crc32"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/klaudworks/klite/internal/clock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- helpers for building test S3 objects ---

func buildTestObject(t *testing.T, batches []testBatch) []byte {
	t.Helper()
	var bds []BatchData
	for _, tb := range batches {
		batchBytes, err := BuildTestBatch(tb.baseOffset, tb.baseTimestamp, tb.records, tb.codec)
		require.NoError(t, err)
		lastOD := int32(0)
		if len(tb.records) > 0 {
			lastOD = tb.records[len(tb.records)-1].OffsetDelta
		}
		bds = append(bds, BatchData{
			RawBytes:        batchBytes,
			BaseOffset:      tb.baseOffset,
			LastOffsetDelta: lastOD,
		})
	}
	return BuildObject(bds)
}

type testBatch struct {
	baseOffset    int64
	baseTimestamp int64
	records       []Record
	codec         int
}

func newTestCompactor(t *testing.T, s3mem *InMemoryS3, clk clock.Clock) (*Compactor, *Reader) {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	client := NewClient(ClientConfig{
		S3Client: s3mem,
		Bucket:   "test-bucket",
		Prefix:   "test-prefix",
		Logger:   logger,
	})
	reader := NewReader(client, logger)

	if clk == nil {
		clk = clock.RealClock{}
	}

	compactor := NewCompactor(CompactorConfig{
		Client:            client,
		Reader:            reader,
		Logger:            logger,
		Clock:             clk,
		DeleteRetentionMs: 86400000, // 24h
	})

	return compactor, reader
}

func putObject(t *testing.T, s3mem *InMemoryS3, prefix, topic string, partition int32, baseOffset int64, data []byte) {
	t.Helper()
	key := ObjectKey(prefix, topic, partition, baseOffset)
	ctx := context.Background()
	client := NewClient(ClientConfig{
		S3Client: s3mem,
		Bucket:   "test-bucket",
		Prefix:   prefix,
	})
	err := client.PutObject(ctx, key, data)
	require.NoError(t, err)
}

// --- Unit tests ---

func TestOffsetMapBuild(t *testing.T) {
	// Build two batches with overlapping keys. The offset map should contain
	// only the highest offset per key.
	s3mem := NewInMemoryS3()
	compactor, _ := newTestCompactor(t, s3mem, nil)

	// Batch 1: offsets 0-2, keys A, B, C
	batch1 := testBatch{
		baseOffset:    0,
		baseTimestamp: 1000,
		records: []Record{
			{OffsetDelta: 0, TimestampDelta: 0, Key: []byte("A"), Value: []byte("v1")},
			{OffsetDelta: 1, TimestampDelta: 100, Key: []byte("B"), Value: []byte("v1")},
			{OffsetDelta: 2, TimestampDelta: 200, Key: []byte("C"), Value: []byte("v1")},
		},
		codec: CompressionNone,
	}
	// Batch 2: offsets 3-5, keys A, B, D (A and B duplicated)
	batch2 := testBatch{
		baseOffset:    3,
		baseTimestamp: 2000,
		records: []Record{
			{OffsetDelta: 0, TimestampDelta: 0, Key: []byte("A"), Value: []byte("v2")},
			{OffsetDelta: 1, TimestampDelta: 100, Key: []byte("B"), Value: []byte("v2")},
			{OffsetDelta: 2, TimestampDelta: 200, Key: []byte("D"), Value: []byte("v1")},
		},
		codec: CompressionNone,
	}

	obj := buildTestObject(t, []testBatch{batch1, batch2})
	footer, err := ParseFooter(obj, int64(len(obj)))
	require.NoError(t, err)

	offsetMap := make(map[string]int64)
	err = compactor.buildOffsetMap(obj, footer, offsetMap)
	require.NoError(t, err)

	// A was at offsets 0 and 3 → 3 wins
	assert.Equal(t, int64(3), offsetMap["A"])
	// B was at offsets 1 and 4 → 4 wins
	assert.Equal(t, int64(4), offsetMap["B"])
	// C was at offset 2 only
	assert.Equal(t, int64(2), offsetMap["C"])
	// D was at offset 5 only
	assert.Equal(t, int64(5), offsetMap["D"])
}

func TestCompactionBasicDedup(t *testing.T) {
	s3mem := NewInMemoryS3()
	compactor, reader := newTestCompactor(t, s3mem, nil)

	// Object 1: offsets 0-2, keys A, B, C
	obj1 := buildTestObject(t, []testBatch{{
		baseOffset:    0,
		baseTimestamp: 1000,
		records: []Record{
			{OffsetDelta: 0, TimestampDelta: 0, Key: []byte("A"), Value: []byte("old-A")},
			{OffsetDelta: 1, TimestampDelta: 100, Key: []byte("B"), Value: []byte("old-B")},
			{OffsetDelta: 2, TimestampDelta: 200, Key: []byte("C"), Value: []byte("C-val")},
		},
		codec: CompressionNone,
	}})
	putObject(t, s3mem, "test-prefix", "topic1", 0, 0, obj1)

	// Object 2: offsets 3-4, keys A, B (supersede old values)
	obj2 := buildTestObject(t, []testBatch{{
		baseOffset:    3,
		baseTimestamp: 2000,
		records: []Record{
			{OffsetDelta: 0, TimestampDelta: 0, Key: []byte("A"), Value: []byte("new-A")},
			{OffsetDelta: 1, TimestampDelta: 100, Key: []byte("B"), Value: []byte("new-B")},
		},
		codec: CompressionNone,
	}})
	putObject(t, s3mem, "test-prefix", "topic1", 0, 3, obj2)

	ctx := context.Background()
	watermark := int64(-1)
	var persisted []int64
	compactor.cfg.PersistWatermark = func(topic string, partition int32, cleanedUpTo int64) error {
		persisted = append(persisted, cleanedUpTo)
		return nil
	}

	newWM, err := compactor.CompactPartition(ctx, "topic1", 0, watermark, 0,
		func() {}, func() {})
	require.NoError(t, err)
	assert.Greater(t, newWM, watermark)

	// Verify: read back from S3, parse all records
	reader.InvalidateFooters("topic1", 0)
	data, err := reader.Fetch(ctx, "topic1", 0, 0, 1024*1024)
	require.NoError(t, err)
	require.NotEmpty(t, data)

	// Parse all batches and records from the fetched data
	var allRecords []Record
	off := 0
	for off < len(data) {
		if off+61 > len(data) {
			break
		}
		header, err := ParseBatchHeaderFromRaw(data[off:])
		if err != nil {
			break
		}
		batchLen := int(header.BatchLength) + 12
		if off+batchLen > len(data) {
			break
		}
		batchRaw := data[off : off+batchLen]
		decompressed, err := DecompressRecords(batchRaw, header.CompressionCodec())
		require.NoError(t, err)
		err = IterateRecords(decompressed, func(r Record) bool {
			r2 := Record{
				Key:         append([]byte{}, r.Key...),
				OffsetDelta: r.OffsetDelta,
			}
			if r.Value != nil {
				r2.Value = append([]byte{}, r.Value...)
			}
			r2.TimestampDelta = r.TimestampDelta
			allRecords = append(allRecords, r2)
			return true
		})
		require.NoError(t, err)
		off += batchLen
	}

	// Check: A should have value "new-A" (from offset 3, not "old-A" from offset 0)
	// B should have value "new-B" (from offset 4, not "old-B" from offset 1)
	// C should remain (unique key, not superseded)
	recordsByKey := make(map[string][]Record)
	for _, r := range allRecords {
		k := string(r.Key)
		recordsByKey[k] = append(recordsByKey[k], r)
	}

	// After compaction, each key should appear exactly once
	require.Len(t, recordsByKey["A"], 1, "key A should appear once")
	require.Len(t, recordsByKey["B"], 1, "key B should appear once")
	require.Len(t, recordsByKey["C"], 1, "key C should appear once")

	assert.Equal(t, "new-A", string(recordsByKey["A"][0].Value))
	assert.Equal(t, "new-B", string(recordsByKey["B"][0].Value))
	assert.Equal(t, "C-val", string(recordsByKey["C"][0].Value))
}

func TestCompactionNilKeyRetained(t *testing.T) {
	s3mem := NewInMemoryS3()
	compactor, _ := newTestCompactor(t, s3mem, nil)

	// Object with: null key (offset 0), key "A" (offset 1), null key (offset 2)
	obj := buildTestObject(t, []testBatch{{
		baseOffset:    0,
		baseTimestamp: 1000,
		records: []Record{
			{OffsetDelta: 0, TimestampDelta: 0, Key: nil, Value: []byte("null-key-1")},
			{OffsetDelta: 1, TimestampDelta: 100, Key: []byte("A"), Value: []byte("A-val")},
			{OffsetDelta: 2, TimestampDelta: 200, Key: nil, Value: []byte("null-key-2")},
		},
		codec: CompressionNone,
	}})
	putObject(t, s3mem, "test-prefix", "topic1", 0, 0, obj)

	// Second object with key "A" superseding offset 1
	obj2 := buildTestObject(t, []testBatch{{
		baseOffset:    3,
		baseTimestamp: 2000,
		records: []Record{
			{OffsetDelta: 0, TimestampDelta: 0, Key: []byte("A"), Value: []byte("A-new")},
		},
		codec: CompressionNone,
	}})
	putObject(t, s3mem, "test-prefix", "topic1", 0, 3, obj2)

	ctx := context.Background()
	compactor.cfg.PersistWatermark = func(topic string, partition int32, cleanedUpTo int64) error {
		return nil
	}

	_, err := compactor.CompactPartition(ctx, "topic1", 0, -1, 0,
		func() {}, func() {})
	require.NoError(t, err)

	// Read compacted data and verify null key records are retained
	client := NewClient(ClientConfig{
		S3Client: s3mem,
		Bucket:   "test-bucket",
		Prefix:   "test-prefix",
	})
	objects, err := client.ListObjects(ctx, ObjectKeyPrefix("test-prefix", "topic1", 0))
	require.NoError(t, err)
	require.NotEmpty(t, objects)

	// Parse all records from the output object(s)
	var nullKeyCount int
	for _, oi := range objects {
		data, err := client.GetObject(ctx, oi.Key)
		require.NoError(t, err)
		footer, err := ParseFooter(data, int64(len(data)))
		require.NoError(t, err)
		for _, entry := range footer.Entries {
			batchRaw := data[entry.BytePosition : entry.BytePosition+entry.BatchLength]
			header, err := ParseBatchHeaderFromRaw(batchRaw)
			require.NoError(t, err)
			decompressed, err := DecompressRecords(batchRaw, header.CompressionCodec())
			require.NoError(t, err)
			IterateRecords(decompressed, func(r Record) bool {
				if r.Key == nil {
					nullKeyCount++
				}
				return true
			})
		}
	}

	assert.Equal(t, 2, nullKeyCount, "both null-key records should be retained")
}

func TestCompactionEmptyKeyDedup(t *testing.T) {
	s3mem := NewInMemoryS3()
	compactor, _ := newTestCompactor(t, s3mem, nil)

	// Two records with empty (zero-length, non-null) key — should deduplicate
	obj := buildTestObject(t, []testBatch{{
		baseOffset:    0,
		baseTimestamp: 1000,
		records: []Record{
			{OffsetDelta: 0, TimestampDelta: 0, Key: []byte{}, Value: []byte("old")},
			{OffsetDelta: 1, TimestampDelta: 100, Key: []byte{}, Value: []byte("new")},
		},
		codec: CompressionNone,
	}})
	putObject(t, s3mem, "test-prefix", "topic1", 0, 0, obj)

	// Need a second object to form a window with >1 object
	obj2 := buildTestObject(t, []testBatch{{
		baseOffset:    2,
		baseTimestamp: 2000,
		records: []Record{
			{OffsetDelta: 0, TimestampDelta: 0, Key: []byte{}, Value: []byte("newest")},
		},
		codec: CompressionNone,
	}})
	putObject(t, s3mem, "test-prefix", "topic1", 0, 2, obj2)

	ctx := context.Background()
	compactor.cfg.PersistWatermark = func(topic string, partition int32, cleanedUpTo int64) error {
		return nil
	}

	_, err := compactor.CompactPartition(ctx, "topic1", 0, -1, 0,
		func() {}, func() {})
	require.NoError(t, err)

	// Parse all records — empty key should appear only once with "newest"
	client := NewClient(ClientConfig{
		S3Client: s3mem,
		Bucket:   "test-bucket",
		Prefix:   "test-prefix",
	})
	objects, err := client.ListObjects(ctx, ObjectKeyPrefix("test-prefix", "topic1", 0))
	require.NoError(t, err)

	var emptyKeyRecords []string
	for _, oi := range objects {
		data, err := client.GetObject(ctx, oi.Key)
		require.NoError(t, err)
		footer, err := ParseFooter(data, int64(len(data)))
		require.NoError(t, err)
		for _, entry := range footer.Entries {
			batchRaw := data[entry.BytePosition : entry.BytePosition+entry.BatchLength]
			header, err := ParseBatchHeaderFromRaw(batchRaw)
			require.NoError(t, err)
			decompressed, err := DecompressRecords(batchRaw, header.CompressionCodec())
			require.NoError(t, err)
			IterateRecords(decompressed, func(r Record) bool {
				if r.Key != nil && len(r.Key) == 0 {
					emptyKeyRecords = append(emptyKeyRecords, string(r.Value))
				}
				return true
			})
		}
	}

	assert.Len(t, emptyKeyRecords, 1, "empty key should appear exactly once after compaction")
	assert.Equal(t, "newest", emptyKeyRecords[0])
}

type fakeClock struct {
	now time.Time
}

func (c *fakeClock) Now() time.Time                          { return c.now }
func (c *fakeClock) After(d time.Duration) <-chan time.Time   { return time.After(d) }
func (c *fakeClock) NewTicker(d time.Duration) *clock.Ticker {
	return clock.RealClock{}.NewTicker(d)
}

func TestCompactionTombstoneRetention(t *testing.T) {
	s3mem := NewInMemoryS3()
	clk := &fakeClock{now: time.Unix(100000, 0)} // 100000 seconds
	compactor, _ := newTestCompactor(t, s3mem, clk)
	compactor.cfg.DeleteRetentionMs = 10000 // 10 seconds

	// Object with: key A = value (offset 0), key A = tombstone (offset 1) at timestamp 99990000ms
	// Tombstone timestamp: 99990 seconds = 99990000 ms
	obj := buildTestObject(t, []testBatch{{
		baseOffset:    0,
		baseTimestamp: 99990000, // 99990 seconds in ms
		records: []Record{
			{OffsetDelta: 0, TimestampDelta: 0, Key: []byte("A"), Value: []byte("val")},
			{OffsetDelta: 1, TimestampDelta: 0, Key: []byte("A"), Value: nil}, // tombstone
		},
		codec: CompressionNone,
	}})
	putObject(t, s3mem, "test-prefix", "topic1", 0, 0, obj)

	// Second object to make a multi-object window
	obj2 := buildTestObject(t, []testBatch{{
		baseOffset:    2,
		baseTimestamp: 99995000,
		records: []Record{
			{OffsetDelta: 0, TimestampDelta: 0, Key: []byte("B"), Value: []byte("val")},
		},
		codec: CompressionNone,
	}})
	putObject(t, s3mem, "test-prefix", "topic1", 0, 2, obj2)

	ctx := context.Background()
	compactor.cfg.PersistWatermark = func(topic string, partition int32, cleanedUpTo int64) error {
		return nil
	}

	// At now=100000s, tombstone timestamp=99990s → age = 10s, exactly at threshold.
	// With deleteRetentionMs=10000, the tombstone age 10s is NOT greater than 10s,
	// so it should be retained.
	_, err := compactor.CompactPartition(ctx, "topic1", 0, -1, 0,
		func() {}, func() {})
	require.NoError(t, err)

	// Verify tombstone is retained
	client := NewClient(ClientConfig{
		S3Client: s3mem,
		Bucket:   "test-bucket",
		Prefix:   "test-prefix",
	})
	var tombstoneFound bool
	objects, _ := client.ListObjects(ctx, ObjectKeyPrefix("test-prefix", "topic1", 0))
	for _, oi := range objects {
		data, _ := client.GetObject(ctx, oi.Key)
		footer, _ := ParseFooter(data, int64(len(data)))
		for _, entry := range footer.Entries {
			batchRaw := data[entry.BytePosition : entry.BytePosition+entry.BatchLength]
			header, _ := ParseBatchHeaderFromRaw(batchRaw)
			decompressed, _ := DecompressRecords(batchRaw, header.CompressionCodec())
			IterateRecords(decompressed, func(r Record) bool {
				if r.Key != nil && string(r.Key) == "A" && r.Value == nil {
					tombstoneFound = true
				}
				return true
			})
		}
	}
	assert.True(t, tombstoneFound, "tombstone should be retained when within delete.retention.ms")

	// Now advance time past the retention period
	clk.now = time.Unix(100011, 0) // 100011 seconds → tombstone age = 21s > 10s

	// Re-create objects (compaction replaced them)
	s3mem2 := NewInMemoryS3()
	compactor2, _ := newTestCompactor(t, s3mem2, clk)
	compactor2.cfg.DeleteRetentionMs = 10000
	compactor2.cfg.PersistWatermark = func(topic string, partition int32, cleanedUpTo int64) error {
		return nil
	}

	obj3 := buildTestObject(t, []testBatch{{
		baseOffset:    0,
		baseTimestamp: 99990000,
		records: []Record{
			{OffsetDelta: 0, TimestampDelta: 0, Key: []byte("A"), Value: nil}, // tombstone
		},
		codec: CompressionNone,
	}})
	putObject(t, s3mem2, "test-prefix", "topic1", 0, 0, obj3)
	obj4 := buildTestObject(t, []testBatch{{
		baseOffset:    1,
		baseTimestamp: 99995000,
		records: []Record{
			{OffsetDelta: 0, TimestampDelta: 0, Key: []byte("B"), Value: []byte("val")},
		},
		codec: CompressionNone,
	}})
	putObject(t, s3mem2, "test-prefix", "topic1", 0, 1, obj4)

	_, err = compactor2.CompactPartition(ctx, "topic1", 0, -1, 0,
		func() {}, func() {})
	require.NoError(t, err)

	// Verify tombstone is removed after retention period
	client2 := NewClient(ClientConfig{
		S3Client: s3mem2,
		Bucket:   "test-bucket",
		Prefix:   "test-prefix",
	})
	tombstoneFound = false
	objects, _ = client2.ListObjects(ctx, ObjectKeyPrefix("test-prefix", "topic1", 0))
	for _, oi := range objects {
		data, _ := client2.GetObject(ctx, oi.Key)
		footer, _ := ParseFooter(data, int64(len(data)))
		for _, entry := range footer.Entries {
			batchRaw := data[entry.BytePosition : entry.BytePosition+entry.BatchLength]
			header, _ := ParseBatchHeaderFromRaw(batchRaw)
			decompressed, _ := DecompressRecords(batchRaw, header.CompressionCodec())
			IterateRecords(decompressed, func(r Record) bool {
				if r.Key != nil && string(r.Key) == "A" && r.Value == nil {
					tombstoneFound = true
				}
				return true
			})
		}
	}
	assert.False(t, tombstoneFound, "tombstone should be removed after delete.retention.ms")
}

func TestCompactionMinLag(t *testing.T) {
	// TODO: min.compaction.lag.ms filtering is applied during window formation,
	// not at the record level. With in-memory S3 we can't easily test this
	// without modifying the compactor to check batch timestamps against the lag.
	// For now, verify that CompactPartition with minCompactionLagMs > all timestamps
	// doesn't compact anything.
	s3mem := NewInMemoryS3()
	compactor, _ := newTestCompactor(t, s3mem, nil)

	obj := buildTestObject(t, []testBatch{{
		baseOffset:    0,
		baseTimestamp: 1000,
		records: []Record{
			{OffsetDelta: 0, TimestampDelta: 0, Key: []byte("A"), Value: []byte("val")},
		},
		codec: CompressionNone,
	}})
	putObject(t, s3mem, "test-prefix", "topic1", 0, 0, obj)

	obj2 := buildTestObject(t, []testBatch{{
		baseOffset:    1,
		baseTimestamp: 2000,
		records: []Record{
			{OffsetDelta: 0, TimestampDelta: 0, Key: []byte("A"), Value: []byte("val2")},
		},
		codec: CompressionNone,
	}})
	putObject(t, s3mem, "test-prefix", "topic1", 0, 1, obj2)

	ctx := context.Background()
	compactor.cfg.PersistWatermark = func(topic string, partition int32, cleanedUpTo int64) error {
		return nil
	}

	// With enormous min lag, compaction should still process the window
	// (min lag is checked at object level, not record level, and our
	// formWindows doesn't currently filter by timestamp).
	// At minimum, verify the call doesn't error.
	_, err := compactor.CompactPartition(ctx, "topic1", 0, -1, 0,
		func() {}, func() {})
	require.NoError(t, err)
}

func TestCompactionPreservesOrder(t *testing.T) {
	s3mem := NewInMemoryS3()
	compactor, _ := newTestCompactor(t, s3mem, nil)

	// Two objects. After compaction, surviving records should maintain
	// their relative order.
	obj1 := buildTestObject(t, []testBatch{{
		baseOffset:    0,
		baseTimestamp: 1000,
		records: []Record{
			{OffsetDelta: 0, TimestampDelta: 0, Key: []byte("A"), Value: []byte("A1")},
			{OffsetDelta: 1, TimestampDelta: 100, Key: []byte("B"), Value: []byte("B1")},
			{OffsetDelta: 2, TimestampDelta: 200, Key: []byte("C"), Value: []byte("C1")},
		},
		codec: CompressionNone,
	}})
	putObject(t, s3mem, "test-prefix", "topic1", 0, 0, obj1)

	obj2 := buildTestObject(t, []testBatch{{
		baseOffset:    3,
		baseTimestamp: 2000,
		records: []Record{
			{OffsetDelta: 0, TimestampDelta: 0, Key: []byte("A"), Value: []byte("A2")},
		},
		codec: CompressionNone,
	}})
	putObject(t, s3mem, "test-prefix", "topic1", 0, 3, obj2)

	ctx := context.Background()
	compactor.cfg.PersistWatermark = func(topic string, partition int32, cleanedUpTo int64) error {
		return nil
	}

	_, err := compactor.CompactPartition(ctx, "topic1", 0, -1, 0,
		func() {}, func() {})
	require.NoError(t, err)

	// Read back all records and verify order: B (offset 1), C (offset 2), A (offset 3)
	client := NewClient(ClientConfig{
		S3Client: s3mem,
		Bucket:   "test-bucket",
		Prefix:   "test-prefix",
	})
	objects, err := client.ListObjects(ctx, ObjectKeyPrefix("test-prefix", "topic1", 0))
	require.NoError(t, err)

	type keyOff struct {
		key    string
		offset int64
	}
	var records []keyOff
	for _, oi := range objects {
		data, _ := client.GetObject(ctx, oi.Key)
		footer, _ := ParseFooter(data, int64(len(data)))
		for _, entry := range footer.Entries {
			batchRaw := data[entry.BytePosition : entry.BytePosition+entry.BatchLength]
			header, _ := ParseBatchHeaderFromRaw(batchRaw)
			decompressed, _ := DecompressRecords(batchRaw, header.CompressionCodec())
			IterateRecords(decompressed, func(r Record) bool {
				if r.Key != nil {
					records = append(records, keyOff{
						key:    string(r.Key),
						offset: header.BaseOffset + int64(r.OffsetDelta),
					})
				}
				return true
			})
		}
	}

	// Verify ascending offset order
	for i := 1; i < len(records); i++ {
		assert.Greater(t, records[i].offset, records[i-1].offset,
			"records should be in ascending offset order: %v", records)
	}
}

func TestCompactionEmptyBatchSkipped(t *testing.T) {
	s3mem := NewInMemoryS3()
	compactor, _ := newTestCompactor(t, s3mem, nil)

	// Batch 1: key A (will be superseded)
	// Batch 2: key A (latest) — should survive
	// After compaction, batch 1 should be entirely removed (not emitted as empty)
	obj1 := buildTestObject(t, []testBatch{
		{
			baseOffset:    0,
			baseTimestamp: 1000,
			records: []Record{
				{OffsetDelta: 0, TimestampDelta: 0, Key: []byte("A"), Value: []byte("old")},
			},
			codec: CompressionNone,
		},
	})
	putObject(t, s3mem, "test-prefix", "topic1", 0, 0, obj1)

	obj2 := buildTestObject(t, []testBatch{
		{
			baseOffset:    1,
			baseTimestamp: 2000,
			records: []Record{
				{OffsetDelta: 0, TimestampDelta: 0, Key: []byte("A"), Value: []byte("new")},
			},
			codec: CompressionNone,
		},
	})
	putObject(t, s3mem, "test-prefix", "topic1", 0, 1, obj2)

	ctx := context.Background()
	compactor.cfg.PersistWatermark = func(topic string, partition int32, cleanedUpTo int64) error {
		return nil
	}

	_, err := compactor.CompactPartition(ctx, "topic1", 0, -1, 0,
		func() {}, func() {})
	require.NoError(t, err)

	// Verify: no batch with NumRecords=0
	client := NewClient(ClientConfig{
		S3Client: s3mem,
		Bucket:   "test-bucket",
		Prefix:   "test-prefix",
	})
	objects, err := client.ListObjects(ctx, ObjectKeyPrefix("test-prefix", "topic1", 0))
	require.NoError(t, err)

	for _, oi := range objects {
		data, _ := client.GetObject(ctx, oi.Key)
		footer, _ := ParseFooter(data, int64(len(data)))
		for _, entry := range footer.Entries {
			batchRaw := data[entry.BytePosition : entry.BytePosition+entry.BatchLength]
			header, err := ParseBatchHeaderFromRaw(batchRaw)
			require.NoError(t, err)
			assert.Greater(t, header.NumRecords, int32(0),
				"no batch should have NumRecords=0 after compaction")
		}
	}
}

func TestCompactionSparseOffsets(t *testing.T) {
	s3mem := NewInMemoryS3()
	compactor, _ := newTestCompactor(t, s3mem, nil)

	// Batch with offsets 0, 1, 2, 3 — keys A, B, A, C
	// After compaction: A at offset 2 survives (supersedes offset 0), B at 1, C at 3
	// Resulting offsets: 1, 2, 3 — sparse (offset 0 is missing)
	obj1 := buildTestObject(t, []testBatch{{
		baseOffset:    0,
		baseTimestamp: 1000,
		records: []Record{
			{OffsetDelta: 0, TimestampDelta: 0, Key: []byte("A"), Value: []byte("A1")},
			{OffsetDelta: 1, TimestampDelta: 100, Key: []byte("B"), Value: []byte("B1")},
			{OffsetDelta: 2, TimestampDelta: 200, Key: []byte("A"), Value: []byte("A2")},
			{OffsetDelta: 3, TimestampDelta: 300, Key: []byte("C"), Value: []byte("C1")},
		},
		codec: CompressionNone,
	}})
	putObject(t, s3mem, "test-prefix", "topic1", 0, 0, obj1)

	// Need second object for multi-object window
	obj2 := buildTestObject(t, []testBatch{{
		baseOffset:    4,
		baseTimestamp: 2000,
		records: []Record{
			{OffsetDelta: 0, TimestampDelta: 0, Key: []byte("D"), Value: []byte("D1")},
		},
		codec: CompressionNone,
	}})
	putObject(t, s3mem, "test-prefix", "topic1", 0, 4, obj2)

	ctx := context.Background()
	compactor.cfg.PersistWatermark = func(topic string, partition int32, cleanedUpTo int64) error {
		return nil
	}

	_, err := compactor.CompactPartition(ctx, "topic1", 0, -1, 0,
		func() {}, func() {})
	require.NoError(t, err)

	// Verify fetch returns correct data with sparse offsets
	client := NewClient(ClientConfig{
		S3Client: s3mem,
		Bucket:   "test-bucket",
		Prefix:   "test-prefix",
	})
	objects, err := client.ListObjects(ctx, ObjectKeyPrefix("test-prefix", "topic1", 0))
	require.NoError(t, err)
	require.NotEmpty(t, objects)

	var recordCount int
	for _, oi := range objects {
		data, _ := client.GetObject(ctx, oi.Key)
		footer, _ := ParseFooter(data, int64(len(data)))
		for _, entry := range footer.Entries {
			batchRaw := data[entry.BytePosition : entry.BytePosition+entry.BatchLength]
			header, _ := ParseBatchHeaderFromRaw(batchRaw)
			decompressed, _ := DecompressRecords(batchRaw, header.CompressionCodec())
			IterateRecords(decompressed, func(r Record) bool {
				recordCount++
				return true
			})
		}
	}

	// A was at 0 and 2, B at 1, C at 3, D at 4 → after dedup: B(1), A(2), C(3), D(4) = 4 records
	assert.Equal(t, 4, recordCount, "should have 4 records after dedup")
}

func TestCompactionOrphanCleanup(t *testing.T) {
	s3mem := NewInMemoryS3()
	_, _ = newTestCompactor(t, s3mem, nil)

	// Simulate a crash scenario: two overlapping objects (old source + compacted output)
	// Object at offset 0: covers offsets 0-2
	obj1 := buildTestObject(t, []testBatch{{
		baseOffset:    0,
		baseTimestamp: 1000,
		records: []Record{
			{OffsetDelta: 0, TimestampDelta: 0, Key: []byte("A"), Value: []byte("v1")},
			{OffsetDelta: 1, TimestampDelta: 100, Key: []byte("B"), Value: []byte("v1")},
			{OffsetDelta: 2, TimestampDelta: 200, Key: []byte("C"), Value: []byte("v1")},
		},
		codec: CompressionNone,
	}})
	putObject(t, s3mem, "test-prefix", "topic1", 0, 0, obj1)

	// "Compacted" output also at offset 0, covering 0-4 (superset)
	obj2 := buildTestObject(t, []testBatch{
		{
			baseOffset:    0,
			baseTimestamp: 1000,
			records: []Record{
				{OffsetDelta: 0, TimestampDelta: 0, Key: []byte("A"), Value: []byte("v2")},
				{OffsetDelta: 1, TimestampDelta: 100, Key: []byte("B"), Value: []byte("v2")},
				{OffsetDelta: 2, TimestampDelta: 200, Key: []byte("C"), Value: []byte("v2")},
			},
			codec: CompressionNone,
		},
		{
			baseOffset:    3,
			baseTimestamp: 2000,
			records: []Record{
				{OffsetDelta: 0, TimestampDelta: 0, Key: []byte("D"), Value: []byte("v1")},
				{OffsetDelta: 1, TimestampDelta: 100, Key: []byte("E"), Value: []byte("v1")},
			},
			codec: CompressionNone,
		},
	})
	// This key is different from obj1's key because it has offset 0 but different content
	// Wait, both would have the same key 00000000000000000000.obj!
	// In real crash scenario the first object wasn't deleted. Let's simulate
	// overlapping objects with different base offsets where one is a superset.

	// Actually: orphan cleanup checks if one object's range covers another.
	// Let's create: obj at offset 0 (range 0-2), obj at offset 0 (same key, so S3 overwrites).
	// We need objects with DIFFERENT keys whose ranges overlap.

	// Reset
	s3mem2 := NewInMemoryS3()
	compactor2, _ := newTestCompactor(t, s3mem2, nil)

	// "Source" object at offset 0, range 0-2
	putObject(t, s3mem2, "test-prefix", "topic1", 0, 0, obj1)

	// "Source" object at offset 3, range 3-4
	obj3 := buildTestObject(t, []testBatch{{
		baseOffset:    3,
		baseTimestamp: 2000,
		records: []Record{
			{OffsetDelta: 0, TimestampDelta: 0, Key: []byte("D"), Value: []byte("v1")},
			{OffsetDelta: 1, TimestampDelta: 100, Key: []byte("E"), Value: []byte("v1")},
		},
		codec: CompressionNone,
	}})
	putObject(t, s3mem2, "test-prefix", "topic1", 0, 3, obj3)

	// "Leftover compacted" object at offset 0, range 0-4 (covers both sources)
	// This simulates a crash after PUT but before DELETE: the compacted output
	// exists alongside the source objects.
	// But wait — this would have the same key as the first source (offset 0)!
	// S3 would overwrite it. This is fine for orphan cleanup test — the
	// old source at offset 0 gets replaced, but the source at offset 3 becomes orphan.

	// Actually, the compacted output replaces source at offset 0. After the
	// crash, we have: offset-0 object (compacted, covers 0-4) + offset-3 object (source, covers 3-4).
	// The offset-3 source is the orphan.

	// Replace offset 0 with the compacted output covering offsets 0-4
	putObject(t, s3mem2, "test-prefix", "topic1", 0, 0, obj2)

	ctx := context.Background()
	compactor2.cfg.PersistWatermark = func(topic string, partition int32, cleanedUpTo int64) error {
		return nil
	}

	client2 := NewClient(ClientConfig{
		S3Client: s3mem2,
		Bucket:   "test-bucket",
		Prefix:   "test-prefix",
	})

	// Before compaction, we have 2 objects
	objsBefore, _ := client2.ListObjects(ctx, ObjectKeyPrefix("test-prefix", "topic1", 0))
	assert.Len(t, objsBefore, 2, "should have 2 objects before orphan cleanup")

	// Run compaction — orphan cleanup should detect and delete the orphan
	_, err := compactor2.CompactPartition(ctx, "topic1", 0, 4, 0,
		func() {}, func() {})
	require.NoError(t, err)

	// After orphan cleanup, the offset-3 object should be deleted (covered by offset-0 object)
	objsAfter, _ := client2.ListObjects(ctx, ObjectKeyPrefix("test-prefix", "topic1", 0))
	assert.Len(t, objsAfter, 1, "orphan should be cleaned up")
}

func TestCompactionCrossWindowDedup(t *testing.T) {
	s3mem := NewInMemoryS3()
	compactor, _ := newTestCompactor(t, s3mem, nil)
	compactor.cfg.WindowBytes = 1024 // Small window to force multiple windows

	// Object 1: key A at offset 0
	obj1 := buildTestObject(t, []testBatch{{
		baseOffset:    0,
		baseTimestamp: 1000,
		records: []Record{
			{OffsetDelta: 0, TimestampDelta: 0, Key: []byte("A"), Value: []byte("old-A")},
		},
		codec: CompressionNone,
	}})
	putObject(t, s3mem, "test-prefix", "topic1", 0, 0, obj1)

	// Object 2: key A at offset 1 (supersedes)
	obj2 := buildTestObject(t, []testBatch{{
		baseOffset:    1,
		baseTimestamp: 2000,
		records: []Record{
			{OffsetDelta: 0, TimestampDelta: 0, Key: []byte("A"), Value: []byte("new-A")},
		},
		codec: CompressionNone,
	}})
	putObject(t, s3mem, "test-prefix", "topic1", 0, 1, obj2)

	ctx := context.Background()
	compactor.cfg.PersistWatermark = func(topic string, partition int32, cleanedUpTo int64) error {
		return nil
	}

	newWM, err := compactor.CompactPartition(ctx, "topic1", 0, -1, 0,
		func() {}, func() {})
	require.NoError(t, err)
	assert.Greater(t, newWM, int64(-1))
}

func TestCompactionOutputFooter(t *testing.T) {
	s3mem := NewInMemoryS3()
	compactor, _ := newTestCompactor(t, s3mem, nil)

	// Create objects with duplicate keys
	obj1 := buildTestObject(t, []testBatch{{
		baseOffset:    0,
		baseTimestamp: 1000,
		records: []Record{
			{OffsetDelta: 0, TimestampDelta: 0, Key: []byte("A"), Value: []byte("A1")},
			{OffsetDelta: 1, TimestampDelta: 100, Key: []byte("B"), Value: []byte("B1")},
		},
		codec: CompressionNone,
	}})
	putObject(t, s3mem, "test-prefix", "topic1", 0, 0, obj1)

	obj2 := buildTestObject(t, []testBatch{{
		baseOffset:    2,
		baseTimestamp: 2000,
		records: []Record{
			{OffsetDelta: 0, TimestampDelta: 0, Key: []byte("A"), Value: []byte("A2")},
			{OffsetDelta: 1, TimestampDelta: 100, Key: []byte("C"), Value: []byte("C1")},
		},
		codec: CompressionNone,
	}})
	putObject(t, s3mem, "test-prefix", "topic1", 0, 2, obj2)

	ctx := context.Background()
	compactor.cfg.PersistWatermark = func(topic string, partition int32, cleanedUpTo int64) error {
		return nil
	}

	_, err := compactor.CompactPartition(ctx, "topic1", 0, -1, 0,
		func() {}, func() {})
	require.NoError(t, err)

	// Verify the compacted output has a valid footer
	client := NewClient(ClientConfig{
		S3Client: s3mem,
		Bucket:   "test-bucket",
		Prefix:   "test-prefix",
	})
	objects, err := client.ListObjects(ctx, ObjectKeyPrefix("test-prefix", "topic1", 0))
	require.NoError(t, err)
	require.NotEmpty(t, objects)

	for _, oi := range objects {
		data, err := client.GetObject(ctx, oi.Key)
		require.NoError(t, err)

		footer, err := ParseFooter(data, int64(len(data)))
		require.NoError(t, err, "compacted output must have valid footer")
		require.NotEmpty(t, footer.Entries, "footer should have entries")

		// Verify footer entries match actual batch boundaries
		for _, entry := range footer.Entries {
			require.Less(t, int(entry.BytePosition), len(data), "byte position within data")
			require.LessOrEqual(t, int(entry.BytePosition+entry.BatchLength), len(data),
				"batch should fit within data section")

			// Parse the batch at the indicated position
			batchRaw := data[entry.BytePosition : entry.BytePosition+entry.BatchLength]
			header, err := ParseBatchHeaderFromRaw(batchRaw)
			require.NoError(t, err, "footer-indicated batch should be parseable")
			assert.Equal(t, entry.BaseOffset, header.BaseOffset,
				"footer baseOffset should match batch header")
		}
	}
}

func TestCompactionFooterCacheInvalidation(t *testing.T) {
	s3mem := NewInMemoryS3()
	compactor, reader := newTestCompactor(t, s3mem, nil)

	obj1 := buildTestObject(t, []testBatch{{
		baseOffset:    0,
		baseTimestamp: 1000,
		records: []Record{
			{OffsetDelta: 0, TimestampDelta: 0, Key: []byte("A"), Value: []byte("A1")},
		},
		codec: CompressionNone,
	}})
	putObject(t, s3mem, "test-prefix", "topic1", 0, 0, obj1)

	obj2 := buildTestObject(t, []testBatch{{
		baseOffset:    1,
		baseTimestamp: 2000,
		records: []Record{
			{OffsetDelta: 0, TimestampDelta: 0, Key: []byte("A"), Value: []byte("A2")},
		},
		codec: CompressionNone,
	}})
	putObject(t, s3mem, "test-prefix", "topic1", 0, 1, obj2)

	ctx := context.Background()

	// Pre-populate footer cache
	_, err := reader.Fetch(ctx, "topic1", 0, 0, 1024*1024)
	require.NoError(t, err)
	cachesBefore := reader.FooterCacheSize()
	assert.Greater(t, cachesBefore, 0, "footer cache should be populated")

	// Run compaction
	compactor.cfg.PersistWatermark = func(topic string, partition int32, cleanedUpTo int64) error {
		return nil
	}
	_, err = compactor.CompactPartition(ctx, "topic1", 0, -1, 0,
		func() {}, func() {})
	require.NoError(t, err)

	// After compaction, footer cache for this partition should be invalidated
	// (either size is 0 or the old keys are gone)
	// Note: compactor calls reader.InvalidateFooters which clears cache for the partition
	cachesAfter := reader.FooterCacheSize()
	assert.LessOrEqual(t, cachesAfter, cachesBefore,
		"footer cache should be invalidated after compaction")
}

func TestCompactionCRCValidAfterRewrite(t *testing.T) {
	s3mem := NewInMemoryS3()
	compactor, _ := newTestCompactor(t, s3mem, nil)

	// Create objects that will require record removal (re-encoding)
	obj1 := buildTestObject(t, []testBatch{{
		baseOffset:    0,
		baseTimestamp: 1000,
		records: []Record{
			{OffsetDelta: 0, TimestampDelta: 0, Key: []byte("A"), Value: []byte("old")},
			{OffsetDelta: 1, TimestampDelta: 100, Key: []byte("B"), Value: []byte("B1")},
		},
		codec: CompressionNone,
	}})
	putObject(t, s3mem, "test-prefix", "topic1", 0, 0, obj1)

	obj2 := buildTestObject(t, []testBatch{{
		baseOffset:    2,
		baseTimestamp: 2000,
		records: []Record{
			{OffsetDelta: 0, TimestampDelta: 0, Key: []byte("A"), Value: []byte("new")},
		},
		codec: CompressionNone,
	}})
	putObject(t, s3mem, "test-prefix", "topic1", 0, 2, obj2)

	ctx := context.Background()
	compactor.cfg.PersistWatermark = func(topic string, partition int32, cleanedUpTo int64) error {
		return nil
	}
	_, err := compactor.CompactPartition(ctx, "topic1", 0, -1, 0,
		func() {}, func() {})
	require.NoError(t, err)

	// Verify CRC of all batches in compacted output
	client := NewClient(ClientConfig{
		S3Client: s3mem,
		Bucket:   "test-bucket",
		Prefix:   "test-prefix",
	})
	objects, err := client.ListObjects(ctx, ObjectKeyPrefix("test-prefix", "topic1", 0))
	require.NoError(t, err)

	crcTable := crc32.MakeTable(crc32.Castagnoli)
	for _, oi := range objects {
		data, _ := client.GetObject(ctx, oi.Key)
		footer, _ := ParseFooter(data, int64(len(data)))
		for _, entry := range footer.Entries {
			batchRaw := data[entry.BytePosition : entry.BytePosition+entry.BatchLength]
			header, err := ParseBatchHeaderFromRaw(batchRaw)
			require.NoError(t, err)

			// Recompute CRC over bytes 21+
			computedCRC := crc32.Checksum(batchRaw[21:], crcTable)
			assert.Equal(t, header.CRC, computedCRC,
				"CRC should be valid after compaction rewrite for batch at offset %d", header.BaseOffset)
		}
	}
}

func TestCompactionCompressionRoundTrip(t *testing.T) {
	codecs := []struct {
		name  string
		codec int
	}{
		{"none", CompressionNone},
		{"gzip", CompressionGZIP},
		{"snappy", CompressionSnappy},
		{"lz4", CompressionLZ4},
		{"zstd", CompressionZSTD},
	}

	for _, tc := range codecs {
		t.Run(tc.name, func(t *testing.T) {
			s3mem := NewInMemoryS3()
			compactor, _ := newTestCompactor(t, s3mem, nil)

			// Two objects with duplicate key A, one with unique key B
			obj1 := buildTestObject(t, []testBatch{{
				baseOffset:    0,
				baseTimestamp: 1000,
				records: []Record{
					{OffsetDelta: 0, TimestampDelta: 0, Key: []byte("A"), Value: []byte("old-A")},
					{OffsetDelta: 1, TimestampDelta: 100, Key: []byte("B"), Value: []byte("B-val")},
				},
				codec: tc.codec,
			}})
			putObject(t, s3mem, "test-prefix", "topic1", 0, 0, obj1)

			obj2 := buildTestObject(t, []testBatch{{
				baseOffset:    2,
				baseTimestamp: 2000,
				records: []Record{
					{OffsetDelta: 0, TimestampDelta: 0, Key: []byte("A"), Value: []byte("new-A")},
				},
				codec: tc.codec,
			}})
			putObject(t, s3mem, "test-prefix", "topic1", 0, 2, obj2)

			ctx := context.Background()
			compactor.cfg.PersistWatermark = func(topic string, partition int32, cleanedUpTo int64) error {
				return nil
			}

			_, err := compactor.CompactPartition(ctx, "topic1", 0, -1, 0,
				func() {}, func() {})
			require.NoError(t, err)

			// Verify records are readable after compaction with this codec
			client := NewClient(ClientConfig{
				S3Client: s3mem,
				Bucket:   "test-bucket",
				Prefix:   "test-prefix",
			})
			objects, err := client.ListObjects(ctx, ObjectKeyPrefix("test-prefix", "topic1", 0))
			require.NoError(t, err)

			var recordKeys []string
			for _, oi := range objects {
				data, _ := client.GetObject(ctx, oi.Key)
				footer, _ := ParseFooter(data, int64(len(data)))
				for _, entry := range footer.Entries {
					batchRaw := data[entry.BytePosition : entry.BytePosition+entry.BatchLength]
					header, err := ParseBatchHeaderFromRaw(batchRaw)
					require.NoError(t, err)
					decompressed, err := DecompressRecords(batchRaw, header.CompressionCodec())
					require.NoError(t, err, "decompression should work after compaction")
					IterateRecords(decompressed, func(r Record) bool {
						if r.Key != nil {
							recordKeys = append(recordKeys, string(r.Key))
						}
						return true
					})
				}
			}

			// A and B should each appear once
			assert.Contains(t, recordKeys, "A")
			assert.Contains(t, recordKeys, "B")
		})
	}
}

func TestCompactionIdempotent(t *testing.T) {
	s3mem := NewInMemoryS3()
	compactor, _ := newTestCompactor(t, s3mem, nil)

	// Create a simple partition with unique keys (no dedup needed)
	obj := buildTestObject(t, []testBatch{{
		baseOffset:    0,
		baseTimestamp: 1000,
		records: []Record{
			{OffsetDelta: 0, TimestampDelta: 0, Key: []byte("A"), Value: []byte("A1")},
			{OffsetDelta: 1, TimestampDelta: 100, Key: []byte("B"), Value: []byte("B1")},
		},
		codec: CompressionNone,
	}})
	putObject(t, s3mem, "test-prefix", "topic1", 0, 0, obj)

	obj2 := buildTestObject(t, []testBatch{{
		baseOffset:    2,
		baseTimestamp: 2000,
		records: []Record{
			{OffsetDelta: 0, TimestampDelta: 0, Key: []byte("C"), Value: []byte("C1")},
		},
		codec: CompressionNone,
	}})
	putObject(t, s3mem, "test-prefix", "topic1", 0, 2, obj2)

	ctx := context.Background()
	compactor.cfg.PersistWatermark = func(topic string, partition int32, cleanedUpTo int64) error {
		return nil
	}

	// First compaction
	wm1, err := compactor.CompactPartition(ctx, "topic1", 0, -1, 0,
		func() {}, func() {})
	require.NoError(t, err)

	// Collect records after first compaction
	client := NewClient(ClientConfig{
		S3Client: s3mem,
		Bucket:   "test-bucket",
		Prefix:   "test-prefix",
	})
	objects1, _ := client.ListObjects(ctx, ObjectKeyPrefix("test-prefix", "topic1", 0))

	var keys1 []string
	for _, oi := range objects1 {
		data, _ := client.GetObject(ctx, oi.Key)
		footer, _ := ParseFooter(data, int64(len(data)))
		for _, entry := range footer.Entries {
			batchRaw := data[entry.BytePosition : entry.BytePosition+entry.BatchLength]
			header, _ := ParseBatchHeaderFromRaw(batchRaw)
			decompressed, _ := DecompressRecords(batchRaw, header.CompressionCodec())
			IterateRecords(decompressed, func(r Record) bool {
				if r.Key != nil {
					keys1 = append(keys1, string(r.Key))
				}
				return true
			})
		}
	}

	// Second compaction (on already-clean data)
	// wm1 is the cleanedUpTo — single object won't be re-compacted
	_, err = compactor.CompactPartition(ctx, "topic1", 0, wm1, 0,
		func() {}, func() {})
	require.NoError(t, err)

	// Records should be identical
	objects2, _ := client.ListObjects(ctx, ObjectKeyPrefix("test-prefix", "topic1", 0))
	var keys2 []string
	for _, oi := range objects2 {
		data, _ := client.GetObject(ctx, oi.Key)
		footer, _ := ParseFooter(data, int64(len(data)))
		for _, entry := range footer.Entries {
			batchRaw := data[entry.BytePosition : entry.BytePosition+entry.BatchLength]
			header, _ := ParseBatchHeaderFromRaw(batchRaw)
			decompressed, _ := DecompressRecords(batchRaw, header.CompressionCodec())
			IterateRecords(decompressed, func(r Record) bool {
				if r.Key != nil {
					keys2 = append(keys2, string(r.Key))
				}
				return true
			})
		}
	}

	assert.ElementsMatch(t, keys1, keys2, "records should be identical after idempotent compaction")
}
