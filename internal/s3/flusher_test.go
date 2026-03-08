package s3

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	s3svc "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/klaudworks/klite/internal/chunk"
	"github.com/klaudworks/klite/internal/clock"
	"github.com/klaudworks/klite/internal/wal"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Test helpers ---

// stubPartitionProvider implements PartitionProvider with a fixed set of FlushPartitions.
type stubPartitionProvider struct {
	partitions []FlushPartition
}

func (s *stubPartitionProvider) FlushablePartitions() []FlushPartition {
	return s.partitions
}

// stubWatermarkProvider implements WatermarkProvider with fixed data.
type stubWatermarkProvider struct {
	watermarks []PartitionWatermarkInfo
}

func (s *stubWatermarkProvider) AllPartitionWatermarks() []PartitionWatermarkInfo {
	return s.watermarks
}

// makeTestChunk creates a chunk containing the given batches. Each batch is a
// valid minimal RecordBatch header (80 bytes). The chunk data buffer is sized
// to hold all batches.
func makeTestChunk(pool *chunk.Pool, batches []chunk.ChunkBatch) *chunk.Chunk {
	c := pool.Acquire()
	if c == nil {
		panic("pool exhausted in test setup")
	}
	for i := range batches {
		b := &batches[i]
		raw := makeMinimalBatch(b.BaseOffset, b.LastOffsetDelta)
		b.Offset = c.Used
		b.Size = len(raw)
		copy(c.Data[c.Used:], raw)
		c.Used += len(raw)
	}
	c.Batches = append(c.Batches[:0], batches...)
	return c
}

// failingClient wraps an InMemoryS3 but can be made to fail on PutObject.
type failingClient struct {
	mu       sync.Mutex
	failNext int // number of PutObject calls to fail before succeeding
	err      error
	calls    int
}

func (f *failingClient) shouldFail() (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.failNext > 0 {
		f.failNext--
		return true, f.err
	}
	return false, nil
}

// newTestFlusher creates a Flusher wired to an InMemoryS3 and FakeClock for
// unit testing. Returns the flusher, in-memory S3, clock, and chunk pool.
func newTestFlusher(t *testing.T, provider PartitionProvider) (*Flusher, *InMemoryS3, *clock.FakeClock, *chunk.Pool) {
	t.Helper()

	mem := NewInMemoryS3()
	clk := clock.NewFakeClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	mem.SetClock(clk)

	client := NewClient(ClientConfig{
		S3Client: mem,
		Bucket:   "test-bucket",
		Prefix:   "test-prefix",
	})

	pool := chunk.NewPool(64*1024*1024, 1024*1024) // 64 MiB budget, 1 MiB chunks
	pool.SetClock(clk)

	flusher := NewFlusher(FlusherConfig{
		Client:           client,
		FlushInterval:    60 * time.Second,
		CheckInterval:    1 * time.Second,
		TargetObjectSize: 1024 * 1024, // 1 MiB
		UploadConcurrency: 4,
		Clock:            clk,
	}, provider)

	return flusher, mem, clk, pool
}

// --- collectBatchesFromChunks tests ---

func TestCollectBatchesFromChunks_Basic(t *testing.T) {
	t.Parallel()

	pool := chunk.NewPool(10*1024*1024, 1024*1024)
	c := makeTestChunk(pool, []chunk.ChunkBatch{
		{BaseOffset: 0, LastOffsetDelta: 4},
		{BaseOffset: 5, LastOffsetDelta: 2},
	})

	batches := collectBatchesFromChunks([]*chunk.Chunk{c})
	require.Len(t, batches, 2)

	assert.Equal(t, int64(0), batches[0].BaseOffset)
	assert.Equal(t, int32(4), batches[0].LastOffsetDelta)
	assert.Len(t, batches[0].RawBytes, 80)

	assert.Equal(t, int64(5), batches[1].BaseOffset)
	assert.Equal(t, int32(2), batches[1].LastOffsetDelta)
}

func TestCollectBatchesFromChunks_MultipleChunks(t *testing.T) {
	t.Parallel()

	pool := chunk.NewPool(10*1024*1024, 1024*1024)
	c1 := makeTestChunk(pool, []chunk.ChunkBatch{
		{BaseOffset: 0, LastOffsetDelta: 9},
	})
	c2 := makeTestChunk(pool, []chunk.ChunkBatch{
		{BaseOffset: 10, LastOffsetDelta: 4},
		{BaseOffset: 15, LastOffsetDelta: 2},
	})

	batches := collectBatchesFromChunks([]*chunk.Chunk{c1, c2})
	require.Len(t, batches, 3)
	assert.Equal(t, int64(0), batches[0].BaseOffset)
	assert.Equal(t, int64(10), batches[1].BaseOffset)
	assert.Equal(t, int64(15), batches[2].BaseOffset)
}

func TestCollectBatchesFromChunks_SkipsShortBatches(t *testing.T) {
	t.Parallel()

	pool := chunk.NewPool(10*1024*1024, 1024*1024)
	c := pool.Acquire()
	require.NotNil(t, c)

	// Write a short batch (< RecordBatchHeaderSize)
	shortData := make([]byte, 30)
	copy(c.Data[0:], shortData)
	c.Used = 30
	c.Batches = append(c.Batches, chunk.ChunkBatch{
		Offset:          0,
		Size:            30,
		BaseOffset:      0,
		LastOffsetDelta: 0,
	})

	batches := collectBatchesFromChunks([]*chunk.Chunk{c})
	assert.Empty(t, batches, "short batches should be skipped")
}

func TestCollectBatchesFromChunks_Empty(t *testing.T) {
	t.Parallel()

	batches := collectBatchesFromChunks(nil)
	assert.Empty(t, batches)

	batches = collectBatchesFromChunks([]*chunk.Chunk{})
	assert.Empty(t, batches)
}

func TestCollectBatchesFromChunks_CopiesData(t *testing.T) {
	t.Parallel()

	pool := chunk.NewPool(10*1024*1024, 1024*1024)
	c := makeTestChunk(pool, []chunk.ChunkBatch{
		{BaseOffset: 42, LastOffsetDelta: 3},
	})

	batches := collectBatchesFromChunks([]*chunk.Chunk{c})
	require.Len(t, batches, 1)

	// Mutate chunk data — the collected batch should be independent.
	original := make([]byte, len(batches[0].RawBytes))
	copy(original, batches[0].RawBytes)
	c.Data[c.Batches[0].Offset] = 0xFF

	assert.Equal(t, original, batches[0].RawBytes, "collected batches should be copies, not slices of chunk data")
}

// --- scanAndFlush candidate selection tests ---

func TestScanAndFlush_FlushAll(t *testing.T) {
	t.Parallel()

	pool := chunk.NewPool(10*1024*1024, 1024*1024)
	var advancedWatermark int64
	chunks := []*chunk.Chunk{
		makeTestChunk(pool, []chunk.ChunkBatch{
			{BaseOffset: 0, LastOffsetDelta: 4},
		}),
	}

	provider := &stubPartitionProvider{
		partitions: []FlushPartition{{
			Topic:       "test-topic",
			Partition:   0,
			TopicID:     [16]byte{1},
			SealedBytes: 100, // below threshold
			DetachChunks: func(flushAll bool) ([]*chunk.Chunk, *chunk.Pool) {
				assert.True(t, flushAll, "flushAll should be true")
				return chunks, pool
			},
			AdvanceWatermark: func(wm int64) { advancedWatermark = wm },
		}},
	}

	flusher, mem, _, _ := newTestFlusher(t, provider)
	err := flusher.scanAndFlush(context.Background(), true)
	require.NoError(t, err)

	assert.Equal(t, int64(5), advancedWatermark) // baseOffset(0) + lastOffsetDelta(4) + 1
	assert.Equal(t, 1, mem.ObjectCount())
}

func TestScanAndFlush_SizeThreshold(t *testing.T) {
	t.Parallel()

	pool := chunk.NewPool(10*1024*1024, 1024*1024)
	var detachCalled bool
	chunks := []*chunk.Chunk{
		makeTestChunk(pool, []chunk.ChunkBatch{
			{BaseOffset: 100, LastOffsetDelta: 9},
		}),
	}

	provider := &stubPartitionProvider{
		partitions: []FlushPartition{{
			Topic:       "test-topic",
			Partition:   0,
			TopicID:     [16]byte{2},
			SealedBytes: 2 * 1024 * 1024, // 2 MiB, above 1 MiB threshold
			DetachChunks: func(flushAll bool) ([]*chunk.Chunk, *chunk.Pool) {
				detachCalled = true
				assert.False(t, flushAll, "size threshold should not set flushAll")
				return chunks, pool
			},
			AdvanceWatermark: func(wm int64) {},
		}},
	}

	flusher, _, _, _ := newTestFlusher(t, provider)
	err := flusher.scanAndFlush(context.Background(), false)
	require.NoError(t, err)
	assert.True(t, detachCalled, "partition should be flushed by size threshold")
}

func TestScanAndFlush_AgeThreshold(t *testing.T) {
	t.Parallel()

	pool := chunk.NewPool(10*1024*1024, 1024*1024)
	clk := clock.NewFakeClock(time.Date(2025, 1, 1, 0, 2, 0, 0, time.UTC))

	var detachFlushAll bool
	chunks := []*chunk.Chunk{
		makeTestChunk(pool, []chunk.ChunkBatch{
			{BaseOffset: 0, LastOffsetDelta: 2},
		}),
	}

	provider := &stubPartitionProvider{
		partitions: []FlushPartition{{
			Topic:       "test-topic",
			Partition:   0,
			TopicID:     [16]byte{3},
			SealedBytes: 100, // below size threshold
			// Oldest chunk is 120 seconds old, well past the 60s flush interval
			OldestChunkTime: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			DetachChunks: func(flushAll bool) ([]*chunk.Chunk, *chunk.Pool) {
				detachFlushAll = flushAll
				return chunks, pool
			},
			AdvanceWatermark: func(wm int64) {},
		}},
	}

	mem := NewInMemoryS3()
	mem.SetClock(clk)
	client := NewClient(ClientConfig{
		S3Client: mem,
		Bucket:   "test-bucket",
		Prefix:   "test-prefix",
	})

	flusher := NewFlusher(FlusherConfig{
		Client:            client,
		FlushInterval:     60 * time.Second,
		TargetObjectSize:  1024 * 1024,
		UploadConcurrency: 1,
		Clock:             clk,
	}, provider)

	err := flusher.scanAndFlush(context.Background(), false)
	require.NoError(t, err)
	assert.True(t, detachFlushAll, "age-triggered flush should include current chunk")
}

func TestScanAndFlush_NoCandidates(t *testing.T) {
	t.Parallel()

	provider := &stubPartitionProvider{
		partitions: []FlushPartition{{
			Topic:           "test-topic",
			Partition:       0,
			SealedBytes:     100,                                        // below threshold
			OldestChunkTime: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), // recent
		}},
	}

	// Clock is close to the chunk time — no age trigger.
	flusher, mem, _, _ := newTestFlusher(t, provider)
	err := flusher.scanAndFlush(context.Background(), false)
	require.NoError(t, err)
	assert.Equal(t, 0, mem.ObjectCount(), "no objects should be uploaded when no candidate matches")
}

func TestScanAndFlush_EmptyPartitions(t *testing.T) {
	t.Parallel()

	provider := &stubPartitionProvider{partitions: nil}
	flusher, _, _, _ := newTestFlusher(t, provider)
	err := flusher.scanAndFlush(context.Background(), false)
	require.NoError(t, err)
}

func TestScanAndFlush_EmptyDetach(t *testing.T) {
	t.Parallel()

	provider := &stubPartitionProvider{
		partitions: []FlushPartition{{
			Topic:       "test-topic",
			Partition:   0,
			SealedBytes: 2 * 1024 * 1024,
			DetachChunks: func(flushAll bool) ([]*chunk.Chunk, *chunk.Pool) {
				return nil, nil // no chunks to detach
			},
		}},
	}

	flusher, mem, _, _ := newTestFlusher(t, provider)
	err := flusher.scanAndFlush(context.Background(), true)
	require.NoError(t, err)
	assert.Equal(t, 0, mem.ObjectCount())
}

// --- Upload failure handling tests ---

func TestScanAndFlush_UploadFailure_ReturnsError(t *testing.T) {
	t.Parallel()

	pool := chunk.NewPool(10*1024*1024, 1024*1024)
	poolFreeBeforeFlush := pool.Free()

	var watermarkAdvanced bool
	chunks := []*chunk.Chunk{
		makeTestChunk(pool, []chunk.ChunkBatch{
			{BaseOffset: 0, LastOffsetDelta: 4},
		}),
	}

	provider := &stubPartitionProvider{
		partitions: []FlushPartition{{
			Topic:       "test-topic",
			Partition:   0,
			TopicID:     [16]byte{4},
			SealedBytes: 2 * 1024 * 1024,
			DetachChunks: func(flushAll bool) ([]*chunk.Chunk, *chunk.Pool) {
				return chunks, pool
			},
			AdvanceWatermark: func(wm int64) { watermarkAdvanced = true },
		}},
	}

	// Create a flusher that uses a failing S3
	clk := clock.NewFakeClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	failS3 := &alwaysFailingS3{err: errors.New("simulated S3 failure")}
	client := NewClient(ClientConfig{
		S3Client: failS3,
		Bucket:   "test-bucket",
		Prefix:   "test-prefix",
	})

	flusher := NewFlusher(FlusherConfig{
		Client:            client,
		FlushInterval:     60 * time.Second,
		TargetObjectSize:  1024 * 1024,
		UploadConcurrency: 1,
		Clock:             clk,
	}, provider)

	// Use a short-lived context so retries don't block
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := flusher.scanAndFlush(ctx, true)
	require.Error(t, err, "should return error on upload failure")
	assert.Contains(t, err.Error(), "partition flush")
	assert.False(t, watermarkAdvanced, "watermark should NOT be advanced on upload failure")

	// Chunks should be released back to pool
	assert.Equal(t, poolFreeBeforeFlush, pool.Free(), "chunks should be released on failure")
}

// --- uploadWithRetry tests ---

func TestUploadWithRetry_Success(t *testing.T) {
	t.Parallel()

	mem := NewInMemoryS3()
	clk := clock.NewFakeClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	client := NewClient(ClientConfig{
		S3Client: mem,
		Bucket:   "test-bucket",
		Prefix:   "test-prefix",
	})

	flusher := NewFlusher(FlusherConfig{
		Client: client,
		Clock:  clk,
	}, &stubPartitionProvider{})

	err := flusher.uploadWithRetry(context.Background(), "test-key", []byte("data"))
	require.NoError(t, err)

	data, ok := mem.GetRaw("test-key")
	require.True(t, ok)
	assert.Equal(t, []byte("data"), data)
}

func TestUploadWithRetry_RetriesOnFailure(t *testing.T) {
	t.Parallel()

	clk := clock.NewFakeClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	var attempts atomic.Int32
	failCount := 3

	countingS3 := &countingFailS3{
		InMemoryS3:    NewInMemoryS3(),
		attempts:      &attempts,
		failUntil:     int32(failCount),
		err:           errors.New("transient error"),
	}
	countingS3.InMemoryS3.SetClock(clk)

	client := NewClient(ClientConfig{
		S3Client: countingS3,
		Bucket:   "test-bucket",
		Prefix:   "test-prefix",
	})

	flusher := NewFlusher(FlusherConfig{
		Client: client,
		Clock:  clk,
	}, &stubPartitionProvider{})

	// Run uploadWithRetry in a goroutine since it blocks on clock.After
	errCh := make(chan error, 1)
	go func() {
		errCh <- flusher.uploadWithRetry(context.Background(), "retry-key", []byte("retry-data"))
	}()

	// Advance clock through backoff retries (1s, 2s, 4s)
	for i := 0; i < failCount; i++ {
		clk.WaitForTimers(1, 2*time.Second)
		clk.Advance(time.Duration(1<<uint(i)) * time.Second)
	}

	err := <-errCh
	require.NoError(t, err)
	assert.GreaterOrEqual(t, int(attempts.Load()), failCount+1)
}

func TestUploadWithRetry_ContextCancellation(t *testing.T) {
	t.Parallel()

	clk := clock.NewFakeClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	failS3 := &alwaysFailingS3{err: errors.New("permanent error")}

	client := NewClient(ClientConfig{
		S3Client: failS3,
		Bucket:   "test-bucket",
		Prefix:   "test-prefix",
	})

	flusher := NewFlusher(FlusherConfig{
		Client: client,
		Clock:  clk,
	}, &stubPartitionProvider{})

	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() {
		errCh <- flusher.uploadWithRetry(ctx, "cancel-key", []byte("data"))
	}()

	// Wait for the first retry backoff timer to be registered
	clk.WaitForTimers(1, 2*time.Second)

	// Cancel context instead of advancing clock
	cancel()

	err := <-errCh
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestUploadWithRetry_StopSignal(t *testing.T) {
	t.Parallel()

	clk := clock.NewFakeClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	failS3 := &alwaysFailingS3{err: errors.New("permanent error")}

	client := NewClient(ClientConfig{
		S3Client: failS3,
		Bucket:   "test-bucket",
		Prefix:   "test-prefix",
	})

	flusher := NewFlusher(FlusherConfig{
		Client: client,
		Clock:  clk,
	}, &stubPartitionProvider{})

	errCh := make(chan error, 1)
	go func() {
		errCh <- flusher.uploadWithRetry(context.Background(), "stop-key", []byte("data"))
	}()

	// Wait for the first retry backoff timer to be registered
	clk.WaitForTimers(1, 2*time.Second)

	// Close the stop channel
	close(flusher.stopCh)

	err := <-errCh
	require.Error(t, err)
	assert.Contains(t, err.Error(), "flusher stopped during retry")
}

func TestUploadWithRetry_ExponentialBackoffCapped(t *testing.T) {
	t.Parallel()

	clk := clock.NewFakeClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	failS3 := &alwaysFailingS3{err: errors.New("permanent error")}

	client := NewClient(ClientConfig{
		S3Client: failS3,
		Bucket:   "test-bucket",
		Prefix:   "test-prefix",
	})

	flusher := NewFlusher(FlusherConfig{
		Client: client,
		Clock:  clk,
	}, &stubPartitionProvider{})

	// Run the upload — it will exhaust all 10 retries.
	errCh := make(chan error, 1)
	go func() {
		errCh <- flusher.uploadWithRetry(context.Background(), "backoff-key", []byte("data"))
	}()

	// Advance through all retries. Backoff: 1s, 2s, 4s, 8s, 16s, 32s, 60s, 60s, 60s, 60s
	backoffs := []time.Duration{
		1 * time.Second, 2 * time.Second, 4 * time.Second,
		8 * time.Second, 16 * time.Second, 32 * time.Second,
		60 * time.Second, 60 * time.Second, 60 * time.Second, 60 * time.Second,
	}
	for _, d := range backoffs {
		clk.WaitForTimers(1, 2*time.Second)
		clk.Advance(d)
	}

	err := <-errCh
	require.Error(t, err)
	assert.Contains(t, err.Error(), "after 10 retries")
}

// --- Watermark advancement tests ---

func TestScanAndFlush_WatermarkAdvancement(t *testing.T) {
	t.Parallel()

	pool := chunk.NewPool(10*1024*1024, 1024*1024)
	var watermarks []int64

	provider := &stubPartitionProvider{
		partitions: []FlushPartition{{
			Topic:       "test-topic",
			Partition:   0,
			TopicID:     [16]byte{5},
			SealedBytes: 2 * 1024 * 1024,
			DetachChunks: func(flushAll bool) ([]*chunk.Chunk, *chunk.Pool) {
				return []*chunk.Chunk{
					makeTestChunk(pool, []chunk.ChunkBatch{
						{BaseOffset: 100, LastOffsetDelta: 9},
						{BaseOffset: 110, LastOffsetDelta: 4},
					}),
				}, pool
			},
			AdvanceWatermark: func(wm int64) { watermarks = append(watermarks, wm) },
		}},
	}

	flusher, _, _, _ := newTestFlusher(t, provider)
	err := flusher.scanAndFlush(context.Background(), true)
	require.NoError(t, err)

	// First batch: baseOffset=100, second batch: baseOffset=110, lastOffsetDelta=4
	// Watermark = 110 + 4 + 1 = 115
	require.Len(t, watermarks, 1)
	assert.Equal(t, int64(115), watermarks[0])
}

func TestScanAndFlush_MultiplePartitions(t *testing.T) {
	t.Parallel()

	pool := chunk.NewPool(10*1024*1024, 1024*1024)
	var mu sync.Mutex
	watermarks := make(map[int32]int64)

	makePartition := func(partition int32, baseOffset int64, lastDelta int32) FlushPartition {
		return FlushPartition{
			Topic:       "test-topic",
			Partition:   partition,
			TopicID:     [16]byte{byte(partition)},
			SealedBytes: 2 * 1024 * 1024,
			DetachChunks: func(flushAll bool) ([]*chunk.Chunk, *chunk.Pool) {
				return []*chunk.Chunk{
					makeTestChunk(pool, []chunk.ChunkBatch{
						{BaseOffset: baseOffset, LastOffsetDelta: lastDelta},
					}),
				}, pool
			},
			AdvanceWatermark: func(wm int64) {
				mu.Lock()
				watermarks[partition] = wm
				mu.Unlock()
			},
		}
	}

	provider := &stubPartitionProvider{
		partitions: []FlushPartition{
			makePartition(0, 0, 9),
			makePartition(1, 50, 4),
			makePartition(2, 200, 19),
		},
	}

	flusher, mem, _, _ := newTestFlusher(t, provider)
	err := flusher.scanAndFlush(context.Background(), true)
	require.NoError(t, err)

	assert.Equal(t, 3, mem.ObjectCount())

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, int64(10), watermarks[0])  // 0 + 9 + 1
	assert.Equal(t, int64(55), watermarks[1])  // 50 + 4 + 1
	assert.Equal(t, int64(220), watermarks[2]) // 200 + 19 + 1
}

// --- updateGlobalFlushWatermark tests ---

func TestUpdateGlobalFlushWatermark(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	walIdx := wal.NewIndex()
	walWriter, err := wal.NewWriter(wal.WriterConfig{Dir: dir}, walIdx)
	require.NoError(t, err)
	walWriter.Start()
	defer walWriter.Stop()

	topicID1 := [16]byte{0x01}
	topicID2 := [16]byte{0x02}

	tp1 := wal.TopicPartition{TopicID: topicID1, Partition: 0}
	tp2 := wal.TopicPartition{TopicID: topicID2, Partition: 0}

	// Populate WAL index directly. WAL sequence acts as a monotonic proxy.
	// tp1: offsets 0-4, WAL seq 10
	walIdx.Add(tp1, wal.IndexEntry{
		BaseOffset: 0, LastOffset: 4, WALSequence: 10,
	})
	// tp2: offsets 10-19, WAL seq 5 (older)
	walIdx.Add(tp2, wal.IndexEntry{
		BaseOffset: 10, LastOffset: 19, WALSequence: 5,
	})

	watermarkProvider := &stubWatermarkProvider{
		watermarks: []PartitionWatermarkInfo{
			{TopicID: topicID1, Partition: 0, S3Watermark: 3},  // partially flushed, entry still unflushed (LastOffset=4 >= 3)
			{TopicID: topicID2, Partition: 0, S3Watermark: 0},  // nothing flushed
		},
	}

	clk := clock.NewFakeClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	mem := NewInMemoryS3()
	client := NewClient(ClientConfig{S3Client: mem, Bucket: "b", Prefix: "p"})

	flusher := NewFlusher(FlusherConfig{
		Client:            client,
		WALWriter:         walWriter,
		WALIndex:          walIdx,
		WatermarkProvider: watermarkProvider,
		Clock:             clk,
	}, &stubPartitionProvider{})

	flusher.updateGlobalFlushWatermark()

	// tp1 oldest unflushed seq=10, tp2 oldest unflushed seq=5
	// Global min = 5
	assert.Equal(t, uint64(5), walWriter.S3FlushWatermark())
}

func TestUpdateGlobalFlushWatermark_AllFlushed(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	walIdx := wal.NewIndex()
	walWriter, err := wal.NewWriter(wal.WriterConfig{Dir: dir}, walIdx)
	require.NoError(t, err)
	walWriter.Start()
	defer walWriter.Stop()

	// Populate index, but watermarks are past all entries — everything flushed.
	tp := wal.TopicPartition{TopicID: [16]byte{1}, Partition: 0}
	walIdx.Add(tp, wal.IndexEntry{
		BaseOffset: 0, LastOffset: 9, WALSequence: 1,
	})

	watermarkProvider := &stubWatermarkProvider{
		watermarks: []PartitionWatermarkInfo{
			{TopicID: [16]byte{1}, Partition: 0, S3Watermark: 100}, // past all entries
		},
	}

	clk := clock.NewFakeClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	mem := NewInMemoryS3()
	client := NewClient(ClientConfig{S3Client: mem, Bucket: "b", Prefix: "p"})

	flusher := NewFlusher(FlusherConfig{
		Client:            client,
		WALWriter:         walWriter,
		WALIndex:          walIdx,
		WatermarkProvider: watermarkProvider,
		Clock:             clk,
	}, &stubPartitionProvider{})

	flusher.updateGlobalFlushWatermark()

	// When all flushed, OldestUnflushedSequence returns 0 for each partition,
	// so globalMin stays MaxUint64, which triggers the fallback to NextSequence().
	assert.Equal(t, walWriter.NextSequence(), walWriter.S3FlushWatermark())
}

// --- S3 test doubles ---

// alwaysFailingS3 wraps InMemoryS3 but PutObject always fails.
type alwaysFailingS3 struct {
	InMemoryS3
	err error
}

func (f *alwaysFailingS3) PutObject(_ context.Context, _ *s3svc.PutObjectInput, _ ...func(*s3svc.Options)) (*s3svc.PutObjectOutput, error) {
	return nil, f.err
}

// countingFailS3 wraps InMemoryS3 and fails PutObject for the first N calls.
type countingFailS3 struct {
	*InMemoryS3
	attempts  *atomic.Int32
	failUntil int32
	err       error
}

func (f *countingFailS3) PutObject(ctx context.Context, input *s3svc.PutObjectInput, opts ...func(*s3svc.Options)) (*s3svc.PutObjectOutput, error) {
	n := f.attempts.Add(1)
	if n <= f.failUntil {
		return nil, f.err
	}
	return f.InMemoryS3.PutObject(ctx, input, opts...)
}
