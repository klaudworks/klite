// Package chunk implements a fixed-size memory chunk pool for the S3 flush
// pipeline. Partitions borrow chunks to hold unflushed RecordBatch data.
// The S3 flusher reads from chunks (zero disk I/O) and returns them to the
// pool after upload.
package chunk

import (
	"sync"
	"sync/atomic"
	"time"
)

// ChunkBatch records the position of one RecordBatch within a chunk.
type ChunkBatch struct {
	Offset          int   // byte offset within chunk.Data
	Size            int   // batch size in bytes
	BaseOffset      int64 // Kafka base offset of this batch
	LastOffsetDelta int32
	MaxTimestamp    int64
	NumRecords      int32
}

// Chunk is a single memory buffer, owned by one partition at a time.
type Chunk struct {
	Data      []byte       // fixed-size backing buffer, allocated once
	Used      int          // bytes written so far
	Batches   []ChunkBatch // index of batches within this chunk
	CreatedAt time.Time    // when this chunk was acquired (for age-based flush)
}

// Reset prepares a chunk for reuse after release back to the pool.
func (c *Chunk) Reset() {
	c.Used = 0
	c.Batches = c.Batches[:0]
	c.CreatedAt = time.Time{}
}

// Remaining returns how many bytes are still available in this chunk.
func (c *Chunk) Remaining() int {
	return len(c.Data) - c.Used
}

// Pool manages a global pool of fixed-size memory chunks.
type Pool struct {
	chunkSize int // bytes per chunk (= max.message.bytes)
	maxChunks int // total chunks = budget / chunkSize

	mu   sync.Mutex
	cond *sync.Cond
	free []*Chunk // available chunks (stack, LIFO for cache warmth)

	allocated atomic.Int64 // number of chunks currently out on loan

	// Emergency flush trigger: signaled when pressure >= 75%.
	triggerCh chan struct{}
}

// NewPool creates a chunk pool with the given memory budget and chunk size.
// All chunks are pre-allocated and placed on the free list.
func NewPool(budget int64, chunkSize int) *Pool {
	if chunkSize <= 0 {
		chunkSize = 1048588 // DefaultMaxMessageBytes
	}
	maxChunks := int(budget / int64(chunkSize))
	if maxChunks < 1 {
		maxChunks = 1
	}

	p := &Pool{
		chunkSize: chunkSize,
		maxChunks: maxChunks,
		free:      make([]*Chunk, maxChunks),
	}
	p.cond = sync.NewCond(&p.mu)

	// Pre-allocate all chunks
	for i := 0; i < maxChunks; i++ {
		p.free[i] = &Chunk{
			Data:    make([]byte, chunkSize),
			Batches: make([]ChunkBatch, 0, 64), // pre-allocate batch index
		}
	}

	return p
}

// SetTriggerCh sets the channel used to signal emergency flush when
// pool pressure reaches 75%.
func (p *Pool) SetTriggerCh(ch chan struct{}) {
	p.triggerCh = ch
}

// Acquire returns a chunk from the pool. If the pool is exhausted
// (pressure >= 90%), this blocks until a chunk is freed. At 75% pressure,
// an emergency flush is signaled.
func (p *Pool) Acquire() *Chunk {
	p.mu.Lock()

	// Check pressure thresholds before trying to pop
	for len(p.free) == 0 {
		// Pool exhausted — block (backpressure on producer)
		p.signalEmergencyFlushLocked()
		p.cond.Wait()
	}

	// Signal emergency flush at 75% pressure
	allocated := int(p.allocated.Load()) + 1
	if allocated*100/p.maxChunks >= 75 {
		p.signalEmergencyFlushLocked()
	}

	// Pop from free list (LIFO)
	n := len(p.free) - 1
	c := p.free[n]
	p.free[n] = nil // clear for GC safety
	p.free = p.free[:n]
	p.mu.Unlock()

	p.allocated.Add(1)
	c.CreatedAt = time.Now()
	return c
}

// TryAcquire attempts to acquire a chunk without blocking.
// Returns nil if the pool is exhausted.
func (p *Pool) TryAcquire() *Chunk {
	p.mu.Lock()
	if len(p.free) == 0 {
		p.mu.Unlock()
		return nil
	}

	n := len(p.free) - 1
	c := p.free[n]
	p.free[n] = nil
	p.free = p.free[:n]
	p.mu.Unlock()

	p.allocated.Add(1)

	// Signal emergency flush at 75% pressure
	if int(p.allocated.Load())*100/p.maxChunks >= 75 {
		p.signalEmergencyFlush()
	}

	c.CreatedAt = time.Now()
	return c
}

// Release returns a chunk to the pool's free list. The chunk is reset
// for reuse. Wakes one blocked Acquire() caller.
func (p *Pool) Release(c *Chunk) {
	c.Reset()
	p.allocated.Add(-1)

	p.mu.Lock()
	p.free = append(p.free, c)
	p.cond.Signal() // wake one blocked Acquire
	p.mu.Unlock()
}

// ReleaseMany returns multiple chunks to the pool.
func (p *Pool) ReleaseMany(chunks []*Chunk) {
	for _, c := range chunks {
		c.Reset()
	}
	p.allocated.Add(-int64(len(chunks)))

	p.mu.Lock()
	p.free = append(p.free, chunks...)
	p.cond.Broadcast() // wake all blocked Acquires
	p.mu.Unlock()
}

// ChunkSize returns the fixed size of each chunk in bytes.
func (p *Pool) ChunkSize() int {
	return p.chunkSize
}

// MaxChunks returns the total number of chunks in the pool.
func (p *Pool) MaxChunks() int {
	return p.maxChunks
}

// Allocated returns the number of chunks currently in use.
func (p *Pool) Allocated() int {
	return int(p.allocated.Load())
}

// Free returns the number of chunks currently available.
func (p *Pool) Free() int {
	p.mu.Lock()
	n := len(p.free)
	p.mu.Unlock()
	return n
}

// Pressure returns the fraction of chunks currently allocated (0.0 to 1.0).
func (p *Pool) Pressure() float64 {
	return float64(p.allocated.Load()) / float64(p.maxChunks)
}

// signalEmergencyFlushLocked sends a non-blocking signal to the trigger channel.
// Caller must hold p.mu.
func (p *Pool) signalEmergencyFlushLocked() {
	if p.triggerCh == nil {
		return
	}
	select {
	case p.triggerCh <- struct{}{}:
	default:
	}
}

// signalEmergencyFlush sends a non-blocking signal without holding the lock.
func (p *Pool) signalEmergencyFlush() {
	if p.triggerCh == nil {
		return
	}
	select {
	case p.triggerCh <- struct{}{}:
	default:
	}
}

// Close wakes all blocked Acquire callers. After Close, Acquire will panic
// if the pool is empty. Callers should ensure no goroutines are blocked
// before calling Close.
func (p *Pool) Close() {
	p.mu.Lock()
	p.cond.Broadcast()
	p.mu.Unlock()
}
