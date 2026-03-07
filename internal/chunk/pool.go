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

type ChunkBatch struct {
	Offset          int   // byte offset within chunk.Data
	Size            int   // batch size in bytes
	BaseOffset      int64 // Kafka base offset of this batch
	LastOffsetDelta int32
	MaxTimestamp    int64
	NumRecords      int32
}

type Chunk struct {
	Data      []byte       // fixed-size backing buffer, allocated once
	Used      int          // bytes written so far
	Batches   []ChunkBatch // index of batches within this chunk
	CreatedAt time.Time    // when this chunk was acquired (for age-based flush)
}

func (c *Chunk) Reset() {
	c.Used = 0
	c.Batches = c.Batches[:0]
	c.CreatedAt = time.Time{}
}

func (c *Chunk) Remaining() int {
	return len(c.Data) - c.Used
}

type Pool struct {
	chunkSize int // bytes per chunk (= max.message.bytes)
	maxChunks int // total chunks = budget / chunkSize

	mu     sync.Mutex
	cond   *sync.Cond
	free   []*Chunk // available chunks (stack, LIFO for cache warmth)
	closed bool

	allocated atomic.Int64 // number of chunks currently out on loan

	// Emergency flush trigger: signaled when pressure >= 75%.
	triggerCh chan struct{}
}

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

	for i := 0; i < maxChunks; i++ {
		p.free[i] = &Chunk{
			Data:    make([]byte, chunkSize),
			Batches: make([]ChunkBatch, 0, 64), // pre-allocate batch index
		}
	}

	return p
}

func (p *Pool) SetTriggerCh(ch chan struct{}) {
	p.triggerCh = ch
}

// Acquire returns a chunk from the pool, blocking if exhausted.
// Returns nil if the pool has been closed.
func (p *Pool) Acquire() *Chunk {
	p.mu.Lock()

	for len(p.free) == 0 {
		if p.closed {
			p.mu.Unlock()
			return nil
		}
		p.signalEmergencyFlushLocked()
		p.cond.Wait()
	}

	allocated := int(p.allocated.Load()) + 1
	if allocated*100/p.maxChunks >= 75 {
		p.signalEmergencyFlushLocked()
	}

	n := len(p.free) - 1
	c := p.free[n]
	p.free[n] = nil // clear for GC safety
	p.free = p.free[:n]
	p.mu.Unlock()

	p.allocated.Add(1)
	c.CreatedAt = time.Now()
	return c
}

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

	if int(p.allocated.Load())*100/p.maxChunks >= 75 {
		p.signalEmergencyFlush()
	}

	c.CreatedAt = time.Now()
	return c
}

func (p *Pool) Release(c *Chunk) {
	c.Reset()
	p.allocated.Add(-1)

	p.mu.Lock()
	p.free = append(p.free, c)
	p.cond.Signal() // wake one blocked Acquire
	p.mu.Unlock()
}

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

func (p *Pool) ChunkSize() int {
	return p.chunkSize
}

func (p *Pool) MaxChunks() int {
	return p.maxChunks
}

func (p *Pool) Allocated() int {
	return int(p.allocated.Load())
}

func (p *Pool) Free() int {
	p.mu.Lock()
	n := len(p.free)
	p.mu.Unlock()
	return n
}

func (p *Pool) Pressure() float64 {
	return float64(p.allocated.Load()) / float64(p.maxChunks)
}

func (p *Pool) signalEmergencyFlushLocked() {
	if p.triggerCh == nil {
		return
	}
	select {
	case p.triggerCh <- struct{}{}:
	default:
	}
}

func (p *Pool) signalEmergencyFlush() {
	if p.triggerCh == nil {
		return
	}
	select {
	case p.triggerCh <- struct{}{}:
	default:
	}
}

func (p *Pool) Close() {
	p.mu.Lock()
	p.closed = true
	p.cond.Broadcast()
	p.mu.Unlock()
}
