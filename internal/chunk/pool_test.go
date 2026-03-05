package chunk

import (
	"sync"
	"testing"
	"time"
)

func TestPoolBasic(t *testing.T) {
	t.Parallel()

	p := NewPool(4*1024*1024, 1024*1024) // 4 MiB budget, 1 MiB chunks → 4 chunks

	if p.MaxChunks() != 4 {
		t.Fatalf("MaxChunks: got %d, want 4", p.MaxChunks())
	}
	if p.ChunkSize() != 1024*1024 {
		t.Fatalf("ChunkSize: got %d, want %d", p.ChunkSize(), 1024*1024)
	}
	if p.Allocated() != 0 {
		t.Fatalf("Allocated: got %d, want 0", p.Allocated())
	}
	if p.Free() != 4 {
		t.Fatalf("Free: got %d, want 4", p.Free())
	}
}

func TestPoolAcquireRelease(t *testing.T) {
	t.Parallel()

	p := NewPool(4*1024*1024, 1024*1024)

	c1 := p.Acquire()
	if c1 == nil {
		t.Fatal("Acquire returned nil")
	}
	if p.Allocated() != 1 {
		t.Errorf("Allocated after 1 acquire: got %d, want 1", p.Allocated())
	}
	if p.Free() != 3 {
		t.Errorf("Free after 1 acquire: got %d, want 3", p.Free())
	}

	c2 := p.Acquire()
	c3 := p.Acquire()
	c4 := p.Acquire()
	if p.Allocated() != 4 {
		t.Errorf("Allocated after 4 acquires: got %d, want 4", p.Allocated())
	}
	if p.Free() != 0 {
		t.Errorf("Free after 4 acquires: got %d, want 0", p.Free())
	}

	// Release one
	p.Release(c1)
	if p.Allocated() != 3 {
		t.Errorf("Allocated after release: got %d, want 3", p.Allocated())
	}
	if p.Free() != 1 {
		t.Errorf("Free after release: got %d, want 1", p.Free())
	}

	// Release rest
	p.ReleaseMany([]*Chunk{c2, c3, c4})
	if p.Allocated() != 0 {
		t.Errorf("Allocated after release all: got %d, want 0", p.Allocated())
	}
	if p.Free() != 4 {
		t.Errorf("Free after release all: got %d, want 4", p.Free())
	}
}

func TestPoolChunkReset(t *testing.T) {
	t.Parallel()

	p := NewPool(1024*1024, 1024*1024) // 1 chunk

	c := p.Acquire()
	// Write some data
	copy(c.Data, []byte("hello"))
	c.Used = 5
	c.Batches = append(c.Batches, ChunkBatch{
		Offset:     0,
		Size:       5,
		BaseOffset: 42,
	})

	p.Release(c)

	// Re-acquire — should be reset
	c2 := p.Acquire()
	if c2.Used != 0 {
		t.Errorf("Used after reset: got %d, want 0", c2.Used)
	}
	if len(c2.Batches) != 0 {
		t.Errorf("Batches after reset: got %d, want 0", len(c2.Batches))
	}
	// Data buffer should be reused (same underlying array)
	if len(c2.Data) != 1024*1024 {
		t.Errorf("Data len: got %d, want %d", len(c2.Data), 1024*1024)
	}
}

func TestPoolExhaustion(t *testing.T) {
	t.Parallel()

	p := NewPool(2*1024*1024, 1024*1024) // 2 chunks

	c1 := p.Acquire()
	c2 := p.Acquire()

	// Pool is now empty. TryAcquire should return nil.
	c3 := p.TryAcquire()
	if c3 != nil {
		t.Error("TryAcquire on empty pool should return nil")
	}

	// Acquire in a goroutine should block until Release.
	acquired := make(chan *Chunk, 1)
	go func() {
		acquired <- p.Acquire() // should block
	}()

	// Give the goroutine time to block
	time.Sleep(10 * time.Millisecond)

	select {
	case <-acquired:
		t.Fatal("Acquire should be blocking")
	default:
	}

	// Release one chunk — should unblock
	p.Release(c1)

	select {
	case c := <-acquired:
		if c == nil {
			t.Error("Acquire returned nil after release")
		}
		p.Release(c)
	case <-time.After(time.Second):
		t.Fatal("Acquire did not unblock after release")
	}

	p.Release(c2)
}

func TestPoolPressure(t *testing.T) {
	t.Parallel()

	p := NewPool(4*1024*1024, 1024*1024) // 4 chunks

	if p.Pressure() != 0 {
		t.Errorf("Pressure empty: got %f, want 0", p.Pressure())
	}

	c1 := p.Acquire()
	if p.Pressure() != 0.25 {
		t.Errorf("Pressure 1/4: got %f, want 0.25", p.Pressure())
	}

	c2 := p.Acquire()
	c3 := p.Acquire()
	if p.Pressure() != 0.75 {
		t.Errorf("Pressure 3/4: got %f, want 0.75", p.Pressure())
	}

	c4 := p.Acquire()
	if p.Pressure() != 1.0 {
		t.Errorf("Pressure 4/4: got %f, want 1.0", p.Pressure())
	}

	p.ReleaseMany([]*Chunk{c1, c2, c3, c4})
}

func TestPoolEmergencyFlush(t *testing.T) {
	t.Parallel()

	p := NewPool(4*1024*1024, 1024*1024) // 4 chunks
	triggerCh := make(chan struct{}, 1)
	p.SetTriggerCh(triggerCh)

	// Acquire 3 chunks (75% pressure) — should trigger emergency flush
	c1 := p.Acquire()
	c2 := p.Acquire()
	_ = p.Acquire() // 3rd acquire triggers at 75%

	select {
	case <-triggerCh:
		// good
	case <-time.After(100 * time.Millisecond):
		t.Error("expected emergency flush signal at 75% pressure")
	}

	p.Release(c1)
	p.Release(c2)
}

func TestPoolConcurrency(t *testing.T) {
	t.Parallel()

	p := NewPool(10*1024*1024, 1024*1024) // 10 chunks
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := p.Acquire()
			// Simulate some work
			c.Data[0] = 0xFF
			c.Used = 1
			time.Sleep(time.Microsecond)
			p.Release(c)
		}()
	}

	wg.Wait()

	if p.Allocated() != 0 {
		t.Errorf("Allocated after all released: got %d, want 0", p.Allocated())
	}
	if p.Free() != 10 {
		t.Errorf("Free after all released: got %d, want 10", p.Free())
	}
}

func TestPoolChunkCreatedAt(t *testing.T) {
	t.Parallel()

	p := NewPool(1024*1024, 1024*1024)

	before := time.Now()
	c := p.Acquire()
	after := time.Now()

	if c.CreatedAt.Before(before) || c.CreatedAt.After(after) {
		t.Errorf("CreatedAt %v not in [%v, %v]", c.CreatedAt, before, after)
	}

	p.Release(c)
}

func TestPoolMinChunks(t *testing.T) {
	t.Parallel()

	// Budget smaller than chunk size — should still get 1 chunk
	p := NewPool(100, 1024*1024)
	if p.MaxChunks() != 1 {
		t.Errorf("MaxChunks: got %d, want 1", p.MaxChunks())
	}

	c := p.Acquire()
	if c == nil {
		t.Fatal("should get at least 1 chunk")
	}
	p.Release(c)
}
