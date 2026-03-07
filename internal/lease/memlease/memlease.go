package memlease

import (
	"context"
	"sync"

	"github.com/klaudworks/klite/internal/lease"
)

// MemCluster coordinates multiple MemElector instances. Only one can
// be primary at a time.
type MemCluster struct {
	mu       sync.Mutex
	electors []*MemElector
}

// NewCluster creates a new in-memory lease cluster for testing.
func NewCluster() *MemCluster {
	return &MemCluster{}
}

// NewElector creates an elector that participates in this cluster's
// leader election.
func (c *MemCluster) NewElector(id string) *MemElector {
	e := &MemElector{
		cluster: c,
		id:      id,
		role:    lease.RoleStandby,
		readyCh: make(chan struct{}),
	}
	c.mu.Lock()
	c.electors = append(c.electors, e)
	c.mu.Unlock()
	return e
}

func (c *MemCluster) demoteOthers(winner *MemElector) {
	c.mu.Lock()
	others := make([]*MemElector, 0)
	for _, e := range c.electors {
		if e != winner && e.Role() == lease.RolePrimary {
			others = append(others, e)
		}
	}
	c.mu.Unlock()

	for _, e := range others {
		e.demote()
	}
}

// MemElector is a deterministic lease elector for testing.
type MemElector struct {
	cluster *MemCluster
	id      string

	mu          sync.Mutex
	role        lease.Role
	cb          lease.Callbacks
	running     bool
	readyCh     chan struct{}      // closed when Run() has set running=true
	leaseCancel context.CancelFunc // cancels the ctx passed to OnElected
}

// Elect forces this elector to become primary. If another elector
// in the same cluster is primary, it is demoted first.
func (m *MemElector) Elect() {
	m.cluster.demoteOthers(m)
	m.elect()
}

func (m *MemElector) elect() {
	m.mu.Lock()
	if m.role == lease.RolePrimary {
		m.mu.Unlock()
		return
	}
	m.role = lease.RolePrimary
	cb := m.cb
	running := m.running

	var leaseCtx context.Context
	leaseCtx, m.leaseCancel = context.WithCancel(context.Background())
	m.mu.Unlock()

	if running && cb.OnElected != nil {
		cb.OnElected(leaseCtx)
	}
}

// Demote forces this elector to become standby.
func (m *MemElector) Demote() {
	m.demote()
}

func (m *MemElector) demote() {
	m.mu.Lock()
	if m.role == lease.RoleStandby {
		m.mu.Unlock()
		return
	}
	m.role = lease.RoleStandby
	cb := m.cb
	running := m.running
	cancel := m.leaseCancel
	m.leaseCancel = nil
	m.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if running && cb.OnDemoted != nil {
		cb.OnDemoted()
	}
}

// Release triggers demotion (same as Demote for in-memory impl).
func (m *MemElector) Release() error {
	m.demote()
	return nil
}

// Run blocks, calling callbacks on role transitions. Returns when
// ctx is cancelled.
func (m *MemElector) Run(ctx context.Context, cb lease.Callbacks) error {
	m.mu.Lock()
	m.cb = cb
	m.running = true
	close(m.readyCh)
	m.mu.Unlock()

	<-ctx.Done()

	m.mu.Lock()
	m.running = false
	m.mu.Unlock()

	return ctx.Err()
}

// Ready returns a channel that is closed when Run() has registered
// the callbacks and set running=true. Useful in tests to ensure
// Elect() will fire OnElected.
func (m *MemElector) Ready() <-chan struct{} {
	return m.readyCh
}

// Role returns the current role.
func (m *MemElector) Role() lease.Role {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.role
}

// compile-time interface check
var _ lease.Elector = (*MemElector)(nil)
