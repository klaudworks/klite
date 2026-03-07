package lease

import "context"

// Role represents whether this node is primary or standby.
type Role int

const (
	RoleStandby Role = iota
	RolePrimary
)

func (r Role) String() string {
	switch r {
	case RolePrimary:
		return "primary"
	default:
		return "standby"
	}
}

// Elector manages leader election. Run blocks; Role() is safe to call
// concurrently from any goroutine.
type Elector interface {
	// Run blocks, calling callbacks on role transitions. Returns when
	// ctx is cancelled. The initial role is always RoleStandby; the
	// elector calls OnElected if this instance wins the lease.
	Run(ctx context.Context, cb Callbacks) error

	// Role returns the current role.
	Role() Role

	// Release performs a best-effort early lease release. Called by
	// the broker during graceful shutdown AFTER draining all writes.
	// Writes an expired renewedAt so the standby can claim the lease
	// on its next poll (~retryInterval) instead of waiting for
	// leaseDuration to elapse. Noop if not currently primary.
	// Errors are logged but not fatal — the lease expires naturally
	// if Release fails.
	Release() error
}

// Callbacks are invoked by the Elector on role transitions.
type Callbacks struct {
	// OnElected is called when this instance becomes primary. The
	// provided ctx is cancelled if the lease is subsequently lost.
	OnElected func(ctx context.Context)

	// OnDemoted is called when this instance loses the primary role.
	// The broker must stop accepting writes before returning.
	OnDemoted func()
}
