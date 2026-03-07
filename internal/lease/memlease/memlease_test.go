package memlease

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/klaudworks/klite/internal/lease"
	"github.com/stretchr/testify/require"
)

func startElector(t *testing.T, e *MemElector, cb lease.Callbacks) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = e.Run(ctx, cb)
	}()
	// Give Run a moment to register callbacks
	time.Sleep(10 * time.Millisecond)
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})
	return cancel
}

func TestMemElectorElect(t *testing.T) {
	c := NewCluster()
	e := c.NewElector("node1")

	elected := make(chan struct{}, 1)
	startElector(t, e, lease.Callbacks{
		OnElected: func(ctx context.Context) {
			elected <- struct{}{}
		},
	})

	require.Equal(t, lease.RoleStandby, e.Role())

	e.Elect()

	select {
	case <-elected:
	case <-time.After(time.Second):
		t.Fatal("OnElected not called")
	}
	require.Equal(t, lease.RolePrimary, e.Role())
}

func TestMemElectorDemote(t *testing.T) {
	c := NewCluster()
	e := c.NewElector("node1")

	demoted := make(chan struct{}, 1)
	startElector(t, e, lease.Callbacks{
		OnElected: func(ctx context.Context) {},
		OnDemoted: func() {
			demoted <- struct{}{}
		},
	})

	e.Elect()
	require.Equal(t, lease.RolePrimary, e.Role())

	e.Demote()

	select {
	case <-demoted:
	case <-time.After(time.Second):
		t.Fatal("OnDemoted not called")
	}
	require.Equal(t, lease.RoleStandby, e.Role())
}

func TestMemClusterSinglePrimary(t *testing.T) {
	c := NewCluster()
	e1 := c.NewElector("node1")
	e2 := c.NewElector("node2")

	e1Elected := make(chan struct{}, 1)
	e1Demoted := make(chan struct{}, 1)
	e2Elected := make(chan struct{}, 1)

	startElector(t, e1, lease.Callbacks{
		OnElected: func(ctx context.Context) {
			e1Elected <- struct{}{}
		},
		OnDemoted: func() {
			e1Demoted <- struct{}{}
		},
	})
	startElector(t, e2, lease.Callbacks{
		OnElected: func(ctx context.Context) {
			e2Elected <- struct{}{}
		},
	})

	e1.Elect()
	<-e1Elected
	require.Equal(t, lease.RolePrimary, e1.Role())
	require.Equal(t, lease.RoleStandby, e2.Role())

	e2.Elect()
	<-e1Demoted
	<-e2Elected
	require.Equal(t, lease.RoleStandby, e1.Role())
	require.Equal(t, lease.RolePrimary, e2.Role())
}

func TestMemElectorContextCancel(t *testing.T) {
	c := NewCluster()
	e := c.NewElector("node1")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- e.Run(ctx, lease.Callbacks{})
	}()

	cancel()

	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("Run did not return after context cancel")
	}
}

func TestMemElectorRelease(t *testing.T) {
	c := NewCluster()
	e := c.NewElector("node1")

	demoted := make(chan struct{}, 1)
	startElector(t, e, lease.Callbacks{
		OnElected: func(ctx context.Context) {},
		OnDemoted: func() {
			demoted <- struct{}{}
		},
	})

	e.Elect()
	require.Equal(t, lease.RolePrimary, e.Role())

	err := e.Release()
	require.NoError(t, err)

	select {
	case <-demoted:
	case <-time.After(time.Second):
		t.Fatal("OnDemoted not called after Release")
	}
	require.Equal(t, lease.RoleStandby, e.Role())
}

func TestMemElectorReleaseNotPrimary(t *testing.T) {
	c := NewCluster()
	e := c.NewElector("node1")

	callbackCalled := false
	startElector(t, e, lease.Callbacks{
		OnElected: func(ctx context.Context) {
			callbackCalled = true
		},
		OnDemoted: func() {
			callbackCalled = true
		},
	})

	require.Equal(t, lease.RoleStandby, e.Role())

	err := e.Release()
	require.NoError(t, err)
	require.False(t, callbackCalled)
	require.Equal(t, lease.RoleStandby, e.Role())
}
