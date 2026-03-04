package integration

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// TestFindCoordinatorGroup tests that FindCoordinator returns self for a group coordinator.
func TestFindCoordinatorGroup(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t)

	cl := NewClient(t, tb.Addr)

	req := kmsg.NewFindCoordinatorRequest()
	req.CoordinatorKey = "test-group"
	req.CoordinatorType = 0 // group

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := req.RequestWith(ctx, cl)
	require.NoError(t, err)
	require.Equal(t, int16(0), resp.ErrorCode, "expected NO_ERROR")

	// Verify the returned host:port matches the broker's advertised address
	advHost, advPort := splitHostPort(t, tb.Addr)
	require.Equal(t, int32(0), resp.NodeID)
	require.Equal(t, advHost, resp.Host)
	require.Equal(t, advPort, resp.Port)
}

// TestFindCoordinatorTransaction tests that FindCoordinator returns self for a transaction coordinator.
func TestFindCoordinatorTransaction(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t)

	cl := NewClient(t, tb.Addr)

	req := kmsg.NewFindCoordinatorRequest()
	req.CoordinatorKey = "test-txn-id"
	req.CoordinatorType = 1 // transaction

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := req.RequestWith(ctx, cl)
	require.NoError(t, err)
	require.Equal(t, int16(0), resp.ErrorCode, "expected NO_ERROR")

	advHost, advPort := splitHostPort(t, tb.Addr)
	require.Equal(t, int32(0), resp.NodeID)
	require.Equal(t, advHost, resp.Host)
	require.Equal(t, advPort, resp.Port)
}

// TestFindCoordinatorBatchLookup tests the v4+ batch lookup (multiple keys in one request).
func TestFindCoordinatorBatchLookup(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t)

	cl := NewClient(t, tb.Addr)

	req := kmsg.NewFindCoordinatorRequest()
	req.Version = 4 // batch lookup
	req.CoordinatorType = 0
	req.CoordinatorKeys = []string{"group-a", "group-b", "group-c"}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := req.RequestWith(ctx, cl)
	require.NoError(t, err)

	advHost, advPort := splitHostPort(t, tb.Addr)

	require.Len(t, resp.Coordinators, 3)
	for i, sc := range resp.Coordinators {
		require.Equal(t, req.CoordinatorKeys[i], sc.Key, "coordinator key mismatch at index %d", i)
		require.Equal(t, int16(0), sc.ErrorCode, "expected NO_ERROR for key %s", sc.Key)
		require.Equal(t, int32(0), sc.NodeID)
		require.Equal(t, advHost, sc.Host)
		require.Equal(t, advPort, sc.Port)
	}
}

// TestFindCoordinatorMultipleGroups tests that different group names all return self.
func TestFindCoordinatorMultipleGroups(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t)

	cl := NewClient(t, tb.Addr)
	advHost, advPort := splitHostPort(t, tb.Addr)

	groups := []string{"group-1", "group-2", "my-consumer-group", ""}
	for _, g := range groups {
		req := kmsg.NewFindCoordinatorRequest()
		req.CoordinatorKey = g
		req.CoordinatorType = 0

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		resp, err := req.RequestWith(ctx, cl)
		cancel()

		require.NoError(t, err, "group=%q", g)
		require.Equal(t, int16(0), resp.ErrorCode, "group=%q", g)
		require.Equal(t, advHost, resp.Host, "group=%q", g)
		require.Equal(t, advPort, resp.Port, "group=%q", g)
	}
}

// TestFindCoordinatorReportedInApiVersions verifies key 10 appears in ApiVersions.
func TestFindCoordinatorReportedInApiVersions(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t)

	cl := NewClient(t, tb.Addr)

	req := kmsg.NewApiVersionsRequest()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := req.RequestWith(ctx, cl)
	require.NoError(t, err)

	var found bool
	for _, ak := range resp.ApiKeys {
		if ak.ApiKey == 10 {
			found = true
			require.Equal(t, int16(0), ak.MinVersion)
			require.Equal(t, int16(6), ak.MaxVersion)
			break
		}
	}
	require.True(t, found, "FindCoordinator (key 10) not found in ApiVersions response")
}

// splitHostPort splits an address into host and port (as int32) for test assertions.
func splitHostPort(t *testing.T, addr string) (string, int32) {
	t.Helper()
	host, portStr, err := net.SplitHostPort(addr)
	require.NoError(t, err)
	port, err := strconv.Atoi(portStr)
	require.NoError(t, err)
	return host, int32(port)
}

// Ensure kgo.Opt is used to avoid lint issues
var _ kgo.Opt = kgo.SeedBrokers("")
