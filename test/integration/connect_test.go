package integration

import (
	"net"
	"testing"
	"time"
)

// TestBrokerAcceptsConnection verifies the broker listens and accepts TCP connections.
func TestBrokerAcceptsConnection(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t)

	conn, err := net.DialTimeout("tcp", tb.Addr, 2*time.Second)
	if err != nil {
		t.Fatal("failed to connect to broker:", err)
	}
	conn.Close()
}
