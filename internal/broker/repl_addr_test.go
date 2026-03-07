package broker

import (
	"net"
	"os"
	"testing"
)

func TestResolveReplAddrExplicit(t *testing.T) {
	addr, warn := resolveReplicationAddr(":9093", "10.0.1.5:9093")
	if addr != "10.0.1.5:9093" {
		t.Errorf("expected 10.0.1.5:9093, got %s", addr)
	}
	if warn {
		t.Error("expected no warning")
	}
}

func TestResolveReplAddrSpecificBind(t *testing.T) {
	addr, warn := resolveReplicationAddr("10.0.1.5:9093", "")
	if addr != "10.0.1.5:9093" {
		t.Errorf("expected 10.0.1.5:9093, got %s", addr)
	}
	if warn {
		t.Error("expected no warning")
	}
}

func TestResolveReplAddrWildcard(t *testing.T) {
	tests := []string{":9093", "0.0.0.0:9093"}
	hostname, _ := os.Hostname()
	expected := net.JoinHostPort(hostname, "9093")

	for _, listen := range tests {
		t.Run(listen, func(t *testing.T) {
			addr, warn := resolveReplicationAddr(listen, "")
			if addr != expected {
				t.Errorf("expected %s, got %s", expected, addr)
			}
			if !warn {
				t.Error("expected warning for wildcard bind")
			}
		})
	}
}
