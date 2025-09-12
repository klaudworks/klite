package integration

import (
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"
)

// TestConnect verifies the broker listens and accepts TCP connections.
func TestConnect(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t)

	conn, err := net.DialTimeout("tcp", tb.Addr, 2*time.Second)
	if err != nil {
		t.Fatal("failed to connect to broker:", err)
	}
	conn.Close()
}

// TestMalformedRequest verifies the broker closes the connection when it
// receives a truncated frame (size says 100 bytes but only 5 arrive).
func TestMalformedRequest(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t)

	conn, err := net.DialTimeout("tcp", tb.Addr, 2*time.Second)
	if err != nil {
		t.Fatal("failed to connect:", err)
	}
	defer conn.Close()

	// Send a frame with size=100 but only 5 bytes of body,
	// then close the write side so the broker sees EOF on the body read.
	var sizeBuf [4]byte
	binary.BigEndian.PutUint32(sizeBuf[:], 100)
	conn.Write(sizeBuf[:])
	conn.Write([]byte("short"))
	// Close write side to trigger EOF
	conn.(*net.TCPConn).CloseWrite()

	// The broker should close the connection. Verify by reading until EOF.
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	buf := make([]byte, 1)
	_, err = conn.Read(buf)
	if err == nil {
		t.Fatal("expected connection to be closed by broker")
	}
}

// TestMetadataInvalidApiKey verifies the broker closes the connection when
// it receives a request with an unknown API key.
func TestMetadataInvalidApiKey(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t)

	conn, err := net.DialTimeout("tcp", tb.Addr, 2*time.Second)
	if err != nil {
		t.Fatal("failed to connect:", err)
	}
	defer conn.Close()

	// Build a minimal Kafka request frame with an invalid API key (999).
	// Header: apiKey(2) + apiVersion(2) + corrID(4) + clientIDLen(2) = 10 bytes
	var frame [10]byte
	binary.BigEndian.PutUint16(frame[0:2], 999)  // api_key = 999 (invalid)
	binary.BigEndian.PutUint16(frame[2:4], 0)     // api_version = 0
	binary.BigEndian.PutUint32(frame[4:8], 1)     // correlation_id = 1
	binary.BigEndian.PutUint16(frame[8:10], 0xFFFF) // client_id = null (-1)

	// Send length-prefixed frame
	var sizeBuf [4]byte
	binary.BigEndian.PutUint32(sizeBuf[:], uint32(len(frame)))
	conn.Write(sizeBuf[:])
	conn.Write(frame[:])

	// The broker should close the connection. Read to verify.
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	buf := make([]byte, 1024)
	_, err = io.ReadAtLeast(conn, buf, 1)
	if err == nil {
		t.Fatal("expected connection to be closed by broker after invalid API key")
	}
}
