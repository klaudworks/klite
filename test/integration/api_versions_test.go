package integration

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// TestApiVersions verifies that a franz-go client can connect and complete
// the ApiVersions handshake, and that the response contains the expected
// API keys with correct min/max versions.
func TestApiVersions(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t)

	// franz-go automatically sends ApiVersions on first connection.
	// Creating a client that successfully connects proves the handshake works.
	cl := NewClient(t, tb.Addr)

	// Explicitly request ApiVersions to inspect the response.
	req := kmsg.NewApiVersionsRequest()
	req.Version = 3 // flexible version
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	kresp, err := req.RequestWith(ctx, cl)
	require.NoError(t, err)

	resp := kresp
	require.Equal(t, int16(0), resp.ErrorCode, "expected no error")

	// Build a map of returned API keys for easy lookup.
	keys := make(map[int16]struct {
		min, max int16
	})
	for _, k := range resp.ApiKeys {
		keys[k.ApiKey] = struct{ min, max int16 }{k.MinVersion, k.MaxVersion}
	}

	// Verify Phase 1 API keys are present with correct version ranges.
	expectedKeys := []struct {
		key        int16
		name       string
		minVersion int16
		maxVersion int16
	}{
		{0, "Produce", 3, 11},
		{1, "Fetch", 4, 16},
		{2, "ListOffsets", 1, 8},
		{3, "Metadata", 4, 12},
		{18, "ApiVersions", 0, 4},
		{19, "CreateTopics", 2, 7},
	}

	for _, exp := range expectedKeys {
		got, ok := keys[exp.key]
		assert.Truef(t, ok, "expected API key %d (%s) to be present", exp.key, exp.name)
		if ok {
			assert.Equalf(t, exp.minVersion, got.min,
				"API key %d (%s) min version", exp.key, exp.name)
			assert.Equalf(t, exp.maxVersion, got.max,
				"API key %d (%s) max version", exp.key, exp.name)
		}
	}
}

// TestApiVersionsUnsupportedVersion verifies that sending an ApiVersions
// request with an unsupported version returns UNSUPPORTED_VERSION error code
// along with the full version list so the client can negotiate down.
func TestApiVersionsUnsupportedVersion(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t)

	conn, err := net.DialTimeout("tcp", tb.Addr, 2*time.Second)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	// Build an ApiVersions v99 request manually.
	// Header: apiKey(2) + apiVersion(2) + corrID(4) + clientIDLen(2) = 10 bytes
	// v99 is a flexible version (>= v3), so we need tagged fields after client ID.
	var frame []byte
	frame = binary.BigEndian.AppendUint16(frame, 18)     // api_key = 18 (ApiVersions)
	frame = binary.BigEndian.AppendUint16(frame, 99)     // api_version = 99 (unsupported)
	frame = binary.BigEndian.AppendUint32(frame, 42)     // correlation_id = 42
	frame = binary.BigEndian.AppendUint16(frame, 0xFFFF) // client_id = null (-1)
	// Flexible version header tagged fields (compact varint 0 = no tags)
	frame = append(frame, 0)
	// Flexible version ApiVersions request body: ClientSoftwareName(compact string) + ClientSoftwareVersion(compact string) + tags
	frame = append(frame, 0) // empty compact string (length 0)
	frame = append(frame, 0) // empty compact string (length 0)
	frame = append(frame, 0) // no tags

	// Send length-prefixed frame
	var sizeBuf [4]byte
	binary.BigEndian.PutUint32(sizeBuf[:], uint32(len(frame)))
	_, err = conn.Write(sizeBuf[:])
	require.NoError(t, err)
	_, err = conn.Write(frame)
	require.NoError(t, err)

	// Read response
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))

	// Response frame: [4 size][4 corrID][body]
	// Note: ApiVersions response has NO tag byte in the header (protocol quirk)
	var respSizeBuf [4]byte
	_, err = io.ReadFull(conn, respSizeBuf[:])
	require.NoError(t, err, "failed to read response size")

	respSize := binary.BigEndian.Uint32(respSizeBuf[:])
	require.Greater(t, respSize, uint32(4), "response too small")

	respBody := make([]byte, respSize)
	_, err = io.ReadFull(conn, respBody)
	require.NoError(t, err, "failed to read response body")

	// Parse correlation ID
	corrID := int32(binary.BigEndian.Uint32(respBody[:4]))
	assert.Equal(t, int32(42), corrID, "correlation ID mismatch")

	// Parse error code (first 2 bytes of response body after corrID)
	// The response was downgraded to v0, so format is:
	// [2 error_code][4 num_api_keys][per key: 2 api_key + 2 min + 2 max]
	errorCode := int16(binary.BigEndian.Uint16(respBody[4:6]))
	assert.Equal(t, kerr.UnsupportedVersion.Code, errorCode,
		"expected UNSUPPORTED_VERSION error code")

	// Verify the response still contains API keys (v0 format: non-flexible)
	numKeys := int32(binary.BigEndian.Uint32(respBody[6:10]))
	assert.Greater(t, numKeys, int32(0), "expected at least one API key in response")
}
