package handler

import (
	"github.com/klaudworks/klite/internal/server"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// apiVersions lists the API keys and version ranges this broker supports.
// This is the single source of truth for the ApiVersions response and for
// version validation in the dispatch path. New handlers added in later phases
// must add their entry here.
var apiVersions = []kmsg.ApiVersionsResponseApiKey{
	// Phase 1: Core data path
	{ApiKey: 0, MinVersion: 3, MaxVersion: 11},  // Produce
	{ApiKey: 1, MinVersion: 4, MaxVersion: 16},  // Fetch
	{ApiKey: 2, MinVersion: 1, MaxVersion: 8},   // ListOffsets
	{ApiKey: 3, MinVersion: 4, MaxVersion: 12},  // Metadata
	{ApiKey: 18, MinVersion: 0, MaxVersion: 4},  // ApiVersions
	{ApiKey: 19, MinVersion: 2, MaxVersion: 7},  // CreateTopics

	// Phase 2: Consumer groups
	{ApiKey: 10, MinVersion: 0, MaxVersion: 6}, // FindCoordinator
}

// apiVersionsMap is a lookup table built from apiVersions for fast version
// validation. Populated by init().
var apiVersionsMap map[int16]kmsg.ApiVersionsResponseApiKey

func init() {
	apiVersionsMap = make(map[int16]kmsg.ApiVersionsResponseApiKey, len(apiVersions))
	for _, v := range apiVersions {
		apiVersionsMap[v.ApiKey] = v
	}
}

// VersionRange returns the supported version range for an API key.
// Returns ok=false if the key is not supported.
func VersionRange(key int16) (min, max int16, ok bool) {
	v, exists := apiVersionsMap[key]
	if !exists {
		return 0, 0, false
	}
	return v.MinVersion, v.MaxVersion, true
}

// HandleApiVersions returns the ApiVersions handler.
// ApiVersions is special: even for unsupported request versions, we return
// the full version list with UNSUPPORTED_VERSION error code so the client
// can negotiate down.
func HandleApiVersions() server.Handler {
	return func(req kmsg.Request) (kmsg.Response, error) {
		r := req.(*kmsg.ApiVersionsRequest)
		resp := r.ResponseKind().(*kmsg.ApiVersionsResponse)

		// Check if the requested version is within our supported range.
		// For ApiVersions specifically, we still return the full response
		// (with error code) so the client can learn our supported versions
		// and downgrade.
		minV, maxV, ok := VersionRange(18)
		if !ok || r.Version < minV || r.Version > maxV {
			// Downgrade response to v0 for maximum compatibility
			resp.Version = 0
			resp.ErrorCode = kerr.UnsupportedVersion.Code
		}

		resp.ApiKeys = make([]kmsg.ApiVersionsResponseApiKey, len(apiVersions))
		copy(resp.ApiKeys, apiVersions)

		return resp, nil
	}
}
