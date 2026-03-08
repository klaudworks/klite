package handler

import (
	"github.com/klaudworks/klite/internal/server"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Kafka API keys used in handler registration and version negotiation.
const (
	APIKeyProduce                 int16 = 0
	APIKeyFetch                   int16 = 1
	APIKeyListOffsets             int16 = 2
	APIKeyMetadata                int16 = 3
	APIKeyOffsetCommit            int16 = 8
	APIKeyOffsetFetch             int16 = 9
	APIKeyFindCoordinator         int16 = 10
	APIKeyJoinGroup               int16 = 11
	APIKeyHeartbeat               int16 = 12
	APIKeyLeaveGroup              int16 = 13
	APIKeySyncGroup               int16 = 14
	APIKeyDescribeGroups          int16 = 15
	APIKeyListGroups              int16 = 16
	APIKeySASLHandshake           int16 = 17
	APIKeyApiVersions             int16 = 18
	APIKeyCreateTopics            int16 = 19
	APIKeyDeleteTopics            int16 = 20
	APIKeyDeleteRecords           int16 = 21
	APIKeyInitProducerID          int16 = 22
	APIKeyOffsetForLeaderEpoch    int16 = 23
	APIKeyAddPartitionsToTxn      int16 = 24
	APIKeyAddOffsetsToTxn         int16 = 25
	APIKeyEndTxn                  int16 = 26
	APIKeyTxnOffsetCommit         int16 = 28
	APIKeyDescribeConfigs         int16 = 32
	APIKeyAlterConfigs            int16 = 33
	APIKeyDescribeLogDirs         int16 = 35
	APIKeySASLAuthenticate        int16 = 36
	APIKeyCreatePartitions        int16 = 37
	APIKeyDeleteGroups            int16 = 42
	APIKeyIncrementalAlterConfigs int16 = 44
	APIKeyOffsetDelete            int16 = 47
	APIKeyDescribeUserScramCreds  int16 = 50
	APIKeyAlterUserScramCreds     int16 = 51
	APIKeyDescribeCluster         int16 = 60
	APIKeyDescribeProducers       int16 = 61
	APIKeyDescribeTransactions    int16 = 65
	APIKeyListTransactions        int16 = 66
)

var apiVersions = []kmsg.ApiVersionsResponseApiKey{
	{ApiKey: APIKeyProduce, MinVersion: 3, MaxVersion: 11},
	{ApiKey: APIKeyFetch, MinVersion: 4, MaxVersion: 16},
	{ApiKey: APIKeyListOffsets, MinVersion: 1, MaxVersion: 8},
	{ApiKey: APIKeyMetadata, MinVersion: 4, MaxVersion: 12},
	{ApiKey: APIKeyApiVersions, MinVersion: 0, MaxVersion: 4},
	{ApiKey: APIKeyCreateTopics, MinVersion: 2, MaxVersion: 7},

	{ApiKey: APIKeyOffsetCommit, MinVersion: 0, MaxVersion: 9},
	{ApiKey: APIKeyOffsetFetch, MinVersion: 0, MaxVersion: 9},
	{ApiKey: APIKeyFindCoordinator, MinVersion: 0, MaxVersion: 6},
	{ApiKey: APIKeyJoinGroup, MinVersion: 0, MaxVersion: 9},
	{ApiKey: APIKeyHeartbeat, MinVersion: 0, MaxVersion: 4},
	{ApiKey: APIKeyLeaveGroup, MinVersion: 0, MaxVersion: 5},
	{ApiKey: APIKeySyncGroup, MinVersion: 0, MaxVersion: 5},
	{ApiKey: APIKeyOffsetDelete, MinVersion: 0, MaxVersion: 0},

	{ApiKey: APIKeyDescribeGroups, MinVersion: 0, MaxVersion: 6},
	{ApiKey: APIKeyListGroups, MinVersion: 0, MaxVersion: 5},
	{ApiKey: APIKeyDeleteTopics, MinVersion: 0, MaxVersion: 6},
	{ApiKey: APIKeyDeleteRecords, MinVersion: 0, MaxVersion: 2},
	{ApiKey: APIKeyOffsetForLeaderEpoch, MinVersion: 3, MaxVersion: 4},
	{ApiKey: APIKeyDescribeConfigs, MinVersion: 0, MaxVersion: 4},
	{ApiKey: APIKeyAlterConfigs, MinVersion: 0, MaxVersion: 2},
	{ApiKey: APIKeyDescribeLogDirs, MinVersion: 0, MaxVersion: 4},
	{ApiKey: APIKeyCreatePartitions, MinVersion: 0, MaxVersion: 3},
	{ApiKey: APIKeyDeleteGroups, MinVersion: 0, MaxVersion: 2},
	{ApiKey: APIKeyIncrementalAlterConfigs, MinVersion: 0, MaxVersion: 1},
	{ApiKey: APIKeyDescribeCluster, MinVersion: 0, MaxVersion: 2},

	{ApiKey: APIKeyInitProducerID, MinVersion: 0, MaxVersion: 5},
	{ApiKey: APIKeyAddPartitionsToTxn, MinVersion: 0, MaxVersion: 4},
	{ApiKey: APIKeyAddOffsetsToTxn, MinVersion: 0, MaxVersion: 3},
	{ApiKey: APIKeyEndTxn, MinVersion: 0, MaxVersion: 4},
	{ApiKey: APIKeyTxnOffsetCommit, MinVersion: 0, MaxVersion: 4},
	{ApiKey: APIKeyDescribeProducers, MinVersion: 0, MaxVersion: 0},
	{ApiKey: APIKeyDescribeTransactions, MinVersion: 0, MaxVersion: 0},
	{ApiKey: APIKeyListTransactions, MinVersion: 0, MaxVersion: 1},

	{ApiKey: APIKeySASLHandshake, MinVersion: 1, MaxVersion: 1}, // v0 not supported
	{ApiKey: APIKeySASLAuthenticate, MinVersion: 0, MaxVersion: 2},
	{ApiKey: APIKeyDescribeUserScramCreds, MinVersion: 0, MaxVersion: 0},
	{ApiKey: APIKeyAlterUserScramCreds, MinVersion: 0, MaxVersion: 0},
}

var apiVersionsMap map[int16]kmsg.ApiVersionsResponseApiKey

func init() {
	apiVersionsMap = make(map[int16]kmsg.ApiVersionsResponseApiKey, len(apiVersions))
	for _, v := range apiVersions {
		apiVersionsMap[v.ApiKey] = v
	}
}

func VersionRange(key int16) (min, max int16, ok bool) {
	v, exists := apiVersionsMap[key]
	if !exists {
		return 0, 0, false
	}
	return v.MinVersion, v.MaxVersion, true
}

// HandleApiVersions returns the ApiVersions handler. Even for unsupported
// request versions, we return the full version list with UNSUPPORTED_VERSION
// so the client can negotiate down.
func HandleApiVersions() server.Handler {
	return func(req kmsg.Request) (kmsg.Response, error) {
		r := req.(*kmsg.ApiVersionsRequest)
		resp := r.ResponseKind().(*kmsg.ApiVersionsResponse)

		minV, maxV, ok := VersionRange(APIKeyApiVersions)
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
