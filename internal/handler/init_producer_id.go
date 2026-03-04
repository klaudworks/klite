package handler

import (
	"github.com/klaudworks/klite/internal/cluster"
	"github.com/klaudworks/klite/internal/metadata"
	"github.com/klaudworks/klite/internal/server"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// HandleInitProducerID returns the InitProducerID handler (API key 22).
// Supports v0-v5.
func HandleInitProducerID(state *cluster.State) server.Handler {
	return func(req kmsg.Request) (kmsg.Response, error) {
		r := req.(*kmsg.InitProducerIDRequest)
		resp := r.ResponseKind().(*kmsg.InitProducerIDResponse)

		minV, maxV, ok := VersionRange(22)
		if !ok || r.Version < minV || r.Version > maxV {
			resp.ErrorCode = 35 // UNSUPPORTED_VERSION
			return resp, nil
		}

		var txnID string
		if r.TransactionalID != nil {
			txnID = *r.TransactionalID
		}

		pid, epoch, errCode := state.PIDManager().InitProducerID(txnID, r.TransactionTimeoutMillis)
		if errCode != 0 {
			resp.ErrorCode = errCode
			resp.ProducerID = -1
			resp.ProducerEpoch = -1
			return resp, nil
		}

		resp.ProducerID = pid
		resp.ProducerEpoch = epoch

		// Persist next producer ID to metadata.log
		if ml := state.MetadataLog(); ml != nil {
			entry := metadata.MarshalProducerID(&metadata.ProducerIDEntry{
				NextProducerID: state.PIDManager().NextPID(),
			})
			ml.Append(entry) //nolint:errcheck
		}

		return resp, nil
	}
}
