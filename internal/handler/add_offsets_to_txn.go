package handler

import (
	"github.com/klaudworks/klite/internal/cluster"
	"github.com/klaudworks/klite/internal/server"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func HandleAddOffsetsToTxn(state *cluster.State) server.Handler {
	return func(req kmsg.Request) (kmsg.Response, error) {
		r := req.(*kmsg.AddOffsetsToTxnRequest)
		resp := r.ResponseKind().(*kmsg.AddOffsetsToTxnResponse)

		minV, maxV, ok := VersionRange(25)
		if !ok || r.Version < minV || r.Version > maxV {
			return resp, nil
		}

		errCode := state.PIDManager().AddOffsetsToTxn(r.ProducerID, r.ProducerEpoch, r.Group)
		resp.ErrorCode = errCode

		return resp, nil
	}
}
