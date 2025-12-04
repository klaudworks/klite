package handler

import (
	"github.com/klaudworks/klite/internal/cluster"
	"github.com/klaudworks/klite/internal/server"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// HandleOffsetDelete returns the OffsetDelete handler (API key 47).
func HandleOffsetDelete(state *cluster.State) server.Handler {
	return func(req kmsg.Request) (kmsg.Response, error) {
		r := req.(*kmsg.OffsetDeleteRequest)
		resp := r.ResponseKind().(*kmsg.OffsetDeleteResponse)

		if r.Group == "" {
			resp.ErrorCode = kerr.InvalidGroupID.Code
			return resp, nil
		}

		g := state.GetGroup(r.Group)
		if g == nil {
			resp.ErrorCode = kerr.GroupIDNotFound.Code
			return resp, nil
		}

		gresp, err := g.Send(r)
		if err != nil {
			resp.ErrorCode = kerr.CoordinatorNotAvailable.Code
			return resp, nil
		}
		return gresp, nil
	}
}
