package handler

import (
	"github.com/klaudworks/klite/internal/cluster"
	"github.com/klaudworks/klite/internal/server"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// HandleSyncGroup returns the SyncGroup handler (API key 14).
func HandleSyncGroup(state *cluster.State) server.Handler {
	return func(req kmsg.Request) (kmsg.Response, error) {
		r := req.(*kmsg.SyncGroupRequest)
		resp := r.ResponseKind().(*kmsg.SyncGroupResponse)

		if r.Group == "" {
			resp.ErrorCode = kerr.InvalidGroupID.Code
			return resp, nil
		}

		g := state.GetGroup(r.Group)
		if g == nil {
			resp.ErrorCode = kerr.UnknownMemberID.Code
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
