package handler

import (
	"github.com/klaudworks/klite/internal/cluster"
	"github.com/klaudworks/klite/internal/server"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func HandleJoinGroup(state *cluster.State) server.Handler {
	return func(req kmsg.Request) (kmsg.Response, error) {
		r := req.(*kmsg.JoinGroupRequest)
		resp := r.ResponseKind().(*kmsg.JoinGroupResponse)

		if r.Group == "" {
			resp.ErrorCode = kerr.InvalidGroupID.Code
			return resp, nil
		}

		g := state.GetOrCreateGroup(r.Group)

		gresp, err := g.Send(r)
		if err != nil {
			resp.ErrorCode = kerr.CoordinatorNotAvailable.Code
			return resp, nil
		}
		return gresp, nil
	}
}
