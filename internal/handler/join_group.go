package handler

import (
	"github.com/klaudworks/klite/internal/cluster"
	"github.com/klaudworks/klite/internal/server"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// HandleJoinGroup returns the JoinGroup handler (API key 11).
func HandleJoinGroup(state *cluster.State) server.Handler {
	return func(req kmsg.Request) (kmsg.Response, error) {
		r := req.(*kmsg.JoinGroupRequest)
		resp := r.ResponseKind().(*kmsg.JoinGroupResponse)

		// Validate group ID
		if r.Group == "" {
			resp.ErrorCode = kerr.InvalidGroupID.Code
			return resp, nil
		}

		// Get or create the group
		g := state.GetOrCreateGroup(r.Group)

		// Send request to group goroutine and wait for response
		gresp, err := g.Send(r)
		if err != nil {
			resp.ErrorCode = kerr.CoordinatorNotAvailable.Code
			return resp, nil
		}
		return gresp, nil
	}
}
