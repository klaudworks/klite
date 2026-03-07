package handler

import (
	"github.com/klaudworks/klite/internal/cluster"
	"github.com/klaudworks/klite/internal/server"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func HandleDescribeGroups(state *cluster.State) server.Handler {
	return func(req kmsg.Request) (kmsg.Response, error) {
		r := req.(*kmsg.DescribeGroupsRequest)
		resp := r.ResponseKind().(*kmsg.DescribeGroupsResponse)

		for _, groupID := range r.Groups {
			sg := kmsg.NewDescribeGroupsResponseGroup()
			sg.Group = groupID

			g := state.GetGroup(groupID)
			if g == nil {
				sg.State = "Dead"
				resp.Groups = append(resp.Groups, sg)
				continue
			}

			g.Control(func() {
				info := g.Describe()
				sg.State = info.State
				sg.ProtocolType = info.ProtocolType
				sg.Protocol = info.Protocol
				for _, m := range info.Members {
					sm := kmsg.NewDescribeGroupsResponseGroupMember()
					sm.MemberID = m.MemberID
					sm.InstanceID = m.InstanceID
					sm.ProtocolMetadata = m.ProtocolMetadata
					sm.MemberAssignment = m.Assignment
					sg.Members = append(sg.Members, sm)
				}
			})

			resp.Groups = append(resp.Groups, sg)
		}

		return resp, nil
	}
}

func HandleListGroups(state *cluster.State) server.Handler {
	return func(req kmsg.Request) (kmsg.Response, error) {
		r := req.(*kmsg.ListGroupsRequest)
		resp := r.ResponseKind().(*kmsg.ListGroupsResponse)

		groups := state.GetAllGroups()
		for _, g := range groups {
			g.Control(func() {
				info := g.Describe()

				if len(r.StatesFilter) > 0 {
					found := false
					for _, sf := range r.StatesFilter {
						if sf == info.State {
							found = true
							break
						}
					}
					if !found {
						return
					}
				}

				sg := kmsg.NewListGroupsResponseGroup()
				sg.Group = g.ID()
				sg.ProtocolType = info.ProtocolType
				sg.GroupState = info.State
				resp.Groups = append(resp.Groups, sg)
			})
		}

		return resp, nil
	}
}

func HandleDeleteGroups(state *cluster.State) server.Handler {
	return func(req kmsg.Request) (kmsg.Response, error) {
		r := req.(*kmsg.DeleteGroupsRequest)
		resp := r.ResponseKind().(*kmsg.DeleteGroupsResponse)

		for _, groupID := range r.Groups {
			sg := kmsg.NewDeleteGroupsResponseGroup()
			sg.Group = groupID

			g := state.GetGroup(groupID)
			if g == nil {
				sg.ErrorCode = kerr.GroupIDNotFound.Code
				resp.Groups = append(resp.Groups, sg)
				continue
			}

			var canDelete bool
			g.Control(func() {
				info := g.Describe()
				switch info.State {
				case "Empty", "Dead":
					canDelete = true
				default:
					sg.ErrorCode = kerr.NonEmptyGroup.Code
				}
			})

			if canDelete {
				state.DeleteGroup(groupID)
			}

			resp.Groups = append(resp.Groups, sg)
		}

		return resp, nil
	}
}
