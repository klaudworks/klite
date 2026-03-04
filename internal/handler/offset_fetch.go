package handler

import (
	"github.com/klaudworks/klite/internal/cluster"
	"github.com/klaudworks/klite/internal/server"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// HandleOffsetFetch returns the OffsetFetch handler (API key 9).
func HandleOffsetFetch(state *cluster.State) server.Handler {
	return func(req kmsg.Request) (kmsg.Response, error) {
		r := req.(*kmsg.OffsetFetchRequest)
		resp := r.ResponseKind().(*kmsg.OffsetFetchResponse)

		// v8+: batch (multiple groups)
		if r.Version >= 8 {
			for _, rg := range r.Groups {
				gResp := kmsg.NewOffsetFetchResponseGroup()
				gResp.Group = rg.Group

				if rg.Group == "" {
					gResp.ErrorCode = kerr.InvalidGroupID.Code
					resp.Groups = append(resp.Groups, gResp)
					continue
				}

				g := state.GetGroup(rg.Group)
				if g == nil {
					// Unknown group: return offset -1 for all requested partitions (not an error)
					fillOffsetFetchGroupNotFound(rg, &gResp)
					resp.Groups = append(resp.Groups, gResp)
					continue
				}

				gresp, err := g.Send(r)
				if err != nil {
					gResp.ErrorCode = kerr.CoordinatorNotAvailable.Code
					resp.Groups = append(resp.Groups, gResp)
					continue
				}
				// The group goroutine returns a fully populated response
				return gresp, nil
			}
			return resp, nil
		}

		// v0-v7: single group
		if r.Group == "" {
			resp.ErrorCode = kerr.InvalidGroupID.Code
			return resp, nil
		}

		g := state.GetGroup(r.Group)
		if g == nil {
			// Unknown group: return offset -1 for all requested partitions
			fillOffsetFetchNotFound(r, resp)
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

// fillOffsetFetchNotFound fills the v0-v7 response with offset=-1 for unknown groups.
func fillOffsetFetchNotFound(req *kmsg.OffsetFetchRequest, resp *kmsg.OffsetFetchResponse) {
	for _, rt := range req.Topics {
		tResp := kmsg.NewOffsetFetchResponseTopic()
		tResp.Topic = rt.Topic
		for _, p := range rt.Partitions {
			pResp := kmsg.NewOffsetFetchResponseTopicPartition()
			pResp.Partition = p
			pResp.Offset = -1
			pResp.LeaderEpoch = -1
			tResp.Partitions = append(tResp.Partitions, pResp)
		}
		resp.Topics = append(resp.Topics, tResp)
	}
}

// fillOffsetFetchGroupNotFound fills a v8+ group response with offset=-1 for unknown groups.
func fillOffsetFetchGroupNotFound(rg kmsg.OffsetFetchRequestGroup, gResp *kmsg.OffsetFetchResponseGroup) {
	for _, rt := range rg.Topics {
		tResp := kmsg.NewOffsetFetchResponseGroupTopic()
		tResp.Topic = rt.Topic
		for _, p := range rt.Partitions {
			pResp := kmsg.NewOffsetFetchResponseGroupTopicPartition()
			pResp.Partition = p
			pResp.Offset = -1
			pResp.LeaderEpoch = -1
			tResp.Partitions = append(tResp.Partitions, pResp)
		}
		gResp.Topics = append(gResp.Topics, tResp)
	}
}
