package handler

import (
	"github.com/klaudworks/klite/internal/cluster"
	"github.com/klaudworks/klite/internal/server"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func HandleOffsetFetch(state *cluster.State) server.Handler {
	return func(req kmsg.Request) (kmsg.Response, error) {
		r := req.(*kmsg.OffsetFetchRequest)
		resp := r.ResponseKind().(*kmsg.OffsetFetchResponse)

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
					fillOffsetFetchGroupNotFound(rg, &gResp)
					resp.Groups = append(resp.Groups, gResp)
					continue
				}

				perGroup := &kmsg.OffsetFetchRequest{Version: r.Version}
				perGroup.Groups = []kmsg.OffsetFetchRequestGroup{rg}

				gresp, err := g.Send(perGroup)
				if err != nil {
					gResp.ErrorCode = kerr.CoordinatorNotAvailable.Code
					resp.Groups = append(resp.Groups, gResp)
					continue
				}

				ofResp := gresp.(*kmsg.OffsetFetchResponse)
				resp.Groups = append(resp.Groups, ofResp.Groups...)
			}
			return resp, nil
		}

		if r.Group == "" {
			resp.ErrorCode = kerr.InvalidGroupID.Code
			return resp, nil
		}

		g := state.GetGroup(r.Group)
		if g == nil {
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
