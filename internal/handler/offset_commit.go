package handler

import (
	"github.com/klaudworks/klite/internal/cluster"
	"github.com/klaudworks/klite/internal/server"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func HandleOffsetCommit(state *cluster.State) server.Handler {
	return func(req kmsg.Request) (kmsg.Response, error) {
		r := req.(*kmsg.OffsetCommitRequest)
		resp := r.ResponseKind().(*kmsg.OffsetCommitResponse)

		if r.Group == "" {
			fillOffsetCommitError(r, resp, kerr.InvalidGroupID.Code)
			return resp, nil
		}

		// GetOrCreateGroup: simple/admin commits (empty member ID, generation -1)
		// can target groups that don't have active members yet.
		g := state.GetOrCreateGroup(r.Group)

		gresp, err := g.Send(r)
		if err != nil {
			fillOffsetCommitError(r, resp, kerr.CoordinatorNotAvailable.Code)
			return resp, nil
		}
		return gresp, nil
	}
}

func fillOffsetCommitError(req *kmsg.OffsetCommitRequest, resp *kmsg.OffsetCommitResponse, code int16) {
	for _, rt := range req.Topics {
		rtResp := kmsg.NewOffsetCommitResponseTopic()
		rtResp.Topic = rt.Topic
		for _, rp := range rt.Partitions {
			rpResp := kmsg.NewOffsetCommitResponseTopicPartition()
			rpResp.Partition = rp.Partition
			rpResp.ErrorCode = code
			rtResp.Partitions = append(rtResp.Partitions, rpResp)
		}
		resp.Topics = append(resp.Topics, rtResp)
	}
}
