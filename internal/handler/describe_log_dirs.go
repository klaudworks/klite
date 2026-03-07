package handler

import (
	"github.com/klaudworks/klite/internal/cluster"
	"github.com/klaudworks/klite/internal/server"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func HandleDescribeLogDirs(state *cluster.State, dataDir string) server.Handler {
	return func(req kmsg.Request) (kmsg.Response, error) {
		r := req.(*kmsg.DescribeLogDirsRequest)
		resp := r.ResponseKind().(*kmsg.DescribeLogDirsResponse)

		rd := kmsg.NewDescribeLogDirsResponseDir()
		rd.Dir = dataDir
		rd.TotalBytes = -1  // TODO: report actual WAL disk usage
		rd.UsableBytes = -1 // TODO: report actual WAL disk available

		if r.Topics == nil {
			for _, td := range state.GetAllTopics() {
				rt := kmsg.NewDescribeLogDirsResponseDirTopic()
				rt.Topic = td.Name
				for _, pd := range td.Partitions {
					rp := kmsg.NewDescribeLogDirsResponseDirTopicPartition()
					rp.Partition = pd.Index
					pd.RLock()
					rp.Size = pd.TotalBytes()
					pd.RUnlock()
					rt.Partitions = append(rt.Partitions, rp)
				}
				rd.Topics = append(rd.Topics, rt)
			}
		} else {
			for _, reqTopic := range r.Topics {
				td := state.GetTopic(reqTopic.Topic)
				if td == nil {
					continue // Unknown partitions are simply omitted
				}
				rt := kmsg.NewDescribeLogDirsResponseDirTopic()
				rt.Topic = reqTopic.Topic
				for _, p := range reqTopic.Partitions {
					if int(p) >= len(td.Partitions) {
						continue
					}
					pd := td.Partitions[p]
					rp := kmsg.NewDescribeLogDirsResponseDirTopicPartition()
					rp.Partition = p
					pd.RLock()
					rp.Size = pd.TotalBytes()
					pd.RUnlock()
					rt.Partitions = append(rt.Partitions, rp)
				}
				rd.Topics = append(rd.Topics, rt)
			}
		}

		resp.Dirs = append(resp.Dirs, rd)
		return resp, nil
	}
}
