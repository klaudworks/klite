package handler

import (
	"net"
	"strconv"

	"github.com/klaudworks/klite/internal/server"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// DescribeClusterConfig holds config for the DescribeCluster handler.
type DescribeClusterConfig struct {
	NodeID         int32
	AdvertisedAddr string // host:port
	ClusterID      string
}

// HandleDescribeCluster returns the DescribeCluster handler (API key 60).
func HandleDescribeCluster(cfg DescribeClusterConfig) server.Handler {
	advHost, advPortStr, _ := net.SplitHostPort(cfg.AdvertisedAddr)
	advPort, _ := strconv.Atoi(advPortStr)

	return func(req kmsg.Request) (kmsg.Response, error) {
		r := req.(*kmsg.DescribeClusterRequest)
		resp := r.ResponseKind().(*kmsg.DescribeClusterResponse)

		sb := kmsg.NewDescribeClusterResponseBroker()
		sb.NodeID = cfg.NodeID
		sb.Host = advHost
		sb.Port = int32(advPort)
		resp.Brokers = append(resp.Brokers, sb)

		resp.ClusterID = cfg.ClusterID
		resp.ControllerID = cfg.NodeID

		return resp, nil
	}
}
