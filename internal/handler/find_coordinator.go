package handler

import (
	"net"
	"strconv"

	"github.com/klaudworks/klite/internal/server"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// FindCoordinatorConfig holds the configuration for the FindCoordinator handler.
type FindCoordinatorConfig struct {
	NodeID         int32
	AdvertisedAddr string // host:port
}

func HandleFindCoordinator(cfg FindCoordinatorConfig) server.Handler {
	advHost, advPortStr, err := net.SplitHostPort(cfg.AdvertisedAddr)
	if err != nil {
		advHost = cfg.AdvertisedAddr
		advPortStr = "9092"
	}
	advPort, _ := strconv.Atoi(advPortStr)

	return func(req kmsg.Request) (kmsg.Response, error) {
		r := req.(*kmsg.FindCoordinatorRequest)
		resp := r.ResponseKind().(*kmsg.FindCoordinatorResponse)

		minV, maxV, ok := VersionRange(10)
		if !ok || r.Version < minV || r.Version > maxV {
			return resp, nil
		}

		var unknownType bool
		if r.CoordinatorType != 0 && r.CoordinatorType != 1 {
			unknownType = true
		}

		// For v0-v3, the request has a single CoordinatorKey field.
		// Normalize by appending it to CoordinatorKeys so we can process uniformly,
		// then copy the first coordinator result back to the top-level fields.
		if r.Version <= 3 {
			r.CoordinatorKeys = append(r.CoordinatorKeys, r.CoordinatorKey)
			defer func() {
				if len(resp.Coordinators) > 0 {
					resp.ErrorCode = resp.Coordinators[0].ErrorCode
					resp.ErrorMessage = resp.Coordinators[0].ErrorMessage
					resp.NodeID = resp.Coordinators[0].NodeID
					resp.Host = resp.Coordinators[0].Host
					resp.Port = resp.Coordinators[0].Port
				}
			}()
		}

		for _, key := range r.CoordinatorKeys {
			sc := kmsg.NewFindCoordinatorResponseCoordinator()
			sc.Key = key

			if unknownType {
				sc.ErrorCode = kerr.InvalidRequest.Code
				msg := "invalid coordinator type"
				sc.ErrorMessage = &msg
			} else {
				sc.NodeID = cfg.NodeID
				sc.Host = advHost
				sc.Port = int32(advPort)
				// ErrorCode defaults to 0 (NO_ERROR)
			}

			resp.Coordinators = append(resp.Coordinators, sc)
		}

		return resp, nil
	}
}
