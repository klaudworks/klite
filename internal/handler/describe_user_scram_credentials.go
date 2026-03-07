package handler

import (
	"github.com/klaudworks/klite/internal/sasl"
	"github.com/klaudworks/klite/internal/server"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func HandleDescribeUserScramCredentials(store *sasl.Store) server.Handler {
	return func(req kmsg.Request) (kmsg.Response, error) {
		r := req.(*kmsg.DescribeUserSCRAMCredentialsRequest)
		resp := r.ResponseKind().(*kmsg.DescribeUserSCRAMCredentialsResponse)

		minV, maxV, ok := VersionRange(50)
		if !ok || r.Version < minV || r.Version > maxV {
			return resp, nil
		}

		describe := make(map[string]bool)
		if r.Users == nil {
			all := store.ListScramUsers()
			for u := range all {
				describe[u] = false
			}
		} else {
			for _, u := range r.Users {
				if _, exists := describe[u.Name]; exists {
					describe[u.Name] = true // duplicate
				} else {
					describe[u.Name] = false
				}
			}
		}

		for u, duplicated := range describe {
			sr := kmsg.NewDescribeUserSCRAMCredentialsResponseResult()
			sr.User = u
			if duplicated {
				sr.ErrorCode = kerr.DuplicateResource.Code
				resp.Results = append(resp.Results, sr)
				continue
			}
			infos := store.ScramUserInfoFor(u)
			if len(infos) == 0 {
				sr.ErrorCode = kerr.ResourceNotFound.Code
				resp.Results = append(resp.Results, sr)
				continue
			}
			for _, info := range infos {
				ci := kmsg.NewDescribeUserSCRAMCredentialsResponseResultCredentialInfo()
				ci.Mechanism = info.Mechanism
				ci.Iterations = info.Iterations
				sr.CredentialInfos = append(sr.CredentialInfos, ci)
			}
			resp.Results = append(resp.Results, sr)
		}

		return resp, nil
	}
}
