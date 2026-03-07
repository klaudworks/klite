package handler

import (
	"errors"

	"github.com/klaudworks/klite/internal/sasl"
	"github.com/klaudworks/klite/internal/server"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func HandleSASLAuthenticate(store *sasl.Store) server.ConnHandler {
	return func(req kmsg.Request, cc server.ConnContext) (kmsg.Response, error) {
		r := req.(*kmsg.SASLAuthenticateRequest)
		resp := r.ResponseKind().(*kmsg.SASLAuthenticateResponse)

		minV, maxV, ok := VersionRange(36)
		if !ok || r.Version < minV || r.Version > maxV {
			resp.ErrorCode = kerr.UnsupportedVersion.Code
			return resp, nil
		}

		switch *cc.SASLStage {
		case server.SASLStageAuthPlain:
			user, pass, err := sasl.ParsePlain(r.SASLAuthBytes)
			if err != nil {
				return nil, err
			}
			if !store.LookupPlain(user, pass) {
				return nil, errors.New("SASL PLAIN authentication failed")
			}
			*cc.SASLStage = server.SASLStageComplete
			*cc.User = user

		case server.SASLStageAuthScram256:
			c0, err := sasl.ParseScramClient0(r.SASLAuthBytes)
			if err != nil {
				return nil, err
			}
			auth, ok := store.LookupScram256(c0.User)
			if !ok {
				return nil, errors.New("SASL SCRAM authentication failed: unknown user")
			}
			s0, serverFirst := sasl.NewScramServerFirst(c0, auth)
			resp.SASLAuthBytes = serverFirst
			*cc.SASLStage = server.SASLStageAuthScram1
			*cc.ScramS0 = &s0
			*cc.User = c0.User

		case server.SASLStageAuthScram512:
			c0, err := sasl.ParseScramClient0(r.SASLAuthBytes)
			if err != nil {
				return nil, err
			}
			auth, ok := store.LookupScram512(c0.User)
			if !ok {
				return nil, errors.New("SASL SCRAM authentication failed: unknown user")
			}
			s0, serverFirst := sasl.NewScramServerFirst(c0, auth)
			resp.SASLAuthBytes = serverFirst
			*cc.SASLStage = server.SASLStageAuthScram1
			*cc.ScramS0 = &s0
			*cc.User = c0.User

		case server.SASLStageAuthScram1:
			if *cc.ScramS0 == nil {
				return nil, errors.New("SASL SCRAM: no server-first state")
			}
			serverFinal, err := (*cc.ScramS0).ServerFinal(r.SASLAuthBytes)
			if err != nil {
				*cc.User = ""
				return nil, err
			}
			resp.SASLAuthBytes = serverFinal
			*cc.SASLStage = server.SASLStageComplete
			*cc.ScramS0 = nil

		default:
			resp.ErrorCode = kerr.IllegalSaslState.Code
		}

		return resp, nil
	}
}
