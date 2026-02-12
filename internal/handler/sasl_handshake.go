package handler

import (
	"github.com/klaudworks/klite/internal/sasl"
	"github.com/klaudworks/klite/internal/server"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// HandleSASLHandshake returns the SASLHandshake handler (key 17).
// v0 is not supported (uses implicit auth flow without SASLAuthenticate framing).
// v1 is supported.
func HandleSASLHandshake() server.ConnHandler {
	return func(req kmsg.Request, cc server.ConnContext) (kmsg.Response, error) {
		r := req.(*kmsg.SASLHandshakeRequest)
		resp := r.ResponseKind().(*kmsg.SASLHandshakeResponse)

		minV, maxV, ok := VersionRange(17)
		if !ok || r.Version < minV || r.Version > maxV {
			resp.ErrorCode = kerr.UnsupportedVersion.Code
			return resp, nil
		}

		if *cc.SASLStage != server.SASLStageBegin {
			resp.ErrorCode = kerr.IllegalSaslState.Code
			return resp, nil
		}

		switch r.Mechanism {
		case sasl.MechanismPlain:
			*cc.SASLStage = server.SASLStageAuthPlain
		case sasl.MechanismScram256:
			*cc.SASLStage = server.SASLStageAuthScram256
		case sasl.MechanismScram512:
			*cc.SASLStage = server.SASLStageAuthScram512
		default:
			resp.ErrorCode = kerr.UnsupportedSaslMechanism.Code
			resp.SupportedMechanisms = []string{
				sasl.MechanismPlain,
				sasl.MechanismScram256,
				sasl.MechanismScram512,
			}
		}
		return resp, nil
	}
}
