package handler

import (
	"log/slog"

	"github.com/klaudworks/klite/internal/metadata"
	"github.com/klaudworks/klite/internal/sasl"
	"github.com/klaudworks/klite/internal/server"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// HandleAlterUserScramCredentials returns the handler for key 51.
func HandleAlterUserScramCredentials(store *sasl.Store, metaLog *metadata.Log) server.Handler {
	return func(req kmsg.Request) (kmsg.Response, error) {
		r := req.(*kmsg.AlterUserSCRAMCredentialsRequest)
		resp := r.ResponseKind().(*kmsg.AlterUserSCRAMCredentialsResponse)

		minV, maxV, ok := VersionRange(51)
		if !ok || r.Version < minV || r.Version > maxV {
			return resp, nil
		}

		// Validate all operations up front
		users := make(map[string]int16) // track validation results

		for _, d := range r.Deletions {
			if d.Name == "" {
				users[d.Name] = kerr.UnacceptableCredential.Code
				continue
			}
			if d.Mechanism != 1 && d.Mechanism != 2 {
				users[d.Name] = kerr.UnsupportedSaslMechanism.Code
				continue
			}
			users[d.Name] = 0
		}
		for _, u := range r.Upsertions {
			if u.Name == "" || u.Iterations < sasl.MinIterations || u.Iterations > sasl.MaxIterations {
				users[u.Name] = kerr.UnacceptableCredential.Code
				continue
			}
			if u.Mechanism != 1 && u.Mechanism != 2 {
				users[u.Name] = kerr.UnsupportedSaslMechanism.Code
				continue
			}
			if code, exists := users[u.Name]; exists && code == 0 {
				users[u.Name] = kerr.DuplicateResource.Code
				continue
			}
			users[u.Name] = 0
		}

		// Add validation failures to response
		for u, code := range users {
			if code != 0 {
				sr := kmsg.NewAlterUserSCRAMCredentialsResponseResult()
				sr.User = u
				sr.ErrorCode = code
				resp.Results = append(resp.Results, sr)
			}
		}

		// Process deletions
		for _, d := range r.Deletions {
			if users[d.Name] != 0 {
				continue
			}
			var deleted bool
			if d.Mechanism == 1 {
				deleted = store.DeleteScram256(d.Name)
			} else {
				deleted = store.DeleteScram512(d.Name)
			}
			sr := kmsg.NewAlterUserSCRAMCredentialsResponseResult()
			sr.User = d.Name
			if !deleted {
				sr.ErrorCode = kerr.ResourceNotFound.Code
			} else if metaLog != nil {
				mech := sasl.MechanismScram256
				if d.Mechanism == 2 {
					mech = sasl.MechanismScram512
				}
				entry := metadata.MarshalScramCredentialDelete(&metadata.ScramCredentialDeleteEntry{
					Username:  d.Name,
					Mechanism: d.Mechanism,
					MechName:  mech,
				})
				if err := metaLog.AppendSync(entry); err != nil {
					slog.Warn("metadata.log: failed to persist SCRAM credential delete",
						"user", d.Name, "err", err)
				}
			}
			resp.Results = append(resp.Results, sr)
		}

		// Process upsertions
		for _, u := range r.Upsertions {
			if users[u.Name] != 0 {
				continue
			}
			mech := sasl.MechanismScram256
			if u.Mechanism == 2 {
				mech = sasl.MechanismScram512
			}
			auth := sasl.ScramAuthFromPreHashed(mech, int(u.Iterations), u.SaltedPassword, u.Salt)
			if u.Mechanism == 1 {
				store.AddScram256(u.Name, auth)
			} else {
				store.AddScram512(u.Name, auth)
			}

		if metaLog != nil {
			entry := metadata.MarshalScramCredential(&metadata.ScramCredentialEntry{
				Username:   u.Name,
				Mechanism:  u.Mechanism,
				Iterations: u.Iterations,
				Salt:       u.Salt,
				SaltedPass: u.SaltedPassword,
			})
			if err := metaLog.AppendSync(entry); err != nil {
				slog.Warn("metadata.log: failed to persist SCRAM credential upsert",
					"user", u.Name, "err", err)
			}
		}

			sr := kmsg.NewAlterUserSCRAMCredentialsResponseResult()
			sr.User = u.Name
			resp.Results = append(resp.Results, sr)
		}

		return resp, nil
	}
}
