# SASL Authentication

SASLHandshake, SASLAuthenticate, AlterUserScramCredentials,
DescribeUserScramCredentials. Phase 5 — first feature after the core broker
is complete with transactions and S3.

Prerequisite: Phase 1 complete (TCP listener, wire framing, request dispatch).
SASL has no dependency on consumer groups, persistence, or transactions — it
gates the connection layer. However, it is scheduled as Phase 5 because it is
not needed for functional testing of the core data path, and credential
persistence depends on `metadata.log` (Phase 3).

Read before starting: `03-tcp-and-wire.md` (connection goroutines, dispatch,
per-connection state), `01-foundations.md` (config, lifecycle),
`16-metadata-log.md` (metadata.log entry types — SCRAM credentials are
persisted here).

---

## Overview

Without authentication, klite cannot be deployed anywhere with network
exposure. SASL is the standard Kafka authentication mechanism. We support
three mechanisms:

| Mechanism | Rounds | Description |
|-----------|--------|-------------|
| PLAIN | 1 | Username+password in cleartext (must use TLS in production) |
| SCRAM-SHA-256 | 2 | Challenge-response, password never sent over wire |
| SCRAM-SHA-512 | 2 | Same as above with SHA-512 |

**Not supported:** OAUTHBEARER (complex token validation, minimal value for
single-broker), GSSAPI/Kerberos (enterprise infrastructure dependency).

This matches kfake's mechanism support exactly.

---

## User Provisioning

Users can be added through three methods:

### 1. Config File (Startup)

TOML config file defines initial users:

```toml
[[sasl.users]]
mechanism = "SCRAM-SHA-256"
username = "admin"
password = "admin-secret"

[[sasl.users]]
mechanism = "SCRAM-SHA-256"
username = "app-producer"
password = "producer-secret"

[[sasl.users]]
mechanism = "PLAIN"
username = "dev"
password = "dev-password"
```

On startup, the broker:
1. Reads user entries from config
2. For SCRAM users: derives `saltedPassword` via PBKDF2 (random salt, 4096
   iterations), then computes `storedKey` and `serverKey` per RFC 5802
3. For PLAIN users: stores username -> password mapping in memory
4. Stores all credentials in the in-memory `sasls` struct

Config-file users are the bootstrap mechanism. They are loaded on every startup
and override any persisted credentials for the same (mechanism, username) pair.
This ensures the admin can always recover access by editing the config file.

### 2. CLI Flags (Startup)

For quick single-user setup (dev/test):

```
klite --sasl-mechanism SCRAM-SHA-256 --sasl-user admin --sasl-password admin-secret
```

Equivalent to a single-entry config file. If both flag and config file specify
users, they are merged (flag users added after config file users, overriding
on conflict).

### 3. AlterUserScramCredentials API (Runtime)

Standard Kafka API (key 51) for dynamic user management. Clients send
pre-hashed credentials (the client derives `saltedPassword` via PBKDF2 before
sending — the cleartext password never crosses the wire).

Runtime credential changes are persisted to `metadata.log` as
`SCRAM_CREDENTIAL` and `SCRAM_CREDENTIAL_DELETE` entries (see "Persistence"
section below). On restart, the broker replays these entries to rebuild the
credential store, then applies config-file users on top.

### Startup Credential Load Order

```
1. Replay metadata.log -> populate sasls with persisted SCRAM credentials
2. Load config file users -> add/override into sasls
3. Load CLI flag users -> add/override into sasls
4. If SASL is enabled and sasls is empty -> log error and refuse to start
```

Config-file and CLI users always win over persisted credentials. This is
intentional: if an admin locks themselves out via the API, they can recover
by adding a user to the config file and restarting.

**PLAIN users are config-only.** They cannot be created or deleted via the API
(there is no Kafka API for PLAIN credential management). PLAIN users exist
only in memory, loaded from config/flags on each startup.

---

## Connection-Level State Machine

SASL authentication is a per-connection gate. When SASL is enabled, every new
connection starts in `saslStageBegin` and must complete authentication before
any data-path requests are allowed.

### States

```go
type saslStage uint8

const (
    saslStageBegin         saslStage = iota // Only ApiVersions + SASLHandshake allowed
    saslStageAuthPlain                      // SASLAuthenticate: 1 round (PLAIN)
    saslStageAuthScram0_256                 // SASLAuthenticate: round 1 of 2 (SCRAM-SHA-256)
    saslStageAuthScram0_512                 // SASLAuthenticate: round 1 of 2 (SCRAM-SHA-512)
    saslStageAuthScram1                     // SASLAuthenticate: round 2 of 2 (SCRAM)
    saslStageComplete                       // Authenticated — all requests allowed
)
```

### Per-Connection Fields

Add to the connection struct (`server/conn.go`):

```go
type clientConn struct {
    // ... existing fields ...
    saslStage saslStage      // current auth state
    scramS0   *scramServer0  // SCRAM server-first state (between rounds 1 and 2)
    user      string         // authenticated username (set after auth completes)
}
```

### Gate Check

Before dispatching any request, check the SASL gate. This check runs in the
read goroutine or at the start of the handler goroutine, before the handler
function is called:

```go
func (cc *clientConn) saslAllowed(kreq kmsg.Request) bool {
    switch cc.saslStage {
    case saslStageBegin:
        // Only ApiVersions and SASLHandshake are allowed before auth
        switch kreq.(type) {
        case *kmsg.ApiVersionsRequest, *kmsg.SASLHandshakeRequest:
            return true
        default:
            return false
        }
    case saslStageAuthPlain, saslStageAuthScram0_256,
         saslStageAuthScram0_512, saslStageAuthScram1:
        // Mid-auth: only ApiVersions and SASLAuthenticate
        switch kreq.(type) {
        case *kmsg.ApiVersionsRequest, *kmsg.SASLAuthenticateRequest:
            return true
        default:
            return false
        }
    case saslStageComplete:
        return true
    default:
        panic("unreachable")
    }
}
```

If `saslAllowed` returns false, close the connection (send no response). This
matches Kafka behavior — sending a non-auth request before completing SASL
causes the broker to close the socket.

When SASL is disabled (`--sasl-enabled=false`, the default), skip the gate
entirely — all connections start at `saslStageComplete` effectively.

---

## SASLHandshake (Key 17)

### Behavior

Client declares which SASL mechanism it wants to use. Server confirms the
mechanism is supported and transitions the connection to the appropriate
auth stage.

### Version Notes

- **v0**: Not supported. v0 uses an implicit auth flow where auth bytes are
  sent raw on the socket without SASLAuthenticate framing. No modern client
  uses v0. Return `UNSUPPORTED_VERSION`.
- **v1**: Supported. Auth bytes are exchanged via SASLAuthenticate requests.

### Implementation

```
1. Validate connection is in saslStageBegin (else ILLEGAL_SASL_STATE)
2. Match req.Mechanism against supported mechanisms:
   - "PLAIN"          -> set saslStage = saslStageAuthPlain
   - "SCRAM-SHA-256"  -> set saslStage = saslStageAuthScram0_256
   - "SCRAM-SHA-512"  -> set saslStage = saslStageAuthScram0_512
   - anything else    -> UNSUPPORTED_SASL_MECHANISM, include supported list
3. Return response (no auth bytes yet — those come in SASLAuthenticate)
```

### Error Responses

| Condition | Error Code |
|-----------|------------|
| Not in saslStageBegin | `ILLEGAL_SASL_STATE` |
| Unknown mechanism | `UNSUPPORTED_SASL_MECHANISM` |

### kfake Reference

- `.cache/repos/franz-go/pkg/kfake/17_sasl_handshake.go`

---

## SASLAuthenticate (Key 36)

### Behavior

Exchanges opaque SASL authentication bytes. The number of rounds depends on
the mechanism:
- PLAIN: 1 round (client sends credentials, server validates)
- SCRAM: 2 rounds (client-first + server-first, then client-final + server-final)

### Version Notes

- **v0-v2**: All supported.
  - v1+: `SessionLifetimeMs` in response (we return 0 — no session expiry)
  - v2+: Flexible versions

### PLAIN Authentication (1 Round)

The client sends auth bytes in the format: `\0<username>\0<password>` (with an
optional authzid prefix: `<authzid>\0<username>\0<password>`).

```
1. Parse auth bytes: split by \0 into [authzid, username, password]
2. If authzid is non-empty and != username -> reject
3. Look up username in sasls.plain map
4. If not found or password doesn't match -> close connection (return error)
5. Set cc.user = username
6. Set cc.saslStage = saslStageComplete
7. Return empty SASLAuthBytes in response
```

On authentication failure: return an error that causes the connection to close.
Do NOT return a Kafka error response with an error code — Kafka closes the
connection on PLAIN auth failure. Match this behavior.

### SCRAM Authentication (2 Rounds)

Implements RFC 5802 server-side. The SCRAM exchange proves the client knows the
password without sending it over the wire.

**Round 1 (client-first-message):**

```
1. Parse client-first-message: n,,n=<username>,r=<client-nonce>[,extensions]
   - Reject extensions (we don't support any)
   - If authzid (a=...) is present and != username -> reject
2. Look up username in sasls.scram256 or sasls.scram512
   - If not found -> close connection
3. Generate server nonce: append random bytes to client nonce
4. Build server-first-message: r=<combined-nonce>,s=<base64-salt>,i=<iterations>
5. Store intermediate state in cc.scramS0 (needed for round 2)
6. Set cc.saslStage = saslStageAuthScram1
7. Return server-first-message as SASLAuthBytes
```

**Round 2 (client-final-message):**

```
1. Parse client-final-message: c=<channel-binding>,r=<nonce>,p=<proof>
2. Verify channel binding is "biws" (base64 of "n,,")
3. Compute expected proof using stored saltedPassword:
   - ClientKey = HMAC(SaltedPassword, "Client Key")
   - StoredKey = H(ClientKey)
   - AuthMessage = client-first-bare + "," + server-first + "," + client-final-without-proof
   - ClientSignature = HMAC(StoredKey, AuthMessage)
   - Expected: ClientProof XOR ClientSignature should equal ClientKey
   - H(result) should equal StoredKey
4. If verification fails -> close connection
5. Compute server signature for the response:
   - ServerKey = HMAC(SaltedPassword, "Server Key")
   - ServerSignature = HMAC(ServerKey, AuthMessage)
6. Build server-final-message: v=<base64-server-signature>
7. Clear cc.scramS0
8. Set cc.user = username
9. Set cc.saslStage = saslStageComplete
10. Return server-final-message as SASLAuthBytes
```

### SCRAM Credential Storage

```go
type scramAuth struct {
    mechanism  string // "SCRAM-SHA-256" or "SCRAM-SHA-512"
    iterations int    // PBKDF2 iterations (min 4096, max 16384)
    saltedPass []byte // PBKDF2(password, salt, iterations, hashSize)
    salt       []byte // random salt (10 bytes)
}

// In-memory credential store
type sasls struct {
    plain    map[string]string     // username -> password
    scram256 map[string]scramAuth  // username -> SCRAM-SHA-256 credential
    scram512 map[string]scramAuth  // username -> SCRAM-SHA-512 credential
}
```

When creating SCRAM credentials from a plaintext password (config file/CLI):

```go
func newScramAuth(mechanism, password string) scramAuth {
    salt := randomBytes(10)
    var saltedPass []byte
    switch mechanism {
    case "SCRAM-SHA-256":
        saltedPass = pbkdf2.Key([]byte(password), salt, 4096, sha256.Size, sha256.New)
    case "SCRAM-SHA-512":
        saltedPass = pbkdf2.Key([]byte(password), salt, 4096, sha512.Size, sha512.New)
    }
    return scramAuth{mechanism: mechanism, iterations: 4096, saltedPass: saltedPass, salt: salt}
}
```

When receiving credentials via AlterUserScramCredentials API, the client sends
pre-derived `saltedPassword` and `salt` — no PBKDF2 derivation needed server-side.

### Error Responses

| Condition | Error Code / Action |
|-----------|---------------------|
| Wrong SASL stage | `ILLEGAL_SASL_STATE` |
| Invalid PLAIN format | Close connection |
| PLAIN: unknown user | Close connection |
| PLAIN: wrong password | Close connection |
| SCRAM: invalid client-first | Close connection |
| SCRAM: unknown user | Close connection |
| SCRAM: invalid proof | Close connection |

### kfake Reference

- `.cache/repos/franz-go/pkg/kfake/36_sasl_authenticate.go`
- `.cache/repos/franz-go/pkg/kfake/sasl.go` (SCRAM crypto, regex parsing)

---

## AlterUserScramCredentials (Key 51)

### Behavior

Creates, updates, or deletes SCRAM credentials at runtime. This is the standard
Kafka API for dynamic user management.

**Important:** The client sends pre-hashed credentials. The `SaltedPassword`
field contains the output of `PBKDF2(password, salt, iterations)`, NOT the
cleartext password. The cleartext password never crosses the wire.

### Version Notes

- **v0**: Supported (only version).

### Implementation

```
1. Validate all operations up front (collect errors):
   - Empty username -> UNACCEPTABLE_CREDENTIAL
   - Unknown mechanism (not 1=SHA-256 or 2=SHA-512) -> UNSUPPORTED_SASL_MECHANISM
   - Iterations < 4096 or > 16384 -> UNACCEPTABLE_CREDENTIAL
   - Same user in both deletions and upsertions -> DUPLICATE_RESOURCE
2. Process deletions:
   - Look up user in scram256/scram512 map
   - If not found -> RESOURCE_NOT_FOUND
   - Delete from map
3. Process upsertions:
   - Store scramAuth{mechanism, iterations, saltedPass, salt} in map
4. Persist to metadata.log (see Persistence section)
5. Return per-user results
```

### Persistence

Each successful upsert writes a `SCRAM_CREDENTIAL` entry to `metadata.log`:

```go
type ScramCredentialEntry struct {
    Username   string
    Mechanism  int8   // 1=SHA-256, 2=SHA-512
    Iterations int32
    Salt       []byte
    SaltedPass []byte
}
```

Each successful deletion writes a `SCRAM_CREDENTIAL_DELETE` entry:

```go
type ScramCredentialDeleteEntry struct {
    Username  string
    Mechanism int8
}
```

On startup replay, these entries rebuild the SCRAM credential maps. Config-file
users are then applied on top (see "Startup Credential Load Order" above).

### Error Responses

| Condition | Error Code |
|-----------|------------|
| Empty username | `UNACCEPTABLE_CREDENTIAL` |
| Invalid mechanism | `UNSUPPORTED_SASL_MECHANISM` |
| Iterations out of range | `UNACCEPTABLE_CREDENTIAL` |
| Duplicate user across ops | `DUPLICATE_RESOURCE` |
| Delete non-existent user | `RESOURCE_NOT_FOUND` |

### kfake Reference

- `.cache/repos/franz-go/pkg/kfake/51_alter_user_scram_credentials.go`

---

## DescribeUserScramCredentials (Key 50)

### Behavior

Returns SCRAM credential metadata (mechanism, iterations) for specified users.
If the user list is null, returns all users with SCRAM credentials. Does NOT
return passwords, salts, or keys — only the mechanism and iteration count.

### Version Notes

- **v0**: Supported (only version).

### Implementation

```
1. If req.Users is null -> enumerate all users from scram256 + scram512 maps
2. For each requested user:
   - If duplicated in the request -> DUPLICATE_RESOURCE
   - Look up in scram256 and scram512 maps
   - Return CredentialInfos (mechanism + iterations) for each match
   - If user has no SCRAM credentials -> RESOURCE_NOT_FOUND
```

### kfake Reference

- `.cache/repos/franz-go/pkg/kfake/50_describe_user_scram_credentials.go`

---

## Configuration

### New Config Options

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--sasl-enabled` | `KLITE_SASL_ENABLED` | `false` | Enable SASL authentication |
| `--sasl-mechanism` | `KLITE_SASL_MECHANISM` | (none) | Mechanism for CLI-specified user |
| `--sasl-user` | `KLITE_SASL_USER` | (none) | Username for CLI-specified user |
| `--sasl-password` | `KLITE_SASL_PASSWORD` | (none) | Password for CLI-specified user |

The `--sasl-mechanism`, `--sasl-user`, and `--sasl-password` flags are a
convenience for creating a single bootstrap user without a config file. For
multiple users, use the config file.

### Config File Section

```toml
[sasl]
enabled = true

[[sasl.users]]
mechanism = "SCRAM-SHA-256"
username = "admin"
password = "admin-secret"

[[sasl.users]]
mechanism = "PLAIN"
username = "dev"
password = "dev-password"
```

### ApiVersions Update

When SASL is enabled, the ApiVersions response must include keys 17 and 36
(and 50, 51 if credential management is supported). These keys should always
be advertised regardless of SASL being enabled — a client may query
ApiVersions before knowing whether SASL is required.

---

## Implementation Sequence

### Step 1: SASL Credential Store

Add the `sasls` struct, `scramAuth` type, `saslStage` enum, and
`newScramAuth()` function. Add SASL config fields to the broker config.
Add SASL-related fields to the connection struct.

### Step 2: Connection Gate

Add the `saslAllowed()` check to request dispatch. When SASL is enabled,
connections start at `saslStageBegin`. When disabled, the gate is a no-op.

### Step 3: SASLHandshake Handler (Key 17)

Register handler. Validate mechanism, transition connection state.

### Step 4: SASLAuthenticate Handler (Key 36)

Implement PLAIN auth (simpler, good for testing the flow). Then implement
SCRAM auth (client-first, server-first, client-final, server-final).

### Step 5: AlterUserScramCredentials (Key 51)

Runtime credential management with metadata.log persistence.

### Step 6: DescribeUserScramCredentials (Key 50)

Read-only credential inspection.

---

## Dependencies

### New Go Dependencies

| Package | Purpose |
|---------|---------|
| `golang.org/x/crypto/pbkdf2` | PBKDF2 key derivation for SCRAM |

`crypto/hmac`, `crypto/sha256`, `crypto/sha512`, `encoding/base64`, and
`regexp` are all stdlib — no additional dependencies needed.

### Interaction with Existing Components

- **`server/conn.go`**: Add `saslStage`, `scramS0`, `user` fields to
  `clientConn`. Add `saslAllowed()` method.
- **`server/dispatch.go`**: Add SASL gate check before handler dispatch.
- **`handler/`**: Two new handler files: `sasl_handshake.go` (key 17),
  `sasl_authenticate.go` (key 36). Plus `alter_user_scram_credentials.go`
  (key 51), `describe_user_scram_credentials.go` (key 50).
- **`broker/config.go`**: Add SASL config fields.
- **`metadata/entries.go`**: Add `SCRAM_CREDENTIAL` and
  `SCRAM_CREDENTIAL_DELETE` entry types.
- **`internal/sasl/`**: New package for SCRAM crypto (shared between handlers
  and credential management). Contains `scram.go` (PBKDF2, HMAC, regex
  parsing), `plain.go` (PLAIN parsing).

---

## When Stuck

These references are for understanding correct behavior and gathering context,
not for copying patterns. Find the optimal solution for our architecture.

1. Read `.cache/repos/franz-go/pkg/kfake/sasl.go` (297 lines) — the primary
   reference for SCRAM server-side implementation. Clean Go code implementing
   RFC 5802: regex parsing of client-first/client-final messages, PBKDF2 +
   HMAC computation, server-first/server-final generation.
2. Read `.cache/repos/franz-go/pkg/kfake/17_sasl_handshake.go` and
   `.cache/repos/franz-go/pkg/kfake/36_sasl_authenticate.go` — handler
   structure and state machine transitions.
3. Read `.cache/repos/franz-go/pkg/kfake/50_describe_user_scram_credentials.go`
   and `.cache/repos/franz-go/pkg/kfake/51_alter_user_scram_credentials.go` —
   credential management handlers (validation rules, error codes).
4. Read `.cache/repos/franz-go/pkg/kfake/config.go` — how kfake configures
   SASL users programmatically (`EnableSASL()`, `Superuser()`, `User()`).
5. For the SCRAM protocol spec, read RFC 5802:
   `https://datatracker.ietf.org/doc/html/rfc5802`
6. For Kafka's credential storage in KRaft mode, search
   `.cache/repos/kafka/` for `ScramControlManager` and
   `UserScramCredentialRecord` — Kafka stores SCRAM credentials as KRaft
   metadata records, similar to how we use metadata.log entries.
7. See `00b-testing.md` "When Stuck: Escalation Path" for additional tools.

---

## Acceptance Criteria

### Smoke Tests

- `TestSASLPlainSuccess` — connect with correct PLAIN credentials, produce succeeds
- `TestSASLPlainBadPassword` — connect with wrong password, connection rejected
- `TestSASLPlainNoAuth` — SASL enabled, client connects without auth, first
  non-ApiVersions request causes disconnect
- `TestSASLScram256Success` — connect with SCRAM-SHA-256, produce succeeds
- `TestSASLScram512Success` — connect with SCRAM-SHA-512, produce succeeds
- `TestSASLScramBadPassword` — SCRAM with wrong password, connection rejected
- `TestSASLUnsupportedMechanism` — request OAUTHBEARER, get error + supported list
- `TestSASLDisabled` — SASL disabled (default), connections work without auth
- `TestAlterUserScramCredentials` — create user via API, authenticate as new user
- `TestAlterUserScramDeleteUser` — delete user via API, subsequent auth fails
- `TestDescribeUserScramCredentials` — create users, describe returns correct info
- `TestDescribeUserScramCredentialsAll` — null user list returns all SCRAM users
- `TestSASLConfigFileUsers` — users from config file can authenticate
- `TestSASLCredentialPersistence` — create user via API, restart broker, user
  can still authenticate (requires Phase 3 metadata.log)

### Verify

```bash
# This work unit:
go test ./internal/sasl/ -v -race -count=5
go test ./test/integration/ -run 'TestSASL|TestAlterUserScram|TestDescribeUserScram' -v -race -count=5

# Regression check (all prior work units):
go test ./... -race -count=5
```

### Commit

After tests pass, stage and commit all changes:

```bash
git add -A
git commit -m "20: SASL authentication — PLAIN, SCRAM-SHA-256/512, credential management"
```
