# ApiVersions Handler (Key 18)

First protocol handler. After this work unit, a franz-go client can connect
and complete the ApiVersions handshake. `kcat -b localhost:9092 -L` shows
broker info (but no topics yet — Metadata is in `05-metadata.md`).

Prerequisite: `03-tcp-and-wire.md` complete (TCP + dispatch working).

Read before starting: `00-overview.md` (Supported APIs table — version ranges
to advertise), `03-tcp-and-wire.md` (dispatch table, response assembly —
ApiVersions key=18 header exception).

---

## Behavior

Returns the list of API keys this broker supports, with min/max versions.
This is always the first request from every Kafka client.

## Implementation

```go
func handleApiVersions(ctx context.Context, req kmsg.Request) kmsg.Response {
    r := req.(*kmsg.ApiVersionsRequest)
    resp := r.ResponseKind().(*kmsg.ApiVersionsResponse)

    // Populate supported API keys
    resp.ApiKeys = []kmsg.ApiVersionsResponseApiKey{
        {ApiKey: 0, MinVersion: 3, MaxVersion: 11},   // Produce
        {ApiKey: 1, MinVersion: 4, MaxVersion: 16},    // Fetch
        {ApiKey: 2, MinVersion: 1, MaxVersion: 8},     // ListOffsets
        {ApiKey: 3, MinVersion: 0, MaxVersion: 12},    // Metadata
        {ApiKey: 18, MinVersion: 0, MaxVersion: 4},    // ApiVersions
        {ApiKey: 19, MinVersion: 2, MaxVersion: 7},    // CreateTopics
        // ... add more as implemented in later phases
    }

    return resp
}
```

## Special Protocol Notes

1. **Response header has no tags** even in flexible versions (unlike every other
   API). This is a known Kafka protocol quirk. The write path must special-case
   `apiKey == 18`. See `03-tcp-and-wire.md` "Response Assembly".

2. **Unsupported request version**: still return the full ApiVersions response
   (with `ErrorCode = UNSUPPORTED_VERSION`) so the client can negotiate down.
   Reference: `ApiVersionsRequestTest.scala` `testApiVersionsRequestWithUnsupportedVersion`.

3. **Version ranges**: advertise only the version ranges we actually implement.
   Consult `.cache/repos/franz-go/pkg/kfake/` handler files to understand
   what version-specific fields each API version introduces.

## Reference

- `.cache/repos/franz-go/pkg/kfake/18_api_versions.go` -- see which APIs and version ranges a working Kafka-compatible broker advertises

---

## When Stuck

These references are for understanding correct behavior and gathering context,
not for copying patterns. Find the optimal solution for our architecture.

1. Read `.cache/repos/franz-go/pkg/kfake/18_api_versions.go` -- see how kfake
   populates the version list and handles unsupported versions.
2. Check `.cache/repos/kafka/core/src/test/scala/unit/kafka/server/ApiVersionsRequestTest.scala`
   for the unsupported-version edge case.
3. For wire framing issues (especially the ApiVersions header exception), check
   `.cache/repos/franz-go/pkg/kmsg/` for how `IsFlexible()` and `AppendTo()` work.
4. See `00b-testing.md` "When Stuck: Escalation Path" for additional tools
   (repo explorer, web search, docs).

---

## Acceptance Criteria

- franz-go client connects and completes ApiVersions handshake
- Response contains correct API keys with min/max versions matching
  `00-overview.md` "Supported APIs" table
- Unsupported version request returns `UNSUPPORTED_VERSION` error code
  with full version list
- Smoke tests pass:
  - `TestApiVersions` -- response contains expected API keys
  - `TestApiVersionsUnsupportedVersion` -- error response with version list

### Verify

```bash
# This work unit:
go test ./test/integration/ -run 'TestApiVersions' -v -race -count=5

# Regression check (all prior work units):
go test ./... -race -count=5
```

### Commit

After tests pass, stage and commit all changes:

```bash
git add -A
git commit -m "04: ApiVersions handler — client handshake and version negotiation"
```
