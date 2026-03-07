# Agent Context

klite is a Kafka-compatible broker in a single Go binary.

## Repo Layout

## Build Conventions

All compiled binaries must be output to the `bin/` directory. Use `-o bin/` when running `go build`:

```sh
go build -o bin/klite ./cmd/klite
```

## Available Tools for Research

When stuck on implementation details, protocol behavior, or edge cases, use
these tools to find answers:

- **Repo explorer subagent**: Use to search `.cache/repos/` efficiently.
  Ask specific questions like "how does kfake handle acks=0 in the produce
  handler?" or "find the ProducerStateManager dedup window in Kafka source."
  Much faster than reading files manually.
- **Docs explorer subagent**: Use to search Kafka's official documentation at
  `https://kafka.apache.org/documentation/` or KIP specifications at
  `https://cwiki.apache.org/confluence/display/KAFKA/`.
- **Perplexity web search**: Use for Kafka protocol questions not answered by
  the local repos. Good for KIP details, wire protocol edge cases, and error
  code semantics.

See `plan/01-initial/00b-testing.md` "When Stuck: Escalation Path" for the full debugging
and research strategy.

## Testing Philosophy

Follow the **testing pyramid**: test at the lowest layer that can verify the behavior.

- **Do not duplicate** coverage across layers — if a unit test already covers a behavior, do not add an integration test for it.
- **Unit tests are the primary test layer** — they cover handler logic, wire encoding/decoding, offset assignment, state machine transitions, and error paths.
- **Integration tests** start the broker in-process on a random port and exercise the real Kafka wire protocol with franz-go clients. Use these for end-to-end API behavior that crosses multiple handlers or requires a real TCP connection.
- **E2E tests** live in `test/e2e/`, gated behind `//go:build e2e`. They exercise real infrastructure via testcontainers: LocalStack for S3, k3s for Kubernetes (Helm deploys, pod labeling, leader election, failover). Use these for behavior that can't be tested at lower layers — S3 flush on shutdown, multi-broker replication failover with real pod deletion, Helm chart correctness. Keep them few and focused. No real AWS or K8s credentials should ever be needed.
- **Do not test third-party library behavior** — e.g., do not write tests that verify franz-go's client encoding. That's their responsibility, not ours.
- When adding a new feature, ask: "What is the cheapest test that can verify this?" — prefer unit over integration.
- **`-race` is deterministic** — running `go test -race -count=5` does not catch more race conditions than `-count=1`. The race detector instruments every memory access; repeating runs only helps catch timing-dependent _logic_ flakiness, not data races. A single `-race -count=1` pass is sufficient.

## Code Comments

- Do not write low-value comments. Comments should be reserved for high-value information that is not easily understood by reading the code alone.
- Keep comments concise and to the point.

## Commit Messages

Follow [Conventional Commits](https://www.conventionalcommits.org/):

## Subagents

- Use subagents sparingly. They are useful when a search would return verbose output that would pollute the main context just to find a small piece of detailed information.
- Do not use the `explore` subagent when gaining a general understanding of the codebase. Read files and grep directly instead.

```

```
