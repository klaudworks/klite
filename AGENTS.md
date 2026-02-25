# Agent Context

klite is a Kafka-compatible broker in a single Go binary.

## Repo Layout

```
research/          Background research
  testing.md         Conformance testing strategy and coverage matrix
  kafka-internals.md Kafka storage internals and RecordBatch format
  automq.md          AutoMQ WAL + S3 architecture
  warpstream.md      WarpStream zero-disk architecture
  mirrormaker.md     MirrorMaker 2 migration support
  compatibility-survey.md

plan/              Implementation plans (one subdirectory per plan)
  01-initial/          Initial broker implementation
    00-overview.md         Vision, scope, compatibility, execution order
    00b-testing.md         General reference: test infrastructure, tiers, kfake adaptation
    01-foundations.md      General reference: project structure, config, lifecycle, concurrency model

    # Phase 1 work units
    02-project-setup.md         Scaffold repo, go mod, main.go, config, broker lifecycle
    03-tcp-and-wire.md          TCP listener, connection goroutines, wire framing, dispatch
    04-api-versions.md          ApiVersions handler
    05-metadata.md              Metadata handler, auto-create, topic IDs
    06-partition-data-model.md  partData, storedBatch, parseBatchHeader, offset assignment
    07-create-topics.md         CreateTopics handler
    08-produce.md               Produce handler, acks, LogAppendTime
    09-fetch.md                 Fetch handler, long-polling, size limits, KIP-74
    10-list-offsets.md          ListOffsets handler

    # Phase 2 work units
    11-find-coordinator.md      FindCoordinator handler
    12-group-coordinator.md     Group state machine, JoinGroup, SyncGroup, Heartbeat, LeaveGroup
    13-offset-management.md     OffsetCommit, OffsetFetch

    # Phase 3 work units
    14-admin-apis.md            DeleteTopics, CreatePartitions, DescribeConfigs, etc.
    15-wal-format-and-writer.md WAL + ring buffer, fsync batching, memory budget, partData evolution, read cascade
    16-metadata-log.md          metadata.log format, entry types, compaction
    17-crash-recovery.md        Startup replay, authority rules, RPO

    # Phase 4 work units
    18-s3-storage.md            S3 object format, flush pipeline, read path, disaster recovery
    19-transactions.md          Idempotency, transactional produce, EndTxn

docs/              Symlink → docs-site/src/content/docs/ (for discoverability)
docs-site/         Astro/Starlight docs build project (see docs-site/AGENTS.md)
  src/content/docs/  Documentation content (.md and .mdx files live here)

bin/               Build output directory (gitignored)

.cache/repos/      Cached reference repos (read-only)
  franz-go/          pkg/kmsg (codec), pkg/kfake (reference broker), pkg/kbin (wire primitives)
  automq/            s3stream/ (S3 storage), tests/ (ducktape)
  kafka/             storage/ (log segments), clients/ (record format), tests/ (ducktape)
```

## Build Conventions

All compiled binaries must be output to the `bin/` directory. Use `-o bin/` when running `go build`:

```sh
go build -o bin/klite ./cmd/klite
```

Never place binaries in the project root or any other directory.

## Where to Look

- **Implementation plans**: Read the relevant `plan/` file before starting work on a feature.
- **Research context**: Read `research/` files for background on design tradeoffs.
- **Testing strategy**: Read `plan/01-initial/00b-testing.md` for test infrastructure, tiers, and kfake adaptation guidance.
- **Protocol behavior reference**: `.cache/repos/franz-go/pkg/kfake/` -- handler files like `00_produce.go`, `01_fetch.go`, etc.
- **Codec usage**: `.cache/repos/franz-go/pkg/kmsg/`

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
- **For S3 (Phase 4)**, use LocalStack with testcontainers. No real AWS credentials should ever be needed to run tests.
- **No E2E / Kubernetes tests** — everything in this project is testable with unit tests or in-process integration tests.
- **Do not test third-party library behavior** — e.g., do not write tests that verify franz-go's client encoding. That's their responsibility, not ours.
- When adding a new feature, ask: "What is the cheapest test that can verify this?" — prefer unit over integration.

## Code Comments

- Do not write low-value comments. Comments should be reserved for high-value information that is not easily understood by reading the code alone.
- Keep comments concise and to the point.

## Subagents

- Use subagents sparingly. They are useful when a search would return verbose output that would pollute the main context just to find a small piece of detailed information.
- Do not use the `explore` subagent when gaining a general understanding of the codebase. Read files and grep directly instead.
