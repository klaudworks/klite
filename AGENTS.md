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

<!-- BEGIN BEADS INTEGRATION -->
## Issue Tracking with bd (beads)

**IMPORTANT**: This project uses **bd (beads)** for ALL issue tracking. Do NOT use markdown TODOs, task lists, or other tracking methods.

### Why bd?

- Dependency-aware: Track blockers and relationships between issues
- Git-friendly: Dolt-powered version control with native sync
- Agent-optimized: JSON output, ready work detection, discovered-from links
- Prevents duplicate tracking systems and confusion

### Quick Start

**Check for ready work:**

```bash
bd ready --json
```

**Create new issues:**

```bash
bd create "Issue title" --description="Detailed context" -t bug|feature|task -p 0-4 --json
bd create "Issue title" --description="What this issue is about" -p 1 --deps discovered-from:bd-123 --json
```

**Claim and update:**

```bash
bd update <id> --claim --json
bd update bd-42 --priority 1 --json
```

**Complete work:**

```bash
bd close bd-42 --reason "Completed" --json
```

### Issue Types

- `bug` - Something broken
- `feature` - New functionality
- `task` - Work item (tests, docs, refactoring)
- `epic` - Large feature with subtasks
- `chore` - Maintenance (dependencies, tooling)

### Priorities

- `0` - Critical (security, data loss, broken builds)
- `1` - High (major features, important bugs)
- `2` - Medium (default, nice-to-have)
- `3` - Low (polish, optimization)
- `4` - Backlog (future ideas)

### Workflow for AI Agents

1. **Check ready work**: `bd ready` shows unblocked issues
2. **Claim your task atomically**: `bd update <id> --claim`
3. **Work on it**: Implement, test, document
4. **Discover new work?** Create linked issue:
   - `bd create "Found bug" --description="Details about what was found" -p 1 --deps discovered-from:<parent-id>`
5. **Complete**: `bd close <id> --reason "Done"`

### Auto-Sync

bd automatically syncs via Dolt:

- Each write auto-commits to Dolt history
- Use `bd dolt push`/`bd dolt pull` for remote sync
- No manual export/import needed!

### Important Rules

- ✅ Use bd for ALL task tracking
- ✅ Always use `--json` flag for programmatic use
- ✅ Link discovered work with `discovered-from` dependencies
- ✅ Check `bd ready` before asking "what should I work on?"
- ❌ Do NOT create markdown TODO lists
- ❌ Do NOT use external issue trackers
- ❌ Do NOT duplicate tracking systems

For more details, see README.md and docs/QUICKSTART.md.

## Landing the Plane (Session Completion)

**When ending a work session**, you MUST complete ALL steps below. Work is NOT complete until `git push` succeeds.

**MANDATORY WORKFLOW:**

1. **File issues for remaining work** - Create issues for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
3. **Update issue status** - Close finished work, update in-progress items
4. **PUSH TO REMOTE** - This is MANDATORY:
   ```bash
   git pull --rebase
   bd sync
   git push
   git status  # MUST show "up to date with origin"
   ```
5. **Clean up** - Clear stashes, prune remote branches
6. **Verify** - All changes committed AND pushed
7. **Hand off** - Provide context for next session

**CRITICAL RULES:**
- Work is NOT complete until `git push` succeeds
- NEVER stop before pushing - that leaves work stranded locally
- NEVER say "ready to push when you are" - YOU must push
- If push fails, resolve and retry until it succeeds

<!-- END BEADS INTEGRATION -->
