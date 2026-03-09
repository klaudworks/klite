---
name: br
description: >-
  Official skill for beads_rust (`br`), a local-first, dependency-aware issue
  tracker for AI agents. Use when creating issues, triaging backlogs, managing
  dependencies, finding ready work, updating status, or syncing to git via JSONL.
license: MIT
domain: project-management
role: specialist
scope: operations
output-format: commands
triggers:
  - br
  - beads
  - beads_rust
  - issue tracker
  - issue triage
  - backlog
  - dependencies
  - ready work
metadata:
  author: Dicklesworthstone
  version: 1.0.0
---

# br -- Beads Rust Issue Tracker

> **Non-invasive:** br NEVER runs git commands. Sync and commit are YOUR responsibility.

## Critical Rules

| Rule                         | Why                                                                   |
| ---------------------------- | --------------------------------------------------------------------- |
| **Binary is `br`**           | NEVER `bd` (that is the old Go version)                               |
| **ALWAYS use `--format toon`** | Token-optimized output; saves context window vs `--json`            |
| **NEVER run bare `bv`**      | Blocks session in interactive TUI mode                                |
| **Sync is EXPLICIT**         | `br sync --flush-only` exports DB to JSONL only                       |
| **Git is YOUR job**          | br only touches `.beads/` -- you must `git add .beads/ && git commit` |
| **No cycles allowed**        | `br dep cycles` must return empty                                     |

Actor: `ACTOR="${BR_ACTOR:-assistant}"` -- resolve once, pass `--actor "$ACTOR"` on create/update/close/comment.

## Quick Workflow

```bash
ACTOR="${BR_ACTOR:-assistant}"

br ready --format toon                                    # 1. Find work
br update --actor "$ACTOR" <id> --status in_progress      # 2. Claim it
# 3. Do work...
br close --actor "$ACTOR" <id> --reason "Implemented X"   # 4. Complete
br sync --flush-only                                      # 5. Sync
git add .beads/ && git commit -m "feat: X (<id>)"
```

## Essential Commands

### Issue Lifecycle

```bash
br init                                              # Initialize .beads/ workspace
br create --actor "$ACTOR" "Title" -p 1 -t task      # Create issue (priority 0-4)
br q --actor "$ACTOR" "Quick note"                   # Quick capture (ID only output)
br show <id> --format toon                           # Show issue details
br update --actor "$ACTOR" <id> --status in_progress # Update status
br update --actor "$ACTOR" <id> --priority 0         # Change priority
br close --actor "$ACTOR" <id> --reason "Done"       # Close with reason
br close --actor "$ACTOR" <id1> <id2> --reason "..."  # Close multiple at once
br reopen --actor "$ACTOR" <id>                      # Reopen closed issue
```

### Create Options

```bash
br create --actor "$ACTOR" "Title" \
  --priority 1 \             # 0-4 scale (0=critical, 4=backlog)
  --type task \              # task, bug, feature, epic, question, docs
  --assignee "user@..." \    # Optional assignee
  --labels backend,auth \    # Comma-separated labels
  --description "..."        # Detailed description
```

### Update Options

```bash
br update --actor "$ACTOR" <id> \
  --title "New title" \
  --priority 0 \
  --status in_progress \     # open, in_progress, closed
  --assignee "new@..." \
  --add-label reliability \
  --parent <parent-id> \
  --claim                    # Shorthand for claim-and-start
```

Bulk update: `br update --actor "$ACTOR" <id1> <id2> <id3> --priority 2 --add-label triage-reviewed`

### Querying (always use --format toon)

```bash
br ready --format toon                      # Actionable work (no blockers)
br list --format toon                       # All issues
br list --status open --sort priority --format toon  # Filter and sort
br list --priority 0-1 --format toon        # Filter by priority range
br list --assignee alice --format toon      # Filter by assignee
br blocked --format toon                    # Show blocked issues
br search "keyword" --format toon           # Full-text search
br stale --days 30 --format toon            # Stale issues
br count --by status --format toon          # Count with grouping
```

### Dependencies

```bash
br dep add <child> <parent>          # child depends on parent
br dep add <id> <dep> --type blocks  # Explicit block type
br dep remove <child> <parent>       # Remove dependency
br dep list <id> --format toon       # List dependencies for issue
br dep tree <id> --format toon       # Show dependency tree
br dep cycles --format toon          # Find circular deps (MUST be empty!)
```

### Labels

```bash
br label add <id> backend auth       # Add multiple labels
br label remove <id> urgent          # Remove label
br label list <id>                   # List issue's labels
br label list-all                    # All labels in project
```

### Comments

```bash
br comments add --actor "$ACTOR" <id> --message "Triage note"
br comments list <id> --format toon
```

### Sync (EXPLICIT -- never automatic)

```bash
br sync --flush-only                 # Export DB to JSONL (before git commit)
br sync --import-only                # Import JSONL to DB (after git pull)
br sync --status                     # Check sync status
```

### Diagnostics

```bash
br doctor                            # Full diagnostics
br stats --format toon               # Project statistics
br lint --format toon                # Lint issues for problems
br where                             # Show workspace location
```

## Priority Scale

`0`=critical `1`=high `2`=medium(default) `3`=low `4`=backlog

## Issue Types

`task`, `bug`, `feature`, `epic`, `question`, `docs`

## bv Integration

**CRITICAL:** Never run bare `bv` -- it launches interactive TUI and blocks.

```bash
bv --robot-next                      # Single top pick + claim command
bv --robot-triage                    # Full triage with recommendations
bv --robot-plan                      # Parallel execution tracks
bv --robot-insights | jq '.Cycles'   # Check graph health (must be empty)
bv --robot-priority                  # Priority misalignment detection
bv --robot-alerts                    # Stale issues, blocking cascades
```

## Session Ending Pattern

```bash
git pull --rebase
br sync --flush-only
git add .beads/ && git commit -m "Update issues"
git push
```

## Triage Decision Matrix

| Classification        | Action                                         |
| --------------------- | ---------------------------------------------- |
| `implemented`         | Close with evidence (commit/PR/file/behavior)  |
| `out-of-scope`        | Close with explicit boundary reason            |
| `needs-clarification` | Comment with specific unanswered questions     |
| `actionable`          | Keep open, correct status/priority/labels/deps |

## Anti-Patterns

- Running `br sync` without `--flush-only` or `--import-only`
- Forgetting sync before git commit
- Creating circular dependencies
- Running bare `bv` (blocks session)
- Assuming auto-commit behavior (br NEVER auto-commits)
- Inventing evidence for closure -- if unsure, comment instead
- Adding speculative dependencies

## Troubleshooting

If something is wrong, run `br doctor` for full diagnostics.
