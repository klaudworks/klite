# Kafka-Light Broker

Implement tasks from `plan/01-initial/`.

## Prerequisites

Files matching `00*` are reference material. Read them every iteration.
Never mark them DONE.

## Workflow

1. Read prerequisite files (`00*`) in `plan/01-initial/`
2. List files in `plan/01-initial/`. Skip `DONE_*` and prerequisite files.
3. Take the first remaining file — this is your only task this iteration.
4. Read it. Read any files mentioned in "Read before starting"
   (these may have a `DONE_` prefix — look for both names).
5. Do NOT read other `DONE_*` files. Your context is: prerequisites +
   the current task + its "Read before starting" references. Nothing else.
6. Confirm `go build ./...` and `go vet ./...` pass.
7. Implement the entire task.
8. Run the verify command from the file's `## Verify` section.
9. Run regression: `go test ./... -count=1`
10. If both pass, rename with `DONE_` prefix.

## Research

When unsure — research first. Use `docs-explore` or `webfetch` freely.
Do not guess.

**Use `explore` and `repo-explorer` subagents sparingly.** Normal code
exploration — reading files, grepping for patterns, navigating `.cache/repos/`
— should be done directly with Read, Grep, and Glob. Only reach for these
subagents to extract narrow, specific information that would otherwise require
grepping through many irrelevant files and bloating your context. If you can
answer the question by reading 1–3 files, do it yourself.

## Dependencies

When adding a new Go package, look up the latest version (e.g. via `webfetch`
or `go list -m -versions`) and install it explicitly with
`go get package@latest`. Do not rely on stale versions from plan files.

## Plan Amendments

If you discover something that affects other task files — update them
BEFORE continuing. Add `> **NOTE:**` blockquotes, rewrite stale instructions.

## Failure

If stuck, add `> **STUCK:**` note to the task file. Do NOT rename.
- Retry might help → `<promise>REMAINING</promise>`
- Needs human → `<promise>COMPLETE</promise>`

## Completion

- ALL non-prerequisite files have `DONE_` → `<promise>COMPLETE</promise>`
- Otherwise → `<promise>REMAINING</promise>`

Then stop.
