# Tackle

You are implementing a change that has a plan. The plan is in the issue comments.
The plan describes the approach and rationale, not a line-by-line diff — you
decide the code-level details. Aim for the most elegant implementation that
fulfills the plan's intent.

## Workflow

1. `br update <issue-id> --status in_progress`
2. `br show <issue-id>` and `br comments <issue-id>` — find the plan
3. Read the relevant source files. Understand the full picture — the plan
   tells you the approach, but you need to understand the code to implement
   it well. Read broadly, not just the files the plan mentions.
4. Confirm the project builds: `go build ./...` and `go vet ./...`
5. Implement the change. Follow the plan's intent but use your judgment on
   code-level decisions. If the cleanest implementation touches more files
   than the plan anticipated, that's fine.
6. Verify:
   - `go build ./...`
   - `go vet ./...`
   - `go test ./... -count=1`
   - **If the change touches replication, leader election, WAL, S3 flushing,
     failover paths, or anything that could affect behavior of a streaming
     replica setup**, also run the e2e tests:
     `go test -tags e2e -run TestK3sHelmReplicationFailover -timeout 10m -v ./test/e2e/`
     These deploy klite into k3s with a standby replica and exercise failover
     cycles. They require Docker and take several minutes.
   - Any additional verification from the plan
7. If all green:
   - Commit: `improve(<scope>): <description>`
     Use a scope that fits: `lint`, `errors`, `refactor`, `tests`, `docs`,
     `naming`, `structure`, or whatever best describes the change.
   - `git rev-parse HEAD` to get the hash
   - Create review issue:
     ```
     br create "review: <description>" \
       -d "Review commit <hash> from <issue-id>." \
       -p 0 -l review \
       --deps discovered-from:<issue-id>
     ```
   - `br close <issue-id> --reason "Implemented in <hash>"`
8. If verification fails:
   - Fix it (up to 3 attempts)
   - If unfixable: revert and defer (see "Getting Stuck" in `ralph/03-improve.md`)

## Commits

One atomic commit per issue. The review agent expects a single hash to
evaluate, and revert must be clean. If a change has logical stages, implement
them all and commit once.

Format: `improve(<scope>): <description>`

Examples:
- `improve(errors): wrap sentinel errors in metadata handler`
- `improve(refactor): extract partition validation into helper`
- `improve(tests): add edge case tests for offset overflow`
- `improve(structure): move TLS helpers to dedicated package`
- `improve(naming): rename confusing variables in replication loop`
- `improve(lint): fix unused parameter warnings in handlers`

## Discovering Related Issues

If you notice additional problems during implementation:

```
br create "title" -d "description" -p 2 -l needs-plan --deps discovered-from:<issue-id>
```

Do NOT fix them now. File and move on.

## Do NOT

- Fix unrelated issues you happen to notice (file them instead)
- Skip verification
- Leave uncommitted changes
- Add new user-facing functionality
- Gold-plate — implement what the plan asks for, elegantly, then stop
