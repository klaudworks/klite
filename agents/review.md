# Review

You are reviewing a change made by a previous iteration.

## Workflow

1. `br update <review-issue-id> --status in_progress`
2. `br show <review-issue-id> --format toon -q` — the description contains
   the **commit hash**. The `discovered-from` dependency points to the
   **original issue**.
3. `git show <commit-hash>` — read the full diff
4. If the diff isn't enough to understand the change (large refactors,
   structural moves), read the affected files in their final state. Start
   from the diff, then read whatever you need to fully understand the result.
5. `br show <original-issue-id> --format toon -q` — understand what was being improved
6. `br comments <original-issue-id> --format toon -q` — read the plan that was executed
7. Evaluate (klite is in active development with no external consumers —
   backwards compatibility is not a constraint, so renamed exports, changed
   package structure, and altered internal APIs are fine if they improve things):
   - **Correctness**: Does the change achieve what the plan intended?
   - **No regressions**: Could it break existing behavior?
   - **Code quality**: Clean, idiomatic Go? Is the result elegant and robust?
   - **Behavioral preservation**: For refactors, does the code still do
     exactly the same thing? Trace through the logic.
   - **Net improvement**: Is the codebase clearly better after this change?
     Large diffs are fine — judge the change by whether it improves klite,
     not by how many files it touches.
8. `go build ./...` and `go vet ./...`
9. `go test ./... -count=1` — **all tests must pass**, not just those
   related to the change. If an unrelated test fails, the change is not
   approved until it is fixed (either by the reviewer as a fixup, or by
   reverting).
10. **If good**: `br close <review-issue-id> --reason "Approved"`
11. **If minor issues** (typo, missed error wrap, off-by-one — things that
    don't change the approach):
    - Fix them directly
    - Commit: `improve(<scope>): fixup <description>`
    - `br close <review-issue-id> --reason "Approved with fixup <new-hash>"`
12. **If fundamentally wrong** (wrong approach, regression, broken logic):
    - `git revert --no-edit <commit-hash>`
    - `br reopen <original-issue-id>`
    - `br comments add <original-issue-id> "Reverted: <what was wrong>"`
    - `br close <review-issue-id> --reason "Reverted: <summary>"`

## What to Look For

- **Semantic changes**: Refactors that subtly change behavior (different error
  types, reordered operations, changed nil handling)
- **Concurrency**: Mutex scope changes, channel behavior changes
- **Test validity**: New tests that don't actually test what they claim
- **Unrelated changes**: Changes to code that have nothing to do with the
  issue's intent (but touching many files for a consistent refactor is fine)

## Discovering Issues

If the review reveals problems beyond "revert or approve" — e.g., the code
was correct but exposed a deeper structural issue — file a followup issue
at **P1** so it gets planned in the next iteration (depth-first chaining):

```
br create "title" -d "description" -p 1 -l needs-plan --deps discovered-from:<review-issue-id>
```

## Do NOT

- Make improvements — review only
- Refactor code touched by the commit
- Add unrelated tests
- Approve changes that fail build or tests — even if the failure looks
  unrelated, the suite must be green before approval
- Skip, disable, or weaken tests to make the suite pass
