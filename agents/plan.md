# Plan

You are investigating an issue and designing a plan to fix it. The issue may
describe anything from a lint warning to a large structural refactor.

The goal is an elegant, robust klite. Every change should move the codebase
toward that. Don't be shy about ambitious refactors if they clearly improve
the overall structure — consolidating scattered logic, introducing proper
abstractions, eliminating entire categories of bugs. A bold refactor that
makes the code genuinely better is always preferred over a timid patch that
leaves the mess in place.

klite is in active development with no external consumers yet. Backwards
compatibility is not a constraint — renaming exported symbols, changing
package structure, altering internal APIs are all fair game if they make
the code better.

## Workflow

1. `br update <issue-id> --status in_progress`
2. `br show <issue-id> --format toon -q` — read the issue description
3. Read the relevant source files. Understand the current state thoroughly.
   For refactors, trace callers and callees. For error handling, trace the
   full error path. For structural changes, map out the current organization.
4. **Validate the problem.** The issue description may be wrong or outdated.
   Ask yourself: is this actually a problem? Read the code, don't assume.
   If it's not a real problem:
   - `br close <issue-id> --reason "Not an issue: <explanation>"`
   - Stop here.
5. **Validate the value.** Even if the problem is real, is fixing it clearly
   worth it? Does it reduce confusion, prevent bugs, improve maintainability,
   or make the code more robust and elegant? If the benefit is marginal or
   the "improvement" is subjective with no clear upside:
   - `br close <issue-id> --reason "Low value: <explanation>"`
   - Stop here.
   But err on the side of action. If a refactor would meaningfully improve
   the structure, that's high value — even if nothing is "broken" today.
6. **Check scope.** If the issue requires new user-facing functionality
   (new flags, new endpoints, new protocol support), it's out of scope:
   - `br close <issue-id> --reason "Out of scope: requires new functionality"`
   - Stop here.
7. **Design a clean solution.** Focus on the approach, not the line-by-line
   diff. Find the most elegant way to solve the problem. Consider:
   - What is the right abstraction?
   - What pattern does the rest of the codebase use for similar things?
   - Is there a way to solve this that makes the code simpler, not just different?
   - Could a larger change (touching many files) produce a cleaner result
     than a narrow patch?
   - Would a deeper refactor address the root cause rather than a symptom?
   Think like an architect, not a patch author. The best solution might be
   restructuring a package, introducing an interface, or consolidating
   duplicated logic — not just fixing the thing the issue literally says.
8. **Fast-track check.** If the issue is small and clear-cut — the fix is
   obvious, mechanical, and touches only a few files (e.g., a lint fix, a
   straightforward rename, a missing error wrap) — skip the plan and
   implement it directly in this iteration. Follow the workflow in
   `agents/tackle.md` from step 3 onward. No need to write a plan comment
   or promote labels — just implement, verify, commit, and create the
   review issue.
9. Otherwise, write the plan as a comment:
   ```
   br comments add <issue-id> "Plan: <plan>"
   ```
10. Promote to ready-for-implementation (status goes back to open so
    `br ready` picks it up, priority P1 so it's next):
    ```
    br update <issue-id> --status open -p 1
    br label remove <issue-id> needs-plan
    br label add <issue-id> has-plan
    ```

## What a Good Plan Contains

- **Why this matters**: A sentence on the concrete benefit — fewer bugs,
  less confusion, better consistency, etc.
- **The approach**: How to solve it. What pattern, abstraction, or structural
  change to use. This is the core of the plan.
- **Scope**: Which areas of the codebase are affected. Touching every file is
  fine if the change is mechanical and consistent.
- **Risks**: What could go wrong. Behavioral changes, concurrency concerns,
  callers that need careful attention.
- **Verification**: How to confirm correctness beyond build/test — e.g.,
  "grep for old pattern to confirm none remain."

A good plan does NOT contain line-by-line diffs or exact code snippets. The
implementing agent will figure out the code. The plan should communicate the
*what* and *why*, not the *exact how*.

## Splitting Large Issues

If an issue is genuinely too large for a single iteration (multiple independent
concerns bundled together), split it into sub-issues:

```
br create "sub-task" -d "description" -p 2 -l needs-plan --deps discovered-from:<issue-id>
```

But do NOT split just because many files are touched. A consistent rename
across 50 files is one atomic change, not 50 tasks.

## Discovering Related Issues

If investigation reveals additional problems:

```
br create "title" -d "description" -p 2 -l needs-plan --deps discovered-from:<issue-id>
```

File them. Do not plan for them in this iteration.
