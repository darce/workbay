# Subfeature Committer

## Overview

Use this skill when the branch has exactly one coherent finished slice ready to commit.

## Trigger

Use this skill when:

- one completed change set is already isolated in the working tree
- the user wants one intentional commit, not a multi-commit grouping pass
- `commit2git` would be unnecessarily broad for the current state

If the branch contains multiple completed slices or mixed unrelated hunks, switch to [../commit2git/SKILL.md](../commit2git/SKILL.md).

## Goal

Create one clean commit for the current completed slice while leaving the rest of the working tree intentionally untouched.

## Canonical Policy

- Use [../../instructions.md](../../instructions.md) for startup, handoff, and evidence-logging policy.
- Use [../../rules/development-workflow.md](../../rules/development-workflow.md) for slice, contract, and review-readiness rules.
- Use this skill for the commit recipe only; do not copy broader process policy into commit notes or ad hoc local guidance.

## Core Process

1. Inspect the current diff shape:
   - `git status --short`
   - `git diff --name-only`
   - targeted `git diff -- <path>`
2. Confirm the slice boundary:
   - one outcome
   - matching tests/docs if they belong to the same slice
   - no unrelated hunks swept in for convenience
3. Stage only the finished slice:
   - full-file staging when safe
   - hunk staging when the file contains unrelated work
4. Re-read the staged diff:
   - `git diff --cached`
5. Commit with a precise subject naming the completed outcome.
6. Re-check the remaining working tree so any leftover changes are clearly intentional.

## Safety Constraints

- Do not use this skill when multiple finished slices still need grouping.
- Do not commit half-finished or ambiguous hunks just to make the tree smaller.
- Stay in the current worktree and branch context; do not switch to a sibling worktree to perform the commit.

## Common Rationalizations

- "This is close enough to one slice." If the diff tells two stories, it is not ready for a single isolated commit.
- "I will commit the extra hunks now and sort them out later." Cleanup commits are not a substitute for deliberate slice boundaries.
- "Switching worktrees will make staging easier." This skill stays in the current branch and worktree on purpose.

## Red Flags

- The staged diff contains unrelated paths or outcomes.
- The working tree still has multiple coherent slices that need grouping.
- The commit message is describing a directory or file batch instead of an outcome.

## Recovery

- If the slice boundary turns out to be mixed with unrelated work, unstage it and switch to `commit2git` or hunk-splitting.
- If a worker lane requires lane-specific commit helpers or prefixes, use those in the same worktree rather than escaping the lane context.
- If the staged diff no longer tells one coherent story, reset the staging area for that slice and regroup.

## Convergence Criteria

- Exactly one coherent finished slice is committed.
- The commit message names the completed outcome clearly.
- Any remaining changes in the worktree are intentionally left for a later slice.

## See Also

- [../commit2git/SKILL.md](../commit2git/SKILL.md)
- [../../rules/development-workflow.md](../../rules/development-workflow.md)
