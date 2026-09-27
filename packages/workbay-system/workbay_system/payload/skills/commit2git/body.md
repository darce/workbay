# Commit2Git

## Overview

Use this skill when the user wants a dirty branch turned into a small set of reviewable commits. It standardizes how to inspect, group, stage, and message commits without changing worktree ownership or sweeping ambiguous changes into a single commit.

## Trigger

Use this skill when the current branch contains multiple completed slices, refactors, or docs/tooling updates that need to be grouped into intentional commits.

Do not use this skill when:

- the tree already contains one clean isolated slice
- the user explicitly wants a checkpoint or WIP commit
- the file history is too interleaved to split safely without user input

If the working tree already contains one clean isolated slice, prefer [subfeature-committer](../subfeature-committer/SKILL.md).

## Goal

Create one commit per completed sub-feature, behavior slice, or reviewable refactor instead of one commit per directory, language, or "everything touched for this task."

## Canonical Policy

- Use [../../instructions.md](../../instructions.md) for startup, handoff, and evidence-logging policy.
- Use [../../rules/development-workflow.md](../../rules/development-workflow.md) for branch isolation, slice, and review-readiness rules.
- Use this skill for commit grouping and staging behavior only; broader task lifecycle policy lives in the linked docs.

## Core Process

1. Confirm the current checkout is the authoritative place to commit:

```bash
pwd
git rev-parse --show-toplevel
git branch --show-current
git rev-parse --git-dir
git rev-parse --git-common-dir
```

2. Inspect the shape of the current change set before staging anything:

```bash
git status --short
git diff --name-only
git diff --cached --name-only
git diff --stat
```

3. Inspect candidate groups with targeted diffs:

```bash
git diff -- path/to/file
git diff --cached -- path/to/file
```

4. Group by completed behavior, workflow slice, or reviewable refactor. Keep tests, fixtures, and docs with the code they verify when they describe the same slice.
5. Stage deliberately. Use full-file staging only when the entire file belongs to one slice. Otherwise use hunk staging:

```bash
git add path/to/file
git add -p
git diff --cached --stat
git diff --cached
```

6. Write a commit message around the completed outcome:

```text
feat(scope): add review dispatch routing
fix(scope): preserve finding provenance on updates
docs(agentic): document lane refresh recovery
refactor(scope): split report rendering from git inspection
test(scope): cover lane status edge cases
```

7. If the checkout is a linked worktree, prefix the subject with the worktree directory name. Detect that with:

```bash
git rev-parse --git-dir
git rev-parse --git-common-dir
basename "$(git rev-parse --show-toplevel)"
```

If `git-dir` and `git-common-dir` differ, use:

```text
<worktree-name>: <subject>
```

8. After each commit, re-run `git status --short` and repeat only for the next clearly completed slice.

## Common Rationalizations

- "I'll just commit everything together because it's all for the same task."
- "The diff is mostly related, so the extra hunks can ride along."
- "A vague commit message is fine because the PR will explain it."
- "I'll clean up the lane or worktree context later."

## Red Flags

- The staged diff tells more than one story.
- A single file contains interleaved hunks that cannot be separated safely.
- The current branch or worktree ownership is unclear.
- A lane helper would hide staging detail you need to inspect first.
- The commit would include half-finished work just to make the tree clean.

## Recovery

- If the staged diff tells more than one story, unstage it and split the slice before committing.
- If a file contains interleaved hunks that cannot be separated safely, stop and ask the user instead of guessing.
- If the lane or worktree prefix behavior is unclear, re-run the `git rev-parse --git-dir` and `git rev-parse --git-common-dir` checks before committing.
- If a repo helper such as `make lane-commit` would hide important staging detail for the current slice, fall back to manual staging in the same worktree instead of switching contexts.

## Convergence Criteria

- Each commit represents exactly one completed slice, sub-feature, or reviewable refactor.
- The staged diff for each commit tells one coherent story with matching tests or docs where applicable.
- The remaining working tree is intentionally left for follow-on slices rather than accidentally swept into the commit.
- The agent stays in the same worktree and branch context it started in.

## See Also

- [subfeature-committer](../subfeature-committer/SKILL.md)
- [development-workflow.md](../../rules/development-workflow.md)
