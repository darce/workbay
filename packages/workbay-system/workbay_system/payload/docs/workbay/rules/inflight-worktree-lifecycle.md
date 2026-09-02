# Inflight worktree and branch lifecycle

Worktrees and branches accumulate because nothing retires them. Creation is
cheap and automatic (every lane dispatch can make one); teardown is manual and
easy to skip, so the inventory only grows. On 2026-08-05 this repository held
**84 local branches across 17 worktrees**, most of them finished work nobody had
reason to revisit.

This note fixes the retention rule. It is deliberately narrow: it says which
branches may be deleted automatically, which need judgment, and where the
authority for "still inflight" actually lives.

## The authority is the lane row, not git

A branch is inflight when its lane row in `handoff.db` is open — not when git
still has the ref. Git cannot tell you whether work is finished; it only tells
you what commits exist. So:

- **Lane close marks the work finished; it is not the teardown.**
  `manage_worktree_lane(operation="close", ...)` closes the lane row so the
  branch is no longer "inflight." That is the authority handoff: once the row
  is closed, retention is a cleanup question rather than an active-work
  question. Skipping close is what leaves finished work looking inflight and
  produced the backlog; the offload skill already requires it after the review
  gate merges.
- **`make task-finish` is the teardown path.** After merge (and after the
  branch-lifecycle close sequence has set status done, closed lanes when they
  exist, and archived the task), `make task-finish TASK=<task-ref>` removes the
  worktree, regenerates the dashboard, and deletes the feature branch with
  `git branch -d` in the repo's canonical order. Do not invent a competing
  retirement event: close records "done"; `task-finish` performs teardown.
  Sweeping weeks later has to re-derive intent that was obvious at finish time.

## Why `git branch --merged` is the wrong check

Lane work rarely lands as a fast-forward. It lands squashed, or through an
integration branch, or as a re-applied patch after a rebase — all of which
change the commit identity, so the original branch never registers as merged.

Measured on this repository the same day: of 83 non-main local branches, exactly
**3** were ancestors of `main`. The other 80 all carried commits absent from
main's history, yet nearly all of them were finished work whose content had
already landed. `--merged` would have retained everything and reclaimed nothing.

`git cherry main <branch>` (patch-id equivalence) is better but still misses
squashes: it compares individual commit patches, and a squash has no matching
patch-id on either side.

## The retention rule

Three tiers, in order. Only the first is automatic.

**Tier 1 — delete without asking.** The branch is a strict ancestor of `main`
(`git merge-base --is-ancestor <branch> main`) *and* its worktree is clean. It
contains no commit that main does not already have, so deletion cannot lose
information. This is the only case safe to automate.

**Tier 2 — delete after a content check.** Every file the branch touched
(`git diff --name-only main...<branch>`) is byte-identical in main
(`git diff --quiet <branch> main -- <those files>`). This catches squash-merged
lanes that Tier 1 and `--merged` both miss. It has a known false negative: if
main later changed one of those files for an unrelated reason, the check reports
"differs" and the branch is retained. Retention on doubt is the correct
direction for the error.

**Tier 3 — never automatic.** Anything with an uncommitted worktree. Check
before every teardown, not once at the start of a sweep:

    git -C <worktree> status --porcelain

A non-empty result means someone's unpushed work is sitting there. On
2026-08-05 the `s2a` worktree held a modified `import_export.py` plus an
untracked test file that existed nowhere else. Deleting that worktree would have
destroyed the only copy.

## Before any sweep

1. **Write a restore script first.** List each branch with its full SHA and
   `git branch <name> <sha>` to recreate it. Deleted refs stay recoverable until
   `git gc`, so a restore script makes the whole sweep reversible in practice.
2. **Exclude other sessions' worktrees — by process, not by mtime.** Concurrent
   agent sessions hold live worktrees that look abandoned from outside.
   **File mtime does not detect them.** A peer running a test suite, a build, or
   any other read-only pass writes nothing, so a `find -newermt '-6 hours'` scan
   reports the worktree as quiet while an agent is actively working inside it —
   and the error points at "safe to delete". Observed: such a scan reported a
   worktree quiet while a peer session's `pytest` was running in it.
   The load-bearing check is the **process list**, matched on the worktree path:

   ```sh
   ps -eo pid,command | grep -F "<worktree-path>" | grep -v grep
   ```

   A hit means somebody is in there. To separate a peer's process from your own,
   compare the shell-snapshot id in the command line — each session gets its own
   `snapshot-zsh-<id>-<slug>.sh`, so a snapshot id that is not yours is a peer.
   Treat mtime as corroboration only, never as the sole clearance.
3. **Exclude anything with a pass in flight.** A lane mid-dispatch has a clean
   tree and an unremarkable branch, and looks exactly like a finished one. Raw
   `scripts/remote_agent.sh` dispatches carry no lane row at all, so the lane
   table will not reveal them either — the same `ps` sweep is what catches them,
   matched on the lane branch name.

## Worktree remove vs branch delete (and why Tier 3 exists)

These are two separate acts, and they are **not** equally safe.

- **Committed content** stays reachable through the branch ref after
  `git worktree remove`. Recovery is `git worktree add <path> <branch>` and the
  tree comes back for everything that was committed. Leaving refs alone while
  reclaiming clean checkouts is how a sweep can free disk without discarding
  landed work.
- **Uncommitted content dies with the worktree.** Tier 3 exists for that case:
  a dirty worktree may hold the only copy of in-progress edits (observed:
  `s2a` held a modified `import_export.py` plus an untracked test that existed
  nowhere else). Removing that worktree is irreversible for the dirty tree, so
  it is never automatic — check `git status --porcelain` before every teardown.

When a sweep is the right call but the branch-retention question is still open,
**remove only clean worktrees and keep the branches.** That reclaims working
trees that are safe to drop while leaving every branch-deletion decision
reversible. Never apply "remove the worktree, keep the branch" to a dirty
checkout. Prune the refs later, as a separate deliberate pass, once the branch
is an ancestor of `main`.

Corollary for a session that finds its own worktree gone: check the branch ref
before assuming committed work is lost, and re-add rather than re-running it.
If the tree was dirty, uncommitted content is already gone.

## Scripting notes

The host runs **bash 3.2.57** (macOS system bash). `mapfile`/`readarray` do not
exist there. A sweep that uses `mapfile` gets an empty array on every iteration
and reports *every* branch as safe to delete — a silent, uniformly wrong,
entirely plausible-looking result. Prefer `while IFS= read -r` over a temp file.

The same class of failure applies to `zsh`, which does not word-split unquoted
expansions: `for b in $BRANCHES` iterates **once** with the whole string as a
single item, so the loop silently does nothing.

Both mistakes fail toward "everything is fine". Give any sweep a control: name a
branch you know carries unmerged work and confirm the sweep reports it as
retained. If the control comes back deletable, the sweep is broken.
