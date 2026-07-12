# Write-Actor Attribution Policy (internal)

This doc records the source-of-truth rule for `_resolve_write_actor` after the internal charter inversion (implementation note resolver precedence collapse). It is the audit gate referenced by the contract test in `tests/test_write_actor_attribution_contract.py`.

## Rule (implementation note onward)

`_resolve_write_actor` follows a single uniform precedence:

```
explicit actor > caller cwd HEAD (when valid) > task_git probe > active row > raw cwd git
```

Where "valid" means the caller's `git rev-parse --abbrev-ref HEAD` returns a real branch (not `unknown-branch` and not the empty/error case).

The caller's cwd HEAD is the ground truth for write provenance. Callers that need different provenance — GC sweeps writing on behalf of an archived task, lifecycle close-sequence writes that intentionally pin to the archived task's branch, lane workers attributing to a sibling lane — pass an explicit `WriteActor`. The explicit channel is the only opt-out.

This replaces the internal invariant ("row's stored target_branch wins over caller cwd") and the implementation note cwd-membership probe (`_cwd_inside_task_worktree`). Both have been removed. The new rule is uniform across every caller; there is no per-caller migration table because no caller has to opt into the new behavior.

## Agent dimension (internal)

The precedence above governs the **branch/commit** dimensions, where the caller's cwd HEAD is a verifiable git probe. The **agent** dimension has no git probe — git does not report which model is at the cwd — so the only caller-side agent signal is the `AGENT_HANDOFF_DEFAULT_AGENT` environment variable a harness exports to declare the live agent driving its MCP writes. That self-declaration is the agent-dimension analogue of "caller cwd HEAD" and is ranked accordingly:

```
explicit actor (agent/identity) > self-declared identity (AGENT_HANDOFF_DEFAULT_AGENT) > inherited last-writer (active row updated_by) > hardcoded codex fallback
```

Before internal the env signal and the hardcoded `codex` were fused into one rung ranked *below* the inherited last-writer, so in an adversarial review→implement handoff a reviewer's actor-less writes were credited to whichever agent last touched the row. Promoting the self-declaration above the inherited last-writer mirrors the branch/commit rule (caller-side truth beats the stored row) while leaving env-unset continuity unchanged: with no self-declaration, agent resolution still falls through to the row's `updated_by`, then to `codex`.

This is an ordering change only — no new env surface, no new actor fields. The env path sets the `agent` slug alone; `model`/`model_label`/`reasoning_level` remain explicit-actor-only so the canonical-label validation is untouched.

## Branch/commit env hints (implementation note)

`_detect_git_write_context()` resolves branch and commit from `WORKBAY_HANDOFF_DEFAULT_BRANCH` / `WORKBAY_HANDOFF_DEFAULT_COMMIT_SHA` (via `resolve_env_alias`) and the `GITHUB_*` / `CI_*` fallbacks. When **both** values are already non-empty from those hints, the helper returns immediately and does **not** spawn `git rev-parse` subprocesses. Live git is consulted only when at least one dimension is still missing. This makes CI/test environments authoritative when they declare both branch and commit, and removes cumulative subprocess latency from write-path hot paths that set both hints. Note the short-circuit is all-or-nothing: when only one dimension is supplied via env, the helper still runs both git rev-parse calls, and successful live-git probes can override env-only hints — a successful branch probe overwrites an env-only branch, and a successful `git rev-parse HEAD` overwrites an env-only commit.

## Why the precedence collapse is durable

The earlier 19-row caller-classification table grew because each caller had to be hand-graded as either:

- (a) "stored row is correct" — no migration, OR
- (b) "must pass explicit actor" — migrate before implementation note lands.

That contract was fragile in two directions:

1. New callers had to be added to the table or the reverse-direction tripwire would fail. Every new write path paid a doc tax.
2. Category-(b) callers that quietly missed migration produced wrong-cwd attribution silently — exactly the internal charter gap surfaced by finding `internal`.

Collapsing the resolver to "caller cwd always wins when valid; explicit actor is the opt-out" makes new callers safe by default. Callers that want the old "row wins" semantics now have to ask for them via an explicit `WriteActor`, which is the right shape: opting *into* row provenance is a deliberate design choice (archive sweeps, lifecycle close), not a default.

## Explicit-actor opt-out: when to use it

Pass an explicit `WriteActor` when the caller's cwd HEAD is **not** the right provenance for the write:

- **`archive_task_state`**: archive sweeps record the close against the archived task's branch via `archive_branch`/`archive_commit_sha`. The CLI exposes both flags; pass them when archiving a task whose worktree is gone.
- **`tasks_gc`**: GC sweeps writing maintenance archive rows pass a synthetic actor (e.g., `agent="tasks_gc"`) when the cleanup write should not be attributed to whichever shell happened to invoke the sweep.
- **Cross-task writes from inside another task's worktree**: a write that is logically owned by task B but executed from inside task A's worktree passes an actor block with task B's branch + commit_sha.

In every other case — including the previously-listed category-(b) callers (`_set_handoff_state_with_conn`, `update_task_status`, `switch_task`, lane worker writes) — the caller's cwd HEAD is the correct provenance and no actor block is required.

## `target_worktree_path` derivation (implementation note)

After internal, `handoff_state.target_worktree_path` is **no longer the source of truth** for the worktree path. The resolver derives it via `git worktree list --porcelain` keyed by canonical `target_branch`. The stored column remains in the schema for migration headroom and is dropped in a follow-up MAINT once one release of derive-only behavior has shipped.

- **Helper choice**: `_canonical_worktree_for_task(target_branch, *, workspace_root=None)` in `shared_write_context.py` is the single derivation entrypoint. `_detect_git_write_context_at(worktree_path)` is **repurposed cleanly** — it remains a path-keyed git probe, but its caller in `_resolve_write_actor` now passes the *derived* path instead of the row's stored value. We chose repurpose over replace because the path-keyed probe has no opinion on where the path comes from; making derivation the caller's job keeps probe semantics narrow and keeps `_canonical_worktree_for_task` a pure derivation helper that is easy to mock per-test.
- **Loud failure**: when `target_branch` is set but `git worktree list --porcelain` produces no match, `_canonical_worktree_for_task` raises `WorktreeNotFoundError`. The write-side resolver propagates the raise so operators see the missing worktree at write time; the read-side `collect_target_context_warnings` converts the same condition into a `context_drift` warning so observability paths do not crash on a stale row.
- **Test bypass**: `AGENT_HANDOFF_SKIP_WORKTREE_DERIVATION=1` (set by default in the handoff package's `conftest.py`) routes `_resolve_write_actor` and `collect_target_context_warnings` back to reading the stored column. Production never sets it; dedicated derivation tests in `test_derive_worktree_path.py` deliberately do not set it and exercise real `git worktree list --porcelain` against `tmp_path` repos.

## Contract enforcement

`tests/test_write_actor_attribution_contract.py` retains a tripwire that asserts the resolver implementation does not regrow a per-caller policy table. The forward + reverse caller-inventory enforcement from implementation note is intentionally retired — the new rule does not require per-caller classification, so the contract test no longer tracks `(file:line)` rows.
