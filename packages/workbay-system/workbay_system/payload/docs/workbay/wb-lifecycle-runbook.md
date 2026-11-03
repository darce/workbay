# `wb` lifecycle runbook (T24)

One-page mnemonic wrappers over married multi-step lifecycle segments.
Each verb is idempotent ([RES-01]), emits a JSON receipt, and uses
named-cause non-zero exits ([OBS-08]). Invocable from a human terminal
or as one LLM Bash call — mechanics stay mechanical.

## Invocation

```bash
# direct (payload path)
python packages/workbay-system/workbay_system/payload/scripts/workbay/wb <verb> [args]

# make facade
make wb VERB=<verb> [TASK=...] [TEST_CMD=...] [DOC=...] [WB_ARGS=...]
```

Receipt shape (stdout, one JSON object):

```json
{
  "ok": true,
  "command": "wb",
  "verb": "ship",
  "status": "ok",
  "cause": null,
  "steps": [],
  "next_verb": null
}
```

`status` is `ok` | `noop` | `error`. Re-running a completed verb returns
`status=noop` with exit 0 (not an error).

## Verb table

| Verb | Collapses | Model needed? |
| --- | --- | --- |
| `wb start <TASK>` | `task-start` + worktree + baseline receipt (incl. net-new-draft path) | no |
| `wb status` | `context` + `status` (bounded read) | no |
| `wb slice <N>` | `slice-start` with the plan's `TEST_CMD` | no |
| `wb close <N>` | checklist tick via `sync-task-plan-checklist` (close_slice MCP + revision retry are server/agent-side) | no* |
| `wb gate` | prints review-parallel → auto-fix → close-check bracket | yes — review/fix content only |
| `wb ship` | merge ff→main **then** `task-finish` (lane reap) **then** doc restore | no |
| `wb stop` | pause/abandon status + `task-reap` teardown, **no** merge | no |
| `wb accept <doc>` | `plan-accept` (inline-commit with `--local` / `WB_ACCEPT_LOCAL=1`) | no |
| `wb doctor` | `doctor` (env/mcp/branch/lifecycle + skew / wedged-writer surfaces) | no |

\* `wb close` ticks the checklist; the agent still owns the `close_slice`
MCP write (session/rationale). Server-side optimistic revision retry is
already in handoff (implementation note / T8).

### `wb ship` ordering trap

`task-finish` is **post-merge teardown only** — it never merges. The
wrapper encodes:

1. `git merge --ff-only` into `main` (or `WB_SHIP_INTO`)
2. `task-finish` (status=done → archive → lane reap → worktree/branch teardown)
3. doc restore hook (best-effort; default noop)

If merge fails, finish is **not** invoked (`cause=merge_failed`,
`finish_invoked=false`). Next verb: `gate`.

## When things refuse

| `cause` | Meaning | Next verb |
| --- | --- | --- |
| `unknown_verb` | typo / unsupported verb | `doctor` |
| `missing_arg` | no verb supplied | `status` |
| `task_required` | `start` / ship context needs `TASK` | `start` |
| `slice_n_required` | `slice` / `close` need `<N>` | `slice` |
| `test_cmd_required` | `slice` needs `TEST_CMD` / `--test-cmd` | `slice` |
| `doc_required` | `accept` needs a plan path or `--task` | `accept` |
| `not_in_git_repo` | cwd is not a git worktree | `doctor` |
| `merge_failed` | ff merge into main refused | `gate` |
| `finish_failed` | merge ok but `task-finish` failed | `ship` |
| `step_failed` | a composed step failed | `status` |
| `lifecycle_failed` | underlying lifecycle handler failed | `doctor` |

Exit codes: `0` = ok/noop, `2` = named-cause refusal, `130` = interrupt.
