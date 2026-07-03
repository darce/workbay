# Environment Variables Registry

Canonical catalog of every environment variable read by the monorepo's runtime
code. Tests-only and dev-tooling-only knobs are listed in their own section so
operators can tell production knobs apart from test scaffolding.

> **Prefix conventions**
>
> - `WORKBAY_HANDOFF_*` — `mcp-workbay-handoff` runtime config (state
>   dir, render paths, write-context overrides, hook knobs).
> - `WORKBAY_*` — cross-cutting knobs owned by `workbay-system` (hook
>   protocol, branch-naming overrides, lane plumbing, lifecycle tooling).
>
> The legacy `AGENT_HANDOFF_*` / `AGENT_ORCHESTRATOR_*` / `AGENTIC_*` /
> `internal_*` env-var names are **no longer read** — the one-release alias shim
> was retired in `internal`. Set the canonical `WORKBAY_*`
> names only.

## How to add a new variable

1. Pick the prefix from the package that owns the read site
   (`WORKBAY_HANDOFF_*` for `mcp-workbay-handoff`, `WORKBAY_*` for
   `workbay-system`). Do not invent a new prefix.
2. Add a row to the appropriate table below with: name, type, default,
   one-line description, source file.
3. If the var configures the compaction subsystem, add the field to
   `workbay_handoff_mcp.compaction.CompactionSettings` and read it through
   `CompactionSettings.from_env()` — never via a fresh `os.environ.get`.
4. If the var has a typo-prone parser (int, enum), prefer Pydantic
   validation at the boundary so bad values raise rather than fall back
   to defaults silently.

## `WORKBAY_HANDOFF_*` — handoff runtime

| Name | Type | Default | Description | Source |
|---|---|---|---|---|
| `WORKBAY_HANDOFF_ACE_GUIDANCE_USED` | bool | unset | Marks that a turn already consumed the in-prompt agent-context guidance so the next prompt can omit it. | `mcp-workbay-handoff/src/workbay_handoff_mcp/api.py` |
| `WORKBAY_HANDOFF_COMPACTION_DISABLED` | bool | `0` | When truthy, silences the unified disable resolver: the compact-session Stop hook short-circuits with `compaction skipped: disabled (source=env)` and `compute_compaction_advisory` returns a `disabled=true,disabled_source="env"` envelope with no threshold/floor evaluation. Host-harness compaction is unaffected. The same surface is also writable per-task or workspace-default via the `compaction(operation="disable"\|"enable"\|"status")` MCP op, `mcp-workbay-handoff compaction --operation <op>` CLI, and `make compaction-disable\|compaction-enable\|compaction-status [TASK=<ref>]` operator wrappers. | `workbay-system/scripts/hooks/compact-session.py`, `mcp-workbay-handoff/src/workbay_handoff_mcp/compaction.py` |
| `WORKBAY_HANDOFF_COMPACTION_MIN_NEW_TURNS` | int (≥0) | _(retired)_ | **Retired** as a compaction trigger (implementation note / D1). No longer read from the environment; the field remains on `CompactionSettings` defaulting to `0` for backward compat only and does not gate compaction. | `mcp-workbay-handoff/src/workbay_handoff_mcp/compaction.py` |
| `WORKBAY_HANDOFF_COMPACTION_MIN_NEW_TOKENS` | int (≥0) | contract `threshold_tokens` | Sole compaction trigger gate: a compaction row is written once the encoded new-turn token count reaches the threshold. When unset, default resolves env → overlay `compaction.thresholds.tokens` → `harness-protocol.yaml` `compaction.threshold_tokens` → code constant. `0` skips every turn; `1` is the documented no-gate idiom; use `WORKBAY_HANDOFF_COMPACTION_DISABLED` to silence compaction entirely. | `workbay-system/scripts/hooks/compact-session.py`, `mcp-workbay-handoff/src/workbay_handoff_mcp/compaction.py` |
| `WORKBAY_HANDOFF_COMPACTION_THRESHOLD_TOKENS` | int (≥0) | contract `threshold_tokens` | Advisory token-threshold override (env > overlay > contract). Does not set the Stop-hook gate — use `MIN_NEW_TOKENS` for that; overlay `compaction.thresholds.tokens` feeds both advisory and gate defaults when the respective env vars are unset. | `mcp-workbay-handoff/src/workbay_handoff_mcp/compaction_contract.py` |
| `WORKBAY_HANDOFF_COMPACTION_THRESHOLD_CHARS` | int (≥0) | contract `threshold_chars` | Advisory char-threshold override (env > overlay > contract). | `mcp-workbay-handoff/src/workbay_handoff_mcp/compaction_contract.py` |
| `WORKBAY_HANDOFF_ACTIVE_TASK` | task_ref | unset | Pin active task attribution for compaction/reinjection hooks when multiple live rows share the workspace. Must name an existing live-active row (`in_progress`, `review`, or `blocked`). MCP write paths still raise on ambiguity unless `task_ref` is explicit. | `mcp-workbay-handoff/src/workbay_handoff_mcp/shared_primitives.py`, `workbay-system/scripts/hooks/compact-session.py`, `workbay-system/scripts/hooks/reinject-context.py` |
| `WORKBAY_HANDOFF_COMPACTION_NOTIFY` | bool | `1` (on) | Emit a user-visible one-line console notification when a compaction fires (Stop hook) and when prior context is re-fed at session start (reinjection). For `claude-code` the notification rides a `systemMessage` JSON envelope; other harnesses keep the stderr line / raw block. Set falsy to suppress the notification only (compaction/reinjection still run). | `workbay-system/scripts/hooks/compact-session.py`, `workbay-system/scripts/hooks/reinject-context.py` |
| `WORKBAY_HANDOFF_CURRENT_TASK_AUTO_REGEN` | bool | unset | Opt-in to server-side auto-regeneration of `CURRENT_TASK.json` after handoff writes. See `docs/CONSUMER.md`. | `mcp-workbay-handoff/src/workbay_handoff_mcp/config.py` |
| `WORKBAY_HANDOFF_CURRENT_TASK_PATH` | path | derived | Override the rendered `CURRENT_TASK.json` location. | `mcp-workbay-handoff/src/workbay_handoff_mcp/config.py` |
| `WORKBAY_HANDOFF_DASHBOARD_PATH` | path | derived | Override the rendered `DASHBOARD.txt` location. | `mcp-workbay-handoff/src/workbay_handoff_mcp/config.py` |
| `WORKBAY_HANDOFF_DEFAULT_AGENT` | string | derived | Stable agent identity used when MCP write payloads omit `actor.agent`. | `mcp-workbay-handoff/src/workbay_handoff_mcp/shared_write_context.py` |
| `WORKBAY_HANDOFF_DEFAULT_BRANCH` | string | derived | Branch hint for write-provenance. When **both** this and `WORKBAY_HANDOFF_DEFAULT_COMMIT_SHA` (or paired `GITHUB_*` / `CI_*` fallbacks) are non-empty, `_detect_git_write_context` returns immediately without `git rev-parse`. Otherwise live git may override a branch-only hint. | `mcp-workbay-handoff/src/workbay_handoff_mcp/shared_write_context.py` |
| `WORKBAY_HANDOFF_DEFAULT_COMMIT_SHA` | sha | derived | Commit-SHA hint for write-provenance. Participates in the same all-or-nothing short-circuit as `WORKBAY_HANDOFF_DEFAULT_BRANCH`; when only commit is set, live git may still supply branch and can overwrite commit when `git rev-parse HEAD` succeeds. | `mcp-workbay-handoff/src/workbay_handoff_mcp/shared_write_context.py` |
| `WORKBAY_HANDOFF_DOCTOR_STRICT` | bool | unset | When truthy, `run_doctor()` treats warnings as failures. | `mcp-workbay-handoff/src/workbay_handoff_mcp/...` |
| `WORKBAY_HANDOFF_ENFORCE_BRANCH` | bool | `0` | Reject writes whose payload branch does not match the workspace branch. | `mcp-workbay-handoff/src/workbay_handoff_mcp/shared_write_context.py` |
| `WORKBAY_HANDOFF_EXPORTS_DIR` | path | `.task-state/exports` | Destination for `export_handoff_state()` artifacts. | `mcp-workbay-handoff/src/workbay_handoff_mcp/config.py` |
| `WORKBAY_HANDOFF_HARNESS` | enum (`claude-code`/`codex`/`cursor`/`manual`) | `claude-code` | Harness label for compaction rows. Unknown values coerce to `manual`. | `workbay-system/scripts/hooks/compact-session.py` |
| `WORKBAY_HANDOFF_SKIP_BRANCH_ENFORCEMENT` | bool | unset | Bypass `WORKBAY_HANDOFF_ENFORCE_BRANCH` for the current process. Tests + bootstrapping. | `mcp-workbay-handoff/src/workbay_handoff_mcp/shared_write_context.py` |
| `WORKBAY_HANDOFF_SKIP_SHA_VALIDATION` | bool | unset | Bypass commit-sha existence checks. Tests + bootstrapping. | `mcp-workbay-handoff/src/workbay_handoff_mcp/shared_write_context.py` |
| `WORKBAY_HANDOFF_SKIP_WORKTREE_DERIVATION` | bool | unset | Bypass git-worktree derivation in write-context resolution. | `mcp-workbay-handoff/src/workbay_handoff_mcp/shared_write_context.py` |
| `WORKBAY_HANDOFF_STATE_DIR` | path | `<workspace>/.task-state` | Override the SQLite + projection file root. Tests use this for isolation. | `mcp-workbay-handoff/src/workbay_handoff_mcp/config.py` |
| `WORKBAY_HANDOFF_TOOL_PROFILE` | string | derived | Tool-profile selector for the MCP server's tool surface. | `mcp-workbay-handoff/src/workbay_handoff_mcp/config.py` |
| `WORKBAY_HANDOFF_WORKSPACE_ROOT` | path | derived | Override workspace-root resolution when the consumer-root probe is ambiguous. | `mcp-workbay-handoff/src/workbay_handoff_mcp/config.py` |

On a successful `compact-session` Stop hook run, stderr preserves
`compaction_id=<id>` as the first line and then appends
`tokens_saved_estimate`, `input_chars`, `summary_chars`, and
`prose_residual_chars` as one `key=value` line each. Those receipt fields
describe the internal `session_compactions` artifact only; host-harness
compaction remains outside these environment knobs.

None of the `WORKBAY_HANDOFF_COMPACTION_*` vars (nor the writable per-task /
workspace DB enable/disable rows behind `compaction(operation=...)`) install a
harness Stop adapter. They only gate whether the compaction surface evaluates
and records once an adapter is already wired. Installing the automatic recorder
is a separate, opt-in step (`workbay-bootstrap install --install-<harness>-stop-hook`);
`make doctor LIFECYCLE_ARGS=--json` reports per-harness adapter wiring as
installed / drifted (`stop_adapters_drifted`) / optional-not-installed. See the
"Enabled vs wired" subsection of
`packages/workbay-system/docs/workbay/rules/development-workflow.md`.

## `WORKBAY_*` — cross-cutting

| Name | Type | Default | Description | Source |
|---|---|---|---|---|
| `WORKBAY_ALLOW_NONCONFORMING_BRANCH` | bool | unset | Opt-out of the branch-naming pre-commit gate for the current commit. | `workbay-system/scripts/hooks/check_branch_naming.py` |
| `WORKBAY_ALLOW_NONCONFORMING_BRANCH_REASON` | string | unset | Required when `WORKBAY_ALLOW_NONCONFORMING_BRANCH=1`; recorded with the override. | `workbay-system/scripts/hooks/check_branch_naming.py` |
| `WORKBAY_ALLOW_NONCONFORMING_BRANCH_PUSH` | bool | unset | Same opt-out, scoped to the pre-push hook. | `workbay-system/scripts/hooks/check_branch_naming.py` |
| `WORKBAY_ALLOW_NONCONFORMING_BRANCH_PUSH_REASON` | string | unset | Reason payload for the pre-push override. | `workbay-system/scripts/hooks/check_branch_naming.py` |
| `WORKBAY_HOOK_PROTOCOL_STRICT` | bool | unset | When truthy, `_protocol.validate_event` raises `SystemExit(2)` instead of logging on schema drift. | `workbay-system/scripts/hooks/_protocol.py` |
| `WORKBAY_LANE_ID` | string | unset | Worktree-lane identifier surfaced in write provenance. | `mcp-workbay-handoff/src/workbay_handoff_mcp/import_export.py` |
| `WORKBAY_LANE_ID_ENV` | string | `WORKBAY_LANE_ID` | Indirection knob: name of the env var that actually carries the lane id. Lets callers redirect lookups. | `mcp-workbay-handoff/src/workbay_handoff_mcp/shared_primitives.py` |
| `WORKBAY_LIFECYCLE_UV_BIN` | path | derived | Override the `uv` binary used by lifecycle scripts (`task-start`, `slice-start`, etc.). | `workbay-system/scripts/workbay/lifecycle/uv_provisioning.py` |
| `WORKBAY_WORKTREE_BOOTSTRAP_CMD` | shell string | unset | Post-provision bootstrap command for linked worktrees. Set by the `task-start` Make recipe from `LIFECYCLE_WORKTREE_BOOTSTRAP`; empty disables. Run as `sh -c` with cwd = worktree root; best-effort (never rolls back `task-start`). | `workbay-system/scripts/workbay/lifecycle/handlers/task_start.py` |
| `WORKBAY_WORKTREE_BOOTSTRAP_TIMEOUT` | int (seconds) | `600` | Timeout for the worktree bootstrap subprocess. Returncode `124` on expiry. | `workbay-system/scripts/workbay/lifecycle/handlers/task_start.py` |
| `WORKBAY_REINJECT_AB` | `0` / `1` | `0` (off) | When `1`, `reinject-context` assigns each session to a SELECTION arm via deterministic `session_id` parity — `treatment` = semantic top-K (arm B), `control` = current selection (arm A). Both arms emit a block and record `arm` + `emitted_chars`; the arm overrides `WORKBAY_REINJECT_SEMANTIC`. Supersedes the implementation note emit/suppress window (control no longer suppresses). Missing/unknown `session_id` records `arm=NULL` so unattributable rows do not pollute one arm. | `workbay-system/scripts/hooks/reinject-context.py`, `harness-protocol.yaml` |
| `WORKBAY_REINJECT_BUDGET_CHARS` | int (>0) | `1500` | Caps the total stdout size of the `reinject-context` SessionStart block. Invalid values fail open with `reinject skipped: invalid budget`. | `workbay-system/scripts/hooks/reinject-context.py` |
| `WORKBAY_REINJECT_SOURCES` | comma-list | `compact,resume` | SessionStart `source` allow-list for context re-injection. Use to add sources such as `startup` without changing hook code. | `workbay-system/scripts/hooks/reinject-context.py` |

## Harness-owned knobs (consumed by the agent harness, not this repo)

Not read by monorepo code — the agent harness consumes them — but they directly
affect WorkBay MCP behaviour, so they are catalogued here for operators.

| Name | Type | Default | Description | Consumed by |
|---|---|---|---|---|
| `MCP_TIMEOUT` | int (ms) | `30000` (~30 s) | Wall-clock budget for an MCP server's stdio `initialize` → `tools/list` handshake at session start. On overrun the harness abandons the server and registers **zero** of its tools — silently, for the whole session. **Keep the default:** the implementation note launcher shim removes `uv run`'s cold project-resolution tail, so the handshake (app-build-bound, ~7–9 s warm; ≈12 s contended p≤8) clears it with wide margin; raising it only masks a slow launcher or a missing venv. See `docs/workbay/runbook-mcp-server-launch.md`. | Claude Code harness |

## Test + dev-only knobs

Read only from test fixtures or developer tooling. Production deployments
do not set these.

| Name | Use |
|---|---|
| `WORKBAY_DISABLE_PYTEST_PATH_GUARD` | Test harness escape hatch for `test_pytest_path_guard.py`. |
| `WORKBAY_RUN_DESCRIPTION_SERVICE_ACCEPTANCE` | Set to `1` to run internal nested-suite acceptance (`test_plan0040_description_service_acceptance.py`). Skipped by default. |

## See also

- `docs/CONSUMER.md` — externally documented runtime knobs.
- `docs/UPGRADING.md` — upgrade notes that mention env vars at version
  boundaries.
- `workbay_handoff_mcp.CompactionSettings` — typed surface for
   `WORKBAY_HANDOFF_COMPACTION_*`. Add new compaction knobs there, not as
  fresh `os.environ.get` calls.
