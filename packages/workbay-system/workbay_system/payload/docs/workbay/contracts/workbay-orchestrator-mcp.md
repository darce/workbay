---
boundary_owner: agentic-tooling
---

# WorkBay Orchestrator MCP Contract

## Purpose

`workbay-orchestrator-mcp` is the MCP server for orchestration, daemon lifecycle, lane management, worker coordination, plan cursors, and turn metrics. It was extracted from `workbay-handoff-mcp` in internal and exposes **~38 tools**. It shares `handoff.db` and `mcp-artifacts.db` with `workbay-handoff-mcp`; SQLite WAL mode makes concurrent readers safe.

For task state, review findings, artifacts, and close checks see [`workbay-handoff-mcp.md`](workbay-handoff-mcp.md).

## Runtime Configuration

CLI args take precedence over env vars. Configuration follows the same shape as `workbay-handoff-mcp`.

Supported config inputs:

- `--workspace-root` or `WORKBAY_HANDOFF_WORKSPACE_ROOT`
- `--state-dir` or `WORKBAY_HANDOFF_STATE_DIR`
- `--current-task-path` or `WORKBAY_HANDOFF_CURRENT_TASK_PATH`
- `--exports-dir` or `WORKBAY_HANDOFF_EXPORTS_DIR`

Default workspace-owned state:

- DB: `.task-state/handoff.db` (shared with core ledger)
- artifact DB: `.task-state/mcp-artifacts.db` (shared with core ledger)
- daemon logs: `logs/worker-daemon/`
- orchestrator logs: `logs/daemon/`
- lock files: `.task-state/`

Delivery is **git-only** (internal): same tagged-ref install
path as [`workbay-handoff-mcp`](workbay-handoff-mcp.md); PyPI `uvx …@` is retired.

## Harness MCP launch

Same stdlib shim contract as [`workbay-handoff-mcp`](workbay-handoff-mcp.md):
`python3 scripts/hooks/mcp_launch.py workbay-orchestrator-mcp` in root MCP
configs. Session serve uses workspace `.venv` or the bootstrap-provisioned
`uv tool` binary; PyPI/`uvx` per-session resolve is not used.

## Runtime bootstrap (monorepo development)

```bash
uv tool install "mcp-workbay-orchestrator @ git+https://github.com/darce/workbay.git@mcp-workbay-orchestrator-vX.Y.Z#subdirectory=packages/mcp-workbay-orchestrator"

mcp-workbay-orchestrator --workspace-root "$(pwd)" doctor
```

Notes:

- `doctor` verifies the `orchestration/` directory, daemon script presence, and DB accessibility.
- When running from repo source: `PYTHONPATH="packages/mcp-workbay-orchestrator/src:packages/workbay-codex-bridge/src" python3 -m workbay_orchestrator_mcp ...`.

## MCP Tool Surface

Surface classes: `action` (mutates state/runtime), `query` (read-only), `generator` (derives/aggregates).

### Cross-task and Review-summary Tools

| Tool                          | Surface class | Idempotent | Notes                                                                                                                                                                              |
| ----------------------------- | ------------- | ---------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `switch_task`                 | action        | no         | Archives outgoing task and activates target task.                                                                                                                                  |
| `get_latest_slice_review_packet` | query      | yes        | Resolves the latest `slice_complete_*` decision into a deterministic review packet.                                                                                                |
| `get_review_findings_summary` | generator     | yes        | Returns aggregated counts and top open findings.                                                                                                                                   |
| `reconcile_review_findings`   | generator     | yes        | Compares open findings with current files; `apply=true` turns it into a mutating action.                                                                                           |

### Lane Management

| Tool                   | Surface class | Idempotent | Notes                                                                                                                                       |
| ---------------------- | ------------- | ---------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| `upsert_worktree_lane` | action        | no         | Updates lane metadata and regenerates `CURRENT_TASK.json`.                                                                                    |
| `close_worktree_lane`  | action        | no         | Transitions lane status to merged or closed.                                                                                                |
| `list_worktree_lanes`  | query         | yes        | Lists registered lane rows.                                                                                                                 |
| `get_lane_activity`    | generator     | yes        | Aggregated lane summary across decisions, tests, blockers, and messages; supports `format="archival"` for compact archive-friendly summaries. |
| `record_lane_message`  | action        | no         | Appends lane message state.                                                                                                                 |
| `update_lane_message`  | action        | no         | Mutates lane-message status.                                                                                                                |
| `list_lane_messages`   | query         | yes        | Lists lane messages.                                                                                                                        |
| `record_lane_brief`    | action        | no         | Creates a structured brief on top of lane messages.                                                                                         |
| `list_lane_briefs`     | query         | yes        | Lists structured lane briefs.                                                                                                               |

### Worker Reports

| Tool                  | Surface class | Idempotent | Notes                                    |
| --------------------- | ------------- | ---------- | ---------------------------------------- |
| `record_worker_report` | action       | no         | Appends structured worker handback state. |
| `list_worker_reports`  | query        | yes        | Lists worker reports.                     |

### Plan Cursors

| Tool                | Surface class | Idempotent | Notes                                                   |
| ------------------- | ------------- | ---------- | ------------------------------------------------------- |
| `get_plan_cursor`   | query         | yes        | Reads one durable plan cursor.                          |
| `list_plan_cursors` | query         | yes        | Lists plan cursor rows.                                 |
| `upsert_plan_cursor` | action       | no         | Mutates plan cursor state; can enforce clean-slice gate. |

### Turn Metrics

| Tool                     | Surface class | Idempotent | Notes                                                                                                               |
| ------------------------ | ------------- | ---------- | ------------------------------------------------------------------------------------------------------------------- |
| `record_turn_metric`     | action        | no         | Records one durable turn-metrics row, including exact-vs-estimated usage metadata and attribution payloads.         |
| `list_turn_metrics`      | query         | yes        | Lists durable turn-metrics rows, optionally filtered by lane, backend, model, or phase.                             |
| `get_turn_metrics_summary` | generator   | yes        | Aggregates turn-metrics rows into exact-vs-estimate coverage, pressure counts, and token totals.                    |

### Daemon Lifecycle

| Tool                    | Surface class | Idempotent | Notes                                                                                                                                  |
| ----------------------- | ------------- | ---------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| `orchestrator_start`    | action        | no         | Starts shared orchestrator daemon.                                                                                                     |
| `orchestrator_status`   | query         | yes        | Inspects orchestrator runtime state.                                                                                                   |
| `orchestrator_stop`     | action        | no         | Stops orchestrator daemon.                                                                                                             |
| `orchestrator_pause`    | action        | no         | Creates pause sentinel.                                                                                                                |
| `orchestrator_resume`   | action        | no         | Clears pause sentinel.                                                                                                                 |
| `orchestrator_single_cycle` | action   | no         | Runs one full dispatch/poll/intake/verify cycle.                                                                                       |
| `worker_start`          | action        | no         | Starts one lane worker daemon.                                                                                                         |
| `worker_status`         | query         | yes        | Inspects worker runtime state and health metadata.                                                                                     |
| `worker_event_history`  | query         | yes        | Reads worker JSONL event history. Use canonical event names (`cycle_start`, `exec_complete`, `review_complete`).                       |
| `worker_stop`           | action        | no         | Stops worker daemon.                                                                                                                   |
| `worker_resume`         | action        | no         | Resumes stopped worker daemon.                                                                                                         |
| `worker_start_all`      | action        | no         | Starts multiple worker daemons.                                                                                                        |

### Backend and Dispatch

| Tool                    | Surface class | Idempotent | Notes                                                                       |
| ----------------------- | ------------- | ---------- | --------------------------------------------------------------------------- |
| `run_structured_turn`   | action        | no         | Executes a synchronous backend turn; may spend tokens or mutate external runtime state. |
| `dispatch_lane_work`    | action        | no         | Mutates lane dispatch parameters for future cycles.                         |
| `list_available_backends` | query       | yes        | Reads registered backend catalog and, by default, probed availability fields. |

### Metrics Snapshot

| Tool                | Surface class | Idempotent | Notes                                                                                                                                |
| ------------------- | ------------- | ---------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| `get_metrics_summary` | generator  | yes        | Derived metrics snapshot across lanes, retrieval, context pressure, process-health signals, and handoff-memory health.               |

## Review-Ready Evaluation

`make review-ready` invokes `review_ready.py` from the orchestrator package. It reads handoff state via `get_handoff_state(sections=...)` to check test evidence, open findings, and staleness.

Behavioral constraints:

- The `identity` token in `sections` is a special override that requests only the `active` and `limits` envelope fields; it cancels all other section tokens. To request data sections alongside identity, omit the `identity` token since identity fields are always included unconditionally.
- `evaluate_review_ready` reads test evidence from `state["data"]["tests_recent"]` (the nested envelope path). Callers that mock the state for testing must place `tests_recent` under `data`.
- Boundary-file detection uses `DEFAULT_BOUNDARY_PREFIXES` (`apps/`, `packages/mcp-workbay-orchestrator/src/`, `packages/shared-contracts/schemas/`). Contract co-change is satisfied when at least one file from `DEFAULT_CONTRACT_PREFIXES` (`docs/workbay/contracts/`, `packages/shared-contracts/`) or the contract-change checklist also appears in the diff.

## `get_metrics_summary` Snapshot Shape

`get_metrics_summary` is additive: existing top-level sections remain stable and new sections are appended rather than replacing prior keys.

Top-level snapshot fields:

- `timestamp`: ISO-8601 generation time.
- `task_ref`: task the snapshot was generated for.
- `token_burn`: aggregate token spend and converged-cycle efficiency.
- `context_pressure`: pressure-level ratios from worker events.
- `fts5_retrieval`: handoff/artifact index counts.
- `lane_health`: scope-violation, exhaustion, and convergence signals.
- `process_health`: repo-process quality signals derived from handoff state and git history.
- `handoff_memory`: hot-state and artifact-footprint signals derived from current MCP state.
- `planning_drift`: plan-cursor completion vs dispatch drift within the evaluation window.
- `stale_artifact_rate`: artifact-source staleness counts and ratio from `mcp-artifacts.db`.
- `archive_rate`: repo-wide task archive cadence derived from `task_archives`.
- `ctx7_adoption`: decision-level `ctx7 library id:` reuse and adoption counts.
- `phase_timing`: exec/review timing aggregates.
- `ace_documentation`: strategy-bullet and pruning-candidate counts from instruction files.

`process_health` currently includes:

- `reopened_finding_rate`: `{ value, reopened_findings, total_findings }`
- `finding_resolution_velocity_hours`: `{ median_hours, resolved_findings }`
- `handoff_decision_completeness`: `{ value, structured_decisions, total_decisions }`
- `contract_co_change_signal`: `{ data_available, recent_commits_scanned, boundary_touching_commits, boundary_commits_with_contract_co_change, value }`

`handoff_memory` currently includes:

- `hot_state_size_bytes`: serialized byte size of the `get_handoff_state`-shaped hot-state payload for the active task
- `total_decisions`: total decisions stored for the task
- `total_findings`: total review findings stored for the task
- `artifact_source_count`: indexed artifact-source count from `mcp-artifacts.db`

`planning_drift` currently includes:

- `window_days`: lookback window used for evaluation
- `total`: total `plan_cursors` rows updated inside the window
- `terminal`: rows in terminal states (`completed`, `skipped`) inside the window
- `drift`: `1 - terminal / total`, or `null` when no rows exist in the window

`stale_artifact_rate` currently includes:

- `window_days`: staleness threshold for artifact freshness
- `total`: total indexed artifact sources
- `stale_count`: artifact sources with `updated_at` older than the threshold
- `stale_rate`: `stale_count / total`, with `0.0` for an empty artifact index

`archive_rate` currently includes:

- `window_days`: lookback window used for the in-window archive count
- `total_archives`: total archived tasks recorded in `task_archives`
- `in_window`: archived tasks whose `archived_at` falls inside the window
- `mean_interval_hours`: average hours between archive events across the repo, or `null` when fewer than two archives exist

`ctx7_adoption` currently includes:

- `decisions_with_ctx7`: number of task decisions containing at least one `ctx7 library id:`
- `unique_library_ids`: distinct library ids referenced by those decisions
- `reuse_ratio`: total library-id mentions divided by distinct library ids, or `null` when none exist
- `library_ids`: sorted distinct library ids referenced in task decisions

### Deferred Instrumentation

The following metrics require new structured telemetry not yet in the schema. These are explicit instrumentation gaps:

- `runtime_parity` coverage: requires structured classification of verification rows beyond raw command text
- `performance_evidence` coverage: requires typed linkage between verification records and latency / queue-health benchmark evidence
- `resolved_from_hot_state` ratio: requires agent-side retrieval telemetry
- `ctx7` token-cost reduction: requires prompt/tooling telemetry outside the current handoff DB schema

Consumers should treat unknown keys as forward-compatible additions and should not require every section to have `data_available=true`.

## Turn Metrics Surfaces

`turn_metrics` is the canonical per-turn ledger for token and prompt-budget telemetry.

Common stored fields:

- Identity: `task_ref`, `lane_id`, `session`, `cycle`, `phase`, `backend`, `model`, `thread_id`, `turn_id`
- Observed usage: `input_tokens`, `output_tokens`, `cached_input_tokens`, `reasoning_output_tokens`, `total_tokens`
- Prompt-budget context: `model_context_window`, `prompt_tokens`, `prompt_chars`, `prompt_token_source`, `utilization_ratio`, `domain_signal_ratio`, `pressure_level`
- Attribution payloads: `attribution`, `section_sizes`, `raw_usage`
- Attribution booleans: `used_ace_guidance`, `used_artifact_context`, `used_slice_packet`, `used_recent_lane_history`, `used_global_context`, `used_ctx7`, plus `ctx7_query_count`
- Usage exactness: `usage_source` with additive values `observed`, `tokenizer_estimate`, or `char_estimate`

Exactness rules:

- `usage_source="observed"` means the token totals came from a provider/backend response, not a local heuristic.
- `prompt_token_source="observed"` means preflight tokenization used an explicitly supported exact tokenizer path for that backend/model combination.
- `prompt_token_source="tokenizer_estimate"` means prompt tokens came from a tokenizer-backed estimate on a non-exact model path.
- `prompt_token_source="char_estimate"` means prompt tokens came from the fallback character heuristic and must not be treated as exact.

`get_turn_metrics_summary` returns:

- `total_turns`
- `usage_source_counts`
- `prompt_token_source_counts`
- `pressure_level_counts`
- `total_tokens`
- `prompt_tokens`
- `by_lane_total_tokens`
- `by_backend_model_total_tokens`

## `get_lane_activity` Archival Example

```json
{
  "ok": true,
  "task_ref": "agentic-development-process-hardening-epic",
  "format": "archival",
  "lane": {
    "lane_id": "backend",
    "status": "active"
  },
  "summary": {
    "decisions": { "count": 3, "latest_rationale_excerpt": "Aligned the API contract..." },
    "findings": { "counts_by_status": { "open": 1, "fixed": 4, "wontfix": 0, "deferred": 0 } },
    "reports": { "count": 2, "latest_merge_ready": true },
    "messages": {
      "counts_by_direction": { "orchestrator_to_worker": 2, "worker_to_orchestrator": 3 },
      "counts_by_status": { "open": 0, "acknowledged": 1, "closed": 4 }
    },
    "tests": { "total": 5, "passed": 5, "pass_rate": 1.0 }
  }
}
```

## Request Shape Notes

- `switch_task(task_ref)` auto-archives the outgoing task (full snapshot) and activates the target, restoring the objective and `target_branch` from its archive when not provided. Pass `target_branch` explicitly to bind a task to its work branch at init time. Idempotent if the target is already active.
- `upsert_plan_cursor` accepts optional `require_clean_slice`. When enabled, the update fails unless there are no open HIGH findings in the relevant lane/task scope and at least one recent `verified_tests` row exists since the cursor's prior update time.
- `record_lane_message` / `update_lane_message` model orchestrator-to-worker and worker-to-orchestrator communication without relying on direct session chat.
- `record_lane_message` accepts artifact refs in its payload; the CLI fallback exposes this as repeated `--artifact <source-id>` flags.
- `record_lane_brief` / `list_lane_briefs` are the structured-brief helpers built on top of `lane_messages`; they persist an open `orchestrator_to_worker` message with a `brief:<reason>` subject plus a compact JSON payload (`source_lane`, `reason`, `summary`, optional `required_actions`, optional `artifacts`).
- `get_lane_activity` is the lane-scoped query surface for decisions, tests, blockers, actions, findings, worker reports, and lane messages.
- `get_lane_activity(format="full")` preserves the existing detailed payload.
- `get_lane_activity(format="archival")` returns a compact `summary` object with decision count plus latest rationale excerpt, finding counts by status, latest worker merge-ready state, message counts by direction/status, and verified-test totals with pass rate.
- `get_latest_slice_review_packet` resolves the latest `slice_complete_*` decision into a deterministic review packet. The packet includes `slice_label`, `decision_id`, `decision`, `session`, `lane_id`, `plan_item_id`, `changed_files`, `test_commands`, `contract_files`, `review_kind`, `review_guide_path`, `scope_source="slice_packet"`, and a rationale excerpt.
- `get_latest_slice_review_packet(review_kind="planning")` only matches docs-only slices (`changed_files` all under `docs/`). Mixed doc-plus-code slices resolve to `branch`.
- `worker_status` should be treated as an inspection tool, not a boolean health check. Use `running`, `worker_state`, `attention_required`, and `state_summary` together. Current durable worker states include `idle`, `waiting_for_orchestrator`, `handoff_failed`, `paused`, and `stopped`.
- `worker_status` also exposes hardening signals: `exhaustion_streak` (consecutive non-converged cycles), `cumulative_tokens` (session token spend), `health` (`healthy` / `degraded` / `unhealthy`), and a `context_utilization` sub-dict with `utilization_ratio`, `domain_signal_ratio`, and `pressure` (`normal` / `elevated` / `high`).
- `worker_status` and dashboard surfaces should be treated as the authoritative runtime view for model size, requested/effective reasoning effort, token burn, and context pressure. Use them before redispatching or promoting a lane.
- `worker_stop` performs authoritative lock cleanup. After stop, the lock file is deleted and a `worker_stopped` JSONL event is emitted.
- `worker_start` and `worker_start_all` accept `session_mode`. Use `fresh_turn` for the default one-turn-per-session isolation, or `shared_lane` to reuse context only within the same lane worker session.
- `worker_start_all` is dependency-aware when a manifest merge order exists. Lanes whose upstream dependencies still have unresolved dispatched work are returned as `skipped` with `reason="unresolved_upstream_dependencies"`.
- `orchestrator_start` / `single-cycle` support `worker_start_mode`. Use `mcp` for the default MCP-first worker pool behavior, or `manual` when the host should keep worker startup in shell space.
- A recorded `handoff_failed` worker state means the implementation/review turn already completed and the saved result must be retried or inspected without silently rerunning the same lane assignment.
- `get_review_findings_summary` accepts optional `review_mode` filter and scopes counts/top lists to that mode.
- `reconcile_review_findings` validates state integrity (duplicates, done+open mismatch, stale open findings, provenance completeness, reopen metadata coherence) and can apply safe dedupe fixes.

## Scope Enforcement and Effective Owned Paths

Scope enforcement is a runtime gate in `lane_exec.py`. After worker execution completes, the worktree diff is validated against `effective_owned_paths`. If violations are found, review is skipped and a `scope_violation` event is emitted.

- The orchestrator can narrow scope for a specific dispatch by embedding `effective_owned_paths` as a JSON-encoded string in the `artifacts` list of the dispatch lane message: `artifacts=[json.dumps({"type": "owned_paths_override", "paths": [...]})]`.
- The `artifacts` field accepts `list[str]`; the normalizer's `_coerce_string_list()` preserves strings but silently drops non-string items, so the override dict must be serialized before dispatch.
- `lane_exec.py` reads the override from the most recent `orchestrator_to_worker` message via `list_lane_messages`, iterating `artifacts` and attempting `json.loads()` on each string to find the entry with `type == "owned_paths_override"`. Non-parseable strings are skipped.

## Lane Health Scoring

The orchestrator daemon computes per-lane health via `_check_lane_health()` based on exhaustion streak, scope violation history, token burn, and context pressure.

- Health enum: `healthy` / `degraded` / `unhealthy`.
- The daemon skips auto-start for lanes with `exhaustion_streak >= 2` and emits `lane_unhealthy`. Unhealthy lanes require an explicit orchestrator decision (e.g., `promote_model`, `split_lane`, `close_lane`, `fresh_worktree`).
- Health transitions emit `lane_health_changed` events.

## Worker Daemon JSONL Events

The worker daemon emits structured JSONL events to `logs/worker-daemon/worker-<lane>.jsonl`:

- `scope_violation`: files modified outside owned_paths; turn rejected before review.
- `exhaustion_streak`: consecutive non-converged review cycles; includes streak count, run_id, lane_id.
- `token_burn_warning`: cumulative session token spend exceeds `token_burn_threshold` (default 2M).
- `worker_stopped`: clean daemon shutdown with lock cleanup.
- `context_pressure`: prompt consuming an unsafe fraction of the model's context window.
- `artifact_indexed`: large execution details were indexed into the artifact sidecar and referenced by `details_artifact_ref`.
- `lane_health_changed`: health state transition (e.g., `healthy` -> `degraded`).

## Lane Manifest Configuration

Lane manifests at `config/lane-orchestration/<task-ref>.json` support these hardening-related fields:

- `token_burn_threshold`: cumulative token spend threshold per session before emitting a warning (default: `2000000`).
- `model_context_window`: model context window size for context utilization measurement (default: `128000`).

## BackendAdapter Protocol

All execution backends MUST implement the `BackendAdapter` protocol defined in `packages/mcp-workbay-orchestrator/src/workbay_orchestrator_mcp/orchestration/backend_registry.py`. This ensures consistent handling of `execute()` and `resolve_reasoning_effort()` across Codex, Claude, and local models.

## Tool Signatures (Key)

- `switch_task(task_ref: string, objective: string = None, focus: string = None, status: string = "in_progress", actor: object = None, target_branch: string = None)`
- `orchestrator_start(task_ref: string, backend: string, poll_interval: float, single_pass: bool, model: string = None)`
- `orchestrator_single_cycle(task_ref: string, backend: string, model: string = None, worker_start_mode: string = "mcp")`
- `worker_start(task_ref: string, lane_id: string, backend: string, poll_interval: float, single_pass: bool, session_mode: string, model: string = None, reasoning_effort: string = None)`
- `worker_event_history(task_ref: string, lane_id: string, limit: int = 20)`
- `worker_start_all(task_ref: string, backend: string, poll_interval: float, single_pass: bool, session_mode: string, model: string = None)`
- `run_structured_turn(prompt: string, schema: object, cwd: string, backend: string, env: dict = None, model: string = None, reasoning_effort: string = None, timeout_seconds: float = 120.0)`
- `dispatch_lane_work(task_ref: string, lane_id: string, model: string = None, backend: string = None, reasoning_effort: string = None)`
- `list_available_backends(probe: bool = true)` -> `{ok: bool, probed: bool, backends: object}`. The default response includes per-backend `is_available`, `availability_state`, and `availability_detail`; pass `probe=false` only for a cheap declaration-only catalog.

## CLI Fallback

Primary entrypoints:

- `mcp-workbay-orchestrator --workspace-root <repo> serve-stdio`
- `mcp-workbay-orchestrator --workspace-root <repo> doctor`

Operational subcommands (all require `--workspace-root`):

**Orchestrator daemon**
- `orchestrator-start --task-ref <ref> [--backend codex-cli] [--poll-interval 60] [--single-pass] [--worker-start-mode mcp] [--worker-reasoning-effort auto] [--model <model>]`
- `orchestrator-status`
- `orchestrator-pause`
- `orchestrator-resume`
- `orchestrator-stop [--force] [--wait 5.0]`
- `orchestrator-cycle --task-ref <ref> [--backend codex-cli] [--dry-run] [--timeout 300.0] [--worker-start-mode mcp] [--worker-reasoning-effort auto] [--model <model>]`

**Worker daemon**
- `worker-start --task-ref <ref> --lane-id <id> [--backend codex-subagent] [--poll-interval 30] [--single-pass] [--session <name>] [--session-mode fresh_turn] [--reasoning-effort inherit] [--model <model>]`
- `worker-status --task-ref <ref> --lane-id <id>`
- `worker-stop --task-ref <ref> --lane-id <id> [--force]`
- `worker-resume --task-ref <ref> --lane-id <id>`
- `worker-start-all --task-ref <ref> [--backend codex-subagent] [--poll-interval 30] [--single-pass] [--session-mode fresh_turn] [--reasoning-effort inherit] [--model <model>]`
- `worker-events --task-ref <ref> --lane-id <id> [--limit 50] [--event-name <name>]`

**Lane and dispatch**
- `dispatch --lane-id <id> [--task-ref <ref>] [--model <model>] [--backend <backend>] [--reasoning-effort <effort>] [--start-worker]`

**Metadata**
- `list-backends`
- `metrics [--task-ref <ref>] [--format markdown|json]`

## HTTP Transport

HTTP transport (`serve-http`) is not yet implemented for the orchestration server. Use `serve-stdio` for both VS Code and Codex sessions. The core ledger server (`workbay-handoff-mcp`) provides `serve-http` on port `8741` if HTTP transport is needed.
