# WorkBay Handoff MCP

Portable MCP server for agent handoff state, review findings, exports, close checks, and lane-scoped worker/orchestrator coordination.

## Scope

This package owns handoff state and related workflow primitives:

- task state, decisions, blockers, and next actions
- review findings and review runs
- generated `CURRENT_TASK.json` snapshots
- lane registration, lane activity, worker reports, and lane messages
- artifact indexing and derived metrics snapshots

It does not include unrelated repo-intel or UI helpers.

## Runtime State

By default, runtime state lives under the workspace you point the CLI at:

- SQLite DB: `.task-state/handoff.db`
- exports: `.task-state/exports/`
- generated markdown: `CURRENT_TASK.json`

Override these with CLI flags or `WORKBAY_HANDOFF_*` environment variables. Legacy pre-rebrand names are still read for one release.

### Tuning compaction thresholds

The compaction advisory (`get_handoff_state(sections="identity").compaction_advisory`)
compares observed token and char totals against thresholds. The single source
of truth for the canonical defaults is the `compaction:` block of
`docs/workbay/contracts/harness-protocol.yaml` (`threshold_tokens`,
`threshold_chars`); see that file for the current values and the rationale
comment above them. Per-deployment overrides resolve with precedence
**env > overlay > contract**, per knob, every call (no module-level cache):

1. **Environment variables**:
    - `WORKBAY_HANDOFF_COMPACTION_THRESHOLD_TOKENS=<int>`
    - `WORKBAY_HANDOFF_COMPACTION_THRESHOLD_CHARS=<int>`
2. **`.workbay-overlay.json`** at the workspace root, sibling of `surfaces`
   (`tokens` and `chars` must be non-negative integers; the values below are
   illustrative overrides, not required values):

   ```json
   {
     "surfaces": {"contracts": {"local_root": "docs/workbay"}},
     "compaction": {"thresholds": {"tokens": 90000, "chars": 360000}}
   }
   ```

   Either knob may be omitted; missing knobs fall through to env or contract.
3. **Contract default** in `harness-protocol.yaml` (single source of truth).

The advisory envelope reports the effective value at `thresholds.{tokens,chars}`
and the resolving layer at `thresholds_source.{tokens,chars}` (`"env"` /
`"overlay"` / `"contract"`). Invalid override values (non-int, negative) append
a `compaction_threshold_override_invalid: <source>=<key>=<value>` warning to
`warnings` and fall through to the next layer.

When the advisory says compaction is recommended, recording is still an
explicit action: call `compaction(operation="record", ...)`, run
`make compact-now`, or let the `compact-session` Stop hook write the row. A
successful record keeps `compaction_id=<id>` as the first operator-visible
line and then prints receipt values in stable order:

```text
compaction_id=C-WORKBAY-99-0001
tokens_saved_estimate=123
input_chars=4200
summary_chars=800
prose_residual_chars=200
```

`load_session(include_context_refresh=True)` can also return a soft
same-session `context_refresh` packet for the latest compaction. Pass the
returned `dedupe_key` back as `last_injected_compaction_id` to avoid injecting
the same packet repeatedly. This refresh packet helps attention and grounding;
it does not replace or disable the host harness's own context compaction.

### Disabling WorkBay compaction

WorkBay unifies the runtime disable surface so the advisory short-circuit
(`compute_compaction_advisory`) and the `compact-session` Stop hook are
silenced together by a single resolver. **Host-harness compaction (Claude
Code's own `/compact`, Codex's internal summarization, etc.) is not affected
— this disables WorkBay's tracker only.** For the operator-facing comparison
between WorkBay's compaction and the host harness's built-in compaction (and
the note on what the compression-ratio benchmark actually measures), see
[`docs/explainers/compaction-vs-default-harness-compaction.md`](docs/explainers/compaction-vs-default-harness-compaction.md).

Precedence (highest first):

1. `WORKBAY_HANDOFF_COMPACTION_DISABLED` env var (truthy). Legacy disable aliases are still honored for one release.
2. Task-scoped row in `compaction_settings` (when a `task_ref` is provided).
3. Workspace-default row in `compaction_settings`.
4. Otherwise: enabled.

Write the DB rows via any of these equivalent surfaces:

```bash
# Operator make wrappers (flagless, no YAML editing).
make compaction-disable                 # workspace default
make compaction-disable TASK=WORKBAY-99   # task-scoped
make compaction-enable  [TASK=...]
make compaction-status  [TASK=...]      # prints the resolved receipt

# CLI form (identical effect).
mcp-workbay-handoff compaction --operation disable [--task-ref WORKBAY-99]
mcp-workbay-handoff compaction --operation enable  [--task-ref WORKBAY-99]
mcp-workbay-handoff compaction --operation status  [--task-ref WORKBAY-99]

# MCP op form (callable from any harness; payload is a discriminated union).
compaction(operation="disable", task_ref="WORKBAY-99")
compaction(operation="enable",  task_ref="WORKBAY-99")
compaction(operation="status",  task_ref="WORKBAY-99")
```

Each call returns a `CompactionStatusReceipt`:

```json
{"disabled": true, "source": "db", "env_override": false,
 "db_row": {"scope_kind": "workspace", "task_ref": null,
            "enabled": false, "updated_at": "...", "updated_by": "..."}}
```

When `disabled=true`, the advisory envelope carries
`disabled=true, disabled_source=<env|db>` (threshold/floor logic skipped), the
Stop hook logs `compaction skipped: disabled (source=<env|db>)`, and the
dashboard's **Needs Attention** rail prints
`compaction: disabled via <source>`.

### Doctor probe timeout budgets

`mcp-workbay-handoff doctor` bounds its subprocess health probes with a
single overall deadline so a wedged probe cannot hang the health check:

- `WORKBAY_DOCTOR_DEADLINE_SECONDS` (default `30`) — overall budget shared
  across the stdio-handshake and CLI startup probes. On exhaustion the
  timed-out probe is reported as `ok=false` with a `remediation` hint
  (the doctor still exits 0 in non-strict mode unless both probes fail).
- `WORKBAY_HANDOFF_DOCTOR_STDIO_TIMEOUT` (default `20`) — per-probe ceiling
  for the stdio MCP handshake; always capped by the remaining overall deadline.
- `WORKBAY_PRIVACY_UV_BUILD_TIMEOUT_SECONDS` (default `120`) — bounds the
  `uv build` step of the shipped-artifact privacy gate
  (`scripts/check_shipped_privacy.py`); a wedged build fails the gate with a
  clear message instead of hanging.

Non-positive or unparseable values fall back to the defaults, so a bad env can
never disable a bound.

## Installation

### From PyPI (recommended)

```bash
pip install mcp-workbay-handoff
# or, as an isolated tool:
uv tool install mcp-workbay-handoff
# or, ad-hoc without installing (pin a release for reproducibility):
uvx mcp-workbay-handoff@0.11.1 --workspace-root /path/to/workspace serve-stdio
```

### From the monorepo source tree (development)

From this package root inside `workbay`:

```bash
cd packages/mcp-workbay-handoff
python -m pip install -e ".[dev]"
```

## Development

All package-local development commands are intended to run from the package root:

```bash
make lint-handoff
make fix-lint-handoff
make format-handoff
make mypy-handoff
make test-handoff
make check-handoff
```

Prefer the package-local Make targets when running checks from editors or agent sessions. Workspace-level interpreter discovery can point at a minimal environment that lacks this package's dev extras such as `pytest`, while `make test-handoff` and `make check-handoff` keep execution anchored to the package root and honor `PYTHON=/path/to/python3` overrides when you need to pin a specific interpreter.

The Makefile automatically adds a sibling `../workbay-codex-bridge/src` to `PYTHONPATH` when that checkout exists. In a fully extracted repo, that fallback is unnecessary once dependencies are installed normally.

Direct commands also work:

```bash
PYTHONPATH=src python -m ruff check src tests
PYTHONPATH=src python -m mypy src
PYTHONPATH=src python -m pytest tests -q
```

## Python API

The package can be used directly as a library — this is the primary fallback when MCP tool calls are unavailable.

**Always import from the package root** (`workbay_handoff_mcp`), never from submodules like `.config`, `.decisions`, or `.core`. Submodules are internal and may change.

```python
from pathlib import Path
from workbay_handoff_mcp import (
    RuntimeConfig,
    configure_runtime,
    get_handoff_state,
    search_handoff,
  validate_decision_id,
    record_event,
    review_findings,
    list_review_findings,
    set_handoff_state,
    update_task_status,
    get_verified_tests,
    render_handoff,
    record_file_touch,
    get_touched_files,
)

# Configure runtime before any read/write call
configure_runtime(RuntimeConfig.for_repo(Path("/path/to/workspace")))

# Read state
state = get_handoff_state(sections="identity")

# Search decisions
results = search_handoff(queries=["slice_complete"], record_types=["decision"], limit=5)

# Preflight a slice-complete decision id before writing
preflight = validate_decision_id(
  decision="codex_slice_complete_plan0004_contract-pinning-and-docs",
  decision_kind="slice_complete",
)

# List open findings
findings = list_review_findings(status="open")
```

Without installation (monorepo source tree):

```bash
PYTHONPATH=packages/mcp-workbay-handoff/src python3 -c "
from pathlib import Path
from workbay_handoff_mcp import RuntimeConfig, configure_runtime, get_handoff_state
configure_runtime(RuntimeConfig.for_repo(Path('.')))
state = get_handoff_state(sections='identity')
print(state['data']['active']['task_ref'])
"
```

### Compaction Advisory

`get_handoff_state(sections="identity")` exposes the canonical compaction
advisory at `data.compaction_advisory`, with a
mirrored boolean at `data.compaction_recommended` (key configurable via
`compaction.advisory_field` in `docs/workbay/contracts/harness-protocol.yaml`).
The same envelope is mirrored by `load_session`, and the workspace-summary
`CURRENT_TASK.json` carries it at `active.compaction_advisory` (no `data`
wrapper — `CURRENT_TASK.json` is the projection file itself, not an MCP
tool envelope).

```python
state = get_handoff_state(sections="identity")
advisory = state["data"]["compaction_advisory"]
if advisory["recommended"]:
    # Schedule a compaction; thresholds + observed totals + transcript path
    # are documented under the advisory envelope.
    ...
```

Cold-start consumers MUST read advisory state from this surface rather
than recomputing token/character totals locally — the evaluator owns
contract loading, transcript discovery, and harness detection.

## CLI Usage

Run the MCP server over stdio:

```bash
mcp-workbay-handoff --workspace-root /path/to/workspace serve-stdio
```

Run the MCP server over HTTP:

```bash
mcp-workbay-handoff --workspace-root /path/to/workspace serve-http
```

FastMCP's current HTTP defaults are:

- host: `127.0.0.1`
- port: `8000`
- path: `/mcp`

Useful CLI checks:

```bash
mcp-workbay-handoff --workspace-root /path/to/workspace doctor
mcp-workbay-handoff --workspace-root /path/to/workspace state
mcp-workbay-handoff --workspace-root /path/to/workspace validate --kind decision-id --decision codex_slice_complete_plan0004_contract-pinning-and-docs --decision-kind slice_complete
mcp-workbay-handoff --workspace-root /path/to/workspace review-findings --operation list
mcp-workbay-handoff --workspace-root /path/to/workspace handoff-close-check
```

## Slice-complete Decision IDs

Do not hard-code the slice-complete regex in client prompts or scripts. Treat `get_handoff_state(sections="identity").data.limits.write.slice_complete_decision_id` as the authoritative registry, and use `validate_decision_id(decision=..., decision_kind="slice_complete")` for side-effect-free preflight when composing ids outside `close_slice(author_tag=..., work_ref=..., slug=...)`.

Valid: `cdx_slice_complete_plan0004_contract_pinning_and_docs`

Invalid: `cdx_slice_complete_plan0004_contract-pinning-and-docs`

## Review Intake

Preferred path when `workbay-orchestrator-mcp` is available:

1. `get_latest_slice_review_packet`
2. `get_review_findings_summary` or `review_findings --operation list` as needed

Handoff-only fallback:

1. `load_session`
2. `search_handoff(queries=["slice_complete"], record_types=["decision"], limit=1)`
3. `get_verified_tests(task_ref=..., commit_sha=...)`
4. `review_findings(review={"operation":"list","status":"open"})`

Use the MCP read surfaces above before inspecting `.task-state/handoff.db` directly. Drop to raw SQLite or filesystem inspection only when the required MCP tool is unavailable or when you are debugging MCP transport or serialization failures.

Source-tree execution without installation:

```bash
PYTHONPATH=src python -m workbay_handoff_mcp --workspace-root /path/to/workspace serve-stdio
```

## Token-Efficient Usage

The v2 envelope is already compact, but callers still save the most tokens by shaping read responses deliberately:

- Prefer named `read_profile=` over hand-rolled `sections=`/`top_n_*` in routine paths. Profiles: `identity`, `hot_summary`, `review_packet`, `open_items`, `full_debug`. Profiles bundle a stable shape and surface `data.read_shape.applied_profile` for verification.
- Pair `read_profile=` with `response_budget_bytes=` in production retry loops. The Layer-2 budget planner reduces detail level, halves `top_n_*`, and drops optional sections *before* heavy rows materialize, so a budgeted call returns within a single round trip. The default policy is `auto_summary` when a budget is set; pass `budget_policy="fail"` to receive `data.read_budget.retry_with` instead of an over-budget payload.
- Use `get_handoff_state(read_profile="identity")` (or the legacy `sections="identity"`) for routine task checks instead of a full state fetch.
- Use `detail="summary"` on read surfaces such as `get_handoff_state`, `load_session`, `review_findings`, `search_handoff`, and `artifacts` when truncated text is acceptable.
- Use `top_n_*`, `limit=`, and `fields=` to cap read size instead of trimming large payloads client-side. `load_session` accepts `top_n_touched_files` (default 20, max 200) to bound the additive `touched_files` list.
- Read from the canonical `data` block — `result["data"]["active"]` etc. The legacy top-level mirror was removed in 0.3.0 and never returns.

Package-local guidance and examples live in [docs/guides/token-efficient-usage.md](docs/guides/token-efficient-usage.md).

### Wire format note (≥0.3.0)

Starting in `mcp-workbay-handoff 0.3.0`, MCP tool responses are **native JSON
objects** on the wire, not JSON strings inside `structured_content.result`.
Every handler is annotated `-> dict` and returns a real dict via
`_envelope()`; FastMCP serialises it once. If you previously did
`json.loads(handoff_tool(...))` to parse a JSON string return value, drop
the `json.loads` — the call returns a dict directly. If you previously
read `result.content[0].text` from the MCP wire payload and parsed it,
read `result.structured_content` directly instead.

The envelope **field set is unchanged** (`ok`, `schema_version`, `tool`,
`scope`, `data`, `mutation`, `artifacts`, `warnings`, `task_ref`) and
`schema_version` stays at `2`. Only the wire-encoding moved from
JSON-string-inside-JSON to a native nested object. See
[CHANGELOG.md](CHANGELOG.md) for the full migration notice.

## Client Adapter Shape

Installed console-script adapter (resolves whatever version is currently installed):

```json
{
  "name": "workbay-handoff-mcp",
  "command": "mcp-workbay-handoff",
  "args": ["--workspace-root", "/path/to/workspace", "serve-stdio"]
}
```

Pinned via uvx (recommended for consumers — reproducible across machines without a global install):

```json
{
  "name": "workbay-handoff-mcp",
  "command": "uvx",
  "args": [
    "mcp-workbay-handoff@0.11.1",
    "--workspace-root",
    "/path/to/workspace",
    "serve-stdio"
  ]
}
```

Source checkout adapter:

```json
{
  "name": "workbay-handoff-mcp",
  "command": "python3",
  "args": [
    "-m",
    "workbay_handoff_mcp",
    "--workspace-root",
    "/path/to/workspace",
    "serve-stdio"
  ],
  "cwd": "/path/to/mcp-workbay-handoff",
  "env": {
    "PYTHONPATH": "/path/to/mcp-workbay-handoff/src"
  }
}
```

## Version pinning

Consumer configs should pin the server version they were validated against so that
client-adapter contract drift is visible at config-review time rather than at
runtime. The installed console-script adapter resolves whatever release is
currently installed; the `uvx` and source-checkout variants above pin a known
release.

To verify the version of the server actually being launched:

```bash
mcp-workbay-handoff --version
# mcp-workbay-handoff 0.11.0
```

The same value is exposed as a top-level field in `run_doctor` output so
adapters can introspect it via the MCP surface:

```json
{
  "ok": true,
  "version": "0.11.0",
  "workspace_root": "/path/to/workspace",
  "...": "..."
}
```

## Tool Surface

`mcp-workbay-handoff` now exposes a single unified MCP surface.

Legacy `--tool-profile all|core|extended` inputs are still accepted for compatibility, but they all expose the same tool set:

```bash
mcp-workbay-handoff --workspace-root /path/to/workspace --tool-profile core serve-stdio
```

### Unified surface

| CLI name | MCP tool |
| --- | --- |
| `state` | `get_handoff_state` |
| `set` | `set_handoff_state` (pass `--status-only` for status-only updates that replace the legacy `update_task_status`) |
| `validate` | `validate` (`--kind decision_id\|write`; replaces the legacy `validate-decision-id` and `validate-write` subcommands) |
| `event` | `record_event` |
| `next-actions` | `next_actions` |
| `review-findings` | `review_findings` |
| `review-runs` | `review_runs` |
| `integrity-check` | `integrity_check` (`--kind working_tree\|post_merge\|close`; replaces the legacy `working-tree-integrity-check`, `post-merge-integrity-check`, and `handoff-close-check` subcommands) |
| `render-handoff` | `render_handoff` (kind=current_task/dashboard) |
| *(no CLI)* | `load_session` |
| *(no CLI)* | `close_slice` |
| `audit-decisions` | `audit_decision_ids` |
| `export` | `export_handoff_state` |
| `import` | `import_handoff_state` |
| `archive` | `archive` (`--operation archive\|gc\|get`; replaces the legacy `archive-task-state`, `tasks-gc`, and `get-archived-task` subcommands) |
| `artifacts` | `artifacts` (typed `--operation` selects `record`, `search`, `get`, or `purge` for indexed artifact rows) |
| `touched-files` | `touched_files` (`--operation record\|list`; replaces the legacy `record-file-touch` and `get-touched-files` subcommands) |
| `compaction` | `compaction` (`--operation record\|get\|get_latest`; replaces the legacy `compact-session`, `get-compaction`, and `get-latest-compaction` subcommands) |
| `handoff-search` | `search_handoff` |
| `handoff-rows` | `list_handoff_rows` |
| `get-verified-tests` | `get_verified_tests` |

`record_event` uses a typed `event` payload with `event_kind="decision" | "test_result" | "blocker"` so each variant keeps its own required fields.
`next_actions` uses `operation="list" | "add" | "update" | "complete" | "skip"`.
`review_findings` uses `operation="record" | "batch_record" | "update" | "list"`.
`review_runs` uses `operation="record" | "list" | "coverage"`.

Branch/worktree drift is surfaced through `warnings` on write responses. If you
set `WORKBAY_HANDOFF_ENFORCE_BRANCH=1`, branch mismatches against the active
task's `target_branch` fail before mutation on enforceable branches. Direct
Python callers see `BranchMismatchError`; MCP clients receive the standard v2
error envelope with `data.expected_branch` and `data.actual_branch`.

CLI-only extras (not part of MCP registry): `artifact-list`, `artifact-terms`.

Surface classes:

- `action`: mutates canonical state; do not blind-retry
- `query`: read-only inspection of canonical state; usually safe to retry
- `generator`: derived output such as close checks, markdown renders, and metrics summaries

## Troubleshooting

The fastest first check is still:

```bash
mcp-workbay-handoff --workspace-root /path/to/workspace doctor
```

If startup succeeds but calls fail, check the workspace paths first: `--workspace-root`, `--state-dir`, `--current-task-path`, and `--exports-dir` must all point at the same workspace state.
