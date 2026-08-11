# Eval Registry

## Purpose

This directory is the durable registry for WorkBay eval suites owned by
`workbay-system`. Suite definitions live in
`packages/workbay-system/config/evals/*.json`; this document explains
what those suites measure, who owns them, and how consumers should read
their outputs.

implementation note uses a project-local validation contract in
`packages/workbay-system/config/evals/schema.json` plus the in-repo
validator at `packages/workbay-system/scripts/workbay/evals/schema.py`.
The contract is intentionally small and stdlib-driven for the MVP; it
is not advertised as a full JSON Schema runtime.

Operator-facing commands are wired through the repo root Make surface:

- `make evals-list LIFECYCLE_ARGS=--json`
- `make evals-run SUITE=<suite-id> [TASK=<task-ref>] LIFECYCLE_ARGS=--json`

## Suite Taxonomy

| Suite ID | Owner | Purpose | Planned Evidence Sink |
| --- | --- | --- | --- |
| `docs-hygiene` | `workbay-system` | Detect stale planning/docs structure and durable-vs-ephemeral drift | eval ledger + handoff findings |
| `handoff-compaction-runtime` | `mcp-workbay-handoff` | Verify compaction advisory provenance, disable precedence, receipt output, dashboard wording, and context-refresh availability | eval ledger + handoff findings |
| `handoff-integrity` | `mcp-workbay-handoff` | Smoke-check task projections and required review evidence reads | eval ledger |
| `lifecycle-smoke` | `workbay-system` | Verify core lifecycle commands and smoke-level checklist-sync wiring stay runnable and coherent | eval ledger |
| `metrics-signal-quality` | `mcp-workbay-orchestrator` | Prevent heuristic ACE signals from being treated as authoritative gates | eval ledger + assessments |
| `plugin-delivery` | `workbay-system` | Validate generated plugin tree shape, marketplace references, pins, delivery-proof docs, and harness CLI availability | eval ledger |
| `workflow-checklist-integrity` | `workbay-system` | Hold detailed checklist-sync regression cases that would otherwise bloat lifecycle smoke | eval ledger |

`lifecycle-smoke` owns operator-facing reachability checks only. Detailed
checklist evidence regressions, such as sub-slice close ids mapping back to a
parent slice checklist item, belong in `workflow-checklist-integrity` so new
workflow regressions do not turn the smoke suite into a mixed bucket.

`plugin-delivery` splits static checks from harness-level checks. Static cases
validate the committed generated plugin trees, Claude/Codex marketplace pins,
and plugin distribution documentation anchors. Harness cases run only a CLI
availability smoke (`<harness> --version`) when `claude` or `codex` is on PATH;
missing CLIs are recorded as `skip` outcomes that fail the suite-level status
gate, surfacing the gap as an explicit blocker rather than a silent pass.

## Docs-Hygiene Cases

`docs-hygiene` scans durable planning and documentation surfaces for drift that
can mislead future task agents. Its current cases check for duplicate
active/proposed task refs, missing `Task Plan Status` metadata on active handoff
plans, active/proposed `Target Branch` metadata that does not match
`feature/<task-ref-lowercase>`, active epics or active/proposed task plans
missing from the repo-visible planning artifact inventory, active planning
surfaces that still depend on local stash-only planning state, stale retired
dashboard filename references, unresolved template placeholders in durable
specs, and durable docs that depend on archived task plans as authority links.

The `Target Branch` convention case is intentionally scoped to active/proposed
plans, but an active plan may keep a long-form branch when handoff state or the
local Git branch list proves that task is already running on that branch. Done,
closed, superseded, or historical plans are grandfathered so old long-form
branch names remain historical evidence instead of fresh blockers.

The stash-only planning case is scoped to epics plus active/proposed task plans.
It flags references that make local stash state the working source for future
implementation, while allowing historical mentions and explicitly triaged
remediation text that points at tracked plans, repo-visible materialization, or
message-based recovery evidence.

The planning artifact inventory case requires root/package epic paths plus
active/proposed task-plan paths to appear in `docs/planning-artifacts.md`. Done,
closed, superseded, or historical plans are not required by this guard; they can
be classified gradually as the inventory grows without blocking the active-work
discovery path. The case reports `indexed_historical_plans` for done or
historical task plans that have already been classified in the inventory.

## Extension Path

`packages/workbay-system/scripts/workbay/evals/registry.py` is the single
dispatch extension point for built-in suites. Adding a new suite should follow
one path:

1. Add the suite definition under `packages/workbay-system/config/evals/`.
2. Add or reuse an implementation callable under
  `packages/workbay-system/scripts/workbay/evals/`.
3. Register that callable in `registry.py` so `runner.py` can stay focused on
  CLI parsing, result ledger writes, dashboard refresh, and handoff mirroring.

Do not add another large `if/elif` runner branch for per-suite logic. The
runner owns orchestration; the registry owns implementation lookup.

`handoff-compaction-runtime` is fixture-driven and deterministic. Its cases
cover resolved-vs-package contract-source metadata, threshold-triggered
recommendations, disable precedence, dashboard recommendation wording, Stop-hook
receipt lines, and `load_session(include_context_refresh=True)` availability.

## Required Suite Fields

Every suite definition must include:

- `version`: local contract version. implementation note locks this to `1`.
- `suite_id`: stable identifier used by `list_suites()` and future runner receipts.
- `owner`: canonical owning package or subsystem.
- `description`: concise durable description of the suite's job.
- `command`: future runner command to execute the suite.
- `threshold`: pass/fail rule. implementation note supports `status` and `max_failures`.
- `evidence_sink`: where operators should expect results or supporting evidence.

## Result Ledger Contract

Future slices write eval results to `.task-state/evals/results.jsonl`.
The fixture at `packages/workbay-system/tests/fixtures/evals/results_v1.jsonl`
documents the initial cross-package contract expected by dashboard and
consumer-facing readers.

Each result row in the `v1` ledger contract uses these fields:

- `recorded_at`
- `suite`
- `case`
- `status`
- `commit`
- `task_ref`
- `metric_payload`
- `failure_summary`

The dashboard renderer should treat this as a versioned contract owned
by `workbay-system`, not as an ad hoc internal JSON blob.

`recorded_at` is an ISO-8601 UTC timestamp generated once per suite run
and shared by every case row appended for that run. Consumers should use
it to select the latest row per suite when rendering status summaries.

## Dashboard Receipt Contract

`make evals-run SUITE=<suite-id> LIFECYCLE_ARGS=--json` appends ledger
rows and then attempts a dashboard render through the handoff renderer.
The JSON receipt always includes `dashboard.attempted`. When
`dashboard.written` is `true`, `dashboard.path` is the refreshed
dashboard file. When `dashboard.written` is `false`, the eval result is
still recorded, but operators should run or repair the handoff dashboard
renderer before treating `DASHBOARD.txt` as current. A false dashboard
write is an observability warning, not a suite-status override.

## Consumer Adoption

Consumer repos should add their own suite-definition files locally while
reusing the same field contract and loader behavior. `workbay-system`
owns the registry format; product-specific suites remain consumer-owned.

A minimal consumer layout mirrors this package without copying its task
ids:

```text
config/evals/<suite-id>.json
docs/evals/registry.md
.task-state/evals/results.jsonl
```

The suite JSON should follow the same fields as the built-in examples:

```json
{
  "version": 1,
  "suite_id": "product-smoke",
  "owner": "consumer-repo",
  "description": "Run the consumer-owned product smoke checks.",
  "command": "make product-smoke LIFECYCLE_ARGS=--json",
  "threshold": {"kind": "status", "required_status": "pass"},
  "evidence_sink": ".task-state/evals/results.jsonl",
  "mirror_failures_to_handoff": true,
  "tags": ["consumer-readiness"]
}
```

Consumers can either vendor a tiny local runner that reads this contract
or call the `workbay-system` runner from their installed workflow bundle
once the bundle exposes that command. In both cases, commands in suite
definitions remain consumer-owned and should write only the shared JSONL
result shape plus optional handoff findings.

## Consumer Readiness Map

Consumer repos may define local eval suites that reuse the same registry
contract while pointing at repo-owned commands and dashboards. These remain
consumer-owned rather than part of the default workbay-system distribution.

Candidate inputs often include:

- contract regression harness metrics
- product/dashboard telemetry checks
- observability and tech-debt hygiene docs
- archived evaluation artifacts that can become durable suite references

The follow-up pattern is to add consumer-local suite definitions that reuse
the same registry contract while pointing at consumer-owned commands and
dashboards. A concrete consumer inventory might look like this:

| Candidate suite | Consumer source anchors | Follow-up scope |
| --- | --- | --- |
| API contract regression | `packages/shared-contracts/example-response.golden.json`, `packages/shared-contracts/schemas/example-response.schema.json`, `scripts/test_check_shared_contract_fixtures.py` | Add a contract suite that validates golden payloads against schemas and records failures in the consumer eval ledger. |
| Service latency/observability | `docs/scopes/example-latency-scope.md`, `scripts/prod-smoke.sh`, `scripts/test_prod_smoke.py` | Add a service-smoke suite for the consumer-owned endpoint and latency/diagnostic expectations once the consumer repo accepts that follow-up. |
| Product media metrics | `apps/example/src/media/writer.py`, `apps/example/tests/test_media_metrics.py`, `packages/shared-contracts/example-media-response.golden.json` | Add a product suite that checks media metric propagation into debug payloads using consumer-owned tests. |
| agentic observability | `docs/workbay/consumer-setup.md`, `scripts/test_handoff_review_run.py`, `packages/workbay-codex-bridge/tests/test_bridge.py` | Add an agentic workflow suite that confirms handoff/dashboard writes and structured-turn telemetry still work in the consumer repo. |

The minimal consumer follow-up task is: add `config/evals/` and
`docs/evals/registry.md`, wire two initial suites (`contract-regression`
and `agentic-observability`), and project latest consumer eval failures
into that repo's handoff dashboard without changing this monorepo's
internal task references.
