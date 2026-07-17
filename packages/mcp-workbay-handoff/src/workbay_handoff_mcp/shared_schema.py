"""shared_schema.py — Handoff database schema, migrations, and connection bootstrap.

Extracted from _shared.py (implementation note of internal, task plan internal-shared-module-extraction-task-plan).

Ownership:
- Ledger-owned DDL: handoff_state, decisions, blockers, next_actions, verified_tests,
  test_traces, review_findings, task_archives, review_runs, FTS virtual tables,
  triggers, indexes.
- Orchestration-owned DDL (currently bootstrapped here because internal moved the Python
  orchestration code but did not relocate the DDL):
  worktree_lanes, worker_reports, lane_messages, plan_cursors, turn_metrics.
    TODO(internal-followon): Move orchestration-owned DDL to mcp-workbay-orchestrator bootstrap.
"""

from __future__ import annotations

import logging
import os
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager

from .runtime import get_runtime_config

_log = logging.getLogger("workbay_handoff_mcp")


class SchemaVersionMismatchError(RuntimeError):
    """Typed refusal when DB ``user_version`` disagrees with this package ([OBS-08]).

    Live migration under a running server produced undefined behavior (T15).
    After bootstrap, an exact match with ``HANDOFF_SCHEMA_VERSION`` is required;
    mismatches name both versions and the remedy instead of proceeding.
    """

    error_code = "schema_version_mismatch"

    def __init__(self, db_version: int, package_version: int) -> None:
        self.db_version = int(db_version)
        self.package_version = int(package_version)
        if self.db_version < self.package_version:
            remedy = (
                "run the handoff migrator / `mcp-workbay-handoff init-state` "
                "(or reopen after upgrade) so the DB is migrated, then restart the server"
            )
        else:
            remedy = (
                "restart the server with a package whose HANDOFF_SCHEMA_VERSION "
                "matches the DB (or restore a compatible handoff.db); a newer DB "
                "must not be served by an older package"
            )
        self.remedy = remedy
        super().__init__(
            f"schema_version_mismatch: db user_version={self.db_version} "
            f"package HANDOFF_SCHEMA_VERSION={self.package_version}; remedy: {remedy}"
        )

    def as_data(self) -> dict[str, object]:
        return {
            "error": str(self),
            "error_code": self.error_code,
            "db_version": self.db_version,
            "package_version": self.package_version,
            "remedy": self.remedy,
        }


# Schema version sentinel that gates the warm-start migration path.
#
# !!! MANDATORY MAINTENANCE RULE !!!
# Whenever you add a new migration step to _apply_handoff_migrations() (e.g.
# an `ALTER TABLE ... ADD COLUMN ...`), you MUST bump this integer in the
# same commit. Failure to bump it is a SILENT bug: the new migration will
# never run on any database that was bootstrapped under the previous
# version, because `_handoff_schema_bootstrapped()` short-circuits as soon
# as `PRAGMA user_version >= HANDOFF_SCHEMA_VERSION`.
#
# If the new column is referenced unconditionally in INSERT/UPDATE statements,
# you MUST also register it in `_HANDOFF_REQUIRED_COLUMNS` in the same
# commit. Otherwise a same-version incomplete stamp can bypass self-heal.
#
# How the bump propagates the migration:
#   1. _get_db_connection() opens the DB.
#   2. _handoff_schema_bootstrapped() reads PRAGMA user_version. If it is
#      strictly less than HANDOFF_SCHEMA_VERSION, the function returns False
#      even though the tables already exist.
#   3. The bootstrap branch in _open_db_connection() then calls
#      `_bootstrap_handoff_schema(conn)`, which — inside one
#      `BEGIN IMMEDIATE` transaction — re-applies HANDOFF_SCHEMA_SQL
#      statement-by-statement (safe — every CREATE uses `IF NOT EXISTS`),
#      runs `_apply_handoff_migrations(conn)` (idempotent — column adds via
#      `_add_column_if_missing`, other steps use `if not _has_column(...)` /
#      `IF NOT EXISTS`), writes the new user_version as the last statement,
#      and COMMITs. On lock contention the transaction rolls back and the
#      version stays unstamped so the next open retries (implementation note D1).
#
# Regression coverage for this rule lives in
# tests/test_schema_migrations.py — see test_warm_start_migration_runs_when_version_bumped.
#
# History:
#   v1 — initial schema
#   v2 — first wave of column additions (lane_id, model/model_label, etc.)
#   v3 — adds handoff_state.target_worktree_path (originally landed without
#        a version bump, which silently broke `set_handoff_state` on every
#        already-bootstrapped DB until internal fixed it).
#   v4 — adds touched_files task-level file-touch ledger.
#   v5 — re-keys handoff_state by task_ref while retaining id=1 as the
#        current-task sentinel so multiple active task rows can coexist.
#   v6 — adds test_traces for raw verification output archival.
#   v7 — adds handoff_state.task_plan_path so active task plans are
#        first-class structured metadata (repo-relative path, resolved
#        against target_worktree_path at read time) instead of being
#        inferred from freeform `focus` prose. Enables root-visible
#        task-plan discovery without switching the root worktree.
#   v8 — adds session_compactions as the durable cross-harness compaction
#        ledger for structured session summaries.
#   v9 — adds repo_instances plus terminal_guard_events as the durable
#        terminal telemetry ledger foundation.
#   v10 — adds compaction_settings (internal) as the durable runtime
#         disable store for the internal custom-compaction surface. One row
#         per (scope_kind, task_ref); the workspace-default row is the
#         singleton with task_ref NULL, enforced via the unique index on
#         (scope_kind, COALESCE(task_ref,'')).
#   v11 — internal: adds the two-anchor finding lifecycle columns to
#         review_findings (resolved_on_branch_at_commit / _ref / _at_ts
#         and integrated_at_commit / _ref / _at_ts), expands the status
#         CHECK constraint to permit 'resolved_on_branch' and 'integrated',
#         and adds handoff_state.last_observed_integration_sha to debounce
#         the opportunistic integrate-reconcile trigger.
#   v12 — internal (implementation note): adds agent_errors as the durable
#         agent-side error telemetry ledger (error_class taxonomy,
#         redacted summary/detail, package provenance, occurrence_count
#         dedup counter keyed by repo_instance_id like
#         terminal_guard_events).
#   v13 — adds session_compactions.tokens_saved_estimate (nullable) for
#         durable compaction savings telemetry (implementation note).
#   v14 — adds session_reinjections for durable reinject firing telemetry
#         (internal).
#   v15 — adds concept_embeddings (durable per-concept embedding store:
#         canonical little-endian float32 vector BLOBs keyed by
#         (entity_kind, entity_id), with a text_hash re-embed gate) and
#         session_compactions.anchor_vector (persisted transcript-anchor
#         vector for semantic reinjection; writer/reader lands in implementation note)
#         for internal semantic-relevant compaction reinjection.
#   v16 — expands review_findings.status CHECK to permit 'superseded' for
#         merge-managed source-row retirement (review-parallel upstream fix).
#   v17 — adds decisions.slice_number for structured slice-complete binding
#         (internal).
#   v18 — adds session_reinjections.semantic_detail_json for canonical
#         semantic reinjection telemetry (internal).
#   v19 — dedupes duplicate decisions rows, adds unique index on
#         (task_ref, decision, session), and enables idempotent
#         record_decision via ON CONFLICT DO NOTHING (implementation note D2).
#   v20 — adds projection_event_dedupe for stable event-id insert-or-noop
#         replay semantics across projection-backed MCP write surfaces.
#   v21 — adds orientation_reads for read-side orientation telemetry.
#   v22 — adds nullable harness provenance to decisions and review_findings.
#   v23 — adds nullable typed terminal outcome to worker_reports.
#   v25 — adds nullable worktree_lanes.test_cmd (structured self-verify command,
#         implementation note). Bumped so already-stamped v24 DBs re-run the add-column
#         migration; test_cmd is also registered in _HANDOFF_REQUIRED_COLUMNS as
#         the warm-start net (belt-and-suspenders, mirroring the lane_messages
#         dispatch_id/payload_json retrofit).
#   v26 — expands turn_metrics.usage_source CHECK to permit 'grok_context_delta'
#         (internal). SQLite cannot ALTER a CHECK in place, so the
#         migration rebuilds turn_metrics (create-new + copy + swap). Without
#         this version bump, already-stamped v25 DBs would silently skip the
#         rebuild (PR-0094-08 / project_handoff_migrator_gate_unreachable).
#   v27 — internal hygiene-residue CHECK expansions (table rebuilds):
#         worker_reports.outcome gains 'no_actionable_work'/'no_work' (reconcile
#         with WORKER_REPORT_OUTCOMES allowlist; expand-not-contract [DATA-03]);
#         plan_cursors.state gains 'expired' (implementation note reclaimer); worktree_lanes.status
#         gains 'closed_stale' (implementation note reclaimer). Single bump so Slices 2/3 need
#         no further version change. Unregistered migrations are unreachable on
#         already-stamped DBs (project_handoff_migrator_gate_unreachable).
#   v28 — implementation note R8: register agent_errors.workbay_release in
#         _HANDOFF_REQUIRED_COLUMNS + _migrate_add_column_extensions so
#         pre-rebrand DBs retrofit the renamed column (inserts were dark).
#   v29 — implementation note S1: nullable turn_metrics.duration_seconds REAL (wall-clock
#         around adapter execute). Registered via _add_column_if_missing inside
#         _migrate_add_column_extensions + _HANDOFF_REQUIRED_COLUMNS so
#         already-stamped v28 DBs re-run the add-column path.
HANDOFF_SCHEMA_VERSION = 29

# Couples the schema constant to the distribution version so a stale-schema build
# cannot silently wear a fresh version label — the copy-editable / uv-cache skew
# that internal closes ([DATA-03]). Every cache/dist-info/
# pin path keys on the package *version*; if two schemas can share one version
# label, a version-keyed cache can serve stale-schema content indistinguishable
# from fresh.
#
# !!! MANDATORY MAINTENANCE RULE (paired with the HANDOFF_SCHEMA_VERSION bump) !!!
# Whenever you bump HANDOFF_SCHEMA_VERSION, append `(new_schema,
# introducing_package_version)` here AND bump `version` in
# packages/mcp-workbay-handoff/pyproject.toml to that (strictly greater) value in
# the SAME commit. tests/test_schema_version_coupling.py enforces: the registry
# is strictly increasing in both fields, its latest schema equals
# HANDOFF_SCHEMA_VERSION, and the packaged version is >= the current schema's
# introducing version. Version-only releases (schema unchanged) need no edit here.
# This is a source-time bump-discipline forcing function, not a global
# version->schema uniqueness proof; the runtime backstop for a mislabeled build
# is assert_boot_schema_compatible (refuses on DB/package schema mismatch).
SCHEMA_VERSION_PACKAGE_INTRODUCED: tuple[tuple[int, str], ...] = (
    (29, "0.2.9"),
)

_HANDOFF_REQUIRED_TABLES = frozenset(
    {
        "handoff_state",
        "decisions",
        "blockers",
        "next_actions",
        "verified_tests",
        "projection_event_dedupe",
        "test_traces",
        "touched_files",
        "task_archives",
        "review_findings",
        "worktree_lanes",
        "worker_reports",
        "lane_messages",
        "plan_cursors",
        "session_compactions",
        "session_reinjections",
        "concept_embeddings",
        "compaction_settings",
        "repo_instances",
        "terminal_guard_events",
        "agent_errors",
        "turn_metrics",
        "orientation_reads",
    }
)
_HANDOFF_REQUIRED_COLUMNS: dict[str, frozenset[str]] = {
    "decisions": frozenset({"slice_number", "changed_files_json", "harness"}),
    "review_findings": frozenset({"harness"}),
    "worker_reports": frozenset({"outcome"}),
    # dispatch_id/payload_json were added to lane_messages at commit 19c0f739
    # without registering them here or bumping HANDOFF_SCHEMA_VERSION, so the
    # warm-start net could not re-add them on already-stamped DBs. Registered so
    # a stamped-current DB missing them re-bootstraps (internal).
    "lane_messages": frozenset({"dispatch_id", "payload_json"}),
    # test_cmd was added to worktree_lanes at HANDOFF_SCHEMA_VERSION=25 (implementation note).
    # Registered so a DB already stamped at 24 (missing test_cmd) re-bootstraps and
    # re-adds the column via _migrate_add_column_extensions — same trap/fix as the
    # lane_messages retrofit above.
    "worktree_lanes": frozenset({"test_cmd"}),
    # implementation note R8: workbay_release is in the agent_errors CREATE DDL (fresh DBs
    # are fine) but was never registered here, so an already-stamped pre-rebrand
    # DB — whose agent_errors table predates the rename and carries the old
    # workstate_release column instead — never retrofits workbay_release through  brand-check: allow
    # the warm-start net. Error telemetry then goes dark exactly during incidents
    # (agent_errors inserts target workbay_release). Registering it flags the
    # missing column so _migrate_add_column_extensions ADD COLUMNs it (the stale
    # workstate_release column is left in place, harmless).  brand-check: allow
    "agent_errors": frozenset({"workbay_release"}),
    # implementation note S1: duration_seconds is in CREATE DDL (fresh DBs) and added via
    # _migrate_add_column_extensions; register so a stamped-current DB missing
    # the column re-bootstraps (same trap as worktree_lanes.test_cmd).
    "turn_metrics": frozenset({"duration_seconds"}),
    "handoff_state": frozenset(
        {
            "focus",
            "target_branch",
            "target_worktree_path",
            "task_plan_path",
            "last_observed_integration_sha",
        }
    ),
}
_HANDOFF_REQUIRED_FTS_TABLES = frozenset(
    {"decisions_fts", "findings_fts", "blockers_fts", "actions_fts", "verified_tests_fts"}
)
_HANDOFF_REQUIRED_FTS_TRIGGERS = frozenset(
    {
        "decisions_fts_insert",
        "decisions_fts_update",
        "decisions_fts_delete",
        "findings_fts_insert",
        "findings_fts_update",
        "findings_fts_delete",
        "blockers_fts_insert",
        "blockers_fts_update",
        "blockers_fts_delete",
        "actions_fts_insert",
        "actions_fts_update",
        "actions_fts_delete",
        "verified_tests_fts_insert",
        "verified_tests_fts_update",
        "verified_tests_fts_delete",
    }
)

# ---------------------------------------------------------------------------
# DDL — schema SQL
# ---------------------------------------------------------------------------

HANDOFF_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS handoff_state (
    id                   INTEGER UNIQUE CHECK (id IS NULL OR id = 1),
    task_ref             TEXT PRIMARY KEY,
    objective            TEXT NOT NULL,
    focus                TEXT,
    status               TEXT NOT NULL DEFAULT 'in_progress'
                         CHECK (status IN ('in_progress', 'blocked', 'review', 'done')),
    target_branch        TEXT,
    target_worktree_path TEXT,
    task_plan_path       TEXT,
    last_observed_integration_sha TEXT,
    revision             INTEGER NOT NULL DEFAULT 0,
    updated_at           TEXT NOT NULL DEFAULT (datetime('now')),
    updated_by           TEXT,
    updated_branch       TEXT,
    updated_commit_sha   TEXT
);

CREATE TABLE IF NOT EXISTS decisions (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    task_ref      TEXT NOT NULL,
    lane_id       TEXT,
    session       TEXT NOT NULL,
    decision      TEXT NOT NULL,
    rationale     TEXT,
    agent         TEXT,
    harness       TEXT,
    model         TEXT,
    model_label   TEXT,
    reasoning_level TEXT,
    input_tokens  INTEGER,
    output_tokens INTEGER,
    total_tokens  INTEGER,
    changed_files_json TEXT NOT NULL DEFAULT '[]',
    slice_number  INTEGER,
    branch        TEXT,
    commit_sha    TEXT,
    created_at    TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS blockers (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    task_ref      TEXT NOT NULL,
    lane_id       TEXT,
    description   TEXT NOT NULL,
    status        TEXT NOT NULL DEFAULT 'open'
                  CHECK (status IN ('open', 'resolved')),
    agent         TEXT,
    branch        TEXT,
    commit_sha    TEXT,
    resolved_at   TEXT,
    created_at    TEXT NOT NULL DEFAULT (datetime('now')),
    CHECK (
        (status = 'open' AND resolved_at IS NULL)
        OR (status = 'resolved' AND resolved_at IS NOT NULL)
    )
);

CREATE TABLE IF NOT EXISTS next_actions (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    task_ref      TEXT NOT NULL,
    lane_id       TEXT,
    action        TEXT NOT NULL,
    priority      INTEGER NOT NULL DEFAULT 100,
    status        TEXT NOT NULL DEFAULT 'pending'
                  CHECK (status IN ('pending', 'done', 'skipped')),
    agent         TEXT,
    branch        TEXT,
    commit_sha    TEXT,
    created_at    TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at    TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS verified_tests (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    task_ref      TEXT NOT NULL,
    lane_id       TEXT,
    command       TEXT NOT NULL,
    passed        INTEGER NOT NULL CHECK (passed IN (0, 1)),
    exit_code     INTEGER,
    result        TEXT,
    session       TEXT NOT NULL,
    agent         TEXT,
    branch        TEXT,
    commit_sha    TEXT,
    verified_at   TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS projection_event_dedupe (
    event_id      TEXT PRIMARY KEY,
    tool_name     TEXT NOT NULL,
    target_table  TEXT NOT NULL,
    target_id     INTEGER,
    task_ref      TEXT,
    created_at    TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS test_traces (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    verified_test_id INTEGER NOT NULL,
    task_ref         TEXT NOT NULL,
    trace_order      INTEGER NOT NULL DEFAULT 0,
    trace            TEXT NOT NULL,
    created_at       TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS touched_files (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    task_ref      TEXT NOT NULL,
    file_path     TEXT NOT NULL,
    change_kind   TEXT NOT NULL CHECK (change_kind IN ('edit', 'add', 'delete')),
    session       TEXT,
    commit_sha    TEXT,
    lane_id       TEXT,
    agent         TEXT,
    branch        TEXT,
    touched_at    TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS task_archives (
    task_ref       TEXT PRIMARY KEY,
    archived_at    TEXT NOT NULL DEFAULT (datetime('now')),
    archived_by    TEXT,
    archived_branch TEXT,
    archived_commit_sha TEXT,
    notes          TEXT,
    snapshot_json  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS review_findings (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    task_ref      TEXT NOT NULL,
    lane_id       TEXT,
    finding_id    TEXT NOT NULL,
    severity      TEXT NOT NULL CHECK (severity IN ('high', 'medium', 'low')),
    file_path     TEXT NOT NULL,
    line_start    INTEGER,
    line_end      INTEGER,
    description   TEXT NOT NULL,
    fix           TEXT,
    status        TEXT NOT NULL DEFAULT 'open'
                  CHECK (status IN ('open', 'fixed', 'wontfix', 'deferred', 'resolved_on_branch', 'integrated', 'superseded')),
    review_mode   TEXT
                  CHECK (review_mode IN ('branch', 'release_audit', 'planning') OR review_mode IS NULL),
    review_run_id TEXT,
    session       TEXT NOT NULL,
    agent         TEXT,
    harness       TEXT,
    branch        TEXT,
    commit_sha    TEXT,
    resolution_notes TEXT,
    reopen_count  INTEGER NOT NULL DEFAULT 0,
    last_reopen_reason TEXT,
    last_reopened_at TEXT,
    resolved_at   TEXT,
    verification_evidence TEXT,
    merged_from_json TEXT,
    resolved_on_branch_at_commit TEXT,
    resolved_on_branch_ref       TEXT,
    resolved_on_branch_at_ts     TEXT,
    integrated_at_commit         TEXT,
    integrated_at_ref            TEXT,
    integrated_at_ts             TEXT,
    created_at    TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at    TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Orchestration-owned tables.
-- TODO(internal-followon): Move these to mcp-workbay-orchestrator bootstrap once that
-- package owns its own DB connection setup (tracked in internal follow-on work).

CREATE TABLE IF NOT EXISTS worktree_lanes (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    task_ref      TEXT NOT NULL,
    lane_id       TEXT NOT NULL,
    title         TEXT,
    objective     TEXT,
    worktree_path TEXT NOT NULL,
    branch        TEXT NOT NULL,
    owner_agent   TEXT,
    model         TEXT,
    backend       TEXT,
    reasoning_effort TEXT,
    status        TEXT NOT NULL DEFAULT 'planned'
                  CHECK (status IN ('planned', 'active', 'blocked', 'review', 'merged', 'closed', 'closed_stale')),
    notes         TEXT,
    created_at    TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at    TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(task_ref, lane_id)
);

CREATE TABLE IF NOT EXISTS worker_reports (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    task_ref          TEXT NOT NULL,
    lane_id           TEXT NOT NULL,
    session           TEXT NOT NULL,
    summary           TEXT NOT NULL,
    changed_files_json TEXT NOT NULL DEFAULT '[]',
    test_commands_json TEXT NOT NULL DEFAULT '[]',
    blockers_json      TEXT NOT NULL DEFAULT '[]',
    merge_ready       INTEGER NOT NULL DEFAULT 0 CHECK (merge_ready IN (0, 1)),
    status            TEXT NOT NULL DEFAULT 'submitted'
                      CHECK (status IN ('submitted', 'acknowledged', 'superseded')),
    outcome           TEXT CHECK (outcome IS NULL OR outcome IN (
                          'finished', 'failed', 'exhausted', 'stopped',
                          'no_actionable_work', 'no_work'
                      )),
    agent             TEXT,
    branch            TEXT,
    commit_sha        TEXT,
    created_at        TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS lane_messages (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    task_ref      TEXT NOT NULL,
    lane_id       TEXT NOT NULL,
    session       TEXT NOT NULL,
    direction     TEXT NOT NULL
                  CHECK (direction IN ('orchestrator_to_worker', 'worker_to_orchestrator')),
    subject       TEXT,
    message       TEXT NOT NULL,
    status        TEXT NOT NULL DEFAULT 'open'
                  CHECK (status IN ('open', 'acknowledged', 'closed')),
    dispatch_id   TEXT,
    payload_json  TEXT,
    agent         TEXT,
    branch        TEXT,
    commit_sha    TEXT,
    created_at    TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at    TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS plan_cursors (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    task_ref      TEXT NOT NULL,
    plan_item_id  TEXT NOT NULL,
    state         TEXT NOT NULL
                  CHECK (state IN ('dispatched', 'completed', 'skipped', 'escalated', 'expired')),
    lane_id       TEXT,
    mcp_action_id INTEGER,
    worker_message_id INTEGER,
    source_heading TEXT,
    summary       TEXT NOT NULL,
    dispatch_count INTEGER NOT NULL DEFAULT 0,
    dispatched_at TEXT,
    completed_at  TEXT,
    created_at    TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at    TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(task_ref, plan_item_id)
);

CREATE TABLE IF NOT EXISTS turn_metrics (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    task_ref      TEXT NOT NULL,
    lane_id       TEXT,
    session       TEXT NOT NULL,
    cycle         INTEGER,
    phase         TEXT NOT NULL,
    backend       TEXT NOT NULL,
    model         TEXT,
    thread_id     TEXT,
    turn_id       TEXT,
    input_tokens  INTEGER,
    output_tokens INTEGER,
    cached_input_tokens INTEGER,
    reasoning_output_tokens INTEGER,
    total_tokens  INTEGER,
    usage_source  TEXT
                  CHECK (usage_source IN ('observed', 'tokenizer_estimate', 'char_estimate', 'grok_context_delta') OR usage_source IS NULL),
    model_context_window INTEGER,
    prompt_tokens INTEGER,
    prompt_chars  INTEGER,
    prompt_token_source TEXT
                  CHECK (prompt_token_source IN ('observed', 'tokenizer_estimate', 'char_estimate') OR prompt_token_source IS NULL),
    utilization_ratio REAL,
    domain_signal_ratio REAL,
    pressure_level TEXT,
    attribution_json TEXT NOT NULL DEFAULT '{}',
    section_sizes_json TEXT NOT NULL DEFAULT '{}',
    raw_usage_json TEXT,
    duration_seconds REAL,
    created_at    TEXT NOT NULL DEFAULT (datetime('now'))
);

-- End orchestration-owned tables.

CREATE TABLE IF NOT EXISTS review_runs (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    review_run_id    TEXT NOT NULL UNIQUE,
    task_ref         TEXT,
    subject_path     TEXT NOT NULL,
    subject_kind     TEXT NOT NULL DEFAULT 'task_plan'
                     CHECK (subject_kind IN ('task_plan', 'epic', 'branch', 'adr', 'roadmap', 'other')),
    review_mode      TEXT NOT NULL
                     CHECK (review_mode IN ('branch', 'release_audit', 'planning')),
    verdict_decision TEXT,
    verdict          TEXT
                     CHECK (verdict IN ('pass', 'pass_with_findings', 'fail', 'conditional_pass') OR verdict IS NULL),
    reviewed_at      TEXT NOT NULL DEFAULT (datetime('now')),
    agent            TEXT,
    model            TEXT,
    model_label      TEXT,
    branch           TEXT,
    commit_sha       TEXT,
    session          TEXT,
    created_at       TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS session_compactions (
    compaction_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL,
    harness TEXT NOT NULL,
    task_ref TEXT NOT NULL,
    turn_range TEXT NOT NULL,
    structured_summary_json TEXT NOT NULL,
    prose_residual TEXT,
    tokens_saved_estimate INTEGER,
    anchor_vector BLOB,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS session_reinjections (
    reinjection_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL,
    harness TEXT NOT NULL,
    task_ref TEXT NOT NULL,
    compaction_id TEXT,
    source TEXT NOT NULL,
    emitted_chars INTEGER NOT NULL,
    arm TEXT,
    semantic_detail_json TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (compaction_id) REFERENCES session_compactions(compaction_id)
);

CREATE TABLE IF NOT EXISTS orientation_reads (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    tool               TEXT NOT NULL,
    task_ref           TEXT NOT NULL,
    resolution_outcome TEXT NOT NULL,
    harness            TEXT NOT NULL,
    source             TEXT,
    session            TEXT,
    read_profile       TEXT,
    created_at         TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS concept_embeddings (
    entity_kind TEXT NOT NULL,
    entity_id   TEXT NOT NULL,
    task_ref    TEXT NOT NULL,
    text_hash   TEXT NOT NULL,
    dim         INTEGER NOT NULL,
    vector      BLOB NOT NULL,
    model_id    TEXT NOT NULL,
    created_at  TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (entity_kind, entity_id)
);

CREATE TABLE IF NOT EXISTS compaction_settings (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    scope_kind  TEXT NOT NULL CHECK (scope_kind IN ('task', 'workspace')),
    task_ref    TEXT,
    enabled     INTEGER NOT NULL CHECK (enabled IN (0, 1)),
    updated_at  TEXT NOT NULL DEFAULT (datetime('now')),
    updated_by  TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_compaction_settings_scope
    ON compaction_settings(scope_kind, COALESCE(task_ref, ''));

CREATE TABLE IF NOT EXISTS repo_instances (
    repo_instance_id TEXT PRIMARY KEY,
    workspace_root   TEXT,
    git_common_dir   TEXT,
    created_at       TEXT NOT NULL DEFAULT (datetime('now')),
    last_seen_at     TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS terminal_guard_events (
    event_key        TEXT PRIMARY KEY,
    repo_instance_id TEXT NOT NULL,
    task_ref         TEXT,
    worktree_path    TEXT,
    harness          TEXT NOT NULL,
    tool_name        TEXT NOT NULL,
    decision         TEXT NOT NULL CHECK (decision IN ('ask', 'block')),
    trigger          TEXT,
    native_tool_hint TEXT,
    command_preview  TEXT NOT NULL,
    policy_version   TEXT,
    policy_source    TEXT,
    fallback_source  TEXT,
    created_at       TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (repo_instance_id) REFERENCES repo_instances(repo_instance_id)
);

CREATE TABLE IF NOT EXISTS agent_errors (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    repo_instance_id  TEXT NOT NULL,
    task_ref          TEXT,
    harness           TEXT NOT NULL,
    error_class       TEXT NOT NULL,
    summary           TEXT NOT NULL,
    detail            TEXT,
    tool_name         TEXT,
    command_preview   TEXT,
    package_name      TEXT,
    package_version   TEXT,
    workbay_release TEXT,
    occurrence_count  INTEGER NOT NULL DEFAULT 1,
    created_at        TEXT NOT NULL DEFAULT (datetime('now')),
    last_seen_at      TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (repo_instance_id) REFERENCES repo_instances(repo_instance_id)
);

CREATE INDEX IF NOT EXISTS idx_agent_errors_repo_created
    ON agent_errors(repo_instance_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_agent_errors_class_created
    ON agent_errors(error_class, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_agent_errors_dedup
    ON agent_errors(error_class, summary, task_ref, last_seen_at DESC);

CREATE INDEX IF NOT EXISTS idx_decisions_task_created
    ON decisions(task_ref, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_blockers_task_status
    ON blockers(task_ref, status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_actions_task_status_priority
    ON next_actions(task_ref, status, priority, created_at);
CREATE INDEX IF NOT EXISTS idx_tests_task_verified
    ON verified_tests(task_ref, verified_at DESC);
CREATE INDEX IF NOT EXISTS idx_projection_event_dedupe_task_created
    ON projection_event_dedupe(task_ref, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_test_traces_test_order
    ON test_traces(verified_test_id, trace_order, id);
CREATE INDEX IF NOT EXISTS idx_test_traces_task_created
    ON test_traces(task_ref, created_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_touched_files_task_touched
    ON touched_files(task_ref, touched_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_task_archives_archived_at
    ON task_archives(archived_at DESC);
CREATE INDEX IF NOT EXISTS idx_review_findings_task_status
    ON review_findings(task_ref, status, severity);
CREATE INDEX IF NOT EXISTS idx_review_findings_lane_status
    ON review_findings(lane_id, status);
CREATE INDEX IF NOT EXISTS idx_lanes_task_status
    ON worktree_lanes(task_ref, status, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_worker_reports_task_lane
    ON worker_reports(task_ref, lane_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_lane_messages_task_lane
    ON lane_messages(task_ref, lane_id, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_plan_cursors_task_state_lane
    ON plan_cursors(task_ref, state, lane_id, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_turn_metrics_task_lane_created
    ON turn_metrics(task_ref, lane_id, created_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_turn_metrics_task_backend_model
    ON turn_metrics(task_ref, backend, model, created_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_review_runs_task_reviewed
    ON review_runs(task_ref, reviewed_at DESC);
CREATE INDEX IF NOT EXISTS idx_review_runs_subject_path
    ON review_runs(subject_path, reviewed_at DESC);
CREATE INDEX IF NOT EXISTS idx_session_compactions_task_recent
    ON session_compactions(task_ref, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_session_reinjections_task_recent
    ON session_reinjections(task_ref, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_session_reinjections_compaction
    ON session_reinjections(compaction_id);
CREATE INDEX IF NOT EXISTS idx_concept_embeddings_task
    ON concept_embeddings(task_ref, entity_kind);
CREATE INDEX IF NOT EXISTS idx_repo_instances_last_seen_at
    ON repo_instances(last_seen_at DESC, repo_instance_id);
CREATE INDEX IF NOT EXISTS idx_terminal_guard_events_repo_created
    ON terminal_guard_events(repo_instance_id, created_at DESC, event_key);
CREATE INDEX IF NOT EXISTS idx_terminal_guard_events_task_created
    ON terminal_guard_events(task_ref, created_at DESC, event_key);
"""

HANDOFF_FTS_SCHEMA_SQL = """
CREATE VIRTUAL TABLE IF NOT EXISTS decisions_fts USING fts5(
    body,
    record_id UNINDEXED,
    task_ref  UNINDEXED,
    lane_id   UNINDEXED,
    tokenize='porter unicode61'
);

CREATE VIRTUAL TABLE IF NOT EXISTS findings_fts USING fts5(
    body,
    record_id UNINDEXED,
    task_ref  UNINDEXED,
    lane_id   UNINDEXED,
    status    UNINDEXED,
    tokenize='porter unicode61'
);

CREATE VIRTUAL TABLE IF NOT EXISTS blockers_fts USING fts5(
    body,
    record_id UNINDEXED,
    task_ref  UNINDEXED,
    lane_id   UNINDEXED,
    status    UNINDEXED,
    tokenize='porter unicode61'
);

CREATE VIRTUAL TABLE IF NOT EXISTS actions_fts USING fts5(
    body,
    record_id UNINDEXED,
    task_ref  UNINDEXED,
    lane_id   UNINDEXED,
    status    UNINDEXED,
    tokenize='porter unicode61'
);

CREATE VIRTUAL TABLE IF NOT EXISTS verified_tests_fts USING fts5(
    body,
    record_id UNINDEXED,
    task_ref  UNINDEXED,
    lane_id   UNINDEXED,
    tokenize='porter unicode61'
);
"""

_HANDOFF_FTS_TRIGGERS_SQL = """
-- decisions triggers
CREATE TRIGGER IF NOT EXISTS decisions_fts_insert AFTER INSERT ON decisions BEGIN
    INSERT INTO decisions_fts(rowid, body, record_id, task_ref, lane_id)
    VALUES (new.id,
            new.decision || ' ' || COALESCE(new.rationale, ''),
            new.id, new.task_ref, new.lane_id);
END;

CREATE TRIGGER IF NOT EXISTS decisions_fts_update AFTER UPDATE ON decisions BEGIN
    DELETE FROM decisions_fts WHERE rowid = old.id;
    INSERT INTO decisions_fts(rowid, body, record_id, task_ref, lane_id)
    VALUES (new.id,
            new.decision || ' ' || COALESCE(new.rationale, ''),
            new.id, new.task_ref, new.lane_id);
END;

CREATE TRIGGER IF NOT EXISTS decisions_fts_delete AFTER DELETE ON decisions BEGIN
    DELETE FROM decisions_fts WHERE rowid = old.id;
END;

-- review_findings triggers
CREATE TRIGGER IF NOT EXISTS findings_fts_insert AFTER INSERT ON review_findings BEGIN
    INSERT INTO findings_fts(rowid, body, record_id, task_ref, lane_id, status)
    VALUES (new.id,
            new.description || ' ' || COALESCE(new.fix, ''),
            new.id, new.task_ref, new.lane_id, new.status);
END;

CREATE TRIGGER IF NOT EXISTS findings_fts_update AFTER UPDATE ON review_findings BEGIN
    DELETE FROM findings_fts WHERE rowid = old.id;
    INSERT INTO findings_fts(rowid, body, record_id, task_ref, lane_id, status)
    VALUES (new.id,
            new.description || ' ' || COALESCE(new.fix, ''),
            new.id, new.task_ref, new.lane_id, new.status);
END;

CREATE TRIGGER IF NOT EXISTS findings_fts_delete AFTER DELETE ON review_findings BEGIN
    DELETE FROM findings_fts WHERE rowid = old.id;
END;

-- blockers triggers
CREATE TRIGGER IF NOT EXISTS blockers_fts_insert AFTER INSERT ON blockers BEGIN
    INSERT INTO blockers_fts(rowid, body, record_id, task_ref, lane_id, status)
    VALUES (new.id, new.description, new.id, new.task_ref, new.lane_id, new.status);
END;

CREATE TRIGGER IF NOT EXISTS blockers_fts_update AFTER UPDATE ON blockers BEGIN
    DELETE FROM blockers_fts WHERE rowid = old.id;
    INSERT INTO blockers_fts(rowid, body, record_id, task_ref, lane_id, status)
    VALUES (new.id, new.description, new.id, new.task_ref, new.lane_id, new.status);
END;

CREATE TRIGGER IF NOT EXISTS blockers_fts_delete AFTER DELETE ON blockers BEGIN
    DELETE FROM blockers_fts WHERE rowid = old.id;
END;

-- next_actions triggers
CREATE TRIGGER IF NOT EXISTS actions_fts_insert AFTER INSERT ON next_actions BEGIN
    INSERT INTO actions_fts(rowid, body, record_id, task_ref, lane_id, status)
    VALUES (new.id, new.action, new.id, new.task_ref, new.lane_id, new.status);
END;

CREATE TRIGGER IF NOT EXISTS actions_fts_update AFTER UPDATE ON next_actions BEGIN
    DELETE FROM actions_fts WHERE rowid = old.id;
    INSERT INTO actions_fts(rowid, body, record_id, task_ref, lane_id, status)
    VALUES (new.id, new.action, new.id, new.task_ref, new.lane_id, new.status);
END;

CREATE TRIGGER IF NOT EXISTS actions_fts_delete AFTER DELETE ON next_actions BEGIN
    DELETE FROM actions_fts WHERE rowid = old.id;
END;

-- verified_tests triggers
CREATE TRIGGER IF NOT EXISTS verified_tests_fts_insert AFTER INSERT ON verified_tests BEGIN
    INSERT INTO verified_tests_fts(rowid, body, record_id, task_ref, lane_id)
    VALUES (new.id,
            new.command || ' ' || COALESCE(new.result, ''),
            new.id, new.task_ref, new.lane_id);
END;

CREATE TRIGGER IF NOT EXISTS verified_tests_fts_update AFTER UPDATE ON verified_tests BEGIN
    DELETE FROM verified_tests_fts WHERE rowid = old.id;
    INSERT INTO verified_tests_fts(rowid, body, record_id, task_ref, lane_id)
    VALUES (new.id,
            new.command || ' ' || COALESCE(new.result, ''),
            new.id, new.task_ref, new.lane_id);
END;

CREATE TRIGGER IF NOT EXISTS verified_tests_fts_delete AFTER DELETE ON verified_tests BEGIN
    DELETE FROM verified_tests_fts WHERE rowid = old.id;
END;
"""

# ---------------------------------------------------------------------------
# Schema probe helpers (used only by _apply_handoff_migrations)
# ---------------------------------------------------------------------------


def _has_column(conn: sqlite3.Connection, table_name: str, column_name: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return any(str(row["name"]) == column_name for row in rows)


def _add_column_if_missing(conn: sqlite3.Connection, table_name: str, column_name: str, column_def: str) -> bool:
    """Add ``column_name`` to ``table_name`` if absent; tolerate a racing ADD.

    The ``_has_column`` guard keeps the steady-state path a no-op, but the
    check-then-ALTER is a TOCTOU window: two connections running the same
    ``v_n -> v_{n+1}`` migration concurrently — or a version-skewed pair of
    writers, e.g. a stale installed package opening a DB the in-tree code is
    bootstrapping — can both observe the column missing before either commits
    its ALTER. SQLite then raises ``OperationalError: duplicate column name``
    for the loser. That is a benign idempotency outcome (the column now
    exists), so swallow *that specific* error and let the migration continue.
    Swallowing at the per-column level is deliberate: a block-level catch
    would skip the remaining migration steps and leave ``user_version`` unset.

    Returns ``True`` when this call performed the ALTER, ``False`` when the
    column was already present or was added concurrently.
    """
    if _has_column(conn, table_name, column_name):
        return False
    try:
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_def}")
    except sqlite3.OperationalError as exc:
        if "duplicate column name" in str(exc).lower():
            return False
        raise
    return True


def _has_index(conn: sqlite3.Connection, table_name: str, index_name: str) -> bool:
    rows = conn.execute(f"PRAGMA index_list({table_name})").fetchall()
    return any(str(row["name"]) == index_name for row in rows)


def _handoff_state_uses_task_keyed_rows(conn: sqlite3.Connection) -> bool:
    rows = conn.execute("PRAGMA table_info(handoff_state)").fetchall()
    task_ref_pk = next((int(row["pk"]) for row in rows if str(row["name"]) == "task_ref"), 0)
    id_pk = next((int(row["pk"]) for row in rows if str(row["name"]) == "id"), 0)
    return task_ref_pk == 1 and id_pk == 0


def _sqlite_objects_exist(conn: sqlite3.Connection, object_type: str, names: frozenset[str]) -> bool:
    rows = conn.execute(
        f"SELECT name FROM sqlite_master WHERE type = ? AND name IN ({','.join('?' for _ in names)})",
        (object_type, *sorted(names)),
    ).fetchall()
    return {str(row["name"]) for row in rows} == names


def _required_columns_present(conn: sqlite3.Connection) -> bool:
    for table, columns in _HANDOFF_REQUIRED_COLUMNS.items():
        for column in columns:
            if not _has_column(conn, table, column):
                return False
    return True


def _handoff_schema_bootstrapped(conn: sqlite3.Connection) -> bool:
    user_version = int(conn.execute("PRAGMA user_version").fetchone()[0])
    if user_version < HANDOFF_SCHEMA_VERSION:
        return False
    if not _sqlite_objects_exist(conn, "table", _HANDOFF_REQUIRED_TABLES):
        return False
    return _required_columns_present(conn)


def _handoff_fts_bootstrapped(conn: sqlite3.Connection) -> bool:
    return _sqlite_objects_exist(conn, "table", _HANDOFF_REQUIRED_FTS_TABLES) and _sqlite_objects_exist(
        conn,
        "trigger",
        _HANDOFF_REQUIRED_FTS_TRIGGERS,
    )


# ---------------------------------------------------------------------------
# FTS bootstrap
# ---------------------------------------------------------------------------


def _backfill_handoff_fts(conn: sqlite3.Connection) -> None:
    """Populate FTS tables for rows that existed before triggers were created."""
    pairs: list[tuple[str, str, str]] = [
        (
            "decisions",
            "decisions_fts",
            "INSERT INTO decisions_fts(rowid, body, record_id, task_ref, lane_id) "
            "SELECT id, decision || ' ' || COALESCE(rationale, ''), id, task_ref, lane_id "
            "FROM decisions",
        ),
        (
            "review_findings",
            "findings_fts",
            "INSERT INTO findings_fts(rowid, body, record_id, task_ref, lane_id, status) "
            "SELECT id, description || ' ' || COALESCE(fix, ''), id, task_ref, lane_id, status "
            "FROM review_findings",
        ),
        (
            "blockers",
            "blockers_fts",
            "INSERT INTO blockers_fts(rowid, body, record_id, task_ref, lane_id, status) "
            "SELECT id, description, id, task_ref, lane_id, status FROM blockers",
        ),
        (
            "next_actions",
            "actions_fts",
            "INSERT INTO actions_fts(rowid, body, record_id, task_ref, lane_id, status) "
            "SELECT id, action, id, task_ref, lane_id, status FROM next_actions",
        ),
        (
            "verified_tests",
            "verified_tests_fts",
            "INSERT INTO verified_tests_fts(rowid, body, record_id, task_ref, lane_id) "
            "SELECT id, command || ' ' || COALESCE(result, ''), id, task_ref, lane_id FROM verified_tests",
        ),
    ]
    existing_fts = {
        row[0]
        for row in conn.execute(
            f"SELECT name FROM sqlite_master WHERE type='table' AND name IN ({','.join('?' for _ in _HANDOFF_REQUIRED_FTS_TABLES)})",
            tuple(sorted(_HANDOFF_REQUIRED_FTS_TABLES)),
        ).fetchall()
    }
    for source_table, fts_table, backfill_sql in pairs:
        if fts_table not in existing_fts:
            continue
        src_count = conn.execute(f"SELECT COUNT(*) FROM {source_table}").fetchone()[0]
        if src_count > 0:
            fts_count = conn.execute(f"SELECT COUNT(*) FROM {fts_table}").fetchone()[0]
            if fts_count == 0:
                conn.execute(backfill_sql)


def _ensure_handoff_fts(conn: sqlite3.Connection) -> None:
    """Create FTS5 virtual tables, insert/update/delete triggers, and backfill existing rows."""
    if _handoff_fts_bootstrapped(conn):
        # Existing installations can end up with empty FTS tables after manual
        # cleanup or partial recovery. Re-run the idempotent backfill so search
        # remains self-healing without requiring a schema rebuild.
        _backfill_handoff_fts(conn)
        return
    try:
        conn.execute("CREATE VIRTUAL TABLE IF NOT EXISTS _fts5_handoff_probe USING fts5(body)")
        conn.execute("DROP TABLE IF EXISTS _fts5_handoff_probe")
    except sqlite3.OperationalError:
        _log.debug("Handoff FTS5 unavailable on this SQLite build; structured search disabled.")
        return
    try:
        conn.executescript(HANDOFF_FTS_SCHEMA_SQL)
        _fts_expected = set(_HANDOFF_REQUIRED_FTS_TABLES)
        _fts_created = {
            row[0]
            for row in conn.execute(
                f"SELECT name FROM sqlite_master WHERE type='table' AND name IN ({','.join('?' for _ in _fts_expected)})",
                tuple(sorted(_fts_expected)),
            ).fetchall()
        }
        if _fts_created != _fts_expected:
            _log.warning(
                "FTS tables partially created (%s of %s); skipping trigger/backfill setup.",
                len(_fts_created),
                len(_fts_expected),
            )
            return
        conn.executescript(_HANDOFF_FTS_TRIGGERS_SQL)
        _backfill_handoff_fts(conn)
    except sqlite3.OperationalError as exc:
        errstr = str(exc).lower()
        if "locked" in errstr or "no such table" in errstr:
            _log.warning("Handoff FTS setup skipped (%s); will retry on next connection.", exc)
        elif "vtable constructor failed" in errstr:
            _log.warning("Handoff FTS5 vtable corrupt (%s); dropping and recreating FTS tables.", exc)
            for _fts_table in sorted(_HANDOFF_REQUIRED_FTS_TABLES):
                conn.execute(f"DROP TABLE IF EXISTS {_fts_table}")
            conn.executescript(HANDOFF_FTS_SCHEMA_SQL)
            conn.executescript(_HANDOFF_FTS_TRIGGERS_SQL)
            _backfill_handoff_fts(conn)
        else:
            raise


# ---------------------------------------------------------------------------
# Schema migrations
# ---------------------------------------------------------------------------


def _ensure_review_findings_unique_index(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_review_findings_task_finding_unique
        ON review_findings(task_ref, finding_id)
        """
    )


def _dedupe_review_findings(conn: sqlite3.Connection, task_ref: str | None = None) -> int:
    query = """
        SELECT task_ref, finding_id, COUNT(*) AS dup_count
        FROM review_findings
        {where_clause}
        GROUP BY task_ref, finding_id
        HAVING COUNT(*) > 1
    """
    params: tuple[object, ...] = ()
    where_clause = ""
    if task_ref is not None:
        where_clause = "WHERE task_ref = ?"
        params = (task_ref,)
    duplicate_groups = conn.execute(query.format(where_clause=where_clause), params).fetchall()
    removed_rows = 0
    for group in duplicate_groups:
        group_task_ref = str(group["task_ref"])
        group_finding_id = str(group["finding_id"])
        rows = conn.execute(
            """
            SELECT *
            FROM review_findings
            WHERE task_ref = ? AND finding_id = ?
            ORDER BY COALESCE(resolved_at, created_at) DESC, id DESC
            """,
            (group_task_ref, group_finding_id),
        ).fetchall()
        if len(rows) <= 1:
            continue
        keep_row = rows[0]
        keep_id = int(keep_row["id"])
        values_by_column = {column: [row[column] for row in rows] for column in keep_row.keys()}
        merged_created_at = min(
            [str(value) for value in values_by_column["created_at"] if isinstance(value, str) and value.strip() != ""],
            default=keep_row["created_at"],
        )
        reopen_counts = [int(value) for value in values_by_column.get("reopen_count", []) if isinstance(value, int)]
        conn.execute(
            """
            UPDATE review_findings
            SET severity = ?,
                file_path = ?,
                line_start = ?,
                line_end = ?,
                description = ?,
                fix = ?,
                status = ?,
                review_mode = ?,
                session = ?,
                agent = ?,
                branch = ?,
                commit_sha = ?,
                resolution_notes = ?,
                reopen_count = ?,
                last_reopen_reason = ?,
                last_reopened_at = ?,
                resolved_at = ?,
                verification_evidence = ?,
                created_at = ?,
                updated_at = COALESCE(updated_at, ?)
            WHERE id = ?
            """,
            (
                keep_row["severity"],
                keep_row["file_path"],
                keep_row["line_start"],
                keep_row["line_end"],
                keep_row["description"],
                keep_row["fix"],
                keep_row["status"],
                keep_row["review_mode"],
                keep_row["session"],
                keep_row["agent"],
                keep_row["branch"],
                keep_row["commit_sha"],
                keep_row["resolution_notes"],
                max(reopen_counts) if reopen_counts else 0,
                keep_row["last_reopen_reason"],
                keep_row["last_reopened_at"],
                keep_row["resolved_at"],
                keep_row["verification_evidence"],
                merged_created_at,
                merged_created_at,
                keep_id,
            ),
        )
        ids_to_delete = [int(row["id"]) for row in rows if int(row["id"]) != keep_id]
        for row_id in ids_to_delete:
            conn.execute("DELETE FROM review_findings WHERE id = ?", (row_id,))
            removed_rows += 1
    return removed_rows


def _migrate_add_audit_tables(conn: sqlite3.Connection) -> None:
    """Create audit and terminal telemetry extension tables.

    Idempotent — safe to call on a DB that already has these tables.
    """
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS session_compactions (
            compaction_id TEXT PRIMARY KEY,
            session_id TEXT NOT NULL,
            harness TEXT NOT NULL,
            task_ref TEXT NOT NULL,
            turn_range TEXT NOT NULL,
            structured_summary_json TEXT NOT NULL,
            prose_residual TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    if not _has_index(conn, "session_compactions", "idx_session_compactions_task_recent"):
        conn.execute(
            "CREATE INDEX idx_session_compactions_task_recent ON session_compactions(task_ref, created_at DESC)"
        )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS repo_instances (
            repo_instance_id TEXT PRIMARY KEY,
            workspace_root   TEXT,
            git_common_dir   TEXT,
            created_at       TEXT NOT NULL DEFAULT (datetime('now')),
            last_seen_at     TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    if not _has_index(conn, "repo_instances", "idx_repo_instances_last_seen_at"):
        conn.execute(
            "CREATE INDEX idx_repo_instances_last_seen_at ON repo_instances(last_seen_at DESC, repo_instance_id)"
        )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS terminal_guard_events (
            event_key        TEXT PRIMARY KEY,
            repo_instance_id TEXT NOT NULL,
            task_ref         TEXT,
            worktree_path    TEXT,
            harness          TEXT NOT NULL,
            tool_name        TEXT NOT NULL,
            decision         TEXT NOT NULL CHECK (decision IN ('ask', 'block')),
            trigger          TEXT,
            native_tool_hint TEXT,
            command_preview  TEXT NOT NULL,
            policy_version   TEXT,
            policy_source    TEXT,
            fallback_source  TEXT,
            created_at       TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (repo_instance_id) REFERENCES repo_instances(repo_instance_id)
        )
        """
    )
    if not _has_index(conn, "terminal_guard_events", "idx_terminal_guard_events_repo_created"):
        conn.execute(
            "CREATE INDEX idx_terminal_guard_events_repo_created "
            "ON terminal_guard_events(repo_instance_id, created_at DESC, event_key)"
        )
    if not _has_index(conn, "terminal_guard_events", "idx_terminal_guard_events_task_created"):
        conn.execute(
            "CREATE INDEX idx_terminal_guard_events_task_created "
            "ON terminal_guard_events(task_ref, created_at DESC, event_key)"
        )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS touched_files (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            task_ref      TEXT NOT NULL,
            file_path     TEXT NOT NULL,
            change_kind   TEXT NOT NULL CHECK (change_kind IN ('edit', 'add', 'delete')),
            session       TEXT,
            commit_sha    TEXT,
            lane_id       TEXT,
            agent         TEXT,
            branch        TEXT,
            touched_at    TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    if not _has_index(conn, "touched_files", "idx_touched_files_task_touched"):
        conn.execute("CREATE INDEX idx_touched_files_task_touched ON touched_files(task_ref, touched_at DESC, id DESC)")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS test_traces (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            verified_test_id INTEGER NOT NULL,
            task_ref         TEXT NOT NULL,
            trace_order      INTEGER NOT NULL DEFAULT 0,
            trace            TEXT NOT NULL,
            created_at       TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    if not _has_index(conn, "test_traces", "idx_test_traces_test_order"):
        conn.execute("CREATE INDEX idx_test_traces_test_order ON test_traces(verified_test_id, trace_order, id)")
    if not _has_index(conn, "test_traces", "idx_test_traces_task_created"):
        conn.execute("CREATE INDEX idx_test_traces_task_created ON test_traces(task_ref, created_at DESC, id DESC)")


def _migrate_add_column_extensions(conn: sqlite3.Connection) -> None:
    """Add incremental columns to core tables and backfill review_findings defaults. Idempotent."""
    for table in ("decisions", "blockers", "next_actions", "verified_tests", "review_findings"):
        _add_column_if_missing(conn, table, "lane_id", "TEXT")
    for column in ("model", "model_label", "reasoning_level"):
        _add_column_if_missing(conn, "decisions", column, "TEXT")
    _add_column_if_missing(conn, "decisions", "harness", "TEXT")
    for column in ("input_tokens", "output_tokens", "total_tokens"):
        _add_column_if_missing(conn, "decisions", column, "INTEGER")
    needs_backfill = False
    for column, column_def in [
        ("resolution_notes", "TEXT"),
        ("reopen_count", "INTEGER NOT NULL DEFAULT 0"),
        ("last_reopen_reason", "TEXT"),
        ("last_reopened_at", "TEXT"),
        ("updated_at", "TEXT"),
        ("verification_evidence", "TEXT"),
        ("review_mode", "TEXT"),
        ("review_run_id", "TEXT"),
        ("merged_from_json", "TEXT"),
        ("harness", "TEXT"),
    ]:
        if _add_column_if_missing(conn, "review_findings", column, column_def):
            needs_backfill = True
    if not needs_backfill:
        needs_backfill = (
            conn.execute(
                """
            SELECT 1
            FROM review_findings
            WHERE reopen_count IS NULL
               OR updated_at IS NULL
               OR TRIM(updated_at) = ''
            LIMIT 1
            """
            ).fetchone()
            is not None
        )
    if needs_backfill:
        conn.execute(
            """
            UPDATE review_findings
            SET reopen_count = COALESCE(reopen_count, 0),
                updated_at = COALESCE(NULLIF(TRIM(updated_at), ''), resolved_at, created_at, datetime('now'))
            """
        )
    _add_column_if_missing(conn, "lane_messages", "payload_json", "TEXT")
    _add_column_if_missing(conn, "lane_messages", "dispatch_id", "TEXT")
    # implementation note R8: retrofit workbay_release onto a pre-rebrand agent_errors table
    # (the rename left already-stamped DBs with the old workstate_release column  brand-check: allow
    # and no workbay_release, so inserts failed silently and telemetry went dark).
    _add_column_if_missing(conn, "agent_errors", "workbay_release", "TEXT")
    # implementation note S1: wall-clock duration for turn_metrics. Guard table existence:
    # on ancient DBs turn_metrics is first created later in _migrate_add_turn_metrics;
    # _add_column_if_missing would raise OperationalError: no such table.
    turn_metrics_present = (
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'turn_metrics' LIMIT 1"
        ).fetchone()
        is not None
    )
    if turn_metrics_present:
        _add_column_if_missing(conn, "turn_metrics", "duration_seconds", "REAL")
    if not _has_index(conn, "lane_messages", "idx_lane_messages_dispatch_id"):
        conn.execute(
            """
            CREATE UNIQUE INDEX idx_lane_messages_dispatch_id
            ON lane_messages(task_ref, lane_id, dispatch_id)
            WHERE dispatch_id IS NOT NULL
            """
        )
    # CHECK values must match WORKER_REPORT_OUTCOMES (lanes.py): HARM-A-006 added the
    # canonical 'no_actionable_work' (with 'no_work' retained as a legacy alias), so
    # the constraint must admit both or the daemon's no-work report violates it.
    _add_column_if_missing(
        conn,
        "worker_reports",
        "outcome",
        "TEXT CHECK (outcome IS NULL OR outcome IN "
        "('finished', 'failed', 'exhausted', 'stopped', 'no_actionable_work', 'no_work'))",
    )
    for column in ("model", "backend", "reasoning_effort", "test_cmd"):
        _add_column_if_missing(conn, "worktree_lanes", column, "TEXT")
    _add_column_if_missing(conn, "handoff_state", "focus", "TEXT")
    _add_column_if_missing(conn, "decisions", "changed_files_json", "TEXT")
    _add_column_if_missing(conn, "decisions", "slice_number", "INTEGER")
    _add_column_if_missing(conn, "handoff_state", "target_branch", "TEXT")
    _add_column_if_missing(conn, "handoff_state", "target_worktree_path", "TEXT")
    _add_column_if_missing(conn, "handoff_state", "task_plan_path", "TEXT")
    if not _has_index(conn, "review_findings", "idx_review_findings_lane_status"):
        conn.execute("CREATE INDEX idx_review_findings_lane_status ON review_findings(lane_id, status)")


def _migrate_handoff_state_schema(conn: sqlite3.Connection) -> None:
    """Convert handoff_state from the legacy id-keyed schema to task_ref PRIMARY KEY. Idempotent."""
    if _handoff_state_uses_task_keyed_rows(conn):
        return
    conn.execute("ALTER TABLE handoff_state RENAME TO handoff_state_legacy_v4")
    conn.execute(
        """
        CREATE TABLE handoff_state (
            id                   INTEGER UNIQUE CHECK (id IS NULL OR id = 1),
            task_ref             TEXT PRIMARY KEY,
            objective            TEXT NOT NULL,
            focus                TEXT,
            status               TEXT NOT NULL DEFAULT 'in_progress'
                                 CHECK (status IN ('in_progress', 'blocked', 'review', 'done')),
            target_branch        TEXT,
            target_worktree_path TEXT,
            task_plan_path       TEXT,
            revision             INTEGER NOT NULL DEFAULT 0,
            updated_at           TEXT NOT NULL DEFAULT (datetime('now')),
            updated_by           TEXT,
            updated_branch       TEXT,
            updated_commit_sha   TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO handoff_state (
            id, task_ref, objective, focus, status,
            target_branch, target_worktree_path, revision,
            updated_at, updated_by, updated_branch, updated_commit_sha
        )
        SELECT
            CASE WHEN id = 1 THEN 1 ELSE NULL END,
            task_ref,
            objective,
            focus,
            status,
            target_branch,
            target_worktree_path,
            revision,
            updated_at,
            updated_by,
            updated_branch,
            updated_commit_sha
        FROM handoff_state_legacy_v4
        """
    )
    conn.execute("DROP TABLE handoff_state_legacy_v4")


def _migrate_add_turn_metrics(conn: sqlite3.Connection) -> None:
    """Create turn_metrics table and its query indexes. Idempotent.

    TODO(internal-followon): this DDL belongs in mcp-workbay-orchestrator bootstrap.
    """
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS turn_metrics (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            task_ref      TEXT NOT NULL,
            lane_id       TEXT,
            session       TEXT NOT NULL,
            cycle         INTEGER,
            phase         TEXT NOT NULL,
            backend       TEXT NOT NULL,
            model         TEXT,
            thread_id     TEXT,
            turn_id       TEXT,
            input_tokens  INTEGER,
            output_tokens INTEGER,
            cached_input_tokens INTEGER,
            reasoning_output_tokens INTEGER,
            total_tokens  INTEGER,
            usage_source  TEXT
                          CHECK (usage_source IN ('observed', 'tokenizer_estimate', 'char_estimate', 'grok_context_delta') OR usage_source IS NULL),
            model_context_window INTEGER,
            prompt_tokens INTEGER,
            prompt_chars  INTEGER,
            prompt_token_source TEXT
                          CHECK (prompt_token_source IN ('observed', 'tokenizer_estimate', 'char_estimate') OR prompt_token_source IS NULL),
            utilization_ratio REAL,
            domain_signal_ratio REAL,
            pressure_level TEXT,
            attribution_json TEXT NOT NULL DEFAULT '{}',
            section_sizes_json TEXT NOT NULL DEFAULT '{}',
            raw_usage_json TEXT,
            duration_seconds REAL,
            created_at    TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    # Belt-and-suspenders for DBs whose turn_metrics was created before v29
    # (CREATE IF NOT EXISTS leaves an older shape untouched).
    _add_column_if_missing(conn, "turn_metrics", "duration_seconds", "REAL")
    if not _has_index(conn, "turn_metrics", "idx_turn_metrics_task_lane_created"):
        conn.execute(
            "CREATE INDEX idx_turn_metrics_task_lane_created "
            "ON turn_metrics(task_ref, lane_id, created_at DESC, id DESC)"
        )
    if not _has_index(conn, "turn_metrics", "idx_turn_metrics_task_backend_model"):
        conn.execute(
            "CREATE INDEX idx_turn_metrics_task_backend_model "
            "ON turn_metrics(task_ref, backend, model, created_at DESC, id DESC)"
        )


def _turn_metrics_usage_source_allows_grok_context_delta(conn: sqlite3.Connection) -> bool:
    """Probe whether turn_metrics.usage_source CHECK accepts grok_context_delta."""
    if not _has_column(conn, "turn_metrics", "usage_source"):
        # Table missing or not yet created — CREATE path lands the widened CHECK.
        return True
    conn.execute("SAVEPOINT grok_context_delta_usage_probe")
    try:
        conn.execute(
            """
            INSERT INTO turn_metrics (
                task_ref, session, phase, backend, total_tokens, usage_source
            ) VALUES ('__probe__', 's', 'execution', 'probe', 1, 'grok_context_delta')
            """
        )
        conn.execute("DELETE FROM turn_metrics WHERE task_ref = '__probe__' AND session = 's'")
        conn.execute("RELEASE SAVEPOINT grok_context_delta_usage_probe")
        return True
    except sqlite3.IntegrityError:
        conn.execute("ROLLBACK TO SAVEPOINT grok_context_delta_usage_probe")
        conn.execute("RELEASE SAVEPOINT grok_context_delta_usage_probe")
        return False


def _migrate_turn_metrics_grok_context_delta(conn: sqlite3.Connection) -> None:
    """v25 -> v26: expand turn_metrics.usage_source CHECK for grok_context_delta.

    Idempotent — probes whether ``grok_context_delta`` inserts succeed before
    rebuilding. SQLite cannot ALTER a CHECK in place; rebuild is create-new +
    copy + drop + rename, then re-create query indexes (PR-0094-08).
    """
    if _turn_metrics_usage_source_allows_grok_context_delta(conn):
        return

    conn.execute("ALTER TABLE turn_metrics RENAME TO turn_metrics_legacy_v25")
    conn.execute(
        """
        CREATE TABLE turn_metrics (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            task_ref      TEXT NOT NULL,
            lane_id       TEXT,
            session       TEXT NOT NULL,
            cycle         INTEGER,
            phase         TEXT NOT NULL,
            backend       TEXT NOT NULL,
            model         TEXT,
            thread_id     TEXT,
            turn_id       TEXT,
            input_tokens  INTEGER,
            output_tokens INTEGER,
            cached_input_tokens INTEGER,
            reasoning_output_tokens INTEGER,
            total_tokens  INTEGER,
            usage_source  TEXT
                          CHECK (usage_source IN ('observed', 'tokenizer_estimate', 'char_estimate', 'grok_context_delta') OR usage_source IS NULL),
            model_context_window INTEGER,
            prompt_tokens INTEGER,
            prompt_chars  INTEGER,
            prompt_token_source TEXT
                          CHECK (prompt_token_source IN ('observed', 'tokenizer_estimate', 'char_estimate') OR prompt_token_source IS NULL),
            utilization_ratio REAL,
            domain_signal_ratio REAL,
            pressure_level TEXT,
            attribution_json TEXT NOT NULL DEFAULT '{}',
            section_sizes_json TEXT NOT NULL DEFAULT '{}',
            raw_usage_json TEXT,
            duration_seconds REAL,
            created_at    TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    # Copy duration_seconds when the legacy table already has it (v29+ rebuild
    # path); otherwise NULL. PRAGMA-driven SELECT list avoids missing-column errors.
    legacy_cols = {str(row[1]) for row in conn.execute("PRAGMA table_info(turn_metrics_legacy_v25)").fetchall()}
    duration_select = "duration_seconds" if "duration_seconds" in legacy_cols else "NULL"
    conn.execute(
        f"""
        INSERT INTO turn_metrics (
            id, task_ref, lane_id, session, cycle, phase, backend, model,
            thread_id, turn_id, input_tokens, output_tokens, cached_input_tokens,
            reasoning_output_tokens, total_tokens, usage_source, model_context_window,
            prompt_tokens, prompt_chars, prompt_token_source, utilization_ratio,
            domain_signal_ratio, pressure_level, attribution_json, section_sizes_json,
            raw_usage_json, duration_seconds, created_at
        )
        SELECT
            id, task_ref, lane_id, session, cycle, phase, backend, model,
            thread_id, turn_id, input_tokens, output_tokens, cached_input_tokens,
            reasoning_output_tokens, total_tokens, usage_source, model_context_window,
            prompt_tokens, prompt_chars, prompt_token_source, utilization_ratio,
            domain_signal_ratio, pressure_level, attribution_json, section_sizes_json,
            raw_usage_json, {duration_select}, created_at
        FROM turn_metrics_legacy_v25
        """
    )
    conn.execute("DROP TABLE turn_metrics_legacy_v25")
    if not _has_index(conn, "turn_metrics", "idx_turn_metrics_task_lane_created"):
        conn.execute(
            "CREATE INDEX idx_turn_metrics_task_lane_created "
            "ON turn_metrics(task_ref, lane_id, created_at DESC, id DESC)"
        )
    if not _has_index(conn, "turn_metrics", "idx_turn_metrics_task_backend_model"):
        conn.execute(
            "CREATE INDEX idx_turn_metrics_task_backend_model "
            "ON turn_metrics(task_ref, backend, model, created_at DESC, id DESC)"
        )


def _migrate_add_compaction_settings(conn: sqlite3.Connection) -> None:
    """Create the internal compaction_settings table on warm-start.

    Idempotent — safe to call on a DB that already has the table. The
    UNIQUE index on (scope_kind, COALESCE(task_ref,'')) makes the
    workspace-default row a singleton; task-scoped rows carry a non-null
    task_ref and do not collide with the workspace row.
    """
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS compaction_settings (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            scope_kind  TEXT NOT NULL CHECK (scope_kind IN ('task', 'workspace')),
            task_ref    TEXT,
            enabled     INTEGER NOT NULL CHECK (enabled IN (0, 1)),
            updated_at  TEXT NOT NULL DEFAULT (datetime('now')),
            updated_by  TEXT
        )
        """
    )
    if not _has_index(conn, "compaction_settings", "uq_compaction_settings_scope"):
        conn.execute(
            "CREATE UNIQUE INDEX uq_compaction_settings_scope "
            "ON compaction_settings(scope_kind, COALESCE(task_ref, ''))"
        )


def _migrate_finding_lifecycle_states(conn: sqlite3.Connection) -> None:
    """internal v10 -> v11: add the two-anchor finding lifecycle columns,
    expand the review_findings.status CHECK to permit 'resolved_on_branch'
    and 'integrated', and add handoff_state.last_observed_integration_sha
    for opportunistic integrate-reconcile debouncing.

    Idempotent — probes for the new column before rebuilding the table.
    The CHECK expansion requires a table rebuild (SQLite cannot ALTER
    a CHECK constraint in place); the same rebuild lands the new
    resolved_on_branch_at_* / integrated_at_* columns.
    """
    if not _has_column(conn, "review_findings", "resolved_on_branch_at_commit"):
        conn.execute("ALTER TABLE review_findings RENAME TO review_findings_legacy_v9")
        conn.execute(
            """
            CREATE TABLE review_findings (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                task_ref      TEXT NOT NULL,
                lane_id       TEXT,
                finding_id    TEXT NOT NULL,
                severity      TEXT NOT NULL CHECK (severity IN ('high', 'medium', 'low')),
                file_path     TEXT NOT NULL,
                line_start    INTEGER,
                line_end      INTEGER,
                description   TEXT NOT NULL,
                fix           TEXT,
                status        TEXT NOT NULL DEFAULT 'open'
                              CHECK (status IN ('open', 'fixed', 'wontfix', 'deferred', 'resolved_on_branch', 'integrated', 'superseded')),
                review_mode   TEXT
                              CHECK (review_mode IN ('branch', 'release_audit', 'planning') OR review_mode IS NULL),
                review_run_id TEXT,
                session       TEXT NOT NULL,
                agent         TEXT,
                harness       TEXT,
                branch        TEXT,
                commit_sha    TEXT,
                resolution_notes TEXT,
                reopen_count  INTEGER NOT NULL DEFAULT 0,
                last_reopen_reason TEXT,
                last_reopened_at TEXT,
                resolved_at   TEXT,
                verification_evidence TEXT,
                merged_from_json TEXT,
                resolved_on_branch_at_commit TEXT,
                resolved_on_branch_ref       TEXT,
                resolved_on_branch_at_ts     TEXT,
                integrated_at_commit         TEXT,
                integrated_at_ref            TEXT,
                integrated_at_ts             TEXT,
                created_at    TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at    TEXT NOT NULL DEFAULT (datetime('now'))
            )
            """
        )
        conn.execute(
            """
            INSERT INTO review_findings (
                id, task_ref, lane_id, finding_id, severity, file_path,
                line_start, line_end, description, fix, status,
                review_mode, review_run_id, session, agent, harness, branch, commit_sha,
                resolution_notes, reopen_count, last_reopen_reason,
                last_reopened_at, resolved_at, verification_evidence,
                merged_from_json, created_at, updated_at
            )
            SELECT
                id, task_ref, lane_id, finding_id, severity, file_path,
                line_start, line_end, description, fix, status,
                review_mode, review_run_id, session, agent, harness, branch, commit_sha,
                resolution_notes, reopen_count, last_reopen_reason,
                last_reopened_at, resolved_at, verification_evidence,
                merged_from_json, created_at, updated_at
            FROM review_findings_legacy_v9
            """
        )
        conn.execute("DROP TABLE review_findings_legacy_v9")
        if not _has_index(conn, "review_findings", "idx_review_findings_lane_status"):
            conn.execute("CREATE INDEX idx_review_findings_lane_status ON review_findings(lane_id, status)")
        # Re-create the FTS triggers — they were dropped together with the legacy table.
        # NOTE: must not use ``executescript`` here — it issues an implicit
        # COMMIT, which would break the atomicity of the surrounding
        # ``_bootstrap_handoff_schema`` BEGIN IMMEDIATE transaction.
        try:
            _execute_sql_script(conn, _HANDOFF_FTS_TRIGGERS_SQL)
        except sqlite3.OperationalError:
            # No FTS — fine, triggers only matter when the virtual tables exist.
            pass
    _add_column_if_missing(conn, "handoff_state", "last_observed_integration_sha", "TEXT")


def _review_findings_status_check_allows_superseded(conn: sqlite3.Connection) -> bool:
    conn.execute("SAVEPOINT superseded_status_probe")
    try:
        conn.execute(
            """
            INSERT INTO review_findings (
                finding_id, task_ref, severity, status, file_path, description, session
            ) VALUES ('__superseded_status_probe__', '__probe__', 'low', 'superseded', 'p.py', 'probe', 's')
            """
        )
        conn.execute("DELETE FROM review_findings WHERE finding_id = '__superseded_status_probe__'")
        conn.execute("RELEASE SAVEPOINT superseded_status_probe")
        return True
    except sqlite3.IntegrityError:
        conn.execute("ROLLBACK TO SAVEPOINT superseded_status_probe")
        conn.execute("RELEASE SAVEPOINT superseded_status_probe")
        return False


def _migrate_review_findings_superseded_status(conn: sqlite3.Connection) -> None:
    """v15 -> v16: expand review_findings.status CHECK to permit superseded.

    Idempotent — probes whether superseded inserts succeed before rebuilding.
    The CHECK expansion requires a table rebuild; recreate task/lane indexes
    and findings FTS triggers afterward.
    """
    if _review_findings_status_check_allows_superseded(conn):
        return

    conn.execute("ALTER TABLE review_findings RENAME TO review_findings_legacy_superseded_v15")
    conn.execute(
        """
        CREATE TABLE review_findings (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            task_ref      TEXT NOT NULL,
            lane_id       TEXT,
            finding_id    TEXT NOT NULL,
            severity      TEXT NOT NULL CHECK (severity IN ('high', 'medium', 'low')),
            file_path     TEXT NOT NULL,
            line_start    INTEGER,
            line_end      INTEGER,
            description   TEXT NOT NULL,
            fix           TEXT,
            status        TEXT NOT NULL DEFAULT 'open'
                          CHECK (status IN ('open', 'fixed', 'wontfix', 'deferred', 'resolved_on_branch', 'integrated', 'superseded')),
            review_mode   TEXT
                          CHECK (review_mode IN ('branch', 'release_audit', 'planning') OR review_mode IS NULL),
            review_run_id TEXT,
            session       TEXT NOT NULL,
            agent         TEXT,
            harness       TEXT,
            branch        TEXT,
            commit_sha    TEXT,
            resolution_notes TEXT,
            reopen_count  INTEGER NOT NULL DEFAULT 0,
            last_reopen_reason TEXT,
            last_reopened_at TEXT,
            resolved_at   TEXT,
            verification_evidence TEXT,
            merged_from_json TEXT,
            resolved_on_branch_at_commit TEXT,
            resolved_on_branch_ref       TEXT,
            resolved_on_branch_at_ts     TEXT,
            integrated_at_commit         TEXT,
            integrated_at_ref            TEXT,
            integrated_at_ts             TEXT,
            created_at    TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at    TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    conn.execute(
        """
        INSERT INTO review_findings (
            id, task_ref, lane_id, finding_id, severity, file_path,
            line_start, line_end, description, fix, status,
            review_mode, review_run_id, session, agent, harness, branch, commit_sha,
            resolution_notes, reopen_count, last_reopen_reason,
            last_reopened_at, resolved_at, verification_evidence,
            merged_from_json, resolved_on_branch_at_commit, resolved_on_branch_ref,
            resolved_on_branch_at_ts, integrated_at_commit, integrated_at_ref,
            integrated_at_ts, created_at, updated_at
        )
        SELECT
            id, task_ref, lane_id, finding_id, severity, file_path,
            line_start, line_end, description, fix, status,
            review_mode, review_run_id, session, agent, harness, branch, commit_sha,
            resolution_notes, reopen_count, last_reopen_reason,
            last_reopened_at, resolved_at, verification_evidence,
            merged_from_json, resolved_on_branch_at_commit, resolved_on_branch_ref,
            resolved_on_branch_at_ts, integrated_at_commit, integrated_at_ref,
            integrated_at_ts, created_at, updated_at
        FROM review_findings_legacy_superseded_v15
        """
    )
    conn.execute("DROP TABLE review_findings_legacy_superseded_v15")
    if not _has_index(conn, "review_findings", "idx_review_findings_task_status"):
        conn.execute("CREATE INDEX idx_review_findings_task_status ON review_findings(task_ref, status, severity)")
    if not _has_index(conn, "review_findings", "idx_review_findings_lane_status"):
        conn.execute("CREATE INDEX idx_review_findings_lane_status ON review_findings(lane_id, status)")
    try:
        _execute_sql_script(conn, _HANDOFF_FTS_TRIGGERS_SQL)
    except sqlite3.OperationalError:
        pass


def _migrate_add_compaction_savings(conn: sqlite3.Connection) -> None:
    """Add ``tokens_saved_estimate`` to ``session_compactions`` (v12→v13)."""
    _add_column_if_missing(conn, "session_compactions", "tokens_saved_estimate", "INTEGER")


def _migrate_add_session_reinjection_semantic_detail(conn: sqlite3.Connection) -> None:
    """Add ``session_reinjections.semantic_detail_json`` (v17→v18). Idempotent."""
    _add_column_if_missing(conn, "session_reinjections", "semantic_detail_json", "TEXT")


def _migrate_add_session_reinjections(conn: sqlite3.Connection) -> None:
    """Create ``session_reinjections`` telemetry table (v13→v14). Idempotent."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS session_reinjections (
            reinjection_id TEXT PRIMARY KEY,
            session_id TEXT NOT NULL,
            harness TEXT NOT NULL,
            task_ref TEXT NOT NULL,
            compaction_id TEXT,
            source TEXT NOT NULL,
            emitted_chars INTEGER NOT NULL,
            arm TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (compaction_id) REFERENCES session_compactions(compaction_id)
        )
        """
    )
    if not _has_index(conn, "session_reinjections", "idx_session_reinjections_task_recent"):
        conn.execute(
            "CREATE INDEX idx_session_reinjections_task_recent ON session_reinjections(task_ref, created_at DESC)"
        )
    if not _has_index(conn, "session_reinjections", "idx_session_reinjections_compaction"):
        conn.execute("CREATE INDEX idx_session_reinjections_compaction ON session_reinjections(compaction_id)")


def _migrate_add_orientation_reads(conn: sqlite3.Connection) -> None:
    """Create ``orientation_reads`` telemetry table (v20→v21). Idempotent."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS orientation_reads (
            id                 INTEGER PRIMARY KEY AUTOINCREMENT,
            tool               TEXT NOT NULL,
            task_ref           TEXT NOT NULL,
            resolution_outcome TEXT NOT NULL,
            harness            TEXT NOT NULL,
            source             TEXT,
            session            TEXT,
            read_profile       TEXT,
            created_at         TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    if not _has_index(conn, "orientation_reads", "idx_orientation_reads_task_recent"):
        conn.execute("CREATE INDEX idx_orientation_reads_task_recent ON orientation_reads(task_ref, created_at DESC)")


def _migrate_add_concept_embeddings(conn: sqlite3.Connection) -> None:
    """Create the ``concept_embeddings`` durable embedding store (v14->v15). Idempotent.

    Canonical little-endian float32 vector BLOBs keyed by
    ``(entity_kind, entity_id)``; ``text_hash`` gates re-embed on text change.
    The vector BLOB is the single source of truth; any sqlite-vec/vec0 ranking
    index is deferred to internal where the ranking consumer exists.
    """
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS concept_embeddings (
            entity_kind TEXT NOT NULL,
            entity_id   TEXT NOT NULL,
            task_ref    TEXT NOT NULL,
            text_hash   TEXT NOT NULL,
            dim         INTEGER NOT NULL,
            vector      BLOB NOT NULL,
            model_id    TEXT NOT NULL,
            created_at  TEXT NOT NULL DEFAULT (datetime('now')),
            PRIMARY KEY (entity_kind, entity_id)
        )
        """
    )
    if not _has_index(conn, "concept_embeddings", "idx_concept_embeddings_task"):
        conn.execute("CREATE INDEX idx_concept_embeddings_task ON concept_embeddings(task_ref, entity_kind)")


def _migrate_add_compaction_anchor_vector(conn: sqlite3.Connection) -> None:
    """Add ``session_compactions.anchor_vector`` BLOB (v14->v15). Idempotent.

    Persisted transcript-anchor vector composed at compaction (Stop) time for
    semantic reinjection; the column writer/reader lands in internal.
    """
    _add_column_if_missing(conn, "session_compactions", "anchor_vector", "BLOB")


def _migrate_add_agent_errors(conn: sqlite3.Connection) -> None:
    """Create agent_errors table and its query indexes. Idempotent.

    v12 (internal / implementation note): durable agent-side error telemetry
    ledger, modeled on terminal_guard_events.
    """
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS agent_errors (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            repo_instance_id  TEXT NOT NULL,
            task_ref          TEXT,
            harness           TEXT NOT NULL,
            error_class       TEXT NOT NULL,
            summary           TEXT NOT NULL,
            detail            TEXT,
            tool_name         TEXT,
            command_preview   TEXT,
            package_name      TEXT,
            package_version   TEXT,
            workbay_release TEXT,
            occurrence_count  INTEGER NOT NULL DEFAULT 1,
            created_at        TEXT NOT NULL DEFAULT (datetime('now')),
            last_seen_at      TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (repo_instance_id) REFERENCES repo_instances(repo_instance_id)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_agent_errors_repo_created ON agent_errors(repo_instance_id, created_at DESC)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_agent_errors_class_created ON agent_errors(error_class, created_at DESC)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_agent_errors_dedup "
        "ON agent_errors(error_class, summary, task_ref, last_seen_at DESC)"
    )


def _split_sql_script(script: str) -> list[str]:
    """Split a SQL script into individual complete statements.

    ``sqlite3.complete_statement`` gates each candidate split so trigger
    bodies (``CREATE TRIGGER ... BEGIN ...; ...; END;``) are not broken at
    their inner semicolons. Blank and ``--`` comment-only lines are dropped.
    Limitation: a line that ends a statement must end with ``;`` (no
    trailing same-line comment after the semicolon); the schema constants
    in this module follow that convention, and the guard tests in
    tests/test_schema_migrations.py assert split integrity for both
    ``HANDOFF_SCHEMA_SQL`` and ``_HANDOFF_FTS_TRIGGERS_SQL``.
    """
    statements: list[str] = []
    buffer: list[str] = []
    for line in script.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("--"):
            continue
        buffer.append(line)
        if stripped.endswith(";") and sqlite3.complete_statement("\n".join(buffer)):
            statements.append("\n".join(buffer))
            buffer = []
    if buffer:
        statements.append("\n".join(buffer))
    return statements


def _execute_sql_script(conn: sqlite3.Connection, script: str) -> None:
    """Run a multi-statement SQL script without ``executescript``'s implicit commit."""
    for statement in _split_sql_script(script):
        conn.execute(statement)


def _worker_reports_outcome_allows_no_actionable_work(conn: sqlite3.Connection) -> bool:
    """Probe whether worker_reports.outcome CHECK accepts no_actionable_work."""
    if not _has_column(conn, "worker_reports", "outcome"):
        return True
    conn.execute("SAVEPOINT wr_outcome_no_actionable_probe")
    try:
        conn.execute(
            """
            INSERT INTO worker_reports (
                task_ref, lane_id, session, summary, outcome
            ) VALUES ('__probe__', '__probe__', 's', 'probe', 'no_actionable_work')
            """
        )
        conn.execute("DELETE FROM worker_reports WHERE task_ref = '__probe__' AND lane_id = '__probe__'")
        conn.execute("RELEASE SAVEPOINT wr_outcome_no_actionable_probe")
        return True
    except sqlite3.IntegrityError:
        conn.execute("ROLLBACK TO SAVEPOINT wr_outcome_no_actionable_probe")
        conn.execute("RELEASE SAVEPOINT wr_outcome_no_actionable_probe")
        return False


def _plan_cursors_state_allows_expired(conn: sqlite3.Connection) -> bool:
    """Probe whether plan_cursors.state CHECK accepts expired."""
    if not _sqlite_objects_exist(conn, "table", frozenset({"plan_cursors"})):
        return True
    conn.execute("SAVEPOINT plan_cursor_expired_probe")
    try:
        conn.execute(
            """
            INSERT INTO plan_cursors (
                task_ref, plan_item_id, state, summary
            ) VALUES ('__probe__', '__probe_expired__', 'expired', 'probe')
            """
        )
        conn.execute("DELETE FROM plan_cursors WHERE task_ref = '__probe__' AND plan_item_id = '__probe_expired__'")
        conn.execute("RELEASE SAVEPOINT plan_cursor_expired_probe")
        return True
    except sqlite3.IntegrityError:
        conn.execute("ROLLBACK TO SAVEPOINT plan_cursor_expired_probe")
        conn.execute("RELEASE SAVEPOINT plan_cursor_expired_probe")
        return False


def _worktree_lanes_status_allows_closed_stale(conn: sqlite3.Connection) -> bool:
    """Probe whether worktree_lanes.status CHECK accepts closed_stale."""
    if not _sqlite_objects_exist(conn, "table", frozenset({"worktree_lanes"})):
        return True
    conn.execute("SAVEPOINT worktree_lane_closed_stale_probe")
    try:
        conn.execute(
            """
            INSERT INTO worktree_lanes (
                task_ref, lane_id, worktree_path, branch, status
            ) VALUES ('__probe__', '__probe_closed_stale__', '/tmp/probe', 'probe', 'closed_stale')
            """
        )
        conn.execute("DELETE FROM worktree_lanes WHERE task_ref = '__probe__' AND lane_id = '__probe_closed_stale__'")
        conn.execute("RELEASE SAVEPOINT worktree_lane_closed_stale_probe")
        return True
    except sqlite3.IntegrityError:
        conn.execute("ROLLBACK TO SAVEPOINT worktree_lane_closed_stale_probe")
        conn.execute("RELEASE SAVEPOINT worktree_lane_closed_stale_probe")
        return False


def _rebuild_worker_reports_outcome_check(conn: sqlite3.Connection) -> None:
    """Rebuild worker_reports with expanded outcome CHECK (v26→v27)."""
    conn.execute("ALTER TABLE worker_reports RENAME TO worker_reports_legacy_v26")
    conn.execute(
        """
        CREATE TABLE worker_reports (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            task_ref          TEXT NOT NULL,
            lane_id           TEXT NOT NULL,
            session           TEXT NOT NULL,
            summary           TEXT NOT NULL,
            changed_files_json TEXT NOT NULL DEFAULT '[]',
            test_commands_json TEXT NOT NULL DEFAULT '[]',
            blockers_json      TEXT NOT NULL DEFAULT '[]',
            merge_ready       INTEGER NOT NULL DEFAULT 0 CHECK (merge_ready IN (0, 1)),
            status            TEXT NOT NULL DEFAULT 'submitted'
                              CHECK (status IN ('submitted', 'acknowledged', 'superseded')),
            outcome           TEXT CHECK (outcome IS NULL OR outcome IN (
                                  'finished', 'failed', 'exhausted', 'stopped',
                                  'no_actionable_work', 'no_work'
                              )),
            agent             TEXT,
            branch            TEXT,
            commit_sha        TEXT,
            created_at        TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    conn.execute(
        """
        INSERT INTO worker_reports (
            id, task_ref, lane_id, session, summary, changed_files_json, test_commands_json,
            blockers_json, merge_ready, status, outcome, agent, branch, commit_sha, created_at
        )
        SELECT
            id, task_ref, lane_id, session, summary, changed_files_json, test_commands_json,
            blockers_json, merge_ready, status, outcome, agent, branch, commit_sha, created_at
        FROM worker_reports_legacy_v26
        """
    )
    conn.execute("DROP TABLE worker_reports_legacy_v26")
    if not _has_index(conn, "worker_reports", "idx_worker_reports_task_lane"):
        conn.execute("CREATE INDEX idx_worker_reports_task_lane ON worker_reports(task_ref, lane_id, created_at DESC)")


def _rebuild_plan_cursors_state_check(conn: sqlite3.Connection) -> None:
    """Rebuild plan_cursors with expanded state CHECK (+expired; v26→v27)."""
    conn.execute("ALTER TABLE plan_cursors RENAME TO plan_cursors_legacy_v26")
    conn.execute(
        """
        CREATE TABLE plan_cursors (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            task_ref      TEXT NOT NULL,
            plan_item_id  TEXT NOT NULL,
            state         TEXT NOT NULL
                          CHECK (state IN ('dispatched', 'completed', 'skipped', 'escalated', 'expired')),
            lane_id       TEXT,
            mcp_action_id INTEGER,
            worker_message_id INTEGER,
            source_heading TEXT,
            summary       TEXT NOT NULL,
            dispatch_count INTEGER NOT NULL DEFAULT 0,
            dispatched_at TEXT,
            completed_at  TEXT,
            created_at    TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at    TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(task_ref, plan_item_id)
        )
        """
    )
    conn.execute(
        """
        INSERT INTO plan_cursors (
            id, task_ref, plan_item_id, state, lane_id, mcp_action_id, worker_message_id,
            source_heading, summary, dispatch_count, dispatched_at, completed_at, created_at, updated_at
        )
        SELECT
            id, task_ref, plan_item_id, state, lane_id, mcp_action_id, worker_message_id,
            source_heading, summary, dispatch_count, dispatched_at, completed_at, created_at, updated_at
        FROM plan_cursors_legacy_v26
        """
    )
    conn.execute("DROP TABLE plan_cursors_legacy_v26")
    if not _has_index(conn, "plan_cursors", "idx_plan_cursors_task_state_lane"):
        conn.execute(
            "CREATE INDEX idx_plan_cursors_task_state_lane ON plan_cursors(task_ref, state, lane_id, updated_at DESC)"
        )


def _rebuild_worktree_lanes_status_check(conn: sqlite3.Connection) -> None:
    """Rebuild worktree_lanes with expanded status CHECK (+closed_stale; v26→v27)."""
    # Preserve additive columns (model/backend/reasoning_effort/test_cmd) when present.
    has_model = _has_column(conn, "worktree_lanes", "model")
    has_backend = _has_column(conn, "worktree_lanes", "backend")
    has_effort = _has_column(conn, "worktree_lanes", "reasoning_effort")
    has_test_cmd = _has_column(conn, "worktree_lanes", "test_cmd")

    conn.execute("ALTER TABLE worktree_lanes RENAME TO worktree_lanes_legacy_v26")
    conn.execute(
        """
        CREATE TABLE worktree_lanes (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            task_ref      TEXT NOT NULL,
            lane_id       TEXT NOT NULL,
            title         TEXT,
            objective     TEXT,
            worktree_path TEXT NOT NULL,
            branch        TEXT NOT NULL,
            owner_agent   TEXT,
            model         TEXT,
            backend       TEXT,
            reasoning_effort TEXT,
            test_cmd      TEXT,
            status        TEXT NOT NULL DEFAULT 'planned'
                          CHECK (status IN (
                              'planned', 'active', 'blocked', 'review', 'merged', 'closed', 'closed_stale'
                          )),
            notes         TEXT,
            created_at    TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at    TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(task_ref, lane_id)
        )
        """
    )
    select_model = "model" if has_model else "NULL"
    select_backend = "backend" if has_backend else "NULL"
    select_effort = "reasoning_effort" if has_effort else "NULL"
    select_test_cmd = "test_cmd" if has_test_cmd else "NULL"
    conn.execute(
        f"""
        INSERT INTO worktree_lanes (
            id, task_ref, lane_id, title, objective, worktree_path, branch, owner_agent,
            model, backend, reasoning_effort, test_cmd, status, notes, created_at, updated_at
        )
        SELECT
            id, task_ref, lane_id, title, objective, worktree_path, branch, owner_agent,
            {select_model}, {select_backend}, {select_effort}, {select_test_cmd},
            status, notes, created_at, updated_at
        FROM worktree_lanes_legacy_v26
        """
    )
    conn.execute("DROP TABLE worktree_lanes_legacy_v26")
    if not _has_index(conn, "worktree_lanes", "idx_lanes_task_status"):
        conn.execute("CREATE INDEX idx_lanes_task_status ON worktree_lanes(task_ref, status, updated_at DESC)")


def _migrate_hygiene_residue_check_expansions(conn: sqlite3.Connection) -> None:
    """v26 → v27: expand CHECKs for implementation note (outcome / expired / closed_stale).

    Idempotent — probes each expanded value before rebuilding. SQLite cannot
    ALTER a CHECK in place; each shortfall rebuilds create-new + copy + swap
    (same pattern as ``_migrate_review_findings_superseded_status`` /
    ``_migrate_turn_metrics_grok_context_delta``).
    """
    if not _worker_reports_outcome_allows_no_actionable_work(conn):
        _rebuild_worker_reports_outcome_check(conn)
    if not _plan_cursors_state_allows_expired(conn):
        _rebuild_plan_cursors_state_check(conn)
    if not _worktree_lanes_status_allows_closed_stale(conn):
        _rebuild_worktree_lanes_status_check(conn)


def _apply_handoff_migrations(conn: sqlite3.Connection) -> None:
    _migrate_add_audit_tables(conn)
    _migrate_add_column_extensions(conn)
    _migrate_handoff_state_schema(conn)
    _migrate_add_turn_metrics(conn)
    _migrate_turn_metrics_grok_context_delta(conn)
    _migrate_add_compaction_settings(conn)
    _migrate_finding_lifecycle_states(conn)
    _migrate_add_agent_errors(conn)
    _migrate_add_compaction_savings(conn)
    _migrate_add_session_reinjections(conn)
    _migrate_add_orientation_reads(conn)
    _migrate_add_session_reinjection_semantic_detail(conn)
    _migrate_add_concept_embeddings(conn)
    _migrate_add_compaction_anchor_vector(conn)
    _migrate_review_findings_superseded_status(conn)
    _ensure_review_findings_unique_index(conn)
    _migrate_dedupe_decisions_and_index(conn)
    _migrate_add_projection_event_dedupe(conn)
    _migrate_hygiene_residue_check_expansions(conn)


def _migrate_dedupe_decisions_and_index(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        DELETE FROM decisions
        WHERE id NOT IN (
            SELECT MIN(id)
            FROM decisions
            GROUP BY task_ref, decision, session
        )
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_decisions_task_decision_session
        ON decisions(task_ref, decision, session)
        """
    )


def _migrate_add_projection_event_dedupe(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS projection_event_dedupe (
            event_id      TEXT PRIMARY KEY,
            tool_name     TEXT NOT NULL,
            target_table  TEXT NOT NULL,
            target_id     INTEGER,
            task_ref      TEXT,
            created_at    TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_projection_event_dedupe_task_created
        ON projection_event_dedupe(task_ref, created_at DESC)
        """
    )


def _bootstrap_handoff_schema(conn: sqlite3.Connection) -> None:
    """Apply base schema, migrations, indexes, and version stamp atomically.

    Runs inside a single ``BEGIN IMMEDIATE`` transaction and COMMITs on
    success, so raw ``_open_db_connection()`` callers cannot silently roll
    back a completed bootstrap by closing without committing (and the write
    lock is released here, not held across the caller's block).

    On database-lock contention — whether acquiring the ``BEGIN IMMEDIATE``
    write lock or during the migrations — roll back and return without
    raising (fail-open), leaving ``user_version`` unstamped so the next open
    retries the full migration set (implementation note D1). Likewise, if required
    tables or manifest columns are still missing after migrations (implementation note
    Prong 2), roll back and return unstamped rather than poisoning the
    version stamp. In that degraded case the connection is usable but the
    schema tables may be absent; ``_open_db_connection`` detects this and
    skips the FTS ensure step.
    """
    saved_isolation = conn.isolation_level
    conn.isolation_level = None
    try:
        try:
            conn.execute("BEGIN IMMEDIATE")
        except sqlite3.OperationalError as exc:
            if "locked" in str(exc).lower():
                _log.warning(
                    "DB locked acquiring bootstrap write lock -- leaving unstamped "
                    "for retry (PRAGMA busy_timeout should serialize subsequent attempts)"
                )
                return
            raise
        try:
            _execute_sql_script(conn, HANDOFF_SCHEMA_SQL)
            _apply_handoff_migrations(conn)
            if not _sqlite_objects_exist(conn, "table", _HANDOFF_REQUIRED_TABLES) or not _required_columns_present(
                conn
            ):
                try:
                    conn.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                _log.warning(
                    "handoff schema bootstrap incomplete -- required structure missing after "
                    "migrations; leaving unstamped for retry"
                )
                return
            conn.execute(f"PRAGMA user_version = {HANDOFF_SCHEMA_VERSION}")
            conn.execute("COMMIT")
        except sqlite3.OperationalError as exc:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            if "locked" in str(exc).lower():
                _log.warning(
                    "DB locked during bootstrap -- leaving unstamped for retry "
                    "(PRAGMA busy_timeout should serialize subsequent attempts)"
                )
                return
            raise
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
    finally:
        conn.isolation_level = saved_isolation


# ---------------------------------------------------------------------------
# DB connection factory
# ---------------------------------------------------------------------------


def _assert_schema_version_compatible(conn: sqlite3.Connection) -> None:
    """Refuse when PRAGMA user_version != HANDOFF_SCHEMA_VERSION ([OBS-08] T15).

    Called only after a successful bootstrap so warm-start migrations still
    run when the DB is behind. A DB *ahead* of this package (or any other
    exact mismatch post-bootstrap) fails closed with a typed error naming
    both versions and the remedy.
    """
    db_version = int(conn.execute("PRAGMA user_version").fetchone()[0])
    if db_version != HANDOFF_SCHEMA_VERSION:
        raise SchemaVersionMismatchError(db_version, HANDOFF_SCHEMA_VERSION)


def assert_boot_schema_compatible(db_path: "os.PathLike[str] | str") -> None:
    """Fail-closed BOOT gate for the serving MCP ([OBS-08], [DBG-05], [DATA-03]).

    Moves the per-call :func:`_assert_schema_version_compatible` refusal forward
    to server construction (``build_handoff_mcp``) so a stale package cannot boot
    into a "connected but every tool errors" state against a newer DB — the
    copy-editable schema-skew failure class (internal).

    Non-mutating: it issues only ``PRAGMA`` reads — it never bootstraps,
    migrates, writes, or creates the file. Open uses a non-creating
    read-write URI (``mode=rw``, not ``rwc``) so a racing unlink cannot make
    ``connect()`` materialize an empty DB. A read-write handle with a bounded
    ``busy_timeout`` is used deliberately (not ``mode=ro``): the live handoff
    DB is WAL, which a read-only open cannot always attach, and a PRAGMA read
    takes no write lock — this mirrors the sibling ``user_version`` guard in
    ``agent_errors`` ([RES-02]). Exempt — pass through to normal lazy
    bootstrap / warm-start migration / per-call guard — are an absent DB, an
    unstamped DB (``user_version`` 0), a DB BEHIND this package
    (``user_version`` < HANDOFF_SCHEMA_VERSION), and any DB that cannot be
    opened or read (locked past the timeout, corrupt / non-sqlite): those
    degrade to the existing lazy path (and log a warning) rather than
    aborting boot with a raw traceback. Only a readable DB strictly AHEAD
    (``user_version`` > HANDOFF_SCHEMA_VERSION) fails closed: the "a newer DB
    must not be served by an older package" case the server would otherwise
    hit lazily on first call.
    """
    from pathlib import Path

    path = os.fspath(db_path)
    if not os.path.exists(path):
        return
    # Non-creating RW URI open: mode=rw (not rwc) refuses to create the file
    # if a racing unlink removed it between exists() and connect(). Path.as_uri
    # percent-encodes special chars (?, #) so they cannot break the query string.
    uri = f"{Path(os.path.abspath(path)).as_uri()}?mode=rw"
    try:
        conn = sqlite3.connect(uri, uri=True, timeout=5.0)
    except sqlite3.OperationalError as exc:
        _log.warning(
            "boot schema gate skipped (degraded to lazy path): cannot open %s: %s",
            path,
            exc,
        )
        return
    try:
        conn.execute("PRAGMA busy_timeout=5000")
        db_version = int(conn.execute("PRAGMA user_version").fetchone()[0])
    except sqlite3.DatabaseError as exc:
        # Corrupt / non-sqlite file, or a lock still held past the timeout
        # (OperationalError is a DatabaseError subclass): defer to the lazy
        # bootstrap / degraded path, which self-heals, rather than abort boot.
        _log.warning(
            "boot schema gate skipped (degraded to lazy path): cannot read "
            "PRAGMA user_version from %s: %s",
            path,
            exc,
        )
        return
    finally:
        conn.close()
    if db_version > HANDOFF_SCHEMA_VERSION:
        raise SchemaVersionMismatchError(db_version, HANDOFF_SCHEMA_VERSION)


def _open_db_connection() -> sqlite3.Connection:
    """Open and bootstrap a handoff DB connection. Caller owns ``close()``.

    Most callers should use :func:`_get_db_connection` instead, which
    wraps this in a context manager that auto-commits/rolls back and
    closes the file handle. Use this raw form only when the caller
    explicitly manages the connection lifecycle (e.g. test helpers that
    return a connection across function boundaries).

    Degraded mode: when a cold bootstrap hits lock contention,
    :func:`_bootstrap_handoff_schema` fails open without stamping the
    schema. The returned connection then points at a (possibly) schema-less
    DB — queries may raise ``no such table`` — and the FTS ensure step is
    skipped so no orphan FTS tables are committed. The next open retries
    the full bootstrap and self-heals.

    T14: proactively reaps dead/stale registered writers before open, and
    re-attempts once after a lock-time reaper pass. busy_timeout=5000 is
    already set here — do not re-add.
    """
    config = get_runtime_config()
    config.state_dir.mkdir(parents=True, exist_ok=True)
    # Proactive dead/stale writer cleanup (sidecar registry; never touches a
    # live PID with a fresh heartbeat).
    try:
        from .db_writer_liveness import reap_stale_db_writers

        reap_stale_db_writers(config.db_path)
    except Exception:  # noqa: BLE001 — reaper must never block open
        _log.exception("wedged-writer reaper failed during connect (continuing)")

    conn = _connect_handoff_sqlite(config.db_path)
    try:
        return _prepare_handoff_connection(conn)
    except sqlite3.OperationalError as exc:
        if "locked" not in str(exc).lower():
            conn.close()
            raise
        # Lock path: reaper may free a dead/stale holder; retry once.
        conn.close()
        try:
            from .db_writer_liveness import reap_stale_db_writers

            reap_stale_db_writers(config.db_path)
        except Exception:  # noqa: BLE001
            _log.exception("wedged-writer reaper failed after lock (continuing)")
        conn = _connect_handoff_sqlite(config.db_path)
        try:
            return _prepare_handoff_connection(conn)
        except Exception:
            conn.close()
            raise
    except Exception:
        conn.close()
        raise


def _connect_handoff_sqlite(db_path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=5000;")
    return conn


def _prepare_handoff_connection(conn: sqlite3.Connection) -> sqlite3.Connection:
    """Bootstrap schema, enforce version match, ensure FTS. Mutates ``conn``."""
    if not _handoff_schema_bootstrapped(conn):
        _bootstrap_handoff_schema(conn)
        if not _handoff_schema_bootstrapped(conn):
            # Bootstrap fail-open: base schema is absent/unstamped. Skip the
            # FTS ensure — creating FTS tables against missing base tables
            # would commit orphans and confuse the next bootstrap retry.
            # Schema-version exact-match is deferred until a successful stamp
            # (degraded opens must still self-heal on the next attempt).
            _log.warning(
                "handoff schema bootstrap incomplete (lock contention or structure shortfall) -- "
                "returning degraded connection; FTS ensure skipped, next open retries"
            )
            return conn
    _assert_schema_version_compatible(conn)
    _ensure_handoff_fts(conn)
    return conn


@contextmanager
def _get_db_connection() -> Iterator[sqlite3.Connection]:
    # sqlite3.Connection as a context manager only commits/rolls back — it
    # does NOT close the file handle. Wrapping the connection in this
    # contextmanager guarantees close-on-exit so callers using
    # `with _get_db_connection() as conn:` do not leak file descriptors.
    # Auto-commit/rollback is preserved to match the prior raw-connection
    # context-manager semantics.
    conn = _open_db_connection()
    try:
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    finally:
        conn.close()
