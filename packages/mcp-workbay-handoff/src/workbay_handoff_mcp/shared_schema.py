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

import contextvars
import json
import logging
import os
import sqlite3
import threading
import time
import weakref
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from workbay_protocol import resolve_env_alias

from .review_findings_retention import (  # noqa: F401
    DEFAULT_REVIEW_FINDINGS_ARCHIVE_RETENTION_DAYS,
    REVIEW_FINDINGS_ARCHIVE_BATCH_LIMIT,
    REVIEW_FINDINGS_ARCHIVE_OPERATOR_VACUUM_SQL,
    TERMINAL_REVIEW_FINDING_STATUSES,
    archive_terminal_review_findings,
)
from .runtime import get_runtime_config
from .sqlite_lock_errors import is_lock_contention_error

_log = logging.getLogger("workbay_handoff_mcp")

# RESERVED-window duration budget (finding 14268 / COMBREV-R1-F5). Observation
# only: never gates, never raises into the write path ([OBS-08], [RES-06]).
# Default 2.0s: healthy handoff writes are sub-100ms; multi-second RESERVED holds
# are the freeze-class signal this complements the hold-path barrier for.
# Override with WORKBAY_HANDOFF_RESERVED_WINDOW_BUDGET_SECONDS.
_RESERVED_WINDOW_BUDGET_DEFAULT_S = 2.0
# In-process counter for tests / process diagnostics (no DB writes).
reserved_window_over_budget_count = 0


def _reserved_window_budget_seconds() -> float:
    """Return the RESERVED-window duration budget in seconds.

    Override with ``WORKBAY_HANDOFF_RESERVED_WINDOW_BUDGET_SECONDS``. A
    non-positive or unparseable value falls back to the default so a bad env
    can never disable the observation.
    """
    raw = resolve_env_alias(
        "WORKBAY_HANDOFF_RESERVED_WINDOW_BUDGET_SECONDS",
        default="",
    ).strip()
    if not raw:
        return _RESERVED_WINDOW_BUDGET_DEFAULT_S
    try:
        value = float(raw)
    except ValueError:
        return _RESERVED_WINDOW_BUDGET_DEFAULT_S
    return value if value > 0 else _RESERVED_WINDOW_BUDGET_DEFAULT_S


def _observe_reserved_window_over_budget(elapsed_s: float, budget_s: float) -> None:
    """Emit one non-gating over-budget observation (logging + in-process counter).

    Must never raise into the write path; callers wrap this in a broad except.
    """
    global reserved_window_over_budget_count
    reserved_window_over_budget_count += 1
    _log.warning(
        "RESERVED write-lock window exceeded budget: elapsed=%.3fs budget=%.3fs "
        "(observation only; does not refuse the write) [OBS-08]",
        elapsed_s,
        budget_s,
    )


class SchemaVersionMismatchError(RuntimeError):
    """Typed refusal when the DB reader floor exceeds this package ([OBS-08]).

    Live migration under a running server produced undefined behavior (T15).
    ``PRAGMA user_version`` is the minimum compatible reader version (compat
    floor), not the true schema version. After bootstrap, refuse only when the
    stamped floor is above this package's ``HANDOFF_SCHEMA_VERSION``; mismatches
    name both versions and the remedy instead of proceeding.
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
                "upgrade the package (or restart the server with a package whose "
                "HANDOFF_SCHEMA_VERSION is at least the DB floor); a newer DB "
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
# True version is recorded in ``schema_meta``; ``PRAGMA user_version`` is the
# reader floor (see MIN_COMPATIBLE_READER_VERSION).
#
# !!! MANDATORY MAINTENANCE RULE !!!
# Whenever you add a new migration step to _apply_handoff_migrations() (e.g.
# an `ALTER TABLE ... ADD COLUMN ...`), you MUST bump this integer in the
# same commit. Failure to bump it is a SILENT bug: the new migration will
# never run on any database that was bootstrapped under the previous
# version, because `_handoff_schema_bootstrapped()` short-circuits as soon
# as the true schema version (schema_meta, else user_version fallback) is
# >= HANDOFF_SCHEMA_VERSION. Also decide whether the bump is additive or
# breaking for MIN_COMPATIBLE_READER_VERSION (see that constant's rule).
#
# If the new column is referenced unconditionally in INSERT/UPDATE statements,
# you MUST also register it in `_HANDOFF_REQUIRED_COLUMNS` in the same
# commit. Otherwise a same-version incomplete stamp can bypass self-heal.
#
# How the bump propagates the migration:
#   1. _get_db_connection() opens the DB.
#   2. _handoff_schema_bootstrapped() reads the true schema version via
#      handoff_schema_version(). If it is strictly less than
#      HANDOFF_SCHEMA_VERSION, the function returns False even though the
#      tables already exist.
#   3. The bootstrap branch in _open_db_connection() then calls
#      `_bootstrap_handoff_schema(conn)`, which — inside one
#      `BEGIN IMMEDIATE` transaction — re-applies HANDOFF_SCHEMA_SQL
#      statement-by-statement (safe — every CREATE uses `IF NOT EXISTS`),
#      runs `_apply_handoff_migrations(conn)` (idempotent — column adds via
#      `_add_column_if_missing`, other steps use `if not _has_column(...)` /
#      `IF NOT EXISTS`), writes schema_meta.schema_version then stamps
#      user_version to MIN_COMPATIBLE_READER_VERSION as the last statement,
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
#   v30 — implementation note S3a: decisions.decision_origin ('agent'|'system') + backfill.
#   v31 — implementation note R3: additive worktree_lanes.lane_kind TEXT NOT NULL DEFAULT
#         'implement' CHECK (lane_kind IN ('implement','review')). Registered via a
#         dedicated _migrate_add_worktree_lanes_lane_kind + _HANDOFF_REQUIRED_COLUMNS
#         so already-stamped v30 DBs retrofit it.
#   v32 — implementation note S2: extends the system-origin classification GLOB list in
#         _migrate_decisions_decision_origin (backfill + trg_decisions_origin_default)
#         and its _HANDOFF_FTS_TRIGGERS_SQL twin with 'lane_landed_*' so lane-landing
#         records are stamped decision_origin='system' at the schema layer when the
#         column is NULL. record_decision accepts an explicit decision_origin kwarg;
#         trg_decisions_origin_default fires only WHEN NEW.decision_origin IS NULL,
#         so it is a default-filler and cannot override an explicit value. No new
#         column, so _HANDOFF_REQUIRED_COLUMNS is unchanged; the bump is what makes
#         the edited trigger re-install on already-stamped v31 DBs —
#         _apply_handoff_migrations replays every migration and this one
#         DROP/CREATEs the trigger unconditionally.
#   v33 — implementation note S1: codemap_reindex_lease singleton table (repo-keyed lease with
#         generation fencing for single-flight codemap reindex) plus
#         codemap_reindex_generation (monotonic watermark). CREATE TABLE IF NOT
#         EXISTS via _migrate_add_codemap_reindex_lease; both registered in
#         _HANDOFF_REQUIRED_TABLES so already-stamped v32 DBs retrofit them.
#         Mutual exclusion is fcntl.flock (not pid/TTL); holder_token was an
#         abandoned pid-identity column and is no longer required. expires_at is
#         a staleness annotation only ([RES-10] fencing stays on generation).
#   v34 — non-unique idx_review_findings_finding_id (finding_id leading) so
#         finding_id-only UPDATEs SEARCH instead of SCAN. Additive: older
#         readers kept serving because this migration did not raise the floor. The
#         index is *not* in HANDOFF_SCHEMA_SQL or _HANDOFF_REQUIRED_*; CREATE
#         INDEX over production-scale review_findings must not run inside
#         _bootstrap_handoff_schema's BEGIN IMMEDIATE. Schema 34 is stamped
#         only after that btree exists; a locked CREATE INDEX leaves the
#         true version at 33 so the next 0.2.19 open retries. See
#         _ensure_review_findings_finding_id_index.
# Predecessor stamp while idx_review_findings_finding_id is still missing.
# Mixed-fleet 0.2.14–0.2.18 readers treat 33 as current and skip bootstrap
# (they have no ensure helper); 0.2.19 sees 33 < 34 and retries the btree.
_PRE_FINDING_ID_INDEX_SCHEMA_VERSION = 33
_REVIEW_FINDINGS_FINDING_ID_INDEX = "idx_review_findings_finding_id"
#   v35 — review_findings_archive cold table (same columns as review_findings
#         plus archived_at) registered in _HANDOFF_REQUIRED_TABLES, plus the
#         bounded archive_terminal_review_findings reaper. Additive: the
#         reader floor stays 32.
#   v36 — additive per-task tool roster used to persist next-boot domain and
#         skill activation intent. The reader floor stays 32.
#   v37 — nullable worker_reports.delivery_id plus a unique index. Existing
#         rows remain NULL and therefore do not collide under SQLite UNIQUE
#         semantics; identified deliveries become atomic insert-or-replay claims.
#         This is deliberately a breaking reader floor: older writers cannot
#         populate delivery claims and would silently weaken exactly-once delivery.
#   v38 — additive nullable branch-identity columns on worktree_lanes
#         (branch_tip_sha / branch_tip_observed_at / branch_tip_source /
#         landing_commit_sha). A lane row previously carried only a branch *name*,
#         so once the ref was collected the row could no longer name the commit it
#         had produced and the row-side disposition became undecidable. Persisting
#         the tip makes it decidable. Registered via a dedicated
#         _migrate_add_worktree_lanes_branch_identity that runs AFTER
#         _migrate_hygiene_residue_check_expansions (whose table rebuild would
#         otherwise drop the columns), plus _HANDOFF_REQUIRED_COLUMNS so
#         already-stamped v37 DBs retrofit them. Additive: the reader floor stays 37
#         — an older reader that ignores the columns loses provenance it never had.
#   v39 — additive blocker lookup index keyed by task and lane. The reader
#         floor stays 37.
HANDOFF_SCHEMA_VERSION = 39

# Minimum compatible reader version stamped into ``PRAGMA user_version``.
#
# ``user_version`` is the compat floor, not the true schema version. The true
# version lives in ``schema_meta`` (key ``schema_version``). Already-shipped
# packages only gate on ``user_version``; making that field mean "minimum
# reader" lets additive N+1 DBs stay readable by N without upgrading N.
#
# !!! MANDATORY MAINTENANCE RULE (paired with every HANDOFF_SCHEMA_VERSION bump) !!!
# A schema bump is breaking unless its author declares it additive.
# - Additive bump: leave this constant alone (older readers keep serving).
# - Breaking bump: raise this constant to the new HANDOFF_SCHEMA_VERSION in the
#   same commit so older readers fail closed on the raised floor.
# Schema 37 is breaking even though its storage change is additive: readers
# predating delivery claims write NULL ids and silently degrade exactly-once
# report delivery to at-least-once behavior.
MIN_COMPATIBLE_READER_VERSION = 37

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
    (30, "0.2.11"),
    (31, "0.2.12"),
    (32, "0.2.13"),
    (33, "0.2.14"),
    (34, "0.2.19"),
    (35, "0.2.20"),
    (36, "0.2.22"),
    (37, "0.2.23"),
    (38, "0.2.24"),
    (39, "0.2.25"),
)

# Bounded archival constants and archive_terminal_review_findings are defined
# in review_findings_retention.py (FindingStatus-derived terminal set) and
# re-exported above so existing imports and the retention suite keep resolving.

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
        "review_findings_archive",
        "worktree_lanes",
        "worker_reports",
        "lane_messages",
        "task_tool_roster",
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
        "codemap_reindex_lease",
        "codemap_reindex_generation",
        "schema_meta",
    }
)
# Additive columns that must exist on both the hot table and its archive twin.
# Future review_findings column-add migrations must append here AND ALTER both
# tables (see _add_review_findings_column_extensions).
_REVIEW_FINDINGS_MIRRORED_REQUIRED_COLUMNS = frozenset({"harness"})
_HANDOFF_REQUIRED_COLUMNS: dict[str, frozenset[str]] = {
    "decisions": frozenset({"slice_number", "changed_files_json", "harness", "decision_origin"}),
    "review_findings": _REVIEW_FINDINGS_MIRRORED_REQUIRED_COLUMNS,
    "review_findings_archive": _REVIEW_FINDINGS_MIRRORED_REQUIRED_COLUMNS | {"archived_at"},
    "worker_reports": frozenset({"outcome", "delivery_id"}),
    # dispatch_id/payload_json were added to lane_messages at commit 19c0f739
    # without registering them here or bumping HANDOFF_SCHEMA_VERSION, so the
    # warm-start net could not re-add them on already-stamped DBs. Registered so
    # a stamped-current DB missing them re-bootstraps (internal).
    "lane_messages": frozenset({"dispatch_id", "payload_json"}),
    # test_cmd was added to worktree_lanes at HANDOFF_SCHEMA_VERSION=25 (implementation note).
    # Registered so a DB already stamped at 24 (missing test_cmd) re-bootstraps and
    # re-adds the column via _migrate_add_column_extensions — same trap/fix as the
    # lane_messages retrofit above.
    # implementation note R3: lane_kind (implement|review) added at HANDOFF_SCHEMA_VERSION=31;
    # register so a stamped-current DB missing it re-bootstraps and re-adds it via
    # _migrate_add_worktree_lanes_lane_kind.
    # v38: the branch-identity quartet, added via
    # _migrate_add_worktree_lanes_branch_identity; registered for the same
    # warm-start reason.
    "worktree_lanes": frozenset(
        {
            "test_cmd",
            "lane_kind",
            "branch_tip_sha",
            "branch_tip_observed_at",
            "branch_tip_source",
            "landing_commit_sha",
        }
    ),
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
    # implementation note: generation fencing token is the load-bearing lease column.
    # holder_token is no longer part of the lease contract (flock pivot); leave
    # any pre-existing column in place but do not require it for warm-start.
    "codemap_reindex_lease": frozenset({"generation", "requested_shas"}),
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
        "findings_fts_filter_columns_update",
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

# One column list for the hot table and its archive twin. Archive adds
# archived_at only. Column-add migrations must ALTER both tables.
_REVIEW_FINDINGS_SHARED_COLUMNS_SQL = """
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
"""

REVIEW_FINDINGS_MIRRORED_COLUMNS: tuple[str, ...] = (
    "id",
    "task_ref",
    "lane_id",
    "finding_id",
    "severity",
    "file_path",
    "line_start",
    "line_end",
    "description",
    "fix",
    "status",
    "review_mode",
    "review_run_id",
    "session",
    "agent",
    "harness",
    "branch",
    "commit_sha",
    "resolution_notes",
    "reopen_count",
    "last_reopen_reason",
    "last_reopened_at",
    "resolved_at",
    "verification_evidence",
    "merged_from_json",
    "resolved_on_branch_at_commit",
    "resolved_on_branch_ref",
    "resolved_on_branch_at_ts",
    "integrated_at_commit",
    "integrated_at_ref",
    "integrated_at_ts",
    "created_at",
    "updated_at",
)

_REVIEW_FINDINGS_CREATE_SQL = f"""CREATE TABLE IF NOT EXISTS review_findings (
{_REVIEW_FINDINGS_SHARED_COLUMNS_SQL}
);"""

_REVIEW_FINDINGS_ARCHIVE_CREATE_SQL = f"""CREATE TABLE IF NOT EXISTS review_findings_archive (
{_REVIEW_FINDINGS_SHARED_COLUMNS_SQL},
    archived_at   TEXT NOT NULL DEFAULT (datetime('now'))
);"""

HANDOFF_SCHEMA_SQL = (
    """
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
    decision_origin TEXT
                  CHECK (decision_origin IS NULL OR decision_origin IN ('agent', 'system')),
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

"""
    + _REVIEW_FINDINGS_CREATE_SQL
    + "\n\n"
    + _REVIEW_FINDINGS_ARCHIVE_CREATE_SQL
    + """

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
    lane_kind     TEXT NOT NULL DEFAULT 'implement'
                  CHECK (lane_kind IN ('implement', 'review')),
    -- v38 branch identity. A branch *name* is not an identity: once the ref is
    -- collected the row can no longer name the commit the lane produced. These
    -- four are nullable because identity is resolved, not assumed -- NULL means
    -- "not observed", which is a distinct state from "observed as absent".
    branch_tip_sha        TEXT,
    branch_tip_observed_at TEXT,
    branch_tip_source     TEXT,
    landing_commit_sha    TEXT,
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
    delivery_id       TEXT,
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

CREATE TABLE IF NOT EXISTS task_tool_roster (
    task_ref          TEXT PRIMARY KEY,
    skill_slugs       TEXT,
    activated_domains TEXT,
    updated_at        TEXT NOT NULL
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

CREATE TABLE IF NOT EXISTS codemap_reindex_lease (
  repo_instance_id TEXT PRIMARY KEY,
  holder_pid INTEGER NOT NULL,
  generation INTEGER NOT NULL,
  acquired_at TEXT NOT NULL,
  expires_at TEXT NOT NULL,
  target_sha TEXT NOT NULL,
  requested_shas TEXT NOT NULL DEFAULT '[]'
);

CREATE TABLE IF NOT EXISTS codemap_reindex_generation (
  repo_instance_id TEXT PRIMARY KEY,
  last_generation INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS schema_meta (
  key TEXT PRIMARY KEY,
  value INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_decisions_task_created
    ON decisions(task_ref, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_blockers_task_status
    ON blockers(task_ref, status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_blockers_task_lane_status
    ON blockers(task_ref, lane_id, status, created_at DESC);
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
-- idx_review_findings_finding_id is intentionally not created here. This
-- script runs inside _bootstrap_handoff_schema's BEGIN IMMEDIATE; a fourth
-- btree over production-scale review_findings must not hold that exclusive
-- lock. See _ensure_review_findings_finding_id_index.
CREATE INDEX IF NOT EXISTS idx_lanes_task_status
    ON worktree_lanes(task_ref, status, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_worker_reports_task_lane
    ON worker_reports(task_ref, lane_id, created_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS idx_worker_reports_delivery_id_unique
    ON worker_reports(delivery_id);

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
)

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

-- UPDATE OF body-relevant columns only: an AFTER INSERT origin-classification
-- trigger may UPDATE decision_origin, and a full AFTER UPDATE FTS rebuild can
-- race the FTS insert trigger (duplicate rowid) depending on trigger creation
-- order after migrations re-DROP/CREATE the origin trigger (implementation note S3a).
CREATE TRIGGER IF NOT EXISTS decisions_fts_update
AFTER UPDATE OF decision, rationale, task_ref, lane_id ON decisions
BEGIN
    DELETE FROM decisions_fts WHERE rowid = old.id;
    INSERT INTO decisions_fts(rowid, body, record_id, task_ref, lane_id)
    VALUES (new.id,
            new.decision || ' ' || COALESCE(new.rationale, ''),
            new.id, new.task_ref, new.lane_id);
END;

CREATE TRIGGER IF NOT EXISTS decisions_fts_delete AFTER DELETE ON decisions BEGIN
    DELETE FROM decisions_fts WHERE rowid = old.id;
END;

-- implementation note S3a: classify machine-generated decision ids as system when origin is omitted.
-- GLOB keeps '_' literal (segment boundary); equivalent to LIKE + ESCAPE '\\' without
-- Python/SQL string-escaping hazards that collapse ESCAPE '\\' to an empty escape.
CREATE TRIGGER IF NOT EXISTS trg_decisions_origin_default
AFTER INSERT ON decisions
FOR EACH ROW
WHEN NEW.decision_origin IS NULL
BEGIN
    UPDATE decisions
    SET decision_origin = CASE
        -- BR-0146-S3-01: slice-complete ids are ALWAYS agent decisions even
        -- when the slug/work_ref contains a machine segment (e.g.
        -- claude_slice_complete_T_token_usage_metering). Exemption arm first.
        WHEN NEW.decision GLOB 'slice_complete_*'
          OR NEW.decision GLOB '*_slice_complete_*'
        THEN 'agent'
        WHEN NEW.decision GLOB 'integrate_finding_*'
          OR NEW.decision GLOB '*_integrate_finding_*'
          OR NEW.decision GLOB 'repair_provenance_*'
          OR NEW.decision GLOB '*_repair_provenance_*'
          OR NEW.decision GLOB 'tasks_gc_*'
          OR NEW.decision GLOB '*_tasks_gc_*'
          OR NEW.decision GLOB 'cascade_archive_*'
          OR NEW.decision GLOB '*_cascade_archive_*'
          OR NEW.decision GLOB 'token_usage_*'
          OR NEW.decision GLOB '*_token_usage_*'
          OR NEW.decision GLOB 'lane_landed_*'
          OR NEW.decision GLOB '*_lane_landed_*'
        THEN 'system'
        ELSE 'agent'
    END
    WHERE id = NEW.id;
END;

-- review_findings triggers. Re-create these two UPDATE programs when FTS
-- setup discovers an older installation without the content-shadow companion:
-- CREATE TRIGGER IF NOT EXISTS alone would leave its unrestricted rebuild in
-- place. The schema migration below performs the same replacement for v33
-- databases while bootstrap holds its migration transaction.
CREATE TRIGGER IF NOT EXISTS findings_fts_insert AFTER INSERT ON review_findings BEGIN
    INSERT INTO findings_fts(rowid, body, record_id, task_ref, lane_id, status)
    VALUES (new.id,
            new.description || ' ' || COALESCE(new.fix, ''),
            new.id, new.task_ref, new.lane_id, new.status);
END;

DROP TRIGGER IF EXISTS findings_fts_update;
CREATE TRIGGER findings_fts_update
AFTER UPDATE OF description, fix ON review_findings
BEGIN
    DELETE FROM findings_fts WHERE rowid = old.id;
    INSERT INTO findings_fts(rowid, body, record_id, task_ref, lane_id, status)
    VALUES (new.id,
            new.description || ' ' || COALESCE(new.fix, ''),
            new.id, new.task_ref, new.lane_id, new.status);
END;

DROP TRIGGER IF EXISTS findings_fts_filter_columns_update;
CREATE TRIGGER findings_fts_filter_columns_update
AFTER UPDATE OF task_ref, lane_id, status ON review_findings
BEGIN
    -- FTS5 virtual-table UPDATE is itself a delete+insert. These UNINDEXED
    -- filter values live in c2/c3/c4 of the content shadow (c0=body,
    -- c1=record_id), so update that one row without touching the term index.
    UPDATE findings_fts_content
    SET c2 = new.task_ref,
        c3 = new.lane_id,
        c4 = new.status
    WHERE id = new.id;
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


def _sqlite_table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1",
        (table_name,),
    ).fetchone()
    return row is not None


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


def _review_findings_finding_id_index_present(conn: sqlite3.Connection) -> bool:
    """Read-only probe for ``idx_review_findings_finding_id``.

    Must not start ``BEGIN IMMEDIATE`` and must not be wired into
    ``_handoff_schema_bootstrapped``: a 34-stamped file whose btree was
    dropped still heals via the post-COMMIT ensure on prepare. Doctor /
    init-state --check use this to distinguish a cured operator DB from a
    skip-scanning one.
    """
    return _has_index(conn, "review_findings", _REVIEW_FINDINGS_FINDING_ID_INDEX)


def _worker_reports_delivery_id_index_present(conn: sqlite3.Connection) -> bool:
    """Read-only probe for the unique worker-report delivery claim index."""
    return _has_index(conn, "worker_reports", "idx_worker_reports_delivery_id_unique")


def _review_findings_finding_id_index_in_stat1(conn: sqlite3.Connection) -> bool:
    """True when ``sqlite_stat1`` already names ``idx_review_findings_finding_id``.

    A missing ``sqlite_stat1`` table (never ANALYZE'd) or a catalog computed
    before this btree existed both return False so the ensure helper can
    refresh planner stats without rebuilding the index.
    """
    try:
        row = conn.execute(
            "SELECT 1 FROM sqlite_stat1 WHERE idx = ? LIMIT 1",
            (_REVIEW_FINDINGS_FINDING_ID_INDEX,),
        ).fetchone()
    except sqlite3.OperationalError as exc:
        if "no such table" in str(exc).lower():
            return False
        raise
    return row is not None


def _refresh_review_findings_planner_stats(conn: sqlite3.Connection, *, fail_open: bool = False) -> None:
    """Run ``ANALYZE review_findings`` when the finding_id btree is unnamed in ``sqlite_stat1``.

    ANALYZE is a write. The connection-open path must fail open on a busy
    database: stale planner statistics are a performance degradation, not a
    correctness break ([SECD-05 inverted], [RES-17], DBINTG-H-02). The
    explicit maintenance entry point (:func:`vacuum_handoff_connection`)
    calls this with ``fail_open=False`` so an operator vacuum still records
    stats via the exclusive-statement bound. Defense-in-depth skip while
    ``_bootstrap_handoff_schema`` holds BEGIN IMMEDIATE (the production gate
    is :func:`_ensure_review_findings_finding_id_index`).
    """
    global review_findings_analyze_skipped_count
    if _in_exclusive_bootstrap_transaction(conn):
        return
    if not _review_findings_finding_id_index_present(conn):
        return
    if _review_findings_finding_id_index_in_stat1(conn):
        return
    if not fail_open:
        _run_exclusive_sqlite_statement(conn, "ANALYZE review_findings")
        return
    previous_busy = int(conn.execute("PRAGMA busy_timeout").fetchone()[0])
    analyze_exc: BaseException | None = None
    work_exc: BaseException | None = None
    restore_exc: sqlite3.Error | None = None
    try:
        conn.execute(f"PRAGMA busy_timeout={HANDOFF_PLANNER_STATS_BUSY_TIMEOUT_MS};")
        conn.execute("ANALYZE review_findings")
    except sqlite3.OperationalError as exc:
        analyze_exc = exc
        if is_lock_contention_error(exc):
            with _obs_counter_lock:
                review_findings_analyze_skipped_count += 1
            _log.warning(
                "ANALYZE review_findings skipped on connect (database busy/locked); "
                "leaving stale planner stats [OBS-08]"
            )
        else:
            work_exc = exc
    except Exception as exc:
        analyze_exc = exc
        work_exc = exc
    finally:
        try:
            conn.execute(f"PRAGMA busy_timeout={previous_busy};")
            restored = int(conn.execute("PRAGMA busy_timeout").fetchone()[0])
            if restored != previous_busy:
                raise sqlite3.OperationalError(
                    f"PRAGMA busy_timeout restore readback {restored} != intended {previous_busy}"
                )
        except sqlite3.Error as exc:
            restore_exc = exc
            _log.warning(
                "failed to restore PRAGMA busy_timeout (previous=%s intended=%s) "
                "after ANALYZE review_findings: %s [OBS-08]",
                previous_busy,
                previous_busy,
                exc,
            )
    if restore_exc is not None:
        err = RuntimeError(
            "ANALYZE review_findings left busy_timeout unrestored "
            f"(intended {previous_busy} ms); connection is not healthy"
        )
        err.__cause__ = restore_exc
        if analyze_exc is not None:
            err.__context__ = analyze_exc
            err.__suppress_context__ = False
        raise err
    if work_exc is not None:
        raise work_exc


def _stamp_handoff_schema_version(conn: sqlite3.Connection, version: int) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO schema_meta (key, value) VALUES (?, ?)",
        ("schema_version", int(version)),
    )


def _in_exclusive_bootstrap_transaction(conn: sqlite3.Connection) -> bool:
    """True while ``_bootstrap_handoff_schema`` holds ``BEGIN IMMEDIATE``.

    That helper sets ``isolation_level = None`` and then issues
    ``BEGIN IMMEDIATE``. Rebuild migrations may call index ensures in that
    window; ANALYZE and the schema-34 stamp must wait until after COMMIT.
    """
    return conn.isolation_level is None and conn.in_transaction


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


def handoff_schema_version(conn: sqlite3.Connection) -> int:
    """Return the DB's true schema version.

    Prefers ``schema_meta.schema_version`` when present. Falls back to
    ``PRAGMA user_version`` for pre-floor DBs that only stamped the old
    equality contract. Must not raise on a pre-floor DB.
    """
    try:
        row = conn.execute(
            "SELECT value FROM schema_meta WHERE key = ?",
            ("schema_version",),
        ).fetchone()
    except sqlite3.OperationalError:
        row = None
    if row is not None:
        return int(row[0])
    return int(conn.execute("PRAGMA user_version").fetchone()[0])


def _handoff_schema_bootstrapped(conn: sqlite3.Connection) -> bool:
    # True version lives in schema_meta; user_version is only the reader floor.
    # Comparing the floor to HANDOFF_SCHEMA_VERSION would re-migrate every open
    # after an additive bump (floor < true version).
    #
    # Indexes are intentionally not in this predicate. Putting
    # idx_review_findings_finding_id here would re-enter BEGIN IMMEDIATE on
    # a 34-stamped file whose btree was dropped; that file heals via the
    # post-COMMIT ensure in _prepare_handoff_connection instead. Doctor /
    # init-state --check use _review_findings_finding_id_index_present.
    if handoff_schema_version(conn) < HANDOFF_SCHEMA_VERSION:
        return False
    # A breaking floor bump must also reach databases already stamped at the
    # current true version. Otherwise an older writer can keep opening the DB
    # and append NULL delivery claims indefinitely.
    if int(conn.execute("PRAGMA user_version").fetchone()[0]) < MIN_COMPATIBLE_READER_VERSION:
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
        # Bounded existence probes, not COUNT(*). Measured 2026-08 on the live
        # 284 MB operator DB (treat as given):
        #   decisions n=9922 COUNT=17.59ms | decisions_fts COUNT=244.13ms | LIMIT 1: 0.05/0.02ms
        #   review_findings n=17883 COUNT=33.01ms | findings_fts COUNT=445.64ms | LIMIT 1: 0.05/0.02ms
        #   blockers n=437 COUNT=0.39ms | blockers_fts COUNT=5.39ms | LIMIT 1: 0.01/0.01ms
        #   next_actions n=66 COUNT=0.14ms | actions_fts COUNT=2.17ms | LIMIT 1: 0.01/0.01ms
        #   verified_tests n=1582 COUNT=0.82ms | verified_tests_fts COUNT=7.56ms | LIMIT 1: 0.02/0.01ms
        # Cold total per open: 756.8 ms. Warm (OS page cache hot): 16 ms.
        # LIMIT 1 equivalent: ~0.2 ms. FTS5 COUNT(*) has no shortcut — it walks
        # the shadow content table — so cost grows linearly with row count
        # forever. Do not "simplify" these probes back to COUNT(*). Semantics
        # unchanged: backfill iff the source has at least one row AND the FTS
        # twin has zero rows. Table names come only from the hardcoded pairs
        # list / _HANDOFF_REQUIRED_FTS_TABLES, never from caller input.
        source_nonempty = conn.execute(f"SELECT 1 FROM {source_table} LIMIT 1").fetchone() is not None
        if source_nonempty:
            fts_empty = conn.execute(f"SELECT 1 FROM {fts_table} LIMIT 1").fetchone() is None
            if fts_empty:
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


def _ensure_review_findings_finding_id_index(conn: sqlite3.Connection) -> None:
    """Non-unique ``finding_id``-leading index for finding_id-only writes.

    Production uniqueness stays ``(task_ref, finding_id)``; the same
    ``finding_id`` may appear under more than one ``task_ref``, so this
    index must not be UNIQUE.

    Must not run inside ``_bootstrap_handoff_schema``'s ``BEGIN IMMEDIATE``
    on the hot v33→v34 path. Building this btree over a large
    ``review_findings`` table (the operator DB has ~17k rows) would hold an
    exclusive write lock for the duration of the build and reproduce the
    stall this index exists to cure.

    Rebuild migrations (``_migrate_finding_lifecycle_states``,
    ``_migrate_review_findings_superseded_status``) may call this helper
    while ``_apply_handoff_migrations`` still holds that exclusive
    transaction: those paths already DROP/CREATE the table under
    ``BEGIN IMMEDIATE``, and the lock cost is dominated by the table copy,
    not the index restore. The v33 operator upgrade skips those rebuilds,
    so the hot path still creates the index after COMMIT via bootstrap /
    ``_prepare_handoff_connection``.

    ``CREATE INDEX IF NOT EXISTS`` is idempotent, so a concurrent opener
    arriving mid-build is safe; a process holding ``BEGIN IMMEDIATE`` for
    the duration of a standalone btree build (no table rebuild) is not.

    Planner-stats refresh (``ANALYZE review_findings``) is a write: it
    rewrites ``sqlite_stat1``. Unbounded ANALYZE on this helper — which
    ``_prepare_handoff_connection`` runs on every open — self-deadlocks a
    nested second connection while the first still holds RESERVED
    (DBINTG-H-02). The btree is still created here. ANALYZE is fail-open
    on the connect path and the fail-closed refresh lives on
    :func:`vacuum_handoff_connection` (the existing explicit maintenance
    entry point). ANALYZE is skipped while this helper runs inside the
    exclusive bootstrap transaction (rebuild path).

    SQLITE_BUSY/LOCKED on the CREATE INDEX write lock or on the
    ``PRAGMA index_list`` presence probe is in-band: catch via
    ``is_lock_contention_error``, leave the connection usable, and retry
    the idempotent CREATE on the next open. Callers on the bootstrap path
    do not wrap this helper, so an uncaught lock error here breaks schema
    preparation outright. Non-lock ``OperationalError`` still propagates.
    (``_refresh_review_findings_planner_stats(fail_open=True)`` already
    absorbs a locked ANALYZE under its own bounded busy_timeout.)
    """
    try:
        if not _review_findings_finding_id_index_present(conn):
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_review_findings_finding_id
                ON review_findings(finding_id)
                """
            )
        if _in_exclusive_bootstrap_transaction(conn):
            return
        if not _review_findings_finding_id_index_present(conn):
            return
        _refresh_review_findings_planner_stats(conn, fail_open=True)
    except sqlite3.OperationalError as exc:
        if is_lock_contention_error(exc):
            _log.warning(
                "DB locked creating idx_review_findings_finding_id; CREATE INDEX IF NOT EXISTS will retry on next open"
            )
            return
        raise


def _ensure_worker_reports_delivery_id_index(conn: sqlite3.Connection) -> None:
    """Restore the unique delivery-claim index on every prepared open.

    The index is a syntactic prerequisite of every worker-report INSERT that
    uses ``ON CONFLICT(delivery_id)`` as well as the exactly-once boundary.
    ``CREATE UNIQUE INDEX`` distinguishes a clean dropped-index repair from a
    corrupt database containing duplicate non-NULL delivery ids: the former
    heals and the latter fails closed with ``IntegrityError``.
    """
    if not _worker_reports_delivery_id_index_present(conn):
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_worker_reports_delivery_id_unique
            ON worker_reports(delivery_id)
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


_REVIEW_FINDINGS_COLUMN_EXTENSIONS: tuple[tuple[str, str], ...] = (
    ("lane_id", "TEXT"),
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
)


def _add_review_findings_column_extensions(conn: sqlite3.Connection, table: str) -> bool:
    """ALTER additive review_findings columns onto ``table`` when it exists.

    Used for both the hot table and ``review_findings_archive`` so a later
    column-add cannot leave the archive twin frozen while INSERT copies the
    hot PRAGMA list.
    """
    if not _sqlite_table_exists(conn, table):
        return False
    added = False
    for column, column_def in _REVIEW_FINDINGS_COLUMN_EXTENSIONS:
        if _add_column_if_missing(conn, table, column, column_def):
            added = True
    if table == "review_findings_archive":
        if _add_column_if_missing(
            conn,
            table,
            "archived_at",
            "TEXT NOT NULL DEFAULT (datetime('now'))",
        ):
            added = True
    return added


def _migrate_add_column_extensions(conn: sqlite3.Connection) -> None:
    """Add incremental columns to core tables and backfill review_findings defaults. Idempotent."""
    for table in ("decisions", "blockers", "next_actions", "verified_tests"):
        _add_column_if_missing(conn, table, "lane_id", "TEXT")
    for column in ("model", "model_label", "reasoning_level"):
        _add_column_if_missing(conn, "decisions", column, "TEXT")
    _add_column_if_missing(conn, "decisions", "harness", "TEXT")
    for column in ("input_tokens", "output_tokens", "total_tokens"):
        _add_column_if_missing(conn, "decisions", column, "INTEGER")
    needs_backfill = _add_review_findings_column_extensions(conn, "review_findings")
    _add_review_findings_column_extensions(conn, "review_findings_archive")
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
        conn.execute("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'turn_metrics' LIMIT 1").fetchone()
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
    # idx_review_findings_finding_id is not created here: this helper always
    # runs inside the exclusive bootstrap transaction. The v34 index is
    # installed after that transaction commits.


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
        if not _has_index(conn, "review_findings", "idx_review_findings_task_status"):
            conn.execute("CREATE INDEX idx_review_findings_task_status ON review_findings(task_ref, status, severity)")
        if not _has_index(conn, "review_findings", "idx_review_findings_lane_status"):
            conn.execute("CREATE INDEX idx_review_findings_lane_status ON review_findings(lane_id, status)")
        _ensure_review_findings_finding_id_index(conn)
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
    _ensure_review_findings_finding_id_index(conn)
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
    delivery_id_expr = "delivery_id" if _has_column(conn, "worker_reports", "delivery_id") else "NULL"
    conn.execute("ALTER TABLE worker_reports RENAME TO worker_reports_legacy_v26")
    conn.execute(
        """
        CREATE TABLE worker_reports (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            task_ref          TEXT NOT NULL,
            lane_id           TEXT NOT NULL,
            session           TEXT NOT NULL,
            delivery_id       TEXT,
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
            id, task_ref, lane_id, session, delivery_id, summary, changed_files_json, test_commands_json,
            blockers_json, merge_ready, status, outcome, agent, branch, commit_sha, created_at
        )
        SELECT
            id, task_ref, lane_id, session, {delivery_id_expr}, summary, changed_files_json, test_commands_json,
            blockers_json, merge_ready, status, outcome, agent, branch, commit_sha, created_at
        FROM worker_reports_legacy_v26
        """.format(delivery_id_expr=delivery_id_expr)
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
    has_lane_kind = _has_column(conn, "worktree_lanes", "lane_kind")
    # v38 branch identity. This rebuild normally runs *before* the v38 migration
    # adds them, so they are usually absent -- but if the rebuild ever re-fires on
    # a v38+ DB, dropping them would destroy exactly the provenance the columns
    # exist to preserve (same reasoning as lane_kind above).
    branch_identity_columns = (
        "branch_tip_sha",
        "branch_tip_observed_at",
        "branch_tip_source",
        "landing_commit_sha",
    )
    has_branch_identity = {
        column: _has_column(conn, "worktree_lanes", column) for column in branch_identity_columns
    }

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
            lane_kind     TEXT NOT NULL DEFAULT 'implement'
                          CHECK (lane_kind IN ('implement', 'review')),
            branch_tip_sha        TEXT,
            branch_tip_observed_at TEXT,
            branch_tip_source     TEXT,
            landing_commit_sha    TEXT,
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
    # lane_kind is NOT NULL — a legacy table lacking it copies the 'implement' default
    # (implementation note review A1/grok-2: never drop lane_kind if this rebuild re-fires on v31+).
    select_lane_kind = "lane_kind" if has_lane_kind else "'implement'"
    identity_target = ", ".join(branch_identity_columns)
    identity_select = ", ".join(
        column if has_branch_identity[column] else "NULL" for column in branch_identity_columns
    )
    conn.execute(
        f"""
        INSERT INTO worktree_lanes (
            id, task_ref, lane_id, title, objective, worktree_path, branch, owner_agent,
            model, backend, reasoning_effort, test_cmd, lane_kind, {identity_target},
            status, notes, created_at, updated_at
        )
        SELECT
            id, task_ref, lane_id, title, objective, worktree_path, branch, owner_agent,
            {select_model}, {select_backend}, {select_effort}, {select_test_cmd}, {select_lane_kind},
            {identity_select},
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
    # Do not create idx_review_findings_finding_id here: this helper runs
    # inside _bootstrap_handoff_schema's BEGIN IMMEDIATE. Rebuild migrations
    # restore the index after DROP/CREATE (same survival idea as the unique
    # composite), and the post-COMMIT ensure in bootstrap / prepare installs
    # it on warm and fresh files without holding the exclusive lock.
    _migrate_dedupe_decisions_and_index(conn)
    _migrate_add_projection_event_dedupe(conn)
    _migrate_hygiene_residue_check_expansions(conn)
    _migrate_decisions_decision_origin(conn)
    _migrate_findings_fts_write_amplification(conn)
    _migrate_add_worktree_lanes_lane_kind(conn)
    # MUST stay after _migrate_hygiene_residue_check_expansions: that step can
    # rebuild worktree_lanes, and a column added before the rebuild is only
    # carried across by an explicit preserve arm.
    _migrate_add_worktree_lanes_branch_identity(conn)
    _migrate_add_codemap_reindex_lease(conn)
    _migrate_add_schema_meta(conn)
    _migrate_add_review_findings_archive(conn)
    _migrate_add_task_tool_roster(conn)
    _migrate_add_worker_report_delivery_claim(conn)
    _migrate_add_blockers_task_lane_status_index(conn)


def _migrate_add_blockers_task_lane_status_index(conn: sqlite3.Connection) -> None:
    """v38 -> v39: add the lane-specific blocker lookup index."""
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_blockers_task_lane_status
        ON blockers(task_ref, lane_id, status, created_at DESC)
        """
    )


def _migrate_add_worker_report_delivery_claim(conn: sqlite3.Connection) -> None:
    """v36 -> v37: add the receiver-owned idempotent delivery claim."""
    _add_column_if_missing(conn, "worker_reports", "delivery_id", "TEXT")
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_worker_reports_delivery_id_unique
        ON worker_reports(delivery_id)
        """
    )


def _migrate_add_task_tool_roster(conn: sqlite3.Connection) -> None:
    """v35 -> v36: persist per-task next-boot tool roster intent."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS task_tool_roster (
            task_ref          TEXT PRIMARY KEY,
            skill_slugs       TEXT,
            activated_domains TEXT,
            updated_at        TEXT NOT NULL
        )
        """
    )


def _migrate_add_review_findings_archive(conn: sqlite3.Connection) -> None:
    """v33/v34 → v35: cold twin of ``review_findings`` for terminal-row archival.

    Additive. ``CREATE TABLE IF NOT EXISTS`` is safe to replay. Warm-start on a
    DB already stamped at 35 that is missing the table still re-enters
    bootstrap because the name is in ``_HANDOFF_REQUIRED_TABLES``. Does not
    create ``idx_review_findings_finding_id`` (idxfix must claim schema 36+
    plus a required-index gate). Does not VACUUM: file-size reclaim is an
    operator exclusive-lock step after the bounded reaper has drained
    terminal rows (see ``REVIEW_FINDINGS_ARCHIVE_OPERATOR_VACUUM_SQL``).
    Shares ``_REVIEW_FINDINGS_SHARED_COLUMNS_SQL`` with the hot-table DDL and
    ALTERs mirrored additive columns onto the archive twin.
    """
    conn.execute(_REVIEW_FINDINGS_ARCHIVE_CREATE_SQL)
    _add_review_findings_column_extensions(conn, "review_findings_archive")


def _migrate_findings_fts_write_amplification(conn: sqlite3.Connection) -> None:
    """Replace the wide findings FTS rebuild and install filter-only sync.

    Schema 34 is already newer than the operator's schema-33 stamp, so that
    database re-enters bootstrap and runs this migration. Current databases
    missing the newly required companion are also repaired by FTS setup,
    whose trigger script performs the same idempotent DROP/CREATE sequence.

    Only ``description`` and ``fix`` feed the tokenized body and may rebuild
    the virtual table. The search filter columns are kept current by updating
    their FTS5 content-shadow cells in place; updating the virtual table would
    still invoke FTS5's delete+insert path for these UNINDEXED values.
    """
    fts_present = (
        conn.execute("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'findings_fts' LIMIT 1").fetchone()
        is not None
    )
    if not fts_present:
        return

    conn.execute("DROP TRIGGER IF EXISTS findings_fts_update")
    conn.execute(
        """
        CREATE TRIGGER findings_fts_update
        AFTER UPDATE OF description, fix ON review_findings
        BEGIN
            DELETE FROM findings_fts WHERE rowid = old.id;
            INSERT INTO findings_fts(rowid, body, record_id, task_ref, lane_id, status)
            VALUES (new.id,
                    new.description || ' ' || COALESCE(new.fix, ''),
                    new.id, new.task_ref, new.lane_id, new.status);
        END
        """
    )
    conn.execute("DROP TRIGGER IF EXISTS findings_fts_filter_columns_update")
    conn.execute(
        """
        CREATE TRIGGER findings_fts_filter_columns_update
        AFTER UPDATE OF task_ref, lane_id, status ON review_findings
        BEGIN
            UPDATE findings_fts_content
            SET c2 = new.task_ref,
                c3 = new.lane_id,
                c4 = new.status
            WHERE id = new.id;
        END
        """
    )


def _migrate_add_schema_meta(conn: sqlite3.Connection) -> None:
    """Introduce ``schema_meta`` and re-stamp ``user_version`` as the reader floor.

    Pre-floor DBs stored the true schema version in ``PRAGMA user_version``.
    After this change the true version lives in ``schema_meta``
    (``key='schema_version'``) and ``user_version`` is the minimum compatible
    reader. An already-stamped v33 DB with no ``schema_meta`` must record
    ``schema_version=33`` and lower ``user_version`` to
    ``MIN_COMPATIBLE_READER_VERSION`` so a v32 package can open it again.

    Idempotent: ``CREATE TABLE IF NOT EXISTS``, insert-if-missing for the
    version row, and only lowers ``user_version`` when it is strictly above
    the floor (never raises a floor, never drops data).
    """
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_meta (
            key TEXT PRIMARY KEY,
            value INTEGER NOT NULL
        )
        """
    )
    user_version = int(conn.execute("PRAGMA user_version").fetchone()[0])
    row = conn.execute(
        "SELECT value FROM schema_meta WHERE key = ?",
        ("schema_version",),
    ).fetchone()
    if row is None:
        # Preserve the pre-floor true-version stamp when present; otherwise the
        # bootstrap tail will write HANDOFF_SCHEMA_VERSION after all migrations.
        true_version = user_version if user_version > 0 else HANDOFF_SCHEMA_VERSION
        conn.execute(
            "INSERT INTO schema_meta (key, value) VALUES (?, ?)",
            ("schema_version", int(true_version)),
        )
    # Old equality stamp (e.g. user_version=33) must become the floor so N-1
    # readers can re-enter additive DBs. Bootstrap also stamps the floor last.
    if user_version > MIN_COMPATIBLE_READER_VERSION:
        conn.execute(f"PRAGMA user_version = {MIN_COMPATIBLE_READER_VERSION}")


def _migrate_add_codemap_reindex_lease(conn: sqlite3.Connection) -> None:
    """v32 → v33 (implementation note S1): ``codemap_reindex_lease`` + generation watermark.

    One row per ``repo_instance_id`` holds the durable reindex queue and the
    generation fencing token. Live mutual exclusion is an OS advisory lock
    (``fcntl.flock``) outside this table; ``generation`` still fences
    paused-then-resumed holders that write after a later grant ([RES-10]).
    ``codemap_reindex_generation`` keeps the monotonic watermark after the lease
    row is deleted. ``expires_at`` is a staleness annotation only.
    Idempotent via ``CREATE TABLE IF NOT EXISTS`` — safe to replay on
    already-stamped DBs. A legacy ``holder_token`` column, if present, is left
    in place and ignored.

    ``generation`` and ``requested_shas`` are also in ``_HANDOFF_REQUIRED_COLUMNS``,
    so warm-start retrofit must re-add them when a stamped-current DB is missing
    either column (CREATE TABLE IF NOT EXISTS is a no-op on an existing table).
    """
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS codemap_reindex_lease (
          repo_instance_id TEXT PRIMARY KEY,
          holder_pid INTEGER NOT NULL,
          generation INTEGER NOT NULL,
          acquired_at TEXT NOT NULL,
          expires_at TEXT NOT NULL,
          target_sha TEXT NOT NULL,
          requested_shas TEXT NOT NULL DEFAULT '[]'
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS codemap_reindex_generation (
          repo_instance_id TEXT PRIMARY KEY,
          last_generation INTEGER NOT NULL
        )
        """
    )
    # Retrofit required columns on pre-existing lease tables (manifest promise).
    # DEFAULT required for NOT NULL ADD COLUMN on tables that may already have rows.
    _add_column_if_missing(
        conn,
        "codemap_reindex_lease",
        "generation",
        "INTEGER NOT NULL DEFAULT 0",
    )
    _add_column_if_missing(
        conn,
        "codemap_reindex_lease",
        "requested_shas",
        "TEXT NOT NULL DEFAULT '[]'",
    )


def _migrate_add_worktree_lanes_lane_kind(conn: sqlite3.Connection) -> None:
    """v30 → v31 (implementation note R3): additive ``worktree_lanes.lane_kind``.

    A lane is ``'implement'`` (default) or ``'review'``. ``run_offload_pass`` reads
    it to type a clean-tree / unchanged-HEAD review handoff as ``review_complete``
    instead of mistaking it for a wedged ``needs_guidance`` transport failure.
    Idempotent via ``_add_column_if_missing``; the ``NOT NULL DEFAULT 'implement'``
    add is legal on an existing table because a constant DEFAULT is supplied
    (existing rows adopt it). No backfill/trigger needed.
    """
    _add_column_if_missing(
        conn,
        "worktree_lanes",
        "lane_kind",
        "TEXT NOT NULL DEFAULT 'implement' CHECK (lane_kind IN ('implement', 'review'))",
    )


WORKTREE_LANE_BRANCH_TIP_SOURCES: tuple[str, ...] = ("branch", "manifest", "backfill")


def _migrate_add_worktree_lanes_branch_identity(conn: sqlite3.Connection) -> None:
    """v37 -> v38: additive nullable branch identity on ``worktree_lanes``.

    A lane row stores a branch *name*. A name stops resolving the moment the ref
    is collected, so a row whose branch has been deleted can no longer say which
    commit the lane produced — the row-side disposition becomes undecidable, and
    no later heuristic can reconstruct it. These columns copy the identity onto
    the row while the ref still resolves:

    * ``branch_tip_sha`` — normalized 40-hex commit id of the lane branch tip.
    * ``branch_tip_observed_at`` — UTC timestamp of the observation. The SHA
      alone cannot say *when* it was true.
    * ``branch_tip_source`` — provenance, one of
      :data:`WORKTREE_LANE_BRANCH_TIP_SOURCES`: ``branch`` (resolved from the
      live ref), ``manifest`` (copied from an identity-bearing record), or
      ``backfill`` (retro-stamped). Not CHECK-constrained: a CHECK cannot be
      ALTERed in place later, and this vocabulary is expected to grow.
    * ``landing_commit_sha`` — the integration event identity, which is a
      *different* commit from the lane tip whenever the product landed
      non-ancestrally.

    All four are nullable with no backfill: NULL means "never observed", which
    is deliberately distinct from "observed as absent". Guessing a value here
    would manufacture the false provenance the columns exist to prevent.
    Idempotent via ``_add_column_if_missing``.
    """
    _add_column_if_missing(conn, "worktree_lanes", "branch_tip_sha", "TEXT")
    _add_column_if_missing(conn, "worktree_lanes", "branch_tip_observed_at", "TEXT")
    _add_column_if_missing(conn, "worktree_lanes", "branch_tip_source", "TEXT")
    _add_column_if_missing(conn, "worktree_lanes", "landing_commit_sha", "TEXT")


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


def _migrate_decisions_decision_origin(conn: sqlite3.Connection) -> None:
    """v29 → v30: decisions.decision_origin + backfill + classification trigger.

    Idempotent. Adds a nullable ``decision_origin`` column (``agent`` | ``system``),
    stamps existing rows from segment-anchored machine-id patterns, and installs
    ``trg_decisions_origin_default`` so inserts that omit origin are classified
    the same way. Explicit non-NULL inserts keep their override (trigger WHEN).

    Classification uses GLOB so ``_`` is a literal segment boundary (equivalent to
    ``LIKE '%\\_pat%' ESCAPE '\\'`` without escape-string hazards).
    """
    # Drop first so DROP COLUMN decision_origin (manifest drift probes) is not
    # blocked by a trigger that references the column.
    conn.execute("DROP TRIGGER IF EXISTS trg_decisions_origin_default")
    _add_column_if_missing(
        conn,
        "decisions",
        "decision_origin",
        "TEXT CHECK (decision_origin IS NULL OR decision_origin IN ('agent', 'system'))",
    )
    # Stamp every NULL origin from decision-id patterns. Segment boundary:
    # pattern at start OR immediately after '_' (GLOB '*_pat*').
    conn.execute(
        """
        UPDATE decisions
        SET decision_origin = CASE
            -- BR-0146-S3-01: slice-complete ids stay agent (exemption first).
            WHEN decision GLOB 'slice_complete_*'
              OR decision GLOB '*_slice_complete_*'
            THEN 'agent'
            WHEN decision GLOB 'integrate_finding_*'
              OR decision GLOB '*_integrate_finding_*'
              OR decision GLOB 'repair_provenance_*'
              OR decision GLOB '*_repair_provenance_*'
              OR decision GLOB 'tasks_gc_*'
              OR decision GLOB '*_tasks_gc_*'
              OR decision GLOB 'cascade_archive_*'
              OR decision GLOB '*_cascade_archive_*'
              OR decision GLOB 'token_usage_*'
              OR decision GLOB '*_token_usage_*'
              OR decision GLOB 'lane_landed_*'
              OR decision GLOB '*_lane_landed_*'
            THEN 'system'
            ELSE 'agent'
        END
        WHERE decision_origin IS NULL
        """
    )
    conn.execute(
        """
        CREATE TRIGGER trg_decisions_origin_default
        AFTER INSERT ON decisions
        FOR EACH ROW
        WHEN NEW.decision_origin IS NULL
        BEGIN
            UPDATE decisions
            SET decision_origin = CASE
                -- BR-0146-S3-01: slice-complete ids stay agent (exemption first).
                WHEN NEW.decision GLOB 'slice_complete_*'
                  OR NEW.decision GLOB '*_slice_complete_*'
                THEN 'agent'
                WHEN NEW.decision GLOB 'integrate_finding_*'
                  OR NEW.decision GLOB '*_integrate_finding_*'
                  OR NEW.decision GLOB 'repair_provenance_*'
                  OR NEW.decision GLOB '*_repair_provenance_*'
                  OR NEW.decision GLOB 'tasks_gc_*'
                  OR NEW.decision GLOB '*_tasks_gc_*'
                  OR NEW.decision GLOB 'cascade_archive_*'
                  OR NEW.decision GLOB '*_cascade_archive_*'
                  OR NEW.decision GLOB 'token_usage_*'
                  OR NEW.decision GLOB '*_token_usage_*'
                  OR NEW.decision GLOB 'lane_landed_*'
                  OR NEW.decision GLOB '*_lane_landed_*'
                THEN 'system'
                ELSE 'agent'
            END
            WHERE id = NEW.id;
        END
        """
    )
    # Refresh FTS update so origin-only UPDATEs do not rebuild FTS (avoids
    # duplicate-rowid races with decisions_fts_insert when trigger order flips).
    # Only when decisions_fts already exists — FTS bootstrap owns first create,
    # and installing the trigger without the virtual table breaks DROP COLUMN
    # rebuilds during incomplete-bootstrap probes.
    fts_present = (
        conn.execute("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'decisions_fts' LIMIT 1").fetchone()
        is not None
    )
    if fts_present:
        conn.execute("DROP TRIGGER IF EXISTS decisions_fts_update")
        conn.execute(
            """
            CREATE TRIGGER decisions_fts_update
            AFTER UPDATE OF decision, rationale, task_ref, lane_id ON decisions
            BEGIN
                DELETE FROM decisions_fts WHERE rowid = old.id;
                INSERT INTO decisions_fts(rowid, body, record_id, task_ref, lane_id)
                VALUES (new.id,
                        new.decision || ' ' || COALESCE(new.rationale, ''),
                        new.id, new.task_ref, new.lane_id);
            END
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

    ``idx_review_findings_finding_id`` is created *after* that COMMIT, while
    ``isolation_level`` is still ``None`` so ``CREATE INDEX`` autocommits as
    its own statement. Holding ``BEGIN IMMEDIATE`` across a btree build on
    production-scale ``review_findings`` would block every other lane at
    first open — the symptom this index exists to cure. ``CREATE INDEX IF
    NOT EXISTS`` is idempotent, so a concurrent opener arriving mid-build
    is safe.

    On database-lock contention — whether acquiring the ``BEGIN IMMEDIATE``
    write lock or during the migrations — roll back and return without
    raising (fail-open), leaving ``user_version`` unstamped so the next open
    retries the full migration set (implementation note D1). Likewise, if required
    tables or manifest columns are still missing after migrations (implementation note
    Prong 2), roll back and return unstamped rather than poisoning the
    version stamp. In that degraded case the connection is usable but the
    schema tables may be absent; ``_open_db_connection`` detects this and
    skips the FTS ensure step. A lock during the post-commit index build
    leaves the true version at ``_PRE_FINDING_ID_INDEX_SCHEMA_VERSION``
    (33), not 34, so mixed-fleet 0.2.14–0.2.18 readers do not treat a
    partial upgrade as current and the next 0.2.19 open retries bootstrap
    plus the idempotent ``CREATE INDEX IF NOT EXISTS``.
    """
    saved_isolation = conn.isolation_level
    conn.isolation_level = None
    # Migration helpers (_has_index / _has_column / required-table probes) index
    # rows by name; bare sqlite3.connect callers (e.g. the implementation note pin fixture)
    # would otherwise raise TypeError on tuple rows.
    if conn.row_factory is None:
        conn.row_factory = sqlite3.Row
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
            # HANDOFF_SCHEMA_SQL now owns the delivery-claim index for fresh
            # databases, but a pre-v37 worker_reports table lacks the indexed
            # column. Reuse the idempotent v37 migration before replaying the
            # base DDL; the normal migration tail still runs afterward so a
            # later table rebuild cannot strand the index.
            if _sqlite_objects_exist(conn, "table", frozenset({"worker_reports"})):
                _migrate_add_worker_report_delivery_claim(conn)
            _execute_sql_script(conn, HANDOFF_SCHEMA_SQL)
            _apply_handoff_migrations(conn)
            if (
                not _sqlite_objects_exist(conn, "table", _HANDOFF_REQUIRED_TABLES)
                or not _required_columns_present(conn)
                or not _worker_reports_delivery_id_index_present(conn)
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
            # True version in schema_meta; user_version is the reader floor.
            # Do not stamp schema 34 until idx_review_findings_finding_id
            # exists: a post-COMMIT CREATE INDEX lock would otherwise leave
            # mixed-fleet readers serving a v34 file that still skip-scans.
            # Rebuilds that already restored the btree inside this transaction
            # may stamp 34 here; the hot v33→v34 path stamps 33 until the
            # post-COMMIT ensure succeeds.
            schema_version_to_stamp = (
                HANDOFF_SCHEMA_VERSION
                if _review_findings_finding_id_index_present(conn)
                else _PRE_FINDING_ID_INDEX_SCHEMA_VERSION
            )
            _stamp_handoff_schema_version(conn, schema_version_to_stamp)
            conn.execute(f"PRAGMA user_version = {MIN_COMPATIBLE_READER_VERSION}")
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
        # isolation_level is still None: CREATE INDEX autocommits outside the
        # exclusive bootstrap transaction. A concurrent opener mid-build hits
        # CREATE INDEX IF NOT EXISTS, which is idempotent. Schema 34 is
        # stamped only after the btree exists.
        try:
            _ensure_review_findings_finding_id_index(conn)
            if _review_findings_finding_id_index_present(conn):
                _stamp_handoff_schema_version(conn, HANDOFF_SCHEMA_VERSION)
        except sqlite3.OperationalError as exc:
            if "locked" in str(exc).lower():
                _log.warning(
                    "DB locked creating idx_review_findings_finding_id after bootstrap "
                    "commit; leaving schema_version at %s so the next 0.2.19 open retries",
                    _PRE_FINDING_ID_INDEX_SCHEMA_VERSION,
                )
            else:
                raise
    finally:
        conn.isolation_level = saved_isolation


# ---------------------------------------------------------------------------
# DB connection factory
# ---------------------------------------------------------------------------


def _assert_schema_version_compatible(conn: sqlite3.Connection) -> None:
    """Refuse when the reader floor exceeds this package ([OBS-08] T15).

    Called only after a successful bootstrap so warm-start migrations still
    run when the DB is behind. ``PRAGMA user_version`` is the compat floor:
    refuse only when it is strictly above ``HANDOFF_SCHEMA_VERSION``. A floor
    at or below this package is served even when the true schema (in
    ``schema_meta``) is ahead via an additive bump, or behind (warm-start
    already handled behind DBs before this assert runs).
    """
    floor = int(conn.execute("PRAGMA user_version").fetchone()[0])
    if floor > HANDOFF_SCHEMA_VERSION:
        raise SchemaVersionMismatchError(floor, HANDOFF_SCHEMA_VERSION)


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
    unstamped DB (``user_version`` 0), a DB whose reader floor is at or below
    this package, and any DB that cannot be opened or read (locked past the
    timeout, corrupt / non-sqlite): those degrade to the existing lazy path
    (and log a warning) rather than aborting boot with a raw traceback. Only
    a readable DB whose stamped floor is strictly above
    ``HANDOFF_SCHEMA_VERSION`` fails closed: the "a newer DB must not be
    served by an older package" case the server would otherwise hit lazily
    on first call. Additive bumps leave the floor alone so N can still serve
    an N+1 DB.
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
        floor = int(conn.execute("PRAGMA user_version").fetchone()[0])
    except sqlite3.DatabaseError as exc:
        # Corrupt / non-sqlite file, or a lock still held past the timeout
        # (OperationalError is a DatabaseError subclass): defer to the lazy
        # bootstrap / degraded path, which self-heals, rather than abort boot.
        _log.warning(
            "boot schema gate skipped (degraded to lazy path): cannot read PRAGMA user_version from %s: %s",
            path,
            exc,
        )
        return
    finally:
        conn.close()
    if floor > HANDOFF_SCHEMA_VERSION:
        raise SchemaVersionMismatchError(floor, HANDOFF_SCHEMA_VERSION)


# Authorizer action codes that acquire (or escalate to) a SQLite RESERVED
# write lock. Matched against the resolved statement plan so CTE wrappers,
# leading comments, and unusual whitespace cannot hide a write ([CON-11],
# [DATA-16], internal). Do not include SQLITE_READ / SQLITE_SELECT.
_WRITE_LOCK_AUTHORIZER_ACTIONS: frozenset[int] = frozenset(
    {
        sqlite3.SQLITE_INSERT,
        sqlite3.SQLITE_UPDATE,
        sqlite3.SQLITE_DELETE,
        sqlite3.SQLITE_CREATE_INDEX,
        sqlite3.SQLITE_CREATE_TABLE,
        sqlite3.SQLITE_CREATE_TEMP_INDEX,
        sqlite3.SQLITE_CREATE_TEMP_TABLE,
        sqlite3.SQLITE_CREATE_TEMP_TRIGGER,
        sqlite3.SQLITE_CREATE_TEMP_VIEW,
        sqlite3.SQLITE_CREATE_TRIGGER,
        sqlite3.SQLITE_CREATE_VIEW,
        sqlite3.SQLITE_CREATE_VTABLE,
        sqlite3.SQLITE_DROP_INDEX,
        sqlite3.SQLITE_DROP_TABLE,
        sqlite3.SQLITE_DROP_TEMP_INDEX,
        sqlite3.SQLITE_DROP_TEMP_TABLE,
        sqlite3.SQLITE_DROP_TEMP_TRIGGER,
        sqlite3.SQLITE_DROP_TEMP_VIEW,
        sqlite3.SQLITE_DROP_TRIGGER,
        sqlite3.SQLITE_DROP_VIEW,
        sqlite3.SQLITE_DROP_VTABLE,
        sqlite3.SQLITE_ALTER_TABLE,
        sqlite3.SQLITE_REINDEX,
    }
)

# Write-capable PRAGMAs observed via SQLITE_PRAGMA with a non-None value arg.
# Unqualified pragmas report schema-name None (treated as main). Schema-qualified
# forms supply main or the attach alias in the fourth argument, so attribution is
# possible for those only (internal).
_WRITE_LOCK_PRAGMAS: frozenset[str] = frozenset({"user_version"})


def _strip_leading_sql_comments(sql: str) -> str:
    """Remove leading line and block comments so BEGIN prefix checks see the verb."""
    text = sql.lstrip()
    while True:
        if text.startswith("--"):
            newline = text.find("\n")
            if newline < 0:
                return ""
            text = text[newline + 1 :].lstrip()
            continue
        if text.startswith("/*"):
            end = text.find("*/")
            if end < 0:
                return ""
            text = text[end + 2 :].lstrip()
            continue
        return text


def _authorizer_target_schema(
    action: int,
    arg1: str | None,
    dbname: str | None,
) -> str | None:
    """Resolve the schema name for a write-lock authorizer callback.

    Most action codes deliver the schema in the fourth argument. ALTER TABLE
    (action 26) delivers it in arg1 and always passes None as dbname
    (internal; [CON-11], [DATA-14]).
    """
    if action == sqlite3.SQLITE_ALTER_TABLE:
        return arg1
    return dbname


def _schema_is_main_file(schema: str | None) -> bool:
    """True when *schema* is the main database file (or unattributed / None)."""
    return schema is None or schema == "main"


def _begin_takes_write_lock(sql: str) -> bool:
    """BEGIN IMMEDIATE/EXCLUSIVE take RESERVED without DML authorizer codes.

    The authorizer reports SQLITE_TRANSACTION with arg ``BEGIN`` for deferred,
    immediate, and exclusive forms alike, so those two lock-taking variants
    still need a narrow text check. Plain BEGIN / BEGIN DEFERRED do not.
    Leading comments before BEGIN, and comments between BEGIN and the
    IMMEDIATE/EXCLUSIVE verb, are stripped so they cannot hide the form
    (internal, internal).
    """
    text = _strip_leading_sql_comments(sql)
    if not text:
        return False
    upper = text.upper()
    if not upper.startswith("BEGIN"):
        return False
    # Re-strip after consuming BEGIN so an interior comment cannot hide
    # IMMEDIATE/EXCLUSIVE (internal).
    rest = _strip_leading_sql_comments(upper[5:])
    return rest.startswith("IMMEDIATE") or rest.startswith("EXCLUSIVE")


class _TrackedCursor(sqlite3.Cursor):
    """Cursor that feeds write-lock bookkeeping back to its connection."""

    def execute(self, sql, parameters=()):
        result = super().execute(sql, parameters)
        conn = self.connection
        if isinstance(conn, _TrackedConnection):
            conn._note_begin_write_lock(sql)
            conn._sync_write_lock_after_statement()
        return result

    def executemany(self, sql, parameters):
        result = super().executemany(sql, parameters)
        conn = self.connection
        if isinstance(conn, _TrackedConnection):
            conn._note_begin_write_lock(sql)
            conn._sync_write_lock_after_statement()
        return result

    def executescript(self, sql_script):
        result = super().executescript(sql_script)
        conn = self.connection
        if isinstance(conn, _TrackedConnection):
            conn._note_begin_write_lock_script(sql_script)
            conn._sync_write_lock_after_statement()
        return result


# Live factory connections for hold-path barrier scans. Blocking-work
# chokepoints (subprocess / network / sleep / flock / lane dispatch) have no
# ``conn`` in hand; they consult this registry via
# :func:`process_holds_write_lock` so a RESERVED hold on a tracked connection
# *owned by the calling thread* fails closed ([CON-18], [CON-21]). Hold-and-
# wait is a property of a thread of control, not of a process — a lock-free
# sibling must proceed. WeakSet so closed/GC'd connections drop out without
# explicit unregister. Guarded by an RLock for multi-threaded writers.
_live_tracked_connections: weakref.WeakSet[_TrackedConnection] = weakref.WeakSet()
_live_tracked_connections_lock = threading.RLock()


class _TrackedConnection(sqlite3.Connection):
    """Connection subclass with an exact write-lock flag ([CON-11], [DATA-14]).

    Tracks whether this connection currently holds a SQLite RESERVED write
    lock from the statements it executes, rather than inferring from
    ``isolation_level`` / ``in_transaction`` (which disagrees with real
    lock state on three of four connection shapes).

    DML/DDL is observed via :meth:`set_authorizer` (resolved plan, not SQL
    text). The authorizer fires at PREPARE; connections are opened with
    ``cached_statements=0`` so every execution re-prepares and the callback
    cannot be skipped by a cached plan (internal; [CON-11], [DATA-14],
    [RES-01], [DATA-16]). ``holds_write_lock`` also requires
    ``in_transaction`` so autocommit and SAVEPOINT RELEASE clear the
    reported value without Python hooks (internal, internal).
    This tracker is a signal, never a gate ([RES-01], [RES-02], [OBS-08]):
    the authorizer always permits.

    Every instance is also registered in the process-level weak set used by
    :func:`process_holds_write_lock` so chokepoints without a ``conn`` can
    still detect a live RESERVED hold on *this thread*.

    Ownership rule (**last-user-owned**): ``_owner_thread_id`` is set at
    construction and refreshed on every statement path (execute /
    executemany / executescript / commit / rollback). The barrier scan
    attributes a RESERVED hold to the thread that last used the connection,
    not merely the creator. Failure mode of creator-owned: a connection
    opened on thread A and used (with ``check_same_thread=False``) on
    thread B would leave B's hold invisible to B's barrier scan. Last-user
    fail-closed residual: after B returns the connection, A can still see
    B's residual ownership until A touches the connection again — safer
    than a false negative under genuine hold-and-wait.
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        # Flag + start stamp are owned exclusively by
        # :meth:`_set_holds_write_lock` after init (finding 14268).
        self._holds_write_lock = False
        self._write_lock_started_at: float | None = None
        # Last-user ownership for thread-scoped barrier scans (see class doc).
        self._owner_thread_id = threading.get_ident()
        # Authorizer covers every statement on this connection (including
        # bootstrap). Failures and unknown codes must not deny.
        self.set_authorizer(self._write_lock_authorizer)
        with _live_tracked_connections_lock:
            _live_tracked_connections.add(self)

    def _set_holds_write_lock(self, value: bool) -> None:
        """Single owner for RESERVED-flag transitions and window timing.

        False→True stamps ``time.monotonic()``; True→False compares elapsed
        against :func:`_reserved_window_budget_seconds` and may emit one
        non-gating observation. Nested True→True does **not** re-stamp
        (outermost window wins). True→False observation is fail-open: any
        exception is swallowed so a broken observer cannot break a write
        ([OBS-08], [RES-01], [RES-02]).
        """
        want = bool(value)
        held = bool(self._holds_write_lock)
        if want and not held:
            # Open the RESERVED window.
            self._holds_write_lock = True
            self._write_lock_started_at = time.monotonic()
            return
        if want and held:
            # Nested / re-entrant hold: keep the outermost start stamp.
            return
        if not want and held:
            # Close the window; observe duration without gating.
            started = self._write_lock_started_at
            self._holds_write_lock = False
            self._write_lock_started_at = None
            try:
                if started is not None:
                    elapsed_s = time.monotonic() - float(started)
                    budget_s = _reserved_window_budget_seconds()
                    if elapsed_s > budget_s:
                        _observe_reserved_window_over_budget(elapsed_s, budget_s)
            except Exception:  # noqa: BLE001 — observation must never break writes
                pass
            return
        # False→False: keep cleared; no observation.
        self._holds_write_lock = False
        self._write_lock_started_at = None

    def _note_thread_owner(self) -> None:
        """Record the calling thread as the last user of this connection."""
        self._owner_thread_id = threading.get_ident()

    def _write_lock_authorizer(
        self,
        action: int,
        arg1: str | None,
        arg2: str | None,
        dbname: str | None,
        source: str | None,
    ) -> int:
        try:
            # Only main-file writes set the flag. Schema resolution is
            # action-code specific via :func:`_authorizer_target_schema`.
            # Unqualified pragmas report schema None (treated as main);
            # qualified forms supply main or the attach alias.
            # False positives here become over-refusals when the guard flips
            # (internal, internal, internal, internal;
            # [CON-11], [DATA-14], [OBS-08]).
            if action == sqlite3.SQLITE_PRAGMA:
                # Write form: arg1 is pragma name, arg2 is the assigned value
                # (string) on a write and None on a read (internal,
                # internal). Gate on schema the same way as other codes
                # (internal).
                if (
                    arg2 is not None
                    and (arg1 or "").lower() in _WRITE_LOCK_PRAGMAS
                    and _schema_is_main_file(_authorizer_target_schema(action, arg1, dbname))
                ):
                    self._set_holds_write_lock(True)
            elif action in _WRITE_LOCK_AUTHORIZER_ACTIONS:
                if _schema_is_main_file(_authorizer_target_schema(action, arg1, dbname)):
                    self._set_holds_write_lock(True)
        except Exception:  # noqa: BLE001 — tracker must never break statements
            pass
        return sqlite3.SQLITE_OK

    def _note_begin_write_lock(self, sql: str) -> None:
        if _begin_takes_write_lock(sql):
            self._set_holds_write_lock(True)

    def _note_begin_write_lock_script(self, sql_script: str) -> None:
        for part in sql_script.split(";"):
            if part.strip():
                self._note_begin_write_lock(part)

    def _sync_write_lock_after_statement(self) -> None:
        # Dual release mechanism (internal): clears the tracked flag
        # when no transaction remains (autocommit DML, SAVEPOINT RELEASE).
        # The peer is the ``in_transaction`` conjunct inside
        # :func:`holds_write_lock`, which masks a stale flag even if this
        # sync never ran. Keep both; removing either alone leaves the
        # remaining pins green and the redundancy invisible.
        if not self.in_transaction:
            self._set_holds_write_lock(False)

    def execute(self, sql, parameters=()):
        self._note_thread_owner()
        result = super().execute(sql, parameters)
        self._note_begin_write_lock(sql)
        self._sync_write_lock_after_statement()
        return result

    def executemany(self, sql, parameters):
        self._note_thread_owner()
        result = super().executemany(sql, parameters)
        self._note_begin_write_lock(sql)
        self._sync_write_lock_after_statement()
        return result

    def executescript(self, sql_script):
        self._note_thread_owner()
        result = super().executescript(sql_script)
        self._note_begin_write_lock_script(sql_script)
        # sqlite3.executescript issues COMMIT around the script; clear when
        # no transaction remains so the flag matches post-script state.
        self._sync_write_lock_after_statement()
        return result

    def commit(self) -> None:
        self._note_thread_owner()
        super().commit()
        self._set_holds_write_lock(False)

    def rollback(self) -> None:
        self._note_thread_owner()
        super().rollback()
        self._set_holds_write_lock(False)

    def cursor(self, factory=None):
        if factory is None:
            factory = _TrackedCursor
        return super().cursor(factory)


def holds_write_lock(conn: sqlite3.Connection) -> bool:
    """Return whether *conn* is tracked and currently holds a write lock.

    Requires both the authorizer/BEGIN flag and an open transaction: a write
    that already released RESERVED (autocommit, RELEASE) reports False
    without a Python-level commit/rollback hook ([CON-22], [OBS-08]).

    Untracked connections (e.g. plain ``sqlite3.connect`` results) return
    False rather than raising ([RES-06]).
    """
    if isinstance(conn, _TrackedConnection):
        # Dual release mechanism (internal): the ``in_transaction``
        # gate masks a stale flag when no transaction is open. The peer is
        # :meth:`_TrackedConnection._sync_write_lock_after_statement`, which
        # clears the flag itself after each statement. Keep both; removing
        # either alone leaves the remaining pins green and the redundancy
        # invisible.
        return bool(conn._holds_write_lock) and bool(conn.in_transaction)
    return False


def process_holds_write_lock() -> bool:
    """Return whether *this thread* currently holds RESERVED on a tracked connection.

    Blocking-work chokepoints (subprocess, network, sleep, flock, lane
    dispatch) rarely hold a ``conn`` reference. They call this view, derived
    from the live :class:`_TrackedConnection` weak set filtered to
    connections last used by ``threading.get_ident()`` — no durable table,
    no sidecar flock ([L067], [L313], [L478]).

    Granularity is thread, not process: [CON-21] hold-and-wait is a property
    of a thread of control. A sibling thread that holds RESERVED must not
    refuse a lock-free caller. A thread that genuinely holds RESERVED and
    then enters a chokepoint still fails closed.

    Thread-safe: the weak set is guarded by an RLock; each connection's
    authorizer flag is then read without holding the set lock so a long
    SQL statement cannot stall other threads registering connections.
    Fail-open on a dead/closed handle (treat as not holding) so a single
    broken connection cannot force a permanent raise.

    Untracked connections (plain ``sqlite3.connect``) are invisible here —
    same documented limit as :func:`holds_write_lock` returning False.
    """
    my_tid = threading.get_ident()
    with _live_tracked_connections_lock:
        conns = list(_live_tracked_connections)
    for conn in conns:
        try:
            if getattr(conn, "_owner_thread_id", None) != my_tid:
                continue
            if holds_write_lock(conn):
                return True
        except (sqlite3.Error, AttributeError, ValueError, TypeError):
            continue
    return False


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
    re-attempts once after a lock-time reaper pass. busy_timeout is set
    to :data:`HANDOFF_SQLITE_BUSY_TIMEOUT_MS` in
    :func:`_connect_handoff_sqlite` — do not re-add.
    """
    config = get_runtime_config()
    config.state_dir.mkdir(parents=True, exist_ok=True)
    # Proactive dead/stale writer cleanup (sidecar registry; never touches a
    # live PID with a fresh heartbeat).
    # Connect-path reaper: non-blocking registry acquire ([L478]). Contention
    # or a held write lock degrades to an observable skip event, not a raise
    # swallowed by a broad except ([OBS-08], [RES-06]).
    try:
        from .db_writer_liveness import reap_stale_db_writers

        reap_stale_db_writers(config.db_path, non_blocking=True)
    except (OSError, RuntimeError, ValueError, TypeError, json.JSONDecodeError):
        # Narrow: genuine reaper faults still surface in logs; barrier /
        # contention skips are returned as events, not raised.
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

            reap_stale_db_writers(config.db_path, non_blocking=True)
        except (OSError, RuntimeError, ValueError, TypeError, json.JSONDecodeError):
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


# Bound for handoff SQLite lock waits ([RES-02]). Matches the codemap lease /
# runner convention (30s). Set on both sqlite3.connect(timeout=...) and
# PRAGMA busy_timeout so the effective wait does not depend on which of the
# two wins for a given Python/SQLite pairing.
HANDOFF_SQLITE_BUSY_TIMEOUT_MS = 30000

# WAL + NORMAL (1): a power loss can lose the most recent committed
# transactions but cannot corrupt the database. FULL (2, SQLite default)
# fsyncs every commit; under four concurrent Claude sessions that fsync is
# the stall. Documented SQLite tradeoff for a coordination/telemetry DB
# (https://www.sqlite.org/pragma.html#pragma_synchronous — "WAL mode is
# safe from corruption with synchronous=NORMAL"). Per-connection — not
# persistent in the file — unlike journal_mode. Must be set on every writer
# handle before it is handed out. Not applied on mode=ro: setting it takes a
# pager lock (measured 30.018s under DELETE-journal BEGIN EXCLUSIVE).
HANDOFF_SQLITE_SYNCHRONOUS = 1

# Negative cache_size is KiB. Default -2000 is 2 MiB against a measured
# 284 MB operator DB. 64 MiB (-65536) holds the hot coordination set:
# review_findings 39.5 MB + findings_fts_content 19.2 MB + lane_messages
# 9.9 MB ≈ 69 MB. concept_embeddings (130.9 MB) is scan/rebuild oriented
# and is left to the OS page cache rather than pinning ~131 MB per
# connection (four sessions × 131 MB would dwarf the stall we are fixing).
# Per-connection, not persistent.
HANDOFF_SQLITE_CACHE_SIZE = -65536

# In-process counter when a mode=ro connect skips cache_size under a locked
# pager ([OBS-08]). Tests / process diagnostics only — no DB writes.
readonly_cache_size_skipped_count = 0

# Connect-time ANALYZE must not burn the factory 30s busy_timeout [RES-17].
# Stale sqlite_stat1 is a performance degradation, not a correctness break
# ([SECD-05 inverted]). Zero matches the RO cache_size fail-open pattern.
HANDOFF_PLANNER_STATS_BUSY_TIMEOUT_MS = 0

# In-process counter when connect-time ANALYZE skips under BUSY/LOCKED
# ([OBS-08]). Tests / process diagnostics only — no DB writes.
review_findings_analyze_skipped_count = 0

# LOAD/ADD/STORE on a module int is not atomic across MCP worker threads.
# Both OBS-08 skip counters share this lock so concurrent opens cannot
# under-count the contention they exist to record.
_obs_counter_lock = threading.Lock()


# RES-06 bounded retry for PRAGMA journal_mode=WAL only.
# Probe (docs/tech-debt/probe_journalmode_race_v8.py) measured COLD delete->WAL
# conversion failures with waits of 8-23ms against a 30000ms busy_timeout: SQLite
# returns SQLITE_BUSY immediately for exclusive journal-mode conversion and never
# invokes the busy handler, so no busy_timeout value can protect this statement
# (FACTORY-JOURNALMODE-RACE-01). RES-01 licenses retry: journal_mode=WAL converges
# to a state rather than accumulating an effect. RES-02: retry only lock classes
# via is_lock_contention_error (BUSY/LOCKED including extended result codes);
# other errors propagate unretried. CARD-07: after
# the attempt ceiling, re-raise — never return a connection that skipped WAL.
# PERF-16: delays escalate from 5ms, cap at 40ms; 6 attempts / ~115ms cumulative
# sleep covers the measured 8-23ms contention window with margin while staying
# far inside any caller's patience. Do NOT read-then-conditionally-write
# (CON-11 / CON-05 check-then-act atomicity violation; inert on COLD arm).
JOURNAL_MODE_WAL_MAX_ATTEMPTS = 6
JOURNAL_MODE_WAL_RETRY_DELAYS_MS: tuple[int, ...] = (5, 10, 20, 40, 40)


def _lock_retry_sleep(attempt: int) -> None:
    """Bounded lock backoff. Must not use blocking_sleep (held-lock barrier)."""
    delay_ms = JOURNAL_MODE_WAL_RETRY_DELAYS_MS[min(attempt, len(JOURNAL_MODE_WAL_RETRY_DELAYS_MS) - 1)]
    # Bounded backoff [CARD-09]. Must NOT use blocking_sleep: under a held
    # write lock that raises WriteLockHeldAcrossBlockingWorkError from
    # inside the handler, destroying the retry and masking last_exc.
    time.sleep(delay_ms / 1000.0)


def _apply_journal_mode_wal(conn: sqlite3.Connection) -> None:
    """Issue ``PRAGMA journal_mode=WAL`` with RES-06 bounded lock retry.

    Single enforcement point for this factory ([ARCH-13]). Retries lock
    errors *and* silent no-ops (SQLite returns the current mode with no
    error when conversion cannot take effect). Re-raises the original lock
    exception after the attempt ceiling, or refuses with RuntimeError when
    the reported mode is not WAL ([CARD-07] — never hand out a non-WAL
    writer).
    """
    last_exc: BaseException | None = None
    last_mode: str | None = None
    for attempt in range(JOURNAL_MODE_WAL_MAX_ATTEMPTS):
        try:
            row = conn.execute("PRAGMA journal_mode=WAL;").fetchone()
            mode = str(row[0]).lower() if row is not None else ""
            if mode == "wal":
                return
            last_mode = mode
            last_exc = None
        except sqlite3.OperationalError as exc:
            if not is_lock_contention_error(exc):
                raise
            last_exc = exc
        if attempt + 1 >= JOURNAL_MODE_WAL_MAX_ATTEMPTS:
            break
        _lock_retry_sleep(attempt)
    if last_exc is not None:
        raise last_exc
    raise RuntimeError(
        f"PRAGMA journal_mode=WAL did not take effect (reported {last_mode!r}); "
        "refusing to hand out a non-WAL writer [CARD-07]"
    )


def _apply_handoff_session_pragmas(conn: sqlite3.Connection) -> None:
    """Apply writer session pragmas that are not persisted in the database file.

    ``journal_mode`` IS persistent and is owned by the WAL retry path; this
    helper must not change it. ``synchronous`` and ``cache_size`` are
    per-connection (like ``busy_timeout``) and must be set on the same handle
    that will write.

    ``HANDOFF_SQLITE_SYNCHRONOUS`` (NORMAL) is applied only after a WAL
    readback. SQLite documents that NORMAL is crash-safe in WAL and is
    **not** equivalent in DELETE/TRUNCATE/PERSIST — a no-op journal_mode
    conversion plus unconditional NORMAL would be a new corruption window.
    Non-WAL handles keep SQLite's default FULL.

    mmap_size is intentionally unset: the stall is commit fsync under WAL
    FULL, which this helper addresses with synchronous=NORMAL. mmap_size
    does not reduce fsyncs and was not measured to help this workload.
    """
    row = conn.execute("PRAGMA journal_mode;").fetchone()
    mode = str(row[0]).lower() if row is not None else ""
    if mode == "wal":
        conn.execute(f"PRAGMA synchronous={HANDOFF_SQLITE_SYNCHRONOUS};")
    conn.execute(f"PRAGMA cache_size={HANDOFF_SQLITE_CACHE_SIZE};")


def _apply_readonly_cache_size(conn: sqlite3.Connection) -> None:
    """Set ``cache_size`` on a ``mode=ro`` handle without burning busy_timeout.

    Setting ``PRAGMA cache_size`` (and ``synchronous``) takes a pager lock.
    Under DELETE journal + ``BEGIN EXCLUSIVE`` that wait uses whatever
    busy_timeout is already installed — the factory's 30s — before a caller
    such as CURRENT_TASK fingerprinting can narrow it to 1s (measured
    20.060s for cache_size, 30.018s for synchronous). WAL readers do not
    contend, so production handoff.db applies cache_size instantly.

    Temporarily set busy_timeout=0 so a locked pager fails immediately,
    then restore the contractual 30s. Skip the cache_size assignment on
    lock errors; do not skip other failures. The skip is an observable
    event ([OBS-08]): warning log + ``readonly_cache_size_skipped_count``.
    ``synchronous`` is not applied on RO handles: readers never commit,
    and the same pager lock would stall the 1s fingerprint path.
    """
    global readonly_cache_size_skipped_count
    conn.execute("PRAGMA busy_timeout=0;")
    try:
        conn.execute(f"PRAGMA cache_size={HANDOFF_SQLITE_CACHE_SIZE};")
    except sqlite3.OperationalError as exc:
        if not is_lock_contention_error(exc):
            raise
        with _obs_counter_lock:
            readonly_cache_size_skipped_count += 1
        _log.warning(
            "PRAGMA cache_size skipped on read-only connect (pager locked); leaving SQLite default cache [OBS-08]"
        )
    finally:
        conn.execute(f"PRAGMA busy_timeout={HANDOFF_SQLITE_BUSY_TIMEOUT_MS};")


def _connect_handoff_sqlite(db_path) -> sqlite3.Connection:
    conn = sqlite3.connect(
        db_path,
        timeout=HANDOFF_SQLITE_BUSY_TIMEOUT_MS / 1000.0,
        factory=_TrackedConnection,
        # Authorizer fires at prepare time; a prepared-statement cache would
        # let a repeated write escape detection. Disable it unconditionally
        # so every execution re-prepares and the callback always runs
        # (internal; ~2.35 µs/statement).
        cached_statements=0,
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON;")
    # busy_timeout before journal_mode so the connection carries configured
    # patience from the earliest possible statement. Ordering does NOT fix the
    # journal_mode race (exclusive-access statements bypass the busy handler);
    # it is correctness of ordering only, not the remedy (RES-06 retry is).
    conn.execute(f"PRAGMA busy_timeout={HANDOFF_SQLITE_BUSY_TIMEOUT_MS};")
    _apply_journal_mode_wal(conn)
    # synchronous=NORMAL is the WAL-mode latency lever; apply after WAL so
    # the pairing is explicit. cache_size is session-scoped on this handle.
    _apply_handoff_session_pragmas(conn)
    return conn


def connect_handoff_db(db_path, *, read_only: bool = False) -> sqlite3.Connection:
    """Public handoff.db connection factory ([ARCH-13], [RES-02]).

    Structural busy_timeout contract for every handoff.db handle. Prefer this
    over bare ``sqlite3.connect``: Python's default ``timeout`` is 5.0s, which
    *does* install a busy timeout of 5000 ms — the defect is the wrong bound
    relative to this package's 30s contract (``HANDOFF_SQLITE_BUSY_TIMEOUT_MS``),
    not a missing pragma. Passing ``timeout=30.0`` to ``sqlite3.connect`` yields
    30000 ms; the factory also applies ``PRAGMA busy_timeout`` and the rest of
    the handoff open contract (WAL for writers, row_factory, foreign_keys,
    per-connection ``synchronous`` / ``cache_size``).

    When ``read_only`` is True the file is opened via URI ``mode=ro`` and
    ``journal_mode`` is left alone (WAL cannot be set on a read-only handle and
    must not be mutated by diagnostic/lease readers). ``PRAGMA busy_timeout``
    still applies so readers wait out short writers. ``cache_size`` is applied
    with a lock fail-open so a DELETE-journal exclusive holder cannot burn
    the 30s factory timeout; ``synchronous`` is writer-only.
    """
    if read_only:
        from pathlib import Path

        path = Path(db_path).expanduser().resolve()
        uri = f"{path.as_uri()}?mode=ro"
        conn = sqlite3.connect(
            uri,
            uri=True,
            timeout=HANDOFF_SQLITE_BUSY_TIMEOUT_MS / 1000.0,
            cached_statements=0,
        )
        conn.row_factory = sqlite3.Row
        # No journal_mode=WAL: SQLite refuses WAL mode changes on a read-only
        # connection. busy_timeout still applies so RO readers wait.
        conn.execute(f"PRAGMA busy_timeout={HANDOFF_SQLITE_BUSY_TIMEOUT_MS};")
        _apply_readonly_cache_size(conn)
        return conn
    return _connect_handoff_sqlite(db_path)


def _run_exclusive_sqlite_statement(conn: sqlite3.Connection, sql: str) -> None:
    """Execute *sql* at busy_timeout=0 with bounded BUSY/LOCKED retry.

    Exclusive statements (VACUUM, journal_mode conversion) may bypass the
    busy handler or wait the full factory timeout. Fail fast, retry lock
    contention with the journal_mode backoff, then restore the previous
    timeout with a readback. Restore failure is WARNING + raise — never
    return a handle left at busy_timeout=0.
    """
    previous_busy = int(conn.execute("PRAGMA busy_timeout").fetchone()[0])
    work_exc: BaseException | None = None
    restore_exc: sqlite3.Error | None = None
    try:
        conn.execute("PRAGMA busy_timeout=0;")
        last_exc: sqlite3.OperationalError | None = None
        for attempt in range(JOURNAL_MODE_WAL_MAX_ATTEMPTS):
            try:
                conn.execute(sql)
                last_exc = None
                break
            except sqlite3.OperationalError as exc:
                if not is_lock_contention_error(exc):
                    raise
                last_exc = exc
                if attempt + 1 >= JOURNAL_MODE_WAL_MAX_ATTEMPTS:
                    break
                _lock_retry_sleep(attempt)
        if last_exc is not None:
            raise RuntimeError(
                f"{sql.strip()} refused: SQLITE_BUSY/LOCKED after bounded retry; "
                "live writers hold RESERVED — drain MCP writer sessions and retry"
            ) from last_exc
    except Exception as exc:
        work_exc = exc
    finally:
        try:
            conn.execute(f"PRAGMA busy_timeout={previous_busy};")
            restored = int(conn.execute("PRAGMA busy_timeout").fetchone()[0])
            if restored != previous_busy:
                raise sqlite3.OperationalError(
                    f"PRAGMA busy_timeout restore readback {restored} != intended {previous_busy}"
                )
        except sqlite3.Error as exc:
            restore_exc = exc
            _log.warning(
                "failed to restore PRAGMA busy_timeout (previous=%s intended=%s) "
                "after exclusive statement %r: %s [OBS-08]",
                previous_busy,
                previous_busy,
                sql,
                exc,
            )
    if restore_exc is not None:
        raise RuntimeError(
            f"exclusive statement {sql!r} left busy_timeout unrestored "
            f"(intended {previous_busy} ms); connection is not healthy"
        ) from restore_exc
    if work_exc is not None:
        raise work_exc


def vacuum_handoff_connection(conn: sqlite3.Connection) -> dict[str, int]:
    """Run VACUUM on an existing connection. Refuses an open transaction.

    VACUUM takes an exclusive lock and rewrites the whole file. It cannot
    run inside BEGIN, and this package does not invoke it at connect —
    callers must call :func:`vacuum_handoff_db` (or this helper) on purpose.
    After VACUUM, this helper also refreshes ``review_findings`` planner
    stats (fail-closed ``ANALYZE`` through
    :func:`_run_exclusive_sqlite_statement`) so ``sqlite_stat1`` names
    ``idx_review_findings_finding_id`` without putting that write on the
    connection-open path (DBINTG-H-02). A peer that takes BEGIN IMMEDIATE
    in the VACUUM/ANALYZE gap gets the same bounded live-writers envelope
    as VACUUM, not a raw factory-timeout ``OperationalError``.

    Why full VACUUM rather than ``PRAGMA incremental_vacuum``: existing
    operator DBs have ``auto_vacuum=NONE`` (0). Switching to INCREMENTAL
    itself requires a full VACUUM to rewrite the database header, so
    incremental_vacuum is not a cheaper first step on the live 284 MB
    file. Sibling lanes are about to delete ~98 MB of embeddings plus
    tens of MB of review findings in a batch; one exclusive VACUUM after
    those deletes returns that space to the filesystem. incremental_vacuum
    would only help *subsequent* steady-state deletes after that conversion,
    which this package does not own.
    """
    if conn.in_transaction:
        raise RuntimeError(
            "VACUUM refused: connection has an open transaction; "
            "VACUUM takes an exclusive lock and must not run inside BEGIN"
        )
    before = int(conn.execute("PRAGMA page_count").fetchone()[0])
    # VACUUM requires autocommit. The default isolation_level="" wrapper
    # may emit an implicit BEGIN around DML/DDL; force autocommit for the
    # statement and restore afterwards. Re-check in_transaction after the
    # switch in case the page_count query opened one. Exclusive lock
    # retry lives in :func:`_run_exclusive_sqlite_statement`.
    previous_isolation = conn.isolation_level
    try:
        conn.isolation_level = None
        if conn.in_transaction:
            raise RuntimeError(
                "VACUUM refused: connection has an open transaction; "
                "VACUUM takes an exclusive lock and must not run inside BEGIN"
            )
        _run_exclusive_sqlite_statement(conn, "VACUUM")
        # Same autocommit window: ANALYZE after isolation restore would sit
        # in an implicit transaction that sqlite3.close() rolls back.
        # fail_open=False routes through the exclusive-statement bound.
        _refresh_review_findings_planner_stats(conn, fail_open=False)
    finally:
        conn.isolation_level = previous_isolation
    after = int(conn.execute("PRAGMA page_count").fetchone()[0])
    return {"page_count_before": before, "page_count_after": after}


def vacuum_handoff_db(db_path) -> dict[str, int]:
    """Open a writer connection to *db_path*, VACUUM it, and close.

    Must not be called from connection-open. See
    :func:`vacuum_handoff_connection` for the auto_vacuum=NONE rationale.
    """
    conn = _connect_handoff_sqlite(db_path)
    try:
        return vacuum_handoff_connection(conn)
    finally:
        conn.close()


# Dropped embed kinds purged at open without a HANDOFF_SCHEMA_VERSION bump
# (implementation note S2 — next version is claimed by implementation note).
_PURGED_CONCEPT_ENTITY_KINDS: tuple[str, ...] = ("handoff_state.task_plan_path",)


def _transfer_interned_embedding_payload(conn: sqlite3.Connection, entity_kind: str, entity_id: str) -> bool:
    """If this row owns an interned payload, move it to a remaining sharer.

    Duplicate ``(text_hash, model_id)`` rows store the BLOB once; sharers keep
    an empty placeholder. Deleting the owner without transferring leaves
    interned rows unresolvable. Returns True when a payload was moved.
    Numpy-free so connection-open purge can call it without the embeddings extra.
    """
    row = conn.execute(
        """
        SELECT text_hash, model_id, vector
        FROM concept_embeddings
        WHERE entity_kind = ? AND entity_id = ?
        """,
        (entity_kind, entity_id),
    ).fetchone()
    if row is None:
        return False
    blob = bytes(row[2] or b"")
    if not blob:
        return False
    other = conn.execute(
        """
        SELECT entity_kind, entity_id
        FROM concept_embeddings
        WHERE text_hash = ? AND model_id = ?
          AND NOT (entity_kind = ? AND entity_id = ?)
          AND LENGTH(vector) = 0
        LIMIT 1
        """,
        (str(row[0]), str(row[1]), entity_kind, entity_id),
    ).fetchone()
    if other is None:
        return False
    conn.execute(
        """
        UPDATE concept_embeddings
        SET vector = ?
        WHERE entity_kind = ? AND entity_id = ?
        """,
        (blob, str(other[0]), str(other[1])),
    )
    return True


def _purge_dropped_concept_embeddings(conn: sqlite3.Connection) -> None:
    """Idempotent cleanup of retired ``concept_embeddings`` kinds (no schema bump).

    Runs on every prepared open after schema is ready. Guarded by a read-only
    existence probe so a steady-state open (nothing left to purge) issues NO
    write: the unconditional per-open ``DELETE`` turned every reader into a
    writer and caused ``database is locked`` contention under nested handoff
    connections (regressed ``test_row25`` after 0140 S2; [RES-07]). Must not
    participate in ``HANDOFF_SCHEMA_VERSION`` migration machinery — implementation note
    owns the next version.

    Before DELETE, owned interned payloads are transferred to a remaining
    sharer so ranking can still resolve empty placeholders.
    """
    if not _sqlite_objects_exist(conn, "table", frozenset({"concept_embeddings"})):
        return
    placeholders = ", ".join("?" for _ in _PURGED_CONCEPT_ENTITY_KINDS)
    present = conn.execute(
        f"SELECT 1 FROM concept_embeddings WHERE entity_kind IN ({placeholders}) LIMIT 1",
        _PURGED_CONCEPT_ENTITY_KINDS,
    ).fetchone()
    if present is None:
        return
    for kind in _PURGED_CONCEPT_ENTITY_KINDS:
        owners = conn.execute(
            """
            SELECT entity_kind, entity_id
            FROM concept_embeddings
            WHERE entity_kind = ? AND LENGTH(vector) > 0
            """,
            (kind,),
        ).fetchall()
        for entity_kind, entity_id in owners:
            _transfer_interned_embedding_payload(conn, str(entity_kind), str(entity_id))
        conn.execute(
            "DELETE FROM concept_embeddings WHERE entity_kind = ?",
            (kind,),
        )
    # Own short transaction: commit (or leave no open write) before the
    # prepared connection is yielded. An uncommitted DELETE leaves RESERVED
    # held across every deferred open, so resolve-first write paths trip the
    # external-resolution guard and roll the purge back (sticky) — DURREV-LOCK-1
    # / [CON-18]. Steady-state opens (no purge work) never reach here.
    conn.commit()


def _prepare_handoff_connection(conn: sqlite3.Connection) -> sqlite3.Connection:
    """Bootstrap schema, enforce version match, ensure FTS. Mutates ``conn``."""
    if _handoff_schema_bootstrapped is False or not _handoff_schema_bootstrapped(conn):
        _bootstrap_handoff_schema(conn)
        if not _sqlite_objects_exist(conn, "table", _HANDOFF_REQUIRED_TABLES) or not _required_columns_present(conn):
            # Bootstrap fail-open: base schema is absent/unstamped. Skip the
            # FTS ensure — creating FTS tables against missing base tables
            # would commit orphans and confuse the next bootstrap retry.
            # Schema-version floor check is deferred until a successful stamp
            # (degraded opens must still self-heal on the next attempt).
            #
            # A v33 stamp with required tables/columns present is *not*
            # degraded: that is the pending-index state after a locked
            # CREATE INDEX. Fall through to the post-COMMIT ensure below.
            _log.warning(
                "handoff schema bootstrap incomplete (lock contention or structure shortfall) -- "
                "returning degraded connection; FTS ensure skipped, next open retries"
            )
            return conn
    _assert_schema_version_compatible(conn)
    # Indexes are not in _HANDOFF_REQUIRED_*; a stamped-current DB missing
    # idx_review_findings_finding_id would otherwise skip bootstrap forever.
    # This ensure is outside BEGIN IMMEDIATE (bootstrap already committed)
    # and isolation_level has already been restored — CREATE INDEX is not
    # autocommit here. SQLITE_BUSY is in-band (another opener's btree
    # build). Fail-open like _ensure_handoff_fts: leave the connection
    # usable and retry CREATE INDEX IF NOT EXISTS on the next open. Wrap
    # the presence probe too so a locked PRAGMA index_list cannot escape
    # connection setup. Do not let _open_db_connection's single lock-path
    # retry turn this DDL into a hard open failure.
    try:
        _ensure_review_findings_finding_id_index(conn)
        if _review_findings_finding_id_index_present(conn) and handoff_schema_version(conn) < HANDOFF_SCHEMA_VERSION:
            _stamp_handoff_schema_version(conn, HANDOFF_SCHEMA_VERSION)
    except sqlite3.OperationalError as exc:
        if is_lock_contention_error(exc):
            _log.warning(
                "DB locked creating idx_review_findings_finding_id during prepare; "
                "CREATE INDEX IF NOT EXISTS will retry on next open"
            )
        else:
            raise
    # As with the finding-id btree above, the readiness predicate deliberately
    # excludes indexes. Repair this load-bearing unique index on every open so
    # a current-stamped database cannot leave report writers unusable.
    try:
        _ensure_worker_reports_delivery_id_index(conn)
    except sqlite3.OperationalError as exc:
        if is_lock_contention_error(exc):
            _log.warning(
                "DB locked creating idx_worker_reports_delivery_id_unique during prepare; "
                "CREATE UNIQUE INDEX IF NOT EXISTS will retry on next open"
            )
        else:
            raise
    if conn.in_transaction:
        conn.commit()
    _ensure_handoff_fts(conn)
    # implementation note S2: drop unresolvable path embeddings without a version bump.
    # Purge commits its own short transaction when it has work; FTS backfill
    # (and any future prep step) may still leave an uncommitted write.
    _purge_dropped_concept_embeddings(conn)
    # Invariant: a prepared deferred open must not yield holding RESERVED
    # ([CON-18] / REV-HARM-01). Any preparation step that wrote without
    # committing (e.g. _backfill_handoff_fts INSERT into an empty FTS twin)
    # is released here generically — special-casing one step reopens on the
    # next prep write. Degraded-bootstrap returns above before FTS/purge.
    if conn.in_transaction:
        conn.commit()
    return conn


def _open_bounded_roster_read_connection(
    db_path: str | os.PathLike[str], *, busy_timeout_ms: int
) -> sqlite3.Connection:
    """Open an existing handoff DB for a bounded, non-bootstrapping roster read.

    This intentionally bypasses the production connection factories: it must
    neither create/bootstrap a missing database nor inherit their 30-second
    write-path busy timeout.
    """
    path = Path(os.path.abspath(db_path))
    if not path.exists():
        raise sqlite3.OperationalError(f"handoff database does not exist: {path}")
    uri = f"{path.as_uri()}?mode=rw"
    conn = sqlite3.connect(uri, uri=True, timeout=busy_timeout_ms / 1000)
    try:
        conn.row_factory = sqlite3.Row
        conn.execute(f"PRAGMA busy_timeout={busy_timeout_ms}")
    except BaseException:
        conn.close()
        raise
    return conn


_ROSTER_RESOLVER_WORKER_GUARD = threading.Lock()
_ROSTER_RESOLVER_WORKER: threading.Thread | None = None


def bounded_resolve_active_task_ref(
    db_path: str | os.PathLike[str], *, deadline_s: float = 2.0
) -> tuple[str | None, bool, int, int | None, int | None, tuple[str, ...] | None, str | None]:
    """Resolve the active task on a daemon worker, taking the boot floor on delay.

    Returns ``(task_ref, floor_taken, elapsed_ms, open_ms, query_ms,
    tiebreak_candidates, exception_class)``. A
    missing/locked database, an unresolved task, or expiry of ``deadline_s``
    yields ``task_ref=None`` and ``floor_taken=True``.
    """
    global _ROSTER_RESOLVER_WORKER

    started = time.monotonic()
    budget_s = max(0.0, float(deadline_s))
    deadline = started + budget_s
    if budget_s == 0:
        return (None, True, 0, None, None, None, None)

    state: dict[str, object] = {}
    state_guard = threading.Lock()

    def _resolve() -> None:
        global _ROSTER_RESOLVER_WORKER

        conn: sqlite3.Connection | None = None
        try:
            open_started = time.monotonic()
            if open_started >= deadline:
                with state_guard:
                    state["floor_taken"] = True
                return
            remaining_ms = int((deadline - open_started) * 1000)
            conn = _open_bounded_roster_read_connection(db_path, busy_timeout_ms=remaining_ms)
            with state_guard:
                state["connection"] = conn
                state["open_ms"] = int((time.monotonic() - open_started) * 1000)
            query_started = time.monotonic()
            if query_started >= deadline:
                with state_guard:
                    state["floor_taken"] = True
                return
            # Opening and resolving share one wall-clock budget.  The connect
            # timeout only bounds lock waits during open; do not give the
            # query a fresh copy of that original allowance after open has
            # already consumed part (or all) of it.
            query_remaining_ms = int((deadline - query_started) * 1000)
            conn.execute(f"PRAGMA busy_timeout={query_remaining_ms}")
            from .shared_primitives import resolve_active_task_ref_for_hook  # noqa: PLC0415

            if time.monotonic() >= deadline:
                with state_guard:
                    state["floor_taken"] = True
                return
            resolution = resolve_active_task_ref_for_hook(conn, strict=False)
            completed = time.monotonic()
            with state_guard:
                state["query_ms"] = int((completed - query_started) * 1000)
                if completed >= deadline:
                    state["floor_taken"] = True
                else:
                    state["task_ref"] = resolution.task_ref
                    state["tiebreak_candidates"] = resolution.tiebreak_candidates
        except (ValueError, sqlite3.Error) as exc:
            with state_guard:
                state["floor_taken"] = True
                state["exception_class"] = type(exc).__name__
        finally:
            try:
                if conn is not None:
                    conn.close()
            finally:
                with _ROSTER_RESOLVER_WORKER_GUARD:
                    if _ROSTER_RESOLVER_WORKER is threading.current_thread():
                        _ROSTER_RESOLVER_WORKER = None

    resolver_context = contextvars.copy_context()
    with _ROSTER_RESOLVER_WORKER_GUARD:
        active_worker = _ROSTER_RESOLVER_WORKER
        if active_worker is not None and active_worker.is_alive():
            return (None, True, int((time.monotonic() - started) * 1000), None, None, None, None)
        worker = threading.Thread(
            target=resolver_context.run,
            args=(_resolve,),
            name="workbay-roster-resolver",
            daemon=True,
        )
        _ROSTER_RESOLVER_WORKER = worker
        worker.start()
    worker.join(max(0.0, deadline - time.monotonic()))
    if worker.is_alive():
        # Request cancellation of an in-flight read when supported. A worker
        # that remains wedged retains the single process-wide resolver slot,
        # so later boots take the floor without accumulating more readers.
        with state_guard:
            conn = state.get("connection")
            open_ms = state.get("open_ms")
        interrupt = getattr(conn, "interrupt", None)
        if callable(interrupt):
            try:
                interrupt()
            except Exception:  # noqa: BLE001 — cancellation is best effort
                pass
        return (
            None,
            True,
            int((time.monotonic() - started) * 1000),
            open_ms if isinstance(open_ms, int) else None,
            None,
            None,
            None,
        )

    with state_guard:
        accepted_at = time.monotonic()
        task_ref = state.get("task_ref")
        floor_taken = bool(state.get("floor_taken")) or not isinstance(task_ref, str)
        open_ms = state.get("open_ms")
        query_ms = state.get("query_ms")
        tiebreak_candidates = state.get("tiebreak_candidates")
        exception_class = state.get("exception_class")
    elapsed_ms = int((accepted_at - started) * 1000)
    if accepted_at >= deadline:
        return (
            None,
            True,
            elapsed_ms,
            open_ms if isinstance(open_ms, int) else None,
            query_ms if isinstance(query_ms, int) else None,
            None,
            None,
        )

    return (
        task_ref if isinstance(task_ref, str) else None,
        floor_taken,
        elapsed_ms,
        open_ms if isinstance(open_ms, int) else None,
        query_ms if isinstance(query_ms, int) else None,
        tiebreak_candidates if isinstance(tiebreak_candidates, tuple) else None,
        exception_class if isinstance(exception_class, str) else None,
    )


@contextmanager
def _get_db_connection(*, begin_immediate: bool = False) -> Iterator[sqlite3.Connection]:
    # sqlite3.Connection as a context manager only commits/rolls back — it
    # does NOT close the file handle. Wrapping the connection in this
    # contextmanager guarantees close-on-exit so callers using
    # `with _get_db_connection() as conn:` do not leak file descriptors.
    # Auto-commit/rollback is preserved to match the prior raw-connection
    # context-manager semantics.
    #
    # begin_immediate=False (default): Python's implicit DEFERRED transaction
    # — readers stay concurrent under WAL ([ARCH-02] single writer only when
    # a caller mutates).
    #
    # begin_immediate=True: take the write lock at entry via BEGIN IMMEDIATE
    # so a later read-then-write upgrade cannot fail with unretryable
    # SQLITE_BUSY_SNAPSHOT ([RES-01] — acquire correctly rather than retry
    # a non-idempotent upgrade). Contention is resolved by busy_timeout
    # at acquisition ([RES-02]). Opt-in only: unconditional IMMEDIATE would
    # serialize every reader against writers and worsen lock storms.
    #
    # Factory-mediated write-holder registration (internal): every path
    # through this sanctioned factory appears in the sidecar writers registry
    # for the connection lifetime, so DbBusyError can name the holder and the
    # reaper can act ([OBS-01], [OBS-08], [RES-02]). Registration runs *before*
    # BEGIN IMMEDIATE so any residual sidecar work stays outside the SQLite
    # RESERVED window ([CON-18]). Modern register/refresh/unregister write only
    # that writer's shard under ``.writers.d/`` with the in-process
    # ``_registry_lock`` — they do **not** take a cross-process
    # ``_registry_file_lock`` / ``fcntl.LOCK_EX`` on the steady-state path
    # (TAIL-R1-01; flock remains only for legacy single-file RMW + reaper
    # compaction). Holding any blocking cross-process flock inside RESERVED
    # would still violate [CON-18], which is why registration precedes BEGIN.
    from .db_writer_liveness import db_writer_heartbeat

    conn = _open_db_connection()
    db_path = get_runtime_config().db_path
    label = "handoff_db_connection begin_immediate=1" if begin_immediate else "handoff_db_connection"
    try:
        # Fail-open: a broken sidecar must not deny the write path; the
        # connection still works, only holder attribution is lost ([RES-06]).
        try:
            heartbeat_cm = db_writer_heartbeat(db_path, label=label)
            heartbeat_cm.__enter__()
            registered = True
        except Exception:  # noqa: BLE001 — registry must never block open
            _log.exception("db writer registration failed during connect (continuing unregistered)")
            heartbeat_cm = None
            registered = False
        try:
            if begin_immediate:
                # isolation_level=None disables implicit BEGIN so we own the
                # transaction boundaries (BEGIN IMMEDIATE / COMMIT / ROLLBACK).
                # Must run *after* registration so flock is never under RESERVED.
                conn.isolation_level = None
                conn.execute("BEGIN IMMEDIATE")
            try:
                yield conn
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        finally:
            if registered and heartbeat_cm is not None:
                try:
                    heartbeat_cm.__exit__(None, None, None)
                except Exception:  # noqa: BLE001
                    _log.exception("db writer unregister failed during connect cleanup")
    finally:
        conn.close()
