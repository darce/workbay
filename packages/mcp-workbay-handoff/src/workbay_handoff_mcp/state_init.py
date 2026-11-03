from __future__ import annotations

import json
from contextlib import closing
from typing import Any

from pydantic import ValidationError
from workbay_protocol import BootstrapManifest

try:
    from workbay_protocol import MANIFEST_NAME_PRECEDENCE
except ImportError:  # pragma: no cover - compatibility with older protocol wheels.
    MANIFEST_NAME_PRECEDENCE = (".workbay-bootstrap.json", ".workbay-overlay.json")

from .config import RuntimeConfig
from .runtime import configure_runtime
from .shared_schema import (
    _get_db_connection,
    _review_findings_finding_id_index_in_stat1,
    _review_findings_finding_id_index_present,
    connect_handoff_db,
    handoff_schema_version,
)


class ForeignStateReuseError(RuntimeError):
    """Raised when init-state encounters a pre-existing untrusted DB."""


def _load_adjacent_overlay_manifest(config: RuntimeConfig) -> BootstrapManifest | None:
    parent = config.state_dir.parent
    # Canonical bootstrap manifest first, then the legacy WorkBay overlay name —
    # via the shared workbay_protocol SSOT (same precedence the bootstrap installer
    # uses), so the two packages can never drift.
    for name in MANIFEST_NAME_PRECEDENCE:
        manifest_path = parent / name
        if not manifest_path.is_file():
            continue
        try:
            payload = json.loads(manifest_path.read_text())
        except (OSError, json.JSONDecodeError):
            continue
        try:
            return BootstrapManifest.model_validate(payload)
        except ValidationError:
            continue
    return None


def _read_db_user_version(db_path) -> int | None:
    if not db_path.exists():
        return None
    # closing(): sqlite3.Connection as a context manager only commits/rolls
    # back — it does NOT close the file handle ([RES-07]).
    with closing(connect_handoff_db(db_path, read_only=True)) as conn:
        # True schema version lives in schema_meta; PRAGMA user_version is only
        # the reader-compat floor. Prefer the shared helper (falls back to the
        # pragma when schema_meta is absent — pre-bootstrap / pre-floor DBs).
        return handoff_schema_version(conn)


def _review_findings_table_exists(conn) -> bool:
    row = conn.execute("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'review_findings'").fetchone()
    return row is not None


def _check_review_findings_finding_id_index(db_path) -> dict[str, Any]:
    """init-state --check facet: fail when the operator btree is absent.

    Uninitialized / non-handoff files are skipped (ok=True) so --check of an
    empty sqlite file stays a non-mutating status report. A handoff
    ``review_findings`` table without ``idx_review_findings_finding_id`` is
    the skip-scan failure this facet exists to surface.
    """
    if not db_path.exists():
        return {"ok": True, "present": False, "skipped": True}
    with closing(connect_handoff_db(db_path, read_only=True)) as conn:
        if not _review_findings_table_exists(conn):
            return {"ok": True, "present": False, "stat1_named": False, "skipped": True}
        present = _review_findings_finding_id_index_present(conn)
        stat1_named = _review_findings_finding_id_index_in_stat1(conn) if present else False
    if present:
        result: dict[str, Any] = {
            "ok": True,
            "present": True,
            "stat1_named": bool(stat1_named),
            "skipped": False,
        }
        if not stat1_named:
            result["warning"] = (
                "idx_review_findings_finding_id is present but unnamed in "
                "sqlite_stat1; planner stats are stale. A quiet reopen "
                "ANALYZE-heals; vacuum_handoff_db is the fail-closed refresh."
            )
        return result
    return {
        "ok": False,
        "present": False,
        "stat1_named": False,
        "skipped": False,
        "remediation": (
            "idx_review_findings_finding_id is missing on review_findings. "
            "Reopen with mcp-workbay-handoff 0.2.19+ or run migrate."
        ),
    }


def init_state(
    config: RuntimeConfig,
    *,
    check: bool = False,
    force_reuse_state: bool = False,
    expected_remote_url: str | None = None,
) -> dict[str, Any]:
    """Create the minimum workspace-owned handoff state for a runtime config.

    This is the programmatic bootstrap path for fresh installs. It ensures the
    exports directory exists, opens ``handoff.db`` through the normal shared
    schema path so bootstrap and migrations run, and returns a machine-readable
    summary that distinguishes created surfaces from reused ones.
    """

    configure_runtime(config)

    state_dir_existed = config.state_dir.exists()
    exports_dir_existed = config.exports_dir.exists()
    db_existed = config.db_path.exists()
    adjacent_overlay_manifest = _load_adjacent_overlay_manifest(config)
    schema_version_before_init = _read_db_user_version(config.db_path)

    if check:
        finding_id_index = _check_review_findings_finding_id_index(config.db_path)
        return {
            "ok": bool(finding_id_index["ok"]),
            "initialized": db_existed,
            "state_dir": str(config.state_dir),
            "exports_dir": str(config.exports_dir),
            "db_path": str(config.db_path),
            "state_dir_created": False,
            "exports_dir_created": False,
            "db_created": False,
            "schema_version": schema_version_before_init,
            "migrated_from": None,
            "migrated_to": None,
            "force_reuse_state": force_reuse_state,
            "review_findings_finding_id_index": finding_id_index,
        }

    if db_existed and not force_reuse_state and adjacent_overlay_manifest is None:
        raise ForeignStateReuseError(
            "Refusing to reuse pre-existing handoff state without an adjacent "
            ".workbay-bootstrap.json / .workbay-overlay.json manifest. "
            "Re-run init-state with --force-reuse-state to accept this existing DB."
        )

    if (
        expected_remote_url is not None
        and adjacent_overlay_manifest is not None
        and adjacent_overlay_manifest.remote_url != expected_remote_url
    ):
        raise ForeignStateReuseError(
            "Refusing to reuse pre-existing handoff state because the adjacent "
            "bootstrap manifest's remote_url does not match the expected "
            f"remote_url {expected_remote_url!r}."
        )

    config.exports_dir.mkdir(parents=True, exist_ok=True)

    with _get_db_connection() as conn:
        schema_version = handoff_schema_version(conn)

    migrated_from = None
    migrated_to = None
    if schema_version_before_init is not None and schema_version_before_init < schema_version:
        migrated_from = schema_version_before_init
        migrated_to = schema_version

    return {
        "ok": True,
        "initialized": True,
        "state_dir": str(config.state_dir),
        "exports_dir": str(config.exports_dir),
        "db_path": str(config.db_path),
        "state_dir_created": not state_dir_existed,
        "exports_dir_created": not exports_dir_existed,
        "db_created": not db_existed,
        "schema_version": schema_version,
        "migrated_from": migrated_from,
        "migrated_to": migrated_to,
        "force_reuse_state": force_reuse_state,
    }
