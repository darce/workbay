"""Exact task-archive authority for opt-in supersession.

The existing Handoff archive carries the proof. An issued live-row nonce
prevents ABA across task writes, even when a recreated row reuses a revision.
The archive binds the native prepared receipt, and reactivation shares the
native repository lock with Git retirement.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import stat
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from importlib import import_module
from pathlib import Path
from typing import Any, TypeGuard
from uuid import uuid4

from . import lifecycle_resource_producer as production
from .config import _resolve_primary_worktree_root
from .enums import TERMINAL_LANE_STATUSES
from .runtime import get_runtime_config

SCHEMA_VERSION = 1
ARCHIVED = "handoff_archived"
REACTIVATED = "handoff_reactivated"
_STORE_KEY = "task_supersession_store_id_v1"
_GENERATION_TABLE = "task_supersession_generations"
_ATTACHED = "attached_supersession"
_INTENT_IDENTITY_FIELDS = (
    "schema",
    "kind",
    "nonce",
    "lane_id",
    "original_worktree_path",
    "tombstone_path",
    "admin_tombstone_path",
    "branch",
    "head_sha",
    "git_common_dir",
    "admin_git_dir",
    "incarnation",
    "branch_handoff",
    "review_preservation",
)
_ATTACHED_IDENTITY_FIELDS = ("request", "db", "selection", "task_generation", "reflog", "checkout_gitfile")
_ARCHIVED_CUSTODY_SQL = f"""CASE WHEN json_valid(snapshot_json) THEN
    json_type(snapshot_json, '$.task_supersession.schema_version') = 'integer'
    AND json_extract(snapshot_json, '$.task_supersession.schema_version') = {SCHEMA_VERSION}
    AND json_extract(snapshot_json, '$.task_supersession.custody_phase') = '{ARCHIVED}'
END"""
_GENERATION_SCHEMA = {
    _GENERATION_TABLE: f"""CREATE TABLE {_GENERATION_TABLE} (
        task_ref TEXT PRIMARY KEY NOT NULL, nonce TEXT NOT NULL
    )""",
    "task_supersession_after_insert": f"""CREATE TRIGGER task_supersession_after_insert
    AFTER INSERT ON handoff_state BEGIN
        DELETE FROM {_GENERATION_TABLE} WHERE task_ref = NEW.task_ref;
        UPDATE task_archives
        SET snapshot_json = json_set(snapshot_json, '$.task_supersession.custody_phase', '{REACTIVATED}')
        WHERE task_ref = NEW.task_ref AND {_ARCHIVED_CUSTODY_SQL};
    END""",
    "task_supersession_after_update": f"""CREATE TRIGGER task_supersession_after_update
    AFTER UPDATE ON handoff_state BEGIN
        DELETE FROM {_GENERATION_TABLE} WHERE task_ref IN (OLD.task_ref, NEW.task_ref);
    END""",
    "task_supersession_after_delete": f"""CREATE TRIGGER task_supersession_after_delete
    AFTER DELETE ON handoff_state BEGIN
        DELETE FROM {_GENERATION_TABLE} WHERE task_ref = OLD.task_ref;
    END""",
}


class SupersessionRefusal(ValueError):
    pass


def _identity(path: Path, *, directory: bool = False) -> dict[str, int]:
    entry = path.lstat()
    matches_type = stat.S_ISDIR if directory else stat.S_ISREG
    if not matches_type(entry.st_mode):
        raise OSError("unexpected file type")
    return {"st_dev": entry.st_dev, "st_ino": entry.st_ino}


@dataclass(frozen=True)
class HandoffAuthority:
    path: Path
    identity: dict[str, int]

    def check(self, conn: sqlite3.Connection | None = None) -> None:
        try:
            if (
                self.path == get_runtime_config().db_path.resolve(strict=True)
                and self.identity == _identity(self.path)
                and (conn is None or conn.execute("PRAGMA database_list").fetchone()[2] == str(self.path))
            ):
                return
        except (OSError, RuntimeError, ValueError):
            pass
        raise SupersessionRefusal("supersession_handoff_db_mismatch")


@dataclass(frozen=True)
class ArchiveBinding:
    authority: HandoffAuthority
    proof: dict

    def check(self, conn: sqlite3.Connection, task_ref: str) -> None:
        self.authority.check(conn)
        selection = self.proof["selection"]
        if (
            generation_store(conn) != selection["handoff"]["store_id"]
            or live_nonce(conn, task_ref) != selection["task_nonce"]
        ):
            raise SupersessionRefusal("supersession_task_generation_changed")
        check_owners(conn, selection, selection["repository"])
        check_checkout_identity(selection)


def repository_identity() -> dict[str, Any]:
    config = get_runtime_config()
    root = _resolve_primary_worktree_root(config.git_workspace_root or config.workspace_root)
    common = production._git(root, "rev-parse", "--path-format=absolute", "--git-common-dir") if root else None
    if root is None or not common:
        raise SupersessionRefusal("supersession_repository_unavailable")
    path = Path(common).resolve(strict=True)
    return {"root": str(root), "git_common_dir": str(path), "identity": _identity(path, directory=True)}


def registered_checkout(path: str, *, branch: str, tip: str) -> dict[str, Any]:
    target = Path(path)
    if str(target.resolve(strict=True)) != path:
        raise SupersessionRefusal("supersession_worktree_registration_mismatch")
    _identity(target / ".git")
    native = import_module("workbay_orchestrator_mcp.lane_reaping")
    observe = getattr(native, "_observe_registry_removal", None)
    if not callable(observe):
        raise SupersessionRefusal("supersession_worktree_registration_unavailable")
    registration, reason = observe(target, verify_links=True)
    if registration is None:
        raise SupersessionRefusal(reason)
    if (registration["branch"], registration["head_sha"]) != (branch, tip):
        raise SupersessionRefusal("supersession_worktree_registration_mismatch")
    return {"path": path, **registration}


def check_checkout_identity(selection: Mapping[str, Any]) -> None:
    check_repository_identity(selection["repository"])
    checkout = selection["checkout"]
    try:
        for path, role in ((checkout["path"], "worktree"), (checkout["admin_git_dir"], "admin")):
            physical = _identity(Path(path), directory=True)
            if any(physical[key] != checkout["incarnation"][role][key] for key in physical):
                raise SupersessionRefusal("supersession_worktree_registration_changed")
    except OSError as exc:
        raise SupersessionRefusal("supersession_worktree_registration_changed") from exc


def check_repository_identity(repository: Mapping[str, Any]) -> None:
    try:
        if _identity(Path(repository["git_common_dir"]), directory=True) == repository["identity"]:
            return
    except OSError:
        pass
    raise SupersessionRefusal("supersession_repository_changed")


@contextmanager
def native_reservation(repository: Mapping[str, Any], *, task_ref: str, operation: str) -> Iterator[None]:
    """Hold the native actuator's repository lock through the Handoff commit."""
    try:
        native = import_module("workbay_orchestrator_mcp.orchestration.orchestrator_lanes")
        registry = import_module("workbay_orchestrator_mcp.lane_reaping")
    except ImportError as exc:
        raise SupersessionRefusal("supersession_repo_lock_unavailable") from exc
    root_path, common_path = repository.get("root"), repository.get("git_common_dir")
    if not isinstance(root_path, str) or not isinstance(common_path, str) or "identity" not in repository:
        raise SupersessionRefusal("supersession_repository_changed")
    root, common = Path(root_path), Path(common_path)
    if not root.is_absolute() or registry._git_common_dir(root) != common:
        raise SupersessionRefusal("supersession_repository_changed")
    check_repository_identity(repository)
    with native._landing_mutation_lock(root, operation=operation, lane=native_lane_id(task_ref)) as acquired:
        if acquired is False:
            raise SupersessionRefusal("supersession_retirement_in_progress")
        if acquired is not True:
            raise SupersessionRefusal("supersession_repo_lock_unavailable")
        check_repository_identity(repository)
        if registry._git_common_dir(root) != common:
            raise SupersessionRefusal("supersession_repository_changed")
        yield


@contextmanager
def reactivation_reservation(proof: object, *, task_ref: str) -> Iterator[bool]:
    if not archive_has_custody(proof):
        yield False
        return
    selection = proof.get("selection")
    if (
        not isinstance(selection, dict)
        or selection.get("task_ref") != task_ref
        or not isinstance(selection.get("repository"), dict)
    ):
        raise SupersessionRefusal("supersession_archive_selection_mismatch")
    with native_reservation(selection["repository"], task_ref=task_ref, operation="supersession_handoff_reopen"):
        yield True


def native_lane_id(task_ref: str) -> str:
    return "task-supersession-" + hashlib.sha256(task_ref.encode()).hexdigest()[:32]


def _read_native_intent(root: Path, task_ref: str, *, sync: bool = False) -> tuple[Path, dict[str, Any]]:
    registry = import_module("workbay_orchestrator_mcp.lane_reaping")
    lane_key = hashlib.sha256(native_lane_id(task_ref).encode()).hexdigest()[:16]
    candidates = list(registry._registry_removal_intent_dir(root).glob(f"{lane_key}-*.json"))
    if not candidates:
        raise SupersessionRefusal("supersession_original_intent_required")
    if len(candidates) != 1:
        raise SupersessionRefusal("supersession_intent_ambiguous")
    path = candidates[0]
    try:
        # The kernel descriptor names the file read and flushed, even if the
        # path is replaced outside the cooperating native lock holders.
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(fd, "r", encoding="utf-8") as handle:
            info = os.fstat(handle.fileno())
            if not stat.S_ISREG(info.st_mode):
                raise OSError("removal intent is not a regular file")
            record = json.load(handle)
            if sync:
                os.fsync(handle.fileno())
            current = path.lstat()
            if not os.path.samestat(info, current) or info.st_ctime_ns != current.st_ctime_ns:
                raise OSError("removal intent changed while reading")
        if sync:
            tombstone = import_module("workbay_orchestrator_mcp.orchestration.worktree_tombstone")
            # The first native prepare may have created the journal directories.
            # Persist their names as well as the file before committing SQLite.
            for parent in path.parents:
                tombstone._fsync_directory(parent)
                if parent == root:
                    break
    except (OSError, ValueError) as exc:
        raise SupersessionRefusal("supersession_intent_unreadable") from exc
    if not isinstance(record, dict):
        raise SupersessionRefusal("supersession_intent_unreadable")
    return path, record


def _native_intent_receipt(proof: Mapping[str, Any], path: Path, record: Mapping[str, Any]) -> dict[str, str]:
    registry = import_module("workbay_orchestrator_mcp.lane_reaping")
    native = import_module("workbay_orchestrator_mcp.orchestration.supersession_retirement")
    selection = proof["selection"]
    root = Path(selection["repository"]["root"])
    task_ref = selection["task_ref"]
    lane_id = native_lane_id(task_ref)
    attached = record.get(_ATTACHED)
    if (
        not isinstance(attached, dict)
        or record.get("kind") != registry._REGISTRY_REMOVAL_INTENT_KIND
        or record.get("lane_id") != lane_id
        or record.get("phase") not in native._PHASES
        or registry._review_removal_targets(record) is None
        or path != registry._registry_removal_intent_path(root, lane_id, record["nonce"])
        or "branch_handoff" not in record
        or record["branch_handoff"] is not None
        or attached.get("selection") != selection
        or attached.get("task_generation") != proof["task_generation"]
    ):
        raise SupersessionRefusal("supersession_intent_selection_mismatch")
    # Bind the native identity and preservation, excluding progress fields the
    # native lifecycle updates after preparation.
    immutable = {key: record[key] for key in _INTENT_IDENTITY_FIELDS}
    immutable[_ATTACHED] = {key: attached[key] for key in _ATTACHED_IDENTITY_FIELDS}
    return {"path": str(path), "binding_sha256": task_generation(immutable)}


def require_prepared_intent(proof: Mapping[str, Any]) -> dict[str, str]:
    """Consume a durable native prepare under the existing repository lock."""
    selection = proof["selection"]
    root = Path(selection["repository"]["root"])
    native = import_module("workbay_orchestrator_mcp.orchestration.supersession_retirement")
    registry = import_module("workbay_orchestrator_mcp.lane_reaping")
    check_checkout_identity(selection)
    path, record = _read_native_intent(root, selection["task_ref"], sync=True)
    receipt = _native_intent_receipt(proof, path, record)
    attached = record[_ATTACHED]
    handoff = selection["handoff"]
    checkout = selection["checkout"]
    request = {
        "task_ref": selection["task_ref"],
        "branch": selection["target_branch"].removeprefix("refs/heads/"),
        "worktree_path": selection["target_worktree_path"],
        "tip": selection["tip"],
        "receipt": selection["receipt"],
        "integration_ref": f"refs/heads/{selection['integration_ref']}",
    }
    if (
        record.get("phase") != "prepared"
        or attached.get("archive_digest") is not None
        or attached["db"] != {"path": handoff["db_path"], **handoff["db_identity"]}
        or attached["request"] != request
        or record["original_worktree_path"] != checkout["path"]
        or any(record.get(key) != value for key, value in checkout.items() if key != "path")
        or record["review_preservation"].get("pin_ref") != f"refs/reclaimed/task-finish/{selection['tip']}"
    ):
        raise SupersessionRefusal("supersession_intent_selection_mismatch")
    main = registry._reclaim_rev_parse(root, "refs/heads/main")
    if main is None:
        raise SupersessionRefusal("supersession_canonical_main_unreadable")
    if hold := (
        native._proof_hold(root, request)
        or native._branch_incarnation_hold(root, record)
        or native._checkout_pointer_hold(record, Path(checkout["path"]))
        or native._preservation_hold(root, record, main)
    ):
        raise SupersessionRefusal(hold)
    return receipt


def generation_store(conn: sqlite3.Connection, *, create: bool = False) -> int | None:
    """Install this private, opt-in extension only in the selection transaction."""
    marker = conn.execute("SELECT value FROM schema_meta WHERE key = ?", (_STORE_KEY,)).fetchone()
    placeholders = ",".join("?" for _ in _GENERATION_SCHEMA)
    existing = dict(
        conn.execute(f"SELECT name, sql FROM sqlite_schema WHERE name IN ({placeholders})", tuple(_GENERATION_SCHEMA))
    )
    if marker is not None and type(marker[0]) is int and existing == _GENERATION_SCHEMA:
        return marker[0]
    if marker is not None or existing:
        raise SupersessionRefusal("supersession_generation_store_unavailable")
    if not create:
        return None
    if not conn.in_transaction:
        raise SupersessionRefusal("supersession_generation_requires_writer_transaction")
    for ddl in _GENERATION_SCHEMA.values():
        conn.execute(ddl)
    store_id = conn.execute(
        "INSERT INTO schema_meta(key, value) VALUES (?, random()) RETURNING value", (_STORE_KEY,)
    ).fetchone()[0]
    return int(store_id)


def live_nonce(conn: sqlite3.Connection, task_ref: str, *, issue: bool = False) -> str | None:
    row = conn.execute(f"SELECT nonce FROM {_GENERATION_TABLE} WHERE task_ref = ?", (task_ref,)).fetchone()
    if row is not None:
        nonce = row[0]
        if not isinstance(nonce, str) or re.fullmatch(r"[0-9a-f]{32}", nonce) is None:
            raise SupersessionRefusal("supersession_task_generation_unreadable")
        return nonce
    if not issue:
        return None
    nonce = uuid4().hex
    conn.execute(f"INSERT INTO {_GENERATION_TABLE}(task_ref, nonce) VALUES (?, ?)", (task_ref, nonce))
    return nonce


def check_owners(conn: sqlite3.Connection, selection: Mapping[str, Any], repository: Mapping[str, Any]) -> None:
    task_ref = selection["task_ref"]
    root = Path(repository["root"])
    branch = selection["target_branch"].removeprefix("refs/heads/")
    path = selection["target_worktree_path"]
    terminals = ",".join("?" for _ in TERMINAL_LANE_STATUSES)
    # A restored Done row still owns its target. Resolve path aliases while
    # the caller holds the writer lock, so a new live claim is visible here.
    rows = conn.execute(
        f"""
        SELECT 'task', task_ref, NULL, target_branch, target_worktree_path
        FROM handoff_state WHERE task_ref != ? AND (target_branch IS NOT NULL OR target_worktree_path IS NOT NULL)
        UNION ALL
        SELECT 'lane', task_ref, lane_id, branch, worktree_path FROM worktree_lanes
        WHERE COALESCE(status, '') NOT IN ({terminals})
        ORDER BY 1, 2, 3
        """,
        (task_ref, *sorted(TERMINAL_LANE_STATUSES)),
    )
    for kind, task, lane, claimed_branch, claimed_path in rows:
        if kind == "lane" and task == task_ref:
            raise SupersessionRefusal(f"supersession_task_has_pending_work: worktree_lanes ({lane})")
        owner = task if lane is None else f"{task}/{lane}"
        if (claimed_branch or "").strip().removeprefix("refs/heads/") == branch:
            raise SupersessionRefusal(f"supersession_competing_{kind}_branch: {owner}")
        if claimed_path and str((root / Path(claimed_path.strip()).expanduser()).resolve()) == path:
            raise SupersessionRefusal(f"supersession_competing_{kind}_worktree: {owner}")
    for table, predicate in (
        ("review_findings", "status = 'open'"),
        ("blockers", "status = 'open'"),
        ("next_actions", "status = 'pending'"),
    ):
        if conn.execute(f"SELECT 1 FROM {table} WHERE task_ref = ? AND {predicate} LIMIT 1", (task_ref,)).fetchone():
            raise SupersessionRefusal(f"supersession_task_has_pending_work: {table}")


def task_generation(selection: Mapping[str, object]) -> str:
    encoded = json.dumps(dict(selection), sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def archive_has_custody(proof: object) -> TypeGuard[dict[str, Any]]:
    return (
        isinstance(proof, dict)
        and type(proof.get("schema_version")) is int
        and proof.get("schema_version") == SCHEMA_VERSION
        and proof.get("custody_phase") == ARCHIVED
    )


def archived_custody_snapshot(conn: sqlite3.Connection, task_ref: str) -> str | None:
    row = conn.execute(
        f"SELECT snapshot_json FROM task_archives WHERE task_ref = ? AND {_ARCHIVED_CUSTODY_SQL}", (task_ref,)
    ).fetchone()
    return row[0] if row is not None else None


def archive_proof(conn: sqlite3.Connection, task_ref: str) -> dict:
    row = conn.execute("SELECT snapshot_json FROM task_archives WHERE task_ref = ?", (task_ref,)).fetchone()
    if row is None:
        return {}
    try:
        snapshot = json.loads(row[0])
    except (ValueError, TypeError) as exc:
        raise SupersessionRefusal("supersession_archive_unreadable") from exc
    if not isinstance(snapshot, dict):
        raise SupersessionRefusal("supersession_archive_unreadable")
    return snapshot


def resume_proof(
    conn: sqlite3.Connection,
    *,
    expected: Mapping[str, Any],
    authority: HandoffAuthority,
    repository: dict,
    generation: str,
    native_intent: tuple[Path, Mapping[str, Any]] | None = None,
) -> dict:
    """Validate archived custody against the single current native journal.

    A native settler holding the repository lock can pass its path and record
    to also check the binding of the specific intent it is about to consume.
    """
    authority.check(conn)
    check_repository_identity(repository)
    task_ref = expected["task_ref"]
    if conn.execute("SELECT 1 FROM handoff_state WHERE task_ref = ?", (task_ref,)).fetchone():
        raise SupersessionRefusal("supersession_task_reactivated")
    snapshot = archive_proof(conn, task_ref)
    proof = snapshot.get("task_supersession")
    if isinstance(proof, dict) and proof.get("custody_phase") == REACTIVATED:
        raise SupersessionRefusal("supersession_task_reactivated")
    if not isinstance(proof, dict) or not archive_has_custody(proof) or proof.get("task_generation") != generation:
        raise SupersessionRefusal("supersession_archive_selection_mismatch")
    selection = proof.get("selection")
    active = snapshot.get("active")
    store_id = generation_store(conn)
    if (
        not isinstance(selection, dict)
        or task_generation(selection) != generation
        or any(selection.get(key) != value for key, value in expected.items())
        or selection.get("owner") != {"kind": "handoff_task", "task_ref": task_ref}
        or selection.get("repository") != repository
        or store_id is None
        or selection.get("handoff") != handoff_identity(authority, store_id)
        or not isinstance(active, dict)
        or active.get("status") != "done"
        or active.get("revision") != expected["revision"] + 1
        or any(active.get(key) != expected[key] for key in ("task_ref", "target_branch", "target_worktree_path"))
    ):
        raise SupersessionRefusal("supersession_archive_selection_mismatch")
    check_owners(conn, selection, repository)
    current = _read_native_intent(Path(repository["root"]), task_ref)
    if proof.get("native_intent") != _native_intent_receipt(proof, *current) or (
        native_intent is not None and proof["native_intent"] != _native_intent_receipt(proof, *native_intent)
    ):
        raise SupersessionRefusal("supersession_archive_intent_mismatch")
    return proof


def handoff_identity(authority: HandoffAuthority, store_id: int) -> dict:
    return {"db_path": str(authority.path), "db_identity": authority.identity, "store_id": store_id}


def valid_generation(value: object) -> bool:
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None
