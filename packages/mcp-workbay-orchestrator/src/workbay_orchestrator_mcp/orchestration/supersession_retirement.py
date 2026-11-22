"""Retire the registered checkout of a task closed by reviewed supersession."""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import sqlite3
import stat
import subprocess
import unicodedata
from collections.abc import Iterator, Mapping
from contextlib import ExitStack, closing, contextmanager
from pathlib import Path
from typing import Any, Literal

from workbay_handoff_mcp import get_runtime_config, task_supersession

from .. import lane_reaping as registry
from . import branch_reclaim_delete as branch_delete
from . import landing_log, review_stage_reap
from .lane_reclaim import _INTEGRATION_TARGET_NAMES
from .orchestrator_lanes import _landing_mutation_lock
from .worktree_tombstone import _write_intent

_SHA = re.compile(r"[0-9a-f]{40}")
_ATTACHED = "attached_supersession"
_PHASES = {"prepared", "cutover", "detached", "deleting_branch", "completed"}
_RetirementState = Literal["prepared", "pending", "completed"]
_PATH_FIELDS = ("original_worktree_path", "admin_git_dir", "tombstone_path", "admin_tombstone_path")
_GIT_LOCATION_OVERRIDES = {
    "GIT_DIR",
    "GIT_COMMON_DIR",
    "GIT_WORK_TREE",
    "GIT_INDEX_FILE",
    "GIT_OBJECT_DIRECTORY",
    "GIT_ALTERNATE_OBJECT_DIRECTORIES",
    "GIT_NAMESPACE",
    "GIT_QUARANTINE_PATH",
}


def _git(root: Path, *args: str, env: Mapping[str, str] | None = None) -> subprocess.CompletedProcess[str] | None:
    environment = dict(os.environ if env is None else env, GIT_NO_REPLACE_OBJECTS="1", GIT_GRAFT_FILE=os.devnull)
    environment.pop("GIT_SHALLOW_FILE", None)
    return registry._run_reclaim_command(
        ["git", "-C", str(root), "-c", "advice.graftFileDeprecated=false", *args], env=environment
    )


def _local_ref(root: Path, name: str) -> str:
    ref = "refs/heads/" + name.removeprefix("refs/heads/")
    checked = _git(root, "check-ref-format", ref)
    if checked is None or checked.returncode != 0:
        raise ValueError("invalid local branch")
    return ref


def _read_intent(path: Path) -> dict[str, Any]:
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, encoding="utf-8") as handle:
        if not stat.S_ISREG(os.fstat(handle.fileno()).st_mode):
            raise OSError("removal intent is not a regular file")
        record = json.load(handle)
    if not isinstance(record, dict):
        raise ValueError("invalid removal intent")
    return record


def _supersession_lane_id(task_ref: str) -> str:
    return "task-supersession-" + hashlib.sha256(task_ref.encode()).hexdigest()[:32]


def _native_intent_paths(root: Path, lane_id: str) -> list[Path]:
    pattern = registry._registry_removal_intent_path(root, lane_id, "*")
    return list(pattern.parent.glob(pattern.name))


def _db_binding(root: Path) -> dict[str, object]:
    config = get_runtime_config()
    if config.workspace_root.resolve(strict=True) != root:
        raise OSError("supersession runtime workspace mismatch")
    path = config.db_path.resolve(strict=True)
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode):
        raise OSError("supersession task database is not a regular file")
    return {"path": str(path), **registry._review_inode_identity(info)}


@contextmanager
def _task_snapshot(root: Path, db: Mapping[str, Any]) -> Iterator[sqlite3.Connection]:
    if _db_binding(root) != db:
        raise OSError("supersession task database changed")
    remaining = registry._registry_reap_remaining()
    path = Path(db["path"])
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=min(remaining or 2.0, 2.0))) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("BEGIN")
        yield conn
    if _db_binding(root) != db:
        raise OSError("supersession task database changed")


def _handoff_authority(db: Mapping[str, Any]) -> task_supersession.HandoffAuthority:
    return task_supersession.HandoffAuthority(Path(db["path"]), {key: db[key] for key in ("st_dev", "st_ino")})


def _check_live_selection(root: Path, db: Mapping[str, Any], proof: dict[str, Any]) -> None:
    selection = proof["selection"]
    task_ref = selection["task_ref"]
    with _task_snapshot(root, db) as conn:
        live = conn.execute("SELECT * FROM handoff_state WHERE task_ref=?", (task_ref,)).fetchone()
        if live is None or any(
            live[key] != selection[key] for key in ("revision", "status", "target_branch", "target_worktree_path")
        ):
            raise ValueError("supersession_task_generation_changed")
        task_supersession.ArchiveBinding(_handoff_authority(db), proof).check(conn, task_ref)


def _archive_digest(root: Path, binding: Mapping[str, Any]) -> str:
    selection, db = binding["selection"], binding["db"]
    with _task_snapshot(root, db) as conn:
        task_supersession.resume_proof(
            conn,
            expected=selection,
            authority=_handoff_authority(db),
            repository=selection["repository"],
            generation=binding["task_generation"],
        )
        archive = conn.execute(
            "SELECT archived_at, snapshot_json FROM task_archives WHERE task_ref=?", (selection["task_ref"],)
        ).fetchone()
        return hashlib.sha256(json.dumps(list(archive)).encode()).hexdigest()


def _proof_hold(root: Path, request: Mapping[str, Any]) -> str | None:
    return landing_log.reviewed_supersession_hold(
        root,
        branch=request["branch"],
        tip=request["tip"],
        receipt=request["receipt"],
        integration_ref=request["integration_ref"],
        deadline=registry._REGISTRY_REAP_DEADLINE.get(),
    )


def _ownership_hold(root: Path, request: Mapping[str, Any], *, preparing: bool = False) -> str | None:
    branch, path = request["branch"], request["worktree_path"]
    refused, detail = registry._live_task_target_refusal(
        root,
        branch,
        path,
        include_done=True,
        exclude_task_ref=request["task_ref"] if preparing else None,
    )
    if refused:
        return detail or refused
    owned, detail = registry._branch_ledger_owner_blocks_reclaim(branch=branch)
    if owned:
        return detail
    shared, detail = registry._shared_path_blocks_reclaim(
        worktree_path=path,
        task_ref=None,
        lane_id=None,
        repo_root=root,
    )
    return detail if shared else None


def _process_hold(root: Path, *paths: Path) -> str | None:
    for path in paths:
        if Path.cwd().is_relative_to(path):
            return "supersession_current_checkout"
        state, detail = registry._probe_worktree_process_owner(str(path))
        if state != "free":
            return detail or f"supersession_process_{state}"
        live, detail = registry._session_heartbeat_blocks_reclaim(repo_root=root, worktree_path=str(path))
        if live:
            return detail
    return None


def _clean_hold(
    checkout: Path,
    admin: Path,
    common: Path,
    checkout_gitfile: Mapping[str, object],
    *,
    deletion_started: bool = False,
    allow_locked: bool = False,
    checkout_parent_fd: int | None = None,
) -> str | None:
    refused, detail = registry._worktree_operation_refusal(admin)
    if refused:
        return detail or refused
    if not allow_locked and _file_exists(admin / "locked"):
        return "supersession_checkout_locked"
    try:
        with ExitStack() as stack:
            checkout_fd = registry._open_review_removal_directory(
                stack, checkout if checkout_parent_fd is None else checkout.name, checkout_parent_fd
            )
            if hold := _checkout_git_inventory_hold(
                checkout_fd, checkout_gitfile, allow_missing_gitfile=deletion_started
            ):
                return hold
    except TimeoutError:
        raise
    except (OSError, RecursionError):
        return "supersession_checkout_inventory_unverifiable"

    env = dict(os.environ, GIT_DIR=str(admin), GIT_COMMON_DIR=str(common), GIT_WORK_TREE=str(checkout))
    # Compare raw checkout names even if Git was configured for a filesystem
    # that folds case or normalizes Unicode.
    flags = (
        "--no-optional-locks",
        "-c",
        "core.fsmonitor=false",
        "-c",
        "core.untrackedCache=false",
        "-c",
        "core.ignoreCase=false",
        "-c",
        "core.precomposeUnicode=false",
    )
    index = _git(checkout, *flags, "ls-files", "-v", "--stage", "-z", env=env)
    if index is None or index.returncode != 0:
        return "supersession_index_probe_failed"
    status = _git(
        checkout,
        *flags,
        "-c",
        "core.filemode=true",
        "-c",
        "core.ignoreStat=false",
        "-c",
        "core.trustctime=true",
        "-c",
        "core.checkStat=default",
        "status",
        "--porcelain=v1",
        "-z",
        "--untracked-files=all",
        "--ignored=matching",
        "--ignore-submodules=none",
        env=env,
    )
    if status is None or status.returncode != 0:
        return "supersession_status_probe_failed"
    for entry in filter(None, index.stdout.split("\0")):
        # Assume-unchanged (lowercase), skip-worktree and gitlinks can hide work.
        if not entry.startswith("H ") or entry.startswith("H 160000 "):
            return "supersession_index_entries_unverifiable"
    changes = list(filter(None, status.stdout.split("\0")))
    if changes and (not deletion_started or any(not change.startswith(" D ") for change in changes)):
        return "supersession_checkout_dirty"
    return None


def _file_exists(path: Path) -> bool:
    try:
        path.lstat()
    except FileNotFoundError:
        return False
    return True


def _file_identity(path: Path | str, *, dir_fd: int | None = None) -> dict[str, object]:
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=dir_fd)
    with os.fdopen(fd, "rb") as handle:
        before = os.fstat(handle.fileno())
        if not stat.S_ISREG(before.st_mode) or not before.st_size:
            raise OSError("custody file has no regular file identity")
        digest = hashlib.file_digest(handle, "sha256").hexdigest()
        after = os.stat(path, dir_fd=dir_fd, follow_symlinks=False)
    if not os.path.samestat(before, after) or (before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (
        after.st_size,
        after.st_mtime_ns,
        after.st_ctime_ns,
    ):
        raise OSError("custody file changed while reading")
    return {**registry._review_inode_identity(before), "ctime_ns": before.st_ctime_ns, "sha256": digest}


def _checkout_git_inventory_hold(
    directory_fd: int,
    checkout_gitfile: Mapping[str, object] | None = None,
    *,
    allow_missing_gitfile: bool = False,
) -> str | None:
    """Inventory Git markers by name, independently of Git's status and index."""
    before = os.fstat(directory_fd)
    verified_gitfile = False
    with os.scandir(directory_fd) as entries:
        for entry in entries:
            registry._registry_reap_remaining()
            if unicodedata.normalize("NFC", entry.name).casefold() == ".git":
                if checkout_gitfile is None or entry.name != ".git":
                    return "supersession_nested_git_marker"
                if _file_identity(entry.name, dir_fd=directory_fd) != checkout_gitfile:
                    return "supersession_checkout_gitfile_changed"
                verified_gitfile = True
                continue
            info = entry.stat(follow_symlinks=False)
            if stat.S_ISDIR(info.st_mode):
                with ExitStack() as stack:
                    child_fd = registry._open_review_removal_directory(stack, entry.name, directory_fd)
                    if not os.path.samestat(info, os.fstat(child_fd)):
                        return "supersession_checkout_inventory_unverifiable"
                    if hold := _checkout_git_inventory_hold(child_fd):
                        return hold
    after = os.fstat(directory_fd)
    if (before.st_mtime_ns, before.st_ctime_ns) != (after.st_mtime_ns, after.st_ctime_ns):
        return "supersession_checkout_inventory_unverifiable"
    if checkout_gitfile is not None and not verified_gitfile and not allow_missing_gitfile:
        return "supersession_checkout_gitfile_changed"
    return None


def _observe_original(root: Path, path: Path, branch: str, tip: str) -> dict[str, object]:
    entries, detail = review_stage_reap._worktree_registry(root)
    if entries is None:
        raise ValueError(detail)
    matches = [entry for entry in entries[1:] if entry.get("branch") == branch or entry.get("path") == str(path)]
    if (
        len(matches) != 1
        or matches[0].get("prunable")
        or (matches[0].get("path"), matches[0].get("branch"), matches[0].get("head")) != (str(path), branch, tip)
    ):
        raise ValueError("supersession_original_registration_unverified")
    observed, detail = registry._observe_registry_removal(path, verify_links=True)
    if observed is None:
        raise ValueError(detail)
    if (observed["branch"], observed["head_sha"], Path(str(observed["git_common_dir"]))) != (
        branch,
        tip,
        registry._git_common_dir(root),
    ):
        raise ValueError("supersession_original_registration_unverified")
    return observed


def _preserve(root: Path, branch: str, tip: str) -> dict[str, object]:
    common = _git(root, "merge-base", tip, "refs/heads/main")
    if common is None or common.returncode != 0 or not _SHA.fullmatch(common.stdout.strip()):
        raise ValueError("supersession_canonical_bundle_base_unavailable")
    bundle = registry._bundle_before_reap(root, branch, retention_count=0, prune_to_cap=False)
    if bundle.get("ok") is not True or (bundle.get("tip_sha"), bundle.get("bundle_mode")) != (tip, "delta"):
        raise ValueError(f"supersession_exact_delta_bundle_required:{bundle.get('error') or ''}")
    pinned, pin = branch_delete.pin_task_finish_tip(root, branch, tip)
    if not pinned:
        raise ValueError(f"supersession_pin_failed:{pin}")
    archive = review_stage_reap._review_archive_binding(root, branch, tip, Path(str(bundle["bundle_path"])), pin)
    if archive is None:
        raise ValueError("supersession_preservation_unverified")
    return archive


def _preservation_hold(root: Path, record: Mapping[str, Any], main: str) -> str | None:
    if not registry._review_removal_archive_matches(root, record):
        return "supersession_preservation_unverified"
    prerequisites = record["review_preservation"]["bundle_identity"]["prerequisite_shas"]
    if not prerequisites:
        return "supersession_canonical_bundle_base_unavailable"
    for sha in prerequisites:
        proc = _git(root, "merge-base", "--is-ancestor", sha, main)
        if proc is None or proc.returncode != 0:
            return "supersession_canonical_bundle_base_unavailable"
    return None


def _validation_hold(root: Path, path: Path, record: Mapping[str, Any]) -> str | None:
    if not _GIT_LOCATION_OVERRIDES.isdisjoint(os.environ):
        return "supersession_git_location_overridden"
    binding = record[_ATTACHED]
    request = binding["request"]
    if (
        record.get("kind") != registry._REGISTRY_REMOVAL_INTENT_KIND
        or record.get("phase") not in _PHASES
        or record.get("branch_handoff") is not None
        or path != registry._registry_removal_intent_path(root, record["lane_id"], record["nonce"])
        or registry._review_removal_targets(record) is None
        or registry._review_removal_identities(record.get("incarnation")) is None
        or (record["branch"], record["head_sha"], record["original_worktree_path"])
        != (request["branch"], request["tip"], request["worktree_path"])
        or registry._git_common_dir(root) != Path(record["git_common_dir"])
        or any(
            record[key] != binding["selection"]["checkout"][key]
            for key in ("branch", "head_sha", "git_common_dir", "admin_git_dir", "incarnation")
        )
        or _local_ref(root, request["integration_ref"]) != request["integration_ref"]
        or _local_ref(root, record["branch"]) != f"refs/heads/{record['branch']}"
        or record["branch"] in _INTEGRATION_TARGET_NAMES
        or record["review_preservation"]["pin_ref"] != f"refs/reclaimed/task-finish/{record['head_sha']}"
        or not _SHA.fullmatch(record["head_sha"])
    ):
        return "supersession_intent_identity_invalid"
    archive = _archive_digest(root, binding)
    if binding.get("archive_digest") not in {None, archive}:
        return "supersession_archive_generation_changed"
    return _ownership_hold(root, request) or _proof_hold(root, request)


@contextmanager
def _checkout_cutover(
    root: Path,
    path: Path,
    record: dict[str, Any],
    targets: dict[str, registry._ReviewRemovalTarget],
    identities: dict[str, dict[str, int]],
    main: str,
) -> Iterator[None]:
    """Resume only original or privately held directory identities from the journal."""
    recorded_holders = registry._review_removal_identities(record["cutover_holders"])
    if recorded_holders is None:
        raise ValueError("supersession_holder_identity_invalid")
    locations = {}
    for kind, target in targets.items():
        holder, populated = target.observe_holder(identities[kind])
        if holder != recorded_holders[kind]:
            raise ValueError("supersession_holder_incarnation_changed")
        if populated:
            if not target.original_absent():
                raise ValueError("supersession_original_path_reused")
            locations[kind] = target.holder / "candidate"
        else:
            if not target.original_matches(identities[kind]):
                raise ValueError("supersession_original_incarnation_changed")
            locations[kind] = target.original
    if locations["admin"] != targets["admin"].original and locations["worktree"] == targets["worktree"].original:
        raise ValueError("supersession_cutover_order_invalid")
    admin = locations["admin"]
    worktree = locations["worktree"]
    common = Path(record["git_common_dir"])
    request = record[_ATTACHED]["request"]
    with ExitStack() as stack:
        admin_fd = registry._open_review_removal_directory(stack, admin)
        if registry._registry_admin_incarnation(admin, admin_fd) != record["incarnation"]["admin"]:
            raise ValueError("supersession_admin_registration_changed")

        def record_locks(locks: dict[str, dict[str, int]]) -> None:
            record["checkout_locks"] = locks
            _write_intent(path, record)

        locked = stack.enter_context(
            registry._review_checkout_write_exclusion(
                root,
                record,
                record["review_preservation"],
                integration_ref=request["integration_ref"],
                additional_refs={"refs/heads/main": main},
                admin_path=admin,
                recorded_locks=record.get("checkout_locks"),
                record_locks=record_locks,
            )
        )
        if not locked:
            raise ValueError("supersession_git_custody_unavailable")
        if hold := (
            _validation_hold(root, path, record)
            or _branch_incarnation_hold(root, record)
            or _process_hold(root, *dict.fromkeys((targets["worktree"].original, worktree)))
            or _clean_hold(
                worktree,
                admin,
                common,
                record[_ATTACHED]["checkout_gitfile"],
                allow_locked=_owns_registration_lock(record, admin),
            )
        ):
            raise ValueError(hold)
        if hold := _preservation_hold(root, record, main):
            raise ValueError(hold)
        for kind, target in targets.items():
            if locations[kind] != target.original:
                continue
            if target.observe_holder(identities[kind]) != (recorded_holders[kind], False):
                raise ValueError("supersession_holder_incarnation_changed")
            if not target.original_matches(identities[kind]):
                raise ValueError("supersession_original_incarnation_changed")
            target.move_candidate()
            if target.observe_holder(identities[kind]) != (recorded_holders[kind], True):
                raise ValueError("supersession_candidate_incarnation_changed")
        yield


def _checkout_pointer_hold(record: Mapping[str, Any], worktree: Path, *, partial: bool = False) -> str | None:
    try:
        same = _file_identity(worktree / ".git") == record[_ATTACHED]["checkout_gitfile"]
    except FileNotFoundError:
        same = partial
    return None if same else "supersession_checkout_gitfile_changed"


def _owns_registration_lock(record: Mapping[str, Any], admin: Path) -> bool:
    try:
        fd = os.open(admin / "locked", os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except FileNotFoundError:
        return False
    recorded = record.get("checkout_locks")
    try:
        evidence = registry._review_checkout_lock_evidence(
            fd,
            registry._review_checkout_lock_marker(record, "locked"),
            recorded.get("locked") if isinstance(recorded, dict) else None,
        )
        return evidence is not None
    finally:
        os.close(fd)


def _retire_directories(root: Path, path: Path, record: dict[str, Any], main: str) -> None:
    if not shutil.rmtree.avoids_symlink_attacks:
        raise ValueError("supersession_safe_rmtree_unavailable")
    paths = registry._review_removal_targets(record)
    identities = registry._review_removal_identities(record["incarnation"])
    assert paths is not None and identities is not None
    phase = record["phase"]
    if phase == "prepared":
        original = paths["worktree"][0]
        observed = _observe_original(root, original, record["branch"], record["head_sha"])
        if any(record.get(name) != value for name, value in observed.items()):
            raise ValueError("supersession_original_incarnation_changed")
        if hold := (
            _process_hold(root, original)
            or _clean_hold(
                original, paths["admin"][0], Path(record["git_common_dir"]), record[_ATTACHED]["checkout_gitfile"]
            )
        ):
            raise ValueError(hold)
    if phase == "detached" and registry._review_holder_identities(record) is None:
        raise ValueError("supersession_detached_identity_invalid")
    if phase == "detached" and not any(_file_exists(Path(record[key])) for key in _PATH_FIELDS):
        return
    with ExitStack() as stack:
        targets = registry._open_registry_removal_targets(
            stack,
            paths,
            create_holders=phase == "prepared",
            resume_empty_holders=True,
        )
        if phase == "prepared":
            holders = {}
            for kind, target in targets.items():
                if not target.original_matches(identities[kind]):
                    raise ValueError("supersession_original_incarnation_changed")
                holder, populated = target.observe_holder(identities[kind])
                if holder is None or populated or holder["st_dev"] != identities[kind]["st_dev"]:
                    raise ValueError("supersession_holder_incarnation_changed")
                os.fsync(target.holder_parent_fd)
                holders[kind] = holder
            record.update(phase="cutover", cutover_holders=holders)
            _write_intent(path, record)
        if record["phase"] == "cutover":
            stack.enter_context(_checkout_cutover(root, path, record, targets, identities, main))
        else:
            integration_ref = record[_ATTACHED]["request"]["integration_ref"]
            integration = registry._reclaim_rev_parse(root, integration_ref)
            if integration is None or not stack.enter_context(
                registry._hold_reclaim_refs(
                    root,
                    {
                        f"refs/heads/{record['branch']}": record["head_sha"],
                        record["review_preservation"]["pin_ref"]: record["head_sha"],
                        integration_ref: integration,
                        "refs/heads/main": main,
                    },
                )
            ):
                raise ValueError("supersession_git_custody_unavailable")

        def check_before_remove(populated: set[str]) -> str | None:
            checkout = targets["worktree"]
            held_checkout = checkout.holder / "candidate"
            hold = (
                _validation_hold(root, path, record)
                or _branch_incarnation_hold(root, record)
                or _preservation_hold(root, record, main)
                or _process_hold(root, checkout.original, held_checkout)
            )
            if hold:
                return hold
            if "admin" not in populated:
                return "supersession_checkout_inventory_unverifiable" if "worktree" in populated else None
            admin = targets["admin"].holder / "candidate"
            owns_lock = _owns_registration_lock(record, admin)
            if "worktree" not in populated:
                refused, detail = registry._worktree_operation_refusal(admin)
                if refused:
                    return detail
                return "supersession_checkout_locked" if not owns_lock and _file_exists(admin / "locked") else None
            assert checkout.holder_fd is not None
            return _clean_hold(
                held_checkout,
                admin,
                Path(record["git_common_dir"]),
                record[_ATTACHED]["checkout_gitfile"],
                deletion_started=phase == "detached",
                allow_locked=owns_lock,
                checkout_parent_fd=checkout.holder_fd,
            )

        if not registry._finish_detached_review_removal(
            path,
            record,
            targets,
            identities,
            replay_stack=stack,
            attached_guard=check_before_remove,
        ):
            raise ValueError("supersession_detached_removal_unverified")
    published = _read_intent(path)
    if published.get("phase") != "detached":
        raise ValueError("supersession_detached_phase_unverified")
    record.update(published)


def _paths_hold(root: Path, record: Mapping[str, Any]) -> str | None:
    if any(_file_exists(Path(record[key])) for key in _PATH_FIELDS):
        return "supersession_retirement_path_present"
    entries, detail = review_stage_reap._worktree_registry(root)
    if entries is None:
        return detail
    for entry in entries:
        if entry.get("branch") == record["branch"] or Path(entry["path"]).resolve() == Path(
            record["original_worktree_path"]
        ):
            return "supersession_checkout_still_registered"
    return None


def _branch_incarnation_hold(root: Path, record: Mapping[str, Any]) -> str | None:
    ref = f"refs/heads/{record['branch']}"
    if registry._reclaim_rev_parse(root, ref) != record["head_sha"]:
        return "supersession_branch_tip_changed"
    reflog = Path(record["git_common_dir"]) / "logs" / ref
    if not _file_exists(reflog) or _file_identity(reflog) != record[_ATTACHED]["reflog"]:
        return "supersession_branch_incarnation_changed"
    return None


def _finish_branch(root: Path, path: Path, record: dict[str, Any], main: str) -> tuple[_RetirementState, str]:
    if hold := _paths_hold(root, record):
        return "pending", hold
    branch, tip = record["branch"], record["head_sha"]
    ref = f"refs/heads/{branch}"
    current = branch_delete._resolve_ref(root, ref)
    phase = record["phase"]
    if current.probe_failed:
        return "pending", "supersession_branch_probe_failed"
    if not current.missing:
        if phase == "completed":
            return "pending", "supersession_branch_recreated"
        if hold := _branch_incarnation_hold(root, record):
            return "pending", hold
        request = record[_ATTACHED]["request"]
        preservation = record["review_preservation"]
        bundle, pin = Path(preservation["bundle_path"]), preservation["pin_ref"]
        reflog = record[_ATTACHED]["reflog"]
        integration_ref = request["integration_ref"]
        integration = registry._reclaim_rev_parse(root, integration_ref)
        if integration is None:
            return "pending", "supersession_integration_unreadable"
        effect = {
            "WORKBAY_AUTH_REMOVED_PATHS": json.dumps([record[key] for key in _PATH_FIELDS]),
            "WORKBAY_AUTH_REFLOG": str(Path(record["git_common_dir"]) / "logs" / ref),
            "WORKBAY_AUTH_REFLOG_SHA": reflog["sha256"],
            "WORKBAY_AUTH_REFLOG_ID": json.dumps([reflog["st_dev"], reflog["st_ino"], reflog["ctime_ns"]]),
            "WORKBAY_AUTH_BUNDLE": str(bundle),
            "WORKBAY_AUTH_BUNDLE_SHA": preservation["bundle_identity"]["bundle_sha256"],
        }

        def revalidate() -> str | None:
            return (
                _validation_hold(root, path, record)
                or _paths_hold(root, record)
                or _branch_incarnation_hold(root, record)
                or _preservation_hold(root, record, main)
            )

        refs = {pin: tip, integration_ref: integration, "refs/heads/main": main}
        with registry._hold_reclaim_refs(root, refs) as locked:
            if not locked:
                return "pending", "supersession_retention_ref_custody_unavailable"
            if hold := revalidate():
                return "pending", hold
            record["phase"] = "deleting_branch"
            _write_intent(path, record)
            deleted = branch_delete._delete_supersession_branch(
                root,
                branch=branch,
                tip=tip,
                pin=pin,
                bundle=bundle,
                task_ref=request["task_ref"],
                lane_id=record["lane_id"],
                revalidate=revalidate,
                effect=effect,
            )
            if deleted.fence_release not in {"", "released"}:
                return "pending", f"supersession_reclaim_fence:{deleted.fence_release}"
            if not deleted.deleted:
                return "pending", deleted.detail or deleted.reason
    elif phase not in {"deleting_branch", "completed"}:
        return "pending", "supersession_branch_absence_unrecorded"
    # A terminal success requires all effects and custody to be observed together.
    if not branch_delete._resolve_ref(root, ref).missing:
        return "pending", "supersession_branch_still_present_or_unknown"
    current_main = registry._reclaim_rev_parse(root, "refs/heads/main")
    if current_main is None:
        return "pending", "supersession_canonical_main_unreadable"
    if (
        hold := _validation_hold(root, path, record)
        or _paths_hold(root, record)
        or _preservation_hold(root, record, current_main)
    ):
        return "pending", hold
    if phase != "completed":
        registry._registry_reap_remaining()
        _write_intent(path, {**record, "phase": "completed"})
    return "completed", "supersession_retired"


def settle_supersession(root: Path, path: Path, record: dict[str, Any]) -> tuple[_RetirementState, str]:
    """Settle an attached operation under the registry and landing locks."""
    try:
        lane_id = _supersession_lane_id(record[_ATTACHED]["request"]["task_ref"])
        if record.get("lane_id") != lane_id or _native_intent_paths(root, lane_id) != [path]:
            return "pending", "supersession_intent_ambiguous"
        if _read_intent(path) != record:
            return "pending", "supersession_intent_changed"
        if hold := _validation_hold(root, path, record):
            return "pending", hold
        main = registry._reclaim_rev_parse(root, "refs/heads/main")
        if main is None:
            return "pending", "supersession_canonical_main_unreadable"
        if hold := _preservation_hold(root, record, main):
            return "pending", hold
        if record["phase"] in {"prepared", "cutover", "detached"}:
            if hold := _branch_incarnation_hold(root, record):
                return "pending", hold
            if record[_ATTACHED].get("archive_digest") is None:
                if record["phase"] != "prepared":
                    return "pending", "supersession_archive_binding_unrecorded"
                record[_ATTACHED]["archive_digest"] = _archive_digest(root, record[_ATTACHED])
                _write_intent(path, record)
            _retire_directories(root, path, record, main)
        elif not record[_ATTACHED].get("archive_digest") or registry._review_holder_identities(record) is None:
            return "pending", "supersession_detached_phase_unverified"
        return _finish_branch(root, path, record, main)
    except (OSError, RuntimeError, sqlite3.Error, subprocess.SubprocessError, ValueError, KeyError, TypeError) as exc:
        return "pending", f"supersession_recovery_held:{exc}"


def supersession_deferral(root: Path, *, branch: str = "", tip: str | None = None, worktree_path: str = "") -> str:
    """An ordinary sweep cannot act on resources with attached proof custody."""
    if (root / ".git").is_file():
        primary = review_stage_reap._review_evidence_root(root)
        if primary is None:
            return "supersession_registry_root_unavailable"
        root = primary
    path_key = registry._shared_path_identity(worktree_path, root)
    branch = branch.removeprefix("refs/heads/")
    for path in registry._registry_removal_intent_dir(root).glob("*.json"):
        try:
            record = _read_intent(path)
        except (OSError, ValueError):
            return "removal_intent_unreadable"
        if _ATTACHED not in record:
            continue
        if path_key and record.get("original_worktree_path") == path_key:
            return "supersession_worktree_custody"
        if branch and record.get("branch") == branch:
            if record.get("phase") != "completed":
                return "supersession_retirement_pending"
            current = tip or registry._reclaim_rev_parse(root, f"refs/heads/{branch}")
            if current is None or current == record.get("head_sha"):
                return "supersession_branch_generation_protected"
    return ""


def retire_superseded_checkout(
    repo_root: Path | str,
    *,
    proof: dict[str, Any],
) -> tuple[_RetirementState, str]:
    """Prepare from Handoff's selected live generation, or settle its archive.

    Preparation binds the original Git identities and records preservation.
    Only an exact Handoff archive permits retirement or replay.
    """
    try:
        if not _GIT_LOCATION_OVERRIDES.isdisjoint(os.environ):
            return "pending", "supersession_git_location_overridden"
        root = Path(repo_root).resolve(strict=True)
        selection, generation = proof["selection"], proof["task_generation"]
        phase = proof.get("custody_phase")
        if (
            type(proof.get("schema_version")) is not int
            or proof["schema_version"] != task_supersession.SCHEMA_VERSION
            or phase not in ("selected", task_supersession.ARCHIVED)
            or not task_supersession.valid_generation(generation)
            or not isinstance(selection, dict)
            or any(
                not isinstance(selection.get(key), str)
                for key in ("task_ref", "target_branch", "target_worktree_path", "integration_ref", "tip", "receipt")
            )
            or task_supersession.task_generation(selection) != generation
        ):
            return "pending", "supersession_exact_selection_required"
        task_ref, tip, receipt = selection["task_ref"], selection["tip"], selection["receipt"]
        original = Path(selection["target_worktree_path"])
        branch = _local_ref(root, selection["target_branch"]).removeprefix("refs/heads/")
        integration_ref = _local_ref(root, selection["integration_ref"])
        db = _db_binding(root)
        handoff = selection["handoff"]
        if (
            not task_ref
            or not _SHA.fullmatch(tip)
            or not _SHA.fullmatch(receipt)
            or branch in _INTEGRATION_TARGET_NAMES
            or f"refs/heads/{branch}" == integration_ref
            or not original.is_absolute()
            or original.resolve() != original
            or root.is_relative_to(original)
            or selection["repository"]["root"] != str(root)
            or handoff != task_supersession.handoff_identity(_handoff_authority(db), handoff["store_id"])
        ):
            return "pending", "supersession_exact_selection_required"
        request = dict(
            task_ref=task_ref,
            branch=branch,
            worktree_path=str(original),
            tip=tip,
            receipt=receipt,
            integration_ref=integration_ref,
        )
        lane_id = _supersession_lane_id(task_ref)
        handle, detail = registry._acquire_lane_worker_lock(lane_id)
        if handle is None:
            return "pending", detail
        try:
            with _landing_mutation_lock(
                root,
                operation="registry_supersession",
                lane=lane_id,
                inherit_for_reclaim=phase == task_supersession.ARCHIVED,
            ) as locked:
                if locked is not True:
                    return "pending", "supersession_repo_lock_unavailable"
                return _prepare_or_replay(root, lane_id, request, db, proof, preparing=phase == "selected")
        finally:
            registry._release_lane_worker_lock(handle)
    except (OSError, RuntimeError, sqlite3.Error, subprocess.SubprocessError, ValueError, KeyError, TypeError) as exc:
        return "pending", f"supersession_retirement_held:{exc}"


def _prepare_or_replay(
    root: Path,
    lane_id: str,
    request: dict[str, Any],
    db: dict[str, object],
    proof: dict[str, Any],
    *,
    preparing: bool,
) -> tuple[_RetirementState, str]:
    candidates = _native_intent_paths(root, lane_id)
    record = None
    if candidates:
        if len(candidates) != 1:
            return "pending", "supersession_intent_ambiguous"
        path = candidates[0]
        record = _read_intent(path)
        binding = record.get(_ATTACHED)
        if (
            not isinstance(binding, dict)
            or binding.get("request") != request
            or binding.get("db") != db
            or binding.get("selection") != proof["selection"]
            or binding.get("task_generation") != proof["task_generation"]
        ):
            return "pending", "supersession_intent_selection_mismatch"
        if not preparing:
            return settle_supersession(root, path, record)
        if record.get("phase") != "prepared" or binding.get("archive_digest") is not None:
            return "pending", "supersession_intent_generation_mismatch"
    elif not preparing:
        return "pending", "supersession_original_intent_required"

    branch, tip = request["branch"], request["tip"]
    original = Path(request["worktree_path"])
    if record is None and (deferred := supersession_deferral(root, branch=branch, tip=tip, worktree_path=str(original))):
        return "pending", deferred
    _check_live_selection(root, db, proof)
    if (
        hold := _proof_hold(root, request)
        or _ownership_hold(root, request, preparing=True)
        or _process_hold(root, original)
    ):
        return "pending", hold
    incarnation = _observe_original(root, original, branch, tip)
    if {"path": str(original), **incarnation} != proof["selection"]["checkout"]:
        return "pending", "supersession_original_registration_changed"
    common, admin = Path(str(incarnation["git_common_dir"])), Path(str(incarnation["admin_git_dir"]))
    checkout_gitfile = _file_identity(original / ".git") if record is None else record[_ATTACHED]["checkout_gitfile"]
    if hold := _clean_hold(original, admin, common, checkout_gitfile, allow_locked=True):
        return "pending", hold
    if record is not None:
        if hold := _branch_incarnation_hold(root, record):
            return "pending", hold
        return "prepared", "supersession_intent_prepared"
    reflog_path = common / "logs" / "refs" / "heads" / branch
    reflog = _file_identity(reflog_path)
    preservation = _preserve(root, branch, tip)
    _check_live_selection(root, db, proof)
    if _file_identity(reflog_path) != reflog or _file_identity(original / ".git") != checkout_gitfile:
        return "pending", "supersession_selection_changed_during_prepare"
    prepared_path, _, detail = registry._record_registry_removal_intent(
        root,
        str(original),
        lane_id=lane_id,
        expected_incarnation=incarnation,
        review=preservation,
        attached_supersession=dict(
            request=request,
            db=db,
            selection=proof["selection"],
            task_generation=proof["task_generation"],
            reflog=reflog,
            checkout_gitfile=checkout_gitfile,
        ),
    )
    return ("prepared", "supersession_intent_prepared") if prepared_path is not None else ("pending", detail)
