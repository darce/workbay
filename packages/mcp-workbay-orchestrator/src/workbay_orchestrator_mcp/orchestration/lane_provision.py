"""Provision a dispatchable lane from one manifest lane specification.

The manifest remains the source of truth. Provisioning records the new lane
atomically, materializes its branch and worktree, opens its row, pins routing,
and finally publishes a receipt. Repeating a completed call returns that
receipt while its branch and worktree still exist.
"""

from __future__ import annotations

import copy
import fcntl
import hashlib
import json
import logging
import os
import re
import subprocess
import tempfile
import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from workbay_orchestrator_mcp.orchestration.lane_lifecycle_contracts import ContractError, ProvisionReceipt

_GIT_TIMEOUT_SECONDS = 30
_FULL_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_LOGGER = logging.getLogger(__name__)


class ProvisionRefused(RuntimeError):
    """Typed provision refusal with an optional durable receipt."""

    def __init__(self, *, reason: str, detail: Any, receipt: ProvisionReceipt | None = None) -> None:
        self.reason = reason
        self.detail = detail
        self.receipt = receipt
        rendered = detail if isinstance(detail, str) else repr(detail)
        super().__init__(f"{reason}: {rendered}")


@dataclass(slots=True)
class ProvisionDeps:
    """Optional call seams; omitted operations bind to production functions lazily."""

    update_manifest: Callable[..., Any] | None = None
    materialize: Callable[..., Any] | None = None
    upsert_row: Callable[..., Any] | None = None
    run_git: Callable[..., subprocess.CompletedProcess[str]] | None = None
    now: Callable[[], datetime] | None = None
    check_held: Callable[..., Any] | None = None
    list_rows: Callable[[], Iterable[Mapping[str, Any]]] | None = None
    pin_routing: Callable[..., Mapping[str, Any]] | None = None


def _timestamp(now: Callable[[], datetime] | None) -> str:
    value = now() if now is not None else datetime.now(timezone.utc)
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    return str(value)


def _run_git_default(args: Sequence[str], *, cwd: Path | str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *[str(arg) for arg in args]],
        cwd=str(cwd),
        capture_output=True,
        text=True,
        check=False,
        timeout=_GIT_TIMEOUT_SECONDS,
    )


def _default_rows() -> list[Mapping[str, Any]]:
    """Read every row, across task refs, for lane-id and worktree ownership checks."""
    from workbay_orchestrator_mcp.orchestration.lane_worktree import _collect_all_lanes_for_path_guard

    rows, error = _collect_all_lanes_for_path_guard()
    if rows is None or error is not None:
        raise RuntimeError(error or "cross-task worktree_lanes census failed")
    if any(not isinstance(row, Mapping) for row in rows):
        raise RuntimeError("cross-task worktree_lanes census returned a malformed row")
    return rows


def _default_upsert_row(**kwargs: Any) -> Any:
    # Deliberately local: importing lanes pulls in the database and handoff API.
    from workbay_orchestrator_mcp.lanes import upsert_worktree_lane

    return upsert_worktree_lane(**kwargs)


def _default_materialize(
    *,
    task_ref: str,
    lane_id: str,
    root: Path,
    apply: bool,
    entry: Mapping[str, Any],
    manifest: Mapping[str, Any],
    run_git: Callable[..., subprocess.CompletedProcess[str]],
) -> Any:
    from workbay_orchestrator_mcp.orchestration.lane_materialize import materialize_lane

    def get_prospective_row(_task_ref: str, _lane_id: str) -> Mapping[str, Any]:
        return {"task_ref": task_ref, "lane_id": lane_id, "worktree_path": entry["worktree_path"]}

    return materialize_lane(
        task_ref,
        lane_id,
        root=root,
        apply=apply,
        load_manifest=lambda _task_ref: manifest,
        get_row=get_prospective_row,
        run_git=run_git,
    )


def _default_pin_routing(*, root: Path, task_ref: str, lane_id: str, entry: Mapping[str, Any]) -> Mapping[str, Any]:
    from workbay_orchestrator_mcp.orchestration.lane_manifest import load_manifest
    from workbay_orchestrator_mcp.orchestration.offload_preflight import materialize_offload_lane_manifest

    routing_keys = {
        "preferred_backend": "preferred_backend",
        "preferred_model": "preferred_model",
        "preferred_reasoning_effort": "preferred_reasoning_effort",
        "preferred_speed": "preferred_speed",
        "preferred_tier": "preferred_tier",
    }
    call: dict[str, Any] = {
        "orchestrator_root": root,
        "task_ref": task_ref,
        "lane_id": lane_id,
        "worktree_path": str(entry["worktree_path"]),
        "branch": str(entry["branch"]),
    }
    for name in routing_keys:
        if name in entry:
            call[name] = entry[name]
    materialize_offload_lane_manifest(**call)
    manifest = load_manifest(task_ref, orchestrator_root=str(root))
    lane = manifest["lanes"][lane_id]
    return {
        "backend": lane.get("preferred_backend"),
        "model": lane.get("preferred_model"),
        "reasoning_effort": lane.get("preferred_reasoning_effort"),
    }


def _manifest_path(root: Path, task_ref: str) -> Path:
    return root / "config" / "lane-orchestration" / f"{task_ref}.json"


def _receipt_path(root: Path, task_ref: str, lane_id: str) -> Path:
    if (
        not task_ref.strip()
        or not lane_id.strip()
        or Path(task_ref).name != task_ref
        or Path(lane_id).name != lane_id
        or task_ref in {".", ".."}
        or lane_id in {".", ".."}
    ):
        raise ProvisionRefused(reason="invalid_identity", detail="task_ref and lane_id must be path components")
    return root / ".task-state" / "lane-provision" / task_ref / f"{lane_id}.json"


def _branch_exists_on_disk(root: Path, branch: str) -> bool:
    try:
        proc = _run_git_default(["show-ref", "--verify", "--quiet", f"refs/heads/{branch}"], cwd=root)
    except (OSError, subprocess.TimeoutExpired):
        return False
    return proc.returncode == 0


def _worktree_exists(path: str) -> bool:
    worktree = Path(path).expanduser()
    if not worktree.is_dir():
        return False
    git_marker = worktree / ".git"
    if git_marker.is_dir():
        return True
    if not git_marker.is_file():
        return False
    try:
        line = git_marker.read_text(encoding="utf-8").strip()
    except OSError:
        return False
    if not line.startswith("gitdir:"):
        return False
    admin = Path(line.partition(":")[2].strip()).expanduser()
    if not admin.is_absolute():
        admin = worktree / admin
    return admin.is_dir()


def _registered_worktree_branch(root: Path, path: str) -> str | None:
    """Return the checked-out branch for this registered worktree path."""
    try:
        target = Path(path).expanduser().resolve()
        listing = _run_git_default(["worktree", "list", "--porcelain"], cwd=root)
    except (OSError, RuntimeError, subprocess.TimeoutExpired):
        return None
    if listing.returncode != 0:
        return None
    for block in listing.stdout.split("\n\n"):
        fields = block.splitlines()
        worktree = next((line.removeprefix("worktree ") for line in fields if line.startswith("worktree ")), None)
        if worktree is None:
            continue
        try:
            if Path(worktree).expanduser().resolve() != target:
                continue
        except (OSError, RuntimeError):
            continue
        branch = next((line.removeprefix("branch refs/heads/") for line in fields if line.startswith("branch ")), None)
        return branch if branch and not branch.startswith("refs/") else None
    return None


def _existing_receipt(path: Path, *, root: Path) -> ProvisionReceipt | None:
    try:
        receipt = ProvisionReceipt.from_json(path.read_text(encoding="utf-8"))
    except (ContractError, json.JSONDecodeError, UnicodeDecodeError, TypeError, ValueError) as exc:
        _LOGGER.warning("ignoring malformed lane provision receipt at %s: %s", path, exc)
        return None
    except (OSError, KeyError, RuntimeError):
        return None
    if (
        receipt.stage == "provisioned"
        and _worktree_exists(receipt.worktree_path)
        and _branch_exists_on_disk(root, receipt.branch)
        and _registered_worktree_branch(root, receipt.worktree_path) == receipt.branch
    ):
        return receipt
    return None


@contextmanager
def _reserve_worktree_path(root: Path, worktree_path: str):
    """Serialize owner checks through row upsert for one resolved path."""
    resolved = str(Path(worktree_path).expanduser().resolve())
    lock_name = hashlib.sha256(resolved.encode("utf-8")).hexdigest() + ".lock"
    lock_path = root / ".task-state" / "lane-provision" / "path-locks" / lock_name
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


def _load_manifest(path: Path, *, validate: bool = True) -> dict[str, Any]:
    from workbay_orchestrator_mcp.orchestration.lane_manifest import validate_manifest

    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ProvisionRefused(reason="manifest_missing", detail=str(exc)) from exc
    except (OSError, json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ProvisionRefused(reason="manifest_read_failed", detail=str(exc)) from exc
    if not isinstance(value, dict):
        raise ProvisionRefused(reason="manifest_invalid", detail="lane manifest must be a JSON object")
    if not validate:
        return value
    try:
        return validate_manifest(value, path)
    except Exception as exc:  # noqa: BLE001 — malformed manifest errors are always typed
        raise ProvisionRefused(reason="manifest_invalid", detail=str(exc)) from exc


def _full_sha(proc: subprocess.CompletedProcess[str]) -> str | None:
    if proc.returncode != 0:
        return None
    value = (proc.stdout or "").strip().splitlines()
    candidate = value[0].strip() if value else ""
    return candidate if _FULL_SHA_RE.fullmatch(candidate) else None


def _invoke_git(
    run_git: Callable[..., subprocess.CompletedProcess[str]], args: Sequence[str], *, root: Path, reason: str
) -> subprocess.CompletedProcess[str]:
    try:
        return run_git(args, cwd=root)
    except subprocess.TimeoutExpired as exc:
        raise ProvisionRefused(reason=f"{reason}_timeout", detail=str(exc)) from exc
    except OSError as exc:
        raise ProvisionRefused(reason=f"{reason}_failed", detail=str(exc)) from exc


def _expand_worktree_path(raw: str, *, root: Path) -> str:
    expanded = raw.replace("{orchestrator_root}", str(root)).strip()
    path = Path(expanded).expanduser()
    if not path.is_absolute():
        path = root / path
    return str(path.resolve())


def _prepare_entry(
    *,
    lane_spec: Mapping[str, Any],
    lane_id: str,
    task_ref: str,
    root: Path,
    lane_kind: str,
    base_sha: str,
    review_of: str | None,
    review_subject: dict[str, str] | None,
) -> dict[str, Any]:
    entry = copy.deepcopy(dict(lane_spec))
    entry["branch"] = str(entry.get("branch") or f"codex/{task_ref}-{lane_id}").strip()
    if not entry["branch"]:
        raise ProvisionRefused(reason="invalid_branch", detail="lane spec branch must be non-empty")
    entry["worktree_path"] = _expand_worktree_path(
        str(entry.get("worktree_path") or f"{{orchestrator_root}}-{task_ref}-{lane_id}"), root=root
    )
    for field_name in ("owned_paths", "test_commands"):
        values = entry.get(field_name)
        if values is None:
            entry[field_name] = []
        elif not isinstance(values, list):
            raise ProvisionRefused(reason="invalid_lane_spec", detail=f"{field_name} must be a list")
    _validate_test_command(entry)
    entry["base_sha"] = base_sha
    entry["lane_kind"] = lane_kind
    if lane_kind == "review":
        if review_subject is None:
            raise ProvisionRefused(reason="review_subject_underivable", detail="review subject could not be derived")
        entry["review_subject"] = dict(review_subject)
        entry["review_of"] = review_of
    else:
        entry.pop("review_subject", None)
        entry.pop("review_of", None)
    return entry


def _replace_values(values: Any, old: str, new: str) -> Any:
    if isinstance(values, list):
        return [new if item == old else item for item in values]
    return values


def _rewrite_relation(mapping: dict[str, Any], *, old: str, new: str) -> None:
    rebuilt: dict[str, Any] = {}
    for key, values in mapping.items():
        target = new if key == old else key
        rewritten = _replace_values(values, old, new)
        if target in rebuilt and isinstance(rebuilt[target], list) and isinstance(rewritten, list):
            rebuilt[target].extend(item for item in rewritten if item not in rebuilt[target])
        else:
            rebuilt[target] = rewritten
    mapping.clear()
    mapping.update(rebuilt)


def _insert_lane(
    manifest: dict[str, Any],
    *,
    lane_id: str,
    entry: dict[str, Any],
    supersedes: str | None,
    stamp: str,
) -> None:
    lanes = manifest.get("lanes")
    if not isinstance(lanes, dict):
        raise ProvisionRefused(reason="manifest_invalid", detail="lane manifest lanes must be an object")
    if lane_id in lanes:
        raise ProvisionRefused(reason="lane_id_reused", detail=f"lane id {lane_id!r} already exists in manifest")
    if supersedes is not None:
        previous = lanes.get(supersedes)
        if not isinstance(previous, dict):
            raise ProvisionRefused(reason="superseded_lane_missing", detail=f"lane {supersedes!r} is missing")
        previous["owned_paths"] = []
        previous["commit_paths"] = []
        previous["retired"] = {"reason": f"superseded_by:{lane_id}", "at": stamp}
    lanes[lane_id] = entry

    merge_order = manifest.setdefault("merge_order", [])
    if not isinstance(merge_order, list):
        raise ProvisionRefused(reason="manifest_invalid", detail="lane manifest merge_order must be a list")
    if supersedes is not None and supersedes in merge_order:
        manifest["merge_order"] = [lane_id if value == supersedes else value for value in merge_order]
    elif lane_id not in merge_order:
        merge_order.append(lane_id)

    downstream = manifest.setdefault("downstream", {})
    if not isinstance(downstream, dict):
        raise ProvisionRefused(reason="manifest_invalid", detail="lane manifest downstream must be an object")
    depends_on = manifest.setdefault("depends_on", {})
    if not isinstance(depends_on, dict):
        raise ProvisionRefused(reason="manifest_invalid", detail="lane manifest depends_on must be an object")
    if supersedes is not None:
        _rewrite_relation(downstream, old=supersedes, new=lane_id)
        _rewrite_relation(depends_on, old=supersedes, new=lane_id)
    downstream.setdefault(lane_id, [])
    depends_on.setdefault(lane_id, [])


def _restore_relation(current: dict[str, Any], original: Mapping[str, Any], *, old: str | None, new: str) -> None:
    """Reverse this call's relation edits while retaining unrelated current entries."""
    original_keys = list(original)
    restored: dict[str, Any] = {}
    for original_key in original_keys:
        current_key = new if old is not None and original_key == old else original_key
        if current_key not in current:
            restored[original_key] = copy.deepcopy(original[original_key])
            continue
        values = copy.deepcopy(current[current_key])
        if old is not None and isinstance(values, list) and isinstance(original[original_key], list):
            remaining = original[original_key].count(old)
            for index, value in enumerate(values):
                if value == new and remaining:
                    values[index] = old
                    remaining -= 1
        restored[original_key] = values
    for key, values in current.items():
        if key == new:
            continue
        if key not in restored:
            restored[key] = copy.deepcopy(values)
    current.clear()
    current.update(restored)


def _rollback_manifest(
    manifest: dict[str, Any],
    *,
    lane_id: str,
    supersedes: str | None,
    snapshot: Mapping[str, Any],
    inserted: Mapping[str, Any],
) -> None:
    lanes = manifest.get("lanes")
    if not isinstance(lanes, dict):
        raise RuntimeError("cannot roll back lane provisioning: manifest lanes is not an object")
    current_lane = lanes.get(lane_id)
    if (
        not isinstance(current_lane, Mapping)
        or current_lane.get("branch") != inserted.get("branch")
        or current_lane.get("worktree_path") != inserted.get("worktree_path")
    ):
        raise RuntimeError(f"cannot roll back lane provisioning: inserted lane {lane_id!r} changed")
    lanes.pop(lane_id)
    if supersedes is not None:
        original_lanes = snapshot.get("lanes")
        old_before = original_lanes.get(supersedes) if isinstance(original_lanes, Mapping) else None
        old_after = lanes.get(supersedes)
        if isinstance(old_before, Mapping) and isinstance(old_after, dict):
            for field_name in ("owned_paths", "commit_paths", "retired"):
                if field_name in old_before:
                    old_after[field_name] = copy.deepcopy(old_before[field_name])
                else:
                    old_after.pop(field_name, None)

    original_order = snapshot.get("merge_order")
    order = manifest.get("merge_order")
    if isinstance(original_order, list) and isinstance(order, list):
        if supersedes is not None and supersedes in original_order:
            result = list(order)
            replaced = False
            for index, value in enumerate(result):
                if value == lane_id and not replaced:
                    result[index] = supersedes
                    replaced = True
            manifest["merge_order"] = result
        else:
            manifest["merge_order"] = [value for value in order if value != lane_id]

    for relation_name in ("downstream", "depends_on"):
        current = manifest.get(relation_name)
        original = snapshot.get(relation_name)
        if isinstance(current, dict) and isinstance(original, Mapping):
            _restore_relation(current, original, old=supersedes, new=lane_id)
        elif isinstance(current, dict):
            current.pop(lane_id, None)


def _row_id(row: Mapping[str, Any]) -> Any:
    return row.get("id", row.get("row_id", row.get("lane_row_id")))


def _path_equal(left: str, right: str, *, root: Path) -> bool:
    try:
        left_path = Path(left).expanduser()
        if not left_path.is_absolute():
            left_path = root / left_path
        return left_path.resolve() == Path(right).expanduser().resolve()
    except (OSError, RuntimeError, ValueError):
        return False


def _row_payload(
    *, task_ref: str, lane_id: str, entry: Mapping[str, Any], lane_kind: str, review_subject: Mapping[str, str] | None
) -> dict[str, Any]:
    test_commands = entry.get("test_commands")
    test_cmd = entry.get("test_cmd")
    if test_cmd is None and isinstance(test_commands, list) and test_commands:
        test_cmd = test_commands[0]
    payload: dict[str, Any] = {
        "task_ref": task_ref,
        "lane_id": lane_id,
        "worktree_path": entry["worktree_path"],
        "branch": entry["branch"],
        "title": entry.get("title"),
        "objective": entry.get("objective"),
        "owner_agent": entry.get("owner_agent"),
        "model": entry.get("model", entry.get("preferred_model")),
        "backend": entry.get("backend", entry.get("preferred_backend")),
        "reasoning_effort": entry.get("reasoning_effort", entry.get("preferred_reasoning_effort")),
        "speed": entry.get("speed", entry.get("preferred_speed")),
        "tier": entry.get("tier", entry.get("preferred_tier")),
        "test_cmd": test_cmd,
        "lane_kind": lane_kind,
        "status": "planned",
    }
    if review_subject is not None:
        payload["review_base_ref"] = review_subject["base_ref"]
        payload["review_tip_ref"] = review_subject["tip_ref"]
        payload["update_review_subject"] = True
        notes = entry.get("notes")
        if isinstance(notes, str):
            try:
                decoded = json.loads(notes)
            except json.JSONDecodeError:
                decoded = None
            envelope = dict(decoded) if isinstance(decoded, dict) else {"prose": notes}
        elif isinstance(notes, Mapping):
            envelope = dict(notes)
        else:
            envelope = {}
        envelope["review_subject"] = dict(review_subject)
        payload["notes"] = json.dumps(envelope, sort_keys=True)
    return payload


def _receipt(
    *,
    task_ref: str,
    lane_id: str,
    branch: str,
    worktree_path: str,
    base_ref: str,
    base_sha: str,
    lane_kind: str,
    review_subject: dict[str, str] | None,
    supersedes: str | None,
    routing: Mapping[str, Any] | None,
    manifest_sha: str,
    stage: str,
    refusal_reason: str | None,
    actions: list[str],
    now: Callable[[], datetime] | None,
    extras: Mapping[str, Any] | None = None,
) -> ProvisionReceipt:
    return ProvisionReceipt(
        task_ref=task_ref,
        lane_id=lane_id,
        branch=branch,
        worktree_path=worktree_path,
        base_ref=base_ref,
        base_sha=base_sha,
        lane_kind=lane_kind,
        review_subject=dict(review_subject) if review_subject is not None else None,
        supersedes=supersedes,
        routing=dict(routing or {}),
        manifest_sha256_after=manifest_sha,
        stage=stage,
        refusal_reason=refusal_reason,
        actions=actions,
        created_at=_timestamp(now),
        extras=dict(extras or {}),
    )


def _write_receipt(path: Path, receipt: ProvisionReceipt) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            stream.write(receipt.to_json())
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp_name, path)
    except Exception:
        try:
            os.unlink(temp_name)
        except OSError:
            pass
        raise


def _manifest_digest(path: Path) -> str:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return ""


def _collision_reason(exc: BaseException) -> str:
    return "owned_path_collision" if "owned_paths collision" in str(exc).lower() else "manifest_update_failed"


def _materialization_actions(actions: list[str], materialized: Any) -> list[str]:
    result = list(actions)
    branch_outcome = str(getattr(materialized, "branch_outcome", "planned"))
    worktree_outcome = str(getattr(materialized, "worktree_outcome", "planned"))
    if branch_outcome not in {"planned", "created"}:
        result[result.index("materialize_branch")] = "verify_branch"
    if worktree_outcome not in {"planned", "created"}:
        result[result.index("ensure_worktree")] = "verify_worktree"
    return result


@dataclass(slots=True)
class _ProvisionPlan:
    task_ref: str
    lane_id: str
    root: Path
    manifest_file: Path
    receipt_file: Path
    base_ref: str
    base_sha: str
    lane_kind: str
    review_subject: dict[str, str] | None
    supersedes: str | None
    entry: dict[str, Any]
    requested_entry: dict[str, Any]
    manifest_before: dict[str, Any]
    actions: list[str]
    now: Callable[[], datetime] | None
    simulated_manifest: dict[str, Any] | None = None
    rollback_snapshot: dict[str, Any] | None = None
    registered: bool = False
    resuming: bool = False
    row_exists: bool = False

    @property
    def branch(self) -> str:
        return str(self.entry["branch"])

    @property
    def worktree_path(self) -> str:
        return str(self.entry["worktree_path"])


def _validate_request(lane_spec: Mapping[str, Any], lane_kind: str) -> None:
    if not isinstance(lane_spec, Mapping):
        raise ProvisionRefused(reason="invalid_lane_spec", detail="lane_spec must be a mapping")
    if lane_kind not in {"implement", "review"}:
        raise ProvisionRefused(reason="invalid_lane_kind", detail=f"unsupported lane_kind {lane_kind!r}")


def _validate_test_command(entry: Mapping[str, Any]) -> None:
    test_cmd = entry.get("test_cmd")
    test_commands = entry.get("test_commands")
    if test_cmd is None and isinstance(test_commands, list) and test_commands:
        test_cmd = test_commands[0]
    try:
        from workbay_orchestrator_mcp.orchestration.lane_test_cmd import classify_test_cmd

        classify_test_cmd(test_cmd)
    except (TypeError, ValueError) as exc:
        raise ProvisionRefused(reason="invalid_test_cmd", detail=str(exc)) from exc


def _check_provision_hold(task_ref: str, root: Path, deps: ProvisionDeps, *, apply: bool) -> None:
    checker = deps.check_held
    if checker is None and apply:
        # Resolve the real coordination store lazily to keep provision imports
        # independent of the coordination CLI and to bind holds to the primary
        # checkout resolved by provision_lane.
        from workbay_orchestrator_mcp.orchestration.coordination import check_held as coordination_check_held

        def checker(*, resource: str) -> Any:
            return coordination_check_held(root, resource=resource)

    if checker is None:
        # Preview must not create the coordination directory or lock, recover a
        # journal in place, or persist expiry events. Read the durable snapshot
        # directly and treat expired records as inactive without changing them.
        from workbay_orchestrator_mcp.orchestration import coordination

        def checker(*, resource: str) -> Any:
            state_dir = root / coordination._STATE_PATH
            journal = coordination._load_journal(state_dir)
            state = journal["state"] if journal is not None else coordination._load_state(state_dir)
            record = coordination._active_record(state, resource, time.time())
            return None if record is None else coordination._held_view(record)

    try:
        held = checker(resource=f"lane.provision:{task_ref}")
    except Exception as exc:  # noqa: BLE001 — hold lookup must fail closed before any writes
        reason = "hold_state_unreadable" if deps.check_held is None else "hold_check_failed"
        raise ProvisionRefused(reason=reason, detail=str(exc)) from exc
    if held is not None:
        raise ProvisionRefused(reason="held_by", detail=held)


def _resolve_base_sha(root: Path, base_ref: str, run_git: Callable[..., Any]) -> str:
    base_proc = _invoke_git(run_git, ["rev-parse", "--verify", f"{base_ref}^{{commit}}"], root=root, reason="base_ref")
    base_sha = _full_sha(base_proc)
    if base_sha is None:
        detail = (base_proc.stderr or base_proc.stdout or "base_ref did not resolve to a full commit SHA").strip()
        raise ProvisionRefused(reason="base_ref_unresolvable", detail=detail)
    return base_sha


def _derive_review_subject(
    root: Path, base_ref: str, review_of: str | None, lane_kind: str, run_git: Callable[..., Any]
) -> dict[str, str] | None:
    if lane_kind != "review":
        return None
    if not review_of:
        raise ProvisionRefused(reason="review_subject_underivable", detail="review lane requires review_of")
    merge_proc = _invoke_git(run_git, ["merge-base", review_of, base_ref], root=root, reason="review_subject")
    tip_proc = _invoke_git(
        run_git, ["rev-parse", "--verify", f"{review_of}^{{commit}}"], root=root, reason="review_subject"
    )
    merge_sha = _full_sha(merge_proc)
    tip_sha = _full_sha(tip_proc)
    if merge_sha is None or tip_sha is None:
        detail = (
            merge_proc.stderr or tip_proc.stderr or "review base or tip did not resolve to a full commit SHA"
        ).strip()
        raise ProvisionRefused(reason="review_subject_underivable", detail=detail)
    return {"base_ref": merge_sha, "tip_ref": tip_sha}


def _build_plan(
    *,
    task_ref: str,
    lane_id: str,
    lane_spec: Mapping[str, Any],
    base_ref: str,
    root: Path,
    receipt_file: Path,
    supersedes: str | None,
    lane_kind: str,
    review_of: str | None,
    deps: ProvisionDeps,
    apply: bool,
) -> _ProvisionPlan:
    _validate_test_command(lane_spec)
    _check_provision_hold(task_ref, root, deps, apply=apply)
    manifest_file = _manifest_path(root, task_ref)
    manifest_before = _load_manifest(manifest_file, validate=supersedes is None)
    run_git = deps.run_git or _run_git_default
    base_sha = _resolve_base_sha(root, base_ref, run_git)
    review_subject = _derive_review_subject(root, base_ref, review_of, lane_kind, run_git)
    entry = _prepare_entry(
        lane_spec=lane_spec,
        lane_id=lane_id,
        task_ref=task_ref,
        root=root,
        lane_kind=lane_kind,
        base_sha=base_sha,
        review_of=review_of,
        review_subject=review_subject,
    )
    actions = [
        "register_manifest_lane",
        "materialize_branch",
        "ensure_worktree",
        "upsert_worktree_lane",
        "pin_routing",
        "write_receipt",
    ]
    if supersedes is not None:
        actions.insert(1, f"retire_and_repoint:{supersedes}")
    return _ProvisionPlan(
        task_ref=task_ref,
        lane_id=lane_id,
        root=root,
        manifest_file=manifest_file,
        receipt_file=receipt_file,
        base_ref=base_ref,
        base_sha=base_sha,
        lane_kind=lane_kind,
        review_subject=review_subject,
        supersedes=supersedes,
        entry=entry,
        requested_entry=copy.deepcopy(entry),
        manifest_before=manifest_before,
        actions=actions,
        now=deps.now,
    )


def _read_owner_rows(deps: ProvisionDeps, plan: _ProvisionPlan) -> list[Mapping[str, Any]]:
    try:
        rows = list((deps.list_rows or _default_rows)())
    except Exception as exc:  # noqa: BLE001 — ownership census must fail closed
        raise ProvisionRefused(reason="worktree_owner_lookup_failed", detail=str(exc)) from exc
    if any(not isinstance(row, Mapping) for row in rows):
        raise ProvisionRefused(
            reason="worktree_owner_lookup_failed", detail="worktree_lanes census has a malformed row"
        )
    if any(not _same_row_identity(row, plan) and _incomplete_owner_row(row) for row in rows):
        raise ProvisionRefused(
            reason="worktree_owner_lookup_failed", detail="worktree_lanes census has an incomplete row"
        )
    return rows


def _incomplete_owner_row(row: Mapping[str, Any]) -> bool:
    return (
        not isinstance(row.get("worktree_path"), str)
        or not str(row.get("worktree_path") or "").strip()
        or not isinstance(row.get("status"), str)
    )


def _same_row_identity(row: Mapping[str, Any], plan: _ProvisionPlan) -> bool:
    return row.get("task_ref") == plan.task_ref and row.get("lane_id") == plan.lane_id


def _row_for_plan(rows: Iterable[Mapping[str, Any]], plan: _ProvisionPlan) -> Mapping[str, Any] | None:
    return next((row for row in rows if _same_row_identity(row, plan)), None)


def _is_terminal_owner(row: Mapping[str, Any]) -> bool:
    from workbay_orchestrator_mcp.orchestration.lane_worktree import _TERMINAL_LANE_STATUSES

    return str(row.get("status") or "").strip().lower() in _TERMINAL_LANE_STATUSES


def _matches_requested_lane(persisted: Mapping[str, Any], requested: Mapping[str, Any], *, root: Path) -> bool:
    if persisted.get("branch") != requested.get("branch"):
        return False
    if not _path_equal(str(persisted.get("worktree_path") or ""), str(requested.get("worktree_path") or ""), root=root):
        return False
    for key in ("owned_paths", "test_commands", "lane_kind", "review_subject"):
        if persisted.get(key, [] if key in {"owned_paths", "test_commands"} else None) != requested.get(
            key, [] if key in {"owned_paths", "test_commands"} else None
        ):
            return False
    return True


def _restore_resumable_lane(plan: _ProvisionPlan, manifest: Mapping[str, Any], rows: list[Mapping[str, Any]]) -> bool:
    lanes = manifest.get("lanes")
    persisted = lanes.get(plan.lane_id) if isinstance(lanes, Mapping) else None
    row = _row_for_plan(rows, plan)
    if not isinstance(persisted, Mapping):
        if row is not None:
            raise ProvisionRefused(
                reason="lane_id_reused", detail=f"worktree_lanes row already uses {plan.task_ref}/{plan.lane_id}"
            )
        return False
    if not _matches_requested_lane(persisted, plan.requested_entry, root=plan.root):
        raise ProvisionRefused(reason="lane_id_reused", detail=f"lane id {plan.lane_id!r} already exists")
    if row is not None:
        if _is_terminal_owner(row):
            raise ProvisionRefused(
                reason="lane_id_reused", detail=f"worktree_lanes row for {plan.lane_id!r} is terminal"
            )
        if not _path_equal(
            str(row.get("worktree_path") or ""), str(persisted.get("worktree_path") or ""), root=plan.root
        ):
            raise ProvisionRefused(reason="lane_id_reused", detail=f"worktree row for {plan.lane_id!r} changed path")
        if row.get("branch") not in (None, persisted.get("branch")):
            raise ProvisionRefused(reason="lane_id_reused", detail=f"worktree row for {plan.lane_id!r} changed branch")
    plan.entry = copy.deepcopy(dict(persisted))
    plan.base_sha = str(plan.entry.get("base_sha") or plan.base_sha)
    plan.lane_kind = str(plan.entry.get("lane_kind") or plan.lane_kind)
    subject = plan.entry.get("review_subject")
    plan.review_subject = dict(subject) if isinstance(subject, Mapping) else None
    plan.row_exists = row is not None
    plan.resuming = True
    return True


def _owner_for_path(rows: Iterable[Mapping[str, Any]], plan: _ProvisionPlan) -> Mapping[str, Any] | None:
    for row in rows:
        if _is_terminal_owner(row) or (plan.resuming and _same_row_identity(row, plan)):
            continue
        path = row.get("worktree_path")
        if isinstance(path, str) and _path_equal(path, plan.worktree_path, root=plan.root):
            return row
    return None


def _update_manifest(
    path: Path, mutation: Callable[[dict[str, Any]], None], *, pre_validate: bool, deps: ProvisionDeps
) -> Any:
    if deps.update_manifest is not None:
        return deps.update_manifest(path, mutation, pre_validate=pre_validate)
    from workbay_orchestrator_mcp.orchestration.lane_manifest import atomic_update_manifest

    return atomic_update_manifest(path, mutation, pre_validate=pre_validate)


def _register_manifest_lane(plan: _ProvisionPlan, deps: ProvisionDeps) -> None:
    snapshot: dict[str, Any] = {}

    def mutate(manifest: dict[str, Any]) -> None:
        snapshot["manifest"] = copy.deepcopy(manifest)
        _insert_lane(
            manifest,
            lane_id=plan.lane_id,
            entry=copy.deepcopy(plan.entry),
            supersedes=plan.supersedes,
            stamp=_timestamp(deps.now),
        )

    try:
        _update_manifest(plan.manifest_file, mutate, pre_validate=plan.supersedes is None, deps=deps)
    except ProvisionRefused:
        raise
    except Exception as exc:  # noqa: BLE001 — failed validation leaves the manifest unchanged
        raise ProvisionRefused(reason=_collision_reason(exc), detail=str(exc)) from exc
    plan.rollback_snapshot = snapshot.get("manifest", plan.manifest_before)
    plan.simulated_manifest = _load_manifest(plan.manifest_file)
    plan.registered = True


def _rollback_manifest_lane(plan: _ProvisionPlan, deps: ProvisionDeps) -> None:
    if not plan.registered:
        return
    _update_manifest(
        plan.manifest_file,
        lambda current: _rollback_manifest(
            current,
            lane_id=plan.lane_id,
            supersedes=plan.supersedes,
            snapshot=plan.rollback_snapshot or plan.manifest_before,
            inserted=plan.entry,
        ),
        pre_validate=plan.supersedes is None,
        deps=deps,
    )


def _refuse_after_insert(
    plan: _ProvisionPlan,
    deps: ProvisionDeps,
    reason: str,
    detail: Any,
    *,
    rollback: bool,
    extras: Mapping[str, Any] | None = None,
) -> None:
    rollback_error: str | None = None
    if rollback:
        try:
            _rollback_manifest_lane(plan, deps)
        except Exception as exc:  # noqa: BLE001 — preserve the original cause
            rollback_error = str(exc)
    actual_reason = "rollback_failed" if rollback_error else reason
    all_extras = dict(extras or {})
    all_extras["cause"] = {"reason": reason, "detail": detail}
    if rollback_error:
        all_extras["rollback_error"] = rollback_error
    refused = _receipt(
        task_ref=plan.task_ref,
        lane_id=plan.lane_id,
        branch=plan.branch,
        worktree_path=plan.worktree_path,
        base_ref=plan.base_ref,
        base_sha=plan.base_sha,
        lane_kind=plan.lane_kind,
        review_subject=plan.review_subject,
        supersedes=plan.supersedes,
        routing={},
        manifest_sha=_manifest_digest(plan.manifest_file),
        stage="refused",
        refusal_reason=actual_reason,
        actions=plan.actions,
        now=deps.now,
        extras=all_extras,
    )
    try:
        _write_receipt(plan.receipt_file, refused)
    except OSError as exc:
        all_extras["refused_receipt_write_error"] = str(exc)
    raise ProvisionRefused(reason=actual_reason, detail=detail, receipt=refused)


def _materialize_plan(plan: _ProvisionPlan, deps: ProvisionDeps, *, apply: bool) -> Any:
    materialize = deps.materialize
    if materialize is None:

        def materialize(task_ref: str, lane_id: str, *, root: Path, apply: bool) -> Any:
            return _default_materialize(
                task_ref=task_ref,
                lane_id=lane_id,
                root=root,
                apply=apply,
                entry=plan.entry,
                manifest=plan.simulated_manifest or plan.manifest_before,
                run_git=deps.run_git or _run_git_default,
            )

    try:
        result = materialize(plan.task_ref, plan.lane_id, root=plan.root, apply=apply)
    except Exception as exc:  # noqa: BLE001 — materialization must be reported as a typed refusal
        if apply:
            _refuse_after_insert(plan, deps, "materialize_failed", str(exc), rollback=plan.registered)
        raise ProvisionRefused(reason="materialize_failed", detail=str(exc)) from exc
    reason = getattr(result, "refusal_kind", None)
    detail = getattr(result, "detail", str(reason)) if reason else ""
    if isinstance(result, Mapping) and result.get("ok") is False:
        reason = str(result.get("reason") or result.get("error_kind") or "materialize_refused")
        detail = result.get("detail", result.get("error", reason))
    if reason:
        if apply:
            _refuse_after_insert(plan, deps, str(reason), detail, rollback=plan.registered)
        raise ProvisionRefused(reason=str(reason), detail=detail)
    return result


def _upsert_lane_row(plan: _ProvisionPlan, deps: ProvisionDeps) -> None:
    if plan.row_exists:
        return
    try:
        result = (deps.upsert_row or _default_upsert_row)(
            **_row_payload(
                task_ref=plan.task_ref,
                lane_id=plan.lane_id,
                entry=plan.entry,
                lane_kind=plan.lane_kind,
                review_subject=plan.review_subject,
            )
        )
    except Exception as exc:  # noqa: BLE001 — row failures trigger manifest compensation
        _refuse_after_insert(plan, deps, "row_upsert_failed", str(exc), rollback=plan.registered)
    if isinstance(result, Mapping) and result.get("ok") is False:
        reason = str(result.get("reason") or result.get("error_kind") or "row_upsert_refused")
        _refuse_after_insert(
            plan, deps, reason, result.get("detail", result.get("error", reason)), rollback=plan.registered
        )
    plan.row_exists = True


def _apply_locked_phases(plan: _ProvisionPlan, deps: ProvisionDeps) -> Any:
    with _reserve_worktree_path(plan.root, plan.worktree_path):
        manifest = _load_manifest(plan.manifest_file, validate=plan.supersedes is None)
        rows = _read_owner_rows(deps, plan)
        resumed = _restore_resumable_lane(plan, manifest, rows)
        plan.simulated_manifest = dict(manifest)
        if not resumed:
            _register_manifest_lane(plan, deps)
        owner = _owner_for_path(rows, plan)
        if owner is not None:
            detail = {"row_id": _row_id(owner), "lane_id": owner.get("lane_id"), "task_ref": owner.get("task_ref")}
            _refuse_after_insert(
                plan,
                deps,
                "worktree_owned",
                detail,
                rollback=plan.registered,
                extras={"worktree_owner": detail},
            )
        materialized = _materialize_plan(plan, deps, apply=True)
        _upsert_lane_row(plan, deps)
    return materialized


def _pin_plan_routing(plan: _ProvisionPlan, deps: ProvisionDeps) -> Mapping[str, Any]:
    pin = deps.pin_routing
    if pin is None:
        try:
            result = _default_pin_routing(
                root=plan.root, task_ref=plan.task_ref, lane_id=plan.lane_id, entry=plan.entry
            )
        except Exception as exc:  # noqa: BLE001 — routing failure keeps committed manifest and row resumable
            _refuse_after_insert(plan, deps, "routing_pin_failed", str(exc), rollback=False)
    else:
        try:
            result = pin(root=plan.root, task_ref=plan.task_ref, lane_id=plan.lane_id, entry=plan.entry)
        except Exception as exc:  # noqa: BLE001 — routing failure keeps committed manifest and row resumable
            _refuse_after_insert(plan, deps, "routing_pin_failed", str(exc), rollback=False)
    if not isinstance(result, Mapping):
        _refuse_after_insert(
            plan, deps, "routing_pin_failed", "routing pin returned a non-mapping result", rollback=False
        )
    return result


def _publish_provisioned_receipt(
    plan: _ProvisionPlan, deps: ProvisionDeps, materialized: Any, routing: Mapping[str, Any]
) -> ProvisionReceipt:
    resolved_routing = {
        "backend": routing.get("backend", routing.get("preferred_backend")),
        "model": routing.get("model", routing.get("preferred_model")),
        "reasoning_effort": routing.get("reasoning_effort", routing.get("preferred_reasoning_effort")),
    }
    receipt = _receipt(
        task_ref=plan.task_ref,
        lane_id=plan.lane_id,
        branch=plan.branch,
        worktree_path=str(getattr(materialized, "worktree_path", plan.worktree_path)),
        base_ref=plan.base_ref,
        base_sha=plan.base_sha,
        lane_kind=plan.lane_kind,
        review_subject=plan.review_subject,
        supersedes=plan.supersedes,
        routing=resolved_routing,
        manifest_sha=_manifest_digest(plan.manifest_file),
        stage="provisioned",
        refusal_reason=None,
        actions=_materialization_actions(plan.actions, materialized),
        now=deps.now,
    )
    try:
        _write_receipt(plan.receipt_file, receipt)
    except OSError as exc:
        raise ProvisionRefused(reason="receipt_write_failed", detail=str(exc), receipt=receipt) from exc
    return receipt


def _apply_plan(plan: _ProvisionPlan, deps: ProvisionDeps) -> ProvisionReceipt:
    materialized = _apply_locked_phases(plan, deps)
    routing = _pin_plan_routing(plan, deps)
    return _publish_provisioned_receipt(plan, deps, materialized, routing)


def _plan_without_writes(plan: _ProvisionPlan, deps: ProvisionDeps) -> ProvisionReceipt:
    lanes = plan.manifest_before.get("lanes")
    if isinstance(lanes, Mapping) and plan.lane_id in lanes:
        raise ProvisionRefused(reason="lane_id_reused", detail=f"lane id {plan.lane_id!r} already exists in manifest")
    rows = _read_owner_rows(deps, plan)
    if _row_for_plan(rows, plan) is not None:
        raise ProvisionRefused(
            reason="lane_id_reused", detail=f"worktree_lanes row already uses {plan.task_ref}/{plan.lane_id}"
        )
    owner = _owner_for_path(rows, plan)
    if owner is not None:
        detail = {"row_id": _row_id(owner), "lane_id": owner.get("lane_id"), "task_ref": owner.get("task_ref")}
        raise ProvisionRefused(reason="worktree_owned", detail=detail)
    from workbay_orchestrator_mcp.orchestration.lane_manifest import validate_manifest

    preview = copy.deepcopy(plan.manifest_before)
    _insert_lane(
        preview,
        lane_id=plan.lane_id,
        entry=copy.deepcopy(plan.entry),
        supersedes=plan.supersedes,
        stamp=_timestamp(deps.now),
    )
    try:
        plan.simulated_manifest = validate_manifest(preview, plan.manifest_file)
    except Exception as exc:  # noqa: BLE001 — malformed previews receive a stable refusal
        raise ProvisionRefused(reason=_collision_reason(exc), detail=str(exc)) from exc
    materialized = _materialize_plan(plan, deps, apply=False)
    preview_bytes = (json.dumps(plan.simulated_manifest, indent=2) + "\n").encode("utf-8")
    return _receipt(
        task_ref=plan.task_ref,
        lane_id=plan.lane_id,
        branch=plan.branch,
        worktree_path=plan.worktree_path,
        base_ref=plan.base_ref,
        base_sha=plan.base_sha,
        lane_kind=plan.lane_kind,
        review_subject=plan.review_subject,
        supersedes=plan.supersedes,
        routing={
            "backend": plan.entry.get("preferred_backend"),
            "model": plan.entry.get("preferred_model"),
            "reasoning_effort": plan.entry.get("preferred_reasoning_effort"),
        },
        manifest_sha=hashlib.sha256(preview_bytes).hexdigest(),
        stage="planned",
        refusal_reason=None,
        actions=_materialization_actions(plan.actions, materialized),
        now=deps.now,
    )


def provision_lane(
    *,
    task_ref: str,
    lane_id: str,
    lane_spec: Mapping[str, Any],
    base_ref: str,
    root: Path,
    apply: bool = False,
    supersedes: str | None = None,
    lane_kind: str = "implement",
    review_of: str | None = None,
    deps: ProvisionDeps | None = None,
) -> ProvisionReceipt:
    """Register one lane and resume incomplete apply phases from persisted state."""
    _validate_request(lane_spec, lane_kind)
    primary = Path(root).expanduser().resolve()
    receipt_file = _receipt_path(primary, task_ref, lane_id)
    existing = _existing_receipt(receipt_file, root=primary)
    if existing is not None:
        return existing
    dependencies = deps or ProvisionDeps()
    plan = _build_plan(
        task_ref=task_ref,
        lane_id=lane_id,
        lane_spec=lane_spec,
        base_ref=base_ref,
        root=primary,
        receipt_file=receipt_file,
        supersedes=supersedes,
        lane_kind=lane_kind,
        review_of=review_of,
        deps=dependencies,
        apply=apply,
    )
    return _apply_plan(plan, dependencies) if apply else _plan_without_writes(plan, dependencies)
