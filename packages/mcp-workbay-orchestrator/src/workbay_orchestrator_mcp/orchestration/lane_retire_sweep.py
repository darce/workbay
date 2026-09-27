"""Bounded single-flight retirement sweep for terminal lane worktrees."""

from __future__ import annotations

import argparse
import ctypes
import fcntl
import json
import multiprocessing
import os
import signal
import subprocess
import sys
import time
import uuid
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, TextIO

_MODULE = "workbay_orchestrator_mcp.orchestration.lane_retire_sweep"
_LOCK_RELATIVE = Path(".task-state/locks/lane-retire-sweep.lock")
_LAUNCH_LOCK_RELATIVE = Path(".task-state/locks/lane-retire-sweep-launch.lock")
_RECEIPT_RELATIVE = Path(".task-state/lane-retire-sweep.jsonl")
_RETRY_STATE_RELATIVE = Path(".task-state/lane-retire-sweep-state.json")
_CHILD_MARKER_RELATIVE = Path(".task-state/lane-retire-sweep-child.json")
_LOG_RELATIVE = Path(".task-state/lane-retire-sweep.log")
_RECEIPT_LIMIT_BYTES = 1024 * 1024
_STATE_MAX_ENTRIES = 500
_STATE_TTL_S = 7 * 24 * 60 * 60
_RETRY_BASE_S = 5 * 60
_RETRY_MAX_S = 60 * 60
_DETAIL_CAP = 200
_RETIRE_OPERATION_TIMEOUT_S = 15.0
_RETIRE_TIMEOUT_CLEANUP_S = 0.2
_SWEEP_CHILD_LIFETIME_S = 180.0
_CHILD_HANDSHAKE_TIMEOUT_S = 3.0
_SWEEP_LOG_LIMIT_BYTES = 256 * 1024
_DEFAULT_SWEEP_BUDGET_S = 30.0
_DEFAULT_MAX_LANES = 20
_GIT_STATUS_TIMEOUT_S = 5.0
_PROCESS_PROBE_TIMEOUT_S = 5.0
_ACTIVE_MERGED_PROBE_TIMEOUT_S = 5.0
_Popen = subprocess.Popen


class _BudgetExhausted(RuntimeError):
    """Lane discovery reached the sweep deadline after bounded progress."""

    outcome = "budget_exhausted"

    def __init__(
        self,
        *,
        pages_fetched: int,
        rows_fetched: int,
        total_matching: int | None,
    ) -> None:
        super().__init__("budget_exhausted")
        self.pages_fetched = pages_fetched
        self.rows_fetched = rows_fetched
        self.total_matching = total_matching


def _monotonic() -> float:
    return time.monotonic()


def _configure_runtime(repo_root: Path) -> Path:
    """Bind handoff APIs to this repository before touching lane state."""
    from workbay_handoff_mcp import configure_runtime  # noqa: PLC0415
    from workbay_handoff_mcp.config import RuntimeConfig  # noqa: PLC0415

    config = RuntimeConfig.for_repo(repo_root)
    configure_runtime(config)
    return Path(config.workspace_root).resolve()


def _try_acquire_lock(repo_root: Path) -> tuple[TextIO | None, str | None]:
    return _try_acquire_file_lock(repo_root, _LOCK_RELATIVE)


def _release_lock(handle: TextIO) -> None:
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    finally:
        handle.close()


def _try_acquire_file_lock(repo_root: Path, relative: Path) -> tuple[TextIO | None, str | None]:
    path = repo_root / relative
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        handle = path.open("a+", encoding="utf-8")
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            handle.close()
            return None, "already_running"
        return handle, None
    except OSError as exc:
        return None, f"sweep_lock_failed:{type(exc).__name__}:{exc}"


def _atomic_json_replace(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, sort_keys=True, separators=(",", ":"))
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _list_lane_rows_page(
    *,
    task_ref: str | None,
    offset: int,
    after_id: int | None,
    limit: int,
) -> dict[str, Any]:
    """Fetch one lane-list page using the appropriate task scope."""
    if task_ref is not None:
        from workbay_orchestrator_mcp.lanes import manage_worktree_lane  # noqa: PLC0415
        from workbay_orchestrator_mcp.orchestration.offload_pass import (  # noqa: PLC0415
            _lane_payload_dict,
        )

        return _lane_payload_dict(
            manage_worktree_lane(
                operation="list",
                task_ref=task_ref,
                status="all",
                limit=limit,
                offset=offset,
            )
        )

    from workbay_orchestrator_mcp.orchestration.lane_worktree import (  # noqa: PLC0415
        _list_lanes,
    )

    envelope = _list_lanes(after_id=after_id, limit=limit)
    if not isinstance(envelope, dict):
        return {}
    data = envelope.get("data")
    if not isinstance(data, dict):
        return envelope
    flattened = dict(envelope)
    flattened.update(data)
    return flattened


def _load_lane_rows(
    repo_root: Path,
    task_ref: str | None,
    *,
    deadline: float,
    progress: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Read a complete paged lane-row snapshot, checking the deadline per page."""
    del repo_root  # Listing is bound to the configured handoff runtime.
    from workbay_orchestrator_mcp.orchestration.offload_pass import (  # noqa: PLC0415
        _MERGECLOSE_LIST_MAX_PAGES,
        _MERGECLOSE_LIST_PAGE_SIZE,
    )

    all_lanes: list[Any] = []
    after_id: int | None = None
    offset = 0
    last_listed: dict[str, Any] = {}
    pages_fetched = 0
    total_matching: int | None = None

    def update_progress() -> None:
        if progress is not None:
            progress.update(
                {
                    "pages_fetched": pages_fetched,
                    "rows_fetched": len(all_lanes),
                    "total_matching": total_matching,
                }
            )

    def budget_exhausted() -> _BudgetExhausted:
        update_progress()
        return _BudgetExhausted(
            pages_fetched=pages_fetched,
            rows_fetched=len(all_lanes),
            total_matching=total_matching,
        )

    for page_idx in range(_MERGECLOSE_LIST_MAX_PAGES):
        if _monotonic() >= deadline:
            raise budget_exhausted()
        listed = _list_lane_rows_page(
            task_ref=task_ref,
            offset=offset,
            after_id=after_id,
            limit=_MERGECLOSE_LIST_PAGE_SIZE,
        )
        last_listed = listed
        if not isinstance(listed, dict) or listed.get("ok") is not True:
            detail = listed.get("error") if isinstance(listed, dict) else None
            raise RuntimeError(str(detail or "lane_list_failed"))
        page = listed.get("lanes")
        if not isinstance(page, list):
            raise RuntimeError(str(listed.get("error") or "lane_list_malformed"))
        all_lanes.extend(page)
        pages_fetched = page_idx + 1
        raw_total = listed.get("total_matching")
        total_matching = raw_total if type(raw_total) is int else None
        update_progress()
        if any(not isinstance(row, dict) for row in all_lanes):
            raise RuntimeError("lane_list_malformed")

        if _monotonic() >= deadline:
            raise budget_exhausted()

        if task_ref is not None:
            if listed.get("has_more") is not True:
                return all_lanes
            if not page:
                break
            offset += len(page)
            continue

        has_more = listed.get("has_more")
        if not isinstance(has_more, bool):
            raise RuntimeError("cross_task_list_lanes has_more missing or non-bool")
        if not has_more:
            if type(raw_total) is not int:
                raise RuntimeError("cross_task_list_lanes total_matching missing or non-int")
            if raw_total != len(all_lanes):
                raise RuntimeError(f"total_matching drift: collected={len(all_lanes)} total_matching={raw_total}")
            return all_lanes
        if not page:
            break
        next_cursor = listed.get("next_after_id")
        if type(next_cursor) is not int:
            raise RuntimeError("cross_task_list_lanes next_after_id missing while has_more")
        after_id = next_cursor
    else:
        raise RuntimeError(str(last_listed.get("list_truncation_reason") or "lane_list_truncated"))

    raise RuntimeError(str(last_listed.get("list_truncation_reason") or "lane_list_truncated"))


def _resolved_path(path: object, *, relative_to: Path | None = None) -> str | None:
    if not isinstance(path, (str, os.PathLike)) or not str(path).strip():
        return None
    candidate = Path(path).expanduser()
    if not candidate.is_absolute() and relative_to is not None:
        candidate = relative_to / candidate
    try:
        return str(candidate.resolve())
    except (OSError, RuntimeError, ValueError):
        return None


def _list_linked_worktrees(
    repo_root: Path,
    *,
    timeout_s: float = _GIT_STATUS_TIMEOUT_S,
) -> dict[str, dict[str, Any]]:
    """Return resolved paths from Git's worktree registry, including primary."""
    try:
        proc = subprocess.run(
            ["git", "-C", str(repo_root), "worktree", "list", "--porcelain"],
            capture_output=True,
            text=True,
            check=False,
            timeout=min(_GIT_STATUS_TIMEOUT_S, timeout_s),
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise RuntimeError(f"worktree_list_failed:{type(exc).__name__}:{exc}") from exc
    if proc.returncode != 0:
        raise RuntimeError(f"worktree_list_failed:{(proc.stderr or proc.stdout).strip()}")

    worktrees: dict[str, dict[str, Any]] = {}
    for block in proc.stdout.split("\n\n"):
        path: str | None = None
        branch: str | None = None
        bare = False
        for line in block.splitlines():
            if line.startswith("worktree "):
                path = line[len("worktree ") :]
            elif line.startswith("branch "):
                branch = line[len("branch ") :]
            elif line == "bare":
                bare = True
        resolved = _resolved_path(path)
        if resolved is not None:
            worktrees[resolved] = {"path": path, "branch": branch, "bare": bare}
    if not worktrees:
        raise RuntimeError("worktree_list_empty")
    return worktrees


def _git_status(worktree_path: str, *, timeout_s: float = _GIT_STATUS_TIMEOUT_S) -> str | None:
    try:
        proc = subprocess.run(
            ["git", "-C", worktree_path, "status", "--porcelain"],
            capture_output=True,
            text=True,
            check=False,
            timeout=max(0.01, min(_GIT_STATUS_TIMEOUT_S, timeout_s)),
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if proc.returncode != 0:
        return None
    return proc.stdout


def _process_snapshot(*, timeout_s: float = _PROCESS_PROBE_TIMEOUT_S) -> tuple[list[str], list[str]] | None:
    """Capture cwd and command-line evidence once for the whole sweep."""
    lsof: subprocess.CompletedProcess[str] | None = None
    ps: subprocess.CompletedProcess[str] | None = None
    per_probe_timeout = max(0.01, min(_PROCESS_PROBE_TIMEOUT_S, timeout_s / 2))
    try:
        lsof = subprocess.run(
            ["lsof", "-a", "-d", "cwd", "-Fn"],
            capture_output=True,
            text=True,
            check=False,
            timeout=per_probe_timeout,
        )
    except (OSError, subprocess.TimeoutExpired):
        pass
    try:
        ps = subprocess.run(
            ["ps", "-axo", "pid=,command="],
            capture_output=True,
            text=True,
            check=False,
            timeout=per_probe_timeout,
        )
    except (OSError, subprocess.TimeoutExpired):
        pass
    if lsof is None or ps is None:
        return None
    if lsof.returncode != 0 or ps.returncode != 0:
        return None
    cwd_paths = [line[1:].removesuffix(" (deleted)") for line in lsof.stdout.splitlines() if line.startswith("n")]
    command_lines = [line.strip() for line in ps.stdout.splitlines() if line.strip()]
    return cwd_paths, command_lines


def _path_is_within(path: str, parent: str) -> bool:
    try:
        Path(path).expanduser().resolve().relative_to(Path(parent).expanduser().resolve())
        return True
    except (OSError, RuntimeError, ValueError):
        return False


def _has_live_process(worktree_path: str, snapshot: tuple[list[str], list[str]]) -> bool:
    cwd_paths, command_lines = snapshot
    if any(_path_is_within(path, worktree_path) for path in cwd_paths):
        return True
    return any(worktree_path in command for command in command_lines)


def _parse_timestamp(value: object) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    raw = value.strip()
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        try:
            parsed = datetime.strptime(raw, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _retire_lane(
    *, lane_id: str | None, task_ref: str | None, apply: bool, delete_merged_branch: bool
) -> dict[str, Any]:
    from workbay_orchestrator_mcp.lanes import _retire_worktree_lane  # noqa: PLC0415

    return _retire_worktree_lane(
        lane_id=lane_id,
        task_ref=task_ref,
        apply=apply,
        delete_merged_branch=delete_merged_branch,
    )


def _read_task_manifest(repo_root: Path, task_ref: str) -> dict[str, Any] | None:
    if not task_ref or Path(task_ref).name != task_ref:
        return None
    try:
        value = json.loads((repo_root / "config/lane-orchestration" / f"{task_ref}.json").read_text())
    except (OSError, ValueError):
        return None
    return value if isinstance(value, dict) else None


def _integration_refs_cached(
    repo_root: Path,
    task_ref: str,
    cache: dict[str, tuple[list[str], str | None]],
) -> tuple[list[str], str | None]:
    if task_ref not in cache:
        from workbay_orchestrator_mcp.orchestration.offload_pass import (  # noqa: PLC0415
            _resolve_close_integration_refs,
        )

        cache[task_ref] = _resolve_close_integration_refs(
            orchestrator_root=repo_root,
            task_ref=task_ref,
            integration_refs=None,
        )
    return cache[task_ref]


def _branch_inventory(repo_root: Path, *, timeout_s: float = _GIT_STATUS_TIMEOUT_S) -> set[str]:
    """Read the local branch namespace once for a sweep."""
    try:
        proc = subprocess.run(
            ["git", "-C", str(repo_root), "for-each-ref", "--format=%(refname:short)", "refs/heads"],
            capture_output=True,
            text=True,
            check=False,
            timeout=max(0.01, min(_GIT_STATUS_TIMEOUT_S, timeout_s)),
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise RuntimeError(f"branch_inventory_failed:{type(exc).__name__}:{exc}") from exc
    if proc.returncode != 0:
        raise RuntimeError(f"branch_inventory_failed:{(proc.stderr or proc.stdout).strip()}")
    return {line.strip().removeprefix("refs/heads/") for line in proc.stdout.splitlines() if line.strip()}


def _worktree_path_exists(path: str) -> bool | None:
    try:
        Path(path).lstat()
        return True
    except FileNotFoundError:
        return False
    except OSError:
        return None


def _read_process_identity(pid: int) -> tuple[str, dict[str, Any] | None]:
    """Read a process start token, cwd, process group, and session from the OS."""
    if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0:
        return "absent", None

    if sys.platform.startswith("linux"):
        proc_dir = Path("/proc") / str(pid)
        try:
            stat_text = (proc_dir / "stat").read_text(encoding="utf-8")
            # comm is parenthesized and may itself contain spaces or parentheses.
            close_paren = stat_text.rfind(")")
            if close_paren < 0:
                return "unknown", None
            fields = stat_text[close_paren + 2 :].split()
            if len(fields) <= 19:
                return "unknown", None
            if fields[0] in {"Z", "X"}:
                return "absent", None
            pgid = int(fields[2])
            sid = int(fields[3])
            start_ticks = fields[19]
            boot_id = Path("/proc/sys/kernel/random/boot_id").read_text(encoding="ascii").strip()
            cwd = os.readlink(proc_dir / "cwd")
            resolved_cwd = _resolved_path(cwd)
            if not boot_id or not start_ticks or resolved_cwd is None:
                return "unknown", None
            verify_text = (proc_dir / "stat").read_text(encoding="utf-8")
            verify_close_paren = verify_text.rfind(")")
            verify_fields = verify_text[verify_close_paren + 2 :].split() if verify_close_paren >= 0 else []
            if (
                verify_text.split(" ", 1)[0] != str(pid)
                or len(verify_fields) <= 19
                or verify_fields[0] in {"Z", "X"}
                or verify_fields[2] != str(pgid)
                or verify_fields[3] != str(sid)
                or verify_fields[19] != start_ticks
            ):
                return "unknown", None
            return "available", {
                "pid": pid,
                "start_token": f"linux:{boot_id}:{start_ticks}",
                "repo_root": resolved_cwd,
                "pgid": pgid,
                "sid": sid,
            }
        except FileNotFoundError:
            return "absent", None
        except (OSError, ValueError, IndexError):
            return "unknown", None

    getpgid = getattr(os, "getpgid", None)
    getsid = getattr(os, "getsid", None)
    if not callable(getpgid) or not callable(getsid):
        return "unknown", None

    def read_group_session() -> tuple[str, tuple[int, int] | None]:
        try:
            pgid = getpgid(pid)
            sid = getsid(pid)
        except ProcessLookupError:
            return "absent", None
        except (OSError, NotImplementedError):
            return "unknown", None
        if any(isinstance(value, bool) or not isinstance(value, int) or value <= 0 for value in (pgid, sid)):
            return "unknown", None
        return "available", (pgid, sid)

    def read_start_token() -> tuple[str, str | None]:
        try:
            proc = subprocess.run(
                ["ps", "-p", str(pid), "-o", "lstart="],
                capture_output=True,
                text=True,
                check=False,
                timeout=1.0,
            )
        except (OSError, subprocess.TimeoutExpired):
            return "unknown", None
        lines = [line.strip() for line in proc.stdout.splitlines() if line.strip()]
        if proc.returncode != 0 or len(lines) != 1:
            return "unknown", None
        return "available", "ps:" + " ".join(lines[0].split())

    group_state, group_session = read_group_session()
    if group_state != "available" or group_session is None:
        return group_state, None
    start_state, start_token = read_start_token()
    if start_state != "available" or start_token is None:
        return start_state, None
    try:
        cwd_proc = subprocess.run(
            ["lsof", "-a", "-p", str(pid), "-d", "cwd", "-Fn"],
            capture_output=True,
            text=True,
            check=False,
            timeout=1.0,
        )
    except (OSError, subprocess.TimeoutExpired):
        return "unknown", None
    cwd_paths = [line[1:] for line in cwd_proc.stdout.splitlines() if line.startswith("n")]
    resolved_cwd = _resolved_path(cwd_paths[0]) if cwd_proc.returncode == 0 and len(cwd_paths) == 1 else None
    if resolved_cwd is None:
        return "unknown", None
    verify_state, verify_start_token = read_start_token()
    if verify_state != "available" or verify_start_token != start_token:
        return "unknown", None
    verify_group_state, verify_group_session = read_group_session()
    if verify_group_state != "available" or verify_group_session != group_session:
        return "unknown", None
    pgid, sid = group_session
    return "available", {
        "pid": pid,
        "start_token": start_token,
        "repo_root": resolved_cwd,
        "pgid": pgid,
        "sid": sid,
    }


def _identity_fields_match(
    actual: object,
    expected: object,
    *,
    repo_root: Path,
    pid: int,
    require_start_token: bool = True,
) -> bool:
    if not isinstance(actual, dict) or not isinstance(expected, dict):
        return False
    root = _resolved_path(repo_root)
    if root is None:
        return False
    required = ("pid", "repo_root", "pgid", "sid")
    if require_start_token:
        required = (*required, "start_token")
    if any(key not in actual or key not in expected for key in required):
        return False
    try:
        actual_pid = int(actual["pid"])
        expected_pid = int(expected["pid"])
        actual_pgid = int(actual["pgid"])
        expected_pgid = int(expected["pgid"])
        actual_sid = int(actual["sid"])
        expected_sid = int(expected["sid"])
    except (TypeError, ValueError):
        return False
    return (
        actual_pid == expected_pid == pid
        and actual_pgid == expected_pgid == pid
        and actual_sid == expected_sid == pid
        and (
            not require_start_token
            or (
                isinstance(actual["start_token"], str)
                and bool(actual["start_token"])
                and actual["start_token"] == expected["start_token"]
            )
        )
        and actual["repo_root"] == expected["repo_root"] == root
    )


def _open_pidfd(pid: int) -> int:
    """Pin a Linux PID across the final identity check and group signal."""
    pidfd_open = getattr(os, "pidfd_open", None)
    if callable(pidfd_open):
        return pidfd_open(pid, 0)
    machine = os.uname().machine.lower()
    if machine not in {"x86_64", "amd64", "aarch64", "arm64", "riscv64", "ppc64le", "s390x"}:
        raise OSError("pidfd_open_unavailable")
    libc = ctypes.CDLL(None, use_errno=True)
    syscall = libc.syscall
    syscall.restype = ctypes.c_long
    descriptor = syscall(ctypes.c_long(434), ctypes.c_int(pid), ctypes.c_uint(0))
    if descriptor < 0:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error))
    return int(descriptor)


def _signal_owned_group(identity: dict[str, Any], repo_root: Path, sig: int) -> str:
    """Signal a group only after a fresh full identity and session check."""
    pid = identity.get("pid")
    if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0:
        return "unknown"
    try:
        pidfd: int | None = None
        if sys.platform.startswith("linux"):
            pidfd = _open_pidfd(pid)
        try:
            state, current = _read_process_identity(pid)
            if state == "absent":
                return "absent"
            if state != "available" or not _identity_fields_match(current, identity, repo_root=repo_root, pid=pid):
                return "unknown"
            os.killpg(pid, sig)
            return "signaled"
        finally:
            if pidfd is not None:
                os.close(pidfd)
    except ProcessLookupError:
        return "absent"
    except OSError:
        return "unknown"


def _retire_child_operation(repo_root: str, lane_id: str, task_ref: str, apply: bool) -> dict[str, Any]:
    """Rebuild child-local runtime state before touching the handoff database."""
    _configure_runtime(Path(repo_root))
    return _retire_lane(
        lane_id=lane_id,
        task_ref=task_ref,
        apply=apply,
        delete_merged_branch=True,
    )


def _isolated_child_main(
    connection: Any,
    nonce: str,
    repo_root: str,
    operation: Any,
    operation_args: tuple[Any, ...],
    operation_kwargs: dict[str, Any],
) -> None:
    """Wait for the parent to verify this child's isolated session before work."""
    root = Path(repo_root).resolve()
    try:
        os.setsid()
        os.chdir(root)
        state, identity = _read_process_identity(os.getpid())
        if (
            state != "available"
            or identity is None
            or not _identity_fields_match(identity, identity, repo_root=root, pid=os.getpid())
        ):
            connection.send({"kind": "abort", "outcome": "retire_session_unverified"})
            return
        connection.send({"kind": "ready", "nonce": nonce, "identity": identity})
        if not connection.poll(_CHILD_HANDSHAKE_TIMEOUT_S):
            return
        acknowledgement = connection.recv()
        if (
            not isinstance(acknowledgement, dict)
            or acknowledgement.get("kind") != "go"
            or acknowledgement.get("nonce") != nonce
        ):
            return
    except BaseException as exc:  # noqa: BLE001 — failed isolation must precede all destructive work
        try:
            connection.send(
                {"kind": "abort", "outcome": "retire_session_failed", "error": f"{type(exc).__name__}: {exc}"}
            )
        except (BrokenPipeError, EOFError, OSError):
            pass
        return

    try:
        result = operation(*operation_args, **operation_kwargs)
        message = {"kind": "result", "result": result}
    except BaseException as exc:  # noqa: BLE001 — parent must retain a bounded result
        message = {
            "kind": "result",
            "result": {"ok": False, "outcome": "retire_raised", "error": f"{type(exc).__name__}: {exc}"},
        }
    try:
        connection.send(message)
    except (BrokenPipeError, EOFError, OSError):
        pass
    finally:
        connection.close()


def _safe_child_context() -> Any:
    methods = multiprocessing.get_all_start_methods()
    for method in ("spawn", "forkserver"):
        if method in methods:
            return multiprocessing.get_context(method)
    raise RuntimeError("no_safe_multiprocessing_context")


def _run_isolated_operation(
    *,
    repo_root: Path,
    operation: Any,
    operation_args: tuple[Any, ...],
    operation_kwargs: dict[str, Any] | None = None,
    timeout_s: float,
) -> dict[str, Any]:
    deadline = _monotonic() + timeout_s
    nonce = uuid.uuid4().hex
    try:
        context = _safe_child_context()
        parent_connection, child_connection = context.Pipe(duplex=True)
        process = context.Process(
            target=_isolated_child_main,
            args=(child_connection, nonce, str(repo_root.resolve()), operation, operation_args, operation_kwargs or {}),
            daemon=False,
        )
        process.start()
        child_connection.close()
    except Exception as exc:  # noqa: BLE001 — fail closed if a bounded child cannot start
        return {"ok": False, "outcome": "retire_spawn_failed", "error": f"{type(exc).__name__}: {exc}"}

    startup_wait = max(0.0, min(_CHILD_HANDSHAKE_TIMEOUT_S, deadline - _monotonic()))
    if not parent_connection.poll(startup_wait):
        parent_connection.close()
        process.join(_RETIRE_TIMEOUT_CLEANUP_S)
        outcome = "retire_session_handshake_timeout"
        if process.is_alive():
            outcome = "retire_session_cleanup_unknown"
        return {"ok": False, "outcome": outcome}
    try:
        handshake = parent_connection.recv()
    except (EOFError, OSError):
        handshake = None
    if not isinstance(handshake, dict) or handshake.get("kind") != "ready" or handshake.get("nonce") != nonce:
        process.join(_RETIRE_TIMEOUT_CLEANUP_S)
        parent_connection.close()
        error = handshake.get("error") if isinstance(handshake, dict) else None
        return {
            "ok": False,
            "outcome": str(handshake.get("outcome") if isinstance(handshake, dict) else "retire_session_unverified"),
            **({"error": error} if error else {}),
        }

    child_identity = handshake.get("identity")
    state, current_identity = _read_process_identity(process.pid)
    if (
        state != "available"
        or not _identity_fields_match(child_identity, child_identity, repo_root=repo_root, pid=process.pid)
        or not _identity_fields_match(current_identity, child_identity, repo_root=repo_root, pid=process.pid)
    ):
        parent_connection.close()
        process.join(_RETIRE_TIMEOUT_CLEANUP_S)
        return {"ok": False, "outcome": "retire_session_unverified"}
    try:
        parent_connection.send({"kind": "go", "nonce": nonce})
    except (BrokenPipeError, EOFError, OSError):
        parent_connection.close()
        process.join(_RETIRE_TIMEOUT_CLEANUP_S)
        return {"ok": False, "outcome": "retire_session_handshake_failed"}

    process.join(max(0.0, deadline - _monotonic()))
    if process.is_alive():
        signal_result = _signal_owned_group(child_identity, repo_root, signal.SIGKILL)
        process.join(_RETIRE_TIMEOUT_CLEANUP_S)
        parent_connection.close()
        if process.is_alive() or signal_result == "unknown":
            return {"ok": False, "outcome": "retire_timeout_cleanup_unknown"}
        return {"ok": False, "outcome": "retire_timeout"}

    message: Any = None
    if parent_connection.poll(0.05):
        try:
            message = parent_connection.recv()
        except (EOFError, OSError):
            message = None
    parent_connection.close()
    if isinstance(message, dict) and message.get("kind") == "result" and isinstance(message.get("result"), dict):
        return message["result"]
    return {"ok": False, "outcome": "retire_malformed", "error": f"child_exit:{process.exitcode}"}


def _retire_lane_bounded(
    *, lane_id: str, task_ref: str, apply: bool, remaining_s: float, repo_root: Path
) -> dict[str, Any]:
    timeout_s = min(_RETIRE_OPERATION_TIMEOUT_S, remaining_s - _RETIRE_TIMEOUT_CLEANUP_S)
    if timeout_s <= 0:
        return {"ok": False, "outcome": "budget_exhausted"}
    return _run_isolated_operation(
        repo_root=repo_root,
        operation=_retire_child_operation,
        operation_args=(str(repo_root.resolve()), lane_id, task_ref, apply),
        timeout_s=timeout_s,
    )


def _absorbed_landing(
    repo_root: Path,
    row: dict[str, Any],
    rows: list[dict[str, Any]],
    *,
    deadline: float,
    manifest_cache: dict[str, dict[str, Any] | None] | None = None,
    integration_refs_cache: dict[str, tuple[list[str], str | None]] | None = None,
) -> tuple[str | None, str | None]:
    """Prove succession and actual Git branch containment; unknowns fail closed."""
    task = str(row.get("task_ref") or "")
    lane = str(row.get("lane_id") or "")
    if not task or Path(task).name != task:
        return None, "manifest_missing"
    if not (repo_root / "config/lane-orchestration" / f"{task}.json").exists():
        return None, "manifest_missing"
    manifests = manifest_cache if manifest_cache is not None else {}
    if task not in manifests:
        manifests[task] = _read_task_manifest(repo_root, task)
    manifest = manifests[task]
    if not isinstance(manifest, dict) or not isinstance(manifest.get("depends_on", {}), dict):
        return None, "manifest_invalid"
    dependencies = manifest.get("depends_on", {})
    successors = {
        successor
        for successor, predecessors in dependencies.items()
        if successor != lane and isinstance(predecessors, list) and lane in predecessors
    }
    if not successors:
        return None, "no_successor"
    if not any(
        other.get("task_ref") == task
        and other.get("lane_id") in successors
        and str(other.get("status") or "").strip().lower() in {"merged", "closed"}
        for other in rows
    ):
        return None, "successor_not_terminal"

    def git(*args: str) -> subprocess.CompletedProcess[str]:
        remaining = deadline - _monotonic()
        if remaining <= 0:
            raise TimeoutError("budget exhausted")
        return subprocess.run(
            ["git", "-C", str(repo_root), *args],
            capture_output=True,
            text=True,
            check=False,
            timeout=min(_ACTIVE_MERGED_PROBE_TIMEOUT_S, remaining),
        )

    branch = str(row.get("branch") or "").strip().removeprefix("refs/heads/")
    try:
        if not branch or git("show-ref", "--verify", "--quiet", f"refs/heads/{branch}").returncode != 0:
            return None, "branch_missing"
        refs, _source = _integration_refs_cached(
            repo_root,
            task,
            integration_refs_cache if integration_refs_cache is not None else {},
        )
        if not refs:
            return None, "integration_ref_missing"
        for ref in refs:
            tip = git("rev-parse", "--verify", f"{ref}^{{commit}}")
            if tip.returncode != 0:
                return None, "integration_ref_missing"
            landing = tip.stdout.strip()
            merged = git("branch", "--merged", landing, "--format=%(refname)")
            if merged.returncode != 0:
                return None, "merge_probe_failed"
            if f"refs/heads/{branch}" in merged.stdout.splitlines():
                return landing, None
        return None, "not_merged"
    except TimeoutError:
        return None, "budget_exhausted"
    except Exception:  # noqa: BLE001 — unknown evidence cannot authorize close
        return None, "merge_probe_failed"


def _close_absorbed_lane(row: dict[str, Any], landing: str) -> dict[str, Any]:
    from workbay_orchestrator_mcp.lanes import manage_worktree_lane

    return manage_worktree_lane(
        operation="close",
        task_ref=str(row["task_ref"]),
        lane_id=str(row["lane_id"]),
        landing_commit_sha=landing,
        notes="Superseded by a terminal dependent lane; branch absorbed into integration.",
    )


def _count_active_but_merged(
    repo_root: Path,
    rows: list[dict[str, Any]],
    *,
    deadline: float,
    integration_refs_cache: dict[str, tuple[list[str], str | None]] | None = None,
    branch_inventory: set[str] | None = None,
) -> tuple[int, list[str], bool]:
    """Count non-terminal lane branches already merged into their task target."""
    from workbay_orchestrator_mcp.orchestration.lane_postmerge import (  # noqa: PLC0415
        TERMINAL_STATUSES,
    )

    grouped: dict[str, set[str]] = {}
    pending: list[tuple[str, str, list[str]]] = []
    errors: list[str] = []
    complete = True
    refs_by_task: dict[str, list[str]] = {}
    shared_refs = integration_refs_cache if integration_refs_cache is not None else {}
    for row in rows:
        if str(row.get("status") or "").strip().lower() in TERMINAL_STATUSES:
            continue
        branch = str(row.get("branch") or "").strip()
        task = str(row.get("task_ref") or "").strip()
        if not branch or not task:
            continue
        normalized_branch = branch.removeprefix("refs/heads/")
        if branch_inventory is not None and normalized_branch not in branch_inventory:
            continue
        if _monotonic() >= deadline:
            complete = False
            break
        try:
            refs = refs_by_task.get(task)
            if refs is None:
                refs, _source = _integration_refs_cached(repo_root, task, shared_refs)
                refs_by_task[task] = refs
            if not refs:
                errors.append(f"active_merged_ref_missing:{task}")
                continue
            pending.append((task, normalized_branch, refs))
            for ref in refs:
                grouped.setdefault(ref, set()).add(normalized_branch)
        except Exception as exc:  # noqa: BLE001 — invalid manifests are visible
            errors.append(f"active_merged_ref_failed:{task}:{type(exc).__name__}:{exc}")

    merged_by_ref: dict[str, set[str]] = {}
    for ref in sorted(grouped):
        remaining = deadline - _monotonic()
        if remaining <= 0:
            complete = False
            break
        try:
            proc = subprocess.run(
                ["git", "-C", str(repo_root), "branch", "--merged", ref, "--format=%(refname:short)"],
                capture_output=True,
                text=True,
                check=False,
                timeout=min(_ACTIVE_MERGED_PROBE_TIMEOUT_S, remaining),
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            errors.append(f"active_merged_probe_failed:{ref}:{type(exc).__name__}:{exc}")
            continue
        if proc.returncode != 0:
            errors.append(f"active_merged_probe_failed:{ref}:{(proc.stderr or proc.stdout).strip()}")
            continue
        merged_by_ref[ref] = {line.strip() for line in proc.stdout.splitlines() if line.strip()}

    count = sum(1 for _task, branch, refs in pending if any(branch in merged_by_ref.get(ref, set()) for ref in refs))
    return count, errors, complete


def _load_retry_state(
    repo_root: Path, *, now_epoch: float
) -> tuple[tuple[str, str] | None, dict[tuple[str, str], dict[str, Any]]]:
    path = repo_root / _RETRY_STATE_RELATIVE
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None, {}
    cursor_value = payload.get("cursor") if isinstance(payload, dict) else None
    cursor: tuple[str, str] | None = None
    if isinstance(cursor_value, dict):
        task = cursor_value.get("task_ref")
        lane = cursor_value.get("lane_id")
        if isinstance(task, str) and isinstance(lane, str):
            cursor = (task, lane)
    entries: dict[tuple[str, str], dict[str, Any]] = {}
    raw_entries = payload.get("refusals", []) if isinstance(payload, dict) else []
    if isinstance(raw_entries, list):
        for item in raw_entries:
            if not isinstance(item, dict):
                continue
            task, lane = item.get("task_ref"), item.get("lane_id")
            try:
                count = max(0.0, float(item.get("count", 0)))
                attempted_at = float(item.get("attempted_at", 0))
                retry_after = float(item.get("retry_after", 0))
            except (TypeError, ValueError):
                continue
            if not isinstance(task, str) or not isinstance(lane, str) or now_epoch - attempted_at > _STATE_TTL_S:
                continue
            entry: dict[str, Any] = {
                "count": count,
                "attempted_at": attempted_at,
                "retry_after": retry_after,
            }
            candidate_state = item.get("candidate_state")
            branch_exists = candidate_state.get("branch_exists") if isinstance(candidate_state, dict) else None
            if (
                isinstance(candidate_state, dict)
                and isinstance(candidate_state.get("worktree_exists"), bool)
                and (branch_exists is None or isinstance(branch_exists, bool))
                and isinstance(candidate_state.get("status"), str)
            ):
                entry["candidate_state"] = {
                    "worktree_exists": candidate_state["worktree_exists"],
                    "branch_exists": branch_exists,
                    "status": candidate_state["status"],
                }
            entries[(task, lane)] = entry
    if len(entries) > _STATE_MAX_ENTRIES:
        newest = sorted(entries.items(), key=lambda item: item[1]["attempted_at"], reverse=True)
        entries = dict(newest[:_STATE_MAX_ENTRIES])
    return cursor, entries


def _save_retry_state(
    repo_root: Path,
    *,
    cursor: tuple[str, str] | None,
    entries: dict[tuple[str, str], dict[str, Any]],
    now_epoch: float,
) -> None:
    live = [
        (key, value) for key, value in entries.items() if now_epoch - value.get("attempted_at", 0.0) <= _STATE_TTL_S
    ]
    newest = sorted(live, key=lambda item: item[1].get("attempted_at", 0.0), reverse=True)[:_STATE_MAX_ENTRIES]
    refusals = []
    for key, value in newest:
        refusal: dict[str, Any] = {
            "task_ref": key[0],
            "lane_id": key[1],
            "count": int(value.get("count", 0)),
            "attempted_at": value.get("attempted_at", 0.0),
            "retry_after": value.get("retry_after", 0.0),
        }
        candidate_state = value.get("candidate_state")
        if isinstance(candidate_state, dict):
            refusal["candidate_state"] = candidate_state
        refusals.append(refusal)
    payload = {
        "version": 1,
        "updated_at": now_epoch,
        "cursor": ({"task_ref": cursor[0], "lane_id": cursor[1]} if cursor else None),
        "refusals": refusals,
    }
    _atomic_json_replace(repo_root / _RETRY_STATE_RELATIVE, payload)


def _rotate_sweep_log(root: Path) -> None:
    path = root / _LOG_RELATIVE
    if path.exists() and path.stat().st_size >= _SWEEP_LOG_LIMIT_BYTES:
        backup = path.with_name(f"{path.name}.1")
        backup.unlink(missing_ok=True)
        os.replace(path, backup)


def _load_child_marker(root: Path) -> tuple[str, dict[str, Any] | None]:
    marker = root / _CHILD_MARKER_RELATIVE
    try:
        raw = marker.read_text(encoding="utf-8")
    except FileNotFoundError:
        return "missing", None
    except OSError:
        return "unknown", None
    try:
        payload = json.loads(raw)
    except ValueError:
        return "unknown", None
    if not isinstance(payload, dict):
        return "unknown", None
    return "present", payload


def _sweep_child_state(root: Path) -> tuple[str, dict[str, Any] | None]:
    """Classify a marker only from durable PID, root, session, and start identity."""
    marker_state, payload = _load_child_marker(root)
    if marker_state != "present" or payload is None:
        return marker_state, payload
    pid = payload.get("pid")
    started_value = payload.get("started_at")
    if (
        isinstance(pid, bool)
        or not isinstance(pid, int)
        or pid <= 0
        or isinstance(started_value, bool)
        or not isinstance(started_value, (int, float))
    ):
        return "unknown", payload

    process_state, identity = _read_process_identity(pid)
    if process_state == "absent":
        return "stale", payload
    if process_state != "available" or identity is None:
        return "unknown", payload
    if not _identity_fields_match(identity, payload, repo_root=root, pid=pid, require_start_token=False):
        return "stale", payload
    start_token = payload.get("start_token")
    if not isinstance(start_token, str) or not start_token.strip():
        return "unknown", payload
    if not _identity_fields_match(identity, payload, repo_root=root, pid=pid):
        return "stale", payload
    if time.time() - float(started_value) > _SWEEP_CHILD_LIFETIME_S:
        return "expired", payload
    return "running", payload


def _clear_child_marker(
    root: Path,
    *,
    expected: dict[str, Any] | None = None,
    identity: dict[str, Any] | None = None,
    launch_lock: TextIO | None = None,
) -> bool:
    """Compare-and-remove a marker while holding the launch lock."""
    if expected is None and identity is None:
        return False
    owns_lock = launch_lock is None
    lock = launch_lock
    if lock is None:
        lock, _error = _try_acquire_file_lock(root, _LAUNCH_LOCK_RELATIVE)
        if lock is None:
            return False
    try:
        state, current = _load_child_marker(root)
        if state != "present" or current is None:
            return False
        if expected is not None and current != expected:
            return False
        if identity is not None:
            pid = identity.get("pid")
            if isinstance(pid, bool) or not isinstance(pid, int):
                return False
            if not _identity_fields_match(identity, current, repo_root=root, pid=pid):
                return False
        marker = root / _CHILD_MARKER_RELATIVE
        marker.unlink(missing_ok=True)
        return True
    finally:
        if owns_lock and lock is not None:
            _release_lock(lock)


def _expire_sweep_child(root: Path, expected: dict[str, Any]) -> str:
    """Recheck both marker and process immediately before an expiry signal."""
    marker_state, current = _load_child_marker(root)
    if marker_state != "present" or current != expected:
        return "changed"
    result = _signal_owned_group(expected, root, signal.SIGKILL)
    if result == "absent":
        _clear_child_marker(root, expected=expected)
    return result


def _write_receipt(repo_root: Path, summary: dict[str, Any]) -> None:
    path = repo_root / _RECEIPT_RELATIVE
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        if path.exists() and path.stat().st_size >= _RECEIPT_LIMIT_BYTES:
            backup = path.with_name(f"{path.name}.1")
            backup.unlink(missing_ok=True)
            os.replace(path, backup)
    except OSError:
        raise
    line = json.dumps(summary, sort_keys=True, separators=(",", ":")) + "\n"
    with path.open("a", encoding="utf-8") as handle:
        handle.write(line)


def _add_unactionable(
    *,
    lane_id: object,
    task_ref: object,
    status: object,
    reason: str,
    could_not_act: list[dict[str, Any]],
    skip_counts: Counter[str],
) -> None:
    lane = str(lane_id or "")
    task = str(task_ref or "")
    row = {"lane_id": lane, "task_ref": task, "status": str(status or ""), "reason": reason}
    if len(could_not_act) < _DETAIL_CAP:
        could_not_act.append(row)
    skip_counts[reason] += 1


def _budget_exhausted_summary(
    *,
    apply: bool,
    task_ref: str | None,
    phase: str,
    started: float,
    phase_timings: dict[str, float],
    lane_progress: dict[str, Any],
) -> dict[str, Any]:
    """Return a typed budget stop with lane-discovery progress attached."""
    rows_fetched = int(lane_progress.get("rows_fetched") or 0)
    pages_fetched = int(lane_progress.get("pages_fetched") or 0)
    total_matching = lane_progress.get("total_matching")
    if type(total_matching) is not int:
        total_matching = None
    return {
        "ok": True,
        "outcome": "budget_exhausted",
        "apply": bool(apply),
        "task_ref": task_ref,
        "candidates": 0,
        "attempted": 0,
        "outcome_counts": {},
        "skip_counts": {},
        "retired_lane_ids": [],
        "could_not_act": [],
        "could_not_act_truncated": False,
        "missing_worktree_and_branch": 0,
        "lane_timings": [],
        "phase_timings_s": {**phase_timings, "total_s": max(0.0, _monotonic() - started)},
        "absorbed_retired": 0,
        "absorbed_skipped": 0,
        "active_but_merged": 0,
        "active_but_merged_complete": False,
        "has_more": True,
        "remaining": total_matching,
        "instrument_errors": [],
        "budget_progress": {
            "phase": phase,
            "lane_rows_fetched": rows_fetched,
            "lane_rows_pages_fetched": pages_fetched,
            "lane_rows_total": total_matching,
        },
    }


def _sweep_locked(
    *,
    repo_root: Path,
    apply: bool,
    max_lanes: int,
    deadline: float,
    grace_s: float,
    task_ref: str | None,
    started: float,
) -> dict[str, Any]:
    from workbay_orchestrator_mcp.orchestration.lane_postmerge import (  # noqa: PLC0415
        TERMINAL_STATUSES,
    )
    from workbay_orchestrator_mcp.orchestration.lane_reclaim import (  # noqa: PLC0415
        _session_live,
    )
    from workbay_orchestrator_mcp.orchestration.offload_pass import (  # noqa: PLC0415
        _worker_driver_lock_is_live,
    )

    instrument_errors: list[str] = []
    phase_timings: dict[str, float] = {}
    manifest_cache: dict[str, dict[str, Any] | None] = {}
    integration_refs_cache: dict[str, tuple[list[str], str | None]] = {}

    def add_error(value: str) -> None:
        if len(instrument_errors) < _DETAIL_CAP:
            instrument_errors.append(value)

    budget_deadline = deadline
    phase_started = _monotonic()
    lane_progress: dict[str, Any] = {"pages_fetched": 0, "rows_fetched": 0, "total_matching": None}
    if _monotonic() >= budget_deadline:
        phase_timings["lane_rows_s"] = max(0.0, _monotonic() - phase_started)
        return _budget_exhausted_summary(
            apply=apply,
            task_ref=task_ref,
            phase="lane_pagination",
            started=started,
            phase_timings=phase_timings,
            lane_progress=lane_progress,
        )
    try:
        rows = _load_lane_rows(repo_root, task_ref, deadline=budget_deadline, progress=lane_progress)
    except _BudgetExhausted as exc:
        lane_progress.update(
            {
                "pages_fetched": exc.pages_fetched,
                "rows_fetched": exc.rows_fetched,
                "total_matching": exc.total_matching,
            }
        )
        phase_timings["lane_rows_s"] = max(0.0, _monotonic() - phase_started)
        return _budget_exhausted_summary(
            apply=apply,
            task_ref=task_ref,
            phase="lane_pagination",
            started=started,
            phase_timings=phase_timings,
            lane_progress=lane_progress,
        )
    except Exception as exc:  # noqa: BLE001 — an unknown row universe cannot authorize retirement
        rows = []
        if _monotonic() >= budget_deadline:
            phase_timings["lane_rows_s"] = max(0.0, _monotonic() - phase_started)
            return _budget_exhausted_summary(
                apply=apply,
                task_ref=task_ref,
                phase="lane_pagination",
                started=started,
                phase_timings=phase_timings,
                lane_progress=lane_progress,
            )
        add_error(f"lane_list_failed:{type(exc).__name__}:{exc}")
    if not lane_progress.get("pages_fetched") and len(rows) > 0:
        lane_progress["rows_fetched"] = len(rows)
    phase_timings["lane_rows_s"] = max(0.0, _monotonic() - phase_started)
    if _monotonic() >= budget_deadline:
        return _budget_exhausted_summary(
            apply=apply,
            task_ref=task_ref,
            phase="lane_pagination",
            started=started,
            phase_timings=phase_timings,
            lane_progress=lane_progress,
        )

    phase_started = _monotonic()
    try:
        remaining = budget_deadline - _monotonic()
        if remaining <= 0:
            phase_timings["worktree_inventory_s"] = max(0.0, _monotonic() - phase_started)
            return _budget_exhausted_summary(
                apply=apply,
                task_ref=task_ref,
                phase="worktree_inventory",
                started=started,
                phase_timings=phase_timings,
                lane_progress=lane_progress,
            )
        linked = _list_linked_worktrees(repo_root, timeout_s=min(_GIT_STATUS_TIMEOUT_S, remaining))
    except Exception as exc:  # noqa: BLE001 — unknown Git registry fails closed
        linked = {}
        if _monotonic() < budget_deadline:
            add_error(f"worktree_list_failed:{type(exc).__name__}:{exc}")
    phase_timings["worktree_inventory_s"] = max(0.0, _monotonic() - phase_started)
    if _monotonic() >= budget_deadline:
        return _budget_exhausted_summary(
            apply=apply,
            task_ref=task_ref,
            phase="worktree_inventory",
            started=started,
            phase_timings=phase_timings,
            lane_progress=lane_progress,
        )

    branches: set[str] | None = None
    phase_started = _monotonic()
    remaining = budget_deadline - _monotonic()
    if remaining > 0:
        try:
            branches = _branch_inventory(repo_root, timeout_s=remaining)
        except Exception as exc:  # noqa: BLE001 — branch-only recovery needs positive branch evidence
            add_error(f"branch_inventory_failed:{type(exc).__name__}:{exc}")
    else:
        add_error("budget_exhausted_before_branch_inventory")
    phase_timings["branch_inventory_s"] = max(0.0, _monotonic() - phase_started)

    candidates: list[tuple[dict[str, Any], str, bool, str | None]] = []
    candidate_states: dict[tuple[str, str], dict[str, Any]] = {}
    could_not_act: list[dict[str, Any]] = []
    skip_counts: Counter[str] = Counter()
    root_identity = _resolved_path(repo_root)
    root_branch = str((linked.get(root_identity or "") or {}).get("branch") or "").removeprefix("refs/heads/")
    absorbed: dict[tuple[str, str], str] = {}
    absorbed_retired = 0
    nonterminal = {
        (str(row.get("task_ref")), str(row.get("lane_id")))
        for row in rows
        if str(row.get("status") or "").strip().lower() not in TERMINAL_STATUSES
    }
    missing_worktree_and_branch = 0

    def integration_branch_names(task: str) -> set[str]:
        if task not in manifest_cache:
            manifest_cache[task] = _read_task_manifest(repo_root, task)
        manifest = manifest_cache.get(task)
        names: set[str] = set()
        if isinstance(manifest, dict):
            for key in ("integration_branch", "integration_ref", "target_branch"):
                value = manifest.get(key)
                if isinstance(value, str) and value.strip():
                    names.add(value.strip().removeprefix("refs/heads/"))
        try:
            refs, _source = _integration_refs_cached(repo_root, task, integration_refs_cache)
            names.update(str(ref).strip().removeprefix("refs/heads/") for ref in refs if str(ref).strip())
        except Exception:
            # The primary worktree stays protected even when its task manifest is unreadable.
            pass
        return names

    phase_started = _monotonic()
    for row in rows:
        status = str(row.get("status") or "").strip().lower()
        lane_id = row.get("lane_id")
        row_task = row.get("task_ref")
        if not lane_id or not row_task:
            _add_unactionable(
                lane_id=lane_id,
                task_ref=row_task,
                status=status,
                reason="lane_identity_missing",
                could_not_act=could_not_act,
                skip_counts=skip_counts,
            )
            continue

        recorded_path = row.get("worktree_path")
        resolved = _resolved_path(recorded_path, relative_to=repo_root)
        if resolved is None:
            _add_unactionable(
                lane_id=lane_id,
                task_ref=row_task,
                status=status,
                reason="worktree_path_missing",
                could_not_act=could_not_act,
                skip_counts=skip_counts,
            )
            continue
        if resolved == root_identity:
            _add_unactionable(
                lane_id=lane_id,
                task_ref=row_task,
                status=status,
                reason="primary_worktree",
                could_not_act=could_not_act,
                skip_counts=skip_counts,
            )
            continue

        path_exists = _worktree_path_exists(resolved)
        if path_exists is None:
            _add_unactionable(
                lane_id=lane_id,
                task_ref=row_task,
                status=status,
                reason="worktree_path_unknown",
                could_not_act=could_not_act,
                skip_counts=skip_counts,
            )
            continue

        linked_entry = linked.get(resolved)
        if path_exists:
            if linked_entry is None:
                _add_unactionable(
                    lane_id=lane_id,
                    task_ref=row_task,
                    status=status,
                    reason="not_linked_worktree",
                    could_not_act=could_not_act,
                    skip_counts=skip_counts,
                )
                continue
            recorded_branch = str(row.get("branch") or "").strip().removeprefix("refs/heads/")
            linked_branch = str(linked_entry.get("branch") or "").strip().removeprefix("refs/heads/")
            if not recorded_branch or not linked_branch or recorded_branch != linked_branch:
                _add_unactionable(
                    lane_id=lane_id,
                    task_ref=row_task,
                    status=status,
                    reason="worktree_branch_mismatch",
                    could_not_act=could_not_act,
                    skip_counts=skip_counts,
                )
                continue
            protected_branches = {root_branch} if root_branch else set()
            protected_branches.update(integration_branch_names(str(row_task)))
            if linked_branch in protected_branches:
                _add_unactionable(
                    lane_id=lane_id,
                    task_ref=row_task,
                    status=status,
                    reason="integration_worktree",
                    could_not_act=could_not_act,
                    skip_counts=skip_counts,
                )
                continue
        else:
            if status not in TERMINAL_STATUSES:
                _add_unactionable(
                    lane_id=lane_id,
                    task_ref=row_task,
                    status=status,
                    reason="nonterminal_worktree_missing",
                    could_not_act=could_not_act,
                    skip_counts=skip_counts,
                )
                continue
            branch = str(row.get("branch") or "").strip().removeprefix("refs/heads/")
            if branches is None:
                _add_unactionable(
                    lane_id=lane_id,
                    task_ref=row_task,
                    status=status,
                    reason="branch_inventory_unknown",
                    could_not_act=could_not_act,
                    skip_counts=skip_counts,
                )
                continue
            if not branch or branch not in branches:
                missing_worktree_and_branch += 1
                skip_counts["missing_worktree_and_branch"] += 1
                continue

        landing: str | None = None
        if status not in TERMINAL_STATUSES:
            landing, reason = _absorbed_landing(
                repo_root,
                row,
                rows,
                deadline=budget_deadline,
                manifest_cache=manifest_cache,
                integration_refs_cache=integration_refs_cache,
            )
            if reason:
                _add_unactionable(
                    lane_id=lane_id,
                    task_ref=row_task,
                    status=status,
                    reason=f"absorbed_skip:{reason}",
                    could_not_act=could_not_act,
                    skip_counts=skip_counts,
                )
                continue
            absorbed[(str(row_task), str(lane_id))] = str(landing)
        candidates.append((row, str(recorded_path), path_exists, landing))
        row_key = (str(row_task), str(lane_id))
        branch_name = str(row.get("branch") or "").strip().removeprefix("refs/heads/")
        branch_exists = branch_name in branches if branches is not None else None
        candidate_states[row_key] = {
            "worktree_exists": path_exists,
            "branch_exists": branch_exists,
            "status": status,
        }
    phase_timings["identity_and_linkage_filter_s"] = max(0.0, _monotonic() - phase_started)

    candidates.sort(key=lambda item: (str(item[0].get("task_ref") or ""), str(item[0].get("lane_id") or "")))
    now_epoch = time.time()
    cursor, retry_entries = _load_retry_state(repo_root, now_epoch=now_epoch)
    candidate_keys = [(str(item[0].get("task_ref") or ""), str(item[0].get("lane_id") or "")) for item in candidates]
    if cursor is not None and candidate_keys:
        if cursor in candidate_keys:
            cursor_index = candidate_keys.index(cursor) + 1
        else:
            cursor_index = next((idx for idx, key in enumerate(candidate_keys) if key > cursor), 0)
        cursor_index %= len(candidates)
        candidates = candidates[cursor_index:] + candidates[:cursor_index]

    outcome_counts: Counter[str] = Counter()
    retired_lane_ids: list[str] = []
    lane_timings: list[dict[str, Any]] = []
    attempted = 0
    has_more = False
    remaining_count = 0
    process_snapshot: tuple[list[str], list[str]] | None = None
    process_probed = False
    retire_seconds = 0.0

    def record_unvisited(start_index: int, reason: str) -> None:
        for pending_row, *_pending_context in candidates[start_index:]:
            _add_unactionable(
                lane_id=pending_row.get("lane_id"),
                task_ref=pending_row.get("task_ref"),
                status=pending_row.get("status"),
                reason=reason,
                could_not_act=could_not_act,
                skip_counts=skip_counts,
            )

    def defer_refused(key: tuple[str, str], candidate_state: dict[str, Any]) -> None:
        prior = retry_entries.get(key, {})
        count = prior.get("count", 0.0) + 1
        delay = min(_RETRY_BASE_S * (2 ** min(int(count) - 1, 8)), _RETRY_MAX_S)
        retry_entries[key] = {
            "count": count,
            "attempted_at": now_epoch,
            "retry_after": now_epoch + delay,
            "candidate_state": candidate_state,
        }

    for index, (row, worktree_path, path_exists, landing) in enumerate(candidates):
        lane_id = str(row.get("lane_id") or "")
        row_task = str(row.get("task_ref") or "")
        key = (row_task, lane_id)
        status = row.get("status")

        if attempted >= max_lanes or _monotonic() >= budget_deadline:
            has_more = True
            remaining_count = len(candidates) - index
            record_unvisited(index, "max_lanes_reached" if attempted >= max_lanes else "budget_exhausted")
            break
        cursor = key

        stamp = _parse_timestamp(row.get("updated_at"))
        if stamp is None:
            _add_unactionable(
                lane_id=lane_id,
                task_ref=row_task,
                status=status,
                reason="updated_at_unknown",
                could_not_act=could_not_act,
                skip_counts=skip_counts,
            )
            continue
        age_s = (datetime.now(timezone.utc) - stamp).total_seconds()
        if age_s < grace_s:
            _add_unactionable(
                lane_id=lane_id,
                task_ref=row_task,
                status=status,
                reason="within_grace",
                could_not_act=could_not_act,
                skip_counts=skip_counts,
            )
            continue
        candidate_state = candidate_states[key]
        retry_entry = retry_entries.get(key)
        if retry_entry is not None and retry_entry.get("retry_after", 0.0) > now_epoch:
            previous_state = retry_entry.get("candidate_state")
            if isinstance(previous_state, dict) and previous_state != candidate_state:
                # A verified Git/worktree transition means the refusal belongs
                # to an earlier physical state. Replay immediately so a crash
                # after worktree removal can finish deleting the branch.
                retry_entries.pop(key, None)
            else:
                _add_unactionable(
                    lane_id=lane_id,
                    task_ref=row_task,
                    status=status,
                    reason="retry_deferred",
                    could_not_act=could_not_act,
                    skip_counts=skip_counts,
                )
                continue

        remaining_s = budget_deadline - _monotonic()
        if remaining_s <= 0:
            has_more = True
            remaining_count = len(candidates) - index
            record_unvisited(index, "budget_exhausted")
            break
        if path_exists:
            status_output = _git_status(worktree_path, timeout_s=remaining_s)
            if status_output is None:
                _add_unactionable(
                    lane_id=lane_id,
                    task_ref=row_task,
                    status=status,
                    reason="status_unknown",
                    could_not_act=could_not_act,
                    skip_counts=skip_counts,
                )
                add_error(f"git_status_failed:{row_task}:{lane_id}")
                continue
            if status_output:
                _add_unactionable(
                    lane_id=lane_id,
                    task_ref=row_task,
                    status=status,
                    reason="dirty",
                    could_not_act=could_not_act,
                    skip_counts=skip_counts,
                )
                continue

        if not process_probed:
            remaining_s = budget_deadline - _monotonic()
            if remaining_s <= 0:
                has_more = True
                remaining_count = len(candidates) - index
                record_unvisited(index, "budget_exhausted")
                break
            process_snapshot = _process_snapshot(timeout_s=remaining_s)
            process_probed = True
        if _monotonic() >= budget_deadline:
            has_more = True
            remaining_count = len(candidates) - index
            record_unvisited(index, "budget_exhausted")
            break
        if process_snapshot is None:
            _add_unactionable(
                lane_id=lane_id,
                task_ref=row_task,
                status=status,
                reason="liveness_unknown",
                could_not_act=could_not_act,
                skip_counts=skip_counts,
            )
            continue
        if _has_live_process(worktree_path, process_snapshot):
            _add_unactionable(
                lane_id=lane_id,
                task_ref=row_task,
                status=status,
                reason="live_process",
                could_not_act=could_not_act,
                skip_counts=skip_counts,
            )
            continue

        try:
            driver_live = _worker_driver_lock_is_live(repo_root, lane_id)
        except Exception:
            driver_live = True
        if driver_live:
            _add_unactionable(
                lane_id=lane_id,
                task_ref=row_task,
                status=status,
                reason="live_worker_driver",
                could_not_act=could_not_act,
                skip_counts=skip_counts,
            )
            continue
        try:
            session_live = _session_live(orchestrator_root=repo_root, worktree=Path(worktree_path))
        except Exception:
            session_live = True
        if session_live is not False:
            _add_unactionable(
                lane_id=lane_id,
                task_ref=row_task,
                status=status,
                reason="live_session_owner",
                could_not_act=could_not_act,
                skip_counts=skip_counts,
            )
            continue

        if _monotonic() >= budget_deadline:
            has_more = True
            remaining_count = len(candidates) - index
            record_unvisited(index, "budget_exhausted")
            break

        if landing is None:
            landing = absorbed.get(key)
        if landing is not None:
            if not apply:
                outcome_counts["would_absorb"] += 1
                continue
            try:
                closed = _close_absorbed_lane(row, landing)
            except Exception as exc:  # noqa: BLE001 — leave the branch intact
                closed = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
            if not isinstance(closed, dict) or closed.get("ok") is not True:
                _add_unactionable(
                    lane_id=lane_id,
                    task_ref=row_task,
                    status=status,
                    reason="absorbed_skip:close_failed",
                    could_not_act=could_not_act,
                    skip_counts=skip_counts,
                )
                defer_refused(key, candidate_state)
                continue
            # Durable close precedes retire: a crash is recovered by the terminal sweep.
            row["status"] = "closed"

        remaining_s = budget_deadline - _monotonic()
        if remaining_s <= _RETIRE_TIMEOUT_CLEANUP_S:
            has_more = True
            remaining_count = len(candidates) - index
            _add_unactionable(
                lane_id=lane_id,
                task_ref=row_task,
                status=status,
                reason="budget_exhausted",
                could_not_act=could_not_act,
                skip_counts=skip_counts,
            )
            break

        attempted += 1
        retire_started = _monotonic()
        result = _retire_lane_bounded(
            lane_id=lane_id,
            task_ref=row_task,
            apply=apply,
            remaining_s=remaining_s,
            repo_root=repo_root,
        )
        elapsed = max(0.0, _monotonic() - retire_started)
        retire_seconds += elapsed
        if len(lane_timings) < _DETAIL_CAP:
            lane_timings.append(
                {
                    "task_ref": row_task,
                    "lane_id": lane_id,
                    "outcome": str(result.get("outcome") or "retire_malformed"),
                    "elapsed_s": elapsed,
                }
            )
        if not isinstance(result, dict) or not isinstance(result.get("outcome"), str):
            outcome_counts["retire_malformed"] += 1
            _add_unactionable(
                lane_id=lane_id,
                task_ref=row_task,
                status=status,
                reason="retire_malformed",
                could_not_act=could_not_act,
                skip_counts=skip_counts,
            )
            add_error(f"retire_result_malformed:{row_task}:{lane_id}")
            defer_refused(key, candidate_state)
            continue

        outcome = str(result["outcome"])
        outcome_counts[outcome] += 1
        if apply and outcome in {"retired", "already_retired"}:
            retired_lane_ids.append(lane_id)
            if key in absorbed:
                absorbed_retired += 1
        failed = result.get("ok") is not True or (
            apply and landing is not None and outcome not in {"retired", "already_retired"}
        )
        if failed:
            _add_unactionable(
                lane_id=lane_id,
                task_ref=row_task,
                status=status,
                reason=outcome,
                could_not_act=could_not_act,
                skip_counts=skip_counts,
            )
            defer_refused(key, candidate_state)
        else:
            retry_entries.pop(key, None)

    phase_timings["retire_s"] = retire_seconds
    try:
        _save_retry_state(repo_root, cursor=cursor, entries=retry_entries, now_epoch=time.time())
    except OSError as exc:
        add_error(f"retry_state_write_failed:{type(exc).__name__}:{exc}")

    phase_started = _monotonic()
    active_but_merged, active_errors, active_complete = _count_active_but_merged(
        repo_root,
        rows,
        deadline=budget_deadline,
        integration_refs_cache=integration_refs_cache,
        branch_inventory=branches,
    )
    for error in active_errors:
        add_error(error)
    phase_timings["active_merged_probe_s"] = max(0.0, _monotonic() - phase_started)
    if not active_complete:
        has_more = True
        remaining_count += sum(
            1 for row in rows if str(row.get("status") or "").strip().lower() not in TERMINAL_STATUSES
        )

    skip_detail_count = sum(count for reason, count in skip_counts.items() if reason != "missing_worktree_and_branch")
    return {
        "ok": not instrument_errors,
        "outcome": "instrument_error" if instrument_errors else ("bounded" if has_more else "completed"),
        "apply": bool(apply),
        "task_ref": task_ref,
        "candidates": len(candidates),
        "attempted": attempted,
        "outcome_counts": dict(sorted(outcome_counts.items())),
        "skip_counts": dict(sorted(skip_counts.items())),
        "retired_lane_ids": retired_lane_ids,
        "could_not_act": could_not_act,
        "could_not_act_truncated": skip_detail_count > len(could_not_act),
        "missing_worktree_and_branch": missing_worktree_and_branch,
        "lane_timings": lane_timings,
        "phase_timings_s": {**phase_timings, "total_s": max(0.0, _monotonic() - started)},
        "absorbed_retired": absorbed_retired,
        "absorbed_skipped": sum((entry["task_ref"], entry["lane_id"]) in nonterminal for entry in could_not_act),
        "active_but_merged": active_but_merged,
        "active_but_merged_complete": active_complete,
        "has_more": has_more,
        "remaining": remaining_count,
        "instrument_errors": instrument_errors[:_DETAIL_CAP],
    }


def sweep(
    *,
    repo_root: Path | str,
    apply: bool,
    max_lanes: int = 20,
    budget_s: float = 120.0,
    grace_s: float = 0.0,
    task_ref: str | None = None,
) -> dict[str, Any]:
    """Retire a bounded set of finished, clean, idle lane worktrees."""
    started = _monotonic()
    budget_deadline = started + max(0.0, float(budget_s))
    try:
        requested_root = Path(repo_root).expanduser().resolve()
        root = _configure_runtime(requested_root)
    except Exception as exc:  # noqa: BLE001 — setup failures fail closed
        return {
            "ok": False,
            "outcome": "instrument_error",
            "apply": bool(apply),
            "has_more": True,
            "remaining": 0,
            "instrument_errors": [f"runtime_config_failed:{type(exc).__name__}:{exc}"],
            "elapsed_s": max(0.0, _monotonic() - started),
        }

    lock, lock_error = _try_acquire_lock(root)
    if lock is None:
        if lock_error == "already_running":
            return {
                "ok": True,
                "outcome": "already_running",
                "apply": bool(apply),
                "has_more": True,
                "remaining": None,
                "elapsed_s": max(0.0, _monotonic() - started),
            }
        return {
            "ok": False,
            "outcome": "instrument_error",
            "apply": bool(apply),
            "has_more": True,
            "remaining": 0,
            "instrument_errors": [str(lock_error or "sweep_lock_failed")],
            "elapsed_s": max(0.0, _monotonic() - started),
        }

    try:
        try:
            summary = _sweep_locked(
                repo_root=root,
                apply=bool(apply),
                max_lanes=max(0, int(max_lanes)),
                deadline=budget_deadline,
                grace_s=max(0.0, float(grace_s)),
                task_ref=task_ref,
                started=started,
            )
        except Exception as exc:  # noqa: BLE001 — top-level instrument containment
            summary = {
                "ok": False,
                "outcome": "instrument_error",
                "apply": bool(apply),
                "task_ref": task_ref,
                "has_more": True,
                "remaining": 0,
                "instrument_errors": [f"sweep_failed:{type(exc).__name__}:{exc}"],
            }
        summary["elapsed_s"] = max(0.0, _monotonic() - started)
        try:
            _write_receipt(root, summary)
        except OSError as exc:
            summary["ok"] = False
            summary["outcome"] = "instrument_error"
            summary.setdefault("instrument_errors", []).append(f"receipt_write_failed:{type(exc).__name__}:{exc}")
    finally:
        _release_lock(lock)
        process_state, process_identity = _read_process_identity(os.getpid())
        if process_state == "available" and process_identity is not None:
            _clear_child_marker(root, identity=process_identity)
    return summary


def spawn_background_sweep(
    repo_root: Path | str,
    *,
    max_lanes: int = _DEFAULT_MAX_LANES,
    budget_s: float = _DEFAULT_SWEEP_BUDGET_S,
    respect_optout: bool = True,
) -> dict[str, Any]:
    """Start an apply sweep detached; return immediately and contain failures."""
    if respect_optout and os.environ.get("WORKBAY_POST_MERGE_RETIRE_SWEEP") == "0":
        return {"ok": True, "outcome": "disabled"}
    try:
        requested_root = Path(repo_root).expanduser().resolve()
        from workbay_handoff_mcp.config import RuntimeConfig  # noqa: PLC0415

        root = Path(RuntimeConfig.for_repo(requested_root).workspace_root).resolve()
        launch_lock, error = _try_acquire_file_lock(root, _LAUNCH_LOCK_RELATIVE)
        if launch_lock is None:
            if error == "already_running":
                return {"ok": True, "outcome": "already_running"}
            return {"ok": False, "outcome": "spawn_failed", "error": str(error)}
        try:
            child_state, child_marker = _sweep_child_state(root)
            child_pid = child_marker.get("pid") if isinstance(child_marker, dict) else None
            if child_state in {"running", "unknown"}:
                return {"ok": True, "outcome": "already_running", "pid": child_pid}
            if child_state == "expired" and child_marker is not None:
                signal_result = _expire_sweep_child(root, child_marker)
                if signal_result == "signaled":
                    return {"ok": True, "outcome": "already_running", "pid": child_pid, "recovered": True}
                if signal_result in {"unknown", "changed"}:
                    return {"ok": True, "outcome": "already_running", "pid": child_pid}
                _clear_child_marker(root, expected=child_marker, launch_lock=launch_lock)
            elif child_state == "stale" and child_marker is not None:
                _clear_child_marker(root, expected=child_marker, launch_lock=launch_lock)

            lock, lock_error = _try_acquire_lock(root)
            if lock is None:
                if lock_error == "already_running":
                    return {"ok": True, "outcome": "already_running"}
                return {"ok": False, "outcome": "spawn_failed", "error": str(lock_error)}
            _release_lock(lock)

            _rotate_sweep_log(root)
            log_path = root / _LOG_RELATIVE
            log_path.parent.mkdir(parents=True, exist_ok=True)
            output = log_path.open("ab")
            try:
                proc = _Popen(
                    [
                        sys.executable,
                        "-m",
                        _MODULE,
                        "--apply",
                        "--max",
                        str(max(0, int(max_lanes))),
                        "--budget-s",
                        str(max(0.0, float(budget_s))),
                    ],
                    cwd=root,
                    stdin=subprocess.DEVNULL,
                    stdout=output,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                    close_fds=True,
                )
            finally:
                output.close()
            pid = getattr(proc, "pid", None)
            if isinstance(pid, int) and pid > 0:
                identity_state, identity = _read_process_identity(pid)
                if (
                    identity_state != "available"
                    or identity is None
                    or not _identity_fields_match(identity, identity, repo_root=root, pid=pid)
                ):
                    # Keep an unknown-token marker while the process may be alive;
                    # it prevents duplicate launches but never grants signal authority.
                    identity = {
                        "pid": pid,
                        "start_token": None,
                        "repo_root": str(root),
                        "pgid": pid,
                        "sid": pid,
                    }
                _atomic_json_replace(
                    root / _CHILD_MARKER_RELATIVE,
                    {"version": 2, **identity, "started_at": time.time()},
                )
            return {"ok": True, "outcome": "started", "pid": pid}
        finally:
            _release_lock(launch_lock)
    except Exception as exc:  # noqa: BLE001 — a hook must never fail the merge
        return {"ok": False, "outcome": "spawn_failed", "error": f"{type(exc).__name__}: {exc}"}


def _main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Bounded sweep of terminal lane worktrees")
    parser.add_argument("--apply", action="store_true", help="retire eligible lanes; default is dry-run")
    parser.add_argument("--max", type=int, default=_DEFAULT_MAX_LANES, dest="max_lanes")
    parser.add_argument("--budget-s", type=float, default=_DEFAULT_SWEEP_BUDGET_S)
    parser.add_argument("--task-ref")
    args = parser.parse_args(argv)
    if args.max_lanes < 0:
        parser.error("--max must be non-negative")
    if args.budget_s < 0:
        parser.error("--budget-s must be non-negative")
    try:
        proc = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            capture_output=True,
            text=True,
            check=False,
            timeout=_GIT_STATUS_TIMEOUT_S,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        summary = {
            "ok": False,
            "outcome": "instrument_error",
            "instrument_errors": [f"repo_root_failed:{type(exc).__name__}:{exc}"],
        }
        print(json.dumps(summary, sort_keys=True))
        return 2
    if proc.returncode != 0 or not proc.stdout.strip():
        summary = {
            "ok": False,
            "outcome": "instrument_error",
            "instrument_errors": [f"repo_root_failed:{(proc.stderr or proc.stdout).strip()}"],
        }
        print(json.dumps(summary, sort_keys=True))
        return 2
    summary = sweep(
        repo_root=Path(proc.stdout.strip()),
        apply=args.apply,
        max_lanes=args.max_lanes,
        budget_s=args.budget_s,
        task_ref=args.task_ref,
    )
    print(json.dumps(summary, sort_keys=True))
    return 0 if summary.get("ok") is True else 2


if __name__ == "__main__":
    raise SystemExit(_main())
