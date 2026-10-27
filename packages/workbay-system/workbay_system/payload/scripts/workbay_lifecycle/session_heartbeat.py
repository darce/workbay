"""Durable session heartbeat for worktree liveness (handoff-independent).

Writer refreshes a session-keyed heartbeat on lifecycle commands; reader
uses pid-existence + fencing token as the primary liveness signal.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator, Literal

try:
    import fcntl as _fcntl
except ImportError:  # non-POSIX: degrade to unlocked RMW rather than raise
    _fcntl = None  # type: ignore[assignment]

import resolver

HEARTBEAT_REL = Path(".task-state") / ".heartbeat"
_PROC_ROOT = Path("/proc")
_COMMAND_PROBE_TIMEOUT_SECONDS = 2.0
_CWD_PROBE_TIMEOUT_SECONDS = 2.0
_SWEEP_PROBE_BUDGET_SECONDS = 5.0

LivenessReason = Literal[
    "live_recent",
    "live_cwd_match",
    "ttl_no_cwd_match",
    "pid_gone",
    "token_mismatch",
    "probe_failed",
]


@dataclass(frozen=True)
class HeartbeatLiveness:
    live: bool
    reason: LivenessReason


CwdProbe = Callable[[int], Iterable[str | Path]]

_SESSION_TRANSCRIPT_ENV_VARS: tuple[str, ...] = (
    "CLAUDE_SESSION_TRANSCRIPT_PATH",
    "CODEX_SESSION_TRANSCRIPT_PATH",
    "GROK_SESSION_TRANSCRIPT_PATH",
    "VSCODE_TARGET_SESSION_LOG",
)

_EPHEMERAL_CMD_RE = re.compile(
    r"(?:^|/)(?:make|sh|bash|zsh|python\d*|lifecycle)(?:\s|$)",
    re.IGNORECASE,
)


def _session_id_from_env() -> str | None:
    for key in _SESSION_TRANSCRIPT_ENV_VARS:
        value = os.environ.get(key, "").strip()
        if value:
            return value
    for key, value in os.environ.items():
        if key.startswith("CLAUDE_SESSION_") and value.strip():
            return value.strip()
    return None


def _session_id_from_getsid() -> str | None:
    try:
        return str(os.getsid(0))
    except OSError:
        return None


def _cmdline_for_pid(pid: int) -> str:
    try:
        proc = subprocess.run(
            ["ps", "-p", str(pid), "-o", "command="],
            capture_output=True,
            text=True,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return ""
    if proc.returncode != 0:
        return ""
    return proc.stdout.strip()


def _is_ephemeral_wrapper(pid: int) -> bool:
    if pid <= 1:
        return False
    cmd = _cmdline_for_pid(pid)
    if not cmd:
        return pid == os.getpid()
    return bool(_EPHEMERAL_CMD_RE.search(cmd))


def _session_id_from_ppid_chain() -> str | None:
    pid = os.getpid()
    visited: set[int] = set()
    while pid > 1 and pid not in visited:
        visited.add(pid)
        if not _is_ephemeral_wrapper(pid):
            return str(pid)
        try:
            pid = os.getppid()
        except OSError:
            break
    return str(os.getpid())


def _resolve_session_pid() -> int:
    try:
        sid = os.getsid(0)
    except OSError:
        sid = None
    if sid is not None and sid > 1:
        try:
            os.kill(sid, 0)
            return sid
        except OSError:
            pass
    pid = os.getpid()
    visited: set[int] = set()
    while pid > 1 and pid not in visited:
        visited.add(pid)
        if not _is_ephemeral_wrapper(pid):
            return pid
        try:
            pid = os.getppid()
        except OSError:
            break
    return os.getpid()


def _process_start_token(pid: int) -> str:
    proc_stat = _PROC_ROOT / str(pid) / "stat"
    if proc_stat.is_file():
        try:
            stat_text = proc_stat.read_text(encoding="utf-8")
            close_paren = stat_text.rfind(")")
            if close_paren != -1:
                fields = stat_text[close_paren + 2 :].split()
                if len(fields) >= 20:
                    return fields[19]
        except OSError:
            pass
    try:
        proc = subprocess.run(
            ["ps", "-p", str(pid), "-o", "lstart="],
            capture_output=True,
            text=True,
            check=False,
            timeout=_COMMAND_PROBE_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.SubprocessError):
        return f"pid:{pid}"
    if proc.returncode != 0:
        return f"pid:{pid}"
    start = proc.stdout.strip()
    return start if start else f"pid:{pid}"


def resolve_session(_repo: Path) -> tuple[str, int, str]:
    """Resolve session identity for heartbeat writes."""
    session_id = _session_id_from_env()
    if not session_id:
        session_id = _session_id_from_getsid()
    if not session_id:
        session_id = _session_id_from_ppid_chain()
    session_pid = _resolve_session_pid()
    token = _process_start_token(session_pid)
    return session_id, session_pid, token


def _warn_exclude_update_failed(exclude: Path) -> None:
    """Best-effort stderr note; must never raise (caller is never-raise)."""
    try:
        sys.stderr.write(f"warning: failed to update git info/exclude at {exclude} (exclude NOT updated)\n")
    except Exception:
        pass


def _ensure_task_state_excluded(repo: Path) -> None:
    """Keep heartbeat artifacts out of ``git status`` without touching ``.gitignore``.

    Resolve the exclude path via ``git rev-parse --git-path`` so linked worktrees
    (where ``.git`` is a file, not a directory) still update the common
    ``info/exclude`` and do not leave ``?? .task-state/`` in porcelain.

    Uses the same shared lock protocol as ``append_git_exclude``: exclusive
    ``fcntl.flock`` on the exclude file handle itself across read-check-append.
    Append-only under that shared lock (no sibling ``exclude.lock`` and no
    snapshot+``os.replace``) so a concurrent co-writer cannot lose its line.

    Every path is never-raise (this runs on every lifecycle dispatch): the git
    path probe has an explicit timeout, lock acquisition is non-blocking with a
    short retry budget, and non-UTF-8 exclude bytes are preserved via
    ``surrogateescape``.
    """
    exclude = repo / ".git" / "info" / "exclude"
    try:
        proc = subprocess.run(
            ["git", "-C", str(repo), "rev-parse", "--show-toplevel", "--git-path", "info/exclude"],
            capture_output=True,
            text=True,
            check=False,
            timeout=5,
        )
        if proc.returncode != 0:
            return
        lines = proc.stdout.strip().splitlines()
        if len(lines) != 2 or Path(lines[0]).resolve() != repo.resolve():
            return
        raw = lines[1]
        exclude = Path(raw) if Path(raw).is_absolute() else (repo / raw).resolve()
        if not repo.is_dir() or not (repo / ".git").exists():
            return
    except (OSError, subprocess.SubprocessError):
        return

    # Update phase: after path resolution succeeded, failures must surface a
    # best-effort warning (never raise — this runs on every lifecycle dispatch).
    try:
        # Never recreate a removed Git directory or checkout.
        exclude.parent.mkdir(exist_ok=True)
        # Same shape as append_git_exclude: ``a+`` + exclusive flock on the
        # exclude handle, hold across read-check-append, flush before unlock.
        # surrogateescape keeps latin-1 / legacy bytes round-trippable so a
        # non-UTF-8 operator exclude never raises UnicodeDecodeError.
        # When fcntl is unavailable, bare append (no snapshot+os.replace).
        if _fcntl is not None:
            with exclude.open("a+", encoding="utf-8", errors="surrogateescape") as handle:
                locked = False
                # Non-blocking exclusive lock with a short retry budget so a
                # stuck holder cannot wedge every lifecycle command forever.
                # Only BlockingIOError is contention; other OSError (e.g.
                # PermissionError) must not be retried as if it were a wait.
                # Budget exceeds co-writer holds in concurrent tests (~1s) while
                # remaining strictly bounded so a stuck holder cannot hang forever.
                _lock_attempts = 50
                _lock_sleep_s = 0.05
                for attempt in range(_lock_attempts):
                    try:
                        _fcntl.flock(
                            handle.fileno(),
                            _fcntl.LOCK_EX | _fcntl.LOCK_NB,
                        )
                        locked = True
                        break
                    except BlockingIOError:
                        if attempt + 1 >= _lock_attempts:
                            break
                        time.sleep(_lock_sleep_s)
                if not locked:
                    # Lock exhausted: leave exclude unchanged; warn clearly that
                    # the entry was NOT written (R8-HB-01).
                    _warn_exclude_update_failed(exclude)
                    return
                try:
                    handle.seek(0)
                    existing = handle.read()
                    existing_lines = existing.splitlines()
                    if ".task-state/" in existing_lines:
                        return
                    # Decide separator from raw text: splitlines() drops a trailing
                    # empty line, so existing_lines[-1] != "" is always true for any
                    # non-empty newline-terminated file and would inject a blank line.
                    if existing and not existing.endswith("\n"):
                        handle.write("\n")
                    handle.write(".task-state/\n")
                    # Flush userspace buffer BEFORE releasing the lock so a waiter
                    # cannot re-observe a missing entry and append a duplicate.
                    handle.flush()
                finally:
                    if locked:
                        try:
                            _fcntl.flock(handle.fileno(), _fcntl.LOCK_UN)
                        except OSError:
                            pass
        else:
            with exclude.open("a+", encoding="utf-8", errors="surrogateescape") as handle:
                handle.seek(0)
                existing = handle.read()
                existing_lines = existing.splitlines()
                if ".task-state/" in existing_lines:
                    return
                if existing and not existing.endswith("\n"):
                    handle.write("\n")
                handle.write(".task-state/\n")
                handle.flush()
    except (OSError, UnicodeDecodeError):
        _warn_exclude_update_failed(exclude)
        return


def _heartbeat_dir(repo: Path) -> Path:
    return repo / HEARTBEAT_REL


def _heartbeat_filename(session_id: str) -> str:
    """Stable, collision-resistant on-disk name for ``session_id``."""
    digest = hashlib.sha256(session_id.encode("utf-8")).hexdigest()[:16]
    safe = re.sub(r"[^\w.@+-]", "_", session_id)[:120] or "session"
    return f"{safe}__{digest}.json"


def heartbeat_path_for_session(repo: Path, session_id: str) -> Path:
    """Return the heartbeat file path for ``session_id`` under ``repo``."""
    return _heartbeat_dir(repo) / _heartbeat_filename(session_id)


def _scan_root_for_worktree(repo: Path, worktree: str) -> Path:
    """Heartbeat artifacts live in the linked checkout, not canonical workspace."""
    wt_root = Path(worktree).resolve()
    repo_root = Path(repo).resolve()
    if wt_root != repo_root and not (wt_root / HEARTBEAT_REL).is_dir():
        return wt_root
    if (wt_root / HEARTBEAT_REL).is_dir() or wt_root == repo_root:
        return wt_root
    return repo_root


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    # Create one level at a time: removal of the checkout must fail closed,
    # never recreate the checkout through recursive mkdir.
    path.parent.parent.mkdir(exist_ok=True)
    path.parent.mkdir(exist_ok=True)
    tmp = path.with_name(f"{path.name}.{os.getpid()}.{time.time_ns()}.tmp")
    tmp.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def touch_heartbeat(repo: Path) -> None:
    """Refresh the session heartbeat for ``repo``; never raises."""
    try:
        session_id, session_pid, token = resolve_session(repo)
        branch = resolver.current_branch(repo) or ""
        worktree = str(resolver.current_worktree(repo) or repo.resolve())
        payload = {
            "session_id": session_id,
            "session_pid": session_pid,
            "token": token,
            "ts": time.time(),
            "branch": branch,
            "worktree": worktree,
        }
        _ensure_task_state_excluded(repo)
        path = heartbeat_path_for_session(repo, session_id)
        _atomic_write_json(path, payload)
    except Exception:
        return


def _read_heartbeat(path: Path) -> dict[str, Any] | None:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, ValueError):
        return None
    return data if isinstance(data, dict) else None


def _pid_alive(pid: int, alive: Callable[[int, int], Any]) -> bool:
    try:
        alive(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        raise


def _remaining_probe_time(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("session cwd probe exceeded its time budget")
    return remaining


def _proc_session_cwds(session_pid: int, deadline: float) -> Iterator[str | Path]:
    try:
        session_id = os.getsid(session_pid)
    except (ProcessLookupError, FileNotFoundError):
        return

    unreadable_member = False
    for entry in _PROC_ROOT.iterdir():
        _remaining_probe_time(deadline)
        if not entry.name.isdigit():
            continue
        pid = int(entry.name)
        try:
            member_session_id = os.getsid(pid)
        except (ProcessLookupError, FileNotFoundError):
            continue
        except OSError:
            raise
        if member_session_id != session_id:
            continue
        try:
            os.kill(pid, 0)
        except (ProcessLookupError, FileNotFoundError):
            continue
        except PermissionError:
            continue
        except OSError:
            raise
        try:
            cwd = os.readlink(_PROC_ROOT / str(pid) / "cwd")
        except (ProcessLookupError, FileNotFoundError):
            continue
        except OSError:
            unreadable_member = True
            continue
        yield cwd

    if unreadable_member:
        raise OSError("a signalable session member cwd could not be read")


def _bsd_session_cwds(session_pid: int, deadline: float) -> Iterator[str | Path]:
    try:
        session_id = os.getsid(session_pid)
    except (ProcessLookupError, FileNotFoundError):
        return

    proc = subprocess.run(
        ["ps", "-axo", "pid="],
        capture_output=True,
        text=True,
        check=False,
        timeout=_remaining_probe_time(deadline),
    )
    if proc.returncode != 0:
        raise OSError(f"ps exited with status {proc.returncode}")
    listed_pids: list[int] = []
    for raw_pid in proc.stdout.splitlines():
        _remaining_probe_time(deadline)
        value = raw_pid.strip()
        if not value:
            continue
        if not value.isdecimal():
            raise ValueError(f"ps returned an invalid pid: {value!r}")
        pid = int(value)
        if pid not in listed_pids:
            listed_pids.append(pid)

    members: list[int] = []
    for pid in listed_pids:
        _remaining_probe_time(deadline)
        try:
            member_session_id = os.getsid(pid)
        except (ProcessLookupError, FileNotFoundError):
            continue
        except OSError:
            raise
        if member_session_id != session_id:
            continue
        try:
            os.kill(pid, 0)
        except (ProcessLookupError, FileNotFoundError):
            continue
        except PermissionError:
            continue
        except OSError:
            raise
        members.append(pid)

    if not members:
        return

    proc = subprocess.run(
        ["lsof", "-a", "-d", "cwd", "-Fpfn", "-p", ",".join(map(str, members))],
        capture_output=True,
        text=True,
        check=False,
        timeout=_remaining_probe_time(deadline),
    )
    if proc.returncode not in (0, 1):
        raise OSError(f"lsof exited with status {proc.returncode}")

    cwd_paths: dict[int, str] = {}
    current_pid: int | None = None
    cwd_record = False
    for line in proc.stdout.splitlines():
        _remaining_probe_time(deadline)
        if not line:
            continue
        if line.startswith("p"):
            value = line[1:]
            if not value.isdecimal():
                raise ValueError(f"lsof returned an invalid pid record: {line!r}")
            current_pid = int(value)
            if current_pid not in members:
                raise ValueError(f"lsof returned an unrequested pid: {current_pid}")
            cwd_record = False
        elif line == "fcwd":
            if current_pid is None:
                raise ValueError("lsof returned a cwd record without a pid")
            cwd_record = True
        elif line.startswith("n"):
            if current_pid is None or not cwd_record or len(line) == 1:
                raise ValueError(f"lsof returned an invalid cwd path: {line!r}")
            cwd_paths[current_pid] = line[1:]
            cwd_record = False
        else:
            raise ValueError(f"lsof returned an invalid field: {line!r}")

    unreadable_member = False
    for pid in members:
        _remaining_probe_time(deadline)
        cwd = cwd_paths.get(pid)
        if cwd is not None:
            yield cwd
            continue
        try:
            os.kill(pid, 0)
        except (ProcessLookupError, FileNotFoundError):
            continue
        except PermissionError:
            continue
        except OSError:
            raise
        unreadable_member = True
    if unreadable_member:
        raise OSError("a signalable session member cwd could not be read")


def _default_cwd_probe(session_pid: int) -> Iterator[str | Path]:
    """Yield inspectable process cwd paths from the session of ``session_pid``.

    Members that cannot be signalled are skipped because they may belong to another uid and are not evidence; the task_finish lsof guard likewise sees only processes lsof can read.
    """
    deadline = time.monotonic() + _CWD_PROBE_TIMEOUT_SECONDS
    proc_dir = _PROC_ROOT / str(session_pid)
    if proc_dir.is_dir():
        yield from _proc_session_cwds(session_pid, deadline)
    else:
        yield from _bsd_session_cwds(session_pid, deadline)


def heartbeat_liveness(
    session_pid: int,
    token: str,
    worktree: str | Path | None,
    *,
    mtime: float,
    ttl_seconds: int = 900,
    now: float | None = None,
    alive: Callable[[int, int], Any] | None = None,
    cwd_probe: CwdProbe | None = None,
) -> HeartbeatLiveness:
    """Return the fail-safe liveness verdict for one heartbeat."""
    alive_fn = alive or os.kill
    try:
        pid_alive = _pid_alive(session_pid, alive_fn)
    except Exception:
        return HeartbeatLiveness(True, "probe_failed")
    if not pid_alive:
        return HeartbeatLiveness(False, "pid_gone")

    try:
        current_token = _process_start_token(session_pid)
    except Exception:
        return HeartbeatLiveness(True, "probe_failed")
    sentinel = f"pid:{session_pid}"
    if token != sentinel and current_token != sentinel and current_token != token:
        return HeartbeatLiveness(False, "token_mismatch")

    now_value = time.time() if now is None else now
    if now_value - mtime <= ttl_seconds:
        return HeartbeatLiveness(True, "live_recent")
    if worktree is None or str(worktree) == "":
        return HeartbeatLiveness(True, "probe_failed")

    try:
        worktree_path = Path(worktree).resolve()
    except Exception:
        return HeartbeatLiveness(True, "probe_failed")
    probe = cwd_probe or _default_cwd_probe
    cwd_iterable: Iterable[str | Path] = ()
    try:
        cwd_iterable = probe(session_pid)
        for cwd in cwd_iterable:
            cwd_path = Path(cwd).resolve()
            if cwd_path == worktree_path or worktree_path in cwd_path.parents:
                return HeartbeatLiveness(True, "live_cwd_match")
    except Exception:
        return HeartbeatLiveness(True, "probe_failed")
    finally:
        close = getattr(cwd_iterable, "close", None)
        if callable(close):
            close()
    return HeartbeatLiveness(False, "ttl_no_cwd_match")


@dataclass(frozen=True)
class _HeartbeatSurvivor:
    path: Path
    payload: dict[str, Any]
    liveness: HeartbeatLiveness


def _heartbeat_version(stat_result: os.stat_result) -> tuple[int, int, int]:
    return (stat_result.st_ino, stat_result.st_mtime_ns, stat_result.st_size)


def _unlink_heartbeat_if_unchanged(
    path: Path, version: tuple[int, int, int]
) -> Literal["gone", "changed", "unlinked", "failed"]:
    try:
        current_version = _heartbeat_version(path.stat())
    except FileNotFoundError:
        return "gone"
    except OSError as exc:
        sys.stderr.write(f"[session_heartbeat] could not verify stale heartbeat {path.name}: {exc}\n")
        return "changed"
    if current_version != version:
        return "changed"

    # A refresh between this stat and unlink can still be lost until the
    # session's next lifecycle command writes its heartbeat again. Closing
    # that window needs a writer lock, which is out of scope for this lane.
    try:
        path.unlink()
    except FileNotFoundError:
        return "gone"
    except OSError as exc:
        sys.stderr.write(f"[session_heartbeat] could not remove stale heartbeat {path.name}: {exc}\n")
        return "failed"
    return "unlinked"


def _sweep_heartbeats(
    repo: Path,
    *,
    ttl_seconds: int,
    clock: Callable[[], float] | None,
    alive: Callable[[int, int], Any] | None,
    cwd_probe: CwdProbe | None,
) -> list[_HeartbeatSurvivor]:
    hb_dir = _heartbeat_dir(repo)
    if not hb_dir.is_dir():
        return []
    now = (clock or time.time)()
    alive_fn = alive or os.kill
    probe_seconds = 0.0

    def measured_probe(session_pid: int) -> Iterator[str | Path]:
        nonlocal probe_seconds
        if _SWEEP_PROBE_BUDGET_SECONDS <= 0 or probe_seconds > _SWEEP_PROBE_BUDGET_SECONDS:
            raise TimeoutError("session cwd probe sweep budget exhausted")
        start = time.monotonic()
        try:
            probe = cwd_probe or _default_cwd_probe
            yield from probe(session_pid)
        finally:
            probe_seconds += time.monotonic() - start

    survivors: list[_HeartbeatSurvivor] = []
    for path in list(hb_dir.glob("*.json")):
        try:
            file_stat = path.stat()
        except FileNotFoundError:
            continue
        except OSError:
            continue
        version = _heartbeat_version(file_stat)
        try:
            raw = path.read_text(encoding="utf-8")
        except FileNotFoundError:
            continue
        except UnicodeError:
            _unlink_heartbeat_if_unchanged(path, version)
            continue
        except OSError:
            continue
        try:
            payload = json.loads(raw)
        except (json.JSONDecodeError, ValueError):
            _unlink_heartbeat_if_unchanged(path, version)
            continue
        if not isinstance(payload, dict):
            _unlink_heartbeat_if_unchanged(path, version)
            continue
        session_pid = payload.get("session_pid")
        token = payload.get("token")
        if type(session_pid) is not int or not isinstance(token, str):
            _unlink_heartbeat_if_unchanged(path, version)
            continue

        verdict = heartbeat_liveness(
            session_pid,
            token,
            payload.get("worktree"),
            mtime=file_stat.st_mtime,
            ttl_seconds=ttl_seconds,
            now=now,
            alive=alive_fn,
            cwd_probe=measured_probe,
        )

        if verdict.live:
            survivors.append(_HeartbeatSurvivor(path, payload, verdict))
            continue
        unlink_status = _unlink_heartbeat_if_unchanged(path, version)
        if unlink_status == "changed":
            survivors.append(_HeartbeatSurvivor(path, payload, HeartbeatLiveness(True, "live_recent")))
    return survivors


def gc_heartbeats(
    repo: Path,
    *,
    ttl_seconds: int = 900,
    clock: Callable[[], float] | None = None,
    alive: Callable[[int, int], Any] | None = None,
    cwd_probe: CwdProbe | None = None,
) -> None:
    """Remove stale heartbeats using one bounded session-liveness sweep."""
    _sweep_heartbeats(
        repo,
        ttl_seconds=ttl_seconds,
        clock=clock,
        alive=alive,
        cwd_probe=cwd_probe,
    )


def worktree_has_live_session(
    repo: Path,
    worktree: str,
    *,
    exclude_session_id: str = "",
    alive: Callable[[int, int], Any] | None = None,
    clock: Callable[[], float] | None = None,
    ttl_seconds: int = 900,
    cwd_probe: CwdProbe | None = None,
) -> bool:
    """Return True when another live session holds ``worktree``.

    Heartbeats are stored under the linked worktree checkout
    (``Path(worktree)/.task-state/.heartbeat``). ``repo`` is retained for
    call-site compatibility; the scan root is derived from ``worktree``.
    """
    scan_root = _scan_root_for_worktree(repo, worktree)
    worktree_norm = str(Path(worktree).resolve())
    survivors = _sweep_heartbeats(
        scan_root,
        ttl_seconds=ttl_seconds,
        clock=clock,
        alive=alive,
        cwd_probe=cwd_probe,
    )
    for survivor in survivors:
        payload = survivor.payload
        session_id = payload.get("session_id")
        if not isinstance(session_id, str) or session_id == exclude_session_id:
            continue
        wt = payload.get("worktree")
        if not isinstance(wt, str) or not wt:
            continue
        try:
            wt_norm = str(Path(wt).resolve())
        except (OSError, ValueError):
            continue
        if wt_norm != worktree_norm:
            continue
        if survivor.liveness.live:
            if survivor.liveness.reason == "probe_failed":
                sys.stderr.write(
                    f"[session_heartbeat] kept heartbeat {survivor.path.name}: {survivor.liveness.reason}\n"
                )
            return True
    return False
