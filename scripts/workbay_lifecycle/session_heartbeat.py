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
import time
from pathlib import Path
from typing import Any, Callable

import resolver

HEARTBEAT_REL = Path(".task-state") / ".heartbeat"

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
    proc_stat = Path(f"/proc/{pid}/stat")
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


def _ensure_task_state_excluded(repo: Path) -> None:
    """Keep heartbeat artifacts out of ``git status`` without touching ``.gitignore``."""
    exclude = repo / ".git" / "info" / "exclude"
    try:
        exclude.parent.mkdir(parents=True, exist_ok=True)
        text = exclude.read_text(encoding="utf-8") if exclude.is_file() else ""
        if ".task-state/" in text.splitlines():
            return
        exclude.write_text(text.rstrip("\n") + "\n.task-state/\n", encoding="utf-8")
    except OSError:
        return


def _heartbeat_dir(repo: Path) -> Path:
    _ensure_task_state_excluded(repo)
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
    path.parent.mkdir(parents=True, exist_ok=True)
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


def _token_matches(pid: int, token: str, alive: Callable[[int, int], Any]) -> bool:
    if not _pid_alive(pid, alive):
        return False
    return _process_start_token(pid) == token


def gc_heartbeats(
    repo: Path,
    *,
    ttl_seconds: int = 900,
    clock: Callable[[], float] | None = None,
    alive: Callable[[int, int], Any] | None = None,
) -> None:
    """Remove dead, recycled-pid, or expired-and-dead heartbeat files."""
    now_fn = clock or time.time
    alive_fn = alive or os.kill
    hb_dir = _heartbeat_dir(repo)
    if not hb_dir.is_dir():
        return
    now = now_fn()
    for path in list(hb_dir.glob("*.json")):
        payload = _read_heartbeat(path)
        if payload is None:
            try:
                path.unlink(missing_ok=True)
            except OSError:
                pass
            continue
        pid_raw = payload.get("session_pid")
        token = payload.get("token")
        if not isinstance(pid_raw, int) or not isinstance(token, str):
            try:
                path.unlink(missing_ok=True)
            except OSError:
                pass
            continue
        try:
            pid_alive = _pid_alive(pid_raw, alive_fn)
        except OSError:
            continue
        token_ok = pid_alive and _token_matches(pid_raw, token, alive_fn)
        try:
            mtime = path.stat().st_mtime
        except OSError:
            mtime = now
        expired = (now - mtime) > ttl_seconds
        if not pid_alive or not token_ok or (expired and not pid_alive):
            try:
                path.unlink(missing_ok=True)
            except OSError:
                pass


def worktree_has_live_session(
    repo: Path,
    worktree: str,
    *,
    exclude_session_id: str = "",
    alive: Callable[[int, int], Any] | None = None,
    clock: Callable[[], float] | None = None,
) -> bool:
    """Return True when another live session holds ``worktree``.

    Heartbeats are stored under the linked worktree checkout
    (``Path(worktree)/.task-state/.heartbeat``). ``repo`` is retained for
    call-site compatibility; the scan root is derived from ``worktree``.
    """
    scan_root = _scan_root_for_worktree(repo, worktree)
    alive_fn = alive or os.kill
    gc_heartbeats(scan_root, clock=clock, alive=alive_fn)
    hb_dir = _heartbeat_dir(scan_root)
    if not hb_dir.is_dir():
        return False
    worktree_norm = str(Path(worktree).resolve())
    for path in hb_dir.glob("*.json"):
        payload = _read_heartbeat(path)
        if payload is None:
            continue
        session_id = payload.get("session_id")
        if not isinstance(session_id, str) or session_id == exclude_session_id:
            continue
        wt = payload.get("worktree")
        if not isinstance(wt, str) or str(Path(wt).resolve()) != worktree_norm:
            continue
        pid_raw = payload.get("session_pid")
        token = payload.get("token")
        if not isinstance(pid_raw, int) or not isinstance(token, str):
            continue
        try:
            if _pid_alive(pid_raw, alive_fn) and _token_matches(pid_raw, token, alive_fn):
                return True
        except OSError:
            return True
    return False
