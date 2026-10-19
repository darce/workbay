#!/usr/bin/env python3
"""Best-effort re-derive of this checkout's root virtualenv after a merge.

The post-merge hook calls this helper after merges that change the dependency
lock or package versions. It only operates on a real ``.venv`` owned by the
current checkout, and it always exits successfully so a merge cannot be
rejected by install repair.
"""

from __future__ import annotations

import math
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

_SOURCE = "post-merge-venv"
_MANUAL_REMEDY = "make sync-version-drift-venv"
_DEFAULT_TIMEOUT_S = 600.0
_OUTPUT_TAIL_CHARS = 300


def _hooks_dir() -> str:
    return str(Path(__file__).resolve().parent)


def _record_failure(
    *, kind: str, detail: str, repo_root: Path | None
) -> None:
    """Append one durable failure record without ever raising into git."""
    try:
        hooks_dir = _hooks_dir()
        if hooks_dir not in sys.path:
            sys.path.insert(0, hooks_dir)
        from _hook_failure_sink import record_hook_failure

        record_hook_failure(
            source=_SOURCE,
            kind=kind,
            detail=detail,
            repo_root=str(repo_root) if repo_root is not None else None,
        )
    except BaseException:  # noqa: BLE001 -- sink must never break the merge
        pass


def _single_line(value: object) -> str:
    return " ".join(str(value).split())


def _tail(*outputs: str | bytes | None) -> str:
    parts: list[str] = []
    for output in outputs:
        if isinstance(output, bytes):
            output = output.decode("utf-8", errors="replace")
        if output:
            parts.append(output)
    return _single_line("\n".join(parts))[-_OUTPUT_TAIL_CHARS:]


def _failure_note(failure: str) -> None:
    try:
        print(
            f"post-merge-venv: {_single_line(failure)}; run {_MANUAL_REMEDY}",
            file=sys.stderr,
        )
    except BaseException:  # noqa: BLE001 -- diagnostics must not break the merge
        pass


def _resolve_repo_root() -> Path | None:
    proc = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"],
        cwd=Path.cwd(),
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    if proc.returncode != 0 or not proc.stdout.strip():
        return None
    return Path(proc.stdout.strip()).resolve()


def _git_output(repo: Path, *args: str) -> str | None:
    proc = subprocess.run(
        ["git", *args],
        cwd=repo,
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    if proc.returncode != 0:
        return None
    return proc.stdout.strip()


def _trigger_targets(changed_paths: list[str]) -> list[str]:
    run_sync = False
    run_drift_sync = False
    for name in changed_paths:
        parts = name.split("/")
        if name in {"uv.lock", "pyproject.toml"}:
            run_sync = True
            run_drift_sync = True
        elif (
            len(parts) == 3
            and parts[0] == "packages"
            and parts[2] == "pyproject.toml"
        ):
            run_drift_sync = True
    targets: list[str] = []
    if run_sync:
        targets.append("sync")
    if run_drift_sync:
        targets.append("sync-version-drift-venv")
    return targets


def _timeout_s() -> float:
    raw = os.environ.get("WORKBAY_POST_MERGE_VENV_TIMEOUT_S")
    if raw is None:
        return _DEFAULT_TIMEOUT_S
    value = float(raw)
    if not math.isfinite(value) or value <= 0:
        raise ValueError("WORKBAY_POST_MERGE_VENV_TIMEOUT_S must be a positive number")
    return value


def _acquire_lock(venv: Path) -> tuple[Any, Any, bool]:
    try:
        import fcntl
    except ImportError:  # pragma: no cover - Windows has no fcntl
        return None, None, False

    lock_file = (venv / ".post-merge-rederive.lock").open("a", encoding="utf-8")
    try:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        lock_file.close()
        return None, None, True
    return lock_file, fcntl, False


def _release_lock(lock_file: Any, fcntl: Any) -> None:
    if lock_file is None or fcntl is None:
        return
    try:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
    finally:
        lock_file.close()


def _stop_make_process_group(proc: subprocess.Popen[str]) -> None:
    """Stop make and any install subprocesses it started, then reap make."""
    killpg = getattr(os, "killpg", None)
    if callable(killpg):
        try:
            killpg(proc.pid, signal.SIGTERM)
        except BaseException:  # noqa: BLE001 -- timeout cleanup must be best effort
            pass
        try:
            proc.wait(timeout=2.0)
        except BaseException:  # noqa: BLE001 -- escalate even if make won't exit
            pass
        try:
            killpg(proc.pid, signal.SIGKILL)
        except BaseException:  # noqa: BLE001 -- group may already be gone
            pass
    else:
        try:
            proc.kill()
        except BaseException:  # noqa: BLE001 -- timeout cleanup must be best effort
            pass

    try:
        proc.communicate(timeout=2.0)
    except subprocess.TimeoutExpired:
        try:
            proc.kill()
        except BaseException:  # noqa: BLE001 -- parent may already be gone
            pass
        try:
            proc.communicate(timeout=2.0)
        except BaseException:  # noqa: BLE001 -- never strand the post-merge hook
            pass
    except BaseException:  # noqa: BLE001 -- never strand the post-merge hook
        pass


def _run_make(
    repo: Path,
    target: str,
    make_executable: str,
    deadline: float,
) -> bool:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        _record_failure(
            kind="exception",
            detail=f"make {target} timed out before it could start",
            repo_root=repo,
        )
        _failure_note(f"make {target} timed out")
        return False

    try:
        proc = subprocess.Popen(
            [make_executable, target],
            cwd=repo,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            start_new_session=(os.name == "posix"),
        )
        stdout, stderr = proc.communicate(timeout=remaining)
    except subprocess.TimeoutExpired as exc:
        output_tail = _tail(exc.stdout, exc.stderr)
        try:
            _stop_make_process_group(proc)
        except BaseException:  # noqa: BLE001 -- timeout cleanup must not break merge
            pass
        detail = f"make {target} timed out"
        if output_tail:
            detail += f"; output tail: {output_tail}"
        _record_failure(kind="exception", detail=detail, repo_root=repo)
        _failure_note(f"make {target} timed out")
        return False
    except BaseException as exc:  # noqa: BLE001 -- spawning make must not break merge
        detail = f"make {target} could not run: {type(exc).__name__}: {exc}"
        _record_failure(kind="exception", detail=detail, repo_root=repo)
        _failure_note(f"make {target} could not run ({type(exc).__name__})")
        return False

    if proc.returncode != 0:
        output_tail = _tail(stdout, stderr)
        detail = f"make {target} exited {proc.returncode}"
        if output_tail:
            detail += f"; output tail: {output_tail}"
        _record_failure(kind="returncode", detail=detail, repo_root=repo)
        _failure_note(f"make {target} failed (exit {proc.returncode})")
        return False
    return True


def _main(argv: list[str], state: dict[str, Path | None]) -> int:
    if argv and argv[0] == "1":
        return 0
    if os.environ.get("WORKBAY_POST_MERGE_SKIP_VENV_REDERIVE") == "1":
        return 0

    repo = _resolve_repo_root()
    if repo is None:
        return 0
    state["repo"] = repo
    if not (repo / "scripts" / "version_of_drift.py").is_file():
        return 0

    venv = repo / ".venv"
    if not (venv / "bin" / "python").exists():
        return 0
    if venv.is_symlink():
        return 0
    try:
        venv.resolve().relative_to(repo.resolve())
    except ValueError:
        return 0

    orig_head = _git_output(repo, "rev-parse", "--verify", "ORIG_HEAD^{commit}")
    head = _git_output(repo, "rev-parse", "--verify", "HEAD^{commit}")
    if not orig_head or not head or orig_head == head:
        return 0

    diff = subprocess.run(
        ["git", "diff", "--name-only", "ORIG_HEAD", "HEAD"],
        cwd=repo,
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    if diff.returncode != 0:
        raise RuntimeError(
            f"git diff --name-only ORIG_HEAD HEAD exited {diff.returncode}: "
            f"{_tail(diff.stdout, diff.stderr)}"
        )
    targets = _trigger_targets(diff.stdout.splitlines())
    if not targets:
        return 0

    timeout_s = _timeout_s()
    lock_file, fcntl, lock_busy = _acquire_lock(venv)
    if lock_busy:
        print("post-merge-venv: re-derive already running", file=sys.stderr)
        return 0

    try:
        deadline = time.monotonic() + timeout_s
        make_executable = os.environ.get("WORKBAY_POST_MERGE_MAKE", "make")
        for target in targets:
            if not _run_make(repo, target, make_executable, deadline):
                return 0
    finally:
        _release_lock(lock_file, fcntl)

    print(
        f"post-merge-venv: re-derived .venv ({', '.join(targets)})",
        file=sys.stderr,
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    state: dict[str, Path | None] = {"repo": None}
    try:
        return _main(list(sys.argv[1:] if argv is None else argv), state)
    except BaseException as exc:  # noqa: BLE001 -- git hooks must always exit 0
        repo = state["repo"]
        _record_failure(
            kind="exception",
            detail=f"{type(exc).__name__}: {_single_line(exc)}",
            repo_root=repo,
        )
        _failure_note(f"failed ({type(exc).__name__})")
        return 0


if __name__ == "__main__":
    sys.exit(main())
