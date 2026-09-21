"""Stdlib-only sink for hook provenance-write failures.

A failed ``record_file_touch`` / ``errors-record`` subprocess must never again
present as success. Hooks keep exit 0 (do not block Edit/Write); they append
here so implementation note's doctor facet and implementation note's reaper can surface the failure.

Path rule (worktree-shared):
  ``<git rev-parse --git-common-dir>/../.task-state/hook-failures.log``

Do **not** resolve via ``os.path`` relative ``.task-state`` under the linked
worktree cwd — that path is not filesystem-shared, and a hook firing in a
linked worktree would write a log ``make doctor`` never opens.

Record format (line-oriented, parseable by implementation note)::

    ts=<iso> source=<name> kind=<returncode|exception> detail=<one line>

Bound: keep the last ``MAX_RECORDS`` lines with per-source fairness
(``MAX_RECORDS_PER_SOURCE`` newest per distinct ``source=``) before the
global cap (truncate on write, stdlib-only). Purge owner: implementation note reaper
(this module only truncates to the bound; implementation note's ``hook_failure_sink``
doctor facet reports but does not clear).

Write path (concurrency):
  - Each record is one ``O_APPEND`` ``os.write`` of a single UTF-8 line
    (detail truncation keeps lines ≤ ~512 B so the write is atomic on
    ordinary POSIX filesystems), performed while holding the shared
    ``<log>.lock`` flock so it serializes with compaction's rewrite.
  - ``MAX_RECORDS`` is enforced by a separate ``fcntl.flock``-guarded
    compaction step that only runs when the file exceeds the line/byte
    threshold, using write-temp + ``os.replace`` while holding the same lock.
  - Readers must tolerate a torn/partial last line; iterate on ``\\n``
    splits only (not ``str.splitlines()``) so Unicode linebreaks inside a
    sanitized detail cannot fragment records.

No imports from ``workbay_handoff_mcp`` — the sink must outlive the stack
failure it records (``[OBS-08]``).
"""

from __future__ import annotations

import fcntl
import os
import subprocess
import sys
import tempfile
from datetime import datetime, timezone

# Last-N line ceiling. Purge owner: implementation note reaper (see module docstring).
MAX_RECORDS = 200

# Per-source fairness: keep this many newest records per distinct source=
# before applying the global MAX_RECORDS cap, so one chatty source cannot
# evict every other source's evidence.
MAX_RECORDS_PER_SOURCE = 50

# Compaction triggers when either bound is exceeded (cheap pre-check; re-checked
# under flock). Byte bound ≈ MAX_RECORDS full records (detail ≤ 500 chars).
_COMPACT_BYTE_THRESHOLD = MAX_RECORDS * 512

# Characters that str.splitlines() treats as line breaks. Writers must flatten
# ALL of them so a foreign detail cannot fragment into a fake own-record line
# under purge/readers that use splitlines(). Readers/compaction/purge iterate
# on "\n" only.
_LINEBREAK_CHARS = (
    "\n",  # LF
    "\r",  # CR
    "\x0b",  # VT
    "\x0c",  # FF
    "\x85",  # NEL
    "\u2028",  # LINE SEPARATOR
    "\u2029",  # PARAGRAPH SEPARATOR
)

_SOURCE_ALLOWED = frozenset(
    {
        "record-file-touch",
        "capture-agent-errors",
        "compact-session",
        "post-merge-reap",
    }
)


def resolve_hook_failures_log_path(repo_root: str | None = None) -> str:
    """Return the worktree-anchored hook-failures.log absolute path.

    ``repo_root`` (when provided) is used as ``cwd`` for the git discovery
    subprocess so a hook invoked with cwd outside the worktree still anchors
    correctly. Empty string when git discovery fails (caller treats as no-op
    write); a single last-resort line is written to stderr.
    """
    try:
        run_kwargs: dict = {
            "capture_output": True,
            "text": True,
            "timeout": 5,
        }
        if repo_root:
            run_kwargs["cwd"] = repo_root
        proc = subprocess.run(
            ["git", "rev-parse", "--path-format=absolute", "--git-common-dir"],
            **run_kwargs,
        )
        if proc.returncode != 0:
            _discovery_failed(
                f"git rev-parse exit {proc.returncode}"
                + (f" cwd={repo_root!r}" if repo_root else "")
            )
            return ""
        common = (proc.stdout or "").strip()
        if not common:
            _discovery_failed("git rev-parse returned empty common-dir")
            return ""
        primary = os.path.dirname(common)
        if not primary:
            _discovery_failed("git common-dir has no parent")
            return ""
        return os.path.join(primary, ".task-state", "hook-failures.log")
    except Exception as exc:  # noqa: BLE001 -- sink must never raise into the hook
        _discovery_failed(f"{type(exc).__name__}: {exc}")
        return ""


def _discovery_failed(reason: str) -> None:
    """Last-resort stderr note when the log path cannot be resolved."""
    try:
        print(
            f"hook-failures.log: could not resolve path ({reason})",
            file=sys.stderr,
        )
    except Exception:  # noqa: BLE001 -- never raise from the sink
        pass


def log_lock_path(log_path: str) -> str:
    """Sidecar lockfile path shared by append, compaction, and purge."""
    return log_path + ".lock"


def _sanitize_detail(detail: str) -> str:
    text = detail or ""
    for ch in _LINEBREAK_CHARS:
        text = text.replace(ch, " ")
    text = text.strip()
    if len(text) > 500:
        text = text[:497] + "..."
    return text or "(no detail)"


def _utc_ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _append_record_line_unlocked(path: str, line: str) -> None:
    """O_APPEND single-line write; caller must hold the exclusive log lock."""
    data = line.encode("utf-8")
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
    try:
        os.write(fd, data)
    finally:
        os.close(fd)


def _split_complete_lines(raw: bytes) -> list[str]:
    """Decode UTF-8 (replace) and return complete non-empty lines.

    Splits on ``\\n`` only (not ``str.splitlines()``) so Unicode linebreaks
    that somehow landed in a detail cannot fragment a record. A torn/partial
    last line (no trailing newline) is dropped so concurrent readers and
    compaction never treat an incomplete record as whole.
    """
    text = raw.decode("utf-8", errors="replace")
    if not text:
        return []
    if text.endswith("\n"):
        pieces = text.split("\n")
    else:
        # Drop the incomplete trailing fragment.
        pieces = text.split("\n")[:-1]
    return [ln for ln in pieces if ln.strip()]


def _source_of_line(line: str) -> str:
    """Extract the positional ``source=`` field (fields[1]); else unknown."""
    fields = line.split()
    if len(fields) > 1 and fields[1].startswith("source="):
        return fields[1][len("source=") :] or "unknown"
    return "unknown"


def _file_exceeds_compact_threshold(path: str) -> bool:
    """Cheap unlocked pre-check: line count or byte size over bound."""
    try:
        st = os.stat(path)
    except OSError:
        return False
    if st.st_size > _COMPACT_BYTE_THRESHOLD:
        return True
    try:
        with open(path, "rb") as fh:
            raw = fh.read()
    except OSError:
        return False
    return len(_split_complete_lines(raw)) > MAX_RECORDS


def _apply_fair_compaction(lines: list[str]) -> tuple[list[str], int]:
    """Keep newest N per source, then global cap. Return (kept, dropped_count)."""
    original = len(lines)
    # Walk newest-first; keep up to MAX_RECORDS_PER_SOURCE per source.
    per_source: dict[str, int] = {}
    fair_rev: list[str] = []
    for line in reversed(lines):
        src = _source_of_line(line)
        count = per_source.get(src, 0)
        if count >= MAX_RECORDS_PER_SOURCE:
            continue
        fair_rev.append(line)
        per_source[src] = count + 1
    fair = list(reversed(fair_rev))
    if len(fair) > MAX_RECORDS:
        fair = fair[-MAX_RECORDS:]
    dropped = original - len(fair)
    return fair, dropped


def _write_lines_atomic(path: str, lines: list[str]) -> None:
    """Write ``lines`` via temp + os.replace (caller holds exclusive lock)."""
    parent = os.path.dirname(path) or "."
    fd, tmp = tempfile.mkstemp(dir=parent, prefix=".hook-failures.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as out:
            if lines:
                out.write("\n".join(lines) + "\n")
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def _compact_log_under_lock(path: str) -> None:
    """Fair + global bound; write-temp + os.replace under flock."""
    lock_path = log_lock_path(path)
    try:
        lock_fd = os.open(lock_path, os.O_WRONLY | os.O_CREAT, 0o644)
    except OSError:
        return
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX)
        try:
            with open(path, "rb") as fh:
                raw = fh.read()
        except OSError:
            return
        lines = _split_complete_lines(raw)
        if len(lines) <= MAX_RECORDS:
            return
        kept, dropped = _apply_fair_compaction(lines)
        if dropped > 0:
            notice = (
                f"ts={_utc_ts()} source=unknown kind=exception "
                f"detail=compaction dropped {dropped} records"
            )
            kept = [notice] + kept
        _write_lines_atomic(path, kept)
    finally:
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
        except OSError:
            pass
        try:
            os.close(lock_fd)
        except OSError:
            pass


def _maybe_compact(path: str) -> None:
    """Run flock-guarded compaction only when the file exceeds the threshold."""
    if not _file_exceeds_compact_threshold(path):
        return
    _compact_log_under_lock(path)


def rewrite_hook_failures_under_lock(
    path: str,
    keep_line,
) -> None:
    """Read-modify-write the sink under the shared exclusive flock.

    ``keep_line(line) -> bool`` decides which complete non-empty lines survive.
    Used by implementation note's selective purge so it serializes with append/compaction.
    Never raises into the caller (best-effort).
    """
    try:
        if not path or not os.path.isfile(path):
            return
        lock_path = log_lock_path(path)
        try:
            lock_fd = os.open(lock_path, os.O_WRONLY | os.O_CREAT, 0o644)
        except OSError:
            return
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX)
            try:
                with open(path, "rb") as fh:
                    raw = fh.read()
            except OSError:
                return
            lines = _split_complete_lines(raw)
            kept = [ln for ln in lines if keep_line(ln)]
            _write_lines_atomic(path, kept)
        finally:
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
            except OSError:
                pass
            try:
                os.close(lock_fd)
            except OSError:
                pass
    except Exception:  # noqa: BLE001 -- purge/rewrite must never break the hook
        pass


def record_hook_failure(
    *,
    source: str,
    kind: str,
    detail: str,
    repo_root: str | None = None,
) -> None:
    """Append one failure record; bound the log; never raise.

    Parameters
    ----------
    source:
        Mandatory identity of the writer (``record-file-touch``,
        ``capture-agent-errors``, ``compact-session``, or ``post-merge-reap``).
    kind:
        ``returncode`` (subprocess non-zero) or ``exception`` (import/exec).
    detail:
        Single-line diagnostic (truncated; all linebreak chars sanitized).
    repo_root:
        Optional worktree root passed to git discovery so hooks with cwd
        outside the repo still resolve the shared sink path.
    """
    try:
        if source not in _SOURCE_ALLOWED:
            source = "unknown"
        if kind not in ("returncode", "exception"):
            kind = "exception"
        path = resolve_hook_failures_log_path(repo_root)
        if not path:
            return
        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        ts = _utc_ts()
        line = (
            f"ts={ts} source={source} kind={kind} detail={_sanitize_detail(detail)}\n"
        )
        # Append under the same exclusive flock compaction holds, so a record
        # cannot land between compaction's read and its os.replace (which would
        # destroy it). O_APPEND write stays inside the lock.
        lock_path = log_lock_path(path)
        try:
            lock_fd = os.open(lock_path, os.O_WRONLY | os.O_CREAT, 0o644)
        except OSError:
            return
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX)
            _append_record_line_unlocked(path, line)
        finally:
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
            except OSError:
                pass
            try:
                os.close(lock_fd)
            except OSError:
                pass
        _maybe_compact(path)
    except Exception:  # noqa: BLE001 -- sink is best-effort; never block the tool
        pass
