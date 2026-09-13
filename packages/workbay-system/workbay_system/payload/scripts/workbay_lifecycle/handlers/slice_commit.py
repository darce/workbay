"""Mutating ``slice-commit`` subcommand.

Stages the current diff and creates one git commit for the active
slice. After a successful commit the handler shells out to
``mcp-workbay-handoff set`` so the actor-resolved write context refreshes
``handoff_state.updated_commit_sha`` from the new worktree HEAD — without
that projection, downstream commit-guarded writes (``update_review_finding``
in particular) see the stale slice-N-1 sha and reject fixes that actually
landed on a descendant (internal).

The projection is best-effort: a non-zero return from the read or write
CLI flips ``handoff_projection`` to ``"pending"`` with a
``projection_warning`` field but never fails the slice-commit itself.

Before ``git add``/``commit``, the handler invokes
``sync-task-plan-checklist --apply --quiet`` (internal; ordering
fix from internal) so any box the sync flips lands inside the
slice commit instead of being left as an uncommitted edit. The slim
sync receipt is merged under the parent receipt's ``checklist_sync``
key. The sync runs against the just-recorded ``close_slice`` decision's
``changed_files`` (by convention the agent records ``close_slice``
before invoking ``make slice-commit``, so its decision row is already
in the DB). Ordinary sync failure surfaces as ``checklist_sync.ok = False``
with a ``warning`` field without failing the slice-commit itself — a
malformed plan must not block a real close. Lock contention and destination
changes fail closed before staging to avoid committing another writer's bytes.

Scoped commits validate existing changes before sync and dry-run the sync
first. A plan outside PATHS is left untouched with a typed skip receipt.
"""

from __future__ import annotations

import argparse
import ctypes
import errno
import fcntl
import hashlib
import json
import os
import re
import select
import shlex
import signal
import stat
import subprocess
import sys
import time
import uuid
from contextlib import ExitStack
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import resolver

from . import _common

# On-demand handoff renders (same set as handoff ``_IMPLICIT_DIRTY_ALLOWLIST``).
# Identity-field set_handoff_state flushes CURRENT_TASK.json so the hook fast
# path stays fresh; that write must not trip the untracked-files guard when the
# operator is committing real work [REVHOO-RED-03]. DASHBOARD.txt is the same
# class of DB-derived operator view. Filtering here is the production fix —
# gitignoring CURRENT_TASK.json only in a test fixture is not sufficient.
_ON_DEMAND_RENDER_UNTRACKED: frozenset[str] = frozenset({"CURRENT_TASK.json", "DASHBOARD.txt"})

_GIT_STDERR_LIMIT = 1000

# A fixture may need to commit the text of a merge marker deliberately.  The
# override is explicit and is reported on the successful receipt; no path is
# exempted implicitly.  Build the expression with quantifiers so this guard's
# own source does not contain a marker line that it would reject when staged.
CONFLICT_MARKER_ESCAPE_HATCH_ENV = "WORKBAY_ALLOW_CONFLICT_MARKERS"
_CONFLICT_MARKER_DEFAULT_SIZE = 7
_CONFLICT_MARKER_MIN_SIZE = 2


def _conflict_marker_line_re(repo: Path) -> re.Pattern[bytes]:
    """Build the marker matcher for this repository's configured marker width.

    WBLAND-H-02: Git writes markers whose width is ``merge.conflictMarkerSize``,
    which is configurable per repository and per attribute.  A matcher hardwired
    to seven characters misses every marker in a repository that widened them,
    and the guard then passes a genuinely conflicted blob.  The width floor is
    honoured, and the pattern stays open-ended above it, so a per-path attribute
    override wider than the config value is still caught.
    """
    configured = _git_stdout_text(repo, "config", "--get", "merge.conflictMarkerSize")
    try:
        size = int(configured)
    except (TypeError, ValueError):
        size = _CONFLICT_MARKER_DEFAULT_SIZE
    size = max(size, _CONFLICT_MARKER_MIN_SIZE)
    # Quantifiers keep this module's own source free of a literal marker line,
    # so the guard does not reject its own file when staged.
    return re.compile(rb"^(?:<{%d,} |>{%d,} )" % (size, size))


def _validated_paths(repo: Path, paths: list[str]) -> list[str] | None:
    """Preserve lexical git paths; refuse parent traversal and symlink ancestors."""
    repo_resolved = repo.resolve()
    normalized: list[str] = []
    for raw_path in paths:
        path = Path(raw_path)
        if not raw_path or path.is_absolute() or ".." in path.parts:
            return None
        try:
            # Git tracks the final symlink itself, never its target. Following
            # an ancestor, however, would select paths outside the lexical scope.
            if any((repo_resolved / parent).is_symlink() for parent in path.parents):
                return None
        except OSError:
            return None
        normalized.append(path.as_posix())
    return normalized


def _changes_outside_paths(changed: list[str], paths: list[str]) -> list[str]:
    prefixes = [path.rstrip("/") for path in paths]
    if "." in prefixes:
        return []
    return sorted(
        path for path in changed if not any(path == prefix or path.startswith(f"{prefix}/") for prefix in prefixes)
    )


def _run_git_bytes(argv: list[str]) -> subprocess.CompletedProcess[bytes]:
    try:
        return subprocess.run(argv, capture_output=True, timeout=30.0, check=False)
    except subprocess.TimeoutExpired:
        return subprocess.CompletedProcess(argv, 124, b"", b"timed out")
    except OSError as exc:
        return subprocess.CompletedProcess(argv, 127, b"", os.fsencode(str(exc)))


def _tracked_changes(repo: Path) -> tuple[list[str], str | None]:
    proc = _run_git_bytes(
        [
            "git",
            "-C",
            str(repo),
            "diff",
            "--no-renames",
            "--name-only",
            "-z",
            "--relative",
            "HEAD",
            "--",
        ]
    )
    if proc.returncode != 0:
        return [], os.fsdecode(proc.stderr)
    return [os.fsdecode(path) for path in proc.stdout.split(b"\0") if path], None


def _scoped_checklist_sync(
    repo: Path,
    task_ref: str,
    paths: list[str],
    *,
    commit_resources: ExitStack | None = None,
) -> dict[str, Any]:
    from . import sync_task_plan_checklist as sync

    # Resolve identity without reading plan bytes. A preview can fail while a
    # cooperating writer holds the lock and has only written partial UTF-8.
    stored_plan = sync._lookup_stored_plan_path(repo, task_ref)
    if stored_plan:
        plan = repo.resolve() / stored_plan
        try:
            relative = plan.relative_to(repo.resolve())
            if ".." in relative.parts:
                raise ValueError("plan contains parent traversal")
        except ValueError:
            return {"ok": True, "applied": False, "skipped": "plan_outside_paths"}
        if paths and _changes_outside_paths([relative.as_posix()], paths):
            return {"ok": True, "applied": False, "skipped": "plan_outside_paths"}
        return _apply_validated_checklist_sync(repo, task_ref, plan, commit_resources=commit_resources)
    preview = _common.run_checklist_sync(repo, task_ref, apply=False)
    if not preview.get("ok"):
        refusal = preview.get("error") or preview.get("warning")
        if refusal in {
            "lock_held",
            "lock_unresolvable",
            "plan_locked",
            "plan_destination_changed",
            "dest_moved_during_write",
            "plan_changed_during_write",
        }:
            return {**preview, "error": refusal}
        # Without an identity we cannot hold the writer's lock through Git.
        # Preview failure may be a partial write, not ordinary malformed input.
        return {**preview, "error": "lock_unresolvable"}
    if preview.get("skipped"):
        return preview
    plan_path = preview.get("plan_path")
    if not isinstance(plan_path, str) or not plan_path:
        return {**preview, "ok": False, "applied": False, "error": "lock_unresolvable"}
    try:
        relative = (repo.resolve() / plan_path).relative_to(repo.resolve())
        if ".." in relative.parts:
            raise ValueError("plan contains parent traversal")
    except (OSError, ValueError):
        return {**preview, "applied": False, "skipped": "plan_outside_paths"}
    if paths and _changes_outside_paths([relative.as_posix()], paths):
        return {**preview, "applied": False, "skipped": "plan_outside_paths"}
    return _apply_validated_checklist_sync(repo, task_ref, repo.resolve() / relative, commit_resources=commit_resources)


def _exchange_plan(parent: int, source: str, destination: str) -> None:
    """Atomically exchange entries, retaining the displaced inode for rollback.

    Refuse unsupported platforms/filesystems instead of falling back to a
    destructive replace. Linux RENAME_EXCHANGE and Darwin RENAME_SWAP are 2.
    """
    libc = ctypes.CDLL(None, use_errno=True)
    function = getattr(libc, "renameat2", None)
    if function is None:
        function = getattr(libc, "renameatx_np", None)
    if function is None:
        raise OSError(errno.ENOTSUP, "atomic plan exchange unavailable")
    function.argtypes = [
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_uint,
    ]
    function.restype = ctypes.c_int
    if function(parent, os.fsencode(source), parent, os.fsencode(destination), 2):
        code = ctypes.get_errno()
        raise OSError(code, os.strerror(code))


def _apply_validated_checklist_sync(
    repo: Path, task_ref: str, plan: Path, *, commit_resources: ExitStack | None = None
) -> dict[str, Any]:
    """Publish direct-sync outcomes using the canonical dashboard sidecar contract."""
    from . import sync_task_plan_checklist as sync

    receipt = _apply_bound_checklist_sync(repo, task_ref, plan, commit_resources=commit_resources)
    warning = receipt.get("warning") or receipt.get("error")
    if receipt.get("skipped") == "plan_not_found":
        warning = f"plan_not_found: {plan}"
    sync._write_sync_sidecar(
        sync._resolve_state_dir(sync.resolve_handoff_workspace_root(repo)),
        task_ref,
        {
            "ok": bool(receipt.get("ok")) and warning is None,
            "ticked": receipt.get("ticked", 0),
            "kept": receipt.get("kept", 0),
            "unresolved": receipt.get("unresolved", 0),
            "warning": warning,
            "plan_path": str(plan),
            "recorded_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        },
    )
    return receipt


def _apply_bound_checklist_sync(
    repo: Path, task_ref: str, plan: Path, *, commit_resources: ExitStack | None = None
) -> dict[str, Any]:
    """Bind the destination before evidence lookup and replace via its directory FD."""
    from . import sync_task_plan_checklist as sync

    locks = ExitStack()
    descriptors: list[int] = []
    temporary: str | None = None
    recovery_path: str | None = None
    refusal = "plan_destination_changed"
    identity = None
    ordinary_failure = None
    try:
        relative = plan.relative_to(repo.resolve())
        parent = os.open(repo.resolve(), os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        descriptors.append(parent)
        for part in relative.parts[:-1]:
            parent = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent)
            descriptors.append(parent)
        name = relative.name
        # Lock the directory, whose identity survives atomic plan replacement.
        # All scoped checklist writers use this lock from read through replace;
        # contention is a bounded refusal rather than an unbounded wait.
        locks.enter_context(sync.checklist_write_lock(parent))
        fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
        descriptors.append(fd)
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise ValueError("plan is not a regular file")
        fcntl.fcntl(fd, fcntl.F_SETFL, fcntl.fcntl(fd, fcntl.F_GETFL) & ~os.O_NONBLOCK)
        with os.fdopen(os.dup(fd), "rb") as stream:
            identity = os.fstat(stream.fileno())
            if not stat.S_ISREG(identity.st_mode):
                raise ValueError("plan is not a regular file")
            original = stream.read()
        original_hash = hashlib.sha256(original).digest()

        def validate_parents() -> None:
            current = os.stat(repo.resolve(), follow_symlinks=False)
            bound = os.fstat(descriptors[0])
            if (current.st_dev, current.st_ino) != (bound.st_dev, bound.st_ino):
                raise ValueError("plan parent changed")
            for index, part in enumerate(relative.parts[:-1]):
                current = os.stat(part, dir_fd=descriptors[index], follow_symlinks=False)
                bound = os.fstat(descriptors[index + 1])
                if (current.st_dev, current.st_ino) != (bound.st_dev, bound.st_ino):
                    raise ValueError("plan parent changed")

        def validate_content() -> None:
            # Read raw bytes: newline normalization must not hide an edit.
            with os.fdopen(os.dup(fd), "rb") as stream:
                stream.seek(0)
                if hashlib.sha256(stream.read()).digest() != original_hash:
                    raise ValueError("plan content changed")
            updated = os.fstat(fd)
            if (updated.st_mtime_ns, updated.st_ctime_ns, updated.st_size) != (
                identity.st_mtime_ns,
                identity.st_ctime_ns,
                identity.st_size,
            ):
                raise ValueError("plan content changed")

        try:
            text = original.decode("utf-8")
            evidence, projection, warning = sync._query_handoff_evidence(repo, task_ref)
            resolutions = sync.resolve(sync.parse(text), evidence)
            rewritten = sync.apply(text, resolutions)
        except (OSError, ValueError) as exc:
            # Malformed input is best-effort, but still validate the bound
            # bytes and retain the lock through Git before allowing a commit.
            ordinary_failure = {"ok": False, "applied": False, "warning": str(exc)}
            text = rewritten = None
        # Check both the directory chain and the final entry after the mutable
        # evidence lookup. Never follow a newly introduced symlink.
        validate_parents()
        current = os.stat(name, dir_fd=parent, follow_symlinks=False)
        if (current.st_dev, current.st_ino) != (identity.st_dev, identity.st_ino):
            raise ValueError("plan destination changed")
        validate_content()
        if rewritten != text:
            temporary = f".slice-plan-{uuid.uuid4().hex}"
            temporary_fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600, dir_fd=parent)
            with os.fdopen(temporary_fd, "w", encoding="utf-8") as stream:
                os.fchmod(stream.fileno(), stat.S_IMODE(identity.st_mode))
                stream.write(rewritten)
                stream.flush()
                os.fsync(stream.fileno())
                written = os.fstat(stream.fileno())
            current = os.stat(name, dir_fd=parent, follow_symlinks=False)
            if (current.st_dev, current.st_ino) != (identity.st_dev, identity.st_ino):
                raise ValueError("plan destination changed")
            validate_content()
            # fsync can yield to a directory rename. Recheck the entire chain
            # at the write boundary before using the bound parent descriptor.
            validate_parents()
            # Cooperating writers MUST hold checklist_write_lock from read to
            # completion. Exchange retains an uncooperative writer's inode and
            # bytes so a race at the syscall boundary can be rolled back.
            refusal = "plan_changed_during_write"
            validate_content()
            _exchange_plan(parent, temporary, name)
            try:
                refusal = "dest_moved_during_write"
                validate_parents()
                current = os.stat(name, dir_fd=parent, follow_symlinks=False)
                displaced = os.stat(temporary, dir_fd=parent, follow_symlinks=False)
                if (current.st_dev, current.st_ino) != (written.st_dev, written.st_ino):
                    raise ValueError("written destination moved")
                if (displaced.st_dev, displaced.st_ino) != (
                    identity.st_dev,
                    identity.st_ino,
                ):
                    raise ValueError("plan destination changed during exchange")
                refusal = "plan_changed_during_write"
                # Exchange itself updates ctime, so compare content and mtime
                # here rather than interpreting our own rename as an edit.
                with os.fdopen(os.dup(fd), "rb") as stream:
                    stream.seek(0)
                    if hashlib.sha256(stream.read()).digest() != original_hash:
                        raise ValueError("plan content changed during exchange")
                if os.fstat(fd).st_mtime_ns != identity.st_mtime_ns:
                    raise ValueError("plan content changed during exchange")
            except (OSError, ValueError):
                # If rollback fails, retain the displaced file for recovery.
                backup = temporary
                temporary = None
                recovery_path = str(plan.parent / backup)
                current = os.stat(name, dir_fd=parent, follow_symlinks=False)
                if (current.st_dev, current.st_ino) != (written.st_dev, written.st_ino):
                    raise ValueError(f"destination moved; original retained at {backup}")
                _exchange_plan(parent, backup, name)
                # The replacement may contain newer progress, including an
                # edit made during rollback itself. Never delete that inode.
                raise
        if commit_resources is not None:
            # Git's scoped commit rereads the worktree even after git add.
            # Cooperating writers must remain excluded through both operations.
            # Register descriptors first so the lock exits before they close.
            for descriptor in descriptors:
                commit_resources.callback(os.close, descriptor)
            commit_resources.enter_context(locks.pop_all())
            descriptors = []
        if ordinary_failure is not None:
            return ordinary_failure
        return {
            "ok": True,
            "applied": rewritten != text,
            "plan_path": str(plan),
            "plan_source": "flag",
            "handoff_projection": projection,
            **sync._classify_counts(resolutions),
            **({"warning": warning} if warning else {}),
        }
    except (OSError, ValueError) as exc:
        if isinstance(exc, FileNotFoundError) and identity is None:
            return {
                "ok": True,
                "applied": False,
                "skipped": "plan_not_found",
                "plan_path": str(plan),
            }
        return {
            "ok": False,
            "applied": False,
            "error": "plan_locked" if isinstance(exc, BlockingIOError) else refusal,
            "warning": str(exc),
            **({"recovery_path": recovery_path} if recovery_path else {}),
        }
    finally:
        if temporary is not None:
            os.unlink(temporary, dir_fd=parent)
        locks.close()
        for descriptor in reversed(descriptors):
            os.close(descriptor)


# TODO(internal G16): consolidate onto orchestration/git_lock.py
def _git_lock_held(stderr: str) -> bool:
    return (".lock" in stderr and "File exists" in stderr) or ("Another git process" in stderr)


def _report_git_stderr(stderr: str) -> str:
    excerpt = stderr.strip()[:_GIT_STDERR_LIMIT]
    if excerpt:
        sys.stderr.write(f"slice-commit: git stderr: {excerpt}\n")
    return excerpt


def _untracked_paths(repo: Path) -> list[str]:
    proc = _common.run_subprocess(["git", "-C", str(repo), "status", "--porcelain=v1", "-uall"])
    if proc.returncode != 0:
        return []
    paths: list[str] = []
    for line in proc.stdout.splitlines():
        if not line.startswith("?? "):
            continue
        path = line[3:]
        if path in _ON_DEMAND_RENDER_UNTRACKED:
            continue
        paths.append(path)
    return paths


def _git_stdout_text(repo: Path, *args: str) -> str:
    """Run a read-only git command and return trimmed stdout, or '' on failure."""
    proc = _run_git_bytes(["git", "-C", str(repo), *args])
    if proc.returncode != 0:
        return ""
    return proc.stdout.decode("utf-8", "replace").strip()


def _staged_blob_paths(repo: Path) -> tuple[list[str] | None, str | None]:
    """Return staged non-gitlink paths, or a typed probe error.

    ``--raw`` carries the destination mode, which ``--name-only`` does not.
    WBLAND-M-03: a staged submodule pointer has mode 160000 and no blob in the
    superproject, so ``git show :<path>`` answers ``bad object``.  Reading the
    mode lets the scan skip gitlinks instead of failing closed on a commit that
    was never conflicted.  ``--no-renames`` guarantees one path per record, so
    the -z stream is a flat (metadata, path) alternation.
    """
    listed = _run_git_bytes(
        [
            "git",
            "-C",
            str(repo),
            "diff",
            "--cached",
            "--no-renames",
            "--raw",
            "-z",
            "--diff-filter=ACMRT",
            "--",
        ]
    )
    if listed.returncode != 0:
        detail = listed.stderr.decode("utf-8", "replace").strip()
        return None, detail or "git staged path scan failed"

    fields = [field for field in listed.stdout.split(b"\0") if field]
    paths: list[str] = []
    for index in range(0, len(fields) - 1, 2):
        meta = fields[index]
        if not meta.startswith(b":"):
            # An unexpected stream shape is an unavailable probe, not a clean
            # scan; refusing here keeps the guard fail-closed.
            return None, "git staged path scan returned an unrecognised record"
        parts = meta[1:].split()
        if len(parts) < 2:
            return None, "git staged path scan returned an unrecognised record"
        dst_mode = parts[1]
        if dst_mode == b"160000":
            continue
        paths.append(os.fsdecode(fields[index + 1]))
    return paths, None


def _staged_conflict_markers(
    repo: Path,
) -> tuple[dict[str, list[int]] | None, str | None]:
    """Return staged paths containing unresolved merge-marker lines.

    The scan reads the index blobs, rather than the worktree, because the
    commit operation is the boundary being protected.  A failed index/blob
    probe is distinct from a clean scan so an unavailable probe cannot be
    mistaken for evidence that the staged set is safe.
    """
    # ``_run`` normally obtains *repo* from ``repo_root`` and therefore never
    # reaches this arm.  Keep lightweight unit-test doubles that provide a
    # synthetic repo root from tripping the production Git probe.
    if not (repo / ".git").exists():
        return {}, None
    staged_paths, scan_error = _staged_blob_paths(repo)
    if staged_paths is None:
        return None, scan_error

    marker_re = _conflict_marker_line_re(repo)
    markers: dict[str, list[int]] = {}
    for rel_path in staged_paths:
        blob = _run_git_bytes(["git", "-C", str(repo), "show", f":{rel_path}"])
        if blob.returncode != 0:
            detail = blob.stderr.decode("utf-8", "replace").strip() or "git staged blob read failed"
            return None, f"{rel_path}: {detail}"
        lines = [
            line_number for line_number, line in enumerate(blob.stdout.splitlines(), start=1) if marker_re.match(line)
        ]
        if lines:
            markers[rel_path] = lines
    return markers, None


def _format_conflict_marker_detail(markers: dict[str, list[int]]) -> str:
    locations = ", ".join(f"{path}:{line}" for path, lines in markers.items() for line in lines)
    return (
        f"unresolved conflict markers staged at {locations}; remove them or set "
        f"{CONFLICT_MARKER_ESCAPE_HATCH_ENV}=1 only for a deliberate fixture"
    )


def _project_commit_sha(repo: Path, task_ref: str) -> tuple[str, str | None]:
    """Project the new HEAD commit_sha into the active row.

    Returns ``(projection_status, warning_or_None)`` where
    ``projection_status`` is ``"synced"`` on success or ``"pending"`` on
    any failure. Best-effort: never raises, never fails the parent
    command. Issues exactly two CLI calls: an identity read to capture
    the row's current revision (for the optimistic-concurrency guard),
    and a single ``set --commit-sha <head> --branch <branch>`` call
    that drives the row's ``updated_commit_sha`` / ``updated_branch``
    directly via the explicit-actor channel introduced in internal
    implementation note — bypassing the resolver's stored-row task_git fallback that
    used to require a calibrate-then-project two-call dance.
    """
    read_argv = _common.handoff_command_argv(repo, "state", "--sections", "identity", task_ref)
    read_proc = _common.run_handoff_subprocess(repo, read_argv)
    if read_proc.returncode != 0:
        return "pending", (
            f"projection_read_failed: rc={read_proc.returncode} stderr={read_proc.stderr.strip()[:200]!r}"
        )
    try:
        envelope = json.loads(read_proc.stdout)
    except json.JSONDecodeError as exc:
        return "pending", f"projection_read_unparseable: {exc!s}"
    active = (envelope.get("data") or {}).get("active") or {}
    revision = active.get("revision")
    if not isinstance(revision, int):
        return "pending", "projection_revision_missing"
    head = resolver.head_sha(repo)
    branch = resolver.current_branch(repo)
    if not head or not branch:
        return "pending", "projection_git_context_unavailable"
    set_argv = _common.handoff_command_argv(
        repo,
        "set",
        "--task-ref",
        task_ref,
        "--expected-revision",
        str(revision),
        "--commit-sha",
        head,
        "--branch",
        branch,
    )
    set_proc = _common.run_handoff_subprocess(repo, set_argv)
    if set_proc.returncode != 0:
        return "pending", (
            f"projection_write_failed: rc={set_proc.returncode} stderr={set_proc.stderr.strip()[:200]!r}"
        )
    return "synced", None


def _derive_and_backfill_changed_files(repo: Path, task_ref: str, commit_sha: str) -> dict[str, Any]:
    """Primary S1 locus: derive paths from the slice commit and patch the decision row."""

    try:
        from workbay_handoff_mcp import configure_runtime
        from workbay_handoff_mcp.changed_files_derivation import (
            derive_changed_files_from_commit,
        )
        from workbay_handoff_mcp.config import RuntimeConfig
        from workbay_handoff_mcp.decisions import backfill_latest_slice_changed_files
        from workbay_handoff_mcp.shared_schema import _get_db_connection
    except ImportError as exc:
        return {"ok": False, "warning": f"handoff_import_failed:{exc}"}

    workspace = resolver.canonical_workspace_root(repo) or repo
    state_dir = workspace / ".task-state"
    runtime = RuntimeConfig.for_workspace(
        workspace,
        state_dir=state_dir,
        current_task_path=workspace / "CURRENT_TASK.json",
        dashboard_path=workspace / "DASHBOARD.txt",
        current_task_auto_regen=False,
    )
    configure_runtime(runtime)
    result = derive_changed_files_from_commit(workspace, commit_sha)
    paths = list(result.paths)
    warning = result.warning
    with _get_db_connection() as conn:
        ok, err = backfill_latest_slice_changed_files(
            conn,
            task_ref=task_ref,
            commit_sha=commit_sha,
            changed_files=paths,
        )
        if ok:
            conn.commit()
        else:
            conn.rollback()
    if not ok:
        return {
            "ok": False,
            "warning": err or "backfill_failed",
            "derived_count": len(paths),
        }
    payload: dict[str, Any] = {"ok": True, "derived_count": len(paths), "paths": paths}
    if warning:
        payload["warning"] = warning
    return payload


def _emit_error(
    reason: str,
    *,
    task_ref: str | None = None,
    branch: str = "",
    head: str = "",
    worktree_path: str = "",
    msg: str = "",
    dirty_summary: dict[str, int] | None = None,
    untracked_paths: list[str] | None = None,
    included_untracked: bool = False,
    outside_paths: list[str] | None = None,
    git_stderr: str = "",
    return_code: int = 2,
    status: str | None = None,
    checklist_sync: dict[str, Any] | None = None,
    error_detail: str | None = None,
    conflict_markers: dict[str, list[int]] | None = None,
) -> int:
    receipt: dict[str, Any] = {
        "ok": False,
        "command": "slice-commit",
        "task_ref": task_ref,
        "branch": branch,
        "worktree_path": worktree_path,
        "head": head,
        "handoff_projection": "pending",
        "events": [],
        "commit_message": msg,
        "commit_sha": "",
        "previous_head": head,
        "dirty_summary": dirty_summary or {"staged": 0, "unstaged": 0, "untracked": 0, "total": 0},
        "untracked_paths": untracked_paths or [],
        "included_untracked": included_untracked,
        "error": reason,
    }
    if outside_paths is not None:
        receipt["outside_paths"] = outside_paths
    if checklist_sync is not None:
        receipt["checklist_sync"] = checklist_sync
    if error_detail is not None:
        receipt["error_detail"] = error_detail
    if conflict_markers is not None:
        receipt["conflict_markers"] = conflict_markers
    if git_stderr:
        receipt["git_stderr"] = git_stderr
    if status is not None:
        sys.stdout.write(f"SLICE_COMMIT_STATUS={status}\n")
    _common.emit(receipt)
    return return_code


def _bounded_checkout_read(deadline: float, read: Callable[[], Any]) -> Any:
    """Bound the synchronous registry API, including DB lock waits.

    The lane API has no timeout parameter. A read-only fork is killed and
    reaped on expiry; no background reader survives a failed guard.
    """
    if time.monotonic() >= deadline:
        raise TimeoutError("ownership read budget exhausted")
    reader, writer = os.pipe()
    try:
        pid = os.fork()
    except BaseException:
        os.close(reader)
        os.close(writer)
        raise
    if pid == 0:
        os.setsid()
        os.close(reader)
        try:
            payload = json.dumps(read()).encode()
            while payload:
                payload = payload[os.write(writer, payload) :]
        finally:
            os._exit(0)
    os.close(writer)
    try:
        chunks = []
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0 or not select.select([reader], [], [], remaining)[0]:
                raise TimeoutError("ownership read budget exhausted")
            chunk = os.read(reader, 65536)
            if not chunk:
                return json.loads(b"".join(chunks))
            chunks.append(chunk)
    finally:
        os.close(reader)
        # Reap a completed child before signaling: some platforms reject
        # signaling its vanished group with EPERM rather than ESRCH.
        reaped, _ = os.waitpid(pid, os.WNOHANG)
        if not reaped:
            try:
                os.killpg(pid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                # The child may not have created its session yet, or may
                # have exited since waitpid. Its unreaped PID remains ours.
                try:
                    os.kill(pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                except PermissionError:
                    # A signal/exit race is harmless only if we can reap it.
                    reaped, _ = os.waitpid(pid, os.WNOHANG)
                    if not reaped:
                        raise
            if not reaped:
                os.waitpid(pid, 0)


def _bounded_lane_page(deadline: float, **kwargs: Any) -> dict[str, Any]:
    from workbay_handoff_mcp import lanes_api

    return _bounded_checkout_read(deadline, lambda: lanes_api.list_lanes(**kwargs))


def _task_checkout_guard(repo: Path, task_ref: str) -> dict[str, Any] | None:
    """Require complete ownership evidence before any slice mutation."""
    deadline = time.monotonic() + _common.handoff_read_timeout()
    candidates: set[str] = set()
    owners: set[str] = set()

    def refusal(reason: str) -> dict[str, Any]:
        return {
            "ok": False,
            "command": "slice-commit",
            "error": reason,
            "task_ref": task_ref,
            "worktree_path": str(repo.resolve()),
            "registered_owners": sorted(owners),
            "candidate_checkout_paths": sorted(candidates)[:20],
            "error_detail": "Rerun from a registered checkout for the requested task; "
            "repair unavailable or conflicting ownership records first.",
            "events": [],
        }

    try:
        argv = _common.handoff_command_argv(repo, "state", "--sections", "identity", task_ref)
        proc = _common.run_handoff_subprocess(repo, argv, timeout=max(0.001, deadline - time.monotonic()))
        if proc.returncode != 0:
            raise ValueError("identity lookup failed")
        envelope = json.loads(proc.stdout)
        active = envelope["data"]["active"]
        if envelope.get("ok") is not True or active.get("task_ref") != task_ref:
            raise ValueError("invalid identity")
        target = active.get("target_worktree_path")
        if target:
            candidates.add(str(Path(target).resolve()))

        from workbay_handoff_mcp import configure_runtime
        from workbay_handoff_mcp.config import RuntimeConfig

        workspace = _bounded_checkout_read(deadline, lambda: str(resolver.canonical_workspace_root(repo) or ""))
        if not workspace:
            raise ValueError("canonical workspace unavailable")
        configure_runtime(RuntimeConfig.for_repo(Path(workspace)))

        def pages(**kwargs: Any) -> list[dict[str, Any]]:
            rows: list[dict[str, Any]] = []
            total = None
            seen: set[int] = set()
            cursor = None
            for _ in range(100):
                result = _bounded_lane_page(deadline, **kwargs, status="all", limit=100, after_id=cursor)
                if result.get("ok") is not True:
                    raise ValueError("lane lookup failed")
                data = result["data"]
                page = data["lanes"]
                count = data["total_matching"]
                more = data["has_more"]
                if (
                    not isinstance(page, list)
                    or type(count) is not int
                    or type(more) is not bool
                    or data.get("returned") != len(page)
                    or (total is not None and total != count)
                ):
                    raise ValueError("incomplete lane lookup")
                for row in page:
                    row_id = row.get("id") if isinstance(row, dict) else None
                    if (
                        type(row_id) is not int
                        or row_id <= 0
                        or row_id in seen
                        or (cursor is not None and row_id >= cursor)
                    ):
                        raise ValueError("duplicate or malformed lane id")
                    seen.add(row_id)
                    cursor = row_id
                total = count
                rows.extend(page)
                if not more:
                    if len(rows) != total:
                        raise ValueError("partial lane lookup")
                    return rows
                if not page or len(rows) >= total:
                    raise ValueError("invalid lane pagination")
            raise ValueError("lane lookup exceeded page budget")

        requested = pages(task_ref=task_ref)
        all_rows = pages(all_tasks=True)
        # Count equality alone is not a snapshot: require the task-specific
        # observation to agree with the complete registry observation as well.
        if {r["id"]: r for r in requested} != {r["id"]: r for r in all_rows if r.get("task_ref") == task_ref}:
            raise ValueError("inconsistent ownership observations")
        if time.monotonic() >= deadline:
            raise TimeoutError("ownership read budget exhausted")
        live = {"planned", "active", "blocked", "review"}
        terminal = {"closed", "merged", "closed_stale"}
        checkout = _bounded_checkout_read(deadline, lambda: str(resolver.current_worktree(repo) or ""))
        registered = _bounded_checkout_read(
            deadline, lambda: [str(Path(row["path"]).resolve()) for row in resolver.linked_worktrees(repo)]
        )
        if not checkout or str(Path(checkout).resolve()) not in registered:
            raise ValueError("checkout registry unavailable")
        cwd = str(Path(checkout).resolve())
        for row in requested + all_rows:
            if row.get("status") not in live | terminal:
                raise ValueError("unknown lane status")
            if row["status"] in terminal:
                continue
            path = str(Path(row["worktree_path"]).resolve())
            owner = row["task_ref"]
            if not owner:
                raise ValueError("missing owner")
            if owner == task_ref:
                candidates.add(path)
            if path == cwd:
                owners.add(owner)
        if cwd in candidates:
            owners.add(task_ref)
        if len(owners) > 1:
            return refusal("task_checkout_ambiguous")
        if cwd not in candidates:
            return refusal("task_checkout_mismatch")
        return None
    except Exception:
        # Failed/partial observations never establish absence of another owner.
        return refusal("task_checkout_lookup_failed")


def run(argv: list[str]) -> int:
    # Release on every return and exception, including staging/commit failures.
    with ExitStack() as commit_resources:
        return _run(argv, commit_resources)


def _run(argv: list[str], commit_resources: ExitStack) -> int:
    parser = argparse.ArgumentParser(prog="lifecycle slice-commit", add_help=True)
    parser.add_argument("--task", dest="task", default="")
    parser.add_argument("--msg", dest="msg", default="")
    parser.add_argument("--paths", dest="paths", default=None)
    parser.add_argument("--allow-unscoped", action="store_true", default=False)
    parser.add_argument(
        "--include-untracked",
        dest="include_untracked",
        action="store_true",
        default=False,
    )
    parser.add_argument("--json", dest="emit_json", action="store_true", default=False)
    args = parser.parse_args(argv)

    raw_paths = args.paths if args.paths is not None else os.environ.get("PATHS", "")
    allow_unscoped = args.allow_unscoped or os.environ.get("ALLOW_UNSCOPED") == "1"
    try:
        requested_paths = shlex.split(raw_paths) if raw_paths.strip() else []
    except ValueError:
        return _emit_error("paths_invalid")

    msg = (args.msg or "").strip()
    if not msg:
        return _emit_error("msg_required", msg=msg, included_untracked=args.include_untracked)

    repo = _common.repo_root()
    if repo is None:
        return _emit_error("not_in_git_repo", msg=msg, included_untracked=args.include_untracked)

    paths = _validated_paths(repo, requested_paths)
    if paths is None:
        return _emit_error(
            "paths_outside_worktree",
            worktree_path=str(repo),
            msg=msg,
            included_untracked=args.include_untracked,
        )

    git_facts = _common.gather_git_facts(repo)
    untracked_paths = _untracked_paths(repo)
    task_ref = (args.task or "").strip().upper() or git_facts.derived_task_ref
    if not task_ref:
        return _emit_error(
            "task_ref_required",
            branch=git_facts.branch,
            head=git_facts.head,
            worktree_path=str(repo),
            msg=msg,
            dirty_summary=git_facts.dirty_summary,
            untracked_paths=untracked_paths,
            included_untracked=args.include_untracked,
        )

    if (args.task or "").strip():
        # Content already in the index can be refused without authorizing any
        # mutation. Keep checkout validation ahead of checklist writes/staging,
        # and scan again after staging to cover newly added worktree content.
        markers, scan_error = _staged_conflict_markers(repo)
        if scan_error is not None or (
            markers and os.environ.get(CONFLICT_MARKER_ESCAPE_HATCH_ENV, "").strip() != "1"
        ):
            return _emit_error(
                "conflict_marker_scan_failed" if scan_error is not None else "conflict_markers_staged",
                task_ref=task_ref,
                branch=git_facts.branch,
                head=git_facts.head,
                worktree_path=str(repo),
                msg=msg,
                dirty_summary=git_facts.dirty_summary,
                untracked_paths=untracked_paths,
                included_untracked=args.include_untracked,
                **(
                    {"git_stderr": scan_error}
                    if scan_error is not None
                    else {"error_detail": _format_conflict_marker_detail(markers), "conflict_markers": markers}
                ),
            )
        refusal = _task_checkout_guard(repo, task_ref)
        if refusal is not None:
            _common.emit(refusal)
            return 2

    if git_facts.dirty_summary.get("total", 0) == 0:
        return _emit_error(
            "nothing_to_commit",
            task_ref=task_ref,
            branch=git_facts.branch,
            head=git_facts.head,
            worktree_path=str(repo),
            msg=msg,
            dirty_summary=git_facts.dirty_summary,
            untracked_paths=untracked_paths,
            included_untracked=args.include_untracked,
        )

    if untracked_paths and not args.include_untracked:
        return _emit_error(
            "untracked_files_present",
            task_ref=task_ref,
            branch=git_facts.branch,
            head=git_facts.head,
            worktree_path=str(repo),
            msg=msg,
            dirty_summary=git_facts.dirty_summary,
            untracked_paths=untracked_paths,
            included_untracked=False,
        )

    tracked_changes, tracked_error = _tracked_changes(repo)
    if tracked_error is not None:
        excerpt = _report_git_stderr(tracked_error)
        locked = _git_lock_held(tracked_error)
        return _emit_error(
            "git_index_lock_held" if locked else "git_status_failed",
            task_ref=task_ref,
            branch=git_facts.branch,
            head=git_facts.head,
            worktree_path=str(repo),
            msg=msg,
            dirty_summary=git_facts.dirty_summary,
            untracked_paths=untracked_paths,
            included_untracked=args.include_untracked,
            git_stderr=excerpt,
            return_code=75 if locked else 2,
            status="git_index_lock_held" if locked and not args.emit_json else None,
        )
    outside_paths = _changes_outside_paths(tracked_changes, paths) if paths else []
    if outside_paths and not allow_unscoped:
        return _emit_error(
            "unstaged_changes_outside_paths",
            task_ref=task_ref,
            branch=git_facts.branch,
            head=git_facts.head,
            worktree_path=str(repo),
            msg=msg,
            dirty_summary=git_facts.dirty_summary,
            untracked_paths=untracked_paths,
            included_untracked=args.include_untracked,
            outside_paths=outside_paths,
        )

    checklist_sync = _scoped_checklist_sync(repo, task_ref, paths, commit_resources=commit_resources)
    sync_error = checklist_sync.get("error")
    if sync_error in {
        "lock_held",
        "lock_unresolvable",
        "plan_destination_changed",
        "plan_locked",
        "dest_moved_during_write",
        "plan_changed_during_write",
    }:
        return _emit_error(
            sync_error,
            task_ref=task_ref,
            branch=git_facts.branch,
            head=git_facts.head,
            worktree_path=str(repo),
            msg=msg,
            dirty_summary=git_facts.dirty_summary,
            untracked_paths=untracked_paths,
            included_untracked=args.include_untracked,
            return_code=75 if sync_error in {"plan_locked", "lock_held"} else 2,
            checklist_sync=checklist_sync,
            status=sync_error if sync_error in {"plan_locked", "lock_held"} and not args.emit_json else None,
        )

    if paths:
        # PATHS names concrete lane-owned files/directories, not git pathspecs.
        # Literal mode prevents glob/pathspec magic from widening the write set.
        add_argv = [
            "git",
            "--literal-pathspecs",
            "-C",
            str(repo),
            "add",
            "--",
            *paths,
        ]
    else:
        swept = tracked_changes + (untracked_paths if args.include_untracked else [])
        sys.stderr.write(
            "WARNING slice-commit: unscoped staging sweeps: " + (" ".join(swept) if swept else "(none)") + "\n"
        )
        add_argv = [
            "git",
            "-C",
            str(repo),
            "add",
            "-A" if args.include_untracked else "-u",
        ]
    add_proc = _common.run_subprocess(add_argv)
    if add_proc.returncode != 0:
        excerpt = _report_git_stderr(add_proc.stderr)
        locked = _git_lock_held(add_proc.stderr)
        return _emit_error(
            "git_index_lock_held" if locked else "git_add_failed",
            task_ref=task_ref,
            branch=git_facts.branch,
            head=git_facts.head,
            worktree_path=str(repo),
            msg=msg,
            dirty_summary=git_facts.dirty_summary,
            untracked_paths=untracked_paths,
            included_untracked=args.include_untracked,
            git_stderr=excerpt,
            return_code=75 if locked else 2,
            status="git_index_lock_held" if locked and not args.emit_json else None,
        )

    staged_conflict_markers, marker_scan_error = _staged_conflict_markers(repo)
    if marker_scan_error is not None:
        return _emit_error(
            "conflict_marker_scan_failed",
            task_ref=task_ref,
            branch=git_facts.branch,
            head=git_facts.head,
            worktree_path=str(repo),
            msg=msg,
            dirty_summary=git_facts.dirty_summary,
            untracked_paths=untracked_paths,
            included_untracked=args.include_untracked,
            git_stderr=marker_scan_error,
        )
    conflict_markers = staged_conflict_markers or {}
    conflict_markers_escape_hatch: str | None = None
    if conflict_markers:
        marker_detail = _format_conflict_marker_detail(conflict_markers)
        if os.environ.get(CONFLICT_MARKER_ESCAPE_HATCH_ENV, "").strip() != "1":
            return _emit_error(
                "conflict_markers_staged",
                task_ref=task_ref,
                branch=git_facts.branch,
                head=git_facts.head,
                worktree_path=str(repo),
                msg=msg,
                dirty_summary=git_facts.dirty_summary,
                untracked_paths=untracked_paths,
                included_untracked=args.include_untracked,
                error_detail=marker_detail,
                conflict_markers=conflict_markers,
            )
        conflict_markers_escape_hatch = CONFLICT_MARKER_ESCAPE_HATCH_ENV

    commit_argv = ["git", "-C", str(repo), "commit", "-m", msg]
    if paths:
        commit_argv.insert(1, "--literal-pathspecs")
        commit_argv.extend(["--", *paths])
    commit_proc = _common.run_subprocess(commit_argv)
    if commit_proc.returncode != 0:
        excerpt = _report_git_stderr(commit_proc.stderr)
        locked = _git_lock_held(commit_proc.stderr)
        return _emit_error(
            "git_index_lock_held" if locked else "git_commit_failed",
            task_ref=task_ref,
            branch=git_facts.branch,
            head=git_facts.head,
            worktree_path=str(repo),
            msg=msg,
            dirty_summary=git_facts.dirty_summary,
            untracked_paths=untracked_paths,
            included_untracked=args.include_untracked,
            git_stderr=excerpt,
            return_code=75 if locked else 2,
            status="git_index_lock_held" if locked and not args.emit_json else None,
        )

    commit_sha = resolver.head_sha(repo) or ""
    changed_files_backfill = _derive_and_backfill_changed_files(repo, task_ref, commit_sha)
    projection_status, projection_warning = _project_commit_sha(repo, task_ref)
    stage_event = "staged_paths" if paths else ("staged_all" if args.include_untracked else "staged_tracked")
    events = [stage_event, "commit_created"]
    if projection_status == "synced":
        events.append("commit_sha_projected")
    if checklist_sync.get("ok") and checklist_sync.get("ticked", 0):
        events.append("checklist_sync_applied")
    if conflict_markers_escape_hatch is not None:
        events.append("conflict_markers_escape_hatch")
    receipt = {
        "ok": True,
        "command": "slice-commit",
        "task_ref": task_ref,
        "branch": git_facts.branch,
        "worktree_path": str(repo),
        "head": commit_sha,
        "handoff_projection": projection_status,
        "events": events,
        "commit_message": msg,
        "commit_sha": commit_sha,
        "previous_head": git_facts.head,
        "dirty_summary": git_facts.dirty_summary,
        "untracked_paths": untracked_paths,
        "included_untracked": args.include_untracked,
        "paths": paths,
        "checklist_sync": checklist_sync,
        "changed_files_backfill": changed_files_backfill,
    }
    if conflict_markers_escape_hatch is not None:
        receipt["conflict_markers"] = conflict_markers
        receipt["conflict_markers_escape_hatch"] = conflict_markers_escape_hatch
    if projection_warning is not None:
        receipt["projection_warning"] = projection_warning

    if not args.emit_json:
        sync_summary = f"sync={'ok' if checklist_sync.get('ok') else 'warn'} ticked={checklist_sync.get('ticked', 0)}"
        sys.stderr.write(
            f"slice-commit: task_ref={task_ref} branch={git_facts.branch} "
            f"previous_head={git_facts.head[:12]} commit_sha={commit_sha[:12]} "
            f"msg={shlex.quote(msg)} projection={projection_status} "
            f"{sync_summary}\n"
        )
        if projection_warning is not None:
            sys.stderr.write(f"slice-commit: projection_warning: {projection_warning}\n")

    _common.emit(receipt)
    return 0
