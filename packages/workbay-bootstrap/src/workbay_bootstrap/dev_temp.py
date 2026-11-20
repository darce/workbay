"""Steady-state reclaimer for sticky dev trees (implementation note S4).

Explicit operator maintenance retains the historical two-root age scan.
Automatic lifecycle calls use ``scope="lifecycle"``: only private repo scratch
with existing native ownership/release evidence is eligible, never ambient
``/tmp``, TMPDIR, or WORKBAY_DEV_TEMP_ROOT. Age is a scheduling threshold,
not lifecycle deletion authority. No ownership or release records are created.

Never raises ([RES-07]/[AGT-10]): permission errors and missing roots degrade
to empty reports.
"""

from __future__ import annotations

import fcntl
import os
import shutil
import socket
import stat
import time
from contextlib import ExitStack
from pathlib import Path
from typing import Any

DEFAULT_MAX_AGE_H = 24.0
DEFAULT_TMP_ROOT = Path("/tmp")
DEV_TEMP_GLOB = "workbay-*"
# Repo-relative second root. Everything directly under it is a candidate: the
# entries are task refs, not a ``workbay-*`` naming convention.
SCRATCH_RELATIVE = (".workbay", "scratch")
SCRATCH_GLOB = "*"
# Optional override for tests / operators (do not touch real /tmp in fixtures).
_TMP_ROOT_ENV = "WORKBAY_DEV_TEMP_ROOT"


def _resolve_scratch_root(repo: "Path | str | None") -> "Path | None":
    """Repo-local scratch root, or None when no repo was supplied."""
    if repo is None:
        return None
    text = str(repo).strip()
    if not text:
        return None
    return Path(text).joinpath(*SCRATCH_RELATIVE)


def _scan_root(
    root: Path,
    glob: str,
    *,
    cutoff: float,
    apply: bool,
    stale: "list[str]",
    removed: "list[str]",
    fresh: "list[str]",
    errors: "list[str]",
    held: list[dict[str, str]] | None = None,
    root_fd: int | None = None,
) -> None:
    """Age out ``glob`` children of ``root``, recording into the caller's lists.

    Fail-closed: a candidate must be a real directory that is a *direct child of
    the real root*, so a symlink planted in the root cannot redirect a deletion
    outside it, and the root itself is never a candidate. Degrades per path and
    per root; never raises.
    """
    try:
        if not root.is_dir():
            return
        root_real = root.resolve()
        try:
            candidates = (
                sorted(root / name for name in os.listdir(root_fd)) if root_fd is not None else sorted(root.glob(glob))
            )
        except OSError as exc:
            errors.append(f"{root}: {exc}")
            return
        for path in candidates:
            try:
                # Directories only -- never follow/unlink arbitrary files.
                if path.is_symlink() or not path.is_dir():
                    continue
                try:
                    real = path.resolve()
                except OSError as exc:
                    errors.append(f"{path}: {exc}")
                    continue
                if real == root_real or real.parent != root_real:
                    errors.append(f"{path}: refused, not a direct child of {root_real}")
                    continue
                try:
                    mtime = path.stat().st_mtime
                except OSError as exc:
                    errors.append(f"{path}: {exc}")
                    continue
                if mtime >= cutoff:
                    fresh.append(str(path))
                    continue
                # Selection precedes effect; names and age convey no authority.
                with ExitStack() as ownership:
                    if root_fd is not None:
                        reason = _released_scratch_hold(path, root, root_fd, ownership)
                        if reason:
                            assert held is not None
                            held.append({"path": str(path), "reason": reason})
                            continue
                    stale.append(str(path))
                    if not apply:
                        continue
                    try:
                        if root_fd is None:
                            shutil.rmtree(path)
                        else:
                            # fd-relative rmtree pins the root across effect.
                            shutil.rmtree(path.name, dir_fd=root_fd)
                        removed.append(str(path))
                    except OSError as exc:
                        errors.append(f"{path}: {exc}")
            except Exception as exc:  # noqa: BLE001 -- per-path degrade
                errors.append(f"{path}: {exc}")
    except Exception as exc:  # noqa: BLE001 -- per-root degrade, never raise
        errors.append(f"{root}: {exc}")


def _private_scratch_fd(repo: Path | str, stack: ExitStack) -> tuple[Path, int]:
    """Open the fixed hierarchy without following links or trusting env roots."""
    base = Path(repo).absolute()
    if base != base.resolve(strict=True):
        raise ValueError("lifecycle repo has a symlink ancestor")
    fd = os.open(base, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    stack.callback(os.close, fd)
    for name in (None, *SCRATCH_RELATIVE):
        if name is not None:
            fd = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            stack.callback(os.close, fd)
        info = os.fstat(fd)
        if info.st_uid != os.geteuid() or info.st_mode & 0o022:
            raise ValueError("lifecycle scratch hierarchy is foreign or writable by others")
    if info.st_mode & 0o077:
        raise ValueError("lifecycle scratch root must be private (0700)")
    return base.joinpath(*SCRATCH_RELATIVE), fd


def _released_scratch_hold(path: Path, root: Path, root_fd: int, stack: ExitStack) -> str | None:
    """Consume the existing VM adapter's records and observations, never mint them.

    Only scratch-class records in this private root are admitted. The adapter
    holds active units, locks, cwd users, native lenders and unknown observations.
    Its service probes are read-only. This is not the privileged late-consumer
    fence and does not extend authority to VM jobs, archives or provider roots.
    """
    from workbay_system.payload.scripts.vm import workbay_vm_reclaim as owner

    before = path.lstat()
    if before.st_uid != os.geteuid() or before.st_mode & 0o077:
        return "foreign_or_nonprivate_scratch"
    for name in ("RETENTION.json", "RELEASE.json"):
        try:
            record = (path / name).lstat()
        except FileNotFoundError:
            return "ownership_or_release_missing"
        if not stat.S_ISREG(record.st_mode) or record.st_uid != os.geteuid() or record.st_mode & 0o022:
            return "unsafe_ownership_record"
    retention = owner.manifest(path / "RETENTION.json")
    released = owner.valid_release(path, retention)
    if (
        retention.get("schema") != 1
        or retention.get("retention_class") != "scratch"
        or retention.get("host") != socket.gethostname()
        or released is None
        or released.get("host") != socket.gethostname()
    ):
        return "ownership_or_release_unknown"
    if not (
        owner.released_time(retention["created_at"]) <= owner.released_time(released["released_at"]) <= time.time()
    ):
        return "ambiguous_release_generation"
    if any(os.path.lexists(path / name) for name in (".git", *owner.SOURCE_PATHS, *owner.RUNTIME_PATHS)):
        return "source_or_provider_custody"
    row = owner.classify(path, root, read_only=True, deadline=time.monotonic() + 5)
    if row["verdict"] != "reclaimable":
        return row["reason"]
    # Acquire the existing producer lock after its read-only observation, and
    # retain it through effect. Never create a lock or an ownership record.
    try:
        lock = os.open(path / "job.lock", os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except FileNotFoundError:
        pass
    else:
        stack.callback(os.close, lock)
        if not stat.S_ISREG(os.fstat(lock).st_mode):
            return "unsafe_job_lock"
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            return "active_job_lock"
    after = os.stat(path.name, dir_fd=root_fd, follow_symlinks=False)
    if (before.st_dev, before.st_ino) != (after.st_dev, after.st_ino):
        return "scratch_identity_changed"
    root_now, root_opened = os.stat(root), os.fstat(root_fd)
    if root.resolve() != root or (root_now.st_dev, root_now.st_ino) != (root_opened.st_dev, root_opened.st_ino):
        return "scratch_root_changed"
    return None


def _resolve_tmp_root(tmp_root: Path | str | None) -> Path:
    if tmp_root is not None:
        return Path(tmp_root)
    env = os.environ.get(_TMP_ROOT_ENV)
    if env and str(env).strip():
        return Path(str(env).strip())
    return DEFAULT_TMP_ROOT


def _coerce_max_age_h(max_age_h: float | int | str | None) -> float:
    try:
        value = float(DEFAULT_MAX_AGE_H if max_age_h is None else max_age_h)
    except (TypeError, ValueError):
        return DEFAULT_MAX_AGE_H
    if value <= 0:
        return DEFAULT_MAX_AGE_H
    return value


def reap_stale_dev_temp(
    *,
    apply: bool = False,
    max_age_h: float | int | str | None = DEFAULT_MAX_AGE_H,
    tmp_root: Path | str | None = None,
    repo: Path | str | None = None,
    scope: str = "operator",
) -> dict[str, Any]:
    """Age out stale dev trees under ``tmp_root`` (and the ``repo`` scratch root).

    Parameters
    ----------
    apply:
        When False (default), dry-run — list stale dirs only, never delete.
    max_age_h:
        Age threshold in hours (default 24). Invalid / non-positive → default.
    tmp_root:
        Directory to scan (default ``/tmp``, or ``WORKBAY_DEV_TEMP_ROOT``).
    repo:
        Repository root. When given, ``<repo>/.workbay/scratch`` is scanned as a
        SECOND root on the same age threshold with the same dry-run default.
        Omitted (the default) that arm does not run at all, so every
        pre-existing call keeps its exact prior behaviour.

    scope:
        ``operator`` retains explicit maintenance semantics. ``lifecycle``
        ignores ambient roots and requires native released private repo scratch.
        Unknown scope fails closed and reports an error.

    Returns a summary dict. Never raises.
    """
    max_age = _coerce_max_age_h(max_age_h)
    if scope == "lifecycle":
        report = _empty_report(apply=apply, max_age_h=max_age)
        report.update(scope=scope, tmp_root=None, held=[])
        if repo is None or not str(repo).strip():
            report["errors"].append("lifecycle repo scope unavailable")
            return report
        try:
            with ExitStack() as stack:
                root, fd = _private_scratch_fd(repo, stack)
                report["scratch_root"] = str(root)
                _scan_root(
                    root,
                    SCRATCH_GLOB,
                    cutoff=time.time() - max_age * 3600,
                    apply=apply,
                    stale=report["stale"],
                    removed=report["removed"],
                    fresh=report["fresh"],
                    errors=report["errors"],
                    held=report["held"],
                    root_fd=fd,
                )
        except FileNotFoundError:
            pass  # An absent fixed scratch hierarchy is an empty scope.
        except Exception as exc:  # noqa: BLE001 -- visible best-effort failure
            report["errors"].append(f"lifecycle scope: {type(exc).__name__}: {exc}")
        report["stale_count"] = len(report["stale"])
        report["would_remove"] = list(report["stale"]) if not apply else []
        return report
    if scope != "operator":
        return _empty_report(apply=apply, max_age_h=max_age, error="unknown reaper scope")
    try:
        root = _resolve_tmp_root(tmp_root)
    except Exception as exc:  # noqa: BLE001 — never-raise
        return _empty_report(apply=apply, max_age_h=max_age, error=str(exc))

    stale: list[str] = []
    removed: list[str] = []
    fresh: list[str] = []
    errors: list[str] = []
    cutoff = time.time() - (max_age * 3600.0)

    try:
        scratch_root = _resolve_scratch_root(repo)
    except Exception as exc:  # noqa: BLE001 — never-raise
        scratch_root = None
        errors.append(f"{repo}: {exc}")

    try:
        _scan_root(
            root,
            DEV_TEMP_GLOB,
            cutoff=cutoff,
            apply=apply,
            stale=stale,
            removed=removed,
            fresh=fresh,
            errors=errors,
        )
        if scratch_root is not None:
            _scan_root(
                scratch_root,
                SCRATCH_GLOB,
                cutoff=cutoff,
                apply=apply,
                stale=stale,
                removed=removed,
                fresh=fresh,
                errors=errors,
            )

        return {
            "ok": True,
            "applied": bool(apply),
            "max_age_h": max_age,
            "tmp_root": str(root),
            "scratch_root": str(scratch_root) if scratch_root is not None else None,
            "stale": stale,
            "would_remove": list(stale) if not apply else [],
            "removed": removed,
            "fresh": fresh,
            "errors": errors,
            "stale_count": len(stale),
        }
    except Exception as exc:  # noqa: BLE001 — top-level never-raise
        return _empty_report(
            apply=apply,
            max_age_h=max_age,
            tmp_root=str(root),
            error=str(exc),
        )


def _empty_report(
    *,
    apply: bool,
    max_age_h: float,
    tmp_root: str | None = None,
    error: str | None = None,
) -> dict[str, Any]:
    return {
        "ok": True,
        "applied": bool(apply),
        "max_age_h": max_age_h,
        "tmp_root": tmp_root if tmp_root is not None else str(DEFAULT_TMP_ROOT),
        "scratch_root": None,
        "stale": [],
        "would_remove": [],
        "removed": [],
        "fresh": [],
        "errors": [error] if error else [],
        "stale_count": 0,
    }
