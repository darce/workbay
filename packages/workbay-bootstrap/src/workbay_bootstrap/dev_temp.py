"""Steady-state reclaimer for sticky dev trees (implementation note S4).

Two roots, one clock:

* ``/tmp/workbay-*`` -- preflight Makefiles reuse a sticky ``PREFLIGHT_TMPDIR``;
  killed mid-run leaves residue there.
* ``<repo>/.workbay/scratch/*`` -- per-task driver scripts, gate logs and
  captured patches accumulate the same class of residue. Scanned only when the
  caller supplies ``repo``; every pre-existing call keeps its exact prior
  behaviour.

Never raises ([RES-07]/[AGT-10]): permission errors and missing roots degrade
to empty reports.
"""

from __future__ import annotations

import os
import shutil
import time
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
            candidates = sorted(root.glob(glob))
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
                stale.append(str(path))
                if not apply:
                    continue
                try:
                    shutil.rmtree(path)
                    removed.append(str(path))
                except OSError as exc:
                    errors.append(f"{path}: {exc}")
            except Exception as exc:  # noqa: BLE001 -- per-path degrade
                errors.append(f"{path}: {exc}")
    except Exception as exc:  # noqa: BLE001 -- per-root degrade, never raise
        errors.append(f"{root}: {exc}")


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

    Returns a summary dict. Never raises.
    """
    max_age = _coerce_max_age_h(max_age_h)
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
