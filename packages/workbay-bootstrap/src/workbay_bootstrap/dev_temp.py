"""Steady-state reclaimer for sticky ``/tmp/workbay-*`` dev trees (implementation note S4).

Preflight Makefiles reuse a sticky ``PREFLIGHT_TMPDIR``; killed mid-run leaves
residue under ``/tmp/workbay-*``. This reclaimer ages those directories out.
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
# Optional override for tests / operators (do not touch real /tmp in fixtures).
_TMP_ROOT_ENV = "WORKBAY_DEV_TEMP_ROOT"


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
) -> dict[str, Any]:
    """Age out ``workbay-*`` directories under ``tmp_root`` older than ``max_age_h``.

    Parameters
    ----------
    apply:
        When False (default), dry-run — list stale dirs only, never delete.
    max_age_h:
        Age threshold in hours (default 24). Invalid / non-positive → default.
    tmp_root:
        Directory to scan (default ``/tmp``, or ``WORKBAY_DEV_TEMP_ROOT``).

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
        if not root.is_dir():
            return {
                "ok": True,
                "applied": bool(apply),
                "max_age_h": max_age,
                "tmp_root": str(root),
                "stale": [],
                "would_remove": [],
                "removed": [],
                "fresh": [],
                "errors": [],
                "stale_count": 0,
            }
        try:
            candidates = sorted(root.glob(DEV_TEMP_GLOB))
        except OSError as exc:
            return _empty_report(
                apply=apply,
                max_age_h=max_age,
                tmp_root=str(root),
                error=str(exc),
            )

        for path in candidates:
            try:
                # Directories only — never follow/unlink arbitrary files.
                if path.is_symlink() or not path.is_dir():
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
            except Exception as exc:  # noqa: BLE001 — per-path degrade
                errors.append(f"{path}: {exc}")

        return {
            "ok": True,
            "applied": bool(apply),
            "max_age_h": max_age,
            "tmp_root": str(root),
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
        "stale": [],
        "would_remove": [],
        "removed": [],
        "fresh": [],
        "errors": [error] if error else [],
        "stale_count": 0,
    }
