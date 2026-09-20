"""Src ↔ installed package skew observability (internal / T13).

Surfaces only ([OBS-08]): compare installed distribution metadata against an
adjacent in-tree source checkout when one is present. No activation or
reinstall side effects (0064/0065 own that story).

Reuses ``workbay_protocol.version`` helpers when available and falls back to
stdlib ``importlib.metadata`` + adjacent ``pyproject.toml`` parse.
"""

from __future__ import annotations

import logging
import sys
from importlib import metadata
from pathlib import Path
from typing import Any

_log = logging.getLogger("workbay_handoff_mcp")

DIST_NAME = "mcp-workbay-handoff"
DEFAULT_ANCHOR = Path(__file__)


def _installed_version(dist_name: str = DIST_NAME) -> str | None:
    try:
        return metadata.version(dist_name)
    except metadata.PackageNotFoundError:
        return None


def _source_version(dist_name: str = DIST_NAME, *, anchor: Path | str = DEFAULT_ANCHOR) -> str | None:
    try:
        from workbay_protocol.version import _adjacent_pyproject_version
    except ImportError:  # pragma: no cover — protocol always present in monorepo
        return None
    return _adjacent_pyproject_version(Path(anchor), dist_name)


def _installed_commit(dist_name: str = DIST_NAME) -> str | None:
    """Best-effort VCS commit from dist ``direct_url.json`` (editable installs)."""
    try:
        dist = metadata.distribution(dist_name)
    except metadata.PackageNotFoundError:
        return None
    try:
        raw = dist.read_text("direct_url.json")
    except (FileNotFoundError, OSError, UnicodeDecodeError):
        return None
    if not raw:
        return None
    try:
        import json

        data = json.loads(raw)
    except json.JSONDecodeError:
        return None
    vcs = data.get("vcs_info") if isinstance(data, dict) else None
    if not isinstance(vcs, dict):
        return None
    commit = vcs.get("commit_id")
    return commit if isinstance(commit, str) and commit else None


def _source_git_commit(*, anchor: Path | str = DEFAULT_ANCHOR) -> str | None:
    """HEAD commit of the package checkout when ``anchor`` lives in a git tree."""
    import subprocess

    start = Path(anchor).resolve().parent
    for parent in (start, *start.parents):
        git_dir = parent / ".git"
        if git_dir.exists():
            try:
                proc = subprocess.run(
                    ["git", "-C", str(parent), "rev-parse", "HEAD"],
                    check=False,
                    capture_output=True,
                    text=True,
                    timeout=5,
                )
            except (OSError, subprocess.TimeoutExpired):
                return None
            if proc.returncode != 0:
                return None
            sha = (proc.stdout or "").strip()
            return sha or None
        # Stop at filesystem root.
        if parent.parent == parent:
            break
    return None


def detect_src_installed_skew(
    dist_name: str = DIST_NAME,
    *,
    anchor: Path | str = DEFAULT_ANCHOR,
) -> dict[str, Any]:
    """Compare installed dist metadata vs adjacent source; no side effects.

    Returns a stable shape always::

        {
          "ok": bool,              # True when no skew (or inconclusive)
          "skew": bool,
          "dist_name": str,
          "installed_version": str | None,
          "source_version": str | None,
          "installed_commit": str | None,
          "source_commit": str | None,
          "interpreter": str,
          "message": str | None,   # set when skew=True
          "remedy": str | None,
        }
    """
    installed_ver = _installed_version(dist_name)
    source_ver = _source_version(dist_name, anchor=anchor)
    installed_commit = _installed_commit(dist_name)
    source_commit = _source_git_commit(anchor=anchor)
    interpreter = sys.executable

    version_skew = installed_ver is not None and source_ver is not None and installed_ver != source_ver
    commit_skew = (
        installed_commit is not None
        and source_commit is not None
        and not (source_commit.startswith(installed_commit) or installed_commit.startswith(source_commit))
    )
    skew = bool(version_skew or commit_skew)
    message = None
    remedy = None
    if skew:
        message = (
            f"src↔installed skew for {dist_name}: "
            f"installed_version={installed_ver!r} source_version={source_ver!r} "
            f"installed_commit={_short(installed_commit)!r} "
            f"source_commit={_short(source_commit)!r} "
            f"(interpreter={interpreter})"
        )
        remedy = (
            "Reconnect alone does not reinstall; reinstall/re-link the package "
            "from the in-tree source (or restart MCP after a fresh install) so "
            "the running server matches the checkout. Observability only — no "
            "auto-activation."
        )
    return {
        "ok": not skew,
        "skew": skew,
        "dist_name": dist_name,
        "installed_version": installed_ver,
        "source_version": source_ver,
        "installed_commit": installed_commit,
        "source_commit": source_commit,
        "interpreter": interpreter,
        "message": message,
        "remedy": remedy,
    }


def _short(value: str | None, n: int = 12) -> str | None:
    if value is None:
        return None
    return value if len(value) <= n else value[:n]


def emit_src_installed_skew_startup_log(
    dist_name: str = DIST_NAME,
    *,
    anchor: Path | str = DEFAULT_ANCHOR,
) -> dict[str, Any]:
    """Startup probe: log a single line when skew is detected. Always returns the probe dict."""
    probe = detect_src_installed_skew(dist_name, anchor=anchor)
    if probe.get("skew"):
        _log.warning("%s | remedy: %s", probe.get("message"), probe.get("remedy"))
    return probe
