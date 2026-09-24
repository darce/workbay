"""Git freshness and environment-consistency probes for the landing gate.

Subprocess and import-path probing stay here so a hung git/import cannot be
confused with a merge-policy refusal [REVIEW-M-07, RES-06].
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any, Sequence

from landing_gate_policy import LandingGateError

#: Modules whose provenance decides whether a gate run measured the branch.
#: These are exactly the packages a lane's ``.venv`` redirect can resolve to
#: the root checkout instead.
DEFAULT_GUARDED_MODULES = (
    "workbay_orchestrator_mcp",
    "workbay_handoff_mcp",
    "workbay_protocol",
)

#: ``pytest_path_guard`` (packages/workbay-system/scripts) hard-fails a session
#: when ANY top-level module matching this prefix resolves outside the
#: worktree.  The import path is therefore not a matter of taste: it is
#: whatever makes every in-repo ``workbay_*`` package resolve in-worktree, and
#: it is derived from that contract rather than restated next to it.
_GUARDED_TOP_LEVEL_PREFIX = "workbay_"

#: Directories holding bare helper modules that ``conftest.py`` files import by
#: name (``pytest_path_guard``, ``landing_gate``, the payload generators).
#: These are not ``workbay_*`` packages, so the derivation below cannot find
#: them; they are listed, and pinned by a test, rather than globbed -- a glob
#: over ``*/scripts`` would sweep in directories that are not import roots.
_HELPER_IMPORT_ROOTS = (
    "scripts",
    "packages/workbay-system/scripts",
    "packages/workbay-system/workbay_system/payload/scripts",
)

_PROBE = r"""
import importlib, json, sys
out = {}
for name in json.loads(sys.argv[1]):
    try:
        mod = importlib.import_module(name)
    except BaseException:
        out[name] = None
        continue
    out[name] = getattr(mod, "__file__", None)
print(json.dumps(out))
"""


def _git(root: Path, *args: str) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            ["git", "-C", str(root), *args],
            capture_output=True,
            text=True,
            check=False,
            timeout=60,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise LandingGateError(f"git {' '.join(args)} failed: {type(exc).__name__}: {exc}") from exc


def _stdout(root: Path, *args: str) -> str:
    proc = _git(root, *args)
    if proc.returncode != 0:
        raise LandingGateError(f"git {' '.join(args)} exited {proc.returncode}: {(proc.stderr or proc.stdout).strip()}")
    return proc.stdout


def subject_freshness(root: Path | str, subject_ref: str, *, integration_ref: str = "main") -> dict[str, Any]:
    """Report the commits ``integration_ref`` holds that ``subject_ref`` does not.

    An unresolvable ref raises rather than reporting ``current``: a typo that
    reads as a fresh branch is the worst available answer, because it is the
    one that lets the merge through.
    """
    root = Path(root)
    for ref in (subject_ref, integration_ref):
        proc = _git(root, "rev-parse", "--verify", f"{ref}^{{commit}}")
        if proc.returncode != 0:
            raise LandingGateError(f"ref {ref!r} does not resolve to a commit in {root}")
    missing = [
        line.strip()
        for line in _stdout(root, "rev-list", f"{subject_ref}..{integration_ref}").splitlines()
        if line.strip()
    ]
    return {
        "outcome": "subject_behind_baseline" if missing else "current",
        "behind": len(missing),
        "missing": missing,
        "subject_ref": subject_ref,
        "integration_ref": integration_ref,
    }


def environment_consistency(
    worktree: Path | str,
    modules: Sequence[str],
    *,
    python: str,
    extra_paths: Sequence[Path | str] = (),
    env: dict[str, str] | None = None,
    probe_source: str | None = None,
) -> dict[str, Any]:
    """Resolve each guarded module and refuse if any lands outside ``worktree``.

    Three ways a module fails this check, all one outcome because all three
    mean the same thing to a gate run -- the suite is not measuring this
    branch:

    * it resolves to a path outside the worktree (the redirect case),
    * it does not import at all (a half-wired tree), or
    * it has no ``__file__`` (a namespace package).  ``None`` is not a path;
      coercing it to ``""`` would make it "inside" every worktree, which is
      precisely the false-consistent this check exists to prevent.
    """
    worktree = Path(worktree).resolve()
    import_env = dict(env) if env is not None else {}
    prefix = [str(Path(p).resolve()) for p in extra_paths]
    if prefix:
        existing = import_env.get("PYTHONPATH", "")
        import_env["PYTHONPATH"] = ":".join([*prefix, existing]) if existing else ":".join(prefix)
    probe = _PROBE if probe_source is None else probe_source
    try:
        proc = subprocess.run(
            [python, "-c", probe, json.dumps(list(modules))],
            capture_output=True,
            text=True,
            check=False,
            timeout=120,
            env={**_base_env(), **import_env},
            cwd=str(worktree) if worktree.is_dir() else None,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise LandingGateError(f"import probe failed: {type(exc).__name__}: {exc}") from exc
    if proc.returncode != 0 or not proc.stdout.strip():
        raise LandingGateError(f"import probe exited {proc.returncode}: {(proc.stderr or proc.stdout).strip()[:400]}")
    try:
        resolved: dict[str, str | None] = json.loads(proc.stdout.strip().splitlines()[-1])
    except (ValueError, IndexError) as exc:
        raise LandingGateError(f"import probe emitted unparseable output: {exc}") from exc

    # A module the probe did not answer for is not a module that passed. The
    # probe is contracted to emit one key per request (``None`` when the import
    # raised), so a gap means the probe, not the environment, is broken --
    # and reading a gap as "nothing to check" is how a partially wired tree
    # gets a clean bill of health [OBS-08].
    unanswered = [name for name in modules if name not in resolved]
    if unanswered:
        raise LandingGateError(
            f"import probe answered for {len(resolved)} of {len(modules)} modules; "
            f"no verdict for: {', '.join(unanswered)}"
        )

    offenders = [name for name in modules if not _inside(resolved.get(name), worktree)]
    return {
        "outcome": "environment_inconsistent" if offenders else "consistent",
        "offenders": offenders,
        "resolved": resolved,
        "worktree": str(worktree),
        # The prefix under which the above verdict holds. A caller that runs
        # the suite WITHOUT it has not run under the environment this gate
        # checked, so it is reported rather than left implicit -- forcing the
        # consistent checkout and verifying it are the same act.
        "pythonpath": import_env.get("PYTHONPATH", ""),
    }


def _base_env() -> dict[str, str]:
    import os

    keep = {"PATH", "HOME", "LANG", "LC_ALL", "TMPDIR", "SYSTEMROOT"}
    return {k: v for k, v in os.environ.items() if k in keep}


def _inside(resolved: str | None, worktree: Path) -> bool:
    if not resolved:
        return False
    try:
        return Path(resolved).resolve().is_relative_to(worktree)
    except OSError:
        return False


def import_path_roots(worktree: Path | str) -> list[Path]:
    """The directories a run must put ahead of site-packages, in ONE place.

    This exists because it did not.  The gate derived its probe path as
    ``glob("packages/*/src")`` while the script that produced the failure ids
    exported a hand-written list; the two shared 3 of 7 entries and neither was
    right.  The hand-written list omitted ``packages/workbay-bootstrap/src``,
    so every subject run in a lane worktree died at ``ERROR: workbay_bootstrap
    loaded from <root>, but cwd is <lane>`` before one test ran.  The glob
    omitted ``packages/workbay-system``, whose importable tree sits at the
    package root rather than under ``src/``.  Certifying one environment while
    measuring under another is the ad-hoc inference this module exists to
    delete, so there is now one producer and two consumers (LANDGATE-PP-01).

    Nonexistent directories are dropped rather than emitted: a path that is not
    there is a silent no-op on ``PYTHONPATH``, and it would let two consumers
    agree on a string while disagreeing on an import.
    """
    root = Path(worktree)
    if not root.is_dir():
        return []
    found: list[Path] = []
    seen: set[str] = set()

    def _add(candidate: Path) -> None:
        if not candidate.is_dir():
            return
        key = str(candidate.resolve())
        if key in seen:
            return
        seen.add(key)
        found.append(candidate)

    # Every parent of an in-repo guarded package, at either supported depth.
    for pattern in (
        f"packages/*/src/{_GUARDED_TOP_LEVEL_PREFIX}*/__init__.py",
        f"packages/*/{_GUARDED_TOP_LEVEL_PREFIX}*/__init__.py",
    ):
        for init in sorted(root.glob(pattern)):
            _add(init.parent.parent)
    for rel in _HELPER_IMPORT_ROOTS:
        _add(root / rel)
    return found


def import_pythonpath(worktree: Path | str) -> str:
    """``import_path_roots`` as the ``PYTHONPATH`` value both consumers use."""
    return ":".join(str(p.resolve()) for p in import_path_roots(worktree))
