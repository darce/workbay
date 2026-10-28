"""Interpreter version skew probes (implementation note D4)."""

from __future__ import annotations

import logging
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

PROBE_PACKAGES = ("workbay-protocol", "mcp-workbay-handoff")
# Typed re-exec degrade carry: entry-point emit and receipt drain both consume
# this name so a rename cannot silently break the drain [ARCH-13, OBS-08].
_REEXEC_DEGRADED_ENV = "WORKBAY_LIFECYCLE_REEXEC_DEGRADED"
# Ambient overrides that silently redirect git reads/writes into another
# repository, index, object store, or ref namespace. Formatting-only vars
# (e.g. GIT_PAGER, GIT_INDEX_VERSION) are intentionally absent. Single named
# constant: scrub_ambient_git_env and the entry-point ImportError fallback
# both consume this set so the two paths cannot drift [ARCH-13, OBS-08].
_GIT_ENV_OVERRIDE_KEYS = (
    "GIT_DIR",
    "GIT_COMMON_DIR",
    "GIT_WORK_TREE",
    "GIT_OBJECT_DIRECTORY",
    "GIT_ALTERNATE_OBJECT_DIRECTORIES",
    "GIT_INDEX_FILE",
    "GIT_NAMESPACE",
    "GIT_CEILING_DIRECTORIES",
)
logger = logging.getLogger(__name__)


def scrub_ambient_git_env(env: dict[str, str] | None = None) -> dict[str, str]:
    """Return a copy of *env* (default: ``os.environ``) without ambient git overrides.

    Hooks, ``rebase -x``, and CI inject the keys in
    :data:`_GIT_ENV_OVERRIDE_KEYS` that redirect every git probe at an
    unrelated repository, index, object store, or ref namespace (or make
    discovery fail with exit 128). Both the lifecycle entry-point root probe
    and the worktree-collapse probe must scrub the same set so they cannot
    disagree about which checkout owns the process [ARCH-13].
    """
    cleaned = dict(os.environ if env is None else env)
    for key in _GIT_ENV_OVERRIDE_KEYS:
        cleaned.pop(key, None)
    return cleaned


@dataclass(frozen=True)
class InterpreterProbe:
    label: str
    python: str
    versions: dict[str, str]
    error: str = ""


def _package_versions(python: str, packages: tuple[str, ...] = PROBE_PACKAGES) -> tuple[dict[str, str], str]:
    script = (
        "import importlib.metadata as m\n"
        "pkgs = " + repr(packages) + "\n"
        "out = {}\n"
        "for name in pkgs:\n"
        "    try:\n"
        "        out[name] = m.version(name)\n"
        "    except Exception as exc:\n"
        "        out[name] = f'<missing:{exc.__class__.__name__}>'\n"
        "import json; print(json.dumps(out))\n"
    )
    try:
        proc = subprocess.run(
            [python, "-c", script],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {}, f"{type(exc).__name__}: {exc}"
    if proc.returncode != 0:
        return {}, (proc.stderr or proc.stdout or f"exit {proc.returncode}").strip()
    try:
        import json

        data = json.loads(proc.stdout)
    except json.JSONDecodeError as exc:
        return {}, f"json decode failed: {exc}"
    if not isinstance(data, dict):
        return {}, "probe returned non-object json"
    return {str(k): str(v) for k, v in data.items()}, ""


def _venv_python(repo_root: Path) -> str:
    for rel in (("bin", "python"), ("Scripts", "python.exe")):
        candidate = repo_root / ".venv" / rel[0] / rel[1]
        if candidate.is_file():
            return str(candidate)
    return ""


def _usable_in_tree_python(candidate: Path, container: Path) -> bool:
    """True when *candidate* is an executable regular file contained in *container*.

    ``exists()`` alone is not enough: directories, non-executable files, and
    paths that lexically leave the container must not be returned [AGT-10, SEC-01].

    Containment uses the candidate's own absolute location, not its symlink
    target: a real ``.venv/bin/python`` always points at an out-of-tree
    interpreter, and rejecting those inverted the module's purpose [AGT-10].
    Both sides use ``os.path.abspath`` (lexical, collapses ``..``, does not
    follow symlinks) so a ``/tmp`` checkout and a candidate built under it
    stay on the same prefix — mixed ``realpath``/``abspath`` pairs break
    macOS ``/tmp`` → ``/private/tmp`` fixtures. A rejected existing path
    logs one warning naming the path and why [OBS-08].
    """
    if not candidate.exists():
        return False
    if not candidate.is_file():
        logger.warning(
            "resolve_lifecycle_python: rejecting %s (not a regular file); continuing probe",
            candidate,
        )
        return False
    if not os.access(candidate, os.X_OK):
        logger.warning(
            "resolve_lifecycle_python: rejecting %s (not executable); continuing probe",
            candidate,
        )
        return False
    try:
        # Lexical absolute paths only — do not follow the candidate symlink.
        candidate_abs = Path(os.path.abspath(candidate))
        container_abs = Path(os.path.abspath(container))
    except OSError as exc:
        logger.warning(
            "resolve_lifecycle_python: rejecting %s (path normalize failed: %s); continuing probe",
            candidate,
            exc,
        )
        return False
    if not candidate_abs.is_relative_to(container_abs):
        logger.warning(
            "resolve_lifecycle_python: rejecting %s (location %s outside %s); continuing probe",
            candidate,
            candidate_abs,
            container_abs,
        )
        return False
    return True


def resolve_lifecycle_python_detailed(
    root: Path | str | None,
) -> tuple[str, str | None]:
    """Return ``(interpreter, give_up_cause)`` for lifecycle handler execution.

    ``give_up_cause`` is ``None`` when an in-tree workspace interpreter was
    genuinely selected. On every fallback to ``sys.executable`` it is a short
    human phrase naming why resolution declined, so a caller can tell "ambient
    already is the workspace venv" from "probed, found nothing usable" without
    reading stderr [OBS-08, RES-13].

    Probe order matches :func:`resolve_lifecycle_python`:
    1. ``<root>/.venv`` — the checkout's own environment (``bin/python`` then
       ``Scripts/python.exe``; first that is a regular executable file whose
       own location lives inside the checkout — symlink *targets* may be
       out of tree, as every real venv does).
    2. the primary checkout's ``.venv``, reached by collapsing a linked git
       worktree through plain ``rev-parse --git-common-dir`` (no
       ``--path-format=absolute``). Run only when the direct probe misses.
       A relative common dir is resolved against ``root``, never the process
       cwd. Ambient ``GIT_DIR`` / ``GIT_COMMON_DIR`` / ``GIT_WORK_TREE`` /
       ``GIT_OBJECT_DIRECTORY`` / ``GIT_CEILING_DIRECTORIES`` are stripped so
       an outer git invocation cannot redirect discovery. The primary is
       taken from ``rev-parse --show-toplevel`` when that succeeds; otherwise
       the common dir's parent when named ``.git``, else the common dir
       itself. The same executable + containment rule applies.
    3. ``sys.executable`` — last resort, never an error. Any probe failure
       falls back here. Exactly one warning per fallback names the cause;
       success stays quiet. ``None`` root means no root to probe.
    """
    if root is None:
        return sys.executable, "no root to probe"

    repo = Path(root)
    rejected = 0
    for candidate in (
        repo / ".venv" / "bin" / "python",
        repo / ".venv" / "Scripts" / "python.exe",
    ):
        if _usable_in_tree_python(candidate, repo):
            return str(candidate), None
        if candidate.exists():
            rejected += 1

    try:
        result = subprocess.run(
            [
                "git",
                "-C",
                str(repo),
                "rev-parse",
                "--git-common-dir",
            ],
            capture_output=True,
            text=True,
            check=False,
            timeout=5,
            env=scrub_ambient_git_env(),
        )
        if result.returncode != 0:
            stderr_tail = (result.stderr or "").strip()
            if len(stderr_tail) > 200:
                stderr_tail = stderr_tail[:200]
            logger.warning(
                "resolve_lifecycle_python: git collapse failed for %s "
                "(returncode=%s, stderr=%s); falling back to ambient interpreter %s",
                repo,
                result.returncode,
                stderr_tail,
                sys.executable,
            )
            return sys.executable, "git collapse failed"

        common_dir = (result.stdout or "").strip()
        if not common_dir:
            logger.warning(
                "resolve_lifecycle_python: git collapse failed for %s "
                "(empty stdout); falling back to ambient interpreter %s",
                repo,
                sys.executable,
            )
            return sys.executable, "git collapse empty stdout"

        common_path = Path(common_dir)
        if not common_path.is_absolute():
            common_path = (repo / common_path).resolve()
        else:
            common_path = common_path.resolve()

        # Prefer the authoritative toplevel; basename heuristic is fallback only
        # (separate-git-dir / submodule / core.worktree layouts) [LIFE-B-GIT-COLLAPSE-01].
        # Anchor order: parent of a ``.git`` common dir (main checkout for linked
        # worktrees), then the probe repo, then the common path itself.
        primary: Path | None = None
        anchors: list[Path] = []
        if common_path.name == ".git":
            anchors.append(common_path.parent)
        anchors.append(repo)
        if common_path not in anchors:
            anchors.append(common_path)
        for anchor in anchors:
            try:
                tl = subprocess.run(
                    [
                        "git",
                        "-C",
                        str(anchor),
                        "rev-parse",
                        "--show-toplevel",
                    ],
                    capture_output=True,
                    text=True,
                    check=False,
                    timeout=5,
                    env=scrub_ambient_git_env(),
                )
            except Exception:
                continue
            if tl.returncode != 0:
                continue
            toplevel = Path((tl.stdout or "").strip())
            if toplevel and toplevel.exists():
                primary = toplevel
                break
        if primary is None:
            logger.warning(
                "resolve_lifecycle_python: show-toplevel failed for %s "
                "(common_path=%s); using basename fallback",
                repo,
                common_path,
            )
            primary = common_path.parent if common_path.name == ".git" else common_path

        for candidate in (
            primary / ".venv" / "bin" / "python",
            primary / ".venv" / "Scripts" / "python.exe",
        ):
            if _usable_in_tree_python(candidate, primary):
                return str(candidate), None
            if candidate.exists():
                rejected += 1
    except Exception as exc:
        logger.warning(
            "resolve_lifecycle_python: git collapse failed for %s (%s); "
            "falling back to ambient interpreter %s",
            repo,
            exc,
            sys.executable,
        )
        return sys.executable, f"git collapse failed ({type(exc).__name__})"

    # Do not claim "no .venv discoverable" when candidates existed and were
    # rejected — that log was false for every real uv/venv layout [OBS-08].
    if rejected:
        logger.warning(
            "resolve_lifecycle_python: %s in-tree candidate(s) rejected under %s; "
            "using ambient interpreter %s",
            rejected,
            repo,
            sys.executable,
        )
        return (
            sys.executable,
            f"{rejected} in-tree candidate(s) rejected; using ambient interpreter",
        )
    logger.warning(
        "resolve_lifecycle_python: no in-tree .venv discoverable under %s; "
        "using ambient interpreter %s",
        repo,
        sys.executable,
    )
    return sys.executable, "no in-tree .venv discoverable; using ambient interpreter"


def resolve_lifecycle_python(root: Path | str | None) -> str:
    """Return the interpreter lifecycle handlers must run under.

    Thin wrapper over :func:`resolve_lifecycle_python_detailed` that preserves
    the historical ``str`` return type for existing call sites [ARCH-13].
    """
    path, _cause = resolve_lifecycle_python_detailed(root)
    return path


def collect_interpreter_probes(repo_root: Path) -> list[InterpreterProbe]:
    probes: list[InterpreterProbe] = []
    ambient = sys.executable
    versions, error = _package_versions(ambient)
    probes.append(
        InterpreterProbe(label="ambient", python=ambient, versions=versions, error=error)
    )
    venv_py = _venv_python(repo_root)
    if venv_py:
        versions, error = _package_versions(venv_py)
        probes.append(
            InterpreterProbe(label="workspace_venv", python=venv_py, versions=versions, error=error)
        )
    return probes


def find_skew(probes: list[InterpreterProbe]) -> list[str]:
    """Return human-readable skew lines for any package with >1 distinct version."""
    by_pkg: dict[str, dict[str, str]] = {}
    for probe in probes:
        if probe.error:
            continue
        for pkg, ver in probe.versions.items():
            if ver.startswith("<missing"):
                continue
            by_pkg.setdefault(pkg, {})[probe.label] = ver
    findings: list[str] = []
    for pkg, label_versions in sorted(by_pkg.items()):
        distinct = set(label_versions.values())
        if len(distinct) > 1:
            findings.append(
                f"{pkg}: " + ", ".join(f"{label}={ver}" for label, ver in sorted(label_versions.items()))
            )
    return findings


def warn_skew_if_needed(repo_root: Path, *, stream: object | None = None) -> list[str]:
    """Emit a one-line stderr warning when skew is detected (D4 visibility arm)."""
    probes = collect_interpreter_probes(repo_root)
    skew = find_skew(probes)
    if skew and stream is not None:
        stream.write(  # type: ignore[attr-defined]
            "workbay: interpreter version skew detected — "
            + "; ".join(skew)
            + "\n"
        )
    return skew


def ci_gate_enabled() -> bool:
    return os.environ.get("WORKBAY_CI_INTERPRETER_GATE", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
