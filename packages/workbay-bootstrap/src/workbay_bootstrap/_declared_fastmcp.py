"""Single reader for the declared FastMCP pin.

This file is duplicated byte-for-byte in:

- ``workbay_bootstrap/_declared_fastmcp.py``
- ``workbay_handoff_mcp/_declared_fastmcp.py``

Bootstrap must not import the handoff package at runtime (handoff is an
install target, not a bootstrap dependency). Handoff must not import
bootstrap (wrong package-graph direction). Keep the two copies identical;
``test_declared_fastmcp_reader_copies_are_byte_equal`` fails on drift.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

HANDOFF_PROJECT_NAME = "mcp-workbay-handoff"
FASTMCP_DIST_NAME = "fastmcp"
DECLARED_FASTMCP_REQUIREMENT_UNREADABLE = "declared_fastmcp_requirement_unreadable"
DECLARED_PIN_UNAVAILABLE = "declared_pin_unavailable"


class DeclaredFastmcpRequirementError(RuntimeError):
    """Raised when the declared ``fastmcp`` pin cannot be read (fail closed)."""


def fastmcp_requirement_from_deps(deps: object) -> str | None:
    """Return the base ``fastmcp`` PEP 508 requirement from a dependency list."""
    if not isinstance(deps, (list, tuple)):
        return None
    prefix = FASTMCP_DIST_NAME
    for dep in deps:
        if not isinstance(dep, str):
            continue
        stripped = dep.strip()
        if ";" in stripped:
            req, marker = stripped.split(";", 1)
            if "extra" in marker:
                continue
            stripped = req.strip()
        if stripped == prefix:
            return stripped
        if stripped.startswith(prefix) and len(stripped) > len(prefix):
            nxt = stripped[len(prefix)]
            if not (nxt.isalnum() or nxt in "-_"):
                return stripped
    return None


def _project_name(pyproject_path: Path) -> str | None:
    try:
        import tomllib

        data = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001 — diagnostic capture
        return None
    project = data.get("project") if isinstance(data, dict) else None
    if not isinstance(project, dict):
        return None
    name = project.get("name")
    return name if isinstance(name, str) else None


def owning_handoff_pyproject(*, start: Path | None = None) -> Path | None:
    """Locate the owning ``mcp-workbay-handoff`` pyproject.toml, if present."""
    origin = start if start is not None else Path(__file__).resolve().parent
    for parent in (origin, *origin.parents):
        sibling = parent / "packages" / HANDOFF_PROJECT_NAME / "pyproject.toml"
        if sibling.is_file() and _project_name(sibling) == HANDOFF_PROJECT_NAME:
            return sibling
        candidate = parent / "pyproject.toml"
        if candidate.is_file() and _project_name(candidate) == HANDOFF_PROJECT_NAME:
            return candidate
        if parent.parent == parent:
            break
    return None


def handoff_pyproject_from_member_specs(
    member_specs: dict[str, str] | None,
) -> Path | None:
    """Return the handoff pyproject when ``member_specs`` carries a filesystem path."""
    if not member_specs:
        return None
    spec = member_specs.get(HANDOFF_PROJECT_NAME)
    if not isinstance(spec, str) or not spec:
        return None
    candidate = Path(spec)
    try:
        if candidate.is_file() and candidate.name == "pyproject.toml":
            return candidate
        if candidate.is_dir():
            pyproject = candidate / "pyproject.toml"
            if pyproject.is_file():
                return pyproject
    except OSError:
        return None
    return None


def declared_fastmcp_requirement_from_pyproject(path: Path) -> str | None:
    """Read the declared ``fastmcp`` PEP 508 requirement from a pyproject.toml."""
    try:
        import tomllib

        data = tomllib.loads(path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001 — diagnostic capture
        return None
    project = data.get("project") if isinstance(data, dict) else None
    if not isinstance(project, dict):
        return None
    return fastmcp_requirement_from_deps(project.get("dependencies") or [])


def declared_fastmcp_requirement_from_metadata() -> str | None:
    """Read the declared ``fastmcp`` pin from installed handoff metadata."""
    try:
        import importlib.metadata

        reqs = importlib.metadata.requires(HANDOFF_PROJECT_NAME)
    except Exception:  # noqa: BLE001 — diagnostic capture
        return None
    return fastmcp_requirement_from_deps(reqs)


def declared_fastmcp_requirement(*, start: Path | None = None) -> str | None:
    """Read the declared pin from the checkout pyproject walk."""
    path = owning_handoff_pyproject(start=start)
    if path is None:
        return None
    return declared_fastmcp_requirement_from_pyproject(path)


def _parse_git_ref_member_spec(spec: str) -> tuple[str, str] | None:
    """Parse a ``git+<url>@<ref>#subdirectory=...`` member spec into ``(url, ref)``.

    Returns ``None`` for anything that is not a recognizable pinned git+ spec
    (a filesystem path, or a spec with no ``@<ref>``).
    """
    if not isinstance(spec, str) or not spec.startswith("git+"):
        return None
    body = spec[len("git+") :]
    body = body.split("#", 1)[0]
    url, sep, ref = body.rpartition("@")
    if not sep or not url or not ref:
        return None
    return url, ref


def _fetch_pyproject_text_from_git_ref(repo_url: str, ref: str) -> str | None:
    """Best-effort shallow fetch of the handoff pyproject at ``ref``.

    Returns ``None`` on any failure (offline, unknown ref, missing file) —
    this is one candidate pin source among several; callers must still fail
    closed when every source misses.
    """
    import subprocess
    import tempfile

    try:
        with tempfile.TemporaryDirectory(prefix="wb-fastmcp-pin-") as tmp_dir:
            subprocess.run(
                [
                    "git",
                    "clone",
                    "--quiet",
                    "--depth",
                    "1",
                    "--branch",
                    ref,
                    "--single-branch",
                    repo_url,
                    tmp_dir,
                ],
                check=True,
                capture_output=True,
                text=True,
                timeout=60,
            )
            candidate = Path(tmp_dir) / "packages" / HANDOFF_PROJECT_NAME / "pyproject.toml"
            if candidate.is_file():
                return candidate.read_text(encoding="utf-8")
    except Exception:  # noqa: BLE001 — best-effort network fallback source
        return None
    return None


def declared_fastmcp_requirement_from_git_ref(
    member_specs: dict[str, str] | None,
    *,
    fetch_pyproject_text: Callable[[str, str], str | None] | None = None,
) -> str | None:
    """Read the declared pin from the git ref itself for git-sourced specs.

    A fresh ``--source package --remote-url <git-url> --remote-ref <tag>``
    install has no local monorepo checkout and no installed
    ``mcp-workbay-handoff`` metadata yet, so the filesystem-member-spec and
    installed-metadata sources both miss even though the member specs name an
    exact, resolvable git ref. Read the pin from that ref directly rather than
    falling straight through to the hard abort.
    """
    if not member_specs:
        return None
    spec = member_specs.get(HANDOFF_PROJECT_NAME)
    parsed = _parse_git_ref_member_spec(spec) if isinstance(spec, str) else None
    if parsed is None:
        return None
    repo_url, ref = parsed
    fetch = fetch_pyproject_text or _fetch_pyproject_text_from_git_ref
    text = fetch(repo_url, ref)
    if text is None:
        return None
    try:
        import tomllib

        data = tomllib.loads(text)
    except Exception:  # noqa: BLE001 — diagnostic capture
        return None
    project = data.get("project") if isinstance(data, dict) else None
    if not isinstance(project, dict):
        return None
    return fastmcp_requirement_from_deps(project.get("dependencies") or [])


def resolve_declared_fastmcp_requirement(
    member_specs: dict[str, str] | None = None,
    *,
    start: Path | None = None,
    metadata_first: bool = False,
) -> str | None:
    """Resolve the declared pin.

    Default order (install argv): member-spec filesystem pyproject, git-ref
    fetch (for git-sourced member specs with no local checkout), installed
    metadata, checkout walk. ``metadata_first=True`` is the doctor order:
    installed metadata, then the checkout walk.
    """
    if not metadata_first:
        member_pyproject = handoff_pyproject_from_member_specs(member_specs)
        if member_pyproject is not None:
            requirement = declared_fastmcp_requirement_from_pyproject(member_pyproject)
            if requirement is not None:
                return requirement
        requirement = declared_fastmcp_requirement_from_git_ref(member_specs)
        if requirement is not None:
            return requirement
        requirement = declared_fastmcp_requirement_from_metadata()
        if requirement is not None:
            return requirement
        return declared_fastmcp_requirement(start=start)

    requirement = declared_fastmcp_requirement_from_metadata()
    if requirement is not None:
        return requirement
    return declared_fastmcp_requirement(start=start)


def require_declared_fastmcp_requirement(
    member_specs: dict[str, str] | None = None,
    *,
    start: Path | None = None,
) -> str:
    """Return the declared pin or fail closed with a named message."""
    requirement = resolve_declared_fastmcp_requirement(
        member_specs, start=start, metadata_first=False
    )
    if requirement is None:
        raise DeclaredFastmcpRequirementError(
            f"{DECLARED_FASTMCP_REQUIREMENT_UNREADABLE}: "
            "could not read the fastmcp pin from member specs, "
            "installed mcp-workbay-handoff metadata, or checkout pyproject.toml"
        )
    return requirement
