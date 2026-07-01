"""Git-only runtime closure install helpers (internal S1)."""

from __future__ import annotations

import re
from pathlib import Path

GITONLY_RUNTIME_MEMBERS: tuple[str, ...] = (
    "workbay-protocol",
    "mcp-workbay-handoff",
    "mcp-workbay-orchestrator",
    "workbay-bootstrap",
    "workbay-system",
)

GITONLY_FRONT_DOOR = "workbay"

GITONLY_CLOSURE_PACKAGES: tuple[str, ...] = GITONLY_RUNTIME_MEMBERS + (GITONLY_FRONT_DOOR,)

GITONLY_MCP_PACKAGES: tuple[str, ...] = (
    "mcp-workbay-handoff",
    "mcp-workbay-orchestrator",
)

# The codex host bridge is an *optional* orchestrator extra (``[bridge]``), not a
# base closure member: only the orchestrator tool install needs it. It must be
# git-sourced too — otherwise the git-only orchestrator install resolves it from
# PyPI (the Q4 trap). Spec-able, but excluded from the universal closure source
# check (``GITONLY_CLOSURE_PACKAGES``) and from the default ``--with`` set.
GITONLY_BRIDGE_MEMBER = "workbay-codex-bridge"

# Per-package extra members to git-source via additional ``--with`` specs so a
# package's optional extras stay PyPI-free. The orchestrator's ``[bridge]`` extra
# pulls ``workbay-codex-bridge`` (the ``codex-subagent`` backend); without this
# the git-only orchestrator install silently drops that backend.
GITONLY_PACKAGE_EXTRA_MEMBERS: dict[str, tuple[str, ...]] = {
    "mcp-workbay-orchestrator": (GITONLY_BRIDGE_MEMBER,),
}

# Packages a git/path member spec may be built for: the closure plus the
# spec-able bridge extra.
_SPECABLE_PACKAGES: tuple[str, ...] = GITONLY_CLOSURE_PACKAGES + (GITONLY_BRIDGE_MEMBER,)

# Members whose specs are materialized into a closure spec map (the runtime
# members plus the spec-able bridge extra, so an orchestrator install can
# ``--with`` it from git).
_SPEC_MAP_MEMBERS: tuple[str, ...] = GITONLY_RUNTIME_MEMBERS + (GITONLY_BRIDGE_MEMBER,)


def member_specs_from_git_ref(*, repo_url: str, tag: str) -> dict[str, str]:
    return {
        member: git_member_spec(member, repo_url=repo_url, tag=tag)
        for member in _SPEC_MAP_MEMBERS
    }


def member_specs_from_repo_root(repo_root: Path) -> dict[str, str]:
    return {member: path_member_spec(repo_root, member) for member in _SPEC_MAP_MEMBERS}


DEFAULT_GIT_REPO_URL = "https://github.com/darce/workbay.git"


def normalize_git_remote_url(url: str) -> str:
    """Normalize an scp-style SSH remote to a uv-parseable ``ssh://`` URL.

    uv's ``git+`` parser rejects the scp shorthand
    ``git@github.com:darce/workbay.git`` ("Expected path to end in a supported
    file extension") and requires ``ssh://git@github.com/darce/workbay.git``.
    URLs already carrying a scheme (``https://``, ``ssh://``, ``file:``) and
    non-scp strings pass through unchanged.
    """
    if "://" in url or url.startswith("file:"):
        return url
    match = re.match(r"^(?P<user>[^@/]+@)?(?P<host>[^:/]+):(?P<path>.+)$", url)
    if match and "/" in match.group("path"):
        user = match.group("user") or ""
        return f"ssh://{user}{match.group('host')}/{match.group('path')}"
    return url


def git_member_spec(
    member: str,
    *,
    repo_url: str = DEFAULT_GIT_REPO_URL,
    tag: str,
) -> str:
    if member not in _SPECABLE_PACKAGES:
        raise ValueError(f"unknown gitonly package: {member}")
    return f"git+{normalize_git_remote_url(repo_url)}@{tag}#subdirectory=packages/{member}"


def path_member_spec(repo_root: Path, member: str) -> str:
    if member not in _SPECABLE_PACKAGES:
        raise ValueError(f"unknown gitonly package: {member}")
    return str((repo_root / "packages" / member).resolve())


def build_uv_tool_install_argv(
    *,
    package: str,
    from_spec: str,
    member_specs: dict[str, str],
    no_cache: bool = False,
    force: bool = True,
) -> list[str]:
    # ``--no-cache`` is OFF by default: ``--force`` already guarantees a fresh
    # reinstall, while leaving the cache reusable lets warm-cache / offline
    # hosts complete without re-fetching every member over the network. Callers
    # may still opt into ``no_cache=True`` for a hard, cache-bypassing install.
    if package not in GITONLY_CLOSURE_PACKAGES:
        raise ValueError(f"unknown gitonly package: {package}")
    # ``--no-sources`` is mandatory, not optional: every shipped member pyproject
    # carries ``[tool.uv.sources] { workspace = true }`` for in-tree dev builds.
    # A consumer ``uv tool install --from git+…#subdirectory=packages/<pkg>`` has
    # no workspace root, so uv rejects those entries ("references a workspace …
    # but is not a workspace member") and the whole git-only install fails. With
    # ``--no-sources`` uv ignores ``[tool.uv.sources]`` and resolves the closure
    # from the explicit ``--with`` git/path specs below (the Q4 mechanism). This
    # is equally correct for the local-path dev install, where the ``--with``
    # paths already pin every member.
    argv = ["tool", "install", "--no-sources"]
    if no_cache:
        argv.append("--no-cache")
    if force:
        argv.append("--force")
    with_members: list[str] = [m for m in GITONLY_RUNTIME_MEMBERS if m != package]
    for member in GITONLY_PACKAGE_EXTRA_MEMBERS.get(package, ()):
        if member != package and member not in with_members:
            with_members.append(member)
    for member in with_members:
        argv.extend(["--with", member_specs[member]])
    argv.extend(["--from", from_spec, package])
    return argv


def member_sources_are_local_or_git(install_output: str) -> bool:
    return all(
        _member_resolved_from_git_or_path(member, install_output)
        for member in GITONLY_CLOSURE_PACKAGES
    )


def _member_resolved_from_git_or_path(member: str, output: str) -> bool:
    pattern = rf"{re.escape(member)}==.*\(from (?:file:|git\+)"
    return re.search(pattern, output) is not None


def installed_members_are_local_or_git(install_output: str, *members: str) -> bool:
    """Check only the named packages (subset of the closure)."""
    return all(_member_resolved_from_git_or_path(member, install_output) for member in members)
