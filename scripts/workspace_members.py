#!/usr/bin/env python3
"""Single source of truth for the WorkBay uv-workspace members.

Membership and the per-member import surface are **derived here** from the
authoritative manifests so no consumer re-authors them (system-of-record vs.
derived data; DRY = one canonical representation):

* the member set comes from the root ``pyproject.toml``
  ``[tool.uv.workspace].members`` glob, minus ``exclude``;
* each member's dist name comes from its own ``[project].name``;
* each member's import name(s) come from its own
  ``[tool.hatch.build.targets.wheel].only-include`` (a trailing ``.py`` on a
  single-module entry is stripped);
* each member's live-editable import root is ``<pkg>/src`` when a ``src/``
  directory exists, else ``<pkg>`` (e.g. ``workbay-system``).

Consumers — ``scripts/dev_install.py``, ``scripts/check_dev_editables_liveness.py``
and the workspace-convergence tests — import :func:`iter_workspace_members`
instead of maintaining their own copy of the list. Previously the same member
list was hand-authored in three places (this triplication is exactly what let
the convergence test, dev-install, and pyproject drift apart).
"""

from __future__ import annotations

import fnmatch
import tomllib
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class WorkspaceMember:
    package_relpath: str  # repo-relative package dir, e.g. "packages/mcp-workbay-handoff"
    dist_name: str  # [project].name, e.g. "mcp-workbay-handoff"
    import_names: tuple[str, ...]  # e.g. ("workbay_handoff_mcp", "workbay_handoff_mcp_launcher")
    src_relpath: str  # repo-relative import root: "<pkg>/src" or "<pkg>"

    @property
    def primary_import(self) -> str:
        return self.import_names[0]


def repo_root(start: Path | None = None) -> Path:
    cur = (start or Path(__file__)).resolve()
    for parent in [cur, *cur.parents]:
        if (parent / ".git").exists() and (parent / "pyproject.toml").is_file():
            return parent
    raise RuntimeError("could not locate repo root")


def _import_names_from_only_include(only_include: list[str]) -> tuple[str, ...]:
    names: list[str] = []
    for entry in only_include:
        leaf = entry.rsplit("/", 1)[-1]
        if leaf.endswith(".py"):
            leaf = leaf[:-3]
        names.append(leaf)
    return tuple(names)


def iter_workspace_members(repo: Path | None = None) -> list[WorkspaceMember]:
    """Resolve every uv-workspace member from the authoritative manifests."""
    repo = (repo or repo_root()).resolve()
    root_cfg = tomllib.loads((repo / "pyproject.toml").read_text(encoding="utf-8"))
    workspace = root_cfg["tool"]["uv"]["workspace"]
    patterns: list[str] = workspace.get("members", [])
    excludes: list[str] = workspace.get("exclude", [])

    members: list[WorkspaceMember] = []
    seen: set[Path] = set()
    for pattern in patterns:
        for path in sorted(repo.glob(pattern)):
            if not path.is_dir() or path in seen:
                continue
            rel = path.relative_to(repo).as_posix()
            if any(fnmatch.fnmatch(rel, ex) for ex in excludes):
                continue
            pyproject = path / "pyproject.toml"
            if not pyproject.is_file():
                continue
            seen.add(path)
            cfg = tomllib.loads(pyproject.read_text(encoding="utf-8"))
            project = cfg.get("project")
            if not project or "name" not in project:
                continue
            wheel = (
                cfg.get("tool", {})
                .get("hatch", {})
                .get("build", {})
                .get("targets", {})
                .get("wheel", {})
            )
            only_include = wheel.get("only-include") or wheel.get("packages") or []
            import_names = _import_names_from_only_include(only_include)
            if not import_names:
                continue
            src_relpath = f"{rel}/src" if (path / "src").is_dir() else rel
            members.append(
                WorkspaceMember(
                    package_relpath=rel,
                    dist_name=project["name"],
                    import_names=import_names,
                    src_relpath=src_relpath,
                )
            )
    return members


def workspace_dist_names(repo: Path | None = None) -> set[str]:
    """The set of member dist names — the membership system of record."""
    return {m.dist_name for m in iter_workspace_members(repo)}


if __name__ == "__main__":
    import json

    print(
        json.dumps(
            [
                {
                    "package_relpath": m.package_relpath,
                    "dist_name": m.dist_name,
                    "import_names": list(m.import_names),
                    "src_relpath": m.src_relpath,
                }
                for m in iter_workspace_members()
            ],
            indent=2,
        )
    )
