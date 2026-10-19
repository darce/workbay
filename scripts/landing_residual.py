"""Candidate-relative landing residual (implementation note C7', LANDCAMP-H-04).

For every branch still unmerged into the integration ref, measure what it would
add ON TOP OF an integration candidate with exactly one ``git merge-tree`` call
per branch (no cumulative tree, so the census is O(n) in git calls), then group
identical residuals by stable patch-id and partition the distinct residuals into
connected components of the file-overlap graph (GRPH-06).

Every git failure raises ``ResidualError``; the report is never partial-silent
(OBS-08). The branch count is bounded by an explicit budget, not a timeout
(RES-02).
"""

from __future__ import annotations

import re
import subprocess
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable

_SHA40 = re.compile(r"[0-9a-f]{40}")
_DEFAULT_MAX_BRANCHES = 500


class ResidualError(RuntimeError):
    """A Git probe could not establish a residual fact, or a budget was exceeded."""


def _git(root: Path, *args: str, stdin: str | None = None) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            ["git", "-C", str(root), *args],
            capture_output=True, text=True, check=False, timeout=60, input=stdin,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise ResidualError(f"git {' '.join(args)} failed: {type(exc).__name__}: {exc}") from exc


def _stdout(root: Path, *args: str, stdin: str | None = None) -> str:
    proc = _git(root, *args, stdin=stdin)
    if proc.returncode != 0:
        raise ResidualError(f"git {' '.join(args)} exited {proc.returncode}: {(proc.stderr or proc.stdout).strip()}")
    return proc.stdout


def _names(output: str) -> list[str]:
    return sorted({line.strip() for line in output.splitlines() if line.strip()})


def _resolve(root: Path, rev: str) -> str:
    value = _stdout(root, "rev-parse", "--verify", f"{rev}^{{commit}}").strip().lower()
    if not _SHA40.fullmatch(value):
        raise ResidualError(f"{rev!r} did not resolve to a commit: {value!r}")
    return value


def _unmerged_not_in_candidate(root: Path, candidate: str, integration_ref: str) -> list[str]:
    fmt = "--format=%(refname:short)"
    unmerged = set(_names(_stdout(root, "branch", "--list", fmt, "--no-merged", integration_ref)))
    absorbed = set(_names(_stdout(root, "branch", "--list", fmt, "--merged", candidate)))
    integration = integration_ref.removeprefix("refs/heads/")
    return sorted(branch for branch in unmerged - absorbed if branch != integration)


def _merge_tree(root: Path, candidate_sha: str, branch: str) -> tuple[str, list[str]]:
    """Return (result tree, conflicted files) from one ``git merge-tree`` call."""

    proc = _git(root, "merge-tree", "--write-tree", "--name-only", candidate_sha, branch)
    if proc.returncode not in (0, 1):
        raise ResidualError(f"git merge-tree {candidate_sha[:12]} {branch} exited {proc.returncode}: {(proc.stderr or proc.stdout).strip()}")
    lines = proc.stdout.splitlines()
    if not lines or not _SHA40.fullmatch(lines[0].strip()):
        raise ResidualError(f"git merge-tree {branch}: no result tree in output")
    conflict_files: list[str] = []
    if proc.returncode == 1:
        for line in lines[1:]:
            if not line.strip():
                break
            conflict_files.append(line.strip())
    return lines[0].strip(), sorted(set(conflict_files))


def _numstat(root: Path, candidate_sha: str, tree: str) -> tuple[list[str], int, int]:
    files, ins, dels = [], 0, 0
    for line in _stdout(root, "diff", "--numstat", candidate_sha, tree).splitlines():
        parts = line.split("\t", 2)
        if len(parts) != 3:
            continue
        added, removed, path = parts
        files.append(path)
        ins += int(added) if added.isdigit() else 0
        dels += int(removed) if removed.isdigit() else 0
    return sorted(files), ins, dels


def _patch_id(root: Path, candidate_sha: str, tree: str) -> str | None:
    diff = _stdout(root, "diff", candidate_sha, tree)
    if not diff:
        return None
    out = _stdout(root, "patch-id", "--stable", stdin=diff).split()
    return out[0] if out else None


def _components(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Connected components of the file-overlap graph among distinct residuals."""

    representative: dict[str, dict[str, Any]] = {}
    for row in rows:
        if row["class"] == "identical":
            continue
        key = row["patch_id"] or row["branch"]
        representative.setdefault(key, row)
    members = sorted(representative.values(), key=lambda row: row["branch"])
    parent = {row["branch"]: row["branch"] for row in members}

    def find(name: str) -> str:
        while parent[name] != name:
            parent[name] = parent[parent[name]]
            name = parent[name]
        return name

    owners: dict[str, str] = {}
    for row in members:
        for path in row["files"] + row["conflict_files"]:
            if path in owners:
                parent[find(row["branch"])] = find(owners[path])
            else:
                owners[path] = row["branch"]
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in members:
        groups[find(row["branch"])].append(row)
    components = []
    for group in sorted(groups.values(), key=lambda rows: (-len(rows), rows[0]["branch"])):
        touched: dict[str, int] = defaultdict(int)
        for row in group:
            for path in set(row["files"] + row["conflict_files"]):
                touched[path] += 1
        components.append({
            "id": f"K{len(components)}",
            "members": sorted(row["branch"] for row in group),
            "hubs": sorted(path for path, count in touched.items() if count > 1),
            "files": sorted(touched),
            "ins": sum(row["ins"] for row in group),
            "conflicting": sum(row["class"] == "conflicting" for row in group),
        })
    return components


def build_residual(
    repo_root: Path | str,
    candidate: str,
    integration_ref: str = "main",
    max_branches: int = _DEFAULT_MAX_BRANCHES,
) -> dict[str, Any]:
    """Measure every unmerged branch's residual on top of ``candidate``."""

    root = Path(repo_root).expanduser().resolve()
    candidate_sha = _resolve(root, candidate)
    branches = _unmerged_not_in_candidate(root, candidate_sha, integration_ref)
    if len(branches) > max_branches:
        raise ResidualError(f"{len(branches)} unmerged branches exceed max_branches={max_branches}; raise the budget explicitly")
    rows: list[dict[str, Any]] = []
    for branch in branches:
        tree, conflict_files = _merge_tree(root, candidate_sha, branch)
        files, ins, dels = _numstat(root, candidate_sha, tree)
        patch_id = _patch_id(root, candidate_sha, tree)
        if conflict_files:
            klass = "conflicting"
        elif not files:
            klass = "identical"
        else:
            klass = "clean"
        rows.append({
            "branch": branch, "class": klass, "files": files, "ins": ins, "del": dels,
            "conflict_files": conflict_files, "patch_id": patch_id,
        })
    by_patch: dict[str, list[str]] = defaultdict(list)
    for row in rows:
        if row["patch_id"]:
            by_patch[row["patch_id"]].append(row["branch"])
    duplicates = sorted(sorted(group) for group in by_patch.values() if len(group) > 1)
    components = _components(rows)
    totals = {
        "branches": len(rows),
        "identical": sum(row["class"] == "identical" for row in rows),
        "clean": sum(row["class"] == "clean" for row in rows),
        "conflicting": sum(row["class"] == "conflicting" for row in rows),
        "duplicates": len(duplicates),
        "components": len(components),
    }
    return {
        "candidate": candidate, "candidate_sha": candidate_sha, "integration_ref": integration_ref,
        "branches": branches, "rows": rows, "duplicates": duplicates, "components": components, "totals": totals,
    }


def residual_table(report: dict[str, Any]) -> str:
    lines = [f"residual vs {report['candidate']} ({report['candidate_sha'][:12]})", "BRANCH\tCLASS\tFILES\tINS\tDEL\tCONFLICTS", "-" * 72]
    lines.extend(
        f"{row['branch']}\t{row['class']}\t{len(row['files'])}\t{row['ins']}\t{row['del']}\t{','.join(row['conflict_files'])}"
        for row in report["rows"]
    )
    lines.append("")
    for group in report["duplicates"]:
        lines.append("duplicate: " + " == ".join(group))
    for item in report["components"]:
        lines.append(f"{item['id']} members={len(item['members'])} conflicting={item['conflicting']} hubs={','.join(item['hubs']) or '-'}")
    totals = report["totals"]
    lines.append("totals: " + ", ".join(f"{key}={value}" for key, value in totals.items()))
    return "\n".join(lines)


__all__ = ["ResidualError", "build_residual", "residual_table"]
