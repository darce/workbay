"""Read-only census of branches waiting for, or discharged from, landing."""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
import subprocess
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable, Sequence

try:
    from workbay_orchestrator_mcp.orchestration import branch_ancestry_dag as bad
except ImportError:  # Permit direct use from an uninstalled source checkout.
    _repo = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(_repo / "packages/mcp-workbay-orchestrator/src"))
    from workbay_orchestrator_mcp.orchestration import branch_ancestry_dag as bad
try:
    from scripts import landing_residual as residual
except ImportError:  # Direct `python scripts/landing_census.py` invocation.
    import landing_residual as residual  # type: ignore[no-redef]


_SHA_RE = re.compile(r"(?<![0-9a-f])[0-9a-f]{7,64}(?![0-9a-f])", re.IGNORECASE)
_SUPERSEDED_RE = re.compile(r"superseded_by\s*=\s*([0-9a-f]{7,64})", re.IGNORECASE)
_DISCHARGE_PREFIX = "claude_branch_discharged_"
_DEFAULT_INTEGRATION_REF = "main"
Outcome = tuple[str, str]


class CensusError(RuntimeError):
    """A Git or outcome-store probe could not establish a census fact."""

    def __init__(self, message: str, dag_provenance: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.dag_provenance = dag_provenance


def _git(root: Path, *args: str) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(["git", "-C", str(root), *args], capture_output=True, text=True, check=False, timeout=30)
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise CensusError(f"git {' '.join(args)} failed: {type(exc).__name__}: {exc}") from exc


def _checked_stdout(root: Path, *args: str) -> str:
    proc = _git(root, *args)
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout).strip()
        raise CensusError(f"git {' '.join(args)} exited {proc.returncode}: {detail}")
    return proc.stdout


def _names(output: str) -> list[str]:
    names: list[str] = []
    for raw in output.splitlines():
        name = raw.strip().removeprefix("* ").removeprefix("refs/heads/")
        if name and name not in names:
            names.append(name)
    return sorted(names)


def _all_branches(root: Path) -> list[str]:
    return _names(_checked_stdout(root, "for-each-ref", "--format=%(refname:short)", "refs/heads/"))


def _unmerged_branches(root: Path, integration_ref: str) -> list[str]:
    return _names(_checked_stdout(root, "branch", "--no-merged", integration_ref, "--format=%(refname:short)"))


def _worktrees(root: Path) -> list[dict[str, str | None]]:
    records: list[dict[str, str | None]] = []
    current: dict[str, str | None] = {}

    def flush() -> None:
        nonlocal current
        if current:
            records.append({key: current.get(key) for key in ("path", "branch", "head")})
        current = {}

    for raw in _checked_stdout(root, "worktree", "list", "--porcelain").splitlines():
        line = raw.strip()
        if not line:
            flush()
        elif raw.startswith("worktree "):
            if current:
                flush()
            current["path"] = raw.removeprefix("worktree ").strip()
        elif raw.startswith("HEAD "):
            current["head"] = raw.removeprefix("HEAD ").strip()
        elif raw.startswith("branch "):
            current["branch"] = raw.removeprefix("branch ").strip().removeprefix("refs/heads/")
    flush()
    return records


def _dag_runner(root: Path):
    """Adapt subprocess Git output to branch_ancestry_dag's typed contract."""

    def run(argv: Sequence[str]) -> bad.CommandOutcome:
        command = tuple(argv)
        if command and command[0] == "git":
            command = command[1:]
        proc = _git(root, *command)
        # CommandOutcome is (stdout, exit_code), in that order [OBS-08].
        return bad.CommandOutcome(proc.stdout, proc.returncode)

    return run


def _recording_runner(root: Path) -> tuple[Any, list[dict[str, Any]]]:
    """Wrap ``_dag_runner`` so exact argv/results survive into provenance."""

    inner = _dag_runner(root)
    probes: list[dict[str, Any]] = []

    def record(argv: Sequence[str], outcome: bad.CommandOutcome) -> None:
        probes.append(
            {
                "argv": [str(part) for part in argv],
                "status": "completed",
                "exit_code": int(outcome.exit_code),
                "stdout": "" if outcome.stdout is None else str(outcome.stdout),
            }
        )

    def run(argv: Sequence[str]) -> bad.CommandOutcome:
        try:
            outcome = inner(argv)
        except CensusError as exc:
            probes.append(
                {
                    "argv": [str(part) for part in argv],
                    "status": "raised",
                    "error": str(exc),
                }
            )
            raise
        record(argv, outcome)
        return outcome

    return run, probes


def _sha_tokens(value: object) -> list[str]:
    return [match.group(0).lower() for match in _SHA_RE.finditer(value)] if isinstance(value, str) else []


def _branch_name(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    branch = value.strip()
    if branch.startswith("refs/heads/"):
        branch = branch.removeprefix("refs/heads/")
    elif branch.startswith("refs/"):
        return None
    return branch or None


def _slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.casefold()).strip("-")


def _decision_matches(decision: object, branch: str) -> bool:
    if not isinstance(decision, str) or not decision.startswith(_DISCHARGE_PREFIX):
        return False
    suffix = decision.removeprefix(_DISCHARGE_PREFIX)
    spellings = {branch, branch.replace("/", "-"), branch.replace("/", "_"), _slug(branch)}
    return suffix in spellings or _slug(suffix) == _slug(branch)


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}


def _outcome_store(root: Path, branches: Iterable[str]) -> tuple[dict[str, list[Outcome]], set[str]]:
    """Read lane outcomes and discharge decisions without opening a write handle."""

    branch_set = set(branches)
    outcomes: dict[str, list[Outcome]] = defaultdict(list)
    lane_rows: set[str] = set()
    db_path = root / ".task-state" / "handoff.db"
    if not db_path.is_file():
        return {}, lane_rows
    try:
        conn = sqlite3.connect(db_path.as_uri() + "?mode=ro", uri=True)
    except sqlite3.Error as exc:
        raise CensusError(f"cannot read outcome database {db_path}: {exc}") from exc
    try:
        tables = {str(row[0]) for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if "worktree_lanes" in tables:
            columns = _table_columns(conn, "worktree_lanes")
            if "branch" not in columns:
                raise CensusError("worktree_lanes exists without required branch column")
            selected = ["branch", *(column for column in ("landing_commit_sha", "notes") if column in columns)]
            for values in conn.execute(f"SELECT {', '.join(selected)} FROM worktree_lanes"):
                row = dict(zip(selected, values, strict=True))
                branch = _branch_name(row["branch"])
                if branch is None or branch not in branch_set:
                    continue
                lane_rows.add(branch)
                for sha in _sha_tokens(row.get("landing_commit_sha")):
                    item = (sha, "landing")
                    if item not in outcomes[branch]:
                        outcomes[branch].append(item)
                for match in _SUPERSEDED_RE.finditer(str(row.get("notes") or "")):
                    item = (match.group(1).lower(), "superseded")
                    if item not in outcomes[branch]:
                        outcomes[branch].append(item)
        if "decisions" in tables:
            columns = _table_columns(conn, "decisions")
            if "decision" not in columns:
                raise CensusError("decisions exists without required decision column")
            selected = "decision" + (", rationale" if "rationale" in columns else "")
            for values in conn.execute(f"SELECT {selected} FROM decisions"):
                decision, rationale = values[0], values[1] if len(values) > 1 else None
                for branch in branch_set:
                    if not _decision_matches(decision, branch):
                        continue
                    for sha in _sha_tokens(rationale):
                        item = (sha, "superseded")
                        if item not in outcomes[branch]:
                            outcomes[branch].append(item)
    except sqlite3.Error as exc:
        raise CensusError(f"cannot query outcome database {db_path}: {exc}") from exc
    finally:
        conn.close()
    return dict(outcomes), lane_rows


def _branch_tips(root: Path, branches: Iterable[str]) -> dict[str, str]:
    tips: dict[str, str] = {}
    for branch in branches:
        value = _checked_stdout(root, "rev-parse", "--verify", f"refs/heads/{branch}^{{commit}}").strip().lower()
        if not re.fullmatch(r"[0-9a-f]{40}", value):
            raise CensusError(f"branch {branch!r} has an invalid tip SHA")
        tips[branch] = value
    return tips


def _resolve_commit(root: Path, value: str) -> str:
    value = _checked_stdout(root, "rev-parse", "--verify", f"{value}^{{commit}}").strip().lower()
    if not re.fullmatch(r"[0-9a-f]{40}", value):
        raise CensusError(f"outcome SHA did not resolve to a full commit: {value!r}")
    return value


def _is_ancestor(root: Path, older: str, newer: str) -> bool:
    proc = _git(root, "merge-base", "--is-ancestor", older, newer)
    if proc.returncode == 0:
        return True
    if proc.returncode == 1:
        return False
    raise CensusError(f"git merge-base failed for {older[:12]}..{newer[:12]}: {(proc.stderr or proc.stdout).strip()}")


def _patch_id_absorbed(root: Path, branch: str, integration_ref: str) -> bool:
    marks = [
        line.strip()[0]
        for line in _checked_stdout(root, "cherry", integration_ref, branch).splitlines()
        if line.strip()
    ]
    return bool(marks) and all(mark == "-" for mark in marks)


def _merge_tree_clean(root: Path, branch: str, integration_ref: str) -> bool:
    proc = _git(root, "merge-tree", "--write-tree", integration_ref, branch)
    if proc.returncode in (0, 1):
        return proc.returncode == 0
    raise CensusError(f"git merge-tree failed for {integration_ref}..{branch}: {(proc.stderr or proc.stdout).strip()}")


def _conflicts(root: Path, left: str, right: str) -> bool:
    return not _merge_tree_clean(root, right, left)


def colour_classes(branches: Iterable[str], conflict_edges: Iterable[Sequence[str]]) -> dict[int, list[str]]:
    """Greedily colour a conflict graph, highest degree first."""

    names = sorted(set(branches))
    adjacency: dict[str, set[str]] = {branch: set() for branch in names}
    for edge in conflict_edges:
        if len(edge) != 2:
            continue
        left, right = edge
        if left in adjacency and right in adjacency and left != right:
            adjacency[left].add(right)
            adjacency[right].add(left)
    colours: dict[str, int] = {}
    for branch in sorted(names, key=lambda name: (-len(adjacency[name]), name)):
        used = {colours[neighbor] for neighbor in adjacency[branch] if neighbor in colours}
        colour = 0
        while colour in used:
            colour += 1
        colours[branch] = colour
    classes: dict[int, list[str]] = defaultdict(list)
    for branch, colour in colours.items():
        classes[colour].append(branch)
    return {colour: sorted(members) for colour, members in sorted(classes.items())}


def _classify(
    root: Path,
    branches: list[str],
    unmerged: set[str],
    integration_ref: str,
    tips: dict[str, str],
    outcomes: dict[str, list[Outcome]],
    lane_rows: set[str],
    containment: dict[str, list[str]],
) -> tuple[list[dict[str, Any]], set[str]]:
    direct: set[str] = set()
    discharge_sha: dict[str, str] = {}
    for branch in branches:
        for raw_sha, kind in outcomes.get(branch, ()):
            resolved = _resolve_commit(root, raw_sha)
            if kind == "superseded" or _is_ancestor(root, tips[branch], resolved):
                direct.add(branch)
                discharge_sha[branch] = resolved
                break
    patch_safe = {
        branch
        for branch in unmerged
        if branch in lane_rows and branch not in direct and _patch_id_absorbed(root, branch, integration_ref)
    }
    dag_safe = direct | patch_safe
    discharged = dag_safe | bad.discharged_by_reachability(containment, dag_safe & unmerged)
    rows: list[dict[str, Any]] = []
    for branch in branches:
        if branch in discharged:
            classification, evidence = "discharged", "recorded outcome or reachable safe tip"
        elif branch not in lane_rows:
            classification, evidence = "rowless", "no worktree_lanes row"
        elif branch in patch_safe:
            classification, evidence = "discharged", "git cherry found only patch-id-equivalent commits"
        elif _merge_tree_clean(root, branch, integration_ref):
            classification, evidence = "main_clean", f"git merge-tree --write-tree {integration_ref} {branch} exited 0"
        else:
            classification, evidence = (
                "conflicts_with_main",
                f"git merge-tree --write-tree {integration_ref} {branch} exited 1",
            )
        row: dict[str, Any] = {
            "branch": branch,
            "tip_sha": tips[branch],
            "classification": classification,
            "worktree_lane_row": branch in lane_rows,
            "evidence": evidence,
        }
        if branch in discharge_sha:
            row["discharge_sha"] = discharge_sha[branch]
        rows.append(row)
    return rows, discharged


def _dag_refusal_provenance(
    root: Path,
    integration_ref: str,
    candidates: Sequence[str],
    probes: Iterable[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "probes": list(probes),
        "invocation_scope": {
            "repo_root": str(root),
            "integration_ref": integration_ref,
            "candidates": list(candidates),
        },
    }


def build_census(repo_root: Path | str = ".", integration_ref: str = _DEFAULT_INTEGRATION_REF) -> dict[str, Any]:
    """Build a deterministic landing census for ``repo_root``."""

    root = Path(repo_root).expanduser().resolve()
    integration_name = integration_ref.removeprefix("refs/heads/")
    branches = [branch for branch in _all_branches(root) if branch != integration_name]
    unmerged_list = [branch for branch in _unmerged_branches(root, integration_ref) if branch in branches]
    unmerged = set(unmerged_list)
    worktrees, tips = _worktrees(root), _branch_tips(root, branches)
    outcomes, lane_rows = _outcome_store(root, branches)
    runner, probes = _recording_runner(root)
    try:
        containment = bad.containment_map(unmerged_list, runner, integration_ref=integration_ref)
    except CensusError as exc:
        raise CensusError(
            str(exc),
            dag_provenance=_dag_refusal_provenance(root, integration_ref, unmerged_list, probes),
        ) from exc
    except (bad.AncestryProbeError, bad.AncestryCycleError) as exc:
        raise CensusError(
            str(exc),
            dag_provenance=_dag_refusal_provenance(root, integration_ref, unmerged_list, probes),
        ) from exc
    maximal = bad.maximal_elements(containment)
    reap_order = bad.reap_order(containment)
    transitive = {branch: sorted(bad.transitive_ancestors(containment, branch)) for branch in unmerged_list}
    rows, discharged = _classify(root, branches, unmerged, integration_ref, tips, outcomes, lane_rows, containment)
    by_branch = {row["branch"]: row["classification"] for row in rows}
    candidates = [branch for branch in unmerged_list if branch not in discharged]
    conflict_edges = [
        [left, right]
        for index, left in enumerate(candidates)
        for right in candidates[index + 1 :]
        if _conflicts(root, left, right)
    ]
    classes = colour_classes(candidates, conflict_edges)
    colour = {branch: number for number, members in classes.items() for branch in members}
    merged = sorted(set(branches) - unmerged)
    totals = {"branches": len(rows), "unmerged": len(unmerged), "merged": len(merged)}
    totals.update(
        {
            name: sum(value == name for value in by_branch.values())
            for name in ("main_clean", "conflicts_with_main", "discharged", "rowless")
        }
    )
    dag_provenance = {
        "probes": list(probes),
        "containment": containment,
        "reap_order": reap_order,
        "maximal": maximal,
    }
    return {
        "repo_root": str(root),
        "integration_ref": integration_ref,
        "branches": branches,
        "unmerged": sorted(unmerged),
        "merged": merged,
        "worktrees": worktrees,
        "containment": containment,
        "maximal": maximal,
        "maximals": maximal,
        "reap_order": reap_order,
        "transitive_ancestors": transitive,
        "discharged": sorted(branch for branch, value in by_branch.items() if value == "discharged"),
        "main_clean": sorted(branch for branch, value in by_branch.items() if value == "main_clean"),
        "conflicts_with_main": sorted(branch for branch, value in by_branch.items() if value == "conflicts_with_main"),
        "rowless": sorted(branch for branch, value in by_branch.items() if value == "rowless"),
        "conflict_edges": conflict_edges,
        "colour_classes": {str(number): members for number, members in classes.items()},
        "colour": colour,
        "rows": rows,
        "totals": totals,
        "invocation_scope": {
            "repo_root": str(root),
            "integration_ref": integration_ref,
            "candidates": list(unmerged_list),
        },
        "dag_provenance": dag_provenance,
    }


def _human_table(census: dict[str, Any]) -> str:
    lines = ["BRANCH\tCLASSIFICATION", "-" * 56]
    lines.extend(f"{row['branch']}\t{row['classification']}" for row in census["rows"])
    totals = census["totals"]
    lines.extend(("", "totals: " + ", ".join(f"{key}={totals[key]}" for key in ("branches", "unmerged", "merged"))))
    return "\n".join(lines)


def _refusal_payload(exc: BaseException) -> dict[str, Any]:
    payload: dict[str, Any] = {"outcome": "refused", "error": str(exc)}
    provenance = getattr(exc, "dag_provenance", None)
    if provenance is not None:
        payload["dag_provenance"] = provenance
    return payload


def _emit_census_refusal(exc: BaseException, *, as_json: bool) -> int:
    payload = _refusal_payload(exc)
    if as_json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(f"landing census: {exc}", file=sys.stderr)
        for probe in (payload.get("dag_provenance") or {}).get("probes") or []:
            if probe.get("status") == "raised":
                argv = " ".join(str(part) for part in probe.get("argv") or [])
                print(
                    f"landing census: probe raised argv={argv} error={probe.get('error')}",
                    file=sys.stderr,
                )
                continue
            if int(probe.get("exit_code", 0)) == 0:
                continue
            argv = " ".join(str(part) for part in probe.get("argv") or [])
            print(
                f"landing census: probe exit {probe.get('exit_code')} argv={argv}",
                file=sys.stderr,
            )
    return 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    parser.add_argument("--integration-ref", default=_DEFAULT_INTEGRATION_REF)
    parser.add_argument("--json", action="store_true", help="emit machine-readable JSON")
    parser.add_argument(
        "--against",
        metavar="REV",
        help="candidate-relative residual mode: what each unmerged branch adds on top of REV",
    )
    parser.add_argument("--max-branches", type=int, default=500, help="typed budget for --against (RES-02)")
    args = parser.parse_args(argv)
    try:
        if args.against:
            report = residual.build_residual(args.repo, args.against, args.integration_ref, args.max_branches)
            print(json.dumps(report, indent=2, sort_keys=True) if args.json else residual.residual_table(report))
            return 0
        census = build_census(args.repo, args.integration_ref)
    except (CensusError, residual.ResidualError, bad.AncestryProbeError, bad.AncestryCycleError) as exc:
        return _emit_census_refusal(exc, as_json=args.json)
    print(json.dumps(census, indent=2, sort_keys=True) if args.json else _human_table(census))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["CensusError", "build_census", "colour_classes", "main"]
