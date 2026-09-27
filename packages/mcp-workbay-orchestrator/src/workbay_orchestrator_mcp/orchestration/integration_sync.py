"""Plan a pinned merge object, then apply its exact gated commit under the landing lock."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import uuid
from dataclasses import dataclass
from pathlib import Path

from . import orchestrator_lanes as lanes
from .landing_log import _TRAILER_LINE, _final_paragraph
from .lane_land import _conflicting_paths, _ref_oid, _verified_test

_KEYS = (
    "Workbay-Sync",
    "Workbay-Sync-Base",
    "Workbay-Sync-Integration-Before",
    "Workbay-Sync-Run",
    "Workbay-Receipt-Version",
    "Workbay-Sync-Integration",
    "Workbay-Sync-Resolution",
)
_SHA = re.compile(r"[0-9a-f]{40}")
_RUN = re.compile(r"[0-9a-f]{32}")


@dataclass(frozen=True)
class SyncReceipt:
    task_ref: str
    base_ref: str
    base_tip: str
    integration_before: str
    run_id: str
    integration_ref: str | None = None
    resolution_commit: str | None = None


def format_sync_trailers(
    task_ref, base_ref, base_tip, integration_before, run_id, integration_ref=None, resolution_commit=None
):
    for value in (task_ref, base_ref, integration_ref or "unused"):
        if not value or any(c.isspace() or ord(c) < 32 for c in value):
            raise ValueError("receipt identifiers must be nonempty tokens")
    if not _SHA.fullmatch(base_tip) or not _SHA.fullmatch(integration_before):
        raise ValueError("expected full commit hashes")
    if not _RUN.fullmatch(run_id):
        raise ValueError("expected a 32-character hexadecimal run id")
    values = [task_ref, f"{base_ref} {base_tip}", integration_before, run_id, "1"]
    if integration_ref is not None:
        values.append(integration_ref)
    trailers = [f"{key}: {value}" for key, value in zip(_KEYS, values)]
    if resolution_commit is not None:
        if not _SHA.fullmatch(resolution_commit):
            raise ValueError("expected full resolution commit hash")
        trailers.append(f"Workbay-Sync-Resolution: {resolution_commit}")
    return trailers


def parse_sync_receipt(message):
    fields = {}
    for line in _final_paragraph(message) or []:
        match = _TRAILER_LINE.fullmatch(line.rstrip())
        if match is None:
            return None
        key, value = match.groups()
        if key in _KEYS:
            if key in fields:
                return None
            fields[key] = value
    try:
        base, tip = fields[_KEYS[1]].split(" ")
        receipt = SyncReceipt(
            fields[_KEYS[0]], base, tip, fields[_KEYS[2]], fields[_KEYS[3]], fields.get(_KEYS[5]), fields.get(_KEYS[6])
        )
        if fields[_KEYS[4]] != "1":
            return None
        format_sync_trailers(
            receipt.task_ref,
            base,
            tip,
            receipt.integration_before,
            receipt.run_id,
            receipt.integration_ref,
            receipt.resolution_commit,
        )
        return receipt
    except (KeyError, ValueError, TypeError):
        return None


def _result(outcome, detail=None, **fields):
    return dict(
        ok=outcome in {"planned", "scaffolded", "no_conflict", "synced", "already_synced", "up_to_date"},
        outcome=outcome,
        detail=detail,
        **fields,
    )


def _git(root, *args):
    return lanes._run_no_ff_git(root, *args)


def _oid(root, ref):
    value, error = _ref_oid(lanes, root, ref)
    if error or value is None:
        raise ValueError(f"cannot resolve {ref}")
    return value


def _branch(root, ref):
    full = ref if ref.startswith("refs/heads/") else f"refs/heads/{ref}"
    if _git(root, "check-ref-format", full).returncode:
        raise ValueError("invalid branch ref")
    return full


def _receipt(root, candidate):
    proc = _git(root, "show", "-s", "--format=%B", candidate)
    return parse_sync_receipt(proc.stdout) if proc.returncode == 0 else None


def plan(
    *,
    workspace_root,
    task_ref,
    integration_ref,
    expected_integration_tip,
    expected_base_tip,
    base="main",
    run_id=None,
    resolution_commit=None,
):
    run_id = run_id or uuid.uuid4().hex
    root = Path(workspace_root)
    try:
        branch = _branch(root, integration_ref)
        format_sync_trailers(
            task_ref, base, expected_base_tip, expected_integration_tip, run_id, branch, resolution_commit
        )
        before, tip = _oid(root, branch), _oid(root, base)
        if (before, tip) != (expected_integration_tip, expected_base_tip):
            return _result("stale_expectation")
        pin = f"refs/workbay/sync/{run_id}"
        existing, error = _ref_oid(lanes, root, pin)
        if error:
            return _result("merge_refused", "pin_probe_failed")
        desired = SyncReceipt(task_ref, base, tip, before, run_id, branch, resolution_commit)
        if existing:
            if _receipt(root, existing) != desired:
                return _result("run_id_conflict")
            return _result(
                "planned",
                candidate=existing,
                tree=_oid_tree(root, existing),
                integration_before=before,
                base_tip=tip,
                run_id=run_id,
            )
        ancestor = _git(root, "merge-base", "--is-ancestor", tip, before)
        if ancestor.returncode == 0:
            return _result("resolution_invalid", "no_conflict") if resolution_commit else _result("up_to_date")
        if ancestor.returncode != 1:
            return _result("merge_refused", "ancestor_probe_failed")
        probe = _git(root, "merge-tree", "--write-tree", "--name-only", before, tip)
        if probe.returncode == 1 and resolution_commit is None:
            return _result("conflict", conflicting_paths=_conflicting_paths(probe.stdout, probe.stderr))
        if probe.returncode not in (0, 1):
            return _result("merge_refused", "merge_tree_probe_failed")
        tree = probe.stdout.splitlines()[0]
        if resolution_commit is not None:
            if probe.returncode == 0:
                return _result("resolution_invalid", "no_conflict")
            tree, reason = _resolution_tree(root, desired)
            if reason:
                return _result("resolution_invalid", reason)
        message = f"Merge {base} {tip[:10]} into {integration_ref}\n\n" + "\n".join(
            format_sync_trailers(task_ref, base, tip, before, run_id, branch, resolution_commit)
        )
        commit = _git(root, "commit-tree", tree, "-p", before, "-p", tip, "-m", message)
        if commit.returncode:
            return _result("merge_refused", "commit_tree_failed")
        candidate = commit.stdout.strip()
        pinned = _git(root, "update-ref", pin, candidate, "0" * 40)
        if pinned.returncode:
            existing = _oid(root, pin)
            if _receipt(root, existing) != desired:
                return _result("run_id_conflict")
            candidate = existing
        return _result(
            "planned", candidate=candidate, tree=tree, integration_before=before, base_tip=tip, run_id=run_id
        )
    except (ValueError, OSError, subprocess.TimeoutExpired) as exc:
        return _result("merge_refused", str(exc))


def format_scaffold_trailers(task_ref, base_ref, base_tip, integration_before, run_id, conflicting_paths):
    format_sync_trailers(task_ref, base_ref, base_tip, integration_before, run_id)
    return [
        f"Workbay-Sync-Scaffold: {task_ref}",
        f"Workbay-Sync-Base: {base_ref} {base_tip}",
        f"Workbay-Sync-Integration-Before: {integration_before}",
        f"Workbay-Sync-Run: {run_id}",
        f"Workbay-Sync-Conflicts: {','.join(conflicting_paths)}",
    ]


def _scaffold_fields(root, commit):
    proc = _git(root, "show", "-s", "--format=%B", commit)
    fields = {}
    if proc.returncode:
        return fields
    for line in _final_paragraph(proc.stdout) or []:
        match = _TRAILER_LINE.fullmatch(line)
        if match is None or match[1] in fields:
            return {}
        fields[match[1]] = match[2]
    return fields


def scaffold(
    *, workspace_root, task_ref, integration_ref, expected_integration_tip, expected_base_tip, base="main", run_id=None
):
    root = Path(workspace_root)
    run_id = run_id or uuid.uuid4().hex
    try:
        branch = _branch(root, integration_ref)
        format_sync_trailers(task_ref, base, expected_base_tip, expected_integration_tip, run_id, branch)
        before, tip = _oid(root, branch), _oid(root, base)
        if (before, tip) != (expected_integration_tip, expected_base_tip):
            return _result("stale_expectation")
        pin = f"refs/workbay/sync-scaffold/{run_id}"
        existing, error = _ref_oid(lanes, root, pin)
        if error:
            return _result("merge_refused", "pin_probe_failed")
        if existing:
            fields = _scaffold_fields(root, existing)
            paths = fields.get("Workbay-Sync-Conflicts", "").split(",")
            trailers = format_scaffold_trailers(task_ref, base, tip, before, run_id, paths)
            message = f"Sync scaffold: {base} {tip[:10]} into {integration_ref}\n\n" + "\n".join(trailers)
            if paths == [""] or _git(root, "show", "-s", "--format=%B", existing).stdout.strip() != message:
                return _result("run_id_conflict")
            return _result(
                "scaffolded",
                scaffold=existing,
                conflicting_paths=paths,
                integration_before=before,
                base_tip=tip,
                run_id=run_id,
            )
        ancestor = _git(root, "merge-base", "--is-ancestor", tip, before)
        if ancestor.returncode == 0:
            return _result("up_to_date")
        if ancestor.returncode != 1:
            return _result("merge_refused", "ancestor_probe_failed")
        probe = _git(root, "merge-tree", "--write-tree", before, tip)
        if probe.returncode == 0:
            return _result("no_conflict")
        if probe.returncode != 1:
            return _result("merge_refused", "merge_tree_probe_failed")
        # Stage records precede the blank line and conflict descriptions.
        records = probe.stdout.split("\n\n", 1)[0].splitlines()[1:]
        paths = sorted({line.split("\t", 1)[1] for line in records if "\t" in line})
        if not paths or any("," in path or "\n" in path or path.startswith('"') for path in paths):
            return _result("merge_refused", "unsupported_conflict_paths")
        tree = probe.stdout.splitlines()[0]
        trailers = format_scaffold_trailers(task_ref, base, tip, before, run_id, paths)
        message = f"Sync scaffold: {base} {tip[:10]} into {integration_ref}\n\n" + "\n".join(trailers)
        pin = f"refs/workbay/sync-scaffold/{run_id}"
        existing, error = _ref_oid(lanes, root, pin)
        if error:
            return _result("merge_refused", "pin_probe_failed")
        if not existing:
            commit = _git(root, "commit-tree", tree, "-p", before, "-m", message)
            if commit.returncode:
                return _result("merge_refused", "commit_tree_failed")
            existing = commit.stdout.strip()
            if _git(root, "update-ref", pin, existing, "0" * 40).returncode:
                existing = _oid(root, pin)
        if _git(root, "show", "-s", "--format=%B", existing).stdout.strip() != message:
            return _result("run_id_conflict")
        return _result(
            "scaffolded",
            scaffold=existing,
            conflicting_paths=paths,
            integration_before=before,
            base_tip=tip,
            run_id=run_id,
        )
    except (ValueError, OSError, subprocess.TimeoutExpired) as exc:
        return _result("merge_refused", str(exc))


def _resolution_tree(root, receipt):
    scaffold_sha, error = _ref_oid(lanes, root, f"refs/workbay/sync-scaffold/{receipt.run_id}")
    if error or not scaffold_sha:
        return None, "scaffold_missing"
    fields = _scaffold_fields(root, scaffold_sha)
    paths = fields.get("Workbay-Sync-Conflicts", "").split(",")
    expected = dict(
        line.split(": ", 1)
        for line in format_scaffold_trailers(
            receipt.task_ref, receipt.base_ref, receipt.base_tip, receipt.integration_before, receipt.run_id, paths
        )
    )
    if fields != expected or paths == [""]:
        return None, "scaffold_mismatch"
    resolution = receipt.resolution_commit
    if _git(root, "merge-base", "--is-ancestor", scaffold_sha, resolution).returncode:
        return None, "not_descended_from_scaffold"
    diff = _git(root, "diff", "--no-renames", "--name-only", "-z", scaffold_sha, resolution)
    if diff.returncode:
        return None, "diff_probe_failed"
    for path in diff.stdout.split("\0"):
        if path and path not in paths:
            return None, f"touched_non_conflict_path:{path}"
    for path in paths:
        entry = _git(root, "ls-tree", resolution, "--", path)
        if entry.returncode:
            return None, "tree_probe_failed"
        if not entry.stdout:
            continue  # Deletion is a valid conflict resolution.
        content = _git(root, "show", f"{resolution}:{path}")
        if content.returncode:
            return None, f"content_probe_failed:{path}"
        if re.search(r"^(<<<<<<< |=======$|>>>>>>> )", content.stdout, re.MULTILINE):
            return None, f"markers_remain:{path}"
    return _oid_tree(root, resolution), None


def _oid_tree(root, commit):
    proc = _git(root, "rev-parse", f"{commit}^{{tree}}")
    if proc.returncode:
        raise ValueError("tree probe failed")
    return proc.stdout.strip()


def apply(*, workspace_root, task_ref, integration_ref, candidate, run_id, base="main"):
    root = Path(workspace_root)
    try:
        if not _SHA.fullmatch(candidate) or not _RUN.fullmatch(run_id):
            return _result("invalid_request")
        branch = _branch(root, integration_ref)
        with lanes._landing_mutation_lock(root) as acquired:
            if acquired is not True:
                return _result("lock_held", "landing_lock_unavailable" if acquired is None else "mutation_lock_held")
            receipt = _receipt(root, candidate)
            if (
                receipt is None
                or receipt.run_id != run_id
                or receipt.task_ref != task_ref
                or receipt.base_ref != base
                or receipt.integration_ref != branch
            ):
                return _result("merge_refused", "invalid_sync_receipt")
            if _oid(root, f"refs/workbay/sync/{run_id}") != candidate:
                return _result("run_id_conflict")
            parents = _git(root, "show", "-s", "--format=%P", candidate)
            if parents.returncode or parents.stdout.strip().split() != [receipt.integration_before, receipt.base_tip]:
                return _result("merge_refused", "invalid_sync_parents")
            current = _oid(root, branch)
            if current == candidate:
                return _result("already_synced", landing_commit=candidate, run_id=run_id)
            if current != receipt.integration_before or _oid(root, base) != receipt.base_tip:
                return _result("stale_expectation")
            import workbay_handoff_mcp.verified_tests as verified_tests

            try:
                raw = verified_tests.get_verified_tests(task_ref=task_ref, commit_sha=candidate, passed=True, limit=1)
                gate_id, readable = _verified_test(raw, candidate)
            except Exception:
                gate_id, readable = None, False
            if gate_id is None:
                return _result("gate_missing", None if readable else "verified_tests_unreadable")
            worktrees = _git(root, "worktree", "list", "--porcelain", "-z")
            if worktrees.returncode:
                return _result("merge_refused", "worktree_probe_failed")
            target = None
            path = None
            for field in worktrees.stdout.split("\0"):
                if field.startswith("worktree "):
                    path = Path(field[9:])
                elif field == f"branch {branch}":
                    target = path
            if target:
                status = _git(target, "status", "--porcelain", "--untracked-files=no")
                merge = _git(target, "rev-parse", "--git-path", "MERGE_HEAD")
                if status.returncode or merge.returncode:
                    return _result("dirty_target", "target_probe_failed")
                merge_path = Path(merge.stdout.strip())
                if not merge_path.is_absolute():
                    merge_path = target / merge_path
                if status.stdout.strip() or merge_path.exists():
                    return _result("dirty_target")
                # Recheck after the external gate read, before mutating the worktree.
                if _oid(root, branch) != current or _oid(root, base) != receipt.base_tip:
                    return _result("stale_expectation")
                moved = _git(target, "merge", "--ff-only", "--no-overwrite-ignore", candidate)
            else:
                if _oid(root, base) != receipt.base_tip:
                    return _result("stale_expectation")
                moved = _git(root, "update-ref", branch, candidate, current)
            if moved.returncode or _oid(root, branch) != candidate:
                return _result("merge_refused", "fast_forward_failed")
            return _result("synced", landing_commit=candidate, gate_id=gate_id, run_id=run_id)
    except (ValueError, OSError, subprocess.TimeoutExpired) as exc:
        return _result("merge_refused", str(exc))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=["plan", "scaffold", "apply"])
    parser.add_argument("--workspace-root", required=True)
    parser.add_argument("--task", dest="task_ref", required=True)
    parser.add_argument("--integration-ref", required=True)
    parser.add_argument("--base", default="main")
    parser.add_argument("--run-id")
    parser.add_argument("--expected-integration-tip")
    parser.add_argument("--expected-base-tip")
    parser.add_argument("--candidate")
    parser.add_argument("--resolution-commit")
    args = vars(parser.parse_args(argv))
    phase = args.pop("phase")
    required = ("expected_integration_tip", "expected_base_tip") if phase != "apply" else ("candidate", "run_id")
    if any(args[key] is None for key in required):
        result = _result("invalid_request", "missing phase arguments")
    else:
        from ..api import configure_runtime
        from ..cli import _build_config

        workspace_root = Path(args["workspace_root"]).expanduser().resolve()
        configure_runtime(_build_config(workspace_root))
        for key in ("candidate",) if phase != "apply" else ("expected_integration_tip", "expected_base_tip"):
            args.pop(key)
        if phase != "plan":
            args.pop("resolution_commit")
        result = {"plan": plan, "scaffold": scaffold, "apply": apply}[phase](**args)
    print(json.dumps(result))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
