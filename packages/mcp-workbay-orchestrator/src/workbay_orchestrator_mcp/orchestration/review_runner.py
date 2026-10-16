#!/usr/bin/env python3
"""Self-review runner: build a review prompt, execute Codex, validate findings, optionally record to MCP."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
import sqlite3
import subprocess
import sys
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Any, Callable, Literal, Mapping, NotRequired, TypedDict

PACKAGE_SRC = Path(__file__).resolve().parents[2]
if str(PACKAGE_SRC) not in sys.path:
    sys.path.insert(0, str(PACKAGE_SRC))

from workbay_orchestrator_mcp.orchestration._env import WORKER_REASONING_EFFORT_CHOICES, apply_backend_runtime_hints
from workbay_orchestrator_mcp.orchestration.backend_registry import (
    backend_supports_adapter_timeout_bounds,
    backend_supports_token_budget_cycle_bounds,
    get_adapter,
    get_backend_choices,
    get_backend_spec,
    validate_backend,
)
from workbay_orchestrator_mcp.orchestration.lane_escalation import EscalationSkipReason
from workbay_orchestrator_mcp.orchestration.lane_manifest import get_lane_config

if TYPE_CHECKING:
    from workbay_handoff_mcp.core import ReviewFindingDetails, WriteActor
    from workbay_handoff_mcp.enums import ReviewKind, ReviewScopeSource
    from workbay_handoff_mcp.review_findings import BatchFindingItem

REPO_ROOT = PACKAGE_SRC.parents[2]
from workbay_orchestrator_mcp._assets import bundled_rules_dir  # noqa: E402

DEFAULT_RULES_DIR = bundled_rules_dir()
RULES_DIR = DEFAULT_RULES_DIR

BACKEND_CHOICES = get_backend_choices()
logger = logging.getLogger(__name__)

MAX_REVIEW_DIFF_BYTES = 120_000
REVIEW_GIT_TIMEOUT_SECONDS = 15

RELATED_PRIOR_WORK_LIMIT = 5
RELATED_PRIOR_WORK_SNIPPET_MAX_CHARS = 400
logger = logging.getLogger(__name__)

# Stack guide selection by file extension
STACK_GUIDES: dict[str, str] = {
    ".py": "branch-review-python.md",
    ".ts": "branch-review-typescript.md",
    ".tsx": "branch-review-typescript.md",
    ".js": "branch-review-typescript.md",
    ".jsx": "branch-review-typescript.md",
    ".php": "branch-review-php.md",
}

REVIEW_OUTPUT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["findings", "summary", "result_parse"],
    "properties": {
        "findings": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": [
                    "severity",
                    "category",
                    "file_path",
                    "line_start",
                    "line_end",
                    "description",
                    "fix",
                ],
                "properties": {
                    "severity": {"type": "string", "enum": ["high", "medium", "low"]},
                    "category": {
                        "type": "string",
                        "enum": ["ANTIPATTERN", "DEAD_CODE", "COMPLEXITY", "GAP"],
                    },
                    "file_path": {"type": "string"},
                    "line_start": {"type": ["integer", "null"]},
                    "line_end": {"type": ["integer", "null"]},
                    "description": {"type": "string", "minLength": 1},
                    "fix": {"type": ["string", "null"]},
                },
            },
        },
        "summary": {"type": "string", "minLength": 1},
        # implementation note D9: harvest skips any result whose result_parse is not "ok".
        # Null means "ok" for clean reviews under strict structured outputs.
        "result_parse": {
            "type": ["string", "null"],
            "enum": ["ok", "degraded", None],
        },
    },
}


def strict_output_schema_violations(schema: dict[str, Any]) -> list[str]:
    """Return recursive object-shape violations for a strict output schema."""
    violations: list[str] = []

    def walk(node: Any, path: tuple[str | int, ...]) -> None:
        if isinstance(node, list):
            for index, child in enumerate(node):
                walk(child, (*path, index))
            return
        if not isinstance(node, dict):
            return

        node_type = node.get("type")
        is_object = (
            node_type == "object" or (isinstance(node_type, list) and "object" in node_type) or "properties" in node
        )
        location = "$" + "".join(f"[{part}]" if isinstance(part, int) else f".{part}" for part in path)
        if is_object:
            if node.get("additionalProperties") is not False:
                violations.append(f"{location}: additionalProperties must be false")
            properties = node.get("properties")
            required = node.get("required")
            property_names = set(properties) if isinstance(properties, dict) else set()
            required_names = set(required) if isinstance(required, list) else set()
            if not isinstance(properties, dict):
                violations.append(f"{location}: properties must be an object")
            if not isinstance(required, list):
                violations.append(f"{location}: required must be an array")
            elif property_names != required_names:
                missing = sorted(property_names - required_names)
                extra = sorted(required_names - property_names)
                violations.append(
                    f"{location}: required must include exactly every properties key (missing={missing}, extra={extra})"
                )

        for key, child in node.items():
            walk(child, (*path, key))

    walk(schema, ())
    return violations


_REVIEW_DOCUMENT_KEYS = frozenset({"findings", "summary", "result_parse"})
_REVIEW_FINDING_KEYS = frozenset({"severity", "category", "file_path", "line_start", "line_end", "description", "fix"})
_REVIEW_SEVERITIES = frozenset({"high", "medium", "low"})
_REVIEW_CATEGORIES = frozenset({"ANTIPATTERN", "DEAD_CODE", "COMPLEXITY", "GAP"})
_BLOCKING_SEVERITIES = frozenset({"HIGH", "MEDIUM"})


def _json_schema_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def review_schema_document(payload: Any) -> dict[str, Any] | None:
    """Return a copy of ``payload`` when it matches ``REVIEW_OUTPUT_SCHEMA``.

    Strict: extra keys fail, and booleans are not integers. A self-reported
    ``result_parse`` of ``degraded`` is valid when the rest of the document is.
    """
    if not isinstance(payload, dict) or set(payload) != _REVIEW_DOCUMENT_KEYS:
        return None
    summary = payload.get("summary")
    if not isinstance(summary, str) or not summary:
        return None
    result_parse = payload.get("result_parse")
    if result_parse not in {"ok", "degraded", None}:
        return None
    findings = payload.get("findings")
    if not isinstance(findings, list):
        return None
    normalized: list[dict[str, Any]] = []
    for item in findings:
        if not isinstance(item, dict) or set(item) != _REVIEW_FINDING_KEYS:
            return None
        if item.get("severity") not in _REVIEW_SEVERITIES:
            return None
        if item.get("category") not in _REVIEW_CATEGORIES:
            return None
        if not isinstance(item.get("file_path"), str):
            return None
        if item.get("line_start") is not None and not _json_schema_int(item.get("line_start")):
            return None
        if item.get("line_end") is not None and not _json_schema_int(item.get("line_end")):
            return None
        description = item.get("description")
        if not isinstance(description, str) or not description:
            return None
        fix = item.get("fix")
        if fix is not None and not isinstance(fix, str):
            return None
        normalized.append(dict(item))
    return {"findings": normalized, "summary": summary, "result_parse": result_parse}


def schema_degraded_findings(result: Any) -> list[dict[str, Any]] | None:
    """Findings on a schema-valid self-reported degraded review, else None.

    Empty findings stay unharvestable: a degraded document with nothing to
    record is not a clean review (OBS-08).
    """
    document = review_schema_document(result)
    if document is None or document.get("result_parse") != "degraded":
        return None
    findings = document.get("findings")
    if not isinstance(findings, list) or not findings:
        return None
    return findings


def review_result_is_harvestable(result: dict[str, Any] | None) -> bool:
    """Return True when a review result may enter the findings harvest (D9).

    Accepts review-runner output (top-level ``result_parse``) and worker
    ``BackendResult.to_dict()`` terminals (``blockers`` /
    ``raw_payload.result_parse`` / ``raw_payload.result_degraded``).
    Absent or null ``result_parse`` means ``ok`` for clean reviews.
    A schema-valid document that self-reports ``degraded`` and carries findings
    is harvestable; the self-report stays visible and is not a marker.
    """
    if not isinstance(result, dict):
        return False
    if schema_degraded_findings(result):
        return True
    parse = result.get("result_parse")
    if parse is not None and parse != "ok":
        return False
    blockers = result.get("blockers")
    if isinstance(blockers, list) and "result_unparseable" in blockers:
        return False
    raw = result.get("raw_payload")
    if isinstance(raw, dict):
        if raw.get("result_degraded") is True:
            return False
        raw_parse = raw.get("result_parse")
        if raw_parse is not None and raw_parse != "ok":
            return False
    return True


# In-lane smoke review is non-authoritative for grok-cli; unparseable output is an
# expected backend-capability miss, not an engine error ([OBS-08] typed degradation).
REVIEW_SKIPPED_UNPARSEABLE = "skipped_unparseable"
_RAW_TAIL_CHARS = 800


class ReviewAdmissionDeferred(RuntimeError):
    """The review turn was deferred by remote VM admission (retryable).

    Raised when the review backend's result carries the ``admission_deferred``
    marker (remote_agent.sh exit 75): no review ran, so the payload must not
    tolerant-degrade to ``skipped_unparseable`` → converged. Callers translate
    this into the retryable ``admission_deferred`` outcome
    (``worker_daemon._review_phase`` → ``StopIteration("admission_deferred")``)
    instead of a terminal review error.
    """

    def __init__(self, message: str, *, defer_reason: str | None = None) -> None:
        self.defer_reason = defer_reason
        super().__init__(message)


class ReviewTransportFailure(RuntimeError):
    """A review backend failed before producing a review product."""

    def __init__(self, *, provider_error_code: str, retryable: bool) -> None:
        self.provider_error_code = provider_error_code
        self.retryable = retryable
        super().__init__(
            f"review transport failure ({provider_error_code}; retryable={str(retryable).lower()}), no review output"
        )


class ReviewNoProduct(RuntimeError):
    """The review backend returned a typed non-product envelope.

    Raised when exhausted empty salvage is stamped on a non-empty
    ``needs_guidance`` payload (``salvage_patch_cause=patch_empty`` and
    ``redispatch_exhausted=true``): no review ran, so the payload must not
    tolerant-degrade to ``skipped_unparseable`` → converged. Distinct from
    provider-refusal ``ReviewTransportFailure``.
    """


def _is_exhausted_empty_salvage(raw_result: Any) -> bool:
    """True when the backend stamped exhausted empty salvage, not a review product.

    Branch on the public cause fields, never on the provider-refusal action.
    """
    return (
        isinstance(raw_result, dict)
        and raw_result.get("salvage_patch_cause") == "patch_empty"
        and raw_result.get("redispatch_exhausted") is True
    )


def finding_stable_id(finding: dict[str, Any]) -> str:
    """Stable identity for a review finding across fix cycles.

    Shared by ``findings_repeated_state`` and ``worker_daemon._finding_stable_id``
    so both callers use one scheme: severity:category:file_path:line_start.
    """
    return (
        f"{finding.get('severity', '')}:"
        f"{finding.get('category', '')}:"
        f"{finding.get('file_path', '')}:"
        f"{finding.get('line_start', 0)}"
    )


def findings_converged(findings: list[dict[str, Any]], *, review_subject: Mapping[str, Any] | None = None) -> bool:
    """Return True when findings contain zero HIGH and zero MEDIUM issues.

    LOW findings remain advisory and do not keep the review cycle running.
    An unresolved subject cannot establish convergence, even with no findings.
    """
    if review_subject is not None and review_subject.get("scope_kind") == "unresolved_base":
        return False
    high = sum(1 for f in findings if f.get("severity") == "high")
    medium = sum(1 for f in findings if f.get("severity") == "medium")
    return high == 0 and medium == 0


def blocking_findings(findings: Any) -> list[dict[str, Any]]:
    """HIGH and MEDIUM findings, using the review loop's case-insensitive rule.

    This is the single severity gate for another rework cycle and for residual
    classification. ``findings_converged`` stays case-sensitive and is not
    reimplemented here.
    """
    if not isinstance(findings, (list, tuple)):
        return []
    return [
        item
        for item in findings
        if isinstance(item, dict) and str(item.get("severity") or "").strip().upper() in _BLOCKING_SEVERITIES
    ]


def residual_max_severity(findings: list[dict[str, Any]]) -> str:
    """Highest residual severity from ``blocking_findings`` (high, else medium, else low)."""
    blocking = blocking_findings(findings)
    if any(str(item.get("severity") or "").strip().upper() == "HIGH" for item in blocking):
        return "high"
    if blocking:
        return "medium"
    return "low"


def _findings_within_lane_scope(
    findings: list[dict[str, Any]],
    *,
    owned_paths: Any,
    changed_files: Any,
) -> tuple[list[dict[str, Any]], int]:
    """Return in-scope findings and the number excluded by lane scope.

    ``owned_paths`` may contain files or directory roots. ``changed_files`` is
    the exact diff scope, so a finding is included when its path matches one of
    those files or is beneath an owned root.
    ``changed_files=None`` means unknown scope: retain every finding unresolved.
    """
    if changed_files is None:
        return [dict(finding, scope="unresolved") for finding in findings], 0

    def _normalize(value: Any, *, allow_repo_root: bool = False) -> str | None:
        if not isinstance(value, str):
            return None
        text = value.strip().replace("\\", "/")
        if not text:
            return "" if allow_repo_root else None
        if text.startswith("/") or re.match(r"^[A-Za-z]:", text):
            return None
        path = PurePosixPath(text)
        if ".." in path.parts:
            return None
        normalized = path.as_posix()
        if normalized in {"", "."}:
            return "" if allow_repo_root else None
        return normalized

    raw_owned = owned_paths if isinstance(owned_paths, (list, tuple, set)) else []
    owned = [path for value in raw_owned if (path := _normalize(value, allow_repo_root=True)) is not None]
    raw_changed = changed_files if isinstance(changed_files, (list, tuple, set)) else []
    changed = {path for value in raw_changed if (path := _normalize(value)) is not None}

    in_scope: list[dict[str, Any]] = []
    excluded_count = 0
    for finding in findings:
        file_path = _normalize(finding.get("file_path"))
        if file_path is not None and (
            file_path in changed
            or "" in owned
            or any(file_path == root or file_path.startswith(root + "/") for root in owned)
        ):
            in_scope.append(finding)
        else:
            excluded_count += 1
    return in_scope, excluded_count


def _out_of_diff_findings(findings: list[dict[str, Any]], changed_files: Any) -> list[dict[str, Any]]:
    """Return typed findings whose paths are not part of the lane's changed-file set."""
    if changed_files is None:
        return []

    def normalize(value: Any) -> str | None:
        if not isinstance(value, str):
            return None
        text = value.strip().replace("\\", "/")
        if not text or text.startswith("/") or re.match(r"^[A-Za-z]:", text):
            return None
        path = PurePosixPath(text)
        if ".." in path.parts:
            return None
        normalized = path.as_posix()
        return normalized if normalized not in {"", "."} else None

    raw_changed = changed_files if isinstance(changed_files, (list, tuple, set)) else []
    changed = {path for value in raw_changed if (path := normalize(value)) is not None}
    return [finding for finding in findings if normalize(finding.get("file_path")) not in changed]


def findings_repeated_state(prev_finding_ids: set[str], findings: list[dict[str, Any]]) -> bool:
    """Return True when the current finding set is identical to the previous cycle.

    Identical means nothing new appeared and nothing was resolved, using the
    shared ``finding_stable_id`` scheme. An unparseable review is not a
    repeated state — callers must not feed ``skipped_unparseable`` into this.
    """
    current_ids = {finding_stable_id(f) for f in findings}
    return current_ids == set(prev_finding_ids)


# ---------------------------------------------------------------------------
# Changed-file discovery
# ---------------------------------------------------------------------------


class ReviewScopeDiscoveryError(RuntimeError):
    """Git could not measure the review subject; no review product is safe to emit."""


def _run_review_git(worktree_path: Path, *args: str) -> subprocess.CompletedProcess[str]:
    """Bound every review Git read; a stalled read leaves the subject unknown."""
    try:
        proc = subprocess.run(
            ["git", *args],
            cwd=worktree_path,
            capture_output=True,
            # NUL-delimited paths must bypass universal newline conversion:
            # carriage returns and newlines can both be literal pathname bytes.
            text="-z" not in args,
            check=False,
            timeout=REVIEW_GIT_TIMEOUT_SECONDS,
        )
        if isinstance(proc.stdout, bytes):
            return subprocess.CompletedProcess(
                proc.args, proc.returncode, os.fsdecode(proc.stdout), os.fsdecode(proc.stderr)
            )
        return proc
    except subprocess.TimeoutExpired as exc:
        raise ReviewScopeDiscoveryError(f"review scope git command timed out: git {' '.join(args)}") from exc
    except OSError as exc:
        raise ReviewScopeDiscoveryError(f"review scope git command could not run: git {' '.join(args)}") from exc


def _git_stdout(worktree_path: Path, *args: str) -> str | None:
    """Return Git stdout (raw for NUL lists), None on failure, or a typed read error."""
    proc = _run_review_git(worktree_path, *args)
    if proc.returncode != 0:
        return None
    return proc.stdout if "-z" in args else proc.stdout.strip()


def _required_git_stdout(worktree_path: Path, *args: str) -> str:
    """Read required scope data without converting a failed command to empty data."""
    stdout = _git_stdout(worktree_path, *args)
    if stdout is None:
        raise ReviewScopeDiscoveryError(f"review scope git command failed: git {' '.join(args)}")
    return stdout


def _changed_files(worktree_path: Path) -> list[str]:
    """Return workspace-relative paths of UNCOMMITTED changes in the lane worktree."""
    result = _run_review_git(worktree_path, "diff", "--name-only", "HEAD", "-z")
    staged = _run_review_git(worktree_path, "diff", "--name-only", "--cached", "-z")
    untracked = _run_review_git(worktree_path, "ls-files", "--others", "--exclude-standard", "-z")
    files: set[str] = set()
    for proc in (result, staged, untracked):
        if proc.returncode != 0:
            raise ReviewScopeDiscoveryError(f"review scope git command failed: {proc.args}")
        files.update(path for path in proc.stdout.split("\0") if path)
    return sorted(f for f in files if not f.startswith(".review/"))


def _synthetic_root_base(worktree_path: Path) -> str | None:
    """Return a verified remote-agent synthetic root, never an ordinary repo root."""
    roots = _required_git_stdout(worktree_path, "rev-list", "--max-parents=0", "HEAD")
    if not roots:
        return None
    commits = [commit for commit in roots.splitlines() if commit]
    if len(commits) != 1:
        return None
    subject = _required_git_stdout(worktree_path, "show", "-s", "--format=%s", commits[0])
    if not subject.startswith("sandbox base (") or not subject.endswith("history-stripped, remote-severed)"):
        return None
    return commits[0]


def _resolve_committed_base(worktree_path: Path, lane_base_sha: str | None = None) -> tuple[str, str] | None:
    """Resolve ``(base_sha, reason)`` for the lane branch's committed diff.

    Prefer a valid ancestral lane SHA, then ``main``, ``origin/main``, ``master``.
    When those miss (repo
    whose trunk is not main/master), falls back to the origin default branch via
    ``refs/remotes/origin/HEAD`` (e.g. ``origin/develop``). Returns None when no
    base resolves (detached repo without a discoverable base, or git failure).
    """
    if isinstance(lane_base_sha, str) and re.fullmatch(r"[0-9a-fA-F]{40}", lane_base_sha):
        commit = _git_stdout(worktree_path, "rev-parse", "--verify", f"{lane_base_sha}^{{commit}}")
        if commit and _git_stdout(worktree_path, "merge-base", "--is-ancestor", commit, "HEAD") is not None:
            return commit, f"lane base_sha {commit[:10]}"
    for base in ("main", "origin/main", "master"):
        if _git_stdout(worktree_path, "rev-parse", "--verify", "--quiet", base) is None:
            continue
        merge_base = _git_stdout(worktree_path, "merge-base", base, "HEAD")
        if merge_base:
            return merge_base, f"fallback merge-base({base})"
    # Trunk is not main/master: use origin's default branch when configured.
    origin_head = _git_stdout(worktree_path, "symbolic-ref", "--short", "-q", "refs/remotes/origin/HEAD")
    if origin_head:
        merge_base = _git_stdout(worktree_path, "merge-base", origin_head, "HEAD")
        if merge_base:
            return merge_base, f"fallback merge-base({origin_head})"
    return None


def _review_subject(
    worktree_path: Path,
    committed_base: tuple[str, str] | None,
    scope: Mapping[str, Any],
) -> dict[str, str]:
    """Return a typed subject matching the diff selected for this review."""
    tip = _git_stdout(worktree_path, "rev-parse", "--verify", "HEAD^{commit}")
    scope_source = getattr(scope.get("scope_source"), "value", scope.get("scope_source"))
    if scope_source == "slice_packet":
        commit_sha = scope.get("commit_sha")
        commit = (
            _git_stdout(worktree_path, "rev-parse", "--verify", f"{commit_sha}^{{commit}}")
            if isinstance(commit_sha, str) and commit_sha
            else None
        )
        if committed_base is not None and commit is not None:
            return {"kind": "bounded", "base_ref": committed_base[0], "tip_ref": commit}
        return {
            "kind": "unbounded",
            "scope_kind": "unresolved_base",
            "reason": "selected slice base or commit could not be resolved",
        }
    if committed_base is not None and tip is not None and _changed_files(worktree_path):
        return {"kind": "working_tree_diff", "base_ref": tip, "tip_ref": "worktree"}
    if committed_base is not None and tip is not None:
        return {"kind": "bounded", "base_ref": committed_base[0], "tip_ref": tip}
    synthetic_base = _synthetic_root_base(worktree_path)
    reason = (
        "no lane base or trunk ref resolved; tried lane base_sha, main, origin/main, "
        "master, refs/remotes/origin/HEAD, and recognized synthetic root"
    )
    if synthetic_base is not None:
        reason += f"; changed paths are recovered from synthetic root {synthetic_base[:10]}"
    return {
        "kind": "unbounded",
        "scope_kind": "synthetic_root" if synthetic_base is not None else "unresolved_base",
        "reason": reason,
    }


def _committed_branch_files(
    worktree_path: Path, committed_base: tuple[str, str] | None = None
) -> tuple[list[str], str | None]:
    """Committed lane scope: files changed on the branch since its base fork point.

    Fallback for a clean worktree where all lane work is already committed —
    e.g. a ported-back grok-remote commit (WIDTH2-H-01): the review must see
    the branch's committed file set, not an empty scope.
    Returns ``(files, base_reason)``; ``([], None)`` when no base resolves.
    A failed diff raises instead of masquerading as a measured empty scope.
    """
    resolved = committed_base or _resolve_committed_base(worktree_path)
    if resolved is None:
        synthetic_base = _synthetic_root_base(worktree_path)
        if synthetic_base is None:
            return [], None
        merge_base, base = synthetic_base, f"synthetic root {synthetic_base[:10]}"
    else:
        merge_base, base = resolved
    diff = _git_stdout(worktree_path, "diff", "--name-only", merge_base, "HEAD", "-z")
    if diff is None:
        raise ReviewScopeDiscoveryError(f"committed review scope diff failed against {merge_base}")
    files = sorted({path for path in diff.split("\0") if path and not path.startswith(".review/")})
    return files, base


def _branch_diff_scope(
    worktree_path: Path, committed_base: tuple[str, str] | None = None
) -> tuple[list[str], str | None]:
    """Branch-diff scope with committed fallback: ``(changed_files, scope_reason)``.

    Uncommitted changes win (the historical behavior). On a clean worktree the
    scope falls back to the committed branch diff vs the base fork point so a
    freshly-committed lane never reviews an empty file set (WIDTH2-H-01).
    """
    dirty = _changed_files(worktree_path)
    if dirty:
        return dirty, None
    committed, base = (
        _committed_branch_files(worktree_path, committed_base)
        if committed_base is not None
        else _committed_branch_files(worktree_path)
    )
    if committed:
        return committed, f"clean worktree; scope from committed branch diff vs {base}"
    return [], None


def _assert_recordable_review_scope(
    *,
    record_findings: bool,
    dirty_files: list[str],
    scope_source: ReviewScopeSource,
) -> None:
    """Reject recorded branch-diff reviews that still point at dirty local changes.

    Keyed on the actual UNCOMMITTED set, not the resolved scope: a clean
    worktree whose scope fell back to the committed branch diff is exactly the
    committed state findings should map to, so it must record.
    """
    if not record_findings:
        return
    if scope_source != "branch_diff":
        return
    if not dirty_files:
        return
    raise RuntimeError(
        "review_runner.py refuses to record findings for branch_diff scope when the worktree has uncommitted "
        "changes. Commit or stash the dirty paths first, or rerun with --latest-slice so MCP findings map to "
        "a committed slice packet instead of the working tree."
    )


def _diff_stat(worktree_path: Path, committed_base: tuple[str, str] | None = None) -> str:
    """Use the same dirty/committed range as the file list and inline diff."""
    refs = ["HEAD"] if _changed_files(worktree_path) else []
    if not refs:
        resolved = committed_base or _resolve_committed_base(worktree_path)
        if resolved is None:
            synthetic_base = _synthetic_root_base(worktree_path)
            resolved = (synthetic_base, f"synthetic root {synthetic_base[:10]}") if synthetic_base else None
        if resolved is None:
            return ""
        refs = [resolved[0], "HEAD"]
    return _required_git_stdout(worktree_path, "diff", "--stat", *refs, "--", ".", ":(exclude).review/**")


def _review_diff(worktree_path: Path, committed_base: tuple[str, str] | None = None) -> str:
    """Carry complete file diffs, reserving space for an explicit omission list."""
    dirty = _changed_files(worktree_path)
    refs = ["HEAD"] if dirty else []
    if not refs:
        resolved = committed_base or _resolve_committed_base(worktree_path)
        if resolved is None:
            synthetic_base = _synthetic_root_base(worktree_path)
            resolved = (synthetic_base, f"synthetic root {synthetic_base[:10]}") if synthetic_base else None
        if resolved is None:
            return ""
        refs = [resolved[0], "HEAD"]
    raw = _required_git_stdout(
        worktree_path, "diff", "--no-color", "--no-renames", *refs, "--", ".", ":(exclude).review/**"
    )
    # Untracked files are part of the historical dirty scope as well.
    if dirty:
        untracked = _required_git_stdout(worktree_path, "ls-files", "--others", "--exclude-standard", "-z")
        for filename in untracked.split("\0"):
            if not filename or filename.startswith(".review/"):
                continue
            result = _run_review_git(
                worktree_path, "diff", "--no-color", "--no-renames", "--no-index", "--", "/dev/null", filename
            )
            if result.returncode == 1:
                raw += "\n" + result.stdout
            elif result.returncode != 0:
                raise ReviewScopeDiscoveryError(f"review scope untracked diff failed: {filename}")
    return _bounded_review_diff(raw)


def _bounded_review_diff(raw: str) -> str:
    if len(raw.encode()) <= MAX_REVIEW_DIFF_BYTES:
        return raw
    chunks = re.split(r"(?=^diff --git )", raw, flags=re.MULTILINE)
    chunks = [chunk for chunk in chunks if chunk.strip()]
    for keep in range(len(chunks) - 1, -1, -1):
        omitted = [chunk.splitlines()[0].split(" b/", 1)[-1] for chunk in chunks[keep:]]
        marker = "\n[DIFF TRUNCATED; omitted files: " + ", ".join(omitted) + "]"
        candidate = "".join(chunks[:keep]) + marker
        if len(candidate.encode()) <= MAX_REVIEW_DIFF_BYTES:
            return candidate
    # A filename list can itself exceed the budget; fail explicitly instead of
    # presenting a partial hunk or silently dropping omitted-file identities.
    raise ValueError("Review diff omission list exceeds MAX_REVIEW_DIFF_BYTES")


def _slice_diff(
    worktree_path: Path, scope: ReviewScope, committed_base: tuple[str, str] | None
) -> tuple[str, str, str] | None:
    """Resolve the packet boundary and read only committed, literal packet paths."""
    commit_sha = scope.get("commit_sha")
    if not isinstance(commit_sha, str) or not commit_sha or committed_base is None:
        return None
    commit_args = ("rev-parse", "--verify", "--end-of-options", f"{commit_sha}^{{commit}}")
    commit_result = _run_review_git(worktree_path, *commit_args)
    if commit_result.returncode != 0:
        if "needed a single revision" in commit_result.stderr.casefold():
            return None
        raise ReviewScopeDiscoveryError(f"review scope git command failed: git {' '.join(commit_args)}")
    commit = commit_result.stdout.strip()
    if not commit:
        return None
    base = committed_base[0]
    paths = [f":(literal){path}" for path in scope["changed_files"]]
    if not paths:
        return "", "", f"{base}..{commit}"
    raw = _required_git_stdout(worktree_path, "diff", "--no-color", "--no-renames", base, commit, "--", *paths)
    stat = _required_git_stdout(worktree_path, "diff", "--stat", base, commit, "--", *paths)
    return _bounded_review_diff(raw), stat, f"{base}..{commit}"


# ---------------------------------------------------------------------------
# Stack guide auto-detection
# ---------------------------------------------------------------------------


def _detect_stack_guides(changed_files: list[str]) -> list[str]:
    """Return deduplicated list of stack guide filenames relevant to the changed files."""
    guides: dict[str, str] = {}
    for file_path in changed_files:
        ext = Path(file_path).suffix.lower()
        guide = STACK_GUIDES.get(ext)
        if guide and guide not in guides:
            guides[guide] = guide
    return list(guides.values())


def _resolve_rules_dir(*, orchestrator_root: str | Path | None = None, rules_dir: str | Path | None = None) -> Path:
    """Resolve the review-rules directory.

    Order: explicit ``rules_dir`` arg > module-level ``RULES_DIR`` override
    (test/CLI hook) > package-bundled defaults. The legacy
    ``<orchestrator_root>/docs/workbay/rules`` lookup is no longer
    consulted: review guides ship with the package itself.
    """
    if rules_dir is not None:
        return Path(rules_dir).expanduser().resolve()
    if RULES_DIR != DEFAULT_RULES_DIR:
        return RULES_DIR
    return RULES_DIR


def _read_guide(
    filename: str, *, orchestrator_root: str | Path | None = None, rules_dir: str | Path | None = None
) -> str:
    """Read a review guide file from the rules directory."""
    guide_path = _resolve_rules_dir(orchestrator_root=orchestrator_root, rules_dir=rules_dir) / filename
    if not guide_path.is_file():
        return ""
    return guide_path.read_text()


# ---------------------------------------------------------------------------
# Prompt rendering
# ---------------------------------------------------------------------------


class ReviewScope(TypedDict):
    changed_files: list[str]
    review_kind: ReviewKind
    scope_source: ReviewScopeSource
    scope_reason: str | None
    commit_sha: NotRequired[str | None]


def _guide_heading(filename: str) -> str:
    return Path(filename).stem.replace("-", " ").replace("_", " ").upper()


def _lookup_related_prior_work(
    *,
    lane_objective: str,
    changed_files: list[str],
    task_ref: str | None,
) -> dict[str, Any] | None:
    """Return advisory semantic matches, omitting typed empty states.

    Expected import, filesystem, and database availability failures are
    observable but non-gating. Unexpected failures propagate so programming
    defects cannot masquerade as an ordinary empty semantic result.
    """
    query_parts: list[str] = []
    objective = lane_objective.strip()
    if objective:
        query_parts.append(f"Lane objective: {objective[:1_200]}")
    if changed_files:
        bounded_files = [path[:300] for path in changed_files[:50]]
        query_parts.append("Changed files:\n" + "\n".join(bounded_files))
    if not query_parts:
        return None

    try:
        from workbay_handoff_mcp import api as handoff_api  # noqa: PLC0415
        from workbay_handoff_mcp.runtime import RuntimeNotConfiguredError  # noqa: PLC0415
    except ImportError as exc:
        logger.warning(
            "prior-work lookup unavailable (%s): %s",
            type(exc).__name__,
            exc,
        )
        return None

    try:
        result = handoff_api.find_related_prior_work(
            text="\n".join(query_parts),
            task_ref=task_ref,
            limit=RELATED_PRIOR_WORK_LIMIT,
        )
    except (OSError, RuntimeNotConfiguredError, sqlite3.Error) as exc:
        logger.warning(
            "prior-work lookup unavailable (%s): %s",
            type(exc).__name__,
            exc,
        )
        return None
    if not isinstance(result, dict) or result.get("status") != "ok_with_results":
        return None
    matches = result.get("results")
    if not isinstance(matches, list) or not matches:
        return None
    return result


def _render_related_prior_work(prior_work: Mapping[str, Any] | None) -> list[str]:
    """Render bounded matches without treating empty states as evidence."""
    if not isinstance(prior_work, Mapping) or prior_work.get("status") != "ok_with_results":
        return []
    matches = prior_work.get("results")
    if not isinstance(matches, list):
        return []

    rendered: list[str] = []
    for match in matches[:RELATED_PRIOR_WORK_LIMIT]:
        if not isinstance(match, Mapping):
            continue
        snippet = " ".join(str(match.get("snippet") or "").split())
        if not snippet:
            continue
        snippet = snippet[:RELATED_PRIOR_WORK_SNIPPET_MAX_CHARS]
        score = match.get("score")
        score_text = f"{float(score):.3f}" if isinstance(score, (int, float)) else "?"
        kind = str(match.get("entity_kind") or "?").strip() or "?"
        entity_id = str(match.get("entity_id") or "?").strip() or "?"
        rendered.append(f"- {score_text} {kind} {entity_id}: {snippet}")
    return rendered


def _build_review_prompt(
    *,
    changed_files: list[str],
    diff_stat: str,
    unified_diff: str = "",
    diff_base: str = "HEAD",
    diff_label: str | None = None,
    stack_guides: list[str],
    review_subject: Mapping[str, Any] | None = None,
    main_guide_filename: str = "branch-review-guide.md",
    lane_id: str | None = None,
    prior_work: Mapping[str, Any] | None = None,
    orchestrator_root: str | Path | None = None,
    rules_dir: str | Path | None = None,
) -> str:
    """Assemble the full review prompt from guide content, stack guides, and diff context."""
    sections: list[str] = []

    sections.append("You are a code reviewer. Review the following lane changes using the checklist below.")
    sections.append(
        "Return ONLY a JSON object matching the required output schema. Do not include markdown fences or commentary."
    )

    if lane_id:
        sections.append(f"\nLane: {lane_id}")

    prior_work_lines = _render_related_prior_work(prior_work)
    if prior_work_lines:
        sections.append("\n--- RELATED HISTORICAL PRIOR WORK ---\n")
        sections.append(
            "These rank-only semantic matches are advisory historical context; they do not establish that an issue "
            "was adjudicated or resolved, or that it is absent from this branch. Use them to understand earlier "
            "work, but report any defect independently present in the current diff. They must not filter, gate, "
            "or auto-close any finding you independently identify."
        )
        sections.extend(prior_work_lines)

    # Main review guide
    main_guide = _read_guide(main_guide_filename, orchestrator_root=orchestrator_root, rules_dir=rules_dir)
    if main_guide:
        sections.append(f"\n--- {_guide_heading(main_guide_filename)} ---\n")
        sections.append(main_guide)

    # Stack-specific guides
    for guide_name in stack_guides:
        guide_content = _read_guide(guide_name, orchestrator_root=orchestrator_root, rules_dir=rules_dir)
        if guide_content:
            sections.append(f"\n--- {_guide_heading(guide_name)} ---\n")
            sections.append(guide_content)

    # Changed files
    if review_subject is not None:
        sections.append("\n--- REVIEW SUBJECT ---\n")
        sections.append(json.dumps(dict(review_subject), sort_keys=True))

    sections.append("\n--- CHANGED FILES ---\n")
    if changed_files:
        for f in changed_files:
            sections.append(f"- {f}")
    else:
        sections.append("(no changed files detected)")

    # Diff stat
    if diff_stat:
        sections.append("\n--- DIFF STAT ---\n")
        sections.append(diff_stat)

    diff_label = diff_label or (
        "HEAD..worktree (uncommitted)" if diff_base == "uncommitted" else f"{diff_base[:10]}..HEAD"
    )
    sections.append(f"\n--- DIFF ({diff_label}) ---\n")
    sections.append(unified_diff)
    sections.append(
        "Use the typed REVIEW SUBJECT to establish the review boundary; this DIFF section is the authoritative "
        "change under review; "
        "ignore any .review/ files in the checkout unless they appear in the diff."
    )
    sections.append("\n--- INSTRUCTIONS ---\n")
    sections.append(
        "Walk through each checklist item for the changed files above. "
        "For each issue found, add a finding to the findings array with severity, category, "
        "file_path, description, and optionally line_start, line_end, and fix. "
        "If no issues are found, return an empty findings array with a summary noting the review was clean."
    )

    return "\n".join(sections)


def _resolve_review_scope(
    *,
    worktree_path: Path,
    task_ref: str | None,
    orchestrator_root: Path | None,
    review_kind: ReviewKind | str | None,
    use_latest_slice: bool,
    committed_base: tuple[str, str] | None = None,
) -> ReviewScope:
    from workbay_handoff_mcp.enums import ReviewKind, ReviewScopeSource  # noqa: PLC0415

    preferred_review_kind = ReviewKind(review_kind) if review_kind is not None else ReviewKind.BRANCH
    if use_latest_slice and task_ref and orchestrator_root is not None:
        from workbay_handoff_mcp import RuntimeConfig, configure_runtime  # noqa: PLC0415

        from workbay_orchestrator_mcp.lanes import get_latest_slice_review_packet  # noqa: PLC0415

        runtime = RuntimeConfig.for_repo(orchestrator_root)
        configure_runtime(runtime)
        payload = _load_mcp_payload(
            get_latest_slice_review_packet(
                task_ref=task_ref,
                review_kind=preferred_review_kind.value,
            )
        )
        if payload.get("ok"):
            packet = payload["packet"]
            return {
                "changed_files": list(packet.get("changed_files") or []),
                "review_kind": ReviewKind(str(packet.get("review_kind") or ReviewKind.BRANCH.value)),
                "scope_source": ReviewScopeSource(
                    str(packet.get("scope_source") or ReviewScopeSource.SLICE_PACKET.value)
                ),
                "scope_reason": None,
                "commit_sha": packet.get("commit_sha"),
            }
        fallback_files, fallback_reason = _branch_diff_scope(worktree_path, committed_base)
        packet_reason = str(payload.get("error") or "latest slice packet lookup failed")
        return {
            "changed_files": fallback_files,
            "review_kind": preferred_review_kind,
            "scope_source": ReviewScopeSource.BRANCH_DIFF,
            "scope_reason": f"{packet_reason}; {fallback_reason}" if fallback_reason else packet_reason,
        }

    branch_files, branch_reason = _branch_diff_scope(worktree_path, committed_base)
    return {
        "changed_files": branch_files,
        "review_kind": preferred_review_kind,
        "scope_source": ReviewScopeSource.BRANCH_DIFF,
        "scope_reason": branch_reason,
    }


# ---------------------------------------------------------------------------
# Finding ID generation
# ---------------------------------------------------------------------------


def _generate_finding_id(lane_id: str | None, index: int, finding: dict[str, Any]) -> str:
    """Return PREFIX-lanehash-S-contenthash; index is ignored for compatibility.

    Lane normalization preserves punctuation; content uses stripped category/path,
    the line number, and whitespace-collapsed lowercase description. Report order
    and operation tokens never enter the key. Recording adds duplicate suffixes.
    """
    lane = (lane_id or "review").strip().lower()
    prefix = lane.upper().replace("-", "")[:6]
    lane_key = hashlib.sha256(lane.encode()).hexdigest()[:16]
    content = [
        str(finding.get("category") or "").strip().lower(),
        str(finding.get("file_path") or "").strip(),
        finding.get("line_start"),
        " ".join(str(finding.get("description") or "").lower().split()),
    ]
    content_key = hashlib.sha256(json.dumps(content, ensure_ascii=False, separators=(",", ":")).encode()).hexdigest()[
        :24
    ]
    severity_char = {"high": "H", "medium": "M", "low": "L"}.get(str(finding.get("severity", "")).strip().lower(), "X")
    return f"{prefix}-{lane_key}-{severity_char}-{content_key}"


# ---------------------------------------------------------------------------
# Codex execution
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Result validation
# ---------------------------------------------------------------------------


def _load_mcp_payload(payload: dict[str, Any] | str | bytes | bytearray) -> dict[str, Any]:
    if isinstance(payload, dict):
        return payload
    loaded = json.loads(payload)
    if not isinstance(loaded, dict):
        raise RuntimeError(f"Expected MCP payload object, got {type(loaded).__name__}.")
    return loaded


def _raw_tail(result: Any, *, max_chars: int = _RAW_TAIL_CHARS) -> str:
    """Bounded tail of a review payload for typed degrade diagnostics ([OBS-04])."""
    if isinstance(result, dict):
        try:
            text = json.dumps(result, ensure_ascii=False, sort_keys=True, default=str)
        except (TypeError, ValueError):
            text = str(result)
    else:
        text = str(result)
    if len(text) <= max_chars:
        return text
    return text[-max_chars:]


def _unparseable_review_result(result: Any, *, reason: str) -> dict[str, Any]:
    """Typed degrade for unparseable smoke-review output ([OBS-08], [OBS-04], D9)."""
    harvested = _findings_from_blockers(result)
    if harvested:
        summary = "Smoke review unparseable; salvaged reviewer blockers as findings."
        if isinstance(result, dict):
            original = result.get("summary")
            if isinstance(original, str) and original.strip():
                summary = original
        return {
            "findings": harvested,
            "summary": summary,
            "raw_tail": _raw_tail(result),
            "result_parse": "ok",
        }
    return {
        "review": REVIEW_SKIPPED_UNPARSEABLE,
        "findings": [],
        "summary": f"Smoke review unparseable ({reason}); non-authoritative degrade.",
        "raw_tail": _raw_tail(result),
        # implementation note D9: empty findings + result_parse=degraded is not a clean review.
        "result_parse": "degraded",
    }


# Engine-generated blockers are control markers, not reviewer defects.
_BLOCKER_CONTROL_TOKENS = frozenset({"result_unparseable", "admission_deferred"})
_BLOCKER_CONTROL_PREFIXES = frozenset({"Review did not converge after"})
_BLOCKER_SEVERITY_RE = re.compile(r"\b(HIGH|MEDIUM|LOW)\b", re.IGNORECASE)
_BLOCKER_FILE_PATH_RE = re.compile(r"(?:[A-Za-z0-9_.-]+/)+[A-Za-z0-9_.-]+\.[A-Za-z0-9]+")


def _findings_from_blockers(result: Any) -> list[dict[str, Any]]:
    """Salvage schema-valid findings from a handoff-shaped ``blockers`` list.

    Used only on the tolerant missing-findings path. Unlabelled reviewer
    blockers default to high so the harvest fails closed ([SECD-05]).
    """
    if not isinstance(result, dict):
        return []
    blockers = result.get("blockers")
    if not isinstance(blockers, list) or not blockers:
        return []
    findings: list[dict[str, Any]] = []
    for item in blockers:
        if not isinstance(item, str):
            continue
        text = item.strip()
        if not text:
            continue
        if text in _BLOCKER_CONTROL_TOKENS or any(text.startswith(prefix) for prefix in _BLOCKER_CONTROL_PREFIXES):
            continue
        severity_match = _BLOCKER_SEVERITY_RE.search(text)
        severity = severity_match.group(1).lower() if severity_match else "high"
        path_match = _BLOCKER_FILE_PATH_RE.search(text)
        findings.append(
            {
                "severity": severity,
                "category": "GAP",
                "file_path": path_match.group(0) if path_match else "unknown",
                "description": item,
                "finding_source": "blockers",
            }
        )
    return findings


def _finding_field_error(finding: Any, index: int) -> str | None:
    """Return a per-finding validation error, or None when the item is well-formed."""
    valid_severities = {"high", "medium", "low"}
    valid_categories = {"ANTIPATTERN", "DEAD_CODE", "COMPLEXITY", "GAP"}
    if not isinstance(finding, dict):
        return f"Finding [{index}] is not an object."
    for required in ("severity", "category", "file_path", "description"):
        if required not in finding:
            return f"Finding [{index}] missing required field '{required}'."
    severity = str(finding["severity"]).lower()
    if severity not in valid_severities:
        return f"Finding [{index}] has invalid severity '{finding['severity']}'. Valid: {sorted(valid_severities)}"
    if finding["category"] not in valid_categories:
        return f"Finding [{index}] has invalid category '{finding['category']}'. Valid: {sorted(valid_categories)}"
    if not isinstance(finding["description"], str) or not finding["description"].strip():
        return f"Finding [{index}] has empty description."
    return None


def _validate_review_result(result: Any, *, tolerant: bool = False) -> dict[str, Any]:
    """Validate the review result against the expected schema shape.

    When ``tolerant`` is True (grok-family smoke review):
    - Only a **non-empty** object that fails review-schema parsing degrades to
      typed ``skipped_unparseable`` ([OBS-08]). Empty/missing ``raw_payload`` is a
      transport failure (VM unreachable, exit-78/3, etc.) and raises exactly like
      the strict path so blockers/handoff_action are not masked as converged
      (0144 R3 / HIGH-2).
    - When the findings key is present as a list, salvage well-formed items and
      drop invalid ones with a note in ``raw_tail`` ([OBS-04]).
    Codex / non-tolerant paths stay strict (any field error raises).
    """
    # Empty/missing payload = transport failure: never tolerant-degrade.
    # Strict-path failure shape (mirror RuntimeError messages below).
    if result is None:
        raise RuntimeError(f"Review result must be an object, got {type(result).__name__}.")
    if isinstance(result, dict) and not result:
        raise RuntimeError("Review result missing required 'findings' key.")

    try:
        if not isinstance(result, dict):
            raise RuntimeError(f"Review result must be an object, got {type(result).__name__}.")
        if "summary" not in result:
            raise RuntimeError("Review result missing required 'summary' key.")
        if "findings" not in result:
            if not tolerant:
                raise RuntimeError("Review result missing required 'findings' key.")
            out = dict(result)
            harvested = _findings_from_blockers(result)
            out["raw_tail"] = _raw_tail(result)
            if harvested:
                out["findings"] = harvested
                out["result_parse"] = "ok"
                out.pop("blockers", None)
                return out
            out["findings"] = []
            out["_missing_findings_key"] = True
            return out
        findings = result.get("findings")
        if not isinstance(findings, list):
            raise RuntimeError("Review result 'findings' must be an array.")
        if not isinstance(result["summary"], str) or not result["summary"].strip():
            raise RuntimeError("Review result 'summary' must be a non-empty string.")

        salvaged: list[Any] = []
        dropped: list[str] = []
        for i, finding in enumerate(findings):
            err = _finding_field_error(finding, i)
            if err is None:
                normalized = dict(finding)
                normalized["severity"] = str(finding["severity"]).lower()
                salvaged.append(normalized)
                continue
            if not tolerant:
                raise RuntimeError(err)
            dropped.append(err)

        if not dropped:
            out = dict(result)
            out["findings"] = salvaged
            return out

        # Tolerant salvage: keep valid findings; note drops in raw_tail ([OBS-04]).
        # S1-A-01: the note is bounded via _raw_tail/_RAW_TAIL_CHARS — a large
        # malformed findings array would otherwise inflate raw_tail without limit
        # (50KB+ observed); the drop COUNT survives ahead of the capped detail.
        out = dict(result)
        out["findings"] = salvaged
        note = f"dropped {len(dropped)} invalid finding(s): {_raw_tail('; '.join(dropped))}"
        prior_tail = out.get("raw_tail")
        if isinstance(prior_tail, str) and prior_tail.strip():
            out["raw_tail"] = f"{_raw_tail(prior_tail)}\n{note}"
        else:
            out["raw_tail"] = note
        return out
    except RuntimeError as exc:
        if tolerant:
            return _unparseable_review_result(result, reason=str(exc))
        raise


# ---------------------------------------------------------------------------
# MCP recording
# ---------------------------------------------------------------------------


class FindingsPersistResult(TypedDict):
    legacy_reconciliation: Literal["reconciled", "none", "unknown"]
    legacy_cause: str | None
    recorded_ids: list[str]
    unrecorded_ids: list[str]
    unrecorded_findings: list[dict[str, Any]]
    causes: dict[str, str]


_TERMINAL_ID_COLLISION_NEEDLE = "is terminal (status="
_TERMINAL_ID_COLLISION_HINT = "Use a new finding_id."
_TERMINAL_ID_REKEY_START = 2
_TERMINAL_ID_REKEY_MAX_ATTEMPTS = 8
_REKEY_DESCRIPTION_TEMPLATE = "[re-keyed from {original_id}: terminal id collision] "


def _is_terminal_finding_id_collision(cause: str) -> bool:
    text = str(cause)
    return _TERMINAL_ID_COLLISION_NEEDLE in text and _TERMINAL_ID_COLLISION_HINT in text


def _rekeyed_finding_id(original_id: str, attempt: int) -> str:
    return f"{original_id}-R{attempt}"


def _rekeyed_description(original_id: str, description: str) -> str:
    prefix = _REKEY_DESCRIPTION_TEMPLATE.format(original_id=original_id)
    if str(description).startswith(prefix):
        return str(description)
    return prefix + str(description)


class FindingsPersistError(RuntimeError):
    """No writes acknowledged; retain reconciliation evidence for rescue."""

    def __init__(self, message: str, persist: FindingsPersistResult):
        super().__init__(message)
        self.persist = persist


class LegacyFindingAmbiguityWarning(UserWarning):
    """Multiple positional rows match a content-addressed finding."""


class LegacyFindingLaneWarning(UserWarning):
    """A positional match has missing or conflicting lane attribution."""


def _persist_result(
    recorded_ids: list[str],
    *,
    unrecorded_ids: list[str] | None = None,
    unrecorded_findings: list[dict[str, Any]] | None = None,
    causes: dict[str, str] | None = None,
    legacy_reconciliation: Literal["reconciled", "none", "unknown"] = "none",
) -> FindingsPersistResult:
    return {
        "legacy_reconciliation": legacy_reconciliation,
        "legacy_cause": (causes or {}).get("legacy_reconciliation"),
        "recorded_ids": list(recorded_ids),
        "unrecorded_ids": list(unrecorded_ids or []),
        "unrecorded_findings": list(unrecorded_findings or []),
        "causes": dict(causes or {}),
    }


def _persist_shortfall_block(source: Any) -> dict[str, Any]:
    """Copy recorded/unrecorded ids, verbatim refused bodies, and causes.

    Accepts a structured persist result or ``FindingsPersistError`` (REF-26).
    Consumers (harvest exception/partial/salvage and ``run_review``) must not
    pick a subset of these keys — a refused body has to stay nameable (OBS-08).
    """
    persist = source.persist if isinstance(source, FindingsPersistError) else source
    if not isinstance(persist, Mapping):
        persist = {}
    findings = persist.get("unrecorded_findings") or []
    return {
        "recorded_ids": list(persist.get("recorded_ids") or []),
        "unrecorded_ids": list(persist.get("unrecorded_ids") or []),
        "unrecorded_findings": [dict(item) for item in findings if isinstance(item, Mapping)],
        "causes": dict(persist.get("causes") or {}),
    }


def _record_findings(
    findings: list[dict[str, Any]],
    *,
    task_ref: str,
    session: str,
    lane_id: str | None = None,
    orchestrator_root: Path,
    reconcile_committed: bool = False,
    operation_token: str | None = None,
) -> FindingsPersistResult:
    """Record each finding into MCP, salvaging failed batches row by row.

    Returns recorded/unrecorded ids after salvage. Raises only when zero rows
    landed — a subset write is a persist result, not an exception.

    A per-row persist ``ok`` is commit-ok: the writer committed that row on
    this connection. It is not crash-durable; WAL ``synchronous=NORMAL`` can
    still lose a committed row across a host crash before the next checkpoint.
    """

    from workbay_handoff_mcp import RuntimeConfig, batch_record_review_findings, configure_runtime

    runtime = RuntimeConfig.for_repo(orchestrator_root)
    configure_runtime(runtime)

    import sqlite3

    from workbay_handoff_mcp import list_review_findings
    from workbay_handoff_mcp.runtime import RuntimeNotConfiguredError

    legacy_rows = []
    offset = 0
    legacy_cause = None
    # A fresh state has no positional rows; let the writer initialize it.
    while findings and (runtime.db_path.exists() or reconcile_committed):
        try:
            response = _load_mcp_payload(
                list_review_findings(
                    task_ref=task_ref,
                    status="all",
                    limit=500,
                    offset=offset,
                )
            )
        except (sqlite3.Error, RuntimeNotConfiguredError) as exc:
            legacy_cause = f"{type(exc).__name__}: {exc}"[:500]
            break
        data = response.get("data") or {}
        if not response.get("ok"):
            if data.get("error") == "Finding not found for task.":
                break
            legacy_cause = str(data.get("error") or response.get("error") or "legacy read returned non-ok")
            break
        rows = data.get("findings")
        if not isinstance(rows, list):
            legacy_cause = "legacy finding reconciliation invalid"
            break
        legacy_rows.extend(rows)
        if not data.get("has_more"):
            break
        if not rows:
            legacy_cause = "legacy finding reconciliation pagination stalled"
            break
        offset += len(rows)
    snapshot_state: Literal["none", "ok", "unknown"] = "unknown" if legacy_cause else "ok" if legacy_rows else "none"
    if legacy_cause:
        legacy_cause = legacy_cause[:500]
        # DATA-03 / CARD-03: an unavailable compatibility snapshot must not
        # block writes or masquerade as proof that no legacy rows exist.
        legacy_rows = []
        logger.warning(
            "legacy_reconciliation=unknown (task_ref=%s session=%s lane_id=%s): %s",
            task_ref,
            session,
            lane_id,
            legacy_cause,
        )
    legacy_ids: set[str] = set()

    def persist_result(
        recorded_ids: list[str],
        *,
        unrecorded_ids: list[str] | None = None,
        unrecorded_findings: list[dict[str, Any]] | None = None,
        causes: dict[str, str] | None = None,
    ) -> FindingsPersistResult:
        return _persist_result(
            recorded_ids,
            unrecorded_ids=unrecorded_ids,
            unrecorded_findings=unrecorded_findings,
            causes={**(causes or {}), **({"legacy_reconciliation": legacy_cause} if legacy_cause else {})},
            legacy_reconciliation=("unknown" if legacy_cause else "reconciled" if legacy_ids else "none"),
        )

    actor: WriteActor = {}
    if lane_id:
        actor["lane_id"] = lane_id

    batch_items: list[BatchFindingItem] = []
    recorded_ids: list[str] = []
    original_findings_by_id: dict[str, dict[str, Any]] = {}
    occurrences: dict[str, int] = {}
    # Canonical order also assigns duplicate ordinals consistently when ancillary
    # fields (such as a suggested fix) differ but the finding identity is equal.
    ordered = sorted(
        findings, key=lambda f: (_generate_finding_id(lane_id, 0, f), json.dumps(f, sort_keys=True, ensure_ascii=False))
    )
    for finding in ordered:
        base_id = _generate_finding_id(lane_id, 0, finding)
        occurrences[base_id] = occurrences.get(base_id, 0) + 1
        ordinal = occurrences[base_id]
        finding_id = base_id if ordinal == 1 else f"{base_id}-{ordinal}"
        prefix, _, severity, _ = base_id.split("-", 3)
        candidates = [
            row
            for row in legacy_rows
            if re.fullmatch(rf"{re.escape(prefix)}-{severity}-[0-9]+", str(row.get("finding_id") or ""))
            and str(row.get("file_path") or "").strip() == str(finding.get("file_path") or "").strip()
            and row.get("line_start") == finding.get("line_start")
            and " ".join(str(row.get("description") or "").lower().split())
            == " ".join(
                f"[{str(finding.get('category') or '').strip()}] {finding.get('description') or ''}".lower().split()
            )
        ]
        if len(candidates) == 1:
            candidate = candidates[0]
            stored_lane = candidate.get("lane_id")
            # Lane-less historical rows may alias only as the sole content
            # match in this task. A truncated prefix is never lane identity.
            lane_less = stored_lane in (None, "")
            if lane_less or not lane_id or stored_lane != lane_id:
                import warnings

                warnings.warn(
                    f"Legacy finding {candidate['finding_id']} has lane_id={stored_lane!r}; "
                    f"replay lane_id={lane_id!r}: "
                    + ("aliasing sole lane-less content match" if lane_less else "keeping separate hashed finding"),
                    LegacyFindingLaneWarning,
                    stacklevel=2,
                )
            if (lane_less or (lane_id and stored_lane == lane_id)) and candidate["finding_id"] not in legacy_ids:
                # Keep the original row intact, including all lifecycle metadata.
                # Each positional row can alias only one occurrence per report.
                finding_id = candidate["finding_id"]
                legacy_ids.add(finding_id)
        elif len(candidates) > 1:
            import warnings

            warnings.warn(
                f"Ambiguous legacy finding {base_id}: " + ", ".join(sorted(row["finding_id"] for row in candidates)),
                LegacyFindingAmbiguityWarning,
                stacklevel=2,
            )
        recorded_ids.append(finding_id)
        original_findings_by_id[finding_id] = dict(finding)

        details: ReviewFindingDetails = {}
        if "line_start" in finding and isinstance(finding["line_start"], int):
            details["line_start"] = finding["line_start"]
        if "line_end" in finding and isinstance(finding["line_end"], int):
            details["line_end"] = finding["line_end"]
        if "fix" in finding and isinstance(finding["fix"], str):
            details["fix"] = finding["fix"]

        description = f"[{finding['category']}] {finding['description']}"
        if finding.get("severity_coerced") is True:
            # The handoff details parser retains only line references and fix.
            description += f" [severity coerced from {finding.get('severity_raw')!r}]"

        batch_items.append(
            {
                "finding_id": finding_id,
                "severity": finding["severity"],
                "file_path": finding["file_path"],
                "description": description,
                "details": details if details else None,
            }
        )

    def record(items: list[BatchFindingItem]) -> dict[str, Any]:
        acknowledged = [{"finding_id": item["finding_id"]} for item in items if item["finding_id"] in legacy_ids]
        items = [item for item in items if item["finding_id"] not in legacy_ids]
        pending = []
        if reconcile_committed and snapshot_state == "ok":
            # All statuses came from the single complete snapshot. An unknown
            # snapshot skips replay and lets the idempotent writer persist IDs.
            for item in items:
                rows = [row for row in legacy_rows if row.get("finding_id") == item["finding_id"]]
                if any(
                    row.get("finding_id") == item["finding_id"]
                    and (
                        (operation_token or row.get("session") == session)
                        and all(row.get(key) == item[key] for key in ("severity", "file_path", "description"))
                    )
                    for row in rows
                ):
                    acknowledged.append({"finding_id": item["finding_id"]})
                else:
                    pending.append(item)
                    if rows and not operation_token:
                        # Legacy receipts lack an operation token. An
                        # existing row with changed attribution is ambiguous;
                        # do not reopen it in an attempt to reconcile history.
                        raise RuntimeError("review replay legacy finding identity mismatch")
        else:
            pending = items
        if not pending:
            return {"ok": True, "data": {"written": len(acknowledged), "results": acknowledged}}
        response = _load_mcp_payload(
            batch_record_review_findings(
                session=session,
                findings=pending,
                actor=actor if actor else None,
                task_ref=task_ref,
            )
        )
        if acknowledged and response.get("ok"):
            data = response.get("data") or {}
            data["results"] = list(data.get("results") or []) + acknowledged
            data["written"] = int(data.get("written") or 0) + len(acknowledged)
            response["data"] = data
        return response

    def failure_cause(result: dict[str, Any] | Exception) -> str:
        if isinstance(result, Exception):
            return f"{type(result).__name__}: {result}"
        error = result.get("error")
        data = result.get("data")
        if error is None and isinstance(data, dict):
            error = data.get("error")
        if error is not None and str(error).strip():
            return str(error)
        returned = json.dumps(result, ensure_ascii=False, sort_keys=True, default=str)
        return f"response omitted 'error'; returned payload={returned}"

    def envelope_data(result: dict[str, Any]) -> dict[str, Any]:
        data = result.get("data")
        return data if isinstance(data, dict) else {}

    def acknowledged_count(data: dict[str, Any]) -> int | None:
        for key in ("written", "findings_recorded_count"):
            value = data.get(key)
            if isinstance(value, int) and not isinstance(value, bool):
                return value
        return None

    def acknowledged_ids(result: dict[str, Any], expected_ids: list[str]) -> list[str]:
        """Ids a claimed-ok receipt actually persisted.

        Top-level ``ok`` is not enough. ``data.written`` (or
        ``data.findings_recorded_count``) must be present and compared with
        the submitted batch, and ``data.results`` must name those ids. A
        missing count is unproven, not agreement.
        """
        if not result.get("ok"):
            return []
        expected = list(expected_ids)
        data = envelope_data(result)
        written = acknowledged_count(data)
        results = data.get("results")
        matched: list[str] = []
        if isinstance(results, list):
            seen: set[str] = set()
            expected_set = set(expected)
            for item in results:
                if not isinstance(item, dict):
                    continue
                finding_id = item.get("finding_id")
                if isinstance(finding_id, str) and finding_id in expected_set and finding_id not in seen:
                    seen.add(finding_id)
                    matched.append(finding_id)
        if written is None:
            return []
        if written != len(expected):
            return matched
        if not isinstance(results, list):
            return []
        if set(matched) != set(expected):
            return matched
        return list(expected)

    def persist_receipt_ok(result: dict[str, Any] | Exception, expected_ids: list[str]) -> bool:
        if not isinstance(result, dict):
            return False
        acknowledged = acknowledged_ids(result, expected_ids)
        return len(acknowledged) == len(expected_ids) and set(acknowledged) == set(expected_ids)

    def receipt_cause(result: dict[str, Any] | Exception, expected_ids: list[str]) -> str:
        if persist_receipt_ok(result, expected_ids):
            return failure_cause(result)
        if isinstance(result, dict) and result.get("ok"):
            data = envelope_data(result)
            acknowledged = acknowledged_ids(result, expected_ids)
            unconfirmed = [finding_id for finding_id in expected_ids if finding_id not in acknowledged]
            return (
                "claimed-ok persist receipt did not acknowledge "
                f"{len(expected_ids)} row(s): submitted={len(expected_ids)} "
                f"acknowledged={len(acknowledged)} unconfirmed={unconfirmed!r} "
                f"data.written={data.get('written')!r} "
                f"results={data.get('results')!r}"
            )
        return failure_cause(result)

    try:
        batch_result: dict[str, Any] | Exception = record(batch_items)
    except Exception as exc:
        batch_result = exc
    if persist_receipt_ok(batch_result, recorded_ids):
        return persist_result(recorded_ids)

    # A claimed-ok receipt that names a proper subset is already the
    # acknowledgement. Publish only those ids; do not re-interpret the same
    # envelope as per-row success and do not raise past the caller.
    if isinstance(batch_result, dict) and batch_result.get("ok"):
        acknowledged = acknowledged_ids(batch_result, recorded_ids)
        if acknowledged:
            unrecorded_ids = [finding_id for finding_id in recorded_ids if finding_id not in acknowledged]
            causes = {
                "batch": receipt_cause(batch_result, recorded_ids),
                **{finding_id: "not confirmed by persist receipt" for finding_id in unrecorded_ids},
            }
            unrecorded_findings = [
                dict(original_findings_by_id[finding_id])
                for finding_id in unrecorded_ids
                if finding_id in original_findings_by_id
            ]
            logger.warning(
                "review finding persist shortfall (task_ref=%s session=%s lane_id=%s recorded=%s unrecorded=%s): %s",
                task_ref,
                session,
                lane_id,
                acknowledged,
                unrecorded_ids,
                causes,
            )
            return persist_result(
                acknowledged,
                unrecorded_ids=unrecorded_ids,
                unrecorded_findings=unrecorded_findings,
                causes=causes,
            )

    # Commit-ok authority, not crash-durable storage: a per-row ok means the
    # writer committed that row, not that the bytes survived a host crash
    # (session synchronous=NORMAL under WAL). Salvage stable-ID rows
    # independently, then fail only when zero rows landed ([DATA-14],
    # [RLSE-05], [OBS-04], [OBS-08], [AGT-10], [REF-37]; CARD-07). A subset
    # write returns the recorded/unrecorded ids so harvest can name both sets.
    recorded_after_failure: list[str] = []
    fallback_failures: dict[str, str] = {}
    rekeyed_from: dict[str, str] = {}
    for item in batch_items:
        finding_id = item["finding_id"]
        try:
            item_result: dict[str, Any] | Exception = record([item])
        except Exception as exc:
            item_result = exc
        if persist_receipt_ok(item_result, [finding_id]):
            recorded_after_failure.append(finding_id)
            continue
        cause = receipt_cause(item_result, [finding_id])
        if _is_terminal_finding_id_collision(cause):
            accepted_id: str | None = None
            last_cause = cause
            for attempt in range(
                _TERMINAL_ID_REKEY_START,
                _TERMINAL_ID_REKEY_START + _TERMINAL_ID_REKEY_MAX_ATTEMPTS,
            ):
                candidate_id = _rekeyed_finding_id(finding_id, attempt)
                rekeyed_item = {
                    **item,
                    "finding_id": candidate_id,
                    "description": _rekeyed_description(finding_id, str(item["description"])),
                }
                try:
                    rekey_result: dict[str, Any] | Exception = record([rekeyed_item])
                except Exception as exc:
                    rekey_result = exc
                if persist_receipt_ok(rekey_result, [candidate_id]):
                    accepted_id = candidate_id
                    break
                last_cause = receipt_cause(rekey_result, [candidate_id])
                if not _is_terminal_finding_id_collision(last_cause):
                    break
            if accepted_id is not None:
                recorded_after_failure.append(accepted_id)
                rekeyed_from[finding_id] = accepted_id
                fallback_failures[finding_id] = f"{finding_id} -> {accepted_id}"
                continue
            fallback_failures[finding_id] = last_cause
        else:
            fallback_failures[finding_id] = cause

    unrecorded_ids = [
        finding_id
        for finding_id in recorded_ids
        if finding_id not in recorded_after_failure and finding_id not in rekeyed_from
    ]
    unrecorded_findings = [
        dict(original_findings_by_id[finding_id])
        for finding_id in unrecorded_ids
        if finding_id in original_findings_by_id
    ]
    if not unrecorded_ids:
        logger.warning(
            "review finding batch failed but row-by-row recovery recorded all %d findings "
            "(task_ref=%s session=%s lane_id=%s): %s",
            len(recorded_ids),
            task_ref,
            session,
            lane_id,
            failure_cause(batch_result),
        )
        return persist_result(
            recorded_after_failure,
            causes=fallback_failures or None,
        )

    causes = {"batch": failure_cause(batch_result), **fallback_failures}
    if recorded_after_failure:
        logger.warning(
            "review finding persist shortfall (task_ref=%s session=%s lane_id=%s recorded=%s unrecorded=%s): %s",
            task_ref,
            session,
            lane_id,
            recorded_after_failure,
            unrecorded_ids,
            causes,
        )
        return persist_result(
            recorded_after_failure,
            unrecorded_ids=unrecorded_ids,
            unrecorded_findings=unrecorded_findings,
            causes=causes,
        )

    raise FindingsPersistError(
        "batch_record_review_findings failed: "
        f"{failure_cause(batch_result)}; expected={len(recorded_ids)}, "
        f"recorded={len(recorded_after_failure)}, unrecorded={len(unrecorded_ids)}; "
        f"unrecorded_ids={unrecorded_ids}; fallback_failures={fallback_failures}",
        persist_result(
            [],
            unrecorded_ids=unrecorded_ids,
            unrecorded_findings=unrecorded_findings,
            causes=causes,
        ),
    )


# ---------------------------------------------------------------------------
# Public API for daemon callers
# ---------------------------------------------------------------------------


class DetachedExecutionSettingsResolutionError(ValueError):
    """Detached identity could not be bound to a fully resolved dispatch."""

    code = "detached_execution_settings_unresolved"

    def __init__(self, setting: str) -> None:
        self.setting = setting
        super().__init__(f"cannot resolve detached execution setting: {setting}")


def _detached_execution_mode_enabled() -> bool:
    return os.environ.get("WORKBAY_REMOTE_EXEC_MODE", "attached").strip().lower() == "detached"


def _digest_setting_value(value: Any) -> Any:
    """Convert dispatch options to stable JSON values without losing their meaning."""
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise TypeError("dispatch option keys must be strings")
        return {key: _digest_setting_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_digest_setting_value(item) for item in value]
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    raise TypeError(f"unsupported dispatch option value: {type(value).__name__}")


def _detached_execution_settings_digest(
    *,
    backend: str,
    model: str | None,
    reasoning_effort: str | None,
    speed: str | None,
    dispatch_options: Mapping[str, Any] | None = None,
    require_resolved: bool = False,
) -> str:
    """Hash the effective settings that select a detached model execution.

    Both review and ACE curation use this single canonical recipe. Backend and
    model defaults are resolved from the backend registry, while explicit
    effort/speed choices are normalized using their dispatch contracts.
    """
    try:
        backend_name = validate_backend(str(backend).strip())
        spec = get_backend_spec(backend_name)
    except (TypeError, ValueError, RuntimeError) as exc:
        raise DetachedExecutionSettingsResolutionError("backend") from exc

    try:
        from workbay_orchestrator_mcp.orchestration.adapters.remote_exec import resolve_effective_model

        if spec.adapter_path != "workbay_orchestrator_mcp.orchestration.adapters.remote_exec.RemoteExecAdapter":
            raise RuntimeError("No detached model resolver for backend")
        effective_model = resolve_effective_model(spec, backend_name, model)
        if not effective_model:
            raise RuntimeError("Unresolved model")
    except (ImportError, TypeError, ValueError, RuntimeError) as exc:
        if require_resolved:
            raise DetachedExecutionSettingsResolutionError("model") from exc
        effective_model = model or ""

    try:
        from workbay_protocol.reasoning_effort import validate_reasoning_effort

        normalized_effort = validate_reasoning_effort(reasoning_effort)
        if normalized_effort in (None, "auto", "inherit") and effective_model:
            from workbay_orchestrator_mcp.orchestration.effort_policy import default_effort_for_model

            normalized_effort = default_effort_for_model(backend_name, effective_model)
    except (TypeError, ValueError, RuntimeError) as exc:
        if require_resolved:
            raise DetachedExecutionSettingsResolutionError("reasoning_effort") from exc
        normalized_effort = str(reasoning_effort) if reasoning_effort is not None else None

    try:
        from workbay_orchestrator_mcp.orchestration.lane_routing import normalize_speed

        normalized_speed = normalize_speed(backend_name, speed)
    except (TypeError, ValueError, RuntimeError) as exc:
        if require_resolved:
            raise DetachedExecutionSettingsResolutionError("speed") from exc
        normalized_speed = str(speed) if speed is not None else None

    try:
        canonical_settings = {
            "backend": backend_name,
            "model": effective_model or None,
            "reasoning_effort": normalized_effort,
            "speed": normalized_speed,
            "dispatch_options": _digest_setting_value(dispatch_options or {}),
        }
        payload = json.dumps(
            canonical_settings,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        if require_resolved:
            raise DetachedExecutionSettingsResolutionError("dispatch_options") from exc
        payload = json.dumps(
            {
                "backend": backend_name,
                "model": effective_model or None,
                "reasoning_effort": str(reasoning_effort) if reasoning_effort is not None else None,
                "speed": str(speed) if speed is not None else None,
                "dispatch_options": repr(dispatch_options or {}),
            },
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def _detached_review_pass_id(
    *,
    task_ref: str,
    lane_id: str,
    reviewed_head_sha: str,
    prompt: str,
    review_round: str | int | None,
    final_result_path: Path | None,
    execution_settings_digest: str,
) -> str:
    """Build a retry-stable job ID bound to the actual rendered review input.

    The prompt digest includes the reviewed diff and scope, so edits to uncommitted
    input change the ID even when HEAD is unchanged. An explicit round or result
    carrier supplies the retry key; standalone calls use the prompt digest instead
    of a shared ``default`` key, keeping identical retries stable without merging
    different review inputs into one detached job.
    """
    input_digest = hashlib.sha256(prompt.encode("utf-8")).hexdigest()
    if review_round is not None:
        round_key: str | int = review_round
    elif final_result_path is not None:
        round_key = str(final_result_path)
    else:
        round_key = f"standalone:{input_digest}"

    pass_digest = hashlib.sha256(
        json.dumps(
            [task_ref, lane_id, reviewed_head_sha, round_key, input_digest, execution_settings_digest],
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()[:16]
    return f"review-{pass_digest}"


def run_review(
    *,
    worktree_path: Path,
    lane_id: str | None = None,
    task_ref: str | None = None,
    session: str | None = None,
    orchestrator_root: Path | None = None,
    backend: str = "codex-cli",
    reasoning_effort: str | None = None,
    speed: str | None = None,
    model: str | None = None,
    codex_bin: str | None = None,
    codex_args: list[str] | None = None,
    grok_bin: str | None = None,
    grok_args: list[str] | None = None,
    adapter_max_turns: int | None = None,
    adapter_timeout: int | None = None,
    # Grok-family adapter constructor fields retained for programmatic callers.
    grok_max_turns: int | None = None,
    grok_timeout: int | None = None,
    review_kind: ReviewKind | str | None = None,
    review_round: str | int | None = None,
    use_latest_slice: bool = False,
    record_findings: bool = False,
    final_result_path: Path | None = None,
    dry_run: bool = False,
    progress_callback: Callable[..., None] | None = None,
    rules_dir: str | Path | None = None,
) -> dict[str, Any]:
    """Run a full review cycle: discover changes, build prompt, execute backend, validate, optionally record.

    implementation note S3 (GK-0145-02): when the lane manifest pins ``preferred_backend``, that
    pin is authoritative from *any* starting backend (defense-in-depth; the pin is
    materialized by ``materialize_offload_lane_manifest``). With no pin, the starting
    backend is used unchanged.
    """
    from workbay_handoff_mcp.enums import ReviewKind  # noqa: PLC0415

    # 1. Load manifest for overrides
    lane_cfg: dict[str, Any] = {}
    if task_ref and lane_id:
        try:
            lane_cfg = (
                get_lane_config(
                    task_ref, lane_id, orchestrator_root=str(orchestrator_root) if orchestrator_root else None
                )
                or {}
            )
        except Exception:
            logger.warning("Lane manifest lookup failed; using fallback review base", exc_info=True)
    committed_base = _resolve_committed_base(worktree_path, lane_cfg.get("base_sha"))

    # Priority: Manifest pin > starting backend (CLI/default). Pin-honor is Plan
    # 0145 S3 (GK-0145-02) — defense-in-depth for materialize_offload_lane_manifest.
    backend_name = backend
    if lane_cfg.get("preferred_backend"):
        backend_name = str(lane_cfg["preferred_backend"])
    backend_name = validate_backend(backend_name)
    # S3R gate fix: reviews obey the install's execution_mode too — a stale
    # local pin (or explicit local backend) must not route a review locally
    # under remote_only. Same failure semantics as a bad pin: raise, and let
    # the caller's existing review-failure mapping surface it.
    from workbay_orchestrator_mcp.orchestration.offload_profiles import (  # noqa: PLC0415
        resolve_offload_backend_for_execution_mode,
    )

    _, _remote_err = resolve_offload_backend_for_execution_mode(backend_name, repo_root=worktree_path)
    if _remote_err is not None:
        raise RuntimeError(_remote_err)

    model_name = model or lane_cfg.get("preferred_model")
    speed = speed if speed is not None else lane_cfg.get("preferred_speed")

    env = None
    if orchestrator_root is not None:
        from workbay_orchestrator_mcp.orchestration._env import pythonpath_env

        env = pythonpath_env(orchestrator_root, task_ref=task_ref, lane_id=lane_id)
    elif reasoning_effort:
        env = {}
    if env is not None:
        apply_backend_runtime_hints(env, reasoning_effort=reasoning_effort)
    scope = _resolve_review_scope(
        worktree_path=worktree_path,
        task_ref=task_ref,
        orchestrator_root=orchestrator_root,
        review_kind=review_kind,
        use_latest_slice=use_latest_slice,
        committed_base=committed_base,
    )
    changed = scope["changed_files"]
    _assert_recordable_review_scope(
        record_findings=record_findings,
        dirty_files=_changed_files(worktree_path),
        scope_source=scope["scope_source"],
    )
    diff_label = None
    if scope["scope_source"] == "slice_packet":
        slice_diff = _slice_diff(worktree_path, scope, committed_base)
        if slice_diff is None:
            unavailable = "Selected slice diff is unavailable: commit_sha or committed base cannot be resolved."
            if record_findings:
                raise RuntimeError(f"review_runner.py refuses to record findings. {unavailable}")
            unified_diff, stat, diff_label = unavailable, "", "slice diff unavailable"
        else:
            unified_diff, stat, diff_label = slice_diff
    else:
        stat = _diff_stat(worktree_path, committed_base) if committed_base is not None else _diff_stat(worktree_path)
        unified_diff = _review_diff(worktree_path, committed_base)
    review_subject = _review_subject(worktree_path, committed_base, scope)
    guides = _detect_stack_guides(changed)
    resolved_diff_base = committed_base[0] if committed_base else (_synthetic_root_base(worktree_path) or "HEAD")
    prior_work = _lookup_related_prior_work(
        lane_objective=str(lane_cfg.get("objective") or ""),
        changed_files=changed,
        task_ref=task_ref,
    )
    prompt = _build_review_prompt(
        changed_files=changed,
        diff_stat=stat,
        unified_diff=unified_diff,
        diff_label=diff_label,
        diff_base="uncommitted" if _changed_files(worktree_path) else resolved_diff_base,
        stack_guides=guides,
        review_subject=review_subject,
        main_guide_filename=(
            "planning-review-guide.md" if scope["review_kind"] == ReviewKind.PLANNING else "branch-review-guide.md"
        ),
        lane_id=lane_id,
        prior_work=prior_work,
        orchestrator_root=orchestrator_root,
        rules_dir=rules_dir,
    )

    if dry_run:
        return {
            "dry_run": True,
            "backend": backend_name,
            "prompt": prompt,
            "findings": [],
            "summary": "Dry-run mode: no review executed.",
            "converged": True,
            "excluded_findings_count": 0,
            "changed_files": changed,
            "review_subject": review_subject,
            "out_of_diff_findings": [],
            "stack_guides": guides,
            "review_kind": scope["review_kind"],
            "scope_source": scope["scope_source"],
            "scope_reason": scope["scope_reason"],
        }

    # Get adapter and execute. Per-backend ctor kwargs (a non-codex CLI adapter
    # rejects codex_bin/codex_args at construction), mirroring lane_exec.
    # Grok-family cycle-bounds capability covers grok-cli and grok-remote so
    # derived max_turns/timeout reach RemoteExecAdapter (0144 S5a / [RES-02]).
    # Routing note: supports_token_budget_cycle_bounds implies the grok CTOR
    # kwargs (grok_bin/grok_args/max_turns/timeout). A backend bounded by wall
    # clock alone declares supports_adapter_timeout_bounds instead and takes the
    # branch below, which passes `timeout` and nothing else — so it is never
    # handed grok kwargs it cannot accept.
    adapter_kwargs: dict[str, Any] = {}
    if backend_name == "codex-cli":
        adapter_kwargs = {"codex_bin": codex_bin, "codex_args": codex_args}
    elif backend_supports_token_budget_cycle_bounds(backend_name):
        adapter_kwargs = {"grok_bin": grok_bin, "grok_args": grok_args}
        cycle_max_turns = grok_max_turns if grok_max_turns is not None else adapter_max_turns
        cycle_timeout = grok_timeout if grok_timeout is not None else adapter_timeout
        if cycle_max_turns is not None:
            adapter_kwargs["max_turns"] = cycle_max_turns
        if cycle_timeout is not None:
            adapter_kwargs["timeout"] = cycle_timeout
        # Materialize the worktree-scoped Composer-only config for standalone
        # review turns that never ran the execute-phase bootstrap, so the
        # attribution config-env is present, not just the prompt-suffix belt
        # (s5-a-008). Idempotent (merge-don't-clobber). Best-effort: a review of a
        # worktree without lane context still runs on the prompt-suffix guarantee.
        # Local grok-cli only — remote turns do not consume a local Composer config.
        if backend_name == "grok-cli" and orchestrator_root is not None and task_ref and lane_id:
            try:
                from workbay_orchestrator_mcp.orchestration.bootstrap_lane import (
                    ensure_grok_lane_config,  # noqa: PLC0415
                )

                ensure_grok_lane_config(worktree_path, model_name)
            except Exception as exc:
                # Never fail a review on config — that contract stays — but do
                # not swallow identity/materialize faults into silent degrade
                # with no .grok/config.toml [OBS-08] (RELAND-A-001).
                print(
                    f"WARNING: grok lane config materialization failed during "
                    f"review (continuing without .grok/config.toml): "
                    f"{type(exc).__name__}: {exc}",
                    file=sys.stderr,
                )
    elif backend_supports_adapter_timeout_bounds(backend_name):
        # Wall-clock-only family: pass the derived bound and NOTHING else.
        # Without this branch the timeout was derived and advertised but never
        # applied, so the adapter silently fell back to its ctor default.
        if adapter_timeout is not None:
            adapter_kwargs = {"timeout": adapter_timeout}
    schema_violations = strict_output_schema_violations(REVIEW_OUTPUT_SCHEMA)
    if schema_violations:
        raise RuntimeError(
            "Review output schema violates strict structured-output mode: " + "; ".join(schema_violations)
        )
    detached_identity: dict[str, str] = {}
    if task_ref and task_ref.strip() and lane_id and lane_id.strip():
        reviewed_head_sha = _git_stdout(worktree_path, "rev-parse", "--verify", "--end-of-options", "HEAD^{commit}")
        if reviewed_head_sha:
            try:
                execution_settings_digest = _detached_execution_settings_digest(
                    backend=backend_name,
                    model=model_name,
                    reasoning_effort=reasoning_effort,
                    speed=speed,
                    dispatch_options=adapter_kwargs,
                    require_resolved=_detached_execution_mode_enabled(),
                )
            except DetachedExecutionSettingsResolutionError as exc:
                return {
                    "kind": "refused",
                    "refusal_cause": "policy_or_classifier_refusal",
                    "retryable": False,
                    "failure_reason": exc.code,
                    "unresolved_setting": exc.setting,
                    "review": "refused",
                    "findings": [],
                    "converged": False,
                    "excluded_findings_count": 0,
                    "summary": f"detached review refused: unresolved execution setting {exc.setting}",
                    "changed_files": changed,
                    "review_subject": review_subject,
                    "out_of_diff_findings": [],
                    "stack_guides": guides,
                    "review_kind": scope["review_kind"],
                    "scope_source": scope["scope_source"],
                    "scope_reason": scope["scope_reason"],
                }
            detached_identity = {
                "task_ref": task_ref.strip(),
                "lane_id": lane_id.strip(),
                "pass_id": _detached_review_pass_id(
                    task_ref=task_ref.strip(),
                    lane_id=lane_id.strip(),
                    reviewed_head_sha=reviewed_head_sha,
                    prompt=prompt,
                    review_round=review_round,
                    final_result_path=final_result_path,
                    execution_settings_digest=execution_settings_digest,
                ),
            }
    adapter = get_adapter(backend_name, **adapter_kwargs)
    result = adapter.execute(
        prompt=prompt,
        schema=REVIEW_OUTPUT_SCHEMA,
        worktree_path=worktree_path,
        model=model_name,
        reasoning_effort=reasoning_effort,
        speed=speed,
        env=env,
        progress_callback=progress_callback,
        **detached_identity,
    )
    raw_result = result.raw_payload
    # Retryable VM admission defer (remote_agent.sh exit 75): the marker dict is
    # non-empty, so it would otherwise tolerant-degrade to skipped_unparseable →
    # converged, masking the defer. No review ran — surface the typed defer so
    # the caller re-dispatches instead of converging (r07163433 HIGH-2).
    if isinstance(raw_result, dict) and raw_result.get("admission_deferred"):
        defer_reason = str(raw_result.get("defer_reason") or "vm_admission")
        raise ReviewAdmissionDeferred(
            f"review turn deferred by VM admission ({defer_reason}); retryable, no review output",
            defer_reason=defer_reason,
        )
    # A typed transport result is not review content.  Reject it before the
    # tolerant parser can turn its non-empty marker payload into an empty,
    # converged skipped_unparseable review.
    if result.handoff_action == "transport_failure" or (
        isinstance(raw_result, dict) and raw_result.get("transport_failure")
    ):
        raw_transport = raw_result if isinstance(raw_result, dict) else {}
        raise ReviewTransportFailure(
            provider_error_code=str(raw_transport.get("provider_error_code") or "turn_failed"),
            retryable=bool(raw_transport.get("retryable", False)),
        )
    # Exhausted empty salvage is also not review content. The envelope is a
    # non-empty needs_guidance marker (salvage_patch_cause=patch_empty AND
    # redispatch_exhausted), so the empty-payload hard fail does not fire and
    # the tolerant parser would otherwise return skipped_unparseable /
    # findings=[] / converged. Reject it the same way admission-defer and
    # provider refusal are rejected: no review product, do not converge.
    # Branch on the stamped cause fields, never on the provider-refusal action.
    if _is_exhausted_empty_salvage(raw_result):
        raise ReviewNoProduct(
            "review turn produced exhausted empty salvage "
            "(salvage_patch_cause=patch_empty, redispatch_exhausted); no review output"
        )
    from workbay_orchestrator_mcp.orchestration.lane_result import adapter_refusal_metadata

    refusal = adapter_refusal_metadata(raw_result)
    if refusal is not None:
        # Fail closed: an adapter refusal contains no review product.
        return {
            **refusal,
            "review": "refused",
            "findings": [],
            "converged": False,
            "excluded_findings_count": 0,
            "summary": result.summary,
            "changed_files": changed,
            "review_subject": review_subject,
            "out_of_diff_findings": [],
            "stack_guides": guides,
            "review_kind": scope["review_kind"],
            "scope_source": scope["scope_source"],
            "scope_reason": scope["scope_reason"],
        }

    # Grok --no-subagents smoke review often emits handoff-shaped JSON (or prose
    # fragments) without findings/summary — tolerate that as a typed degrade so a
    # green self-verify is never hard-failed ([OBS-08]). Codex stays strict.
    # Capability-gated (not a grok-cli name literal) so grok-remote gets the same
    # tolerant degrade the grok family needs (0144 R2). Empty raw_payload still
    # fails loud (0144 R3 / HIGH-2): transport failures must not become
    # skipped_unparseable → converged.
    # Two ways a result arrives as prose: the grok family narrates despite
    # carrying --json-schema, and a backend with NO schema flag at all can only
    # narrate. Gating on the grok-family predicate alone inverted the intent —
    # it routed cursor, the one lane CLI that cannot produce structured output,
    # to STRICT validation while schema-capable grok got the tolerant path.
    _spec = get_backend_spec(backend_name)
    tolerant = backend_supports_token_budget_cycle_bounds(backend_name) or not (
        _spec and _spec.capabilities.supports_structured_output
    )
    validated = _validate_review_result(raw_result, tolerant=tolerant)
    review_status = validated.get("review")
    findings = list(validated.get("findings") or [])
    summary = str(validated.get("summary") or "")

    # implementation note D9: absent result_parse means ok (backward-compatible clean reviews).
    result_parse = str(validated.get("result_parse") or "ok")
    if review_status == REVIEW_SKIPPED_UNPARSEABLE:
        result_parse = "degraded"
    scope_files = None if review_subject.get("scope_kind") == "unresolved_base" else changed
    in_scope_findings, excluded_findings_count = _findings_within_lane_scope(
        findings,
        # Preserve manifest-owned findings for convergence; routing and receipt
        # HIGH counts are narrowed separately to the actual lane diff below.
        owned_paths=lane_cfg.get("owned_paths"),
        changed_files=scope_files,
    )
    if scope_files is None:
        findings = in_scope_findings
    # Convergence = "the fix loop may stop"; harvestability = "these findings may
    # be recorded". A non-authoritative degrade is both converged (nothing to fix)
    # and unharvestable (empty findings are not a clean review). Keep them separate.
    output: dict[str, Any] = {
        "findings": findings,
        "summary": summary,
        "converged": findings_converged(in_scope_findings, review_subject=review_subject),
        "excluded_findings_count": excluded_findings_count,
        "changed_files": changed,
        "review_subject": review_subject,
        "out_of_diff_findings": _out_of_diff_findings(findings, scope_files),
        "stack_guides": guides,
        "review_kind": scope["review_kind"],
        "scope_source": scope["scope_source"],
        "scope_reason": scope["scope_reason"],
        "result_parse": result_parse,
    }
    if (
        review_subject["kind"] == "unbounded"
        and not changed
        and any(str(item.get("severity") or "").strip().upper() == "HIGH" for item in findings)
    ):
        output["escalation_skipped"] = EscalationSkipReason.REVIEW_SUBJECT_UNBOUNDED
    if review_status == REVIEW_SKIPPED_UNPARSEABLE:
        # Explicit discriminator so the pass engine can branch without archaeology.
        output["review"] = REVIEW_SKIPPED_UNPARSEABLE
    if validated.pop("_missing_findings_key", False):
        output["review"] = REVIEW_SKIPPED_UNPARSEABLE
        output["result_parse"] = "degraded"
    # Always forward salvage/degrade notes ([OBS-04]): mixed valid/invalid findings
    # keep salvaged items while raw_tail records dropped items — not only on full
    # skipped_unparseable degrade (BR-0108-S1-05).
    if "raw_tail" in validated:
        output["raw_tail"] = validated["raw_tail"]

    if output.get("review") is not None:
        review_status = str(output["review"])
    operation_token = None
    if final_result_path is not None:
        import uuid

        operation_token = uuid.uuid4().hex
        from workbay_orchestrator_mcp.orchestration.pass_rescue import _atomic_write

        # Publish the review product before any coordinator side effect. The
        # engine reads this carrier even when recording below raises.
        carrier = {
            **output,
            "review_result_carrier": True,
            "review_operation_token": operation_token,
            "raw_payload": raw_result,
            "details": "GROK_REVIEW_FINDINGS_JSON\n" + json.dumps(findings),
        }
        final_result_path.parent.mkdir(parents=True, exist_ok=True)
        _atomic_write(final_result_path, json.dumps(carrier))
    raw_harvestable = isinstance(raw_result, dict) and review_result_is_harvestable(raw_result)
    if (
        record_findings
        and review_status != REVIEW_SKIPPED_UNPARSEABLE
        and (review_result_is_harvestable(output) or raw_harvestable)
    ):
        if not task_ref:
            raise RuntimeError("--task-ref is required when --record-findings is set.")
        if not session:
            raise RuntimeError("--session is required when --record-findings is set.")
        if not orchestrator_root:
            raise RuntimeError("--orchestrator-root is required when --record-findings is set.")
        try:
            persist = _record_findings(
                findings,
                task_ref=task_ref,
                session=session,
                lane_id=lane_id,
                orchestrator_root=orchestrator_root,
                **({"operation_token": operation_token} if operation_token else {}),
            )
        except FindingsPersistError as exc:
            shortfall = _persist_shortfall_block(exc)
            output["legacy_reconciliation"] = exc.persist.get("legacy_reconciliation", "none")
            output["legacy_cause"] = exc.persist.get("legacy_cause", shortfall["causes"].get("legacy_reconciliation"))
            output["recorded_finding_ids"] = shortfall["recorded_ids"]
            output["error"] = "record_failed"
            output.update(shortfall)
            if final_result_path is not None:
                carrier.update(output)
                _atomic_write(final_result_path, json.dumps(carrier))
            raise
        shortfall = _persist_shortfall_block(persist)
        recorded_ids = shortfall["recorded_ids"]
        unrecorded_ids = shortfall["unrecorded_ids"]
        unrecorded_findings = shortfall["unrecorded_findings"]
        causes = shortfall["causes"]
        output["legacy_reconciliation"] = persist.get("legacy_reconciliation", "none")
        output["legacy_cause"] = persist.get("legacy_cause", causes.get("legacy_reconciliation"))
        if causes:
            output["causes"] = causes
        output["recorded_finding_ids"] = recorded_ids
        if final_result_path is not None:
            # Retain the pre-write carrier until this atomic replacement lands.
            carrier.update(output)
            carrier.update(shortfall)
            _atomic_write(final_result_path, json.dumps(carrier))
        if unrecorded_ids:
            output["unrecorded_ids"] = unrecorded_ids
            output["unrecorded_findings"] = unrecorded_findings
            output["causes"] = causes
            output["error"] = "record_failed"
            logger.warning(
                "review finding persist shortfall (task_ref=%s session=%s lane_id=%s recorded=%s unrecorded=%s): %s",
                task_ref,
                session,
                lane_id,
                recorded_ids,
                unrecorded_ids,
                causes,
            )
            raise RuntimeError(
                "review finding persist shortfall: "
                f"recorded_ids={recorded_ids} unrecorded_ids={unrecorded_ids} causes={causes}"
            )

    return output


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Self-review runner: execute structured code review via Codex.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("schema", help="Print the review output JSON schema.")

    run_parser = subparsers.add_parser("run", help="Execute a review against a lane worktree.")
    run_parser.add_argument("--worktree-path", required=True, help="Path to the lane worktree.")
    run_parser.add_argument("--lane-id", help="Lane identifier for finding ID generation.")
    run_parser.add_argument("--task-ref", help="Task reference (required with --record-findings).")
    run_parser.add_argument("--session", help="Session identifier (required with --record-findings).")
    run_parser.add_argument("--orchestrator-root", help="Orchestrator root path (required with --record-findings).")
    run_parser.add_argument(
        "--rules-dir",
        help="Optional review-rules directory override. Defaults to the package-bundled rules.",
    )
    run_parser.add_argument(
        "--backend",
        default="codex-cli",
        choices=BACKEND_CHOICES,
        help="Execution backend to use (default: codex-cli).",
    )
    run_parser.add_argument(
        "--reasoning-effort",
        choices=WORKER_REASONING_EFFORT_CHOICES,
        help="Optional reasoning effort hint for codex-subagent review turns.",
    )
    run_parser.add_argument("--model", help="Explicit model to use (e.g. gpt-5.4-mini).")
    run_parser.add_argument("--grok-bin", help="Explicit path to the grok binary.")
    run_parser.add_argument("--grok-args", help="Extra args for grok exec (space-separated).")
    run_parser.add_argument(
        "--adapter-max-turns",
        "--grok-max-turns",
        dest="adapter_max_turns",
        type=int,
        default=None,
        help="Per-invocation adapter turn cap (default: adapter/backend default; --grok-max-turns is deprecated).",
    )
    run_parser.add_argument(
        "--adapter-timeout",
        "--grok-timeout",
        dest="adapter_timeout",
        type=int,
        default=None,
        help="Per-invocation adapter wall-clock timeout seconds (default: adapter/backend default; --grok-timeout is deprecated).",
    )
    run_parser.add_argument(
        "--review-kind",
        choices=("branch", "planning"),
        help="Preferred review workflow. Packet-backed planning reviews only match docs-only slices.",
    )
    run_parser.add_argument(
        "--latest-slice",
        action="store_true",
        help="Resolve changed files from the latest completed slice packet when available, with branch-diff fallback.",
    )
    run_parser.add_argument(
        "--record-findings",
        action="store_true",
        help="Record findings into MCP before returning.",
    )
    run_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the assembled prompt and skip Codex/MCP side effects.",
    )

    return parser.parse_args()


def main() -> int:
    args = _parse_args()

    if args.command == "schema":
        print(json.dumps(REVIEW_OUTPUT_SCHEMA, indent=2))
        return 0

    worktree_path = Path(args.worktree_path).expanduser().resolve()
    orchestrator_root = Path(args.orchestrator_root).expanduser().resolve() if args.orchestrator_root else None
    rules_dir = Path(args.rules_dir).expanduser().resolve() if args.rules_dir else None

    result = run_review(
        worktree_path=worktree_path,
        lane_id=args.lane_id,
        task_ref=args.task_ref,
        session=args.session,
        orchestrator_root=orchestrator_root,
        backend=args.backend,
        reasoning_effort=args.reasoning_effort,
        model=args.model,
        grok_bin=args.grok_bin,
        grok_args=args.grok_args.split() if args.grok_args else None,
        adapter_max_turns=args.adapter_max_turns,
        adapter_timeout=args.adapter_timeout,
        review_kind=args.review_kind,
        use_latest_slice=args.latest_slice,
        record_findings=args.record_findings,
        dry_run=args.dry_run,
        rules_dir=rules_dir,
    )

    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
