#!/usr/bin/env python3
"""Deterministic review-pipeline primitives: stage, adjudicate, merge-gate.

Mechanism for three transitions of ``docs/workbay/rules/branch-pipeline.md``
that previously existed only as prose:

* ``stage``       — IMPLEMENTING -> STAGED_FOR_REVIEW: cut the review worktree
  (``worktree add -b feature/rev<r>-<subject>`` from the integration head) when
  ``--worktree`` is not given, write and commit the review input patch there,
  and derive proof-of-reading keys into a dispatcher-held sidecar under
  ``.task-state/review-por/`` (gitignored; the keys must never travel in a
  brief).
* ``adjudicate``  — ADJUDICATING -> verdict: re-derive the keys at gate time,
  byte-compare them against each review doc, parse the verdict line, count
  MEDIUM+ findings, and write a durable adjudication artifact under
  ``.task-state/review-adjudication/``.
* ``merge-gate``  — MERGING precondition: refuse unless the adjudication
  artifact exists, says MERGE, and is fenced to the subject branch's current
  tip (an adjudication of an older tip is stale evidence, not permission).

Every probe fails closed: a missing file, an unparseable doc, or an unknown
answer refuses, never proceeds. Exit 0 = pass, exit 2 = refuse, with a JSON
receipt on stdout either way. Policy knobs (verdict tokens, severity floor)
are module constants; the mechanism below them does not embed judgment calls.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path

VERDICT_MERGE = "VERDICT: MERGE"
VERDICT_REVISE = "VERDICT: REVISE"
_VERDICT_TOKENS = {VERDICT_MERGE: "MERGE", VERDICT_REVISE: "REVISE"}
# Severities at or above this set survive to block a MERGE verdict.
BLOCKING_SEVERITIES = ("MEDIUM", "HIGH", "CRITICAL", "BLOCKING")
_ALL_SEVERITIES = (*BLOCKING_SEVERITIES, "LOW")
# Sampled patch lines for proof-of-reading (1-indexed; clamped to length).
POR_SAMPLE_LINES = (41, 173)
POR_FIELD_NAMES = frozenset({"file_count", "line_count", "md5", "sample_lines"})
POR_PREFIXES = ("POR:", "PROOF-OF-READING:", "PROOF:")

POR_DIR = Path(".task-state") / "review-por"
ADJUDICATION_DIR = Path(".task-state") / "review-adjudication"

# A branch may legitimately need to commit the *text* of a merge marker (a
# fixture, a runbook). The override is explicit and is reported on the receipt;
# no path is exempted implicitly. Mirrors the slice-commit guard's hatch so an
# operator learns one name.
CONFLICT_MARKER_ESCAPE_HATCH_ENV = "WORKBAY_ALLOW_CONFLICT_MARKERS"
_CONFLICT_MARKER_DEFAULT_SIZE = 7
_CONFLICT_MARKER_MIN_SIZE = 2

_ARTIFACT_BRANCH_SLUG_RE = re.compile(r"[^A-Za-z0-9._-]+")
_FINDING_HEADER = re.compile(
    r"^\s*(?:(?:[-*+]\s+)|(?:#{1,6}\s+))?"
    r"(?P<id>(?:F\d+|[HML]-\d+|G\d+-\d+))(?=$|[\s:—–-])(?P<rest>.*)$",
    re.IGNORECASE,
)
_FINDING_CANDIDATE = re.compile(
    r"^\s*(?:(?:[-*+]\s+)|(?:#{1,6}\s+))?"
    r"(?P<id>[FHGML](?:-?\d+)(?:-\d+)?[A-Za-z0-9_-]*)(?=$|[\s:—–-])",
    re.IGNORECASE,
)
_SEVERITY_WORD = r"(?:LOW|MEDIUM|HIGH|CRITICAL|BLOCKING)(?![A-Za-z0-9_-])"
_INLINE_SEVERITY = re.compile(
    rf"(?:\(\s*(?P<paren>{_SEVERITY_WORD})\s*\)"
    rf"|\[\s*(?P<bracket>{_SEVERITY_WORD})\s*\]"
    rf"|\bSeverity\s*:\s*(?P<label>{_SEVERITY_WORD})\b"
    rf"|^[\s:—–-]+(?P<bare>{_SEVERITY_WORD})(?=\s*(?:[:—–-]|$)))",
    re.IGNORECASE,
)
_UNKNOWN_INLINE_SEVERITY = re.compile(
    r"(?:\(\s*[A-Za-z][^)]*\)|\[\s*[A-Za-z][^]]*\]|"
    r"\bSeverity\s*:\s*[A-Za-z]+\b|^[\s:—–-]+"
    r"(?:LOW|MEDIUM|HIGH|CRITICAL|BLOCKING|SEVERE|URGENT)\b)",
    re.IGNORECASE,
)
_FOLLOWING_SEVERITY = re.compile(
    rf"^\s*[*_`#-]*\s*Severity\s*:\s*[*_`]*(?P<severity>{_SEVERITY_WORD})\b"
    rf"\s*[*_`]*\s*$",
    re.IGNORECASE,
)


class DiffOutcome(str, Enum):
    """Typed result for the subject diff probe."""

    FAILED = "failed"
    EMPTY = "empty"
    NONEMPTY = "nonempty"


@dataclass(frozen=True)
class DiffResult:
    """A diff probe result whose failure direction cannot collapse to empty."""

    outcome: DiffOutcome
    base: str
    subject: str
    data: bytes = b""
    stderr: str = ""
    returncode: int = 0


@dataclass(frozen=True)
class FindingResult:
    """Canonical finding parse result, including malformed finding markers."""

    blocking: int
    findings: tuple[dict, ...]
    invalid: tuple[str, ...]


class GitCommandError(RuntimeError):
    """A git probe failed and must be surfaced as a typed refusal."""

    def __init__(self, argv: tuple[str, ...], returncode: int, stderr: str):
        self.argv = argv
        self.returncode = returncode
        self.stderr = stderr
        super().__init__(f"git {' '.join(argv)} failed ({returncode})")


def _coerce_bytes(value: bytes | str | None) -> bytes:
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode("utf-8")
    return b""


def _git(root: Path, *args: str) -> str:
    proc = subprocess.run(
        ["git", "-C", str(root), *args],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    if proc.returncode != 0:
        raise GitCommandError(tuple(args), proc.returncode, proc.stderr.strip()[:400])
    return proc.stdout.strip()


def _git_bytes(root: Path, *args: str) -> bytes:
    proc = subprocess.run(
        ["git", "-C", str(root), *args],
        capture_output=True,
        text=False,
        timeout=60,
        check=False,
    )
    if proc.returncode != 0:
        raise GitCommandError(tuple(args), proc.returncode, _coerce_bytes(proc.stderr).decode("utf-8", "replace")[:400])
    return _coerce_bytes(proc.stdout)


def _default_judge_root() -> Path:
    source = Path(__file__).resolve()
    for parent in source.parents:
        if (parent / "Makefile").is_file() and (parent / "packages").is_dir():
            return parent
    return source.parents[5] if len(source.parents) > 5 else source.parent


def _resolve_executable(value: str, *, relative_to: Path | None = None) -> Path:
    resolved = shutil.which(value) if "/" not in value else value
    candidate = Path(resolved or value).expanduser()
    if not candidate.is_absolute() and relative_to is not None:
        candidate = relative_to / candidate
    return candidate.resolve()


def _configured_executable_path(value: str, judge_root: Path) -> Path:
    """Keep the configured path identity before resolving symlinks."""
    if "/" not in value:
        resolved = shutil.which(value)
        return Path(resolved or value).expanduser().absolute()
    candidate = Path(value).expanduser()
    if not candidate.is_absolute():
        candidate = judge_root / candidate
    return candidate.absolute()


def judge_provenance() -> dict:
    """Return the judge root/interpreter identity used for this invocation."""
    source = Path(__file__).resolve()
    judge_root = Path(os.environ.get("JUDGE_ROOT") or _default_judge_root()).expanduser().resolve()
    configured_python = os.environ.get("SYSTEM_PYTHON")
    configured_path = _configured_executable_path(configured_python, judge_root) if configured_python else None
    system_python = _resolve_executable(configured_python or sys.executable, relative_to=judge_root)
    executable = Path(sys.executable).resolve()
    return {
        "judge_root": str(judge_root),
        "system_python": str(system_python),
        "configured_python": configured_python,
        "executable": str(executable),
        "source": str(source),
        "verified": (
            source.is_relative_to(judge_root)
            and executable == system_python
            and (configured_path is None or configured_path.is_relative_to(judge_root))
        ),
    }


def _judge_provenance_refusal() -> dict | None:
    provenance = judge_provenance()
    if provenance["verified"]:
        return None
    return {"judge_provenance": provenance}


def _emit(payload: dict) -> None:
    output = dict(payload)
    output.setdefault("judge_provenance", judge_provenance())
    print(json.dumps(output, indent=2, sort_keys=True))


def _refuse(reason: str, extra: dict | None = None) -> int:
    payload = {"ok": False, "reason": reason}
    if extra:
        payload.update(extra)
    _emit(payload)
    return 2


def _branch_file_slug(branch: str) -> str:
    return _ARTIFACT_BRANCH_SLUG_RE.sub("-", branch).strip("-")


def _review_branch(subject: str, rev: int) -> str:
    stripped = subject.split("/", 1)[-1]
    return f"feature/rev{rev}-{_branch_file_slug(stripped)}"


def _path_exists_including_broken_symlink(path: Path) -> bool:
    return path.exists() or path.is_symlink()


def derive_por_keys(patch_path: Path) -> dict:
    """Derive the proof-of-reading keys from a patch file's exact bytes."""
    data = patch_path.read_bytes()
    lines = data.decode("utf-8", errors="replace").splitlines()
    nonblank = [(index, line) for index, line in enumerate(lines, start=1) if line.strip()]
    selected: list[tuple[int, str]] = []
    for lineno in POR_SAMPLE_LINES:
        if lineno <= len(lines) and lines[lineno - 1].strip():
            selected.append((lineno, lines[lineno - 1]))
    for candidate in nonblank:
        if len(selected) >= min(2, len(nonblank)):
            break
        if candidate not in selected:
            selected.append(candidate)
    samples = {str(lineno): content for lineno, content in selected}
    return {
        "line_count": len(lines),
        "md5": hashlib.md5(data).hexdigest(),
        "file_count": sum(1 for line in lines if line.startswith("+++ ")),
        "sample_lines": samples,
    }


def _run_subject_diff(root: Path, base: str, subject: str) -> DiffResult:
    """Run the diff probe while preserving failed/empty/nonempty identity."""
    proc = subprocess.run(
        ["git", "-C", str(root), "diff", f"{base}..{subject}"],
        capture_output=True,
        text=False,
        timeout=120,
        check=False,
    )
    data = _coerce_bytes(proc.stdout)
    stderr = _coerce_bytes(proc.stderr).decode("utf-8", "replace").strip()[:400]
    if proc.returncode != 0:
        return DiffResult(DiffOutcome.FAILED, base, subject, stderr=stderr, returncode=proc.returncode)
    if not data.strip():
        return DiffResult(DiffOutcome.EMPTY, base, subject, stderr=stderr)
    return DiffResult(DiffOutcome.NONEMPTY, base, subject, data=data, stderr=stderr)


def _worktree_entries(root: Path) -> list[dict[str, str]]:
    """Parse root-owned porcelain worktree records without trusting a path alone."""
    output = _git(root, "worktree", "list", "--porcelain")
    entries: list[dict[str, str]] = []
    current: dict[str, str] = {}

    def flush() -> None:
        if current:
            entries.append(dict(current))
            current.clear()

    for line in output.splitlines():
        if not line.strip():
            flush()
            continue
        key, separator, value = line.partition(" ")
        if separator:
            current[key] = value
    flush()
    return entries


def _validate_review_worktree(root: Path, worktree: Path, expected_branch: str) -> tuple[bool, dict]:
    """Require a registered worktree on this repo's dedicated review branch."""
    if not worktree.is_dir():
        return False, {"reason": "worktree_missing", "worktree": str(worktree)}
    try:
        entries = _worktree_entries(root)
    except GitCommandError as exc:
        return False, {
            "reason": "review_worktree_probe_failed",
            "worktree": str(worktree),
            "stderr": exc.stderr,
        }
    entry = next(
        (item for item in entries if Path(item.get("worktree", "")).resolve() == worktree),
        None,
    )
    if entry is None:
        return False, {
            "reason": "review_worktree_not_linked",
            "worktree": str(worktree),
            "expected_branch": expected_branch,
        }
    expected_ref = f"refs/heads/{expected_branch}"
    if entry.get("branch") != expected_ref:
        return False, {
            "reason": "review_worktree_not_dedicated",
            "worktree": str(worktree),
            "expected_branch": expected_branch,
            "actual_branch": entry.get("branch"),
        }
    try:
        head = _git(worktree, "rev-parse", "HEAD")
    except GitCommandError as exc:
        return False, {
            "reason": "review_worktree_probe_failed",
            "worktree": str(worktree),
            "stderr": exc.stderr,
        }
    if entry.get("HEAD") and entry["HEAD"] != head:
        return False, {
            "reason": "review_worktree_head_changed_during_probe",
            "worktree": str(worktree),
            "listed_head": entry.get("HEAD"),
            "current_head": head,
        }
    return True, {
        "worktree": str(worktree),
        "review_branch": expected_branch,
        "review_head": head,
    }


def _cleanup_created_review_worktree(root: Path, worktree: Path, branch: str) -> dict:
    """Remove only the review tree/branch created by this stage transaction."""
    failures: list[dict] = []
    worktree_removed = False
    branch_deleted = False
    remove = subprocess.run(
        ["git", "-C", str(root), "worktree", "remove", "--force", str(worktree)],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    if remove.returncode == 0:
        worktree_removed = True
    else:
        failures.append(
            {
                "operation": "worktree_remove",
                "returncode": remove.returncode,
                "stderr": remove.stderr.strip()[:400],
            }
        )
    if worktree_removed:
        delete = subprocess.run(
            ["git", "-C", str(root), "branch", "-D", branch],
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
        if delete.returncode == 0:
            branch_deleted = True
        else:
            failures.append(
                {
                    "operation": "branch_delete",
                    "returncode": delete.returncode,
                    "stderr": delete.stderr.strip()[:400],
                }
            )
    return {
        "attempted": True,
        "ok": not failures,
        "worktree_removed": worktree_removed,
        "branch_deleted": branch_deleted,
        "failures": failures,
    }


def _stage_refusal(
    root: Path,
    reason: str,
    extra: dict | None,
    *,
    created_worktree: Path | None,
    created_branch: str | None,
) -> int:
    """Refuse and roll back only resources owned by this stage invocation."""
    details = dict(extra or {})
    if created_worktree is None or created_branch is None:
        return _refuse(reason, details)
    cleanup = _cleanup_created_review_worktree(root, created_worktree, created_branch)
    details["cleanup"] = cleanup
    if not cleanup["ok"]:
        details["original_reason"] = reason
        return _refuse("cleanup_failed", details)
    return _refuse(reason, details)


def cmd_stage(args: argparse.Namespace) -> int:
    root = Path(args.root).resolve()
    provenance_error = _judge_provenance_refusal()
    if provenance_error is not None:
        return _refuse("judge_provenance_mismatch", provenance_error)
    try:
        subject_tip = _git(root, "rev-parse", args.subject)
    except GitCommandError as exc:
        return _refuse(
            "git_failed",
            {"argv": list(exc.argv), "returncode": exc.returncode, "stderr": exc.stderr},
        )
    short = subject_tip[:9]
    rev = int(args.rev)
    rev_branch = _review_branch(args.subject, rev)
    sidecar = root / POR_DIR / f"{args.slug}-{short}.json"
    if _path_exists_including_broken_symlink(sidecar):
        return _refuse("por_sidecar_exists", {"sidecar": str(sidecar)})
    created_worktree: Path | None = None
    if args.worktree:
        worktree = Path(args.worktree).resolve()
        valid, detail = _validate_review_worktree(root, worktree, rev_branch)
        if not valid:
            return _refuse(detail.pop("reason"), detail)
    else:
        worktree = root.parent / f"{root.name}-rev{rev}-{_branch_file_slug(args.subject.split('/', 1)[-1])}"
        if _path_exists_including_broken_symlink(worktree):
            return _refuse("rev_worktree_path_exists", {"worktree": str(worktree)})
        add = subprocess.run(
            [
                "git",
                "-C",
                str(root),
                "worktree",
                "add",
                "-b",
                rev_branch,
                str(worktree),
                args.integration,
            ],
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )
        if add.returncode != 0:
            return _refuse(
                "rev_worktree_add_failed",
                {
                    "branch": rev_branch,
                    "worktree": str(worktree),
                    "stderr": add.stderr.strip()[:400],
                },
            )
        created_worktree = worktree
    try:
        base = _git(root, "merge-base", args.integration, args.subject)
        diff = _run_subject_diff(root, base, args.subject)
        diff_detail = {
            "base": base,
            "subject": args.subject,
            "diff_outcome": diff.outcome.value,
            "stderr": diff.stderr,
        }
        if diff.outcome is DiffOutcome.FAILED:
            return _stage_refusal(
                root,
                "diff_failed",
                {**diff_detail, "returncode": diff.returncode},
                created_worktree=created_worktree,
                created_branch=rev_branch if created_worktree else None,
            )
        if diff.outcome is DiffOutcome.EMPTY:
            return _stage_refusal(
                root,
                "empty_diff",
                diff_detail,
                created_worktree=created_worktree,
                created_branch=rev_branch if created_worktree else None,
            )

        reviews_dir = worktree / "docs" / "reviews"
        reviews_dir.mkdir(parents=True, exist_ok=True)
        patch_name = f"_input-{args.slug}-{short}.patch"
        patch_path = reviews_dir / patch_name
        if _path_exists_including_broken_symlink(patch_path):
            return _stage_refusal(
                root,
                "review_patch_path_exists",
                {"patch": str(patch_path)},
                created_worktree=created_worktree,
                created_branch=rev_branch if created_worktree else None,
            )
        patch_path.write_bytes(diff.data)

        rel = patch_path.relative_to(worktree)
        _git(worktree, "add", str(rel))
        _git(
            worktree,
            "commit",
            "-m",
            f"review: stage input patch for {args.slug} at {short}",
            "--",
            str(rel),
        )
        staged_commit = _git(worktree, "rev-parse", "HEAD")

        keys = derive_por_keys(patch_path)
        sidecar_dir = root / POR_DIR
        sidecar_dir.mkdir(parents=True, exist_ok=True)
        sidecar.write_text(
            json.dumps(
                {
                    "schema_version": 2,
                    "slug": args.slug,
                    "rev": rev,
                    "subject": args.subject,
                    "subject_tip": subject_tip,
                    "integration": args.integration,
                    "base": base,
                    "patch": str(rel),
                    "patch_sha256": hashlib.sha256(diff.data).hexdigest(),
                    "keys": keys,
                    "staged_commit": staged_commit,
                    "worktree": str(worktree),
                    "review_branch": rev_branch,
                    "judge_provenance": judge_provenance(),
                    "ts": datetime.now(timezone.utc).isoformat(),
                },
                indent=2,
                sort_keys=True,
            ),
            encoding="utf-8",
        )
    except GitCommandError as exc:
        return _stage_refusal(
            root,
            "git_failed",
            {"argv": list(exc.argv), "returncode": exc.returncode, "stderr": exc.stderr},
            created_worktree=created_worktree,
            created_branch=rev_branch if created_worktree else None,
        )
    except OSError as exc:
        return _stage_refusal(
            root,
            "stage_write_failed",
            {"error": str(exc)[:400]},
            created_worktree=created_worktree,
            created_branch=rev_branch if created_worktree else None,
        )

    _emit(
        {
            "ok": True,
            "worktree": str(worktree),
            "rev_branch": rev_branch,
            "patch": str(patch_path),
            "staged_commit": staged_commit,
            "subject_tip": subject_tip,
            "por_sidecar": str(sidecar),
            "diff_outcome": DiffOutcome.NONEMPTY.value,
            "judge_provenance": judge_provenance(),
            "note": "POR keys are dispatcher-held; never copy them into a brief.",
        }
    )
    return 0


def _doc_por_passes(doc_text: str, keys: dict) -> tuple[bool, str]:
    """Require one exact structured POR object, including every sample field."""
    if not isinstance(keys, dict) or set(keys) != POR_FIELD_NAMES:
        return False, "por_keys_malformed"
    if type(keys.get("line_count")) is not int or type(keys.get("file_count")) is not int:
        return False, "por_counts_malformed"
    samples = keys.get("sample_lines")
    if (
        not isinstance(samples, dict)
        or not samples
        or any(
            not isinstance(line, str)
            or not isinstance(value, str)
            or not value.strip()
            or not line.isdigit()
            or int(line) < 1
            or int(line) > keys["line_count"]
            for line, value in samples.items()
        )
    ):
        return False, "por_samples_empty_or_clamped"
    candidates = []
    for line in doc_text.splitlines():
        stripped = line.strip()
        prefix = next((item for item in POR_PREFIXES if stripped.startswith(item)), None)
        if prefix is not None:
            candidates.append(stripped[len(prefix) :].strip())
    if len(candidates) != 1:
        return False, "por_structure_missing_or_ambiguous"
    try:
        provided = json.loads(candidates[0])
    except (TypeError, json.JSONDecodeError):
        return False, "por_structure_malformed"
    if not isinstance(provided, dict) or set(provided) != POR_FIELD_NAMES:
        return False, "por_fields_mismatch"
    if type(provided.get("line_count")) is not int or type(provided.get("file_count")) is not int:
        return False, "por_counts_malformed"
    if not isinstance(provided.get("md5"), str) or not isinstance(provided.get("sample_lines"), dict):
        return False, "por_fields_malformed"
    if provided != keys:
        return False, "por_values_mismatch"
    return True, "ok"


def _doc_verdict(doc_text: str) -> str | None:
    first = doc_text.lstrip().splitlines()[0].strip() if doc_text.strip() else ""
    return _VERDICT_TOKENS.get(first)


def _inline_severity(rest: str) -> str | None:
    match = _INLINE_SEVERITY.search(rest)
    if match is None:
        return None
    return next((value for value in match.groupdict().values() if value), "").upper()


def _parse_findings(doc_text: str) -> FindingResult:
    """Parse only canonical finding IDs and explicit severity metadata."""
    lines = doc_text.splitlines()
    findings: list[dict] = []
    invalid: list[str] = []
    for index, line in enumerate(lines):
        header = _FINDING_HEADER.match(line)
        if header is None:
            candidate = _FINDING_CANDIDATE.match(line)
            if candidate is not None:
                invalid.append(f"{candidate.group('id').upper()}:unknown_finding_id")
            continue
        finding_id = header.group("id").upper()
        rest = header.group("rest")
        severity = _inline_severity(rest)
        if severity is None and _UNKNOWN_INLINE_SEVERITY.search(rest):
            invalid.append(f"{finding_id}:invalid_inline_severity")
            continue
        if severity is None:
            for following in lines[index + 1 :]:
                if _FINDING_HEADER.match(following):
                    break
                if not following.strip():
                    continue
                marker = _FOLLOWING_SEVERITY.match(following)
                if marker is not None:
                    severity = marker.group("severity").upper()
                    break
                if re.match(r"^\s*[*_`#-]*\s*Severity\s*:", following, re.IGNORECASE):
                    invalid.append(f"{finding_id}:invalid_following_severity")
                    break
        if severity not in _ALL_SEVERITIES:
            invalid.append(f"{finding_id}:missing_or_unknown_severity")
            continue
        findings.append({"id": finding_id, "severity": severity})
    return FindingResult(
        blocking=sum(item["severity"] in BLOCKING_SEVERITIES for item in findings),
        findings=tuple(findings),
        invalid=tuple(invalid),
    )


def _count_blocking_findings(doc_text: str) -> int:
    return _parse_findings(doc_text).blocking


def _stage_sidecar_for_patch(
    root: Path, patch_path: Path, subject: str, subject_tip: str
) -> tuple[Path | None, dict | None, dict | None]:
    """Find the unique sidecar that names this exact staged patch."""
    sidecar_dir = root / POR_DIR
    if not sidecar_dir.is_dir():
        return None, None, {"reason": "stage_sidecar_missing", "sidecar_dir": str(sidecar_dir)}
    candidates = sorted(sidecar_dir.glob(f"*-{subject_tip[:9]}.json"))
    matching: list[tuple[Path, dict]] = []
    malformed: list[str] = []
    for sidecar in candidates:
        try:
            record = json.loads(sidecar.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError):
            malformed.append(str(sidecar))
            continue
        if not isinstance(record, dict):
            malformed.append(str(sidecar))
            continue
        if record.get("subject") != subject or record.get("subject_tip") != subject_tip:
            continue
        try:
            worktree = Path(record["worktree"]).resolve()
            relative_patch = patch_path.relative_to(worktree)
        except (KeyError, TypeError, ValueError):
            continue
        if str(relative_patch) == record.get("patch"):
            matching.append((sidecar, record))
    if len(matching) == 1:
        sidecar, record = matching[0]
        return sidecar, record, None
    if len(matching) > 1:
        return (
            None,
            None,
            {
                "reason": "stage_sidecar_ambiguous",
                "sidecars": [str(path) for path, _ in matching],
            },
        )
    return (
        None,
        None,
        {
            "reason": "stage_sidecar_missing",
            "candidates": [str(path) for path in candidates],
            "malformed_sidecars": malformed,
            "patch": str(patch_path),
        },
    )


def _is_ancestor(root: Path, ancestor: str, descendant: str) -> bool | None:
    proc = subprocess.run(
        ["git", "-C", str(root), "merge-base", "--is-ancestor", ancestor, descendant],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    if proc.returncode == 0:
        return True
    if proc.returncode == 1:
        return False
    return None


def _committed_file_matches(worktree: Path, commit: str, relative: Path, path: Path) -> tuple[bool, str]:
    try:
        committed = _git_bytes(worktree, "show", f"{commit}:{relative.as_posix()}")
        current = path.read_bytes()
    except (GitCommandError, OSError):
        return False, "review_doc_not_committed"
    if current != committed:
        return False, "review_doc_worktree_changed"
    return True, "ok"


def _validate_stage_binding(
    root: Path, patch_path: Path, subject: str, subject_tip: str
) -> tuple[dict | None, dict | None]:
    sidecar_path, record, error = _stage_sidecar_for_patch(root, patch_path, subject, subject_tip)
    if error is not None:
        return None, error
    assert sidecar_path is not None and record is not None
    required = {
        "schema_version",
        "subject",
        "subject_tip",
        "integration",
        "base",
        "patch",
        "patch_sha256",
        "keys",
        "staged_commit",
        "worktree",
        "review_branch",
        "rev",
        "judge_provenance",
    }
    if record.get("schema_version") != 2 or not required.issubset(record):
        return None, {"reason": "stage_sidecar_malformed", "sidecar": str(sidecar_path)}
    try:
        worktree = Path(record["worktree"]).resolve()
        staged_commit = str(record["staged_commit"])
        relative_patch = patch_path.relative_to(worktree)
        patch_data = patch_path.read_bytes()
    except (KeyError, TypeError, ValueError, OSError) as exc:
        return None, {"reason": "stage_binding_invalid", "error": str(exc)[:400]}
    if str(relative_patch) != record["patch"]:
        return None, {"reason": "stage_patch_path_mismatch", "sidecar": str(sidecar_path)}
    try:
        record_rev = int(record["rev"])
    except (KeyError, TypeError, ValueError):
        return None, {"reason": "stage_sidecar_malformed", "sidecar": str(sidecar_path)}
    expected_branch = _review_branch(subject, record_rev)
    if record.get("review_branch") != expected_branch:
        return None, {
            "reason": "stage_review_branch_mismatch",
            "expected_branch": expected_branch,
            "actual_branch": record.get("review_branch"),
        }
    valid_worktree, worktree_detail = _validate_review_worktree(root, worktree, record["review_branch"])
    if not valid_worktree:
        return None, {"reason": "stage_review_worktree_invalid", **worktree_detail}
    current_head = worktree_detail["review_head"]
    descendant = _is_ancestor(root, staged_commit, current_head)
    if descendant is not True:
        return None, {
            "reason": "stage_commit_not_in_review_worktree",
            "staged_commit": staged_commit,
            "review_head": current_head,
        }
    try:
        staged_patch = _git_bytes(worktree, "show", f"{staged_commit}:{relative_patch.as_posix()}")
    except GitCommandError as exc:
        return None, {
            "reason": "stage_patch_not_committed",
            "staged_commit": staged_commit,
            "stderr": exc.stderr,
        }
    if staged_patch != patch_data:
        return None, {
            "reason": "stage_patch_not_committed",
            "staged_commit": staged_commit,
            "patch_sha256": hashlib.sha256(patch_data).hexdigest(),
            "staged_patch_sha256": hashlib.sha256(staged_patch).hexdigest(),
        }
    keys = derive_por_keys(patch_path)
    if record["keys"] != keys or record["patch_sha256"] != hashlib.sha256(patch_data).hexdigest():
        return None, {"reason": "stage_por_mismatch", "sidecar": str(sidecar_path)}
    try:
        base = _git(root, "merge-base", record["integration"], subject)
    except GitCommandError as exc:
        return None, {"reason": "git_failed", "argv": list(exc.argv), "stderr": exc.stderr}
    if base != record["base"]:
        return None, {
            "reason": "stage_base_moved",
            "staged_base": record["base"],
            "current_base": base,
        }
    actual = _run_subject_diff(root, base, subject)
    if actual.outcome is DiffOutcome.FAILED:
        return None, {
            "reason": "diff_failed",
            "diff_outcome": actual.outcome.value,
            "returncode": actual.returncode,
            "stderr": actual.stderr,
        }
    if actual.outcome is DiffOutcome.EMPTY:
        return None, {"reason": "empty_diff", "diff_outcome": actual.outcome.value}
    if actual.data != patch_data:
        return None, {
            "reason": "patch_not_actual_subject_diff",
            "actual_patch_sha256": hashlib.sha256(actual.data).hexdigest(),
            "provided_patch_sha256": hashlib.sha256(patch_data).hexdigest(),
        }
    return {
        "sidecar": str(sidecar_path),
        "record": record,
        "worktree": worktree,
        "relative_patch": relative_patch,
        "staged_commit": staged_commit,
        "review_head": current_head,
        "keys": keys,
        "actual_diff": actual,
    }, None


def _inspect_review_doc(doc_arg: str, binding: dict) -> tuple[dict, dict | None]:
    doc_path = Path(doc_arg).resolve()
    if not doc_path.is_file():
        return {"doc": doc_arg, "counted": False, "status": "excluded", "reason": "missing"}, None
    worktree = binding["worktree"]
    try:
        relative = doc_path.relative_to(worktree)
    except ValueError:
        return {
            "doc": doc_arg,
            "counted": False,
            "status": "excluded",
            "reason": "review_doc_outside_worktree",
        }, None
    if relative.parts[:2] != ("docs", "reviews") or relative == binding["relative_patch"]:
        return {
            "doc": doc_arg,
            "counted": False,
            "status": "excluded",
            "reason": "review_doc_path_invalid",
        }, None
    committed, committed_reason = _committed_file_matches(worktree, binding["review_head"], relative, doc_path)
    if not committed:
        return {
            "doc": doc_arg,
            "counted": False,
            "status": "excluded",
            "reason": committed_reason,
        }, None
    try:
        text = doc_path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as exc:
        return {
            "doc": doc_arg,
            "counted": False,
            "status": "excluded",
            "reason": "review_doc_unreadable",
            "error": str(exc)[:400],
        }, None
    if not text.strip():
        return {"doc": doc_arg, "counted": False, "status": "empty", "reason": "empty"}, None
    por_ok, por_reason = _doc_por_passes(text, binding["keys"])
    verdict = _doc_verdict(text)
    if not por_ok or verdict is None:
        return {
            "doc": doc_arg,
            "counted": False,
            "status": "excluded",
            "reason": por_reason if not por_ok else "no_verdict_line",
        }, None
    finding_result = _parse_findings(text)
    if finding_result.invalid:
        return {
            "doc": doc_arg,
            "counted": False,
            "status": "excluded",
            "reason": "invalid_finding_grammar",
            "invalid_findings": list(finding_result.invalid),
        }, None
    report = {
        "doc": doc_arg,
        "counted": True,
        "status": "counted",
        "verdict": verdict,
        "blocking_findings": finding_result.blocking,
        "findings": list(finding_result.findings),
    }
    return report, report


def _doc_counts(docs_report: list[dict]) -> dict:
    counted = [item for item in docs_report if item.get("counted")]
    excluded = [item for item in docs_report if item.get("status") == "excluded"]
    empty = [item for item in docs_report if item.get("status") == "empty"]
    return {
        "counted_count": len(counted),
        "excluded_count": len(excluded),
        "empty_count": len(empty),
        "empty": bool(empty),
    }


def _invalidate_prior_adjudication(root: Path, subject: str) -> dict | None:
    """Remove prior gate evidence before reporting a failed adjudication."""
    artifact = root / ADJUDICATION_DIR / f"{_branch_file_slug(subject)}.json"
    if not _path_exists_including_broken_symlink(artifact):
        return None
    try:
        artifact.unlink()
    except OSError as exc:
        return {"artifact": str(artifact), "error": str(exc)[:400]}
    return {"artifact": str(artifact), "invalidated": True}


def cmd_adjudicate(args: argparse.Namespace) -> int:
    root = Path(args.root).resolve()
    provenance_error = _judge_provenance_refusal()
    if provenance_error is not None:
        return _refuse("judge_provenance_mismatch", provenance_error)
    patch_path = Path(args.patch).resolve()
    if not patch_path.is_file():
        return _refuse("patch_missing", {"patch": str(patch_path)})
    staged = re.search(r"-([0-9a-f]{9})\.patch$", patch_path.name)
    if staged is None:
        return _refuse("patch_name_missing_staged_tip", {"patch": patch_path.name})
    try:
        subject_tip = _git(root, "rev-parse", args.subject)
    except GitCommandError as exc:
        return _refuse(
            "git_failed",
            {"argv": list(exc.argv), "returncode": exc.returncode, "stderr": exc.stderr},
        )
    if not subject_tip.startswith(staged.group(1)):
        return _refuse(
            "subject_tip_moved_since_stage",
            {"staged_tip_short": staged.group(1), "current_tip": subject_tip},
        )
    binding, binding_error = _validate_stage_binding(root, patch_path, args.subject, subject_tip)
    if binding_error is not None:
        return _refuse(binding_error.pop("reason"), binding_error)
    assert binding is not None
    if not args.docs:
        report = {"docs": [], **_doc_counts([])}
        invalidation = _invalidate_prior_adjudication(root, args.subject)
        if invalidation is not None:
            report["prior_adjudication"] = invalidation
            if not invalidation.get("invalidated", False):
                return _refuse("adjudication_artifact_invalidation_failed", report)
        return _refuse("empty_expected_documents", report)
    if len(set(args.docs)) != len(args.docs):
        report = {"docs": args.docs, "duplicate_count": len(args.docs) - len(set(args.docs))}
        invalidation = _invalidate_prior_adjudication(root, args.subject)
        if invalidation is not None:
            report["prior_adjudication"] = invalidation
            if not invalidation.get("invalidated", False):
                return _refuse("adjudication_artifact_invalidation_failed", report)
        return _refuse("duplicate_expected_documents", report)

    docs_report: list[dict] = []
    counted: list[dict] = []
    for doc_arg in args.docs:
        report, counted_report = _inspect_review_doc(doc_arg, binding)
        docs_report.append(report)
        if counted_report is not None:
            counted.append(counted_report)
    counts = _doc_counts(docs_report)
    report = {
        "docs": docs_report,
        **counts,
        "stage_binding": {
            "sidecar": binding["sidecar"],
            "worktree": str(binding["worktree"]),
            "review_head": binding["review_head"],
            "staged_commit": binding["staged_commit"],
        },
    }
    if counts["empty_count"]:
        invalidation = _invalidate_prior_adjudication(root, args.subject)
        if invalidation is not None:
            report["prior_adjudication"] = invalidation
            if not invalidation.get("invalidated", False):
                return _refuse("adjudication_artifact_invalidation_failed", report)
        return _refuse("empty_expected_documents", report)
    if counts["excluded_count"]:
        invalidation = _invalidate_prior_adjudication(root, args.subject)
        if invalidation is not None:
            report["prior_adjudication"] = invalidation
            if not invalidation.get("invalidated", False):
                return _refuse("adjudication_artifact_invalidation_failed", report)
        return _refuse("excluded_expected_documents", report)
    if not counted:
        invalidation = _invalidate_prior_adjudication(root, args.subject)
        if invalidation is not None:
            report["prior_adjudication"] = invalidation
            if not invalidation.get("invalidated", False):
                return _refuse("adjudication_artifact_invalidation_failed", report)
        return _refuse("no_counted_reviews", report)

    total_blocking = sum(d["blocking_findings"] for d in counted)
    all_merge = all(d["verdict"] == "MERGE" for d in counted)
    verdict = "MERGE" if (all_merge and total_blocking == 0) else "REVISE"

    artifact_dir = root / ADJUDICATION_DIR
    artifact_dir.mkdir(parents=True, exist_ok=True)
    artifact = artifact_dir / f"{_branch_file_slug(args.subject)}.json"
    artifact.write_text(
        json.dumps(
            {
                "subject": args.subject,
                "subject_tip": subject_tip,
                "verdict": verdict,
                "blocking_findings": total_blocking,
                "docs": docs_report,
                "patch": str(patch_path),
                "por_keys_rederived": True,
                "stage_binding": report["stage_binding"],
                "judge_provenance": judge_provenance(),
                "ts": datetime.now(timezone.utc).isoformat(),
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    _emit(
        {
            "ok": verdict == "MERGE",
            "verdict": verdict,
            "blocking_findings": total_blocking,
            "docs": docs_report,
            "artifact": str(artifact),
            "stage_binding": report["stage_binding"],
            "judge_provenance": judge_provenance(),
        }
    )
    return 0 if verdict == "MERGE" else 2


def _conflict_marker_pattern(root: Path) -> str:
    """Marker matcher honouring this repository's ``merge.conflictMarkerSize``.

    Git writes markers whose width is configurable per repository and per
    attribute, so a matcher hardwired to seven characters misses every marker
    in a repository that widened them. Quantifiers keep this module's own
    source free of a literal marker line, so the gate never rejects itself.
    """
    proc = subprocess.run(
        ["git", "-C", str(root), "config", "--get", "merge.conflictMarkerSize"],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    try:
        size = int(proc.stdout.strip())
    except (TypeError, ValueError):
        size = _CONFLICT_MARKER_DEFAULT_SIZE
    size = max(size, _CONFLICT_MARKER_MIN_SIZE)
    return r"^(<{%d,} |>{%d,} )" % (size, size)


def scan_tip_for_conflict_markers(root: Path, tip: str) -> tuple[list[str], str | None]:
    """Return ``(hits, error)`` for tracked text blobs at ``tip``.

    ``git grep`` against a tree needs no checkout, so the subject branch is
    scanned without disturbing the working copy. ``-I`` skips binary blobs,
    which cannot carry a marker git would have written.

    Exit 1 means "no match" and is the passing case. Any other non-zero exit is
    an unknown answer and is reported as an error rather than as absence: a
    gate that read a scan failure as "clean" would authorise exactly the merge
    it exists to refuse.
    """
    proc = subprocess.run(
        ["git", "-C", str(root), "grep", "-I", "-n", "-E", _conflict_marker_pattern(root), tip, "--"],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    if proc.returncode == 1:
        return [], None
    if proc.returncode != 0:
        return [], (proc.stderr or "").strip()[:200] or f"git grep exit {proc.returncode}"
    hits = [line for line in proc.stdout.splitlines() if line.strip()]
    return hits, None


def check_merge_gate(root: Path, subject: str) -> tuple[bool, dict]:
    """Shared gate: adjudication artifact present, MERGE, fenced to the tip."""
    artifact = root / ADJUDICATION_DIR / f"{_branch_file_slug(subject)}.json"
    if not artifact.is_file():
        return False, {"reason": "adjudication_artifact_missing", "expected": str(artifact)}
    try:
        record = json.loads(artifact.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        return False, {"reason": "adjudication_artifact_unreadable", "error": str(exc)[:200]}
    if not isinstance(record, dict):
        return False, {"reason": "adjudication_artifact_malformed"}
    if record.get("verdict") != "MERGE":
        return False, {"reason": "verdict_not_merge", "verdict": record.get("verdict")}
    if record.get("subject") != subject:
        return False, {"reason": "subject_mismatch", "recorded_subject": record.get("subject")}
    proc = subprocess.run(
        ["git", "-C", str(root), "rev-parse", subject],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    if proc.returncode != 0:
        return False, {"reason": "subject_tip_unresolvable", "stderr": proc.stderr.strip()[:200]}
    tip = proc.stdout.strip()
    if record.get("subject_tip") != tip:
        return False, {
            "reason": "stale_adjudication_tip",
            "adjudicated_tip": record.get("subject_tip"),
            "current_tip": tip,
        }
    allowed = os.environ.get(CONFLICT_MARKER_ESCAPE_HATCH_ENV, "").strip() == "1"
    hits, scan_error = scan_tip_for_conflict_markers(root, tip)
    if scan_error is not None:
        return False, {"reason": "conflict_marker_scan_failed", "error": scan_error}
    if hits and not allowed:
        return False, {
            "reason": "subject_tip_has_conflict_markers",
            "subject_tip": tip,
            "hit_count": len(hits),
            "hits": hits[:20],
            "escape_hatch": CONFLICT_MARKER_ESCAPE_HATCH_ENV,
        }
    return True, {
        "reason": "ok",
        "artifact": str(artifact),
        "subject_tip": tip,
        "conflict_markers_allowed": bool(hits) and allowed,
    }


def cmd_merge_gate(args: argparse.Namespace) -> int:
    root = Path(args.root).resolve()
    provenance_error = _judge_provenance_refusal()
    if provenance_error is not None:
        return _refuse("judge_provenance_mismatch", provenance_error)
    ok, detail = check_merge_gate(root, args.subject)
    _emit({"ok": ok, "subject": args.subject, **detail})
    return 0 if ok else 2


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="review_pipeline")
    parser.add_argument("--root", default=".", help="integration repo root")
    sub = parser.add_subparsers(dest="command", required=True)

    p_stage = sub.add_parser("stage")
    p_stage.add_argument("--subject", required=True)
    p_stage.add_argument("--slug", required=True)
    p_stage.add_argument("--integration", default="main")
    p_stage.add_argument(
        "--worktree",
        default=None,
        help="existing review worktree to commit into; omitted -> stage cuts one",
    )
    p_stage.add_argument(
        "--rev",
        type=int,
        default=1,
        help="review round number for the created worktree/branch (feature/rev<r>-<subject>)",
    )
    p_stage.set_defaults(func=cmd_stage)

    p_adj = sub.add_parser("adjudicate")
    p_adj.add_argument("--subject", required=True)
    p_adj.add_argument("--patch", required=True)
    p_adj.add_argument("--docs", nargs="*", required=True)
    p_adj.set_defaults(func=cmd_adjudicate)

    p_gate = sub.add_parser("merge-gate")
    p_gate.add_argument("--subject", required=True)
    p_gate.set_defaults(func=cmd_merge_gate)

    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except SystemExit as exc:
        return int(exc.code) if isinstance(exc.code, int) else 2


if __name__ == "__main__":
    raise SystemExit(main())
