"""Deterministic evidence checks against immutable Git blobs, without model calls.

Grammar: an EVIDENCE-labelled line or Markdown EVIDENCE section designates
existing code. Backticks, Markdown link destinations, and bare path citations
there support repo-relative paths, :line[-line], #Lline[-Lline], and Python
:qualified.symbol (also #qualified.symbol). Explicit PROPOSED/FUTURE lines
designate future work and are excluded. Ordinary prose and URLs are excluded.
Standalone semantic versions (optionally v-prefixed) are prose, not paths.
Complex anchors and non-Python symbols are unknown, never guessed invalid.

Only the frozen subject producer owns review identity. This module consumes
its DTO; it never fabricates a freeze or records a review verdict.
"""
from __future__ import annotations

import argparse
import ast
import hashlib
import json
import os
import re
import subprocess
from pathlib import Path, PurePosixPath
from typing import Any

STATUS_EXIT = {"valid": 0, "invalid": 1, "unknown": 2}
_SHA = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})\Z")
_DIGEST = re.compile(r"[0-9a-f]{64}\Z")
_EVIDENCE = re.compile(r"\bEVIDENCE\b", re.I)
_LABEL = re.compile(
    r"(?:^|[\s|])(?:(?:\*\*)?EVIDENCE(?:\*\*)?\s*:|\*\*EVIDENCE:\*\*|\[EVIDENCE\](?:\s*:)?|EVIDENCE\s*\|)",
    re.I,
)
_FUTURE = re.compile(r"^\s*(?:[-*|]\s*)?(?:\[|\*\*)?(?:PROPOSED|FUTURE)\b", re.I)
_HEADING = re.compile(r"^\s{0,3}#{1,6}\s+(.+)")
_TOKENS = re.compile(r"`([^`\n]+)`|\[[^\]\n]*\]\(([^)\n]+)\)|([^\s`<>]+)")
_PATHISH = re.compile(
    r"(?:[^\s:#]+/[^\s:#]+|[^\s:#]+\.[A-Za-z0-9_]+|Makefile|Dockerfile|README|LICENSE|BUILD|WORKSPACE)(?:[:#].*)?\Z"
)
_SEMVER_PRERELEASE_ID = r"(?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*)"
_SEMVER = re.compile(
    r"v?(?:0|[1-9][0-9]*)(?:\.(?:0|[1-9][0-9]*)){2}"
    rf"(?:-{_SEMVER_PRERELEASE_ID}(?:\.{_SEMVER_PRERELEASE_ID})*)?"
    r"(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?\Z"
)
_LINE = re.compile(r"(?:L)?([0-9]+)(?:-(?:L)?([0-9]+))?\Z")
_SYMBOL = re.compile(r"[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*(?:\(\))?\Z")


def _git(repo: Path, *args: str) -> bytes:
    env = dict(os.environ, GIT_NO_LAZY_FETCH="1", GIT_OPTIONAL_LOCKS="0")
    return subprocess.run(
        ["git", "-C", str(repo), *args], check=True, capture_output=True,
        timeout=10, env=env,
    ).stdout


def _result(plan_bytes: bytes | None) -> dict[str, Any]:
    return {
        "schema": "workbay/plan-anchor-lint@1", "status": "valid",
        "subject_sha256": hashlib.sha256(plan_bytes).hexdigest() if isinstance(plan_bytes, bytes) else None,
        "source_commit": None, "source_tree": None, "citations": [], "errors": [],
    }


def _error(result: dict[str, Any], code: str, status: str = "unknown") -> dict[str, Any]:
    result["errors"].append({"code": code, "status": status})
    result["status"] = status
    return result


def _citations(text: str):
    in_evidence = False
    in_fence = False
    for line_number, line in enumerate(text.splitlines(), 1):
        # Fences may contain cited code samples, not citation declarations.
        if line.lstrip().startswith(("```", "~~~")):
            in_fence = not in_fence
            continue
        if in_fence:
            continue
        heading = _HEADING.match(line)
        if heading:
            in_evidence = bool(_EVIDENCE.search(heading.group(1)))
        marker = _LABEL.search(line)
        if not in_evidence and not marker:
            continue
        start = marker.end() if marker else 0
        content = line[start:]
        if _FUTURE.match(line) or _FUTURE.match(content.lstrip(":*] |")):
            continue
        for token in _TOKENS.finditer(content):
            raw = next(group for group in token.groups() if group is not None)
            raw = raw.strip().rstrip(",;.")
            if "://" in raw or raw.startswith(("mailto:", "#")):
                continue
            # Full-token match keeps explicit paths and citation anchors checked.
            if _SEMVER.fullmatch(raw):
                continue
            if _PATHISH.fullmatch(raw):
                yield raw, line_number, start + token.start() + 1


def _symbols(blob: bytes) -> set[str]:
    """Collect lexically named Python declarations without importing source."""
    names: set[str] = set()

    def target(node: ast.AST, prefix: str) -> None:
        if isinstance(node, ast.Name):
            names.add(prefix + node.id)
        elif isinstance(node, (ast.Tuple, ast.List)):
            for item in node.elts:
                target(item, prefix)

    def walk(node: ast.AST, prefix: str = "") -> None:
        for child in ast.iter_child_nodes(node):
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                name = prefix + child.name
                names.add(name)
                walk(child, name + ".")
            elif isinstance(child, (ast.Assign, ast.AnnAssign)):
                for item in child.targets if isinstance(child, ast.Assign) else [child.target]:
                    target(item, prefix)
            elif isinstance(child, (ast.Import, ast.ImportFrom)):
                for alias in child.names:
                    names.add(prefix + (alias.asname or alias.name.split(".")[0]))
            else:
                walk(child, prefix)

    walk(ast.parse(blob))
    return names


def _check(repo: Path, tree: str, raw: str) -> tuple[str, str]:
    split = re.split(r"[:#]", raw, maxsplit=1)
    path, anchor = split[0], split[1] if len(split) == 2 else None
    pure = PurePosixPath(path)
    if (pure.is_absolute() or ".." in pure.parts or str(pure) != path
            or "\\" in path or "\x00" in path):
        return "unknown", "malformed_citation"
    line_match = _LINE.fullmatch(anchor) if anchor is not None else None
    if anchor is not None and not line_match and not _SYMBOL.fullmatch(anchor):
        return "unknown", "unsupported_syntax"
    entries = _git(repo, "ls-tree", "-z", tree, "--", path).split(b"\0")
    entry = next((e for e in entries if e.partition(b"\t")[2] == path.encode("utf-8")), None)
    if entry is None:
        return "invalid", "missing_path"
    metadata = entry.partition(b"\t")[0].split()
    if metadata[1] != b"blob" or metadata[0] == b"120000":
        return "unknown", "unsupported_source_kind"
    if anchor is None:
        return "valid", "path_exists"
    blob = _git(repo, "cat-file", "blob", metadata[2].decode("ascii"))
    if line_match:
        first = int(line_match[1])
        last = int(line_match[2] or first)
        if not (1 <= first <= last <= len(blob.splitlines())):
            return "invalid", "line_out_of_range"
        return "valid", "lines_exist"
    if pure.suffix != ".py":
        return "unknown", "unsupported_language"
    try:
        names = _symbols(blob)
    except (SyntaxError, ValueError, UnicodeError, RecursionError):
        return "unknown", "source_parse_error"
    symbol = anchor.removesuffix("()")
    return ("valid", "symbol_exists") if symbol in names else ("invalid", "missing_symbol")


def lint_plan(
    plan_bytes: bytes, *, repo: Path | str, source_ref: str,
    expected_sha256: str | None = None,
) -> dict[str, Any]:
    """Lint exact bytes against a full immutable commit; return a JSON-safe receipt."""
    result = _result(plan_bytes)
    if not isinstance(plan_bytes, bytes):
        return _error(result, "malformed_plan")
    if expected_sha256 is not None:
        if not isinstance(expected_sha256, str) or not _DIGEST.fullmatch(expected_sha256):
            return _error(result, "malformed_expected_sha256")
        if expected_sha256 != result["subject_sha256"]:
            return _error(result, "subject_hash_mismatch", "invalid")
    try:
        text = plan_bytes.decode("utf-8")
    except UnicodeError:
        return _error(result, "malformed_plan")
    if not isinstance(source_ref, str) or not _SHA.fullmatch(source_ref):
        return _error(result, "immutable_source_required")
    repo = Path(repo)
    try:
        if _git(repo, "cat-file", "-t", source_ref).strip() != b"commit":
            return _error(result, "source_unavailable")
        tree = _git(repo, "rev-parse", "--verify", source_ref + "^{tree}").decode("ascii").strip()
        result.update(source_commit=source_ref, source_tree=tree)
    except (OSError, subprocess.SubprocessError, UnicodeError):
        return _error(result, "source_unavailable")
    for raw, line, column in _citations(text):
        try:
            status, code = _check(repo, tree, raw)
        except (OSError, subprocess.SubprocessError, UnicodeError):
            status, code = "unknown", "source_unavailable"
        result["citations"].append({
            "citation": raw, "plan_line": line, "plan_column": column,
            "status": status, "code": code,
        })
    statuses = {citation["status"] for citation in result["citations"]}
    result["status"] = "invalid" if "invalid" in statuses else "unknown" if "unknown" in statuses else "valid"
    return result


def lint_review_subject(subject: dict[str, Any], *, repo: Path | str) -> dict[str, Any]:
    """Verify the producer DTO and lint its delivery bytes, never the mutable plan."""
    required = ("freeze_id", "task_ref", "review_run_id", "subject_path", "subject_sha256",
                "source_commit", "source_tree", "snapshot_path")
    if (not isinstance(subject, dict) or subject.get("schema") != "workbay/plan-review-subject@1"
            or any(not isinstance(subject.get(k), str) or not subject[k] for k in required)):
        return _error(_result(None), "malformed_review_subject")
    path = Path(subject["snapshot_path"])
    if not path.is_absolute():
        path = Path(repo) / path
    try:
        plan_bytes = path.read_bytes()
    except OSError:
        return _error(_result(None), "plan_unreadable")
    result = lint_plan(plan_bytes, repo=repo, source_ref=subject["source_commit"],
                       expected_sha256=subject["subject_sha256"])
    if result["source_tree"] is not None and result["source_tree"] != subject["source_tree"]:
        return _error(result, "source_tree_mismatch", "invalid")
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", required=True)
    parser.add_argument("--source-ref", required=True)
    parser.add_argument("--expected-sha256")
    parser.add_argument("--json", action="store_true", help="emit versioned JSON (also the default)")
    args = parser.parse_args(argv)
    try:
        plan_bytes = Path(args.plan).read_bytes()
    except OSError:
        result = _error(_result(None), "plan_unreadable")
    else:
        try:
            repo = Path(_git(Path.cwd(), "rev-parse", "--show-toplevel").decode().strip())
        except (OSError, subprocess.SubprocessError, UnicodeError):
            result = _error(_result(plan_bytes), "source_unavailable")
        else:
            result = lint_plan(plan_bytes, repo=repo, source_ref=args.source_ref,
                               expected_sha256=args.expected_sha256)
    print(json.dumps(result, sort_keys=True))
    return STATUS_EXIT[result["status"]]


if __name__ == "__main__":
    raise SystemExit(main())
