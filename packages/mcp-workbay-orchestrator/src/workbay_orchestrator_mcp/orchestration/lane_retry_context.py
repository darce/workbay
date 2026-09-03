"""Deterministic retry-context block for dispatch briefs.

Pure functions over injected inputs. Renderers never touch git, sqlite,
subprocess, or the network. ``collect_review_docs`` is the only loader and
takes an injected ``runner`` callable (tests use fakes).

Section order is fixed and numbered. Empty sections render a typed empty
line rather than going silent. Over-cap output is truncated at a line
boundary with a ``[retry-context truncated at <n> bytes]`` marker.

TECH DEBT F3: ``Finding N — HIGH`` (severity-only rest) renders an empty title
on the retry one-liner.
TECH DEBT F4: ``VERDICT`` is first-line only; later VERDICT headings are ignored.
TECH DEBT F5: ``**Location:**`` is not harvested as where.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Callable, Sequence

# ---------------------------------------------------------------------------
# Public constants
# ---------------------------------------------------------------------------

RETRY_CONTEXT_MAX_BYTES = 16384
OPEN_FINDINGS_CAP = 25
FINDING_BODY_MAX_LINES = 6
FINDING_BODY_MAX_LINE_CHARS = 300

FAILED_REVIEWS_HEADER = "### 1. FAILED REVIEWS"
OPEN_FINDINGS_HEADER = "### 2. OPEN FINDINGS"
CODEMAP_HEADER = "### 3. CODEMAP POINTERS + PRIOR ART"
PRIOR_ATTEMPTS_HEADER = "### 4. PRIOR ATTEMPTS"

_TYPED_EMPTY_OPEN_FINDINGS = "(typed empty: no open high/medium findings under owned paths)"
_TYPED_EMPTY_CODEMAP = "(typed empty: no codemap packet)"
_TYPED_EMPTY_PRIOR_ATTEMPTS = "(typed empty: no prior result.json for this lane id)"

_SEVERITY_RANK = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
_HIGH_MEDIUM = frozenset({"HIGH", "MEDIUM"})
_SEVERITY_ALIASES = {
    "HIGH": "HIGH",
    "HI": "HIGH",
    "MEDIUM": "MEDIUM",
    "MED": "MEDIUM",
    "LOW": "LOW",
    "LO": "LOW",
}

_REV_LANE_RE = re.compile(r"^rev(\d+)-")
_TRAILING_LANE_NUM_RE = re.compile(r"-\d+$")
_VERDICT_RE = re.compile(r"^(?:#+\s*)?VERDICT:\s*(\S+)", re.IGNORECASE)
# House-corpus header dialects (compatibility schema). Keep Finding N / FN,
# and also numbered ``N. SEVERITY — title``, ``TOKEN (SEVERITY) title``,
# letter ids ``F-A (SEVERITY) — title``, and ``FINDING - TOKEN``.
# ASCII hyphen / en-dash as the Finding-N / FN rest separator are still
# not accepted (em dash only on that form).
_SEVERITY_TOKEN = r"HIGH|MEDIUM|LOW|HI|MED|LO"
_DASH_CLASS = r"[-\u2013\u2014]"
_NUMBERED_SEVERITY_HEADER_RE = re.compile(
    rf"^#{{2,4}}\s+(\d+)\.\s+({_SEVERITY_TOKEN})\b\s*"
    rf"(?:{_DASH_CLASS}\s*)?(.*?)\s*$",
    re.IGNORECASE,
)
_PAREN_SEVERITY_HEADER_RE = re.compile(
    rf"^#{{2,4}}\s+(\S+)\s+\(({_SEVERITY_TOKEN})\)\s*"
    rf"(?:{_DASH_CLASS}\s*)?(.*?)\s*$",
    re.IGNORECASE,
)
_FINDING_TOKEN_HEADER_RE = re.compile(
    rf"^#{{2,4}}\s+FINDING\s*{_DASH_CLASS}\s+(\S+)(?:\s+(.*))?\s*$",
    re.IGNORECASE,
)
_FINDING_HEADER_RE = re.compile(
    r"^#{2,4}\s+(?:Finding\s+(\d+)|(F\d+\.?))\s+(?:\u2014\s+)?(.+?)\s*$",
    re.IGNORECASE,
)
_WHERE_RE = re.compile(
    r"^(?:[-*]\s+)?(?:\*\*(?:file:line|where):\*\*|where:)\s*(.+)$",
    re.IGNORECASE,
)
_SEVERITY_LINE_RE = re.compile(
    r"^(?:[-*]\s+)?(?:\*\*)?Severity:(?:\*\*)?\s*(\S+)",
    re.IGNORECASE,
)
# House corpus: REV<N>-<STEM>.md and REV<N>-<STEM>-<suffix>.md. STEM is the
# subject with the trailing -NN lane number dropped so family-wide reviews
# (REV1-FIX-STOCKBULKHEAD-01, REV2-FIX-STOCKBULKHEAD-02) match subject
# fix-stockbulkhead-03. A greedy [^/]* suffix is rejected so STEM is not a
# prefix of a longer token (REV2-FIX-CEREMONYCLASSROOM-01.md).
_REVIEW_PATH_TEMPLATE = r"^docs/reviews/REV\d+-{token}(?:-[^/]+)?\.md$"

Runner = Callable[[Sequence[str]], str]


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------


@dataclass
class ReviewFinding:
    fid: str
    severity: str
    title: str
    where: str
    body: str


@dataclass
class ReviewDoc:
    path: str
    ref: str
    verdict: str
    findings: list[ReviewFinding]


@dataclass
class OpenFinding:
    finding_id: str
    severity: str
    file_path: str
    description: str


@dataclass
class PriorAttempt:
    dispatch_dir: str
    outcome: str
    wall_seconds: float | int | None
    commits: Sequence[str] | int | None
    error: str | None
    self_verify_tail: str | None


class RetryContextMissing(ValueError):
    """Raised when a retry lane brief lacks the mandatory FAILED REVIEWS section."""

    def __init__(self, lane_id: str, header: str) -> None:
        self.lane_id = lane_id
        self.header = header
        super().__init__(
            f"retry context missing {header!r} for lane {lane_id}"
        )


# ---------------------------------------------------------------------------
# Parsing / loading
# ---------------------------------------------------------------------------


def _subject_token(subject: str) -> str:
    """Uppercase subject stem; drop the trailing -NN lane number.

    ``fix-stockbulkhead-03`` and ``fix-stockbulkhead-01`` share stem
    ``FIX-STOCKBULKHEAD``, which is the corpus identity (DATA-13).
    """
    token = (subject or "").strip().upper().replace("_", "-")
    token = _TRAILING_LANE_NUM_RE.sub("", token)
    return token or "*"


def _failed_reviews_empty(subject: str) -> str:
    token = _subject_token(subject)
    return f"(typed empty: no docs/reviews/REV*-{token}*.md on any local ref)"


def _normalize_severity(token: str) -> str | None:
    key = (token or "").strip().strip(".,;:").upper()
    return _SEVERITY_ALIASES.get(key)


def _finding_fields(fid: str, severity: str, title: str) -> dict[str, str]:
    return {
        "fid": fid,
        "severity": severity,
        "title": title,
        "where": "",
    }


def _parse_finding_header(line: str) -> dict[str, str] | None:
    numbered = _NUMBERED_SEVERITY_HEADER_RE.match(line)
    if numbered is not None:
        mapped = _normalize_severity(numbered.group(2))
        if mapped is not None:
            return _finding_fields(
                f"F{numbered.group(1)}", mapped, numbered.group(3).strip()
            )
    paren = _PAREN_SEVERITY_HEADER_RE.match(line)
    if paren is not None:
        mapped = _normalize_severity(paren.group(2))
        if mapped is not None:
            return _finding_fields(
                paren.group(1).upper(), mapped, paren.group(3).strip()
            )
    finding_token = _FINDING_TOKEN_HEADER_RE.match(line)
    if finding_token is not None:
        token = finding_token.group(1)
        extra = (finding_token.group(2) or "").strip()
        return _finding_fields(token.upper(), "unknown", extra or token)
    match = _FINDING_HEADER_RE.match(line)
    if match is None:
        return None
    digits, fid_token, rest = match.group(1), match.group(2), match.group(3).strip()
    fid = f"F{digits}" if digits else (fid_token or "").upper()
    head, sep, tail = rest.partition("\u2014")
    mapped = _normalize_severity(head.strip())
    if mapped is not None:
        return _finding_fields(fid, mapped, tail.strip() if sep else "")
    return _finding_fields(fid, "unknown", rest)


def parse_review_doc(path: str, ref: str, text: str) -> ReviewDoc:
    """Parse a review markdown document into a ``ReviewDoc``.

    First line supplies ``VERDICT: X``. Findings are house-corpus header
    dialects after 2-4 hashes: ``Finding N`` / ``FN``, numbered
    ``N. SEVERITY — title``, ``TOKEN (SEVERITY) title``, letter ids
    ``F-A (SEVERITY) — title``, and ``FINDING - TOKEN``. Severity may be
    HIGH/MEDIUM/LOW (or HI/MED/LO) in the header or parentheses, omitted
    (``unknown``), or filled from a following ``**Severity:**`` /
    ``severity:`` line. Up to six non-empty body lines are kept (300
    characters max per line).
    """
    lines = (text or "").splitlines()
    verdict = ""
    if lines:
        match = _VERDICT_RE.match(lines[0].strip())
        if match:
            verdict = match.group(1).rstrip(".,;")

    findings: list[ReviewFinding] = []
    current: dict[str, str] | None = None
    body_lines: list[str] = []

    def flush() -> None:
        nonlocal current, body_lines
        if current is None:
            return
        kept: list[str] = []
        where = current.get("where") or ""
        severity = current.get("severity") or "unknown"
        if severity == "unknown":
            for raw in body_lines:
                sev_match = _SEVERITY_LINE_RE.match(raw.strip())
                if sev_match:
                    mapped = _normalize_severity(sev_match.group(1))
                    if mapped is not None:
                        severity = mapped
                    break
        for raw in body_lines:
            stripped = raw.strip()
            if not stripped:
                continue
            if len(kept) >= FINDING_BODY_MAX_LINES:
                break
            clipped = stripped[:FINDING_BODY_MAX_LINE_CHARS]
            kept.append(clipped)
            if not where:
                where_match = _WHERE_RE.match(clipped)
                if where_match:
                    where = where_match.group(1).strip().strip("`")
        findings.append(
            ReviewFinding(
                fid=current["fid"],
                severity=severity,
                title=current["title"],
                where=where,
                body="\n".join(kept),
            )
        )
        current = None
        body_lines = []

    for line in lines[1:]:
        header = _parse_finding_header(line)
        if header is not None:
            flush()
            current = header
            body_lines = []
            continue
        if current is not None:
            body_lines.append(line)
    flush()
    return ReviewDoc(path=path, ref=ref, verdict=verdict, findings=findings)


def _ls_tree_paths(stdout: str) -> list[str]:
    paths: list[str] = []
    for raw in (stdout or "").splitlines():
        line = raw.strip()
        if not line:
            continue
        if "\t" in line:
            line = line.split("\t", 1)[1]
        paths.append(line)
    return paths


def _review_path_re(subject: str) -> re.Pattern[str]:
    token = re.escape(_subject_token(subject))
    return re.compile(_REVIEW_PATH_TEMPLATE.format(token=token), re.IGNORECASE)


def collect_review_docs(subject: str, *, runner: Runner) -> list[ReviewDoc]:
    """Load house-corpus review docs via an injected git runner.

    Matches ``docs/reviews/REV<N>-<STEM>.md`` and
    ``docs/reviews/REV<N>-<STEM>-<suffix>.md``, where ``STEM`` is the
    subject with a trailing ``-NN`` lane number dropped. Family-wide:
    ``REV1-FIX-STOCKBULKHEAD-01`` and ``REV2-FIX-STOCKBULKHEAD-02`` both
    belong to subject ``fix-stockbulkhead-03``.

    Refs are listed with ``git for-each-ref``, then ``ls-tree`` per ref.
    The first ref in sorted ref order wins for a given path; the returned
    list is stable-sorted by path.
    """
    refs_out = runner(["git", "for-each-ref", "--format=%(refname)"])
    refs = sorted({line.strip() for line in (refs_out or "").splitlines() if line.strip()})
    path_re = _review_path_re(subject)
    seen_paths: set[str] = set()
    collected: list[ReviewDoc] = []

    for ref in refs:
        tree_out = runner(
            ["git", "ls-tree", "-r", "--name-only", ref, "--", "docs/reviews/"]
        )
        for path in _ls_tree_paths(tree_out):
            if path in seen_paths:
                continue
            if not path_re.match(path):
                continue
            seen_paths.add(path)
            text = runner(["git", "show", f"{ref}:{path}"]) or ""
            collected.append(parse_review_doc(path, ref, text))

    collected.sort(key=lambda doc: doc.path)
    return collected


# ---------------------------------------------------------------------------
# Renderers
# ---------------------------------------------------------------------------


def render_failed_reviews(docs: Sequence[ReviewDoc], *, subject: str = "") -> str:
    lines = [FAILED_REVIEWS_HEADER]
    if not docs:
        lines.append(_failed_reviews_empty(subject))
        return "\n".join(lines)
    for doc in docs:
        n = len(doc.findings)
        lines.append(
            f"- {doc.path} @ {doc.ref} \u2014 VERDICT: {doc.verdict} ({n} findings)"
        )
        for finding in doc.findings:
            lines.append(f"  {finding.fid} {finding.severity} {finding.title}")
            if finding.where:
                lines.append(f"    where: {finding.where}")
            if finding.body:
                for body_line in finding.body.splitlines():
                    lines.append(f"    {body_line}")
    return "\n".join(lines)


def render_open_findings(
    findings: Sequence[OpenFinding],
    owned_paths: Sequence[str],
) -> str:
    lines = [OPEN_FINDINGS_HEADER]
    owned = set(owned_paths or ())
    matched = [
        item
        for item in findings
        if item.file_path in owned and item.severity.upper() in _HIGH_MEDIUM
    ]
    matched.sort(
        key=lambda item: (
            _SEVERITY_RANK.get(item.severity.upper(), 99),
            item.file_path,
            item.finding_id,
        )
    )
    matched = matched[:OPEN_FINDINGS_CAP]
    if not matched:
        lines.append(_TYPED_EMPTY_OPEN_FINDINGS)
        return "\n".join(lines)
    for item in matched:
        lines.append(
            f"- {item.severity.upper()} {item.finding_id} {item.file_path} {item.description}"
        )
    return "\n".join(lines)


def _render_codemap_section(codemap_packet: str | None) -> str:
    lines = [CODEMAP_HEADER]
    packet = (codemap_packet or "").strip("\n")
    if not packet.strip():
        lines.append(_TYPED_EMPTY_CODEMAP)
        return "\n".join(lines)
    lines.append(packet)
    return "\n".join(lines)


def _commit_count(commits: Sequence[str] | int | None) -> int:
    if commits is None:
        return 0
    if isinstance(commits, int):
        return commits
    return len(commits)


def render_prior_attempts(attempts: Sequence[PriorAttempt]) -> str:
    lines = [PRIOR_ATTEMPTS_HEADER]
    if not attempts:
        lines.append(_TYPED_EMPTY_PRIOR_ATTEMPTS)
        return "\n".join(lines)
    for attempt in attempts:
        wall = 0 if attempt.wall_seconds is None else attempt.wall_seconds
        n_commits = _commit_count(attempt.commits)
        lines.append(
            f"- {attempt.dispatch_dir} outcome={attempt.outcome} "
            f"wall_seconds={wall} commits={n_commits}"
        )
        if attempt.error:
            lines.append(f"  error: {attempt.error}")
        if attempt.self_verify_tail:
            tail = str(attempt.self_verify_tail).strip()
            if tail:
                first = tail.splitlines()[0][:FINDING_BODY_MAX_LINE_CHARS]
                lines.append(f"  self_verify: {first}")
    return "\n".join(lines)


def _cap_retry_context(text: str, *, max_bytes: int) -> str:
    """Truncate at a line boundary; mirror the packet cap+marker contract."""
    raw = text.encode("utf-8")
    if len(raw) <= max_bytes:
        return text
    marker = f"[retry-context truncated at {max_bytes} bytes]"
    marker_bytes = marker.encode("utf-8")
    # Reserve the marker plus a separating newline.
    budget = max(0, max_bytes - len(marker_bytes) - 1)
    kept: list[str] = []
    used = 0
    # splitlines(keepends) preserves original line endings so the prefix of
    # the uncapped render is byte-identical to the kept lines.
    for line in text.splitlines(keepends=True):
        encoded_line = line.encode("utf-8")
        if used + len(encoded_line) > budget:
            break
        kept.append(line)
        used += len(encoded_line)
    out = "".join(kept)
    if out and not out.endswith("\n"):
        # If the last kept line had no newline we still need one before the
        # marker, but only if it fits. Drop the partial last line otherwise.
        extra = 1
        if used + extra + len(marker_bytes) > max_bytes:
            if kept:
                kept.pop()
                out = "".join(kept)
        else:
            out += "\n"
    elif not out.endswith("\n"):
        out += "\n" if out else ""
    if out and not out.endswith("\n"):
        out += "\n"
    out += marker
    encoded = out.encode("utf-8")
    if len(encoded) > max_bytes:
        # Last-resort byte trim should not fire when budget math holds; keep
        # the marker intact by dropping whole lines until it fits.
        while kept and len(out.encode("utf-8")) > max_bytes:
            kept.pop()
            prefix = "".join(kept)
            if prefix and not prefix.endswith("\n"):
                prefix += "\n"
            out = prefix + marker
        encoded = out.encode("utf-8")
        if len(encoded) > max_bytes:
            # Marker-only fallback (pathological tiny cap).
            out = marker[:max_bytes]
    if not out.endswith("\n"):
        # Marker is the final line; a trailing newline is optional and must
        # not push us over the cap.
        with_nl = out + "\n"
        if len(with_nl.encode("utf-8")) <= max_bytes:
            out = with_nl
    return out


def render_retry_context(
    *,
    lane_id: str,
    subject: str,
    docs: Sequence[ReviewDoc],
    findings: Sequence[OpenFinding],
    owned_paths: Sequence[str],
    attempts: Sequence[PriorAttempt],
    codemap_packet: str | None,
    max_bytes: int = RETRY_CONTEXT_MAX_BYTES,
) -> str:
    """Compose the four numbered sections and apply the byte cap.

    ``lane_id`` is accepted for call-site symmetry with the assertion helper;
    rendering itself is a pure function of the remaining inputs.
    """
    del lane_id  # identity is for callers; it does not change the block
    parts = [
        render_failed_reviews(docs, subject=subject),
        render_open_findings(findings, owned_paths),
        _render_codemap_section(codemap_packet),
        render_prior_attempts(attempts),
    ]
    text = "\n".join(part.rstrip("\n") for part in parts) + "\n"
    return _cap_retry_context(text, max_bytes=max_bytes)


# ---------------------------------------------------------------------------
# Predicate / assertion / prepend
# ---------------------------------------------------------------------------


def requires_failed_reviews(lane_id: str, *, attempt: int) -> bool:
    """True when this is a retry lane (``revN-`` with N>=2, or attempt>=2)."""
    match = _REV_LANE_RE.match(lane_id or "")
    if match and int(match.group(1)) >= 2:
        return True
    try:
        attempt_n = int(attempt)
    except (TypeError, ValueError):
        attempt_n = 0
    return attempt_n >= 2


def _header_line_re(header: str) -> re.Pattern[str]:
    """Exact header line, anchored at line start. Not a substring test."""
    return re.compile(rf"^{re.escape(header)}[ \t]*$", re.MULTILINE)


def _has_header_line(text: str, header: str) -> bool:
    return _header_line_re(header).search(text or "") is not None


def _section_body(brief: str, header: str) -> str:
    match = _header_line_re(header).search(brief or "")
    if match is None:
        return ""
    after_header = (brief or "")[match.end() :]
    newline = after_header.find("\n")
    if newline < 0:
        return ""
    rest = after_header[newline + 1 :]
    next_header = re.search(r"^### ", rest, re.MULTILINE)
    if next_header:
        rest = rest[: next_header.start()]
    return rest.strip()


def _is_typed_empty(body: str) -> bool:
    stripped = (body or "").strip()
    if not stripped:
        return True
    first = stripped.splitlines()[0].strip()
    return first.startswith("(typed empty:")


_DOC_FINDING_COUNT_RE = re.compile(r"\((\d+) findings\)\s*$")


def _harvested_finding_count(section_body: str) -> int:
    """Sum ``(N findings)`` from FAILED REVIEWS doc summary lines."""
    total = 0
    for line in (section_body or "").splitlines():
        if not line.startswith("- "):
            continue
        match = _DOC_FINDING_COUNT_RE.search(line)
        if match:
            total += int(match.group(1))
    return total


def assert_retry_context(
    brief: str,
    *,
    lane_id: str,
    attempt: int,
    expected_docs: int = 0,
) -> None:
    """Fail fast when a retry lane brief lacks FAILED REVIEWS content.

    Raises ``RetryContextMissing`` (message names ``lane_id`` and the missing
    header) when ``requires_failed_reviews`` is true and the header is absent.
    A present header whose section is typed-empty raises only when
    ``expected_docs > 0``; typed-empty stays legal when no docs were
    expected. Once the section is not typed-empty, harvested finding count
    must be greater than zero even when ``expected_docs`` is the default 0.
    A rendered ``VERDICT: REVISE (0 findings)`` line must never satisfy
    the rail.
    """
    if not requires_failed_reviews(lane_id, attempt=attempt):
        return
    text = brief or ""
    if not _has_header_line(text, FAILED_REVIEWS_HEADER):
        raise RetryContextMissing(lane_id, FAILED_REVIEWS_HEADER)
    body = _section_body(text, FAILED_REVIEWS_HEADER)
    if _is_typed_empty(body):
        if expected_docs > 0:
            raise RetryContextMissing(lane_id, FAILED_REVIEWS_HEADER)
        return
    if _harvested_finding_count(body) == 0:
        raise RetryContextMissing(lane_id, FAILED_REVIEWS_HEADER)


def append_retry_context(brief: str, block: str) -> str:
    """Prepend ``block`` before ``brief``, separated by a blank line.

    Idempotent: a brief that already has ``### 1. FAILED REVIEWS`` as its
    own line (anchored at line start) is returned unchanged. A mere
    substring mention of those words does not count.
    """
    brief_s = brief if brief is not None else ""
    if _has_header_line(brief_s, FAILED_REVIEWS_HEADER):
        return brief_s
    block_s = (block or "").rstrip()
    if not block_s:
        return brief_s
    if not brief_s.strip():
        return block_s
    return f"{block_s}\n\n{brief_s.lstrip()}"
