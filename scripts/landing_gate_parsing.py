"""Failure-id extraction and checkout-token normalization for the landing gate.

Parser/ID extraction lives here so summary-region rules cannot drift into
scope comparison or merge policy [REVIEW-M-07].
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Iterable, Sequence

from landing_gate_policy import LandingGateError

#: The checkout token only ever appears as a pytest *parameter*, i.e. inside
#: the ``[...]`` of a test id.  Anchoring on the bracket is load-bearing: a
#: test whose own name contains the repository name (there is one:
#: ``test_agentic_protocol_monorepo_layout``) must not be collapsed, because
#: folding two genuinely distinct ids into one hides a regression behind a fix.
# The repository token is an ASCII-lowercase name ending in ``-monorepo``.
# Keep this byte-for-byte aligned with gate_ids.sh; locale-aware character
# classes or case folding would collapse controls Python intentionally keeps
# distinct [internal].
_CHECKOUT_PARAM_RE = re.compile(r"\[workbay(?:-wb-[^\]]*)?\]")
_CHECKOUT_TOKEN = "[CHECKOUT]"

#: Pytest's terminal counts line. Quiet ``-q`` writes the same body *without*
#: the ``====`` wrap; verbose/default wraps it. Either form is forensic
#: evidence of what pytest printed. Completion is authenticated by a
#: producer-owned receipt, not by this printable line [LANDGA-H-01, AGT-04].
_COUNTS_BODY = (
    r"(?:\d+\s+(?:passed|failed|error|errors|skipped|xfailed|xpassed|deselected|warning|warnings)\b"
    r"|no tests ran\b)"
)
_PYTEST_SUMMARY_RE = re.compile(rf"^(?:={{2,}}\s+)?{_COUNTS_BODY}.*in\s+\d.*?(?:\s+={{2,}})?$")
_ANSI_RE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")

#: Pytest writes this header once, immediately before the FAILED/ERROR node
#: list.  Logging records that happen to start with ``ERROR `` live above it
#: and must not be treated as node ids [LANDGATE-LOGID-01].  Match the full
#: equals-wrapped line so failure text containing the title cannot retarget
#: the summary region.
_SHORT_SUMMARY_HEADER = "short test summary info"
_SHORT_SUMMARY_HEADER_RE = re.compile(rf"^={{2,}}\s*{re.escape(_SHORT_SUMMARY_HEADER)}\s*={{2,}}$")
_FAILURE_STATUS_RE = re.compile(r"^(FAILED|ERROR)(?:\s+(.*))?$")
#: File-level collection errors have no ``::``. Parameter brackets may contain
#: spaces. The file/function path itself does not.
_NODE_ID_RE = re.compile(r"^[^\s:[]+(?:::[^\s:[]+)*(?:\[.*\])?$")
_COUNT_WORD_RE = re.compile(
    r"(\d+)\s+(passed|failed|error|errors|skipped|xfailed|xpassed|deselected|warning|warnings)\b"
)
_EXECUTED_COUNT_WORDS = frozenset({"passed", "failed", "error", "errors", "xfailed", "xpassed"})


def normalize_failure_id(raw: str) -> str:
    """Collapse a checkout-name pytest parameter to a stable token."""
    return _CHECKOUT_PARAM_RE.sub(_CHECKOUT_TOKEN, raw)


def normalize_failure_ids(ids: Iterable[str]) -> list[str]:
    """Normalize, de-duplicate and order a failure-id set."""
    return sorted({normalize_failure_id(i.strip()) for i in ids if i.strip()})


def _ids_not_measured_diff(
    *,
    baseline_raw: int,
    subject_raw: int,
    baseline_count: int,
    subject_count: int,
    collapsed: int,
) -> dict[str, Any]:
    return {
        "outcome": "ids_not_measured",
        "new": [],
        "fixed": [],
        "baseline_raw": baseline_raw,
        "subject_raw": subject_raw,
        "baseline_count": baseline_count,
        "subject_count": subject_count,
        "collapsed": collapsed,
    }


def failure_id_diff(
    baseline: Iterable[str],
    subject: Iterable[str],
    *,
    subject_executed: bool = False,
) -> dict[str, Any]:
    """Compare two failure-id sets after checkout normalization.

    ``fixed`` is reported but never gates: closing a failure is not a reason to
    refuse a merge, and a branch that closes nothing is still landable.  Only
    ``new`` decides the outcome -- the diff, never the absolute count, because
    both suites carry a large pre-existing residual and gating on green would
    mean never landing anything [EVAL-25].

    An empty subject set against a nonempty baseline is all-fixed only when
    ``subject_executed`` proves the subject actually ran the applicable suite.
    Otherwise it is ``ids_not_measured``: omitted ids, a ``no tests ran``
    collection, or any other zero-execution subject must not read as having
    fixed every residual failure.
    """
    base_raw = [i.strip() for i in baseline if i.strip()]
    subj_raw = [i.strip() for i in subject if i.strip()]
    if not base_raw and not subj_raw:
        if subject_executed:
            return {
                "outcome": "zero_regression",
                "new": [],
                "fixed": [],
                "baseline_raw": 0,
                "subject_raw": 0,
                "baseline_count": 0,
                "subject_count": 0,
                "collapsed": 0,
            }
        # Without execution proof, two empty sets are not a zero-regression
        # result; they are a run that produced no measurement. A suite whose
        # whole collection errored out emits exactly this shape [OBS-08].
        return _ids_not_measured_diff(
            baseline_raw=0,
            subject_raw=0,
            baseline_count=0,
            subject_count=0,
            collapsed=0,
        )
    if base_raw and not subj_raw and not subject_executed:
        base = set(normalize_failure_ids(base_raw))
        return _ids_not_measured_diff(
            baseline_raw=len(set(base_raw)),
            subject_raw=0,
            baseline_count=len(base),
            subject_count=0,
            collapsed=len(set(base_raw)) - len(base),
        )
    base = set(normalize_failure_ids(base_raw))
    subj = set(normalize_failure_ids(subj_raw))
    new = sorted(subj - base)
    fixed = sorted(base - subj)
    return {
        "outcome": "regressed" if new else "zero_regression",
        "new": new,
        "fixed": fixed,
        "baseline_raw": len(set(base_raw)),
        "subject_raw": len(set(subj_raw)),
        "baseline_count": len(base),
        "subject_count": len(subj),
        # How much of the raw id space normalization merged, counted over the
        # UNION of both sides. Summing the two sides separately would report 2
        # for the very case this exists to describe -- one failure seen once in
        # each checkout -- when the honest count is the one phantom NEW/FIXED
        # pair the collapse removed. Zero means the two sides were already
        # comparable; nonzero is the operator's cue that the gate's answer
        # depends on the collapse [OBS-08].
        "collapsed": len(set(base_raw) | set(subj_raw)) - len(set(normalize_failure_ids([*base_raw, *subj_raw]))),
    }


def _looks_like_node_id(node_id: str) -> bool:
    if not node_id or _NODE_ID_RE.fullmatch(node_id) is None:
        return False
    base = node_id.split("[", 1)[0]
    # Pytest node ids have either a collection separator, a path separator,
    # or a Python-file suffix. A bare ``ERROR logger-only`` line is logging,
    # not a file-level collection error [LANDGA-M-01].
    return "::" in base or "/" in base or "\\" in base or base.endswith(".py")


def _node_id_from_status_rest(rest: str) -> str | None:
    """STATUS + node ID + optional `` - `` message, preserving spaces in the id."""
    rest = rest.strip()
    if not rest:
        return None
    if " - " not in rest:
        return rest if _looks_like_node_id(rest) else None
    start = 0
    while True:
        idx = rest.find(" - ", start)
        if idx == -1:
            break
        candidate = rest[:idx].rstrip()
        if _looks_like_node_id(candidate):
            return candidate
        start = idx + 3
    return rest if _looks_like_node_id(rest) else None


def _visible_line(line: str) -> str:
    """Strip ANSI so colored pytest status/count lines still parse."""
    return _ANSI_RE.sub("", line).strip()


def _parse_summary_status_line(line: str) -> tuple[str | None, bool]:
    """Return ``(node_id, is_status_line)``. Malformed status lines have no node."""
    match = _FAILURE_STATUS_RE.match(_visible_line(line))
    if match is None:
        return None, False
    rest = match.group(2)
    if rest is None:
        return None, True
    return _node_id_from_status_rest(rest), True


def _announced_failure_count(line: str) -> int | None:
    stripped = _visible_line(line)
    if not _PYTEST_SUMMARY_RE.match(stripped):
        return None
    if re.search(r"\bno tests ran\b", stripped):
        return 0
    failed = 0
    errors = 0
    for num, word in _COUNT_WORD_RE.findall(stripped):
        value = int(num)
        if word == "failed":
            failed = value
        elif word in {"error", "errors"}:
            errors = value
    return failed + errors


def _ids_from_summary_region(lines: Sequence[str]) -> tuple[list[str], bool]:
    ids: list[str] = []
    seen: set[str] = set()
    malformed = False
    for line in lines:
        stripped = _visible_line(line)
        if not stripped or _PYTEST_SUMMARY_RE.match(stripped):
            continue
        node_id, is_status = _parse_summary_status_line(stripped)
        if not is_status:
            continue
        if node_id is None:
            malformed = True
            continue
        if node_id in seen:
            continue
        seen.add(node_id)
        ids.append(node_id)
    return ids, malformed


def _read_extract_text(source: str | Path) -> str | None:
    if not isinstance(source, Path):
        return source
    try:
        return source.read_text(encoding="utf-8", errors="surrogateescape")
    except OSError:
        return None


def _extract_from_lines(lines: Sequence[str]) -> dict[str, Any]:
    visible = [_visible_line(line) for line in lines]
    header_idx = None
    for index, line in enumerate(visible):
        if _SHORT_SUMMARY_HEADER_RE.match(line):
            header_idx = index
    tail = [line for line in visible if line]
    finished = bool(tail and _PYTEST_SUMMARY_RE.match(tail[-1]))
    announced = _announced_failure_count(tail[-1]) if finished else None
    if header_idx is None:
        if finished and announced == 0:
            return {"outcome": "extracted", "ids": []}
        return {"outcome": "incomplete", "ids": []}
    ids, malformed = _ids_from_summary_region(visible[header_idx + 1 :])
    if malformed or not finished or announced is None or announced != len(ids):
        return {"outcome": "incomplete", "ids": []}
    return {"outcome": "extracted", "ids": ids}


def extract_failure_ids(source: str | Path) -> dict[str, Any]:
    """Extract pytest node ids from the short-test-summary region.

    Outcomes are a closed set so examined-zero is never spelled the same way
    as "we could not tell" [OBS-08]:

    ``extracted``
        The log was readable and either contained the full-line short-summary
        header (ids taken from that region only) or finished with pytest's
        equals-wrapped counts line and no summary header, which is how an
        all-pass run looks. Announced failed+error counts must equal the
        unique extracted ids.
    ``incomplete``
        The log was readable but has no full-line summary header and did not
        finish, announced failures/errors that do not reconcile with extracted
        ids, or malformed status lines. Pre-summary ``ERROR`` logger lines
        are not evidence of a finished extraction.
    ``unavailable``
        The log could not be read at all.
    """
    text = _read_extract_text(source)
    if text is None:
        return {"outcome": "unavailable", "ids": []}
    return _extract_from_lines(text.splitlines())


def _read_ids(path: Path | None) -> list[str]:
    if path is None:
        return []
    try:
        return path.read_text(encoding="utf-8", errors="surrogateescape").splitlines()
    except OSError as exc:
        raise LandingGateError(f"could not read failure ids from {path}: {exc}") from exc


def _suite_executed(text: str) -> bool:
    """True only when pytest's terminal line proves tests actually ran."""
    tail = [line for line in (_visible_line(raw) for raw in text.splitlines()) if line]
    if not tail:
        return False
    last = tail[-1]
    if not _PYTEST_SUMMARY_RE.match(last):
        return False
    if re.search(r"\bno tests ran\b", last):
        return False
    executed = 0
    for num, word in _COUNT_WORD_RE.findall(last):
        if word in _EXECUTED_COUNT_WORDS:
            executed += int(num)
    return executed > 0


def _subject_suite_executed(path: Path | None) -> bool:
    if path is None:
        return False
    try:
        text = Path(path).read_text(encoding="utf-8", errors="surrogateescape")
    except OSError as exc:
        raise LandingGateError(f"could not read the subject pytest log at {path}: {exc}") from exc
    return _suite_executed(text)


def _ids_bound_to_raw(raw_path: Path | None, ids_path: Path | None) -> tuple[list[str], str]:
    """Return (ids, status). Status is bound, mismatch, or ids_not_measured.

    Canonical ids come from raw extraction. Incomplete or unavailable extraction
    must not fall back to sidecar IDs as authority [REVIEW-H-02].
    """
    if raw_path is None:
        return [], "ids_not_measured"
    extracted = extract_failure_ids(raw_path)
    if extracted["outcome"] != "extracted":
        return [], "ids_not_measured"
    extracted_ids = list(extracted["ids"])
    if ids_path is None:
        return [], "ids_not_measured"
    supplied = _read_ids(ids_path)
    if normalize_failure_ids(extracted_ids) != normalize_failure_ids(supplied):
        return extracted_ids, "mismatch"
    return extracted_ids, "bound"
