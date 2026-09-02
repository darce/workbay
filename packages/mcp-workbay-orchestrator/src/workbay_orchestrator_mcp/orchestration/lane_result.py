#!/usr/bin/env python3
"""Handle structured lane run results.

F2 (production ``build_final_handoff_argv`` never threads ``max_turns``) is
deferred to a follow-up lane that owns that argv builder. Turn count is read
only from ``receiver_num_turns``: harvest copies ``token_usage.numTurns`` from
the worker result object, and ``stamp_attempt_evidence`` merges worker-seeded
``phase_timing``, so neither location is a dedicated receiver copy.
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import logging
import os
import re
import subprocess
import sys
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal

# Bare-script bootstrap (SG-19): worker_daemon spawns this file as a script
# path with no env=PYTHONPATH. Sibling orchestration modules use SCRIPT_DIR
# on sys.path and bare imports for the same reason.
SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

logger = logging.getLogger(__name__)
_SUBPROCESS_TIMEOUT_SECONDS = 300


def _lane_message_available() -> bool:
    return importlib.util.find_spec("workbay_handoff_mcp") is not None


def _record_artifact_lane_message(
    *,
    orchestrator_root: Path,
    task_ref: str,
    lane_id: str,
    session: str,
    details: str,
    artifact_ref: Any,
    delivery_id: str | None = None,
) -> None:
    from workbay_handoff_mcp.api import configure_runtime  # noqa: PLC0415
    from workbay_handoff_mcp.config import RuntimeConfig  # noqa: PLC0415

    from workbay_orchestrator_mcp.lanes import (  # noqa: PLC0415
        _handoff_lane_message_dispatch_id,
        lane_communication,
    )

    configure_runtime(RuntimeConfig.for_repo(orchestrator_root))
    payload = {"artifacts": [str(artifact_ref)]}
    normalized_delivery_id = _normalize_text(delivery_id)
    if normalized_delivery_id:
        payload["dispatch_id"] = _handoff_lane_message_dispatch_id(
            normalized_delivery_id,
            "artifact-message",
        )
    lane_communication(
        kind="message",
        operation="record",
        task_ref=task_ref,
        lane_id=lane_id,
        session=session,
        direction="worker_to_orchestrator",
        message=details,
        subject=f"{lane_id} handoff",
        status="open",
        payload=payload,
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Handle structured lane run results.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("schema", help="Print the JSON schema for codex exec lane results.")

    handoff = subparsers.add_parser("handoff", help="Turn a structured codex result into a lane handoff.")
    handoff.add_argument("--orchestrator-root", required=True)
    handoff.add_argument("--task-ref", required=True)
    handoff.add_argument("--lane-id", required=True)
    handoff.add_argument("--session", required=True)
    handoff.add_argument("--worktree-path", required=True)
    handoff.add_argument("--result-file", required=True)
    handoff.add_argument("--outcome", default=None)
    handoff.add_argument("--dry-run", action="store_true")
    handoff.add_argument(
        "--delivery-id",
        default=None,
        help="Stable identifier for one durable replay delivery attempt.",
    )
    handoff.add_argument(
        "--max-turns",
        type=int,
        default=None,
        metavar="N",
        help=(
            "Lane turn cap used to classify turn_cap_exhausted. Defaults to "
            "WORKBAY_LANE_MAX_TURNS, then to a receiver-stamped max_turns "
            "field on the result file."
        ),
    )

    return parser.parse_args()


# Format-class blanks: Unicode whitespace (\s, including NBSP/EM SPACE) plus
# Cf/format controls that are not \s (ZWSP/ZWNJ/ZWJ/BOM, SHY, CGJ, ALM, MVS,
# LRM/RLM, bidi embeddings/overrides, WJ and the rest of U+2060..U+206F
# except the unassigned U+2065 hole). jsonschema `pattern` is Python re here.
# Strip this class, then apply the substance minimum (20 for prose/blockers,
# 3 for tests_run command items). `\S` alone is not enough: U+200B is not \s.
_FORMAT_CONTROLS = (
    "\u00ad"  # SOFT HYPHEN
    "\u034f"  # COMBINING GRAPHEME JOINER
    "\u061c"  # ARABIC LETTER MARK
    "\u180e"  # MONGOLIAN VOWEL SEPARATOR
    "\u200b"  # ZERO WIDTH SPACE
    "\u200c"  # ZERO WIDTH NON-JOINER
    "\u200d"  # ZERO WIDTH JOINER
    "\u200e"  # LEFT-TO-RIGHT MARK
    "\u200f"  # RIGHT-TO-LEFT MARK
    "\u202a"  # LRE
    "\u202b"  # RLE
    "\u202c"  # PDF
    "\u202d"  # LRO
    "\u202e"  # RLO
    "\u2060"  # WORD JOINER
    "\u2061"  # FUNCTION APPLICATION
    "\u2062"  # INVISIBLE TIMES
    "\u2063"  # INVISIBLE SEPARATOR
    "\u2064"  # INVISIBLE PLUS
    "\u2066"  # LRI
    "\u2067"  # RLI
    "\u2068"  # FSI
    "\u2069"  # PDI
    "\u206a"  # ISS
    "\u206b"  # ASS
    "\u206c"  # IAFS
    "\u206d"  # AAFS
    "\u206e"  # NADS
    "\u206f"  # NODS
    "\ufeff"  # BOM / ZERO WIDTH NO-BREAK SPACE
)
_FORMAT_BLANKS = "\\s" + _FORMAT_CONTROLS

# Separator role of the substance floor. Same blanks as _FORMAT_BLANKS except
# the U+0000..U+001F range that JSON forbids unescaped inside a string. The
# generic `\s` shorthand is kept on the exclusion side (a newline is not
# substance) and is replaced here by the JSON-safe members of Unicode
# whitespace: ordinary space, next line, no-break space, ogham space mark,
# en-quad through hair space, line/paragraph separators, narrow no-break
# space, medium mathematical space, and ideographic space.
_JSON_SAFE_SEPARATORS = (
    " "
    "\u0085"
    "\u00a0"
    "\u1680"
    "\u2000"
    "\u2001"
    "\u2002"
    "\u2003"
    "\u2004"
    "\u2005"
    "\u2006"
    "\u2007"
    "\u2008"
    "\u2009"
    "\u200a"
    "\u2028"
    "\u2029"
    "\u202f"
    "\u205f"
    "\u3000" + _FORMAT_CONTROLS
)


def _substance_floor(minimum: int) -> str:
    """Require `minimum` chars that are not format-class blanks.

    Separators between those chars may be JSON-safe blanks only: the
    exclusion class still uses `_FORMAT_BLANKS` (so newline is not
    substance), but the separator class drops U+0000..U+001F.
    """
    return f"([^{_FORMAT_BLANKS}][{_JSON_SAFE_SEPARATORS}]*){{{minimum},}}"


_SUBSTANCE_20 = _substance_floor(20)
_SUBSTANCE_3 = _substance_floor(3)

# Per-action floors used to live in schema anyOf / minItems. A hard demotion
# of merge_ready-with-empty-tests_run breaks review lanes that legitimately
# ran no tests. These notes are appended to blockers; handoff_action is never
# changed.
_ADVISORY_EMPTY_TESTS_RUN = (
    "Advisory: merge_ready was accepted with empty tests_run; review lanes may legitimately run no tests."
)
_ADVISORY_EMPTY_BLOCKERS = (
    "Advisory: needs_guidance was accepted with empty blockers; the attempt is recorded in other fields."
)


class SubstanceIssue:
    """One hard substance-floor failure on a parsed field or array item."""

    __slots__ = ("path", "message")

    def __init__(self, path: tuple[Any, ...], message: str) -> None:
        self.path = path
        self.message = message


class SubstanceValidationError(ValueError):
    """Hard failure: a parsed lane result failed the substance floor."""

    def __init__(self, issues: list[SubstanceIssue]) -> None:
        self.issues = list(issues)
        paths = [issue.path for issue in self.issues]
        super().__init__(f"lane result failed the substance floor: {paths!r}")


class DegenerateHandoffReason(StrEnum):
    """Machine-routable reasons a structurally valid handoff is refused."""

    GUIDANCE_WITHOUT_ATTEMPT = "guidance_without_attempt"


class DegenerateHandoffError(RuntimeError):
    """A received handoff is shaped correctly but carries no actionable state."""

    def __init__(self, reason: DegenerateHandoffReason, requirement: str) -> None:
        self.reason = reason
        self.requirement = requirement
        super().__init__(f"degenerate lane handoff ({reason.value}): {requirement}")


DegeneracyClassification = Literal["attempted", "degenerate_no_attempt", "turn_cap_exhausted"]
_TURN_CAP_EXHAUSTED = "turn_cap_exhausted"
_UNKNOWN_MAX_TURNS = 2**31 - 1
_MAX_TURNS_ENV = "WORKBAY_LANE_MAX_TURNS"
# Distinct envelope key for adapter-owned harvest. Worker result JSON cannot
# populate it (schema additionalProperties is false; harvest copies
# token_usage from the worker object, not this field).
RECEIVER_NUM_TURNS_KEY = "receiver_num_turns"


def _is_typed_transport_result(result: dict[str, Any]) -> bool:
    """Return whether the adapter, rather than the worker, ended the run."""
    original_action = result.get("original_handoff_action")
    if original_action in {"rate_limited", "transport_failure"}:
        return True
    raw_payload = result.get("raw_payload")
    return isinstance(raw_payload, dict) and (
        raw_payload.get("transport_failure") is True
        or raw_payload.get("rate_limited") is True
        or raw_payload.get("admission_deferred") is True
    )


def _nonneg_int(value: Any) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    return value


def _first_nonneg_int(*values: Any) -> int | None:
    for value in values:
        parsed = _nonneg_int(value)
        if parsed is not None:
            return parsed
    return None


def _raw_payload(result: dict[str, Any]) -> dict[str, Any]:
    raw_payload = result.get("raw_payload")
    return raw_payload if isinstance(raw_payload, dict) else {}


def _drop_worker_authored_tool_call_count(result: dict[str, Any]) -> dict[str, Any]:
    """Drop worker-visible counts before classify (F4). Keep adapter phase_timing."""
    out = dict(result)
    out.pop("tool_call_count", None)
    raw_payload = out.get("raw_payload")
    if isinstance(raw_payload, dict) and "tool_call_count" in raw_payload:
        raw_payload = dict(raw_payload)
        raw_payload.pop("tool_call_count", None)
        out["raw_payload"] = raw_payload
    return out


def _payload_tool_call_count(result: dict[str, Any]) -> int | None:
    """Read tool-call count only from adapter-owned counted phase_timing (F2).

    A nested integer without ``tool_call_count_status == counted`` is not
    evidence: workers can mint ``phase_timing.tool_call_count`` in a result
    document, and ``from_dict`` used to copy that object into ``raw_payload``.
    """
    if _tool_call_count_status(result) != "counted":
        return None
    phase_timing = _phase_timing(result)
    if "tool_call_count" not in phase_timing:
        return None
    return _nonneg_int(phase_timing.get("tool_call_count"))


def _phase_timing(result: dict[str, Any]) -> dict[str, Any]:
    phase_timing = _raw_payload(result).get("phase_timing")
    return phase_timing if isinstance(phase_timing, dict) else {}


def _tool_call_count_status(result: dict[str, Any]) -> str | None:
    status = _phase_timing(result).get("tool_call_count_status")
    return status if isinstance(status, str) and status else None


def _tool_call_count_source(result: dict[str, Any]) -> str | None:
    """Read the adapter-owned source for the tool-call count, if any."""
    for container in (_phase_timing(result), _raw_payload(result), result):
        source = container.get("tool_call_count_source")
        if isinstance(source, str) and source.strip():
            return source.strip()
    return None


def _payload_num_turns(result: dict[str, Any]) -> int | None:
    """Read turn count only from the dedicated receiver copy (F3).

    Worker-visible ``result`` / ``raw_payload`` fields, harvested
    ``token_usage.numTurns``, and nested ``phase_timing.num_turns`` are
    ignored so a placeholder cannot reclassify itself. ``from_dict`` copies
    the worker document into ``raw_payload``, and ``stamp_attempt_evidence``
    merges that ``phase_timing``, so nested ``num_turns`` is not adapter-owned.
    The receiver copy lives on ``receiver_num_turns``, a distinct envelope
    key the worker schema cannot populate. Adapters that serialize through
    ``BackendResult.to_dict`` keep that copy on ``raw_payload``.
    """
    return _first_nonneg_int(
        result.get(RECEIVER_NUM_TURNS_KEY),
        _raw_payload(result).get(RECEIVER_NUM_TURNS_KEY),
    )


def _payload_stop_reason(result: dict[str, Any]) -> str | None:
    raw_payload = _raw_payload(result)
    for value in (
        result.get("stopReason"),
        result.get("stop_reason"),
        raw_payload.get("stopReason"),
        raw_payload.get("stop_reason"),
    ):
        if isinstance(value, str) and value:
            return value
    return None


def _payload_structured_output_error(result: dict[str, Any]) -> str | None:
    raw_payload = _raw_payload(result)
    for value in (result.get("structuredOutputError"), raw_payload.get("structuredOutputError")):
        if isinstance(value, str) and value.strip():
            return value
    return None


def _has_structured_output_error_with_placeholder(result: dict[str, Any]) -> bool:
    if _payload_structured_output_error(result) is None:
        return False
    raw_payload = _raw_payload(result)
    structured = (
        raw_payload["structuredOutput"]
        if "structuredOutput" in raw_payload
        else result.get("structuredOutput")
    )
    if structured is None:
        return result.get("handoff_action") == "needs_guidance"
    if not isinstance(structured, dict):
        return False
    tests_run = structured.get("tests_run")
    return structured.get("handoff_action") == "needs_guidance" and (
        not isinstance(tests_run, list) or len(tests_run) == 0
    )


def _has_attempt_evidence(result: dict[str, Any]) -> bool:
    """Use adapter-owned lifecycle evidence, not token spend or prose (F1 / OBS-08).

    An ``unobservable`` tool-call source is unknown, never evidence of work.
    Fail open only when the receiver already recorded that the lane ran:
    ``num_turns > 1`` or a counted positive ``tool_call_count``. A first-turn
    envelope with source ``unobservable`` and neither of those stays
    ``degenerate_no_attempt``. A completed schema-shaped ``merge_ready`` with
    a counted 0 and a receiver turn copy is attempt evidence (F1).
    """
    num_turns = _payload_num_turns(result)
    if num_turns is not None and num_turns > 1:
        return True
    tool_call_count = _payload_tool_call_count(result)
    if tool_call_count is not None and tool_call_count > 0:
        return True
    return (
        result.get("handoff_action") == "merge_ready"
        and _tool_call_count_status(result) == "counted"
        and tool_call_count == 0
        and num_turns is not None
    )


def _cli_max_turns(args: argparse.Namespace) -> int | None:
    """Resolve the handoff CLI cap: ``--max-turns``, else ``WORKBAY_LANE_MAX_TURNS``."""
    explicit = _nonneg_int(getattr(args, "max_turns", None))
    if explicit is not None and explicit > 0:
        return explicit
    env_raw = os.environ.get(_MAX_TURNS_ENV)
    if env_raw is None or not str(env_raw).strip():
        return None
    try:
        parsed = int(str(env_raw).strip())
    except ValueError:
        return None
    if parsed < 1:
        return None
    return parsed


def _resolve_max_turns(result: dict[str, Any], explicit: int | None) -> int:
    """Prefer the adapter-stamped cap, then CLI/env, then the unknown sentinel."""
    from_payload = _first_nonneg_int(
        result.get("max_turns"),
        _raw_payload(result).get("max_turns"),
        _phase_timing(result).get("max_turns"),
    )
    if from_payload is not None and from_payload > 0:
        return from_payload
    parsed_explicit = _nonneg_int(explicit)
    if parsed_explicit is not None and parsed_explicit > 0:
        return parsed_explicit
    return _UNKNOWN_MAX_TURNS


def _is_first_turn(payload: dict[str, Any], *, num_turns: int | None, tool_call_count: int | None) -> bool:
    """First-turn unless adapter-owned evidence says the lane already ran.

    A missing turn count is first-turn by default so a toolless placeholder
    cannot buy passage. Once tool-call status is ``counted`` and the count is
    positive, the missing harvest is not first-turn (F3). A counted zero with
    no receiver-stamped turn count remains first-turn (F1).
    """
    if num_turns is not None:
        return num_turns <= 1
    counted = _tool_call_count_status(payload) == "counted"
    if counted and tool_call_count is not None and tool_call_count > 0:
        return False
    return True


def classify_degeneracy(payload: dict[str, Any], *, max_turns: int) -> DegeneracyClassification:
    """Classify a received lane result by receiver-visible lifecycle facts.

    ``turn_cap_exhausted`` is a distinct typed outcome: never degenerate, never
    merge_ready. ``degenerate_no_attempt`` is only for first-turn documents
    with no adapter-owned lifecycle evidence. Observed token spend and an
    ``unobservable`` tool-call source are not attempt evidence (F1).
    """
    payload = _drop_worker_authored_tool_call_count(payload)
    num_turns = _payload_num_turns(payload)
    cap = _nonneg_int(max_turns)
    if (
        cap is not None
        and cap > 0
        and num_turns is not None
        and num_turns >= cap
        and (
            _payload_stop_reason(payload) == "Cancelled"
            or _has_structured_output_error_with_placeholder(payload)
        )
    ):
        return "turn_cap_exhausted"

    tool_call_count = _payload_tool_call_count(payload)
    first_turn = _is_first_turn(payload, num_turns=num_turns, tool_call_count=tool_call_count)
    if first_turn and not _has_attempt_evidence(payload):
        return "degenerate_no_attempt"
    return "attempted"


def _apply_turn_cap_exhausted(result: dict[str, Any]) -> dict[str, Any]:
    """Stamp the typed cap outcome; never leave the document as merge_ready."""
    out = dict(result)
    out["original_handoff_action"] = _TURN_CAP_EXHAUSTED
    out["handoff_action"] = "needs_guidance"
    return _apply_advisory_floors(out)


def _degenerate_handoff_reason(
    result: dict[str, Any],
    *,
    max_turns: int | None = None,
) -> DegenerateHandoffReason | None:
    """Return the exact receiving-boundary anti-degeneracy failure, if any.

    ``degenerate_no_attempt`` is vetoed regardless of ``handoff_action`` so a
    first-turn toolless ``merge_ready`` is refused like a first-turn toolless
    ``needs_guidance`` (F1).
    """
    if _is_typed_transport_result(result):
        return None
    classification = classify_degeneracy(result, max_turns=_resolve_max_turns(result, max_turns))
    if classification == "degenerate_no_attempt":
        return DegenerateHandoffReason.GUIDANCE_WITHOUT_ATTEMPT
    return None


def _meets_substance_floor(value: str, pattern: str) -> bool:
    """Same unanchored search jsonschema used for the former `pattern` keyword."""
    return re.search(pattern, value) is not None


def _hard_substance_issues(result: dict[str, Any]) -> list[SubstanceIssue]:
    """Reject zero-width-only, whitespace-only, and separator-padded junk."""
    issues: list[SubstanceIssue] = []
    summary = result.get("summary")
    if isinstance(summary, str) and not _meets_substance_floor(summary, _SUBSTANCE_20):
        issues.append(SubstanceIssue(("summary",), "summary failed the 20-substance floor"))
    details = result.get("details")
    if isinstance(details, str) and not _meets_substance_floor(details, _SUBSTANCE_20):
        issues.append(SubstanceIssue(("details",), "details failed the 20-substance floor"))
    blockers = result.get("blockers")
    if isinstance(blockers, list):
        for index, item in enumerate(blockers):
            if isinstance(item, str) and not _meets_substance_floor(item, _SUBSTANCE_20):
                issues.append(SubstanceIssue(("blockers", index), "blocker failed the 20-substance floor"))
    tests_run = result.get("tests_run")
    if isinstance(tests_run, list):
        for index, item in enumerate(tests_run):
            if isinstance(item, str) and not _meets_substance_floor(item, _SUBSTANCE_3):
                issues.append(
                    SubstanceIssue(
                        ("tests_run", index),
                        "tests_run item failed the 3-substance floor",
                    )
                )
    return issues


def _apply_advisory_floors(result: dict[str, Any]) -> dict[str, Any]:
    """Append a note/blocker for per-action floor misses; never change the action."""
    out = dict(result)
    action = out.get("handoff_action")
    tests_run = out.get("tests_run")
    blockers = out.get("blockers")
    notes: list[str] = []
    if action == "merge_ready" and isinstance(tests_run, list) and len(tests_run) == 0:
        notes.append(_ADVISORY_EMPTY_TESTS_RUN)
    if action == "needs_guidance" and isinstance(blockers, list) and len(blockers) == 0:
        notes.append(_ADVISORY_EMPTY_BLOCKERS)
    if not notes:
        return out
    existing = list(blockers) if isinstance(blockers, list) else []
    for note in notes:
        if note not in existing:
            existing.append(note)
    out["blockers"] = existing
    return out


def _validate_parsed_lane_result(result: dict[str, Any]) -> dict[str, Any]:
    """Post-parse substance check for a lane-result document.

    Hard-rejects the junk the former schema `pattern` keyword rejected.
    Per-action empty arrays remain advisory because their meaning depends on
    lifecycle context that this generic parsed-document validator does not own.
    """
    issues = _hard_substance_issues(result)
    if issues:
        raise SubstanceValidationError(issues)
    return _apply_advisory_floors(result)


def _schema() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["handoff_action", "summary", "details", "tests_run", "blockers"],
        "properties": {
            "handoff_action": {
                "type": "string",
                "enum": ["merge_ready", "needs_guidance"],
                "description": (
                    "Use merge_ready only when lane-owned code changes were made "
                    "and are ready for orchestrator review. Do not use "
                    "needs_guidance as a first-turn or turn-0 exit, as a "
                    "placeholder, or before attempting the assigned work. "
                    "needs_guidance is licensed ONLY for external blockers "
                    "encountered AFTER attempting the work (sandbox failures, "
                    "verification blockers, already-resolved findings needing "
                    "orchestrator review). A first-turn needs_guidance document "
                    "is prohibited."
                ),
            },
            "summary": {
                "type": "string",
                "minLength": 40,
                "description": "One short sentence the orchestrator can scan quickly.",
            },
            "details": {
                "type": "string",
                "minLength": 120,
                "description": (
                    "Concise explanation of what changed or what was verified, "
                    "plus why the lane is ready or blocked. When "
                    "handoff_action is needs_guidance, this must describe what "
                    "was attempted before requesting guidance."
                ),
            },
            "tests_run": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Only the commands actually run in this session.",
            },
            "blockers": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Concrete blockers or asks for the orchestrator. Use an empty array when none.",
            },
        },
    }


def _load_result(path: Path, *, max_turns: int | None = None) -> dict[str, Any]:
    """Load a saved result without relaxing its acceptance contract."""
    payload = json.loads(path.read_text())
    if not isinstance(payload, dict):
        raise RuntimeError("Expected JSON object in result file.")
    try:
        result = _validate_parsed_lane_result(payload)
    except SubstanceValidationError as exc:
        raise RuntimeError(str(exc)) from exc
    # Constrained decoding cannot reliably enforce an action-dependent schema
    # branch. At the durable receiving boundary, fail on the fact the policy
    # actually requires: evidence that work was attempted. Summary, details,
    # and blockers are worker-authored prose and therefore are not evidence.
    # Typed adapter failures are transport outcomes, not worker bailouts.
    if _is_typed_transport_result(result):
        return result
    resolved_max_turns = _resolve_max_turns(result, max_turns)
    classification = classify_degeneracy(result, max_turns=resolved_max_turns)
    if classification == "turn_cap_exhausted":
        return _apply_turn_cap_exhausted(result)
    # Veto first-turn toolless placeholders for every action, including
    # schema-shaped merge_ready (F1). Do not wait for the needs_guidance-only
    # helper to fire.
    if classification == "degenerate_no_attempt":
        raise DegenerateHandoffError(
            DegenerateHandoffReason.GUIDANCE_WITHOUT_ATTEMPT,
            "needs_guidance requires positive receiver-recorded lifecycle evidence",
        )
    reason = _degenerate_handoff_reason(result, max_turns=resolved_max_turns)
    if reason is not None:
        raise DegenerateHandoffError(
            reason,
            "needs_guidance requires positive receiver-recorded lifecycle evidence",
        )
    return result


def _normalize_text(value: Any) -> str:
    without_format_blanks = "".join(char for char in str(value or "") if char not in _FORMAT_CONTROLS)
    return " ".join(without_format_blanks.split())


def _normalize_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [_normalize_text(item) for item in value if _normalize_text(item)]


def _build_report_command(
    *,
    orchestrator_root: Path,
    task_ref: str,
    lane_id: str,
    session: str,
    worktree_path: Path,
    result: dict[str, Any],
    outcome: str | None = None,
    delivery_id: str | None = None,
) -> list[str]:
    action = _normalize_text(result.get("handoff_action"))
    summary = _normalize_text(result.get("summary"))
    details = _normalize_text(result.get("details"))
    tests_run = _normalize_list(result.get("tests_run"))
    blockers = _normalize_list(result.get("blockers"))

    if not action:
        raise RuntimeError("Missing handoff_action in result payload.")
    if not summary:
        raise RuntimeError("Missing summary in result payload.")
    # Details may be omitted when the caller only needs the commit plan; fall
    # back to summary so the report still carries a non-empty message.
    if not details:
        details = summary

    from workbay_orchestrator_mcp._assets import bundled_script_path  # noqa: PLC0415

    report_cmd = [
        str(bundled_script_path("worktree-lane")),
        "report",
        "--orchestrator-root",
        str(orchestrator_root),
        "--task-ref",
        task_ref,
        "--lane-id",
        lane_id,
        "--session",
        session,
        "--summary",
        summary,
        "--worktree-path",
        str(worktree_path),
    ]
    normalized_delivery_id = _normalize_text(delivery_id)
    if normalized_delivery_id:
        report_cmd.extend(["--delivery-id", normalized_delivery_id])
    normalized_outcome = _normalize_text(outcome)
    if normalized_outcome:
        report_cmd.extend(["--outcome", normalized_outcome])
    for test_command in tests_run:
        report_cmd.extend(["--test-command", test_command])
    if details:
        report_cmd.extend(["--message", details])
    if action == "merge_ready":
        report_cmd.append("--merge-ready")
        return report_cmd
    if action == "needs_guidance":
        report_cmd.append("--guidance-request")
        # Guidance requests may describe already-present lane-local state or
        # verification on top of uncommitted files, so they must not hard-fail
        # on a dirty worktree before the orchestrator can intake the report.
        report_cmd.append("--allow-dirty")
        for blocker in blockers:
            report_cmd.extend(["--blocker", blocker])
        return report_cmd
    raise RuntimeError(f"Unsupported handoff_action: {action}")


def _build_command_plan(
    *,
    orchestrator_root: Path,
    task_ref: str,
    lane_id: str,
    session: str,
    worktree_path: Path,
    result: dict[str, Any],
    outcome: str | None = None,
    delivery_id: str | None = None,
) -> list[tuple[list[str], bool]]:
    """Return a list of (command, critical) tuples."""
    action = _normalize_text(result.get("handoff_action"))
    report_cmd = _build_report_command(
        orchestrator_root=orchestrator_root,
        task_ref=task_ref,
        lane_id=lane_id,
        session=session,
        worktree_path=worktree_path,
        result=result,
        outcome=outcome,
        delivery_id=delivery_id,
    )
    if action == "merge_ready":
        # No commit subject is built here: the worker already committed before
        # this sink runs, and merge-ready reports treat those lane commits as
        # the source of truth. A commit step would either be a no-op or would
        # capture work the worker deliberately left uncommitted.
        from workbay_orchestrator_mcp._assets import bundled_script_path  # noqa: PLC0415

        status_cmd = [
            str(bundled_script_path("worktree-lane")),
            "status",
            "--orchestrator-root",
            str(orchestrator_root),
            "--task-ref",
            task_ref,
            "--lane-id",
            lane_id,
            "--worktree-path",
            str(worktree_path),
        ]
        return [
            (report_cmd, True),
            (status_cmd, False),
        ]
    if action == "needs_guidance":
        return [(report_cmd, True)]
    raise RuntimeError(f"Unsupported handoff_action: {action}")


def main() -> int:
    args = _parse_args()
    if args.command == "schema":
        print(json.dumps(_schema(), indent=2))
        return 0

    result_path = Path(args.result_file).expanduser().resolve()
    worktree_path = Path(args.worktree_path).expanduser().resolve()
    try:
        result = _load_result(result_path, max_turns=_cli_max_turns(args))
        commands = _build_command_plan(
            orchestrator_root=Path(args.orchestrator_root).expanduser().resolve(),
            task_ref=args.task_ref,
            lane_id=args.lane_id,
            session=args.session,
            worktree_path=worktree_path,
            result=result,
            outcome=args.outcome,
            delivery_id=args.delivery_id,
        )
    except (OSError, json.JSONDecodeError, RuntimeError) as exc:
        logger.error("lane-result: invalid result: %s", exc)
        print(str(exc), file=sys.stderr)
        return 1

    if args.dry_run:
        print(json.dumps({"commands": [cmd for cmd, _ in commands]}, indent=2))
        return 0

    # Pin worktree-lane's orchestrator CLI to the *running* package (sys.executable
    # + this import tree), not PATH / ~/.local/share/uv/tools. Without this, a
    # version-skewed uv-tool install steals every lane handoff identically.
    from workbay_orchestrator_mcp.lanes import handoff_subprocess_env  # noqa: PLC0415

    try:
        handoff_env = handoff_subprocess_env(os.environ)
    except RuntimeError as exc:
        logger.error("lane-result: handoff orchestrator resolution failed: %s", exc)
        print(str(exc), file=sys.stderr)
        return 2

    for command, critical in commands:
        try:
            completed = subprocess.run(
                command,
                check=False,
                timeout=_SUBPROCESS_TIMEOUT_SECONDS,
                env=handoff_env,
            )
        except subprocess.TimeoutExpired:
            logger.warning(
                "lane-result: command timed out after %ss: %s",
                _SUBPROCESS_TIMEOUT_SECONDS,
                command,
            )
            if critical:
                return 124
            continue
        if completed.returncode != 0:
            if critical:
                return completed.returncode
            logger.warning("lane-result: non-critical step failed (exit %s), continuing", completed.returncode)

    artifact_ref = result.get("details_artifact_ref")
    if artifact_ref is not None and _lane_message_available():
        details = _normalize_text(result.get("details"))
        try:
            _record_artifact_lane_message(
                orchestrator_root=Path(args.orchestrator_root).expanduser().resolve(),
                task_ref=args.task_ref,
                lane_id=args.lane_id,
                session=args.session,
                details=details,
                artifact_ref=artifact_ref,
                delivery_id=args.delivery_id,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("lane-result: artifact-carrying lane message failed: %s", exc)
    return 0


if __name__ == "__main__":
    sys.exit(main())
