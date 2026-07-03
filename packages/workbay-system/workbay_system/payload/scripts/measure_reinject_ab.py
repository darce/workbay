#!/usr/bin/env python3
"""Offline analyzer for WORKBAY_REINJECT_AB reorientation experiment (implementation note B8).

SUPERSEDED for implementation note (2026-06-15): under WORKBAY_REINJECT_AB the arm now
governs *selection* — treatment=semantic top-K (arm B), control=current
selection (arm A) — and BOTH arms emit. The m1/m2 reorientation metrics and the
>=20% m1-reduction decision rule below were calibrated for the 0042 emit-vs-
suppress window (where control emitted nothing); they apply ONLY to 0042-era
data and do NOT measure the 0046 selection difference. For the 0046 efficacy
gate use the offline recall@K eval (``embeddings/eval_recall.py``) and read
live per-arm token cost directly from ``session_reinjections.emitted_chars``
grouped by ``arm`` (offline recall fixture and operator notes document the gate).

Reads ``session_reinjections`` rows (arm treatment/control), pairs each session
with an on-disk JSONL transcript, and computes:

- m1: re-orientation cost in the first N post-compaction turns (load_session +
  search_handoff calls plus Read re-touches of files touched before compaction)
- m2: turns-to-first-write-action relative to the post-compaction window
  (Write/Edit/MultiEdit/NotebookEdit)

Writes one JSON object per session to a results.jsonl sink and can summarize
arms with the pre-registered >=20% median m1 reduction decision rule.

Operator runbook::

  python3 packages/workbay-system/workbay_system/payload/scripts/measure_reinject_ab.py \\
    --workspace-root . \\
    --transcripts-dir ~/.claude/projects/<project> \\
    --print-decision
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import statistics
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable, Sequence

HANDOFF_TOOL_NAMES = frozenset({"load_session", "search_handoff"})
WRITE_TOOL_NAMES = frozenset({"Write", "Edit", "MultiEdit", "NotebookEdit"})
READ_TOOL_NAMES = frozenset({"Read"})

DEFAULT_WINDOW_TURNS = 10
DECISION_M1_REDUCTION = 0.20
DEFAULT_MIN_SESSIONS_PER_ARM = 10
_JSONL_TURN_RECORD_TYPES = frozenset({"user", "assistant"})


@dataclass(frozen=True)
class TurnAction:
    turn: int
    tool_name: str
    file_path: str | None


@dataclass(frozen=True)
class AbSession:
    session_id: str
    arm: str
    task_ref: str
    compaction_id: str | None
    source: str
    created_at: str


@dataclass(frozen=True)
class MeasurementWindow:
    start_turn: int
    end_turn: int


@dataclass(frozen=True)
class SessionResult:
    session_id: str
    arm: str
    task_ref: str
    compaction_id: str | None
    m1_reorientation_cost: int
    m2_turns_to_first_write: int | None
    turns_analyzed: int
    window_start_turn: int


def _normalize_tool_name(name: str) -> str:
    if "__" in name:
        return name.rsplit("__", 1)[-1]
    return name


def _normalize_file_path(path: str, *, workspace_root: Path | None = None) -> str:
    candidate = Path(path)
    if candidate.is_absolute() and workspace_root is not None:
        try:
            return str(candidate.resolve().relative_to(workspace_root.resolve()))
        except ValueError:
            pass
    normalized = path.replace("\\", "/")
    if normalized.startswith("./"):
        normalized = normalized[2:]
    return normalized


def iter_turn_actions(transcript_text: str) -> list[TurnAction]:
    """Parse real-shape JSONL; increment turn on each user/assistant record."""
    actions: list[TurnAction] = []
    turn = 0
    for raw_line in transcript_text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(record, dict):
            continue
        record_type = record.get("type")
        if record_type not in _JSONL_TURN_RECORD_TYPES:
            continue
        turn += 1
        if record_type != "assistant":
            continue
        message = record.get("message") or {}
        if not isinstance(message, dict):
            continue
        for block in message.get("content") or []:
            if not isinstance(block, dict) or block.get("type") != "tool_use":
                continue
            tool_name = str(block.get("name") or "")
            raw_input = block.get("input") or {}
            file_path = None
            if isinstance(raw_input, dict):
                for key in ("path", "file_path", "target_file"):
                    value = raw_input.get(key)
                    if value:
                        file_path = str(value)
                        break
            actions.append(TurnAction(turn=turn, tool_name=tool_name, file_path=file_path))
    return actions


def max_transcript_turn(transcript_text: str) -> int:
    """Return the highest user/assistant ordinal in a JSONL transcript."""
    turn = 0
    for raw_line in transcript_text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(record, dict):
            continue
        if record.get("type") in _JSONL_TURN_RECORD_TYPES:
            turn += 1
    return turn


def derive_measurement_window(
    *,
    post_compaction_end_turn: int | None,
    window_turns: int = DEFAULT_WINDOW_TURNS,
) -> MeasurementWindow:
    start_turn = 1 if post_compaction_end_turn is None else post_compaction_end_turn + 1
    return MeasurementWindow(start_turn=start_turn, end_turn=start_turn + window_turns - 1)


def compute_m1(
    actions: Sequence[TurnAction],
    *,
    pre_compaction_files: set[str],
    window: MeasurementWindow,
    workspace_root: Path | None = None,
) -> int:
    cost = 0
    for action in actions:
        if action.turn < window.start_turn:
            continue
        if action.turn > window.end_turn:
            break
        norm = _normalize_tool_name(action.tool_name)
        if norm in HANDOFF_TOOL_NAMES:
            cost += 1
        elif norm in READ_TOOL_NAMES and action.file_path is not None:
            normalized_path = _normalize_file_path(action.file_path, workspace_root=workspace_root)
            if normalized_path in pre_compaction_files:
                cost += 1
    return cost


def compute_m2(
    actions: Sequence[TurnAction],
    *,
    window: MeasurementWindow,
) -> int | None:
    for action in sorted(actions, key=lambda item: item.turn):
        if action.turn < window.start_turn:
            continue
        if action.turn > window.end_turn:
            return None
        if _normalize_tool_name(action.tool_name) in WRITE_TOOL_NAMES:
            return action.turn - window.start_turn + 1
    return None


def load_ab_sessions(conn: sqlite3.Connection) -> list[AbSession]:
    if conn.row_factory is None:
        conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT session_id, arm, task_ref, compaction_id, source, created_at
        FROM session_reinjections
        WHERE arm IN ('treatment', 'control')
        ORDER BY created_at ASC, reinjection_id ASC
        """
    ).fetchall()
    by_session: dict[str, AbSession] = {}
    for row in rows:
        session = AbSession(
            session_id=str(row["session_id"]),
            arm=str(row["arm"]),
            task_ref=str(row["task_ref"]),
            compaction_id=row["compaction_id"],
            source=str(row["source"] or ""),
            created_at=str(row["created_at"]),
        )
        by_session[session.session_id] = session
    return list(by_session.values())


def _load_compaction_metadata(
    conn: sqlite3.Connection,
    compaction_id: str | None,
) -> tuple[int | None, str | None, str | None]:
    if not compaction_id:
        return None, None, None
    row = conn.execute(
        """
        SELECT turn_range, created_at, session_id
        FROM session_compactions
        WHERE compaction_id = ?
        """,
        (compaction_id,),
    ).fetchone()
    if row is None:
        return None, None, None
    end_turn: int | None = None
    try:
        payload = json.loads(str(row["turn_range"]))
        end_turn = int(payload["end_turn"])
    except (TypeError, ValueError, KeyError, json.JSONDecodeError):
        end_turn = None
    created_at = str(row["created_at"]) if row["created_at"] is not None else None
    compaction_session_id = str(row["session_id"]) if row["session_id"] is not None else None
    return end_turn, created_at, compaction_session_id


def _resolve_post_compaction_end_turn(
    *,
    source: str,
    session_id: str,
    compaction_end_turn: int | None,
    compaction_session_id: str | None,
) -> int | None:
    """Anchor AB windows to transcript-local ordinals.

    Resume sessions (and any reinject whose session_id differs from the linked
    compaction row) carry transcripts that restart turn numbering at 1.
    """
    if source.strip().lower() == "resume":
        return None
    if compaction_session_id and compaction_session_id != session_id:
        return None
    return compaction_end_turn


def _load_pre_compaction_files(
    conn: sqlite3.Connection,
    *,
    task_ref: str,
    before_created_at: str,
) -> set[str]:
    rows = conn.execute(
        """
        SELECT DISTINCT file_path
        FROM touched_files
        WHERE task_ref = ? AND touched_at <= ?
        """,
        (task_ref, before_created_at),
    ).fetchall()
    return {str(row["file_path"]) for row in rows}


def _resolve_transcript_path(transcripts_dir: Path, session_id: str) -> Path | None:
    for suffix in (".jsonl", ".json", ".md", ".txt"):
        candidate = transcripts_dir / f"{session_id}{suffix}"
        if candidate.is_file():
            return candidate
    return None


def analyze_session(
    session: AbSession,
    *,
    transcript_text: str,
    pre_compaction_files: set[str],
    post_compaction_end_turn: int | None,
    window_turns: int = DEFAULT_WINDOW_TURNS,
    workspace_root: Path | None = None,
) -> SessionResult:
    actions = iter_turn_actions(transcript_text)
    window = derive_measurement_window(
        post_compaction_end_turn=post_compaction_end_turn,
        window_turns=window_turns,
    )
    window_actions = [action for action in actions if window.start_turn <= action.turn <= window.end_turn]
    turns_analyzed = len({action.turn for action in window_actions})
    return SessionResult(
        session_id=session.session_id,
        arm=session.arm,
        task_ref=session.task_ref,
        compaction_id=session.compaction_id,
        m1_reorientation_cost=compute_m1(
            actions,
            pre_compaction_files=pre_compaction_files,
            window=window,
            workspace_root=workspace_root,
        ),
        m2_turns_to_first_write=compute_m2(actions, window=window),
        turns_analyzed=turns_analyzed,
        window_start_turn=window.start_turn,
    )


def write_results_jsonl(path: Path, results: Iterable[SessionResult]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in results:
            handle.write(json.dumps(asdict(row), sort_keys=True) + "\n")


def analyze_workspace(
    *,
    workspace_root: Path,
    transcripts_dir: Path,
    output_path: Path,
    state_dir: Path | None = None,
    window_turns: int = DEFAULT_WINDOW_TURNS,
) -> list[SessionResult]:
    resolved_state = state_dir or (workspace_root / ".task-state")
    db_path = resolved_state / "handoff.db"
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        sessions = load_ab_sessions(conn)
        results: list[SessionResult] = []
        for session in sessions:
            transcript_path = _resolve_transcript_path(transcripts_dir, session.session_id)
            if transcript_path is None:
                print(
                    f"measure_reinject_ab: skip session {session.session_id}: "
                    f"no transcript under {transcripts_dir}",
                    file=sys.stderr,
                )
                continue
            end_turn, compaction_created_at, compaction_session_id = _load_compaction_metadata(
                conn, session.compaction_id
            )
            post_end_turn = _resolve_post_compaction_end_turn(
                source=session.source,
                session_id=session.session_id,
                compaction_end_turn=end_turn,
                compaction_session_id=compaction_session_id,
            )
            boundary = compaction_created_at or session.created_at
            pre_files = _load_pre_compaction_files(
                conn,
                task_ref=session.task_ref,
                before_created_at=boundary,
            )
            results.append(
                analyze_session(
                    session,
                    transcript_text=transcript_path.read_text(encoding="utf-8"),
                    pre_compaction_files=pre_files,
                    post_compaction_end_turn=post_end_turn,
                    window_turns=window_turns,
                    workspace_root=workspace_root,
                )
            )
        write_results_jsonl(output_path, results)
        return results
    finally:
        conn.close()


def summarize_by_arm(results: Sequence[SessionResult]) -> dict[str, dict[str, float | int | None]]:
    summary: dict[str, dict[str, float | int | None]] = {}
    for arm in ("treatment", "control"):
        arm_rows = [row for row in results if row.arm == arm]
        m1_values = [row.m1_reorientation_cost for row in arm_rows]
        m2_values = [row.m2_turns_to_first_write for row in arm_rows if row.m2_turns_to_first_write is not None]
        summary[arm] = {
            "sessions": len(arm_rows),
            "m1_median": statistics.median(m1_values) if m1_values else None,
            "m2_median": statistics.median(m2_values) if m2_values else None,
        }
    return summary


def apply_decision_rule(
    summary: dict[str, dict[str, float | int | None]],
    *,
    min_sessions_per_arm: int = DEFAULT_MIN_SESSIONS_PER_ARM,
) -> dict[str, object]:
    treatment = summary.get("treatment") or {}
    control = summary.get("control") or {}
    treat_n = treatment.get("sessions")
    ctrl_n = control.get("sessions")
    treat_m1 = treatment.get("m1_median")
    ctrl_m1 = control.get("m1_median")
    treat_m2 = treatment.get("m2_median")
    ctrl_m2 = control.get("m2_median")

    recommendation = "insufficient_data"
    reason: str | None = None
    m1_delta_pct: float | None = None
    m2_same_sign = False

    if isinstance(treat_n, int) and isinstance(ctrl_n, int):
        if treat_n < min_sessions_per_arm or ctrl_n < min_sessions_per_arm:
            reason = (
                f"below min_sessions_per_arm={min_sessions_per_arm} "
                f"(treatment={treat_n}, control={ctrl_n})"
            )
            return {
                "recommendation": recommendation,
                "reason": reason,
                "m1_median_delta_pct": m1_delta_pct,
                "m2_consistent_sign": m2_same_sign,
                "threshold_m1_reduction": DECISION_M1_REDUCTION,
                "min_sessions_per_arm": min_sessions_per_arm,
                "summary": summary,
            }

    if not isinstance(treat_m1, (int, float)) or not isinstance(ctrl_m1, (int, float)):
        reason = "missing m1 median for one or both arms"
        return {
            "recommendation": recommendation,
            "reason": reason,
            "m1_median_delta_pct": m1_delta_pct,
            "m2_consistent_sign": m2_same_sign,
            "threshold_m1_reduction": DECISION_M1_REDUCTION,
            "min_sessions_per_arm": min_sessions_per_arm,
            "summary": summary,
        }

    if not isinstance(treat_m2, (int, float)) or not isinstance(ctrl_m2, (int, float)):
        reason = "missing m2 median for one or both arms"
        return {
            "recommendation": recommendation,
            "reason": reason,
            "m1_median_delta_pct": m1_delta_pct,
            "m2_consistent_sign": m2_same_sign,
            "threshold_m1_reduction": DECISION_M1_REDUCTION,
            "min_sessions_per_arm": min_sessions_per_arm,
            "summary": summary,
        }

    if float(ctrl_m1) == 0:
        recommendation = "freeze"
        reason = "control m1 median is zero (no reorientation cost baseline)"
        return {
            "recommendation": recommendation,
            "reason": reason,
            "m1_median_delta_pct": 0.0,
            "m2_consistent_sign": True,
            "threshold_m1_reduction": DECISION_M1_REDUCTION,
            "min_sessions_per_arm": min_sessions_per_arm,
            "summary": summary,
        }

    m1_delta_pct = (float(treat_m1) - float(ctrl_m1)) / float(ctrl_m1)
    if float(treat_m2) == float(ctrl_m2):
        m2_same_sign = True
    elif (float(treat_m2) < float(ctrl_m2) and m1_delta_pct < 0) or (
        float(treat_m2) > float(ctrl_m2) and m1_delta_pct > 0
    ):
        m2_same_sign = True

    if m1_delta_pct <= -DECISION_M1_REDUCTION and m2_same_sign:
        recommendation = "invest"
    else:
        recommendation = "freeze"

    return {
        "recommendation": recommendation,
        "reason": reason,
        "m1_median_delta_pct": m1_delta_pct,
        "m2_consistent_sign": m2_same_sign,
        "threshold_m1_reduction": DECISION_M1_REDUCTION,
        "min_sessions_per_arm": min_sessions_per_arm,
        "summary": summary,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace-root", type=Path, default=Path.cwd())
    parser.add_argument("--transcripts-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=Path(".task-state/evals/reinject-ab-results.jsonl"))
    parser.add_argument("--state-dir", type=Path, default=None)
    parser.add_argument("--window-turns", type=int, default=DEFAULT_WINDOW_TURNS)
    parser.add_argument("--print-decision", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    results = analyze_workspace(
        workspace_root=args.workspace_root.resolve(),
        transcripts_dir=args.transcripts_dir.resolve(),
        output_path=args.output.resolve(),
        state_dir=args.state_dir.resolve() if args.state_dir else None,
        window_turns=args.window_turns,
    )
    if args.print_decision:
        print(json.dumps(apply_decision_rule(summarize_by_arm(results)), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())