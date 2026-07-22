"""One-shot conservative backfill of estimated output token splits (implementation note S2).

Historical usage-less backend rows carry ``usage_source=grok_context_delta``
totals but NULL ``input_tokens`` / ``output_tokens`` and no session pointer.
This tool matches heuristically and conservatively:

1. Resolve the row's ``lane_id`` → lane worktree cwd (``worktree_lanes``).
2. Percent-encode the cwd via ``encode_cwd_for_session_dir`` to list candidate
   session dirs under the sessions root.
3. Require **exact equality** between the row's ``total_tokens``
   (``grok_context_delta``) and a candidate session's cumulative total.
4. Zero or multiple matches → skip and count (never guess).
5. Unique match → estimate output from that session's ``updates.jsonl`` (same
   extraction as record-time) and UPDATE ``output_tokens`` + estimate
   provenance in ``raw_usage_json``.

Idempotent: rows whose ``output_tokens`` is already non-NULL are skipped.
Default is dry-run; mutations require ``--apply``. Explicit ``--db`` is
required. Do not invent ``input_tokens``.

Invoke::

    python -m workbay_orchestrator_mcp.tools.backfill_grok_token_splits \\
        --db /path/to/handoff.db [--apply] [--sessions-root PATH]
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from workbay_orchestrator_mcp.orchestration.adapters.grok_session_tokens import (
    DEFAULT_SESSIONS_ROOT,
    encode_cwd_for_session_dir,
    read_cumulative_total,
)
from workbay_orchestrator_mcp.orchestration.token_estimate import (
    estimate_output_tokens_from_session_dir,
)

_USAGE_SOURCE = "grok_context_delta"


@dataclass
class BackfillCounts:
    """Coverage counters printed at the end of a run."""

    updated: int = 0
    skipped_no_match: int = 0
    skipped_ambiguous: int = 0
    skipped_ambiguous_lane: int = 0
    skipped_already_split: int = 0
    skipped_no_lane: int = 0
    total: int = 0

    def as_dict(self) -> dict[str, int]:
        return {
            "updated": self.updated,
            "skipped_no_match": self.skipped_no_match,
            "skipped_ambiguous": self.skipped_ambiguous,
            "skipped_ambiguous_lane": self.skipped_ambiguous_lane,
            "skipped_already_split": self.skipped_already_split,
            "skipped_no_lane": self.skipped_no_lane,
            "total": self.total,
        }


@dataclass
class BackfillResult:
    counts: BackfillCounts = field(default_factory=BackfillCounts)
    applied: bool = False


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="workbay_orchestrator_mcp.tools.backfill_grok_token_splits",
        description=(
            "Backfill estimated output_tokens for historical grok_context_delta "
            "turn_metrics rows (exact session-total match; dry-run by default)."
        ),
    )
    parser.add_argument(
        "--db",
        required=True,
        type=Path,
        help="Path to the handoff SQLite database (required).",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Persist UPDATEs. Without this flag the run is dry-run only.",
    )
    parser.add_argument(
        "--sessions-root",
        type=Path,
        default=None,
        help="Override sessions root (default: ~/.grok/sessions). Tests only.",
    )
    return parser


def _lane_worktree_paths(conn: sqlite3.Connection) -> tuple[dict[str, str], set[str]]:
    """Map lane_id → worktree_path plus the set of *ambiguous* lane_ids.

    ``worktree_lanes`` is ``UNIQUE(task_ref, lane_id)``, so the same
    ``lane_id`` can appear across multiple tasks pointing at different
    worktrees. A last-write-wins collapse would silently attribute one task's
    session dir to another task's row, so a lane_id that resolves to more than
    one distinct ``worktree_path`` is reported in the ambiguous set instead of
    an arbitrary mapping; the caller skips those rows rather than guess
    (DATA-13).
    """
    seen: dict[str, set[str]] = {}
    try:
        rows = conn.execute(
            """
            SELECT lane_id, worktree_path
            FROM worktree_lanes
            WHERE lane_id IS NOT NULL AND worktree_path IS NOT NULL
            ORDER BY updated_at ASC, id ASC
            """
        ).fetchall()
    except sqlite3.Error:
        return {}, set()
    for lane_id, worktree_path in rows:
        if isinstance(lane_id, str) and lane_id.strip() and isinstance(worktree_path, str) and worktree_path.strip():
            seen.setdefault(lane_id.strip(), set()).add(worktree_path.strip())
    mapping: dict[str, str] = {}
    ambiguous: set[str] = set()
    for lane_id, paths in seen.items():
        if len(paths) == 1:
            mapping[lane_id] = next(iter(paths))
        else:
            ambiguous.add(lane_id)
    return mapping, ambiguous


def _persisted_session_id(raw_usage_json: str | None) -> str | None:
    """Return an explicitly-persisted ``session_id`` from ``raw_usage_json``.

    When a historical row already recorded which grok session produced it, that
    exact pointer is authoritative and must be preferred over heuristic
    total-matching (which can be ambiguous). Returns ``None`` when absent or
    unparseable.
    """
    if not isinstance(raw_usage_json, str) or not raw_usage_json.strip():
        return None
    try:
        parsed = json.loads(raw_usage_json)
    except (json.JSONDecodeError, TypeError, ValueError):
        return None
    if not isinstance(parsed, dict):
        return None
    session_id = parsed.get("session_id")
    if isinstance(session_id, str) and session_id.strip():
        return session_id.strip()
    return None


def _session_dir_for_id(
    *,
    worktree_path: str,
    session_id: str,
    sessions_root: Path,
) -> Path | None:
    """Resolve the on-disk session dir for an exact ``session_id`` join."""
    try:
        encoded = encode_cwd_for_session_dir(worktree_path)
    except OSError:
        return None
    session_dir = sessions_root / encoded / session_id
    return session_dir if session_dir.is_dir() else None


def _candidate_session_dirs(
    *,
    worktree_path: str,
    sessions_root: Path,
) -> list[Path]:
    """List session dirs under the encoded-cwd key for *worktree_path*."""
    try:
        encoded = encode_cwd_for_session_dir(worktree_path)
    except OSError:
        return []
    cwd_root = sessions_root / encoded
    if not cwd_root.is_dir():
        return []
    try:
        return sorted(p for p in cwd_root.iterdir() if p.is_dir())
    except OSError:
        return []


def _sessions_matching_total(
    *,
    worktree_path: str,
    target_total: int,
    sessions_root: Path,
) -> list[Path]:
    """Return session dirs whose cumulative total equals *target_total* exactly."""
    matches: list[Path] = []
    for session_dir in _candidate_session_dirs(
        worktree_path=worktree_path,
        sessions_root=sessions_root,
    ):
        session_id = session_dir.name
        cumulative = read_cumulative_total(
            session_id,
            worktree_path,
            sessions_root=sessions_root,
        )
        if cumulative is not None and int(cumulative) == int(target_total):
            matches.append(session_dir)
    return matches


def _merge_raw_usage(
    raw_usage_json: str | None,
    *,
    session_id: str,
    output_token_source: str,
    estimated_output_tokens: int,
) -> str:
    raw: dict[str, Any]
    if isinstance(raw_usage_json, str) and raw_usage_json.strip():
        try:
            parsed = json.loads(raw_usage_json)
            raw = dict(parsed) if isinstance(parsed, dict) else {}
        except (json.JSONDecodeError, TypeError, ValueError):
            raw = {}
    else:
        raw = {}
    raw["session_id"] = session_id
    raw["output_token_source"] = output_token_source
    raw["estimated_output_tokens"] = estimated_output_tokens
    return json.dumps(raw, sort_keys=True)


def run_backfill(
    *,
    db_path: Path | str,
    apply: bool = False,
    sessions_root: Path | str | None = None,
) -> BackfillResult:
    """Run the conservative backfill. Default dry-run (``apply=False``)."""
    db = Path(db_path)
    if not db.is_file():
        raise FileNotFoundError(f"database not found: {db}")
    root = Path(sessions_root) if sessions_root is not None else DEFAULT_SESSIONS_ROOT
    result = BackfillResult(applied=apply)
    counts = result.counts

    # Immutable when dry-run: open read-only via URI when possible.
    if apply:
        conn = sqlite3.connect(str(db))
    else:
        conn = sqlite3.connect(f"file:{db.resolve()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        lane_paths, ambiguous_lanes = _lane_worktree_paths(conn)
        rows = conn.execute(
            """
            SELECT id, lane_id, total_tokens, output_tokens, raw_usage_json, usage_source
            FROM turn_metrics
            WHERE usage_source = ?
              AND total_tokens IS NOT NULL
            ORDER BY id ASC
            """,
            (_USAGE_SOURCE,),
        ).fetchall()

        # Pass 1: resolve each eligible row to a session dir (exact join when a
        # session_id was persisted, else a unique heuristic total-match). We
        # defer the estimate + UPDATE to pass 2 so a session→row assignment can
        # be enforced 1:1 (injective); two rows that heuristically claim the
        # same session dir must both be skipped, not both estimated identically.
        candidates: list[tuple[sqlite3.Row, Path, bool]] = []  # (row, session_dir, is_exact)
        for row in rows:
            counts.total += 1
            if row["output_tokens"] is not None:
                counts.skipped_already_split += 1
                continue
            lane_id = row["lane_id"]
            if not isinstance(lane_id, str) or not lane_id.strip():
                counts.skipped_no_lane += 1
                continue
            lane_key = lane_id.strip()
            if lane_key in ambiguous_lanes:
                # lane_id maps to >1 distinct worktree_path across tasks.
                counts.skipped_ambiguous_lane += 1
                continue
            worktree_path = lane_paths.get(lane_key)
            if not worktree_path:
                counts.skipped_no_lane += 1
                continue

            persisted_id = _persisted_session_id(row["raw_usage_json"])
            if persisted_id is not None:
                exact_dir = _session_dir_for_id(
                    worktree_path=worktree_path,
                    session_id=persisted_id,
                    sessions_root=root,
                )
                if exact_dir is not None:
                    candidates.append((row, exact_dir, True))
                    continue
                # Persisted pointer no longer resolves on disk — fall back to
                # heuristic total-matching below rather than guessing.

            try:
                target_total = int(row["total_tokens"])
            except (TypeError, ValueError):
                counts.skipped_no_match += 1
                continue

            matches = _sessions_matching_total(
                worktree_path=worktree_path,
                target_total=target_total,
                sessions_root=root,
            )
            if not matches:
                counts.skipped_no_match += 1
                continue
            if len(matches) > 1:
                counts.skipped_ambiguous += 1
                continue
            candidates.append((row, matches[0], False))

        # Injectivity: a heuristic-matched session dir claimed by more than one
        # row is ambiguous (we cannot tell which row it belongs to). Exact joins
        # from a persisted session_id are authoritative and exempt.
        heuristic_claims: dict[Path, int] = {}
        for _row, session_dir, is_exact in candidates:
            if not is_exact:
                heuristic_claims[session_dir] = heuristic_claims.get(session_dir, 0) + 1

        # Pass 2: estimate + UPDATE the survivors.
        for row, session_dir, is_exact in candidates:
            if not is_exact and heuristic_claims.get(session_dir, 0) > 1:
                counts.skipped_ambiguous += 1
                continue
            output_tokens, provenance = estimate_output_tokens_from_session_dir(session_dir)
            if output_tokens is None or provenance is None:
                counts.skipped_no_match += 1
                continue

            counts.updated += 1
            if not apply:
                continue
            new_raw = _merge_raw_usage(
                row["raw_usage_json"],
                session_id=session_dir.name,
                output_token_source=provenance,
                estimated_output_tokens=int(output_tokens),
            )
            conn.execute(
                """
                UPDATE turn_metrics
                SET output_tokens = ?, raw_usage_json = ?
                WHERE id = ?
                  AND output_tokens IS NULL
                """,
                (int(output_tokens), new_raw, row["id"]),
            )

        if apply:
            conn.commit()
    finally:
        conn.close()
    return result


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        result = run_backfill(
            db_path=args.db,
            apply=bool(args.apply),
            sessions_root=args.sessions_root,
        )
    except FileNotFoundError as exc:
        print(f"backfill_grok_token_splits: {exc}", file=sys.stderr)
        return 2
    except sqlite3.Error as exc:
        print(f"backfill_grok_token_splits: database error: {exc}", file=sys.stderr)
        return 2

    counts = result.counts
    mode = "apply" if result.applied else "dry-run"
    # Coverage line required by implementation note S2.
    print(
        f"backfill_grok_token_splits ({mode}): "
        f"updated={counts.updated} "
        f"skipped_no_match={counts.skipped_no_match} "
        f"skipped_ambiguous={counts.skipped_ambiguous} "
        f"skipped_ambiguous_lane={counts.skipped_ambiguous_lane} "
        f"total={counts.total}"
    )
    if counts.skipped_already_split or counts.skipped_no_lane:
        print(
            f"  (also skipped_already_split={counts.skipped_already_split} "
            f"skipped_no_lane={counts.skipped_no_lane})",
            file=sys.stderr,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
