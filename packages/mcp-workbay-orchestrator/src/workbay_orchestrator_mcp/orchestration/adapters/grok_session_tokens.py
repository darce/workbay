"""Grok session-artifact token reader (internal).

Pure best-effort reader of ``~/.grok/sessions/**`` artifacts. Never raises on
missing/malformed input — callers get a structured unavailable result.

## Encoded-cwd rule (discovery, 2026-07-09)

Live tree under ``~/.grok/sessions/`` uses one directory per absolute cwd,
named by **percent-encoding the full absolute path with an empty safe set**
(every ``/`` becomes ``%2F``), matching ``urllib.parse.quote(abs_cwd, safe="")``:

    cwd  = "/home/<user>/src/<repo>"
    dir  = "%2Fhome%2F<user>%2Fsrc%2F<repo>"

Layout observed:

    ~/.grok/sessions/<encoded-cwd>/<session-id>/
        signals.json      # contextTokensUsed, turnCount (session totals)
        updates.jsonl     # per-event lines; cumulative in params._meta.totalTokens
        events.jsonl      # turn_started / turn_ended boundaries
        …

If the direct encoded-cwd path is missing, fall back to a recursive
``**/<session-id>`` search under the sessions root and confirm via
``params.sessionId`` on an updates line or ``summary.json`` ``info.id``.

## WorkBay-turn mapping (PR-0094-04)

One WorkBay offload turn = one grok CLI invocation. Token delta is:

    turn_delta = (post-call cumulative totalTokens) − (pre-call cumulative)

NOT a sum of per-grok-turn rows. Cumulative is the last flushed
``params._meta.totalTokens`` from ``updates.jsonl``, falling back to
``signals.json`` ``contextTokensUsed``.

``usage_source`` is always ``grok_context_delta``: cumulative **context fill**
(input + output + cached history) — a labeled approximation, never conflated
with observed input/output tokens (PR-0094-05).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from urllib.parse import quote

#: Marker persisted/presented for this approximation unit (PR-0094-05).
USAGE_SOURCE_GROK_CONTEXT_DELTA = "grok_context_delta"

#: Default sessions root; overridable in tests via ``sessions_root=``.
DEFAULT_SESSIONS_ROOT = Path.home() / ".grok" / "sessions"


def encode_cwd_for_session_dir(lane_cwd: str | Path) -> str:
    """Percent-encode an absolute lane cwd the way grok names session dirs.

    Discovery: ``urllib.parse.quote(abs_path, safe="")`` — every ``/`` → ``%2F``.
    Relative paths are resolved first so the encoding is stable.
    """
    abs_cwd = str(Path(lane_cwd).expanduser().resolve())
    return quote(abs_cwd, safe="")


def _sessions_root(sessions_root: Path | None) -> Path:
    return Path(sessions_root) if sessions_root is not None else DEFAULT_SESSIONS_ROOT


def resolve_session_dir(
    session_id: str,
    lane_cwd: str | Path,
    *,
    sessions_root: Path | None = None,
) -> Path | None:
    """Locate ``~/.grok/sessions/<encoded-cwd>/<session-id>/`` if present.

    Best-effort: returns ``None`` when missing or unreadable. Never raises.
    Falls back to a ``**/session_id`` glob under the sessions root when the
    encoded-cwd path is absent (encoding drift / renamed cwd).
    """
    if not session_id or not str(session_id).strip():
        return None
    sid = str(session_id).strip()
    root = _sessions_root(sessions_root)
    try:
        direct = root / encode_cwd_for_session_dir(lane_cwd) / sid
        if direct.is_dir():
            return direct
    except OSError:
        pass

    # Glob fallback: match session-id directory anywhere under sessions root.
    try:
        if not root.is_dir():
            return None
        # Prefer exact ``*/<sid>`` then recursive.
        for candidate in root.glob(f"*/{sid}"):
            if candidate.is_dir() and _session_dir_matches(candidate, sid):
                return candidate
        for candidate in root.glob(f"**/{sid}"):
            if candidate.is_dir() and _session_dir_matches(candidate, sid):
                return candidate
    except OSError:
        return None
    return None


def _session_dir_matches(session_dir: Path, session_id: str) -> bool:
    """Confirm a candidate dir is the session (name match + optional signals)."""
    if session_dir.name != session_id:
        return False
    # Prefer positive confirmation when summary/signals/updates are present.
    summary = session_dir / "summary.json"
    if summary.is_file():
        data = _load_json_object(summary)
        if isinstance(data, dict):
            info = data.get("info")
            if isinstance(info, dict) and info.get("id") == session_id:
                return True
    # updates.jsonl sessionId is authoritative when present.
    first_sid = _first_updates_session_id(session_dir / "updates.jsonl")
    if first_sid is not None:
        return first_sid == session_id
    # Directory name match alone is acceptable when artifacts are not yet flushed.
    return True


def read_cumulative_total(
    session_id: str,
    lane_cwd: str | Path,
    *,
    sessions_root: Path | None = None,
) -> int | None:
    """Best-effort read of the session's current cumulative context totalTokens.

    Prefers the last flushed ``params._meta.totalTokens`` from ``updates.jsonl``;
    falls back to ``signals.json`` ``contextTokensUsed``. Returns ``None`` when
    unavailable (missing dir/files/not-yet-flushed). Never raises.
    """
    session_dir = resolve_session_dir(session_id, lane_cwd, sessions_root=sessions_root)
    if session_dir is None:
        return None
    return _read_cumulative_from_dir(session_dir)


def read_session_token_deltas(
    session_id: str,
    lane_cwd: str | Path,
    *,
    pre_total: int | None = None,
    pre_session_id: str | None = None,
    sessions_root: Path | None = None,
) -> dict[str, Any]:
    """Return per-WorkBay-turn context-token delta + session totals.

    Args:
        session_id: Grok envelope ``sessionId`` (or known session uuid) observed
            AFTER the CLI call.
        lane_cwd: Lane **worktree** cwd that keys the session directory.
        pre_total: Cumulative total snapshotted before the adapter CLI call.
            Pass ``0`` when **no prior session existed** (fresh call — the whole
            session total legitimately IS this turn). Pass ``None`` when a
            pre-snapshot was attempted but **failed/unavailable**: the reader
            then refuses to claim an exact delta (``turn_delta=None``,
            ``baseline="pre_snapshot_unavailable"``) instead of attributing the
            whole resumed-session cumulative to one turn (REV-S1-02).
        pre_session_id: Session id the ``pre_total`` snapshot was taken from,
            when one existed. If it differs from ``session_id`` the session was
            restarted between snapshot and post-call: the stale ``pre_total`` is
            discarded and the new session's full total is the turn delta,
            marked ``baseline="session_changed"`` (REV-S1-01).
        sessions_root: Override for tests; default ``~/.grok/sessions``.

    Returns a dict that always includes ``usage_source`` and never raises:

    Available::

        {
          "available": True,
          "turn_delta": int | None,  # None when baseline unavailable
          "session_total": int,
          "turn_count": int | None,
          "usage_source": "grok_context_delta",
          "baseline": "pre_snapshot" | "session_changed" | "pre_snapshot_unavailable",
          "reason": None,
        }

    Unavailable::

        {
          "available": False,
          "turn_delta": None,
          "session_total": None,
          "turn_count": None,
          "usage_source": "grok_context_delta",
          "baseline": None,
          "reason": "unavailable" | "unavailable (pending)" | str,
        }
    """
    base: dict[str, Any] = {
        "available": False,
        "turn_delta": None,
        "session_total": None,
        "turn_count": None,
        "usage_source": USAGE_SOURCE_GROK_CONTEXT_DELTA,
        "baseline": None,
        "reason": "unavailable",
    }
    if not session_id or not str(session_id).strip():
        base["reason"] = "unavailable"
        return base

    try:
        session_dir = resolve_session_dir(session_id, lane_cwd, sessions_root=sessions_root)
        if session_dir is None:
            base["reason"] = "unavailable"
            return base

        turn_count = _read_turn_count(session_dir)
        session_total = _read_cumulative_from_dir(session_dir)
        if session_total is None:
            # Dir exists but tokens not flushed yet.
            base["reason"] = "unavailable (pending)"
            base["turn_count"] = turn_count
            return base

        available: dict[str, Any] = {
            "available": True,
            "turn_delta": None,
            "session_total": session_total,
            "turn_count": turn_count,
            "usage_source": USAGE_SOURCE_GROK_CONTEXT_DELTA,
            "baseline": None,
            "reason": None,
        }

        # REV-S1-01: a pre_total snapshotted from a DIFFERENT session must not be
        # subtracted from (or clamp) this session's total — a restart means the
        # new session's cumulative is entirely this WorkBay turn.
        if pre_session_id is not None and pre_session_id != str(session_id).strip():
            available["turn_delta"] = session_total
            available["baseline"] = "session_changed"
            return available

        # REV-S1-02: pre-snapshot attempted but unavailable — the exact delta is
        # unknowable; do NOT claim one.
        if pre_total is None:
            available["baseline"] = "pre_snapshot_unavailable"
            return available

        turn_delta = session_total - int(pre_total)
        if turn_delta < 0:
            # Compaction or rewind can drop cumulative; surface 0 rather than negative.
            turn_delta = 0
        available["turn_delta"] = turn_delta
        available["baseline"] = "pre_snapshot"
        return available
    except Exception:  # noqa: BLE001 — best-effort contract: never raise
        base["reason"] = "unavailable"
        return base


def _read_cumulative_from_dir(session_dir: Path) -> int | None:
    """Last updates.jsonl totalTokens, else signals.contextTokensUsed."""
    from_updates = _last_updates_total_tokens(session_dir / "updates.jsonl")
    if from_updates is not None:
        return from_updates
    signals = _load_json_object(session_dir / "signals.json")
    if isinstance(signals, dict):
        ctx = signals.get("contextTokensUsed")
        if isinstance(ctx, (int, float)) and not isinstance(ctx, bool):
            return int(ctx)
    return None


def _read_turn_count(session_dir: Path) -> int | None:
    signals = _load_json_object(session_dir / "signals.json")
    if isinstance(signals, dict):
        tc = signals.get("turnCount")
        if isinstance(tc, (int, float)) and not isinstance(tc, bool):
            return int(tc)
    # Count turn_started events as a soft fallback.
    events = session_dir / "events.jsonl"
    if not events.is_file():
        return None
    count = 0
    try:
        with events.open(encoding="utf-8", errors="replace") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except (json.JSONDecodeError, TypeError, ValueError):
                    continue
                if isinstance(obj, dict) and obj.get("type") == "turn_started":
                    count += 1
    except OSError:
        return None
    return count if count > 0 else None


def _last_updates_total_tokens(updates_path: Path) -> int | None:
    """Scan updates.jsonl for the last numeric params._meta.totalTokens.

    Malformed lines are skipped. Missing file → None (caller may treat as pending).
    """
    if not updates_path.is_file():
        return None
    last: int | None = None
    try:
        with updates_path.open(encoding="utf-8", errors="replace") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except (json.JSONDecodeError, TypeError, ValueError):
                    continue
                if not isinstance(obj, dict):
                    continue
                total = _extract_total_tokens(obj)
                if total is not None:
                    last = total
    except OSError:
        return None
    return last


def _extract_total_tokens(obj: dict[str, Any]) -> int | None:
    """Pull totalTokens from real grok update shapes.

    Observed (2026-07-09): ``params._meta.totalTokens``. Also accept top-level
    ``_meta.totalTokens`` for forward-compat if nesting drifts.
    """
    for meta in (
        (obj.get("params") or {}).get("_meta") if isinstance(obj.get("params"), dict) else None,
        obj.get("_meta"),
    ):
        if not isinstance(meta, dict):
            continue
        raw = meta.get("totalTokens")
        if isinstance(raw, (int, float)) and not isinstance(raw, bool):
            return int(raw)
    return None


def _first_updates_session_id(updates_path: Path) -> str | None:
    if not updates_path.is_file():
        return None
    try:
        with updates_path.open(encoding="utf-8", errors="replace") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except (json.JSONDecodeError, TypeError, ValueError):
                    continue
                if not isinstance(obj, dict):
                    continue
                params = obj.get("params")
                if isinstance(params, dict):
                    sid = params.get("sessionId")
                    if isinstance(sid, str) and sid:
                        return sid
    except OSError:
        return None
    return None


def _load_json_object(path: Path) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    try:
        raw = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None
    if not raw.strip():
        return None
    try:
        obj = json.loads(raw)
    except (json.JSONDecodeError, TypeError, ValueError):
        return None
    return obj if isinstance(obj, dict) else None
