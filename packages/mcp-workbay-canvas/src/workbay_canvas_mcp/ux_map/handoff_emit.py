"""Optional adapter: UX map critique findings → handoff review_findings.

Pure conversion is always available. The default writer calls
``workbay_handoff_mcp.review_findings`` (batch_record). Callers inject a
writer for tests; emit is opt-in when ``task_ref`` is provided.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Callable, Mapping, Sequence
from typing import Any

from workbay_canvas_mcp.ux_map.critique import CritiqueFinding

HandoffFindingWriter = Callable[[Mapping[str, Any]], Any]


def _slug(part: str, *, max_len: int = 40) -> str:
    s = re.sub(r"[^A-Za-z0-9_-]+", "-", part).strip("-").lower()
    return (s or "x")[:max_len]


def handoff_finding_id(map_ref: str, finding: CritiqueFinding) -> str:
    """Stable finding_id for a critique row (idempotent re-emit)."""
    digest = hashlib.sha1(f"{map_ref}|{finding.rule_id}|{finding.pointer}|{finding.fix}".encode()).hexdigest()[:10]
    return f"uxmap-{_slug(map_ref)}-{_slug(finding.rule_id, max_len=20)}-{digest}"


def handoff_file_path_for_map(
    map_ref: str,
    *,
    map_path: str | None = None,
    workspace_root: str | None = None,
) -> str:
    """Stable, portable ``file_path`` for handoff rows (never absolute).

    Prefer ``docs/ux-maps/{map_ref}.uxmap.json``. When ``map_path`` is under
    ``workspace_root``, emit that relative path; otherwise fall back to the
    map_ref convention so durable findings resolve across worktrees.
    """
    default = f"docs/ux-maps/{map_ref}.uxmap.json"
    if not map_path:
        return default
    from pathlib import Path

    p = Path(map_path)
    if workspace_root is not None:
        try:
            return str(p.resolve().relative_to(Path(workspace_root).resolve()))
        except ValueError:
            pass
    # Absolute or foreign path → map_ref convention only (portable)
    if p.is_absolute():
        return default
    return str(p).replace("\\", "/")


def critique_to_handoff_rows(
    findings: Sequence[CritiqueFinding],
    *,
    map_ref: str,
    map_path: str | None = None,
    workspace_root: str | None = None,
) -> list[dict[str, Any]]:
    """Convert critique findings into ``batch_record`` finding row dicts."""
    file_path = handoff_file_path_for_map(map_ref, map_path=map_path, workspace_root=workspace_root)
    rows: list[dict[str, Any]] = []
    for f in findings:
        # Human-facing description cites lexicon form [ID]; wire rule_id is bare
        bare = f.rule_id.strip("[]")
        desc = f"[{bare}] {f.pointer}: {f.fix}"
        if f.detail:
            desc = f"{desc} ({f.detail})"
        rows.append(
            {
                "finding_id": handoff_finding_id(map_ref, f),
                "severity": f.severity,
                "file_path": file_path,
                "description": desc,
                "review_mode": "branch",
                "details": {
                    "fix": f.fix,
                },
            }
        )
    return rows


def default_handoff_finding_writer(payload: Mapping[str, Any]) -> Any:
    """Write via public handoff package API (``review_findings`` batch_record)."""
    from workbay_handoff_mcp import review_findings

    return review_findings(review=dict(payload))


def emit_critique_to_handoff(
    findings: Sequence[CritiqueFinding],
    *,
    task_ref: str,
    session: str,
    map_ref: str,
    map_path: str | None = None,
    workspace_root: str | None = None,
    writer: HandoffFindingWriter | None = None,
) -> dict[str, Any]:
    """Emit critique findings to handoff under ``task_ref``.

    Returns a receipt ``{ok, written, finding_ids, result?}``. Empty findings
    short-circuit with ``written=0`` and do not call the writer.
    """
    rows = critique_to_handoff_rows(
        findings,
        map_ref=map_ref,
        map_path=map_path,
        workspace_root=workspace_root,
    )
    if not rows:
        return {
            "ok": True,
            "written": 0,
            "finding_ids": [],
            "skipped": "no_findings",
        }
    payload = {
        "operation": "batch_record",
        "session": session,
        "task_ref": task_ref,
        "findings": rows,
    }
    write = writer if writer is not None else default_handoff_finding_writer
    result = write(payload)
    written = len(rows)
    # Prefer structured envelope counts when present
    if isinstance(result, Mapping):
        data = result.get("data") if "data" in result else result
        if isinstance(data, Mapping) and "written" in data:
            written = int(data["written"])  # type: ignore[arg-type]
        ok = bool(result.get("ok", True)) if "ok" in result else True
    else:
        ok = True
    return {
        "ok": ok,
        "written": written,
        "finding_ids": [r["finding_id"] for r in rows],
        "result": result,
    }
