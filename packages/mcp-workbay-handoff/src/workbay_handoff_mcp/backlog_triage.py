"""Open-findings backlog classifier + apply (implementation note Slices 1–2).

implementation note — classify buckets:

- ``rebrand_orphaned`` — path is covered by the rename map and neither the old
  nor the mapped path resolves to a live file (safe bulk-disposition candidate).
- ``remappable`` — path maps to a *live* file via the rename map (open-preserving
  re-anchor candidate; **never** treat as rebrand_orphaned — false-close guard).
- ``live`` — not rename-map covered (or no map supplied); leave alone.
- ``high_needs_human`` — high severity; never bulk-dispositioned.

implementation note — ``apply_reviewed_manifest`` executes a reviewed manifest via sanctioned
``disposition`` / ``reanchor`` ops: batched, idempotent, concurrency-skip.

The done/archived-orphan axis is **reused** from
``review_findings_queries._collect_stale_nonscratch_open_finding_items`` and
joined onto each classified row — this module does not reimplement that query.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, cast

from .enums import FindingSeverity, FindingStatus
from .review_findings_api import review_findings
from .review_findings_queries import _collect_stale_nonscratch_open_finding_items

BUCKET_REBRAND_ORPHANED = "rebrand_orphaned"
BUCKET_REMAPPABLE = "remappable"
BUCKET_LIVE = "live"
BUCKET_HIGH_NEEDS_HUMAN = "high_needs_human"

ALL_BUCKETS = (
    BUCKET_REBRAND_ORPHANED,
    BUCKET_REMAPPABLE,
    BUCKET_LIVE,
    BUCKET_HIGH_NEEDS_HUMAN,
)

ACTION_REANCHOR = "reanchor"
ACTION_DISPOSITION = "disposition"
ACTION_SKIP = "skip"

DEFAULT_APPLY_BATCH_SIZE = 200
DEFAULT_DISPOSITION_STATUS = "wontfix"
PLAN_ID = "0097"


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _normalize_rename_map(rename_map: Mapping[str, str] | None) -> dict[str, str]:
    if not rename_map:
        return {}
    out: dict[str, str] = {}
    for raw_old, raw_new in rename_map.items():
        old = str(raw_old or "").strip().replace("\\", "/")
        new = str(raw_new or "").strip().replace("\\", "/")
        if not old or not new:
            continue
        out[old] = new
    return out


def _map_path(file_path: str, rename_map: dict[str, str]) -> str | None:
    """Return mapped path for *file_path*, or None if not covered by the map.

    Supports exact keys and longest directory/prefix keys, e.g. a
    ``workstate-x`` → ``workbay-x`` prefix key (brand-check: allow, implementation note).
    """
    normalized = (file_path or "").strip().replace("\\", "/")
    if not normalized or not rename_map:
        return None
    if normalized in rename_map:
        return rename_map[normalized]

    best_old: str | None = None
    best_new: str | None = None
    best_len = -1
    for old, new in rename_map.items():
        old_prefix = old.rstrip("/")
        if not old_prefix:
            continue
        if normalized == old_prefix or normalized.startswith(old_prefix + "/"):
            if len(old_prefix) > best_len:
                best_len = len(old_prefix)
                best_old = old_prefix
                best_new = new.rstrip("/")
    if best_old is None or best_new is None:
        return None
    suffix = normalized[len(best_old) :]  # includes leading '/' when present
    return f"{best_new}{suffix}" if suffix else best_new


def _default_path_is_live(rel_path: str, *, workspace_root: Path) -> bool:
    rel = (rel_path or "").strip().replace("\\", "/")
    if not rel or rel.startswith("/"):
        return False
    root = workspace_root.resolve()
    candidate = (root / rel).resolve()
    try:
        candidate.relative_to(root)
    except ValueError:
        return False
    return candidate.is_file()


def _default_path_escapes_root(rel_path: str, *, workspace_root: Path) -> bool:
    """Fail-safe signal: True when *rel_path* cannot be *confirmed* to live
    under ``workspace_root`` — absolute, unresolvable, or a symlink whose real
    target escapes the root (e.g. this repo's ``docs/**/contracts`` mirrors).

    Such a path has *unknown* liveness, distinct from a resolved-but-missing
    file (genuinely dead). ``_default_path_is_live`` returns False for BOTH, so
    the classifier must consult this predicate before ever routing a
    rename-map-covered path into the auto-close ``rebrand_orphaned`` bucket —
    the headline "renamed-but-LIVE never rebrand_orphaned" guarantee.
    """
    rel = (rel_path or "").strip().replace("\\", "/")
    if not rel:
        return False
    if rel.startswith("/"):
        return True
    try:
        root = workspace_root.resolve()
        candidate = (root / rel).resolve()
        candidate.relative_to(root)
    except (ValueError, OSError):
        return True
    return False


def _ascend_to_repo_root(start: Path) -> Path:
    """Pin the liveness base to the enclosing repo root.

    Returns the nearest ancestor (including *start*) that contains a ``.git``
    entry — a directory in a primary checkout, a file in a linked worktree —
    else *start* unchanged. Prevents a worktree/subdir cwd from shifting the
    liveness base so that genuinely live files read as dead (and could be
    mis-bucketed rebrand_orphaned). Non-repo temp roots have no ``.git``
    ancestor and are returned as-is, so callers passing an explicit test root
    are unaffected.
    """
    try:
        base = start.resolve()
    except OSError:
        return start
    for candidate in (base, *base.parents):
        try:
            if (candidate / ".git").exists():
                return candidate
        except OSError:
            continue
    return base


def _resolve_workspace_root(workspace_root: Path | None) -> Path:
    if workspace_root is not None:
        return _ascend_to_repo_root(Path(workspace_root))
    try:
        from .runtime import get_runtime_config

        return _ascend_to_repo_root(get_runtime_config().workspace_root)
    except Exception:
        return _ascend_to_repo_root(Path.cwd())


def classify_open_findings(
    conn: sqlite3.Connection,
    rename_map: Mapping[str, str] | None,
    *,
    workspace_root: Path | None = None,
    path_is_live: Callable[[str], bool] | None = None,
) -> dict[str, Any]:
    """Classify every open finding into triage buckets. Pure read; no writes.

    Parameters
    ----------
    conn:
        Open handoff DB connection (read-only usage).
    rename_map:
        Old-path → new-path map (implementation note). Empty/None degrades safely: no
        finding is classified as rebrand_orphaned or remappable.
    workspace_root:
        Root for filesystem live-path checks. Defaults to runtime config
        workspace, then cwd.
    path_is_live:
        Optional override for the live-file predicate (tests inject this).
    """
    normalized_map = _normalize_rename_map(rename_map)
    root = _resolve_workspace_root(workspace_root)
    is_live = path_is_live or (lambda p: _default_path_is_live(p, workspace_root=root))

    # Done/archived-orphan axis — reuse existing collector (do not reimplement).
    stale_items = _collect_stale_nonscratch_open_finding_items(conn, batch_size=None)
    stale_by_db_id: dict[int, dict[str, object]] = {int(cast(int, item["finding_db_id"])): item for item in stale_items}

    rows = conn.execute(
        """
        SELECT rf.id, rf.task_ref, rf.finding_id, rf.severity, rf.file_path, rf.description
        FROM review_findings rf
        WHERE rf.status = ?
        ORDER BY rf.task_ref, rf.id
        """,
        (FindingStatus.OPEN.value,),
    ).fetchall()

    buckets: dict[str, list[dict[str, Any]]] = {name: [] for name in ALL_BUCKETS}

    for row in rows:
        finding_db_id = int(row["id"])
        task_ref = str(row["task_ref"])
        finding_id = str(row["finding_id"])
        severity = str(row["severity"])
        file_path = str(row["file_path"] or "")
        description = str(row["description"] or "")

        stale = stale_by_db_id.get(finding_db_id)
        mapped_path = _map_path(file_path, normalized_map)

        old_live = bool(file_path) and is_live(file_path)
        mapped_live = is_live(mapped_path) if mapped_path else False

        # Fail-safe liveness signal: a path whose liveness cannot be confirmed
        # under the workspace root (absolute, unresolvable, or symlink-escaping
        # — e.g. the repo's docs/**/contracts mirrors, or a wrong cwd) is
        # UNKNOWN, not dead. It must never fall into the auto-close
        # rebrand_orphaned bucket. Uses the real filesystem regardless of any
        # injected ``path_is_live`` predicate (escape is an FS-safety axis).
        liveness_unknown = (bool(file_path) and _default_path_escapes_root(file_path, workspace_root=root)) or (
            _default_path_escapes_root(mapped_path, workspace_root=root) if mapped_path else False
        )

        if severity == FindingSeverity.HIGH.value:
            bucket = BUCKET_HIGH_NEEDS_HUMAN
            if mapped_path and mapped_live:
                rationale = (
                    "high severity with live mapped path; requires explicit "
                    "human re-anchor-or-disposition decision (no bulk write)"
                )
            elif mapped_path and not (old_live or mapped_live):
                rationale = (
                    "high severity on rebrand-orphaned path; requires explicit human disposition (no bulk write)"
                )
            else:
                rationale = "high severity open finding; requires human triage"
        elif mapped_path is not None and mapped_live:
            # False-close guard: renamed-but-live must never land in rebrand_orphaned.
            # The re-anchor target MUST be live — bucketing remappable on old_live
            # alone (pre-rename path on disk, mapped target dead) would let Slice-2
            # re-anchor rewrite file_path to a DEAD target (BR-S1-01). Only claim
            # "maps to live file" when the mapped target is actually live.
            bucket = BUCKET_REMAPPABLE
            rationale = (
                f"path maps to live file via rename_map "
                f"({file_path!r} → {mapped_path!r}); open-preserving re-anchor candidate"
            )
        elif mapped_path is not None and old_live:
            # Pre-rename/old path still on disk but the mapped re-anchor target is
            # dead. NOT remappable (re-anchor target is dead) and NOT
            # rebrand_orphaned (a file still exists on disk, so it is not orphaned
            # — auto-close would false-close it). Leave live for explicit human
            # handling (BR-S1-01).
            bucket = BUCKET_LIVE
            rationale = (
                f"old path still on disk but mapped re-anchor target is dead "
                f"({file_path!r} → {mapped_path!r}); not remappable (dead target) and "
                f"not rebrand_orphaned (a file exists) — leave live for human handling"
            )
        elif mapped_path is not None and liveness_unknown:
            # Fail-safe: rename-map-covered but liveness unconfirmable
            # (out-of-root / symlink-escape / unresolvable). Never auto-close;
            # leave live for explicit human handling — a genuinely LIVE renamed
            # file behind a symlink mirror must not be treated as an orphan.
            bucket = BUCKET_LIVE
            rationale = (
                f"path covered by rename_map but liveness is unconfirmable "
                f"(out-of-root/symlink-escape/unresolvable: {file_path!r} → {mapped_path!r}); "
                f"fail-safe to live, never rebrand_orphaned"
            )
        elif mapped_path is not None:
            bucket = BUCKET_REBRAND_ORPHANED
            rationale = (
                f"path covered by rename_map but neither old nor mapped path is live ({file_path!r} → {mapped_path!r})"
            )
        else:
            bucket = BUCKET_LIVE
            if not normalized_map:
                rationale = "no rename_map coverage (empty or absent map); leave live"
            else:
                rationale = "file_path not covered by rename_map; leave live"

        entry: dict[str, Any] = {
            "task_ref": task_ref,
            "finding_id": finding_id,
            "finding_db_id": finding_db_id,
            "severity": severity,
            "file_path": file_path,
            "description": description,
            "mapped_path": mapped_path,
            "bucket": bucket,
            "rationale": rationale,
            "old_path_live": old_live,
            "mapped_path_live": mapped_live,
            "stale_task": stale is not None,
            "has_live_handoff_row": (bool(stale["has_live_handoff_row"]) if stale is not None else None),
            "handoff_status": (stale["handoff_status"] if stale is not None else None),
        }
        buckets[bucket].append(entry)

    counts = {name: len(buckets[name]) for name in ALL_BUCKETS}
    open_total = sum(counts.values())
    degrade = "empty_rename_map" if not normalized_map else None
    generated_at = _utcnow_iso()

    # internal [OBS-08]: stamp debt digest so DASHBOARD can tell
    # healthy-zero dead-path from "classifier has not run". Best-effort —
    # classify remains pure for callers that only consume the return value;
    # stamp failure never fails the classify result.
    try:
        from .review_findings_queries import (  # noqa: PLC0415
            collect_finding_debt_digest,
            stamp_finding_debt_digest,
        )

        digest = collect_finding_debt_digest(conn, workspace_root=root)
        # Prefer classifier dead-path (rebrand_orphaned) when rename_map is live.
        if normalized_map:
            digest["dead_path_count"] = counts.get(BUCKET_REBRAND_ORPHANED, 0)
        stamp_finding_debt_digest(
            digest,
            last_run_at=generated_at,
            source="classify",
            workspace_root=root,
        )
    except Exception:
        pass

    return {
        "ok": True,
        "generated_at": generated_at,
        "open_total": open_total,
        "counts": {
            **counts,
            "stale_task_open": len(stale_items),
        },
        "buckets": buckets,
        "rename_map_size": len(normalized_map),
        "degrade": degrade,
        "plan": PLAN_ID,
        "slice": 1,
        "mode": "classify",
    }


def _disposition_evidence_for_entry(entry: Mapping[str, Any]) -> str:
    """Build rename-map provenance string for a manifest entry."""
    explicit = entry.get("disposition_evidence")
    if explicit is not None and str(explicit).strip():
        return str(explicit).strip()
    old = str(entry.get("file_path") or "").strip()
    new = str(entry.get("mapped_path") or entry.get("target_file_path") or "").strip()
    if old and new:
        return f"{old} → {new}"
    if old:
        return f"file_path={old}"
    return "plan:0097 backlog triage"


def _resolve_entry_action(entry: Mapping[str, Any]) -> str | None:
    """Return action name, or None to skip (no bulk write)."""
    explicit = entry.get("action")
    if explicit is not None:
        action = str(explicit).strip().lower()
        if action in {ACTION_REANCHOR, ACTION_DISPOSITION, ACTION_SKIP}:
            return action
        return ACTION_SKIP

    bucket = str(entry.get("bucket") or "").strip()
    if bucket == BUCKET_REMAPPABLE:
        return ACTION_REANCHOR
    if bucket == BUCKET_REBRAND_ORPHANED:
        return ACTION_DISPOSITION
    # high_needs_human and live require explicit action; never bulk-write.
    return None


def _flatten_manifest_entries(manifest: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Accept classifier-shaped buckets or a flat reviewed ``entries`` list."""
    raw_entries = manifest.get("entries")
    if isinstance(raw_entries, list):
        return [dict(item) for item in raw_entries if isinstance(item, Mapping)]

    entries: list[dict[str, Any]] = []
    buckets = manifest.get("buckets") or {}
    if isinstance(buckets, Mapping):
        for bucket_name, items in buckets.items():
            if not isinstance(items, list):
                continue
            for item in items:
                if not isinstance(item, Mapping):
                    continue
                row = dict(item)
                row.setdefault("bucket", bucket_name)
                entries.append(row)
    return entries


def _live_finding_row(
    conn: sqlite3.Connection,
    *,
    task_ref: str,
    finding_id: str,
    finding_db_id: int | None,
) -> sqlite3.Row | None:
    if finding_db_id is not None:
        row: sqlite3.Row | None = conn.execute(
            "SELECT id, task_ref, finding_id, status, file_path, resolution_notes, severity "
            "FROM review_findings WHERE id = ? AND task_ref = ?",
            (finding_db_id, task_ref),
        ).fetchone()
        if row is not None:
            return row
    fallback: sqlite3.Row | None = conn.execute(
        "SELECT id, task_ref, finding_id, status, file_path, resolution_notes, severity "
        "FROM review_findings WHERE finding_id = ? AND task_ref = ?",
        (finding_id, task_ref),
    ).fetchone()
    return fallback


def _result_outcome(envelope: object) -> tuple[bool, dict[str, Any]]:
    """Normalize review_findings envelope (schema v2 or flat)."""
    if not isinstance(envelope, Mapping):
        return False, {"error": "non-mapping response"}
    if envelope.get("schema_version") == 2:
        data = envelope.get("data") if isinstance(envelope.get("data"), Mapping) else {}
        ok = bool(envelope.get("ok"))
        return ok, dict(data) if isinstance(data, Mapping) else {}
    ok = bool(envelope.get("ok"))
    return ok, dict(envelope)


def apply_reviewed_manifest(
    conn: sqlite3.Connection,
    manifest: Mapping[str, Any] | None,
    *,
    batch_size: int = DEFAULT_APPLY_BATCH_SIZE,
    dry_run: bool = False,
    actor: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Execute a reviewed triage manifest via sanctioned disposition/reanchor ops.

    - Chunks entries for iteration/telemetry (``batch_size``, default 200 —
      mirrors apply_stale_findings_gc's chunk size and reports a ``batches``
      count). This is a reporting/iteration boundary only: each entry commits
      in its own sanctioned-op transaction (see below); there is **no**
      per-batch commit boundary or backpressure throttle, so ``batch_size``
      does not change durability or transaction granularity ([RES-14]).
    - Idempotent re-run no-ops ([RES-01]).
    - Concurrency-skip ([CON-11]): re-reads live status/file_path before write;
      skips rows whose status left open or whose file_path changed since classify.
    - High-severity / live buckets are skipped unless the entry carries an
      explicit ``action``.
    - Empty/missing manifest degrades safely (no writes).

    Parameters
    ----------
    conn:
        Open handoff DB connection used for live-state re-checks only. Writes
        go through ``review_findings`` (sanctioned ops), each of which opens and
        commits its own short per-entry transaction — finer-grained than one
        giant apply txn, but committed per entry, not per batch ([RES-14]).
    """
    bounded = max(1, int(batch_size))
    if not manifest:
        return {
            "ok": True,
            "mode": "apply",
            "plan": PLAN_ID,
            "slice": 2,
            "generated_at": _utcnow_iso(),
            "degrade": "empty_manifest",
            "batch_size": bounded,
            "dry_run": dry_run,
            "counts": {
                "considered": 0,
                "applied": 0,
                "reanchored": 0,
                "dispositioned": 0,
                "already_applied": 0,
                "skipped_concurrency": 0,
                "skipped_policy": 0,
                "errors": 0,
                "batches": 0,
            },
            "results": [],
        }

    entries = _flatten_manifest_entries(manifest)
    if not entries:
        return {
            "ok": True,
            "mode": "apply",
            "plan": PLAN_ID,
            "slice": 2,
            "generated_at": _utcnow_iso(),
            "degrade": "empty_manifest",
            "batch_size": bounded,
            "dry_run": dry_run,
            "counts": {
                "considered": 0,
                "applied": 0,
                "reanchored": 0,
                "dispositioned": 0,
                "already_applied": 0,
                "skipped_concurrency": 0,
                "skipped_policy": 0,
                "errors": 0,
                "batches": 0,
            },
            "results": [],
        }

    results: list[dict[str, Any]] = []
    counts = {
        "considered": 0,
        "applied": 0,
        "reanchored": 0,
        "dispositioned": 0,
        "already_applied": 0,
        "skipped_concurrency": 0,
        "skipped_policy": 0,
        "errors": 0,
        "batches": 0,
    }

    for batch_start in range(0, len(entries), bounded):
        batch = entries[batch_start : batch_start + bounded]
        counts["batches"] += 1
        for entry in batch:
            counts["considered"] += 1
            task_ref = str(entry.get("task_ref") or "").strip()
            finding_id = str(entry.get("finding_id") or "").strip()
            finding_db_id_raw = entry.get("finding_db_id")
            finding_db_id: int | None
            try:
                finding_db_id = int(finding_db_id_raw) if finding_db_id_raw is not None else None
            except (TypeError, ValueError):
                finding_db_id = None
            expected_path = str(entry.get("file_path") or "").strip().replace("\\", "/")
            base_result: dict[str, Any] = {
                "task_ref": task_ref,
                "finding_id": finding_id,
                "finding_db_id": finding_db_id,
                "bucket": entry.get("bucket"),
                "expected_file_path": expected_path or None,
            }

            if not task_ref or not finding_id:
                counts["errors"] += 1
                results.append({**base_result, "outcome": "error", "error": "task_ref and finding_id required"})
                continue

            action = _resolve_entry_action(entry)
            if action is None or action == ACTION_SKIP:
                counts["skipped_policy"] += 1
                results.append(
                    {
                        **base_result,
                        "outcome": "skipped_policy",
                        "reason": "no bulk action (high_needs_human/live require explicit action)",
                    }
                )
                continue

            live = _live_finding_row(
                conn,
                task_ref=task_ref,
                finding_id=finding_id,
                finding_db_id=finding_db_id,
            )
            if live is None:
                counts["skipped_concurrency"] += 1
                results.append(
                    {
                        **base_result,
                        "outcome": "skipped_concurrency",
                        "reason": "finding row not found at apply time",
                    }
                )
                continue

            live_status = str(live["status"])
            live_path = str(live["file_path"] or "").replace("\\", "/")
            live_notes = str(live["resolution_notes"] or "")

            if action == ACTION_REANCHOR:
                target_path = (
                    str(entry.get("mapped_path") or entry.get("target_file_path") or "").strip().replace("\\", "/")
                )
                if not target_path:
                    counts["errors"] += 1
                    results.append(
                        {
                            **base_result,
                            "outcome": "error",
                            "error": "reanchor requires mapped_path/target_file_path",
                        }
                    )
                    continue

                # Idempotent: already open at mapped path.
                if live_status == FindingStatus.OPEN.value and live_path == target_path:
                    counts["already_applied"] += 1
                    results.append(
                        {
                            **base_result,
                            "outcome": "already_applied",
                            "action": ACTION_REANCHOR,
                            "file_path": live_path,
                            "status": live_status,
                        }
                    )
                    continue

                # Concurrency-skip: non-open or path drifted from classify-time.
                if live_status != FindingStatus.OPEN.value:
                    counts["skipped_concurrency"] += 1
                    results.append(
                        {
                            **base_result,
                            "outcome": "skipped_concurrency",
                            "reason": "live status is non-open",
                            "live_status": live_status,
                        }
                    )
                    continue
                if expected_path and live_path != expected_path:
                    counts["skipped_concurrency"] += 1
                    results.append(
                        {
                            **base_result,
                            "outcome": "skipped_concurrency",
                            "reason": "live file_path changed since classify",
                            "live_file_path": live_path,
                        }
                    )
                    continue

                evidence = _disposition_evidence_for_entry(entry)
                notes: str | None = (
                    str(entry.get("resolution_notes") or "").strip()
                    or f"plan:{PLAN_ID} reanchor; disposition_evidence={evidence}"
                )
                if dry_run:
                    counts["applied"] += 1
                    counts["reanchored"] += 1
                    results.append(
                        {
                            **base_result,
                            "outcome": "would_reanchor",
                            "action": ACTION_REANCHOR,
                            "target_file_path": target_path,
                            "resolution_notes": notes,
                        }
                    )
                    continue

                envelope = review_findings(
                    review=cast(
                        Any,
                        {
                            "operation": "reanchor",
                            "task_ref": task_ref,
                            "finding_id": finding_id,
                            "file_path": target_path,
                            "expected_file_path": expected_path or None,
                            "resolution_notes": notes,
                            **({"actor": dict(actor)} if actor else {}),
                        },
                    )
                )
                ok, data = _result_outcome(envelope)
                if not ok:
                    # Treat expected_file_path mismatch as concurrency-skip, not hard error.
                    err = str(data.get("error") or "")
                    if "concurrency skip" in err or "expected_file_path" in err:
                        counts["skipped_concurrency"] += 1
                        results.append(
                            {
                                **base_result,
                                "outcome": "skipped_concurrency",
                                "reason": err,
                                "live_file_path": data.get("current_file_path"),
                            }
                        )
                    else:
                        counts["errors"] += 1
                        results.append({**base_result, "outcome": "error", "error": err or data})
                    continue
                if data.get("already_applied"):
                    counts["already_applied"] += 1
                    results.append(
                        {
                            **base_result,
                            "outcome": "already_applied",
                            "action": ACTION_REANCHOR,
                            "file_path": target_path,
                            "status": FindingStatus.OPEN.value,
                        }
                    )
                    continue
                counts["applied"] += 1
                counts["reanchored"] += 1
                finding_raw = data.get("finding")
                finding = finding_raw if isinstance(finding_raw, Mapping) else {}
                results.append(
                    {
                        **base_result,
                        "outcome": "reanchored",
                        "action": ACTION_REANCHOR,
                        "file_path": finding.get("file_path", target_path),
                        "status": finding.get("status", FindingStatus.OPEN.value),
                    }
                )
                continue

            # disposition
            target_status = str(
                entry.get("disposition_status") or entry.get("status") or DEFAULT_DISPOSITION_STATUS
            ).strip()
            if target_status not in {"deferred", "wontfix", "fixed"}:
                counts["errors"] += 1
                results.append(
                    {
                        **base_result,
                        "outcome": "error",
                        "error": f"invalid disposition status: {target_status!r}",
                    }
                )
                continue

            evidence = _disposition_evidence_for_entry(entry)
            # Idempotent: already at terminal status with evidence (or matching status).
            if live_status == target_status:
                counts["already_applied"] += 1
                results.append(
                    {
                        **base_result,
                        "outcome": "already_applied",
                        "action": ACTION_DISPOSITION,
                        "status": live_status,
                        "resolution_notes": live_notes or None,
                    }
                )
                continue

            if live_status != FindingStatus.OPEN.value:
                counts["skipped_concurrency"] += 1
                results.append(
                    {
                        **base_result,
                        "outcome": "skipped_concurrency",
                        "reason": "live status is non-open",
                        "live_status": live_status,
                    }
                )
                continue
            if expected_path and live_path != expected_path:
                counts["skipped_concurrency"] += 1
                results.append(
                    {
                        **base_result,
                        "outcome": "skipped_concurrency",
                        "reason": "live file_path changed since classify",
                        "live_file_path": live_path,
                    }
                )
                continue

            notes = str(entry.get("resolution_notes") or "").strip() or None
            if dry_run:
                counts["applied"] += 1
                counts["dispositioned"] += 1
                results.append(
                    {
                        **base_result,
                        "outcome": "would_disposition",
                        "action": ACTION_DISPOSITION,
                        "status": target_status,
                        "disposition_evidence": evidence,
                        "resolution_notes": notes,
                    }
                )
                continue

            envelope = review_findings(
                review=cast(
                    Any,
                    {
                        "operation": "disposition",
                        "task_ref": task_ref,
                        "finding_id": finding_id,
                        "status": target_status,
                        "resolution_notes": notes,
                        "disposition_evidence": evidence,
                        **({"actor": dict(actor)} if actor else {}),
                    },
                )
            )
            ok, data = _result_outcome(envelope)
            if not ok:
                counts["errors"] += 1
                results.append(
                    {
                        **base_result,
                        "outcome": "error",
                        "error": data.get("error") or data,
                    }
                )
                continue
            counts["applied"] += 1
            counts["dispositioned"] += 1
            finding_raw = data.get("finding")
            finding = finding_raw if isinstance(finding_raw, Mapping) else {}
            results.append(
                {
                    **base_result,
                    "outcome": "dispositioned",
                    "action": ACTION_DISPOSITION,
                    "status": finding.get("status", target_status),
                    "disposition_evidence": evidence,
                    "resolution_notes": finding.get("resolution_notes"),
                }
            )

    return {
        "ok": counts["errors"] == 0,
        "mode": "apply",
        "plan": PLAN_ID,
        "slice": 2,
        "generated_at": _utcnow_iso(),
        "degrade": None,
        "batch_size": bounded,
        "dry_run": dry_run,
        "counts": counts,
        "results": results,
    }
