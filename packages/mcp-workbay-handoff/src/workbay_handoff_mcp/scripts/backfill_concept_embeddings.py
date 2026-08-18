"""CLI: backfill concept embeddings over existing handoff rows (internal).

Opt-in + offline. Resolves the embedding provider from the hash-pinned env
configuration; with no provider configured (the default) it reports
``provider_unavailable`` and writes nothing, so the feature stays off. The
backfill is idempotent and resumable — safe to re-run after an interruption.

    python -m workbay_handoff_mcp.scripts.backfill_concept_embeddings [--task-ref REF]
    python -m workbay_handoff_mcp.scripts.backfill_concept_embeddings --dry-run
    python -m workbay_handoff_mcp.scripts.backfill_concept_embeddings --kinds decision.rationale --limit 50
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path

from ..config import RuntimeConfig
from ..embeddings import store
from ..runtime import configure_runtime, get_runtime_config
from ..shared_schema import _get_db_connection, connect_handoff_db


def _parse_kinds(value: str) -> tuple[str, ...]:
    kinds = tuple(part.strip() for part in value.split(",") if part.strip())
    if not kinds:
        raise argparse.ArgumentTypeError("--kinds requires at least one entity kind")
    allowed = set(store.CONCEPT_ENTITY_KINDS)
    unknown = [kind for kind in kinds if kind not in allowed]
    if unknown:
        valid = ", ".join(store.CONCEPT_ENTITY_KINDS)
        raise argparse.ArgumentTypeError(
            f"unknown entity kind(s): {', '.join(unknown)}. valid: {valid}"
        )
    return kinds


def _parse_limit(value: str) -> int:
    try:
        limit = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("--limit must be an integer") from exc
    if limit < 0:
        raise argparse.ArgumentTypeError("--limit must be >= 0")
    return limit


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Backfill concept embeddings over existing handoff rows.")
    parser.add_argument("--task-ref", default=None, help="Limit the backfill to a single task_ref.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Report per-kind counts that would be embedded. Opens the DB "
            "read-only (does not create, migrate, or purge) and does not "
            "call provider.embed. When a provider resolves, counts use the "
            "same (text_hash, model_id) gate as the write path; when none "
            "is configured, counts are text_hash-only and may under-count "
            "after a model change. An existing DB that cannot be opened, "
            "is missing concept_embeddings, or fails mid-probe exits "
            "non-zero with ok: false (not an empty success)."
        ),
    )
    parser.add_argument(
        "--kinds",
        default=None,
        type=_parse_kinds,
        help="Comma-separated subset of embeddable entity kinds (default: all).",
    )
    parser.add_argument(
        "--limit",
        default=None,
        type=_parse_limit,
        help="Maximum rows to embed in this run.",
    )
    # Picked up by RuntimeConfig.from_args() so this CLI targets the same
    # .task-state/handoff.db as the rest of the toolchain.
    parser.add_argument("--workspace-root")
    parser.add_argument("--state-dir")
    parser.add_argument("--current-task-path")
    parser.add_argument("--dashboard-path")
    parser.add_argument("--exports-dir")
    return parser


def _empty_by_kind(kinds: tuple[str, ...]) -> dict[str, int]:
    return {kind: 0 for kind in kinds}


def _open_readonly_handoff_db() -> tuple[sqlite3.Connection | None, str | None]:
    """Open ``handoff.db`` read-only; never create, migrate, or purge.

    Returns ``(None, None)`` only when the file is absent — that is a real
    empty corpus. An existing file that cannot be opened, whose catalog is
    unreadable, or that lacks ``concept_embeddings`` returns
    ``(None, reason)`` so dry-run can fail instead of impersonating clean.
    """
    db_path = get_runtime_config().db_path
    if not Path(db_path).is_file():
        return None, None
    try:
        conn = connect_handoff_db(db_path, read_only=True)
    except (OSError, sqlite3.Error):
        return None, "db_unreadable"
    try:
        names = {
            str(row[0])
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
        }
    except sqlite3.Error:
        conn.close()
        return None, "db_unreadable"
    if "concept_embeddings" not in names:
        conn.close()
        return None, "concept_embeddings_missing"
    return conn, None


def _would_embed_by_kind(
    conn: sqlite3.Connection,
    *,
    task_ref: str | None,
    kinds: tuple[str, ...],
    limit: int | None,
    model_id: str | None,
) -> dict[str, int]:
    """Count remaining work per kind via the shared store classify helper.

    ``limit`` caps remaining work, not already-embedded source rows. When
    ``model_id`` is set this is the write-path ``(text_hash, model_id)``
    gate; when it is ``None`` skip is hash-only.
    """
    kind_set = set(kinds)
    by_kind = _empty_by_kind(kinds)
    remaining = limit
    for entity_kind, entity_id, _tref, text in store.gather_concepts(conn, task_ref):
        if entity_kind not in kind_set:
            continue
        outcome, _new_hash = store.classify_concept_for_backfill(
            conn, entity_kind, entity_id, text, model_id=model_id
        )
        if outcome != "pending":
            continue
        if remaining is not None and remaining <= 0:
            break
        by_kind[entity_kind] += 1
        if remaining is not None:
            remaining -= 1
    return by_kind


def _dry_run_payload(
    *,
    by_kind: dict[str, int],
    model_id: str | None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "ok": True,
        "dry_run": True,
        "by_kind": by_kind,
        "would_embed": sum(by_kind.values()),
    }
    if model_id is None:
        payload["classify_gate"] = "text_hash_only"
        payload["classify_limitation"] = (
            "provider unavailable; remaining-work counts skip on text_hash "
            "only and may under-count rows the write path would re-embed "
            "after a model change"
        )
    else:
        payload["classify_gate"] = "text_hash_and_model_id"
        payload["model_id"] = model_id
    return payload


def _dry_run_failure_payload(reason: str) -> dict[str, object]:
    return {
        "ok": False,
        "dry_run": True,
        "reason": reason,
        "by_kind": {},
        "would_embed": 0,
    }


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    configure_runtime(RuntimeConfig.from_args(args))

    kinds = args.kinds if args.kinds is not None else store.CONCEPT_ENTITY_KINDS

    if args.dry_run:
        # Preview only: resolve the provider to read model_id, never embed.
        # Never open a writable prepared connection (that path can create the
        # DB, bootstrap schema, and DELETE purged concept_embeddings kinds).
        provider = store._resolve_provider()
        model_id = None if provider is None else provider.model_id
        conn, open_error = _open_readonly_handoff_db()
        try:
            if open_error is not None:
                sys.stdout.write(json.dumps(_dry_run_failure_payload(open_error)) + "\n")
                return 1
            if conn is None:
                by_kind = _empty_by_kind(kinds)
            else:
                try:
                    by_kind = _would_embed_by_kind(
                        conn,
                        task_ref=args.task_ref,
                        kinds=kinds,
                        limit=args.limit,
                        model_id=model_id,
                    )
                except sqlite3.Error:
                    sys.stdout.write(json.dumps(_dry_run_failure_payload("probe_failed")) + "\n")
                    return 1
        finally:
            if conn is not None:
                conn.close()
        sys.stdout.write(json.dumps(_dry_run_payload(by_kind=by_kind, model_id=model_id)) + "\n")
        return 0

    provider = store._resolve_provider()
    if provider is None:
        sys.stdout.write(json.dumps({"ok": False, "reason": "provider_unavailable"}) + "\n")
        return 0

    with _get_db_connection() as conn:
        # Always use the production classify/embed/write path so --kinds and
        # --limit inherit embed timeout, commit_every durability, and the
        # lock-free split (internal). kinds=None means all kinds.
        counts = store.backfill_concept_embeddings(
            conn,
            provider,
            task_ref=args.task_ref,
            kinds=args.kinds,
            limit=args.limit,
        )
    sys.stdout.write(json.dumps({"ok": True, **counts}) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
