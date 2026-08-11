"""CLI: backfill concept embeddings over existing handoff rows (internal).

Opt-in + offline. Resolves the embedding provider from the hash-pinned env
configuration; with no provider configured (the default) it reports
``provider_unavailable`` and writes nothing, so the feature stays off. The
backfill is idempotent and resumable — safe to re-run after an interruption.

    python -m workbay_handoff_mcp.scripts.backfill_concept_embeddings [--task-ref REF]
"""

from __future__ import annotations

import argparse
import json
import sys

from ..embeddings import store
from ..shared_schema import _get_db_connection


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Backfill concept embeddings over existing handoff rows.")
    parser.add_argument("--task-ref", default=None, help="Limit the backfill to a single task_ref.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    provider = store._resolve_provider()
    if provider is None:
        sys.stdout.write(json.dumps({"ok": False, "reason": "provider_unavailable"}) + "\n")
        return 0
    with _get_db_connection() as conn:
        counts = store.backfill_concept_embeddings(conn, provider, task_ref=args.task_ref)
    sys.stdout.write(json.dumps({"ok": True, **counts}) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
