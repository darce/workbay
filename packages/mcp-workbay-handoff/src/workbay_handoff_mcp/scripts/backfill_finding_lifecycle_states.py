"""Backfill legacy ``status='fixed'`` rows into the internal two-anchor model.

Walks every ``review_findings`` row whose ``status='fixed'`` AND whose
``resolved_on_branch_at_*`` / ``integrated_at_*`` columns are all NULL (rows
the implementation note schema migration could not retroactively anchor). For each row the
script derives a close-commit anchor in this priority order:

  (a) the first 40-char hex SHA found inside ``verification_evidence``;
  (b) the ``commit_sha`` from the most recent decision row whose ``decision``
      text or ``rationale`` mentions the finding id.

Reachable anchors (``git merge-base --is-ancestor <sha> <integration_ref>``)
populate ``integrated_at_commit`` / ``integrated_at_ref`` / ``integrated_at_ts``.
Unreachable or underivable anchors populate ``resolved_on_branch_at_commit``
(``NULL`` when nothing could be derived) / ``resolved_on_branch_ref`` /
``resolved_on_branch_at_ts``. Legacy rows keep ``status='fixed'`` — the implementation note
flag flip is what changes the wire-level status string.

One ``backfill_finding_lifecycle_<finding_id>`` decision row is recorded per
walked finding, carrying the chosen anchor + provenance in its rationale so the
backfill is traceable after the fact.
"""

from __future__ import annotations

import argparse
import re
import sys
from typing import Any

from ..review_findings_updates import (
    _is_ancestor_of_ref,
    _resolve_integration_ref_head_sha,
)
from ..shared_schema import _get_db_connection

_SHA_RE = re.compile(r"\b([0-9a-fA-F]{40})\b")


def _extract_evidence_sha(verification_evidence: str | None) -> str | None:
    if not verification_evidence:
        return None
    match = _SHA_RE.search(verification_evidence)
    return match.group(1).lower() if match else None


def _decision_commit_for_finding(conn: Any, task_ref: str, finding_id: str) -> str | None:
    row = conn.execute(
        """
        SELECT commit_sha FROM decisions
        WHERE task_ref = ?
          AND commit_sha IS NOT NULL
          AND (decision LIKE ? OR rationale LIKE ?)
        ORDER BY id DESC
        LIMIT 1
        """,
        (task_ref, f"%{finding_id}%", f"%{finding_id}%"),
    ).fetchone()
    if row is None:
        return None
    sha = row["commit_sha"]
    return sha.lower() if isinstance(sha, str) and _SHA_RE.fullmatch(sha) else None


def backfill_finding_lifecycle_states(
    *,
    integration_ref: str = "main",
    task_ref: str | None = None,
) -> dict[str, Any]:
    """Walk legacy ``fixed`` rows and populate the internal anchor columns.

    Idempotent: rows that already have any of the new column sets populated are
    skipped so re-running the script is a no-op.
    """
    head_sha = _resolve_integration_ref_head_sha(integration_ref)
    walked = 0
    integrated = 0
    resolved_on_branch = 0
    null_anchor = 0
    migration_decisions = 0

    with _get_db_connection() as conn:
        query = """
            SELECT id, finding_id, task_ref, verification_evidence
            FROM review_findings
            WHERE status = 'fixed'
              AND resolved_on_branch_at_commit IS NULL
              AND resolved_on_branch_at_ts IS NULL
              AND integrated_at_commit IS NULL
              AND integrated_at_ts IS NULL
        """
        params: tuple[Any, ...] = ()
        if task_ref is not None:
            query += " AND task_ref = ?"
            params = (task_ref,)
        rows = list(conn.execute(query, params).fetchall())

        for row in rows:
            walked += 1
            finding_id = row["finding_id"]
            row_task_ref = row["task_ref"]
            row_pk = row["id"]

            anchor = _extract_evidence_sha(row["verification_evidence"])
            anchor_source = "verification_evidence" if anchor else None
            if anchor is None:
                anchor = _decision_commit_for_finding(conn, row_task_ref, finding_id)
                if anchor is not None:
                    anchor_source = "decision_commit_sha"

            reachable = anchor is not None and head_sha is not None and _is_ancestor_of_ref(anchor, integration_ref)

            if reachable:
                conn.execute(
                    """
                    UPDATE review_findings
                    SET integrated_at_commit = ?,
                        integrated_at_ref = ?,
                        integrated_at_ts = datetime('now')
                    WHERE id = ?
                    """,
                    (anchor, integration_ref, row_pk),
                )
                outcome = "integrated"
                integrated += 1
            else:
                conn.execute(
                    """
                    UPDATE review_findings
                    SET resolved_on_branch_at_commit = ?,
                        resolved_on_branch_at_ts = datetime('now')
                    WHERE id = ?
                    """,
                    (anchor, row_pk),
                )
                outcome = "resolved_on_branch"
                if anchor is None:
                    null_anchor += 1
                else:
                    resolved_on_branch += 1

            decision_id = f"backfill_finding_lifecycle_{finding_id}"
            rationale_lines = [
                f"internal backfill of legacy fixed row {finding_id} on task {row_task_ref}.",
                f"anchor_source={anchor_source or 'none'}",
                f"anchor_commit={anchor or 'null'}",
                f"integration_ref={integration_ref}",
                f"integration_head={head_sha or 'unresolved'}",
                f"outcome={outcome}",
            ]
            conn.execute(
                """
                INSERT INTO decisions (task_ref, session, decision, rationale, commit_sha)
                VALUES (?, 'backfill-internal-68', ?, ?, ?)
                """,
                (row_task_ref, decision_id, "\n".join(rationale_lines), anchor),
            )
            migration_decisions += 1

        conn.commit()

    return {
        "ok": True,
        "walked": walked,
        "integrated": integrated,
        "resolved_on_branch": resolved_on_branch,
        "null_anchor": null_anchor,
        "migration_decisions": migration_decisions,
        "integration_ref": integration_ref,
        "integration_head": head_sha,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="workbay_handoff_mcp.scripts.backfill_finding_lifecycle_states",
        description=(
            "Backfill the internal two-anchor lifecycle columns on legacy review_findings rows with status='fixed'."
        ),
    )
    parser.add_argument("--integration-ref", default="main")
    parser.add_argument("--task-ref", default=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    report = backfill_finding_lifecycle_states(
        integration_ref=args.integration_ref,
        task_ref=args.task_ref,
    )
    import json

    sys.stdout.write(json.dumps(report) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
