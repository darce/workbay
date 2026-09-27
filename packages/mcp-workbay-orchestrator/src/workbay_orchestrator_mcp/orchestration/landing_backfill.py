"""Operator-only, report-first census for merged lane landing evidence.

The report derives its answers from one pinned main history and one registry
snapshot. Applying a report only records that provenance decision; it never
patches a lane row or writes a derived landing value.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sqlite3
import subprocess
import sys
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable

from workbay_handoff_mcp.runtime import RuntimeNotConfiguredError

from workbay_orchestrator_mcp.orchestration import lane_reclaim
from workbay_orchestrator_mcp.orchestration.landing_log import (
    CarrierResult,
    LandingReceipt,
    ScanResult,
    find_carrier,
    scan_first_parent,
    verify_receipt,
)

REPORT_SCHEMA = "landing_backfill_report_v1"
RULE = "derived_v1"
EVIDENCE_KINDS = ("landing_column", "landing_decision", "row_tip", "live_branch", "gate_row")
CLASSES = (
    "receipt_v1",
    "receipt_unverified",
    "derived_v1",
    "evidence_disputed",
    "tip_unreachable",
    "tip_missing",
    "no_tip",
    "probe_failed",
)
_HEX40 = re.compile(r"^[0-9a-f]{40}$")
_RECEIPT_PROBE_FAILURES = frozenset({"v1_probe_failed", "v3_lookup_failed", "v4_lookup_failed"})


def _run_git(repo: str | Path, *args: str, timeout_s: float) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", "-C", str(repo), *args],
        capture_output=True,
        text=True,
        check=False,
        timeout=timeout_s,
    )


def _canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _digest(schema: object, rule: object, main_tip: object, receipt_scan: object, rows: object) -> str:
    payload = {
        "schema": schema,
        "rule": rule,
        "main_tip": main_tip,
        "receipt_scan": receipt_scan,
        "rows": rows,
    }
    return hashlib.sha256(_canonical_json(payload).encode("ascii")).hexdigest()


def _empty_receipt_scan() -> dict[str, Any]:
    return {
        "complete": False,
        "scanned": 0,
        "receipts": 0,
        "unverified": 0,
        "error": None,
        "unmatched_total": 0,
        "unmatched": [],
    }


def _empty_counts() -> dict[str, int]:
    return {label: 0 for label in CLASSES}


def _assemble_report(
    *,
    main_ref: str,
    main_tip: str | None,
    receipt_scan: dict[str, Any],
    rows: list[dict[str, Any]],
    complete: bool,
    error: str | None,
    started: float,
) -> dict[str, Any]:
    counts = _empty_counts()
    counts_by_evidence: dict[str, int] = {}
    for row in rows:
        counts[row["class"]] += 1
        evidence = row["evidence"] if row["evidence"] is not None else "none"
        key = f"{row['class']}/{evidence}"
        counts_by_evidence[key] = counts_by_evidence.get(key, 0) + 1
    report: dict[str, Any] = {
        "schema": REPORT_SCHEMA,
        "rule": RULE,
        "complete": bool(complete),
        "error": error,
        "main_ref": main_ref,
        "main_tip": main_tip,
        "receipt_scan": receipt_scan,
        "digest": _digest(REPORT_SCHEMA, RULE, main_tip, receipt_scan, rows),
        "counts": counts,
        "counts_by_evidence": counts_by_evidence,
        "rows": rows,
        "generated_at": datetime.now(UTC).isoformat(),
        "elapsed_s": round(time.monotonic() - started, 6),
    }
    return report


def _pin_main(repo: str | Path, main_ref: str, timeout_s: float) -> tuple[str | None, str | None]:
    try:
        proc = _run_git(
            repo,
            "rev-parse",
            "--verify",
            "--quiet",
            "--end-of-options",
            f"{main_ref}^{{commit}}",
            timeout_s=timeout_s,
        )
    except subprocess.TimeoutExpired:
        return None, "main_unresolvable"
    except OSError:
        return None, "main_unresolvable"
    if proc.returncode != 0:
        return None, "main_unresolvable"
    value = proc.stdout.strip().splitlines()
    if not value or _HEX40.fullmatch(value[0].lower()) is None:
        return None, "main_unresolvable"
    return value[0].lower(), None


def _list_branch_heads(repo: str | Path, timeout_s: float) -> tuple[dict[str, str] | None, str | None]:
    try:
        proc = _run_git(
            repo,
            "for-each-ref",
            "--format=%(refname)%00%(objectname)",
            "refs/heads/",
            timeout_s=timeout_s,
        )
    except subprocess.TimeoutExpired:
        return None, "branch_list_failed"
    except OSError:
        return None, "branch_list_failed"
    if proc.returncode != 0:
        return None, "branch_list_failed"
    heads: dict[str, str] = {}
    for line in proc.stdout.splitlines():
        refname, separator, object_name = line.partition("\x00")
        if separator and refname.startswith("refs/heads/"):
            heads[refname] = object_name.strip().lower()
    return heads, None


def _read_registry_snapshot(
    scanned_receipts: tuple[tuple[str, LandingReceipt], ...],
) -> tuple[
    dict[tuple[str, str], dict[str, Any]],
    dict[tuple[str, str], dict[str, Any]],
    dict[tuple[str, str], dict[str, Any]],
    dict[int, dict[str, Any]],
]:
    lane_rows: dict[tuple[str, str], dict[str, Any]] = {}
    landing_decisions: dict[tuple[str, str], dict[str, Any]] = {}
    passing_gates: dict[tuple[str, str], dict[str, Any]] = {}
    receipt_gates: dict[int, dict[str, Any]] = {}
    with lane_reclaim._scan_read_connection() as conn:
        transaction_started = False
        try:
            conn.execute("BEGIN")
            transaction_started = True
            for raw in conn.execute("SELECT * FROM worktree_lanes").fetchall():
                row = dict(raw)
                key = (str(row.get("task_ref", "")), str(row.get("lane_id", "")))
                lane_rows[key] = row

            decision_rows = conn.execute(
                """
                SELECT id, task_ref, lane_id, decision, commit_sha, created_at
                FROM decisions
                WHERE decision LIKE 'lane_landed_%'
                  AND commit_sha IS NOT NULL
                  AND TRIM(commit_sha) <> ''
                ORDER BY created_at DESC, id DESC
                """
            ).fetchall()
            decision_targets = {
                (task_ref, f"lane_landed_{task_ref}_{lane_id}"): (task_ref, lane_id) for task_ref, lane_id in lane_rows
            }
            for raw in decision_rows:
                row = dict(raw)
                task_ref = str(row.get("task_ref", ""))
                target = decision_targets.get((task_ref, row.get("decision")))
                if target is None:
                    continue
                landing_decisions.setdefault(target, row)

            gate_rows = conn.execute(
                """
                SELECT id, task_ref, lane_id, commit_sha, passed
                FROM verified_tests
                WHERE lane_id IS NOT NULL
                  AND commit_sha IS NOT NULL
                  AND TRIM(commit_sha) <> ''
                ORDER BY id DESC
                """
            ).fetchall()
            for raw in gate_rows:
                row = dict(raw)
                if not bool(row.get("passed")):
                    continue
                key = (str(row.get("task_ref", "")), str(row.get("lane_id", "")))
                passing_gates.setdefault(key, row)

            gate_ids = sorted({int(receipt.gate_id) for _, receipt in scanned_receipts})
            for offset in range(0, len(gate_ids), 500):
                chunk = gate_ids[offset : offset + 500]
                placeholders = ",".join("?" for _ in chunk)
                for raw in conn.execute(
                    f"SELECT * FROM verified_tests WHERE id IN ({placeholders})", tuple(chunk)
                ).fetchall():
                    row = dict(raw)
                    receipt_gates[int(row["id"])] = row
        finally:
            if transaction_started:
                conn.execute("ROLLBACK")
    return lane_rows, landing_decisions, passing_gates, receipt_gates


def _as_candidate(kind: str, value: object, malformed: list[dict[str, str]]) -> dict[str, str] | None:
    if value is None:
        return None
    text = value.strip() if isinstance(value, str) else str(value).strip()
    if not text:
        return None
    normalized = text.lower()
    if _HEX40.fullmatch(normalized) is None:
        malformed.append({"kind": kind, "value": text[:64]})
        return None
    return {"kind": kind, "sha": normalized}


def _branch_ref(branch: object) -> str | None:
    if not isinstance(branch, str) or not branch.strip():
        return None
    try:
        short = lane_reclaim._short_branch_name(branch.strip())
    except (AttributeError, TypeError, ValueError):
        return None
    return f"refs/heads/{short}" if short else None


def _receipt_lookups(
    lane_rows: dict[tuple[str, str], dict[str, Any]], receipt_gates: dict[int, dict[str, Any]]
) -> tuple[Callable[[str, str], dict[str, Any]], Callable[[int], dict[str, Any]]]:
    def lane_lookup(task_ref: str, lane_id: str) -> dict[str, Any]:
        row = lane_rows.get((task_ref, lane_id))
        if row is None:
            return {
                "ok": False,
                "tool": "get_lane",
                "data": {"error": f"Lane '{lane_id}' not found for task '{task_ref}'."},
            }
        return {"ok": True, "tool": "get_lane", "data": {"lane": row, "task_ref": task_ref}}

    def gate_lookup(test_id: int) -> dict[str, Any]:
        row = receipt_gates.get(int(test_id))
        if row is None:
            return {
                "ok": True,
                "tool": "get_verified_test_by_id",
                "data": {"test_id": int(test_id), "found": False, "test": None},
            }
        gate = dict(row)
        gate["passed"] = bool(gate.get("passed"))
        return {
            "ok": True,
            "tool": "get_verified_test_by_id",
            "data": {"test_id": int(test_id), "found": True, "test": gate},
        }

    return lane_lookup, gate_lookup


def _receipt_classification(
    repo: str | Path,
    main_tip: str,
    receipts: list[tuple[str, LandingReceipt]],
    lane_lookup: Callable[[str, str], object],
    gate_lookup: Callable[[int], object],
) -> dict[str, Any] | None:
    newest_commit: str | None = None
    newest_failure: str | None = None
    for commit, _receipt in receipts:
        if newest_commit is None:
            newest_commit = commit
        try:
            verdict = verify_receipt(
                repo,
                commit,
                main_ref=main_tip,
                lane_lookup=lane_lookup,
                gate_lookup=gate_lookup,
            )
        except (OSError, subprocess.TimeoutExpired, sqlite3.Error, RuntimeNotConfiguredError) as exc:
            return {
                "class": "probe_failed",
                "evidence": "receipt",
                "evidence_sha": commit,
                "carrier": None,
                "lineage": None,
                "detail": f"receipt:{type(exc).__name__}",
            }
        except Exception as exc:  # noqa: BLE001 — an unexpected receipt probe is still typed unknown
            return {
                "class": "probe_failed",
                "evidence": "receipt",
                "evidence_sha": commit,
                "carrier": None,
                "lineage": None,
                "detail": f"receipt:{type(exc).__name__}",
            }
        if verdict.verified:
            return {
                "class": "receipt_v1",
                "evidence": "receipt",
                "evidence_sha": commit,
                "carrier": commit,
                "lineage": "receipt",
                "detail": f"depth:{verdict.depth or 0}",
            }
        failed_check = verdict.failed_check or "receipt_unverified"
        if failed_check in _RECEIPT_PROBE_FAILURES or verdict.detail == "timeout":
            return {
                "class": "probe_failed",
                "evidence": "receipt",
                "evidence_sha": commit,
                "carrier": None,
                "lineage": None,
                "detail": f"receipt:{failed_check}",
            }
        if newest_failure is None:
            newest_failure = failed_check
    if newest_commit is None:
        return None
    return {
        "class": "receipt_unverified",
        "evidence": "receipt",
        "evidence_sha": newest_commit,
        "carrier": None,
        "lineage": None,
        "detail": newest_failure or "receipt_unverified",
    }


def _classify_row(
    *,
    repo: str | Path,
    main_tip: str,
    row: dict[str, Any],
    branch_heads: dict[str, str],
    lane_rows: dict[tuple[str, str], dict[str, Any]],
    landing_decisions: dict[tuple[str, str], dict[str, Any]],
    passing_gates: dict[tuple[str, str], dict[str, Any]],
    receipt_gates: dict[int, dict[str, Any]],
    receipts: list[tuple[str, LandingReceipt]],
    probes: dict[str, CarrierResult],
) -> dict[str, Any]:
    task_ref = str(row.get("task_ref", ""))
    lane_id = str(row.get("lane_id", ""))
    key = (task_ref, lane_id)
    malformed: list[dict[str, str]] = []
    raw_values: dict[str, object] = {
        "landing_column": row.get("landing_commit_sha"),
        "landing_decision": (landing_decisions.get(key) or {}).get("commit_sha"),
        "row_tip": row.get("branch_tip_sha"),
    }
    branch_ref = _branch_ref(row.get("branch"))
    if branch_ref is not None and branch_ref in branch_heads:
        raw_values["live_branch"] = branch_heads[branch_ref]
    raw_values["gate_row"] = (passing_gates.get(key) or {}).get("commit_sha")

    candidates: dict[str, dict[str, str]] = {}
    for kind in EVIDENCE_KINDS:
        candidate = _as_candidate(kind, raw_values.get(kind), malformed)
        if candidate is not None:
            candidates[kind] = candidate
    evidence_present: list[dict[str, Any]] = []
    for kind in EVIDENCE_KINDS:
        candidate = candidates.get(kind)
        if candidate is None:
            continue
        sha = candidate["sha"]
        probe = probes[sha]
        error = probe.error
        contained: bool | None
        if error == "commit_missing":
            contained = False
        elif error is not None:
            contained = None
        else:
            contained = bool(probe.contained)
        evidence_present.append(
            {
                "kind": kind,
                "sha": sha,
                "contained": contained,
                "carrier": probe.carrier,
                "lineage": probe.lineage,
                "error": error,
            }
        )

    lane_lookup, gate_lookup = _receipt_lookups(lane_rows, receipt_gates)
    receipt_result = _receipt_classification(repo, main_tip, receipts, lane_lookup, gate_lookup)
    has_column = "landing_column" in candidates
    if receipt_result is not None:
        classification = receipt_result
    elif any(item["error"] not in (None, "commit_missing") for item in evidence_present):
        failed = next(item for item in evidence_present if item["error"] not in (None, "commit_missing"))
        classification = {
            "class": "probe_failed",
            "evidence": failed["kind"],
            "evidence_sha": failed["sha"],
            "carrier": None,
            "lineage": None,
            "detail": f"{failed['kind']}:{failed['error']}",
        }
    elif not evidence_present:
        classification = {
            "class": "no_tip",
            "evidence": None,
            "evidence_sha": None,
            "carrier": None,
            "lineage": None,
            "detail": "no_present_evidence",
        }
    else:
        contained_items = [item for item in evidence_present if item["contained"] is True]
        absent_items = [item for item in evidence_present if item["contained"] is False]
        first = evidence_present[0]
        if len(contained_items) == len(evidence_present):
            classification = {
                "class": "derived_v1",
                "evidence": first["kind"],
                "evidence_sha": first["sha"],
                "carrier": first["carrier"],
                "lineage": first["lineage"],
                "detail": "all_present_evidence_contained",
            }
        elif contained_items:
            disagreements = [
                f"{item['kind']}:{'commit_missing' if item['error'] == 'commit_missing' else 'not_contained'}"
                for item in absent_items
            ]
            classification = {
                "class": "evidence_disputed",
                "evidence": first["kind"],
                "evidence_sha": first["sha"],
                "carrier": None,
                "lineage": None,
                "detail": ", ".join(disagreements),
            }
        else:
            missing = first["error"] == "commit_missing"
            classification = {
                "class": "tip_missing" if missing else "tip_unreachable",
                "evidence": first["kind"],
                "evidence_sha": first["sha"],
                "carrier": None,
                "lineage": None,
                "detail": "commit_missing" if missing else "not_contained",
            }

    return {
        "task_ref": task_ref,
        "lane_id": lane_id,
        "branch": row.get("branch"),
        "has_column": has_column,
        **classification,
        "evidence_present": evidence_present,
        "malformed": malformed,
    }


def build_report(repo: str | Path, *, main_ref: str = "main", timeout_s: float = 10.0) -> dict[str, Any]:
    """Build a read-only census of merged lane landing evidence."""
    started = time.monotonic()
    receipt_scan = _empty_receipt_scan()
    main_tip, pin_error = _pin_main(repo, main_ref, timeout_s)
    if pin_error is not None:
        return _assemble_report(
            main_ref=main_ref,
            main_tip=None,
            receipt_scan=receipt_scan,
            rows=[],
            complete=False,
            error=pin_error,
            started=started,
        )
    assert main_tip is not None

    try:
        scan: ScanResult = scan_first_parent(repo, main_ref=main_tip, timeout_s=timeout_s)
    except Exception as exc:  # noqa: BLE001 — keep a failed scan visible while classifying registry evidence
        scan = ScanResult(False, (), (), (), 0, 0.0, f"scan_failed:{type(exc).__name__}")
    receipt_pairs = tuple(scan.receipts)
    receipt_scan.update(
        {
            "complete": bool(scan.complete),
            "scanned": int(scan.scanned),
            "receipts": len(receipt_pairs),
            "unverified": len(scan.unverified),
            "error": scan.error,
        }
    )

    branch_heads, branch_error = _list_branch_heads(repo, timeout_s)
    if branch_heads is None:
        branch_heads = {}

    try:
        lane_rows, landing_decisions, passing_gates, receipt_gates = _read_registry_snapshot(receipt_pairs)
    except (RuntimeNotConfiguredError, sqlite3.Error) as exc:
        return _assemble_report(
            main_ref=main_ref,
            main_tip=main_tip,
            receipt_scan=receipt_scan,
            rows=[],
            complete=False,
            error=f"registry_unreadable:{type(exc).__name__}",
            started=started,
        )
    except Exception as exc:  # noqa: BLE001 — registry failures must not look like an empty successful census
        return _assemble_report(
            main_ref=main_ref,
            main_tip=main_tip,
            receipt_scan=receipt_scan,
            rows=[],
            complete=False,
            error=f"registry_unreadable:{type(exc).__name__}",
            started=started,
        )

    merged = {key: row for key, row in lane_rows.items() if str(row.get("status", "")).lower() == "merged"}
    merged_keys = set(merged)
    unmatched = [
        {"commit": commit, "task_ref": receipt.task_ref, "lane_id": receipt.lane_id}
        for commit, receipt in receipt_pairs
        if (receipt.task_ref, receipt.lane_id) not in merged_keys
    ]
    receipt_scan["unmatched_total"] = len(unmatched)
    receipt_scan["unmatched"] = unmatched[:50]

    evidence_shas: set[str] = set()
    row_inputs: list[tuple[tuple[str, str], dict[str, Any], list[tuple[str, LandingReceipt]]]] = []
    receipts_by_lane: dict[tuple[str, str], list[tuple[str, LandingReceipt]]] = {}
    for receipt_pair in receipt_pairs:
        receipt_commit, receipt = receipt_pair
        receipts_by_lane.setdefault((receipt.task_ref, receipt.lane_id), []).append((receipt_commit, receipt))
    for key, row in merged.items():
        branch_ref = _branch_ref(row.get("branch"))
        raw_values: dict[str, object] = {
            "landing_column": row.get("landing_commit_sha"),
            "landing_decision": (landing_decisions.get(key) or {}).get("commit_sha"),
            "row_tip": row.get("branch_tip_sha"),
            "live_branch": branch_heads.get(branch_ref) if branch_ref is not None else None,
            "gate_row": (passing_gates.get(key) or {}).get("commit_sha"),
        }
        scratch_malformed: list[dict[str, str]] = []
        for kind in EVIDENCE_KINDS:
            candidate = _as_candidate(kind, raw_values[kind], scratch_malformed)
            if candidate is not None:
                evidence_shas.add(candidate["sha"])
        row_inputs.append((key, row, receipts_by_lane.get(key, [])))

    probes: dict[str, CarrierResult] = {}
    for sha in sorted(evidence_shas):
        try:
            probes[sha] = find_carrier(repo, sha, main_ref=main_tip, timeout_s=timeout_s)
        except subprocess.TimeoutExpired:
            probes[sha] = CarrierResult(sha, main_tip, None, False, None, None, "timeout")
        except OSError as exc:
            probes[sha] = CarrierResult(sha, main_tip, None, False, None, None, f"git_failed:{exc}")
        except Exception as exc:  # noqa: BLE001 — retain an unexpected Git probe as a typed failure
            probes[sha] = CarrierResult(sha, main_tip, None, False, None, None, f"probe_failed:{type(exc).__name__}")

    rows = [
        _classify_row(
            repo=repo,
            main_tip=main_tip,
            row=row,
            branch_heads=branch_heads,
            lane_rows=lane_rows,
            landing_decisions=landing_decisions,
            passing_gates=passing_gates,
            receipt_gates=receipt_gates,
            receipts=receipts,
            probes=probes,
        )
        for _key, row, receipts in row_inputs
    ]
    rows.sort(key=lambda item: (item["task_ref"], item["lane_id"]))

    counts = _empty_counts()
    for row in rows:
        counts[row["class"]] += 1
    complete = bool(scan.complete and branch_error is None and counts["probe_failed"] == 0)
    error = branch_error or (scan.error if not scan.complete else None)
    if counts["probe_failed"] and error is None:
        error = "probe_failed"
    return _assemble_report(
        main_ref=main_ref,
        main_tip=main_tip,
        receipt_scan=receipt_scan,
        rows=rows,
        complete=complete,
        error=error,
        started=started,
    )


def _row_map(rows: list[dict[str, Any]]) -> dict[tuple[str, str], dict[str, Any]]:
    result: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        if isinstance(row, dict):
            result[(str(row.get("task_ref", "")), str(row.get("lane_id", "")))] = row
    return result


def _different_rows(left: object, right: object) -> int:
    left_rows = _row_map(left) if isinstance(left, list) else {}
    right_rows = _row_map(right) if isinstance(right, list) else {}
    keys = left_rows.keys() | right_rows.keys()
    return sum(
        1
        for key in keys
        if key not in left_rows
        or key not in right_rows
        or _canonical_json(left_rows[key]) != _canonical_json(right_rows[key])
    )


def _apply_result(
    outcome: str,
    reason: str | None,
    decision: str | None,
    digest: str | None,
    main_tip: str | None,
    differing_rows: int = 0,
) -> dict[str, Any]:
    return {
        "outcome": outcome,
        "reason": reason,
        "decision": decision,
        "digest": digest,
        "main_tip": main_tip,
        "differing_rows": differing_rows,
    }


def apply_report(
    repo: str | Path,
    report: object,
    *,
    task_ref: str,
    main_ref: str = "main",
    report_path: str | Path | None = None,
    session: str | None = None,
    record_decision: Callable[..., object] | None = None,
    timeout_s: float = 10.0,
) -> dict[str, Any]:
    """Replay and record one operator-approved census decision."""
    supplied = report if isinstance(report, dict) else {}
    supplied_digest = supplied.get("digest") if isinstance(supplied.get("digest"), str) else None
    main_tip = supplied.get("main_tip") if isinstance(supplied.get("main_tip"), str) else None
    decision = None
    try:
        valid_shape = (
            supplied.get("schema") == REPORT_SCHEMA
            and supplied.get("rule") == RULE
            and isinstance(main_tip, str)
            and _HEX40.fullmatch(main_tip) is not None
            and isinstance(supplied.get("rows"), list)
            and isinstance(supplied.get("receipt_scan"), dict)
        )
        calculated_digest = _digest(
            supplied.get("schema"),
            supplied.get("rule"),
            main_tip,
            supplied.get("receipt_scan"),
            supplied.get("rows"),
        )
        valid = bool(valid_shape and supplied_digest == calculated_digest)
    except (TypeError, ValueError, OverflowError):
        valid = False
    if not valid:
        return _apply_result("refused", "report_invalid", None, supplied_digest, main_tip)
    decision = f"landing_backfill_census_v1_{main_tip[:12]}_{supplied_digest[:12]}"
    if supplied.get("complete") is not True:
        return _apply_result("refused", "report_incomplete", decision, supplied_digest, main_tip)

    try:
        ancestry = _run_git(
            repo,
            "merge-base",
            "--is-ancestor",
            main_tip,
            main_ref,
            timeout_s=timeout_s,
        )
    except (subprocess.TimeoutExpired, OSError):
        return _apply_result("refused", "probe_failed", decision, supplied_digest, main_tip)
    if ancestry.returncode == 1:
        return _apply_result("refused", "pinned_tip_not_on_main", decision, supplied_digest, main_tip)
    if ancestry.returncode != 0:
        return _apply_result("refused", "probe_failed", decision, supplied_digest, main_tip)

    try:
        replay = build_report(repo, main_ref=main_tip, timeout_s=timeout_s)
    except Exception:  # noqa: BLE001 — never allow apply to write after an uncertain replay
        return _apply_result("refused", "replay_incomplete", decision, supplied_digest, main_tip)
    if replay.get("complete") is not True:
        return _apply_result("refused", "replay_incomplete", decision, supplied_digest, main_tip)
    if replay.get("digest") != supplied_digest:
        return _apply_result(
            "refused",
            "report_drift",
            decision,
            supplied_digest,
            main_tip,
            _different_rows(supplied.get("rows"), replay.get("rows")),
        )

    try:
        if record_decision is None:
            from workbay_handoff_mcp import record_decision as record_decision_fn  # noqa: PLC0415

            record_decision = record_decision_fn
        derived_counts = _empty_counts()
        for row in supplied["rows"]:
            if isinstance(row, dict) and row.get("class") in derived_counts:
                derived_counts[row["class"]] += 1
        count_text = ", ".join(f"{label}={derived_counts[label]}" for label in CLASSES)
        disputed = derived_counts["evidence_disputed"]
        rationale_parts = [
            f"landing census counts: {count_text}",
            f"digest={supplied_digest}",
            f"main_tip={main_tip}",
        ]
        if report_path is not None:
            rationale_parts.append(f"report_path={report_path}")
        rationale_parts.extend((f"evidence_disputed={disputed}", "phase 1: no per-row writes"))
        rationale = "; ".join(rationale_parts)[:1500]
        raw = record_decision(
            session=session or f"landing-backfill-{main_tip[:12]}",
            decision=decision,
            rationale=rationale,
            actor={"agent": "landing-backfill", "commit_sha": main_tip, "branch": main_ref},
            task_ref=task_ref,
        )
        if isinstance(raw, str):
            raw = json.loads(raw)
        if not isinstance(raw, dict) or raw.get("ok") is not True:
            error = raw.get("data", {}).get("error") if isinstance(raw, dict) else None
            return _apply_result(
                "write_failed", str(error or "record_decision_not_ok"), decision, supplied_digest, main_tip
            )
        mutation = raw.get("mutation") if isinstance(raw.get("mutation"), dict) else {}
        operation = mutation.get("operation")
        if operation == "insert":
            return _apply_result("applied", None, decision, supplied_digest, main_tip)
        if operation == "noop":
            return _apply_result("already_applied", None, decision, supplied_digest, main_tip)
        return _apply_result("write_failed", f"unexpected_mutation:{operation}", decision, supplied_digest, main_tip)
    except Exception as exc:  # noqa: BLE001 — caller receives the failed provenance write as data
        return _apply_result("write_failed", str(exc), decision, supplied_digest, main_tip)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="landing_backfill")
    commands = parser.add_subparsers(dest="command", required=True)
    report = commands.add_parser("report")
    report.add_argument("--repo", default=".")
    report.add_argument("--main-ref", default="main")
    report.add_argument("--out")
    report.add_argument("--timeout-s", type=float, default=10.0)
    apply = commands.add_parser("apply")
    apply.add_argument("--report", required=True)
    apply.add_argument("--task-ref", required=True)
    apply.add_argument("--repo", default=".")
    apply.add_argument("--main-ref", default="main")
    apply.add_argument("--session")
    apply.add_argument("--timeout-s", type=float, default=10.0)
    return parser


def _configure_runtime_for_cli(repo: str | Path) -> None:
    from workbay_handoff_mcp.runtime import configure_runtime, get_runtime_config  # noqa: PLC0415

    try:
        get_runtime_config()
    except RuntimeNotConfiguredError:
        from workbay_handoff_mcp.config import RuntimeConfig  # noqa: PLC0415

        configure_runtime(RuntimeConfig.for_repo(repo))


def _write_json_atomic(path: str | Path, payload: str) -> None:
    target = Path(path)
    parent = target.parent if str(target.parent) else Path(".")
    fd, temporary = tempfile.mkstemp(prefix=f".{target.name}.", suffix=".tmp", dir=parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            stream.write(payload)
            stream.write("\n")
        os.replace(temporary, target)
    except BaseException:
        try:
            os.unlink(temporary)
        except OSError:
            pass
        raise


def _emit_json(value: object) -> None:
    print(json.dumps(value, sort_keys=True, ensure_ascii=True))


def _report_summary(report: dict[str, Any]) -> str:
    counts = report.get("counts") if isinstance(report.get("counts"), dict) else {}
    return "counts: " + ", ".join(f"{label}={counts.get(label, 0)}" for label in CLASSES)


def main(argv: list[str] | None = None) -> int:
    """Run the operator-facing JSON report/apply interface."""
    try:
        args = _parser().parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)

    repo = Path(args.repo)
    try:
        _configure_runtime_for_cli(repo)
    except Exception as exc:  # noqa: BLE001 — CLI emits a typed, visible failure
        print(f"error: runtime_configuration_failed:{type(exc).__name__}:{exc}", file=sys.stderr)
        if args.command == "report":
            report = _assemble_report(
                main_ref=args.main_ref,
                main_tip=None,
                receipt_scan=_empty_receipt_scan(),
                rows=[],
                complete=False,
                error=f"registry_unreadable:{type(exc).__name__}",
                started=time.monotonic(),
            )
            _emit_json(report)
            print(_report_summary(report), file=sys.stderr)
            return 4
        result = _apply_result("write_failed", str(exc), None, None, None)
        _emit_json(result)
        return 5

    if args.command == "report":
        report = build_report(repo, main_ref=args.main_ref, timeout_s=args.timeout_s)
        payload = json.dumps(report, sort_keys=True, ensure_ascii=True)
        _emit_json(report)
        print(_report_summary(report), file=sys.stderr)
        if report.get("error"):
            print(f"error: {report['error']}", file=sys.stderr)
        elif report.get("complete") is not True:
            print("error: report_incomplete", file=sys.stderr)
        if args.out:
            try:
                _write_json_atomic(args.out, payload)
            except OSError as exc:
                print(f"error: report_write_failed:{exc}", file=sys.stderr)
                return 4
        return 0 if report.get("complete") is True else 4

    try:
        report_value = json.loads(Path(args.report).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        report_value = None
        load_error = str(exc)
    else:
        load_error = None
    if load_error is not None:
        result = _apply_result("refused", f"report_invalid:{load_error}", None, None, None)
    else:
        result = apply_report(
            repo,
            report_value,
            task_ref=args.task_ref,
            main_ref=args.main_ref,
            report_path=args.report,
            session=args.session,
            timeout_s=args.timeout_s,
        )
    _emit_json(result)
    summary = f"apply: outcome={result['outcome']} reason={result['reason'] or 'none'}"
    print(summary, file=sys.stderr)
    if result["outcome"] in {"applied", "already_applied"}:
        return 0
    if result["outcome"] == "refused":
        return 3
    return 5


if __name__ == "__main__":
    raise SystemExit(main())
