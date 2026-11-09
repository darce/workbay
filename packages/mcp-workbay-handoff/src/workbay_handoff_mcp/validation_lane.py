"""Trusted native validation carrier and lane policy guards.

JSON is a diagnostic projection, never a way to construct a milestone permit.
The in-process permit is not a same-UID security boundary.
"""

from __future__ import annotations

import hashlib
import json
import subprocess
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal


@dataclass(frozen=True)
class NativeValidationResult:
    applicable: bool
    readiness: Literal["pass", "fail", "unknown", "not_applicable"]
    qualified_pass: bool
    reason: str
    source: dict[str, Any] = field(default_factory=dict)
    policy_digest: str | None = None
    receipt_path: str | None = None
    receipt_sha256: str | None = None
    evidence_digest: str | None = None
    categories: dict[str, str] = field(default_factory=dict)
    runtime: dict[str, Any] = field(default_factory=dict)
    held_inputs: dict[str, Any] = field(default_factory=dict)
    attempt_id: str | None = None
    # Only the live consumer retains this permit; no deserializer exists.
    _observed: object | None = field(default=None, repr=False, compare=False)

    def __bool__(self) -> bool:
        raise TypeError("inspect native readiness and qualification explicitly")

    def as_dict(self) -> dict[str, Any]:
        return {
            key: getattr(self, key)
            for key in (
                "applicable",
                "readiness",
                "qualified_pass",
                "reason",
                "source",
                "policy_digest",
                "receipt_path",
                "receipt_sha256",
                "evidence_digest",
                "categories",
                "runtime",
                "held_inputs",
                "attempt_id",
            )
        } | {"review_required": True, "review_policy": "strict_until_independent_full92_backtest"}


_OBSERVED = object()


@dataclass(frozen=True)
class NativeReviewGate:
    validation: NativeValidationResult
    review_digest: str
    findings_digest: str
    _observed: object = field(default=None, repr=False)


def observed_review_gate(
    result: NativeValidationResult, review: dict[str, Any], findings_digest: str = ""
) -> NativeReviewGate:
    return NativeReviewGate(
        result, hashlib.sha256(json.dumps(review, sort_keys=True).encode()).hexdigest(), findings_digest, _OBSERVED
    )


def review_findings_snapshot(conn: Any, task_ref: str) -> str:
    rows = [
        dict(row)
        for row in conn.execute(
            "SELECT id,lane_id,finding_id,severity,file_path,status,impact,origin,updated_at "
            "FROM review_findings WHERE task_ref=? AND status='open' ORDER BY id LIMIT 5001",
            (task_ref,),
        )
    ]
    if len(rows) > 5000:
        raise ValueError("native review snapshot exceeds bounded coverage")
    return hashlib.sha256(json.dumps(rows, sort_keys=True).encode()).hexdigest()


def observed_result(**fields: Any) -> NativeValidationResult:
    """Harness-internal constructor, called only after supervised fresh execution."""
    return NativeValidationResult(**fields, _observed=_OBSERVED)


def current_source(repo: Path, source: dict[str, Any], *, deadline: float | None = None) -> bool:
    try:

        def git(*args: str) -> str:
            remaining = 5.0 if deadline is None else min(5.0, deadline - time.monotonic())
            if remaining <= 0:
                raise subprocess.TimeoutExpired(["git", *args], 0)
            return subprocess.run(
                ["git", "-C", str(repo), *args],
                check=True,
                capture_output=True,
                text=True,
                timeout=remaining,
            ).stdout.strip()

        tip = source["tip"]
        return (
            git("rev-parse", "HEAD") == tip["commit"]
            and git("rev-parse", "HEAD^{tree}") == tip["tree"]
            and not git("status", "--porcelain", "--untracked-files=normal")
        )
    except (KeyError, TypeError, ValueError, OSError, subprocess.SubprocessError):
        return False


def fresh_qualified(result: object, repo: Path) -> bool:
    if not isinstance(result, NativeValidationResult) or result._observed is not _OBSERVED:
        return False
    if result.qualified_pass is not True or result.readiness != "pass" or not result.applicable:
        return False
    try:
        raw = Path(result.receipt_path).read_bytes()
        if hashlib.sha256(raw).hexdigest() != result.receipt_sha256:
            return False
        document = json.loads(raw)
        evidence = document["execution_evidence"]
        if (
            document["profile"] != "full40377"
            or evidence["role"] != "positive_suite"
            or document["readiness"] != "pass"
            or evidence["qualification"]["qualified_pass"] is not True
            or evidence["evidence_digest"] != result.evidence_digest
            or {arm: evidence["identity"][arm] for arm in ("base", "tip")} != result.source
            or set(document["required_categories"]) != set(result.categories)
            or len(result.categories) != 6
            or any(document["categories"][name]["status"] != "pass" for name in result.categories)
            or any(value != "pass" for value in result.categories.values())
            or document["categories"]["acceptance"]["reason"] != "fix_observed"
        ):
            return False
    except (OSError, TypeError, ValueError, KeyError):
        return False
    return current_source(repo, result.source)


def register_policy(task_ref: str, lane_id: str, digest: str, attempt_id: str | None = None) -> None:
    """Activate policy only for this current task/lane; legacy rows are untouched."""
    from .shared_schema import _get_db_connection

    with _get_db_connection() as conn:
        prior = conn.execute(
            "SELECT status FROM worktree_lanes WHERE task_ref=? AND lane_id=?", (task_ref, lane_id)
        ).fetchone()
    if prior is not None and prior["status"] in {"validated", "gated"}:
        from .lanes_recording import update_lane

        invalidated = update_lane(task_ref=task_ref, lane_id=lane_id, status="active", expected_status=prior["status"])
        if invalidated.get("ok") is not True:
            raise ValueError("could not invalidate previous native milestone")
    with _get_db_connection(begin_immediate=True) as conn:
        row = conn.execute(
            "SELECT id, status, lane_kind FROM worktree_lanes WHERE task_ref=? AND lane_id=?",
            (task_ref, lane_id),
        ).fetchone()
        if row is None or row["status"] in {"merged", "closed", "closed_stale"} or row["lane_kind"] == "review":
            raise ValueError("native validation needs a live implementation lane")
        conn.execute(
            "INSERT INTO lane_validation_state(task_ref,lane_id,lane_row_id,policy_digest,attempt_id) VALUES(?,?,?,?,?) "
            "ON CONFLICT(lane_row_id) DO UPDATE SET policy_digest=excluded.policy_digest, task_ref=excluded.task_ref, lane_id=excluded.lane_id, "
            "attempt_id=excluded.attempt_id, milestone=NULL, result_json=NULL",
            (task_ref, lane_id, row["id"], digest, attempt_id or uuid.uuid4().hex),
        )


def transition_error(
    conn: Any, row: Any, target: str | None, permit: object = None, *, source_current: bool = False
) -> str | None:
    """Guard native authority, including old orphan labels, preserving legacy edges."""
    if target is None:
        return None
    policy = conn.execute(
        "SELECT * FROM lane_validation_state WHERE lane_row_id=?",
        (row["id"],),
    ).fetchone()
    if (
        policy is not None
        and (policy["task_ref"], policy["lane_id"]) != (row["task_ref"], row["lane_id"])
        and target in {"validated", "gated", "merged"}
    ):
        return "native validation identity changed; coordinator revalidation required"
    if target in {"validated", "gated"}:
        if row["status"] in {"merged", "closed", "closed_stale"}:
            return "terminal lane cannot acquire validation milestones"
        if target == "gated":
            if not isinstance(permit, NativeReviewGate) or permit._observed is not _OBSERVED:
                return "observed strict native review required after validation"
            if permit.findings_digest != review_findings_snapshot(conn, row["task_ref"]):
                return "review findings changed after native scope check"
            permit = permit.validation
            if (
                conn.execute(
                    "SELECT 1 FROM blockers WHERE task_ref=? AND (lane_id=? OR lane_id IS NULL) AND status='open' LIMIT 1",
                    (row["task_ref"], row["lane_id"]),
                ).fetchone()
                is not None
            ):
                return "existing task/lane blocker holds gating"
        if (
            policy is None
            or not source_current
            or not isinstance(permit, NativeValidationResult)
            or permit._observed is not _OBSERVED
        ):
            return "fresh harness full40377 qualification required"
        if permit.policy_digest != policy["policy_digest"]:
            return "native validation policy changed"
        if permit.attempt_id != policy["attempt_id"]:
            return "native validation attempt changed; fresh execution required"
        if target == "validated" and row["status"] not in {"active", "review", "blocked", "validated"}:
            return "validated requires an active implementation result"
        if target == "gated" and (row["status"] != "validated" or policy["milestone"] != "validated"):
            return "gated requires persisted validated first"
    elif target == "merged" and (policy is not None or row["status"] in {"validated", "gated"}):
        if policy is None:
            return "native milestone has no applicable validation policy; fresh qualification required"
        if row["status"] != "gated" or policy["milestone"] != "gated":
            return "active full40377 policy requires gated before merged"
        try:
            result = json.loads(policy["result_json"])
            if (
                result["applicable"] is not True
                or result["readiness"] != "pass"
                or result["qualified_pass"] is not True
                or result["policy_digest"] != policy["policy_digest"]
                or result["attempt_id"] != policy["attempt_id"]
                or not isinstance(result["review_digest"], str)
                or len(result["review_digest"]) != 64
            ):
                return "gated policy or review authority unavailable"
            event = conn.execute(
                "SELECT result_json FROM lane_validation_events WHERE lane_row_id=? AND task_ref=? "
                "AND lane_id=? AND milestone='gated' ORDER BY id DESC LIMIT 1",
                (row["id"], row["task_ref"], row["lane_id"]),
            ).fetchone()
            if event is None or event["result_json"] != policy["result_json"]:
                return "gated milestone audit unavailable or changed"
            if result.get("findings_digest") != review_findings_snapshot(conn, row["task_ref"]):
                return "gated review findings changed"
            if (
                conn.execute(
                    "SELECT 1 FROM blockers WHERE task_ref=? AND (lane_id=? OR lane_id IS NULL) AND status='open' LIMIT 1",
                    (row["task_ref"], row["lane_id"]),
                ).fetchone()
                is not None
            ):
                return "existing task/lane blocker holds merge"
            raw = Path(result["receipt_path"]).read_bytes()
            if hashlib.sha256(raw).hexdigest() != result["receipt_sha256"] or not source_current:
                return "gated source or custody changed"
        except (OSError, ValueError, TypeError, KeyError):
            return "gated custody unavailable"
    return None


def retain_native_exit_hold(conn: Any, row: Any) -> None:
    """An old orphan label may be invalidated, but cannot become legacy authority.

    Called in the lane producer's savepoint on the original row. This records
    only a hold, never qualification or a milestone, and preserves any existing
    policy. A fresh register_policy call replaces the hold with its new attempt.
    """
    if row is not None and row["status"] in {"validated", "gated"}:
        conn.execute(
            "INSERT INTO lane_validation_state(task_ref,lane_id,lane_row_id,policy_digest,attempt_id) "
            "VALUES(?,?,?,?,?) ON CONFLICT(lane_row_id) DO NOTHING",
            (row["task_ref"], row["lane_id"], row["id"], "unqualified-native-status", uuid.uuid4().hex),
        )


def policy_source_current(conn: Any, row: Any) -> bool:
    """Read-only preflight. Git must run before acquiring a write transaction."""
    policy = conn.execute(
        "SELECT result_json FROM lane_validation_state WHERE lane_row_id=?",
        (row["id"],),
    ).fetchone()
    try:
        return policy is not None and current_source(Path(row["worktree_path"]), json.loads(policy[0])["source"])
    except (ValueError, TypeError, KeyError):
        return False


def persist_milestone(conn: Any, row: Any, target: str, permit: NativeValidationResult | NativeReviewGate) -> None:
    view = (
        permit.validation.as_dict() | {"review_digest": permit.review_digest, "findings_digest": permit.findings_digest}
        if isinstance(permit, NativeReviewGate)
        else permit.as_dict()
    )
    conn.execute(
        "INSERT INTO lane_validation_events(task_ref,lane_id,lane_row_id,milestone,result_json) VALUES(?,?,?,?,?)",
        (row["task_ref"], row["lane_id"], row["id"], target, json.dumps(view, sort_keys=True)),
    )
    conn.execute(
        "UPDATE lane_validation_state SET milestone=?,result_json=? WHERE lane_row_id=?",
        (target, json.dumps(view, sort_keys=True), row["id"]),
    )


def publish_milestone(
    task_ref: str, lane_id: str, target: str, result: NativeValidationResult | NativeReviewGate
) -> dict[str, Any]:
    from .lanes_recording import update_lane

    return update_lane(lane_id=lane_id, task_ref=task_ref, status=target, _validation_permit=result)
