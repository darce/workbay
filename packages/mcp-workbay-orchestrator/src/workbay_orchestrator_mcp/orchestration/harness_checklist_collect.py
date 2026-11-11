"""Read-only collectors and replaceable detail cache for harness-checklist-v1.

Canon v0.26.2 (immutable INPUT/canon-rows.md): **Independent watcher the
optimizer cannot tune** [OBS-09], lexicons/engineering.md#obs-09, Thinking in
Systems ch. 6: claims never enter the observation channel. **Evidence before
label** [HAI-01], lexicons/interaction-ux.md#hai-01, Designing AI Interfaces /
Guidelines for Human-AI Interaction: retain source identities and bindings.
"""

from __future__ import annotations

import copy
import hashlib
import json
import os
import re
import sqlite3
import stat
import subprocess
import tempfile
import threading
import time
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

from workbay_handoff_mcp.config import RuntimeConfig
from workbay_handoff_mcp.shared_schema import bounded_handoff_read
from workbay_handoff_mcp.shared_write_context import write_deadline

from .harness_checklist import Claim, Evidence, Judgment, Observation, Observations, Subject, build_checklist

_SHA = re.compile(r"[0-9a-f]{40}\Z")
# A slow filesystem/receipt reader must not accumulate detached workers. Workers
# perform reads only and publish detached immutable inputs; late results are lost.
_SLOT = threading.Lock()


class UnresolvedSubjectError(ValueError):
    """A completed trusted lookup found no exact requested task/lane identity."""


def _remaining(deadline):
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("checklist deadline")
    return remaining


def _git(root, deadline, *args):
    env = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
    env["GIT_OPTIONAL_LOCKS"] = "0"
    proc = subprocess.run(
        ["git", "-C", str(root), *args],
        env=env,
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        timeout=_remaining(deadline),
        check=False,
    )
    if proc.returncode:
        raise OSError(f"git read failed: {args[0]}")
    return proc.stdout.strip()


def _ref(root, deadline, ref):
    if not isinstance(ref, str) or not ref:
        raise ValueError("ref unavailable")
    value = _git(root, deadline, "rev-parse", "--verify", "--end-of-options", f"{ref}^{{commit}}")
    if not _SHA.fullmatch(value):
        raise ValueError("ref is not a commit")
    return value


def _integration_checkout(root, deadline, ref):
    # Same canonical branch-to-worktree read used by handoff attribution. The
    # stored target_worktree_path is deprecated. Keep completed absence distinct
    # from a failed git lookup, and use NUL records for paths containing newlines.
    listing = _git(root, deadline, "worktree", "list", "--porcelain", "-z")
    for record in listing.split("\0\0"):
        fields = dict(field.split(" ", 1) for field in record.split("\0") if " " in field)
        if fields.get("branch") == ref:
            return fields.get("worktree")
    return None


def _utc(value):
    # SQLite's documented datetime('now') is UTC even without a suffix. Keep
    # missing/invalid times unavailable; reading a test never refreshes its age.
    if not isinstance(value, str):
        return None
    try:
        stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=timezone.utc)
    return stamp.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _json_file(path):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, "rb") as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_size > 1024 * 1024:
            raise ValueError("source file bound")
        data = stream.read(1024 * 1024 + 1)
        if len(data) > 1024 * 1024:
            raise ValueError("source file bound")
    value = json.loads(data)
    if not isinstance(value, dict):
        raise ValueError("source is not an object")
    return value


def _read_receipt(root, runtime, subject, lane, deadline):
    """Use the existing ancestry verifier with explicit read-only lookup paths.

    The existing subprocess deadline context bounds every nested git probe;
    the caller's single admitted daemon/watchdog also bounds stuck source reads.
    """
    from . import landing_log

    scan = landing_log.scan_first_parent(root, main_ref=subject.integration_ref, timeout_s=_remaining(deadline))
    if not scan.complete:
        if scan.error == "scan_timeout":
            raise TimeoutError("receipt scan incomplete")
        raise OSError("receipt scan unreadable")
    # Preserve find_landing's newest-first malformed-record precedence.
    for _, candidate in scan.ordered_receipts:
        if (candidate.task_ref, candidate.lane_id) != (subject.task_ref, subject.lane_id):
            continue
        if candidate.tip not in (None, subject.tip_sha):
            continue
        if isinstance(candidate, landing_log.UnverifiedReceipt):
            raise ValueError("receipt malformed")
        break
    unverified = False
    for commit, receipt in scan.receipts:
        _remaining(deadline)
        if (receipt.task_ref, receipt.lane_id, receipt.tip) != (subject.task_ref, subject.lane_id, subject.tip_sha):
            continue

        def gate_lookup(identifier):
            def read(conn):
                row = conn.execute("SELECT * FROM verified_tests WHERE id = ?", (identifier,)).fetchone()
                return {"ok": True, "test": dict(row) if row else None}

            return bounded_handoff_read(runtime.db_path, read, deadline=deadline)

        verdict = landing_log.verify_receipt(
            root,
            commit,
            main_ref=subject.integration_ref,
            lane_lookup=lambda task, lane_id: (
                {"ok": True, "lane": lane} if (task, lane_id) == (subject.task_ref, subject.lane_id) else {"ok": False}
            ),
            gate_lookup=gate_lookup,
        )
        _remaining(deadline)
        if verdict.verified:
            return commit
        if verdict.failed_check in {"v1_probe_failed", "v2_probe_failed", "v3_lookup_failed", "v4_lookup_failed"}:
            if verdict.detail == "timeout":
                raise TimeoutError("receipt probe timeout")
            raise OSError("receipt probe unreadable")
        unverified = True
    if unverified:
        raise ValueError("receipt unverified")
    return None


def _capture(root, task_ref, lane_id, deadline, publish):
    runtime = RuntimeConfig.for_repo(root)

    def identity(conn):
        task = conn.execute("SELECT * FROM handoff_state WHERE task_ref = ?", (task_ref,)).fetchone()
        lane = (
            conn.execute(
                "SELECT * FROM worktree_lanes WHERE task_ref = ? AND lane_id = ?", (task_ref, lane_id)
            ).fetchone()
            if lane_id is not None
            else None
        )
        return dict(task) if task else None, dict(lane) if lane else None

    task, lane = bounded_handoff_read(runtime.db_path, identity, deadline=deadline)
    if task is None or (lane_id is not None and lane is None):
        raise UnresolvedSubjectError("unresolved requested subject")
    # Lane status is the child chart's stored state. handoff_state.status is a
    # many-to-one projection of the outer machine, never a chart-state claim.
    subject = Subject(task_ref, lane_id, lane.get("status") if lane else None, None, None)
    publish(subject=subject)
    publish(chart=_json_file(Path(__file__).with_name("lifecycle_statechart.json")))
    branch = lane.get("branch") if lane else task.get("target_branch")
    target = task.get("target_branch")
    integration_ref = (
        target if isinstance(target, str) and target.startswith("refs/") else f"refs/heads/{target}" if target else None
    )
    issues = []
    tip = base = None
    try:
        tip = _ref(root, deadline, branch)
    except (OSError, ValueError):
        issues.append("tip_unreadable")
    try:
        base = _ref(root, deadline, integration_ref)
    except (OSError, ValueError):
        issues.append("integration_ref_unreadable")
    subject = replace(subject, tip_sha=tip, base_sha=base, integration_ref=integration_ref)
    publish(subject=subject, issues=tuple(issues))
    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    def evidence(**overrides):
        fields = dict(
            task_ref=task_ref, lane_id=lane_id, sha=tip, base_sha=base, command=None, parameters={}, observed_at=now
        )
        fields.update(overrides)
        return Evidence(**fields)

    def record(key, reader):
        _remaining(deadline)
        try:
            item = reader()
        except (TimeoutError, subprocess.TimeoutExpired):
            item = Observation(key, "timed_out", evidence())
            issues.append("deadline")
        except (OSError, sqlite3.Error):
            item = Observation(key, "unreadable", evidence())
            issues.append(f"{key}:unreadable")
        except (ValueError, KeyError, TypeError):
            item = Observation(key, "malformed", evidence())
            issues.append(f"{key}:malformed")
        publish(record=item, issues=tuple(issues))

    def db_read(reader):
        return bounded_handoff_read(runtime.db_path, reader, deadline=deadline)

    def test_at_tip():
        def read(conn):
            # Prefer the existing gate's passing exact-tip record, retaining a
            # previous measurement when none matches so STALE stays visible.
            row = conn.execute(
                "SELECT * FROM verified_tests WHERE task_ref = ? AND lane_id IS ? "
                "ORDER BY (commit_sha = ?) DESC, passed DESC, verified_at DESC, id DESC LIMIT 1",
                (task_ref, lane_id, tip),
            ).fetchone()
            return dict(row) if row else None

        row = db_read(read)
        if row is None:
            return Observation("verified_test_at_tip", "absent", evidence())
        return Observation(
            "verified_test_at_tip",
            "present",
            evidence(
                sha=row.get("commit_sha"),
                base_sha=None,
                command=row["command"],
                observed_at=_utc(row.get("verified_at")),
                verified_test_id=row["id"],
                parameters={"passed": bool(row["passed"]), "limit": 1, "requested_tip_sha": tip},
            ),
            {"passed": bool(row["passed"])},
        )

    record("verified_test_at_tip", test_at_tip)

    def findings():
        query = "SELECT COUNT(*) FROM review_findings WHERE task_ref = ? AND severity = 'high' AND status = 'open'"
        params = (task_ref,)
        if lane_id is not None:
            query += " AND lane_id = ?"
            params += (lane_id,)
        count = db_read(lambda conn: conn.execute(query, params).fetchone()[0])
        return Observation(
            "open_high_findings",
            "present",
            evidence(findings_count=count, parameters={"severity": "high", "status": "open", "since": None}),
            {"count": count},
        )

    record("open_high_findings", findings)

    def recent_tests():
        # SG's query is task-wide and accepts an explicitly absent cutoff.
        # Preserve that actual scope; never pretend this means a passing suite.
        count, row = db_read(
            lambda conn: (
                conn.execute("SELECT COUNT(*) FROM verified_tests WHERE task_ref = ?", (task_ref,)).fetchone()[0],
                conn.execute(
                    "SELECT * FROM verified_tests WHERE task_ref = ? ORDER BY verified_at DESC, id DESC LIMIT 1",
                    (task_ref,),
                ).fetchone(),
            )
        )
        return Observation(
            "recent_tests",
            "present",
            evidence(
                lane_id=None,
                sha=row["commit_sha"] if row else tip,
                command=row["command"] if row else None,
                observed_at=_utc(row["verified_at"]) if row else now,
                verified_test_id=row["id"] if row else None,
                parameters={"since": None},
            ),
            {"count": count},
        )

    record("recent_tests", recent_tests)
    if lane is not None:
        record(
            "lane_identity",
            lambda: Observation(
                "lane_identity",
                "present",
                evidence(
                    parameters={"branch": branch, "expected_tip": lane.get("branch_tip_sha"), "lane_row_id": lane["id"]}
                ),
                {"satisfied": tip is not None and tip == lane.get("branch_tip_sha")},
            ),
        )

        def integration_target():
            if base is None:
                raise OSError("integration ref unreadable")
            checkout = _integration_checkout(root, deadline, integration_ref)
            if checkout is None:
                return Observation("integration_target", "absent", evidence())
            head = _ref(checkout, deadline, "HEAD")
            dirty = _git(checkout, deadline, "status", "--porcelain", "--untracked-files=no")
            return Observation(
                "integration_target",
                "present",
                evidence(
                    parameters={"integration_ref": integration_ref, "checkout_head": head, "tracked_clean": not dirty}
                ),
                {"satisfied": head == base and not dirty},
            )

        record("integration_target", integration_target)

        def receipt():
            if tip is None or base is None:
                raise OSError("receipt binding unreadable")
            commit = _read_receipt(root, runtime, subject, lane, deadline)
            return Observation(
                "landing_receipt",
                "present" if commit else "absent",
                evidence(
                    receipt_path=f"git:{commit}" if commit else None, parameters={"integration_ref": integration_ref}
                ),
                {"verified": True, "commit": commit, "integration_ref": integration_ref} if commit else {},
            )

        record("landing_receipt", receipt)
    # No persisted harness-owned CG/GR result reader exists at this source base.
    # Omit those checks and merge_tree. Never execute gates or acquire locks.

    def claims_and_judgments(conn):
        reports = [
            dict(r)
            for r in conn.execute(
                "SELECT id,summary,merge_ready,created_at FROM worker_reports WHERE task_ref = ? AND lane_id IS ? ORDER BY id DESC LIMIT 33",
                (task_ref, lane_id),
            )
        ]
        messages = [
            dict(r)
            for r in conn.execute(
                "SELECT id,message,created_at FROM lane_messages WHERE task_ref = ? AND lane_id IS ? ORDER BY id DESC LIMIT 33",
                (task_ref, lane_id),
            )
        ]
        decisions = [
            dict(r)
            for r in conn.execute(
                "SELECT id,decision,created_at FROM decisions WHERE task_ref = ? AND lane_id IS ? "
                "AND (decision LIKE 'review_verdict:%' OR decision LIKE 'operator_hold:%') ORDER BY id DESC LIMIT 33",
                (task_ref, lane_id),
            )
        ]
        return reports, messages, decisions

    try:
        reports, messages, decisions = db_read(claims_and_judgments)
        claims = [
            Claim(
                "merge_ready",
                f"worker-report:{r['id']}",
                json.dumps(bool(r["merge_ready"])) + ": " + r["summary"],
                _utc(r["created_at"]),
                f"worker-report:{r['id']}",
            )
            for r in reports
        ]
        claims += [
            Claim(
                "agent_message",
                f"lane-message:{r['id']}",
                r["message"],
                _utc(r["created_at"]),
                f"lane-message:{r['id']}",
            )
            for r in messages
        ]
        judgments = [
            Judgment(
                r["decision"].split(":", 1)[0],
                r["id"],
                True,
                _utc(r["created_at"]),
                "harness_checklist_collect.decisions",
                f"decision:{r['id']}",
            )
            for r in decisions
        ]
        publish(claims=tuple(claims), judgments=tuple(judgments))
    except (OSError, sqlite3.Error):
        issues.append("claims_judgments:unreadable")
    # Enumerate only fixed producer filenames, never a task/lane-derived path.
    for index, path in enumerate(runtime.state_dir.glob("offload-pass-*.json")):
        _remaining(deadline)
        if index >= 1024:
            issues.append("source_cap")
            break
        try:
            payload = _json_file(path)
            if payload.get("task_ref") != task_ref or payload.get("lane_id") != lane_id:
                continue
            result = payload.get("result")
            if isinstance(result, dict) and "self_verify_gate" in result:
                publish(
                    claim=Claim(
                        "self_verify_gate",
                        path.stem,
                        json.dumps(result["self_verify_gate"], ensure_ascii=True),
                        _utc(payload.get("updated_at")),
                        str(path),
                    )
                )
        except (OSError, ValueError):
            issues.append("pass_claims:unreadable")
    publish(issues=tuple(issues))


def collect_checklist(root, task_ref, lane_id=None, *, deadline):
    """Collect under one absolute monotonic deadline; never write source/cache.

    Timeout and source failure produce partial unknown envelopes. Only a
    completed negative identity lookup raises UnresolvedSubjectError.
    """
    for value in (task_ref, lane_id):
        if value is not None and (not isinstance(value, str) or not value or len(value) > 1024):
            raise ValueError("invalid subject identity")
    if task_ref is None:
        raise ValueError("task_ref is required")
    end = min(deadline, time.monotonic() + 30)
    guard = threading.Lock()
    captured = dict(
        subject=Subject(task_ref, lane_id, None, None, None), records=(), claims=(), judgments=(), issues=(), chart={}
    )
    errors = []

    def publish(**items):
        with guard:
            for key, value in items.items():
                if key == "record":
                    captured["records"] += (value,)
                elif key == "claim":
                    captured["claims"] += (value,)
                else:
                    captured[key] = value

    def run():
        try:
            # This context changes only nested probe timeouts; it acquires no
            # locks, initializes no database and performs no writes.
            with write_deadline(end):
                _capture(Path(root), task_ref, lane_id, end, publish)
        except UnresolvedSubjectError as exc:
            with guard:
                errors.append(exc)
        except (TimeoutError, subprocess.TimeoutExpired):
            publish(issues=(*captured["issues"], "deadline"))
        except (OSError, sqlite3.Error, ValueError, TypeError, KeyError):
            publish(issues=(*captured["issues"], "subject_source_unreadable"))
        finally:
            _SLOT.release()

    worker = None
    if time.monotonic() < end and _SLOT.acquire(blocking=False):
        worker = threading.Thread(target=run, name="workbay-checklist-reader", daemon=True)
        worker.start()
        worker.join(max(0, end - time.monotonic()))
    with guard:
        data = dict(captured)
        error = errors[0] if errors else None
    if error is not None:
        raise error
    issues = set(data["issues"])
    if worker is None or worker.is_alive() or time.monotonic() >= end:
        issues.add("deadline")
    report = build_checklist(
        data["subject"],
        now=datetime.now(timezone.utc),
        observations=Observations(data["records"]),
        claims=data["claims"],
        judgments=data["judgments"],
        chart=data["chart"],
        pause_point="on_demand",
        deadline=end,
    )
    if issues:
        report.update(partial=True, counts=None, excluded_exit_count=None)
        report["limits_hit"] = sorted(set(report["limits_hit"]) | issues)
        report["diagnostics"] = sorted(set(report["diagnostics"]) | issues)
    return bound_checklist_detail(report)


def bound_checklist_detail(envelope):
    """Bound cache/output again after collector diagnostics, omitting whole items."""
    report = copy.deepcopy(envelope)

    def partial():
        report.update(partial=True, counts=None, excluded_exit_count=None)
        if "detail_cap" not in report["limits_hit"]:
            report["limits_hit"].append("detail_cap")

    def fits(value):
        if isinstance(value, str):
            return len(value) <= 1024
        if isinstance(value, dict):
            return all(fits(k) and fits(v) for k, v in value.items())
        if isinstance(value, list):
            return all(fits(v) for v in value)
        return True

    if not fits(report["subject"]):
        raise ValueError("oversized subject identity")
    for key, cap in (("rows", 64), ("claims", 32), ("judgments", 32), ("diagnostics", 64), ("limits_hit", 64)):
        kept = []
        for item in report[key]:
            if key == "rows":
                original = item["checks"]
                item["checks"] = [check for check in original[:8] if fits(check)]
                if len(original) != len(item["checks"]):
                    item.update(state="unknown", reason="detail_cap")
                    partial()
            if len(kept) < cap and fits(item):
                kept.append(item)
            else:
                partial()
        report[key] = kept

    def encoded_size():
        return len(json.dumps(report, sort_keys=True, allow_nan=False).encode("utf-8")) + 1

    while encoded_size() > 65536:
        partial()
        for key in ("claims", "judgments", "rows", "diagnostics"):
            if report[key]:
                report[key].pop()
                break
        else:
            raise ValueError("required envelope exceeds detail cap")
    return report


def write_checklist_cache(root, envelope):
    """Atomically replace a derived cache record; identity strings are never paths."""
    report = bound_checklist_detail(envelope)
    subject = report["subject"]
    identity = json.dumps([subject["task_ref"], subject["lane_id"]], ensure_ascii=True, separators=(",", ":"))
    digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()
    runtime = RuntimeConfig.for_repo(Path(root))
    directory = runtime.state_dir / "fleet" / "checklists"
    directory.mkdir(parents=True, exist_ok=True)
    destination = directory / f"{digest}.json"
    fd, temporary = tempfile.mkstemp(prefix=".checklist-", suffix=".tmp", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(report, stream, sort_keys=True, allow_nan=False)
            stream.write("\n")
        os.replace(temporary, destination)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
    return destination
