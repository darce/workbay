"""Native FULL40377 consumer shared by worker and one-pass offload.

The coordinator configures WORKBAY_NATIVE_VALIDATION_CONFIG and its held
WORKBAY_NATIVE_VALIDATION_DIGEST before execution. The config is a request,
never an attestation. Only a fresh supervised production battery can qualify.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import shutil
import signal
import sqlite3
import subprocess
import sys
import time
import uuid
from dataclasses import replace
from pathlib import Path
from typing import Any

from workbay_handoff_mcp.validation_lane import (
    NativeValidationResult,
    current_source,
    fresh_qualified,
    observed_result,
    observed_review_gate,
    publish_milestone,
    register_policy,
)

from . import validation_battery as battery

FREE_FLOOR = 6 * 1024**3
REQUIRED = tuple(battery.FULL40377_REQUIRED)


def _cancelled() -> bool:
    # These are the existing native worker SIGTERM latches, including the
    # direct script entrypoint and the supported bare-module loader.
    return any(
        getattr(sys.modules.get(name), "_shutdown_requested", False) is True
        for name in ("workbay_orchestrator_mcp.orchestration.worker_daemon", "worker_daemon", "__main__")
    )


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode()


def _json(raw: bytes) -> dict[str, Any]:
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError(f"duplicate key: {key}")
            result[key] = value
        return result

    def nonfinite(value):
        raise ValueError(f"nonfinite number: {value}")

    result = json.loads(raw, object_pairs_hook=pairs, parse_constant=nonfinite)
    if not isinstance(result, dict):
        raise ValueError("expected a JSON object")
    return result


def select_request_pair(path: Any, digest: Any) -> tuple[Any, Any]:
    """Select one authority pair without repairing or coercing invalid intent."""
    if path is None and digest is None:
        return (
            os.environ.get("WORKBAY_NATIVE_VALIDATION_CONFIG"),
            os.environ.get("WORKBAY_NATIVE_VALIDATION_DIGEST"),
        )
    return path, digest


def configure(config: Any) -> Any:
    """Capture one coordinator pair before execution, including partial signals.

    Explicit inputs select the explicit pair; ambient inputs cannot complete or
    replace it. Only the absence of BOTH channels selects legacy verification.
    Keep malformed/empty values for the native consumer's typed UNKNOWN result.
    """
    path, digest = select_request_pair(
        getattr(config, "native_validation_config", None), getattr(config, "native_validation_digest", None)
    )
    if path is not None or digest is not None:
        if path is None:
            path = Path("/missing-native-validation-config")
        elif isinstance(path, str) and path:
            path = Path(path)
        return replace(
            config,
            native_validation_config=path,
            native_validation_digest=digest,
        )
    return config


def _unknown(reason: str, digest: str | None = None, **fields: Any) -> NativeValidationResult:
    return NativeValidationResult(True, "unknown", False, reason, policy_digest=digest, **fields)


def _held_path(value: object, candidate_roots: list[Path], *, directory: bool = False) -> Path:
    if not isinstance(value, str) or not Path(value).is_absolute():
        raise ValueError("held input paths must be absolute")
    path = Path(value).resolve(strict=True)
    if any(path.is_relative_to(root) for root in candidate_roots):
        raise ValueError(f"held input overlaps candidate write root: {path}")
    if directory:
        if not path.is_dir():
            raise ValueError("held cases root must be a directory")
    elif not path.is_file():
        raise ValueError("held input must be a readable regular file")
    return path


def load_request(
    config: Any, *, deadline: float | None = None, started: float | None = None
) -> tuple[dict[str, Any], Path]:
    started = time.monotonic() if started is None else started
    repo = Path(config.worktree_path).resolve()
    roots = [repo, Path(config.orchestrator_root).resolve()]
    if not isinstance(config.native_validation_config, (str, Path)) or not str(config.native_validation_config).strip():
        raise ValueError("native config path must be a nonempty path string")
    path = _held_path(str(config.native_validation_config), roots)
    raw = path.read_bytes()
    if not isinstance(config.native_validation_digest, str) or _sha(raw) != config.native_validation_digest:
        raise ValueError("native config held digest mismatch or missing")
    request = _json(raw)
    if request.get("schema") != "workbay/native-validation-config@1" or request.get("profile") != "full40377":
        raise ValueError("native consumer requires explicit full40377 config@1")
    if (request.get("task_ref"), request.get("lane_id")) != (config.task_ref, config.lane_id):
        raise ValueError("native config task/lane mismatch")
    if request.get("evidence_role") != "positive_suite":
        raise ValueError("native qualification requires held positive_suite role")
    bound = request.get("deadline_seconds")
    if type(bound) not in (int, float) or not math.isfinite(bound) or not 0 < bound <= 3600:
        raise ValueError("bounded deadline_seconds required")
    deadline = min(started + bound, deadline if deadline is not None else float("inf"))
    source = request.get("source")
    if not isinstance(source, dict) or set(source) != {"base", "tip"}:
        raise ValueError("exact base/tip source required")
    for arm in ("base", "tip"):
        identity = source[arm]
        for key in ("commit", "tree"):
            value = identity[key]
            if not isinstance(value, str) or len(value) != 40 or any(c not in "0123456789abcdef" for c in value):
                raise ValueError("full source commit/tree required")
        remaining = min(5, deadline - time.monotonic())
        if remaining <= 0:
            raise ValueError("battery setup deadline")
        actual_tree = subprocess.run(
            ["git", "-C", str(repo), "rev-parse", identity["commit"] + "^{tree}"],
            capture_output=True,
            text=True,
            check=True,
            timeout=remaining,
        ).stdout.strip()
        if actual_tree != identity["tree"]:
            raise ValueError("source tree mismatch")
    if not current_source(repo, source, deadline=deadline):
        raise ValueError("source tip stale or worktree dirty")
    for name in ("test_cmd", "suite_cmd"):
        if not isinstance(request.get(name), str) or not request[name].strip():
            raise ValueError(f"readable full request requires {name}")
    for name in ("model_max_depth", "model_max_states", "max_hunks"):
        if type(request.get(name)) is not int or request[name] <= 0:
            raise ValueError(f"explicit positive {name} required")
    if request["max_hunks"] > battery.MAX_HUNKS:
        raise ValueError("max_hunks exceeds production bound")
    if type(request.get("seed")) is not int or not 0 <= request["seed"] < 2**32:
        raise ValueError("explicit unsigned seed required")
    for name in ("deadline_seconds", "model_deadline_seconds"):
        if type(request.get(name)) not in (int, float) or not math.isfinite(request[name]) or request[name] <= 0:
            raise ValueError(f"bounded {name} required")
    if request["deadline_seconds"] > 3600 or request["model_deadline_seconds"] > request["deadline_seconds"]:
        raise ValueError("invalid whole battery/model deadline")
    for name in ("acceptance_manifest", "acceptance_cases_root", "regression_selection", "regression_catalog"):
        request[name] = str(_held_path(request.get(name), roots, directory=name == "acceptance_cases_root"))
    for name in ("acceptance_expected_digest", "regression_expected_digest"):
        value = request.get(name)
        if not isinstance(value, str) or len(value) != 64 or any(c not in "0123456789abcdef" for c in value):
            raise ValueError(f"held {name} required")
    receipt_root = _held_path(request.get("receipt_root"), roots, directory=True)
    # This location belongs to the trusted control plane, separate from the
    # candidate's roots. Same UID can still interfere: this is not isolation.
    return request, receipt_root


def _snapshot_descendants(pid: int, deadline: float) -> list[Any]:
    """Inventory with normal short-lived child exit treated as absence.

    The worker's timeout-only inventory deliberately refuses a changing tree.
    Here Git/pytest children finish during routine polling; a vanished PID is
    not an inventory error. Identity changes and unavailable reads still hold.
    """
    from .worker_daemon import _process_descendant_pids, _process_identity, _ProcessIdentity

    if not sys.platform.startswith("linux"):
        return _process_descendant_pids(pid, deadline)
    children: dict[int, list[tuple[int, str]]] = {}
    for entry in Path("/proc").iterdir():
        if not entry.name.isdecimal():
            continue
        if time.monotonic() >= deadline:
            raise OSError("descendant inventory deadline")
        try:
            identity = _process_identity(int(entry.name))
        except (FileNotFoundError, ProcessLookupError):
            continue
        children.setdefault(identity[2], []).append((int(entry.name), identity[1]))
    pending = list(children.get(pid, []))
    seen = {pid}
    records = []
    try:
        while pending:
            child, start = pending.pop()
            if child in seen:
                raise OSError("inconsistent descendant tree")
            seen.add(child)
            pending.extend(children.get(child, []))
            handle = None
            try:
                before = _process_identity(child)
                if hasattr(os, "pidfd_open") and hasattr(signal, "pidfd_send_signal"):
                    try:
                        handle = os.pidfd_open(child)
                    except ProcessLookupError:
                        continue
                after = _process_identity(child)
            except (FileNotFoundError, ProcessLookupError):
                if handle is not None:
                    os.close(handle)
                continue
            except BaseException:
                if handle is not None:
                    os.close(handle)
                raise
            record = _ProcessIdentity(child, after[0], after[1], handle)
            records.append(record)
            # Scheduling state and parent/group membership can change while a
            # child exits or calls setsid. UID + start (and pidfd) bind identity.
            if record.start != start or before[:2] != after[:2]:
                raise OSError("descendant identity changed")
        return records
    except BaseException:
        for record in records:
            if record.handle is not None:
                os.close(record.handle)
        raise


def _supervise(argv: list[str], root: Path, timeout: float, env: dict[str, str]) -> tuple[int, str | None]:
    """Bound setup/battery/descendants and continuously watch the free floor."""
    if shutil.disk_usage(root).free < FREE_FLOOR:
        return 2, "disk_below_6GiB"
    if _cancelled():
        return 2, "battery_cancelled"
    deadline = time.monotonic() + timeout
    from .worker_daemon import _identity_live

    descendants: dict[tuple[int, str], Any] = {}
    errors = []
    with (root / "stdout.log").open("xb") as stdout, (root / "stderr.log").open("xb") as stderr:
        process = subprocess.Popen(argv, stdout=stdout, stderr=stderr, env=env, start_new_session=True)
        reason = None
        try:
            while process.poll() is None:
                if _cancelled():
                    reason = "battery_cancelled"
                    break
                if time.monotonic() >= deadline:
                    reason = "battery_deadline"
                    break
                try:
                    for record in _snapshot_descendants(process.pid, min(deadline, time.monotonic() + 0.5)):
                        key = (record.pid, record.start)
                        if key in descendants and record.handle is not None:
                            os.close(record.handle)
                        else:
                            descendants[key] = record
                except (OSError, ValueError, IndexError, subprocess.SubprocessError) as exc:
                    if process.poll() is None:
                        errors.append(f"{type(exc).__name__}: {exc}")
                        reason = (
                            "battery_deadline"
                            if time.monotonic() >= deadline
                            else f"descendant_inventory_unknown: {type(exc).__name__}: {exc}"
                        )
                        break
                if shutil.disk_usage(root).free < FREE_FLOOR:
                    reason = "disk_below_6GiB"
                    break
                if time.monotonic() >= deadline:
                    reason = "battery_deadline"
                    break
                time.sleep(min(0.1, max(0, deadline - time.monotonic())))
        finally:
            # Also clean descendants whose leader exited. Files, not pipes,
            # prevent inherited output handles from blocking completion.
            try:
                for sig in (signal.SIGTERM, signal.SIGKILL):
                    for record in descendants.values():
                        try:
                            if _identity_live(record):
                                if record.handle is not None:
                                    signal.pidfd_send_signal(record.handle, sig)
                                else:
                                    os.kill(record.pid, sig)
                        except (OSError, ValueError, IndexError):
                            reason = reason or "cleanup_unknown"
                    # Never signal a numeric group after reaping its leader:
                    # the old PID could then name an unrelated new session.
                    # Bound descendant handles remain valid independently.
                    if process.returncode is None:
                        try:
                            os.killpg(process.pid, sig)
                        except ProcessLookupError:
                            pass
                    try:
                        process.wait(timeout=0.5)
                    except subprocess.TimeoutExpired:
                        pass
                process.wait(timeout=2)
                cleanup_deadline = time.monotonic() + 1
                for record in descendants.values():
                    try:
                        while _identity_live(record) and time.monotonic() < cleanup_deadline:
                            time.sleep(0.01)
                        if _identity_live(record):
                            reason = reason or "cleanup_unknown"
                    except (OSError, ValueError, IndexError):
                        reason = reason or "cleanup_unknown"
            finally:
                for record in descendants.values():
                    if record.handle is not None:
                        os.close(record.handle)
    (root / "supervision.json").write_bytes(
        _canonical(dict(returncode=process.returncode, reason=reason, errors=errors))
    )
    return process.returncode, reason


def _check_report(document: dict[str, Any], request: dict[str, Any], returncode: int) -> bool:
    evidence = document["execution_evidence"]
    identity = evidence["identity"]
    if {arm: identity[arm] for arm in ("base", "tip")} != request["source"]:
        raise ValueError("observed battery source mismatch")
    if evidence["role"] != "positive_suite" or document["profile"] != "full40377":
        raise ValueError("unqualified battery role/profile")
    if evidence["evidence_digest"] != _sha(_canonical({k: v for k, v in evidence.items() if k != "evidence_digest"})):
        raise ValueError("battery execution digest mismatch")
    if document["validation_digest"] != battery.validation_digest(document):
        raise ValueError("battery validation digest mismatch")
    if evidence["categories_sha256"] != _sha(_canonical(document["categories"])):
        raise ValueError("battery categories digest mismatch")
    if identity["runtime"]["python"]["binary_sha256"] != _sha(Path(sys.executable).resolve().read_bytes()):
        raise ValueError("battery runtime mismatch")
    if set(document["required_categories"]) != set(REQUIRED):
        raise ValueError("battery required category mismatch")
    inputs = document["inputs"]
    for key in (
        "test_cmd",
        "suite_cmd",
        "seed",
        "max_hunks",
        "acceptance_manifest",
        "acceptance_cases_root",
        "acceptance_expected_digest",
    ):
        if inputs[key] != request[key]:
            raise ValueError(f"battery {key} mismatch")
    if inputs["model_bounds"] != dict(
        max_depth=request["model_max_depth"],
        max_states=request["model_max_states"],
        deadline_seconds=request["model_deadline_seconds"],
        seed=request["seed"],
    ):
        raise ValueError("observed MODEL bounds mismatch")
    if inputs["regression"] != dict(
        selection_path=request["regression_selection"],
        expected_digest=request["regression_expected_digest"],
        catalog_path=request["regression_catalog"],
    ):
        raise ValueError("observed regression pins mismatch")
    if evidence.get("external_contract", {}).get("expected_digest") != request["acceptance_expected_digest"]:
        raise ValueError("observed frozen acceptance digest mismatch")
    return (
        returncode == 0
        and document["readiness"] == "pass"
        and all(document["categories"].get(name, {}).get("status") == "pass" for name in REQUIRED)
        and document["categories"]["acceptance"].get("reason") == "fix_observed"
        and evidence["qualification"].get("qualified_pass") is True
        and evidence["qualification"].get("acceptance_mode") == "external_frozen_contract"
    )


def run_native(config: Any, *, deadline: float | None = None, attempt_id: str | None = None) -> NativeValidationResult:
    started = time.monotonic()
    config = configure(config)
    if getattr(config, "native_validation_config", None) is None:
        return NativeValidationResult(False, "not_applicable", False, "legacy_strict_review_only")
    digest = config.native_validation_digest
    try:
        if _cancelled():
            return _unknown("battery_cancelled", digest)
        if shutil.disk_usage(config.worktree_path).free < FREE_FLOOR:
            return _unknown("disk_below_6GiB", digest)
        request, receipt_root = load_request(config, deadline=deadline, started=started)
        if config.dry_run:
            return _unknown("dry_run_is_not_validation", digest, source=request["source"])
        if shutil.disk_usage(receipt_root).free < FREE_FLOOR:
            return _unknown("disk_below_6GiB", digest, source=request["source"])
        timeout = request["deadline_seconds"] - (time.monotonic() - started)
        if deadline is not None:
            timeout = min(timeout, deadline - time.monotonic())
        if timeout <= 0:
            return _unknown("battery_deadline", digest, source=request["source"])
        root = receipt_root / ("native-" + uuid.uuid4().hex)
        root.mkdir(mode=0o700)
        receipt = root / "battery.json"
        argv = [
            sys.executable,
            "-P",
            "-m",
            battery.__name__,
            "--repo",
            str(config.worktree_path),
            "--base",
            request["source"]["base"]["commit"],
            "--tip",
            request["source"]["tip"]["commit"],
            "--profile",
            "full40377",
            "--evidence-role",
            "positive_suite",
            "--out",
            str(receipt),
        ]
        for key in (
            "test_cmd",
            "suite_cmd",
            "seed",
            "max_hunks",
            "model_max_depth",
            "model_max_states",
            "model_deadline_seconds",
            "acceptance_manifest",
            "acceptance_cases_root",
            "acceptance_expected_digest",
            "regression_selection",
            "regression_expected_digest",
            "regression_catalog",
        ):
            argv += ["--" + key.replace("_", "-"), str(request[key])]
        # Trusted installed harness paths; never point the driver at the lane.
        roots = [
            str(Path(battery.__file__).parents[2]),
            str(Path(sys.modules[NativeValidationResult.__module__].__file__).parents[1]),
        ]
        protocol = Path(battery.__file__).parents[4] / "workbay-protocol" / "src"
        if protocol.is_dir():
            roots.append(str(protocol))
        env = {k: v for k, v in os.environ.items() if not k.startswith("PYTHON") and not k.startswith("PYTEST")}
        env.update(
            PYTHONPATH=os.pathsep.join(roots),
            PYTHONDONTWRITEBYTECODE="1",
            PYTEST_DISABLE_PLUGIN_AUTOLOAD="1",
            TMPDIR=str(root),
        )
        (root / "invocation.json").write_bytes(
            _canonical(dict(argv=argv, source=request["source"], policy_digest=digest, attempt_id=attempt_id))
        )
        rc, refusal = _supervise(argv, root, timeout, env)
        if refusal is not None:
            return _unknown(refusal, digest, source=request["source"], receipt_path=str(receipt))
        raw = receipt.read_bytes()
        document = _json(raw)
        qualified = _check_report(document, request, rc)
        # Re-read held config and source after candidate processes and before issuance.
        load_request(config, deadline=deadline, started=started)
        if time.monotonic() - started > request["deadline_seconds"] or (
            deadline is not None and time.monotonic() > deadline
        ):
            return _unknown("battery_deadline", digest, source=request["source"], receipt_path=str(receipt))
        evidence = document["execution_evidence"]
        readiness = document["readiness"] if document["readiness"] in {"pass", "fail", "unknown"} else "unknown"
        if readiness == "pass" and not qualified:
            readiness = "unknown"
        return observed_result(
            applicable=True,
            readiness=readiness,
            qualified_pass=qualified,
            reason="fresh_full40377_qualified" if qualified else "below_floor_strict_review_required",
            source=request["source"],
            policy_digest=digest,
            receipt_path=str(receipt),
            receipt_sha256=_sha(raw),
            evidence_digest=evidence["evidence_digest"],
            categories={name: document["categories"].get(name, {}).get("status", "unknown") for name in REQUIRED},
            runtime=evidence["identity"]["runtime"],
            attempt_id=attempt_id,
            held_inputs={
                key: request[key]
                for key in (
                    "profile",
                    "evidence_role",
                    "acceptance_manifest",
                    "acceptance_cases_root",
                    "acceptance_expected_digest",
                    "regression_selection",
                    "regression_expected_digest",
                    "regression_catalog",
                    "seed",
                    "max_hunks",
                    "model_max_depth",
                    "model_max_states",
                    "model_deadline_seconds",
                    "test_cmd",
                    "suite_cmd",
                )
            },
        )
    except (OSError, ValueError, TypeError, KeyError, subprocess.SubprocessError) as exc:
        return _unknown(f"{type(exc).__name__}: {exc}", digest)


def _publish(
    ctx: Any,
    target: str,
    result: NativeValidationResult,
    *,
    review: dict[str, Any] | None = None,
    findings_digest: str = "",
) -> dict[str, Any]:
    from workbay_handoff_mcp.lanes_recording import get_lane

    from .statechart_conformance import (
        ChartUnavailable,
        ConformanceError,
        assert_validation_transition,
        load_runtime_chart,
    )

    row = get_lane(task_ref=ctx.config.task_ref, lane_id=ctx.config.lane_id)
    if row.get("ok") is not True:
        raise ValueError("native lane unavailable before milestone")
    chart = load_runtime_chart()
    if isinstance(chart, ChartUnavailable):
        raise ValueError(str(chart))
    try:
        assert_validation_transition(
            chart,
            row["data"]["lane"]["status"],
            target,
            active_policy=result.applicable,
            fresh_qualified=fresh_qualified(result, Path(ctx.config.worktree_path)),
            strict_review=review is not None,
        )
    except ConformanceError as exc:
        raise ValueError(str(exc)) from exc
    permit = observed_review_gate(result, review, findings_digest) if review is not None else result
    return publish_milestone(ctx.config.task_ref, ctx.config.lane_id, target, permit)


def prepare_native(ctx: Any) -> None:
    """Invalidate cycle-local authority before another native execute begins."""
    ctx.config = configure(ctx.config)
    ctx.native_validation_result = None
    ctx.native_validation_milestone = None
    ctx.native_validation_error = None
    ctx.native_validation_attempt_id = uuid.uuid4().hex
    ctx.native_validation_prepared = True
    if (
        ctx.config.native_validation_config is not None
        and getattr(ctx, "lane_kind", None) != "review"
        and not ctx.config.dry_run
    ):
        try:
            register_policy(
                ctx.config.task_ref,
                ctx.config.lane_id,
                ctx.config.native_validation_digest or "missing",
                ctx.native_validation_attempt_id,
            )
        except (OSError, ValueError, RuntimeError, sqlite3.Error) as exc:
            ctx.log("ERROR", "native_validation_policy_unknown", reason=f"{type(exc).__name__}: {exc}")


def begin_native(ctx: Any, *, deadline: float | None = None) -> NativeValidationResult:
    ctx.config = configure(ctx.config)
    if not getattr(ctx, "native_validation_prepared", False):
        ctx.native_validation_attempt_id = uuid.uuid4().hex
    ctx.native_validation_prepared = False
    active = ctx.config.native_validation_config is not None and getattr(ctx, "lane_kind", None) != "review"
    try:
        if active and not ctx.config.dry_run:
            register_policy(
                ctx.config.task_ref,
                ctx.config.lane_id,
                ctx.config.native_validation_digest or "missing",
                ctx.native_validation_attempt_id,
            )
        result = (
            run_native(ctx.config, deadline=deadline, attempt_id=ctx.native_validation_attempt_id)
            if active
            else NativeValidationResult(False, "not_applicable", False, "legacy_or_review_only_strict_review")
        )
    except (OSError, ValueError, RuntimeError, sqlite3.Error) as exc:
        result = _unknown(f"native_policy_persistence: {exc}", ctx.config.native_validation_digest)
    ctx.native_validation_result = result
    ctx.native_validation_milestone = None
    ctx.native_validation_error = None
    if result.applicable:
        try:
            if result.qualified_pass:
                receipt = _publish(ctx, "validated", result)
                if receipt.get("ok") is not True:
                    raise ValueError(f"validated persistence refused: {receipt}")
                ctx.native_validation_milestone = "validated"
        except (OSError, ValueError, RuntimeError, sqlite3.Error) as exc:
            ctx.native_validation_error = f"native_policy_persistence: {exc}"
    ctx.log(
        "INFO",
        "native_validation_complete",
        native_validation=result.as_dict(),
        milestone=ctx.native_validation_milestone,
        milestone_error=ctx.native_validation_error,
    )
    return result


def finish_native(ctx: Any, review: dict[str, Any], check: Any) -> Any:
    """After strict native review and existing verification, publish gated."""
    from .review_runner import blocking_findings, review_result_is_harvestable
    from .worker_daemon import LaneGateResult

    result = getattr(ctx, "native_validation_result", None)
    if result is None:
        result = begin_native(ctx)
    elif not isinstance(result, NativeValidationResult):
        result = _unknown("untrusted_native_result_carrier", ctx.config.native_validation_digest)
        ctx.native_validation_result = result
    if not result.applicable:
        return check
    hold = None
    if getattr(ctx, "adapter_refusal_evidence_error", None) or getattr(ctx, "checkpoint_failed", False):
        hold = "existing_native_source_or_checkpoint_hold"
    if getattr(ctx.config, "pass_id", None):
        from .offload_pass import read_pass_state

        try:
            state = read_pass_state(ctx.state_dir, ctx.config.pass_id)
            custody = state.get("custody_hold") if isinstance(state, dict) else None
            if not isinstance(state, dict):
                hold = "native_custody_lookup_unknown"
            elif isinstance(custody, dict) and custody.get("active") is True:
                hold = "existing_native_custody_hold"
        except (OSError, ValueError, TypeError) as exc:
            hold = f"native_custody_lookup_unknown: {exc}"
    fresh = fresh_qualified(result, Path(ctx.config.worktree_path))
    subject = review.get("review_subject")
    review_ok = (
        review.get("converged") is True
        and isinstance(review.get("findings"), (list, tuple))
        and all(isinstance(finding, dict) for finding in review["findings"])
        and not blocking_findings(review["findings"])
        and review_result_is_harvestable(review)
        and review.get("result_parse") == "ok"
        and review.get("review") in (None, "ok")
        and isinstance(subject, dict)
        and subject.get("kind") == "bounded"
        and subject.get("scope_kind") != "unresolved_base"
        and subject.get("tip_ref") == result.source.get("tip", {}).get("commit")
    )
    gate_ok = (
        isinstance(check, dict)
        and check.get("passed") is True
        and check.get("status") == "passed"
        and type(check.get("exit_code")) is int
        and check.get("exit_code") == 0
        and check.get("skipped") is not True
        and check.get("cleanup_outcome") != "unknown"
    )
    allowed = fresh and gate_ok and review_ok and hold is None and ctx.native_validation_milestone == "validated"
    reason = getattr(ctx, "native_validation_error", None) or result.reason
    if allowed:
        try:
            from workbay_handoff_mcp.shared_schema import _get_db_connection
            from workbay_handoff_mcp.validation_lane import review_findings_snapshot

            from .offload_pass import _scope_lane_open_findings

            with _get_db_connection() as conn:
                findings_digest = review_findings_snapshot(conn, ctx.config.task_ref)
            scope = _scope_lane_open_findings(
                task_ref=ctx.config.task_ref, lane_id=ctx.config.lane_id, orchestrator_root=ctx.config.orchestrator_root
            )
            if not scope.ok or scope.lane_high:
                raise ValueError("existing lane findings scope holds gating")
            receipt = _publish(ctx, "gated", result, review=review, findings_digest=findings_digest)
            allowed = receipt.get("ok") is True
            if allowed:
                ctx.native_validation_milestone = "gated"
            else:
                reason = f"gated persistence refused: {receipt}"
        except (OSError, ValueError, RuntimeError, sqlite3.Error) as exc:
            allowed, reason = False, f"gated persistence unavailable: {exc}"
    elif not fresh and result.qualified_pass:
        reason = "source_or_receipt_changed_after_validation"
    elif not review_ok:
        reason = "strict_source_bound_review_incomplete"
    elif not gate_ok:
        reason = (
            str(check.get("reason") or check.get("status") or "existing_native_verification_failed")
            if isinstance(check, dict)
            else "existing_native_verification_failed"
        )
    if hold is not None:
        reason = hold
    result_view = result.as_dict() | {"milestone": ctx.native_validation_milestone, "gate_reason": reason}
    ctx.log("INFO", "native_validation_gate", native_validation=result_view, gated=allowed)
    gate = LaneGateResult(dict(check) if isinstance(check, dict) else {})
    status = "passed" if allowed else ("failed" if result.readiness == "fail" else "system_error")
    if not check and isinstance(check, dict):
        status = check.get("status", status)
    gate.update(passed=allowed, status=status, reason=None if allowed else reason, native_validation=result_view)
    if allowed:
        gate.update(skipped=False, exit_code=0)
    elif check:
        gate.update(skipped=False, exit_code=1 if status == "failed" else 2)
    return gate
