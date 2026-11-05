"""Native source coverage, not an N verdict or a worker attestation.

Policies and authentication callbacks belong to the trusted collector, outside
the worker's write grant. The callback authenticates the *whole* interval,
including binding, route, version, store identity and both cursor endpoints.
Admission records must come from the trusted admission adapter, as required by
the public production store's caller-authentication contract. Strings such as
writer_authority_ref and worker complete=True are not authentication mechanisms.

No recorder is deployed here. Current shell/remote routes remain unavailable
until independently retained recording and confinement actually exist (3c.11).
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from enum import Enum
from types import MappingProxyType
from typing import Any, Literal

from workbay_handoff_mcp.lane_completion_production import (
    MAX_DECODED_BYTES,
    MAX_PAGE_ROWS,
    MAX_PAGES,
    ProductionAppend,
    ProductionBinding,
    ProductionRefusal,
    get_lane_production_history,
    record_production_settle,
)

POLICY_VERSION = "native-coverage-v1"


class CoverageReason(str, Enum):
    ROUTE_NOT_N_CAPABLE = "route_not_n_capable"
    CURSOR_GAP = "cursor_gap"
    CURSOR_REPLACED = "cursor_replaced"
    OPEN_OPERATION = "open_operation"
    UNSUPPORTED_VERSION = "unsupported_version"
    HISTORY_TRUNCATED = "history_truncated"
    UNAUTHENTICATED_SOURCE = "unauthenticated_source"
    IDENTITY_MISMATCH = "identity_mismatch"
    INVALID_HISTORY = "invalid_history"
    HISTORY_CHANGED = "history_changed"


@dataclass(frozen=True)
class CoverageResult:
    status: Literal["covered", "unavailable"]
    reason: CoverageReason | None = None


@dataclass(frozen=True, kw_only=True)
class RouteCapability:
    supported_versions: frozenset[str]
    recorder: Literal[
        "trusted_external",
        "writable_hook",
        "worker_checkout",
        "editable_hooks_path",
        "unmediated",
        "refusal_before_start",
    ]


# Explicit registrations describe this tree, not a promise about future hooks.
# Unknown backend names fail closed without relying on a name suffix heuristic.
ROUTE_REGISTRY = MappingProxyType(
    {
        route: RouteCapability(supported_versions=frozenset({"1"}), recorder="unmediated")
        for route in (
            "codex-cli",
            "codex-subagent",
            "copilot-host",
            "claude-code",
            "grok-cli",
            "grok-remote",
            "codex-remote",
            "openrouter-remote",
            "cursor-remote",
            "claude-remote",
            "claude-api-remote",
            "muse-remote",
            "cursor-cli",
            "structured-turn",
            "local-model-openai",
        )
    }
)


@dataclass(frozen=True, kw_only=True)
class CoveragePolicy:
    binding: ProductionBinding
    admission_id: str
    routes: tuple[str, ...]
    registry: Mapping[str, RouteCapability]
    assessment_cursors: Mapping[str, Mapping[str, Any]]
    authenticate: Callable[[Mapping[str, Any]], bool]
    version: str = POLICY_VERSION


def route_capability(route: str, producer_version: str, registry: Mapping[str, RouteCapability]) -> CoverageResult:
    capability = registry.get(route)
    if capability is None:
        return CoverageResult("unavailable", CoverageReason.ROUTE_NOT_N_CAPABLE)
    if producer_version not in capability.supported_versions:
        return CoverageResult("unavailable", CoverageReason.UNSUPPORTED_VERSION)
    if capability.recorder != "trusted_external":
        return CoverageResult("unavailable", CoverageReason.ROUTE_NOT_N_CAPABLE)
    return CoverageResult("covered")


class _Hold(Exception):
    def __init__(self, reason: CoverageReason):
        self.reason = reason


def _require(condition: bool, reason: CoverageReason) -> None:
    if not condition:
        raise _Hold(reason)


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _digest(value: Any) -> str:
    return hashlib.sha256(_json(value).encode()).hexdigest()


def _cursor(value: Mapping[str, Any]) -> dict:
    _require(
        isinstance(value, dict)
        and set(value) == {"store_id", "sequence", "digest"}
        and isinstance(value["store_id"], str)
        and bool(value["store_id"])
        and type(value["sequence"]) is int
        and value["sequence"] >= 0
        and isinstance(value["digest"], str)
        and bool(value["digest"]),
        CoverageReason.CURSOR_GAP,
    )
    return dict(value)


def _check_capability(route: str, version: str, policy: CoveragePolicy) -> None:
    _require(route in policy.routes, CoverageReason.ROUTE_NOT_N_CAPABLE)
    result = route_capability(route, version, policy.registry)
    if result.reason:
        raise _Hold(result.reason)


def _advance(interval: dict, current: dict, policy: CoveragePolicy) -> dict:
    _require(
        interval["binding"] == policy.binding and interval["admission_id"] == policy.admission_id,
        CoverageReason.IDENTITY_MISMATCH,
    )
    _check_capability(interval["route"], interval["producer_version"], policy)
    try:
        authenticated = policy.authenticate(interval) is True
    except Exception:
        authenticated = False
    _require(authenticated, CoverageReason.UNAUTHENTICATED_SOURCE)
    start, end = _cursor(interval["start"]), _cursor(interval["end"])
    _require(start["store_id"] == current["store_id"] == end["store_id"], CoverageReason.CURSOR_REPLACED)
    _require(start == current and end["sequence"] >= start["sequence"], CoverageReason.CURSOR_GAP)
    entries = interval["entries"]
    _require(
        isinstance(entries, list) and len(entries) == end["sequence"] - start["sequence"], CoverageReason.CURSOR_GAP
    )
    previous_oid = None
    for entry in entries:
        _require(
            type(entry["sequence"]) is int
            and entry["sequence"] == current["sequence"] + 1
            and entry["previous_digest"] == current["digest"],
            CoverageReason.CURSOR_GAP,
        )
        _require(
            _digest({k: v for k, v in entry.items() if k != "digest"}) == entry["digest"], CoverageReason.CURSOR_GAP
        )
        _require(
            all(isinstance(entry[k], str) and bool(entry[k]) for k in ("old_oid", "new_oid"))
            and (previous_oid is None or entry["old_oid"] == previous_oid),
            CoverageReason.CURSOR_GAP,
        )
        current = dict(store_id=current["store_id"], sequence=entry["sequence"], digest=entry["digest"])
        previous_oid = entry["new_oid"]
    _require(current == end, CoverageReason.CURSOR_GAP)
    return end


def _bounded_data(history: dict) -> dict:
    _require(history["ok"] is True, CoverageReason.INVALID_HISTORY)
    data = history["data"]
    rows = data["history"]
    if (
        data["has_more"] is not False
        or data["truncated"] is not False
        or data["truncation"]["truncated"] is not False
        or data["truncation"]["reasons"]
        or len(rows) != data["returned"]
        or len(rows) != data["total_matching"]
        or len(rows) != data["head"]["sequence"]
    ):
        raise _Hold(CoverageReason.HISTORY_TRUNCATED)
    _require(isinstance(rows, list) and 0 < len(rows) <= MAX_PAGE_ROWS * MAX_PAGES, CoverageReason.INVALID_HISTORY)
    _require(len(_json(history).encode()) <= MAX_DECODED_BYTES, CoverageReason.HISTORY_TRUNCATED)
    return data


def _scan(history: dict, policy: CoveragePolicy) -> tuple[dict, set[str], dict]:
    data = _bounded_data(history)
    rows = data["history"]
    root = rows[0]["payload"]
    _require(root["evidence_kind"] == "production_admission", CoverageReason.INVALID_HISTORY)
    _require(
        policy.version == POLICY_VERSION
        and data["coverage_policy_version"] == policy.version
        and root["facts"]["coverage_policy_version"] == policy.version,
        CoverageReason.UNSUPPORTED_VERSION,
    )
    _require(
        bool(policy.routes)
        and len(set(policy.routes)) == len(policy.routes)
        and set(root["facts"]["routes"]) == set(policy.routes)
        and set(policy.assessment_cursors) == set(policy.routes),
        CoverageReason.ROUTE_NOT_N_CAPABLE,
    )
    cursors = root["facts"]["source_cursors"]
    _require(set(cursors) == set(policy.routes), CoverageReason.CURSOR_GAP)
    cursors = {route: _cursor(value) for route, value in cursors.items()}
    for route in policy.routes:
        capability = policy.registry.get(route)
        if capability is not None and capability.recorder == "refusal_before_start":
            refusal = root["facts"].get("refused_routes", {}).get(route, {})
            _require(
                len(rows) == 1
                and all(
                    refusal.get(key) is False
                    for key in (
                        "worker_started",
                        "source_access_started",
                        "product_published",
                    )
                ),
                CoverageReason.ROUTE_NOT_N_CAPABLE,
            )
            _require(root["producer_version"] in capability.supported_versions, CoverageReason.UNSUPPORTED_VERSION)
        else:
            _check_capability(route, root["producer_version"], policy)
    predecessor = None
    open_operations: set[str] = set()
    settled: set[str] = set()
    for sequence, row in enumerate(rows, 1):
        payload = row["payload"]
        _require(
            payload["binding"] == policy.binding and payload["admission_id"] == policy.admission_id,
            CoverageReason.IDENTITY_MISMATCH,
        )
        _require(
            row["sequence"] == payload["sequence"] == sequence
            and row["previous_evidence_id"] == payload["previous_evidence_id"] == predecessor
            and row["evidence_id"] == _digest(payload)
            and row["evidence_kind"] == payload["evidence_kind"],
            CoverageReason.INVALID_HISTORY,
        )
        predecessor = row["evidence_id"]
        kind, operation, facts = payload["evidence_kind"], payload["operation_id"], payload["facts"]
        if kind == "production_prepare":
            _require(operation not in open_operations | settled, CoverageReason.INVALID_HISTORY)
            open_operations.add(operation)
        elif kind == "production_settle":
            _require(operation in open_operations, CoverageReason.INVALID_HISTORY)
            open_operations.remove(operation)
            settled.add(operation)
        elif kind == "production_gap":
            raise _Hold(CoverageReason.CURSOR_GAP)
        elif kind not in ("production_admission", "production_seal"):
            # Identity transitions/backfills need their own lineage validator.
            raise _Hold(CoverageReason.INVALID_HISTORY)
        if "source_interval" in facts:
            _require(kind == "production_settle", CoverageReason.INVALID_HISTORY)
            interval = facts["source_interval"]
            route = interval["route"]
            _require(route in cursors, CoverageReason.ROUTE_NOT_N_CAPABLE)
            cursors[route] = _advance(interval, cursors[route], policy)
    _require(predecessor == data["head"]["evidence_id"], CoverageReason.INVALID_HISTORY)
    _require(len(open_operations) == data["open_operation_count"], CoverageReason.INVALID_HISTORY)
    return cursors, open_operations, data


def _assessment(cursors: dict, policy: CoveragePolicy) -> None:
    for route, current in cursors.items():
        end = _cursor(policy.assessment_cursors[route])
        _require(end["store_id"] == current["store_id"], CoverageReason.CURSOR_REPLACED)
        _require(end == current, CoverageReason.CURSOR_GAP)


def validate_coverage(history: dict, policy: CoveragePolicy) -> CoverageResult:
    """Validate a single complete bounded 3c.1 reader envelope, never endpoints.

    A covered result attests source-cursor coverage only. A recorded product or
    rewind remains production; consumers must independently assess R and N.
    """
    try:
        cursors, open_operations, _ = _scan(history, policy)
        _require(not open_operations, CoverageReason.OPEN_OPERATION)
        _assessment(cursors, policy)
        return CoverageResult("covered")
    except _Hold as exc:
        return CoverageResult("unavailable", exc.reason)
    except (KeyError, TypeError, ValueError, AttributeError):
        return CoverageResult("unavailable", CoverageReason.INVALID_HISTORY)


def import_source_cursor(
    request: ProductionAppend, interval: Mapping[str, Any], policy: CoveragePolicy
) -> CoverageResult:
    """Settle an existing prepared import via the public store and its head CAS.

    Refused imports leave the prepare open. Crashes never acknowledge zero.
    The caller retains source objects; authentication must also verify their
    retained manifests. No raw worker boolean or editable hook grants trust.
    Each import reaches that source's independently captured assessment point.
    Other sources may be collected separately; validate_coverage holds until
    every declared source reaches its assessment cursor.
    """
    try:
        _require(
            request.binding == policy.binding and request.admission_id == policy.admission_id,
            CoverageReason.IDENTITY_MISMATCH,
        )
        # Snapshot before authenticating, so mutation by a caller cannot swap
        # authenticated bytes for different bytes at the persistence boundary.
        interval = json.loads(_json(interval))
        _require(len(_json(interval).encode()) <= MAX_DECODED_BYTES, CoverageReason.HISTORY_TRUNCATED)
        history = get_lane_production_history(request.binding, request.admission_id)
        cursors, open_operations, data = _scan(history, policy)
        settled = next(
            (
                row
                for row in data["history"]
                if row["payload"]["evidence_kind"] == "production_settle"
                and row["payload"]["operation_id"] == request.operation_id
            ),
            None,
        )
        if settled is not None:
            # Reconstruct the original settlement's cursor snapshot, not the
            # latest head's. The public store compares the entire canonical
            # request (including facts and expected_head) before its head CAS
            # and returns the retained receipt without appending on exact replay.
            cursors = {
                route: _cursor(value)
                for route, value in data["history"][0]["payload"]["facts"]["source_cursors"].items()
            }
            for row in data["history"]:
                if row["sequence"] == settled["sequence"]:
                    break
                previous = row["payload"]["facts"].get("source_interval")
                if previous is not None:
                    cursors[previous["route"]] = _cursor(previous["end"])
        else:
            _require(
                data["head"]
                == {"sequence": request.expected_head.sequence, "evidence_id": request.expected_head.evidence_id},
                CoverageReason.HISTORY_CHANGED,
            )
            _require(request.operation_id in open_operations, CoverageReason.OPEN_OPERATION)
        route = interval["route"]
        _require(route in cursors, CoverageReason.ROUTE_NOT_N_CAPABLE)
        cursors[route] = _advance(interval, cursors[route], policy)
        _assessment({route: cursors[route]}, policy)
        record_production_settle(
            replace(
                request,
                facts={
                    **request.facts,
                    "source_interval": interval,
                    "source_cursors": cursors,
                    "coverage_policy_version": policy.version,
                },
            )
        )
        return CoverageResult("covered")
    except _Hold as exc:
        return CoverageResult("unavailable", exc.reason)
    except ProductionRefusal:
        return CoverageResult("unavailable", CoverageReason.HISTORY_CHANGED)
    except (KeyError, TypeError, ValueError, AttributeError):
        return CoverageResult("unavailable", CoverageReason.INVALID_HISTORY)
