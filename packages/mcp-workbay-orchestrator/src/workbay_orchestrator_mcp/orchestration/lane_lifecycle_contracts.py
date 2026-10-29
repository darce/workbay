"""Frozen lane-lifecycle contracts: dataclasses, enums, and JSON codecs.

Stdlib only. No package-internal imports, subprocess, filesystem, or database
access. Other slices import these types; they must stay cheap to load.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field, fields
from enum import Enum

SCHEMA_VERSION = 1
JOB_ID_RE = re.compile(r"^j[0-9a-f]{20}$")

ENV_TASK_REF = "WORKBAY_REMOTE_JOB_TASK_REF"
ENV_LANE_ID = "WORKBAY_REMOTE_JOB_LANE_ID"
ENV_PASS_ID = "WORKBAY_REMOTE_JOB_PASS_ID"
ENV_ATTEMPT = "WORKBAY_REMOTE_JOB_ATTEMPT"

_ATTEMPT_RE = re.compile(r"[0-9]+")
_UNSAFE_LANE_KEY_CHAR = re.compile(r"[^A-Za-z0-9-]")


class ContractError(Exception):
    """Typed refusal for a missing, mistyped, or unknown contract field."""

    def __init__(self, field: str, reason: str) -> None:
        self.field = field
        self.reason = reason
        super().__init__(f"{field}: {reason}")


class LaneStage(str, Enum):
    """Lifecycle stage of a remote lane."""

    provisioned = "provisioned"
    submitted = "submitted"
    running = "running"
    harvest_ready = "harvest_ready"
    harvested = "harvested"
    gated = "gated"
    landed = "landed"
    reaped = "reaped"
    refused = "refused"
    lost = "lost"


def job_id_for(task_ref: str, lane_id: str, pass_id: str, attempt: int) -> str:
    """Stable directory- and unit-safe id: ``j`` plus 20 lowercase hex chars."""
    material = "\x1f".join((str(task_ref), str(lane_id), str(pass_id), str(attempt)))
    digest = hashlib.sha256(material.encode("utf-8")).hexdigest()
    return "j" + digest[:20]


def lane_key_for_branch(branch: str) -> str:
    """Pure-Python copy of ``scripts/remote_agent.sh`` lane-key derivation.

    The key is derived from the full branch name, never from task_ref or lane_id.
    """
    digest = hashlib.sha256(branch.encode("utf-8", errors="surrogatepass")).hexdigest()[:8]
    key = _UNSAFE_LANE_KEY_CHAR.sub("-", branch)[:40]
    while key.startswith("-"):
        key = key[1:]
    return f"{key or 'lane'}-{digest}"


def _annotation_name(annotation: object) -> str:
    if isinstance(annotation, str):
        return annotation.replace(" ", "")
    return str(annotation).replace(" ", "")


def _coerce(field_name: str, annotation: str, value: object) -> object:
    optional = False
    if annotation.endswith("|None"):
        optional = True
        annotation = annotation[: -len("|None")]
    elif annotation.startswith("None|"):
        optional = True
        annotation = annotation[len("None|") :]
    if optional and value is None:
        return None
    if annotation == "str":
        if not isinstance(value, str):
            raise ContractError(field_name, "wrong type")
        return value
    if annotation == "int":
        if isinstance(value, bool) or not isinstance(value, int):
            raise ContractError(field_name, "wrong type")
        if field_name == "schema_version" and value != SCHEMA_VERSION:
            raise ContractError(field_name, "unknown schema_version")
        return value
    if annotation == "bool":
        if not isinstance(value, bool):
            raise ContractError(field_name, "wrong type")
        return value
    if annotation == "list[str]":
        if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
            raise ContractError(field_name, "wrong type")
        return list(value)
    if annotation == "dict" or annotation.startswith("dict["):
        if not isinstance(value, dict):
            raise ContractError(field_name, "wrong type")
        return dict(value)
    raise ContractError(field_name, "wrong type")


def _known_field_names(cls: type) -> list[str]:
    return [item.name for item in fields(cls) if item.name != "extras"]


def _field_annotation(cls: type, name: str) -> str:
    for item in fields(cls):
        if item.name == name:
            return _annotation_name(item.type)
    raise ContractError(name, "missing")


def _freeze_payload(obj: object) -> None:
    extras = getattr(obj, "extras", None)
    if extras is None:
        object.__setattr__(obj, "extras", {})
    else:
        object.__setattr__(obj, "extras", dict(extras))
    for item in fields(obj):
        if item.name == "extras":
            continue
        value = getattr(obj, item.name)
        if isinstance(value, dict):
            object.__setattr__(obj, item.name, dict(value))
        elif isinstance(value, list):
            object.__setattr__(obj, item.name, list(value))


def _dump_contract(obj: object) -> str:
    payload = dict(getattr(obj, "extras", {}) or {})
    for item in fields(obj):
        if item.name == "extras":
            continue
        payload[item.name] = getattr(obj, item.name)
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def _load_contract(cls: type, text: str) -> object:
    try:
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ContractError("$", "invalid json") from exc
    if not isinstance(data, dict):
        raise ContractError("$", "wrong type")
    known = _known_field_names(cls)
    kwargs = {}
    for name in known:
        if name not in data:
            raise ContractError(name, "missing")
        kwargs[name] = _coerce(name, _field_annotation(cls, name), data[name])
    kwargs["extras"] = {key: value for key, value in data.items() if key not in known}
    return cls(**kwargs)


class _ContractMixin:
    __slots__ = ()

    def __post_init__(self) -> None:
        _freeze_payload(self)

    def to_json(self) -> str:
        return _dump_contract(self)

    @classmethod
    def from_json(cls, text: str):
        return _load_contract(cls, text)


@dataclass(frozen=True, slots=True)
class ProvisionReceipt(_ContractMixin):
    task_ref: str
    lane_id: str
    branch: str
    worktree_path: str
    base_ref: str
    base_sha: str
    lane_kind: str
    review_subject: dict | None
    supersedes: str | None
    routing: dict
    manifest_sha256_after: str
    stage: str
    refusal_reason: str | None
    actions: list[str]
    created_at: str
    schema_version: int = SCHEMA_VERSION
    extras: dict = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class LandReceipt(_ContractMixin):
    task_ref: str
    lane_id: str
    branch: str
    into: str
    landing_commit_sha: str
    branch_tip_sha: str
    bundle_path: str
    bundle_verified: bool
    worktree_removed: bool
    branch_deleted: bool
    reap_vm: str
    stage: str
    refusal_reason: str | None
    landed_at: str
    schema_version: int = SCHEMA_VERSION
    extras: dict = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ReapSignal(_ContractMixin):
    task_ref: str
    lane_id: str
    branches: list[str]
    lane_keys: list[str]
    sandbox_head_sha: str
    host_merge_sha: str
    harvest_patch_id: str | None
    reason: str
    merged_at: str
    schema_version: int = SCHEMA_VERSION
    extras: dict = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class RemoteJobIdentity(_ContractMixin):
    task_ref: str
    lane_id: str
    pass_id: str
    attempt: int
    schema_version: int = SCHEMA_VERSION
    extras: dict = field(default_factory=dict)

    @property
    def job_id(self) -> str:
        return job_id_for(self.task_ref, self.lane_id, self.pass_id, self.attempt)

    def to_env(self) -> dict:
        return {
            ENV_TASK_REF: str(self.task_ref),
            ENV_LANE_ID: str(self.lane_id),
            ENV_PASS_ID: str(self.pass_id),
            ENV_ATTEMPT: str(self.attempt),
        }

    @classmethod
    def from_env(cls, env: object) -> RemoteJobIdentity:
        def _read(key: str) -> object:
            try:
                present = key in env  # type: ignore[operator]
            except TypeError:
                present = False
            if not present:
                raise ContractError(key, "missing")
            value = env[key]  # type: ignore[index]
            if value is None or value == "":
                raise ContractError(key, "empty")
            return value

        task_ref = _read(ENV_TASK_REF)
        lane_id = _read(ENV_LANE_ID)
        pass_id = _read(ENV_PASS_ID)
        if not isinstance(task_ref, str):
            raise ContractError(ENV_TASK_REF, "wrong type")
        if not isinstance(lane_id, str):
            raise ContractError(ENV_LANE_ID, "wrong type")
        if not isinstance(pass_id, str):
            raise ContractError(ENV_PASS_ID, "wrong type")
        raw_attempt = _read(ENV_ATTEMPT)
        attempt = _parse_attempt(raw_attempt)
        return cls(task_ref=task_ref, lane_id=lane_id, pass_id=pass_id, attempt=attempt)


def _parse_attempt(raw: object) -> int:
    if isinstance(raw, bool):
        raise ContractError(ENV_ATTEMPT, "not a non-negative integer")
    if isinstance(raw, int):
        if raw < 0:
            raise ContractError(ENV_ATTEMPT, "not a non-negative integer")
        return raw
    if isinstance(raw, str) and _ATTEMPT_RE.fullmatch(raw):
        return int(raw)
    raise ContractError(ENV_ATTEMPT, "not a non-negative integer")


@dataclass(frozen=True, slots=True)
class RemoteJobSpec(_ContractMixin):
    job_id: str
    task_ref: str
    lane_id: str
    pass_id: str
    attempt: int
    backend: str
    branch: str
    lane_key: str
    argv: list[str]
    bound_seconds: int
    created_at: str
    schema_version: int = SCHEMA_VERSION
    extras: dict = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class RemoteJobStatus(_ContractMixin):
    job_id: str
    state: str
    heartbeat_age_s: int | None
    unit_active: bool
    rc: int | None
    queue_stale: bool
    observed_at: str
    schema_version: int = SCHEMA_VERSION
    extras: dict = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class RemoteJobDone(_ContractMixin):
    job_id: str
    rc: int
    finished_at: str
    stdout_bytes: int
    stdout_sha256: str
    reason: str | None
    schema_version: int = SCHEMA_VERSION
    extras: dict = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class HarvestReceipt(_ContractMixin):
    job_id: str
    pass_id: str
    run_dir: str
    patch_bytes: int
    patch_sha256: str
    patch_id: str | None
    collected_at: str
    schema_version: int = SCHEMA_VERSION
    extras: dict = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class WakeEvent(_ContractMixin):
    job_id: str
    pass_id: str
    kind: str
    rc: int | None
    observed_at: str
    schema_version: int = SCHEMA_VERSION
    extras: dict = field(default_factory=dict)
