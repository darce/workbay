"""Typed embeddings policy reads and migration mapping (implementation note E1).

Install receipt (``embeddings_mode``), operator policy, and availability
are separate facts. This module does not load models and does not depend
on codemap readers beyond the shared 64 KiB bound.
"""

from __future__ import annotations

import json
import os
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from pydantic import ValidationError

from workbay_protocol.bootstrap import (
    EmbeddingsInferenceReceipt,
    EmbeddingsOperatorPolicy,
)
from workbay_protocol.capability_mode import MANIFEST_READ_LIMIT_BYTES
from workbay_protocol import paths

OPERATOR_POLICY_FIELD = "embeddings_operator_policy"
INFERENCE_RECEIPT_FIELD = "embeddings_inference_receipt"
POLICY_REVISION_ENV_KEY = "WORKBAY_EMBEDDINGS_POLICY_REVISION"

PolicyReadStatus = Literal[
    "ok",
    "manifest_missing",
    "field_missing",
    "unreadable",
    "malformed",
    "oversized",
    "invalid_value",
    "unsupported_schema",
]
EmbeddingsPolicyValue = Literal["off", "preferred", "required"]
ReinjectionPolicyValue = Literal["off", "on"]
PolicyOrigin = Literal["explicit", "migrated", "default", "missing"]
PolicyIntent = Literal["known", "unknown"]
EmbeddingsModeValue = Literal["unspecified", "verified", "disabled"]
AvailabilityStatus = Literal["ready", "missing", "unavailable", "failed", "unknown"]

_OPEN_FLAGS = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NONBLOCK", 0)


class EmbeddingPolicyError(ValueError):
    """Malformed or unknown embeddings operator policy (OBS-08)."""


@dataclass(frozen=True, slots=True)
class EmbeddingPolicyRead:
    """One bounded read of operator policy plus the install receipt."""

    status: PolicyReadStatus
    embeddings_policy: EmbeddingsPolicyValue | None
    reinjection_policy: ReinjectionPolicyValue | None
    policy_revision: int | None
    origin: PolicyOrigin
    intent: PolicyIntent
    embeddings_mode: EmbeddingsModeValue
    inference_receipt: EmbeddingsInferenceReceipt | None


@dataclass(frozen=True, slots=True)
class AvailabilityObservation:
    """Runtime provider observation; not written into the install receipt."""

    status: AvailabilityStatus
    reason: str | None
    observed_at: str | None
    provider_id: str | None


@dataclass(frozen=True, slots=True)
class MigrationDecision:
    """Pure mapping from historical receipt + file gate to operator policy."""

    persist: bool
    embeddings_policy: EmbeddingsPolicyValue | None
    reinjection_policy: ReinjectionPolicyValue | None
    origin: Literal["explicit", "migrated", "default"]
    intent: PolicyIntent
    report: str


def derive_migration_decision(
    *,
    embeddings_mode: EmbeddingsModeValue,
    file_gate_disabled: bool,
) -> MigrationDecision:
    """Map legacy receipt + explicit file-off onto operator policy.

    File-gate off wins even when the receipt is historical ``verified``.
    Unspecified history is unknown intent and is not persisted as a default.
    """
    if file_gate_disabled:
        return MigrationDecision(
            persist=True,
            embeddings_policy="off",
            reinjection_policy="off",
            origin="migrated",
            intent="known",
            report="file_gate_disabled_maps_off",
        )
    if embeddings_mode == "disabled":
        return MigrationDecision(
            persist=True,
            embeddings_policy="off",
            reinjection_policy="off",
            origin="migrated",
            intent="known",
            report="disabled_receipt_maps_off",
        )
    if embeddings_mode == "verified":
        return MigrationDecision(
            persist=True,
            embeddings_policy="preferred",
            reinjection_policy="on",
            origin="migrated",
            intent="known",
            report="verified_receipt_maps_preferred",
        )
    return MigrationDecision(
        persist=False,
        embeddings_policy=None,
        reinjection_policy=None,
        origin="default",
        intent="unknown",
        report="unspecified_legacy_unknown_intent",
    )


def _typed_missing(
    status: PolicyReadStatus,
    *,
    embeddings_mode: EmbeddingsModeValue = "unspecified",
) -> EmbeddingPolicyRead:
    return EmbeddingPolicyRead(
        status=status,
        embeddings_policy=None,
        reinjection_policy=None,
        policy_revision=None,
        origin="missing",
        intent="unknown",
        embeddings_mode=embeddings_mode,
        inference_receipt=None,
    )


def _select_manifest(
    root: Path,
    manifest_path: Path | str | None,
) -> Path | None:
    if manifest_path is not None:
        candidate = Path(manifest_path)
        try:
            os.lstat(candidate)
        except OSError:
            return None
        return candidate
    for name in paths.MANIFEST_NAME_PRECEDENCE:
        candidate = root / name
        try:
            os.lstat(candidate)
        except OSError:
            continue
        return candidate
    return None


def _read_manifest_bytes(path: Path) -> tuple[bytes | None, PolicyReadStatus | None]:
    try:
        fd = os.open(path, _OPEN_FLAGS)
    except OSError:
        return None, "unreadable"
    try:
        try:
            info = os.fstat(fd)
        except OSError:
            return None, "unreadable"
        if not stat.S_ISREG(info.st_mode):
            return None, "unreadable"
        if info.st_size > MANIFEST_READ_LIMIT_BYTES:
            return None, "oversized"
        try:
            raw = os.read(fd, MANIFEST_READ_LIMIT_BYTES + 1)
        except OSError:
            return None, "unreadable"
    finally:
        os.close(fd)
    if len(raw) > MANIFEST_READ_LIMIT_BYTES:
        return None, "oversized"
    return raw, None


def _schema_version_supported(data: dict[object, object]) -> bool:
    if "schema_version" not in data:
        return True
    version = data["schema_version"]
    if isinstance(version, bool) or not isinstance(version, int):
        return False
    return version >= 1


def _mode_from_payload(data: dict[object, object]) -> EmbeddingsModeValue:
    value = data.get("embeddings_mode")
    return value if value in ("verified", "disabled") else "unspecified"


def load_embeddings_policy(
    repo_root: Path | str,
    *,
    manifest_path: Path | str | None = None,
) -> EmbeddingPolicyRead:
    """Read operator policy as a typed record; never loads a model."""
    root = Path(repo_root)
    selected = _select_manifest(root, manifest_path)
    if selected is None:
        return _typed_missing("manifest_missing")

    raw, fail_status = _read_manifest_bytes(selected)
    if fail_status is not None or raw is None:
        return _typed_missing(fail_status or "unreadable")

    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        return _typed_missing("unreadable")

    try:
        parsed = json.loads(text)
    except ValueError:
        return _typed_missing("malformed")
    if not isinstance(parsed, dict):
        return _typed_missing("malformed")

    if not _schema_version_supported(parsed):
        return _typed_missing("unsupported_schema")

    embeddings_mode = _mode_from_payload(parsed)

    if (
        OPERATOR_POLICY_FIELD not in parsed
        or parsed[OPERATOR_POLICY_FIELD] is None
    ):
        return _typed_missing("field_missing", embeddings_mode=embeddings_mode)

    raw_policy = parsed[OPERATOR_POLICY_FIELD]
    try:
        policy = EmbeddingsOperatorPolicy.model_validate(raw_policy)
    except ValidationError:
        return _typed_missing("invalid_value", embeddings_mode=embeddings_mode)

    receipt: EmbeddingsInferenceReceipt | None = None
    if INFERENCE_RECEIPT_FIELD in parsed and parsed[INFERENCE_RECEIPT_FIELD] is not None:
        try:
            receipt = EmbeddingsInferenceReceipt.model_validate(
                parsed[INFERENCE_RECEIPT_FIELD]
            )
        except ValidationError:
            return _typed_missing("invalid_value", embeddings_mode=embeddings_mode)

    return EmbeddingPolicyRead(
        status="ok",
        embeddings_policy=policy.embeddings_policy,
        reinjection_policy=policy.reinjection_policy,
        policy_revision=policy.policy_revision,
        origin=policy.origin,
        intent=policy.intent,
        embeddings_mode=embeddings_mode,
        inference_receipt=receipt,
    )


def require_readable_policy(read: EmbeddingPolicyRead) -> EmbeddingPolicyRead:
    """Writers fail loudly on malformed/unknown persisted policy (OBS-08)."""
    if read.status in {"unreadable", "malformed", "oversized", "invalid_value", "unsupported_schema"}:
        raise EmbeddingPolicyError(
            f"invalid embeddings operator policy: {read.status}"
        )
    return read


__all__ = [
    "INFERENCE_RECEIPT_FIELD",
    "OPERATOR_POLICY_FIELD",
    "POLICY_REVISION_ENV_KEY",
    "AvailabilityObservation",
    "EmbeddingPolicyError",
    "EmbeddingPolicyRead",
    "MigrationDecision",
    "derive_migration_decision",
    "load_embeddings_policy",
    "require_readable_policy",
]
