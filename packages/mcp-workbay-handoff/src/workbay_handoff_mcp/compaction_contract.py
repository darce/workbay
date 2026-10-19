"""Typed loader for the canonical compaction contract."""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from json import JSONDecodeError
from pathlib import Path
from typing import Literal, cast

import yaml
from pydantic import BaseModel, ConfigDict, Field
from workbay_protocol import HARNESS_CONTRACT_RELPATH

try:
    from workbay_protocol import MANIFEST_NAME_PRECEDENCE
except ImportError:  # pragma: no cover - compatibility with older protocol wheels.
    MANIFEST_NAME_PRECEDENCE = (".workbay-bootstrap.json", ".workbay-overlay.json")

_CONTRACT_RELATIVE_PATH = HARNESS_CONTRACT_RELPATH
# implementation note S3: the workbay-system overlay payload (incl. the canonical
# harness-protocol contract) moved under workbay_system/payload/.
_PACKAGE_REFERENCE_CONTRACT_RELATIVE_PATH = (
    Path("packages/workbay-system/workbay_system/payload") / HARNESS_CONTRACT_RELPATH
)
_OVERLAY_MANIFEST_NAMES = MANIFEST_NAME_PRECEDENCE
_CANONICAL_HARNESSES: tuple[str, ...] = ("claude-code", "codex", "grok", "vscode", "manual")
_HARNESS_ALIASES: dict[str, str] = {"cursor": "vscode"}

CompactionContractHarness = Literal["claude-code", "codex", "grok", "vscode", "manual"]


class TranscriptDiscoveryRule(BaseModel):
    model_config = ConfigDict(extra="forbid")

    env_var: str
    fallback_glob: str


class HarnessIdentityMarkersRule(BaseModel):
    """Presence-only env-var markers that identify a harness (not transcript paths)."""

    model_config = ConfigDict(extra="forbid")

    markers: list[str]


class CompactionContract(BaseModel):
    model_config = ConfigDict(extra="forbid")

    contract_path: Path
    advisory_field: str
    threshold_tokens: int = Field(ge=0)
    threshold_chars: int = Field(ge=0)
    unknown_harness: str
    transcript_discovery: dict[str, TranscriptDiscoveryRule]
    # Optional section: missing/absent in YAML loads as empty (degrade, never raise).
    harness_identity_markers: dict[str, HarnessIdentityMarkersRule] = Field(default_factory=dict)


class ContractSourceReport(BaseModel):
    model_config = ConfigDict(extra="forbid")

    resolved: dict[str, object]
    package_reference: dict[str, object] | None
    drift: dict[str, object]


class ActiveHarnessResolution(BaseModel):
    model_config = ConfigDict(extra="forbid")

    harness: CompactionContractHarness | None = None
    env_var: str | None = None
    warnings: tuple[str, ...] = ()
    # internal: distinguish the two ``harness is None`` states —
    # ABSENT (no source matched) vs AMBIGUOUS (multiple conflicting sources). A
    # caller (``_infer_harness_agent_from_env``) must degrade to None on a genuine
    # conflict rather than fall through to a lower-confidence signal, instead of
    # string-matching the "Multiple active harnesses" warning.
    ambiguous: bool = False


ThresholdSource = Literal["env", "overlay", "contract", "constant"]
_ENV_VAR_TOKENS = "WORKBAY_HANDOFF_COMPACTION_THRESHOLD_TOKENS"
_ENV_VAR_CHARS = "WORKBAY_HANDOFF_COMPACTION_THRESHOLD_CHARS"
_ENV_VAR_MIN_NEW_TOKENS = "WORKBAY_HANDOFF_COMPACTION_MIN_NEW_TOKENS"


class MinNewTokensGateResolution(BaseModel):
    model_config = ConfigDict(extra="forbid")

    value: int = Field(ge=0)
    source: ThresholdSource
    warnings: tuple[str, ...] = ()


class EffectiveThresholds(BaseModel):
    model_config = ConfigDict(extra="forbid")

    tokens: int = Field(ge=0)
    chars: int = Field(ge=0)
    tokens_source: ThresholdSource
    chars_source: ThresholdSource
    warnings: tuple[str, ...] = ()


def _parse_threshold_override(raw: object) -> int | None:
    if isinstance(raw, bool):
        return None
    if isinstance(raw, int):
        return raw if raw >= 0 else None
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return None
        try:
            value = int(text)
        except ValueError:
            return None
        return value if value >= 0 else None
    return None


def resolve_effective_thresholds(
    contract: CompactionContract,
    *,
    env: Mapping[str, str] | None = None,
    workspace_root: Path | None = None,
) -> EffectiveThresholds:
    """Resolve per-knob compaction thresholds with env > overlay > contract precedence.

    Reads ``env`` (or ``os.environ`` when omitted) and the bootstrap manifest at
    ``workspace_root`` each call. No module-level caching.
    Invalid override values append a ``compaction_threshold_override_invalid``
    warning and fall through to the next layer.
    """
    warnings: list[str] = []
    source_env: Mapping[str, str] = env if env is not None else os.environ

    overlay_thresholds: dict = {}
    if workspace_root is not None:
        overlay_payload = _load_overlay_payload(workspace_root)
        compaction_block = overlay_payload.get("compaction")
        if isinstance(compaction_block, dict):
            raw_thresholds = compaction_block.get("thresholds")
            if isinstance(raw_thresholds, dict):
                overlay_thresholds = raw_thresholds

    def _resolve_knob(
        env_var: str,
        overlay_key: str,
        contract_value: int,
    ) -> tuple[int, ThresholdSource]:
        raw_env = source_env.get(env_var)
        used_var = env_var
        if raw_env is not None and raw_env.strip() != "":
            parsed = _parse_threshold_override(raw_env)
            if parsed is not None:
                return parsed, "env"
            warnings.append(f"compaction_threshold_override_invalid: env={used_var}={raw_env}")

        if overlay_key in overlay_thresholds:
            raw_overlay = overlay_thresholds[overlay_key]
            parsed = _parse_threshold_override(raw_overlay)
            if parsed is not None:
                return parsed, "overlay"
            warnings.append(
                f"compaction_threshold_override_invalid: overlay=compaction.thresholds.{overlay_key}={raw_overlay}"
            )

        return contract_value, "contract"

    tokens, tokens_source = _resolve_knob(_ENV_VAR_TOKENS, "tokens", contract.threshold_tokens)
    chars, chars_source = _resolve_knob(_ENV_VAR_CHARS, "chars", contract.threshold_chars)

    return EffectiveThresholds(
        tokens=tokens,
        chars=chars,
        tokens_source=tokens_source,
        chars_source=chars_source,
        warnings=tuple(warnings),
    )


def resolve_min_new_tokens_gate(
    contract: CompactionContract,
    *,
    env: Mapping[str, str] | None = None,
    workspace_root: Path | None = None,
) -> MinNewTokensGateResolution:
    """Resolve Stop-hook token gate default with env > overlay > contract > constant."""
    warnings: list[str] = []
    source_env: Mapping[str, str] = env if env is not None else os.environ

    raw_env = source_env.get(_ENV_VAR_MIN_NEW_TOKENS)
    if raw_env is not None and raw_env.strip() != "":
        parsed = _parse_threshold_override(raw_env)
        if parsed is not None:
            return MinNewTokensGateResolution(value=parsed, source="env")
        warnings.append(f"compaction_threshold_override_invalid: env={_ENV_VAR_MIN_NEW_TOKENS}={raw_env}")

    overlay_thresholds: dict = {}
    if workspace_root is not None:
        overlay_payload = _load_overlay_payload(workspace_root)
        compaction_block = overlay_payload.get("compaction")
        if isinstance(compaction_block, dict):
            raw_thresholds = compaction_block.get("thresholds")
            if isinstance(raw_thresholds, dict):
                overlay_thresholds = raw_thresholds

    if "tokens" in overlay_thresholds:
        parsed = _parse_threshold_override(overlay_thresholds["tokens"])
        if parsed is not None:
            return MinNewTokensGateResolution(value=parsed, source="overlay", warnings=tuple(warnings))
        warnings.append(
            "compaction_threshold_override_invalid: "
            f"overlay=compaction.thresholds.tokens={overlay_thresholds['tokens']}"
        )

    return MinNewTokensGateResolution(
        value=contract.threshold_tokens,
        source="contract",
        warnings=tuple(warnings),
    )


def resolve_min_new_tokens_gate_with_fallback(
    *,
    env: Mapping[str, str] | None = None,
    workspace_root: Path | None = None,
    constant_fallback: int,
) -> MinNewTokensGateResolution:
    """Load contract when possible; fall back to ``constant_fallback`` when unreadable."""
    if workspace_root is None:
        return MinNewTokensGateResolution(value=constant_fallback, source="constant")
    try:
        contract = load_compaction_contract(workspace_root)
    except (FileNotFoundError, ValueError, OSError, yaml.YAMLError):
        return MinNewTokensGateResolution(value=constant_fallback, source="constant")
    return resolve_min_new_tokens_gate(
        contract,
        env=env,
        workspace_root=workspace_root,
    )


def normalize_compaction_harness(harness: str) -> CompactionContractHarness:
    normalized = harness.strip().lower()
    normalized = _HARNESS_ALIASES.get(normalized, normalized)
    if normalized not in _CANONICAL_HARNESSES:
        allowed = ", ".join(_CANONICAL_HARNESSES)
        raise ValueError(f"Invalid harness: {normalized!r}. Valid values: {allowed}")
    return cast(CompactionContractHarness, normalized)


def detect_active_harness(
    contract: CompactionContract,
    env: dict[str, str] | None = None,
) -> ActiveHarnessResolution:
    source = env if env is not None else {}
    matches: list[tuple[CompactionContractHarness, str]] = []
    known_env_vars: list[str] = []

    for harness_name, discovery_rule in contract.transcript_discovery.items():
        known_env_vars.append(discovery_rule.env_var)
        raw_value = source.get(discovery_rule.env_var, "")
        if raw_value.strip():
            matches.append((normalize_compaction_harness(harness_name), discovery_rule.env_var))

    if len(matches) == 1:
        harness_name, env_var = matches[0]
        return ActiveHarnessResolution(harness=harness_name, env_var=env_var)

    if not matches:
        env_var_list = ", ".join(known_env_vars)
        return ActiveHarnessResolution(
            warnings=(f"No active harness detected from transcript env vars: {env_var_list}",)
        )

    harness_list = ", ".join(harness_name for harness_name, _ in matches)
    return ActiveHarnessResolution(
        ambiguous=True,
        warnings=(f"Multiple active harnesses detected from transcript env vars: {harness_list}",),
    )


def detect_harness_from_identity_markers(
    contract: CompactionContract,
    env: dict[str, str] | None = None,
) -> ActiveHarnessResolution:
    """Resolve harness from presence-only identity markers (not transcript paths).

    Exactly-one-match discipline mirrors ``detect_active_harness``: zero matches
    or multiple harnesses with markers present → ``harness=None`` (ambiguous /
    absent). Empty ``harness_identity_markers`` (missing contract section) →
    ``None``, never raises.

    Multi-marker semantics are ANY-present (OR), not all-present: a harness that
    lists several markers counts as present when *any one* of them is set, and
    the first listed present marker is reported as ``env_var``. Markers are
    presence-only — a set-but-empty value (``""`` / whitespace) does not count,
    matching ``detect_active_harness``.
    """
    source = env if env is not None else {}
    matches: list[tuple[CompactionContractHarness, str]] = []
    known_markers: list[str] = []

    for harness_name, markers_rule in contract.harness_identity_markers.items():
        matched_marker: str | None = None
        for marker in markers_rule.markers:
            known_markers.append(marker)
            raw_value = source.get(marker, "")
            if raw_value.strip() and matched_marker is None:
                matched_marker = marker
        if matched_marker is not None:
            matches.append((normalize_compaction_harness(harness_name), matched_marker))

    if len(matches) == 1:
        harness_name, env_var = matches[0]
        return ActiveHarnessResolution(harness=harness_name, env_var=env_var)

    if not matches:
        # Missing/empty markers section: silent None (degrade). Configured
        # markers with no env hits: warn listing the markers checked.
        if not known_markers:
            return ActiveHarnessResolution()
        marker_list = ", ".join(known_markers)
        return ActiveHarnessResolution(warnings=(f"No active harness detected from identity markers: {marker_list}",))

    harness_list = ", ".join(harness_name for harness_name, _ in matches)
    return ActiveHarnessResolution(
        ambiguous=True,
        warnings=(f"Multiple active harnesses detected from identity markers: {harness_list}",),
    )


def _load_overlay_payload(workspace_root: Path) -> dict:
    """Return parsed bootstrap/legacy overlay manifest payload, or an empty dict."""
    overlay_path = next(
        (workspace_root / n for n in _OVERLAY_MANIFEST_NAMES if (workspace_root / n).exists()),
        None,
    )
    if overlay_path is None:
        return {}
    try:
        payload = json.loads(overlay_path.read_text(encoding="utf-8"))
    except (JSONDecodeError, OSError):
        return {}
    if not isinstance(payload, dict):
        return {}
    return payload


def _iter_contract_candidates(workspace_root: Path) -> list[Path]:
    # The legacy `surfaces.contracts` overlay override was removed (implementation note):
    # it was dead (no production writer). Contract resolution uses the default
    # path + the package-reference fallback only.
    candidates: list[Path] = [
        workspace_root / _CONTRACT_RELATIVE_PATH,
        workspace_root / _PACKAGE_REFERENCE_CONTRACT_RELATIVE_PATH,
    ]

    deduped: list[Path] = []
    seen: set[Path] = set()
    for candidate in candidates:
        resolved = candidate.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        deduped.append(candidate)
    return deduped


def _resolve_contract_path(workspace_root: Path) -> Path | None:
    for candidate in _iter_contract_candidates(workspace_root):
        if candidate.exists():
            return candidate
    return None


def _load_compaction_contract_at_path(contract_path: Path) -> CompactionContract:
    payload = yaml.safe_load(contract_path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"Compaction contract at {contract_path} must be a mapping")
    compaction_payload = payload.get("compaction")
    if not isinstance(compaction_payload, dict):
        raise ValueError(f"Compaction contract at {contract_path} is missing the top-level 'compaction' block")

    discovery_payload = compaction_payload.get("transcript_discovery")
    if not isinstance(discovery_payload, dict):
        raise ValueError(f"Compaction contract at {contract_path} is missing 'compaction.transcript_discovery'")

    transcript_discovery: dict[str, TranscriptDiscoveryRule] = {}
    for harness_name, harness_payload in discovery_payload.items():
        if not isinstance(harness_name, str):
            raise ValueError(f"Compaction contract at {contract_path} has a non-string harness name")
        if not isinstance(harness_payload, dict):
            raise ValueError(
                f"Compaction contract at {contract_path} has a non-mapping transcript discovery entry for {harness_name!r}"
            )
        transcript_discovery[normalize_compaction_harness(harness_name)] = TranscriptDiscoveryRule.model_validate(
            harness_payload
        )

    # Optional section: missing/null → empty (degrade). Non-mapping → raise.
    harness_identity_markers: dict[str, HarnessIdentityMarkersRule] = {}
    markers_payload = compaction_payload.get("harness_identity_markers")
    if markers_payload is not None:
        if not isinstance(markers_payload, dict):
            raise ValueError(
                f"Compaction contract at {contract_path} has a non-mapping 'compaction.harness_identity_markers' block"
            )
        for harness_name, harness_payload in markers_payload.items():
            if not isinstance(harness_name, str):
                raise ValueError(
                    f"Compaction contract at {contract_path} has a non-string harness name in harness_identity_markers"
                )
            if not isinstance(harness_payload, dict):
                raise ValueError(
                    f"Compaction contract at {contract_path} has a non-mapping "
                    f"harness_identity_markers entry for {harness_name!r}"
                )
            harness_identity_markers[normalize_compaction_harness(harness_name)] = (
                HarnessIdentityMarkersRule.model_validate(harness_payload)
            )

    return CompactionContract.model_validate(
        {
            "contract_path": contract_path,
            "advisory_field": compaction_payload.get("advisory_field"),
            "threshold_tokens": compaction_payload.get("threshold_tokens"),
            "threshold_chars": compaction_payload.get("threshold_chars"),
            "unknown_harness": compaction_payload.get("unknown_harness"),
            "transcript_discovery": transcript_discovery,
            "harness_identity_markers": harness_identity_markers,
        }
    )


def load_compaction_contract(workspace_root: str | Path) -> CompactionContract:
    root_path = Path(workspace_root).expanduser().resolve()
    contract_path = _resolve_contract_path(root_path)
    if contract_path is None:
        raise FileNotFoundError(f"No compaction contract found under {root_path}")

    return _load_compaction_contract_at_path(contract_path)


def build_contract_source_report(
    contract: CompactionContract,
    *,
    workspace_root: str | Path,
) -> ContractSourceReport:
    root_path = Path(workspace_root).expanduser().resolve()
    resolved_thresholds = {
        "tokens": contract.threshold_tokens,
        "chars": contract.threshold_chars,
    }
    report: dict[str, object] = {
        "resolved": {
            "path": str(contract.contract_path),
            "thresholds": resolved_thresholds,
        },
        "package_reference": None,
        "drift": {"detected": False, "thresholds": {}},
    }

    package_path = root_path / _PACKAGE_REFERENCE_CONTRACT_RELATIVE_PATH
    if not package_path.exists():
        return ContractSourceReport.model_validate(report)

    package_contract = _load_compaction_contract_at_path(package_path)
    package_thresholds = {
        "tokens": package_contract.threshold_tokens,
        "chars": package_contract.threshold_chars,
    }
    drift_thresholds: dict[str, dict[str, int]] = {}
    for key in ("tokens", "chars"):
        resolved_value = resolved_thresholds[key]
        package_value = package_thresholds[key]
        if resolved_value != package_value:
            drift_thresholds[key] = {
                "resolved": resolved_value,
                "package_reference": package_value,
            }

    report["package_reference"] = {
        "path": str(package_path),
        "thresholds": package_thresholds,
    }
    report["drift"] = {
        "detected": bool(drift_thresholds),
        "thresholds": drift_thresholds,
    }
    return ContractSourceReport.model_validate(report)
