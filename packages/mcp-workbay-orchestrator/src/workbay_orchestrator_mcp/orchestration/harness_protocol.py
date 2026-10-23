"""Small fail-closed reader for the harness overlay-source contract."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

_CONTRACT_PATH = Path("docs/workbay/contracts/harness-protocol.yaml")
_KEY = "tracked_overlay_source_paths:"
_LANDING_KEY = "landing:"
_LANDING_SEVERITIES = frozenset({"LOW", "MEDIUM", "HIGH", "CRITICAL", "BLOCKING"})


@dataclass(frozen=True)
class LandingPolicy:
    receipt_version: int
    require_gate_receipt: bool
    require_review_verdict: bool
    blocking_severities: tuple[str, ...]
    scratch_globs: tuple[str, ...]


def load_tracked_overlay_source_paths(
    workspace_root: Path | str,
) -> frozenset[str] | None:
    """Read ``tracked_overlay_source_paths`` without adding a YAML dependency.

    The contract field is a flat YAML sequence.  ``None`` distinguishes an
    unreadable/missing key from a valid empty sequence, allowing callers to
    classify fail-closed instead of silently treating dead instrumentation as
    "no overlay paths" ([OBS-08]).
    """
    path = Path(workspace_root).expanduser().resolve() / _CONTRACT_PATH
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return None

    key_indent: int | None = None
    values: list[str] = []
    for raw in lines:
        stripped = raw.strip()
        indent = len(raw) - len(raw.lstrip(" "))
        if key_indent is None:
            if stripped == _KEY:
                key_indent = indent
            continue
        if not stripped or stripped.startswith("#"):
            continue
        if indent <= key_indent:
            break
        if not stripped.startswith("- "):
            # A nested mapping begins: the flat sequence has ended.
            break
        value = stripped[2:].split(" #", 1)[0].strip().strip("'\"")
        if value:
            values.append(value.rstrip("/"))
    if key_indent is None:
        return None
    return frozenset(values)


def load_landing_policy(workspace_root: Path | str) -> LandingPolicy | None:
    """Read the required landing policy without adding a YAML dependency.

    A missing, unreadable, or malformed policy returns ``None`` so callers can
    fail closed instead of treating an absent policy as no landing requirements.
    Unknown keys in the block are ignored for forward compatibility (OBS-08
    lexicons/engineering.md:478; API-11 lexicons/engineering.md:512).
    """
    path = Path(workspace_root).expanduser().resolve() / _CONTRACT_PATH
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError):
        return None

    landing_blocks = [index for index, raw in enumerate(lines) if raw == _LANDING_KEY]
    if len(landing_blocks) != 1:
        return None

    landing_indent = 0
    section_end = len(lines)
    index = landing_blocks[0]
    for end in range(index + 1, len(lines)):
        candidate = lines[end]
        stripped = candidate.strip()
        if stripped and not stripped.startswith("#"):
            indent = len(candidate) - len(candidate.lstrip(" "))
            if indent <= landing_indent:
                section_end = end
                break
    section_start = index + 1

    fields: dict[str, object] = {}
    malformed: set[str] = set()
    explicit_empty_sequences: set[str] = set()
    current_key: str | None = None
    current_indent: int | None = None
    direct_indent: int | None = None
    for raw in lines[section_start:section_end]:
        stripped = raw.strip()
        if not stripped or stripped.startswith("#"):
            continue
        indent = len(raw) - len(raw.lstrip(" "))
        if indent <= landing_indent:
            break
        if direct_indent is None:
            direct_indent = indent

        if indent == direct_indent:
            key, separator, value = stripped.partition(":")
            if not separator:
                current_key = None
                current_indent = indent
                continue
            current_key = key.strip()
            current_indent = indent
            if current_key not in {
                "receipt_version",
                "require_gate_receipt",
                "require_review_verdict",
                "blocking_severities",
                "scratch_globs",
            }:
                continue
            if current_key in fields or current_key in malformed:
                malformed.add(current_key)
                continue

            value = value.split(" #", 1)[0].strip()
            if current_key in {"blocking_severities", "scratch_globs"}:
                if value == "[]":
                    fields[current_key] = []
                    explicit_empty_sequences.add(current_key)
                elif value:
                    malformed.add(current_key)
                else:
                    fields[current_key] = []
            else:
                if value:
                    fields[current_key] = value.strip("'\"")
                else:
                    malformed.add(current_key)
            continue

        if current_key is None or current_indent is None or indent <= current_indent:
            continue
        if current_key in {"blocking_severities", "scratch_globs"}:
            if current_key in explicit_empty_sequences:
                malformed.add(current_key)
                continue
            if not stripped.startswith("- "):
                malformed.add(current_key)
                continue
            item = stripped[2:].split(" #", 1)[0].strip().strip("'\"")
            if not item:
                malformed.add(current_key)
                continue
            values = fields.get(current_key)
            if isinstance(values, list):
                values.append(item.upper() if current_key == "blocking_severities" else item)
        elif current_key in {"receipt_version", "require_gate_receipt", "require_review_verdict"}:
            malformed.add(current_key)

    required = {
        "receipt_version",
        "require_gate_receipt",
        "require_review_verdict",
        "blocking_severities",
        "scratch_globs",
    }
    for key in ("blocking_severities", "scratch_globs"):
        if fields.get(key) == [] and key not in explicit_empty_sequences:
            malformed.add(key)
    if not required.issubset(fields) or malformed.intersection(required):
        return None

    raw_version = fields["receipt_version"]
    if not isinstance(raw_version, str) or not raw_version.isdecimal() or int(raw_version) != 1:
        return None

    booleans: dict[str, bool] = {}
    for key in ("require_gate_receipt", "require_review_verdict"):
        value = fields[key]
        if value == "true":
            booleans[key] = True
        elif value == "false":
            booleans[key] = False
        else:
            return None

    severities = fields["blocking_severities"]
    scratch_globs = fields["scratch_globs"]
    if not isinstance(severities, list) or not severities:
        return None
    if any(value not in _LANDING_SEVERITIES for value in severities):
        return None
    if not isinstance(scratch_globs, list):
        return None

    return LandingPolicy(
        receipt_version=1,
        require_gate_receipt=booleans["require_gate_receipt"],
        require_review_verdict=booleans["require_review_verdict"],
        blocking_severities=tuple(severities),
        scratch_globs=tuple(scratch_globs),
    )
