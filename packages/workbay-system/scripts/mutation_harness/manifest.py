"""Load and validate a mutant manifest (JSON)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from mutation_harness.models import Mutant


class ManifestError(ValueError):
    """Invalid or unreadable mutant manifest."""


def _require_str(entry: dict[str, Any], key: str, mutant_index: int) -> str:
    val = entry.get(key)
    if not isinstance(val, str) or not val.strip():
        raise ManifestError(
            f"mutant[{mutant_index}]: field {key!r} must be a non-empty string"
        )
    return val


def _parse_mutant(entry: Any, index: int) -> Mutant:
    if not isinstance(entry, dict):
        raise ManifestError(f"mutant[{index}]: expected object, got {type(entry).__name__}")

    mid = _require_str(entry, "id", index)
    target = _require_str(entry, "target", index)
    mutation = entry.get("mutation")
    if not isinstance(mutation, dict) or not mutation:
        raise ManifestError(f"mutant[{index}] ({mid}): mutation must be a non-empty object")

    tests_raw = entry.get("tests", entry.get("test_nodes", []))
    if tests_raw is None:
        tests_raw = []
    if not isinstance(tests_raw, list) or not all(isinstance(t, str) for t in tests_raw):
        raise ManifestError(
            f"mutant[{index}] ({mid}): tests must be a list of node-id strings"
        )
    tests = tuple(tests_raw)

    allowed = bool(entry.get("allowed_survivor", False))
    rationale = entry.get("allowed_survivor_rationale")
    if rationale is not None and not isinstance(rationale, str):
        raise ManifestError(
            f"mutant[{index}] ({mid}): allowed_survivor_rationale must be a string"
        )
    if allowed and not (rationale and rationale.strip()):
        raise ManifestError(
            f"mutant[{index}] ({mid}): allowed_survivor requires a non-empty rationale"
        )

    timeout = entry.get("timeout")
    if timeout is not None:
        if not isinstance(timeout, (int, float)) or timeout <= 0:
            raise ManifestError(
                f"mutant[{index}] ({mid}): timeout must be a positive number"
            )
        timeout = float(timeout)

    expected_duration = entry.get("expected_duration", entry.get("duration_hint"))
    if expected_duration is not None:
        if not isinstance(expected_duration, (int, float)) or expected_duration < 0:
            raise ManifestError(
                f"mutant[{index}] ({mid}): expected_duration must be a non-negative number"
            )
        expected_duration = float(expected_duration)

    return Mutant(
        id=mid,
        target=target,
        mutation=dict(mutation),
        tests=tests,
        allowed_survivor=allowed,
        allowed_survivor_rationale=rationale,
        timeout=timeout,
        expected_duration=expected_duration,
    )


def load_manifest(path: Path | str) -> list[Mutant]:
    """Load mutants from a JSON file.

    Accepted shapes:
    - ``{"mutants": [ ... ]}``
    - bare ``[ ... ]`` list of mutant objects
    """
    p = Path(path)
    try:
        text = p.read_text(encoding="utf-8")
    except OSError as exc:
        raise ManifestError(f"cannot read manifest {p}: {exc}") from exc
    try:
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ManifestError(f"invalid JSON in {p}: {exc}") from exc
    return parse_manifest(data)


def parse_manifest(data: Any) -> list[Mutant]:
    """Parse and validate an in-memory manifest document."""
    if isinstance(data, dict):
        if "mutants" not in data:
            raise ManifestError("manifest object must contain a 'mutants' array")
        raw = data["mutants"]
    elif isinstance(data, list):
        raw = data
    else:
        raise ManifestError(
            f"manifest must be an object or array, got {type(data).__name__}"
        )
    if not isinstance(raw, list):
        raise ManifestError("'mutants' must be an array")
    if not raw:
        raise ManifestError("manifest contains no mutants")

    mutants = [_parse_mutant(entry, i) for i, entry in enumerate(raw)]
    ids = [m.id for m in mutants]
    if len(ids) != len(set(ids)):
        seen: set[str] = set()
        for mid in ids:
            if mid in seen:
                raise ManifestError(f"duplicate mutant id: {mid!r}")
            seen.add(mid)
    return mutants
