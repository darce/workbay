"""Producer-scope identity and invocation comparison for the landing gate.

Scope/provenance validation is a seam of its own so fingerprint and option
binding cannot silently diverge from the merge verdict [REVIEW-M-07].
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any, Mapping, Sequence

import landing_measurement as lm

#: ``--basetemp`` is a checkout-local scratch path. ``--rootdir`` and
#: ``--confcutdir`` change collection and are compared, not stripped [DATA-03].
_RELOCATABLE_PATH_OPTION_NAMES = frozenset({"--basetemp"})
_SCOPE_IDENTITY_FIELDS = (
    "producer_fingerprint",
    "producer_compat",
    "interpreter",
    "python_version",
)
_GATE_SCRIPT = Path(__file__).resolve().parent / "landing_gate.py"


def _scope_well_formed(scope: Any) -> bool:
    if not isinstance(scope, dict):
        return False
    targets = scope.get("pytest_targets")
    options = scope.get("pytest_options")
    return (
        isinstance(targets, list)
        and all(isinstance(item, str) for item in targets)
        and isinstance(options, list)
        and all(isinstance(item, str) for item in options)
        and _pytest_options_well_formed(options)
    )


def _pytest_options_well_formed(options: Sequence[str]) -> bool:
    """Reject options whose arity was lost while recording the invocation."""
    for index, option in enumerate(options):
        if not option.strip():
            return False
        name, separator, value = option.partition("=")
        if name != "--basetemp":
            continue
        if separator:
            if not value.strip():
                return False
            continue
        if index + 1 >= len(options) or not options[index + 1].strip() or options[index + 1].startswith("-"):
            return False
    return True


def _stable_pytest_options(options: Sequence[str]) -> list[str]:
    """Drop relocatable scratch paths so two worktrees can still be comparable.

    ``--rootdir`` and ``--confcutdir`` are retained: they change collection.
    Path relocation is not normalized unless subject-relative equivalence is
    proven; different absolute values therefore mismatch.
    """
    stable: list[str] = []
    skip_next = False
    for option in options:
        if skip_next:
            skip_next = False
            continue
        name, sep, _value = option.partition("=")
        if name in _RELOCATABLE_PATH_OPTION_NAMES:
            if not sep:
                skip_next = True
            continue
        stable.append(option)
    return stable


def _nonempty_identity(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _scope_identity_missing(scope: Mapping[str, Any]) -> bool:
    return any(not _nonempty_identity(scope.get(field)) for field in _SCOPE_IDENTITY_FIELDS)


def _scope_identity_outcome(
    baseline: Mapping[str, Any],
    subject: Mapping[str, Any],
    *,
    trusted_fingerprint: str,
) -> str | None:
    if _scope_identity_missing(baseline) or _scope_identity_missing(subject):
        return "scope_unverified"
    if baseline["producer_fingerprint"].strip() != subject["producer_fingerprint"].strip():
        return "producer_mismatched"
    if baseline["producer_fingerprint"].strip() != trusted_fingerprint.strip():
        return "producer_mismatched"
    if (
        baseline["producer_compat"].strip() != subject["producer_compat"].strip()
        or baseline["producer_compat"].strip() != lm.COMPAT_VERSION
    ):
        return "producer_mismatched"
    if baseline["python_version"].strip() != subject["python_version"].strip():
        return "producer_mismatched"
    # Interpreter paths belong to their checkouts; python_version and the
    # trusted producer fingerprint above establish the effective runtime
    # identity without requiring both arms to resolve one filesystem path.
    return None


def _trusted_scope_fingerprint(scope: Mapping[str, Any]) -> str:
    script = scope.get("producer_script")
    wrapper = scope.get("producer_wrapper")
    if isinstance(script, str) and script.strip() and isinstance(wrapper, str) and wrapper.strip():
        return lm.trusted_producer_fingerprint(script, wrapper=wrapper)
    return lm.trusted_producer_fingerprint(_GATE_SCRIPT)


def compare_invocation_scopes(
    baseline: Any,
    subject: Any,
    *,
    trusted_fingerprint: str | None = None,
) -> dict[str, Any]:
    """Compare two producer scope receipts without requiring identical paths.

    Targets and stable pytest options must match. Worktree and interpreter
    *paths* may differ; effective runtime identity (python_version) and the
    local trusted producer fingerprint may not. Missing or malformed receipts
    are named refusals, never an inferred pass [OBS-08, DDIA].
    """
    if baseline is None or subject is None:
        return {"outcome": "scope_missing", "baseline": baseline, "subject": subject}
    if not _scope_well_formed(baseline) or not _scope_well_formed(subject):
        return {"outcome": "scope_malformed", "baseline": baseline, "subject": subject}
    if list(baseline["pytest_targets"]) != list(subject["pytest_targets"]):
        return {"outcome": "scope_mismatched", "baseline": baseline, "subject": subject}
    if _stable_pytest_options(baseline["pytest_options"]) != _stable_pytest_options(subject["pytest_options"]):
        return {"outcome": "scope_mismatched", "baseline": baseline, "subject": subject}
    try:
        expected = trusted_fingerprint if trusted_fingerprint is not None else _trusted_scope_fingerprint(baseline)
    except (OSError, ValueError):
        return {"outcome": "scope_unverified", "baseline": baseline, "subject": subject}
    identity = _scope_identity_outcome(baseline, subject, trusted_fingerprint=expected)
    if identity is not None:
        return {"outcome": identity, "baseline": baseline, "subject": subject}
    return {"outcome": "comparable", "baseline": baseline, "subject": subject}


def build_invocation_scope(
    *,
    worktree: Path | str,
    pytest_targets: Sequence[str],
    pytest_options: Sequence[str],
    interpreter: str,
    producer_revision: str | None = None,
    producer_wrapper: Path | str | None = None,
    producer_script: Path | str | None = None,
) -> dict[str, Any]:
    """Non-secret measurement identity for a gate-id producing run."""
    import sys as _sys

    revision = producer_revision if producer_revision is not None else _producer_revision()
    script_path = Path(producer_script).resolve() if producer_script is not None else _GATE_SCRIPT.resolve()
    wrapper_path = (
        Path(producer_wrapper).resolve()
        if producer_wrapper is not None
        else (script_path.parent / lm.WRAPPER_LAYER_NAME).resolve()
    )
    fingerprint = lm.trusted_producer_fingerprint(script_path, wrapper=wrapper_path)
    version = _sys.version_info
    return {
        "pytest_targets": [str(item) for item in pytest_targets],
        "pytest_options": [str(item) for item in pytest_options],
        "worktree": str(Path(worktree).resolve()),
        "interpreter": str(Path(interpreter).resolve()),
        "python_version": f"{version.major}.{version.minor}.{version.micro}",
        "python_executable": str(Path(_sys.executable).resolve()),
        "producer_revision": revision,
        "producer_fingerprint": fingerprint,
        "producer_compat": lm.COMPAT_VERSION,
        "producer_script": str(script_path),
        "producer_wrapper": str(wrapper_path),
    }


def _producer_revision() -> str:
    root = Path(__file__).resolve().parent.parent
    try:
        proc = subprocess.run(
            ["git", "-C", str(root), "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=False,
            timeout=30,
        )
    except (OSError, subprocess.TimeoutExpired):
        return ""
    return (proc.stdout or "").strip() if proc.returncode == 0 else ""


def _load_scope(path: Path | None) -> Any:
    if path is None:
        return None
    try:
        text = Path(path).read_text(encoding="utf-8")
    except OSError:
        return None
    try:
        return json.loads(text)
    except ValueError:
        return {"pytest_targets": "invalid-json"}
