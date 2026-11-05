"""Node-ID baseline diff / adjudication.

Detects silent test removals (and optional absolute floor) so a green mutant
sweep cannot hide a deleted assertion that used to kill a mutant.

An empty, truncated, or unreadable baseline is a hard failure — not a pass —
unless the caller passes an explicit bootstrap flag (no baseline yet).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

from mutation_harness.models import BaselineReport


class BaselineError(ValueError):
    """Baseline file unreadable or malformed."""


# Default floor: empty observations never silently pass.
_DEFAULT_ABSOLUTE_FLOOR = 1


def load_node_ids(path: Path | str) -> list[str]:
    """Load expected or observed node IDs from a baseline file.

    Accepted formats:
    - JSON array of strings
    - JSON object with ``node_ids`` or ``tests`` array
    - plain text, one node ID per line (# comments and blanks ignored)

    Empty files parse to ``[]``; callers that treat expected baselines as
    authoritative must fail closed via :func:`reconcile_baseline` (not here).
    """
    p = Path(path)
    try:
        text = p.read_text(encoding="utf-8")
    except OSError as exc:
        raise BaselineError(f"cannot read baseline {p}: {exc}") from exc
    return parse_node_ids(text, source=str(p))


def parse_node_ids(text: str, *, source: str = "<string>") -> list[str]:
    """Parse node IDs from text content."""
    stripped = text.strip()
    if not stripped:
        return []
    if stripped[0] in "[{":
        try:
            data = json.loads(stripped)
        except json.JSONDecodeError as exc:
            raise BaselineError(f"invalid JSON baseline {source}: {exc}") from exc
        if isinstance(data, list):
            ids = data
        elif isinstance(data, dict):
            ids = data.get("node_ids", data.get("tests"))
            if ids is None:
                raise BaselineError(
                    f"baseline object {source} must contain 'node_ids' or 'tests'"
                )
        else:
            raise BaselineError(
                f"baseline {source} must be array or object, got {type(data).__name__}"
            )
        if not isinstance(ids, list) or not all(isinstance(x, str) for x in ids):
            raise BaselineError(f"baseline {source}: node ids must be strings")
        return list(ids)

    out: list[str] = []
    for line in text.splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        out.append(s)
    return out


def _coerce_bootstrap(bootstrap: object) -> bool:
    """Accept only a real bool; reject truthy strings like ``'false'``."""
    if not isinstance(bootstrap, bool):
        raise BaselineError(
            f"bootstrap must be a bool, got {type(bootstrap).__name__}: {bootstrap!r}"
        )
    return bootstrap


def _resolve_floor(absolute_floor: int | None) -> int:
    return _DEFAULT_ABSOLUTE_FLOOR if absolute_floor is None else int(absolute_floor)


def reconcile_baseline(
    expected: Iterable[str],
    observed: Iterable[str],
    *,
    absolute_floor: int | None = _DEFAULT_ABSOLUTE_FLOOR,
    allow_additions: bool = True,
    bootstrap: bool = False,
    expected_source: str | Path | None = None,
) -> BaselineReport:
    """Compare expected vs observed test node IDs.

    Fail closed when any expected node is missing from observed. Additions are
    allowed by default (suite growth) unless ``allow_additions`` is False.

    ``absolute_floor`` defaults to 1 so an empty observation cannot pass the
    floor check by accident. Pass a larger int to raise the backstop, or
    ``absolute_floor=0`` only when intentionally disabling the numeric floor
    (empty *expected* is still a hard failure unless ``bootstrap=True``).

    The floor counts **unique** observed node IDs so duplicate collector output
    cannot inflate the backstop.

    An empty expected set is a hard failure unless ``bootstrap=True`` (first-run
    / no baseline yet). Bootstrap is recorded in the verdict message so a green
    sweep cannot silently claim an empty baseline as success. Bootstrap does
    **not** skip the absolute floor: an explicit floor still applies.
    """
    bootstrap = _coerce_bootstrap(bootstrap)
    exp_list = list(expected)
    obs_list = list(observed)
    source_label = str(expected_source) if expected_source is not None else "<inline>"
    floor = _resolve_floor(absolute_floor)
    # Unique observations only — duplicates must not satisfy the numeric floor.
    obs_unique = len(set(obs_list))
    exp_unique = len(set(exp_list))

    # Empty / missing baseline is the exact failure mode this module exists to
    # prevent: removed = exp_set - obs_set is empty for ANY obs when exp is empty.
    if exp_unique == 0:
        if bootstrap:
            messages = [
                f"bootstrap=true (no baseline yet)",
                f"baseline_file={source_label}",
                f"expected_node_ids=0",
                f"observed_node_ids={obs_unique}",
            ]
            ok = True
            if floor > 0 and obs_unique < floor:
                ok = False
                messages.append(
                    f"observed count {obs_unique} below absolute floor {floor}"
                )
            return BaselineReport(
                ok=ok,
                expected_count=0,
                observed_count=obs_unique,
                added=sorted(set(obs_list)),
                removed=[],
                message="; ".join(messages),
            )
        return BaselineReport(
            ok=False,
            expected_count=0,
            observed_count=obs_unique,
            added=sorted(set(obs_list)),
            removed=[],
            message=(
                f"empty baseline is a hard failure; baseline_file={source_label}; "
                f"expected_node_ids=0; observed_node_ids={obs_unique}; "
                f"pass bootstrap=True only for first-run with no baseline yet"
            ),
        )

    exp_set = set(exp_list)
    obs_set = set(obs_list)
    removed = sorted(exp_set - obs_set)
    added = sorted(obs_set - exp_set)

    messages: list[str] = [
        f"baseline_file={source_label}",
        f"expected_node_ids={exp_unique}",
        f"observed_node_ids={obs_unique}",
    ]
    if bootstrap:
        messages.append("bootstrap=true")
    ok = True
    if removed:
        ok = False
        messages.append(f"removed {len(removed)} node id(s): {removed[:5]}")
    if added and not allow_additions:
        ok = False
        messages.append(f"unexpected added {len(added)} node id(s): {added[:5]}")
    if floor > 0 and obs_unique < floor:
        ok = False
        messages.append(
            f"observed count {obs_unique} below absolute floor {floor}"
        )
    if ok:
        messages.append("baseline reconciled")

    return BaselineReport(
        ok=ok,
        expected_count=exp_unique,
        observed_count=obs_unique,
        added=added,
        removed=removed,
        message="; ".join(messages),
    )


def load_and_reconcile(
    expected_path: Path | str,
    observed_path: Path | str,
    *,
    absolute_floor: int | None = _DEFAULT_ABSOLUTE_FLOOR,
    allow_additions: bool = True,
    bootstrap: bool = False,
) -> BaselineReport:
    """Load both files and reconcile.

    Records which expected baseline file was read and how many node IDs it
    contained. Unreadable expected files raise :class:`BaselineError` (hard
    failure for the CLI). Empty expected files fail closed unless
    ``bootstrap=True``.
    """
    expected = load_node_ids(expected_path)
    observed = load_node_ids(observed_path)
    return reconcile_baseline(
        expected,
        observed,
        absolute_floor=absolute_floor,
        allow_additions=allow_additions,
        bootstrap=bootstrap,
        expected_source=str(expected_path),
    )
