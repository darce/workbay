"""Join complexity units with coverage and rank by CRAP."""

from __future__ import annotations

import re
from pathlib import Path

from crap_report.coverage_load import CoverageIndex, normalize_repo_path
from crap_report.formula import compute_crap
from crap_report.models import CrapReport, MethodScore, MethodUnit

_TEST_ONLY_DIRS = frozenset({"tests", "test"})
_HARD_EXCLUDES = frozenset({".venv", "venv", "__pycache__", "generated", ".git"})
_TEST_FILE_RE = re.compile(r"(^|/)test_[^/]+\.py$|(^|/).+_test\.py$")


def is_default_excluded(file: str, *, include_tests: bool = False) -> bool:
    """True when path matches default test/generated/venv excludes.

    *include_tests* only lifts test-path excludes; hard excludes
    (venv/generated/.git/__pycache__) always match.
    """
    posix = file.replace("\\", "/")
    parts = set(posix.split("/"))
    hard = bool(parts & _HARD_EXCLUDES)
    testish = bool(parts & _TEST_ONLY_DIRS) or bool(_TEST_FILE_RE.search(posix))
    return hard or (testish and not include_tests)


def score_methods(
    units: list[MethodUnit],
    index: CoverageIndex,
    *,
    threshold: float = 30.0,
    apply_default_excludes: bool = True,
    include_tests: bool = False,
    include_excluded: bool = False,
    measured_only: bool = True,
    unmeasured_cc_min: int = 10,
    repo_root: Path | None = None,
    provenance: dict | None = None,
) -> CrapReport:
    """Score *units* against *index*; return ranked CrapReport.

    Synthetic units are supported (no radon required). File keys are
    normalized to repo-relative POSIX paths before join.

    *include_tests* only lifts test-path excludes; venv/generated/.git still drop.

    *measured_only* (default True): omit methods whose files/ranges are not in the
    coverage report from the ranked list. Unmeasured high-CC units are attached
    separately as informational (not scored as if cov=0).
    """
    kinds: list[str] = []
    ranked: list[MethodScore] = []
    unmeasured_pool: list[MethodScore] = []
    unmeasured_dropped_low_cc = 0
    for unit in units:
        file_key = normalize_repo_path(unit.file, repo_root=repo_root)
        excluded = apply_default_excludes and is_default_excluded(
            file_key, include_tests=include_tests
        )
        if excluded and not include_excluded:
            continue
        cov, known, kind, status = index.coverage_for(
            file_key, unit.line_start, unit.line_end
        )
        if known:
            kinds.append(kind)
            crap = compute_crap(unit.comp, cov)
        else:
            # Do not pretend unmeasured code is 0% covered for ranking.
            cov = 0.0
            crap = float(unit.comp)  # informational floor = pure CC
        score = MethodScore(
            file=file_key,
            name=unit.name,
            line_start=unit.line_start,
            line_end=unit.line_end,
            comp=unit.comp,
            cov=round(cov, 4),
            crap=round(crap, 4),
            coverage_unknown=not known,
            coverage_status=status,  # type: ignore[arg-type]
            excluded=excluded,
        )
        if not known:
            if unit.comp >= unmeasured_cc_min:
                unmeasured_pool.append(score)
            elif measured_only:
                # Low-CC unmeasured: not ranked, not pooled — still count it.
                unmeasured_dropped_low_cc += 1
            if measured_only:
                continue
            # include-unmeasured: score as classic CRAP with cov=0
            score = MethodScore(
                file=score.file,
                name=score.name,
                line_start=score.line_start,
                line_end=score.line_end,
                comp=score.comp,
                cov=0.0,
                crap=round(compute_crap(unit.comp, 0.0), 4),
                coverage_unknown=True,
                coverage_status=status,  # type: ignore[arg-type]
                excluded=excluded,
            )
        ranked.append(score)

    ranked.sort(key=lambda m: m.crap, reverse=True)
    unmeasured_pool.sort(key=lambda m: m.comp, reverse=True)
    coverage_kind = (
        "branch"
        if kinds and all(k == "branch" for k in kinds)
        else ("mixed" if kinds and any(k == "branch" for k in kinds) else "line")
    )
    return CrapReport(
        threshold=threshold,
        coverage_kind=coverage_kind,
        provenance=dict(provenance or {}),
        methods=ranked,
        unmeasured_high_cc=unmeasured_pool if measured_only else [],
        unmeasured_dropped_low_cc=unmeasured_dropped_low_cc if measured_only else 0,
    )
