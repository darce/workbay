"""Load coverage.py JSON reports and query line/branch coverage by range."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


def normalize_repo_path(path: str | Path, *, repo_root: Path | None = None) -> str:
    """Return repo-relative POSIX path for stable join keys.

    Absolute coverage.py keys are reduced relative to *repo_root* when possible.
    Never use ``lstrip("./")`` on absolute paths (corrupts prefixes).
    """
    root = (repo_root or Path.cwd()).resolve()
    raw = str(path).replace("\\", "/")
    candidate = Path(raw)
    try:
        if candidate.is_absolute():
            return candidate.resolve().relative_to(root).as_posix()
        # Relative: resolve against root then re-relativize
        return (root / candidate).resolve().relative_to(root).as_posix()
    except (ValueError, OSError):
        # Outside repo or missing on disk — strip leading ./ only.
        # Path-aware prefix check: string startswith(root) false-positives on
        # sibling worktrees named <repo>-<slug> (shares root string prefix).
        while raw.startswith("./"):
            raw = raw[2:]
        root_s = str(root).replace("\\", "/")
        if raw == root_s or raw.startswith(root_s + "/"):
            try:
                return Path(raw).relative_to(root).as_posix()
            except (ValueError, OSError):
                pass
        return raw.lstrip("/")


@dataclass
class CoverageIndex:
    """In-memory coverage map keyed by repo-relative POSIX paths."""

    files: dict[str, dict[str, Any]] = field(default_factory=dict)
    coverage_kind_preference: str = "branch"  # branch|line

    def line_coverage(self, file: str, line_start: int, line_end: int) -> tuple[float, bool]:
        """Return (percent, known). known=False when no executable lines in range."""
        entry = self.files.get(file)
        if not entry:
            return 0.0, False
        executed = set(entry.get("executed_lines") or [])
        missing = set(entry.get("missing_lines") or [])
        # Prefer explicit executed+missing; fall back to summary-only files
        executable = executed | missing
        if not executable and "summary" in entry:
            # No line detail — treat as unknown for method-level join
            return 0.0, False
        in_range = {n for n in executable if line_start <= n <= line_end}
        if not in_range:
            return 0.0, False
        hit = sum(1 for n in in_range if n in executed)
        return 100.0 * hit / len(in_range), True

    def branch_coverage(self, file: str, line_start: int, line_end: int) -> tuple[float, bool]:
        """Return (percent, known) for branches whose source line is in range."""
        entry = self.files.get(file)
        if not entry:
            return 0.0, False
        executed_b = entry.get("executed_branches") or []
        missing_b = entry.get("missing_branches") or []
        if not executed_b and not missing_b:
            return 0.0, False

        def _line(branch: Any) -> int | None:
            # coverage.py: [line, ...] or dict with line
            if isinstance(branch, (list, tuple)) and branch:
                return int(branch[0])
            if isinstance(branch, dict) and "line" in branch:
                return int(branch["line"])
            return None

        ex_in: list[Any] = []
        miss_in: list[Any] = []
        for b in executed_b:
            ln = _line(b)
            if ln is not None and line_start <= ln <= line_end:
                ex_in.append(b)
        for b in missing_b:
            ln = _line(b)
            if ln is not None and line_start <= ln <= line_end:
                miss_in.append(b)
        total = len(ex_in) + len(miss_in)
        if total == 0:
            return 0.0, False
        return 100.0 * len(ex_in) / total, True

    def has_file(self, file: str) -> bool:
        return file in self.files

    def coverage_for(
        self, file: str, line_start: int, line_end: int
    ) -> tuple[float, bool, str, str]:
        """Prefer branch when available, else line.

        Returns ``(pct, known, kind, status)`` where status is
        ``measured`` | ``missing_file`` | ``empty_range``.
        """
        if not self.has_file(file):
            return 0.0, False, "line", "missing_file"
        if self.coverage_kind_preference == "branch":
            pct, known = self.branch_coverage(file, line_start, line_end)
            if known:
                return pct, True, "branch", "measured"
        pct, known = self.line_coverage(file, line_start, line_end)
        if known:
            return pct, True, "line", "measured"
        return 0.0, False, "line", "empty_range"


def load_coverage_json(path: Path, *, repo_root: Path | None = None) -> CoverageIndex:
    """Parse a coverage.py JSON report into a CoverageIndex."""
    raw = json.loads(path.read_text(encoding="utf-8"))
    files_raw = raw.get("files")
    if not isinstance(files_raw, dict):
        raise ValueError("coverage JSON missing top-level 'files' object")
    index = CoverageIndex()
    for key, entry in files_raw.items():
        if not isinstance(entry, dict):
            continue
        norm = normalize_repo_path(key, repo_root=repo_root)
        index.files[norm] = entry
    return index
