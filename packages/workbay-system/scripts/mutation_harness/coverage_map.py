"""Build and cache deterministic per-test line coverage for mutant selection.

The map records direct dynamic execution only.  It deliberately does not try
to infer the transitive import/fixture graph: execution outside an individual
test context is retained as ``unattributed_lines`` and selection falls back
loudly for import-time/module-level mutation sites.
"""

from __future__ import annotations

import ast
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from mutation_harness.models import Mutant
from mutation_harness.mutate import MutationError, locate_mutation_spans


COVERAGE_MAP_SCHEMA_VERSION = 2
MAX_CACHE_FILES = 3
MAX_TRACKED_SOURCE_FILES = 20_000
MAX_TRACKED_SOURCE_BYTES = 256 * 1024 * 1024
MAX_LINE_ENTRIES = 1_000_000
MAX_CACHE_BYTES = 64 * 1024 * 1024
_CACHE_DIR = ".mutation-harness-cache/coverage"


class CoverageMapError(RuntimeError):
    """Coverage instrumentation/cache failed and selection is unsafe."""


class StaleCoverageMapError(CoverageMapError):
    """A persisted map no longer describes the current source tree."""


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def source_hashes(source_root: Path) -> dict[str, str]:
    """Hash bounded Python, pytest-config, and test-data coverage inputs."""
    root = Path(source_root).resolve()
    ignored = {
        ".git",
        ".hg",
        ".svn",
        ".venv",
        "venv",
        ".tox",
        "node_modules",
        "__pycache__",
        ".mutation-harness-cache",
    }
    pytest_configs = {"pyproject.toml", "pytest.ini", "tox.ini", "setup.cfg"}
    paths = sorted(
        path
        for path in root.rglob("*")
        if path.is_file()
        and not any(part in ignored for part in path.relative_to(root).parts)
        and (
            path.suffix == ".py"
            or path.name in pytest_configs
            or "tests" in path.relative_to(root).parts
        )
    )
    if len(paths) > MAX_TRACKED_SOURCE_FILES:
        raise CoverageMapError(
            f"coverage input bound exceeded: {len(paths)} input files "
            f"> {MAX_TRACKED_SOURCE_FILES}"
        )
    total = 0
    result: dict[str, str] = {}
    for path in paths:
        try:
            total += path.stat().st_size
            if total > MAX_TRACKED_SOURCE_BYTES:
                raise CoverageMapError(
                    "coverage input bound exceeded: coverage inputs total more than "
                    f"{MAX_TRACKED_SOURCE_BYTES} bytes"
                )
            result[path.relative_to(root).as_posix()] = _sha256(path)
        except OSError as exc:
            raise CoverageMapError(f"cannot hash coverage input {path}: {exc}") from exc
    return result


def _tree_hash(hashes: dict[str, str]) -> str:
    canonical = json.dumps(hashes, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class CoverageMap:
    """Immutable direct-execution index, keyed by source-relative path/line."""

    source_hashes: dict[str, str]
    line_tests: dict[str, dict[int, tuple[str, ...]]]
    unattributed_lines: dict[str, frozenset[int]]
    content_hash: str
    pytest_workers: int = 0

    def tests_for(self, target: str, lines: Iterable[int]) -> list[str]:
        by_line = self.line_tests.get(Path(target).as_posix(), {})
        return sorted({node for line in lines for node in by_line.get(line, ())})

    def has_unattributed_execution(self, target: str, lines: Iterable[int]) -> bool:
        unscoped = self.unattributed_lines.get(Path(target).as_posix(), frozenset())
        return any(line in unscoped for line in lines)

    def assert_fresh(self, source_root: Path) -> None:
        current = source_hashes(source_root)
        current_hash = _tree_hash(current)
        if current_hash != self.content_hash or current != self.source_hashes:
            raise StaleCoverageMapError(
                "coverage map is stale: source/test content hashes changed; rebuild required"
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": COVERAGE_MAP_SCHEMA_VERSION,
            "content_hash": self.content_hash,
            "pytest_workers": self.pytest_workers,
            "source_hashes": dict(sorted(self.source_hashes.items())),
            "line_tests": {
                path: {str(line): list(nodes) for line, nodes in sorted(lines.items())}
                for path, lines in sorted(self.line_tests.items())
            },
            "unattributed_lines": {
                path: sorted(lines)
                for path, lines in sorted(self.unattributed_lines.items())
            },
        }

    @classmethod
    def from_dict(cls, raw: Any) -> "CoverageMap":
        if (
            not isinstance(raw, dict)
            or raw.get("schema_version") != COVERAGE_MAP_SCHEMA_VERSION
        ):
            raise CoverageMapError("unsupported or malformed coverage-map schema")
        try:
            hashes = {str(k): str(v) for k, v in raw["source_hashes"].items()}
            line_tests = {
                str(path): {
                    int(line): tuple(sorted(str(node) for node in nodes))
                    for line, nodes in lines.items()
                }
                for path, lines in raw["line_tests"].items()
            }
            unattributed = {
                str(path): frozenset(int(line) for line in lines)
                for path, lines in raw.get("unattributed_lines", {}).items()
            }
            content_hash = str(raw["content_hash"])
            pytest_workers = int(raw["pytest_workers"])
        except (KeyError, TypeError, ValueError, AttributeError) as exc:
            raise CoverageMapError(f"malformed coverage map: {exc}") from exc
        entries = sum(len(lines) for lines in line_tests.values())
        if entries > MAX_LINE_ENTRIES:
            raise CoverageMapError(
                f"coverage map bound exceeded: {entries} line entries > {MAX_LINE_ENTRIES}"
            )
        if _tree_hash(hashes) != content_hash:
            raise CoverageMapError(
                "coverage map content hash does not match stored hashes"
            )
        if pytest_workers < 0:
            raise CoverageMapError("malformed coverage map: pytest_workers is negative")
        return cls(hashes, line_tests, unattributed, content_hash, pytest_workers)


def _configured_pytest_workers(source_root: Path) -> int:
    """Resolve the suite topology from the exported override or root Makefile."""
    raw = os.environ.get("PYTEST_WORKERS")
    if raw is None:
        makefile = Path(source_root).resolve() / "Makefile"
        try:
            contents = makefile.read_text(encoding="utf-8")
        except OSError:
            contents = ""
        match = re.search(
            r"(?m)^\s*PYTEST_WORKERS\s*(?:\?=|=)\s*([0-9]+)\s*(?:#.*)?$",
            contents,
        )
        raw = match.group(1) if match else "0"
    try:
        workers = int(raw)
    except ValueError as exc:
        raise CoverageMapError(f"invalid PYTEST_WORKERS value: {raw!r}") from exc
    if workers < 0:
        raise CoverageMapError(f"invalid PYTEST_WORKERS value: {raw!r}")
    return workers


def mutation_lines(mutant: Mutant, source_root: Path) -> tuple[int, ...]:
    """Resolve every touched source line deterministically from the manifest spec."""
    path = Path(source_root).resolve() / mutant.target
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise CoverageMapError(
            f"cannot read mutant target {mutant.target}: {exc}"
        ) from exc
    try:
        spans = locate_mutation_spans(text, mutant.mutation)
    except MutationError as exc:
        raise CoverageMapError(str(exc)) from exc
    if not spans:
        raise CoverageMapError(
            f"cannot locate mutation site for mutant {mutant.id!r} in {mutant.target}"
        )
    lines: set[int] = set()
    for start, end in spans:
        first = text.count("\n", 0, start) + 1
        last_pos = max(start, end - 1)
        last = text.count("\n", 0, last_pos) + 1
        lines.update(range(first, last + 1))
    return tuple(sorted(lines))


def is_import_or_module_scope(
    mutant: Mutant, source_root: Path, lines: Iterable[int]
) -> bool:
    """Conservatively identify sites that can execute outside a test context."""
    path = Path(source_root).resolve() / mutant.target
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except (OSError, SyntaxError, UnicodeError):
        return True
    function_body_ranges: list[tuple[int, int]] = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.body:
            start = min(getattr(stmt, "lineno", node.lineno) for stmt in node.body)
            end = getattr(node, "end_lineno", start)
            function_body_ranges.append((start, end))
    return any(
        not any(start <= line <= end for start, end in function_body_ranges)
        for line in lines
    )


def _cache_dir(source_root: Path) -> Path:
    return Path(source_root).resolve() / _CACHE_DIR


def _purge_cache(cache_dir: Path, *, keep: Path | None = None) -> None:
    """LRU eviction: retain at most the newest three bounded map files."""
    try:
        files = sorted(
            cache_dir.glob("*.json"),
            key=lambda path: (path.stat().st_mtime_ns, path.name),
            reverse=True,
        )
    except OSError:
        return
    retained: set[Path] = set()
    if keep is not None and keep in files:
        retained.add(keep)
    for path in files:
        if len(retained) >= MAX_CACHE_FILES:
            break
        retained.add(path)
    for path in files:
        if path not in retained:
            try:
                path.unlink()
            except OSError:
                pass


def load_cached_coverage_map(source_root: Path) -> CoverageMap | None:
    hashes = source_hashes(source_root)
    expected_hash = _tree_hash(hashes)
    path = _cache_dir(source_root) / f"{expected_hash}.json"
    if not path.is_file():
        return None
    try:
        if path.stat().st_size > MAX_CACHE_BYTES:
            raise CoverageMapError(f"coverage cache exceeds {MAX_CACHE_BYTES} bytes")
        result = CoverageMap.from_dict(json.loads(path.read_text(encoding="utf-8")))
        result.assert_fresh(source_root)
        if result.pytest_workers != _configured_pytest_workers(source_root):
            raise StaleCoverageMapError(
                "coverage map pytest worker topology changed; rebuild required"
            )
    except (OSError, json.JSONDecodeError, CoverageMapError):
        try:
            path.unlink()
        except OSError:
            pass
        return None
    try:
        path.touch()
    except OSError:
        pass
    _purge_cache(path.parent, keep=path)
    return result


def _coverage_to_map(
    data_file: Path,
    source_root: Path,
    hashes: dict[str, str],
    *,
    pytest_workers: int,
) -> CoverageMap:
    try:
        from coverage import Coverage
    except ImportError as exc:
        raise CoverageMapError(
            "coverage.py is required to build per-test mutant selection"
        ) from exc
    cov = Coverage(data_file=str(data_file))
    cov.combine(data_paths=[str(data_file.parent)], strict=True, keep=False)
    cov.save()
    data = cov.get_data()
    root = source_root.resolve()
    line_tests: dict[str, dict[int, tuple[str, ...]]] = {}
    unattributed: dict[str, frozenset[int]] = {}
    for filename in sorted(data.measured_files()):
        measured = Path(filename)
        if not measured.is_absolute():
            measured = root / measured
        resolved = measured.resolve()
        keys: set[str] = set()
        for candidate in (measured, resolved):
            try:
                keys.add(candidate.relative_to(root).as_posix())
            except ValueError:
                pass
        # coverage.py and Python loaders do not promise to retain the lexical
        # spelling used to import a symlink. Add every in-tree alias whose
        # resolved file is the measured file so manifest targets remain valid.
        for source_path in hashes:
            try:
                if (root / source_path).resolve() == resolved:
                    keys.add(source_path)
            except OSError:
                continue
        if not keys:
            continue
        contexts = data.contexts_by_lineno(filename)
        scoped: dict[int, tuple[str, ...]] = {}
        unscoped: set[int] = set()
        for line, values in sorted(contexts.items()):
            tests = tuple(sorted({value for value in values if value}))
            if tests:
                scoped[int(line)] = tests
            if "" in values:
                unscoped.add(int(line))
        for relative in sorted(keys):
            if scoped:
                line_tests[relative] = scoped
            if unscoped:
                unattributed[relative] = frozenset(unscoped)
    entries = sum(len(lines) for lines in line_tests.values())
    if entries > MAX_LINE_ENTRIES:
        raise CoverageMapError(
            f"coverage map bound exceeded: {entries} line entries > {MAX_LINE_ENTRIES}"
        )
    return CoverageMap(
        hashes, line_tests, unattributed, _tree_hash(hashes), pytest_workers
    )


def _validate_worker_artifacts(data_file: Path, pytest_workers: int) -> None:
    """Prove that every expected xdist worker saved its own coverage artifact."""
    if pytest_workers == 0:
        expected = ("controller",)
    else:
        expected = tuple(f"gw{index}" for index in range(pytest_workers))
    missing = [
        worker
        for worker in expected
        if not list(data_file.parent.glob(f"{data_file.name}.{worker}.*"))
    ]
    if missing:
        raise CoverageMapError(
            "coverage collection missing worker artifacts: " + ", ".join(missing)
        )


def build_coverage_map(
    source_root: Path,
    *,
    python: str | None = None,
    timeout: float = 900.0,
) -> CoverageMap:
    """Run pytest once with per-test contexts and combine all xdist worker data."""
    root = Path(source_root).resolve()
    hashes = source_hashes(root)
    pytest_workers = _configured_pytest_workers(root)
    cache_dir = _cache_dir(root)
    cache_dir.mkdir(parents=True, exist_ok=True)
    exe = python or sys.executable
    scripts = str(Path(__file__).resolve().parents[1])
    env = os.environ.copy()
    env["MUTATION_HARNESS_COVERAGE_ROOT"] = str(root)
    with tempfile.TemporaryDirectory(
        prefix="mutation-coverage-", dir=cache_dir
    ) as temp:
        temp_path = Path(temp)
        data_file = temp_path / ".coverage"
        env["MUTATION_HARNESS_COVERAGE_FILE"] = str(data_file)
        coverage_config = temp_path / "coverage-startup.ini"
        coverage_config.write_text(
            "[run]\n"
            f"data_file = {data_file}\n"
            "parallel = true\n"
            f"source = {root}\n",
            encoding="utf-8",
        )
        startup_dir = temp_path / "startup"
        startup_dir.mkdir()
        (startup_dir / "sitecustomize.py").write_text(
            "import coverage\ncoverage.process_startup()\n", encoding="utf-8"
        )
        # The pytest plugin promotes this to COVERAGE_PROCESS_START only after
        # the controller has started, so sitecustomize instruments children
        # without starting a competing collector in the controller itself.
        env["MUTATION_HARNESS_COVERAGE_CONFIG"] = str(coverage_config)
        existing = env.get("PYTHONPATH")
        python_paths = [str(startup_dir), scripts]
        if existing:
            python_paths.append(existing)
        env["PYTHONPATH"] = os.pathsep.join(python_paths)
        pytest_args = [exe, "-m", "pytest", "-p", "mutation_harness.coverage_plugin"]
        if pytest_workers:
            pytest_args.extend(["-n", str(pytest_workers)])
        try:
            proc = subprocess.run(
                pytest_args,
                cwd=root,
                env=env,
                capture_output=True,
                text=True,
                timeout=timeout,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise CoverageMapError(
                f"coverage collection could not complete: {exc}"
            ) from exc
        if proc.returncode != 0:
            detail = (proc.stderr or proc.stdout or "pytest failed").strip()
            raise CoverageMapError(
                f"coverage collection pytest exit {proc.returncode}: {detail[-1000:]}"
            )
        _validate_worker_artifacts(data_file, pytest_workers)
        result = _coverage_to_map(
            data_file, root, hashes, pytest_workers=pytest_workers
        )
    # Tests are not trusted to leave the source tree untouched. Do not expose
    # a map even once if collection changed any coverage input underneath it.
    result.assert_fresh(root)
    raw = json.dumps(result.to_dict(), sort_keys=True, separators=(",", ":"))
    if len(raw.encode("utf-8")) > MAX_CACHE_BYTES:
        raise CoverageMapError(f"coverage cache exceeds {MAX_CACHE_BYTES} bytes")
    path = cache_dir / f"{result.content_hash}.json"
    temporary = path.with_suffix(f".json.{os.getpid()}.{time.time_ns()}.tmp")
    try:
        temporary.write_text(raw + "\n", encoding="utf-8")
        temporary.replace(path)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
    _purge_cache(cache_dir, keep=path)
    return result


def load_or_build_coverage_map(
    source_root: Path, *, python: str | None = None
) -> CoverageMap:
    """Use the exact content-addressed cache or rebuild it on any miss/staleness."""
    cached = load_cached_coverage_map(source_root)
    if cached is not None:
        return cached
    return build_coverage_map(source_root, python=python)
