"""Hatch build hook: scrub staged package copies into wheel+sdist artifacts."""

from __future__ import annotations

import importlib.util
import shutil
import tempfile
from pathlib import Path
from typing import Any, NamedTuple, Protocol

try:  # hatchling is a build-time-only dep (build-system.requires), not a test dep.
    from hatchling.builders.hooks.plugin.interface import BuildHookInterface
    from hatchling.metadata.plugin.interface import MetadataHookInterface
    from hatchling.plugin import hookimpl
except ModuleNotFoundError:  # keep pure discover_scrub_targets importable for tests
    BuildHookInterface = object  # type: ignore[assignment,misc]
    MetadataHookInterface = object  # type: ignore[assignment,misc]

    def hookimpl(func):  # type: ignore[no-redef]
        return func

_TEXT_SUFFIXES = {
    ".md",
    ".py",
    ".yaml",
    ".yml",
    ".json",
    ".toml",
    ".txt",
    ".mk",
    ".sh",
    ".in",
    ".cfg",
    ".ini",
    ".template",
    ".j2",
}

# Build-support files vendored into the sdist so the wheel can be rebuilt from it.
# They carry the scrub matcher vocabulary verbatim (the literal AHMCP/AOMCP/... ids
# the transform looks for) and MUST NOT themselves be scrubbed — doing so rewrites
# those literals to "internal" and ships a gutted scrub engine. They ride into the
# sdist via hatchling's own only-include and are scan-exempt in the privacy gate
# (scripts/check_shipped_privacy.py: _SCAN_EXEMPT_RELPATHS), so skip them here.
_BUILD_SUPPORT_NAMES = {"_scrub_core.py", "hatch_build.py"}


class ScrubTarget(NamedTuple):
    source_path: Path
    wheel_prefix: str
    is_dir: bool


class _WheelConfigView(Protocol):
    packages: list[str] | None
    only_include: list[str] | None
    sources: list[str] | None


def _strip_src_prefix(path: str) -> str:
    if path.startswith("src/"):
        return path.removeprefix("src/")
    return path


def _rel_entry(entry: str, *, root: Path) -> str:
    entry_path = Path(entry)
    if entry_path.is_absolute():
        return entry_path.relative_to(root).as_posix()
    return entry.replace("\\", "/")


def _wheel_prefix_for_entry(entry: str, *, root: Path) -> str:
    rel = _rel_entry(entry, root=root)
    if rel.endswith(".py"):
        return Path(rel).name
    return _strip_src_prefix(rel.rstrip("/"))


def discover_scrub_targets(
    build_config: _WheelConfigView | Any,
    *,
    root: Path,
) -> list[ScrubTarget]:
    """Resolve layout-aware scrub targets from the wheel build config."""
    packages = list(getattr(build_config, "packages", None) or [])
    only_include = list(getattr(build_config, "only_include", None) or [])
    sources = list(getattr(build_config, "sources", None) or [])

    targets: list[ScrubTarget] = []
    covered_sources: set[Path] = set()

    def _append_target(source_path: Path, wheel_prefix: str) -> None:
        resolved = source_path.resolve()
        if resolved in covered_sources:
            return
        if not source_path.exists():
            raise FileNotFoundError(source_path)
        covered_sources.add(resolved)
        targets.append(
            ScrubTarget(
                source_path=source_path,
                wheel_prefix=wheel_prefix,
                is_dir=source_path.is_dir(),
            )
        )

    package_prefixes = {
        _strip_src_prefix(_rel_entry(package, root=root).rstrip("/")) for package in packages
    }

    for package in packages:
        rel = _rel_entry(package, root=root)
        _append_target(root / rel, _strip_src_prefix(rel.rstrip("/")))

    for entry in only_include:
        rel = _rel_entry(entry, root=root)
        if Path(rel).name in _BUILD_SUPPORT_NAMES:
            # Vendored build machinery ships verbatim; never stage/scrub it.
            continue
        wheel_prefix = _wheel_prefix_for_entry(entry, root=root)
        if rel.endswith(".py"):
            source_path = root / rel
            if not source_path.exists():
                source_path = root / "src" / Path(rel).name
        elif wheel_prefix in package_prefixes:
            source_path = root / rel
        else:
            # Root-layout only-include names map to on-disk src/<pkg> trees.
            candidate = root / "src" / wheel_prefix
            source_path = candidate if candidate.exists() else root / rel
        _append_target(source_path, wheel_prefix)

    if targets:
        return targets

    if sources:
        for source_root in sources:
            rel = _rel_entry(source_root, root=root)
            source_path = root / rel
            if not source_path.exists():
                raise FileNotFoundError(source_path)
            wheel_prefix = _strip_src_prefix(rel.rstrip("/"))
            targets.append(
                ScrubTarget(
                    source_path=source_path,
                    wheel_prefix=wheel_prefix,
                    is_dir=source_path.is_dir(),
                )
            )
        return targets

    return targets


def _load_scrub_text():
    scrub_core = Path(__file__).resolve().parent / "_scrub_core.py"
    spec = importlib.util.spec_from_file_location("scrub_core_build", scrub_core)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.scrub_text


def _should_scrub(path: Path) -> bool:
    return path.suffix.lower() in _TEXT_SUFFIXES or path.name in {
        "Makefile",
        "LICENSE",
        "README.md",
    }


def _scrub_tree(tree: Path, scrub_text) -> None:
    for path in tree.rglob("*"):
        if not path.is_file() or path.is_symlink():
            continue
        if not _should_scrub(path):
            continue
        try:
            original = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue
        scrubbed = scrub_text(original)
        if scrubbed != original:
            path.write_text(scrubbed, encoding="utf-8")


def _stage_target(staging: Path, target: ScrubTarget, scrub_text) -> Path:
    if target.is_dir:
        staged = staging / target.wheel_prefix
        # Drop compiled bytecode/caches: force_include bypasses the sdist `exclude`
        # globs, so an uncleaned source tree would otherwise ship unscanned .pyc.
        shutil.copytree(
            target.source_path,
            staged,
            ignore=shutil.ignore_patterns("__pycache__", "*.pyc", "*.pyo"),
        )
        _scrub_tree(staged, scrub_text)
        return staged

    staged = staging / target.wheel_prefix
    staged.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(target.source_path, staged)
    if _should_scrub(staged):
        original = staged.read_text(encoding="utf-8")
        scrubbed = scrub_text(original)
        if scrubbed != original:
            staged.write_text(scrubbed, encoding="utf-8")
    return staged


def _force_include_staged(
    force_include: dict[str, str],
    staged_root: Path,
    wheel_prefix: str,
) -> None:
    for path in staged_root.rglob("*"):
        if not path.is_file():
            continue
        rel = path.relative_to(staged_root).as_posix()
        force_include[str(path)] = f"{wheel_prefix}/{rel}" if rel else wheel_prefix


class ScrubAtBuildHook(BuildHookInterface):
    PLUGIN_NAME = "custom"

    @property
    def _staging_dirs(self) -> list[Path]:
        dirs = getattr(self, "_staging_dirs_store", None)
        if dirs is None:
            dirs = []
            self._staging_dirs_store = dirs
        return dirs

    def initialize(self, version: str, build_data: dict) -> None:
        scrub_text = _load_scrub_text()
        root = Path(self.root)
        targets = discover_scrub_targets(self.build_config, root=root)
        if not targets:
            return

        staging = Path(tempfile.mkdtemp(prefix="workbay-scrub-stage-", dir=None))
        if not staging.is_absolute():
            staging = staging.resolve()
        self._staging_dirs.append(staging)
        force_include = build_data.setdefault("force_include", {})

        for target in targets:
            staged = _stage_target(staging, target, scrub_text)
            if target.is_dir:
                _force_include_staged(force_include, staged, target.wheel_prefix)
            else:
                force_include[str(staged)] = target.wheel_prefix

    def finalize(self, version: str, build_data: dict, artifact_path: str) -> None:
        while self._staging_dirs:
            shutil.rmtree(self._staging_dirs.pop(), ignore_errors=True)


class ScrubMetadataHook(MetadataHookInterface):
    PLUGIN_NAME = "custom"

    def update(self, metadata: dict) -> None:
        """Scrub the README long-description hatchling bakes into core metadata.

        hatchling resolves ``readme`` -> the Description in wheel METADATA / sdist
        PKG-INFO straight from the authored README, OUTSIDE the build hook's
        force_include payload, so internal refs there would ship unscrubbed. A
        package opts in by declaring ``dynamic = ["readme"]`` (and dropping the
        static ``readme`` key); we supply the scrubbed long-description here.
        """
        scrub_text = _load_scrub_text()
        readme_path = Path(self.root) / "README.md"
        if readme_path.is_file():
            metadata["readme"] = {
                "content-type": "text/markdown",
                "text": scrub_text(readme_path.read_text(encoding="utf-8")),
            }


@hookimpl
def hatch_register_build_hook():
    return ScrubAtBuildHook


@hookimpl
def hatch_register_metadata_hook():
    return ScrubMetadataHook