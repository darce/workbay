#!/usr/bin/env python3
"""Fail-closed privacy gate for surfaces shipped to PyPI.

implementation note: scan unpacked built wheel+sdist artifacts instead of the raw source
tree. The authored tree stays pristine (unscrubbed); scrubbing happens at build
time via ``packages/workbay-system/hatch_build.py``.

Run standalone (``python scripts/check_shipped_privacy.py``) or via
``make preflight``; exits non-zero with a per-file report when anything leaks.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import zipfile
from pathlib import Path

from export_public import (
    FORBIDDEN_TEXT_TOKENS,
    FORBIDDEN_TEXT_RES,
    INTERNAL_DOC_REF_RE,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
MANIFEST_PATH = REPO_ROOT / "config" / "release" / "packages.json"

SCRUB_CORRUPTED_IDENTIFIER_RE = re.compile(
    r"(?<![A-Za-z0-9_])internal_[A-Z][A-Z0-9]*(?:_[A-Z0-9]+)+"
)

_SKIP_DIR_NAMES = {"dist", "build", "__pycache__"}
_SKIP_SUFFIXES = {".pyc"}

# The scrub transform's own source is the one file that MUST contain the internal
# project-id vocabulary verbatim — it is the matcher dictionary, not prose. It
# ships at the sdist top level (vendored so the wheel can be rebuilt from sdist)
# and would otherwise self-trip the gate. Exempt it by exact top-level path only
# (a same-named file anywhere under the package payload is still scanned). The
# build hook is exempt for the same reason. This is the single, narrow scan
# carve-out; everything else in every artifact is scanned.
_SCAN_EXEMPT_RELPATHS = {"_scrub_core.py", "hatch_build.py"}


def _publishable_packages() -> list[dict[str, object]]:
    manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    return [pkg for pkg in manifest.get("packages", []) if pkg.get("git_mirror")]


def _shipped_roots(package_dir: Path) -> list[Path]:
    """Source-tree roots a git-mirror package would ship (report-only helper).

    Consumed by scripts/scrub_shipped_source.py to preview which authored files
    the shared scrub transform would change. The privacy gate itself scans built
    artifacts, not these source roots.
    """
    roots: list[Path] = []
    if not package_dir.is_dir():
        return roots
    src = package_dir / "src"
    search_bases = [src] if src.is_dir() else [package_dir]
    for base in search_bases:
        if not base.is_dir():
            continue
        for child in base.iterdir():
            if child.is_dir() and (child / "__init__.py").exists():
                roots.append(child)
    readme = package_dir / "README.md"
    if readme.exists():
        roots.append(readme)
    return roots


def _iter_shipped_files(root: Path):
    """Iterate shippable source files under a root (report-only helper).

    Consumed by scripts/scrub_shipped_source.py alongside _shipped_roots.
    """
    if root.is_file():
        yield root
        return
    for path in root.rglob("*"):
        if not path.is_file() or path.is_symlink():
            continue
        if any(part in _SKIP_DIR_NAMES for part in path.parts):
            continue
        if path.suffix in _SKIP_SUFFIXES:
            continue
        yield path


def _iter_text_files(root: Path):
    if root.is_file():
        yield root
        return
    for path in root.rglob("*"):
        if not path.is_file() or path.is_symlink():
            continue
        if any(part in _SKIP_DIR_NAMES for part in path.parts):
            continue
        if path.suffix in _SKIP_SUFFIXES:
            continue
        try:
            path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue
        yield path


def _scan_text(rel: str, text: str) -> list[str]:
    findings: list[str] = []
    for token in FORBIDDEN_TEXT_TOKENS:
        if token in text:
            findings.append(f"  {rel}: forbidden token {token!r}")
    for regex in FORBIDDEN_TEXT_RES:
        match = regex.search(text)
        if match:
            findings.append(f"  {rel}: internal ref/process token {match.group(0)!r}")
            break
    for match in INTERNAL_DOC_REF_RE.finditer(text):
        ref = match.group(0)
        if (REPO_ROOT / ref).is_file():
            findings.append(f"  {rel}: internal planning-doc reference {ref!r}")
            break
    corrupted = SCRUB_CORRUPTED_IDENTIFIER_RE.search(text)
    if corrupted:
        findings.append(
            f"  {rel}: scrub-corrupted identifier {corrupted.group(0)!r} "
            "(an UPPER_SNAKE name was eaten by an over-eager scrub; restore the "
            "original prefix, e.g. WORKBAY_*)"
        )
    return findings


def _scan_tree(root: Path, label: str) -> list[str]:
    findings: list[str] = []
    for path in _iter_text_files(root):
        relpath = path.relative_to(root).as_posix()
        if relpath in _SCAN_EXEMPT_RELPATHS:
            continue
        # dist-info/RECORD is a hash manifest: base64 sha256 digests collide
        # nondeterministically with the internal-ref patterns (e.g. 'internal'
        # fragments). Every member file RECORD lists is scanned individually,
        # so skipping the manifest itself loses no coverage.
        if relpath.endswith(".dist-info/RECORD"):
            continue
        text = path.read_text(encoding="utf-8")
        rel = f"{label}:{relpath}"
        findings.extend(_scan_text(rel, text))
    return findings


_PRIVACY_UV_BUILD_TIMEOUT_DEFAULT = 120.0


def _resolve_uv_build_timeout(default: float = _PRIVACY_UV_BUILD_TIMEOUT_DEFAULT) -> float:
    raw = os.environ.get("WORKBAY_PRIVACY_UV_BUILD_TIMEOUT_SECONDS", "").strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError:
        return default
    return value if value > 0 else default


def _build_artifacts(package_dir: Path, out_dir: Path) -> tuple[Path, Path]:
    uv = shutil.which("uv")
    if uv is None:
        raise RuntimeError(
            "uv is required to build publishable artifacts for the privacy gate"
        )
    out_dir.mkdir(parents=True, exist_ok=True)
    timeout_s = _resolve_uv_build_timeout()
    try:
        proc = subprocess.run(
            [uv, "build", "--out-dir", str(out_dir), str(package_dir)],
            capture_output=True,
            text=True,
            timeout=timeout_s,
        )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(
            f"uv build timed out after {timeout_s:.0f}s "
            "(override with WORKBAY_PRIVACY_UV_BUILD_TIMEOUT_SECONDS)"
        ) from exc
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr or proc.stdout)
    wheels = sorted(out_dir.glob("*.whl"))
    sdists = sorted(out_dir.glob("*.tar.gz"))
    if len(wheels) != 1 or len(sdists) != 1:
        raise RuntimeError(f"expected one wheel and one sdist in {out_dir}")
    return wheels[0], sdists[0]


def _unpack_wheel(wheel: Path, dest: Path) -> None:
    dest.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(wheel) as zf:
        zf.extractall(dest)


def _unpack_sdist(sdist: Path, dest: Path) -> None:
    dest.mkdir(parents=True, exist_ok=True)
    with tarfile.open(sdist) as tf:
        tf.extractall(dest, filter="data")


def _sdist_top_dir(sdist_root: Path) -> Path:
    """Return the single top-level directory inside an unpacked sdist."""
    candidates = sorted(sdist_root.iterdir())
    if len(candidates) != 1 or not candidates[0].is_dir():
        raise RuntimeError(f"unexpected sdist layout under {sdist_root}")
    return candidates[0]


def _check_scrub_engine_parity(
    sdist_top: Path, label: str, *, source_dir: Path | None
) -> list[str]:
    """Detect a gutted scrub engine in the shipped sdist.

    _scan_tree exempts _SCAN_EXEMPT_RELPATHS from the text matchers, so a
    corrupted (itself-scrubbed) engine copy is otherwise invisible to this
    gate — the 0.2.11 incident class, where every matcher prefix shipped as
    the literal "internal". The shipped copies must be byte-identical to the
    package's own vendored sources (which test_scrub_transform_parity in turn
    pins to scripts/_scrub_core.py at body level — vendored docstrings differ).
    """
    if source_dir is None:
        return []
    findings: list[str] = []
    sources = {
        "_scrub_core.py": source_dir / "_scrub_core.py",
        "hatch_build.py": source_dir / "hatch_build.py",
    }
    for name, source in sources.items():
        shipped = sdist_top / name
        if not shipped.is_file():
            continue
        if not source.is_file() or shipped.read_bytes() != source.read_bytes():
            findings.append(
                f"  {label}:{name}: shipped scrub-engine file diverged from its "
                "source (a scrub pass over the engine guts its matchers; "
                "rebuild from clean sources)"
            )
    return findings


def _scan_built_artifacts(
    wheel: Path,
    sdist: Path | None = None,
    *,
    package_name: str,
    source_dir: Path | None = None,
) -> list[str]:
    findings: list[str] = []
    with tempfile.TemporaryDirectory(prefix="shipped-privacy-") as tmp:
        tmp_path = Path(tmp)
        wheel_root = tmp_path / "wheel"
        _unpack_wheel(wheel, wheel_root)
        findings.extend(_scan_tree(wheel_root, f"{wheel.name}"))
        if sdist is not None:
            sdist_root = tmp_path / "sdist"
            _unpack_sdist(sdist, sdist_root)
            # Scan the ENTIRE unpacked sdist, not just the importable package
            # subtree: the sdist also ships top-level build tooling and any other
            # included file, all of which reach PyPI and must be gated.
            sdist_top = _sdist_top_dir(sdist_root)
            findings.extend(_scan_tree(sdist_top, f"{sdist.name}"))
            findings.extend(
                _check_scrub_engine_parity(
                    sdist_top, f"{sdist.name}", source_dir=source_dir
                )
            )
    return findings


def _scan_prebuilt_dist(
    dist_dir: Path, *, package_name: str, source_dir: Path | None = None
) -> list[str]:
    wheels = sorted(dist_dir.glob("*.whl"))
    sdists = sorted(dist_dir.glob("*.tar.gz"))
    # Fail closed: a git-mirror build emits exactly one wheel and one sdist.
    # Treating a missing/extra sdist as "skip the sdist scan" would let an
    # sdist-borne leak ship unexamined.
    if len(wheels) != 1 or len(sdists) != 1:
        raise RuntimeError(
            f"expected exactly one wheel and one sdist in {dist_dir} "
            f"(found {len(wheels)} wheels, {len(sdists)} sdists)"
        )
    return _scan_built_artifacts(
        wheels[0], sdists[0], package_name=package_name, source_dir=source_dir
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dist-dir",
        type=Path,
        default=None,
        help="Use prebuilt wheel+sdist artifacts from this directory instead of building.",
    )
    parser.add_argument(
        "--package",
        default=None,
        help="Only scan the named git-mirror package (matches packages.json name).",
    )
    args = parser.parse_args(argv)

    # A dist dir holds ONE package's wheel+sdist; scanning it once per
    # git-mirror package would mislabel findings 8x and "pass" packages
    # whose artifacts were never built.
    if args.dist_dir is not None and args.package is None:
        parser.error(
            "--dist-dir requires --package (a dist dir holds one package's artifacts)"
        )

    packages = _publishable_packages()
    if args.package is not None:
        packages = [pkg for pkg in packages if pkg.get("name") == args.package]
        if not packages:
            raise SystemExit(f"unknown git-mirror package: {args.package}")

    findings: list[str] = []
    with tempfile.TemporaryDirectory(prefix="shipped-privacy-build-") as build_tmp:
        build_root = Path(build_tmp)
        for pkg in packages:
            package_dir = REPO_ROOT / str(pkg["path"])
            name = str(pkg["name"])
            if args.dist_dir is not None:
                findings.extend(
                    _scan_prebuilt_dist(
                        args.dist_dir, package_name=name, source_dir=package_dir
                    )
                )
                continue
            pkg_out = build_root / name
            wheel, sdist = _build_artifacts(package_dir, pkg_out)
            findings.extend(
                _scan_built_artifacts(
                    wheel, sdist, package_name=name, source_dir=package_dir
                )
            )

    if findings:
        print(
            "shipped-surface privacy gate — built artifacts ship personal info or",
            file=sys.stderr,
        )
        print(
            "internal project ids and must be fixed before releasing:", file=sys.stderr
        )
        for line in findings:
            print(line, file=sys.stderr)
        return 1
    print("ok: no publishable wheel/sdist ships personal info or internal project ids")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
