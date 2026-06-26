#!/usr/bin/env python3
"""Per-package SOURCE_DATE_EPOCH sign-off for implementation note A1-bit published-bytes gate.

Uses the same commit-timestamp SDE as ``release.sh`` and verifies:
1. two local private-source builds are bit-identical, and
2. a private build matches a mirror export build at the same SDE.

Emit JSON receipts for operator sign-off before a package's first gated release.

Sign-off binding semantics (documented attestation, not commit-binding):
a receipt records the ``commit_sha`` and ``source_date_epoch`` it was produced
at, but ``--require-signoff`` only checks the boolean ``signed_off`` flag — it
is a one-time attestation that the package's build is SDE-reproducible, not a
per-release proof for the current HEAD. When the recorded commit differs from
the current HEAD, the check prints a stderr notice with the recorded commit so
the operator can judge receipt age; re-run ``--package <name> --record`` after
build-toolchain or packaging changes to refresh the attestation.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def source_date_epoch_from_git(repo_root: Path) -> str:
    result = subprocess.run(
        ["git", "log", "-1", "--format=%ct", "HEAD"],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.strip()


def commit_sha_from_git(repo_root: Path) -> str:
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.strip()


def default_signoff_registry(repo_root: Path) -> Path:
    return repo_root / "config" / "published-bytes-sde-signoff.json"


# Sentinel for a bare `--record` (no path argument): resolved against
# --repo-root at runtime via default_signoff_registry().
_DEFAULT_REGISTRY = Path("__default_signoff_registry__")


def _load_gate_module(repo_root: Path):
    import importlib.util

    gate_path = repo_root / "scripts" / "published_bytes_gate.py"
    spec = importlib.util.spec_from_file_location("published_bytes_gate", gate_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {gate_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def dist_digests_equal(left: dict[str, str], right: dict[str, str]) -> bool:
    return left == right


def make_signoff_receipt(
    *,
    package: str,
    source_date_epoch: str,
    commit_sha: str,
    local_repeat_bit_identical: bool,
    private_mirror_bit_identical: bool,
    digests: dict[str, str],
) -> dict[str, object]:
    signed_off = local_repeat_bit_identical and private_mirror_bit_identical
    return {
        "package": package,
        "commit_sha": commit_sha,
        "source_date_epoch": source_date_epoch,
        "local_repeat_bit_identical": local_repeat_bit_identical,
        "private_mirror_bit_identical": private_mirror_bit_identical,
        "wheel_sha256": digests["wheel_sha256"],
        "sdist_sha256": digests["sdist_sha256"],
        "signed_off": signed_off,
    }


def _run(cmd: list[str], *, env: dict[str, str] | None = None, cwd: Path) -> None:
    merged = None
    if env:
        merged = {**os.environ, **env}
    subprocess.run(cmd, cwd=cwd, env=merged, check=True)


def build_private_dist(
    package: str,
    *,
    repo_root: Path,
    dist_dir: Path,
    source_date_epoch: str,
) -> dict[str, str]:
    gate = _load_gate_module(repo_root)
    pkg_dir = repo_root / "packages" / package
    if dist_dir.exists():
        shutil.rmtree(dist_dir)
    dist_dir.mkdir(parents=True, exist_ok=True)
    env = {"SOURCE_DATE_EPOCH": source_date_epoch}
    _run(
        ["uvx", "--from", "build", "pyproject-build", "--outdir", str(dist_dir)],
        env=env,
        cwd=pkg_dir,
    )
    return gate.digest_dist_dir(dist_dir)


def build_mirror_dist(
    package: str,
    *,
    repo_root: Path,
    dist_dir: Path,
    source_date_epoch: str,
) -> dict[str, str]:
    gate = _load_gate_module(repo_root)
    with tempfile.TemporaryDirectory(prefix="pubgate-mirror-") as tmp:
        export_root = Path(tmp) / "export"
        _run(
            [
                sys.executable,
                str(repo_root / "scripts" / "export_public.py"),
                "--out",
                str(export_root),
                "--force",
                "--no-git",
            ],
            cwd=repo_root,
        )
        if dist_dir.exists():
            shutil.rmtree(dist_dir)
        dist_dir.mkdir(parents=True, exist_ok=True)
        env = {"SOURCE_DATE_EPOCH": source_date_epoch}
        _run(
            [
                sys.executable,
                "-m",
                "build",
                "--outdir",
                str(dist_dir),
                str(export_root / "packages" / package),
            ],
            env=env,
            cwd=repo_root,
        )
        return gate.digest_dist_dir(dist_dir)


def signoff_package(
    package: str,
    *,
    repo_root: Path,
    source_date_epoch: str | None = None,
    work_dir: Path | None = None,
) -> dict[str, object]:
    sde = source_date_epoch or source_date_epoch_from_git(repo_root)
    commit_sha = commit_sha_from_git(repo_root)
    base = work_dir or Path(tempfile.mkdtemp(prefix=f"pubgate-sde-{package}-"))
    local_a = base / "local-a"
    local_b = base / "local-b"
    mirror = base / "mirror"

    first = build_private_dist(package, repo_root=repo_root, dist_dir=local_a, source_date_epoch=sde)
    second = build_private_dist(package, repo_root=repo_root, dist_dir=local_b, source_date_epoch=sde)
    mirror_digests = build_mirror_dist(
        package, repo_root=repo_root, dist_dir=mirror, source_date_epoch=sde
    )

    return make_signoff_receipt(
        package=package,
        source_date_epoch=sde,
        commit_sha=commit_sha,
        local_repeat_bit_identical=dist_digests_equal(first, second),
        private_mirror_bit_identical=dist_digests_equal(first, mirror_digests),
        digests=first,
    )


def list_release_packages(repo_root: Path) -> list[str]:
    result = subprocess.run(
        [
            sys.executable,
            str(repo_root / "scripts" / "release_manifest.py"),
            "list",
            "--release-only",
            "--field",
            "name",
        ],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=True,
    )
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def load_registry(path: Path) -> dict[str, object]:
    if not path.exists():
        return {"packages": {}}
    return json.loads(path.read_text(encoding="utf-8"))


def require_signoff(
    package: str,
    *,
    repo_root: Path,
    registry_path: Path | None = None,
) -> bool:
    path = registry_path or default_signoff_registry(repo_root)
    registry = load_registry(path)
    packages = registry.get("packages", {})
    if not isinstance(packages, dict):
        return False
    receipt = packages.get(package)
    if not isinstance(receipt, dict):
        return False
    return bool(receipt.get("signed_off"))


def record_registry(path: Path, receipt: dict[str, object]) -> None:
    registry = load_registry(path)
    packages = registry.setdefault("packages", {})
    if not isinstance(packages, dict):
        raise SystemExit("invalid signoff registry: packages must be an object")
    packages[str(receipt["package"])] = receipt
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(registry, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="published-bytes-sde-signoff")
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--package", help="single publishable package to sign off")
    parser.add_argument("--all", action="store_true", help="sign off every release-only package")
    parser.add_argument(
        "--source-date-epoch",
        help="override SDE (default: git log -1 --format=%%ct HEAD, same as release.sh)",
    )
    parser.add_argument(
        "--record",
        nargs="?",
        type=Path,
        const=_DEFAULT_REGISTRY,
        default=None,
        help=(
            "record receipt(s) to the sign-off registry; bare --record uses "
            "config/published-bytes-sde-signoff.json, or pass an explicit path. "
            "Receipts are recorded to the default registry even without --record."
        ),
    )
    parser.add_argument("--emit-json", action="store_true", help="print JSON receipt(s) to stdout")
    parser.add_argument(
        "--require-signoff",
        metavar="PACKAGE",
        help="exit 0 only when PACKAGE has signed_off=true in the registry",
    )
    args = parser.parse_args(argv)

    if args.require_signoff:
        ok = require_signoff(args.require_signoff, repo_root=args.repo_root)
        if ok:
            # Documented attestation, not commit-binding (see module docstring):
            # surface the recorded commit so the operator can judge receipt age.
            registry = load_registry(default_signoff_registry(args.repo_root))
            packages = registry.get("packages", {})
            receipt = packages.get(args.require_signoff, {}) if isinstance(packages, dict) else {}
            recorded_sha = str(receipt.get("commit_sha", "")) if isinstance(receipt, dict) else ""
            head_sha = commit_sha_from_git(args.repo_root)
            if recorded_sha and recorded_sha != head_sha:
                print(
                    f"notice: {args.require_signoff} sign-off was recorded at "
                    f"{recorded_sha[:12]} (HEAD is {head_sha[:12]}); the receipt is a "
                    "one-time reproducibility attestation — re-record after "
                    "build-toolchain or packaging changes.",
                    file=sys.stderr,
                )
        return 0 if ok else 1

    if args.all:
        packages = list_release_packages(args.repo_root)
    elif args.package:
        packages = [args.package]
    else:
        parser.error("require --package <name> or --all")

    # Receipts always persist (consistently for --package and --all): bare
    # --record or no --record uses the default registry; an explicit --record
    # path overrides it.
    if args.record is None or args.record == _DEFAULT_REGISTRY:
        record_path = default_signoff_registry(args.repo_root)
    else:
        record_path = args.record

    receipts: list[dict[str, object]] = []
    failures = 0
    for package in packages:
        receipt = signoff_package(
            package,
            repo_root=args.repo_root,
            source_date_epoch=args.source_date_epoch,
        )
        receipts.append(receipt)
        record_registry(record_path, receipt)
        print(f"recorded sign-off receipt for {package} -> {record_path}", file=sys.stderr)
        if not receipt["signed_off"]:
            failures += 1
            print(
                f"signoff failed: {package} "
                f"(local_repeat={receipt['local_repeat_bit_identical']}, "
                f"private_mirror={receipt['private_mirror_bit_identical']})",
                file=sys.stderr,
            )

    if args.emit_json or len(receipts) == 1:
        payload = receipts[0] if len(receipts) == 1 else {"packages": receipts}
        print(json.dumps(payload, indent=2, sort_keys=True))

    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
