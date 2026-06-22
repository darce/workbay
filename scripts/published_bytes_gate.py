#!/usr/bin/env python3
"""Published-bytes A1-bit parity gate for ``release-publish.yml`` (implementation note).

The locally gate-validated wheel+sdist digests are recorded by ``release.sh``
and passed as workflow dispatch inputs. The runner rebuild must match before
artifact upload / Trusted Publishing proceeds.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def digest_dist_dir(dist_dir: Path) -> dict[str, str]:
    """Return wheel and sdist sha256 digests for exactly one dist pair."""
    wheels = sorted(dist_dir.glob("*.whl"))
    sdists = sorted(dist_dir.glob("*.tar.gz"))
    if len(wheels) != 1 or len(sdists) != 1:
        raise SystemExit(
            f"expected exactly one wheel and one sdist in {dist_dir} "
            f"(found {len(wheels)} wheels, {len(sdists)} sdists)"
        )
    return {
        "wheel_sha256": sha256_file(wheels[0]),
        "sdist_sha256": sha256_file(sdists[0]),
    }


def verify_dist_digests(
    dist_dir: Path,
    *,
    expected_wheel_sha256: str,
    expected_sdist_sha256: str,
) -> None:
    actual = digest_dist_dir(dist_dir)
    if actual["wheel_sha256"] != expected_wheel_sha256:
        raise SystemExit(
            "published-bytes gate failed: wheel sha256 mismatch "
            f"(expected {expected_wheel_sha256}, got {actual['wheel_sha256']})"
        )
    if actual["sdist_sha256"] != expected_sdist_sha256:
        raise SystemExit(
            "published-bytes gate failed: sdist sha256 mismatch "
            f"(expected {expected_sdist_sha256}, got {actual['sdist_sha256']})"
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="published-bytes-gate")
    parser.add_argument("--dist-dir", required=True, type=Path, help="dist directory")
    parser.add_argument(
        "--expected-sha256-wheel",
        default=None,
        help="expected wheel sha256 from the locally gate-validated build",
    )
    parser.add_argument(
        "--expected-sha256-sdist",
        default=None,
        help="expected sdist sha256 from the locally gate-validated build",
    )
    parser.add_argument(
        "--emit-json",
        action="store_true",
        help="print digest_dist_dir JSON to stdout and exit",
    )
    args = parser.parse_args(argv)

    if args.emit_json:
        print(json.dumps(digest_dist_dir(args.dist_dir), sort_keys=True))
        return 0

    if args.expected_sha256_wheel is None or args.expected_sha256_sdist is None:
        parser.error(
            "verify mode requires --expected-sha256-wheel and --expected-sha256-sdist "
            "(or pass --emit-json)"
        )

    verify_dist_digests(
        args.dist_dir,
        expected_wheel_sha256=args.expected_sha256_wheel,
        expected_sdist_sha256=args.expected_sha256_sdist,
    )
    print("ok: published bytes match locally gate-validated digests")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
