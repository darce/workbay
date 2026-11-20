"""Copy prerequisite tree closures into an archive-side Git repository.

The caller owns the delta's shallow boundary and exact-tip bundle verification.
This step supplies independent objects without changing source refs/worktrees or
borrowing their object store. It does not authorize reaping.
"""

from __future__ import annotations

import math
import os
import re
import subprocess
import tempfile
import time
from collections.abc import Iterable
from itertools import zip_longest
from pathlib import Path
from typing import BinaryIO, TypedDict

_DIAGNOSTIC_BYTES = 8192
_OBJECT_ID = re.compile(rb"[0-9a-fA-F]{40}(?:[0-9a-fA-F]{24})?")


class TransferResult(TypedDict, total=False):
    ok: bool
    outcome: str
    failed_stage: str
    error: str


def transfer_prerequisites(
    source_repo: Path | str,
    scratch_repo: Path | str,
    prerequisite_shas: Iterable[str],
    *,
    timeout_s: float = 60,
) -> TransferResult:
    """Return ``transferred`` or a causal, staged ``failed`` result.

    ``scratch_repo`` is a caller-owned bare repository on the archive filesystem.
    Binary pack output and enumeration are spooled there, never buffered in
    Python or written through a source-side destination prefix. Every Git step
    shares one total deadline. Diagnostics retain at most 8192 bytes. Failed
    attempts remove only newly installed pack files and their own temp directory;
    pre-existing archive files and all source evidence are preserved.
    """
    stage = "prerequisite_objects"
    result: TransferResult = {"ok": False, "outcome": "failed"}
    pack_dir: Path | None = None
    initial: set[Path] | None = None

    try:
        if not math.isfinite(timeout_s) or timeout_s <= 0:
            raise ValueError("timeout: prerequisite transfer requires a positive finite deadline")
        deadline = time.monotonic() + timeout_s
        source_repo, scratch_repo = Path(source_repo).resolve(), Path(scratch_repo).resolve()
        pack_dir = scratch_repo / "objects" / "pack"
        initial = set(pack_dir.iterdir())
        if (scratch_repo / "objects" / "info" / "alternates").exists():
            raise OSError("independent prerequisite validation refuses archive object alternates")

        # Ambient Git repository selectors must not redirect either endpoint or
        # make missing archive objects appear present through source alternates.
        env = {name: value for name, value in os.environ.items() if not name.startswith("GIT_")}
        env.update(
            GIT_CONFIG_NOSYSTEM="1",
            GIT_CONFIG_GLOBAL=os.devnull,
            GIT_NO_REPLACE_OBJECTS="1",
            GIT_OPTIONAL_LOCKS="0",
        )

        def remaining() -> float:
            value = deadline - time.monotonic()
            if value <= 0:
                raise subprocess.TimeoutExpired("prerequisite transfer", timeout_s)
            return value

        with tempfile.TemporaryDirectory(prefix="prerequisite-transfer-", dir=scratch_repo.parent) as raw:
            temp = Path(raw)
            errors, ordinary = temp / "stderr", temp / "stdout"

            def run(
                repo: Path,
                args: list[str],
                *,
                stdin: BinaryIO | None = None,
                stdout: BinaryIO | None = None,
            ) -> None:
                with errors.open("wb") as err, ordinary.open("wb") as out:
                    completed = subprocess.run(
                        ["git", "-C", str(repo), *args],
                        stdin=stdin if stdin is not None else subprocess.DEVNULL,
                        stdout=stdout if stdout is not None else out,
                        stderr=err,
                        env=env,
                        timeout=remaining(),
                        check=False,
                    )
                if completed.returncode:
                    with errors.open("rb") as err:
                        detail = err.read(_DIAGNOSTIC_BYTES).decode("utf-8", errors="replace")
                    raise OSError(f"git {args[0]} exit {completed.returncode}: {detail}")
                remaining()

            requested = temp / "requested"
            with requested.open("wb") as stream:
                count = 0
                for sha in prerequisite_shas:
                    remaining()
                    if not isinstance(sha, str) or _OBJECT_ID.fullmatch(sha.encode("ascii")) is None:
                        raise ValueError("prerequisite must be a full hexadecimal Git object SHA")
                    stream.write(sha.encode("ascii") + b"\n")
                    count += 1
                if not count:
                    raise ValueError("no prerequisite object SHAs supplied")

            objects = temp / "objects"
            with requested.open("rb") as src, objects.open("wb") as dst:
                run(
                    source_repo,
                    ["rev-list", "--objects", "--no-object-names", "--no-walk", "--stdin"],
                    stdin=src,
                    stdout=dst,
                )
            with objects.open("rb") as stream:
                count = 0
                for line in stream:
                    remaining()
                    if _OBJECT_ID.fullmatch(line.rstrip(b"\n")) is None:
                        raise OSError("invalid prerequisite object enumeration")
                    count += 1
                if not count:
                    raise OSError("no prerequisite objects enumerated")

            stage = "prerequisite_pack"
            pack = temp / "prerequisite.pack"
            with objects.open("rb") as src, pack.open("wb") as dst:
                run(
                    source_repo,
                    [
                        "-c",
                        "pack.threads=1",
                        "-c",
                        "pack.windowMemory=64m",
                        "-c",
                        "pack.deltaCacheSize=32m",
                        "pack-objects",
                        "--stdout",
                    ],
                    stdin=src,
                    stdout=dst,
                )

            stage = "prerequisite_index"
            with pack.open("rb") as src:
                run(scratch_repo, ["index-pack", "--stdin"], stdin=src)

            stage = "prerequisite_index_validation"
            indexes = list(pack_dir.glob("*.idx"))
            if not indexes:
                raise OSError("prerequisite index validation found no installed pack index")
            for index in indexes:
                run(scratch_repo, ["verify-pack", str(index)])
            # A writer's zero exit status is not proof of installed objects.
            # Batch-check reports missing objects on stdout even with exit zero.
            checked = temp / "checked"
            with objects.open("rb") as src, checked.open("wb") as dst:
                run(scratch_repo, ["cat-file", "--batch-check=%(objectname) %(objecttype)"], stdin=src, stdout=dst)
            with objects.open("rb") as expected, checked.open("rb") as actual:
                for oid, row in zip_longest(expected, actual):
                    remaining()
                    fields = row.split() if row is not None else []
                    if (
                        oid is None
                        or len(fields) != 2
                        or fields[0] != oid.strip().lower()
                        or fields[1] not in (b"commit", b"tree", b"blob", b"tag")
                    ):
                        raise OSError("prerequisite object validation found missing or invalid archive objects")
            result = {"ok": True, "outcome": "transferred"}
    except subprocess.TimeoutExpired:
        result = {
            "ok": False,
            "outcome": "failed",
            "failed_stage": stage,
            "error": "timeout: prerequisite transfer deadline exhausted",
        }
    except (OSError, ValueError, TypeError, subprocess.SubprocessError) as exc:
        result = {"ok": False, "outcome": "failed", "failed_stage": stage, "error": str(exc)}
    finally:
        if not result["ok"] and pack_dir is not None and initial is not None:
            try:
                for path in set(pack_dir.iterdir()) - initial:
                    if path.is_file():
                        path.unlink()
            except OSError as exc:
                result["error"] = f"{result.get('error', '')}; prerequisite pack cleanup failed: {exc}"
        if "error" in result:
            result["error"] = result["error"].encode("utf-8")[:_DIAGNOSTIC_BYTES].decode("utf-8", errors="ignore")
    return result
