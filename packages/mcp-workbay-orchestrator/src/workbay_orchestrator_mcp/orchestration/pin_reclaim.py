"""Report and explicitly purge redundant reclaimed pins and archive bundles.

Pin writers do not take the landing mutation lock, and this module does not
take it either. Each pin deletion uses Git's old-value compare-and-swap while
the report's main tip remains an ancestor of current main, or after a backing
bundle has passed the isolated strong restore check. A same-value re-pin does
not reset the loose ref's mtime and cannot make a pin unsafe to delete. Bundle
deletion first renames the path to a private quarantine name and compares the
inode actually moved before unlinking, then re-validates main immediately
before unlinking; the remaining filesystem-versus-ref window is not atomic.
It never deletes by a stale pathname alone. Only bundles whose heads are
already held by main are eligible.
"""

from __future__ import annotations

import argparse
import errno
import hashlib
import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from workbay_orchestrator_mcp.lane_reaping import (
    _bundle_dir,
    _bundle_prerequisite_shas,
    _verified_bundle_contains,
)

from .landing_log import CarrierResult, find_carrier

REPORT_SCHEMA = "pin_reclaim_report_v1"
RULE = "retention_v1"
DEFAULT_RETENTION_DAYS = 7
MIN_RETENTION_DAYS = 1

PIN_CLASSES = (
    "pin_malformed",
    "pin_probe_failed",
    "pin_cited",
    "pin_unbundled",
    "pin_age_unknown",
    "pin_young",
    "pin_landed_expired",
    "pin_bundled_expired",
)
BUNDLE_CLASSES = (
    "bundle_probe_failed",
    "bundle_holds_unlanded",
    "bundle_young",
    "bundle_redundant_expired",
)
ELIGIBLE = frozenset({"pin_landed_expired", "pin_bundled_expired", "bundle_redundant_expired"})

_HEX40_RE = re.compile(r"^[0-9a-f]{40}$")
_TOKEN_RE = re.compile(r"^[0-9a-f]{7,40}$")
_CITATION_PATTERN = r"[0-9a-f]{7,40}"


def _run_git(
    repo: Path | str, *args: str, timeout_s: float, input_text: str | None = None
) -> tuple[int | None, str, str]:
    """Run one bounded, list-form git command and normalize process failures."""
    try:
        proc = subprocess.run(
            ["git", "-C", str(repo), *args],
            capture_output=True,
            text=True,
            timeout=timeout_s,
            check=False,
            input=input_text,
        )
    except subprocess.TimeoutExpired:
        return None, "", "timeout"
    except OSError as exc:
        return None, "", f"{type(exc).__name__}:{exc}"
    return proc.returncode, proc.stdout, proc.stderr


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _digest_payload(report: dict[str, Any]) -> dict[str, Any]:
    return {
        key: report[key]
        for key in (
            "schema",
            "rule",
            "complete",
            "error",
            "main_tip",
            "clock",
            "retention_days",
            "archive_dir",
            "product_bundle_dir",
            "pins",
            "bundles",
            "product_bundles",
            "skipped",
        )
    }


def _digest(report: dict[str, Any]) -> str:
    return hashlib.sha256(_canonical(_digest_payload(report)).encode("ascii")).hexdigest()


def _primary_and_common(repo: Path, timeout_s: float) -> tuple[Path | None, Path | None, str | None]:
    rc, stdout, _stderr = _run_git(repo, "rev-parse", "--git-common-dir", timeout_s=timeout_s)
    raw = stdout.strip()
    if rc != 0 or not raw:
        return None, None, "primary_worktree_unresolvable"
    common = Path(raw)
    if not common.is_absolute():
        common = repo / common
    common = common.resolve()
    if common.name != ".git":
        return None, common, "primary_worktree_unresolvable"
    return common.parent, common, None


def _packed_ref_names(common: Path) -> tuple[set[str], int | None]:
    packed_file = common / "packed-refs"
    try:
        stat = packed_file.stat()
        names: set[str] = set()
        with packed_file.open("r", encoding="utf-8", errors="replace") as handle:
            for line in handle:
                if line.startswith(("#", "^")):
                    continue
                fields = line.rstrip("\n").split(" ", 1)
                if len(fields) == 2:
                    names.add(fields[1])
        return names, stat.st_mtime_ns
    except FileNotFoundError:
        return set(), None
    except OSError:
        return set(), None


def _ref_mtime_ns(common: Path, refname: str, packed: set[str]) -> tuple[int | None, str]:
    """Return loose-ref age or the packed-refs mtime lower bound."""
    loose = common / refname
    try:
        stat = os.lstat(loose)
        if os.path.isfile(loose) and not os.path.islink(loose):
            return stat.st_mtime_ns, "loose_ref"
    except OSError:
        pass
    if refname in packed:
        try:
            return os.lstat(common / "packed-refs").st_mtime_ns, "packed_refs_bound"
        except OSError:
            pass
    return None, "unknown"


def _parse_ref_list(output: str) -> list[tuple[str, str, str]]:
    rows: list[tuple[str, str, str]] = []
    for line in output.splitlines():
        parts = line.rstrip("\r").split("\x00")
        if len(parts) == 3:
            ref, sha, objecttype = parts
            if ref:
                rows.append((ref, sha.lower(), objecttype))
    return sorted(rows)


def _shape(ref: str, sha: str) -> tuple[str, str | None, str | None, str | None]:
    prefix = "refs/reclaimed/"
    rest = ref[len(prefix) :] if ref.startswith(prefix) else ref
    lane, slash, leaf = rest.rpartition("/")
    if slash and lane and _HEX40_RE.fullmatch(leaf):
        return "new", lane, None, None if leaf == sha else "leaf_mismatch"
    return "legacy", None, rest, None


def _citation_scan(repo: Path, tip: str) -> tuple[dict[str, set[str]], str | None]:
    rc, stdout, _stderr = _run_git(
        repo,
        "grep",
        "-I",
        "-o",
        "-w",
        "-i",
        "-E",
        "--null",
        "-e",
        _CITATION_PATTERN,
        tip,
        "--",
        timeout_s=60.0,
    )
    if rc == 1:
        return {}, None
    if rc != 0:
        return {}, "citation_scan_failed"
    tokens: dict[str, set[str]] = {}
    for record in stdout.splitlines():
        if "\x00" not in record:
            continue
        location, token = record.split("\x00", 1)
        token = token.strip().lower()
        # With an explicit tree-ish Git prefixes the path with "<sha>:".
        path = location.split(":", 1)[1] if ":" in location else location
        if _TOKEN_RE.fullmatch(token):
            tokens.setdefault(token, set()).add(path)
    return tokens, None


def _cited_paths(sha: str, tokens: dict[str, set[str]]) -> list[str]:
    paths: set[str] = set()
    for length in range(7, min(len(sha), 40) + 1):
        paths.update(tokens.get(sha[:length].lower(), set()))
    return sorted(paths)[:5]


def _scan_bundle_dir(
    directory: Path,
    *,
    location: str,
    skipped: list[dict[str, str]],
) -> tuple[list[Path], str | None]:
    try:
        entries = list(os.scandir(directory))
    except FileNotFoundError:
        return [], None
    except OSError:
        return [], "archive_unreadable" if location == "archive" else "product_bundle_unreadable"
    paths: list[Path] = []
    for entry in entries:
        name = entry.name
        if ".bundle.purge-" in name:
            skipped.append({"location": location, "name": name, "reason": "purge_quarantine"})
            continue
        if not name.endswith(".bundle"):
            continue
        try:
            if entry.is_symlink():
                skipped.append({"location": location, "name": name, "reason": "symlink"})
                continue
            if not entry.is_file(follow_symlinks=False):
                skipped.append({"location": location, "name": name, "reason": "not_regular_file"})
                continue
        except OSError:
            skipped.append({"location": location, "name": name, "reason": "not_regular_file"})
            continue
        paths.append(Path(entry.path))
    return sorted(paths, key=lambda path: path.name), None


def _read_bundle(path: Path, location: str) -> dict[str, Any]:
    try:
        stat = os.lstat(path)
        if not os.path.isfile(path) or os.path.islink(path):
            raise OSError("not a regular bundle")
    except OSError:
        return {
            "name": path.name,
            "size": 0,
            "mtime_ns": 0,
            "ino": 0,
            "heads": [],
            "prerequisites": [],
            "full_history": True,
            "detail": "bundle_stat_failed",
            "_path": path,
            "_location": location,
            "_listed": False,
            "_probe_error": True,
        }
    rc, stdout, _stderr = _run_git(path.parent, "bundle", "list-heads", str(path), timeout_s=30.0)
    if rc == 0:
        heads = sorted({line.split(maxsplit=1)[0].lower() for line in stdout.splitlines() if line.strip()})
        heads = [head for head in heads if _HEX40_RE.fullmatch(head)]
    else:
        heads = []
    prereq = sorted(set(sha.lower() for sha in _bundle_prerequisite_shas(path)))
    if rc is None:
        detail = "list_heads_failed:timeout"
    elif rc != 0:
        detail = f"list_heads_failed:{rc}"
    elif not heads:
        detail = "no_heads"
    else:
        detail = None
    return {
        "name": path.name,
        "size": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
        "ino": stat.st_ino,
        "heads": heads,
        "prerequisites": prereq,
        "full_history": not prereq,
        "detail": detail,
        "_path": path,
        "_location": location,
        "_listed": rc == 0 and bool(heads),
        "_probe_error": rc != 0 or not heads,
    }


def _probe_error(result: CarrierResult | None) -> bool:
    return result is None or (result.error is not None and result.error != "commit_missing")


def _class_counts(rows: list[dict[str, Any]], classes: tuple[str, ...]) -> dict[str, int]:
    counts = Counter(row.get("class") for row in rows)
    return {name: int(counts.get(name, 0)) for name in classes}


def _build_purge_report_impl(
    repo: str | Path,
    *,
    main_ref: str = "main",
    archive_dir: str | Path | None = None,
    retention_days: int = DEFAULT_RETENTION_DAYS,
    now: int | None = None,
    timeout_s: float = 10.0,
) -> dict[str, Any]:
    """Build a read-only, stable-digest retention report over pins and bundles."""
    if isinstance(retention_days, bool) or not isinstance(retention_days, int) or retention_days < MIN_RETENTION_DAYS:
        raise ValueError(f"retention_days must be an integer of at least {MIN_RETENTION_DAYS}")
    started = time.monotonic()
    try:
        clock = int(time.time() if now is None else now)
        clock_error = None
    except (TypeError, ValueError, OverflowError):
        clock = int(time.time())
        clock_error = "clock_invalid"
    root = Path(repo).expanduser().resolve()
    report: dict[str, Any] = {
        "schema": REPORT_SCHEMA,
        "rule": RULE,
        "complete": False,
        "error": clock_error,
        "main_ref": main_ref,
        "main_tip": None,
        "clock": clock,
        "retention_days": retention_days,
        "archive_dir": None,
        "product_bundle_dir": None,
        "digest": "",
        "counts": {**{key: 0 for key in PIN_CLASSES}, **{key: 0 for key in BUNDLE_CLASSES}},
        "totals": {
            "pins": 0,
            "bundles": 0,
            "eligible_pins": 0,
            "eligible_bundles": 0,
            "sole_copy_pins": 0,
            "cited_pins": 0,
            "archive_bytes": 0,
            "redundant_bytes": 0,
            "full_history_unlanded_bytes": 0,
            "product_bundles": 0,
            "product_bytes": 0,
        },
        "pins": [],
        "bundles": [],
        "product_bundles": [],
        "skipped": [],
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "elapsed_s": 0.0,
    }

    rc, stdout, _stderr = _run_git(
        root, "rev-parse", "--verify", "--quiet", f"{main_ref}^{{commit}}", timeout_s=timeout_s
    )
    main_tip = stdout.strip().lower() if rc == 0 else ""
    if rc != 0 or not _HEX40_RE.fullmatch(main_tip):
        report["error"] = report["error"] or "main_unresolvable"
        report["digest"] = _digest(report)
        report["elapsed_s"] = round(time.monotonic() - started, 6)
        return report
    report["main_tip"] = main_tip

    primary, common, path_error = _primary_and_common(root, timeout_s)
    if path_error is not None or primary is None or common is None:
        report["error"] = report["error"] or "primary_worktree_unresolvable"
        report["digest"] = _digest(report)
        report["elapsed_s"] = round(time.monotonic() - started, 6)
        return report
    archive = (
        Path(archive_dir).expanduser().resolve()
        if archive_dir is not None
        else primary / ".task-state" / "branch-archive"
    )
    report["archive_dir"] = str(archive.resolve())
    try:
        product = Path(_bundle_dir(primary)).expanduser().resolve()
        report["product_bundle_dir"] = str(product)
    except OSError:
        product = None
        report["error"] = report["error"] or "product_bundle_dir_unavailable"

    rc, stdout, _stderr = _run_git(
        root,
        "for-each-ref",
        "--format=%(refname)%00%(objectname)%00%(objecttype)",
        "refs/reclaimed/",
        timeout_s=timeout_s,
    )
    pin_specs: list[tuple[str, str, str]] = []
    if rc != 0:
        report["error"] = report["error"] or "pin_list_failed"
    else:
        pin_specs = _parse_ref_list(stdout)

    packed, _packed_mtime = _packed_ref_names(common)
    aged_pins: list[dict[str, Any]] = []
    for ref, sha, objecttype in pin_specs:
        shape, lane, name, shape_error = _shape(ref, sha)
        mtime_ns, age_source = _ref_mtime_ns(common, ref, packed)
        age_s = max(0, clock - (mtime_ns // 1_000_000_000)) if mtime_ns is not None else None
        aged_pins.append(
            {
                "ref": ref,
                "sha": sha,
                "objecttype": objecttype,
                "shape": shape,
                "lane": lane,
                "name": name,
                "class": "",
                "detail": None,
                "contained": None,
                "carrier": None,
                "lineage": None,
                "cited_by": [],
                "backing": None,
                "age_source": age_source,
                "age_s": age_s,
                "live_head": None,
                "_malformed_detail": f"objecttype:{objecttype}" if objecttype != "commit" else shape_error,
            }
        )

    rc, stdout, _stderr = _run_git(root, "for-each-ref", "--format=%(objectname)", "refs/heads/", timeout_s=timeout_s)
    live_heads: set[str] | None
    if rc == 0:
        live_heads = {line.strip().lower() for line in stdout.splitlines() if _HEX40_RE.fullmatch(line.strip().lower())}
    else:
        live_heads = None
        report["error"] = report["error"] or "branch_list_failed"
    for row in aged_pins:
        row["live_head"] = None if live_heads is None else row["sha"] in live_heads

    tokens, citation_error = _citation_scan(root, main_tip)
    if citation_error:
        report["error"] = report["error"] or citation_error
    for row in aged_pins:
        if row["_malformed_detail"] is None and not citation_error:
            row["cited_by"] = _cited_paths(row["sha"], tokens)

    skipped: list[dict[str, str]] = report["skipped"]
    archive_paths, archive_error = _scan_bundle_dir(archive, location="archive", skipped=skipped)
    if archive_error:
        report["error"] = report["error"] or archive_error
    product_paths: list[Path] = []
    product_error = None
    if product is not None:
        product_paths, product_error = _scan_bundle_dir(product, location="product", skipped=skipped)
        if product_error:
            report["error"] = report["error"] or product_error

    archive_internal = [_read_bundle(path, "archive") for path in archive_paths]
    product_internal = [_read_bundle(path, "product") for path in product_paths]

    shas: set[str] = set()
    for row in aged_pins:
        if row["_malformed_detail"] is None:
            shas.add(row["sha"])
    for bundle in archive_internal + product_internal:
        shas.update(bundle["heads"])
        shas.update(bundle["prerequisites"])
    probes: dict[str, CarrierResult | None] = {}
    for sha in sorted(shas):
        try:
            probes[sha] = find_carrier(root, sha, main_ref=main_tip, timeout_s=timeout_s)
        except Exception as exc:  # containment is a fail-closed probe
            probes[sha] = CarrierResult(sha, main_tip, None, False, None, None, f"probe_exception:{type(exc).__name__}")

    for bundle in archive_internal + product_internal:
        if bundle["_listed"]:
            bundle["_head_error"] = any(_probe_error(probes.get(sha)) for sha in bundle["heads"])
        else:
            bundle["_head_error"] = True
    if any(_probe_error(result) for result in probes.values()):
        report["error"] = report["error"] or "probe_failed"

    retention_seconds = retention_days * 86400
    for row in aged_pins:
        malformed = row.pop("_malformed_detail")
        if malformed is not None:
            row["class"] = "pin_malformed"
            row["detail"] = malformed
            continue
        probe = probes.get(row["sha"])
        probe_failed = _probe_error(probe)
        contained = bool(probe.contained) if probe is not None else False
        row["contained"] = contained if not probe_failed else None
        row["carrier"] = probe.carrier if probe is not None else None
        row["lineage"] = probe.lineage if probe is not None else None
        if citation_error:
            row["class"] = "pin_probe_failed"
            row["detail"] = "citation_scan_failed"
            continue
        if probe_failed:
            row["class"] = "pin_probe_failed"
            row["detail"] = probe.error if probe is not None else "probe_missing"
            continue

        candidates: list[tuple[int, int, str, dict[str, Any]]] = []
        backing_probe_failed = False
        if not contained:
            for bundle in archive_internal + product_internal:
                if not bundle["_listed"]:
                    # A failed list-heads probe could hide this pin as a head,
                    # so do not call an unknown bundle an absent backup.
                    backing_probe_failed = True
                    continue
                if row["sha"] not in bundle["heads"]:
                    continue
                prereq_results = [probes.get(sha) for sha in bundle["prerequisites"]]
                if any(_probe_error(result) for result in prereq_results):
                    backing_probe_failed = True
                    continue
                if any(result is None or not result.contained for result in prereq_results):
                    continue
                location_order = 0 if bundle["_location"] == "archive" else 1
                candidates.append((location_order, bundle["size"], bundle["name"], bundle))
        if candidates:
            bundle = min(candidates, key=lambda item: item[:3])[3]
            row["backing"] = {
                "location": bundle["_location"],
                "name": bundle["name"],
                "size": bundle["size"],
                "mtime_ns": bundle["mtime_ns"],
                "ino": bundle["ino"],
                "full_history": bundle["full_history"],
            }
        if backing_probe_failed:
            row["class"] = "pin_probe_failed"
            row["detail"] = "backing_probe_failed"
        elif row["cited_by"]:
            row["class"] = "pin_cited"
        elif not contained and row["backing"] is None:
            row["class"] = "pin_unbundled"
        elif row["age_source"] == "unknown":
            row["class"] = "pin_age_unknown"
        elif row["age_s"] is None or row["age_s"] < retention_seconds:
            row["class"] = "pin_young"
        elif contained:
            row["class"] = "pin_landed_expired"
        else:
            row["class"] = "pin_bundled_expired"

    bundle_rows: list[dict[str, Any]] = []
    for bundle in archive_internal:
        heads = bundle["heads"]
        unlanded = [sha for sha in heads if not _probe_error(probes.get(sha)) and not bool(probes[sha].contained)]
        errored_head = bundle["_head_error"] or any(_probe_error(probes.get(sha)) for sha in heads)
        age_s = max(0, clock - bundle["mtime_ns"] // 1_000_000_000)
        if errored_head:
            class_name = "bundle_probe_failed"
            detail = bundle["detail"] if not bundle["_listed"] else "head_probe_failed"
        elif unlanded:
            class_name = "bundle_holds_unlanded"
            detail = ",".join(unlanded[:5])
        elif age_s < retention_seconds:
            class_name = "bundle_young"
            detail = None
        else:
            class_name = "bundle_redundant_expired"
            detail = None
        bundle_rows.append(
            {
                "name": bundle["name"],
                "size": bundle["size"],
                "mtime_ns": bundle["mtime_ns"],
                "ino": bundle["ino"],
                "heads": heads,
                "prerequisites": bundle["prerequisites"],
                "full_history": bundle["full_history"],
                "class": class_name,
                "detail": detail,
                "age_s": age_s,
            }
        )

    product_rows: list[dict[str, Any]] = []
    for bundle in product_internal:
        detail = bundle["detail"]
        if bundle["_listed"] and bundle["_head_error"]:
            detail = "head_probe_failed"
        product_rows.append(
            {
                "name": bundle["name"],
                "size": bundle["size"],
                "mtime_ns": bundle["mtime_ns"],
                "ino": bundle["ino"],
                "heads": bundle["heads"],
                "prerequisites": bundle["prerequisites"],
                "full_history": bundle["full_history"],
                "detail": detail,
            }
        )

    pins = [
        {key: value for key, value in row.items() if not key.startswith("_")}
        for row in sorted(aged_pins, key=lambda item: item["ref"])
    ]
    bundle_rows.sort(key=lambda item: item["name"])
    product_rows.sort(key=lambda item: item["name"])
    report["pins"] = pins
    report["bundles"] = bundle_rows
    report["product_bundles"] = product_rows
    skipped.sort(key=lambda item: (item["location"], item["name"], item["reason"]))
    report["counts"] = {**_class_counts(pins, PIN_CLASSES), **_class_counts(bundle_rows, BUNDLE_CLASSES)}
    report["totals"] = {
        "pins": len(pins),
        "bundles": len(bundle_rows),
        "eligible_pins": sum(row["class"] in ELIGIBLE for row in pins),
        "eligible_bundles": sum(row["class"] in ELIGIBLE for row in bundle_rows),
        "sole_copy_pins": sum(row["class"] == "pin_unbundled" for row in pins),
        "cited_pins": sum(row["class"] == "pin_cited" for row in pins),
        "archive_bytes": sum(row["size"] for row in bundle_rows),
        "redundant_bytes": sum(row["size"] for row in bundle_rows if row["class"] == "bundle_redundant_expired"),
        "full_history_unlanded_bytes": sum(
            row["size"] for row in bundle_rows if row["class"] == "bundle_holds_unlanded" and row["full_history"]
        ),
        "product_bundles": len(product_rows),
        "product_bytes": sum(row["size"] for row in product_rows),
    }
    if report["counts"]["pin_probe_failed"] or report["counts"]["bundle_probe_failed"]:
        report["error"] = report["error"] or "probe_failed"
    complete = (
        report["error"] is None
        and not report["counts"]["pin_probe_failed"]
        and not report["counts"]["bundle_probe_failed"]
    )
    if any(row["detail"] is not None for row in product_rows):
        complete = False
        report["error"] = report["error"] or "product_bundle_probe_failed"
    report["complete"] = bool(complete)
    report["digest"] = _digest(report)
    report["elapsed_s"] = round(time.monotonic() - started, 6)
    return report


def build_purge_report(
    repo: str | Path,
    *,
    main_ref: str = "main",
    archive_dir: str | Path | None = None,
    retention_days: int = DEFAULT_RETENTION_DAYS,
    now: int | None = None,
    timeout_s: float = 10.0,
) -> dict[str, Any]:
    """Return a typed incomplete report for unexpected probe/runtime errors."""
    if isinstance(retention_days, bool) or not isinstance(retention_days, int) or retention_days < MIN_RETENTION_DAYS:
        raise ValueError(f"retention_days must be an integer of at least {MIN_RETENTION_DAYS}")
    try:
        return _build_purge_report_impl(
            repo,
            main_ref=main_ref,
            archive_dir=archive_dir,
            retention_days=retention_days,
            now=now,
            timeout_s=timeout_s,
        )
    except Exception as exc:
        try:
            clock = int(time.time() if now is None else now)
        except (TypeError, ValueError, OverflowError):
            clock = int(time.time())
        failed: dict[str, Any] = {
            "schema": REPORT_SCHEMA,
            "rule": RULE,
            "complete": False,
            "error": f"report_failed:{type(exc).__name__}",
            "main_ref": main_ref,
            "main_tip": None,
            "clock": clock,
            "retention_days": retention_days,
            "archive_dir": None,
            "product_bundle_dir": None,
            "digest": "",
            "counts": {**{key: 0 for key in PIN_CLASSES}, **{key: 0 for key in BUNDLE_CLASSES}},
            "totals": {
                "pins": 0,
                "bundles": 0,
                "eligible_pins": 0,
                "eligible_bundles": 0,
                "sole_copy_pins": 0,
                "cited_pins": 0,
                "archive_bytes": 0,
                "redundant_bytes": 0,
                "full_history_unlanded_bytes": 0,
                "product_bundles": 0,
                "product_bytes": 0,
            },
            "pins": [],
            "bundles": [],
            "product_bundles": [],
            "skipped": [],
            "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "elapsed_s": 0.0,
        }
        failed["digest"] = _digest(failed)
        return failed


def _outcome(
    outcome: str,
    reason: str,
    *,
    decision: str | None = None,
    digest: str | None = None,
    main_tip: str | None = None,
    results: list[dict[str, Any]] | None = None,
    purged_bytes: int = 0,
    detail: str | None = None,
) -> dict[str, Any]:
    value = {
        "outcome": outcome,
        "reason": reason,
        "decision": decision,
        "digest": digest,
        "main_tip": main_tip,
        "results": results or [],
        "purged_bytes": purged_bytes,
    }
    if detail is not None:
        value["detail"] = detail
    return value


def _validate_report(report: object) -> tuple[dict[str, Any] | None, str | None]:
    if not isinstance(report, dict):
        return None, "report_invalid"
    try:
        valid = (
            report.get("schema") == REPORT_SCHEMA
            and report.get("rule") == RULE
            and isinstance(report.get("main_tip"), str)
            and _HEX40_RE.fullmatch(report["main_tip"]) is not None
            and isinstance(report.get("clock"), int)
            and not isinstance(report.get("clock"), bool)
            and report["clock"] <= int(time.time()) + 300
            and isinstance(report.get("retention_days"), int)
            and not isinstance(report.get("retention_days"), bool)
            and report["retention_days"] >= MIN_RETENTION_DAYS
            and all(isinstance(report.get(key), list) for key in ("pins", "bundles", "product_bundles", "skipped"))
            and isinstance(report.get("archive_dir"), str)
            and (report.get("product_bundle_dir") is None or isinstance(report.get("product_bundle_dir"), str))
            and all(
                isinstance(row, dict)
                for key in ("pins", "bundles", "product_bundles", "skipped")
                for row in report[key]
            )
            and isinstance(report.get("digest"), str)
            and report["digest"] == _digest(report)
        )
    except (KeyError, TypeError, ValueError, OverflowError):
        valid = False
    if not valid:
        return None, "report_invalid"
    return report, None


def _row_canonical(row: dict[str, Any]) -> str:
    return _canonical(row)


def _pin_is_cited(sha: str, tokens: dict[str, set[str]]) -> bool:
    return bool(_cited_paths(sha, tokens))


def _resolve_main(repo: Path, main_ref: str, timeout_s: float) -> tuple[str | None, str | None]:
    rc, stdout, stderr = _run_git(
        repo, "rev-parse", "--verify", "--quiet", f"{main_ref}^{{commit}}", timeout_s=timeout_s
    )
    if rc != 0 or not _HEX40_RE.fullmatch(stdout.strip().lower()):
        return None, stderr.strip() or ("timeout" if rc is None else f"git_failed:{rc}")
    return stdout.strip().lower(), None


def _resolve_main_ref(repo: Path, main_ref: str, timeout_s: float) -> tuple[str | None, str | None]:
    rc, stdout, stderr = _run_git(
        repo,
        "rev-parse",
        "--symbolic-full-name",
        "--verify",
        "--quiet",
        main_ref,
        timeout_s=timeout_s,
    )
    resolved_ref = stdout.strip()
    if rc != 0 or not resolved_ref.startswith("refs/"):
        return None, stderr.strip() or ("timeout" if rc is None else f"git_failed:{rc}")
    return resolved_ref, None


def _purge_citations(
    repo: Path, main_ref: str, timeout_s: float
) -> tuple[dict[str, set[str]], str | None, str | None, str | None]:
    resolved_ref, error = _resolve_main_ref(repo, main_ref, timeout_s)
    if error:
        return {}, None, None, error
    tip, error = _resolve_main(repo, resolved_ref or "", timeout_s)
    if error:
        return {}, resolved_ref, None, error
    tokens, scan_error = _citation_scan(repo, tip or "")
    return tokens, resolved_ref, tip, scan_error


def _disk_preflight(
    primary: Path,
    archive_path: Path,
    size: int,
    *,
    delta: bool,
    timeout_s: float,
    pack_cache: dict[str, int | None],
) -> tuple[str | None, str | None]:
    from .offload_preflight import OffloadPreflightError, resolve_disk_floor_bytes  # noqa: PLC0415

    try:
        free = shutil.disk_usage(archive_path.parent).free
        floor = resolve_disk_floor_bytes(primary)
    except OffloadPreflightError:
        return "verify_skipped_disk_floor", "floor_unresolvable"
    except Exception:
        return "verify_skipped_disk_floor", "floor_unresolvable"
    need = 2 * size
    if delta:
        if "pack" not in pack_cache:
            rc, stdout, _stderr = _run_git(primary, "count-objects", "-v", timeout_s=timeout_s)
            match = re.search(r"^size-pack:\s*(\d+)\s*$", stdout, re.MULTILINE) if rc == 0 else None
            pack_cache["pack"] = int(match.group(1)) * 1024 if match else None
        pack_size = pack_cache["pack"]
        if pack_size is None:
            return "verify_skipped_disk_floor", "pack_size_unknown"
        need += pack_size
    if free - floor < need:
        return "verify_skipped_disk_floor", f"free={free};floor={floor};need={need}"
    return None, None


def _result(kind: str, item_id: str, class_name: str, result: str, detail: str | None = None) -> dict[str, Any]:
    return {"kind": kind, "id": item_id, "class": class_name, "result": result, "detail": detail}


def _planned_item_present(
    root: Path,
    archive_dir: Path,
    kind: str,
    row: dict[str, Any],
    timeout_s: float,
) -> tuple[bool, str | None]:
    if kind == "pin":
        ref = row.get("ref")
        if not isinstance(ref, str):
            return False, "invalid_pin_ref"
        rc, _stdout, stderr = _run_git(root, "show-ref", "--verify", "--quiet", ref, timeout_s=timeout_s)
        if rc == 0:
            return True, None
        if rc == 1:
            return False, None
        return False, stderr.strip() or ("timeout" if rc is None else f"git_failed:{rc}")

    name = row.get("name")
    if not _safe_basename(name):
        return False, "invalid_bundle_name"
    try:
        os.lstat(archive_dir / name)
    except FileNotFoundError:
        return False, None
    except OSError as exc:
        return False, f"{type(exc).__name__}:{exc}"
    return True, None


def _write_purge_intent(
    record_decision: Callable[..., Any] | None,
    *,
    session: str | None,
    decision_id: str,
    rationale: str,
    main_tip: str,
    digest: str,
    task_ref: str,
    results: list[dict[str, Any]],
) -> dict[str, Any] | None:
    record_decision_fn = record_decision
    if record_decision_fn is None:
        try:
            from workbay_handoff_mcp import record_decision as record_decision_fn  # noqa: PLC0415
        except Exception as exc:
            return _outcome(
                "write_failed",
                "decision_import_failed",
                decision=decision_id,
                digest=digest,
                main_tip=main_tip,
                results=results,
                detail=type(exc).__name__,
            )
    try:
        recorded = record_decision_fn(
            session=session or f"pin-reclaim-{main_tip[:12]}",
            decision=decision_id,
            rationale=rationale,
            actor={"agent": "pin-reclaim", "commit_sha": main_tip},
            task_ref=task_ref,
        )
    except Exception as exc:
        return _outcome(
            "write_failed",
            "decision_write_failed",
            decision=decision_id,
            digest=digest,
            main_tip=main_tip,
            results=results,
            detail=type(exc).__name__,
        )
    mutation = recorded.get("mutation") if isinstance(recorded, dict) else None
    operation = mutation.get("operation") if isinstance(mutation, dict) else None
    if isinstance(recorded, dict) and recorded.get("ok") is True and operation == "noop":
        return _outcome(
            "partial",
            "decision_intent_exists",
            decision=decision_id,
            digest=digest,
            main_tip=main_tip,
            results=results,
        )
    if not isinstance(recorded, dict) or recorded.get("ok") is not True or operation != "insert":
        return _outcome(
            "write_failed",
            "decision_rejected",
            decision=decision_id,
            digest=digest,
            main_tip=main_tip,
            results=results,
        )
    return None


def _safe_basename(name: object) -> bool:
    return isinstance(name, str) and bool(name) and Path(name).name == name and name not in {".", ".."}


def _delete_pin_if_main_unchanged(
    repo: Path,
    row: dict[str, Any],
    scanned_main_ref: str,
    scanned_main_oid: str,
    timeout_s: float,
) -> dict[str, Any]:
    transaction = (
        f"start\nverify {scanned_main_ref} {scanned_main_oid}\ndelete {row['ref']} {row['sha']}\nprepare\ncommit\n"
    )
    rc, _stdout, stderr = _run_git(repo, "update-ref", "--stdin", timeout_s=timeout_s, input_text=transaction)
    if rc == 0:
        return _result("pin", row["ref"], row["class"], "purged")
    current_main_oid, resolve_error = _resolve_main(repo, scanned_main_ref, timeout_s)
    if resolve_error or current_main_oid != scanned_main_oid:
        return _result(
            "pin",
            row["ref"],
            row["class"],
            "main_moved_during_purge",
            stderr.strip() or resolve_error or "main_ref_changed",
        )
    return _result(
        "pin",
        row["ref"],
        row["class"],
        "cas_failed",
        stderr.strip() or ("timeout" if rc is None else f"git_failed:{rc}"),
    )


def _act_pin_landed(
    root: Path,
    row: dict[str, Any],
    scanned_main_ref: str,
    scanned_main_oid: str,
    timeout_s: float,
) -> dict[str, Any]:
    return _delete_pin_if_main_unchanged(root, row, scanned_main_ref, scanned_main_oid, timeout_s)


def _act_pin_bundled(
    primary: Path,
    product_dir: Path | None,
    archive_dir: Path,
    row: dict[str, Any],
    scanned_main_ref: str,
    scanned_main_oid: str,
    timeout_s: float,
    pack_cache: dict[str, int | None],
) -> dict[str, Any]:
    backing = row.get("backing")
    if not isinstance(backing, dict) or backing.get("location") not in {"archive", "product"}:
        return _result("pin", row["ref"], row["class"], "backing_missing", "missing_backing")
    name = backing.get("name")
    if not _safe_basename(name):
        return _result("pin", row["ref"], row["class"], "backing_missing", "invalid_backing_name")
    archive_path = archive_dir / name
    if backing["location"] == "product":
        if product_dir is None:
            return _result("pin", row["ref"], row["class"], "backing_missing", "product_dir_unavailable")
        product_path = product_dir / name
        try:
            archive_dir.mkdir(parents=True, exist_ok=True)
            try:
                os.link(product_path, archive_path)
            except FileExistsError:
                existing = os.lstat(archive_path)
                if existing.st_ino != backing.get("ino"):
                    return _result("pin", row["ref"], row["class"], "backing_link_conflict", "archive_name_exists")
            linked = os.lstat(archive_path)
            if linked.st_ino != backing.get("ino") or linked.st_size != backing.get("size"):
                return _result("pin", row["ref"], row["class"], "backing_link_conflict", "linked_inode_or_size_changed")
        except FileNotFoundError:
            return _result("pin", row["ref"], row["class"], "backing_missing", "product_bundle_missing")
        except OSError as exc:
            return _result(
                "pin",
                row["ref"],
                row["class"],
                "backing_link_failed",
                errno.errorcode.get(exc.errno, "OSError"),
            )
    else:
        try:
            current = os.lstat(archive_path)
        except OSError:
            return _result("pin", row["ref"], row["class"], "backing_missing", "archive_bundle_missing")
        if (current.st_ino, current.st_size, current.st_mtime_ns) != (
            backing.get("ino"),
            backing.get("size"),
            backing.get("mtime_ns"),
        ):
            return _result("pin", row["ref"], row["class"], "bundle_changed", "archive_backing_changed")
    disk_result, disk_detail = _disk_preflight(
        primary,
        archive_path,
        int(backing.get("size", 0)),
        delta=not bool(backing.get("full_history")),
        timeout_s=timeout_s,
        pack_cache=pack_cache,
    )
    if disk_result:
        return _result("pin", row["ref"], row["class"], disk_result, disk_detail)
    errors: list[str] = []
    try:
        verified = _verified_bundle_contains(primary, archive_path, row["sha"], errors=errors)
    except Exception as exc:  # fail closed on a verifier implementation error
        return _result("pin", row["ref"], row["class"], "bundle_verify_failed", f"{type(exc).__name__}:{exc}")
    if not verified:
        return _result(
            "pin", row["ref"], row["class"], "bundle_verify_failed", ",".join(errors) or "verification_failed"
        )
    return _delete_pin_if_main_unchanged(primary, row, scanned_main_ref, scanned_main_oid, timeout_s)


def _restore_quarantined_bundle(quarantine: Path, path: Path) -> str | None:
    try:
        os.link(quarantine, path)
        os.unlink(quarantine)
    except OSError as exc:
        return f"{type(exc).__name__}:{exc}"
    return None


def _act_bundle(
    root: Path,
    archive_dir: Path,
    row: dict[str, Any],
    digest: str,
    scanned_main_ref: str,
    scanned_main_oid: str,
    timeout_s: float,
) -> tuple[dict[str, Any], int]:
    name = row.get("name")
    if not _safe_basename(name):
        return _result("bundle", str(name), row["class"], "bundle_changed", "invalid_bundle_name"), 0
    path = archive_dir / name
    quarantine = archive_dir / f"{name}.purge-{digest[:12]}"
    if os.path.lexists(quarantine):
        return _result("bundle", name, row["class"], "bundle_changed", "quarantine_exists"), 0
    try:
        os.rename(path, quarantine)
    except FileNotFoundError:
        return _result("bundle", name, row["class"], "bundle_changed", "missing"), 0
    except OSError as exc:
        return _result("bundle", name, row["class"], "bundle_changed", f"rename_failed:{type(exc).__name__}"), 0
    try:
        actual = os.lstat(quarantine)
        same = (actual.st_ino, actual.st_size, actual.st_mtime_ns) == (
            row.get("ino"),
            row.get("size"),
            row.get("mtime_ns"),
        )
    except OSError:
        same = False
    if same:
        current_main_oid, resolve_error = _resolve_main(root, scanned_main_ref, timeout_s)
        if resolve_error or current_main_oid != scanned_main_oid:
            restore_error = _restore_quarantined_bundle(quarantine, path)
            detail = resolve_error or "main_ref_changed"
            if restore_error:
                detail = f"{detail};restore_failed:{restore_error};quarantine={quarantine}"
            return _result("bundle", name, row["class"], "main_moved_during_purge", detail), 0
        try:
            os.unlink(quarantine)
            return _result("bundle", name, row["class"], "purged"), int(row["size"])
        except OSError:
            return _result("bundle", name, row["class"], "quarantined", str(quarantine)), 0
    restore_error = _restore_quarantined_bundle(quarantine, path)
    if restore_error:
        return _result("bundle", name, row["class"], "quarantined", f"{quarantine}:{restore_error}"), 0
    return _result("bundle", name, row["class"], "bundle_changed", "inode_or_metadata_changed"), 0


def _purge_impl(
    repo: str | Path,
    report: object,
    *,
    task_ref: str,
    main_ref: str = "main",
    report_path: str | Path | None = None,
    session: str | None = None,
    record_decision: Callable[..., Any] | None = None,
    timeout_s: float = 10.0,
) -> dict[str, Any]:
    """Replay a complete report, persist purge intent, then reconcile its actions."""
    raw_digest = report.get("digest") if isinstance(report, dict) else None
    raw_tip = report.get("main_tip") if isinstance(report, dict) else None
    validated, validation_error = _validate_report(report)
    if validation_error or validated is None:
        return _outcome(
            "refused",
            "report_invalid",
            digest=raw_digest if isinstance(raw_digest, str) else None,
            main_tip=raw_tip if isinstance(raw_tip, str) else None,
        )
    digest = validated["digest"]
    main_tip = validated["main_tip"]
    if validated.get("complete") is not True:
        return _outcome("refused", "report_incomplete", digest=digest, main_tip=main_tip)
    pins = [
        row for row in validated["pins"] if row.get("class") in ELIGIBLE and row.get("class", "").startswith("pin_")
    ]
    bundles = [row for row in validated["bundles"] if row.get("class") == "bundle_redundant_expired"]
    pins.sort(key=lambda row: row.get("ref", ""))
    bundles.sort(key=lambda row: row.get("name", ""))
    plan: list[tuple[str, dict[str, Any]]] = [("pin", row) for row in pins] + [("bundle", row) for row in bundles]
    if not plan:
        return _outcome("nothing_to_purge", "nothing_to_purge", digest=digest, main_tip=main_tip)
    decision_id = f"pin_reclaim_purge_v1_{main_tip[:12]}_{digest[:12]}"
    root = Path(repo).expanduser().resolve()
    archive_dir = Path(validated["archive_dir"])

    try:
        from workbay_handoff_mcp.runtime import RuntimeNotConfiguredError  # noqa: PLC0415

        from .lane_reclaim import _scan_read_connection  # noqa: PLC0415

        with _scan_read_connection() as conn:
            existing = conn.execute(
                "SELECT 1 FROM decisions WHERE task_ref = ? AND decision = ? LIMIT 1",
                (task_ref, decision_id),
            ).fetchone()
        decision_exists = existing is not None
    except (RuntimeNotConfiguredError, sqlite3.Error) as exc:
        return _outcome(
            "refused",
            "probe_failed",
            decision=decision_id,
            digest=digest,
            main_tip=main_tip,
            detail=f"decision_lookup:{type(exc).__name__}",
        )
    except Exception as exc:
        return _outcome(
            "refused",
            "probe_failed",
            decision=decision_id,
            digest=digest,
            main_tip=main_tip,
            detail=f"decision_lookup:{type(exc).__name__}",
        )

    results: list[dict[str, Any]] = []
    already_gone: set[tuple[str, str]] = set()
    candidates = plan
    if decision_exists:
        candidates = []
        for kind, row in plan:
            present, presence_error = _planned_item_present(root, archive_dir, kind, row, timeout_s)
            item_id = row["ref"] if kind == "pin" else row["name"]
            if presence_error:
                return _outcome(
                    "refused",
                    "probe_failed",
                    decision=decision_id,
                    digest=digest,
                    main_tip=main_tip,
                    results=results,
                    detail=f"planned_item_probe:{kind}:{item_id}:{presence_error}",
                )
            if present:
                candidates.append((kind, row))
            else:
                already_gone.add((kind, item_id))
                results.append(_result(kind, item_id, row["class"], "already_purged"))
        if not candidates:
            return _outcome(
                "already_applied",
                "decision_complete",
                decision=decision_id,
                digest=digest,
                main_tip=main_tip,
                results=results,
            )

    tokens, scanned_main_ref, scanned_main_oid, citation_error = _purge_citations(root, main_ref, timeout_s)
    if citation_error or scanned_main_ref is None or scanned_main_oid is None:
        return _outcome(
            "refused",
            "probe_failed",
            decision=decision_id,
            digest=digest,
            main_tip=main_tip,
            detail="citation_scan_failed",
        )

    rc, _stdout, _stderr = _run_git(
        root, "merge-base", "--is-ancestor", main_tip, scanned_main_oid, timeout_s=timeout_s
    )
    if rc == 1:
        return _outcome("refused", "pinned_tip_not_on_main", decision=decision_id, digest=digest, main_tip=main_tip)
    if rc != 0:
        return _outcome(
            "refused",
            "probe_failed",
            decision=decision_id,
            digest=digest,
            main_tip=main_tip,
            detail=f"main_ancestor:{'timeout' if rc is None else rc}",
        )

    replay = build_purge_report(
        root,
        main_ref=main_tip,
        archive_dir=validated["archive_dir"],
        retention_days=validated["retention_days"],
        now=validated["clock"],
        timeout_s=timeout_s,
    )
    if not replay["complete"]:
        return _outcome(
            "refused",
            "replay_incomplete",
            decision=decision_id,
            digest=digest,
            main_tip=main_tip,
            detail=str(replay.get("error")),
        )
    if replay.get("product_bundle_dir") != validated.get("product_bundle_dir"):
        return _outcome(
            "refused",
            "report_drift",
            decision=decision_id,
            digest=digest,
            main_tip=main_tip,
            detail="product_bundle_dir_changed",
        )
    replay_pins = {row["ref"]: row for row in replay["pins"]}
    replay_bundles = {row["name"]: row for row in replay["bundles"]}
    remaining: list[tuple[str, dict[str, Any]]] = []
    drifted = 0
    for kind, row in candidates:
        key = row.get("ref") if kind == "pin" else row.get("name")
        if decision_exists:
            present, presence_error = _planned_item_present(root, archive_dir, kind, row, timeout_s)
            if presence_error:
                return _outcome(
                    "refused",
                    "probe_failed",
                    decision=decision_id,
                    digest=digest,
                    main_tip=main_tip,
                    results=results,
                    detail=f"planned_item_probe:{kind}:{key}:{presence_error}",
                )
            if not present:
                already_gone.add((kind, str(key)))
                results.append(_result(kind, str(key), str(row.get("class")), "already_purged"))
                continue
        fresh = replay_pins.get(key) if kind == "pin" else replay_bundles.get(key)
        if fresh is None or _row_canonical(row) != _row_canonical(fresh):
            item_id = str(key or "")
            results.append(_result(kind, item_id, str(row.get("class")), "drifted", "replay_row_changed"))
            drifted += 1
        else:
            remaining.append((kind, row))

    newly_cited: set[str] = set()
    still_planned: list[tuple[str, dict[str, Any]]] = []
    for kind, row in remaining:
        if kind == "pin" and _pin_is_cited(row["sha"], tokens):
            results.append(
                _result("pin", row["ref"], row["class"], "newly_cited", ",".join(_cited_paths(row["sha"], tokens)))
            )
            newly_cited.add(row["ref"])
        else:
            still_planned.append((kind, row))
    remaining = still_planned
    if not remaining:
        if decision_exists:
            if len(already_gone) == len(plan):
                return _outcome(
                    "already_applied",
                    "decision_complete",
                    decision=decision_id,
                    digest=digest,
                    main_tip=main_tip,
                    results=results,
                )
            return _outcome(
                "partial",
                "purge_partial",
                decision=decision_id,
                digest=digest,
                main_tip=main_tip,
                results=results,
            )
        return _outcome(
            "refused", "all_rows_drifted", decision=decision_id, digest=digest, main_tip=main_tip, results=results
        )

    primary, _common, path_error = _primary_and_common(root, timeout_s)
    if path_error or primary is None:
        return _outcome(
            "refused",
            "probe_failed",
            decision=decision_id,
            digest=digest,
            main_tip=main_tip,
            results=results,
            detail="primary_worktree_unresolvable",
        )
    product_dir = (
        Path(validated["product_bundle_dir"]) if isinstance(validated.get("product_bundle_dir"), str) else None
    )

    planned_counts = Counter(row["class"] for _kind, row in plan)
    rationale_data: dict[str, Any] = {
        "planned_counts": dict(sorted(planned_counts.items())),
        "drifted": drifted,
        "newly_cited": len(newly_cited),
        "digest": digest,
        "pinned_main_tip": main_tip,
        "clock": validated["clock"],
        "retention_days": validated["retention_days"],
        "main_ref": main_ref,
    }
    if report_path is not None:
        rationale_data["report_path"] = str(report_path)
    rationale = "pin reclaim purge " + _canonical(rationale_data) + "; write-ahead: recorded before any deletion"
    rationale = rationale[:1500]
    if not decision_exists:
        decision_error = _write_purge_intent(
            record_decision,
            session=session,
            decision_id=decision_id,
            rationale=rationale,
            main_tip=main_tip,
            digest=digest,
            task_ref=task_ref,
            results=results,
        )
        if decision_error is not None:
            return decision_error

    pack_cache: dict[str, int | None] = {}
    purged_bytes = 0
    main_moved = False
    for kind, row in remaining:
        item_id = row.get("ref", "") if kind == "pin" else row.get("name", "")
        if main_moved:
            results.append(
                _result(kind, item_id, row.get("class", ""), "main_moved_during_purge", "skipped_after_main_moved")
            )
            continue
        try:
            if kind == "pin":
                if row["class"] == "pin_landed_expired":
                    result = _act_pin_landed(root, row, scanned_main_ref, scanned_main_oid, timeout_s)
                elif row["class"] == "pin_bundled_expired":
                    result = _act_pin_bundled(
                        primary,
                        product_dir,
                        archive_dir,
                        row,
                        scanned_main_ref,
                        scanned_main_oid,
                        timeout_s,
                        pack_cache,
                    )
                else:
                    result = _result("pin", row.get("ref", ""), row.get("class", ""), "ineligible")
                results.append(result)
                main_moved = result.get("result") == "main_moved_during_purge"
            else:
                result, removed_size = _act_bundle(
                    root,
                    archive_dir,
                    row,
                    digest,
                    scanned_main_ref,
                    scanned_main_oid,
                    timeout_s,
                )
                results.append(result)
                purged_bytes += removed_size
                main_moved = result.get("result") == "main_moved_during_purge"
        except Exception as exc:
            results.append(_result(kind, item_id, row.get("class", ""), "act_failed", type(exc).__name__))
    original_rows = len(plan)
    purged = sum(result.get("result") == "purged" for result in results)
    fully_removed = sum(result.get("result") in {"purged", "already_purged"} for result in results) == original_rows
    outcome = (
        "already_applied"
        if decision_exists and fully_removed and purged == 0
        else "applied"
        if fully_removed
        else "partial"
    )
    return _outcome(
        outcome,
        "main_moved_during_purge"
        if main_moved
        else "decision_complete"
        if outcome == "already_applied"
        else "purge_complete"
        if outcome == "applied"
        else "purge_partial",
        decision=decision_id,
        digest=digest,
        main_tip=main_tip,
        results=results,
        purged_bytes=purged_bytes,
    )


def purge(
    repo: str | Path,
    report: object,
    *,
    task_ref: str,
    main_ref: str = "main",
    report_path: str | Path | None = None,
    session: str | None = None,
    record_decision: Callable[..., Any] | None = None,
    timeout_s: float = 10.0,
) -> dict[str, Any]:
    """Fail closed as a structured result for every operational failure."""
    try:
        return _purge_impl(
            repo,
            report,
            task_ref=task_ref,
            main_ref=main_ref,
            report_path=report_path,
            session=session,
            record_decision=record_decision,
            timeout_s=timeout_s,
        )
    except Exception as exc:  # purge is a CLI-safe procedure and never leaks operational exceptions
        return _outcome("refused", "probe_failed", detail=f"purge_exception:{type(exc).__name__}")


def _write_json_atomic(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=False, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(value, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except Exception:
        try:
            os.unlink(temporary)
        except OSError:
            pass
        raise


def _print_summary(prefix: str, payload: dict[str, Any]) -> None:
    if "counts" in payload:
        print(
            f"{prefix} complete={payload.get('complete')} counts={_canonical(payload.get('counts'))} totals={_canonical(payload.get('totals'))}",
            file=sys.stderr,
        )
        if payload.get("error"):
            print(f"error: {payload['error']}", file=sys.stderr)
    else:
        print(
            f"{prefix} outcome={payload.get('outcome')} reason={payload.get('reason')} purged_bytes={payload.get('purged_bytes', 0)}",
            file=sys.stderr,
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m workbay_orchestrator_mcp.orchestration.pin_reclaim")
    subparsers = parser.add_subparsers(dest="command", required=True)
    report_parser = subparsers.add_parser("report")
    report_parser.add_argument("--repo", default=".")
    report_parser.add_argument("--main-ref", default="main")
    report_parser.add_argument("--archive-dir")
    report_parser.add_argument("--retention-days", type=int, default=DEFAULT_RETENTION_DAYS)
    report_parser.add_argument("--out")
    report_parser.add_argument("--timeout-s", type=float, default=10.0)
    purge_parser = subparsers.add_parser("purge")
    purge_parser.add_argument("--repo", default=".")
    purge_parser.add_argument("--main-ref", default="main")
    purge_parser.add_argument("--report", required=True)
    purge_parser.add_argument("--task-ref", required=True)
    purge_parser.add_argument("--session")
    purge_parser.add_argument("--timeout-s", type=float, default=10.0)
    try:
        args = parser.parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    repo = Path(args.repo).expanduser().resolve()
    if args.command == "report":
        try:
            report = build_purge_report(
                repo,
                main_ref=args.main_ref,
                archive_dir=args.archive_dir,
                retention_days=args.retention_days,
                timeout_s=args.timeout_s,
            )
        except ValueError as exc:
            print(str(exc), file=sys.stderr)
            return 2
        if args.out:
            try:
                _write_json_atomic(Path(args.out).expanduser().resolve(), report)
            except OSError as exc:
                print(f"error: report_write_failed:{type(exc).__name__}", file=sys.stderr)
                return 4
        print(json.dumps(report, indent=2, sort_keys=True))
        _print_summary("report", report)
        return 0 if report["complete"] else 4

    try:
        report_value = json.loads(Path(args.report).expanduser().read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        payload = _outcome("refused", "report_invalid", detail=f"report_read:{type(exc).__name__}")
        print(json.dumps(payload, indent=2, sort_keys=True))
        _print_summary("purge", payload)
        return 3
    try:
        from workbay_handoff_mcp.runtime import (  # noqa: PLC0415
            RuntimeNotConfiguredError,
            configure_runtime,
            get_runtime_config,
        )

        try:
            get_runtime_config()
        except RuntimeNotConfiguredError:
            from workbay_handoff_mcp.config import RuntimeConfig  # noqa: PLC0415

            configure_runtime(RuntimeConfig.for_repo(repo))
    except Exception as exc:
        payload = _outcome("refused", "probe_failed", detail=f"runtime_config:{type(exc).__name__}")
        print(json.dumps(payload, indent=2, sort_keys=True))
        _print_summary("purge", payload)
        return 3
    result = purge(
        repo,
        report_value,
        task_ref=args.task_ref,
        main_ref=args.main_ref,
        report_path=args.report,
        session=args.session,
        timeout_s=args.timeout_s,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    _print_summary("purge", result)
    outcome = result.get("outcome")
    if outcome in {"applied", "already_applied", "nothing_to_purge"}:
        return 0
    if outcome == "write_failed":
        return 5
    if outcome == "partial":
        return 6
    return 3


if __name__ == "__main__":
    raise SystemExit(main())
