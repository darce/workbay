"""Bounded selection from held WorkBay semantic exports; never a test verdict.

See docs/contracts/regression-selection-v1.md for the catalog and CLI contracts.
Only local JSON, Git identity, and filesystem metadata are read. No retrieval,
embedding provider, database, pytest execution, or packet text execution occurs.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import subprocess
import tempfile
from pathlib import Path
from typing import Any

SCHEMA = "workbay/regression-selection@1"
CATALOG_SCHEMA = "workbay/regression-catalog@1"
MAX_TESTS = 10_000
MAX_EXPORT_BYTES = 16 * 1024 * 1024
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_SOURCE = re.compile(r"[0-9a-f]{40}\Z")
_PATH_PART = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_.-]*\Z")
_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_CASE = re.compile(r"[A-Za-z0-9_.,=+@/-]+\Z")
_INVALID = {
    "invalid_arguments",
    "invalid_max_tests",
    "invalid_source",
    "source_mismatch",
    "invalid_expected_digest",
    "catalog_digest_mismatch",
    "semantic_packet_digest_mismatch",
    "invalid_json",
    "invalid_catalog",
    "invalid_packet",
    "invalid_score",
    "output_aliases_input",
}


class _Refusal(ValueError):
    pass


def _report(source: str | None) -> dict[str, Any]:
    return dict(
        schema=SCHEMA,
        status="unavailable",
        source=source,
        catalog_sha256=None,
        semantic_packet_sha256=None,
        query_model_id=None,
        node_ids=[],
        coverage=None,
        limitations=["pytest_collection_required"],
    )


def _text(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _object(value: Any, reason: str) -> dict:
    if not isinstance(value, dict):
        raise _Refusal(reason)
    return value


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise _Refusal("invalid_json")
        result[key] = value
    return result


def _load(raw: bytes) -> dict:
    def nonfinite(_):
        raise _Refusal("invalid_score")

    try:
        return _object(json.loads(raw, object_pairs_hook=_unique_object, parse_constant=nonfinite), "invalid_json")
    except (ValueError, UnicodeError, RecursionError) as exc:
        if isinstance(exc, _Refusal):
            raise
        raise _Refusal("invalid_json") from exc


def _read(path: str | Path) -> bytes:
    # Bound both memory and parsing work even for an accidentally enormous export.
    with Path(path).open("rb") as handle:
        raw = handle.read(MAX_EXPORT_BYTES + 1)
    if len(raw) > MAX_EXPORT_BYTES:
        raise _Refusal("export_too_large")
    return raw


def _check_source(repo: Path, source: str) -> None:
    if not isinstance(source, str) or not _SOURCE.fullmatch(source):
        raise _Refusal("invalid_source")
    for args, expected in ((["--show-toplevel"], str(repo)), (["--verify", "HEAD"], source)):
        proc = subprocess.run(
            ["git", "-C", str(repo), "rev-parse", *args], capture_output=True, text=True, timeout=5, check=False
        )
        if proc.returncode or proc.stdout.strip() != expected:
            raise _Refusal("source_mismatch")


def _valid_node(repo: Path, node: Any) -> bool:
    """Conservative syntax and containment only: pytest must still collect it."""
    if not isinstance(node, str) or not node.isascii() or any(c.isspace() for c in node):
        return False
    parts = node.split("::")
    if len(parts) < 2:
        return False
    path_parts = parts[0].split("/")
    if not all(_PATH_PART.fullmatch(p) and p not in (".", "..") for p in path_parts):
        return False
    filename = path_parts[-1]
    if not (filename.endswith(".py") and (filename.startswith("test_") or filename.endswith("_test.py"))):
        return False
    final = parts[-1]
    if "[" in final:
        name, case = final.split("[", 1)
        if not case.endswith("]") or not _CASE.fullmatch(case[:-1]) or ".." in case:
            return False
    else:
        name = final
    if not name.startswith("test_") or not _NAME.fullmatch(name):
        return False
    if not all(_NAME.fullmatch(p) for p in parts[1:-1]):
        return False
    try:
        path = (repo / parts[0]).resolve(strict=True)
        return path.is_relative_to(repo) and path.is_file()
    except (OSError, RuntimeError, ValueError):
        return False


def _catalog(doc: dict, source: str) -> dict[tuple[str, str], list[str]]:
    if doc.get("schema") != CATALOG_SCHEMA or not _text(doc.get("model_id")):
        raise _Refusal("invalid_catalog")
    if doc.get("source") != source:
        raise _Refusal("source_mismatch")
    entries = doc.get("entries")
    if not isinstance(entries, list):
        raise _Refusal("invalid_catalog")
    mapping: dict[tuple[str, str], list[str]] = {}
    for value in entries:
        row = _object(value, "invalid_catalog")
        if row.get("source") != source:
            raise _Refusal("source_mismatch")
        if not all(_text(row.get(k)) for k in ("entity_kind", "entity_id", "provenance")):
            raise _Refusal("invalid_catalog")
        if not isinstance(row.get("node_ids"), list):
            raise _Refusal("invalid_catalog")
        key = row["entity_kind"], row["entity_id"]
        mapping.setdefault(key, []).extend(row["node_ids"])
    return mapping


def _coverage(packet: dict, report: dict) -> None:
    coverage = packet.get("coverage")
    if coverage is not None and not isinstance(coverage, dict):
        raise _Refusal("invalid_packet")
    report["coverage"] = coverage
    reason = packet.get("semantic_degrade_reason")
    if reason is not None:
        if not _text(reason):
            raise _Refusal("invalid_packet")
        report["limitations"].append(reason)
    if coverage is None or coverage.get("status") in (None, "unknown"):
        report["limitations"].append("coverage_unknown")
    elif coverage.get("status") not in ("complete", "ready"):
        report["limitations"].append("coverage_partial")
    if coverage and coverage.get("pending", 0) != 0:
        report["limitations"].append("embedding_coverage_pending")


def select_regressions(
    *,
    repo: str | Path,
    source: str,
    catalog: str | Path,
    semantic_packet: str | Path,
    max_tests: int,
    expected_catalog_sha256: str,
    expected_semantic_packet_sha256: str,
) -> dict[str, Any]:
    """Read held exports and return exactly the frozen @1 selection fields.

    Expected hashes are mandatory, trusted caller inputs over exact file bytes.
    Expected input/filesystem failures yield ``unavailable`` and an empty list.
    Similarity ranks mapped membership only; it cannot establish any test result.
    """
    report = _report(source)
    try:
        if type(max_tests) is not int or not 1 <= max_tests <= MAX_TESTS:
            raise _Refusal("invalid_max_tests")
        root = Path(repo).resolve(strict=True)
        _check_source(root, source)
        for expected in (expected_catalog_sha256, expected_semantic_packet_sha256):
            if not isinstance(expected, str) or not _SHA256.fullmatch(expected):
                raise _Refusal("invalid_expected_digest")
        raw_catalog, raw_packet = _read(catalog), _read(semantic_packet)
        for export_name, raw, expected in (
            ("catalog", raw_catalog, expected_catalog_sha256),
            ("semantic_packet", raw_packet, expected_semantic_packet_sha256),
        ):
            digest = hashlib.sha256(raw).hexdigest()
            report[export_name + "_sha256"] = digest
            if digest != expected:
                raise _Refusal(export_name + "_digest_mismatch")
        # No JSON-derived selection is considered before both holds and source.
        catalog_doc = _load(raw_catalog)
        mapping = _catalog(catalog_doc, source)
        packet = _load(raw_packet)
        if "structuredContent" in packet:
            packet = _object(packet["structuredContent"], "invalid_packet")
        model = packet.get("query_model_id")
        report["query_model_id"] = model if _text(model) else None
        _coverage(packet, report)
        status = packet.get("status")
        if status not in ("ok_with_results", "no_results_above_threshold", "store_empty"):
            report["limitations"].append(
                status
                if status in ("provider_unavailable", "disabled", "model_mismatch", "unavailable", "skipped")
                else "semantic_status_unknown"
            )
            return report
        if packet.get("ok") is False or packet.get("embeddings_mode") == "disabled":
            raise _Refusal("disabled" if packet.get("embeddings_mode") == "disabled" else "semantic_unavailable")
        rows = packet.get("results")
        if not isinstance(rows, list) or (status != "ok_with_results" and rows):
            raise _Refusal("invalid_packet")
        if not _text(model):
            # Native store-empty packets may have no model/provider at all.
            if status == "store_empty":
                report["status"] = "no_matches"
                return report
            raise _Refusal("query_model_unavailable")
        coverage_model = (report["coverage"] or {}).get("model_id")
        if model != catalog_doc["model_id"] or coverage_model not in (None, model):
            raise _Refusal("model_mismatch")
        entity_scores: dict[tuple[str, str], float] = {}
        for value in rows:
            row = _object(value, "invalid_packet")
            if row.get("model_id") != model:
                raise _Refusal("model_mismatch")
            score = row.get("score")
            if (
                isinstance(score, bool)
                or not isinstance(score, (int, float))
                or not -1 <= score <= 1
                or not math.isfinite(score)
            ):
                raise _Refusal("invalid_score")
            if not all(_text(row.get(k)) for k in ("entity_kind", "entity_id")):
                raise _Refusal("invalid_packet")
            key = row["entity_kind"], row["entity_id"]
            entity_scores[key] = max(score, entity_scores.get(key, -math.inf))
        # Collapse repeated hits before expanding catalog nodes: duplicates must
        # not multiply filesystem work by the number of hits times catalog size.
        scores: dict[str, float] = {}
        for key, score in entity_scores.items():
            if key not in mapping:
                report["limitations"].append("unmapped_entities")
                continue
            for node in mapping[key]:
                if not _valid_node(root, node):
                    report["limitations"].append("excluded_catalog_nodes")
                    continue
                scores[node] = max(score, scores.get(node, -math.inf))
        report["node_ids"] = sorted(scores, key=lambda node: (-scores[node], node))[:max_tests]
        report["status"] = "selected" if report["node_ids"] else "no_matches"
    except _Refusal as exc:
        report["limitations"].append(str(exc))
        report["status"], report["node_ids"] = "unavailable", []
    except (OSError, RuntimeError, subprocess.SubprocessError):
        report["limitations"].append("input_unavailable")
        report["status"], report["node_ids"] = "unavailable", []
    finally:
        report["limitations"] = sorted(set(report["limitations"]))
    return report


def write_report(report: dict, destination: str | Path) -> None:
    """Atomic JSON write; cleanup must preserve the primary filesystem error."""
    path, temporary = Path(destination), None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=path.parent, prefix=f".{path.name}.", delete=False
        ) as handle:
            temporary = Path(handle.name)
            json.dump(report, handle, sort_keys=True, indent=2, allow_nan=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except BaseException as exc:
        if temporary is not None:
            try:
                temporary.unlink(missing_ok=True)
            except OSError as cleanup_error:
                exc.add_note(f"temporary report cleanup failed for {temporary}: {cleanup_error}")
        raise


class _Parser(argparse.ArgumentParser):
    def error(self, message):
        raise _Refusal("invalid_arguments")


def main(argv: list[str] | None = None) -> int:
    parser = _Parser(description=__doc__, allow_abbrev=False)
    for flag in (
        "repo",
        "source",
        "catalog",
        "semantic-packet",
        "expected-catalog-sha256",
        "expected-semantic-packet-sha256",
        "out",
    ):
        parser.add_argument("--" + flag, required=True)
    parser.add_argument("--max-tests", type=int, required=True)
    args = None
    report = _report(None)
    try:
        args = parser.parse_args(argv)
        params = vars(args).copy()
        destination = Path(params.pop("out"))
        report = select_regressions(**params)
        for value in (args.catalog, args.semantic_packet):
            path = Path(value)
            if destination.resolve() == path.resolve() or (
                destination.exists() and path.exists() and destination.samefile(path)
            ):
                raise _Refusal("output_aliases_input")
        write_report(report, destination)
    except _Refusal as exc:
        report["status"], report["node_ids"] = "unavailable", []
        report["limitations"].append(str(exc))
    except (OSError, RuntimeError) as exc:
        # Emitting a report is separate from selecting tests. Never hide the
        # original write error behind a cleanup error or keep runnable nodes.
        report["status"], report["node_ids"] = "unavailable", []
        report["limitations"].extend(
            ["output_unavailable", f"{type(exc).__name__}: {exc}", *getattr(exc, "__notes__", [])]
        )
    report["limitations"] = sorted(set(report["limitations"]))
    print(json.dumps(report, sort_keys=True, allow_nan=False))
    if any(reason in _INVALID for reason in report["limitations"]):
        return 2
    return 3 if report["status"] == "unavailable" else 0


if __name__ == "__main__":
    raise SystemExit(main())
