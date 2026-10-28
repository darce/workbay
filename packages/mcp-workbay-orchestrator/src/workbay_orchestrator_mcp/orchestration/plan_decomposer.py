#!/usr/bin/env python3
"""Plan → operator work-items decomposer (internal).

Pure core (``work_items_from_plan``) is deterministic text-in / list-out with no
I/O and no env reads. Codemap blast-radius enrichment is CLI-only: the CLI
builds the production subprocess transport, calls ``codemap_adapter.trace_callers``
per anchor, normalizes caller items to ``{file_path: ...}`` via
``_normalize_caller_items``, then injects envelopes into the pure core.

Rule IDs cited at use time (versionless): forced-declaration schema refusals,
AIPX-05 fail-closed when enrichment is absent (COMPLETE-or-refuse). See
docs/workbay/rules/heuristics-canon.md.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any, Mapping, Sequence

# ---------------------------------------------------------------------------
# Public errors
# ---------------------------------------------------------------------------


class PlanDecomposerError(ValueError):
    """Schema / forced-declaration / overlap refusal (CLI exit 2)."""


class EnrichmentRefuseError(PlanDecomposerError):
    """Codemap enrichment refusal (CLI exit 3)."""


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Use fullmatch semantics: ``$`` / re.match lets a trailing newline slip through.
_ITEM_ID_RE = re.compile(r"s[0-9]+")
_BLAST_VALUES = frozenset({"none", "full"})
_DECOMPOSITION_HEADING_RE = re.compile(
    r"^##\s+Decomposition\b[^\n]*\n",
    re.MULTILINE,
)
# First fenced ```json ... ``` block under the Decomposition heading.
_FENCED_JSON_RE = re.compile(
    r"```json\s*\n(.*?)```",
    re.DOTALL | re.IGNORECASE,
)

# Measured source-field for a caller's file location on codebase-memory-mcp
# graph Function nodes (same binary as trace_path). Pinned by
# tests/fixtures/trace_callers_envelope.json — do not invent alternate keys.
_CALLER_FILE_PATH_KEY = "file_path"

_OVERLAP_REMEDY = (
    "Remedies: (1) declare depends so the overlap is ordered; "
    "(2) fuse the items into one owns set; "
    "(3) keep them independent only with disjoint owns "
    "(S2 colour/conflict_edges recovers prefix collisions at compile time)."
)

_ENRICHMENT_REMEDY = (
    "Remedies: run query_graph for an exhaustive COMPLETE answer on a fresh "
    'index, or set blast: "none" to skip enrichment.'
)


# ---------------------------------------------------------------------------
# Path helpers (lexical only — mirror lane_manifest overlap semantics)
# ---------------------------------------------------------------------------


def _normalize_root(path: str) -> str:
    """Lexical normalize for exact-root / prefix overlap checks (no FS I/O)."""
    value = str(path).strip().replace("\\", "/")
    while value.endswith("/"):
        value = value[:-1]
    if not value or value == ".":
        return ""
    # Collapse // and . only; leave .. as-is for refuse-on-escape style checks.
    parts: list[str] = []
    for part in value.split("/"):
        if part in ("", "."):
            continue
        parts.append(part)
    return "/".join(parts)


def _roots_overlap(left: str, right: str) -> bool:
    """Exact-root or parent/child prefix relation on ``/`` boundaries."""
    if not left or not right:
        return True
    if left == right:
        return True
    return left.startswith(f"{right}/") or right.startswith(f"{left}/")


# ---------------------------------------------------------------------------
# Block load + schema validation
# ---------------------------------------------------------------------------


def _load_block(plan_text: str) -> dict[str, Any]:
    """Parse the FIRST fenced ```json block under ``## Decomposition``.

    Prose, inline backticks, and Files tables are documentation-only and must
    never be scraped. A plan with no Decomposition block or no fenced JSON
    under it refuses loudly.
    """
    if not isinstance(plan_text, str):
        raise PlanDecomposerError("plan_text must be a string")
    heading = _DECOMPOSITION_HEADING_RE.search(plan_text)
    if heading is None:
        raise PlanDecomposerError(
            "no ## Decomposition heading — plan is not decomposer-eligible "
            "(legacy plans use --operator-items / findings instead)"
        )
    # Slice the plan from the heading to the next same-or-higher heading, else EOF.
    rest = plan_text[heading.end() :]
    next_heading = re.search(r"^##\s+\S", rest, re.MULTILINE)
    section = rest[: next_heading.start()] if next_heading else rest
    match = _FENCED_JSON_RE.search(section)
    if match is None:
        raise PlanDecomposerError("no fenced ```json block under ## Decomposition — prose is never parsed")
    raw = match.group(1).strip()
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise PlanDecomposerError(f"Decomposition block is not valid JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise PlanDecomposerError("Decomposition block must be a JSON object")
    return payload


def _validate_payload(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Fail-closed schema + forced-declaration + independent overlap cross-check."""
    version = payload.get("version")
    if version != 1:
        raise PlanDecomposerError(f"Decomposition version must be 1, got {version!r}")
    items_raw = payload.get("items")
    if not isinstance(items_raw, list) or not items_raw:
        raise PlanDecomposerError("Decomposition items must be a non-empty list")

    items: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for idx, raw in enumerate(items_raw):
        if not isinstance(raw, dict):
            raise PlanDecomposerError(f"items[{idx}] must be an object")
        item_id = raw.get("id")
        # fullmatch (not match+$): reject ids with trailing newlines (``"s1\n"``).
        if not isinstance(item_id, str) or not _ITEM_ID_RE.fullmatch(item_id):
            raise PlanDecomposerError(f"items[{idx}] id must match ^s[0-9]+$, got {item_id!r}")
        if item_id in seen_ids:
            raise PlanDecomposerError(f"duplicate item id {item_id!r}")
        seen_ids.add(item_id)

        title = raw.get("title")
        if not isinstance(title, str) or not title.strip():
            raise PlanDecomposerError(f"item {item_id!r} title must be a non-empty string")

        independent = raw.get("independent")
        if not isinstance(independent, bool):
            raise PlanDecomposerError(f"item {item_id!r} independent must be a bool")

        owns = raw.get("owns")
        if not isinstance(owns, list) or not owns:
            raise PlanDecomposerError(f"item {item_id!r} owns must be a non-empty list of repo-relative paths")
        owns_clean: list[str] = []
        for p in owns:
            if not isinstance(p, str) or not p.strip():
                raise PlanDecomposerError(f"item {item_id!r} owns entries must be non-empty strings")
            owns_clean.append(p.strip())

        depends = raw.get("depends")
        if depends is None:
            depends = []
        if not isinstance(depends, list):
            raise PlanDecomposerError(f"item {item_id!r} depends must be a list")
        # De-duplicate depends preserving first-occurrence order before emit.
        depends_clean: list[str] = []
        seen_depends: set[str] = set()
        for d in depends:
            if not isinstance(d, str) or not d.strip():
                raise PlanDecomposerError(f"item {item_id!r} depends entries must be non-empty strings")
            dep = d.strip()
            if dep in seen_depends:
                continue
            seen_depends.add(dep)
            depends_clean.append(dep)

        blast = raw.get("blast", "none")
        if blast not in _BLAST_VALUES:
            raise PlanDecomposerError(f"item {item_id!r} blast must be 'none'|'full', got {blast!r}")

        anchors = raw.get("anchors")
        if blast == "full":
            if not isinstance(anchors, list) or not anchors:
                raise PlanDecomposerError(f"item {item_id!r} blast=='full' requires non-empty anchors")
            anchors_clean: list[str] = []
            for a in anchors:
                if not isinstance(a, str) or not a.strip():
                    raise PlanDecomposerError(f"item {item_id!r} anchors entries must be non-empty strings")
                anchors_clean.append(a.strip())
        else:
            anchors_clean = []
            if anchors is not None:
                if not isinstance(anchors, list):
                    raise PlanDecomposerError(f"item {item_id!r} anchors must be a list when present")
                for a in anchors:
                    if not isinstance(a, str) or not a.strip():
                        raise PlanDecomposerError(f"item {item_id!r} anchors entries must be non-empty strings")
                    anchors_clean.append(a.strip())

        # Forced-declaration check (independent must match depends emptiness).
        if independent is False and not depends_clean:
            raise PlanDecomposerError(
                f"item {item_id!r}: independent:false with empty depends is refused "
                f"(declare at least one depends, or set independent:true)"
            )
        if independent is True and depends_clean:
            raise PlanDecomposerError(
                f"item {item_id!r}: independent:true with non-empty depends is refused "
                f"(clear depends, or set independent:false)"
            )

        items.append(
            {
                "id": item_id,
                "title": title.strip(),
                "independent": independent,
                "owns": owns_clean,
                "depends": depends_clean,
                "blast": blast,
                "anchors": anchors_clean,
            }
        )

    id_set = {it["id"] for it in items}
    for it in items:
        for dep in it["depends"]:
            # Self-dependency is a cycle of length 1 — refuse naming the item.
            if dep == it["id"]:
                raise PlanDecomposerError(f"item {it['id']!r} depends on itself")
            # Exact-id resolution only — never prefix-match (s1 ≠ s11).
            if dep not in id_set:
                raise PlanDecomposerError(
                    f"item {it['id']!r} depends on unknown id {dep!r} "
                    f"(exact-id match only; known ids: {sorted(id_set)})"
                )

    # Overlap cross-check: two independent:true items with overlapping owns refuse.
    independents = [it for it in items if it["independent"] is True]
    for i, left in enumerate(independents):
        left_roots = [_normalize_root(p) for p in left["owns"]]
        for right in independents[i + 1 :]:
            right_roots = [_normalize_root(p) for p in right["owns"]]
            for lr in left_roots:
                for rr in right_roots:
                    if _roots_overlap(lr, rr):
                        raise PlanDecomposerError(
                            f"independent items {left['id']!r} and {right['id']!r} "
                            f"share overlapping owns ({lr!r} / {rr!r}). {_OVERLAP_REMEDY}"
                        )

    return items


# ---------------------------------------------------------------------------
# Codemap caller normalization (CLI; source key pinned by measured fixture)
# ---------------------------------------------------------------------------


def _normalize_caller_items(items: Sequence[Any]) -> list[dict[str, str]]:
    """Project raw adapter caller items to stable ``{file_path: ...}`` dicts.

    The source field name is **not** adapter-defined — it is the measured
    codebase-memory-mcp graph node key ``file_path`` (see
    ``tests/fixtures/trace_callers_envelope.json``). An item missing that key
    or carrying a non-string / empty value is unnormalizable and refuses.
    """
    if not isinstance(items, (list, tuple)):
        raise EnrichmentRefuseError(f"caller items must be a list, got {type(items).__name__}")
    out: list[dict[str, str]] = []
    for idx, raw in enumerate(items):
        if not isinstance(raw, Mapping):
            raise EnrichmentRefuseError(f"caller item[{idx}] is not an object — unnormalizable")
        fp = raw.get(_CALLER_FILE_PATH_KEY)
        if not isinstance(fp, str) or not fp.strip():
            raise EnrichmentRefuseError(
                f"caller item[{idx}] missing measured key "
                f"{_CALLER_FILE_PATH_KEY!r} — unnormalizable "
                f"(got keys {sorted(str(k) for k in raw.keys())})"
            )
        out.append({"file_path": fp.strip()})
    return out


def _require_enrichment_ready(envelope: Mapping[str, Any], *, item_id: str, anchor: str) -> None:
    """Refuse unless completeness is COMPLETE and index_state is fresh."""
    completeness = str(envelope.get("completeness") or "").strip().lower()
    index_state = str(envelope.get("index_state") or "").strip().lower()
    if completeness != "complete":
        raise EnrichmentRefuseError(
            f"item {item_id!r} anchor {anchor!r}: enrichment requires "
            f"completeness=complete, got {completeness!r}. {_ENRICHMENT_REMEDY}"
        )
    if index_state != "fresh":
        raise EnrichmentRefuseError(
            f"item {item_id!r} anchor {anchor!r}: enrichment requires "
            f"index_state=fresh, got {index_state!r}. {_ENRICHMENT_REMEDY}"
        )


def _apply_enrichment(
    owns: list[str],
    *,
    item_id: str,
    anchors: list[str],
    codemap_envelopes: Mapping[str, Any],
) -> list[str]:
    """Widen-only: append caller file paths from normalized envelopes.

    ``codemap_envelopes`` maps anchor symbol → QualifiedResult-like dict whose
    ``items`` are already projected to ``{file_path: ...}`` (CLI responsibility).
    """
    widened = list(owns)
    seen = set(owns)
    # Anchor order is declaration order; paths discovered append deterministically.
    for anchor in anchors:
        if anchor not in codemap_envelopes:
            raise EnrichmentRefuseError(
                f"item {item_id!r} anchor {anchor!r}: no codemap envelope provided. {_ENRICHMENT_REMEDY}"
            )
        envelope = codemap_envelopes[anchor]
        if not isinstance(envelope, Mapping):
            raise EnrichmentRefuseError(f"item {item_id!r} anchor {anchor!r}: envelope must be an object")
        _require_enrichment_ready(envelope, item_id=item_id, anchor=anchor)
        raw_items = envelope.get("items")
        if raw_items is None:
            raw_items = []
        # Items are expected pre-normalized; still fail-closed if not.
        try:
            callers = _normalize_caller_items(raw_items)  # type: ignore[arg-type]
        except EnrichmentRefuseError as exc:
            raise EnrichmentRefuseError(f"item {item_id!r} anchor {anchor!r}: {exc}") from exc
        for caller in callers:
            fp = caller["file_path"]
            if fp not in seen:
                seen.add(fp)
                widened.append(fp)
    return widened


# ---------------------------------------------------------------------------
# Pure core
# ---------------------------------------------------------------------------


def work_items_from_plan(
    plan_text: str,
    codemap_envelopes: Mapping[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Parse ``## Decomposition`` JSON → compiler operator work items.

    Each emitted item is exactly::

        {"id", "kind": "operator", "file_paths": owns, "summary": title,
         "depends_on": depends}

    Ids are emitted sorted (string order). Identical inputs yield byte-identical
    JSON. ``codemap_envelopes`` maps anchor → envelope for ``blast=="full"``
    items (widen-only). When any item has ``blast=="full"``, envelopes must be
    supplied (not ``None``) — COMPLETE-or-refuse; an empty mapping means
    enrichment ran with zero anchors/callers and is accepted into the apply
    path. The CLI normalizes caller items before injection; the core never
    opens a codemap transport.
    """
    payload = _load_block(plan_text)
    items = _validate_payload(payload)

    work_items: list[dict[str, Any]] = []
    for it in sorted(items, key=lambda x: x["id"]):
        owns = list(it["owns"])
        if it["blast"] == "full":
            # Fail-closed: None means no enrichment data was supplied at all.
            # Empty-but-not-None (e.g. {}) is accepted into the apply path —
            # enrichment ran; missing anchors still refuse inside apply.
            if codemap_envelopes is None:
                raise PlanDecomposerError(
                    f"item {it['id']!r}: blast=='full' requires codemap_envelopes "
                    f"(COMPLETE-or-refuse; None under-reports blast radius)"
                )
            owns = _apply_enrichment(
                owns,
                item_id=it["id"],
                anchors=it["anchors"],
                codemap_envelopes=codemap_envelopes,
            )
        work_items.append(
            {
                "id": it["id"],
                "kind": "operator",
                "file_paths": owns,
                "summary": it["title"],
                "depends_on": list(it["depends"]),
            }
        )
    return work_items


# ---------------------------------------------------------------------------
# CLI transport + enrichment harvest
# ---------------------------------------------------------------------------


def _subprocess_codemap_transport(
    tool: str,
    payload: Mapping[str, Any],
) -> Mapping[str, Any]:
    """Production transport: ``codebase-memory-mcp cli <tool> '<json>'``.

    Mirrors the subprocess path used by ``lane_context_packet.run_codemap_cli``
    — the same binary surface ``codemap_adapter.trace_callers`` expects as an
    injected transport.
    """
    # Local import keeps the pure core importable without the packet module.
    from workbay_orchestrator_mcp.orchestration.lane_context_packet import (
        run_codemap_cli,
    )

    result = run_codemap_cli(tool, dict(payload))
    if not result.get("ok"):
        err = result.get("error") or "codemap transport failed"
        raise EnrichmentRefuseError(f"codemap transport error for {tool}: {err}")
    data = result.get("data")
    if not isinstance(data, dict):
        raise EnrichmentRefuseError(f"codemap transport for {tool} returned non-object data")
    return data


def _collect_envelopes_via_trace(
    anchors: Sequence[str],
    *,
    transport: Any | None = None,
    head_sha: str | None = None,
    index_sha: str | None = None,
) -> dict[str, dict[str, Any]]:
    """Call ``trace_callers`` per anchor; normalize items; return anchor→envelope."""
    from workbay_orchestrator_mcp.orchestration import codemap_adapter

    xport = transport if transport is not None else _subprocess_codemap_transport
    out: dict[str, dict[str, Any]] = {}
    for anchor in anchors:
        try:
            result = codemap_adapter.trace_callers(
                xport,
                target=anchor,
                head_sha=head_sha,
                index_sha=index_sha,
            )
        except codemap_adapter.CodemapAdapterError as exc:
            raise EnrichmentRefuseError(f"trace_callers refused for anchor {anchor!r}: {exc}") from exc
        except EnrichmentRefuseError:
            raise
        except Exception as exc:  # noqa: BLE001 — transport / unexpected
            raise EnrichmentRefuseError(f"trace_callers failed for anchor {anchor!r}: {exc}") from exc
        envelope = result.to_dict()
        # Normalize before injection so the core only sees {file_path: ...}.
        try:
            envelope["items"] = _normalize_caller_items(envelope.get("items") or [])
        except EnrichmentRefuseError as exc:
            raise EnrichmentRefuseError(f"anchor {anchor!r}: {exc}") from exc
        out[anchor] = envelope
    return out


def _load_codemap_json(path: Path) -> dict[str, dict[str, Any]]:
    """Load ``--codemap-json``: ``{anchor: envelope, ...}`` or ``{envelopes: {...}}``."""
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise EnrichmentRefuseError(f"failed to load --codemap-json: {exc}") from exc
    if not isinstance(raw, dict):
        raise EnrichmentRefuseError("--codemap-json must be a JSON object")
    if "envelopes" in raw and isinstance(raw["envelopes"], dict):
        raw = raw["envelopes"]
    out: dict[str, dict[str, Any]] = {}
    for key, env in raw.items():
        if not isinstance(key, str) or not key.strip():
            raise EnrichmentRefuseError(f"--codemap-json key must be a non-empty anchor string, got {key!r}")
        if not isinstance(env, dict):
            raise EnrichmentRefuseError(f"--codemap-json envelope for {key!r} must be an object")
        # Normalize items at the CLI boundary (same as live transport path).
        items = env.get("items")
        if items is not None:
            try:
                env = dict(env)
                env["items"] = _normalize_caller_items(items)
            except EnrichmentRefuseError as exc:
                raise EnrichmentRefuseError(f"anchor {key!r}: {exc}") from exc
        out[key.strip()] = env
    return out


def _anchors_needing_enrichment(plan_text: str) -> list[str]:
    """Return sorted unique anchors for blast=full items (schema-validated)."""
    payload = _load_block(plan_text)
    items = _validate_payload(payload)
    anchors: list[str] = []
    seen: set[str] = set()
    for it in items:
        if it["blast"] != "full":
            continue
        for a in it["anchors"]:
            if a not in seen:
                seen.add(a)
                anchors.append(a)
    return anchors


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=("Decompose a plan's ## Decomposition JSON block into compiler operator work items (implementation note S1).")
    )
    parser.add_argument(
        "--plan-path",
        required=True,
        help="Path to the plan markdown containing ## Decomposition.",
    )
    parser.add_argument(
        "--codemap-json",
        help=(
            "Optional pre-built anchor→QualifiedResult.to_dict() map. "
            "When omitted and blast=full items exist, the CLI invokes "
            "codemap_adapter.trace_callers via the production subprocess transport."
        ),
    )
    parser.add_argument(
        "--stdout",
        action="store_true",
        help="Print the operator work-items JSON list to stdout.",
    )
    parser.add_argument(
        "--head-sha",
        default=None,
        help="Optional worktree HEAD sha for index-freshness (live codemap path).",
    )
    parser.add_argument(
        "--index-sha",
        default=None,
        help="Optional codemap index sha for index-freshness (live codemap path).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    plan_path = Path(args.plan_path).expanduser()
    try:
        plan_text = plan_path.read_text(encoding="utf-8")
    except OSError as exc:
        print(f"failed to read --plan-path: {exc}", file=sys.stderr)
        return 2

    try:
        anchors = _anchors_needing_enrichment(plan_text)
        envelopes: dict[str, dict[str, Any]] | None = None
        if args.codemap_json:
            envelopes = _load_codemap_json(Path(args.codemap_json).expanduser())
        elif anchors:
            envelopes = _collect_envelopes_via_trace(
                anchors,
                head_sha=args.head_sha,
                index_sha=args.index_sha,
            )

        work_items = work_items_from_plan(plan_text, codemap_envelopes=envelopes)
    except EnrichmentRefuseError as exc:
        print(str(exc), file=sys.stderr)
        return 3
    except PlanDecomposerError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    except ValueError as exc:
        # Subclass hierarchy already covered; plain ValueError → schema exit.
        print(str(exc), file=sys.stderr)
        return 2

    payload = json.dumps(work_items, indent=2) + "\n"
    if args.stdout:
        sys.stdout.write(payload)
    else:
        # Default: still emit to stdout so piping to --operator-items - works
        # when the compiler accepts stdin; operators may redirect to a file.
        sys.stdout.write(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
