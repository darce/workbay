"""``sync-task-plan-checklist`` subcommand (internal).

Evidence-driven projection of handoff DB state onto the `- [ ]` /
`- [x]` checkboxes in a task plan markdown file. Granular: every
flipped box must trace back to a specific DB record (a `close_slice`
decision's `changed_files`, a `record_event(event_kind="test_result")`
`command`, or an explicit decision id reference). One-way ratchet:
boxes only go `- [ ]` -> `- [x]`. Dry-run by default; `--apply`
mutates.

Three pure layers (parse / resolve / apply) so the slice-1 unit tests
can drive each independently with hand-built Evidence inputs. The
``run(argv)`` wrapper composes them with a real MCP query; implementation note
adds the lifecycle.mk recipe-level post-step that captures the
emitted JSON receipt.

Section-class semantics (the canonical headings live in
``TASK_PLAN.template.md``):

* ``## Stretch Goals`` items are filtered before the resolver — they
  never tick automatically, even when their referenced artifact ships.
* ``### Checklist for Slice N: <title>`` items match against the
  slice-N close decision's ``changed_files`` and recorded
  ``test_result`` commands. Slice identity prefers the structured
  ``slice_number`` column on the decision row; the slug regex is a
  NULL-only fallback for historical rows.
* ``## Context and Ownership`` / ``## Review Readiness`` /
  ``## Success Criteria`` items match against the union of all
  recorded evidence for the task.

The in-doc Consolidated Checklist is a **non-authoritative rendered cache**; canonical completion state is queried via ``make plan-status`` / ``render_handoff(kind='plan_checklist')`` (DB + archive projection). Manual checklist edits to archived plans on ``main`` are blocked by ``guard_archived_plan_checklist.py``.

The writer rewrites only the `- [ ]` -> `- [x]` lines whose
resolution is ``tick``; all other bytes are preserved verbatim.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import datetime, timezone
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable

import resolver

from . import _common

SECTION_STRETCH = "stretch"
SECTION_SLICE = "slice"
SECTION_CONTEXT = "context"
SECTION_REVIEW = "review"
SECTION_SUCCESS = "success"
SECTION_OTHER = "other"

RESOLUTION_TICK = "tick"
RESOLUTION_KEEP = "keep"
RESOLUTION_UNRESOLVED = "unresolved"
RESOLUTION_ALREADY_TICKED = "already_ticked"

_HEADING_RE = re.compile(r"^(#{2,3})\s+(.+?)\s*$")
_CHECKBOX_RE = re.compile(r"^(\s*-\s*)\[( |x|X)\]\s+(.*)$")
_SLICE_HEADING_RE = re.compile(
    r"^Checklist\s+for\s+Slice\s+(\d+)\b", re.IGNORECASE
)
_BACKTICK_RE = re.compile(r"`([^`]+)`")
_MAKE_TARGET_RE = re.compile(r"\bmake\s+([a-z][a-z0-9-]+)")
_SLICE_REF_RE = re.compile(r"\bSlice\s+(\d+)\b")
_DECISION_ID_RE = re.compile(
    r"\b([a-z][a-z0-9_]+_slice_complete_[A-Za-z0-9][A-Za-z0-9_-]*_\w+)\b"
)
_ATTEST_ANCHOR_RE = re.compile(r"<!--\s*attest:\s*([a-z0-9_]+)\s*-->")


@dataclass(frozen=True)
class Anchors:
    """Evidence anchors extracted from one checklist item body.

    Backtick-quoted token classification:

    * contains whitespace -> ``commands`` (shell command line, matched
      against recorded ``test_result`` commands by substring).
    * contains `/` and no whitespace -> ``paths`` (file path).
    * no `/`, has `.` or is a known extensionless project file ->
      ``basenames`` (matched only against the slice-scoped basename
      set, never via repo-wide path grep).

    ``make_targets`` come from ``make <token>`` mentions outside
    backticks. ``slice_refs`` come from ``Slice N`` mentions.
    ``decision_ids`` come from explicit references to the canonical
    ``<author>_slice_complete_...`` form.
    """
    paths: tuple[str, ...] = ()
    basenames: tuple[str, ...] = ()
    commands: tuple[str, ...] = ()
    make_targets: tuple[str, ...] = ()
    slice_refs: tuple[int, ...] = ()
    decision_ids: tuple[str, ...] = ()
    attestations: tuple[str, ...] = ()

    def is_empty(self) -> bool:
        return not (
            self.paths
            or self.basenames
            or self.commands
            or self.make_targets
            or self.slice_refs
            or self.decision_ids
            or self.attestations
        )


@dataclass(frozen=True)
class ChecklistItem:
    line_index: int  # 0-based index into the original text's lines
    raw_line: str  # original line including newline-less suffix
    section_class: str
    slice_number: int | None  # populated only for SECTION_SLICE items
    already_ticked: bool
    body: str  # text after `- [ ] ` / `- [x] `
    anchors: Anchors


@dataclass(frozen=True)
class ParsedPlan:
    items: tuple[ChecklistItem, ...]
    text: str  # the original plan text (preserved for round-trip writes)

    def lines(self) -> list[str]:
        return self.text.splitlines(keepends=True)


@dataclass(frozen=True)
class Resolution:
    line_index: int
    action: str  # tick / keep / unresolved / already_ticked
    reason: str


@dataclass
class Evidence:
    """Pre-collected evidence the resolver consults. implementation note builds this
    from the resolver-friendly handoff projection; tests construct it
    directly to drive each resolution path.

    ``slice_changed_files`` maps a slice number to the set of file paths
    that slice's ``close_slice`` decision recorded under ``changed_files``.
    ``slice_basenames`` is the basename projection of the same set.
    ``slice_close_decision_ids`` maps a slice number to the canonical
    decision id (``<author>_slice_complete_<work_ref>_<slug>``) — used
    so an item that references that id by hand resolves cleanly.
    """
    slice_changed_files: dict[int, set[str]] = field(default_factory=dict)
    slice_basenames: dict[int, set[str]] = field(default_factory=dict)
    slice_close_decision_ids: dict[int, str] = field(default_factory=dict)
    test_commands: set[str] = field(default_factory=set)
    all_changed_files: set[str] = field(default_factory=set)
    all_basenames: set[str] = field(default_factory=set)
    all_decision_ids: set[str] = field(default_factory=set)
    attestation_criteria: set[str] = field(default_factory=set)
    # internal collision invariant. When True, bare ``Slice N``
    # anchors do NOT match against ``slice_close_decision_ids`` —
    # attribution falls back to plan-specific file-path, decision-id,
    # command, and make-target anchors. Set this when a ``task_ref`` is
    # known to collide across packages (e.g. internal) so a slice-close
    # decision recorded for one plan cannot tick a bare ``Slice N`` box
    # in the other plan.
    suppress_bare_slice_refs: bool = False


# ---------------------------------------------------------------------------
# Layer 1: parse
# ---------------------------------------------------------------------------


def _classify_heading(level: int, title: str) -> tuple[str, int | None]:
    norm = title.strip()
    if level == 2:
        if norm.lower() == "stretch goals":
            return SECTION_STRETCH, None
        if norm.lower() == "context and ownership":
            return SECTION_CONTEXT, None
        if norm.lower() == "review readiness":
            return SECTION_REVIEW, None
        if norm.lower() == "success criteria":
            return SECTION_SUCCESS, None
        return SECTION_OTHER, None
    if level == 3:
        m = _SLICE_HEADING_RE.match(norm)
        if m:
            return SECTION_SLICE, int(m.group(1))
        return SECTION_OTHER, None
    return SECTION_OTHER, None


def _extract_anchors(body: str) -> Anchors:
    paths: list[str] = []
    basenames: list[str] = []
    commands: list[str] = []
    for match in _BACKTICK_RE.finditer(body):
        token = match.group(1).strip()
        if not token:
            continue
        has_ws = any(c.isspace() for c in token)
        if has_ws:
            commands.append(token)
        elif "/" in token:
            paths.append(token)
        elif "." in token or token in ("Makefile", "Dockerfile", "lifecycle.mk"):
            # bare basenames must look like a filename — at least one '.'
            # or a known extensionless project file
            basenames.append(token)
    make_targets = [m.group(1) for m in _MAKE_TARGET_RE.finditer(body)]
    slice_refs = [int(m.group(1)) for m in _SLICE_REF_RE.finditer(body)]
    decision_ids = [m.group(1) for m in _DECISION_ID_RE.finditer(body)]
    attestations = [m.group(1) for m in _ATTEST_ANCHOR_RE.finditer(body)]
    return Anchors(
        paths=tuple(dict.fromkeys(paths)),
        basenames=tuple(dict.fromkeys(basenames)),
        commands=tuple(dict.fromkeys(commands)),
        make_targets=tuple(dict.fromkeys(make_targets)),
        slice_refs=tuple(dict.fromkeys(slice_refs)),
        decision_ids=tuple(dict.fromkeys(decision_ids)),
        attestations=tuple(dict.fromkeys(attestations)),
    )


def parse(text: str) -> ParsedPlan:
    """Parse a task plan into ``ChecklistItem`` records.

    The parser is lenient: any `- [ ]` / `- [x]` line that isn't inside
    a fenced code block and that lives under one of the canonical
    section headings yields an item. Lines under other headings
    (heading classification ``other``) are tracked so callers can spot
    them in diagnostics, but resolver semantics treat them as
    ``unresolved`` by default.
    """
    items: list[ChecklistItem] = []
    section_class = SECTION_OTHER
    slice_number: int | None = None
    in_fence = False
    fence_marker = ""
    for line_index, raw in enumerate(text.splitlines()):
        stripped = raw.lstrip()
        if stripped.startswith("```") or stripped.startswith("~~~"):
            marker = stripped[:3]
            if not in_fence:
                in_fence = True
                fence_marker = marker
            elif stripped.startswith(fence_marker):
                in_fence = False
                fence_marker = ""
            continue
        if in_fence:
            continue
        heading = _HEADING_RE.match(raw)
        if heading:
            level = len(heading.group(1))
            section_class, slice_number = _classify_heading(level, heading.group(2))
            continue
        cb = _CHECKBOX_RE.match(raw)
        if not cb:
            continue
        already_ticked = cb.group(2) in ("x", "X")
        body = cb.group(3)
        anchors = _extract_anchors(body)
        items.append(
            ChecklistItem(
                line_index=line_index,
                raw_line=raw,
                section_class=section_class,
                slice_number=slice_number if section_class == SECTION_SLICE else None,
                already_ticked=already_ticked,
                body=body,
                anchors=anchors,
            )
        )
    return ParsedPlan(items=tuple(items), text=text)


# ---------------------------------------------------------------------------
# Layer 2: resolve
# ---------------------------------------------------------------------------


def _command_matches(anchor: str, recorded: str) -> bool:
    """Return True when ``anchor`` is contained in ``recorded`` either as
    a full substring or when both share a non-trivial common suffix.

    Anchors written in plans are usually a verbatim copy of the test
    command the agent intended to run; recorded commands may have
    extra prefix wrapping (``cd packages/example && uv run ...``) or
    an additional flag. Substring containment in either direction
    handles both shapes without over-matching.
    """
    if not anchor or not recorded:
        return False
    if anchor in recorded:
        return True
    if recorded in anchor:
        return True
    return False


def _match_item(item: ChecklistItem, evidence: Evidence) -> tuple[bool, str]:
    if item.section_class == SECTION_SLICE and item.slice_number is not None:
        n = item.slice_number
        slice_files = evidence.slice_changed_files.get(n, set())
        slice_basenames = evidence.slice_basenames.get(n, set())
        slice_decision_id = evidence.slice_close_decision_ids.get(n)
        for path in item.anchors.paths:
            if path in slice_files:
                return True, f"path_in_slice_{n}_changed_files:{path}"
        for base in item.anchors.basenames:
            if base in slice_basenames:
                return True, f"basename_in_slice_{n}_changed_files:{base}"
        if not evidence.suppress_bare_slice_refs:
            for sref in item.anchors.slice_refs:
                if sref == n and slice_decision_id:
                    return True, f"slice_{n}_closed:{slice_decision_id}"
        for did in item.anchors.decision_ids:
            if did == slice_decision_id:
                return True, f"decision_id_match:{did}"
        for anchor in item.anchors.commands:
            for cmd in evidence.test_commands:
                if _command_matches(anchor, cmd):
                    return True, f"test_command_match:{cmd!r}"
        for target in item.anchors.make_targets:
            token = f"make {target}"
            for cmd in evidence.test_commands:
                if token in cmd:
                    return True, f"make_target_in_test_command:{token!r}"
        return False, "no_slice_evidence_match"
    if item.section_class in (SECTION_CONTEXT, SECTION_REVIEW, SECTION_SUCCESS):
        for path in item.anchors.paths:
            if path in evidence.all_changed_files:
                return True, f"path_in_changed_files:{path}"
        for base in item.anchors.basenames:
            if base in evidence.all_basenames:
                return True, f"basename_in_changed_files:{base}"
        for did in item.anchors.decision_ids:
            if did in evidence.all_decision_ids:
                return True, f"decision_id_recorded:{did}"
        for anchor in item.anchors.commands:
            for cmd in evidence.test_commands:
                if _command_matches(anchor, cmd):
                    return True, f"test_command_match:{cmd!r}"
        for target in item.anchors.make_targets:
            token = f"make {target}"
            for cmd in evidence.test_commands:
                if token in cmd:
                    return True, f"make_target_in_test_command:{token!r}"
        for criterion in item.anchors.attestations:
            if criterion in evidence.attestation_criteria:
                return True, f"attestation_recorded:{criterion}"
        return False, "no_task_wide_evidence_match"
    return False, f"section_class_{item.section_class}_not_synced"


def resolve(parsed: ParsedPlan, evidence: Evidence) -> dict[int, Resolution]:
    """Resolve every checklist item to a tick / keep / unresolved verdict.

    ``## Stretch Goals`` items short-circuit to ``keep`` with
    ``stretch_section_never_auto_ticks`` regardless of anchors — that
    invariant is the whole point of the Stretch carveout.

    One-way ratchet: any item that is already ticked maps to
    ``already_ticked`` (the writer leaves the line untouched).
    """
    resolutions: dict[int, Resolution] = {}
    for item in parsed.items:
        if item.already_ticked:
            resolutions[item.line_index] = Resolution(
                line_index=item.line_index,
                action=RESOLUTION_ALREADY_TICKED,
                reason="already_ticked",
            )
            continue
        if item.section_class == SECTION_STRETCH:
            resolutions[item.line_index] = Resolution(
                line_index=item.line_index,
                action=RESOLUTION_KEEP,
                reason="stretch_section_never_auto_ticks",
            )
            continue
        if item.anchors.is_empty():
            resolutions[item.line_index] = Resolution(
                line_index=item.line_index,
                action=RESOLUTION_UNRESOLVED,
                reason="no_evidence_anchors_in_body",
            )
            continue
        matched, reason = _match_item(item, evidence)
        resolutions[item.line_index] = Resolution(
            line_index=item.line_index,
            action=RESOLUTION_TICK if matched else RESOLUTION_KEEP,
            reason=reason,
        )
    return resolutions


# ---------------------------------------------------------------------------
# Layer 3: apply
# ---------------------------------------------------------------------------


def apply(text: str, resolutions: dict[int, Resolution]) -> str:
    """Return ``text`` with the resolved ``tick`` lines flipped.

    Only lines whose resolution is ``RESOLUTION_TICK`` and whose
    current state is `- [ ]` are rewritten. Whitespace, indentation,
    and trailing text are preserved byte-for-byte.
    """
    if not resolutions:
        return text
    lines = text.splitlines(keepends=True)
    for line_index, resolution in resolutions.items():
        if resolution.action != RESOLUTION_TICK:
            continue
        if line_index < 0 or line_index >= len(lines):
            continue
        original = lines[line_index]
        # Strip trailing newline for the regex match, then re-append.
        if original.endswith("\r\n"):
            eol = "\r\n"
            body = original[:-2]
        elif original.endswith("\n"):
            eol = "\n"
            body = original[:-1]
        else:
            eol = ""
            body = original
        m = _CHECKBOX_RE.match(body)
        if not m or m.group(2) in ("x", "X"):
            continue
        prefix = m.group(1)
        rest = m.group(3)
        lines[line_index] = f"{prefix}[x] {rest}{eol}"
    return "".join(lines)


# ---------------------------------------------------------------------------
# Layer 4: CLI / receipt
# ---------------------------------------------------------------------------


def _classify_counts(resolutions: dict[int, Resolution]) -> dict[str, int]:
    counts = {"ticked": 0, "kept": 0, "unresolved": 0, "already_ticked": 0}
    for r in resolutions.values():
        if r.action == RESOLUTION_TICK:
            counts["ticked"] += 1
        elif r.action == RESOLUTION_UNRESOLVED:
            counts["unresolved"] += 1
        elif r.action == RESOLUTION_ALREADY_TICKED:
            counts["already_ticked"] += 1
        else:
            counts["kept"] += 1
    return counts


def _diff_preview(
    original: str, rewritten: str, resolutions: dict[int, Resolution]
) -> list[dict[str, Any]]:
    """Return per-tick line entries so a dry-run prints which boxes
    *would* flip. Lightweight — full unified diff is overkill for a
    one-character change.
    """
    if original == rewritten:
        return []
    original_lines = original.splitlines()
    entries: list[dict[str, Any]] = []
    for r in resolutions.values():
        if r.action != RESOLUTION_TICK:
            continue
        idx = r.line_index
        if idx < 0 or idx >= len(original_lines):
            continue
        entries.append(
            {
                "line": idx + 1,
                "before": original_lines[idx],
                "after": original_lines[idx].replace("- [ ]", "- [x]", 1),
                "reason": r.reason,
            }
        )
    return entries


def build_evidence_from_handoff_payload(
    search_payload: dict[str, Any] | None = None,
    tests_payload: dict[str, Any] | None = None,
) -> Evidence:
    """Project two handoff CLI envelopes into an :class:`Evidence`.

    ``search_payload`` is the ``handoff-search`` envelope (decision
    rows + ``changed_files_json``). ``tests_payload`` is the
    ``get-verified-tests`` envelope (verified_test rows with
    ``command``). Either may be ``None`` for partial-evidence
    fallback. implementation note ships this projector so the CLI path stays thin;
    the unit tests bypass it by constructing :class:`Evidence`
    directly.
    """
    evidence = Evidence()
    # Historical rows only: slug regex fallback when slice_number was not stored.
    slice_complete_re = re.compile(
        r"_slice_complete_[A-Za-z0-9_-]+?_slice_(\d+)[A-Za-z]*(?:_|$)"
    )
    results = (
        (search_payload or {}).get("data", {}).get("results", [])
        if isinstance(search_payload, dict)
        else []
    )
    for row in results:
        if not isinstance(row, dict):
            continue
        if row.get("record_type") != "decision":
            continue
        decision_id = row.get("decision") or row.get("decision_id")
        if isinstance(decision_id, str):
            evidence.all_decision_ids.add(decision_id)
            if decision_id.startswith("attestation:"):
                criterion = decision_id[len("attestation:") :]
                if criterion:
                    evidence.attestation_criteria.add(criterion)
        changed_raw = row.get("changed_files_json") or row.get("changed_files")
        changed: list[str] = []
        if isinstance(changed_raw, str) and changed_raw:
            try:
                parsed_changed = json.loads(changed_raw)
            except (ValueError, json.JSONDecodeError):
                parsed_changed = []
            if isinstance(parsed_changed, list):
                changed = [c for c in parsed_changed if isinstance(c, str)]
        elif isinstance(changed_raw, list):
            changed = [c for c in changed_raw if isinstance(c, str)]
        for path in changed:
            evidence.all_changed_files.add(path)
            evidence.all_basenames.add(path.rsplit("/", 1)[-1])
        slice_n: int | None = None
        raw_slice_number = row.get("slice_number")
        if raw_slice_number is not None and raw_slice_number != "":
            try:
                slice_n = int(raw_slice_number)
            except (TypeError, ValueError):
                slice_n = None
        if slice_n is None and isinstance(decision_id, str):
            m = slice_complete_re.search(decision_id)
            if m:
                slice_n = int(m.group(1))
        if slice_n is not None and isinstance(decision_id, str):
            evidence.slice_close_decision_ids[slice_n] = decision_id
            bucket = evidence.slice_changed_files.setdefault(slice_n, set())
            base_bucket = evidence.slice_basenames.setdefault(slice_n, set())
            for path in changed:
                bucket.add(path)
                base_bucket.add(path.rsplit("/", 1)[-1])
    tests = (
        (tests_payload or {}).get("data", {}).get("tests", [])
        if isinstance(tests_payload, dict)
        else []
    )
    for row in tests:
        if not isinstance(row, dict):
            continue
        cmd = row.get("command")
        if isinstance(cmd, str) and cmd:
            evidence.test_commands.add(cmd)
    return evidence


def resolve_workspace_root(plan_path: Path | None = None) -> Path:
    """Return the current worktree root for task-plan file operations.

    A task plan may live under a nested package path (e.g.
    ``packages/mcp-workbay-handoff/docs/tasks/internal-...``), but relative
    ``PLAN=`` values and filesystem discovery must resolve against the
    git worktree the operator is running in. In linked-worktree tasks,
    that is the feature worktree, not the primary checkout that owns the
    handoff DB.

    Resolution order:

    1. ``resolver.repo_root(cwd)`` — current worktree git toplevel.
    2. ``resolver.repo_root(plan_path.parent)`` — when cwd is outside any
       checkout but the explicit plan itself is inside one.
    3. ``plan_path.parent`` or cwd as the legacy fallback so the
       handler still emits a receipt instead of crashing when no git
       context is available at all.
    """
    cwd = Path.cwd()
    root = resolver.repo_root(cwd)
    if root is not None:
        return root
    if plan_path is not None:
        root = resolver.repo_root(plan_path.parent)
        if root is not None:
            return root
        return plan_path.parent
    return cwd


def resolve_handoff_workspace_root(workspace_root: Path) -> Path:
    """Return the canonical root used for MCP handoff state.

    The handoff DB and sidecar state belong to the primary workspace even
    when a lifecycle command runs from a linked feature worktree. Keep
    this separate from :func:`resolve_workspace_root`, which intentionally
    returns the current worktree for plan-file reads and writes.
    """
    return resolver.canonical_workspace_root(workspace_root) or workspace_root


def _lookup_stored_plan_path(repo: Path, task_ref: str) -> str | None:
    """Resolve the task's stored ``task_plan_path`` via ``handoff state``.

    Returns the absolute path string when found, ``None`` on any failure
    (CLI absent, task row missing, field unset, payload unparseable).
    Callers treat ``None`` as "plan unresolved" and emit a clear error.
    """
    handoff_root = resolve_handoff_workspace_root(repo)
    argv = [
        _common.mcp_handoff_bin(),
        "--workspace-root", str(handoff_root),
        "state",
        "--sections", "identity",
        task_ref,
    ]
    proc = _common.run_subprocess(argv)
    if proc.returncode != 0:
        return None
    try:
        payload = json.loads(proc.stdout)
    except (ValueError, json.JSONDecodeError):
        return None
    active = (
        (payload.get("data") or {}).get("active")
        if isinstance(payload, dict)
        else None
    )
    if not isinstance(active, dict):
        return None
    abs_path = active.get("task_plan_abs_path")
    if isinstance(abs_path, str) and abs_path:
        return abs_path
    rel_path = active.get("task_plan_path")
    if isinstance(rel_path, str) and rel_path:
        return str(repo / rel_path)
    return None


SIDECAR_FILENAME = "checklist_sync.json"


def _resolve_state_dir(repo: Path) -> Path:
    """Return the ``.task-state/`` directory for sidecar writes.

    Honors ``WORKBAY_HANDOFF_STATE_DIR`` (the same override the
    ``mcp-workbay-handoff`` runtime reads) so tests and operators that
    point at an alternate state directory get a consistent sidecar
    location with the handoff DB.
    """
    override = os.environ.get("WORKBAY_HANDOFF_STATE_DIR")
    if override:
        return Path(override)
    return repo / ".task-state"


def _write_sync_sidecar(state_dir: Path, task_ref: str, entry: dict[str, Any]) -> None:
    """Merge ``entry`` into ``state_dir/checklist_sync.json`` under ``task_ref``.

    internal: the sidecar is the cross-package contract between
    the sync handler (in ``workbay-system``) and the dashboard renderer
    (in ``mcp-workbay-handoff``). Both sides agree on the file location
    and the per-task shape ``{ok, ticked, warning, plan_path,
    recorded_at}``. Best-effort write — never raises, never blocks the
    sync's primary receipt emission.
    """
    try:
        state_dir.mkdir(parents=True, exist_ok=True)
    except OSError:
        return
    target = state_dir / SIDECAR_FILENAME
    existing: dict[str, Any] = {}
    if target.is_file():
        try:
            parsed = json.loads(target.read_text(encoding="utf-8"))
            if isinstance(parsed, dict):
                existing = parsed
        except (OSError, ValueError, json.JSONDecodeError):
            existing = {}
    existing[task_ref] = entry
    try:
        target.write_text(
            json.dumps(existing, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    except OSError:
        return


def _query_handoff_evidence(
    repo: Path, task_ref: str
) -> tuple[Evidence, str, str | None]:
    """Shell out to ``state`` and ``get-verified-tests``, project the two
    envelopes into Evidence.

    Returns ``(evidence, handoff_projection, warning_or_None)``.
    ``handoff_projection`` is ``"synced"`` only when both CLI calls
    succeed. Either failure flips it to ``"pending"`` with a warning;
    Sync never raises — a missing CLI degrades to empty evidence
    (every item resolves to ``keep`` / ``unresolved``), which is the
    safe fallback.

    ``state --sections decisions_recent --detail full`` is the read
    path for close_slice decisions; the FTS ``handoff-search`` CLI
    requires a literal search term and cannot enumerate all decisions
    by record_type alone. ``--detail full`` is required for the
    ``decisions_recent`` section to populate (the summary detail level
    returns an empty list).
    """
    # ``--decision-fields`` is nargs='+', so its values would greedily
    # consume the positional ``task_ref`` argument if it followed.
    # Pass the positional FIRST and keep the variadic option last.
    handoff_root = resolve_handoff_workspace_root(repo)
    decisions_argv = [
        _common.mcp_handoff_bin(),
        "--workspace-root", str(handoff_root),
        "state",
        task_ref,
        "--sections", "decisions_recent",
        "--detail", "full",
        "--top-n-decisions", "100",
        "--decision-fields", "decision", "changed_files_json", "slice_number",
    ]
    tests_argv = [
        _common.mcp_handoff_bin(),
        "--workspace-root", str(handoff_root),
        "get-verified-tests",
        "--task-ref", task_ref,
        "--passed", "true",
        "--exclude-never-passed",
        "--limit", "100",
    ]
    warnings: list[str] = []
    decisions_proc = _common.run_subprocess(decisions_argv)
    search_payload: dict[str, Any] | None = None
    if decisions_proc.returncode != 0:
        warnings.append(
            f"state_decisions_failed: rc={decisions_proc.returncode} "
            f"stderr={decisions_proc.stderr.strip()[:120]!r}"
        )
    else:
        try:
            state_payload = json.loads(decisions_proc.stdout)
        except (ValueError, json.JSONDecodeError):
            warnings.append("state_decisions_unparseable")
        else:
            # Normalize ``decisions_recent`` rows into the
            # ``data.results`` shape that build_evidence_from_handoff_payload
            # consumes (each row is implicitly record_type=decision).
            recents = (
                (state_payload.get("data") or {}).get("decisions_recent")
                if isinstance(state_payload, dict)
                else None
            )
            if isinstance(recents, list):
                search_payload = {
                    "data": {
                        "results": [
                            {**row, "record_type": "decision"}
                            for row in recents
                            if isinstance(row, dict)
                        ]
                    }
                }
    tests_proc = _common.run_subprocess(tests_argv)
    tests_payload: dict[str, Any] | None = None
    if tests_proc.returncode != 0:
        warnings.append(
            f"get_verified_tests_failed: rc={tests_proc.returncode} "
            f"stderr={tests_proc.stderr.strip()[:120]!r}"
        )
    else:
        try:
            tests_payload = json.loads(tests_proc.stdout)
        except (ValueError, json.JSONDecodeError):
            warnings.append("get_verified_tests_unparseable")
    evidence = build_evidence_from_handoff_payload(search_payload, tests_payload)
    projection = "synced" if not warnings else "pending"
    warning = "; ".join(warnings) if warnings else None
    return evidence, projection, warning


def _fetch_archive_envelope(handoff_root: Path, task_ref: str) -> dict[str, Any] | None:
    """Return the ``archive --operation get`` envelope when the task is archived."""

    argv = [
        _common.mcp_handoff_bin(),
        "--workspace-root",
        str(handoff_root),
        "archive",
        "--operation",
        "get",
        "--task-ref",
        task_ref,
        "--include-snapshot",
    ]
    proc = _common.run_subprocess(argv)
    if proc.returncode != 0:
        return None
    try:
        payload = json.loads(proc.stdout)
    except (ValueError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict) or not payload.get("ok"):
        return None
    return payload


def _handoff_payloads_from_snapshot(
    snapshot: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Shape an archive snapshot into the envelopes ``build_evidence`` expects."""

    decisions = snapshot.get("decisions") or []
    tests = snapshot.get("verified_tests") or []
    search_payload = {
        "data": {
            "results": [
                {**row, "record_type": "decision"}
                for row in decisions
                if isinstance(row, dict)
            ]
        }
    }
    tests_payload = {
        "data": {
            "tests": [row for row in tests if isinstance(row, dict)],
        }
    }
    return search_payload, tests_payload


def _repo_relative_path(repo: Path, path: Path) -> str | None:
    try:
        return str(path.resolve().relative_to(repo.resolve()))
    except ValueError:
        return None


def _git_show_text_at_ref(repo: Path, ref: str, rel_path: str) -> str | None:
    proc = _common.run_subprocess(
        ["git", "-C", str(repo), "show", f"{ref}:{rel_path}"],
    )
    if proc.returncode != 0:
        return None
    return proc.stdout


def _resolve_projection_plan(
    workspace_root: Path,
    handoff_root: Path,
    task_ref: str,
    plan_path: Path | None,
) -> tuple[str | None, str | None, str, str | None]:
    """Load plan text for projection: worktree (live) or ``git show`` (archived)."""

    if plan_path is not None:
        plan_file = plan_path if plan_path.is_absolute() else workspace_root / plan_path
        if plan_file.is_file():
            return plan_file.read_text(encoding="utf-8"), str(plan_file), "flag", None
        return None, str(plan_file), "flag", "plan_not_found"

    stored = _lookup_stored_plan_path(workspace_root, task_ref)
    if stored is not None:
        plan_file = Path(stored)
        if not plan_file.is_absolute():
            plan_file = workspace_root / plan_file
        if plan_file.is_file():
            return plan_file.read_text(encoding="utf-8"), str(plan_file), "worktree", None

    archive_env = _fetch_archive_envelope(handoff_root, task_ref)
    if archive_env is None:
        if stored:
            return None, stored, "worktree", "plan_not_found"
        return None, None, "unresolved", "plan_unresolved"

    snapshot = (archive_env.get("data") or {}).get("snapshot")
    if not isinstance(snapshot, dict):
        display = stored
        return None, display, "archive", "archive_snapshot_missing"

    active = snapshot.get("active") or {}
    rel_path = active.get("task_plan_path")
    abs_path = active.get("task_plan_abs_path")
    plan_display: str | None
    if isinstance(abs_path, str) and abs_path:
        plan_display = abs_path
    elif isinstance(rel_path, str) and rel_path:
        plan_display = str(workspace_root / rel_path)
    else:
        plan_display = stored

    archive_meta = (archive_env.get("data") or {}).get("archive") or {}
    integration_ref = active.get("last_observed_integration_sha")
    if not isinstance(integration_ref, str) or not integration_ref:
        integration_ref = archive_meta.get("archived_commit_sha")
    if not isinstance(integration_ref, str) or not integration_ref:
        integration_ref = "main"

    rel_for_git = rel_path if isinstance(rel_path, str) and rel_path else None
    if rel_for_git is None and plan_display is not None:
        rel_for_git = _repo_relative_path(workspace_root, Path(plan_display))
    if not rel_for_git:
        return None, plan_display, "archive", "plan_path_missing_in_archive"

    text = _git_show_text_at_ref(workspace_root, integration_ref, rel_for_git)
    if text is None:
        return (
            None,
            plan_display,
            "git_history",
            f"git_show_failed:{integration_ref}:{rel_for_git}",
        )
    return text, plan_display or rel_for_git, "git_history", None


def _query_projection_evidence(
    workspace_root: Path,
    handoff_root: Path,
    task_ref: str,
    *,
    prefer_archive: bool = False,
    archive_envelope: dict[str, Any] | None = None,
) -> tuple[Evidence, str, str | None]:
    """Read evidence from live handoff rows, falling back to archive snapshot."""

    live_evidence = Evidence()
    live_projection = "pending"
    live_warning: str | None = None
    if not prefer_archive:
        live_evidence, live_projection, live_warning = _query_handoff_evidence(
            workspace_root, task_ref
        )
        if live_projection == "synced":
            return live_evidence, live_projection, live_warning

    if archive_envelope is None:
        archive_envelope = _fetch_archive_envelope(handoff_root, task_ref)
    if archive_envelope is not None:
        snapshot = (archive_envelope.get("data") or {}).get("snapshot")
        if isinstance(snapshot, dict):
            search_payload, tests_payload = _handoff_payloads_from_snapshot(snapshot)
            archive_evidence = build_evidence_from_handoff_payload(
                search_payload, tests_payload
            )
            return archive_evidence, "archived_snapshot", live_warning

    if not prefer_archive:
        return live_evidence, live_projection, live_warning
    return Evidence(), "pending", live_warning or "archive_evidence_unavailable"


def _collect_orphaned_evidence(
    parsed: ParsedPlan, evidence: Evidence
) -> list[dict[str, Any]]:
    """Report stored evidence that no longer maps to a checklist box in the plan."""

    referenced_slices = {
        item.slice_number
        for item in parsed.items
        if item.section_class == SECTION_SLICE and item.slice_number is not None
    }
    referenced_attestations: set[str] = set()
    referenced_decision_ids: set[str] = set()
    for item in parsed.items:
        referenced_attestations.update(item.anchors.attestations)
        referenced_decision_ids.update(item.anchors.decision_ids)

    orphaned: list[dict[str, Any]] = []
    for slice_n, decision_id in evidence.slice_close_decision_ids.items():
        if slice_n not in referenced_slices:
            orphaned.append(
                {
                    "kind": "slice_close",
                    "slice_number": slice_n,
                    "decision_id": decision_id,
                }
            )
    for criterion in sorted(evidence.attestation_criteria):
        if criterion not in referenced_attestations:
            orphaned.append({"kind": "attestation", "criterion": criterion})

    slice_decision_ids = set(evidence.slice_close_decision_ids.values())
    for decision_id in sorted(evidence.all_decision_ids):
        if decision_id.startswith("attestation:"):
            continue
        if decision_id in referenced_decision_ids:
            continue
        if decision_id in slice_decision_ids:
            slice_for = next(
                (
                    slice_n
                    for slice_n, did in evidence.slice_close_decision_ids.items()
                    if did == decision_id
                ),
                None,
            )
            if slice_for is not None and slice_for in referenced_slices:
                continue
        orphaned.append({"kind": "decision", "decision_id": decision_id})
    return orphaned



def run(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        prog="lifecycle sync-task-plan-checklist", add_help=True
    )
    parser.add_argument(
        "--task", dest="task_ref", required=True,
        help="Task ref whose handoff evidence drives the sync.",
    )
    parser.add_argument(
        "--plan", dest="plan_path", required=False, default=None,
        help=(
            "Absolute or repo-relative path to the task plan markdown "
            "file. When omitted, the handler resolves the task's stored "
            "``task_plan_path`` from handoff state (set via "
            "``set_handoff_state(task_plan_path=...)``)."
        ),
    )
    parser.add_argument(
        "--apply", dest="apply_changes", action="store_true", default=False,
        help="Rewrite the plan in place. Without this flag the run is dry-run.",
    )
    parser.add_argument(
        "--quiet", dest="quiet", action="store_true", default=False,
        help="Suppress stderr human-readable summary; JSON receipt still emits.",
    )
    args = parser.parse_args(argv)

    workspace_root = resolve_workspace_root(
        Path(args.plan_path) if args.plan_path else None
    )
    handoff_root = resolve_handoff_workspace_root(workspace_root)

    plan_source = "flag" if args.plan_path else "handoff_state"
    resolved_plan: str | None = args.plan_path
    if resolved_plan is None:
        resolved_plan = _lookup_stored_plan_path(workspace_root, args.task_ref)
        if resolved_plan is None:
            receipt = {
                "ok": False,
                "command": "sync-task-plan-checklist",
                "task_ref": args.task_ref,
                "plan_path": None,
                "error": "plan_unresolved",
                "ticked": 0,
                "kept": 0,
                "unresolved": 0,
                "already_ticked": 0,
            }
            _common.emit(receipt)
            return 2

    plan_path = Path(resolved_plan)
    if not plan_path.is_absolute():
        plan_path = workspace_root / plan_path
    if not plan_path.is_file():
        receipt = {
            "ok": False,
            "command": "sync-task-plan-checklist",
            "task_ref": args.task_ref,
            "plan_path": str(plan_path),
            "plan_source": plan_source,
            "error": "plan_not_found",
            "ticked": 0,
            "kept": 0,
            "unresolved": 0,
            "already_ticked": 0,
        }
        _write_sync_sidecar(
            _resolve_state_dir(handoff_root),
            args.task_ref,
            {
                "ok": False,
                "ticked": 0,
                "kept": 0,
                "unresolved": 0,
                "warning": f"plan_not_found: {plan_path}",
                "plan_path": str(plan_path),
                "recorded_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            },
        )
        _common.emit(receipt)
        return 2

    text = plan_path.read_text(encoding="utf-8")
    parsed = parse(text)

    evidence, projection, warning = _query_handoff_evidence(
        workspace_root, args.task_ref
    )

    resolutions = resolve(parsed, evidence)
    rewritten = apply(text, resolutions)
    counts = _classify_counts(resolutions)
    diff_entries = _diff_preview(text, rewritten, resolutions)

    wrote = False
    if args.apply_changes and rewritten != text:
        plan_path.write_text(rewritten, encoding="utf-8")
        wrote = True

    receipt: dict[str, Any] = {
        "ok": True,
        "command": "sync-task-plan-checklist",
        "task_ref": args.task_ref,
        "plan_path": str(plan_path),
        "plan_source": plan_source,
        "handoff_projection": projection,
        "applied": wrote,
        "dry_run": not args.apply_changes,
        **counts,
        "diff": diff_entries,
    }
    if warning:
        receipt["warning"] = warning

    sidecar_entry: dict[str, Any] = {
        "ok": warning is None,
        "ticked": counts["ticked"],
        "kept": counts["kept"],
        "unresolved": counts["unresolved"],
        "warning": warning,
        "plan_path": str(plan_path),
        "recorded_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    _write_sync_sidecar(
        _resolve_state_dir(handoff_root), args.task_ref, sidecar_entry
    )

    if not args.quiet:
        prefix = "applied" if wrote else "dry-run"
        sys.stderr.write(
            f"sync-task-plan-checklist: {prefix} task={args.task_ref} "
            f"ticked={counts['ticked']} kept={counts['kept']} "
            f"unresolved={counts['unresolved']} "
            f"already_ticked={counts['already_ticked']}\n"
        )
        for entry in diff_entries:
            sys.stderr.write(f"  L{entry['line']}: {entry['reason']}\n")

    _common.emit(receipt)
    return 0



def probe_unattested_attestations(
    repo: Path, task_ref: str
) -> tuple[list[str], str | None]:
    """Return missing attestation criteria for acceptance-section boxes."""

    # Use the caller-provided repo root rather than re-deriving from CWD. The
    # `repo` argument was silently discarded, so a caller whose CWD differed
    # from the resolved repo (e.g. a linked-worktree finalize/review-ready
    # invocation) probed the wrong workspace and saw a false-empty missing-list.
    workspace_root = repo
    stored = _lookup_stored_plan_path(workspace_root, task_ref)
    if not stored:
        return [], None
    plan_path = Path(stored)
    if not plan_path.is_absolute():
        plan_path = workspace_root / plan_path
    if not plan_path.is_file():
        return [], str(plan_path)
    try:
        parsed = parse(plan_path.read_text(encoding="utf-8"))
        evidence, _, _ = _query_handoff_evidence(workspace_root, task_ref)
    except Exception:
        return [], str(plan_path)
    missing: list[str] = []
    for item in parsed.items:
        if item.already_ticked:
            continue
        if item.section_class not in (SECTION_CONTEXT, SECTION_REVIEW, SECTION_SUCCESS):
            continue
        for criterion in item.anchors.attestations:
            if criterion not in evidence.attestation_criteria:
                missing.append(criterion)
    return sorted(dict.fromkeys(missing)), str(plan_path)

def project_plan_checklist(
    repo: Path,
    task_ref: str,
    *,
    plan_path: Path | None = None,
) -> dict[str, Any]:
    """Read-only checklist projection: join plan boxes with handoff evidence."""

    workspace_root = resolve_workspace_root(plan_path)
    handoff_root = resolve_handoff_workspace_root(workspace_root)
    text_body, plan_display, plan_source, plan_warning = _resolve_projection_plan(
        workspace_root, handoff_root, task_ref, plan_path
    )
    if text_body is None:
        error = plan_warning or "plan_unresolved"
        payload: dict[str, Any] = {
            "ok": False,
            "error": error.split(":", 1)[0] if error else "plan_unresolved",
            "task_ref": task_ref,
        }
        if plan_display:
            payload["plan_path"] = plan_display
        if plan_source:
            payload["plan_source"] = plan_source
        if plan_warning:
            payload["warning"] = plan_warning
        return payload

    archive_envelope = (
        _fetch_archive_envelope(handoff_root, task_ref)
        if plan_source == "git_history"
        else None
    )
    parsed = parse(text_body)
    evidence, projection, warning = _query_projection_evidence(
        workspace_root,
        handoff_root,
        task_ref,
        prefer_archive=plan_source == "git_history",
        archive_envelope=archive_envelope,
    )
    resolutions = resolve(parsed, evidence)
    items: list[dict[str, Any]] = []
    for item in parsed.items:
        resolution = resolutions.get(item.line_index)
        items.append(
            {
                "line": item.line_index + 1,
                "section": item.section_class,
                "slice_number": item.slice_number,
                "doc_ticked": item.already_ticked,
                "projected": resolution.action if resolution else RESOLUTION_KEEP,
                "reason": resolution.reason if resolution else "no_resolution",
                "body_preview": item.body[:120],
            }
        )
    result: dict[str, Any] = {
        "ok": True,
        "task_ref": task_ref,
        "plan_path": plan_display,
        "plan_source": plan_source,
        "handoff_projection": projection,
        "counts": _classify_counts(resolutions),
        "items": items,
        "orphaned_evidence": _collect_orphaned_evidence(parsed, evidence),
    }
    warnings: list[str] = []
    if warning:
        warnings.append(warning)
    if plan_warning:
        warnings.append(plan_warning)
    if warnings:
        result["warning"] = "; ".join(warnings)
    return result
