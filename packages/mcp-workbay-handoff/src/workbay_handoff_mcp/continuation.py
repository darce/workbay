"""Typed cross-session continuation packets (internal).

Storage decision (ARCH-08 boring-first)
--------------------------------------
Reuse the existing artifacts sidecar (`artifact_sources` + FTS chunks) via
``record_artifact`` / ``get_artifact`` rather than a new handoff table or a
``HANDOFF_SCHEMA_VERSION`` bump. Collision-free upsert is already keyed by
``(task_ref, lane_id, source_kind, source_label)``; we pin
``source_kind='continuation'`` and compose ``source_label`` from the optional
lane (fixed label when no lane). Packet sections + supersedes lineage live in
the JSON body (and mirrored ``metadata``) so load stays schema'd without new
columns. Lineage is stored as ``prior_packet_id`` inside the body because the
artifacts upsert keeps the same numeric ``source_id`` on update.
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from datetime import UTC, datetime
from typing import Any

from . import artifact_index
from .runtime import get_runtime_config
from .shared_primitives import _envelope, _normalize_optional_text, _resolve_task_ref
from .shared_schema import _get_db_connection

SOURCE_KIND = "continuation"
ARCHIVE_SOURCE_KIND = "continuation_archive"
ARCHIVE_KEEP = 20
DEFAULT_SOURCE_LABEL = "packet"
CONTENT_TYPE = "application/json"
SECTION_KEYS: tuple[str, ...] = (
    "done_do_not_redo",
    "next_actions",
    "verified_anchors",
    "gotchas",
)
# Cap across all section text (UTF-8 bytes) so a single packet stays prompt-cheap.
MAX_SECTIONS_BYTES = 16 * 1024


class _PhaseTimer:
    """Measure non-overlapping wall-clock phases without changing packet data."""

    def __init__(self) -> None:
        self._last = time.perf_counter_ns()
        self._durations: dict[str, int] = {}

    def mark(self, name: str) -> None:
        now = time.perf_counter_ns()
        self._durations[name] = now - self._last
        self._last = now

    def snapshot(self) -> dict[str, object]:
        # The session transport budget is checked against serialized bytes.
        # Keep measurement digits from changing its size between equivalent
        # requests while retaining the actual numeric nanosecond values.
        padding = max(0, 20 * len(self._durations) - sum(len(str(v)) for v in self._durations.values()))
        return {**self._durations, "serialization_padding": " " * padding}


def continuation_source_label(lane_id: str | None) -> str:
    """Compose the artifacts source_label for a continuation packet key."""
    normalized = _normalize_optional_text(lane_id)
    if normalized is None:
        return DEFAULT_SOURCE_LABEL
    return f"lane:{normalized}"


def _new_packet_id() -> str:
    # Monotonic-ish: UTC timestamp prefix + short random suffix.
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    return f"cont-{stamp}-{uuid.uuid4().hex[:8]}"


def _normalize_sections(
    *,
    done_do_not_redo: str | None,
    next_actions: str | None,
    verified_anchors: str | None,
    gotchas: str | None,
) -> dict[str, str] | dict[str, object]:
    """Return non-empty section map or an error envelope payload."""
    raw = {
        "done_do_not_redo": done_do_not_redo,
        "next_actions": next_actions,
        "verified_anchors": verified_anchors,
        "gotchas": gotchas,
    }
    sections: dict[str, str] = {}
    total_bytes = 0
    for key in SECTION_KEYS:
        value = raw[key]
        if value is None:
            continue
        if not isinstance(value, str):
            return {
                "error": f"section {key!r} must be a string.",
                "code": "invalid_section_type",
            }
        text = value.strip()
        if not text:
            continue
        total_bytes += len(text.encode("utf-8"))
        sections[key] = text
    if not sections:
        return {
            "error": (f"At least one continuation section is required ({', '.join(SECTION_KEYS)})."),
            "code": "missing_sections",
        }
    if total_bytes > MAX_SECTIONS_BYTES:
        return {
            "error": (f"Continuation sections total {total_bytes} bytes; max allowed is {MAX_SECTIONS_BYTES} bytes."),
            "code": "sections_too_large",
            "total_bytes": total_bytes,
            "max_bytes": MAX_SECTIONS_BYTES,
        }
    return sections


def _packet_from_source(source: dict[str, Any] | None) -> dict[str, Any] | None:
    if source is None:
        return None
    metadata = source.get("metadata")
    if isinstance(metadata, dict) and metadata.get("packet_id"):
        return dict(metadata)
    # Fallback: reassemble JSON body from FTS chunks (title = label.key).
    chunks = source.get("chunks") or []
    if not chunks:
        return None
    reassembled: dict[str, Any] = {}
    for chunk in chunks:
        title = str(chunk.get("title") or "")
        body = chunk.get("body")
        if not isinstance(body, str):
            continue
        key = title.rsplit(".", 1)[-1] if "." in title else title
        try:
            reassembled[key] = json.loads(body)
        except (ValueError, TypeError):
            reassembled[key] = body
    if reassembled.get("packet_id"):
        return reassembled
    if "sections" in reassembled:
        return reassembled
    return None


def _load_existing_packet(
    *,
    task_ref: str,
    lane_id: str | None,
    source_label: str,
) -> tuple[dict[str, Any] | None, int | None]:
    config = get_runtime_config()
    source = artifact_index.get_artifact_source(
        task_ref=task_ref,
        source_label=source_label,
        artifact_db_path=config.artifact_db_path,
    )
    if source is None:
        return None, None
    # Prefer the row that matches our source_kind + lane scoping when labels collide.
    if source.get("source_kind") and source.get("source_kind") != SOURCE_KIND:
        return None, None
    packet = _packet_from_source(dict(source))
    source_id = source.get("id")
    return packet, int(source_id) if source_id is not None else None


def _load_packet_by_id(packet_id: str, task_ref: str | None = None) -> tuple[dict[str, Any] | None, bool]:
    config = get_runtime_config()
    archive = None
    if task_ref is not None:
        archive = artifact_index.get_artifact_source(
            task_ref=task_ref,
            source_label=f"archive:{packet_id}",
            artifact_db_path=config.artifact_db_path,
        )
    if archive is None or archive.get("source_kind") != ARCHIVE_SOURCE_KIND:
        archive = artifact_index.find_artifact_source(
            source_kind=ARCHIVE_SOURCE_KIND,
            source_label=f"archive:{packet_id}",
            artifact_db_path=config.artifact_db_path,
        )
    if archive is not None and archive.get("source_kind") == ARCHIVE_SOURCE_KIND:
        packet = _packet_from_source(dict(archive))
        if packet and packet.get("packet_id") == packet_id:
            packet["source_id"] = int(archive["id"])
            return packet, True

    offset = 0
    while True:
        sources = artifact_index.list_artifact_sources(
            task_ref=task_ref,
            source_kind=SOURCE_KIND,
            limit=200,
            offset=offset,
            artifact_db_path=config.artifact_db_path,
        )
        for row in sources:
            source = artifact_index.get_artifact_source(
                source_id=int(row["id"]),
                artifact_db_path=config.artifact_db_path,
            )
            packet = _packet_from_source(dict(source) if source else None)
            if packet and packet.get("packet_id") == packet_id:
                packet["source_id"] = int(row["id"])
                return packet, False
        if len(sources) < 200:
            break
        offset += len(sources)
    return None, False


def save_continuation(
    *,
    task_ref: str | None = None,
    lane_id: str | None = None,
    done_do_not_redo: str | None = None,
    next_actions: str | None = None,
    verified_anchors: str | None = None,
    gotchas: str | None = None,
) -> dict:
    """Upsert the newest continuation packet for ``(task_ref, lane_id)``."""
    timer = _PhaseTimer()

    def finish(result: dict, phase: str) -> dict:
        timer.mark(phase)
        result["data"]["phase_timings_ns"] = timer.snapshot()
        return result

    sections = _normalize_sections(
        done_do_not_redo=done_do_not_redo,
        next_actions=next_actions,
        verified_anchors=verified_anchors,
        gotchas=gotchas,
    )
    if "error" in sections:
        return finish(
            _envelope(ok=False, tool="continuation", data={**sections, "outcome": "refused"}, task_ref=task_ref),
            "validate",
        )
    timer.mark("validate")

    try:
        with _get_db_connection() as conn:
            resolved_task_ref = _resolve_task_ref(conn, task_ref)
    except Exception as exc:  # noqa: BLE001 — surface as write-contract-shaped error
        return finish(
            _envelope(
                ok=False,
                tool="continuation",
                data={"error": str(exc), "code": "task_ref_unresolved", "outcome": "refused"},
                task_ref=task_ref,
            ),
            "resolve_task",
        )
    timer.mark("resolve_task")

    normalized_lane = _normalize_optional_text(lane_id)
    source_label = continuation_source_label(normalized_lane)
    config = get_runtime_config()
    # The prior read, the archive and the head write share one BEGIN IMMEDIATE
    # transaction, so a concurrent saver (thread or process) waits for this commit and
    # then archives this packet rather than the stale prior (CON-05, DATA-18). Failures
    # are caught outside the ``with`` so the archive rolls back with a failed head.
    phase = "open"
    try:
        with artifact_index._immediate_write(config.artifact_db_path) as conn:
            phase = "read"
            head = artifact_index._source_by_key_in(
                conn,
                task_ref=resolved_task_ref,
                lane_id=normalized_lane,
                source_kind=SOURCE_KIND,
                source_label=source_label,
            )
            prior_packet = _packet_from_source(dict(head) if head is not None else None)
            prior_source_id = int(head["id"]) if head is not None else None
            prior_packet_id = prior_packet.get("packet_id") if isinstance(prior_packet, dict) else None
            if prior_packet_id is not None and not isinstance(prior_packet_id, str):
                prior_packet_id = str(prior_packet_id)
            prior_sections = prior_packet.get("sections", {}) if prior_packet else {}
            dropped_sections = sorted(set(prior_sections) - set(sections)) if isinstance(prior_sections, dict) else []

            if prior_packet_id is not None:
                phase = "archive"
                artifact_index._upsert_source_in(
                    conn,
                    task_ref=resolved_task_ref,
                    lane_id=normalized_lane,
                    app_root=None,
                    source_kind=ARCHIVE_SOURCE_KIND,
                    source_label=f"archive:{prior_packet_id}",
                    content_type=CONTENT_TYPE,
                    summary=f"archived continuation packet {prior_packet_id}",
                    content=json.dumps(prior_packet, ensure_ascii=False, separators=(",", ":")),
                    metadata=prior_packet,
                )

            phase = "head"
            packet_id = _new_packet_id()
            saved_at = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            body: dict[str, Any] = {
                "packet_id": packet_id,
                "prior_packet_id": prior_packet_id,
                "task_ref": resolved_task_ref,
                "lane_id": normalized_lane,
                "saved_at": saved_at,
                "sections": sections,
            }
            content = json.dumps(body, ensure_ascii=False, separators=(",", ":"))
            head_result = artifact_index._upsert_source_in(
                conn,
                task_ref=resolved_task_ref,
                lane_id=normalized_lane,
                app_root=None,
                source_kind=SOURCE_KIND,
                source_label=source_label,
                content_type=CONTENT_TYPE,
                summary=f"continuation packet {packet_id}",
                content=content,
                metadata=body,
            )
    except Exception as exc:  # noqa: BLE001 — rolled back; never replace a packet whose archive failed
        code = "continuation_archive_failed" if phase == "archive" else "continuation_write_failed"
        return finish(
            _envelope(
                ok=False,
                tool="continuation",
                data={"error": str(exc) or type(exc).__name__, "code": code, "outcome": "refused"},
                task_ref=resolved_task_ref,
            ),
            "durable_write",
        )
    timer.mark("durable_write")

    retention = "complete"
    try:
        artifact_index.prune_artifact_sources(
            task_ref=resolved_task_ref,
            lane_id=normalized_lane,
            source_kind=ARCHIVE_SOURCE_KIND,
            keep=ARCHIVE_KEEP,
            artifact_db_path=get_runtime_config().artifact_db_path,
        )
    except Exception:  # noqa: BLE001 — the head is committed; retention is best effort
        retention = "failed"
        logging.getLogger(__name__).warning(
            "Continuation archive prune failed for task_ref=%s lane_id=%s packet_id=%s",
            resolved_task_ref,
            normalized_lane,
            packet_id,
            exc_info=True,
        )

    source_id = head_result["source_id"]
    return finish(
        _envelope(
            ok=True,
            tool="continuation",
            data={
                "operation": "save",
                "outcome": "saved",
                "packet_id": packet_id,
                "prior_packet_id": prior_packet_id,
                "source_id": source_id,
                "prior_source_id": prior_source_id,
                "task_ref": resolved_task_ref,
                "lane_id": normalized_lane,
                "source_label": source_label,
                "saved_at": saved_at,
                "sections": sections,
                "superseded": prior_packet_id is not None,
                "dropped_sections": dropped_sections,
                "archive_retention": retention,
            },
            task_ref=resolved_task_ref,
            mutation={"entity": "continuation_packet", "operation": "upsert"},
        ),
        "archive_retention",
    )


def load_continuation(
    *,
    task_ref: str | None = None,
    lane_id: str | None = None,
    packet_id: str | None = None,
) -> dict:
    """Load newest packet for ``(task_ref, lane_id)`` or a specific packet by id.

    Absent packets return ``ok=True`` with ``packet: null`` and a typed
    ``not_found`` outcome. An explicitly supplied id is compared exactly.
    """
    timer = _PhaseTimer()

    def finish(result: dict, phase: str) -> dict:
        timer.mark(phase)
        result["data"]["phase_timings_ns"] = timer.snapshot()
        return result

    resolved_task_ref: str | None = None
    try:
        if task_ref is not None or packet_id is None:
            with _get_db_connection() as conn:
                resolved_task_ref = _resolve_task_ref(conn, task_ref)
    except Exception as exc:  # noqa: BLE001
        # Load by explicit packet_id may still succeed without task resolution.
        if packet_id is None:
            return finish(
                _envelope(
                    ok=False,
                    tool="continuation",
                    data={"error": str(exc), "code": "task_ref_unresolved"},
                    task_ref=task_ref,
                ),
                "resolve_task",
            )
    timer.mark("resolve_task")

    if packet_id is not None:
        try:
            packet, archived = _load_packet_by_id(packet_id, task_ref=resolved_task_ref)
            if packet is None and resolved_task_ref is not None:
                # Preserve the existing global packet-id lookup contract.
                packet, archived = _load_packet_by_id(packet_id, task_ref=None)
        except Exception as exc:  # noqa: BLE001 — storage failure is not an absent packet
            return finish(
                _envelope(
                    ok=False,
                    tool="continuation",
                    data={"error": str(exc) or type(exc).__name__, "code": "continuation_load_failed"},
                    task_ref=resolved_task_ref,
                ),
                "lookup",
            )
        return finish(
            _envelope(
                ok=True,
                tool="continuation",
                data={
                    "operation": "load",
                    "outcome": "found" if packet is not None else "not_found",
                    "packet": packet,
                    "archived": archived,
                },
                task_ref=(packet or {}).get("task_ref") if packet else resolved_task_ref,
            ),
            "lookup",
        )

    if resolved_task_ref is None:
        try:
            with _get_db_connection() as conn:
                resolved_task_ref = _resolve_task_ref(conn, task_ref)
        except Exception as exc:  # noqa: BLE001
            return finish(
                _envelope(
                    ok=False,
                    tool="continuation",
                    data={"error": str(exc), "code": "task_ref_unresolved"},
                    task_ref=task_ref,
                ),
                "lookup",
            )

    normalized_lane = _normalize_optional_text(lane_id)
    source_label = continuation_source_label(normalized_lane)
    try:
        packet, source_id = _load_existing_packet(
            task_ref=resolved_task_ref,
            lane_id=normalized_lane,
            source_label=source_label,
        )
    except Exception as exc:  # noqa: BLE001 — storage failure is not an absent packet
        return finish(
            _envelope(
                ok=False,
                tool="continuation",
                data={"error": str(exc) or type(exc).__name__, "code": "continuation_load_failed"},
                task_ref=resolved_task_ref,
            ),
            "lookup",
        )
    if packet is not None:
        packet = dict(packet)
        packet["source_id"] = source_id
        # Guard: wrong kind should never surface.
        if packet.get("task_ref") and packet["task_ref"] != resolved_task_ref:
            packet = None

    return finish(
        _envelope(
            ok=True,
            tool="continuation",
            data={
                "operation": "load",
                "outcome": "found" if packet is not None else "not_found",
                "packet": packet,
                "archived": False,
            },
            task_ref=resolved_task_ref,
        ),
        "lookup",
    )


def build_session_continuation(
    task_ref: str | None,
    *,
    last_injected_continuation_id: str | None = None,
) -> dict[str, Any] | None:
    """Compact continuation packet for ``load_session`` injection.

    Returns ``None`` when no packet exists (caller leaves the section absent).
    A storage failure returns an unavailable status instead of an empty result.
    When ``last_injected_continuation_id`` matches the newest packet, returns
    only ``{packet_id, deduped: true}`` instead of the full body.
    Compact full shape: ``packet_id``, ``saved_at``, ``lane_id``, ``sections``.
    """
    if not task_ref:
        return None
    envelope = load_continuation(task_ref=task_ref)
    if not envelope.get("ok"):
        data = envelope.get("data") if isinstance(envelope.get("data"), dict) else {}
        return {"status": "unavailable", "code": data.get("code", "continuation_load_failed")}
    data = envelope.get("data") if isinstance(envelope.get("data"), dict) else {}
    packet = data.get("packet")
    if not isinstance(packet, dict) or not packet.get("packet_id"):
        return None

    packet_id = str(packet["packet_id"])
    injected = (last_injected_continuation_id or "").strip()
    if injected and injected == packet_id:
        return {"packet_id": packet_id, "deduped": True}

    sections = packet.get("sections")
    if not isinstance(sections, dict):
        sections = {}
    compact_sections = {
        key: sections[key]
        for key in SECTION_KEYS
        if key in sections and isinstance(sections[key], str) and sections[key].strip()
    }
    return {
        "packet_id": packet_id,
        "saved_at": packet.get("saved_at"),
        "lane_id": packet.get("lane_id"),
        "sections": compact_sections,
    }
