"""Bounded host-side semantic context shared by remote prompt entrypoints.

This module stays cheap to import. It loads only the thin sidecar client and
the existing packet selector when semantic context is enabled; model loading
remains owned by the resident handoff embedding sidecar.
"""

from __future__ import annotations

import math
import os
import sqlite3
import subprocess
import threading
import time
from collections.abc import Mapping, Sequence
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import Any

_OFFLOAD_FLAG = "WORKBAY_OFFLOAD_SEMANTIC_REINJECTION"
_TIMEOUT_ENV = "WORKBAY_OFFLOAD_SEMANTIC_TIMEOUT_SECONDS"
_DEFAULT_TIMEOUT_SECONDS = 8.0
_MAX_TIMEOUT_SECONDS = 20.0
_MAX_CONTEXT_CHARS = 1500
_MAX_ANCHORS = 16
_MAX_ANCHOR_CHARS = 1000
_MAX_LABEL_CHARS = 48
_MAX_SNIPPET_CHARS = 500
_MAX_STARTUP_SECONDS = 6.0
_MIN_PACKET_RESERVE_SECONDS = 0.25
_MIN_EMBED_RESERVE_SECONDS = 0.03
_STARTUP_REAP_RESERVE_SECONDS = 2.1
_SQL_PROGRESS_OPS = 1000
_EMBEDDING_ENV_KEYS = (
    "WORKBAY_HANDOFF_EMBEDDING_MODEL",
    "WORKBAY_HANDOFF_EMBEDDING_TOKENIZER",
    "WORKBAY_HANDOFF_EMBEDDING_MODEL_SHA256",
    "WORKBAY_HANDOFF_EMBEDDING_TOKENIZER_SHA256",
    "WORKBAY_REINJECT_SEMANTIC",
    "WORKBAY_HANDOFF_EMBEDDINGS_DISABLED",
)
_ENV_ABSENT = object()
_EMBEDDING_ENV_LOCK = threading.RLock()


class _SemanticFailure(RuntimeError):
    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def _result(
    *,
    status: str,
    skip_reason: str | None = None,
    relevant_lines: list[str] | None = None,
    fallback_scope: str | None = None,
    provenance: list[str] | None = None,
    chars_used: int = 0,
    config_hint: str | None = None,
) -> dict[str, Any]:
    lines = relevant_lines or []
    if status == "selected" and lines:
        display_line = None
    else:
        reason = skip_reason or status or "unavailable"
        marker_kind = "skipped" if status == "skipped" else "unavailable"
        display_line = f"relevant concepts: ({marker_kind}: {reason})"
    return {
        "status": status,
        "skip_reason": skip_reason,
        "relevant_lines": lines,
        "fallback_scope": fallback_scope,
        "provenance": provenance or [],
        "chars_used": chars_used,
        "config_hint": config_hint,
        "display_line": display_line,
    }


def _load_service_components() -> Any:
    """Lazy-load the shared client and existing selector, never a model provider."""
    from workbay_handoff_mcp.embeddings.reinjection import (  # noqa: PLC0415
        ReinjectionConfig,
        build_semantic_reinjection_packet,
    )
    from workbay_handoff_mcp.embeddings.sidecar_client import (  # noqa: PLC0415
        EmbeddingClient,
        SidecarUnavailable,
    )

    return SimpleNamespace(
        EmbeddingClient=EmbeddingClient,
        SidecarUnavailable=SidecarUnavailable,
        ReinjectionConfig=ReinjectionConfig,
        build_semantic_reinjection_packet=build_semantic_reinjection_packet,
    )


def _runtime_config(workspace_root: str | Path, *, deadline: float) -> Any:
    """Resolve canonical state with the request's remaining Git probe budget."""
    from workbay_handoff_mcp.config import RuntimeConfig  # noqa: PLC0415

    if time.monotonic() >= deadline:
        raise _SemanticFailure("semantic_deadline")
    start = Path(workspace_root).expanduser().resolve()
    primary = None
    start_exists = start.exists()
    if time.monotonic() >= deadline:
        raise _SemanticFailure("semantic_deadline")
    if start_exists:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise _SemanticFailure("semantic_deadline")
        clean_env = os.environ.copy()
        for key in ("GIT_DIR", "GIT_COMMON_DIR", "GIT_WORK_TREE"):
            clean_env.pop(key, None)
        try:
            proc = subprocess.run(
                ["git", "-C", str(start), "rev-parse", "--git-common-dir"],
                stdin=subprocess.DEVNULL,
                capture_output=True,
                text=True,
                timeout=remaining,
                env=clean_env,
            )
        except subprocess.TimeoutExpired as exc:
            raise _SemanticFailure("semantic_deadline") from exc
        except FileNotFoundError:
            proc = None
        if time.monotonic() >= deadline:
            raise _SemanticFailure("semantic_deadline")
        if proc is not None and proc.returncode == 0 and proc.stdout.strip():
            common_path = Path(proc.stdout.strip())
            if not common_path.is_absolute():
                common_path = (start / common_path).resolve()
            else:
                common_path = common_path.resolve()
            primary = common_path.parent if common_path.name == ".git" else common_path

    # for_repo's canonical-root behavior is the probe above followed by this
    # constructor. for_workspace retains its explicit state_dir/env precedence.
    runtime = RuntimeConfig.for_workspace(primary or start, git_workspace_root=start)
    if time.monotonic() >= deadline:
        raise _SemanticFailure("semantic_deadline")
    return runtime


@contextmanager
def _canonical_embedding_env(workspace_root: Path, *, deadline: float):
    """Temporarily apply the shared workspace env without leaving process drift."""
    # apply_embedding_env writes os.environ. Serialize calls through this
    # module and restore its documented keys so different workspaces cannot
    # inherit one another's file-only model pins.
    remaining = deadline - time.monotonic()
    if remaining <= 0 or not _EMBEDDING_ENV_LOCK.acquire(timeout=max(0.0, remaining)):
        raise _SemanticFailure("semantic_deadline")
    try:
        if time.monotonic() >= deadline:
            raise _SemanticFailure("semantic_deadline")
        before = {key: os.environ.get(key, _ENV_ABSENT) for key in _EMBEDDING_ENV_KEYS}
        try:
            from workbay_handoff_mcp.embedding_env import apply_embedding_env  # noqa: PLC0415

            apply_embedding_env(workspace_root)
            yield
        finally:
            for key, value in before.items():
                if value is _ENV_ABSENT:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value
    finally:
        _EMBEDDING_ENV_LOCK.release()


def _workspace_semantic_policy(workspace_root: Path) -> tuple[bool, str | None, str | None]:
    """Resolve absent caller flags from the workspace's typed embeddings policy."""
    try:
        from workbay_protocol.embedding_policy import load_embeddings_policy  # noqa: PLC0415

        policy = load_embeddings_policy(workspace_root)
    except Exception:  # noqa: BLE001 - unavailable policy is a typed fail-closed result
        return (
            False,
            "offload_semantic_service_unconfigured",
            f"Set {_OFFLOAD_FLAG}=1 or configure the workspace embeddings policy.",
        )

    if policy.status == "ok":
        if policy.embeddings_policy == "off" or policy.reinjection_policy == "off":
            return False, "offload_semantic_disabled", None
        if policy.embeddings_policy in ("preferred", "required") and policy.reinjection_policy == "on":
            return True, None, None
    return (
        False,
        "offload_semantic_service_unconfigured",
        f"Set {_OFFLOAD_FLAG}=1 or configure the workspace embeddings policy.",
    )


def _enabled_state(workspace_root: Path) -> tuple[bool, str | None, str | None]:
    """Honor an explicit lane switch; otherwise use workspace policy."""
    if os.environ.get("WORKBAY_HANDOFF_EMBEDDINGS_DISABLED", "").strip().lower() in {
        "1",
        "true",
        "yes",
    }:
        return False, "offload_semantic_disabled", None

    raw = os.environ.get(_OFFLOAD_FLAG)
    if raw is not None and raw.strip():
        normalized = raw.strip().lower()
        if normalized in {"0", "false", "no", "off"}:
            return False, "offload_semantic_disabled", None
        if normalized in {"1", "true", "yes", "on"}:
            return True, None, None
        return (
            False,
            "offload_semantic_service_unconfigured",
            f"Use {_OFFLOAD_FLAG}=0 or {_OFFLOAD_FLAG}=1; invalid value {raw!r} was ignored.",
        )

    legacy_reinjection = os.environ.get("WORKBAY_REINJECT_SEMANTIC")
    if legacy_reinjection is not None and legacy_reinjection.strip():
        if legacy_reinjection.strip().lower() in {"0", "false", "no", "off"}:
            return False, "offload_semantic_disabled", None
        if legacy_reinjection.strip().lower() in {"1", "true", "yes", "on"}:
            return True, None, None
    return _workspace_semantic_policy(workspace_root)


def _timeout_seconds(override: float | None) -> float:
    raw: object = override if override is not None else os.environ.get(_TIMEOUT_ENV)
    try:
        value = float(raw) if raw is not None else _DEFAULT_TIMEOUT_SECONDS
    except (TypeError, ValueError):
        return _DEFAULT_TIMEOUT_SECONDS
    if not math.isfinite(value) or value <= 0:
        return _DEFAULT_TIMEOUT_SECONDS
    return min(value, _MAX_TIMEOUT_SECONDS)


def _clean_strings(values: Sequence[Any] | None, *, max_items: int, max_chars: int) -> list[str]:
    if not values:
        return []
    if isinstance(values, (str, Mapping)):
        values = [values]
    cleaned: list[str] = []
    for raw in values:
        if isinstance(raw, Mapping):
            raw = raw.get("path") or raw.get("file_path")
        if not isinstance(raw, str):
            continue
        text = raw.strip()
        if text:
            cleaned.append(text[:max_chars])
        if len(cleaned) >= max_items:
            break
    return cleaned


def declared_file_anchors(activity: Mapping[str, Any]) -> list[str]:
    """Return only explicit lane file declarations and reported changed paths."""
    lane = activity.get("lane")
    lane_row = lane if isinstance(lane, Mapping) else {}
    paths: list[str] = []
    for key in ("declared_file_anchors", "file_anchors", "context_targets", "changed_files"):
        paths.extend(_clean_strings(lane_row.get(key), max_items=_MAX_ANCHORS, max_chars=_MAX_ANCHOR_CHARS))
        paths.extend(_clean_strings(activity.get(key), max_items=_MAX_ANCHORS, max_chars=_MAX_ANCHOR_CHARS))

    reports = activity.get("reports")
    if isinstance(reports, list):
        for report in reports:
            if not isinstance(report, Mapping):
                continue
            for key in ("changed_files", "changed_files_json"):
                changed = report.get(key)
                if isinstance(changed, str) and key.endswith("_json"):
                    try:
                        import json  # noqa: PLC0415

                        changed = json.loads(changed)
                    except (TypeError, ValueError):
                        changed = []
                paths.extend(_clean_strings(changed, max_items=_MAX_ANCHORS, max_chars=_MAX_ANCHOR_CHARS))
            if len(paths) >= _MAX_ANCHORS:
                break

    findings = activity.get("findings")
    if isinstance(findings, list):
        paths.extend(
            _clean_strings(
                [finding.get("file_path") for finding in findings if isinstance(finding, Mapping)],
                max_items=_MAX_ANCHORS,
                max_chars=_MAX_ANCHOR_CHARS,
            )
        )

    # Preserve declaration order while avoiding duplicate anchor weight.
    return list(dict.fromkeys(paths))[:_MAX_ANCHORS]


def _visible_texts(
    *,
    task_ref: str,
    lane_id: str | None,
    objective: str,
    declared_file_anchors: Sequence[str] | None,
    anchor_texts: Sequence[str] | None,
) -> list[str]:
    texts = [f"Task identity: {task_ref[:_MAX_ANCHOR_CHARS]}"]
    if lane_id and lane_id.strip():
        texts.append(f"Lane identity: {lane_id.strip()[:_MAX_ANCHOR_CHARS]}")
    if objective and objective.strip():
        texts.append(f"Objective: {objective.strip()[:_MAX_ANCHOR_CHARS]}")
    for path in _clean_strings(declared_file_anchors, max_items=_MAX_ANCHORS, max_chars=_MAX_ANCHOR_CHARS):
        texts.append(f"Declared file: {path}")
    for text in _clean_strings(anchor_texts, max_items=_MAX_ANCHORS, max_chars=_MAX_ANCHOR_CHARS):
        texts.append(text)
    return texts[:_MAX_ANCHORS]


def _open_canonical_db(runtime: Any, *, deadline: float) -> sqlite3.Connection:
    """Open the one canonical handoff DB read-only with deadline-aware waits."""
    db_path = Path(runtime.db_path).expanduser().resolve()
    if not db_path.is_file():
        raise _SemanticFailure("offload_semantic_service_unconfigured")
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise _SemanticFailure("semantic_deadline")
    uri = f"{db_path.as_uri()}?mode=ro"
    try:
        conn = sqlite3.connect(uri, uri=True, timeout=max(0.01, remaining))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only=ON")
        conn.set_progress_handler(lambda: int(time.monotonic() >= deadline), _SQL_PROGRESS_OPS)
        return conn
    except sqlite3.Error as exc:
        raise _SemanticFailure("semantic_database_unavailable") from exc


class _DeadlineEmbeddingAdapter:
    """Adapt EmbeddingClient.embed(texts, deadline_seconds=...) to SupportsEmbed.

    Every call shares the packet's one absolute deadline. The only model owner
    is the resident sidecar; no abandoned local worker thread is created.
    """

    def __init__(self, client: Any, *, deadline: float, unavailable_type: type[BaseException]) -> None:
        self._client = client
        self._deadline = deadline
        self._unavailable_type = unavailable_type
        self.dim = int(client.dim)
        self.model_id = str(client.model_id)

    def embed(self, texts: list[str]):
        import numpy as np  # noqa: PLC0415

        remaining = self._deadline - time.monotonic()
        request_budget = remaining - _MIN_EMBED_RESERVE_SECONDS
        if request_budget <= 0:
            raise _SemanticFailure("semantic_deadline")
        try:
            vectors = self._client.embed(texts, deadline_seconds=request_budget)
        except Exception as exc:  # noqa: BLE001 - map adapter faults to typed degradation
            raise _SemanticFailure("semantic_embedding_unavailable") from exc
        if isinstance(vectors, self._unavailable_type):
            reason = str(getattr(vectors, "reason", "unavailable"))
            raise _SemanticFailure(f"semantic_embedding_{reason}")
        if time.monotonic() >= self._deadline:
            raise _SemanticFailure("semantic_deadline")
        try:
            array = np.asarray(vectors, dtype="<f4")
        except (TypeError, ValueError) as exc:
            raise _SemanticFailure("semantic_embedding_invalid_response") from exc
        if array.shape != (len(texts), self.dim) or not np.isfinite(array).all():
            raise _SemanticFailure("semantic_embedding_invalid_response")
        return array


def _ensure_client_ready(client: Any, *, deadline: float, unavailable_type: type[BaseException]) -> None:
    """Bound synchronous sidecar cold start inside the packet's remaining time."""
    remaining = deadline - time.monotonic()
    ping = getattr(client, "ping", None)
    if callable(ping):
        try:
            ready = ping(timeout_seconds=min(0.25, max(0.01, remaining)))
        except Exception:  # noqa: BLE001 - startup path below returns a typed result
            ready = False
        if ready is True:
            client.ensure_started = lambda: unavailable_type("service_unavailable_after_start")
            return
        remaining = deadline - time.monotonic()
    startup_budget = remaining - _MIN_PACKET_RESERVE_SECONDS - _STARTUP_REAP_RESERVE_SECONDS - 0.25
    # EmbeddingClient.ensure_started has its own bounded process lifecycle and
    # terminates an unsuccessful cold start. Reserve time for the full packet;
    # RES-02/RES-13: every blocking wait has a timeout and typed degrade path.
    if startup_budget <= 0:
        raise _SemanticFailure("semantic_deadline")
    client.load_timeout_seconds = min(_MAX_STARTUP_SECONDS, startup_budget)
    try:
        unavailable = client.ensure_started()
    except Exception as exc:  # noqa: BLE001 - startup errors are typed below
        raise _SemanticFailure("semantic_embedding_startup_failed") from exc
    if unavailable is not None:
        reason = str(getattr(unavailable, "reason", "startup_failed"))
        raise _SemanticFailure(f"semantic_embedding_{reason}")
    if time.monotonic() >= deadline:
        raise _SemanticFailure("semantic_deadline")
    # If the ready socket disappears, prevent the client's automatic retry
    # from starting a second cold load outside this call's budget.
    client.ensure_started = lambda: unavailable_type("service_unavailable_after_start")


def _packet_lines(packet: Any, *, max_chars: int) -> tuple[list[str], list[str], str | None]:
    if isinstance(packet, Mapping):
        status = packet.get("status")
        skip_reason = packet.get("skip_reason")
        selected = packet.get("selected")
        fallback_scope = packet.get("fallback_scope")
    else:
        status = getattr(packet, "status", None)
        skip_reason = getattr(packet, "skip_reason", None)
        selected = getattr(packet, "selected", None)
        fallback_scope = getattr(packet, "fallback_scope", None)
    if status not in {"selected", "skipped", "degraded"} or not isinstance(selected, list):
        raise _SemanticFailure("semantic_packet_malformed")
    if status != "selected":
        reason = str(skip_reason or status)
        return [], [], reason
    if not selected:
        raise _SemanticFailure("semantic_packet_malformed")
    if fallback_scope:
        # GRPH-09/GRPH-31: a packet must preserve the explicit task identity;
        # related-task fallback would blur provenance in a lane prompt.
        raise _SemanticFailure("semantic_related_task_fallback_suppressed")

    lines: list[str] = []
    sources: list[str] = []
    used = 0
    for item in selected:
        if isinstance(item, Mapping):
            kind, entity_id, label, snippet = (item.get(k) for k in ("kind", "id", "label", "snippet"))
        else:
            kind, entity_id, label, snippet = (
                getattr(item, "kind", None),
                getattr(item, "id", None),
                getattr(item, "label", None),
                getattr(item, "snippet", None),
            )
        if not all(isinstance(value, str) and value.strip() for value in (kind, entity_id, label)):
            raise _SemanticFailure("semantic_packet_malformed")
        source = f"{kind.strip()}:{entity_id.strip()}"
        suffix = f" [source:{source}]"
        remaining = max_chars - used - (1 if lines else 0)
        fixed_chars = len("- ") + len(": ") + len(suffix)
        label_chars = min(_MAX_LABEL_CHARS, max(0, remaining - fixed_chars))
        if label_chars <= 0:
            break
        label_text = label.strip()[:label_chars]
        snippet_text = str(snippet or "").strip()[:_MAX_SNIPPET_CHARS]
        prefix = f"- {label_text}: "
        snippet_room = remaining - len(prefix) - len(suffix)
        if snippet_room < 0:
            break
        line = f"{prefix}{snippet_text[:snippet_room]}{suffix}"
        if len(line) > remaining:
            raise _SemanticFailure("semantic_packet_malformed")
        used += len(line) + (1 if lines else 0)
        lines.append(line)
        sources.append(source)
    if not lines:
        return [], [], "semantic_context_budget_too_small"
    return lines, sources, None


def build_lane_semantic_context(
    *,
    task_ref: str,
    workspace_root: str | Path,
    objective: str = "",
    lane_id: str | None = None,
    declared_file_anchors: Sequence[str] | None = None,
    anchor_texts: Sequence[str] | None = None,
    semantic_content_budget_chars: int | None = None,
    timeout_seconds: float | None = None,
) -> dict[str, Any]:
    """Build one task-scoped, rendered semantic block from the resident service.

    The service uses RuntimeConfig.for_repo's primary-worktree resolution and
    the canonical task DB (RES-05/RES-07). Only rendered lines and compact
    source identifiers leave this function. GRPH-09/GRPH-31 keep selection
    tied to the explicit task identity.
    """
    resolved_ref = str(task_ref or "").strip()
    if not resolved_ref:
        return _result(status="unavailable", skip_reason="semantic_task_identity_missing")

    raw_flag = os.environ.get(_OFFLOAD_FLAG)
    if raw_flag is not None and raw_flag.strip().lower() in {"0", "false", "no", "off"}:
        return _result(status="skipped", skip_reason="offload_semantic_disabled")

    started = time.monotonic()
    deadline = started + _timeout_seconds(timeout_seconds)
    try:
        runtime = _runtime_config(workspace_root, deadline=deadline)
    except _SemanticFailure as exc:
        return _result(status="unavailable", skip_reason=exc.reason)
    except Exception:  # noqa: BLE001 - config resolution is a typed degrade
        return _result(status="unavailable", skip_reason="semantic_workspace_unavailable")

    try:
        with _canonical_embedding_env(Path(runtime.workspace_root), deadline=deadline):
            enabled, policy_reason, config_hint = _enabled_state(Path(runtime.workspace_root))
            if not enabled:
                status = "skipped" if policy_reason == "offload_semantic_disabled" else "unavailable"
                return _result(
                    status=status,
                    skip_reason=policy_reason or "offload_semantic_service_unconfigured",
                    config_hint=config_hint,
                )
    except _SemanticFailure as exc:
        return _result(status="unavailable", skip_reason=exc.reason)

    try:
        conn = _open_canonical_db(runtime, deadline=deadline)
    except _SemanticFailure as exc:
        return _result(
            status="unavailable",
            skip_reason=exc.reason,
            config_hint=(
                f"Set {_OFFLOAD_FLAG}=1 or configure the workspace embeddings policy."
                if exc.reason == "offload_semantic_service_unconfigured"
                else None
            ),
        )

    try:
        remaining = deadline - time.monotonic()
        conn.execute(f"PRAGMA busy_timeout={max(1, int(max(0.0, remaining) * 1000))}")
        try:
            task_row = conn.execute("SELECT task_ref FROM handoff_state WHERE task_ref = ?", (resolved_ref,)).fetchone()
        except sqlite3.Error as exc:
            raise _SemanticFailure("semantic_task_state_malformed") from exc
        if task_row is None:
            raise _SemanticFailure("semantic_task_not_found")

        if time.monotonic() >= deadline:
            raise _SemanticFailure("semantic_deadline")
        components = _load_service_components()
        remaining = deadline - time.monotonic()
        if remaining <= _MIN_PACKET_RESERVE_SECONDS:
            raise _SemanticFailure("semantic_deadline")
        startup_cap = min(_MAX_STARTUP_SECONDS, remaining - _MIN_PACKET_RESERVE_SECONDS)
        with _canonical_embedding_env(Path(runtime.workspace_root), deadline=deadline):
            client = components.EmbeddingClient(
                state_dir=runtime.state_dir,
                load_timeout_seconds=startup_cap,
            )
            _ensure_client_ready(client, deadline=deadline, unavailable_type=components.SidecarUnavailable)
        provider = _DeadlineEmbeddingAdapter(
            client,
            deadline=deadline,
            unavailable_type=components.SidecarUnavailable,
        )

        config = components.ReinjectionConfig.from_env()
        requested_budget = (
            config.refresh_budget_chars if semantic_content_budget_chars is None else semantic_content_budget_chars
        )
        try:
            content_budget = max(0, min(_MAX_CONTEXT_CHARS, int(requested_budget)))
        except (TypeError, ValueError, OverflowError):
            content_budget = min(_MAX_CONTEXT_CHARS, int(config.refresh_budget_chars))
        texts = _visible_texts(
            task_ref=resolved_ref,
            lane_id=lane_id,
            objective=objective,
            declared_file_anchors=declared_file_anchors,
            anchor_texts=anchor_texts,
        )
        if not texts:
            raise _SemanticFailure("semantic_anchor_unavailable")
        packet = components.build_semantic_reinjection_packet(
            conn,
            task_ref=resolved_ref,
            provider=provider,
            persisted_anchor=None,
            visible_texts=texts,
            semantic_content_budget_chars=content_budget,
            config=config,
        )
        if time.monotonic() >= deadline:
            raise _SemanticFailure("semantic_deadline")
        lines, provenance, reason = _packet_lines(packet, max_chars=content_budget)
        if time.monotonic() >= deadline:
            raise _SemanticFailure("semantic_deadline")
        if reason is not None:
            status = (
                "skipped"
                if reason in {"no_embeddings", "anchor_unavailable", "semantic_context_budget_too_small"}
                else "unavailable"
            )
            return _result(status=status, skip_reason=reason)
        return _result(
            status="selected",
            relevant_lines=lines,
            provenance=provenance,
            chars_used=len("\n".join(lines)),
        )
    except _SemanticFailure as exc:
        return _result(status="unavailable", skip_reason=exc.reason)
    except sqlite3.Error:
        reason = "semantic_deadline" if time.monotonic() >= deadline else "semantic_database_unavailable"
        return _result(status="unavailable", skip_reason=reason)
    except Exception:  # noqa: BLE001 - prompt hydration is fail-open
        return _result(status="unavailable", skip_reason="semantic_service_error")
    finally:
        conn.close()
