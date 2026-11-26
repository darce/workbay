"""Shared CLI JSON output and exit-code contract for WorkBay commands.

This module is strictly layer-clean: it depends only on the Python standard
library and :mod:`workbay_protocol.envelope`, with no references to higher-layer
WorkBay packages.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Callable, Mapping
from enum import IntEnum
from typing import NoReturn

from .envelope import classify_response


class ExitCode(IntEnum):
    """Canonical exit status contract for WorkBay JSON CLI commands."""

    OK = 0
    REFUSED = 1
    USAGE = 2
    POLICY_REFUSED = 3
    UNAVAILABLE = 4
    DEGRADED = 5
    INTERNAL = 6


POLICY_KINDS: frozenset[str] = frozenset(
    {
        "admission_deferred",
        "admission_refused",
        "branch_delete_refused",
        "coordination_declaration_held",
        "custody_held",
        "guard_refused",
        "hold_active",
        "illegal_transition",
        "lane_reclaim_fenced",
        "lane_terminal",
        "native_validation_required",
        "permission_denied",
        "policy_refused",
        "refused_lock_held",
        "refused_unmerged",
        "reserved_engine_field",
        "review_ratchet_refused",
        "status_reserved_for_reaper",
        "unlanded_branch_tip",
        "unlanded_lanes",
    }
)

UNAVAILABLE_KINDS: frozenset[str] = frozenset(
    {
        "branch_tip_unavailable",
        "dependency_unavailable",
        "plugin_load_failed",
        "plugin_unavailable",
        "provider_unavailable",
        "service_unavailable",
        "sidecar_cold",
        "sidecar_disabled",
        "sidecar_unavailable",
        "timeout",
        "unavailable",
    }
)


def _extract_error_kind(payload: Mapping[object, object]) -> str | None:
    """Return a typed error kind from canonical envelope locations, or ``None``.

    Only structured kind fields are inspected; prose fields (``reason``,
    ``message``, ``detail``, or string ``error``) are never parsed.
    """
    error = payload.get("error")
    if isinstance(error, Mapping):
        kind = error.get("kind")
        if isinstance(kind, str) and kind:
            return kind

    data = payload.get("data")
    if isinstance(data, Mapping):
        data_kind = data.get("error_kind")
        if isinstance(data_kind, str) and data_kind:
            return data_kind

    top_kind = payload.get("error_kind")
    if isinstance(top_kind, str) and top_kind:
        return top_kind

    return None


def exit_code_for(payload: object) -> ExitCode:
    """Map a WorkBay response payload to its canonical :class:`ExitCode`.

    Uses :func:`workbay_protocol.envelope.classify_response` as the single
    authority for success / refusal / invalid classification:

    * ``invalid`` -> :attr:`ExitCode.INTERNAL` (6)
    * ``success`` with ``payload["degraded"] is True`` -> :attr:`ExitCode.DEGRADED` (5)
    * ``success`` otherwise -> :attr:`ExitCode.OK` (0)
    * ``refused`` with typed kind in :data:`POLICY_KINDS` -> :attr:`ExitCode.POLICY_REFUSED` (3)
    * ``refused`` with typed kind in :data:`UNAVAILABLE_KINDS` -> :attr:`ExitCode.UNAVAILABLE` (4)
    * ``refused`` otherwise (including unknown kinds) -> :attr:`ExitCode.REFUSED` (1)
    """
    classification = classify_response(payload)
    if classification.status == "invalid":
        return ExitCode.INTERNAL

    if classification.status == "success":
        if isinstance(payload, Mapping) and payload.get("degraded") is True:
            return ExitCode.DEGRADED
        return ExitCode.OK

    if isinstance(payload, Mapping):
        kind = _extract_error_kind(payload)
        if kind == "usage":
            return ExitCode.USAGE
        if kind == "internal":
            return ExitCode.INTERNAL
        if kind in POLICY_KINDS:
            return ExitCode.POLICY_REFUSED
        if kind in UNAVAILABLE_KINDS:
            return ExitCode.UNAVAILABLE

    return ExitCode.REFUSED


def emit_json(payload: Mapping[str, object]) -> None:
    """Write ``payload`` as one compact JSON object on ``stdout``."""
    data = payload if isinstance(payload, dict) else dict(payload)
    sys.stdout.write(json.dumps(data, separators=(",", ":"), ensure_ascii=False) + "\n")
    sys.stdout.flush()


class JsonArgumentParser(argparse.ArgumentParser):
    """ArgumentParser whose parse errors emit a USAGE JSON envelope and exit 2."""

    def error(self, message: str) -> NoReturn:
        emit_json(
            {
                "ok": False,
                "source": self.prog,
                "error": {
                    "kind": "usage",
                    "message": message,
                },
            }
        )
        self.exit(int(ExitCode.USAGE))


def _internal_envelope(*, source: str, exc_type: str) -> dict[str, object]:
    """Build a fail-closed INTERNAL envelope containing only type name and source."""
    return {
        "ok": False,
        "source": source,
        "error": {
            "kind": "internal",
            "type": exc_type,
        },
    }


def run_json_command(fn: Callable[[], Mapping[str, object]], *, source: str) -> int:
    """Execute ``fn``, emit exactly one JSON object on stdout, and return an exit code.

    Any uncaught ``Exception`` is converted into an :attr:`ExitCode.INTERNAL`
    envelope containing only the exception type name and ``source`` — never
    exception arguments, tracebacks, or environment values.
    """
    try:
        payload = fn()
        code = exit_code_for(payload)
        if not isinstance(payload, Mapping):
            emit_json(_internal_envelope(source=source, exc_type=type(payload).__name__))
            return int(ExitCode.INTERNAL)
        emit_json(payload)
        return int(code)
    except Exception as exc:
        emit_json(_internal_envelope(source=source, exc_type=type(exc).__name__))
        return int(ExitCode.INTERNAL)


__all__ = [
    "ExitCode",
    "JsonArgumentParser",
    "POLICY_KINDS",
    "UNAVAILABLE_KINDS",
    "emit_json",
    "exit_code_for",
    "run_json_command",
]
