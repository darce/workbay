"""Antigravity CLI (``agy``) lane configuration (model pin + published slug set).

Single source of truth ([REF-45]) for the ``agy-remote`` backend's binary path,
model pin, and empirically verified ``agy models`` slug allowlist (v1.5.16).

Like ``cursor-agent`` and ``muse``, ``agy`` has no ``--max-turns`` flag, so an
``agy-remote`` lane is bounded by wall-clock timeout only
(``BOUND_ADAPTER_TIMEOUT``) rather than claiming an unenforced turn cap.
"""

from __future__ import annotations

import os

from workbay_protocol import reasoning_effort as model_routing

AGY_REMOTE_BIN = ".local/bin/agy"
AGY_LIST_MODELS_ARGV: tuple[str, ...] = ("agy", "models")
WORKBAY_AGY_MODEL_ENV = "WORKBAY_AGY_MODEL"
TRACKED_AGY_MODEL = model_routing.MODEL_ROUTING_DEFAULTS["agy"]

# Subset of the agy --effort vocabulary admitted by the shared operator-effort grammar.
AGY_EFFORTS = frozenset({"low", "medium", "high", "xhigh", "max"})

# Published model slugs verified against live `agy models` (v1.5.16).
AGY_REMOTE_ALLOWED_MODELS: frozenset[str] = frozenset(
    {
        "gemini-4-neon-low",
        "gemini-4-neon-medium",
        "gemini-4-neon-high",
        "gemini-4-neon-xhigh",
        "gemini-4-carbon-low",
        "gemini-4-carbon-medium",
        "gemini-4-carbon-high",
        "gemini-4-argon-low",
        "gemini-4-argon-medium",
        "gemini-4-argon-high",
        "gemini-4-hydrogen-low",
        "gemini-4-hydrogen-medium",
        "gemini-4-hydrogen-high",
        "gemini-4-hydrogen-xhigh",
        "gemini-pro-orchestrator-neon",
        "gemini-pro-orchestrator",
        "gemini-pro-orchestrator-argon",
        "gemini-3.8-flash-low",
        "gemini-3.8-flash-medium",
        "gemini-3.8-flash-high",
        "claude-opus-5-5-low",
        "claude-opus-5-5-medium",
        "claude-opus-5-5-high",
        "claude-sonnet-5-5-low",
        "claude-sonnet-5-5-medium",
        "claude-sonnet-5-5-high",
        "gpt-oss-120b-medium",
    }
)


def resolve_tracked_or_env_agy_model() -> str:
    """Return WORKBAY_AGY_MODEL if set and non-empty, else the tracked pin."""
    raw = (os.environ.get(WORKBAY_AGY_MODEL_ENV) or "").strip()
    return raw or TRACKED_AGY_MODEL


DEFAULT_AGY_MODEL = resolve_tracked_or_env_agy_model()
