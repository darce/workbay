from __future__ import annotations

from contextvars import ContextVar

from .config import RuntimeConfig

_runtime_config: ContextVar[RuntimeConfig | None] = ContextVar("agent_handoff_runtime_config", default=None)


class RuntimeNotConfiguredError(RuntimeError):
    """Raised when handoff APIs are used before ``configure_runtime()``.

    Typed sentinel so callers can distinguish the hermetic/unconfigured
    path from real RuntimeError failures without substring-matching the
    message (which fails open on unrelated "…not configured…" text and
    flips silently if the prose is reworded).
    """


def configure_runtime(config: RuntimeConfig) -> RuntimeConfig:
    _runtime_config.set(config)
    return config


def reset_runtime_config() -> None:
    _runtime_config.set(None)


def get_runtime_config() -> RuntimeConfig:
    config = _runtime_config.get()
    if config is None:
        raise RuntimeNotConfiguredError("Agent handoff runtime is not configured. Call configure_runtime() first.")
    return config
