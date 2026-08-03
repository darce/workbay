from __future__ import annotations

from contextvars import ContextVar

from .config import RuntimeConfig

_runtime_config: ContextVar[RuntimeConfig | None] = ContextVar("agent_handoff_runtime_config", default=None)


def configure_runtime(config: RuntimeConfig) -> RuntimeConfig:
    _runtime_config.set(config)
    return config


def reset_runtime_config() -> None:
    _runtime_config.set(None)


def get_runtime_config() -> RuntimeConfig:
    config = _runtime_config.get()
    if config is None:
        raise RuntimeError("Agent handoff runtime is not configured. Call configure_runtime() first.")
    return config
