"""Install-time embeddings consent (internal / S3)."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

from workbay_bootstrap.embedding_provision import (
    maybe_provision_embeddings,
    set_embeddings_gate,
)

EMBEDDINGS_CONSENT_PROMPT = "Enable semantic embeddings? [Y/n] "


def _interpret_consent_answer(raw: str) -> bool:
    text = raw.strip().lower()
    if text in ("n", "no"):
        return False
    return True


def _default_prompt_fn() -> str:
    return input(EMBEDDINGS_CONSENT_PROMPT)


def resolve_embeddings_consent(
    *,
    interactive: bool,
    no_embeddings: bool,
    prompt_fn: Callable[[], str] = _default_prompt_fn,
) -> bool:
    """Return whether install should provision embeddings and enable the gate."""
    if no_embeddings:
        return False
    if not interactive:
        return True
    return _interpret_consent_answer(prompt_fn())


def apply_install_embeddings_consent(
    worktree_root: Path | str,
    *,
    interactive: bool,
    no_embeddings: bool,
    prompt_fn: Callable[[], str] = _default_prompt_fn,
) -> list[str]:
    """Resolve consent, persist the gate, and provision when consented."""
    consented = resolve_embeddings_consent(
        interactive=interactive,
        no_embeddings=no_embeddings,
        prompt_fn=prompt_fn,
    )
    root = Path(worktree_root)
    if consented:
        set_embeddings_gate(root, enabled=True)
        return maybe_provision_embeddings(root, no_embeddings=False)
    set_embeddings_gate(root, enabled=False)
    return []
