"""Shared helper for validating hook payloads against workbay-protocol.

Hook entrypoints call ``validate_event(payload, expected="PreToolUse")``
on the dict they read from ``json.load(sys.stdin)``. The helper:

- Returns the validated Pydantic model when ``workbay-protocol`` is
  installed and the payload validates.
- Returns ``None`` when ``workbay-protocol`` is not installed (partial
  rollouts) so hooks can fall back to local parsing.
- Logs a single-line warning to stderr when the payload doesn't match
  the expected event type, instead of swallowing the failure silently.

Hooks that need strict behavior can import the model directly and
raise on failure; the helper is the lenient default.

The optional ``log_warning`` flag lets callers route the warning
elsewhere (e.g. a file, syslog) without rewriting the helper.
"""

from __future__ import annotations

import sys
from typing import Any, Callable


def _default_warn(message: str) -> None:
    # Stderr keeps drift visible without breaking the calling tool's
    # stdout contract. Hooks that strictly cannot speak to stderr can
    # pass log_warning=lambda _: None.
    print(f"[hook-protocol] {message}", file=sys.stderr)


def validate_event(
    payload: dict[str, Any],
    *,
    expected: str | None = None,
    log_warning: Callable[[str], None] = _default_warn,
) -> Any:
    """Validate ``payload`` against the matching workbay_protocol event model.

    Args:
        payload: The dict read from hook stdin.
        expected: Optional event name (``"PreToolUse"`` etc.). When
            given, mismatch is reported. When ``None``, the helper
            picks the model from ``payload['hook_event_name']``.
        log_warning: Function called with a one-line message when
            validation fails. Defaults to stderr.

    Returns:
        The validated event model on success, or ``None`` when
        validation failed or the protocol package is not installed.

    Strict mode: when ``WORKBAY_HOOK_PROTOCOL_STRICT=1`` is set in the
    environment, validation failures raise ``SystemExit(2)`` instead
    of returning ``None``. This lets CI/strict developer setups treat
    payload drift as a hard failure without each hook re-implementing
    the gate. The protocol-package-missing case never escalates — a
    hook running without the contract package installed is a build
    issue, not a payload issue.
    """
    strict = is_protocol_validation_strict()

    try:
        from workbay_protocol.hooks import parse_hook_event  # type: ignore[import-not-found]
    except ImportError:
        return None

    name = payload.get("hook_event_name") or expected
    if expected is not None and name != expected:
        log_warning(
            f"hook payload reported hook_event_name={name!r}, expected {expected!r}"
        )
        if strict:
            raise SystemExit(2)
        if name is None:
            payload = {**payload, "hook_event_name": expected}
        else:
            payload = {**payload}

    try:
        return parse_hook_event(payload)
    except Exception as exc:  # noqa: BLE001
        # Compact, single-line — hooks run on every tool call; verbose
        # tracebacks here would flood stderr.
        log_warning(f"event validation failed for {name!r}: {exc!s}")
        if strict:
            raise SystemExit(2)
        return None


def is_protocol_validation_strict() -> bool:
    """Return True when WORKBAY_HOOK_PROTOCOL_STRICT=1 is set.

    Hook scripts can branch on this to escalate from logging-only
    validation to a hard refusal. Off by default so a contract drift
    cannot brick a developer's session.

    Resolves via the shared ``_interp`` alias. The lazy import keeps this
    stdlib-only helper loadable for hooks that import it before any stack
    interpreter is available.
    """
    from _interp import resolve_env_alias

    value = resolve_env_alias("WORKBAY_HOOK_PROTOCOL_STRICT", default="") or ""
    return value.lower() in {"1", "true", "yes"}
