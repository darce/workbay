"""Execution adapter for the Cursor CLI (``cursor-agent``) headless turn.

Cursor is the third CLI harness in the fleet, and differs from grok/codex in
three measured ways that shape this adapter:

1. **No structured-output flag.** grok has ``--json-schema`` and codex has
   ``--output-schema``; cursor has neither. The schema is appended to the
   prompt as an instruction and the ``BackendResult`` is recovered from the
   response with the shared :func:`extract_result_payload`, exactly as the
   claude adapter already does. Schema support is not a grounding guarantee
   anyway — a schema-constrained grok run in that same review returned
   perfectly-shaped findings citing files that do not exist (``agent_error``
   #210) — so the prose path costs reliability that was never actually bought.

2. **Binary-name collision.** ``~/.local/bin/agent`` (cursor) and
   ``~/.grok/bin/agent`` (grok) are both named ``agent`` and take incompatible
   flags; whichever wins is decided by PATH order. This adapter resolves
   ``cursor-agent`` by absolute path and refuses a binary named bare ``agent``.

3. **No turn bound.** ``cursor-agent`` has no ``--max-turns``, so the
   cycle is bounded by wall-clock only, enforced here by a process-group kill.
   The capability table and offload profile say so rather than borrowing grok's
   turn+time pair ([FM-08] bound declared honestly, [AGT-10] no silent caps).
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
from pathlib import Path
from typing import Any, Callable

from ..backend_adapter import BackendAdapter, BackendResult
from ..cursor_lane_config import (
    CURSOR_TIMEOUT_CAP,
    DEFAULT_CURSOR_MODEL,
    FORBIDDEN_CURSOR_FLAGS,
    resolve_cursor_model,
)
from ._proc import run_bounded
from ._result_text import extract_result_payload, normalize_cli_usage

_LOGGER = logging.getLogger(__name__)

# cursor-agent takes the prompt as a POSITIONAL argument (there is no
# --prompt-file, unlike grok), so an oversized lane packet blows the kernel's
# argv limits. The binding constraint is Linux MAX_ARG_STRLEN — 128 KiB for a
# SINGLE argument — which is far stricter than macOS's ~1 MiB total ARG_MAX, so
# the cap is sized to the strictest platform rather than the development one.
# This bounds the prompt argument only; the total argv+env block can still
# exceed ARG_MAX independently, which is why oversize is reported with the
# measured size rather than claimed to be impossible.
MAX_PROMPT_BYTES = 120_000


def normalize_cursor_usage(payload: dict[str, Any]) -> dict[str, Any] | None:
    """Normalize cursor's camelCase ``usage`` block into the shared shape.

    Measured from a live turn::

        "usage": {"inputTokens": 15760, "outputTokens": 30,
                  "cacheReadTokens": 128, "cacheWriteTokens": 0}

    The shared :func:`normalize_cli_usage` recognizes only claude-style
    snake_case keys and deliberately returns ``None`` for an unrecognized block
    rather than fabricating an all-zeros breakdown stamped ``observed``. These
    are genuine per-turn API counts, so they are mapped explicitly here instead.

    Surfacing usage does NOT flip ``supports_token_telemetry``: that capability
    gates whether the offload governor hard-errors a zero-token turn, and one
    observed success is not evidence that every turn kind (checkpoint, error,
    interrupted) carries a usage block. Until that is measured across turn
    kinds, governance stays on the wall-clock bound and this is observability
    only.
    """
    usage = payload.get("usage")
    if not isinstance(usage, dict):
        return None
    keys = ("inputTokens", "outputTokens", "cacheReadTokens", "cacheWriteTokens")
    if not any(key in usage for key in keys):
        return None

    def _int(value: Any) -> int | None:
        """Coerce a token count, or None when it is not a usable number.

        Absent (None) reads as 0. A real numeric value — including a JSON float
        — is converted. Anything else (a string, a dict) returns None so the
        caller can decline the whole block: mapping junk to 0 while stamping
        ``usage_source='observed'`` would fabricate telemetry, which is exactly
        what this normalizer exists to avoid.
        """
        if value is None:
            return 0
        if isinstance(value, bool):
            return None
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value)
        return None

    counts = {
        "input": _int(usage.get("inputTokens")),
        "output": _int(usage.get("outputTokens")),
        "cache_read": _int(usage.get("cacheReadTokens")),
        "cache_write": _int(usage.get("cacheWriteTokens")),
    }
    if any(value is None for value in counts.values()):
        return None

    input_tokens = counts["input"]
    output_tokens = counts["output"]
    cached = counts["cache_read"] + counts["cache_write"]

    def _breakdown() -> dict[str, int]:
        # Built twice on purpose: `last` and `total` must not alias the SAME
        # dict, or a consumer mutating one silently mutates the other.
        return {
            "cached_input_tokens": cached,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "reasoning_output_tokens": 0,
            "total_tokens": input_tokens + output_tokens,
        }

    return {
        "last": _breakdown(),
        "total": _breakdown(),
        "model_context_window": None,
        "usage_source": "observed",
    }


def _is_worktree_flag(arg: str) -> bool:
    """True when ``arg`` is one of cursor's worktree flags, in any spelling.

    Covers the attached forms a plain membership test misses: ``--worktree=x``
    (long, ``=``) and ``-wlane-1`` (short, value glued on). A bare-token check
    lets ``-wlane-1`` slip past and hand cursor its own checkout.
    """
    head = arg.split("=", 1)[0]
    if head in FORBIDDEN_CURSOR_FLAGS:
        return True
    # Short flags may carry their value attached: -w<value>.
    return any(
        arg.startswith(flag) and not flag.startswith("--") and len(arg) > len(flag) for flag in FORBIDDEN_CURSOR_FLAGS
    )


def find_cursor_agent(explicit_path: str | None = None) -> str:
    """Resolve the ``cursor-agent`` executable to an ABSOLUTE path.

    Order: explicit override > ``WORKBAY_CURSOR_BIN`` > PATH lookup.

    Two distinct guards, because the grok CLI also installs a binary named
    ``agent`` and the two take incompatible flags:

    * the resolved target's name must not be bare ``agent``; and
    * the result must be ABSOLUTE. A relative override (``WORKBAY_CURSOR_BIN=
      cursor-agent``) passes the name check yet still leaves PATH order to
      decide which binary runs — reintroducing the exact ambiguity the name
      check exists to remove. Checking the configured string alone is not
      enough; the symlink target is what actually executes.
    """
    candidate = explicit_path or os.environ.get("WORKBAY_CURSOR_BIN") or shutil.which("cursor-agent")
    if not candidate:
        raise RuntimeError(
            "cursor-agent CLI not found in PATH. Install the Cursor CLI or set WORKBAY_CURSOR_BIN to its absolute path."
        )
    path = Path(candidate)
    if not path.is_absolute():
        raise RuntimeError(
            f"refusing non-absolute cursor binary {candidate!r}: PATH order would decide whether this "
            "resolves to the Cursor CLI or the grok CLI (both ship an 'agent' entrypoint). Set "
            "WORKBAY_CURSOR_BIN to an absolute path."
        )
    for name in (path.name, Path(os.path.realpath(path)).name):
        if name == "agent":
            raise RuntimeError(
                f"refusing ambiguous cursor binary {candidate!r}: the bare name 'agent' is shared by "
                "the cursor and grok CLIs, which take incompatible flags. Point WORKBAY_CURSOR_BIN at "
                "the 'cursor-agent' entrypoint instead."
            )
    return str(path)


class CursorCliAdapter(BackendAdapter):
    """Execution adapter for the ``cursor-agent`` CLI headless turn."""

    supports_jail = True

    def __init__(
        self,
        cursor_bin: str | None = None,
        cursor_args: list[str] | None = None,
        *,
        timeout: int = CURSOR_TIMEOUT_CAP,
    ):
        # Resolve the binary LAZILY (in execute), not here: an eager resolve
        # raises when cursor-agent is absent, and the daemon constructs the
        # adapter OUTSIDE its EXEC_FAILED try/except, so an unresolved binary
        # would crash the whole worker process instead of logging a failed
        # cycle (s4-a-001). Same reasoning as grok_cli/claude_code.
        self.cursor_bin = cursor_bin
        self.cursor_args = cursor_args or []
        self.timeout = timeout

    def resolve_reasoning_effort(
        self,
        *,
        orchestrator_root: Path,
        task_ref: str,
        lane_id: str,
        requested: str,
        cycle: int,
        prompt_override: str | None,
        previous_run_exhausted: bool = False,
    ) -> tuple[str | None, list[str]]:
        """Resolve reasoning effort via the shared auto-resolver."""
        from .._env import resolve_auto_reasoning_effort  # noqa: PLC0415

        return resolve_auto_reasoning_effort(
            orchestrator_root=orchestrator_root,
            task_ref=task_ref,
            lane_id=lane_id,
            requested=requested,
            cycle=cycle,
            prompt_override=prompt_override,
            previous_run_exhausted=previous_run_exhausted,
        )

    def _resolve_model(self, model: str | None, reasoning_effort: str | None) -> tuple[str, str | None]:
        """Resolve ``--model`` and the effort that slug ACTUALLY encodes.

        Cursor carries reasoning effort in the model id itself; the bracket
        parameterization in ``cursor-agent --help`` is rejected by the live CLI.
        Selection is therefore a lookup in a table of published slugs
        (:data:`CURSOR_EFFORT_SLUGS`) rather than string surgery, so no code path
        can synthesize an id the vendor does not publish.

        Applies to explicitly-pinned models too, because the offload profile
        pins one on every dispatch — skipping pinned models made the mechanism
        dead code exactly where it mattered.

        Returns the effective effort so the caller stamps what the vendor
        actually got, never the unhonored request. A request that cannot be
        honored is logged at WARNING ([AGT-10] degrade loudly).
        """
        slug, effective_effort, downgrade = resolve_cursor_model(model or DEFAULT_CURSOR_MODEL, reasoning_effort)
        if downgrade:
            _LOGGER.warning("cursor-cli effort not applied: %s", downgrade)
        return slug, effective_effort

    def _build_argv(
        self,
        *,
        cursor_bin: str,
        prompt: str,
        worktree_path: Path,
        model_slug: str,
        jail_prefix: list[str],
    ) -> list[str]:
        forbidden = [a for a in self.cursor_args if _is_worktree_flag(a)]
        if forbidden:
            raise RuntimeError(
                f"cursor_args may not contain worktree flags {forbidden}: the lane already owns "
                "exactly one worktree; letting cursor-agent create its own under "
                "~/.cursor/worktrees/ would fork the lane's checkout."
            )
        argv = [
            *jail_prefix,
            cursor_bin,
            "--print",
            "--output-format",
            "json",
            # Pin the agent to the lane worktree. NOT -w/--worktree, which would
            # create a second, vendor-owned checkout.
            "--workspace",
            str(worktree_path),
            # Headless autonomy: run tools without prompting, and trust the
            # workspace (--trust only takes effect with --print).
            "--force",
            "--trust",
            "--model",
            model_slug,
        ]
        sandbox = (os.environ.get("WORKBAY_CURSOR_SANDBOX") or "").strip()
        if sandbox:
            if sandbox not in ("enabled", "disabled"):
                # Silently dropping this would leave a --force --trust agent
                # running UNSANDBOXED while the operator believes otherwise —
                # the failure mode a sandbox knob exists to prevent ([AGT-10]).
                raise RuntimeError(
                    f"WORKBAY_CURSOR_SANDBOX must be 'enabled' or 'disabled', got {sandbox!r}. "
                    "Refusing to dispatch rather than silently running unsandboxed."
                )
            argv.extend(["--sandbox", sandbox])
        argv.extend(self.cursor_args)
        # `--` terminates option parsing so a prompt that begins with '-' (a
        # markdown bullet, a '---' front-matter fence, a diff hunk) is treated as
        # the positional prompt instead of being parsed as flags. Verified
        # against the live CLI. Must be the last thing before the prompt.
        argv.append("--")
        argv.append(prompt)
        return argv

    def execute(
        self,
        prompt: str,
        schema: dict[str, Any],
        worktree_path: Path,
        model: str | None = None,
        reasoning_effort: str | None = None,
        session_mode: str | None = None,
        env: dict[str, str] | None = None,
        progress_callback: Callable[..., None] | None = None,
        **kwargs: Any,
    ) -> BackendResult:
        """Execute one bounded turn via the ``cursor-agent`` CLI."""
        from workbay_handoff_mcp.enums import WorkerEventName  # noqa: PLC0415

        if progress_callback:
            progress_callback(WorkerEventName.EXEC_SPAWNED, backend="cursor-cli")

        cursor_bin = find_cursor_agent(self.cursor_bin)

        # No --json-schema equivalent: the schema rides in the prompt.
        full_prompt = (
            f"{prompt}\n\n"
            f"IMPORTANT: Your final output must be a single JSON object matching this schema:\n"
            f"{json.dumps(schema, indent=2)}\n"
        )
        prompt_bytes = len(full_prompt.encode("utf-8"))
        if prompt_bytes > MAX_PROMPT_BYTES:
            raise RuntimeError(
                f"cursor prompt is {prompt_bytes} bytes, over the {MAX_PROMPT_BYTES}-byte limit. "
                "cursor-agent takes the prompt as a positional argument (no --prompt-file), so an "
                "oversized packet would exceed the kernel's per-argument limit "
                "(MAX_ARG_STRLEN is 128 KiB on Linux). Shrink the lane context packet."
            )

        model_slug, effective_effort = self._resolve_model(model, reasoning_effort)
        argv = self._build_argv(
            cursor_bin=cursor_bin,
            prompt=full_prompt,
            worktree_path=worktree_path,
            model_slug=model_slug,
            jail_prefix=list(kwargs.get("jail_argv_prefix") or []),
        )

        # `env is None` means inherit; an explicitly-passed EMPTY mapping means
        # "run with no environment" and must be honored, which `env or os.environ`
        # silently inverted.
        run_env = dict(os.environ if env is None else env)
        try:
            completed = run_bounded(
                argv,
                env=run_env,
                cwd=str(worktree_path),
                timeout=self.timeout,
            )
        except subprocess.TimeoutExpired:
            # [FM-08]: the declared wall-clock bound is the ONLY bound cursor
            # has (no --max-turns), so a breach is a terminal failure, loudly.
            raise RuntimeError(
                f"cursor-agent exceeded its {self.timeout}s single-cycle wall-clock bound; process group killed."
            )
        except FileNotFoundError:
            raise RuntimeError(f"cursor-agent CLI '{cursor_bin}' not found.")

        if completed.returncode != 0:
            stdout_tail = (completed.stdout or "").strip()[-500:]
            stderr_tail = (completed.stderr or "").strip()[-500:]
            hint = ""
            if "Not logged in" in stderr_tail or "Authentication required" in stderr_tail:
                hint = (
                    " Cursor CLI is not authenticated — run 'cursor-agent login' (or set CURSOR_API_KEY) on this host."
                )
            raise RuntimeError(
                f"cursor-agent failed (exit {completed.returncode}).{hint}\n"
                f"STDOUT: {stdout_tail}\nSTDERR: {stderr_tail}"
            )

        payload = self._parse_output(completed.stdout or "")

        # A zero exit code is NOT sufficient: the envelope carries its own
        # is_error discriminator, and treating an errored turn as success would
        # feed a failure message to the result extractor and surface it as a
        # legitimate handoff ([AGT-10] never swallow a signalled failure).
        if payload.get("is_error") is True:
            detail = payload.get("result") or payload.get("text") or ""
            raise RuntimeError(f"cursor-agent reported is_error=true: {str(detail).strip()[-500:]}")

        # Cursor's own camelCase block first; fall back to the shared
        # snake_case reader in case a future CLI version emits that shape.
        token_usage = normalize_cursor_usage(payload) or normalize_cli_usage(payload)
        response_model = payload.get("model") or model_slug

        if progress_callback:
            if token_usage:
                progress_callback(
                    WorkerEventName.SUBAGENT_TURN_COMPLETE,
                    backend="cursor-cli",
                    phase="execution",
                    token_usage=token_usage,
                    response_model=response_model,
                    reasoning_effort=effective_effort,
                )
            progress_callback(WorkerEventName.EXEC_COMPLETE, backend="cursor-cli")

        result = BackendResult.from_dict(extract_result_payload(payload))
        return BackendResult(
            handoff_action=result.handoff_action,
            summary=result.summary,
            details=result.details,
            tests_run=result.tests_run,
            blockers=result.blockers,
            changed_files=result.changed_files,
            merge_ready=result.merge_ready,
            token_usage=token_usage or result.token_usage,
            response_model=response_model,
            # The effort the SELECTED SLUG encodes, never the unhonored request:
            # stamping the request would fabricate an audit trail claiming an
            # effort the vendor never received.
            reasoning_effort=effective_effort,
            raw_payload=result.raw_payload,
        )

    @staticmethod
    def _parse_output(stdout: str) -> dict[str, Any]:
        """Normalize cursor's stdout into a single dict for payload extraction.

        ``--output-format json`` is a transport envelope, not a schema-
        constrained result, and its exact shape is version-dependent, so accept
        the three shapes a CLI of this kind emits rather than asserting one:

        * a JSON object     -> used directly;
        * a JSON array of stream events -> assistant text concatenated, so the
          narrated result object is still recoverable;
        * anything else     -> treated as prose and wrapped, which routes it to
          ``extract_result_payload``'s embedded-JSON scan.
        """
        text = stdout.strip()
        if not text:
            raise RuntimeError("cursor-agent produced no output.")
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            return {"result": text}

        if isinstance(parsed, dict):
            return parsed
        if isinstance(parsed, list):
            chunks: list[str] = []
            for event in parsed:
                if isinstance(event, str):
                    chunks.append(event)
                elif isinstance(event, dict):
                    for key in ("text", "content", "message", "result"):
                        value = event.get(key)
                        if isinstance(value, str):
                            chunks.append(value)
                            break
            return {"result": "\n".join(chunks)}
        return {"result": text}
