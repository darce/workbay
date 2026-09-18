"""Resolve the runtime ``uv tool`` interpreter for a managed MCP server.

Git-only installs place each MCP server as a ``uv tool install`` console script
(NOT a workspace ``.venv``). The verify / doctor / reinject seams must probe
*this* interpreter — the one ``mcp_launch.py`` execs at runtime — never the
front-door ``workbay`` tool venv (which carries different extras) nor a
``<target>/.venv`` that git-only installs never create.

The uv-tool bin-dir resolution mirrors the shim
(``packages/workbay-system/.../payload/scripts/hooks/mcp_launch.py``:
``_uv_tool_bin_dirs`` / ``_tool_console_path``). The shim is a payload script,
not an importable module, so the small, stable logic is duplicated here rather
than imported; keep the two in sync by contract. Stdlib-only on purpose.

POSIX (macOS/Linux) is the supported surface for the runtime-interpreter probe:
uv-tool console scripts are shebang-pinned to their own venv python, so the
interpreter is read from the console's ``#!`` line. On Windows the console is a
compiled ``.exe`` launcher with no readable shebang; ``resolve_tool_python``
degrades to ``""`` there (callers already treat ``""`` as "not resolvable").
"""

from __future__ import annotations

import hashlib
import os
import re
import secrets
import shutil

# Probe acceptance: the candidate must execute the ``-c`` body as Python so it
# can import stdlib modules and print a full-line response. The nonce is bound
# OUT OF BAND in the child environment (not embedded in argv), so a binary that
# only scrapes its argv cannot recover it. This rejects fixed-string echo and
# argv-scraper forgeries; it does not cryptographically prove a CPython runtime
# against a program that reads the same env var. Parent verifies the digest.
_PROBE_SENTINEL = "WORKBAY_PY_OK"
_PROBE_NONCE_ENV = "WORKBAY_PY_PROBE_NONCE"
# Full-line match: major ≥ 3, then 64-char lowercase hex digest of the nonce.
_PROBE_OK_RE = re.compile(
    rf"^{_PROBE_SENTINEL}:([3-9]|[1-9]\d+):([0-9a-f]{{64}})$",
    re.MULTILINE,
)
# Cap on *accepted* probe stdout. ``run_external`` fully buffers the child via
# ``communicate()`` on a PIPE, so a flooder is still allocated in the parent;
# this bound only rejects oversized payloads after capture (not allocation).
# Oversized output is rejected with a typed reason (timeout alone is not enough).
_PROBE_STDOUT_MAX_BYTES = 4096

# env(1) option sets used by ``_env_program_token``.
# No-argument flags (operand is not consumed).
# ``-S`` stays no-arg on purpose: its operand is a single *quoted* string that
# the shebang-body whitespace split already shreds; treating ``-S`` as takes-arg
# would consume only the first word and mis-parse the rest.
_ENV_NO_ARG = frozenset({"-i", "-S", "-0", "-v"})
# Single-operand flags (next token is consumed as the option's argument).
# ``-u`` / ``--unset``: portable unset of a single name (GNU long form also
# accepts ``--unset=NAME``; see glued handling below).
# ``-P``: BSD/macOS env option for an alternate util path; GNU coreutils env
# rejects ``-P`` with rc=125, but shebang parsing still consumes the operand so
# a portable ``env -P /usr/bin python3`` yields program ``python3``.
# ``-C``: change working directory; supported by both BSD env (macOS) and GNU
# env (``env -C workdir``). Glued ``-C<path>`` is also accepted.
# GNU long options other than ``--unset`` / ``--unset=`` (e.g. ``--chdir=``,
# ``--split-string=``) are intentionally unsupported: they yield
# ``env_unsupported_option`` rather than silent mis-parse.
_ENV_TAKES_ARG = frozenset({"-u", "-P", "-C", "--unset"})

# Last structured rejection reason for ``resolve_tool_python`` / direct
# ``_is_usable_python`` calls (defect 7). Return type stays ``str`` (``""`` on
# reject); callers may read this for a discriminator without API churn.
# Direct-call contract: ``_is_usable_python`` clears any prior reason on entry
# so a direct caller never observes a stale token from a previous probe.
_last_reject_reason: str = ""


def last_reject_reason() -> str:
    """Most recent probe/resolution rejection reason, or ``""`` on success.

    Reasons are short machine-oriented tokens (e.g. ``probe_timeout``,
    ``env_program_not_absolute``). Chosen over logging because this package has
    no logger and callers already branch only on falsy return — attaching a
    module-level reason lets doctor/install surface *why* without API churn.
    """
    return _last_reject_reason


def _set_reject_reason(reason: str) -> str:
    """Record ``reason`` and return ``""`` (reject)."""
    global _last_reject_reason
    _last_reject_reason = reason
    return ""


def _clear_reject_reason() -> None:
    global _last_reject_reason
    _last_reject_reason = ""


def _uv_tool_bin_dirs() -> list[str]:
    """uv's default ``uv tool install`` console bin-dir resolution order.

    ``UV_TOOL_BIN_DIR`` → ``$XDG_BIN_HOME`` → ``$XDG_DATA_HOME/../bin`` →
    ``~/.local/bin``, de-duplicated preserving first-seen order.
    """
    candidates: list[str] = []
    for env in ("UV_TOOL_BIN_DIR", "XDG_BIN_HOME"):
        value = os.environ.get(env)
        if value:
            candidates.append(value)
    xdg_data_home = os.environ.get("XDG_DATA_HOME")
    if xdg_data_home:
        candidates.append(os.path.join(os.path.dirname(xdg_data_home), "bin"))
    candidates.append(os.path.join(os.path.expanduser("~"), ".local", "bin"))
    seen: set[str] = set()
    ordered: list[str] = []
    for candidate in candidates:
        if candidate and candidate not in seen:
            seen.add(candidate)
            ordered.append(candidate)
    return ordered


def resolve_tool_console(console: str) -> str:
    """Path to the ``uv tool`` console script for ``console``, or ``""``.

    Probes ``PATH`` (``shutil.which``) first, then uv's default tool bin-dir
    order under POSIX (``<dir>/<console>`` or ``<dir>/bin/<console>``) and
    Windows (``<dir>/Scripts/<console>.exe``) layouts.
    """
    which = shutil.which(console)
    if which and os.path.exists(which):
        return which
    for bin_dir in _uv_tool_bin_dirs():
        for candidate in (
            os.path.join(bin_dir, console),
            os.path.join(bin_dir, "bin", console),
            os.path.join(bin_dir, "Scripts", f"{console}.exe"),
        ):
            if os.path.exists(candidate):
                return candidate
    return ""


def _env_program_token(parts: list[str]) -> tuple[str | None, str | None]:
    """Return ``(program, error_reason)`` from an ``env`` shebang's split body.

    ``parts[0]`` is the env binary path. Skips env's own options before the
    program name: known no-arg flags (``-i``, ``-S``, ``-0``, ``-v``), known
    takes-arg flags (``-u``, ``-P``, ``-C``, ``--unset``), ``--``, and
    ``NAME=VALUE`` assignments.

    Glued short forms (``-uNAME``, ``-P/path``, ``-C/path``) and
    ``--unset=NAME`` are consumed as single tokens. Other GNU long options
    (``--chdir=``, ``--split-string=``, …) are not accepted — they return
    ``env_unsupported_option``.

    Any leading token beginning with ``-`` that is not in the known sets yields
    ``(None, "env_unsupported_option")`` rather than silently becoming the
    program token (which would degrade into a which() miss).
    """
    i = 1
    n = len(parts)
    while i < n:
        tok = parts[i]
        if tok == "--":
            i += 1
            break
        if tok in _ENV_NO_ARG:
            i += 1
            continue
        if tok in _ENV_TAKES_ARG:
            i += 2  # flag + operand (may run past end; treated as missing prog)
            continue
        # Glued forms: -uNAME, -P/path, -C/path, --unset=NAME
        if tok.startswith("-u") and len(tok) > 2:
            i += 1
            continue
        if tok.startswith("-P") and len(tok) > 2:
            i += 1
            continue
        if tok.startswith("-C") and len(tok) > 2:
            i += 1
            continue
        if tok.startswith("--unset="):
            i += 1
            continue
        # NAME=VALUE assignment (env allows these before the program).
        if "=" in tok and not tok.startswith("-"):
            i += 1
            continue
        # Unknown option (any other leading "-…") — do not treat as program.
        if tok.startswith("-"):
            return None, "env_unsupported_option"
        # First non-option, non-assignment token is the program.
        return tok, None
    if i < n:
        return parts[i], None
    return None, None


def _load_probe_gateway() -> tuple[type[Exception], object]:
    """Import the probe gateway. Isolated so ImportError is catchable (defect 5)."""
    from workbay_bootstrap.external import (
        ExternalCallTimeout,
        run_resolved_interpreter_probe,
    )

    return ExternalCallTimeout, run_resolved_interpreter_probe


def _build_probe() -> str:
    """Python ``-c`` body that digests the env-bound nonce and prints the line.

    The nonce is *not* interpolated into this string — it is read from
    ``os.environ`` at child runtime so it never appears in argv.
    """
    return (
        "import hashlib,os,sys;"
        f"n=os.environ[{_PROBE_NONCE_ENV!r}];"
        "print('%s:%%d:%%s'%%("
        "sys.version_info[0],"
        "hashlib.sha256(n.encode('utf-8')).hexdigest()"
        "))" % (_PROBE_SENTINEL,)
    )


def _stdout_as_text(stdout: object) -> str:
    """Coerce probe stdout to ``str`` (gateway may return bytes or other)."""
    if stdout is None:
        return ""
    if isinstance(stdout, str):
        return stdout
    if isinstance(stdout, bytes):
        return stdout.decode("utf-8", "replace")
    return str(stdout)


def _is_usable_python(candidate: str) -> bool:
    """True iff ``candidate`` runs Python well enough for the install probe.

    This is a lightweight capability check — not a full package-hosting
    proof. Acceptance criteria (fail-closed):

    * ``candidate`` is a non-empty existing path
    * ``candidate -c`` executes a probe that imports ``sys``, ``hashlib``, and
      ``os``, reads a fresh nonce from the child environment (not argv), prints
      a full line ``WORKBAY_PY_OK:<major>:<sha256(nonce)>`` with major ≥ 3,
      and exits 0 (rc alone is not enough — true/echo ignore argv; an
      argv-scraper cannot recover the env-bound nonce)
    * Captured stdout larger than :data:`_PROBE_STDOUT_MAX_BYTES` is rejected
    * On ``OSError``, probe timeout, ``ImportError`` loading the probe
      gateway, or any other exception: returns ``False`` (never raises into
      the caller)

    Direct-call contract: clears any prior :func:`last_reject_reason` on entry
    so callers of ``_is_usable_python`` never see a stale token. On failure
    sets a typed reason; on success leaves reason ``""``.

    Routes through :func:`run_resolved_interpreter_probe` (never bare
    ``subprocess.run``). The nonce is bound only in a per-call child ``env``
    mapping — the parent process environment is never mutated.
    """
    # Direct-call contract: never leave a stale reason from a previous probe.
    _clear_reject_reason()
    if not candidate or not os.path.exists(candidate):
        return False
    # Import via helper so a broken/partial install yields False, not an
    # exception escaping ``resolve_tool_python`` (defect 5).
    try:
        ExternalCallTimeout, run_resolved_interpreter_probe = _load_probe_gateway()
    except ImportError:
        _set_reject_reason("probe_import_error")
        return False

    nonce = secrets.token_hex(16)
    expected_digest = hashlib.sha256(nonce.encode("utf-8")).hexdigest()
    probe_code = _build_probe()

    # Bind nonce out-of-band in the child env only — never mutate os.environ
    # (process-global mutation races concurrent probes and leaks the key).
    child_env = {**os.environ, _PROBE_NONCE_ENV: nonce}
    try:
        result = run_resolved_interpreter_probe(  # type: ignore[operator]
            candidate, probe_code, timeout=5, env=child_env
        )
    except ExternalCallTimeout:
        _set_reject_reason("probe_timeout")
        return False
    except OSError:
        _set_reject_reason("probe_os_error")
        return False
    except Exception:
        # Final containment arm: typed reasons above stay distinct; anything
        # else (RuntimeError, ValueError, MemoryError, TypeError on bytes
        # stdout handling, …) fails closed without aborting install/doctor.
        _set_reject_reason("probe_unexpected_error")
        return False

    try:
        if result.returncode != 0:
            _set_reject_reason("probe_nonzero_exit")
            return False
        raw_stdout = getattr(result, "stdout", None)
        # Cap before full decode of huge payloads: measure bytes when
        # available, else encoded UTF-8 length of the text form.
        if isinstance(raw_stdout, (bytes, bytearray)):
            if len(raw_stdout) > _PROBE_STDOUT_MAX_BYTES:
                _set_reject_reason("probe_stdout_too_large")
                return False
        stdout = _stdout_as_text(raw_stdout)
        if not isinstance(raw_stdout, (bytes, bytearray)):
            if len(stdout.encode("utf-8", "replace")) > _PROBE_STDOUT_MAX_BYTES:
                _set_reject_reason("probe_stdout_too_large")
                return False
        match = _PROBE_OK_RE.search(stdout)
        if match is None or match.group(2) != expected_digest:
            _set_reject_reason("probe_invalid_output")
            return False
        return True
    except Exception:
        _set_reject_reason("probe_unexpected_error")
        return False


def resolve_tool_python(console: str) -> str:
    """The venv python behind the ``uv tool`` ``console`` script, or ``""``.

    Reads the console script's shebang (``#!<venv>/bin/python``) — uv-tool
    console scripts are shebang-pinned to their own venv interpreter, so this is
    the interpreter the runtime server actually runs under. Returns ``""`` when
    the console is unresolved, unreadable, has no shebang (e.g. a Windows
    ``.exe`` launcher), the shebang interpreter path does not exist, the
    resolved path is not absolute, or the candidate fails the usable-Python
    probe (see :func:`_is_usable_python`). On rejection,
    :func:`last_reject_reason` carries a short discriminator; the return type
    remains ``str`` so callers' existing falsy guards keep working.

    ``#!/usr/bin/env <prog>`` is resolved through PATH to the real program
    (never returns ``/usr/bin/env`` itself — implementation note / RLSE-05). Env's own
    options (``-i``, ``-S``, ``-P``, ``-C``, ``-u``, ``--``, ``NAME=VALUE``)
    are skipped before taking the program token; unknown ``-…`` / GNU long
    options reject with ``env_unsupported_option``. Unusable candidates yield
    ``""``.

    Only the interpreter *path* is returned. Interpreter flags that appear in
    the shebang after the program token (e.g. ``python3 -O``,
    ``env -S python3 -X utf8``) are intentionally dropped — callers re-exec
    the bare interpreter path, not the full shebang argv.
    """
    _clear_reject_reason()
    script = resolve_tool_console(console)
    if not script:
        return _set_reject_reason("console_unresolved")
    try:
        with open(script, "rb") as handle:
            first_line = handle.readline()
    except OSError:
        return _set_reject_reason("shebang_unreadable")
    if not first_line.startswith(b"#!"):
        return _set_reject_reason("no_shebang")
    body = first_line[2:].strip().decode("utf-8", "replace")
    if not body:
        return _set_reject_reason("empty_shebang")
    parts = body.split()
    interpreter = parts[0]
    # env form: ``#!/usr/bin/env <opts> <prog>`` — resolve the real program via
    # PATH rather than treating ``/usr/bin/env`` as the interpreter (0137).
    if os.path.basename(interpreter) == "env":
        prog, env_err = _env_program_token(parts)
        if env_err:
            return _set_reject_reason(env_err)
        if not prog:
            return _set_reject_reason("env_missing_program")
        resolved = shutil.which(prog)
        if not resolved:
            return _set_reject_reason("env_program_not_on_path")
        # Empty PATH components make which return a bare relative name that
        # exists relative to cwd — reject non-absolute results (defect 4).
        if not os.path.isabs(resolved):
            return _set_reject_reason("env_program_not_absolute")
        interpreter = resolved
    else:
        if not os.path.isabs(interpreter):
            return _set_reject_reason("interpreter_not_absolute")
        if not os.path.exists(interpreter):
            return _set_reject_reason("interpreter_missing")
    if not _is_usable_python(interpreter):
        # _is_usable_python sets a more specific reason when it fails.
        if not _last_reject_reason:
            _set_reject_reason("probe_rejected")
        return ""
    _clear_reject_reason()
    return interpreter
