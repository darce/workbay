"""Worktree-scoped grok lane config materialization (implementation note S5, D3+D4).

Composer-only + attribution guarantees live at the integration seam (Farley
Ports & Adapters / information hiding): a worktree ``./.grok/config.toml`` pins
``fork_secondary_model`` to Composer and sets ``WORKBAY_HANDOFF_DEFAULT_AGENT``
on each WorkBay MCP server's launch env, so the handoff server attributes grok's
writes correctly (``_resolve_write_actor`` precedence slot 2) without depending
on the junior worker's compliance. The operator's global ``~/.grok/config.toml``
is never mutated.

Config generation is a pure function (config dict -> TOML text), unit-testable
without a real grok binary (decision #2802).
"""

from __future__ import annotations

import datetime
import fcntl
import re
import subprocess
import tomllib
from pathlib import Path
from typing import Any

_ATTRIBUTION_ENV_KEY = "WORKBAY_HANDOFF_DEFAULT_AGENT"
_GROK_CONFIG_RELPATH = ".grok/config.toml"
DEFAULT_GROK_MODEL = "grok-composer-2.5-fast"

# A TOML bare key may contain only ASCII letters, digits, ``-`` and ``_`` (and
# must be non-empty). Anything else (spaces, dots, empty string, unicode, ...)
# must be emitted as a quoted basic-string key so it round-trips with its
# original meaning instead of silently restructuring into nested tables.
_BARE_KEY_RE = re.compile(r"^[A-Za-z0-9_-]+$")


def build_grok_lane_config(
    *,
    model: str,
    fork_secondary_model: str,
    default_agent: str,
    servers: list[dict[str, Any]],
) -> dict[str, Any]:
    """Build the canonical grok lane config dict.

    ``servers`` is a list of ``{name, command, args}`` dicts (from
    ``grok mcp add`` shape). Each server's launch env gets the attribution key
    so writes made through it credit the Composer identity.
    """
    mcp_servers: dict[str, Any] = {}
    for server in servers:
        name = server["name"]
        entry: dict[str, Any] = {
            "command": server["command"],
            "args": list(server.get("args", [])),
            "env": {_ATTRIBUTION_ENV_KEY: default_agent},
        }
        # Preserve any caller-provided extra env, attribution key wins.
        extra_env = server.get("env")
        if isinstance(extra_env, dict):
            entry["env"] = {**extra_env, _ATTRIBUTION_ENV_KEY: default_agent}
        mcp_servers[name] = entry
    return {
        "model": model,
        "ui": {"fork_secondary_model": fork_secondary_model},
        "mcp_servers": mcp_servers,
    }


def _toml_escape(value: str) -> str:
    out: list[str] = []
    for ch in value:
        if ch == "\\":
            out.append("\\\\")
        elif ch == '"':
            out.append('\\"')
        elif ch == "\n":
            out.append("\\n")
        elif ch == "\t":
            out.append("\\t")
        elif ch == "\r":
            out.append("\\r")
        elif ord(ch) < 0x20 or ord(ch) == 0x7F:
            # TOML basic strings forbid control chars C0 (< 0x20) AND DEL (0x7F).
            out.append(f"\\u{ord(ch):04x}")
        else:
            out.append(ch)
    return "".join(out)


def _toml_key(key: str) -> str:
    """Render ``key`` as a TOML key: bare when safe, else a quoted basic string.

    A bare key is emitted only for a non-empty ``^[A-Za-z0-9_-]+$`` match (e.g.
    ``model``, ``workbay-handoff-mcp``). Anything else -- a space (``my server``),
    a dot (``a.b`` which would otherwise silently nest into ``[a][b]``), or the
    empty string -- is quoted via :func:`_toml_escape` so it survives the
    merge-don't-clobber round trip with its original meaning.
    """
    if key and _BARE_KEY_RE.match(key):
        return key
    return f'"{_toml_escape(key)}"'


def _is_table_array(value: Any) -> bool:
    return isinstance(value, list) and len(value) > 0 and all(isinstance(item, dict) for item in value)


def _fmt_scalar(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, str):
        return f'"{_toml_escape(value)}"'
    # tomllib legitimately yields date/datetime/time from a valid operator config
    # (``updated = 2026-01-01``); emit their TOML-native (unquoted) literal.
    # NB: ``datetime.datetime`` subclasses ``datetime.date`` so the combined
    # isinstance covers all three, and this must precede the int/float branch.
    if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
        return value.isoformat()
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, dict):
        # A dict reaches here only as an element of a MIXED array (e.g.
        # ``x = [1, {a = 1}]``): ``_is_table_array`` is False when items aren't
        # all dicts, so the array is a scalar and its inline table serializes here.
        inner = ", ".join(f"{_toml_key(k)} = {_fmt_scalar(v)}" for k, v in value.items())
        return "{" + inner + "}"
    if isinstance(value, list):
        return "[" + ", ".join(_fmt_scalar(item) for item in value) + "]"
    raise TypeError(f"Unsupported TOML scalar type: {type(value)!r}")


def render_config_toml(config: dict[str, Any], _segments: tuple[str, ...] = ()) -> str:
    """Serialize a (possibly nested) config dict to deterministic TOML text.

    Emits the current table's scalar keys first, then each sub-table under a
    dotted ``[a.b]`` header (recursively). ``_segments`` carries the enclosing
    table path as *already-quoted* key segments (via :func:`_toml_key`), so a
    header is assembled by joining safe segments -- e.g. ``mcp_servers`` +
    ``"my server"`` -> ``[mcp_servers."my server"]`` -- instead of string-joining
    raw names, which would corrupt keys that aren't bare-key-safe.
    """
    scalars: dict[str, Any] = {}
    tables: dict[str, Any] = {}
    table_arrays: dict[str, Any] = {}
    for key, value in config.items():
        if isinstance(value, dict):
            tables[key] = value
        elif _is_table_array(value):
            table_arrays[key] = value
        else:
            scalars[key] = value

    lines: list[str] = []
    for key, value in scalars.items():
        lines.append(f"{_toml_key(key)} = {_fmt_scalar(value)}")
    for key, value in tables.items():
        segments = _segments + (_toml_key(key),)
        header = ".".join(segments)
        if lines and lines[-1] != "":
            lines.append("")
        lines.append(f"[{header}]")
        body = render_config_toml(value, segments)
        if body:
            lines.append(body)
    for key, array in table_arrays.items():
        segments = _segments + (_toml_key(key),)
        header = ".".join(segments)
        for item in array:
            if lines and lines[-1] != "":
                lines.append("")
            lines.append(f"[[{header}]]")
            body = render_config_toml(item, segments)
            if body:
                lines.append(body)
    return "\n".join(lines)


def _deep_merge(base: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
    """Recursively merge ``overlay`` into ``base`` (overlay wins on scalar keys)."""
    result = dict(base)
    for key, value in overlay.items():
        existing = result.get(key)
        if isinstance(existing, dict) and isinstance(value, dict):
            result[key] = _deep_merge(existing, value)
        else:
            result[key] = value
    return result


def merge_config_toml(existing_toml: str | None, config: dict[str, Any]) -> str:
    """Merge ``config`` into an existing config's parsed contents (don't clobber).

    Unmanaged keys the operator set survive; the managed Composer-only pins
    (``model``, ``ui.fork_secondary_model``, and each server's attribution env)
    are forced on. Returns rendered TOML text.
    """
    base: dict[str, Any] = {}
    if existing_toml and existing_toml.strip():
        base = tomllib.loads(existing_toml)
    merged = _deep_merge(base, config)
    return render_config_toml(merged) + "\n"


def _resolve_git_exclude_path(worktree_path: str | Path) -> Path | None:
    """Resolve the exclude file git actually honors for ``worktree_path``.

    Uses ``git rev-parse --git-path info/exclude`` so it works for BOTH a
    regular repo (``.git`` is a directory) and a linked worktree (``.git`` is a
    ``gitdir:`` pointer FILE — ``<worktree>/.git/info/exclude`` does not exist and
    ``mkdir`` under the ``.git`` file would raise). Returns None when the path
    can't be resolved (not a git repo / git unavailable).
    """
    try:
        result = subprocess.run(
            ["git", "-C", str(worktree_path), "rev-parse", "--git-path", "info/exclude"],
            capture_output=True,
            text=True,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if result.returncode != 0 or not result.stdout.strip():
        return None
    resolved = Path(result.stdout.strip())
    if not resolved.is_absolute():
        resolved = (Path(worktree_path) / resolved).resolve()
    return resolved


def append_git_exclude(worktree_path: str | Path, entry: str) -> bool:
    """Idempotently append ``entry`` to the worktree's effective git exclude file.

    Returns True if written, False if already present or unresolvable. Keeps the
    materialized ``.grok/config.toml`` invisible to the post-turn scope gate
    repo-agnostically (this monorepo gitignores ``.grok/`` but consumer repos may
    not). Resolves the exclude path via git so linked worktrees (``.git`` is a
    file) are handled correctly, not just regular repos.

    ``git rev-parse --git-path info/exclude`` intentionally resolves to git's
    SHARED (common-dir) exclude file -- git has no per-worktree exclude -- so the
    entry is repo-shared across every linked worktree by design. That is fine
    here: the entry (``.grok/config.toml``) is deliberately harmless and
    idempotent, so multiple grok lanes converging on the same shared exclude is a
    no-op after the first write.

    The read-check-append critical section is guarded by an exclusive
    ``fcntl.flock`` on the exclude file handle so two grok lanes bootstrapping
    concurrently can't both observe a missing entry and both append (TOCTOU) --
    the in-function idempotence check is authoritative under the lock. Behavior
    is identical to the unlocked path for the single-caller case.
    """
    exclude_path = _resolve_git_exclude_path(worktree_path)
    if exclude_path is None:
        # Fallback for a plain directory whose ``.git`` is a real dir (or absent);
        # never try to mkdir under a ``.git`` FILE (linked worktree without git).
        dot_git = Path(worktree_path) / ".git"
        if dot_git.is_file():
            return False
        exclude_path = dot_git / "info" / "exclude"
    exclude_path.parent.mkdir(parents=True, exist_ok=True)
    # ``a+`` creates the file if absent and appends on write (O_APPEND forces
    # every write to end); we hold an exclusive lock across read + append so the
    # check-then-write is atomic w.r.t. concurrent lane bootstraps.
    with exclude_path.open("a+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            handle.seek(0)
            existing_lines = handle.read().splitlines()
            if entry in existing_lines:
                return False
            if existing_lines and existing_lines[-1] != "":
                handle.write("\n")
            handle.write(f"{entry}\n")
            # Flush the userspace buffer BEFORE releasing the lock: otherwise
            # the data reaches the file only at ``with``-exit (after LOCK_UN),
            # so a waiter acquiring the lock in that window still reads the
            # entry as absent and appends a duplicate.
            handle.flush()
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    return True


def materialize_grok_lane_config(
    worktree_path: str | Path,
    *,
    model: str,
    fork_secondary_model: str,
    default_agent: str,
    servers: list[dict[str, Any]],
) -> Path:
    """Write (merge-don't-clobber) ``./.grok/config.toml`` into the lane worktree.

    Also appends ``.grok/config.toml`` to ``.git/info/exclude`` so the scope gate
    stays clean. Never touches the operator's global config. Returns the config
    path.
    """
    worktree = Path(worktree_path)
    config_path = worktree / _GROK_CONFIG_RELPATH
    config_path.parent.mkdir(parents=True, exist_ok=True)

    config = build_grok_lane_config(
        model=model,
        fork_secondary_model=fork_secondary_model,
        default_agent=default_agent,
        servers=servers,
    )
    existing = config_path.read_text() if config_path.exists() else None
    config_path.write_text(merge_config_toml(existing, config), encoding="utf-8")

    append_git_exclude(worktree, _GROK_CONFIG_RELPATH)
    return config_path
