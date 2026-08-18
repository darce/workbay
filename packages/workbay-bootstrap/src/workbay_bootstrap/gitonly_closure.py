"""Git-only runtime closure install helpers (internal S1)."""

from __future__ import annotations

import os
import re
import tomllib
from pathlib import Path
from urllib.parse import ParseResult, parse_qs, unquote, urlparse

from workbay_protocol.brand import REPO_HTTPS_GIT_URL

GITONLY_RUNTIME_MEMBERS: tuple[str, ...] = (
    "workbay-protocol",
    "mcp-workbay-handoff",
    "mcp-workbay-orchestrator",
    "workbay-bootstrap",
    "workbay-system",
)

GITONLY_FRONT_DOOR = "workbay"

GITONLY_CLOSURE_PACKAGES: tuple[str, ...] = GITONLY_RUNTIME_MEMBERS + (GITONLY_FRONT_DOOR,)

GITONLY_MCP_PACKAGES: tuple[str, ...] = (
    "mcp-workbay-handoff",
    "mcp-workbay-orchestrator",
)

# Front-door tool whose uv-receipt.toml pins the whole git-sourced closure when
# consumers install via ``uv tool install workbay --with …`` (package mode).
FRONT_DOOR_TOOL_NAME = GITONLY_FRONT_DOOR
_UV_RECEIPT_NAME = "uv-receipt.toml"

# The codex host bridge is an *optional* orchestrator extra (``[bridge]``), not a
# base closure member: only the orchestrator tool install needs it. It must be
# git-sourced too — otherwise the git-only orchestrator install resolves it from
# PyPI (the Q4 trap). Spec-able, but excluded from the universal closure source
# check (``GITONLY_CLOSURE_PACKAGES``) and from the default ``--with`` set.
GITONLY_BRIDGE_MEMBER = "workbay-codex-bridge"

# Per-package extra members to git-source via additional ``--with`` specs so a
# package's optional extras stay PyPI-free. The orchestrator's ``[bridge]`` extra
# pulls ``workbay-codex-bridge`` (the ``codex-subagent`` backend); without this
# the git-only orchestrator install silently drops that backend.
GITONLY_PACKAGE_EXTRA_MEMBERS: dict[str, tuple[str, ...]] = {
    "mcp-workbay-orchestrator": (GITONLY_BRIDGE_MEMBER,),
}

# Packages a git/path member spec may be built for: the closure plus the
# spec-able bridge extra.
_SPECABLE_PACKAGES: tuple[str, ...] = GITONLY_CLOSURE_PACKAGES + (GITONLY_BRIDGE_MEMBER,)

# Members whose specs are materialized into a closure spec map (the runtime
# members plus the spec-able bridge extra, so an orchestrator install can
# ``--with`` it from git).
_SPEC_MAP_MEMBERS: tuple[str, ...] = GITONLY_RUNTIME_MEMBERS + (GITONLY_BRIDGE_MEMBER,)


def member_specs_from_git_ref(*, repo_url: str, tag: str) -> dict[str, str]:
    return {
        member: git_member_spec(member, repo_url=repo_url, tag=tag)
        for member in _SPEC_MAP_MEMBERS
    }


def member_specs_from_repo_root(repo_root: Path) -> dict[str, str]:
    return {member: path_member_spec(repo_root, member) for member in _SPEC_MAP_MEMBERS}


def _fallback_uv_tools_root() -> Path:
    """Environment-based uv tools dir when ``uv tool dir`` is unavailable."""
    env = os.environ.get("UV_TOOL_DIR")
    if env:
        return Path(env).expanduser()
    xdg = os.environ.get("XDG_DATA_HOME")
    if xdg:
        return Path(xdg).expanduser() / "uv" / "tools"
    return Path.home() / ".local" / "share" / "uv" / "tools"


def default_uv_tools_root() -> Path:
    """uv's tool install directory (matches ``uv tool dir`` resolution).

    Prefers a live ``uv tool dir`` query via the external-call gateway so the
    path tracks uv's own policy. On missing uv, non-zero exit, timeout, or any
    other failure, falls back to the environment-based computation
    (``UV_TOOL_DIR``, else ``$XDG_DATA_HOME/uv/tools``, else
    ``~/.local/share/uv/tools``). Never raises; always returns a path. Tests
    pin a temp root via ``UV_TOOL_DIR`` (honoured by both uv and the fallback).
    """
    try:
        from workbay_bootstrap.external import run_external

        proc = run_external(
            ["uv", "tool", "dir"],
            call_class="probe",
            capture_output=True,
            text=True,
            check=False,
            timeout_override=5,
        )
        if proc.returncode == 0 and proc.stdout:
            line = proc.stdout.strip().splitlines()
            if line:
                candidate = Path(line[0].strip()).expanduser()
                if str(candidate):
                    return candidate
    except Exception:
        # OSError / DeferredExternalCall / ExternalCallTimeout / ValueError —
        # never raise from path resolution; env fallback is the contract.
        pass
    return _fallback_uv_tools_root()


def front_door_uv_receipt_path(*, tools_root: Path | None = None) -> Path:
    """Path to the front-door ``workbay`` tool's ``uv-receipt.toml``."""
    root = tools_root if tools_root is not None else default_uv_tools_root()
    return root / FRONT_DOOR_TOOL_NAME / _UV_RECEIPT_NAME


# Placeholder returned when a spec cannot be safely decomposed. Receipts must
# never fail-open to the raw input (credentials in scp form, query tokens, or
# odd netlocs would otherwise land on disk). Callers that need the original for
# clone auth must keep it on the trusted argv side, not the receipt.
REDACTED_UNPARSEABLE_GIT_SPEC = "<redacted-unparseable-git-spec>"

# Query/fragment keys that are structural for uv/git member specs and never
# carry credentials. Everything else is dropped (fail closed).
_SAFE_GIT_QUERY_FRAGMENT_KEYS = frozenset({"subdirectory", "rev", "tag", "branch"})


def _path_may_carry_userinfo(path: str) -> bool:
    """True when ``path`` must not be re-attached (possible embedded userinfo).

    Receipt rebuild used to scrub only ``netloc`` and re-attach ``path``
    verbatim. Double-scheme and control-char-spliced inputs put
    ``user:SECRET@host`` into ``.path`` while ``.netloc`` has no ``@``.

    Fail closed on any ``@`` in the path, with one exception required for
    currently valid uv member pins: a single trailing ``.git@REV`` where
    ``REV`` has no ``/`` or ``@`` and the path prefix does not itself embed
    an authority (``://`` / leading ``//`` / another ``@``). Credential-free
    mid-segment ``@`` (e.g. ``/o/re@po.git``) is redacted — same direction as
    the empty-authority ``file://`` policy.
    """
    if "@" not in path:
        return False
    # Sole safe form: /…/repo.git@REV (uv git+ pin with explicit rev).
    if path.count("@") == 1 and ".git@" in path:
        before_git, _sep, rev = path.partition(".git@")
        if (
            rev
            and "/" not in rev
            and "@" not in rev
            and ":" not in rev
            and "@" not in before_git
            and "://" not in before_git
            and not path.startswith("//")
        ):
            return False
    return True


def _netloc_without_userinfo(parsed: ParseResult) -> str:
    """Rebuild netloc without credential-bearing userinfo.

    Specs rebuilt from uv-receipt git fields are persisted into the install
    receipt / ``.workbay-bootstrap.json`` (a tracked consumer file). Embedding
    ``user:password@`` (or token userinfo) there is a credential leak. Auth for
    private clones must use the host's git credential helper, netrc, or SSH —
    not secrets baked into reconstructed member specs (internal).

    Username rule (keyed on the *value*, not the scheme):

    * Keep the username only when it is the literal ``git`` (required for
      common SSH remotes; not a secret).
    * Strip every other username *and* any password, for every scheme.
      Tokens often live in the username slot (``ssh://ghs_…@host/…``,
      ``https://x-access-token:…@host/…``).

    String surgery on ``netloc`` preserves IPv6 brackets / hostport.

    Malformed ports raise ``ValueError`` from ``parsed.port``; catch and fall
    back to stripping all userinfo via ``rsplit`` so callers never crash.
    """
    netloc = parsed.netloc or ""

    # parsed.port / hostname raise ValueError when the port is non-integer.
    try:
        host = parsed.hostname
        _ = parsed.port
    except ValueError:
        if "@" in netloc:
            return netloc.rsplit("@", 1)[-1]
        return netloc

    if not host:
        # No hostname (opaque / odd forms): strip any userinfo@ prefix.
        if "@" in netloc:
            return netloc.rsplit("@", 1)[-1]
        return netloc

    if "@" not in netloc:
        return netloc

    # Preserve host[:port] (and IPv6 brackets) by splitting netloc, not
    # rebuilding from hostname/port components.
    hostport = netloc.rsplit("@", 1)[-1]
    # Value-keyed: only the non-secret literal "git" login is preserved.
    if parsed.username == "git":
        return f"git@{hostport}"
    return hostport


def _scrub_query_or_fragment(component: str) -> str:
    """Keep only known-safe key=value pairs; drop credential-bearing keys.

    Fail closed: unknown keys (``token``, ``access_token``, …) are removed
    rather than re-attached verbatim. Original encoding of kept pairs is
    preserved (no re-``urlencode``) so structural pins stay byte-stable.
    """
    if not component:
        return ""
    # Bare fragments without ``=`` are not structural uv pins — drop them.
    if "=" not in component:
        return ""
    kept: list[str] = []
    for part in component.split("&"):
        key, sep, _value = part.partition("=")
        if sep and key.lower() in _SAFE_GIT_QUERY_FRAGMENT_KEYS:
            kept.append(part)
    return "&".join(kept)


def _is_filesystem_path_spec(url: str) -> bool:
    """True for path / file specs that cannot carry URL userinfo credentials.

    Authority-form URLs (``scheme://…``, protocol-relative ``//host/…``) can
    embed userinfo and must *not* take this branch — including ``file://user:pass@…``.
    Scheme prefixes are recognized case-insensitively regardless of how many
    slashes follow (zero, one, or two). Bare ``file:`` / ``FILE:`` local path
    refs without ``@`` pass through; any ``@`` that could hold userinfo forces
    the URL scrub path so credentials are never returned verbatim.
    """
    # Authority form before any scheme-as-path classification.
    if "://" in url:
        return False
    # Protocol-relative ``//host/…`` is not an absolute filesystem path.
    if url.startswith("//"):
        return False
    # Windows drive paths (``C:\…`` / ``C:/…``) before scheme detection so a
    # single-letter drive is not treated as a URL scheme.
    if len(url) >= 3 and url[0].isalpha() and url[1] == ":" and url[2] in {"/", "\\"}:
        return True
    # Scheme prefix (RFC 3986 scheme charset), case-insensitive. A scheme is a
    # scheme regardless of slash count — including ``file:user:SECRET@host/…``
    # and ``file:/user:SECRET@…`` (zero / one slash after the colon).
    scheme_m = re.match(r"^([A-Za-z][A-Za-z0-9+.-]*):", url)
    if scheme_m:
        scheme = scheme_m.group(1).lower()
        if scheme == "file":
            # Local file: path refs cannot embed URL userinfo when no @ is present.
            # Any @ may be userinfo — refuse the pass-through branch (fail closed).
            return "@" not in url
        # Other schemes without :// are not filesystem paths.
        return False
    # Absolute or explicit relative filesystem paths (not protocol-relative).
    if url.startswith(("/", "./", "../", "~")):
        return True
    # Relative path without scp ``host:path`` / userinfo shape.
    if "@" not in url and ":" not in url:
        return True
    return False


def scrub_git_spec_userinfo(spec: str) -> str:
    """Remove credentials from a uv git/path member spec before receipt write.

    Trust boundary: this scrubber is for the *untrusted* side (install receipt /
    ``.workbay-bootstrap.json``). It must fail closed — never return the raw
    input when the string might still embed secrets.

    Contract:

    * Strips userinfo except the literal ``git`` username (value-keyed, not
      scheme-keyed).
    * Drops query/fragment keys other than structural uv pins
      (``subdirectory``, ``rev``, ``tag``, ``branch``).
    * Normalizes scp-form remotes (``user@host:path``) via
      :func:`normalize_git_remote_url` before scrubbing.
    * Filesystem / bare ``file:`` path specs without userinfo pass through
      unchanged; any scheme or ``@`` that could hold credentials is scrubbed
      or refused (never returned raw).
    * Genuinely unparseable input (including parser exceptions such as
      malformed IPv6 authorities) returns
      :data:`REDACTED_UNPARSEABLE_GIT_SPEC` (never the raw string, never raises).

    Handles ``git+https://user:pass@host/…``, plain ``https://…``,
    ``ssh://ghs_token@host/…``, ``user:token@host:org/repo.git``,
    ``file:user:pass@host/…`` (any slash count, any case), and
    ``https://host/repo.git?token=SECRET``.

    Also refuse specs where credentials can land in the path component
    (double-scheme, control-char splices): CPython's ``urlparse`` can put
    userinfo in ``.path`` while ``.netloc`` has no ``@``, and it silently
    deletes LF/CR/TAB so the parse no longer describes the input.
    """
    # Control characters: urlparse silently deletes LF, CR, and TAB (WHATWG),
    # concatenating the remaining halves. Recording a *different* spec than the
    # input is itself a defect; refuse before any parse/rebuild (fail closed).
    if any(ch in spec for ch in ("\n", "\r", "\t")):
        return REDACTED_UNPARSEABLE_GIT_SPEC

    raw = spec.strip()
    if not raw:
        return raw

    # Optional git+ prefix; scrub the URL part and reattach.
    prefix = ""
    url = raw
    if raw.lower().startswith("git+"):
        prefix = "git+"
        url = raw[4:]

    # Path specs have no URL credentials.
    if _is_filesystem_path_spec(url):
        return raw

    # Scheme-less remotes (scp form, including credentialed ``user:tok@host:path``)
    # must be normalized before parse — never returned raw (D2).
    if "://" not in url:
        normalized = normalize_git_remote_url(url)
        if "://" not in normalized:
            # Still scheme-less and not a filesystem path → unparseable.
            # Also covers bare ``file:user:SECRET@…`` / ``file:/user:…@…`` forms
            # that cannot be decomposed into a safe authority (fail closed).
            return REDACTED_UNPARSEABLE_GIT_SPEC
        url = normalized

    try:
        parsed = urlparse(url)
        # Fail closed on undecomposable URLs (D3): never echo the raw input.
        # Distinguish "no authority" from "authority I could not parse": an
        # empty-authority file URL (``file:///path``, ``file:///C:/path``) is
        # well-formed and credential-free when no ``@`` is present — return it
        # usable so the receipt can still audit a legitimate local dev path.
        if not parsed.scheme:
            return REDACTED_UNPARSEABLE_GIT_SPEC
        if not parsed.netloc:
            if parsed.scheme.lower() == "file" and "@" not in url:
                return raw
            return REDACTED_UNPARSEABLE_GIT_SPEC

        # Path must never carry userinfo into the receipt. Double-scheme and
        # other malformed forms put ``user:SECRET@host`` into ``.path`` while
        # ``("@" in .netloc)`` is False; re-attaching path verbatim was the leak.
        # Credential-free paths that contain mid-segment ``@`` (e.g. ``re@po.git``)
        # are also redacted. A sole trailing ``.git@REV`` pin is allowed so
        # existing uv member specs remain usable.
        if _path_may_carry_userinfo(parsed.path or ""):
            return REDACTED_UNPARSEABLE_GIT_SPEC

        clean_netloc = _netloc_without_userinfo(parsed)
        cleaned = f"{parsed.scheme}://{clean_netloc}{parsed.path}"
        # Drop RFC 3986 ``params`` (``;…``): unused by uv git specs and unsafe to
        # re-attach verbatim under a fail-closed receipt contract.
        safe_query = _scrub_query_or_fragment(parsed.query)
        if safe_query:
            cleaned = f"{cleaned}?{safe_query}"
        safe_fragment = _scrub_query_or_fragment(parsed.fragment)
        if safe_fragment:
            cleaned = f"{cleaned}#{safe_fragment}"
        return f"{prefix}{cleaned}"
    except ValueError:
        # urlparse raises ValueError on malformed IPv6 authorities, etc.
        # Fail closed: placeholder, never the raw input, never an uncaught raise.
        return REDACTED_UNPARSEABLE_GIT_SPEC


def _parse_gitplus_member_spec(member: str, raw: str) -> tuple[str, str, str] | None:
    """Parse a legacy ``git+…@rev#subdirectory=…`` field into (base, rev, sub).

    Returns ``None`` when the field lacks a grounded ``@rev`` or names a
    subdirectory other than ``packages/<member>``. Never accepts a revless
    pin (default-branch install is the wrong-version-silently trap).
    """
    body = raw[4:] if raw.startswith("git+") else raw
    if "#" in body:
        url_part, fragment = body.split("#", 1)
    else:
        url_part, fragment = body, ""
    # Rev sits after the final ``@`` that is not userinfo. Userinfo is only
    # present when ``://`` is followed by ``user[:pass]@host``; the rev ``@``
    # is the last ``@`` in the URL part for both credential-free and
    # userinfo-bearing forms once host is present.
    if "@" not in url_part:
        return None
    # Split scheme://netloc/path@rev carefully: find the rev marker after path.
    scheme_sep = url_part.find("://")
    if scheme_sep < 0:
        return None
    after_scheme = url_part[scheme_sep + 3 :]
    # Host ends at first ``/``; rev ``@`` is after that path segment.
    slash = after_scheme.find("/")
    if slash < 0:
        # No path — refuse (need …/repo.git@rev).
        return None
    host_and_path = after_scheme
    # Last @ in the full url_part is the rev separator for uv git+ specs.
    rev_at = url_part.rfind("@")
    # Ensure the rev @ is after the scheme and inside/after the path, not the
    # userinfo @ (userinfo @ sits before the first / of the path).
    first_slash_abs = scheme_sep + 3 + slash
    if rev_at <= first_slash_abs:
        # Only userinfo @ present, no rev.
        return None
    base_url = url_part[:rev_at]
    rev = url_part[rev_at + 1 :]
    if not rev or not base_url:
        return None
    # Fragment: subdirectory=packages/<member>
    sub = ""
    for part in fragment.split("&"):
        if part.startswith("subdirectory="):
            sub = unquote(part[len("subdirectory=") :])
            break
    expected_sub = f"packages/{member}"
    if not sub:
        sub = expected_sub
    if sub.rstrip("/") != expected_sub:
        return None
    parsed = urlparse(base_url)
    if not parsed.scheme or not parsed.netloc:
        return None
    netloc = _netloc_without_userinfo(parsed)
    clean_base = f"{parsed.scheme}://{netloc}{parsed.path}"
    return clean_base, rev, expected_sub


def _git_url_field_to_member_spec(member: str, git_field: str) -> str | None:
    """Convert a uv-receipt ``git = "url?subdirectory=…&rev=…"`` field to a uv spec.

    Receipt shape (observed from ``uv tool install``)::

        git = "https://github.com/…/workbay.git?subdirectory=packages%2Fpkg&rev=v0.1.55"

    becomes::

        git+https://github.com/…/workbay.git@v0.1.55#subdirectory=packages/pkg

    Also accepts a legacy ``git+…@rev#subdirectory=…`` form when it carries an
    explicit ``@rev`` (revless pins are refused). Returns ``None`` when the
    field is not a grounded git pin for *member*. Password userinfo is always
    stripped; only the literal ``git`` username is preserved (value-keyed).
    """
    if member not in _SPECABLE_PACKAGES:
        return None
    raw = git_field.strip()
    if not raw:
        return None
    # Legacy full git+…@rev#subdirectory=… form: parse properly (require rev,
    # scrub credentials) rather than passthrough (D9).
    if raw.startswith("git+") and f"packages/{member}" in raw:
        parsed_plus = _parse_gitplus_member_spec(member, raw)
        if parsed_plus is None:
            return None
        base, rev, subdirectory = parsed_plus
        return f"git+{normalize_git_remote_url(base)}@{rev}#subdirectory={subdirectory}"
    parsed = urlparse(raw)
    if not parsed.scheme:
        return None
    query = parse_qs(parsed.query)
    rev_vals = query.get("rev") or query.get("tag") or []
    sub_vals = query.get("subdirectory") or []
    if not rev_vals:
        return None
    rev = rev_vals[0]
    subdirectory = unquote(sub_vals[0]) if sub_vals else f"packages/{member}"
    # Expected monorepo layout; refuse a pin that names a different package path.
    expected_sub = f"packages/{member}"
    if subdirectory.rstrip("/") != expected_sub:
        return None
    # Rebuild base URL without query/fragment and without credential userinfo.
    netloc = _netloc_without_userinfo(parsed)
    base = f"{parsed.scheme}://{netloc}{parsed.path}"
    return f"git+{normalize_git_remote_url(base)}@{rev}#subdirectory={subdirectory}"


def _directory_field_to_member_spec(member: str, directory: str) -> str | None:
    """Accept a directory pin only when the package path still exists on disk.

    A stale path from a moved/reaped worktree is ungrounded (None), not a
    resolved spec (D8). Mirrors the overlay-clone probe that requires
    ``packages/<member>/pyproject.toml``.
    """
    if member not in _SPECABLE_PACKAGES:
        return None
    path = Path(directory).expanduser()
    if path.name != member:
        return None
    resolved = path.resolve() if path.is_absolute() else path
    if not (resolved / "pyproject.toml").is_file():
        return None
    return str(resolved) if path.is_absolute() else str(path)


def _parent_repo_root_from_member_dir(member_dir: Path) -> Path | None:
    """Resolve monorepo root from a ``…/packages/<member>`` directory pin.

    Returns ``None`` when the path is not under a ``packages/`` parent, so
    callers can refuse ungrounded layouts rather than inventing a root.
    """
    # Prefer the on-disk absolute form for identity; relative pins keep layout.
    path = member_dir.resolve() if member_dir.is_absolute() else member_dir
    if path.parent.name != "packages":
        return None
    root = path.parent.parent
    return root.resolve() if root.is_absolute() or path.is_absolute() else root


def _git_base_from_field(git_field: str) -> tuple[str, str] | None:
    """Extract scrubbed (repo_url, rev) from a receipt git field, or None."""
    raw = git_field.strip()
    if not raw:
        return None
    if raw.startswith("git+"):
        # Recover base+rev without needing the member name for path check;
        # subdirectory is not part of the repo base.
        body = raw[4:]
        url_part = body.split("#", 1)[0]
        scheme_sep = url_part.find("://")
        if scheme_sep < 0 or "@" not in url_part:
            return None
        slash = url_part.find("/", scheme_sep + 3)
        rev_at = url_part.rfind("@")
        if slash < 0 or rev_at <= slash:
            return None
        base_url = url_part[:rev_at]
        rev = url_part[rev_at + 1 :]
        if not rev:
            return None
        parsed = urlparse(base_url)
        if not parsed.scheme or not parsed.netloc:
            return None
        netloc = _netloc_without_userinfo(parsed)
        return f"{parsed.scheme}://{netloc}{parsed.path}", rev
    parsed = urlparse(raw)
    query = parse_qs(parsed.query)
    rev_vals = query.get("rev") or query.get("tag") or []
    if not rev_vals or not parsed.scheme:
        return None
    netloc = _netloc_without_userinfo(parsed)
    base = f"{parsed.scheme}://{netloc}{parsed.path}"
    return base, rev_vals[0]


def member_specs_from_uv_receipt(
    receipt_path: Path | None = None,
    *,
    tools_root: Path | None = None,
) -> dict[str, str] | None:
    """Build member specs from the front-door tool's grounded ``uv-receipt.toml``.

    The receipt records the exact git URL+rev (or local directory) used to install
    each ``--with`` member of ``workbay``. That is the only package-mode pin that
    is *not* a reconstructed guess: hardcoding the public repo URL or deriving a
    tag from ``__version__`` is forbidden (wrong-version-silently trap).

    Returns ``None`` when the receipt is missing, unreadable, lacks grounded
    specs for every required runtime/MCP member, pins mixed (repo_url, rev)
    pairs across members (D7), mixes git and directory source kinds (or
    directory pins that resolve to more than one parent repo root), or names
    directory pins that no longer exist on disk (D8). The bridge extra is
    synthesized from the single shared git base when absent (orchestrator
    ``[bridge]`` extra) — never by majority vote.
    """
    path = (
        Path(receipt_path)
        if receipt_path is not None
        else front_door_uv_receipt_path(tools_root=tools_root)
    )
    if not path.is_file():
        return None
    try:
        data = tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError):
        return None
    tool = data.get("tool")
    if not isinstance(tool, dict):
        return None
    requirements = tool.get("requirements")
    if not isinstance(requirements, list):
        return None

    specs: dict[str, str] = {}
    git_bases: list[tuple[str, str]] = []  # (repo_url, rev) for bridge synthesis
    source_kinds: set[str] = set()
    dir_repo_roots: set[str] = set()
    for entry in requirements:
        if not isinstance(entry, dict):
            continue
        name = entry.get("name")
        if not isinstance(name, str) or name not in _SPECABLE_PACKAGES:
            continue
        git_field = entry.get("git")
        directory = entry.get("directory")
        if isinstance(git_field, str) and git_field.strip():
            spec = _git_url_field_to_member_spec(name, git_field)
            if spec is None:
                continue
            specs[name] = spec
            source_kinds.add("git")
            base_pair = _git_base_from_field(git_field)
            if base_pair is not None:
                git_bases.append(base_pair)
        elif isinstance(directory, str) and directory.strip():
            spec = _directory_field_to_member_spec(name, directory)
            if spec is not None:
                specs[name] = spec
                source_kinds.add("directory")
                root = _parent_repo_root_from_member_dir(Path(spec))
                if root is None:
                    # Not under packages/ — each such pin is its own incoherent root.
                    dir_repo_roots.add(str(Path(spec).resolve()))
                else:
                    dir_repo_roots.add(str(root))

    # Every runtime member must be grounded; MCP tools install needs the full
    # --with closure (plus bridge for orchestrator).
    missing_runtime = [m for m in GITONLY_RUNTIME_MEMBERS if m not in specs]
    if missing_runtime:
        return None

    # Coherence: all runtime members must come from one source kind and one
    # grounding identity. Mixed git+directory (local patch of one member while
    # others stay git-pinned) or directory pins from two different parent
    # checkouts install versions the operator did not ask for — fail closed.
    if "git" in source_kinds and "directory" in source_kinds:
        return None
    if len(dir_repo_roots) > 1:
        return None

    # Mixed (repo_url, rev) across members is a wrong-version trap — fail closed
    # rather than majority-voting a bridge rev (D7).
    unique_git_bases = set(git_bases)
    if len(unique_git_bases) > 1:
        return None

    if GITONLY_BRIDGE_MEMBER not in specs:
        if unique_git_bases:
            repo_url, rev = next(iter(unique_git_bases))
            specs[GITONLY_BRIDGE_MEMBER] = git_member_spec(
                GITONLY_BRIDGE_MEMBER, repo_url=repo_url, tag=rev
            )
        else:
            # Path-only receipt: derive bridge as sibling of any known member dir.
            # Refuse URL-shaped samples and missing on-disk paths (D8/D9).
            sample = next(iter(specs.values()), None)
            if sample is None:
                return None
            sample_path = Path(sample)
            if not sample_path.is_absolute() or not sample_path.is_dir():
                return None
            # …/packages/<member> → …/packages/workbay-codex-bridge
            if sample_path.name in _SPECABLE_PACKAGES and sample_path.parent.name == "packages":
                bridge_path = sample_path.parent / GITONLY_BRIDGE_MEMBER
                if not (bridge_path / "pyproject.toml").is_file():
                    return None
                specs[GITONLY_BRIDGE_MEMBER] = str(bridge_path)
            else:
                return None

    return {member: specs[member] for member in _SPEC_MAP_MEMBERS if member in specs}


# Single-sourced from the brand SSOT — never a hand-copied literal (PPSSOT-URL-01).
DEFAULT_GIT_REPO_URL = REPO_HTTPS_GIT_URL


def _looks_like_scp_host_path(host: str, path: str) -> bool:
    """True when ``host:path`` is scp-like, not an opaque ``scheme:body`` URI.

    The host:path scp fallback must not rewrite arbitrary scheme-opaque strings
    (e.g. ``data:text/plain,PAYLOAD`` → ``ssh://data/text/plain,PAYLOAD``). Real
    scp remotes have a path that is absolute/home-relative, ends in ``.git``,
    or sit under a hostname-shaped left side (FQDN / localhost).
    """
    if "/" not in path:
        return False
    # Opaque URI bodies often carry media types / comma-separated payloads.
    if "," in path:
        return False
    if path.startswith(("/", "~")):
        return True
    stripped = path.rstrip("/")
    if stripped.endswith(".git") or ".git/" in path:
        return True
    # Hostname-shaped authority: FQDN or localhost with a multi-segment path.
    if "." in host or host.lower() == "localhost":
        return True
    return False


def normalize_git_remote_url(url: str) -> str:
    """Normalize an scp-style SSH remote to a uv-parseable ``ssh://`` URL.

    uv's ``git+`` parser rejects the scp shorthand
    ``git@github.com:darce/workbay.git`` ("Expected path to end in a supported
    file extension") and requires ``ssh://git@github.com/darce/workbay.git``.
    URLs already carrying a scheme (``https://``, ``ssh://``, ``file:``) and
    non-scp strings pass through unchanged.

    Schemeless slash form with userinfo (``user:token@host/path``) is *not*
    scp and must not be rewritten: the old optional-user regex backtracked into
    ``ssh://user/token@host/path``, folding the secret into the path. Leave it
    scheme-less so the fail-closed scrubber can refuse it.

    Opaque non-git schemes (``data:text/plain,…``) are also left unchanged so
    the fail-closed scrubber can return the redacted placeholder rather than a
    bogus ``ssh://`` rewrite.
    """
    if "://" in url or url.startswith("file:"):
        return url
    # scp with userinfo: user[:pass]@host:path  (colon after host, not only in userinfo)
    match = re.match(
        r"^(?P<user>[^@]+)@(?P<host>[^:/]+):(?P<path>.+)$",
        url,
    )
    if match and _looks_like_scp_host_path(match.group("host"), match.group("path")):
        return f"ssh://{match.group('user')}@{match.group('host')}/{match.group('path')}"
    # scp without userinfo: host:path (refuse if @ present — ambiguous / non-scp)
    if "@" not in url:
        match = re.match(r"^(?P<host>[^:/]+):(?P<path>.+)$", url)
        if match and _looks_like_scp_host_path(match.group("host"), match.group("path")):
            return f"ssh://{match.group('host')}/{match.group('path')}"
    return url


def git_member_spec(
    member: str,
    *,
    repo_url: str = DEFAULT_GIT_REPO_URL,
    tag: str,
) -> str:
    if member not in _SPECABLE_PACKAGES:
        raise ValueError(f"unknown gitonly package: {member}")
    return f"git+{normalize_git_remote_url(repo_url)}@{tag}#subdirectory=packages/{member}"


def path_member_spec(repo_root: Path, member: str) -> str:
    if member not in _SPECABLE_PACKAGES:
        raise ValueError(f"unknown gitonly package: {member}")
    return str((repo_root / "packages" / member).resolve())


def build_uv_tool_install_argv(
    *,
    package: str,
    from_spec: str,
    member_specs: dict[str, str],
    no_cache: bool = False,
    force: bool = True,
    package_extras: tuple[str, ...] = (),
) -> list[str]:
    # Trust boundary: argv is the *trusted* side. Credentials in from_spec /
    # member_specs are REQUIRED here so ``uv tool install`` can clone a private
    # member. Do NOT call scrub_git_spec_userinfo on these values — scrubbing
    # belongs only on the receipt / manifest write path (untrusted side).
    #
    # ``--no-cache`` is OFF by default: ``--force`` already guarantees a fresh
    # reinstall, while leaving the cache reusable lets warm-cache / offline
    # hosts complete without re-fetching every member over the network. Callers
    # may still opt into ``no_cache=True`` for a hard, cache-bypassing install.
    if package not in GITONLY_CLOSURE_PACKAGES:
        raise ValueError(f"unknown gitonly package: {package}")
    # ``--no-sources`` is mandatory, not optional: every shipped member pyproject
    # carries ``[tool.uv.sources] { workspace = true }`` for in-tree dev builds.
    # A consumer ``uv tool install --from git+…#subdirectory=packages/<pkg>`` has
    # no workspace root, so uv rejects those entries ("references a workspace …
    # but is not a workspace member") and the whole git-only install fails. With
    # ``--no-sources`` uv ignores ``[tool.uv.sources]`` and resolves the closure
    # from the explicit ``--with`` git/path specs below (the Q4 mechanism). This
    # is equally correct for the local-path dev install, where the ``--with``
    # paths already pin every member.
    argv = ["tool", "install", "--no-sources"]
    if no_cache:
        argv.append("--no-cache")
    if force:
        argv.append("--force")
    with_members: list[str] = [m for m in GITONLY_RUNTIME_MEMBERS if m != package]
    for member in GITONLY_PACKAGE_EXTRA_MEMBERS.get(package, ()):
        if member != package and member not in with_members:
            with_members.append(member)
    for member in with_members:
        argv.extend(["--with", member_specs[member]])
    # Same-package extras (e.g. handoff ``[embeddings]``) use PEP 508 name-with-
    # extras on ``--from``: ``name[extra] @ <git/path-spec>``. Appending the
    # extra to the URL tail is rejected by uv (F6). Empty extras keep the bare
    # from_spec so default installs stay byte-identical.
    if package_extras:
        extras = ",".join(package_extras)
        from_value = f"{package}[{extras}] @ {from_spec}"
    else:
        from_value = from_spec
    argv.extend(["--from", from_value, package])
    return argv


def member_sources_are_local_or_git(install_output: str) -> bool:
    return all(
        _member_resolved_from_git_or_path(member, install_output)
        for member in GITONLY_CLOSURE_PACKAGES
    )


def _member_resolved_from_git_or_path(member: str, output: str) -> bool:
    pattern = rf"{re.escape(member)}==.*\(from (?:file:|git\+)"
    return re.search(pattern, output) is not None


def installed_members_are_local_or_git(install_output: str, *members: str) -> bool:
    """Check only the named packages (subset of the closure)."""
    return all(_member_resolved_from_git_or_path(member, install_output) for member in members)
