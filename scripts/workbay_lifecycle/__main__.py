"""Entry point: ``python <abs-path>/scripts/workbay_lifecycle ...``.

Delegates to :func:`lifecycle.cli.main`. Kept intentionally thin so the
dispatch table stays in :mod:`cli` and is testable without the
``-m`` / package-as-script invocation indirection.

Before any handler import work, re-exec under the workspace ``.venv`` when
the ambient interpreter differs — so ``make`` *and* a direct
``python3 scripts/workbay_lifecycle ...`` both land on a deps-bearing
interpreter for in-process handler imports.
"""

from __future__ import annotations

import os
import stat
import subprocess
import sys
from pathlib import Path


_REEXEC_SENTINEL = "WORKBAY_LIFECYCLE_REEXEC"
_REEXEC_DISABLE = "WORKBAY_LIFECYCLE_NO_REEXEC"
# Accumulated typed degrade lines for the current process. Injected into the
# execve child env as _REEXEC_DEGRADED_ENV at re-exec time only — never written
# into ambient os.environ — so a process that never execs leaves no residual
# for later unrelated lifecycle children [OBS-08, AGT-10, RES-03]. Shared name
# lives in interpreter_skew so the receipt drain cannot drift from the emitter
# [ARCH-13].
try:
    from interpreter_skew import _REEXEC_DEGRADED_ENV
except ImportError:  # cold-start only; must match interpreter_skew
    _REEXEC_DEGRADED_ENV = "WORKBAY_LIFECYCLE_REEXEC_DEGRADED"
_REEXEC_DEGRADED_LINES: list[str] = []


def _repo_root_from_cwd() -> str:
    # Strip ambient git env via the same helper resolve_lifecycle_python uses so
    # hooks / rebase -x / CI cannot make the two probes disagree about which
    # repository owns this process [ARCH-13].
    try:
        from interpreter_skew import scrub_ambient_git_env
    except ImportError:
        return ""
    try:
        proc = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
            env=scrub_ambient_git_env(),
        )
    except (OSError, subprocess.SubprocessError):
        return ""
    if proc.returncode != 0:
        return ""
    return (proc.stdout or "").strip()



# ---------------------------------------------------------------------------
# Validated repo-root ascent.
#
# Canonical source (keep the three copies findable together) [REF-20]:
#   packages/mcp-workbay-handoff/src/workbay_handoff_mcp/backlog_triage.py
#   (_validate_headref / _is_git_directory / _ascend_to_repo_root)
# Duplicated here on purpose: this entrypoint must run before any package is
# installed, so it cannot import the handoff helper.
# ---------------------------------------------------------------------------

_GIT_HEADREF_WS = " \t\n\r"


def _validate_headref(head: Path) -> bool:
    """Port of git's ``validate_headref`` (refs.c): is *head* a well-formed HEAD.

    Accepts a symlink whose link text begins ``refs/`` (unresolved — a dangling
    symlink to ``refs/heads/main`` is a valid unborn branch), a regular file
    whose first 40 characters are hexadecimal (detached HEAD), or a regular
    file beginning ``ref:`` whose remainder, after leading space/tab/LF/CR only,
    begins ``refs/``. ``PermissionError`` is treated as success (unreadable entry
    pins as a repository; deliberate divergence from git, which walks past —
    this helper has no fatal error channel); other ``OSError`` fails.
    """
    try:
        st = os.lstat(head)
    except PermissionError:
        return True
    except OSError:
        return False

    mode = st.st_mode
    if stat.S_ISLNK(mode):
        try:
            link = os.readlink(head)
        except PermissionError:
            return True
        except OSError:
            return False
        return link.startswith("refs/")

    if not stat.S_ISREG(mode):
        return False

    try:
        with open(head, "rb") as fh:
            data = fh.read(255)
    except PermissionError:
        return True
    except OSError:
        return False

    text = data.decode("utf-8", errors="replace")
    if len(text) >= 40 and all(c in "0123456789abcdefABCDEF" for c in text[:40]):
        return True
    if text.startswith("ref:"):
        return text[4:].lstrip(_GIT_HEADREF_WS).startswith("refs/")
    return False


def _is_git_directory(suspect: Path) -> bool:
    """Port of git's ``is_git_directory`` (setup.c): three filesystem checks.

    Requires a well-formed ``HEAD`` via :func:`_validate_headref`, then that
    ``objects`` and ``refs`` are accessible under the *common* directory with
    ``os.access(..., X_OK)`` (git's probe — executable bit, not is-dir). The
    common directory defaults to *suspect* only when ``suspect/commondir`` is
    absent. When the entry is present it must be a readable regular file
    (after following a symlink): a zero-length file is refused (git dies with
    ``failed to read commondir``); otherwise the whole body is read and a
    trailing run of CR/LF only is stripped. A non-empty result names the
    common directory, resolved against *suspect* when relative; a body that
    is only trailing CR/LF leaves the default (*suspect*) in place (git
    accepts that shape). A present but unusable entry rejects the candidate
    (returns False): non-regular target (directory, FIFO, whether named
    directly or reached through a link — so ``open`` never blocks), broken
    symlink, undecodable body, or any other read/probe failure. Only a missing
    ``commondir`` entry keeps the default common directory. Linked worktrees
    hold ``HEAD`` + ``commondir`` without local ``objects``/``refs``; submodule
    gitdirs under ``.git/modules`` hold all three locally with no
    ``commondir``. ``PermissionError`` while reading ``commondir`` is the sole
    deliberate pin (returns True; git walks past — this helper has no fatal
    error channel); every other present-but-unusable probe failure rejects.
    """
    if not _validate_headref(suspect / "HEAD"):
        return False

    common = suspect
    try:
        commondir = suspect / "commondir"
        try:
            # Existence without following: a missing entry keeps the default
            # common dir. A present entry that cannot be opened as a regular
            # file must reject — not fall through as if the entry were absent.
            os.lstat(commondir)
        except FileNotFoundError:
            pass
        else:
            # Dereference deliberately: git's open() follows a symlink to a
            # regular file. S_ISREG still refuses a FIFO target (stat never
            # blocks on a FIFO; only open does), so the long-lived-server
            # hang guard survives for both a named FIFO and a link to one.
            try:
                cd_st = os.stat(commondir)
            except FileNotFoundError:
                # Broken symlink: entry exists, target does not.
                return False
            if not stat.S_ISREG(cd_st.st_mode):
                return False
            # Zero-length file: git refuses with "failed to read commondir".
            # Newlines-only (non-zero size, empty after CR/LF strip) keeps the
            # default common dir and must not take this arm.
            if cd_st.st_size == 0:
                return False
            with open(commondir, "rb") as fh:
                raw = fh.read().decode("utf-8")
            # Whole file, trailing CR/LF run only (not a first-line read; not
            # spaces/tabs). Matches git's commondir parse.
            body = raw.rstrip("\r\n")
            if body:
                common_path = Path(body)
                common = common_path if common_path.is_absolute() else suspect / common_path
    except PermissionError:
        return True
    except (OSError, UnicodeDecodeError, ValueError):
        # ValueError: NUL in the path must not abort the walk (not an OSError).
        # Present-but-unusable commondir rejects the candidate (git: failed to
        # read commondir); only PermissionError above is the deliberate pin.
        return False

    for name in ("objects", "refs"):
        try:
            # git probes with access(X_OK): accepts an executable regular file,
            # rejects mode-644 files and unsearchable (mode-000) directories.
            # os.access returns False on permission failure rather than raising.
            if not os.access(common / name, os.X_OK):
                return False
        except (OSError, ValueError):
            return False
    return True


def _ascend_to_repo_root(start: Path) -> Path | None:
    """Walk up from *start* and return the first ancestor that is a real repo.

    Ports the core of git's ``is_git_directory`` / ``validate_headref`` so litter
    ``.git`` shapes git refuses do not truncate the walk. Returns ``None`` when
    no repository is found (callers choose their own fallback).

    Normalization of *start* is lexical (``os.path.abspath``), deliberately not
    ``resolve``: the lifecycle entry point anchors on the path the operator
    invoked, so a package symlinked into a checkout still re-execs under *that*
    checkout rather than the symlink target's [ARCH-13]. Do not "sync" this line
    with the copy in ``scripts/dev_install.py``, whose caller resolves first
    because editable-install redirects want the physical root -- the split is
    intentional. A caller needing ``..`` collapsed *after* following symlinks
    must resolve before calling.

    Deliberate divergence from git, carried from the canonical source: a
    ``PermissionError`` while reading ``commondir`` pins the candidate (returns
    True from :func:`_is_git_directory`) because this helper has no fatal error
    channel; git walks past.
    """
    try:
        base = Path(os.path.abspath(start))
    except (OSError, RuntimeError, ValueError):
        return None
    for candidate in (base, *base.parents):
        try:
            git_entry = candidate / ".git"
            try:
                entry_st = os.lstat(git_entry)
            except FileNotFoundError:
                continue
            except PermissionError:
                # Unreadable .git (or unsearchable candidate) pins only when
                # the candidate itself is reachable. PermissionError under a
                # deeper path means an unreadable *ancestor* — keep walking
                # until that ancestor is the candidate.
                try:
                    os.lstat(candidate)
                except PermissionError:
                    continue
                except OSError:
                    continue
                return candidate
            except OSError:
                continue

            entry_mode = entry_st.st_mode
            if stat.S_ISLNK(entry_mode):
                # A .git symlink denotes a repository when it resolves, so
                # the type comes from a dereferencing probe; lstat above
                # reports the link itself and matches neither arm below.
                try:
                    entry_mode = os.stat(git_entry).st_mode
                except FileNotFoundError:
                    continue
                except PermissionError:
                    return candidate
                except OSError:
                    continue

            # Linked worktree: .git is a file with a gitdir: pointer.
            if stat.S_ISREG(entry_mode):
                try:
                    # Binary read so universal-newlines cannot swallow CR as
                    # a newline; we only rstrip an explicit CR/LF run below.
                    text = git_entry.read_bytes().decode("utf-8")
                except PermissionError:
                    return candidate
                except (OSError, UnicodeDecodeError):
                    continue
                # Exact gitfile prefix at byte zero (git rejects all variants).
                # Whole buffer after prefix; strip only a trailing CR/LF run
                # (spaces/tabs stay; interior newlines stay — same as
                # commondir). First-line partition would false-accept a valid
                # path followed by a second-line garbage that git refuses.
                text = text.rstrip("\r\n")
                if not text.startswith("gitdir: "):
                    continue
                payload = text[len("gitdir: ") :]
                if not payload:
                    continue
                try:
                    target = Path(payload)
                    if not target.is_absolute():
                        target = candidate / target
                    if not _is_git_directory(target):
                        continue
                except (OSError, ValueError):
                    continue
                return candidate

            # Primary checkout: .git is a directory; require is_git_directory
            # (well-formed HEAD + common objects/refs), not mere HEAD presence.
            if stat.S_ISDIR(entry_mode):
                if not _is_git_directory(git_entry):
                    continue
                return candidate
        except OSError:
            continue
    return None


def _repo_root_from_script() -> tuple[str, bool]:
    """Return ``(root, used_cwd_fallback)`` for the re-exec target.

    Prefer the checkout that owns this file (walk up from ``__file__``) so a
    ``python /repoA/scripts/workbay_lifecycle ...`` launched from ``/repoB``
    cannot re-exec under ``/repoB``'s interpreter [ARCH-13]. Use
    ``abspath`` rather than ``resolve`` so a symlinked package still anchors
    on the path the operator invoked. Fall back to cwd only when the script
    sits outside any checkout.

    The walk validates each ``.git`` candidate (see :func:`_ascend_to_repo_root`)
    so litter shapes between this file and the true root cannot defeat the
    ownership guarantee.
    """
    start = Path(os.path.abspath(__file__)).parent
    found = _ascend_to_repo_root(start)
    if found is not None:
        return str(found), False
    return _repo_root_from_cwd(), True


# Ambient Python import/venv surface stripped from every re-exec child.
# Unconditional: resolving a workspace interpreter means the ambient Python
# environment is not trusted, isolation flags or not [OBS-08].
_REEXEC_PYTHON_ENV_KEYS = (
    "PYTHONPATH",
    "PYTHONHOME",
    "VIRTUAL_ENV",
)


def _python_flag_argv() -> list[str]:
    """Reconstruct interpreter flags that isolation-sensitive launches need.

    ``os.execve(resolved, [resolved, *sys.argv], ...)`` drops ``-I`` / ``-E``
    / ``-s`` / ``-S`` / ``-P`` / ``-W`` / ``-X`` while still inheriting
    ``PYTHONPATH`` from the environment copy — silently widening the import
    surface the original flags were suppressing [ARCH-13].
    """
    flags: list[str] = []
    f = sys.flags
    if f.isolated:
        flags.append("-I")
    else:
        if f.ignore_environment:
            flags.append("-E")
        if f.no_user_site:
            flags.append("-s")
    # -I does not imply -S; reconstruct no_site independently [9414].
    if getattr(f, "no_site", False):
        flags.append("-S")
    if getattr(f, "safe_path", False):
        flags.append("-P")
    if f.dont_write_bytecode:
        flags.append("-B")
    if f.optimize == 1:
        flags.append("-O")
    elif f.optimize >= 2:
        flags.append("-OO")
    if f.bytes_warning == 1:
        flags.append("-b")
    elif f.bytes_warning >= 2:
        flags.append("-bb")
    if f.verbose:
        flags.append("-v" if f.verbose == 1 else "-vv")
    if f.quiet:
        flags.append("-q")
    for opt in sys.warnoptions:
        flags.extend(["-W", opt])
    for key, value in getattr(sys, "_xoptions", {}).items():
        if value is True:
            flags.extend(["-X", str(key)])
        else:
            flags.extend(["-X", f"{key}={value}"])
    return flags


def _reexec_env() -> dict[str, str]:
    """Build the child env so it matches the git world used for interpreter choice.

    Starts from ``scrub_ambient_git_env(os.environ)`` rather than a raw
    ``os.environ`` copy: probes deliberately strip ambient ``GIT_*`` before
    choosing the interpreter, and the child must not reintroduce those
    overrides [ARCH-13]. Then set the re-exec sentinel and always strip the
    ambient Python import/venv surface. When ``interpreter_skew`` is
    unavailable, still strip the same ``_GIT_ENV_OVERRIDE_KEYS`` inline so
    the child is never an unscrubbed passthrough [OBS-08].
    """
    try:
        from interpreter_skew import scrub_ambient_git_env as _scrub_git
    except ImportError:
        _scrub_git = None
    try:
        # Single named constant shared with scrub_ambient_git_env [ARCH-13].
        from interpreter_skew import _GIT_ENV_OVERRIDE_KEYS as _git_keys
    except ImportError:
        # Cold-start only. Must match interpreter_skew._GIT_ENV_OVERRIDE_KEYS.
        _git_keys = (
            "GIT_DIR",
            "GIT_COMMON_DIR",
            "GIT_WORK_TREE",
            "GIT_OBJECT_DIRECTORY",
            "GIT_ALTERNATE_OBJECT_DIRECTORIES",
            "GIT_INDEX_FILE",
            "GIT_NAMESPACE",
            "GIT_CEILING_DIRECTORIES",
        )
    if _scrub_git is not None:
        # Pass the live environ so an explicit source is documented; the helper
        # defaults to os.environ when called with no args [ARCH-13].
        env = _scrub_git(os.environ)
    else:
        env = os.environ.copy()
    # Both scrub_ambient_git_env and this fallback consume the same named
    # constant so the two paths cannot drift; the pops are idempotent when
    # scrub already ran [ARCH-13, OBS-08].
    for key in _git_keys:
        env.pop(key, None)
    # Tag the sentinel with this process id. execve replaces the process image
    # without forking, so the post-exec process keeps the same pid: a matching
    # "1:<pid>" means our own successful heal, not a foreign ambient export
    # [IRV-01, OBS-08].
    env[_REEXEC_SENTINEL] = f"1:{os.getpid()}"
    # Ambient Python env is never trusted for a workspace-interpreter heal.
    # Strip unconditionally — isolation flags or not [OBS-08].
    for key in _REEXEC_PYTHON_ENV_KEYS:
        env.pop(key, None)
    # Carry typed degrade lines only through the execve child env — never the
    # ambient process os.environ. An emit that never reaches execve must leave
    # no residual for later unrelated lifecycle children to inherit [RES-03].
    if _REEXEC_DEGRADED_LINES:
        existing = env.get(_REEXEC_DEGRADED_ENV, "")
        parts = [p for p in existing.splitlines() if p] if existing else []
        for line in _REEXEC_DEGRADED_LINES:
            if line not in parts:
                parts.append(line)
        if parts:
            env[_REEXEC_DEGRADED_ENV] = "\n".join(parts)
    return env


def _env_flag_enabled(raw: str | None) -> bool:
    """Shared truthiness for re-exec sentinel and disable flags [ARCH-13]."""
    return (raw or "").strip().lower() in {"1", "true", "yes", "on"}


def _sentinel_is_own_heal(raw: str | None) -> bool:
    """True when *raw* is this process's post-execve heal tag [IRV-01].

    ``_reexec_env`` writes ``1:<pid>``. Because ``execve`` does not fork, the
    process that resumes after a successful heal has the same ``os.getpid()``.
    """
    text = (raw or "").strip()
    if not text.startswith("1:"):
        return False
    pid_s = text[2:]
    return pid_s.isdigit() and int(pid_s) == os.getpid()


def _sentinel_blocks_heal(raw: str | None) -> bool:
    """True when a non-self sentinel should skip a further heal attempt [IRV-01].

    Own successful heal (``1:<same-pid>``) is not a block. Ambient truthy values
    (``1`` / ``true`` / …), a different pid, or a malformed ``1:…`` tag are.
    """
    text = (raw or "").strip()
    if not text:
        return False
    if _sentinel_is_own_heal(text):
        return False
    if text.startswith("1:"):
        return True
    return _env_flag_enabled(text)


def _same_interpreter(path_a: str, path_b: str) -> bool:
    """True when *path_a* and *path_b* name the same interpreter [IRV-02].

    Venv layouts expose ``bin/python``, ``bin/python3``, and ``bin/python3.N``
    as sibling names for one binary; ``sys.executable`` keeps the invoked name.
    ``abspath`` alone spuriously treats those as different interpreters.
    Prefer ``samefile``; fall back to ``realpath`` when either path is missing.
    """
    try:
        return os.path.samefile(path_a, path_b)
    except OSError:
        return os.path.realpath(path_a) == os.path.realpath(path_b)


def _emit_reexec_degraded(cause: str) -> None:
    """Typed degraded line so a failed heal is never silent [OBS-08].

    Accumulates the full line in-process only. ``_reexec_env`` injects the
    joined value into the execve child env so a genuine re-exec child (and its
    receipt drain) can surface parent causes without reading stderr — without
    mutating ambient ``os.environ`` of a process that never execs [AGT-10,
    OBS-08, RES-03].
    """
    line = f"lifecycle_reexec: degraded: {cause}"
    sys.stderr.write(line + "\n")
    _REEXEC_DEGRADED_LINES.append(line)


def _probe_python_identity(candidate: str) -> str | None:
    """Prove *candidate* is a Python 3 interpreter before execve.

    ``isfile`` + ``X_OK`` only prove the path is an executable file. A shell
    script, broken venv shim, or arbitrary binary must not receive the full
    lifecycle argv. Returns ``None`` on success, else a degraded cause string
    naming the path and reason [OBS-08].
    """
    script = (
        "import sys; "
        "print(f'{sys.version_info.major}.{sys.version_info.minor}')"
    )
    # Drop ambient Python surface for the probe: PYTHONHOME poison would make
    # a good interpreter look broken. The probe measures the candidate, not
    # the parent environment that re-exec is about to discard [OBS-08].
    probe_env = os.environ.copy()
    for key in _REEXEC_PYTHON_ENV_KEYS:
        probe_env.pop(key, None)
    try:
        proc = subprocess.run(
            [candidate, "-c", script],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=5,
            check=False,
            env=probe_env,
        )
    except subprocess.TimeoutExpired:
        return f"interpreter identity probe timed out path={candidate!r}"
    except OSError as exc:
        return (
            f"interpreter identity probe failed path={candidate!r}: "
            f"{type(exc).__name__}: {exc}"
        )
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout or "").strip()
        return (
            f"interpreter identity probe exit={proc.returncode} "
            f"path={candidate!r} detail={detail!r}"
        )
    lines = (proc.stdout or "").strip().splitlines()
    if not lines:
        return f"interpreter identity probe empty output path={candidate!r}"
    text = lines[0].strip()
    try:
        major_s, minor_s = text.split(".", 1)
        major = int(major_s)
        int(minor_s)  # require a parseable minor
    except ValueError:
        return (
            f"interpreter identity probe unparseable path={candidate!r} "
            f"output={text!r}"
        )
    if major < 3:
        return (
            f"interpreter major version {major} < 3 path={candidate!r}"
        )
    return None


def _maybe_reexec_under_workspace_venv() -> None:
    """Re-exec under the resolved lifecycle interpreter when needed.

    Guarded by ``WORKBAY_LIFECYCLE_REEXEC`` against infinite loops.
    ``WORKBAY_LIFECYCLE_NO_REEXEC`` disables the heal for debugging.
    Both the already-re-exec'd and disabled arms emit a typed degraded line
    so an operator can see why the heal did not run [OBS-08].

    Precedence for the heal target [AGT-20]:
    1. ``LIFECYCLE_PYTHON`` when set and non-empty — operator override,
       exempt from checkout containment (may live outside the tree).
       Still requires a present executable regular file (``isfile`` +
       ``X_OK``), not merely a searchable path. When the pin fails that
       gate, emit a degraded line naming ``LIFECYCLE_PYTHON`` and fall
       back to (2).
    2. ``resolve_lifecycle_python(root)`` — workspace ``.venv`` discovery.

    After the file gate, prove the candidate is a Python 3 interpreter via a
    short identity probe; a failed probe never falls through to execve.

    Re-exec failures emit a typed degraded line then return so the CLI
    continues under the current interpreter rather than dying because of
    the heal attempt — but silence is not success [OBS-08].
    """
    # Pop the sentinel once consumed so it cannot leak to descendants that
    # inherit this process env without an explicit env= override [RES-03].
    sentinel_raw = os.environ.pop(_REEXEC_SENTINEL, "")
    # Own successful heal: sentinel is "1:<this-pid>" written by _reexec_env
    # before execve. Silent return — do not report a working heal as degraded
    # [IRV-01, OBS-08].
    if _sentinel_is_own_heal(sentinel_raw):
        return
    if _sentinel_blocks_heal(sentinel_raw):
        _emit_reexec_degraded(
            f"re-exec sentinel already set (WORKBAY_LIFECYCLE_REEXEC={sentinel_raw}); "
            "heal skipped"
        )
        return
    if _env_flag_enabled(os.environ.get(_REEXEC_DISABLE)):
        _emit_reexec_degraded(
            "re-exec disabled via WORKBAY_LIFECYCLE_NO_REEXEC; heal skipped"
        )
        return

    root, used_cwd_fallback = _repo_root_from_script()
    if not root:
        _emit_reexec_degraded(
            "no repo root resolvable from script path or cwd"
        )
        return
    if used_cwd_fallback:
        _emit_reexec_degraded(
            "repo root anchored on cwd fallback; script path is outside any checkout"
        )

    try:
        from interpreter_skew import resolve_lifecycle_python
    except ImportError as exc:
        _emit_reexec_degraded(
            f"interpreter_skew import failed: {type(exc).__name__}: {exc}"
        )
        return
    try:
        from interpreter_skew import resolve_lifecycle_python_detailed
    except ImportError as exc:
        resolve_lifecycle_python_detailed = None  # type: ignore[assignment]
        _emit_reexec_degraded(
            f"resolve_lifecycle_python_detailed import failed: "
            f"{type(exc).__name__}: {exc}"
        )

    def _resolve_with_cause(probe_root: str) -> tuple[str, str | None]:
        if resolve_lifecycle_python_detailed is not None:
            path, cause = resolve_lifecycle_python_detailed(probe_root)
            return (path or ""), cause
        return (resolve_lifecycle_python(probe_root) or ""), None

    resolved = ""
    resolve_cause: str | None = None
    override = os.environ.get("LIFECYCLE_PYTHON", "").strip()
    if override:
        # Operator pin: skip containment; require a real executable file, not
        # merely X_OK (directories are searchable/X_OK without being an
        # interpreter path) [9410].
        if os.path.isfile(override) and os.access(override, os.X_OK):
            resolved = override
            resolve_cause = None
        else:
            if not os.path.exists(override):
                _emit_reexec_degraded(
                    f"LIFECYCLE_PYTHON missing path={override!r}; "
                    "falling back to resolve_lifecycle_python"
                )
            elif not os.path.isfile(override):
                _emit_reexec_degraded(
                    f"LIFECYCLE_PYTHON not a file path={override!r}; "
                    "falling back to resolve_lifecycle_python"
                )
            else:
                _emit_reexec_degraded(
                    f"LIFECYCLE_PYTHON not executable path={override!r}; "
                    "falling back to resolve_lifecycle_python"
                )
            resolved, resolve_cause = _resolve_with_cause(root)
    else:
        resolved, resolve_cause = _resolve_with_cause(root)

    if not resolved:
        _emit_reexec_degraded(
            "resolve_lifecycle_python returned no lifecycle interpreter"
        )
        return
    # isfile + X_OK: a present-but-non-executable (or non-file) dangling
    # .venv/bin/python must not count as a viable heal target. When the
    # candidate fails this gate, say so — including the missing-path TOCTOU
    # arm — silence is not success [OBS-08, 9412]. (Operator override already
    # gated above; this covers resolve_lifecycle_python and TOCTOU.)
    if not (os.path.isfile(resolved) and os.access(resolved, os.X_OK)):
        if not os.path.exists(resolved):
            _emit_reexec_degraded(
                f"interpreter missing path={resolved!r}"
            )
        elif not os.path.isfile(resolved):
            _emit_reexec_degraded(
                f"interpreter not a file path={resolved!r}"
            )
        else:
            _emit_reexec_degraded(
                f"interpreter not executable path={resolved!r}"
            )
        return

    # -S suppresses site, so re-exec cannot supply site-packages. Preserve
    # the flag (dropping it would silently re-enable site against an explicit
    # operator -S) but never stay silent about the no-op heal — including on
    # a genuine ambient match, where the give-up return below would otherwise
    # swallow this warning [OBS-08].
    if getattr(sys.flags, "no_site", False):
        _emit_reexec_degraded(
            "launch under -S (no_site); re-exec to the resolved path cannot "
            "supply site-packages so dependencies will remain unavailable"
        )

    if _same_interpreter(resolved, sys.executable):
        # Give-up returns ambient and must not look like a genuine match:
        # emit the typed line naming the cause. A real match (cause is None)
        # stays silent [OBS-08, RES-13] — except the no_site arm above.
        # Sibling venv names (python vs python3) share one realpath and must
        # not force a pointless re-exec [IRV-02].
        if resolve_cause:
            _emit_reexec_degraded(resolve_cause)
        return

    probe_fail = _probe_python_identity(resolved)
    if probe_fail is not None:
        _emit_reexec_degraded(probe_fail)
        return

    env = _reexec_env()
    argv = [resolved, *_python_flag_argv(), *sys.argv]
    try:
        os.execve(resolved, argv, env)
    except OSError as exc:
        errno_part = f" errno={exc.errno}" if exc.errno is not None else ""
        _emit_reexec_degraded(
            f"execve failed path={resolved!r}{errno_part}: {type(exc).__name__}: {exc}"
        )
        return


if __name__ == "__main__":
    _maybe_reexec_under_workspace_venv()
    from cli import main

    sys.exit(main(sys.argv[1:]))
