"""Shared hook helper: resolve / re-exec under a deps-bearing interpreter.

Harness-agnostic. Every harness (Claude Code, VS Code, Codex, Cursor, Grok)
launches these hooks via a bare ``python3`` command -- see the per-harness
``*_command`` fields rendered from ``harness-protocol.yaml``. The hook
*scripts* are the single shared artifact all of those commands invoke, so the
interpreter hardening lives here (one place -> every harness) rather than in
the ~60 per-harness command strings.

A system Python upgrade can leave the launch ``python3`` without the workbay
runtime deps (``pydantic`` and friends), which silently turns stack-importing
hooks into no-ops. These helpers route hook work to the project ``.venv``
python, which the bootstrap install keeps synced. Repo root is resolved via
``git`` -- no vendor-specific env var.

Two entry points by hook shape:

- ``ensure_deps_interpreter()`` re-execs the *current* process under the venv.
  Use it for hooks that import the stack in-process (compaction, reinjection);
  they fire infrequently (Stop / SessionStart) so the re-exec cost is fine.
- ``resolve_deps_python()`` returns a deps-bearing interpreter path *without*
  re-execing. Use it for hooks that run the stack in a subprocess and fire on
  every event (e.g. record-file-touch on PostToolUse), where re-execing the
  whole hook per event would add latency.
"""

from __future__ import annotations

import ast
import importlib.abc
import importlib.util
import json
import math
import os
import shlex
import site
import subprocess
import sys
import threading
import time
from pathlib import Path

# Carries the hook's subprocess deadline across the venv re-exec (see
# ``ensure_deps_interpreter``); a ``time.monotonic()`` value.
_HOOK_DEADLINE_ENV = "WORKBAY_HOOK_SUBPROCESS_DEADLINE"
_SUBPROCESS_BUDGET_MAX_SECONDS = 600


class _SubprocessTimeoutBudget:
    """One wall-clock deadline for every subprocess wait in this hook invocation.

    ``take(cap)`` returns ``min(cap, time left)`` and charges nothing: the
    deadline alone bounds the total, so time a fast subprocess did not use stays
    available to the next one. A zero grant means: do not start the subprocess.
    Same shape as ``_SubprocessTimeoutBudget`` in ``guard-bash-main-branch.py``.
    """

    def __init__(self, seconds: float, deadline: float | None = None) -> None:
        self.seconds = seconds
        fresh = time.monotonic() + seconds
        self.deadline = fresh if deadline is None else min(fresh, deadline)
        self.exhausted_noted = False

    def take(self, cap: int | float) -> int | float:
        wall_left = self.deadline - time.monotonic()
        if wall_left <= 0:
            return 0
        return cap if cap <= wall_left else wall_left


# Set by ``ensure_deps_interpreter`` for the current pass, and kept for the rest
# of the process so the import finder's later checks draw from the same deadline.
# ``None`` keeps every wait at its own cap.
_SUBPROCESS_BUDGET: _SubprocessTimeoutBudget | None = None


def _seconds_text(seconds: int | float) -> str:
    return f"{seconds:g}"


def _bounded_timeout(cap: int | float, skipped: str | None = None) -> int | float:
    """``cap``, or less when the hook's subprocess budget is active; 0 means skip.

    The first zero grant in a process says so on stderr once, naming ``skipped``.
    """
    budget = _SUBPROCESS_BUDGET
    if budget is None:
        return cap
    granted = budget.take(cap)
    if granted <= 0 and skipped and not budget.exhausted_noted:
        budget.exhausted_noted = True
        sys.stderr.write(
            f"workbay: hook subprocess budget of {_seconds_text(budget.seconds)}s exhausted; "
            f"skipping {skipped} and any later subprocess wait\n"
        )
    return granted


def _is_bounded_seconds(value: object, upper: int | float) -> bool:
    """A real, finite number of seconds in ``(0, upper]``; bools and strings are not."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    try:
        return math.isfinite(value) and 0 < value <= upper
    except OverflowError:
        return False


def _carried_deadline(text: str | None) -> float | None:
    """The deadline the first pass set before ``os.execv``; None when unusable."""
    if text is None:
        return None
    try:
        deadline = float(text)
    except (TypeError, ValueError, OverflowError):
        deadline = None
    if deadline is None or not math.isfinite(deadline):
        rendered = repr(text)
        if len(rendered) > 40:
            rendered = rendered[:37] + "..."
        sys.stderr.write(f"hook_deadline_carried_invalid: value={rendered}\n")
        return None
    return deadline


def _start_subprocess_budget(seconds: object) -> _SubprocessTimeoutBudget | None:
    """Budget for this pass. A re-exec'd second pass may only shorten it.

    The carried deadline is read once and removed from the environment, so a
    process this hook starts later does not inherit it. An absent, non-finite
    or unparsable value starts a fresh budget of ``seconds``.
    """
    if seconds is None:
        return None
    carried_text = os.environ.pop(_HOOK_DEADLINE_ENV, None)
    if not _is_bounded_seconds(seconds, _SUBPROCESS_BUDGET_MAX_SECONDS):
        sys.stderr.write(
            f"subprocess_budget_seconds must be a number in (0, {_SUBPROCESS_BUDGET_MAX_SECONDS}]; "
            "using each subprocess's own timeout\n"
        )
        return None
    carried = _carried_deadline(carried_text) if os.environ.get("WORKBAY_HOOK_REEXEC") == "1" else None
    return _SubprocessTimeoutBudget(seconds, carried)


def _git_repo_root() -> str:
    timeout = _bounded_timeout(5, "git rev-parse")
    if timeout <= 0:
        return ""
    try:
        proc = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        if proc.returncode == 0:
            return proc.stdout.strip()
    except Exception:  # noqa: BLE001 -- best-effort discovery
        pass
    return ""


def _primary_checkout_root() -> str:
    """Root of the *primary* checkout (parent of the shared git dir).

    A linked worktree (feature / review / harness-session) often has no
    ``.venv`` of its own, but the primary checkout does. ``git rev-parse
    --git-common-dir`` resolves to the shared ``.git`` regardless of which
    worktree we are in; its parent is the primary checkout root. Empty on
    failure (graceful no-op).
    """
    timeout = _bounded_timeout(5, "git rev-parse")
    if timeout <= 0:
        return ""
    try:
        proc = subprocess.run(
            ["git", "rev-parse", "--path-format=absolute", "--git-common-dir"],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        if proc.returncode == 0:
            common = proc.stdout.strip()
            if common:
                return os.path.dirname(common)
    except Exception:  # noqa: BLE001 -- best-effort discovery
        pass
    return ""


def _venv_python(repo_root: str | None = None) -> str:
    """First existing project-venv interpreter, or "" if none found.

    Probes, in order: the git toplevel's ``.venv`` then the primary
    checkout's ``.venv`` (so a worktree lacking its own venv still heals),
    and within each both POSIX (``bin/python``) and Windows
    (``Scripts/python.exe``) layouts.
    """
    roots: list[str] = []
    root = repo_root if repo_root is not None else _git_repo_root()
    if root:
        roots.append(root)
    primary = _primary_checkout_root()
    if primary and primary not in roots:
        roots.append(primary)
    for base in roots:
        for rel in (("bin", "python"), ("Scripts", "python.exe")):
            candidate = os.path.join(base, ".venv", *rel)
            if os.path.exists(candidate):
                return candidate
    return ""


_ORIGIN_CHECK_STATE = threading.local()
_ORIGIN_PROBE_TIMEOUT_SECONDS = 5
# No caller's probe cap may exceed the longest hook budget (SessionStart, 60s).
_ORIGIN_PROBE_TIMEOUT_MAX_SECONDS = 60
_HOOK_ANCHOR_CACHE: tuple[str, ...] | None = None
_ORIGIN_PROBE_CODE = """
import importlib.metadata as metadata
import importlib.util
import json
import site
import sys
from pathlib import Path

names = json.load(sys.stdin)
scans = []
degraded = []

def claimants(top_name):
    # One metadata scan per probe process, reused for every name probed.
    if not scans:
        try:
            scans.append(metadata.packages_distributions())
        except Exception as exc:
            scans.append({})
            degraded.append({"reason": "hook_origin_scan_failed", "error": type(exc).__name__})
    try:
        return list(scans[0].get(top_name) or [])
    except Exception as exc:
        degraded.append({"reason": "hook_origin_claimant_lookup_failed", "error": type(exc).__name__})
        return []

def direct_url(top_name):
    # Same order as _read_direct_url: scanned claimants, then the two names.
    dist_names = claimants(top_name)
    for candidate in (top_name, top_name.replace("_", "-")):
        if candidate not in dist_names:
            dist_names.append(candidate)
    for dist_name in dist_names:
        try:
            text = metadata.distribution(dist_name).read_text("direct_url.json")
        except metadata.PackageNotFoundError:
            continue
        if text:
            return text
    return None

def site_packages(prefix):
    found = []
    try:
        root = Path(prefix).resolve()
    except OSError:
        return found
    try:
        candidates = list(site.getsitepackages())
    except Exception:
        candidates = []
    for path in candidates:
        try:
            resolved = Path(path).resolve()
        except OSError:
            continue
        if resolved.is_relative_to(root):
            found.append(str(resolved))
    return found

packages = {}
for name in names:
    try:
        spec = importlib.util.find_spec(name)
    except (ImportError, ValueError, AttributeError) as exc:
        packages[name] = {"error": type(exc).__name__}
        continue
    if spec is None:
        packages[name] = None
        continue
    packages[name] = {
        "origin": spec.origin,
        "locations": list(spec.submodule_search_locations or ()),
        "direct_url": direct_url(name),
    }
print(json.dumps({
    "prefix": sys.prefix,
    "site_packages": site_packages(sys.prefix),
    "packages": packages,
    "degraded": degraded,
}, sort_keys=True))
"""


def _workbay_import_names(path: Path, hooks_root: Path) -> set[str]:
    """Follow imports from the hook being launched and collect workbay roots."""
    aliases: dict[str, Path] = {}
    if hooks_root.is_dir():
        for candidate in hooks_root.rglob("*.py"):
            if "__pycache__" in candidate.parts:
                continue
            parts = list(candidate.relative_to(hooks_root).with_suffix("").parts)
            if parts[-1] == "__init__":
                parts.pop()
            if parts:
                aliases.setdefault(".".join(parts), candidate)
                if len(parts) == 1:
                    aliases.setdefault(parts[0], candidate)

    work = [path]
    visited: set[Path] = set()
    names: set[str] = set()
    while work:
        current = work.pop().resolve()
        if current in visited or not current.is_file() or current.suffix != ".py":
            continue
        visited.add(current)
        try:
            tree = ast.parse(current.read_text(encoding="utf-8", errors="replace"), filename=str(current))
        except (OSError, SyntaxError):
            continue
        for node in ast.walk(tree):
            imported: list[str] = []
            if isinstance(node, ast.Import):
                imported.extend(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported.append(node.module)
            for fullname in imported:
                top_name = fullname.split(".", 1)[0]
                if top_name.startswith("workbay_"):
                    names.add(top_name)
                    continue
                local = aliases.get(fullname) or aliases.get(top_name)
                if local is None:
                    for base in (current.parent, hooks_root):
                        candidate = base / f"{top_name}.py"
                        package = base / top_name / "__init__.py"
                        if candidate.is_file():
                            local = candidate
                            break
                        if package.is_file():
                            local = package
                            break
                if local is not None:
                    work.append(local)
    return names


def _argv_python_file(arg: object, repo_root: str | None) -> Path | None:
    """Resolve one argv token to a Python file, or None when it is not one."""
    if not isinstance(arg, str) or not arg.endswith(".py"):
        return None
    path = Path(arg)
    ordered: list[Path] = []
    if path.is_absolute():
        ordered.append(path)
    else:
        if repo_root:
            ordered.append(Path(repo_root) / path)
        ordered.append(path)
    for candidate in ordered:
        try:
            if candidate.is_file():
                return candidate.resolve()
        except OSError:
            continue
    return None


def _launched_hook_paths(repo_root: str | None = None) -> list[Path]:
    """The launched hook, plus the handler when the entrypoint is ``_run_guard.py``.

    Later ``.py`` arguments are not part of the scan. A test file or an
    unrelated path on argv must not widen the set of packages the guard checks.
    """
    if not sys.argv:
        return []
    paths: list[Path] = []
    entry = _argv_python_file(sys.argv[0], repo_root)
    if entry is not None:
        paths.append(entry)
    if Path(str(sys.argv[0])).name == "_run_guard.py":
        rest = list(sys.argv[1:])
        while rest and isinstance(rest[0], str) and rest[0].startswith("--"):
            rest = rest[1:]
        if rest:
            handler = _argv_python_file(rest[0], repo_root)
            if handler is not None and handler not in paths:
                paths.append(handler)
    return paths


def _current_hook_import_names(repo_root: str | None = None) -> set[str]:
    """Infer the package imports of the launched hook, not every argv path."""
    candidates = _launched_hook_paths(repo_root)
    if not candidates:
        return set()
    root = Path(repo_root).resolve() if repo_root else Path(_git_repo_root() or "").resolve()
    hooks_root = root / "scripts" / "hooks"
    names: set[str] = set()
    for path in candidates:
        names.update(_workbay_import_names(path, hooks_root))
    return names


def _git_from_dir(cwd: str, *args: str) -> str:
    """Bounded ``git -C <cwd>``. Empty when ``cwd`` is not a checkout."""
    timeout = _bounded_timeout(5, "git -C")
    if timeout <= 0:
        return ""
    try:
        proc = subprocess.run(
            ["git", "-C", cwd, *args],
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return ""
    if proc.returncode != 0:
        return ""
    return proc.stdout.strip()


def _hook_checkout_anchors() -> tuple[str, ...]:
    """Toplevel and primary checkout of the worktree that contains this file.

    ``git -C`` uses the directory of the running ``_interp.py``, not the
    process cwd, and the result is cached for the process. Empty means no
    checkout anchor resolved; callers fail closed instead of skipping.
    """
    global _HOOK_ANCHOR_CACHE
    cached = _HOOK_ANCHOR_CACHE
    if cached is not None:
        return cached
    interp_dir = str(Path(__file__).resolve().parent)
    found: list[str] = []
    toplevel = _git_from_dir(interp_dir, "rev-parse", "--show-toplevel")
    if toplevel:
        found.append(str(Path(toplevel).resolve()))
    common = _git_from_dir(interp_dir, "rev-parse", "--path-format=absolute", "--git-common-dir")
    if common:
        primary = str(Path(common).resolve().parent)
        if primary not in found:
            found.append(primary)
    _HOOK_ANCHOR_CACHE = tuple(found)
    return _HOOK_ANCHOR_CACHE


def _prefix_site_packages(prefix: str | None = None) -> tuple[str, ...]:
    """Site-packages directories that live under ``prefix`` (never user site)."""
    root_text = sys.prefix if prefix is None else prefix
    try:
        root = Path(root_text).resolve()
    except OSError:
        return ()
    try:
        candidates = list(site.getsitepackages())
    except Exception:  # noqa: BLE001 -- discovery failure denies the prefix trust path
        candidates = []
    found: list[str] = []
    for path in candidates:
        try:
            resolved = Path(path).resolve()
        except (OSError, TypeError):
            continue
        if resolved.is_relative_to(root):
            found.append(str(resolved))
    return tuple(found)


def _sites_under_prefix(prefix: str, reported: object) -> tuple[str, ...]:
    try:
        root = Path(prefix).resolve()
    except OSError:
        return ()
    if not isinstance(reported, list):
        return ()
    found: list[str] = []
    for item in reported:
        if not isinstance(item, str):
            continue
        try:
            resolved = Path(item).resolve()
        except OSError:
            continue
        if resolved.is_relative_to(root):
            found.append(str(resolved))
    return tuple(found)


def _read_direct_url(top_name: str) -> str | None:
    """``direct_url.json`` text for ``top_name``, if that distribution has one."""
    import importlib.metadata as metadata

    dist_names: list[str] = []
    try:
        dist_names.extend(metadata.packages_distributions().get(top_name) or [])
    except Exception:  # noqa: BLE001 -- metadata scan must not become an import
        dist_names = []
    for candidate in (top_name, top_name.replace("_", "-")):
        if candidate not in dist_names:
            dist_names.append(candidate)
    for dist_name in dist_names:
        try:
            text = metadata.distribution(dist_name).read_text("direct_url.json")
        except metadata.PackageNotFoundError:
            continue
        if text:
            return text
    return None


def _direct_url_is_editable(text: str | None) -> bool:
    """True only for an editable install (a ``.pth`` redirect is not a wheel)."""
    if not text:
        return False
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        return False
    dir_info = data.get("dir_info") if isinstance(data, dict) else None
    return isinstance(dir_info, dict) and bool(dir_info.get("editable"))


def _origin_is_trusted(origin: str, expected_root: str) -> bool:
    if not origin or not expected_root or origin in {"built-in", "frozen"}:
        return False
    try:
        Path(origin).resolve().relative_to(Path(expected_root).resolve())
    except (OSError, ValueError):
        return False
    return True


def _origin_under_trust_roots(
    origin: str,
    anchors: tuple[str, ...],
    site_packages: tuple[str, ...],
    *,
    editable: bool,
) -> bool:
    if any(_origin_is_trusted(origin, anchor) for anchor in anchors):
        return True
    # (iii) admits a non-editable install under the interpreter prefix only.
    if editable:
        return False
    return any(_origin_is_trusted(origin, site_dir) for site_dir in site_packages)


def _emit_origin_failure(reason: str, detail: str) -> None:
    message = f"{reason}: {detail}"
    sys.stderr.write(message + "\n")
    raise ImportError(message)


def _resolve_origin_probe_timeout(origin_probe_timeout_seconds: object) -> int | float:
    """The caller's probe cap when it is a number in ``(0, max]``, else the default.

    A bool, a string, a non-finite or an oversized value falls back loudly.
    """
    if origin_probe_timeout_seconds is None:
        return _ORIGIN_PROBE_TIMEOUT_SECONDS
    if not _is_bounded_seconds(origin_probe_timeout_seconds, _ORIGIN_PROBE_TIMEOUT_MAX_SECONDS):
        sys.stderr.write(
            f"origin_probe_timeout_seconds must be a number in (0, {_ORIGIN_PROBE_TIMEOUT_MAX_SECONDS}]; "
            f"using default deadline_seconds={_ORIGIN_PROBE_TIMEOUT_SECONDS}\n"
        )
        return _ORIGIN_PROBE_TIMEOUT_SECONDS
    return origin_probe_timeout_seconds


def _probe_runtime_origins(
    interpreter: str,
    names: set[str],
    repo_root: str | None,
    *,
    origin_probe_timeout_seconds: float | None = None,
) -> tuple[str, dict[str, object]]:
    """Probe ``interpreter`` for package origins. Status is ok, timeout, or failed.

    A timeout payload carries the ``deadline_seconds`` actually used, and
    ``budget_seconds`` when the hook's subprocess budget left nothing to start it.
    """
    timeout = _bounded_timeout(_resolve_origin_probe_timeout(origin_probe_timeout_seconds))
    if timeout <= 0:
        budget = _SUBPROCESS_BUDGET
        return "timeout", {"deadline_seconds": 0, "budget_seconds": budget.seconds if budget else 0}
    try:
        probe = subprocess.run(
            [interpreter, "-c", _ORIGIN_PROBE_CODE],
            input=json.dumps(sorted(names)),
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=repo_root or None,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return "timeout", {"deadline_seconds": timeout}
    except OSError:
        return "failed", {"exit": "oserror"}
    except (OverflowError, ValueError) as exc:
        # Unlaunchable (a NUL in argv) or an unusable timeout: refuse, never escape.
        return "failed", {"exit": type(exc).__name__}
    if probe.returncode != 0:
        return "failed", {"exit": str(probe.returncode)}
    try:
        parsed = json.loads(probe.stdout)
    except (json.JSONDecodeError, TypeError, UnicodeError):
        return "failed", {"exit": "invalid-json"}
    if not isinstance(parsed, dict) or not isinstance(parsed.get("packages"), dict):
        return "failed", {"exit": "invalid-json"}
    if "degraded" in parsed:
        degraded = parsed["degraded"]
        if not isinstance(degraded, list):
            return "failed", {"exit": "invalid-json"}
        for entry in degraded:
            if not isinstance(entry, dict):
                return "failed", {"exit": "invalid-json"}
            reason = entry.get("reason")
            error = entry.get("error")
            if not isinstance(reason, str) or not isinstance(error, str):
                return "failed", {"exit": "invalid-json"}
            sys.stderr.write(f"{reason}: error={error} interpreter={interpreter}\n")
    return "ok", parsed


def _origin_from_runtime_spec(spec: object) -> tuple[str, str | None]:
    if not isinstance(spec, dict) or spec.get("error"):
        return "<unresolved>", None
    origin = "<unresolved>"
    if spec.get("origin"):
        origin = str(spec["origin"])
    elif spec.get("locations"):
        locations = spec.get("locations")
        if isinstance(locations, list) and locations:
            origin = os.fspath(locations[0])
    direct_url = spec.get("direct_url")
    return origin, direct_url if isinstance(direct_url, str) else None


def _inprocess_origin(name: str) -> str:
    try:
        spec = importlib.util.find_spec(name)
    except (ImportError, ValueError, AttributeError, OSError):
        return "<unresolved>"
    if spec is not None and spec.origin:
        return spec.origin
    if spec is not None and spec.submodule_search_locations:
        return os.fspath(next(iter(spec.submodule_search_locations)))
    return "<unresolved>"


def assert_workbay_origins_trusted(
    *package_names: str,
    repo_root: str | None = None,
    interpreter: str | None = None,
    origin_probe_timeout_seconds: float | None = None,
) -> None:
    """Require each ``workbay_*`` origin under this hook's checkout or a wheel install.

    Names come from the caller or from the launched hook (argv[0], plus the
    ``_run_guard.py`` handler). An origin is trusted when it lies under the git
    toplevel of the worktree that contains this file, that worktree's primary
    checkout, or the interpreter's own prefix site-packages for a non-editable
    install. No resolved anchor fails closed with ``hook_origin_anchor_unavailable``.
    A probe timeout is ``probe_timeout`` and a non-zero probe is ``probe_failed``,
    distinct from an origin that resolved outside the anchor.
    """
    names = set(package_names) if package_names else _current_hook_import_names(repo_root)
    names = {name.split(".", 1)[0] for name in names if name.startswith("workbay_")}
    if not names:
        return
    external_runtime = bool(interpreter and os.path.abspath(interpreter) != os.path.abspath(sys.executable))
    runtime_packages: dict[str, object] | None = None
    site_packages = _prefix_site_packages()
    if external_runtime:
        runtime = interpreter or ""
        status, payload = _probe_runtime_origins(
            runtime,
            names,
            repo_root,
            origin_probe_timeout_seconds=origin_probe_timeout_seconds,
        )
        if status == "timeout":
            recovery_command = f"{shlex.quote(runtime)} -c 'import sys; print(sys.executable)'"
            deadline = _seconds_text(payload.get("deadline_seconds", 0))
            exhausted = ""
            if "budget_seconds" in payload:
                exhausted = f" hook_subprocess_budget_seconds={_seconds_text(payload['budget_seconds'])} exhausted"
            _emit_origin_failure(
                "probe_timeout",
                f"interpreter={runtime} deadline_seconds={deadline}{exhausted}; "
                f"check the interpreter with {recovery_command}",
            )
        if status == "failed":
            _emit_origin_failure("probe_failed", f"interpreter={runtime} exit={payload.get('exit', 'nonzero')}")
        packages = payload.get("packages")
        runtime_packages = packages if isinstance(packages, dict) else {}
        prefix = payload.get("prefix")
        if isinstance(prefix, str):
            site_packages = _sites_under_prefix(prefix, payload.get("site_packages"))
    anchors = _hook_checkout_anchors()
    expected_root = os.pathsep.join(anchors)
    for name in sorted(names):
        if runtime_packages is not None:
            origin, direct_url = _origin_from_runtime_spec(runtime_packages.get(name))
            editable = _direct_url_is_editable(direct_url)
        else:
            origin = _inprocess_origin(name)
            editable = _direct_url_is_editable(_read_direct_url(name))
        if _origin_under_trust_roots(origin, anchors, site_packages, editable=editable):
            continue
        if not anchors:
            _emit_origin_failure("hook_origin_anchor_unavailable", f"package={name} origin={origin}")
        sys.stderr.write(
            "hook_package_origin_untrusted: "
            f"package={name} origin={origin} expected_root={expected_root or '<unavailable>'}\n"
        )
        raise ImportError(
            f"hook_package_origin_untrusted: {name} origin {origin} is outside {expected_root or '<unavailable>'}"
        )


class _WorkbayOriginFinder(importlib.abc.MetaPathFinder):
    """Reference monitor that checks a package immediately before import."""

    def find_spec(self, fullname, path=None, target=None):
        if getattr(_ORIGIN_CHECK_STATE, "checking", False):
            return None
        top_name = fullname.split(".", 1)[0]
        if "." not in fullname and top_name.startswith("workbay_"):
            _ORIGIN_CHECK_STATE.checking = True
            try:
                assert_workbay_origins_trusted(top_name)
            finally:
                _ORIGIN_CHECK_STATE.checking = False
        return None


_ORIGIN_FINDER = _WorkbayOriginFinder()


def _install_workbay_origin_guard() -> None:
    if not any(finder is _ORIGIN_FINDER for finder in sys.meta_path):
        sys.meta_path.insert(0, _ORIGIN_FINDER)


def _guard_current_hook(
    repo_root: str | None = None,
    interpreter: str | None = None,
    origin_probe_timeout_seconds: float | None = None,
) -> None:
    assert_workbay_origins_trusted(
        repo_root=repo_root,
        interpreter=interpreter,
        origin_probe_timeout_seconds=origin_probe_timeout_seconds,
    )


# Leaf deps the stack-importing hooks load (via ``workbay_handoff_mcp``) at
# module import time. Probing only ``pydantic`` under-detects: a system Python
# can carry pydantic yet lack ``fastmcp`` / ``workbay_protocol``, so the hook
# would wrongly stay on it and silently no-op instead of healing to the venv
# (MAINT-postmerge-review REV-A-HOOK-DEPS-01).
_REQUIRED_DEPS = ("pydantic", "fastmcp", "workbay_protocol")
_PROBE_PKG = "workbay-protocol"


def _warn_interpreter_skew() -> None:
    """implementation note D4 visibility arm: one-line stderr warning on version skew."""
    repo = _git_repo_root()
    if not repo:
        return
    venv = _venv_python(repo)
    if not venv or os.path.abspath(venv) == os.path.abspath(sys.executable):
        return
    ambient_ver = _probe_pkg_version(sys.executable)
    venv_ver = _probe_pkg_version(venv)
    if (
        ambient_ver
        and venv_ver
        and ambient_ver != venv_ver
        and not ambient_ver.startswith("<")
        and not venv_ver.startswith("<")
    ):
        sys.stderr.write(
            f"workbay: interpreter version skew — ambient {_PROBE_PKG}={ambient_ver}, "
            f"workspace .venv {_PROBE_PKG}={venv_ver}\n"
        )


def _probe_pkg_version(python: str) -> str:
    # Advisory (skew warning only): an exhausted budget skips it.
    timeout = _bounded_timeout(15, "the interpreter version-skew check")
    if timeout <= 0:
        return ""
    try:
        proc = subprocess.run(
            [
                python,
                "-c",
                f"import importlib.metadata as m; print(m.version('{_PROBE_PKG}'))",
            ],
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return ""
    if proc.returncode != 0:
        return ""
    return proc.stdout.strip()


def _deps_present() -> bool:
    """True only when every workbay runtime dep the hooks import is importable.

    Short-circuits on the first missing dep. ``find_spec`` can raise
    (``ModuleNotFoundError`` for a missing parent, ``ValueError`` for a
    half-initialised module) -- treat any failure as "not present" so the
    caller heals to the venv rather than crashing the hook.
    """
    for name in _REQUIRED_DEPS:
        try:
            if importlib.util.find_spec(name) is None:
                return False
        except (ImportError, ValueError):
            return False
    return True


def resolve_deps_python(repo_root: str | None = None) -> str:
    """Return an interpreter that carries the workbay deps.

    For hooks that run the stack in a *subprocess* (no re-exec -- safe for
    high-frequency / per-event hooks). Prefer the project ``.venv`` when it
    exists and differs from the current interpreter, even when the ambient
    interpreter already imports the stack (implementation note D3). Fall back to the
    current interpreter only when no distinct venv exists.
    """
    candidate = _venv_python(repo_root)
    _guard_current_hook(repo_root, candidate)
    if candidate and os.path.abspath(candidate) != os.path.abspath(sys.executable):
        return candidate
    if _deps_present():
        return sys.executable
    return candidate or sys.executable


def resolve_env_alias(canonical: str, default: str | None = None) -> str | None:
    """Resolve a canonical ``WORKBAY_*`` env var.

    Delegates to ``workbay_protocol.resolve_env_alias`` when the stack is
    importable (the re-exec hooks), and falls back to a stdlib-only equivalent so
    a bare ``python3`` hook (pre-re-exec, no stack) still resolves the variable.
    Blank / whitespace-only values are treated as unset, matching the canonical
    resolver.
    """
    try:
        from workbay_protocol import resolve_env_alias as _canonical
    except Exception:  # noqa: BLE001 -- bare python3 / partial stack: stdlib fallback
        value = os.environ.get(canonical)
        if value is not None and value.strip():
            return value.strip()
        return default
    return _canonical(canonical, default=default)


def ensure_deps_interpreter(
    *,
    origin_probe_timeout_seconds: float | None = None,
    subprocess_budget_seconds: float | None = None,
) -> None:
    """Re-exec the current hook under the venv when a distinct venv exists.

    Prefer the project ``.venv`` even when the ambient interpreter already
    imports the full stack (implementation note D3). The ``WORKBAY_HOOK_REEXEC``
    sentinel bars a second re-exec so a broken venv cannot loop. Compares
    abspath, not realpath: a venv ``python`` usually symlinks to the same base
    binary as the system interpreter yet activates a different site-packages
    (where the deps live), so realpath equality would wrongly skip the
    re-exec. Call as the first statement of ``main()`` -- before stdin is read
    -- so ``os.execv`` preserves the unread event payload.

    ``origin_probe_timeout_seconds`` caps the origin probe alone.
    ``subprocess_budget_seconds`` is one deadline for every subprocess wait
    from here on, in both passes of the re-exec: each wait gets ``min(its own
    cap, time left)``. ``None`` keeps each wait at its own cap.
    """
    global _SUBPROCESS_BUDGET
    budget = _start_subprocess_budget(subprocess_budget_seconds)
    _SUBPROCESS_BUDGET = budget
    candidate = _venv_python()
    _guard_current_hook(
        interpreter=candidate,
        origin_probe_timeout_seconds=origin_probe_timeout_seconds,
    )
    _install_workbay_origin_guard()
    _warn_interpreter_skew()
    if os.environ.get("WORKBAY_HOOK_REEXEC") == "1":
        return
    if candidate and os.path.abspath(candidate) != os.path.abspath(sys.executable):
        os.environ["WORKBAY_HOOK_REEXEC"] = "1"
        if budget is not None:
            os.environ[_HOOK_DEADLINE_ENV] = repr(budget.deadline)
        os.execv(candidate, [candidate, *sys.argv])
