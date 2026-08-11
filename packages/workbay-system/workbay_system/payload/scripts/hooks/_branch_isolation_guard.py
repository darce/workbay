#!/usr/bin/env python3
"""Shared helpers for main-branch branch-isolation guards."""

from __future__ import annotations

import errno
import importlib.util
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

from _harness_protocol import (
    find_permitted_main_surface,
    is_branch_isolation_protected_path,
    is_state_dirty_path,
)


def _load_task_ref_re() -> re.Pattern[str]:
    """Load the canonical branch regex without requiring MCP runtime deps.

    Editor hooks run under plain ``python3`` in some environments. Importing
    ``workbay_handoff_mcp`` pulls in the full MCP API surface and therefore
    optional runtime dependencies such as pydantic; this guard only needs the
    lightweight branch-naming regex.
    """

    hook_path = Path(__file__).resolve()
    for parent in hook_path.parents:
        candidate = (
            parent
            / "packages"
            / "workbay-protocol"
            / "src"
            / "workbay_protocol"
            / "branch_naming.py"
        )
        if candidate.is_file():
            spec = importlib.util.spec_from_file_location(
                "_workbay_branch_naming_for_hooks",
                candidate,
            )
            if spec is not None and spec.loader is not None:
                module = importlib.util.module_from_spec(spec)
                sys.modules[spec.name] = module
                spec.loader.exec_module(module)
                task_ref_re = getattr(module, "TASK_REF_RE", None)
                if isinstance(task_ref_re, re.Pattern):
                    return task_ref_re

    try:
        from workbay_handoff_mcp import TASK_REF_RE as task_ref_re

        return task_ref_re
    except Exception:
        return re.compile(
            r"^feature/"
            r"(?=[a-z])"
            r"(?=[a-z0-9-]*\d)"
            r"(?P<task_ref>[a-z0-9]+(?:-[a-z0-9]+)+)"
            r"$"
        )


TASK_REF_RE = _load_task_ref_re()

# implementation note branch-class taxonomy. Branches in these sets are
# "protected" — neither the post-checkout warn nor the PreToolUse /
# pre-commit / pre-push hard gates validate naming on them. Listed
# here (not next to the regex in workbay_protocol) because the carve-
# out is a *gate-side* policy: the canonical regex itself only
# expresses conforming feature-branch names.
#
# PROTECTED_EDIT_BRANCHES is the single shared set for main/master edit
# isolation (bash scanner, inline guard, check_file_edit callers). Naming
# protection reuses the same names; release/hotfix remain naming-only via
# the prefix tuple. [ARCH-13]
PROTECTED_EDIT_BRANCHES = frozenset({"main", "master"})
_NAMING_PROTECTED_BRANCH_NAMES = PROTECTED_EDIT_BRANCHES
_NAMING_PROTECTED_BRANCH_PREFIXES: tuple[str, ...] = ("release/", "hotfix/")


def check_branch_naming(branch: str | None) -> str | None:
    """Return ``branch`` when non-conforming, else ``None``.

    Implements the implementation note branch-class taxonomy used by every gate
    (post-checkout / PreToolUse / pre-commit / pre-push):

    - ``protected`` → ``main``, ``master``, ``release/*``, ``hotfix/*``:
      not validated by name (return ``None``).
    - ``conforming feature`` → matches the canonical
      ``workbay_protocol.branch_naming.TASK_REF_RE`` re-exported from
      ``workbay_handoff_mcp``: allowed (return ``None``).
    - ``non-conforming`` → everything else (``feature/<bad>``,
      ``fix/<foo>``, ``chore/<x>``, ``wip-<y>``, bare names): return
      ``branch`` so callers render a rejection message.

    Detached-HEAD / unknown (empty / ``None`` branch) returns ``None``
    so the gate cannot wedge a branchless checkout; the dirty-paths
    guard owns that concern via a separate carve-out.
    """
    if not branch:
        return None
    if branch in _NAMING_PROTECTED_BRANCH_NAMES:
        return None
    for prefix in _NAMING_PROTECTED_BRANCH_PREFIXES:
        if branch.startswith(prefix):
            return None
    if TASK_REF_RE.match(branch):
        return None
    return branch


def build_branch_naming_block_reason(branch: str) -> str:
    """Render the PreToolUse rejection message for a non-conforming branch.

    Cites the canonical module path so the operator can find the rule
    without grepping through gate code, and names the escape-valve
    env var so legitimate one-off work is not stranded.
    """
    return (
        "BLOCKED: Branch name does not match the canonical feature-branch grammar.\n\n"
        f"Branch: {branch}\n\n"
        "Allowed branch classes:\n"
        "  - protected: main, master, release/*, hotfix/*\n"
        "  - conforming feature: feature/<task-ref> matching\n"
        "    workbay_protocol.branch_naming.TASK_REF_RE\n\n"
        "Rename the branch to feature/<task-ref>[-<slug>] (lowercase,\n"
        "task ref must contain a digit), or set\n"
        "WORKBAY_ALLOW_NONCONFORMING_BRANCH=1 to override (the override\n"
        "is audited).\n\n"
        "See: docs/workbay/rules/development-workflow.md"
        "#branch-isolation-protocol-mandatory"
    )


def resolve_path_branch(abs_path: str) -> str | None:
    """Return the git branch of the worktree containing ``abs_path``.

    The harness cwd is always the project root, which by repo convention stays
    on ``main`` even when active work happens in linked feature-branch
    worktrees. Without per-path resolution, the guards misclassify edits to
    files that physically live in a feature-branch worktree as main-branch
    edits and block them.

    Returns the branch reported by ``git branch --show-current`` when run
    inside the worktree containing ``abs_path``. Returns ``None`` when the
    path is not inside a git working tree (so the caller can fall back to
    the harness branch and preserve the conservative default).
    """
    if not abs_path:
        return None
    try:
        candidate = Path(abs_path).expanduser().resolve(strict=False)
    except (OSError, RuntimeError):
        return None
    anchor = candidate if candidate.is_dir() else candidate.parent
    while not anchor.exists():
        parent = anchor.parent
        if parent == anchor:
            return None
        anchor = parent
    try:
        proc = subprocess.run(
            ["git", "-C", str(anchor), "branch", "--show-current"],
            capture_output=True,
            text=True,
            timeout=3,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if proc.returncode != 0:
        return None
    return proc.stdout.strip() or None


_EDIT_TOOLS = {
    "Edit",
    "Write",
    "apply_patch",
    "create_file",
    "multi_replace_string_in_file",
    "replace_string_in_file",
}


def _payload_value(mapping: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = mapping.get(key)
        if value is not None:
            return value
    return None


def normalize_path_token(path: str) -> str:
    """Strip whitespace then surrounding quotes — shared by both isolation guards.

    Both guards must normalize identically before resolution and before
    dedup/reporting. [ARCH-13][WEB-33][SEC-01]
    """
    return path.strip().strip("'\"")


def collect_protected_inodes(
    repo_root: str | Path,
    policy: Any,
) -> frozenset[tuple[int, int]]:
    """Bounded ``(st_dev, st_ino)`` set for policy-protected files under ``repo_root``.

    Walks ``code_roots`` for ``protected_extensions`` and stats
    ``root_protected_files``. Used by the unrelativizable arm to catch
    hardlink aliases of protected content that live outside the repo tree
    (inode match without a samefile ancestor on the repo root). [SECD-05]
    """
    inodes: set[tuple[int, int]] = set()
    try:
        root = Path(str(repo_root)).expanduser().resolve(strict=False)
    except (OSError, ValueError, RuntimeError):
        return frozenset()
    if not str(repo_root).strip() or not root.exists():
        return frozenset()

    extensions = tuple(getattr(policy, "protected_extensions", ()) or ())
    code_roots = tuple(getattr(policy, "code_roots", ()) or ())
    for code_root in code_roots:
        rel = str(code_root).strip().strip("/")
        if not rel:
            continue
        base = root / rel
        if not base.is_dir():
            continue
        for dirpath, _dirnames, filenames in os.walk(base, followlinks=False):
            for name in filenames:
                if extensions and not name.endswith(extensions):
                    continue
                path = Path(dirpath) / name
                try:
                    st = path.stat()
                except OSError:
                    continue
                inodes.add((st.st_dev, st.st_ino))

    for name in tuple(getattr(policy, "root_protected_files", ()) or ()):
        rel = str(name).strip().lstrip("/")
        if not rel:
            continue
        path = root / rel
        try:
            st = path.stat()
        except OSError:
            continue
        inodes.add((st.st_dev, st.st_ino))
    return frozenset(inodes)


def path_identifies_repo_root(
    path: str,
    repo_root: str | Path,
    *,
    protected_inodes: frozenset[tuple[int, int]] | set[tuple[int, int]] | None = None,
) -> bool:
    """Return True when ``path`` must be treated as the protected checkout.

    Used by the unrelativizable (``None``) arm: refuse spoofed in-repo spellings
    that defeat ``relative_to`` (case/symlink/bind-mount identity) while allowing
    genuine outsiders (``/dev/null``, ``$TMPDIR``, ``~``).

    Fail-closed [SEC-07]: when the identity check cannot complete (missing
    repo root, ``ValueError``/NUL, ``RuntimeError``/ELOOP, other OS errors on
    resolve), return True so protected-surface decisions refuse rather than
    allow. Empty/whitespace tokens remain False (no target).

    Optional ``protected_inodes``: if the candidate file's ``(st_dev, st_ino)``
    matches a protected-surface inode (hardlink outside the tree), refuse even
    when no ancestor is samefile with the repo root. [SECD-05][ARCH-13]
    [WEB-13][WEB-33]
    """
    token = normalize_path_token(path)
    if not token:
        return False
    if not str(repo_root).strip():
        # No basis for a protected-surface decision → refuse. [SEC-07]
        return True
    try:
        root = Path(str(repo_root)).expanduser().resolve(strict=False)
    except (OSError, ValueError, RuntimeError):
        return True
    try:
        if not root.exists():
            return True
    except (OSError, ValueError, RuntimeError):
        return True
    try:
        candidate = Path(token).expanduser().resolve(strict=False)
    except (OSError, ValueError, RuntimeError):
        # Cannot complete identity (NUL, ELOOP, permission) → refuse. [SEC-07]
        return True

    # Platform-robust ambiguity probe [SEC-07]. Path.resolve(strict=False) does
    # NOT raise on a symlink cycle on macOS (it returns the token unresolved),
    # unlike Linux where it raises OSError/ELOOP and is caught above. Without
    # this probe an ELOOP path falls through the parent walk as a genuine
    # outsider (current.exists() swallows ELOOP as False) and is wrongly allowed.
    # Probe with os.stat (follows symlinks; raises the cycle errno on both
    # platforms) and refuse on the inherent-ambiguity errnos only; ENOENT (a
    # not-yet-created write target) must continue to normal evaluation so real
    # in-repo write targets still resolve via the ancestor walk.
    try:
        os.stat(os.path.expanduser(token))
    except ValueError:
        return True
    except OSError as exc:
        if exc.errno in (errno.ELOOP, errno.ENAMETOOLONG):
            return True

    if protected_inodes:
        try:
            st = candidate.stat() if candidate.exists() else None
            if st is None:
                # Candidate may not exist yet (write target); try the raw token.
                try:
                    st = Path(token).expanduser().stat()
                except OSError:
                    st = None
            if st is not None and (st.st_dev, st.st_ino) in protected_inodes:
                return True
        except (OSError, ValueError, RuntimeError):
            # Ambiguous identity under protected-inode mode → refuse. [SEC-07]
            return True

    current = candidate
    seen: set[str] = set()
    while True:
        key = str(current)
        if key in seen:
            # Cycle in parent walk without matching root → refuse. [SEC-07]
            return True
        seen.add(key)
        try:
            if current.exists() and os.path.samefile(current, root):
                return True
        except (OSError, ValueError, RuntimeError):
            pass
        parent = current.parent
        if parent == current:
            break
        current = parent
    return False


def to_repo_relative(path: str, repo_root: str) -> str | None:
    """Map a path to a repo-relative posix string.

    Returns:
        - repo-relative posix string on success
        - ``""`` when the input is empty/whitespace
        - ``""`` when ``repo_root`` is empty (no basis → skip; shared contract
          with ``_bash_isolation_guard._to_repo_relative`` and
          ``guard-task-plan-findings._to_repo_relative``)
        - ``None`` when relativization fails after resolve

    ``None`` is the unrelativizable signal: callers apply ancestor-identity
    refusal (``path_identifies_repo_root``) rather than treating the token as
    absent or as an automatic block. [SECD-05][ARCH-13][WEB-13][WEB-33]

    Relative tokens resolve against ``repo_root``, not the hook process cwd —
    parity with ``_bash_isolation_guard._to_repo_relative`` so the same
    spelling cannot BLOCK via bash and ALLOW via ``check_file_edit`` when the
    hook cwd is outside the checkout. [SEC-01][ARCH-13][F-GA2-03]
    """
    normalized_path = normalize_path_token(path)
    if not normalized_path:
        return ""
    if not repo_root:
        # Shared contract: no basis → skip (empty string), never return the
        # original token (that made protected-path prefix checks miss).
        return ""
    try:
        root = Path(repo_root).expanduser().resolve(strict=False)
    except OSError:
        return None
    candidate = Path(normalized_path).expanduser()
    if not candidate.is_absolute():
        # Anchor relatives to repo_root (not process cwd). [F-GA2-03][SEC-01]
        try:
            candidate = (root / candidate).resolve(strict=False)
        except OSError:
            return normalized_path.replace("\\", "/").lstrip("/")
    else:
        try:
            candidate = candidate.resolve(strict=False)
        except OSError:
            pass
    try:
        # Normalize the relativization result (posix form) before policy match.
        return candidate.relative_to(root).as_posix()
    except ValueError:
        # Unrelativizable after resolve — caller decides via ancestor identity.
        return None


def extract_candidate_paths(tool_name: str, tool_input: dict[str, Any]) -> list[str]:
    file_path = _payload_value(tool_input, "filePath", "file_path")
    if tool_name != "apply_patch":
        if tool_name not in _EDIT_TOOLS and not (isinstance(file_path, str) and file_path.strip()):
            return []
        return [str(file_path)] if isinstance(file_path, str) and file_path.strip() else []

    patch_input = tool_input.get("input")
    if not isinstance(patch_input, str) or not patch_input.strip():
        return []

    paths: list[str] = []
    for line in patch_input.splitlines():
        if not line.startswith("*** ") or " File: " not in line:
            continue
        _, raw_path = line.split(" File: ", 1)
        parsed_path = raw_path.split(" -> ", 1)[0].strip()
        if parsed_path:
            paths.append(parsed_path)
    return paths


def check_file_edit(
    tool_name: str,
    tool_input: dict[str, Any],
    *,
    branch: str,
    repo_root: str,
    policy,
    protected_branches: set[str] | frozenset[str],
) -> tuple[str, list[str]] | None:
    if branch not in protected_branches:
        return None

    blocked_paths: list[str] = []
    seen: set[str] = set()
    protected_inodes: frozenset[tuple[int, int]] | None = None
    for raw_path in extract_candidate_paths(tool_name, tool_input):
        relative_path = to_repo_relative(raw_path, repo_root)
        if relative_path is None:
            # Unrelativizable after resolve: refuse when an existing ancestor
            # is samefile with repo_root (spoofed in-repo spelling) OR when
            # the candidate's inode matches a protected-surface file
            # (hardlink alias outside the tree). Genuine outsiders
            # (/dev/null, $TMPDIR, ~) continue unblocked. Do NOT call
            # resolve_path_branch here: it is cwd-sensitive on padded/quoted
            # tokens (fail-open). [SECD-05][ARCH-13][WEB-13][WEB-33][SEC-07]
            token = normalize_path_token(raw_path) or raw_path
            if token in seen:
                continue
            seen.add(token)
            if protected_inodes is None:
                protected_inodes = collect_protected_inodes(repo_root, policy)
            if path_identifies_repo_root(
                token, repo_root, protected_inodes=protected_inodes
            ):
                blocked_paths.append(token)
            continue
        if not relative_path:
            continue
        if relative_path in seen:
            continue
        seen.add(relative_path)
        if not is_branch_isolation_protected_path(relative_path, policy):
            continue
        # Per-path worktree resolution: a file living inside a linked
        # worktree on a feature branch is not a main-branch edit even when
        # the harness cwd reports ``main``. Fall back to the harness
        # branch when the path is not inside any git working tree.
        # Normalize first so padded/quoted absolute spellings do not
        # re-anchor to the hook cwd. [SEC-01]
        per_path_branch = resolve_path_branch(normalize_path_token(raw_path) or raw_path)
        effective_branch = per_path_branch if per_path_branch else branch
        if effective_branch in protected_branches:
            blocked_paths.append(relative_path)

    if not blocked_paths:
        return None
    return branch, blocked_paths


def find_dirty_protected_paths(
    *,
    branch: str,
    repo_root: str,
    policy,
    protected_branches: set[str] | frozenset[str],
) -> tuple[str, list[str]] | None:
    if branch not in protected_branches or not repo_root:
        return None

    dirty_paths: list[str] = []
    for candidate in _git_dirty_paths(Path(repo_root)):
        if not is_branch_isolation_protected_path(candidate, policy):
            continue
        # Permitted-surface carve-out: a dirty file that matches an entry in
        # ``permitted_main_surfaces`` is explicitly allowed on main. Counting
        # it as "dirty protected" would block unrelated permitted edits and
        # contradicts the carve-out itself.
        if find_permitted_main_surface(candidate, policy) is not None:
            continue
        dirty_paths.append(candidate)

    if not dirty_paths:
        return None
    return branch, sorted(dict.fromkeys(dirty_paths))


def _run_git_degraded(args: list[str], *, timeout: float = 5.0) -> subprocess.CompletedProcess | None:
    """Run a guard git command; degrade loudly on timeout instead of crashing.

    Two hardenings (internal, [RES-03]/[AGT-10]):

    * ``-c core.fsmonitor=false`` — the observed 5s stalls come from the
      fsmonitor daemon over a slow volume; the guard's scans must not depend
      on it.
    * ``subprocess.TimeoutExpired`` is caught and reported as a WARNING with a
      ``None`` return (could-not-determine), so a slow git call degrades the
      check instead of aborting the whole hook with a raw traceback
      ([OBS-08]: "no answer" is a distinct outcome, not a crash).
    """
    cmd = ["git", "-c", "core.fsmonitor=false", *args]
    try:
        return subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired:
        preview = " ".join(args[2:5]) if len(args) > 2 else " ".join(args)
        print(
            f"warning: `git {preview}` timed out after {timeout:.0f}s; the guard "
            "degrades this scan to could-not-determine instead of crashing. "
            "Verify with `git status` by hand if in doubt.",
            file=sys.stderr,
        )
        return None


def _git_dirty_paths(repo_root: Path) -> list[str]:
    proc = _run_git_degraded(
        ["-C", str(repo_root), "status", "--porcelain=v1", "-z", "--untracked-files=all"],
    )
    if proc is None or proc.returncode != 0 or not proc.stdout:
        return []

    entries = proc.stdout.split("\0")
    paths: list[str] = []
    index = 0
    while index < len(entries):
        entry = entries[index]
        index += 1
        if not entry:
            continue

        status = entry[:2]
        path = entry[3:]
        if path:
            paths.append(path)

        if status[0] in {"R", "C"} and index < len(entries):
            renamed_path = entries[index]
            index += 1
            if renamed_path:
                paths.append(renamed_path)

    return [path.replace("\\", "/").lstrip("/") for path in paths if path.strip()]


def find_dirty_state_files(*, repo_root: str, policy) -> list[str]:
    """Return the runtime state files that are dirty on the current worktree.

    The post-merge ``check-main-clean`` tripwire (internal) consumes
    this helper. It combines two passes so state paths that Git reports at
    directory granularity still resolve to concrete files, while ignored
    untracked local projections stay non-blocking:

    1. ``git status --porcelain=v1 --untracked-files=all --ignored``
       captures tracked-modified, untracked, and ignored entries that Git
       can see. Each entry is filtered through ``is_state_dirty_path`` and
       ignored-untracked paths are dropped so planning artefacts, code
       edits, and by-design local handoff files never trip this surface.
    2. A direct filesystem walk over every pattern in
       ``policy.state_dirty_surfaces`` catches state paths that git
       summarises at the directory level (e.g. ``!! .task-state/``) or
       that exist on disk but are otherwise invisible to Git. The same
       ignored-untracked filter keeps generated local handoff projections
       from making the main control-plane checkout unpushable.

    Returns a sorted, de-duplicated list of repo-relative POSIX paths.
    """
    repo = Path(repo_root) if repo_root else None
    if repo is None or not repo.is_dir():
        return []

    found: set[str] = set()

    # Pass 1 — git status with --ignored. Same parsing shape as
    # ``_git_dirty_paths`` plus a ``--ignored`` flag so gitignored
    # entries surface in the porcelain output. On a status timeout the pass
    # is skipped (warned by the helper); pass 2's filesystem walk still
    # covers on-disk state surfaces, so the tripwire is degraded, not blind.
    proc = _run_git_degraded(
        [
            "-C",
            str(repo),
            "status",
            "--porcelain=v1",
            "-z",
            "--untracked-files=all",
            "--ignored",
        ],
    )
    if proc is not None and proc.returncode == 0 and proc.stdout:
        entries = proc.stdout.split("\0")
        index = 0
        while index < len(entries):
            entry = entries[index]
            index += 1
            if not entry:
                continue
            status = entry[:2]
            raw_path = entry[3:]
            normalized = raw_path.replace("\\", "/").lstrip("/")
            if (
                normalized
                and is_state_dirty_path(normalized, policy)
                and not _is_untracked_ignored_path(repo, normalized)
            ):
                found.add(normalized.rstrip("/"))
            if status[0] in {"R", "C"} and index < len(entries):
                renamed = entries[index]
                index += 1
                renamed_normalized = renamed.replace("\\", "/").lstrip("/") if renamed else ""
                if (
                    renamed_normalized
                    and is_state_dirty_path(renamed_normalized, policy)
                    and not _is_untracked_ignored_path(repo, renamed_normalized)
                ):
                    found.add(renamed_normalized.rstrip("/"))

    # Pass 2 — direct filesystem walk for every state surface pattern.
    # Necessary because ``git status --ignored`` often collapses an
    # ignored directory into one entry (``!! .task-state/``) instead of
    # listing the individual files beneath it, and the tripwire needs to
    # name the specific dirty file.
    for surface in policy.state_dirty_surfaces:
        for path in _surface_filesystem_matches(repo, surface.pattern):
            try:
                rel = path.relative_to(repo).as_posix()
            except ValueError:
                continue
            if is_state_dirty_path(rel, policy) and not _is_untracked_ignored_path(repo, rel):
                found.add(rel)

    return sorted(found)


def _is_untracked_ignored_path(repo: Path, rel_path: str) -> bool:
    """Return True when Git classifies ``rel_path`` as ignored local state.

    Generated handoff projections are intentionally ignored in this repo.
    Their mere existence should not make ``check-main-clean`` fail, but a
    tracked state surface that becomes modified must still block.
    """
    tracked = _run_git_degraded(
        ["-C", str(repo), "ls-files", "--error-unmatch", "--", rel_path],
    )
    if tracked is not None and tracked.returncode == 0:
        return False
    ignored = _run_git_degraded(
        ["-C", str(repo), "check-ignore", "-q", "--", rel_path],
    )
    # Timeout (None) keeps the path flagged — conservative, and the finding
    # names the concrete path so the operator can judge it directly.
    return ignored is not None and ignored.returncode == 0


def _surface_filesystem_matches(repo: Path, pattern: str):
    """Yield existing files under ``repo`` that match a YAML-style glob.

    Supports the subset used by ``state_dirty_surfaces``:

    - literal paths (``CURRENT_TASK.json``)
    - shallow globs handled by ``Path.glob`` (``foo/*.json``)
    - trailing ``**`` recursion (``.task-state/**``,
      ``docs/tasks/archive/**``)

    The walker yields file paths only (directories are skipped) so the
    caller can normalise them to repo-relative POSIX strings without
    re-checking ``is_file``.
    """
    if "**" in pattern:
        head, _, _ = pattern.partition("**")
        head = head.rstrip("/")
        base = repo / head if head else repo
        if base.is_file():
            yield base
            return
        if base.is_dir():
            for entry in base.rglob("*"):
                if entry.is_file():
                    yield entry
        return
    try:
        matches = list(repo.glob(pattern))
    except (OSError, ValueError):
        return
    for match in matches:
        if match.is_file():
            yield match
