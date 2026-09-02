#!/usr/bin/env python3
"""Shared helpers for main-branch branch-isolation guards."""

from __future__ import annotations

import errno
import importlib.util
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from _harness_protocol import (
    find_permitted_main_surface,
    is_branch_isolation_protected_path,
    is_state_dirty_path,
)

# Distinct fail-closed wording when the main-branch dirty probe cannot complete.
# Must not reuse the ordinary "already dirty" block text — operators need to
# know the gate blocked because cleanliness is *unknown*, not because a
# specific protected path was proven dirty.
GIT_PROBE_DEGRADED_BLOCK_MESSAGE = (
    "BLOCKED: Could not determine whether protected paths are clean on the "
    "main branch because the git status probe degraded (timeout or non-zero "
    "exit). Run `git status` by hand, then re-try once git is healthy."
)

# Whole-tree ignored-file inventory (`git ls-files --others --ignored`) walks
# every ignored path. Status-style probes keep the 5.0s default on
# ``_run_git_degraded``; this budget is for the inventory only.
IGNORED_INVENTORY_TIMEOUT_S = 30.0


@dataclass(frozen=True)
class DirtyProtectedDegraded:
    """Third state for ``find_dirty_protected_paths``: probe could not complete.

    * ``None`` — nothing to block on (clean tree, or not a protected branch)
    * ``tuple[str, list[str]]`` — ordinary dirty protected paths (block)
    * ``DirtyProtectedDegraded`` — cleanliness unknown; callers must fail
      CLOSED (block) with :attr:`block_message`, not treat this as allow.

    Callers MUST branch with ``isinstance(result, DirtyProtectedDegraded)``
    before any two-tuple unpack. There is intentionally no ``__iter__``
    compat shim: unpacking this object raises ``TypeError`` so forgotten
    call sites fail loudly instead of treating a synthetic diagnostic
    string as a real dirty path.
    """

    branch: str

    @property
    def block_message(self) -> str:
        return GIT_PROBE_DEGRADED_BLOCK_MESSAGE


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


def _path_has_symlink_ancestor(repo: Path, path: Path) -> bool:
    """Return True when any component of ``path`` under ``repo`` is a symlink.

    Used to keep the on-disk protected walk from emitting paths that only
    exist through a symlinked directory (e.g. ``.github/hooks`` → payload).
    Git cannot classify those pathspecs (tracked/ignored both false), so they
    would fail closed forever even though the real content is tracked under
    the symlink target.
    """
    try:
        rel = path.relative_to(repo)
    except ValueError:
        return True
    cur = repo
    for part in rel.parts:
        cur = cur / part
        try:
            if cur.is_symlink():
                return True
        except OSError:
            return True
    return False


def _collect_on_disk_protected_relpaths(repo: Path, policy) -> list[str]:
    """Return repo-relative paths of on-disk files that may be policy-protected.

    Used by ``find_dirty_protected_paths`` so porcelain-suppressing index flags
    (skip-worktree / assume-unchanged) cannot hide a divergent protected file:
    the filesystem walk still nominates the path for index-tag compensation,
    matching the pass-2 pattern in ``find_dirty_state_files``.

    Does not descend into symlinked directories (and skips any path that has a
    symlink ancestor): a symlink code-root or surface base must not re-emit
    target files under the link name.
    """
    found: list[str] = []
    extensions = tuple(getattr(policy, "protected_extensions", ()) or ())
    for code_root in tuple(getattr(policy, "code_roots", ()) or ()):
        rel_root = str(code_root).strip().strip("/")
        if not rel_root:
            continue
        base = repo / rel_root
        # Skip symlink roots (os.walk would still enter a symlink top) and
        # non-directories. is_symlink before is_dir so a link-to-dir is skipped.
        try:
            if base.is_symlink() or not base.is_dir():
                continue
        except OSError:
            continue
        for dirpath, dirnames, filenames in os.walk(base, followlinks=False):
            # Belt-and-suspenders: never descend into symlink subdirs even if a
            # future caller flips followlinks, and drop them before any scan.
            dirnames[:] = [
                d for d in dirnames if not (Path(dirpath) / d).is_symlink()
            ]
            for name in filenames:
                if extensions and not name.endswith(extensions):
                    continue
                path = Path(dirpath) / name
                if _path_has_symlink_ancestor(repo, path):
                    continue
                try:
                    found.append(path.relative_to(repo).as_posix())
                except ValueError:
                    continue
    for name in tuple(getattr(policy, "root_protected_files", ()) or ()):
        rel = str(name).strip().lstrip("/")
        if not rel:
            continue
        candidate = repo / rel
        if candidate.is_file() and not _path_has_symlink_ancestor(repo, candidate):
            found.append(rel)
    for surface in tuple(getattr(policy, "first_edit_protected_surfaces", ()) or ()):
        pattern = getattr(surface, "pattern", None)
        if not pattern:
            continue
        for path in _surface_filesystem_matches(repo, str(pattern)):
            if _path_has_symlink_ancestor(repo, path):
                continue
            try:
                found.append(path.relative_to(repo).as_posix())
            except ValueError:
                continue
    return found


def find_dirty_protected_paths(
    *,
    branch: str,
    repo_root: str,
    policy,
    protected_branches: set[str] | frozenset[str],
) -> tuple[str, list[str]] | DirtyProtectedDegraded | None:
    """Return dirty protected paths on a protected branch, or a degrade signal.

    Return values (three-state):

    * ``None`` — allow: not a protected branch, or a successful clean probe
      with no dirty protected paths.
    * ``(branch, paths)`` — block: successful probe found dirty protected
      paths (ordinary dirty-paths message).
    * ``DirtyProtectedDegraded`` — block fail-closed: the git status probe
      timed out or exited non-zero, path bases could not be reconciled, the
      ignored-file inventory could not be determined, the index-tag map was
      unavailable, or the scan root was missing on a protected branch — so
      cleanliness is unknown. Callers must not treat this as ``None``/allow;
      use ``.block_message`` for the distinct degraded wording (timeout /
      non-zero exit; run ``git status``).

    Porcelain alone is not a cleanliness proof: ``git status`` suppresses
    worktree divergence for ``--skip-worktree`` / ``--assume-unchanged``
    index entries. This probe reuses ``_batch_ls_files_v_tags`` and
    ``_is_tracked_clean_no_suppress`` so only a plain ``H`` tag with a
    porcelain-clean result may be treated as clean. Porcelain paths are
    always top-level-relative and are mapped onto ``repo_root`` before
    policy matching (same base reconciliation as ``find_dirty_state_files``).
    """
    # Non-protected branches short-circuit to allow. A falsy scan root on a
    # *protected* branch is "could not determine", not clean — degrade.
    if branch not in protected_branches:
        return None
    if not repo_root:
        return DirtyProtectedDegraded(branch=branch)

    repo = Path(repo_root)

    # Path-base reconciliation: porcelain is always top-level-relative even
    # under ``-C <subdir>``. Unreconciled bases must not silently drop paths
    # (that would ALLOW).
    toplevel = _git_toplevel(repo)
    if toplevel is None:
        return DirtyProtectedDegraded(branch=branch)
    repo_prefix = _repo_prefix_under_toplevel(repo, toplevel)
    if repo_prefix is None:
        return DirtyProtectedDegraded(branch=branch)

    raw_dirty = _git_dirty_paths(repo)
    if raw_dirty is None:
        # Could not determine — fail CLOSED (distinct from clean / None).
        return DirtyProtectedDegraded(branch=branch)

    pass1_dirty: set[str] = set()
    for toplevel_rel in raw_dirty:
        mapped = _toplevel_path_to_repo_relative(toplevel_rel, repo_prefix)
        if mapped is not None:
            pass1_dirty.add(mapped)

    # Candidates = porcelain dirt ∪ on-disk protected surfaces. The walk is
    # required so suppress-flagged tracked files (invisible to porcelain) still
    # enter the index-tag compensation path. Untracked gitignored paths from
    # the walk (e.g. venv/build artifacts under code_roots) must be excluded:
    # they carry no index tag, so compensation cannot prove them clean and
    # would false-block a clean main. One ``ls-files --others --ignored``
    # inventory intersected with the walk — never per-path git forks, and
    # never ``check-ignore --stdin`` (symlink pathspecs exit 128 and discard
    # the whole batch).
    on_disk = _collect_on_disk_protected_relpaths(repo, policy)
    ignored_on_disk = _batch_untracked_ignored_paths(repo, on_disk)
    if ignored_on_disk is None:
        # Inventory degrade is not "nothing ignored": without a complete
        # ignore set the on-disk walk would false-block clean trees that
        # only have gitignored artifacts under code_roots.
        return DirtyProtectedDegraded(branch=branch)
    if ignored_on_disk:
        on_disk = [p for p in on_disk if p not in ignored_on_disk]
    candidates = list(dict.fromkeys([*pass1_dirty, *on_disk]))

    index_tags = _batch_ls_files_v_tags(repo, candidates)
    if index_tags is None:
        # Tag map unavailable — no cleanliness proof stronger than porcelain.
        return DirtyProtectedDegraded(branch=branch)

    dirty_paths: list[str] = []
    for candidate in candidates:
        if not is_branch_isolation_protected_path(candidate, policy):
            continue
        # Permitted-surface carve-out: a dirty file that matches an entry in
        # ``permitted_main_surfaces`` is explicitly allowed on main. Counting
        # it as "dirty protected" would block unrelated permitted edits and
        # contradicts the carve-out itself.
        if find_permitted_main_surface(candidate, policy) is not None:
            continue
        # Fail closed on non-H tags / missing tags / porcelain dirt. Only a
        # plain H + porcelain-clean path may be subtracted.
        if _is_tracked_clean_no_suppress(
            repo,
            candidate,
            index_tags=index_tags,
            pass1_ok=True,
            pass1_dirty=pass1_dirty,
        ):
            continue
        dirty_paths.append(candidate)

    if not dirty_paths:
        return None
    return branch, sorted(dict.fromkeys(dirty_paths))


def _run_git_degraded(
    args: list[str],
    *,
    timeout: float = 5.0,
    text: bool = True,
    input: bytes | str | None = None,
) -> subprocess.CompletedProcess | None:
    """Run a guard git command; degrade loudly on timeout/OSError instead of crashing.

    Hardenings (internal, [RES-03]/[AGT-10]):

    * ``-c core.fsmonitor=false`` — the observed 5s stalls come from the
      fsmonitor daemon over a slow volume; the guard's scans must not depend
      on it.
    * ``subprocess.TimeoutExpired`` and ``OSError`` (including
      ``[Errno 7] Argument list too long``) are caught and reported as a
      WARNING with a ``None`` return (could-not-determine), so a slow or
      oversized git call degrades the check instead of aborting the whole
      hook with a raw traceback ([OBS-08]: "no answer" is a distinct outcome,
      not a crash). Callers already fail closed on ``None``.
    * ``text`` defaults to True for non-``-z`` callers (stable str return
      type). Callers that request ``git … -z`` MUST pass ``text=False`` so
      path bytes (including CR) are not rewritten by universal-newlines
      before NUL-splitting / ``os.fsdecode``.
    * ``input`` feeds stdin for batched git commands without forking once per
      path (historical callers; current ignore inventory uses argv-only
      ``ls-files``).
    """
    cmd = ["git", "-c", "core.fsmonitor=false", *args]
    try:
        return subprocess.run(
            cmd,
            capture_output=True,
            text=text,
            timeout=timeout,
            check=False,
            input=input,
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
    except OSError as exc:
        preview = " ".join(args[2:5]) if len(args) > 2 else " ".join(args)
        print(
            f"warning: `git {preview}` failed ({exc}); the guard degrades this "
            "scan to could-not-determine instead of crashing. Verify with "
            "`git status` by hand if in doubt.",
            file=sys.stderr,
        )
        return None


def _git_dirty_paths(repo_root: Path) -> list[str] | None:
    """Return dirty repo-relative paths, or ``None`` when the probe degraded.

    Distinguishes three outcomes:

    * ``None`` — could not determine (``_run_git_degraded`` returned ``None``,
      or ``git status`` exited non-zero). Callers must fail closed.
    * ``[]`` — successful probe with empty porcelain (genuinely clean tree).
    * non-empty ``list[str]`` — successful probe with dirty paths.

    Bytes mode: ``-z`` paths must not pass through ``text=True`` (CR→LF).
    """
    proc = _run_git_degraded(
        ["-C", str(repo_root), "status", "--porcelain=v1", "-z", "--untracked-files=all"],
        text=False,
    )
    if proc is None or proc.returncode != 0:
        return None
    if not proc.stdout:
        return []
    return [path for path in _parse_porcelain_z_paths(proc.stdout) if path.strip()]


def _git_toplevel(repo: Path) -> Path | None:
    """Return the absolute git top-level for ``repo``, or ``None`` if unavailable."""
    proc = _run_git_degraded(
        ["-C", str(repo), "rev-parse", "--show-toplevel"],
    )
    if proc is None or proc.returncode != 0:
        return None
    raw = (proc.stdout or "").strip()
    if not raw:
        return None
    try:
        return Path(raw).resolve()
    except OSError:
        return None


def _repo_prefix_under_toplevel(repo: Path, toplevel: Path) -> str | None:
    """Return ``repo`` as a POSIX path relative to ``toplevel``.

    Empty string means ``repo`` *is* the top-level. ``None`` means ``repo`` is
    not under the top-level (cannot reconcile path bases).
    """
    try:
        rel = repo.resolve().relative_to(toplevel.resolve())
    except (ValueError, OSError):
        return None
    text = rel.as_posix()
    return "" if text == "." else text


def _toplevel_path_to_repo_relative(toplevel_rel: str, repo_prefix: str) -> str | None:
    """Map a git top-level-relative path onto ``repo_root``-relative form.

    ``git status --porcelain`` always emits top-level-relative paths, even when
    invoked with ``-C <subdir>``. Pass-2 candidates and ``ls-files`` pathspecs
    are ``repo_root``-relative. Returns ``None`` when the path lies outside
    ``repo_root``.
    """
    normalized = toplevel_rel.replace("\\", "/").lstrip("/").rstrip("/")
    if not repo_prefix:
        return normalized
    if normalized == repo_prefix:
        return ""
    prefix = repo_prefix + "/"
    if normalized.startswith(prefix):
        return normalized[len(prefix) :]
    return None


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
       On success, the full porcelain path set is retained so pass 2 can
       decide tracked-cleanliness without re-spawning status per path.
       Porcelain paths are always top-level-relative; they are normalized to
       ``repo_root``-relative form before comparison. When the git top-level
       cannot be resolved (or ``repo_root`` is not under it), pass 1 is
       treated as degraded so subtraction fails closed rather than matching
       against an incompatible path base.
    2. A direct filesystem walk over every pattern in
       ``policy.state_dirty_surfaces`` catches state paths that git
       summarises at the directory level (e.g. ``!! .task-state/``) or
       that exist on disk but are otherwise invisible to Git. The same
       ignored-untracked filter keeps generated local handoff projections
       from making the main control-plane checkout unpushable.

       Pass 2 then *subtracts* paths that are positively proven
       tracked-and-clean: present in a batched ``git ls-files -v`` with a
       plain ``H`` index tag (not ``S`` skip-worktree, not a lowercase
       assume-unchanged tag, not any other flag) **and** absent from the
       pass-1 porcelain dirty set. When pass 1 degraded (timeout /
       non-zero / ``None`` / unreconciled path bases), a path-scoped
       porcelain fallback is used instead of the batched dirty set. Fail
       CLOSED: unknown index tags, missing batch results, timeouts, or
       non-zero git exits keep the path flagged dirty — only the proven
       tracked+clean+no-suppress-flag case is excluded. On-disk existence
       alone does **not** trip the tripwire for a committed, unmodified,
       unflagged state file.

    Returns a sorted, de-duplicated list of repo-relative POSIX paths.
    """
    repo = Path(repo_root) if repo_root else None
    if repo is None or not repo.is_dir():
        return []

    found: set[str] = set()
    pass1_ok = False
    pass1_dirty: set[str] = set()

    # Resolve the git top-level once so porcelain (always top-level-relative)
    # and repo_root-relative candidates share one path base. When this fails,
    # subtraction must not treat mismatched forms as "absent from dirty set".
    toplevel = _git_toplevel(repo)
    repo_prefix: str | None = (
        _repo_prefix_under_toplevel(repo, toplevel) if toplevel is not None else None
    )
    bases_reconciled = repo_prefix is not None

    # Pass 1 — git status with --ignored. Same parsing shape as
    # ``_git_dirty_paths`` plus a ``--ignored`` flag so gitignored
    # entries surface in the porcelain output. On a status timeout the pass
    # is skipped (warned by the helper); pass 2's filesystem walk still
    # covers on-disk state surfaces, so the tripwire is degraded, not blind.
    # Bytes mode required for ``-z`` (see ``_run_git_degraded``).
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
        text=False,
    )
    if proc is not None and proc.returncode == 0 and bases_reconciled:
        # Only mark pass1 usable when path bases can be reconciled; otherwise
        # fall through to path-scoped porcelain (fail closed on mismatch).
        assert repo_prefix is not None  # for type checkers; gated by bases_reconciled
        pass1_ok = True
        if proc.stdout:
            for toplevel_rel in _parse_porcelain_z_paths(proc.stdout):
                normalized = _toplevel_path_to_repo_relative(toplevel_rel, repo_prefix)
                if normalized is None:
                    continue
                pass1_dirty.add(normalized)
                if is_state_dirty_path(normalized, policy) and not _is_untracked_ignored_path(
                    repo, normalized
                ):
                    found.add(normalized)
    elif proc is not None and proc.returncode == 0 and not bases_reconciled:
        # Status itself succeeded but we cannot safely map paths onto
        # repo_root. Leave pass1_ok False so subtraction uses the path-scoped
        # fallback (or keeps candidates dirty) rather than a vacuous match.
        pass

    # Pass 2 — direct filesystem walk for every state surface pattern.
    # Necessary because ``git status --ignored`` often collapses an
    # ignored directory into one entry (``!! .task-state/``) instead of
    # listing the individual files beneath it, and the tripwire needs to
    # name the specific dirty file. Collect candidates first so index-tag
    # and (when needed) cleanliness checks can be batched / reused.
    candidates: list[str] = []
    for surface in policy.state_dirty_surfaces:
        for path in _surface_filesystem_matches(repo, surface.pattern):
            try:
                rel = path.relative_to(repo).as_posix()
            except ValueError:
                continue
            if is_state_dirty_path(rel, policy) and not _is_untracked_ignored_path(repo, rel):
                candidates.append(rel)

    index_tags = _batch_ls_files_v_tags(repo, candidates)
    for rel in candidates:
        if not _is_tracked_clean_no_suppress(
            repo,
            rel,
            index_tags=index_tags,
            pass1_ok=pass1_ok,
            pass1_dirty=pass1_dirty,
        ):
            found.add(rel)

    return sorted(found)


def _decode_git_z_path(raw: bytes | str) -> str:
    """Decode a single path field from ``git … -z`` output.

    Bytes mode uses ``os.fsdecode`` (surrogateescape) so undecodable path
    bytes round-trip and CR is preserved. Str input is accepted for
    non-``-z`` / test fixtures that already decoded text.
    """
    if isinstance(raw, bytes):
        return os.fsdecode(raw)
    return raw


def _parse_porcelain_z_paths(stdout: bytes | str) -> list[str]:
    """Parse ``git status --porcelain=v1 -z`` into normalized repo-relative paths.

    Prefer bytes stdout from ``_run_git_degraded(..., text=False)`` so CR and
    other non-newline path bytes are not rewritten by universal-newlines.
    """
    paths: list[str] = []
    if isinstance(stdout, bytes):
        entries: list[bytes | str] = stdout.split(b"\0")
    else:
        entries = stdout.split("\0")
    index = 0
    while index < len(entries):
        entry = entries[index]
        index += 1
        if not entry:
            continue
        # Status is always two ASCII bytes/chars; path follows the space.
        if isinstance(entry, bytes):
            if len(entry) < 3:
                continue
            status = entry[:2].decode("ascii", errors="replace")
            raw_path = _decode_git_z_path(entry[3:])
        else:
            status = entry[:2]
            raw_path = entry[3:]
        normalized = raw_path.replace("\\", "/").lstrip("/").rstrip("/")
        if normalized:
            paths.append(normalized)
        if status[0] in {"R", "C"} and index < len(entries):
            renamed = entries[index]
            index += 1
            renamed_text = _decode_git_z_path(renamed) if renamed else ""
            renamed_normalized = (
                renamed_text.replace("\\", "/").lstrip("/").rstrip("/") if renamed_text else ""
            )
            if renamed_normalized:
                paths.append(renamed_normalized)
    return paths


# Keep spawn count O(1) for ordinary state-surface walks. Chunk only when a
# candidate set is large enough that a single argv risks E2BIG / OSError.
# Bounds chosen well under typical ARG_MAX (~2MiB) while still covering tens of
# thousands of paths via a handful of spawns rather than one-per-path.
_LS_FILES_V_MAX_PATHS = 1024
_LS_FILES_V_MAX_ARGV_BYTES = 64 * 1024


def _chunk_pathspecs(
    paths: list[str],
    *,
    max_paths: int = _LS_FILES_V_MAX_PATHS,
    max_argv_bytes: int = _LS_FILES_V_MAX_ARGV_BYTES,
) -> list[list[str]]:
    """Split pathspecs into argv-safe batches (O(1) spawns in the common case).

    A single pathspec longer than ``max_argv_bytes`` is emitted as its own
    chunk (not co-packed with neighbours). That spawn may still raise
    OSError / degrade to ``None`` (fail-closed dirty) on a tight ARG_MAX —
    preferred over silently packing an oversized path with other pathspecs
    or skipping the path without a documented fail-closed outcome.
    """
    if not paths:
        return []
    chunks: list[list[str]] = []
    current: list[str] = []
    current_bytes = 0
    for path in paths:
        path_bytes = len(path.encode("utf-8", errors="surrogateescape")) + 1
        if current and (
            len(current) >= max_paths or current_bytes + path_bytes > max_argv_bytes
        ):
            chunks.append(current)
            current = []
            current_bytes = 0
        current.append(path)
        current_bytes += path_bytes
        # Note: an oversized singleton (path_bytes > max_argv_bytes) is left
        # in ``current`` and emitted by the loop-top flush on the next path
        # or by the trailing flush — an explicit mid-loop singleton flush is
        # a no-op (same chunk boundaries either way). The docstring above
        # still documents the accepted degrade when that spawn hits ARG_MAX.
    if current:
        chunks.append(current)
    return chunks


def _parse_ls_files_v_z_tags(stdout: bytes | str) -> dict[str, str]:
    """Parse ``git ls-files -v -z`` stdout into ``{path: tag}``.

    Prefer bytes stdout from ``_run_git_degraded(..., text=False)`` so CR in
    paths is preserved (text mode would rewrite CR→LF and break tag lookup).
    """
    tags: dict[str, str] = {}
    if isinstance(stdout, bytes):
        records: list[bytes | str] = (stdout or b"").split(b"\0")
    else:
        records = (stdout or "").split("\0")
    for record in records:
        if not record:
            continue
        # Record shape is still ``<tag><space><path>``; ``-z`` only changes the
        # terminator and disables quotePath (verified against real git output).
        if isinstance(record, bytes):
            if len(record) < 3 or record[1:2] != b" ":
                continue
            tag = chr(record[0])
            path = _decode_git_z_path(record[2:]).replace("\\", "/").lstrip("/")
        else:
            if len(record) < 3 or record[1] != " ":
                continue
            tag = record[0]
            path = record[2:].replace("\\", "/").lstrip("/")
        if path:
            tags[path] = tag
    return tags


def _batch_ls_files_v_tags(repo: Path, rel_paths: list[str]) -> dict[str, str] | None:
    """Return ``{path: index_tag}`` from batched ``git ls-files -v -z`` calls.

    Tags follow ``git ls-files -v``: ``H`` is a normal cached entry, ``S`` is
    skip-worktree, and a **lowercase** letter marks assume-unchanged. ``-z``
    disables C-quoting so non-ASCII paths match the raw pathspecs used as
    dict keys. Pathspecs are chunked so huge candidate sets do not hit
    ``OSError: [Errno 7] Argument list too long``. Returns ``None`` on timeout
    / OSError / non-zero so callers fail closed.

    ``-z`` spawns use bytes mode (``text=False``) so path bytes are matched
    against raw filesystem candidates after ``os.fsdecode``.
    """
    if not rel_paths:
        return {}
    # De-dupe while preserving order for stable command lines in tests.
    unique = list(dict.fromkeys(rel_paths))
    tags: dict[str, str] = {}
    for chunk in _chunk_pathspecs(unique):
        # ``-z``: NUL-terminated records, no quotePath; bytes stdout required.
        proc = _run_git_degraded(
            ["-C", str(repo), "ls-files", "-v", "-z", "--", *chunk],
            text=False,
        )
        if proc is None or proc.returncode != 0:
            return None
        tags.update(_parse_ls_files_v_z_tags(proc.stdout or b""))
    return tags


def _is_tracked_clean_no_suppress(
    repo: Path,
    rel_path: str,
    *,
    index_tags: dict[str, str] | None,
    pass1_ok: bool,
    pass1_dirty: set[str],
) -> bool:
    """Return True only when ``rel_path`` may be subtracted as tracked-and-clean.

    A path is subtractable only when all of the following hold:

    * ``git ls-files -v`` reports it with a plain ``H`` tag (no skip-worktree,
      assume-unchanged, or other index flag that suppresses porcelain);
    * it has no porcelain dirt — taken from the pass-1 dirty set when pass 1
      succeeded, otherwise from a path-scoped status fallback.

    Fail CLOSED: missing/failed index-tag batch, non-``H`` tag, pass-1
    degraded without a successful clean fallback, timeout, or non-zero git
    exit → ``False`` (path stays flagged dirty).
    """
    if index_tags is None:
        return False
    if index_tags.get(rel_path) != "H":
        return False
    if pass1_ok:
        return rel_path not in pass1_dirty
    # Pass 1 degraded: fall back to one path-scoped porcelain check.
    return _path_porcelain_is_clean(repo, rel_path)


def _path_porcelain_is_clean(repo: Path, rel_path: str) -> bool:
    """Fail-closed path-scoped porcelain cleanliness (fallback only)."""
    status = _run_git_degraded(
        [
            "-C",
            str(repo),
            "status",
            "--porcelain=v1",
            "--untracked-files=all",
            "--",
            rel_path,
        ],
    )
    if status is None or status.returncode != 0:
        return False
    return not (status.stdout or "").strip()


def _is_tracked_and_clean(repo: Path, rel_path: str) -> bool:
    """Compatibility helper: tracked + plain ``H`` + porcelain-clean.

    Prefer the batched path inside ``find_dirty_state_files``. This single-path
    form is retained for direct callers/tests and keeps the same fail-closed
    contract (including the index-flag rule).
    """
    tags = _batch_ls_files_v_tags(repo, [rel_path])
    return _is_tracked_clean_no_suppress(
        repo,
        rel_path,
        index_tags=tags,
        pass1_ok=False,
        pass1_dirty=set(),
    )


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


def _batch_untracked_ignored_paths(
    repo: Path, rel_paths: list[str]
) -> set[str] | None:
    """Return the subset of ``rel_paths`` that Git classifies as ignored.

    Enumerates the ignored untracked set once via
    ``git ls-files --others --ignored --exclude-standard -z`` and intersects
    with ``rel_paths``. This never resolves caller-supplied pathspecs, so a
    single symlink directory cannot poison the whole batch the way
    ``git check-ignore --stdin`` does (exit 128 after writing partial
    stdout — which must not be trusted as complete).

    Return values:

    * ``set[str]`` — successful inventory; may be empty when none of the
      candidates are ignored (distinct from degrade).
    * ``None`` — could not determine (timeout / OSError / non-zero exit /
      ``_run_git_degraded`` returned ``None``). Callers must treat this as
      probe degrade, not as "nothing ignored".

    Uses :data:`IGNORED_INVENTORY_TIMEOUT_S` rather than the short default
    status-probe budget: a whole-tree ignored walk is slower than a single
    path status check.
    """
    if not rel_paths:
        return set()
    candidates = set(rel_paths)
    proc = _run_git_degraded(
        [
            "-C",
            str(repo),
            "ls-files",
            "--others",
            "--ignored",
            "--exclude-standard",
            "-z",
        ],
        text=False,
        timeout=IGNORED_INVENTORY_TIMEOUT_S,
    )
    if proc is None or proc.returncode != 0:
        return None
    if not proc.stdout:
        return set()
    ignored: set[str] = set()
    for part in proc.stdout.split(b"\0"):
        if not part:
            continue
        path = os.fsdecode(part)
        if path in candidates:
            ignored.add(path)
    return ignored


def _surface_filesystem_matches(repo: Path, pattern: str):
    """Yield existing files under ``repo`` that match a YAML-style glob.

    Supports the subset used by ``state_dirty_surfaces``:

    - literal paths (``CURRENT_TASK.json``)
    - shallow globs handled by ``Path.glob`` (``foo/*.json``)
    - trailing ``**`` recursion (``.task-state/**``,
      ``docs/tasks/archive/**``)

    The walker yields file paths only (directories are skipped) so the
    caller can normalise them to repo-relative POSIX strings without
    re-checking ``is_file``. Paths that only exist through a symlinked
    directory are omitted (same rule as ``_collect_on_disk_protected_relpaths``).
    """
    if "**" in pattern:
        head, _, _ = pattern.partition("**")
        head = head.rstrip("/")
        base = repo / head if head else repo
        try:
            if base.is_symlink():
                return
        except OSError:
            return
        if base.is_file():
            if not _path_has_symlink_ancestor(repo, base):
                yield base
            return
        if base.is_dir():
            # Prefer os.walk(followlinks=False) over Path.rglob so symlink
            # directories under the surface head are never entered.
            for dirpath, dirnames, filenames in os.walk(base, followlinks=False):
                dirnames[:] = [
                    d for d in dirnames if not (Path(dirpath) / d).is_symlink()
                ]
                for name in filenames:
                    entry = Path(dirpath) / name
                    if entry.is_file() and not _path_has_symlink_ancestor(repo, entry):
                        yield entry
        return
    try:
        matches = list(repo.glob(pattern))
    except (OSError, ValueError):
        return
    for match in matches:
        if match.is_file() and not _path_has_symlink_ancestor(repo, match):
            yield match
