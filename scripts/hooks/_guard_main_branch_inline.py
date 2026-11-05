#!/usr/bin/env python3
"""Inline Python implementation for scripts/hooks/guard-main-branch.sh.

Promoted from a `python -c '...'` heredoc inside guard-main-branch.sh
to a standalone module so bash quoting cannot break the script. This is
the same bug class eradication as internal (_task_start_inline.py).

Reads two positional arguments set by guard-main-branch.sh:
    sys.argv[1]  REPO_ROOT  — absolute path to the repository root
    sys.argv[2]  BRANCH     — current git branch name

Reads JSON tool-invocation payload on stdin (Claude Code hook protocol).

Exit codes:
    0 — allow (optionally prints a BLOCKED reason to stdout for the
        bash wrapper to relay to stderr)
    2 — hard block (contract missing or unrecoverable error)
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

repo_root = Path(sys.argv[1])
branch = sys.argv[2]
sys.path.insert(0, str(repo_root / "scripts" / "hooks"))

from _branch_isolation_guard import (
    PROTECTED_EDIT_BRANCHES,
    DirtyProtectedDegraded,
    build_branch_naming_block_reason,
    check_branch_naming,
    check_file_edit,
    extract_candidate_paths,
    find_dirty_protected_paths,
    normalize_path_token,
    path_identifies_repo_root,
    resolve_path_branch,
    to_repo_relative,
)
from _harness_protocol import (
    HarnessContractMissingError,
    is_branch_isolation_protected_path,
    load_branch_isolation_policy,
)

from _interp import resolve_env_alias

try:
    payload = json.load(sys.stdin)
except Exception:
    raise SystemExit(0)

tool_name = payload.get("toolName") or payload.get("tool_name") or ""
tool_input = payload.get("toolInput") or payload.get("tool_input") or {}
if not isinstance(tool_input, dict):
    raise SystemExit(0)

try:
    policy = load_branch_isolation_policy(repo_root)
except HarnessContractMissingError as exc:
    print(str(exc), file=sys.stderr)
    raise SystemExit(2)

# Shared with bash/check_file_edit so the protected set cannot drift. [ARCH-13]
protected_branches = set(PROTECTED_EDIT_BRANCHES)

# Branch-naming gate (internal). Fires before the
# protected-path check so that non-conforming branches are rejected
# uniformly — including ``fix/foo`` / ``chore/bar`` / ``wip-thing``
# shapes that the protected-path guard never inspected. The override
# env var is the documented escape valve; it bypasses PreToolUse but
# does not propagate to the pre-commit / pre-push gates (they read
# their own env vars per implementation note §4 / §4b).
non_conforming = check_branch_naming(branch)
if (
    non_conforming is not None
    and resolve_env_alias("WORKBAY_ALLOW_NONCONFORMING_BRANCH") != "1"
):
    print(build_branch_naming_block_reason(non_conforming))
    raise SystemExit(0)

attempted = check_file_edit(
    tool_name,
    tool_input,
    branch=branch,
    repo_root=str(repo_root),
    policy=policy,
    protected_branches=protected_branches,
)
if attempted is not None:
    resolved_branch, blocked_paths = attempted
    rendered_paths = "\n".join(f"  - {path}" for path in blocked_paths)
    print(
        "BLOCKED: Protected edits are not allowed on the main branch.\n\n"
        f"Branch: {resolved_branch}\n"
        "Files:\n"
        f"{rendered_paths}\n\n"
        "Create a feature branch first:\n"
        "  git checkout -b feature/<task-id>-<slug>\n\n"
        "If you already have dirty code changes on main, move them to a feature "
        "branch or stash them before continuing.\n\n"
        "Isolation options:\n"
        "  1. Feature branch for single-agent work\n"
        "  2. Worktree isolation for delegated subtasks\n"
        "  3. Lane orchestration for multi-agent parallel work\n\n"
        "Only explicitly permitted operator docs/config surfaces remain allowed "
        "on main.\n"
        "Planning docs and implementation files now require a feature branch "
        "from the first edit.\n"
        "See: docs/workbay/rules/development-workflow.md"
        "#branch-isolation-protocol-mandatory"
    )
    raise SystemExit(0)

dirty = find_dirty_protected_paths(
    branch=branch,
    repo_root=str(repo_root),
    policy=policy,
    protected_branches=protected_branches,
)
if dirty is None:
    raise SystemExit(0)

# Third state: git status probe degraded — fail CLOSED with a distinct
# message (not the ordinary dirty-paths wording). Cleanliness is unknown.
if isinstance(dirty, DirtyProtectedDegraded):
    print(dirty.block_message)
    raise SystemExit(0)

resolved_branch, dirty_paths = dirty

# Per-path worktree resolution: if every candidate edit path resolves (via
# its own worktree) to a non-protected branch, the edit isn't a main-branch
# write at all and the dirty-paths-on-main check shouldn't apply. This
# unblocks edits to sibling worktrees while the harness cwd remains on main.
#
# F-HARM-S2-01 / [SEC-01]: do NOT short-circuit on raw un-normalized tokens.
# Padded/quoted absolute spellings of the protected checkout re-anchor through
# resolve_path_branch to the hook cwd (a feature worktree) and would ALLOW
# while the bash guard BLOCKs. Refuse the short-circuit when any normalized
# token identifies the repo root, and feed normalize_path_token into
# resolve_path_branch so the inline guard normalizes at least as much as the
# bash executor.
raw_paths = [p for p in extract_candidate_paths(tool_name, tool_input) if p]
if raw_paths:
    if any(
        path_identifies_repo_root(normalize_path_token(p), repo_root)
        for p in raw_paths
    ):
        pass  # fall through to the BR-21 identity arm; do NOT SystemExit(0)
    else:
        resolved_branches = [
            (resolve_path_branch(normalize_path_token(p)) or branch)
            for p in raw_paths
        ]
        if all(b not in protected_branches for b in resolved_branches):
            raise SystemExit(0)

# BR-21: if every candidate edit path is non-protected (permitted_main_surfaces
# is already a subset of that set) AND none of the candidate paths are
# themselves in the dirty-protected set, the edit must not be blocked by
# unrelated dirty code. Emit a warning and allow the edit; the dirty state
# still needs remediation, but that is a separate cleanup.
#
# Ancestor-identity for the None (unrelativizable) arm [SECD-05][ARCH-13]
# [WEB-33]: spoofed in-repo spellings must not take the WARN+allow carve-out
# (fall through to hard BLOCK). Genuine outsiders count as non-protected so
# dirty-main does not hard-block ordinary /tmp edits (WARN-vs-BLOCK preserved).
candidate_rel_paths: list[str] = []
has_outsider = False
has_spoofed_identity = False
for raw in extract_candidate_paths(tool_name, tool_input):
    rel = to_repo_relative(raw, str(repo_root))
    if rel is None:
        if path_identifies_repo_root(raw, repo_root):
            has_spoofed_identity = True
        else:
            has_outsider = True
        continue
    if not rel:
        continue
    candidate_rel_paths.append(rel)
if has_spoofed_identity:
    # Spoofed identity of the protected checkout: do not WARN+allow.
    all_non_protected = False
    candidates_clean = False
else:
    all_non_protected = bool(candidate_rel_paths or has_outsider) and all(
        not is_branch_isolation_protected_path(p, policy) for p in candidate_rel_paths
    )
    candidates_clean = not any(p in set(dirty_paths) for p in candidate_rel_paths)
if all_non_protected and candidates_clean:
    rendered_paths = "\n".join(f"  - {path}" for path in dirty_paths)
    print(
        "WARNING: Protected code files are dirty on main, but this edit targets "
        "only non-protected paths — allowing.\n\n"
        f"Branch: {resolved_branch}\n"
        "Dirty files (still need remediation):\n"
        f"{rendered_paths}\n\n"
        "Remediation options (not required for this edit):\n"
        "  1. git checkout -b feature/<task-id>-<slug> to move dirty changes off main\n"
        "  2. git stash push -m 'pre-main-cleanup' to set them aside\n"
        "  3. git restore <files> to discard them\n",
        file=sys.stderr,
    )
    raise SystemExit(0)

rendered_paths = "\n".join(f"  - {path}" for path in dirty_paths)
print(
    "BLOCKED: Protected code files are already dirty on the main branch.\n\n"
    f"Branch: {resolved_branch}\n"
    "Dirty files:\n"
    f"{rendered_paths}\n\n"
    "Move the work onto a feature branch or stash it before making more edits.\n\n"
    "Recommended recovery:\n"
    "  1. git checkout -b feature/<task-id>-<slug>\n"
    "  2. keep the dirty changes on that branch, or stash them intentionally\n"
    "  3. return to main only after the protected paths are clean again\n\n"
    "See: docs/workbay/rules/development-workflow.md"
    "#branch-isolation-protocol-mandatory"
)
