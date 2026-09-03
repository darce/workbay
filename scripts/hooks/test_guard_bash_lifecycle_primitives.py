"""Regression coverage for the raw-git-lifecycle Bash guard.

The guard's job is to make the lifecycle primitives non-optional: a rule an
agent has to remember is a buffer, not a drain. These tests pin both halves of
that contract, because a guard that blocks too much is abandoned and a guard
that blocks too little is decorative:

- **Every raw lifecycle intent is refused**, including the forms that hide the
  ``git`` token behind a global option (``git -C <path> commit``), a compound
  command (``cd x && git commit``), or an env prefix.
- **Read-only git is never touched.** ``git status``, ``git log``, and the
  listing forms of ``git branch`` / ``git worktree`` must pass through, or the
  guard becomes friction operators route around.

Driven as a subprocess against the real hook so the stdin/exit-code contract is
exercised exactly as the harness invokes it.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from pathlib import Path

import pytest

# Resolve against this test file's directory so the same constants work from the
# root tree (scripts/hooks/) and the packaged payload twin.
HOOK_SCRIPT = Path(__file__).resolve().parent / "guard-bash-lifecycle-primitives.py"

BYPASS_ENV = "WORKBAY_ALLOW_RAW_GIT_LIFECYCLE"


def _run(command: str, *, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    payload = {"tool_name": "Bash", "tool_input": {"command": command}}
    run_env = dict(os.environ)
    run_env.pop(BYPASS_ENV, None)
    if env:
        run_env.update(env)
    return subprocess.run(
        [sys.executable, str(HOOK_SCRIPT)],
        input=json.dumps(payload),
        capture_output=True,
        text=True,
        env=run_env,
    )


BLOCKED_COMMANDS = [
    # commit
    'git commit -m "message"',
    'git -C /tmp/some-worktree commit -m "message"',
    'cd /tmp/wt && git commit -m "message"',
    'git commit --amend --no-edit',
    # segmentation traps: glued `;` and bare newlines must still split — shlex
    # alone fuses these into one segment and the guard failed open on both.
    'cd /tmp/wt; git commit -m "message"',
    'echo hi\ngit commit -m "message"',
    # an export of anything OTHER than the exact bypass token is not an opt-in
    'export FOO=bar; git commit -m "message"',
    f'export {BYPASS_ENV}=0; git commit -m "message"',
    # worktree lifecycle
    "git worktree add ../repo-fix-thing-01 feature/fix-thing-01",
    "git worktree remove ../repo-fix-thing-01",
    "git worktree prune",
    # branch creation
    "git checkout -b feature/fix-thing-01",
    "git checkout -B feature/fix-thing-01",
    "git switch -c feature/fix-thing-01",
    "git switch --create feature/fix-thing-01",
    "git branch feature/fix-thing-01",
    # branch deletion / rename
    "git branch -d feature/fix-thing-01",
    "git branch -D feature/fix-thing-01",
    "git branch --delete feature/fix-thing-01",
    "git branch -m old-name feature/fix-thing-01",
]


ALLOWED_COMMANDS = [
    # read-only git
    "git status --porcelain",
    "git log --oneline -5",
    "git diff --stat HEAD",
    "git show --stat HEAD",
    "git rev-parse HEAD",
    "git commit --dry-run",
    # listing / query forms that share a subcommand with a blocked intent
    "git branch",
    "git branch -a",
    "git branch --list 'feature/*'",
    "git branch -v",
    "git branch --merged main",
    "git branch --contains HEAD",
    "git branch --show-current",
    "git worktree list",
    # the primitives themselves, and unrelated commands
    'make slice-commit TASK=FIX-THING-01 MSG="message"',
    'make task-start TASK=fix-thing-01 OBJECTIVE="objective"',
    "make task-reap REAP_ARGS=--apply",
    "pytest -q",
    'echo "git commit is the thing you should not run"',
]


@pytest.mark.parametrize("command", BLOCKED_COMMANDS)
def test_raw_lifecycle_command_is_blocked(command: str) -> None:
    result = _run(command)
    assert result.returncode == 2, f"expected block for {command!r}: {result.stderr}"
    assert "BLOCKED" in result.stderr
    # The whole point is naming the replacement, not just refusing.
    assert "make " in result.stderr, result.stderr


@pytest.mark.parametrize("command", ALLOWED_COMMANDS)
def test_read_only_and_primitive_commands_pass_through(command: str) -> None:
    result = _run(command)
    assert result.returncode == 0, f"unexpected block for {command!r}: {result.stdout}"


def test_bypass_env_allows_the_raw_command() -> None:
    result = _run('git commit -m "salvage"', env={BYPASS_ENV: "1"})
    assert result.returncode == 0, result.stdout


def test_bypass_env_only_honours_exact_opt_in() -> None:
    result = _run('git commit -m "message"', env={BYPASS_ENV: "0"})
    assert result.returncode == 2, result.stderr


def test_in_command_bypass_prefix_is_not_an_opt_in() -> None:
    # Only the standalone `export` segment form opts in; a per-segment env
    # prefix is stripped like any other assignment and the command is scanned.
    result = _run(f"{BYPASS_ENV}=1 git branch feature/rev4-fix-thing-01 abc1234")
    assert result.returncode == 2, result.stderr


def test_export_bypass_segment_enables_following_segments() -> None:
    result = _run(
        f"export {BYPASS_ENV}=1; git worktree add ../repo-fix-thing-01 feature/fix-thing-01"
    )
    assert result.returncode == 0, result.stderr


def test_export_bypass_works_with_spaced_semicolon_and_newline() -> None:
    for joiner in (" ; ", "\n", " && "):
        result = _run(f"export {BYPASS_ENV}=1{joiner}git commit -m salvage")
        assert result.returncode == 0, (joiner, result.stderr)


def test_bypass_is_audit_logged(tmp_path: Path) -> None:
    result = _run(
        f"export {BYPASS_ENV}=1 ; git commit -m salvage",
        env={"CLAUDE_PROJECT_DIR": str(tmp_path)},
    )
    assert result.returncode == 0, result.stderr
    log = tmp_path / ".task-state" / "lifecycle_guard_bypass.jsonl"
    assert log.exists(), "bypass must leave an audit record"
    record = json.loads(log.read_text().splitlines()[-1])
    assert record["bypassed_segments"] == ["git commit -m salvage"]


def test_shell_tool_name_is_scanned() -> None:
    # Cursor registers the guard under its Shell tool with no payload adapter.
    payload = {"tool_name": "Shell", "tool_input": {"command": 'git commit -m "m"'}}
    result = subprocess.run(
        [sys.executable, str(HOOK_SCRIPT)],
        input=json.dumps(payload),
        capture_output=True,
        text=True,
    )
    assert result.returncode == 2, result.stderr


def test_bypass_token_as_argument_does_not_bypass() -> None:
    result = _run(f"git commit -m {BYPASS_ENV}=1")
    assert result.returncode == 2, result.stderr


def test_in_command_prefix_honours_exact_opt_in_only() -> None:
    result = _run(f"{BYPASS_ENV}=0 git commit -m message")
    assert result.returncode == 2, result.stderr


def test_export_after_violation_does_not_rescue_it() -> None:
    result = _run(f"git branch feature/fix-thing-01; export {BYPASS_ENV}=1")
    assert result.returncode == 2, result.stderr


def test_block_message_lands_on_stderr() -> None:
    # Exit-2 PreToolUse feedback is read from stderr; stdout is dropped.
    result = _run('git commit -m "message"')
    assert result.returncode == 2
    assert "BLOCKED" in result.stderr, (result.stdout, result.stderr)


def test_non_bash_tool_is_ignored() -> None:
    payload = {"tool_name": "Edit", "tool_input": {"command": 'git commit -m "message"'}}
    result = subprocess.run(
        [sys.executable, str(HOOK_SCRIPT)],
        input=json.dumps(payload),
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout


def test_unparseable_command_still_scans() -> None:
    """Unbalanced quotes must not become a silent bypass."""
    result = _run('git commit -m "unterminated')
    assert result.returncode == 2, result.stdout


def test_malformed_payload_fails_open() -> None:
    """A broken payload must not wedge every Bash call in the session."""
    result = subprocess.run(
        [sys.executable, str(HOOK_SCRIPT)],
        input="not json",
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout


def test_remedy_names_the_matching_primitive() -> None:
    """Each intent points at the primitive that actually replaces it."""
    assert "slice-commit" in _run('git commit -m "m"').stderr
    assert "task-start" in _run("git worktree add ../wt feature/fix-thing-01").stderr
    assert "task-start" in _run("git checkout -b feature/fix-thing-01").stderr
    delete_err = _run("git branch -D feature/fix-thing-01").stderr
    assert "task-finish" in delete_err, delete_err


# --- remedies must name commands the consumer can actually run -----------------
#
# The remedy text used to offer `make lane-reap`, a target that exists only in
# WorkBay's own Makefile and is never shipped in the payload. The two tests above
# could not catch it: one asserts only that the substring "make " appears, and the
# other was an `or` that passed on the sibling. A gate that cannot fail is not a
# gate, so this one is fed both a known-good and a known-bad target below.

# The tree this guard actually lives in: the payload for the shipped copy, the
# repo root for the root twin. Deliberately relative, because "does this target
# exist" is only meaningful against the Makefile the operator reading the remedy
# would run. `lane-reap` exists at the repo root and not in the payload, which is
# the whole defect.
TREE_ROOT = Path(__file__).resolve().parents[2]


def _shipped_make_targets() -> set[str]:
    targets: set[str] = set()
    for mk in list((TREE_ROOT / "Makefile.d").glob("*.mk")) + [TREE_ROOT / "Makefile"]:
        if not mk.exists():
            continue
        for line in mk.read_text().splitlines():
            m = re.match(r"^([A-Za-z0-9][A-Za-z0-9._-]*)\s*:(?!=)", line)
            if m:
                targets.add(m.group(1))
    return targets


def _cited_make_targets() -> set[str]:
    # A remedy is an invocation, so the target is followed by an argument
    # assignment: `make task-finish TASK=<ref>`. Prose never is. Anchoring on
    # the string-literal start instead looks correct and is not: it goes blind
    # to a second command inside the same string -- exactly where the defect
    # lived, and the mutation probe caught the blindness.
    text = HOOK_SCRIPT.read_text()
    return set(re.findall(r"\bmake ([a-z][a-z0-9._-]*)(?=\s+[A-Z][A-Z0-9_]*=)", text))


def test_remedy_gate_detects_a_bogus_target() -> None:
    """The check must be able to fail, or it proves nothing (Principle 2)."""
    shipped = _shipped_make_targets()
    assert "task-finish" in shipped, "known-good target missing; the fixture is broken"
    if TREE_ROOT.name == "payload":
        # Payload twin: `lane-reap` is a WorkBay-repo-only target, so the check
        # here has real detection power. At the repo root it is shipped, and this
        # test degrades to the known-good half by design.
        assert "lane-reap" not in shipped, (
            "lane-reap is now shipped in the payload -- this fixture no longer "
            "distinguishes the defect it was written for; pick another known-bad "
            "target"
        )


def test_every_make_target_named_in_a_remedy_is_shipped() -> None:
    cited = _cited_make_targets()
    assert cited, "no `make <target>` found in the guard; the extractor has drifted"
    missing = sorted(cited - _shipped_make_targets())
    assert not missing, (
        f"guard remedies name make target(s) the payload does not ship: {missing}. "
        "An operator following the remedy gets 'No rule to make target'."
    )
