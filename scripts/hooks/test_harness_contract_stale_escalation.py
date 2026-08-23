"""The root contract loader must not fail open on an untrustworthy contract.

``scripts/hooks/`` is a materialized mirror of the payload tree, but nothing
gates it: ``make check-overlay-drift`` covers only
``docs/workbay/{contracts,rules}``. internal (``b8333ca1``, on main)
therefore hardened the payload copy of ``_harness_protocol.py`` and left the
root copy — the one ``.claude/settings.json`` actually executes via
``$CLAUDE_PROJECT_DIR/scripts/hooks/*`` — on the pre-cutover behaviour. This
module pins the three behaviours the root copy is missing, so the port can be
verified rather than eyeballed.

1. **Schema vs missing.** A contract that is present but unparseable is
   untrustworthy and must exit 2 whatever the caller's policy;
   ``guard-bash-main-branch.py`` hardcodes ``WARN``, so today every load failure
   returns 0 and the main-branch isolation guard allows the command.
2. **No empty-policy fail-open.** A ``branch_isolation:`` block that yields no
   ``first_edit_protected_surfaces`` currently parses into an all-empty policy
   with no error at all, and every path reports unprotected.
3. **The intent-named key wins.** ``is_branch_isolation_protected_path`` must
   read ``first_edit_protected_surfaces``, not the legacy
   ``protected_main_surfaces``. The two lists agree today (11 patterns each,
   both contracts), so this is latent — but internal phase B deletes
   the legacy key, and an unported root predicate would then read ``()`` and
   turn first-edit protection off silently. That is the plan's own headline
   stale-overlay fail-open, landing in-repo instead of in a consumer.

Each group has a control: the missing-file case must still honour the caller's
policy (a ``--profile minimal`` install legitimately ships no contract), and a
well-formed contract must still load, so a broken loader cannot fake a pass.
"""

from __future__ import annotations

import io
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "scripts" / "hooks"))

from _harness_protocol import (  # noqa: E402
    CONTRACT_RELATIVE_PATH,
    HarnessContractMissingError,
    HarnessContractMissingPolicy,
    handle_missing_contract,
    is_branch_isolation_protected_path,
    load_branch_isolation_policy,
)

# Absent from the root copy today; the payload twin defines it. Imported
# defensively so this module reports clean assertion failures instead of a
# collection error while the port is outstanding.
try:  # pragma: no cover - depends which copy is under test
    from _harness_protocol import HarnessContractSchemaError  # noqa: E402
except ImportError:  # pragma: no cover
    HarnessContractSchemaError = None  # type: ignore[assignment]


def _plant(tmp_path: Path, body: str) -> Path:
    contract = tmp_path / CONTRACT_RELATIVE_PATH
    contract.parent.mkdir(parents=True, exist_ok=True)
    contract.write_text(body, encoding="utf-8")
    return tmp_path


def _raised(workspace: Path) -> HarnessContractMissingError:
    with pytest.raises(HarnessContractMissingError) as excinfo:
        load_branch_isolation_policy(workspace)
    return excinfo.value


# ---------------------------------------------------------------------------
# 1. present-but-untrustworthy contracts escalate regardless of policy
# ---------------------------------------------------------------------------

# Every shape below is a file that EXISTS, so no hook may excuse it as a
# minimal install, yet nothing usable can be read out of it.
MALFORMED_CONTRACTS = {
    "empty_file": "",
    "no_branch_isolation_block": "some_other_key:\n  value: 1\n",
    "key_present_but_no_items": "branch_isolation:\n  first_edit_protected_surfaces:\n",
    "wrong_scalar_type": "branch_isolation:\n  first_edit_protected_surfaces: 7\n",
    "entry_missing_pattern": (
        "branch_isolation:\n"
        "  first_edit_protected_surfaces:\n"
        "    - reason: no pattern key at all\n"
    ),
    "entry_missing_reason": (
        "branch_isolation:\n"
        "  first_edit_protected_surfaces:\n"
        "    - pattern: docs/tasks/**\n"
    ),
}


@pytest.mark.parametrize("shape", sorted(MALFORMED_CONTRACTS))
def test_malformed_contract_raises_schema_error(tmp_path: Path, shape: str) -> None:
    """A present-but-unusable contract is a schema failure, not a missing one."""
    assert HarnessContractSchemaError is not None, (
        "_harness_protocol does not define HarnessContractSchemaError; the root "
        "copy is behind its payload twin"
    )
    error = _raised(_plant(tmp_path, MALFORMED_CONTRACTS[shape]))
    assert isinstance(error, HarnessContractSchemaError), (
        f"{shape}: expected HarnessContractSchemaError, got {type(error).__name__}"
    )


@pytest.mark.parametrize("shape", sorted(MALFORMED_CONTRACTS))
@pytest.mark.parametrize(
    "policy", [HarnessContractMissingPolicy.WARN, HarnessContractMissingPolicy.SILENT]
)
def test_malformed_contract_exits_2_regardless_of_policy(
    tmp_path: Path, shape: str, policy: HarnessContractMissingPolicy
) -> None:
    """WARN and SILENT must not downgrade an untrustworthy contract to exit 0.

    This is the live bypass: guard-bash-main-branch.py pins WARN, so a 0 here
    means the main-branch guard let the command through.
    """
    error = _raised(_plant(tmp_path, MALFORMED_CONTRACTS[shape]))
    stream = io.StringIO()
    code = handle_missing_contract(error, policy=policy, stream=stream)
    assert code == 2, (
        f"{shape} under {policy.value}: returned {code}; main-branch isolation "
        "is silently bypassed"
    )
    # An exit 2 with an empty stream is an unexplained hard block: the operator
    # sees a refusal with no reason and no remediation. Asserting the code
    # alone leaves that mutant alive.
    written = stream.getvalue()
    assert str(CONTRACT_RELATIVE_PATH) in written, (
        f"{shape} under {policy.value}: exit 2 was emitted with no reference to "
        f"{CONTRACT_RELATIVE_PATH}; stream was {written!r}"
    )


def test_empty_surface_block_does_not_yield_an_unprotected_policy(
    tmp_path: Path,
) -> None:
    """The fail-open with teeth: no raise AND nothing protected.

    Asserting the raise alone would not show why it matters. If the loader ever
    returns here instead of raising, these probes are what a hook would then
    compute -- every one of them a path the contract lists as protected.
    """
    workspace = _plant(
        tmp_path, "branch_isolation:\n  first_edit_protected_surfaces:\n"
    )
    try:
        policy = load_branch_isolation_policy(workspace)
    except HarnessContractMissingError:
        return  # loader refused the contract: correct
    unprotected = [
        probe
        for probe in (
            "docs/tasks/some-task-plan.md",
            "packages/workbay-system/workbay_system/cli.py",
            "Makefile",
            "scripts/hooks/guard-bash-main-branch.py",
        )
        if not is_branch_isolation_protected_path(probe, policy)
    ]
    pytest.fail(
        "a contract whose branch_isolation block yields no surfaces loaded "
        f"without error into an all-empty policy; unprotected: {unprotected}"
    )


@pytest.mark.parametrize(
    ("policy", "expected"),
    [
        (HarnessContractMissingPolicy.WARN, 0),
        (HarnessContractMissingPolicy.SILENT, 0),
        (HarnessContractMissingPolicy.BLOCK, 2),
    ],
)
def test_absent_contract_still_honours_caller_policy(
    tmp_path: Path, policy: HarnessContractMissingPolicy, expected: int
) -> None:
    """Control: the escalation must not swallow the missing-file case.

    A minimal consumer install ships no contract at all. If this turns red
    alongside a green group above, the fix over-escalated and now hard-blocks
    those installs.
    """
    error = _raised(tmp_path)  # nothing planted: the file is simply absent
    if HarnessContractSchemaError is not None:
        assert not isinstance(error, HarnessContractSchemaError), (
            "an absent contract must not be classified as a schema failure"
        )
    code = handle_missing_contract(error, policy=policy, stream=io.StringIO())
    assert code == expected


# ---------------------------------------------------------------------------
# 2. the predicate reads the intent-named key (internal)
# ---------------------------------------------------------------------------

# code_roots and protected_extensions are deliberately omitted so the surface
# lists are the ONLY route to a True verdict; a hit here cannot come from the
# extension branch.
DIVERGENT_LISTS = """\
branch_isolation:
  protected_main_surfaces:
    - pattern: docs/legacy-only/**
      reason: present only in the legacy list
  first_edit_protected_surfaces:
    - pattern: docs/first-edit-only/**
      reason: present only in the intent-named list
"""


def test_predicate_reads_first_edit_list_not_legacy(tmp_path: Path) -> None:
    """A divergent fixture proves which key the live predicate consults."""
    policy = load_branch_isolation_policy(_plant(tmp_path, DIVERGENT_LISTS))
    assert is_branch_isolation_protected_path("docs/first-edit-only/a.md", policy), (
        "the predicate ignored first_edit_protected_surfaces; when implementation note "
        "implementation note phase B deletes the legacy key this predicate reads () and "
        "first-edit protection silently turns off"
    )
    assert not is_branch_isolation_protected_path("docs/legacy-only/a.md", policy), (
        "the predicate is still reading the legacy protected_main_surfaces list"
    )


# ---------------------------------------------------------------------------
# 3. controls on the real contract
# ---------------------------------------------------------------------------


def test_repo_contract_still_loads() -> None:
    """Control: a well-formed contract loads, so the shapes above are genuinely
    malformed rather than tripping a loader this pin broke."""
    policy = load_branch_isolation_policy(REPO_ROOT)
    assert policy.first_edit_protected_surfaces, (
        "the repo contract parsed but yielded no protected surfaces; every "
        "malformed-shape assertion above would then prove nothing"
    )


def test_repo_contract_protects_a_known_first_edit_surface() -> None:
    """Control: the predicate is live on the real contract, not vacuously True."""
    policy = load_branch_isolation_policy(REPO_ROOT)
    assert is_branch_isolation_protected_path("docs/tasks/example-task-plan.md", policy)
    assert not is_branch_isolation_protected_path("README.md", policy)
