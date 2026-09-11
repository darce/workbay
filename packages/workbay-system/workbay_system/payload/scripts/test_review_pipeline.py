"""End-to-end coverage for the review-pipeline primitives (stage/adjudicate/merge-gate).

Each transition must be re-runnable from durable artifacts alone and fail
closed: an absent doc, a doc without proof-of-reading, or a stale adjudication
must refuse, never proceed.
"""

from __future__ import annotations

import importlib.util
import json
import os
import re
import subprocess
import sys
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parent / "review_pipeline.py"
REPO_ROOT = Path(__file__).resolve().parents[5]
ROOT_GUARD = REPO_ROOT / "scripts" / "hooks" / "guard-bash-lifecycle-primitives.py"
PAYLOAD_GUARD = SCRIPT.parent / "hooks" / "guard-bash-lifecycle-primitives.py"


def _git(repo: Path, *args: str) -> str:
    env = {
        **os.environ,
        "GIT_AUTHOR_NAME": "t",
        "GIT_AUTHOR_EMAIL": "t@t",
        "GIT_COMMITTER_NAME": "t",
        "GIT_COMMITTER_EMAIL": "t@t",
    }
    return subprocess.run(
        ["git", "-C", str(repo), *args], capture_output=True, text=True, check=True, env=env
    ).stdout.strip()


def _run(repo: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), "--root", str(repo), *args],
        capture_output=True,
        text=True,
    )


@pytest.fixture()
def repo(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-b", "main")
    (repo / "a.txt").write_text("a\n")
    _git(repo, "add", "a.txt")
    _git(repo, "commit", "-m", "base")
    _git(repo, "branch", "feature/fix-case-01")
    _git(repo, "checkout", "feature/fix-case-01")
    (repo / "b.txt").write_text("b\n")
    _git(repo, "add", "b.txt")
    _git(repo, "commit", "-m", "subject work")
    _git(repo, "checkout", "main")
    return repo


def _stage(repo: Path) -> dict:
    result = _run(repo, "stage", "--subject", "feature/fix-case-01", "--slug", "case")
    assert result.returncode == 0, result.stdout + result.stderr
    return json.loads(result.stdout)


def _review_doc(
    _repo: Path,
    receipt: dict,
    *,
    verdict: str,
    findings: str = "",
    commit: bool = True,
) -> Path:
    sidecar = json.loads(Path(receipt["por_sidecar"]).read_text())
    keys = sidecar["keys"]
    samples = "\n".join(v for v in keys["sample_lines"].values() if v.strip())
    doc = Path(receipt["worktree"]) / "docs" / "reviews" / "REV.md"
    doc.write_text(f"VERDICT: {verdict}\n\nPOR: {json.dumps(keys, sort_keys=True)}\n{samples}\n\n{findings}\n")
    if commit:
        rel = doc.relative_to(Path(receipt["worktree"]))
        _git(Path(receipt["worktree"]), "add", str(rel))
        _git(Path(receipt["worktree"]), "commit", "-m", "review: record adjudication document", "--", str(rel))
    return doc


def _pipeline_module():
    sys.path.insert(0, str(SCRIPT.parent))
    import review_pipeline

    return review_pipeline


def _load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _prepare_explicit_review_worktree(repo: Path) -> Path:
    worktree = repo.parent / "explicit-review"
    _git(
        repo,
        "worktree",
        "add",
        "-b",
        "feature/rev1-fix-case-01",
        str(worktree),
        "main",
    )
    return worktree


def test_stage_commits_patch_and_withholds_keys(repo: Path) -> None:
    receipt = _stage(repo)
    patch = Path(receipt["patch"])
    assert patch.is_file()
    assert receipt["diff_outcome"] == "nonempty"
    worktree = Path(receipt["worktree"])
    assert _git(worktree, "log", "-1", "--pretty=%s").startswith("review: stage input patch")
    sidecar = Path(receipt["por_sidecar"])
    assert ".task-state" in str(sidecar) and sidecar.is_file()
    # the patch is tracked in the review worktree, the sidecar is not
    assert str(patch.relative_to(worktree)) in _git(worktree, "ls-files")
    assert "review-por" not in _git(worktree, "ls-files")


def test_stage_without_worktree_cuts_rev_worktree_from_integration(repo: Path) -> None:
    receipt = _stage(repo)
    assert receipt["rev_branch"] == "feature/rev1-fix-case-01"
    worktree = Path(receipt["worktree"])
    assert worktree == repo.parent / f"{repo.name}-rev1-fix-case-01"
    assert _git(worktree, "rev-parse", "--abbrev-ref", "HEAD") == "feature/rev1-fix-case-01"
    # cut from the integration head: the parent of the stage commit is main's tip
    assert _git(worktree, "rev-parse", "HEAD~1") == _git(repo, "rev-parse", "main")
    # the integration checkout gains only the gitignored sidecar dir
    dirty = [line for line in _git(repo, "status", "--porcelain").splitlines() if ".task-state" not in line]
    assert dirty == []


def test_stage_refuses_when_rev_worktree_path_exists(repo: Path) -> None:
    (repo.parent / f"{repo.name}-rev1-fix-case-01").mkdir()
    result = _run(repo, "stage", "--subject", "feature/fix-case-01", "--slug", "case")
    assert result.returncode == 2
    assert json.loads(result.stdout)["reason"] == "rev_worktree_path_exists"


def test_stage_refuses_missing_explicit_worktree(repo: Path) -> None:
    result = _run(
        repo,
        "stage",
        "--subject",
        "feature/fix-case-01",
        "--slug",
        "case",
        "--worktree",
        str(repo.parent / "nonexistent"),
    )
    assert result.returncode == 2
    assert json.loads(result.stdout)["reason"] == "worktree_missing"


def test_adjudicate_merge_when_proof_present_and_no_blocking_findings(repo: Path) -> None:
    receipt = _stage(repo)
    doc = _review_doc(repo, receipt, verdict="MERGE", findings="- F1 (LOW): cosmetic nit")
    result = _run(
        repo,
        "adjudicate",
        "--subject",
        "feature/fix-case-01",
        "--patch",
        receipt["patch"],
        "--docs",
        str(doc),
    )
    assert result.returncode == 0, result.stdout
    payload = json.loads(result.stdout)
    assert payload["verdict"] == "MERGE"
    gate = _run(repo, "merge-gate", "--subject", "feature/fix-case-01")
    assert gate.returncode == 0, gate.stdout


def test_adjudicate_revise_on_surviving_medium_finding(repo: Path) -> None:
    receipt = _stage(repo)
    doc = _review_doc(repo, receipt, verdict="MERGE", findings="- F1 (MEDIUM): real defect")
    result = _run(
        repo,
        "adjudicate",
        "--subject",
        "feature/fix-case-01",
        "--patch",
        receipt["patch"],
        "--docs",
        str(doc),
    )
    assert result.returncode == 2
    assert json.loads(result.stdout)["verdict"] == "REVISE"


def test_adjudicate_discards_doc_without_proof_of_reading(repo: Path) -> None:
    receipt = _stage(repo)
    doc = Path(receipt["worktree"]) / "docs" / "reviews" / "REV.md"
    doc.write_text("VERDICT: MERGE\n\nno proof block here\n")
    _git(Path(receipt["worktree"]), "add", str(doc.relative_to(Path(receipt["worktree"]))))
    _git(Path(receipt["worktree"]), "commit", "-m", "review: record malformed document")
    result = _run(
        repo,
        "adjudicate",
        "--subject",
        "feature/fix-case-01",
        "--patch",
        receipt["patch"],
        "--docs",
        str(doc),
    )
    assert result.returncode == 2
    assert json.loads(result.stdout)["reason"] == "excluded_expected_documents"


def test_adjudicate_discards_doc_without_a_verdict(repo: Path) -> None:
    receipt = _stage(repo)
    sidecar = json.loads(Path(receipt["por_sidecar"]).read_text())
    keys = sidecar["keys"]
    samples = "\n".join(value for value in keys["sample_lines"].values() if value.strip())
    doc = Path(receipt["worktree"]) / "docs" / "reviews" / "NO-VERDICT.md"
    doc.write_text(f"POR: {json.dumps(keys, sort_keys=True)}\n{samples}\n")
    _git(Path(receipt["worktree"]), "add", str(doc.relative_to(Path(receipt["worktree"]))))
    _git(Path(receipt["worktree"]), "commit", "-m", "review: record verdictless document")
    result = _run(
        repo,
        "adjudicate",
        "--subject",
        "feature/fix-case-01",
        "--patch",
        receipt["patch"],
        "--docs",
        str(doc),
    )
    assert result.returncode == 2
    payload = json.loads(result.stdout)
    assert payload["reason"] == "excluded_expected_documents"
    assert payload["docs"][0]["reason"] == "no_verdict_line"


def test_merge_gate_refuses_stale_tip(repo: Path) -> None:
    receipt = _stage(repo)
    doc = _review_doc(repo, receipt, verdict="MERGE")
    assert (
        _run(
            repo,
            "adjudicate",
            "--subject",
            "feature/fix-case-01",
            "--patch",
            receipt["patch"],
            "--docs",
            str(doc),
        ).returncode
        == 0
    )
    # a commit lands on the subject after adjudication -> the artifact is stale
    _git(repo, "checkout", "feature/fix-case-01")
    (repo / "c.txt").write_text("c\n")
    _git(repo, "add", "c.txt")
    _git(repo, "commit", "-m", "post-adjudication commit")
    _git(repo, "checkout", "main")
    gate = _run(repo, "merge-gate", "--subject", "feature/fix-case-01")
    assert gate.returncode == 2
    assert json.loads(gate.stdout)["reason"] == "stale_adjudication_tip"


def test_adjudicate_refuses_when_subject_moved_after_stage(repo: Path) -> None:
    receipt = _stage(repo)
    doc = _review_doc(repo, receipt, verdict="MERGE")
    # a commit lands on the subject after staging -> reviewers hashed an old patch
    _git(repo, "checkout", "feature/fix-case-01")
    (repo / "d.txt").write_text("d\n")
    _git(repo, "add", "d.txt")
    _git(repo, "commit", "-m", "post-stage commit")
    _git(repo, "checkout", "main")
    result = _run(
        repo,
        "adjudicate",
        "--subject",
        "feature/fix-case-01",
        "--patch",
        receipt["patch"],
        "--docs",
        str(doc),
    )
    assert result.returncode == 2
    assert json.loads(result.stdout)["reason"] == "subject_tip_moved_since_stage"


def test_merge_gate_refuses_when_no_artifact(repo: Path) -> None:
    gate = _run(repo, "merge-gate", "--subject", "feature/fix-case-01")
    assert gate.returncode == 2
    assert json.loads(gate.stdout)["reason"] == "adjudication_artifact_missing"


def _marker_line(fill: str, label: str, size: int = 7) -> str:
    """Build a real conflict-marker line without writing one in this source.

    A literal marker here would make this test file itself trip the very gate
    it exercises, and would break `git merge` on this file forever.
    """
    return f"{fill * size} {label}\n"


def _commit_markers_on_subject(repo: Path, *, path: str = "doc.md", size: int = 7) -> None:
    _git(repo, "checkout", "feature/fix-case-01")
    (repo / path).write_text(
        _marker_line("<", "HEAD", size) + "ours\n" + "=" * size + "\n" + "theirs\n" + _marker_line(">", "main", size)
    )
    _git(repo, "add", path)
    _git(repo, "commit", "-m", "wip: main sync with unresolved conflict markers")
    _git(repo, "checkout", "main")


def _adjudicated(repo: Path) -> None:
    receipt = _stage(repo)
    doc = _review_doc(repo, receipt, verdict="MERGE")
    assert (
        _run(
            repo,
            "adjudicate",
            "--subject",
            "feature/fix-case-01",
            "--patch",
            receipt["patch"],
            "--docs",
            str(doc),
        ).returncode
        == 0
    )


def test_merge_gate_refuses_a_subject_tip_carrying_conflict_markers(repo: Path) -> None:
    """A ``wip: ... unresolved conflict markers`` branch must not reach main.

    This is the shape that actually landed: commit ``6b43bdfa6`` committed both
    halves of a three-way conflict into the landing-plan document as
    "resolution lane input", was never resolved, and merged. The slice-commit
    guard covers lane commits; nothing covered the merge, so the orchestration
    document for the landing work sat corrupt for rounds.
    """
    _commit_markers_on_subject(repo)
    _adjudicated(repo)
    gate = _run(repo, "merge-gate", "--subject", "feature/fix-case-01")
    assert gate.returncode == 2, gate.stdout
    payload = json.loads(gate.stdout)
    assert payload["reason"] == "subject_tip_has_conflict_markers"
    assert payload["hit_count"] >= 2
    assert any("doc.md" in hit for hit in payload["hits"])


def test_merge_gate_honours_a_widened_conflict_marker_size(repo: Path) -> None:
    """``merge.conflictMarkerSize`` is configurable; a hardwired 7 misses them.

    Without this the gate passes a genuinely conflicted blob in any repository
    that widened its markers, which is the silent-pass direction.
    """
    _git(repo, "config", "merge.conflictMarkerSize", "11")
    _commit_markers_on_subject(repo, size=11)
    _adjudicated(repo)
    gate = _run(repo, "merge-gate", "--subject", "feature/fix-case-01")
    assert gate.returncode == 2, gate.stdout
    assert json.loads(gate.stdout)["reason"] == "subject_tip_has_conflict_markers"


def test_clean_subject_tip_still_passes_and_says_so(repo: Path) -> None:
    """The scan must not refuse an ordinary branch, and must report the field."""
    _adjudicated(repo)
    gate = _run(repo, "merge-gate", "--subject", "feature/fix-case-01")
    assert gate.returncode == 0, gate.stdout
    payload = json.loads(gate.stdout)
    assert payload["reason"] == "ok"
    assert payload["conflict_markers_allowed"] is False


def test_escape_hatch_permits_deliberate_marker_text_and_is_reported(repo: Path) -> None:
    """A fixture may need the literal text; the override is never implicit.

    Asserting the receipt field matters as much as the exit code: an override
    that passed silently would be indistinguishable from a scan that found
    nothing, and nobody auditing the merge later could tell them apart.
    """
    _commit_markers_on_subject(repo)
    _adjudicated(repo)
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--root", str(repo), "merge-gate", "--subject", "feature/fix-case-01"],
        capture_output=True,
        text=True,
        env={**os.environ, "WORKBAY_ALLOW_CONFLICT_MARKERS": "1"},
    )
    assert result.returncode == 0, result.stdout + result.stderr
    payload = json.loads(result.stdout)
    assert payload["reason"] == "ok"
    assert payload["conflict_markers_allowed"] is True


def test_scan_failure_refuses_rather_than_reading_as_clean(repo: Path, monkeypatch) -> None:
    """An unknown answer is not absence.

    ``git grep`` exits 1 for "no match" and 0 for "matched"; every other exit is
    a failure. Treating those as clean would authorise exactly the merge the
    gate exists to refuse, so they must surface as a distinct refusal.
    """
    sys.path.insert(0, str(SCRIPT.parent))
    import review_pipeline

    monkeypatch.setattr(
        review_pipeline.subprocess,
        "run",
        lambda *a, **k: subprocess.CompletedProcess(a[0], 128, "", "fatal: bad object"),
    )
    hits, error = review_pipeline.scan_tip_for_conflict_markers(repo, "deadbeef")
    assert hits == []
    assert error is not None and "fatal" in error


@pytest.mark.parametrize(
    ("line", "expected"),
    [
        ("VERDICT: MERGE", "MERGE"),
        ("VERDICT: REVISE", "REVISE"),
        ("VERDICT: MERGEABLE", None),
        ("VERDICT: MERGE-EXTRA", None),
        ("VERDICT: REVISE-ME", None),
    ],
)
def test_verdict_parser_accepts_only_closed_tokens(line: str, expected: str | None) -> None:
    assert _pipeline_module()._doc_verdict(line + "\n") == expected


def test_finding_grammar_counts_house_style_and_ignores_unknown_prose() -> None:
    review_pipeline = _pipeline_module()
    text = "\n".join(
        [
            "- F1 (HIGH): direct finding",
            "### H-1 — heading-style finding",
            "Severity: high",
            "G1-1 — grouped finding",
            "Severity: HIGH",
            "ordinary prose says HIGH but is not a finding",
            "- F2 (LOW): harmless note",
        ]
    )
    parsed = review_pipeline._parse_findings(text)
    assert parsed.blocking == 3
    assert parsed.invalid == ()
    assert review_pipeline._count_blocking_findings(text) == 3


def test_finding_grammar_counts_letter_prefixed_medium_and_low_ids() -> None:
    review_pipeline = _pipeline_module()

    parsed = review_pipeline._parse_findings("### M-2 — medium — defect\n### L-3 — low — note\n")

    assert parsed.blocking == 1
    assert parsed.findings == (
        {"id": "M-2", "severity": "MEDIUM"},
        {"id": "L-3", "severity": "LOW"},
    )


def test_unknown_finding_shape_is_rejected_in_adjudication(repo: Path) -> None:
    receipt = _stage(repo)
    doc = _review_doc(repo, receipt, verdict="MERGE", findings="F2 HIGH prose without a severity marker")
    result = _run(
        repo,
        "adjudicate",
        "--subject",
        "feature/fix-case-01",
        "--patch",
        receipt["patch"],
        "--docs",
        str(doc),
    )
    assert result.returncode == 2
    payload = json.loads(result.stdout)
    assert payload["reason"] == "excluded_expected_documents"
    assert payload["docs"][0]["reason"] == "invalid_finding_grammar"


def test_finding_parser_rejects_unknown_id_suffixes() -> None:
    review_pipeline = _pipeline_module()
    parsed = review_pipeline._parse_findings("F1foo (HIGH): not a canonical finding\n")
    assert parsed.blocking == 0
    assert parsed.findings == ()
    assert parsed.invalid


@pytest.mark.parametrize("severity", ["high-ish", "high-level"])
def test_finding_parser_rejects_suffixed_severity_tokens(severity: str) -> None:
    review_pipeline = _pipeline_module()

    parsed = review_pipeline._parse_findings(f"F1 — Severity: {severity} — not canonical\n")

    assert parsed.blocking == 0
    assert parsed.findings == ()
    assert parsed.invalid


def test_missing_expected_sibling_cannot_authorize_merge(repo: Path) -> None:
    receipt = _stage(repo)
    valid = _review_doc(repo, receipt, verdict="MERGE")
    missing = Path(receipt["worktree"]) / "docs" / "reviews" / "MISSING.md"
    result = _run(
        repo,
        "adjudicate",
        "--subject",
        "feature/fix-case-01",
        "--patch",
        receipt["patch"],
        "--docs",
        str(valid),
        str(missing),
    )
    assert result.returncode == 2
    payload = json.loads(result.stdout)
    assert payload["reason"] == "excluded_expected_documents"
    assert payload["counted_count"] == 1
    assert payload["excluded_count"] == 1
    assert payload["empty_count"] == 0
    assert payload["docs"][1]["status"] == "excluded"


def test_excluded_only_documents_are_not_reported_as_empty(repo: Path) -> None:
    receipt = _stage(repo)
    missing = Path(receipt["worktree"]) / "docs" / "reviews" / "MISSING.md"
    result = _run(
        repo,
        "adjudicate",
        "--subject",
        "feature/fix-case-01",
        "--patch",
        receipt["patch"],
        "--docs",
        str(missing),
    )
    assert result.returncode == 2
    payload = json.loads(result.stdout)
    assert payload["reason"] == "excluded_expected_documents"
    assert payload["counted_count"] == 0
    assert payload["excluded_count"] == 1
    assert payload["empty_count"] == 0
    assert payload["empty"] is False


def test_failed_re_adjudication_cannot_leave_old_merge_authorization(repo: Path) -> None:
    receipt = _stage(repo)
    valid = _review_doc(repo, receipt, verdict="MERGE")
    first = _run(
        repo,
        "adjudicate",
        "--subject",
        "feature/fix-case-01",
        "--patch",
        receipt["patch"],
        "--docs",
        str(valid),
    )
    assert first.returncode == 0, first.stdout
    assert _run(repo, "merge-gate", "--subject", "feature/fix-case-01").returncode == 0

    missing = Path(receipt["worktree"]) / "docs" / "reviews" / "MISSING.md"
    second = _run(
        repo,
        "adjudicate",
        "--subject",
        "feature/fix-case-01",
        "--patch",
        receipt["patch"],
        "--docs",
        str(valid),
        str(missing),
    )
    assert second.returncode == 2
    assert json.loads(second.stdout)["reason"] == "excluded_expected_documents"
    gate = _run(repo, "merge-gate", "--subject", "feature/fix-case-01")
    assert gate.returncode == 2


def test_empty_expected_documents_cannot_leave_old_merge_authorization(repo: Path) -> None:
    receipt = _stage(repo)
    valid = _review_doc(repo, receipt, verdict="MERGE")
    first = _run(
        repo,
        "adjudicate",
        "--subject",
        "feature/fix-case-01",
        "--patch",
        receipt["patch"],
        "--docs",
        str(valid),
    )
    assert first.returncode == 0, first.stdout

    second = _run(
        repo,
        "adjudicate",
        "--subject",
        "feature/fix-case-01",
        "--patch",
        receipt["patch"],
        "--docs",
    )

    assert second.returncode == 2
    assert json.loads(second.stdout)["reason"] == "empty_expected_documents"
    assert _run(repo, "merge-gate", "--subject", "feature/fix-case-01").returncode == 2


def test_duplicate_expected_documents_cannot_leave_old_merge_authorization(repo: Path) -> None:
    receipt = _stage(repo)
    valid = _review_doc(repo, receipt, verdict="MERGE")
    first = _run(
        repo,
        "adjudicate",
        "--subject",
        "feature/fix-case-01",
        "--patch",
        receipt["patch"],
        "--docs",
        str(valid),
    )
    assert first.returncode == 0, first.stdout

    second = _run(
        repo,
        "adjudicate",
        "--subject",
        "feature/fix-case-01",
        "--patch",
        receipt["patch"],
        "--docs",
        str(valid),
        str(valid),
    )

    assert second.returncode == 2
    assert json.loads(second.stdout)["reason"] == "duplicate_expected_documents"
    assert _run(repo, "merge-gate", "--subject", "feature/fix-case-01").returncode == 2


def test_empty_expected_document_is_distinct_from_excluded_missing(repo: Path) -> None:
    receipt = _stage(repo)
    doc = Path(receipt["worktree"]) / "docs" / "reviews" / "EMPTY.md"
    doc.write_text("")
    _git(Path(receipt["worktree"]), "add", str(doc.relative_to(Path(receipt["worktree"]))))
    _git(Path(receipt["worktree"]), "commit", "-m", "review: record empty document")
    result = _run(
        repo,
        "adjudicate",
        "--subject",
        "feature/fix-case-01",
        "--patch",
        receipt["patch"],
        "--docs",
        str(doc),
    )
    assert result.returncode == 2
    payload = json.loads(result.stdout)
    assert payload["reason"] == "empty_expected_documents"
    assert payload["empty_count"] == 1
    assert payload["excluded_count"] == 0
    assert payload["docs"][0]["status"] == "empty"


def test_explicit_worktree_must_be_root_linked_dedicated_branch_before_writes(repo: Path) -> None:
    before_head = _git(repo, "rev-parse", "HEAD")
    result = _run(
        repo,
        "stage",
        "--subject",
        "feature/fix-case-01",
        "--slug",
        "case",
        "--worktree",
        str(repo),
    )
    assert result.returncode == 2
    payload = json.loads(result.stdout)
    assert payload["reason"] == "review_worktree_not_dedicated"
    assert _git(repo, "rev-parse", "HEAD") == before_head
    assert not (repo / "docs" / "reviews").exists()
    assert _git(repo, "status", "--porcelain") == ""


def test_explicit_root_linked_review_worktree_is_accepted(repo: Path) -> None:
    worktree = _prepare_explicit_review_worktree(repo)
    result = _run(
        repo,
        "stage",
        "--subject",
        "feature/fix-case-01",
        "--slug",
        "case",
        "--worktree",
        str(worktree),
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert json.loads(result.stdout)["rev_branch"] == "feature/rev1-fix-case-01"


def test_auto_created_review_tree_is_removed_on_empty_diff(repo: Path) -> None:
    result = _run(repo, "stage", "--subject", "main", "--slug", "empty")
    assert result.returncode == 2
    payload = json.loads(result.stdout)
    assert payload["reason"] == "empty_diff"
    assert payload["diff_outcome"] == "empty"
    worktree = repo.parent / f"{repo.name}-rev1-main"
    assert not worktree.exists()
    branch = subprocess.run(
        ["git", "-C", str(repo), "show-ref", "--verify", "--quiet", "refs/heads/feature/rev1-main"],
        check=False,
    )
    assert branch.returncode != 0


def test_auto_created_review_tree_is_removed_on_failed_diff(repo: Path, monkeypatch, capsys) -> None:
    review_pipeline = _pipeline_module()
    real_run = review_pipeline.subprocess.run

    def fake_run(argv, *args, **kwargs):
        if len(argv) > 3 and argv[0] == "git" and argv[3] == "diff":
            return subprocess.CompletedProcess(argv, 128, b"", b"diff failed")
        return real_run(argv, *args, **kwargs)

    monkeypatch.setattr(review_pipeline.subprocess, "run", fake_run)
    args = type(
        "StageArgs",
        (),
        {
            "root": str(repo),
            "subject": "feature/fix-case-01",
            "slug": "case",
            "integration": "main",
            "worktree": None,
            "rev": 1,
        },
    )()
    assert review_pipeline.cmd_stage(args) == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["reason"] == "diff_failed"
    worktree = repo.parent / f"{repo.name}-rev1-fix-case-01"
    assert not worktree.exists()
    branch = subprocess.run(
        ["git", "-C", str(repo), "show-ref", "--verify", "--quiet", "refs/heads/feature/rev1-fix-case-01"],
        check=False,
    )
    assert branch.returncode != 0


def test_failed_diff_has_a_distinct_typed_refusal(repo: Path, monkeypatch, capsys) -> None:
    review_pipeline = _pipeline_module()
    worktree = _prepare_explicit_review_worktree(repo)
    real_run = review_pipeline.subprocess.run

    def fake_run(argv, *args, **kwargs):
        if len(argv) > 3 and argv[0] == "git" and argv[3] == "diff":
            return subprocess.CompletedProcess(argv, 128, "", "diff failed")
        return real_run(argv, *args, **kwargs)

    monkeypatch.setattr(review_pipeline.subprocess, "run", fake_run)
    args = type(
        "StageArgs",
        (),
        {
            "root": str(repo),
            "subject": "feature/fix-case-01",
            "slug": "case",
            "integration": "main",
            "worktree": str(worktree),
            "rev": 1,
        },
    )()
    assert review_pipeline.cmd_stage(args) == 2
    payload = json.loads(capsys.readouterr().out)
    assert review_pipeline.DiffOutcome.FAILED.value == "failed"
    assert payload["reason"] == "diff_failed"
    assert payload["diff_outcome"] == "failed"


def test_por_is_structured_exact_and_samples_are_not_clamped(tmp_path: Path) -> None:
    review_pipeline = _pipeline_module()
    patch = tmp_path / "small.patch"
    patch.write_bytes(b"first\nsecond\n")
    keys = review_pipeline.derive_por_keys(patch)
    assert keys["sample_lines"]
    assert all(value.strip() for value in keys["sample_lines"].values())
    assert all(1 <= int(line) <= keys["line_count"] for line in keys["sample_lines"])
    assert len(set(keys["sample_lines"])) == len(keys["sample_lines"])
    legacy = f"PROOF: lines={keys['line_count']} md5={keys['md5']}\nfirst\nsecond\n"
    assert review_pipeline._doc_por_passes(legacy, keys)[0] is False
    tampered = dict(keys)
    tampered["file_count"] += 1
    structured = "POR: " + json.dumps(tampered, sort_keys=True)
    assert review_pipeline._doc_por_passes(structured, keys)[0] is False


def test_review_pipeline_artifact_policy_matches_both_guard_twins() -> None:
    review_pipeline = _pipeline_module()
    root_guard = _load_module(ROOT_GUARD, "root_lifecycle_guard_for_review_parity")
    payload_guard = _load_module(PAYLOAD_GUARD, "payload_lifecycle_guard_for_review_parity")
    assert review_pipeline.ADJUDICATION_DIR == root_guard._ADJUDICATION_DIR
    assert review_pipeline.ADJUDICATION_DIR == payload_guard._ADJUDICATION_DIR
    for subject in ("feature/fix-case-01", "feature/rev2/a-b", "feature/odd name"):
        expected = review_pipeline._branch_file_slug(subject)
        assert root_guard._branch_file_slug(subject) == expected
        assert payload_guard._branch_file_slug(subject) == expected


def test_same_tip_fake_patch_is_rejected_without_authorization(repo: Path, tmp_path: Path) -> None:
    receipt = _stage(repo)
    doc = _review_doc(repo, receipt, verdict="MERGE")
    tip = json.loads(Path(receipt["por_sidecar"]).read_text())["subject_tip"][:9]
    fake = tmp_path / f"_input-case-{tip}.patch"
    fake.write_bytes(b"fake content with a current-looking tip\n")
    result = _run(
        repo,
        "adjudicate",
        "--subject",
        "feature/fix-case-01",
        "--patch",
        str(fake),
        "--docs",
        str(doc),
    )
    assert result.returncode == 2
    assert json.loads(result.stdout)["reason"] == "stage_sidecar_missing"


def test_malformed_stage_sidecar_is_rejected_without_traceback(repo: Path) -> None:
    receipt = _stage(repo)
    sidecar = Path(receipt["por_sidecar"])
    sidecar.write_text("[]")

    result = _run(
        repo,
        "adjudicate",
        "--subject",
        "feature/fix-case-01",
        "--patch",
        receipt["patch"],
        "--docs",
    )

    assert result.returncode == 2
    payload = json.loads(result.stdout)
    assert payload["reason"] == "stage_sidecar_missing"
    assert str(sidecar) in payload["malformed_sidecars"]


def test_uncommitted_review_doc_is_not_evidence(repo: Path) -> None:
    receipt = _stage(repo)
    doc = _review_doc(repo, receipt, verdict="MERGE", commit=False)
    result = _run(
        repo,
        "adjudicate",
        "--subject",
        "feature/fix-case-01",
        "--patch",
        receipt["patch"],
        "--docs",
        str(doc),
    )
    assert result.returncode == 2
    payload = json.loads(result.stdout)
    assert payload["reason"] == "excluded_expected_documents"
    assert payload["docs"][0]["reason"] == "review_doc_not_committed"


def test_committed_patch_must_match_actual_subject_diff(repo: Path) -> None:
    receipt = _stage(repo)
    doc = _review_doc(repo, receipt, verdict="MERGE")
    patch = Path(receipt["patch"])
    patch.write_bytes(b"same tip, fake patch\n")
    worktree = Path(receipt["worktree"])
    _git(worktree, "add", str(patch.relative_to(worktree)))
    _git(worktree, "commit", "-m", "review: mutate staged input")
    result = _run(
        repo,
        "adjudicate",
        "--subject",
        "feature/fix-case-01",
        "--patch",
        str(patch),
        "--docs",
        str(doc),
    )
    assert result.returncode == 2
    assert json.loads(result.stdout)["reason"] == "stage_patch_not_committed"


def test_make_standing_gate_wires_review_pipeline_without_wildcards() -> None:
    makefile = (REPO_ROOT / "Makefile").read_text()
    fragment = (
        REPO_ROOT / "packages" / "workbay-system" / "workbay_system" / "payload" / "Makefile.d" / "review-pipeline.mk"
    ).read_text()
    assert re.search(r"^check-review-pipeline:\s+", makefile, re.MULTILINE)
    assert "test_review_pipeline.py" in makefile
    assert re.search(r"^check-system:.*check-review-pipeline", makefile, re.MULTILINE)
    assert re.search(r"^check-all:.*check-review-pipeline", makefile, re.MULTILINE)
    assert "check-review-pipeline" in makefile[makefile.index("check-all:") :]
    assert re.search(
        r"-m pytest -q .*test_review_pipeline\.py",
        makefile,
    )
    assert '"$(REVIEW_PIPELINE_PYTHON)" -m pytest' in makefile
    assert 'JUDGE_ROOT="$(REVIEW_PIPELINE_JUDGE_ROOT)"' in makefile
    assert "$(REVIEW_PIPELINE_JUDGE_ROOT)/scripts/assert_gate_interpreter.sh" in makefile
    assert 'PYTHONPATH="$(REVIEW_PIPELINE_PYTHONPATH)"' in makefile
    assert (
        '"$(REVIEW_PIPELINE_JUDGE_ROOT)/packages/workbay-system/workbay_system/payload/scripts/test_review_pipeline.py"'
        in makefile
    )
    assert re.search(
        r"^REVIEW_PIPELINE_PYTHON\s*:=\s*\$\(REVIEW_PIPELINE_JUDGE_ROOT\)/\.venv/bin/python$",
        fragment,
        re.MULTILINE,
    )


def test_default_judge_provenance_uses_the_repository_root(monkeypatch) -> None:
    review_pipeline = _pipeline_module()
    monkeypatch.delenv("JUDGE_ROOT", raising=False)
    monkeypatch.delenv("SYSTEM_PYTHON", raising=False)
    provenance = review_pipeline.judge_provenance()
    assert Path(provenance["judge_root"]) == REPO_ROOT


def test_guard_twins_are_byte_identical_and_share_artifact_policy() -> None:
    review_pipeline = _pipeline_module()
    root_guard = _load_module(ROOT_GUARD, "root_lifecycle_guard_for_review_parity_bytes")
    payload_guard = _load_module(PAYLOAD_GUARD, "payload_lifecycle_guard_for_review_parity_bytes")
    assert ROOT_GUARD.read_bytes() == PAYLOAD_GUARD.read_bytes()
    assert root_guard._ADJUDICATION_DIR == payload_guard._ADJUDICATION_DIR == review_pipeline.ADJUDICATION_DIR
    assert root_guard._ARTIFACT_BRANCH_SLUG_RE.pattern == payload_guard._ARTIFACT_BRANCH_SLUG_RE.pattern


def test_merge_gate_refuses_non_object_adjudication_artifact(repo: Path) -> None:
    artifact = repo / ".task-state" / "review-adjudication" / "feature-fix-case-01.json"
    artifact.parent.mkdir(parents=True, exist_ok=True)
    artifact.write_text("[]")

    result = _run(repo, "merge-gate", "--subject", "feature/fix-case-01")

    assert result.returncode == 2
    assert json.loads(result.stdout)["reason"] == "adjudication_artifact_malformed"


def test_stage_receipt_asserts_judge_provenance(repo: Path) -> None:
    receipt = _stage(repo)
    provenance = receipt["judge_provenance"]
    assert provenance["verified"] is True
    assert provenance["executable"]
    assert provenance["judge_root"]
    assert provenance["system_python"]


def test_judge_provenance_rejects_subject_local_alias(monkeypatch, tmp_path: Path) -> None:
    review_pipeline = _pipeline_module()
    subject_python = tmp_path / "subject" / ".venv" / "bin" / "python"
    subject_python.parent.mkdir(parents=True)
    subject_python.symlink_to(Path(sys.executable))
    monkeypatch.setenv("JUDGE_ROOT", str(REPO_ROOT))
    monkeypatch.setenv("SYSTEM_PYTHON", str(subject_python))

    provenance = review_pipeline.judge_provenance()

    assert provenance["verified"] is False
    assert provenance["configured_python"] == str(subject_python)


def test_merge_gate_refuses_judge_provenance_mismatch(repo: Path, monkeypatch, capsys) -> None:
    review_pipeline = _pipeline_module()
    monkeypatch.setattr(review_pipeline, "_judge_provenance_refusal", lambda: {"judge_provenance": {"verified": False}})
    args = type("MergeGateArgs", (), {"root": str(repo), "subject": "feature/fix-case-01"})()

    assert review_pipeline.cmd_merge_gate(args) == 2
    payload = json.loads(capsys.readouterr().out)

    assert payload["reason"] == "judge_provenance_mismatch"
