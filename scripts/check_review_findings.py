#!/usr/bin/env python3
"""Gate: a review lane produced a real findings artifact and left the tree alone.

Canon: AGT-04 (evidence verbatim — a finding names mechanism + location + the
decisive output), CLM-04 (adjectives are not evidence — the full claim is
mechanism + location + failure behaviour), TEST-06 (a claim never checked
against a predicted failure asserts nothing), TEST-15 (an assertion that
cannot go red for the failure it names is not coverage), AGT-21 (distinct
failure reasons deserve distinct outcomes), OBS-01 (name the thing that
failed).

Usage: check_review_findings.py <findings.md> <min_findings> <diff_path> \\
         <diff_sha256> --base-rev <sha> [--allowed PATH]...

Contract:
  1. The findings artifact exists, is not a symlink, and is non-empty.
  2. It carries a `verdict:` line whose value is one of the allowed verdicts.
     Read with `utf-8-sig` so a leading UTF-8 BOM cannot hide line 1.
  3. It carries at least <min_findings> `### FINDING - <id>` blocks, ids unique.
  4. Every block carries severity / file / mechanism / failure_scenario /
     evidence, each non-empty and not a placeholder. These shape checks run
     unconditionally, even when the findings path is outside a git repo (only
     the cite-existence sub-check needs a resolved repo).
  5. `severity` is high|medium|low.
  6. The diff artifact the lane was given still hashes to <diff_sha256> — the
     reviewer must not edit the evidence it is reviewing (invariant outside the
     lane's writable scope).
  7. Every `file:` value inside a `### FINDING` block resolves to an existing
     *file* (not a directory) that is repo-relative, non-empty, and not `.` or
     absolute. Failure names the finding id and the offending path.
  8. The lane did not modify files outside the allowed set. The caller must
     pass `--base-rev <sha>` (do not infer a branch; sandboxes are history-
     stripped). Optional `--allowed PATH` entries are added to the allowed
     set; the findings artifact is always allowed. Both the findings artifact
     and each `--allowed PATH` are resolved (relative entries against CWD) to
     a repo-root-relative path before comparison, since git output is always
     repo-root-relative. Committed changes (`git diff --name-only --no-renames
     <base_rev> HEAD` — renames disabled so a renamed-away production file is
     never hidden behind its destination) are unioned with uncommitted
     `git status --porcelain --untracked-files=all` paths, plus every
     gitignored file named individually via `git ls-files --others --ignored
     --exclude-standard` (never a collapsed ignored-directory line, so a real
     file dropped inside an ignored directory cannot hide behind the
     directory's name). Ignored writes outside an explicit build-noise
     allowlist count as scope violations; the allowlist checks leaf content,
     not just directory name — `__pycache__/` only allows CPython cache
     filenames (`<name>.<tag>.pyc`/`<name>.<tag>.opt-N.pyc`) directly inside
     it whose first four bytes match `importlib.util.MAGIC_NUMBER`,
     `.pytest_cache/` only allows its known files (`CACHEDIR.TAG`,
     `README.md`, `.gitignore`) plus a small fixed set of known pytest
     cache-tree leaves (`v/cache/lastfailed`, `v/cache/nodeids`,
     `v/cache/stepwise`) by exact relative path, and there is no bare
     top-level `*.pyc` fallback.
     Index skip bits (assume-unchanged / skip-worktree, via `git ls-files -v`)
     fail closed by name, since they hide worktree edits from `git status`.
     A non-empty remainder fails and names every offending path together with
     its real state (untracked file / tracked file / staged deletion /
     renamed — never a blanket "tracked file"). Missing `--base-rev`,
     unavailable git, an unresolvable base revision, or a findings/allowed
     path outside the repository fail closed — the scope check is never
     skipped.

Exit 0 only when all hold. Any violation exits 1 and names the block.
Every failing rule is reported (scope, cite, and artifact); a later shape
failure cannot hide a writable-scope violation. Scope violations print
`REVIEW-SCOPE FAIL:`; missing cited paths print `REVIEW-CITE FAIL:`; the
original six rules print `REVIEW-ARTIFACT FAIL:`. A findings path outside any
git repository prints `REVIEW-SCOPE FAIL: <path> is not inside a git
repository`, distinct from the git-binary-missing case.
"""

from __future__ import annotations

import hashlib
import importlib.util
import pathlib
import re
import subprocess
import sys

FIELDS = ("severity", "file", "mechanism", "failure_scenario", "evidence")
VERDICTS = {"pass", "pass_with_findings", "conditional_pass", "fail"}
SEVERITIES = {"high", "medium", "low"}
PLACEHOLDERS = {
    "", "tbd", "todo", "n/a", "na", "none", "pending", "unknown",
    "<fill>", "...", "xxx", "-",
}

# House review format: path:LINE, path:LINE-LINE, or path:LINE:COL.
_CITE_LOCATION_SUFFIX = re.compile(r":\d+(?:-\d+|:\d+)?$")

_FAILURES: list[str] = []


def fail(msg: str) -> None:
    _FAILURES.append(f"REVIEW-ARTIFACT FAIL: {msg}")


def fail_scope(msg: str) -> None:
    _FAILURES.append(f"REVIEW-SCOPE FAIL: {msg}")


def fail_cite(msg: str) -> None:
    _FAILURES.append(f"REVIEW-CITE FAIL: {msg}")


def _emit_and_exit() -> None:
    for line in _FAILURES:
        print(line)
    sys.exit(1)


def _parse_args(argv: list[str]) -> tuple[str, int, str, str, str, list[str]]:
    if len(argv) < 5:
        fail(
            "usage: check_review_findings.py <findings.md> <min> <diff_path> "
            "<diff_sha256> --base-rev <sha> [--allowed PATH]..."
        )
        _emit_and_exit()
    art, raw_min, diff_path, want_sha = argv[1], argv[2], argv[3], argv[4]
    try:
        min_findings = int(raw_min)
    except ValueError:
        fail(f"min_findings {raw_min!r} is not an integer")
        _emit_and_exit()

    base_rev: str | None = None
    allowed: list[str] = []
    rest = argv[5:]
    i = 0
    while i < len(rest):
        tok = rest[i]
        if tok == "--base-rev":
            if i + 1 >= len(rest) or rest[i + 1].startswith("--"):
                fail_scope(
                    "base revision argument is required "
                    "(fail-closed; scope check cannot be skipped)"
                )
                _emit_and_exit()
            base_rev = rest[i + 1]
            i += 2
            continue
        if tok == "--allowed":
            if i + 1 >= len(rest) or rest[i + 1].startswith("--"):
                fail_scope("allowed path argument is required (fail-closed)")
                _emit_and_exit()
            allowed.append(rest[i + 1])
            i += 2
            continue
        fail_scope(
            f"unexpected argument {tok!r} "
            "(fail-closed; pass --base-rev <sha> [--allowed PATH]...)"
        )
        _emit_and_exit()
    if not base_rev:
        fail_scope(
            "base revision argument is required "
            "(fail-closed; scope check cannot be skipped)"
        )
        _emit_and_exit()
    return art, min_findings, diff_path, want_sha, base_rev, allowed


def _run_git(repo: pathlib.Path, *args: str) -> subprocess.CompletedProcess[str] | None:
    try:
        return subprocess.run(
            ["git", "-C", str(repo), *args],
            capture_output=True,
            text=True,
        )
    except FileNotFoundError:
        fail_scope("git is unavailable")
        return None


def _repo_root(start: pathlib.Path) -> pathlib.Path | None:
    proc = _run_git(start, "rev-parse", "--show-toplevel")
    if proc is None:
        return None
    if proc.returncode != 0:
        # Distinct from "git is unavailable" (recorded by _run_git when the
        # binary itself is missing): git ran fine here, it just reports that
        # `start` is not inside a repository.
        fail_scope(f"{start} is not inside a git repository")
        return None
    return pathlib.Path(proc.stdout.strip())


def _normalize_allowed(raw: str, repo: pathlib.Path) -> str | None:
    p = pathlib.Path(raw)
    if ".." in p.parts:
        fail_scope(
            f"allowed path {raw!r} contains '..' and is rejected (fail-closed)"
        )
        return None
    abs_p = p.resolve() if p.is_absolute() else (pathlib.Path.cwd() / p).resolve()
    try:
        return abs_p.relative_to(repo.resolve()).as_posix()
    except ValueError:
        fail_scope(f"allowed path {raw!r} is outside the repository")
        return None


def _skip_bit_paths(repo: pathlib.Path) -> list[str] | None:
    """Paths with an assume-unchanged (lowercase) or skip-worktree (S) bit.

    `git status --porcelain` silently omits these even when the worktree
    content differs from the index; only `git ls-files -v` surfaces them.
    """
    proc = _run_git(repo, "ls-files", "-v")
    if proc is None:
        return None
    if proc.returncode != 0:
        fail_scope("git ls-files failed")
        return None
    flagged: list[str] = []
    for line in proc.stdout.splitlines():
        if not line:
            continue
        code, _, name = line.partition(" ")
        if not code or not name:
            continue
        if code.islower() or code == "S":
            flagged.append(name.strip())
    return flagged


# CPython bytecode-cache filename: <name>.<tag>.pyc or <name>.<tag>.opt-N.pyc
# (e.g. prod.cpython-311.pyc, prod.cpython-311.opt-1.pyc).
_PYCACHE_LEAF_RE = re.compile(r"^[^/]+\.[A-Za-z0-9_]+-[0-9]+(?:\.opt-[0-9]+)?\.pyc$")

# Relative-to-`.pytest_cache/` paths pytest itself is known to write. No
# wildcard subtree acceptance: every path pytest can legitimately create
# under `.pytest_cache/` must be named here exactly.
_PYTEST_CACHE_ALLOWED_PATHS = {
    "CACHEDIR.TAG",
    "README.md",
    ".gitignore",
    "v/cache/lastfailed",
    "v/cache/nodeids",
    "v/cache/stepwise",
}


def _is_expected_ignored_noise(name: str, repo: pathlib.Path) -> bool:
    """Explicit allowlist for build noise the scope check should not flag.

    Directory-name membership alone is not enough — a compliant reviewer
    could otherwise smuggle real source into `__pycache__/` or
    `.pytest_cache/` and have it wave through unexamined. Inside each
    allowlisted directory the leaf must itself look like genuine noise:
    `__pycache__/` only allows a CPython cache filename directly inside it
    (`<name>.<tag>.pyc` or `<name>.<tag>.opt-N.pyc`) whose first four bytes
    match the running interpreter's `importlib.util.MAGIC_NUMBER` — an
    unreadable, short, or content-mismatched file is never treated as noise,
    it fails scope; `.pytest_cache/` only allows its known files
    (`CACHEDIR.TAG`, `README.md`, `.gitignore`) plus the small fixed set of
    known pytest cache-tree leaves (`v/cache/lastfailed`, `v/cache/nodeids`,
    `v/cache/stepwise`) by exact relative path — no wildcard `v/` subtree
    acceptance. There is no bare top-level `*.pyc` fallback: every
    `.pyc`-suffixed file must be a genuine `__pycache__/` cache leaf or it
    fails scope regardless of location. Everything else that is
    gitignored-and-present is a scope failure (fail closed): the allowlist
    is the only escape hatch, never a broad pattern.
    """
    parts = pathlib.PurePosixPath(name).parts
    if "__pycache__" in parts:
        idx = parts.index("__pycache__")
        remainder = parts[idx + 1 :]
        if len(remainder) != 1 or not _PYCACHE_LEAF_RE.match(remainder[0]):
            return False
        try:
            header = (repo / name).open("rb").read(4)
        except OSError:
            return False
        return header == importlib.util.MAGIC_NUMBER
    if ".pytest_cache" in parts:
        idx = parts.index(".pytest_cache")
        remainder = parts[idx + 1 :]
        if not remainder:
            return False
        return "/".join(remainder) in _PYTEST_CACHE_ALLOWED_PATHS
    return False


_STATE_TRACKED = "tracked file"
_STATE_UNTRACKED = "untracked file"
_STATE_STAGED_DELETION = "staged deletion"
_STATE_RENAMED = "renamed"


def _changed_paths(repo: pathlib.Path, base_rev: str) -> dict[str, str] | None:
    """Paths changed since base_rev, mapped to a named state for messaging.

    Named per AGT-21: "lane modified tracked file" must not be printed for
    an untracked/renamed/staged-deletion path — each state gets its own word.
    """
    verify = _run_git(repo, "rev-parse", "--verify", f"{base_rev}^{{commit}}")
    if verify is None:
        return None
    if verify.returncode != 0:
        fail_scope(f"base revision {base_rev!r} does not resolve")
        return None

    committed = _run_git(repo, "diff", "--name-only", "--no-renames", base_rev, "HEAD")
    if committed is None:
        return None
    if committed.returncode != 0:
        fail_scope(f"git diff against base revision {base_rev!r} failed")
        return None

    # Deliberately no --ignored=matching here: `git status` collapses an
    # entirely-ignored directory (e.g. `src/__pycache__/`) into a single
    # directory line instead of naming the files inside it, which would
    # hide real source dropped inside an ignored dir behind the directory
    # name. Ignored paths are enumerated file-by-file below instead.
    status = _run_git(repo, "status", "--porcelain", "--untracked-files=all")
    if status is None:
        return None
    if status.returncode != 0:
        fail_scope("git status failed")
        return None

    ignored = _run_git(repo, "ls-files", "--others", "--ignored", "--exclude-standard")
    if ignored is None:
        return None
    if ignored.returncode != 0:
        fail_scope("git ls-files --ignored failed")
        return None

    paths: dict[str, str] = {}
    for line in committed.stdout.splitlines():
        name = line.strip().strip('"')
        if name:
            paths[name] = _STATE_TRACKED

    for line in status.stdout.splitlines():
        if len(line) < 4:
            continue
        code = line[:2]
        rest = line[3:]
        if " -> " in rest:
            left, right = rest.split(" -> ", 1)
            for part in (left, right):
                name = part.strip().strip('"')
                if name:
                    paths[name] = _STATE_RENAMED
            continue
        name = rest.strip().strip('"')
        if not name:
            continue
        if code == "??":
            paths[name] = _STATE_UNTRACKED
        elif "D" in code:
            paths[name] = _STATE_STAGED_DELETION
        else:
            paths[name] = _STATE_TRACKED

    for line in ignored.stdout.splitlines():
        name = line.strip().strip('"')
        if not name or _is_expected_ignored_noise(name, repo):
            continue
        paths[name] = _STATE_UNTRACKED
    return paths


def _strip_cite_location(cited: str) -> str:
    """Strip a trailing :LINE, :LINE-LINE, or :LINE:COL location suffix."""
    return _CITE_LOCATION_SUFFIX.sub("", cited)


def _cited_exists(cited: str, repo: pathlib.Path) -> bool:
    raw = _strip_cite_location(cited).strip()
    if not raw or raw == ".":
        return False
    p = pathlib.Path(raw)
    if p.is_absolute():
        # Cites must be repo-relative; an absolute path is rejected outright
        # even when it happens to resolve inside the repo.
        return False
    if ".." in p.parts:
        return False
    try:
        repo_res = repo.resolve()
        resolved = (repo_res / p).resolve()
        resolved.relative_to(repo_res)
    except (ValueError, OSError):
        return False
    return resolved.is_file()


def _assert_writable_scope(
    *,
    findings: pathlib.Path,
    base_rev: str,
    extra_allowed: list[str],
    repo: pathlib.Path,
) -> None:
    allowed: set[str] = set()
    norm = _normalize_allowed(str(findings), repo)
    if norm is None:
        return
    allowed.add(norm)
    for raw in extra_allowed:
        extra = _normalize_allowed(raw, repo)
        if extra is None:
            return
        allowed.add(extra)

    skip_bits = _skip_bit_paths(repo)
    if skip_bits is None:
        return
    for name in skip_bits:
        fail_scope(f"index skip bits set on {name}")

    changed = _changed_paths(repo, base_rev)
    if changed is None:
        return
    offending = sorted(p for p in changed if p not in allowed)
    if not offending:
        return
    if len(offending) == 1:
        p = offending[0]
        fail_scope(f"lane modified {changed[p]} {p} outside allowed set")
        return
    fail_scope(
        "lane modified files outside allowed set: "
        + ", ".join(f"{changed[p]} {p}" for p in offending)
    )


def _read_diff_bytes(diff: pathlib.Path, diff_path: str) -> bytes | None:
    if not diff.exists():
        fail(f"diff artifact {diff_path} is missing — it must stay in the tree")
        return None
    if not diff.is_file():
        fail(f"diff artifact {diff_path} is not a readable file")
        return None
    try:
        return diff.read_bytes()
    except OSError:
        fail(f"diff artifact {diff_path} is not a readable file")
        return None


def _read_findings_text(path: pathlib.Path) -> str | None:
    """Read the findings artifact. Fail-closed on I/O or decode errors.

    Distinct from missing (`no findings artifact`) and empty (`is empty`).
    The reason token is `findings_unreadable:<exception-class>` so callers
    can bind to an invariant line, not a substring of the payload.
    """
    try:
        return path.read_text(encoding="utf-8-sig")
    except (OSError, UnicodeDecodeError) as exc:
        fail(f"findings_unreadable:{type(exc).__name__}")
        return None


def main() -> None:
    _FAILURES.clear()
    art, min_findings, diff_path, want_sha, base_rev, extra_allowed = _parse_args(
        sys.argv
    )

    # Collect every independent rule failure. A later artifact-shape error
    # must not hide a writable-scope violation (and vice versa).
    diff = pathlib.Path(diff_path)
    data = _read_diff_bytes(diff, diff_path)
    if data is not None:
        got_sha = hashlib.sha256(data).hexdigest()
        if got_sha != want_sha:
            fail(
                f"diff artifact {diff_path} was modified (sha256 {got_sha[:16]} != "
                f"expected {want_sha[:16]}). Review the evidence; do not edit it."
            )

    path = pathlib.Path(art)
    text: str | None
    if path.is_symlink():
        fail("findings artifact is a symlink")
        text = None
    elif not path.exists():
        fail(f"no findings artifact at {art}")
        text = None
    else:
        text = _read_findings_text(path)
        if text is not None and not text.strip():
            fail(f"{art} is empty")

    start = path.parent if path.parent.as_posix() not in ("", ".") else pathlib.Path.cwd()
    if not start.exists():
        start = pathlib.Path.cwd()
    repo = _repo_root(start)
    if repo is not None:
        _assert_writable_scope(
            findings=path,
            base_rev=base_rev,
            extra_allowed=extra_allowed,
            repo=repo,
        )

    vm = None
    ids: list[str] = []
    if text is not None and text.strip():
        vm = re.search(r"(?m)^verdict:[ \t]*(\S+)[ \t]*$", text)
        if not vm:
            fail("no `verdict: <pass|pass_with_findings|conditional_pass|fail>` line")
        elif vm.group(1) not in VERDICTS:
            fail(f"verdict {vm.group(1)!r} is not one of {sorted(VERDICTS)}")

        ids = re.findall(r"(?m)^### FINDING - (\S+)[ \t]*$", text)
        if len(ids) < min_findings:
            fail(f"{len(ids)} finding blocks, floor is {min_findings}")
        if len(set(ids)) != len(ids):
            dupes = sorted({i for i in ids if ids.count(i) > 1})
            fail(f"duplicate finding ids: {dupes}")

        blocks = dict(
            re.findall(
                r"(?m)^### FINDING - (\S+)[ \t]*$\n(.*?)(?=^### FINDING - |\Z)",
                text,
                re.S,
            )
        )
        # Shape checks (missing/placeholder fields, severity) always run —
        # only the cite-existence sub-check needs a resolved repo, and it is
        # skipped (not the whole loop) when there is none.
        for fid in ids:
            body = blocks.get(fid, "")
            got = dict(
                re.findall(r"(?m)^(%s):[ \t]*(.*)$" % "|".join(FIELDS), body)
            )
            shape_ok = True
            for key in FIELDS:
                if key not in got:
                    fail(f"finding {fid}: missing field {key!r}")
                    shape_ok = False
                    continue
                if got[key].strip().lower() in PLACEHOLDERS:
                    fail(
                        f"finding {fid}: field {key!r} is a placeholder "
                        f"({got[key]!r})"
                    )
                    shape_ok = False
            if not shape_ok:
                continue
            sev = got["severity"].strip().lower()
            if sev not in SEVERITIES:
                fail(f"finding {fid}: severity {sev!r} is not high|medium|low")
                continue
            cited = got["file"].strip()
            if repo is not None and not _cited_exists(cited, repo):
                fail_cite(f"finding {fid} cites missing path {cited}")

    if _FAILURES:
        _emit_and_exit()

    # vm is a match here: missing/empty artifact and missing/invalid verdict
    # already recorded a failure and exited (CL0816-L-02: the previous
    # `if vm is None` guard after this emit was unreachable).
    print(
        f"REVIEW-ARTIFACT OK: verdict={vm.group(1)} findings={len(ids)} "
        f"diff_sha256_verified={want_sha[:16]}"
    )


if __name__ == "__main__":
    main()
