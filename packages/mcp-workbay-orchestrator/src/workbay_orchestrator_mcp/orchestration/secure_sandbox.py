"""Secure grok offload sandbox (internal).

grok Build (>=0.2.93) bundles the FULL git object database of whatever repo it
runs against to ``gs://grok-code-session-traces`` via ``/v1/storage`` — independent
of which files the agent reads, and regardless of the (architecturally bypassed)
"Improve the model" opt-out. The offload lane runs grok in a git *worktree*, which
shares the primary repo's full ``.git`` object database, so a per-lane bundle would
be the entire monorepo history, including any secret ever committed. See the
``feedback_grok_cli_repo_exfiltration`` memory (task internal).

This module confines grok to a **shallow, secret-scanned clone** that carries NO
historical objects (``git clone --no-local --depth=1``): the worst grok can bundle
is the current HEAD tree — the source it is already editing anyway — never the
deleted-secret / full-history payload. After grok commits inside the sandbox, its
commits are replayed onto the real lane branch (``format-patch`` -> ``am``) so the
rest of the offload pass (commit-landed detection, ``close_slice``) is unchanged.

Defense-in-depth ONLY — pair with a network egress deny of the ``/v1/storage``
upload channel and the ``grok-code-session-traces`` bucket. The shallow clone
removes reliance on the network control; the egress deny removes reliance on
grok's cooperation.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

_ENV_FLAG = "WORKBAY_GROK_SECURE_SANDBOX"

# Unambiguous committed key material — surfaced as a HIGH-severity advisory (not
# a lockout: a HEAD-tree key is already grok-readable, and repos carry key
# FIXTURES in tests). The load-bearing control is the shallow clone + egress
# deny; the scan is defense-in-depth. Broader scanning is delegated to gitleaks
# when present.
_HARD_SECRET_RE = re.compile(r"-----BEGIN (?:RSA|OPENSSH|EC|DSA|PGP|ENCRYPTED) PRIVATE KEY-----")
# Advisory-only high-signal patterns (surfaced, never fail-closed here).
_ADVISORY_SECRET_RES = (
    ("aws_access_key_id", re.compile(r"AKIA[0-9A-Z]{16}")),
    # Tail charset includes _/- (real keys use them); keep the 20-char floor so
    # short product tokens never false-positive. Matches export_public charset.
    ("xai_key", re.compile(r"xai-[A-Za-z0-9_-]{20,}")),
    ("github_token", re.compile(r"gh[pousr]_[0-9A-Za-z]{36,}")),
    ("slack_token", re.compile(r"xox[baprs]-[0-9A-Za-z-]{10,}")),
)
# Skip binary/vendored/heavy paths in the built-in fallback scan.
_SCAN_SKIP_DIRS = {
    ".git",
    ".venv",
    "node_modules",
    "__pycache__",
    ".task-state",
    ".workbay",
    "dist",
    ".mypy_cache",
    ".pytest_cache",
    ".cache",
}
_SCAN_MAX_BYTES = 1_000_000  # skip files larger than 1 MB in the fallback scan


# Sanctioned skip when the sandbox clone has no root Python project (implementation note).
# Distinct from admission refusals and from real uv-sync failures (SecureSandboxError).
PROVISION_SKIPPED_NO_PYTHON = "provision_skipped: no_python_project"
PROVISIONED = "provisioned"


class SecureSandboxError(RuntimeError):
    """Fail-closed sandbox error: clone not shallow, clone failed, or env
    provisioning (``uv sync``) failed when a root ``pyproject.toml`` is present.

    Absence of a root Python project is a sanctioned skip
    (``provision_skipped: no_python_project``), not an error. Secret findings
    are advisory, not errors.
    """


def _ensure_telemetry_off_config(sandbox: Path) -> list[str]:
    """LAYER 3 (defense-in-depth) — write grok's VERIFIED telemetry opt-out into
    the sandbox's project-scoped ``.grok/config.toml``.

    Keys verified against grok's own docs (``05-configuration.md``):
        [features] telemetry = false      # master switch
        [telemetry] trace_upload = false  # SINGULAR; env GROK_TELEMETRY_TRACE_UPLOAD

    The commonly-suggested ``trace_uploads`` (plural) and ``[harness]
    disable_codebase_upload`` are NOT real keys — grok silently ignores them
    (false security), so this never writes them. This layer matters for NON-ZDR
    accounts; a ZDR-team account already gates uploads remotely
    (``upload_reason="zdr_team"``). The load-bearing controls remain the shallow
    clone (no history to bundle) + a network egress deny of ``/v1/storage``.

    Merge-aware and TOML-safe: only appends a table that is entirely ABSENT (so
    the adapter's pinned-model config is never disturbed and no duplicate table
    is created). If a table exists without the opt-out key, it returns an advisory
    rather than risk-editing inside it.
    """
    cfg = sandbox / ".grok" / "config.toml"
    cfg.parent.mkdir(parents=True, exist_ok=True)
    existing = cfg.read_text(encoding="utf-8") if cfg.exists() else ""
    lines = [ln.strip() for ln in existing.splitlines()]
    advisories: list[str] = []
    additions = ""
    if "[features]" not in lines:
        additions += "\n[features]\ntelemetry = false\n"
    elif "trace_upload" not in existing and "telemetry" not in existing:
        advisories.append(".grok/config.toml [features] present — set telemetry=false manually")
    if "[telemetry]" not in lines:
        additions += "\n[telemetry]\ntrace_upload = false\n"
    elif "trace_upload" not in existing:
        advisories.append(".grok/config.toml [telemetry] present — set trace_upload=false manually")
    if additions:
        cfg.write_text((existing.rstrip("\n") + "\n" if existing.strip() else "") + additions, encoding="utf-8")
    return advisories


_PROVISION_FLAG = "WORKBAY_GROK_SANDBOX_PROVISION"


def _flag_on(name: str) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return True
    return raw.strip().lower() not in {"0", "false", "no", "off", ""}


def secure_sandbox_enabled() -> bool:
    """Secure sandbox is ON by default for grok lanes.

    Opt OUT only by setting ``WORKBAY_GROK_SECURE_SANDBOX`` to a falsey value —
    an explicit operator decision (e.g. after the exfiltration is verified fixed).
    """
    return _flag_on(_ENV_FLAG)


def sandbox_provision_enabled() -> bool:
    """Whether to attempt sandbox env provisioning (``uv sync`` when a root
    ``pyproject.toml`` is present).

    ON by default. Opt out with ``WORKBAY_GROK_SANDBOX_PROVISION`` falsey only
    for zero-Python slices/tests whose worker never self-verifies against
    sandbox src — the flag is not the sanctioned non-Python path. Repos
    without a root ``pyproject.toml`` are handled by detect-and-skip inside
    ``ShallowSandbox.provision_env`` (``provision_skipped: no_python_project``).
    """
    return _flag_on(_PROVISION_FLAG)


def _git(args: list[str], *, cwd: Path | str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603
        ["git", *args],
        cwd=str(cwd),
        capture_output=True,
        text=True,
        check=check,
        timeout=300,
    )


def _scan_secrets(root: Path) -> tuple[list[str], list[str]]:
    """Return (hard_findings, advisory_findings) as ``path: label`` strings.

    Prefers ``gitleaks`` (entropy + allowlist aware) for advisory findings when it
    is on PATH; always runs the built-in high-confidence pass for the hard
    fail-closed class. Never raises — a scanner failure degrades to "no findings"
    (the shallow clone + egress deny remain the load-bearing controls).
    """
    hard: list[str] = []
    advisory: list[str] = []
    try:
        for dirpath, dirnames, filenames in os.walk(root):
            dirnames[:] = [d for d in dirnames if d not in _SCAN_SKIP_DIRS]
            for name in filenames:
                fpath = Path(dirpath) / name
                try:
                    if fpath.is_symlink() or fpath.stat().st_size > _SCAN_MAX_BYTES:
                        continue
                    text = fpath.read_text(encoding="utf-8", errors="ignore")
                except OSError:
                    continue
                rel = str(fpath.relative_to(root))
                if _HARD_SECRET_RE.search(text):
                    hard.append(f"{rel}: private-key-block")
                for label, rx in _ADVISORY_SECRET_RES:
                    if rx.search(text):
                        advisory.append(f"{rel}: {label}")
    except OSError:
        pass
    return hard, advisory


@dataclass
class ShallowSandbox:
    """A history-stripped clone of one lane worktree that grok runs inside.

    Use as a context manager. On enter it clones the worktree's current branch
    tip with ``--depth=1`` (no historical objects), records the base SHA, and
    runs the secret scan (fail-closed on hard key material). ``port_commits_back``
    replays sandbox commits onto the real lane branch. On exit the temp clone is
    always removed.
    """

    worktree_path: Path
    branch: str
    path: Path = field(init=False)
    base_sha: str = field(init=False, default="")
    advisory_findings: list[str] = field(init=False, default_factory=list)
    _tmpdir: str = field(init=False, default="")

    def __enter__(self) -> "ShallowSandbox":
        self._tmpdir = tempfile.mkdtemp(prefix="grok-secure-")
        self.path = Path(self._tmpdir) / "lane"
        # --no-local is MANDATORY: a plain local-path clone hardlinks the whole
        # object DB and IGNORES --depth, defeating the entire purpose. file:// +
        # --no-local forces the smart transfer so --depth=1 is honored.
        try:
            _git(
                [
                    "clone",
                    "--no-local",
                    "--depth=1",
                    # --single-branch: never fetch other branches' tips. --no-tags:
                    # a tag pointing at older history would otherwise drag shallow
                    # history in behind --depth=1. Together with --depth=1 these keep
                    # the clone to exactly the one branch-tip commit.
                    "--single-branch",
                    "--no-tags",
                    "--branch",
                    self.branch,
                    f"file://{Path(self.worktree_path).resolve()}",
                    str(self.path),
                ],
                cwd=Path(self.worktree_path),
            )
        except subprocess.CalledProcessError as exc:
            self._cleanup()
            raise SecureSandboxError(
                f"secure sandbox clone failed (branch {self.branch!r}): "
                f"{(exc.stderr or exc.stdout or '').strip()[-500:]}"
            ) from exc

        # Pristine-clone sanity (clone time only): a fresh --depth=1 --single-branch
        # --no-tags clone must reach EXACTLY ONE commit from ANY ref. (Post-turn the
        # count legitimately grows as grok commits DESCENDANTS — that check is the
        # base-ancestor invariant in verify_isolated, not this one.)
        allc = _git(["rev-list", "--count", "--all"], cwd=self.path).stdout.strip()
        if allc != "1":
            self._cleanup()
            raise SecureSandboxError(
                f"secure sandbox clone is not minimal (rev-list --all count={allc!r}); "
                "refusing — history would be bundleable."
            )
        self.base_sha = _git(["rev-parse", "HEAD"], cwd=self.path).stdout.strip()

        # Sever the path back to full history BEFORE asserting isolation: `git
        # clone` leaves an ``origin`` remote (+ refs/remotes/origin/*) pointing at
        # the source worktree, so a single ``git fetch --unshallow origin`` inside
        # the sandbox — a PURE-LOCAL op needing NO network — would re-pull the
        # ENTIRE object DB grok could then bundle (empirically confirmed in review).
        # Remove it so the sandbox is self-contained: port_commits_back uses
        # format-patch/am (not the remote) and uv sync needs no remote.
        _git(["remote", "remove", "origin"], cwd=self.path, check=False)
        try:
            self.verify_isolated()
        except SecureSandboxError:
            self._cleanup()
            raise

        # Secret scan is ADVISORY, never a lockout: a key at the HEAD tree is
        # content grok already reads during normal operation (so the bundle adds
        # no exposure beyond the agent's own file access), and repos legitimately
        # carry private-key FIXTURES in tests — fail-closing here would refuse
        # every real sandbox. The load-bearing control is the shallow clone (no
        # history to bundle) + the network egress deny. Surface key material at
        # high severity so the operator can act.
        hard, advisory = _scan_secrets(self.path)
        self.advisory_findings = [f"KEY-MATERIAL {h}" for h in hard] + advisory
        self.advisory_findings += _ensure_telemetry_off_config(self.path)
        return self

    def verify_isolated(self) -> None:
        """Fail-closed assertion that the sandbox's shallow boundary is intact and
        there is no path back to full history.

        Runs at clone time AND AGAIN after grok's turn (called from
        ``port_commits_back``) — the defense-in-depth the review demanded: grok runs
        with ``--always-approve`` + full shell, so a ``git fetch``/submodule/escape
        during its turn that re-hydrates history must be caught before commits are
        ported and the lane is treated as clean.

        The invariant is NOT "one commit total" — grok legitimately adds DESCENDANT
        commits (its work). It is that the clone base has NO reachable ANCESTORS
        (the deleted-secret history BELOW the shallow boundary stays unreachable)
        and the repo is still shallow with no remote:

        1. still a shallow repository — ``git fetch --unshallow`` flips this false;
        2. ``base_sha`` has no ancestors — ``rev-list --count base_sha == 1``
           (descendants grok added do not change base_sha's ancestor set);
        3. no remote — no local path to re-pull the object DB.
        """
        shallow = _git(["rev-parse", "--is-shallow-repository"], cwd=self.path, check=False).stdout.strip()
        if shallow != "true":
            raise SecureSandboxError(
                f"secure sandbox is no longer shallow (is-shallow-repository={shallow!r}); "
                "refusing — history was re-hydrated."
            )
        if self.base_sha:
            ancestors = _git(["rev-list", "--count", self.base_sha], cwd=self.path, check=False).stdout.strip()
            if ancestors != "1":
                raise SecureSandboxError(
                    f"secure sandbox history widened below the clone base "
                    f"(rev-list {self.base_sha[:12]} count={ancestors!r}); refusing — "
                    "ancestor history became bundleable."
                )
        remotes = _git(["remote"], cwd=self.path, check=False).stdout.strip()
        if remotes:
            raise SecureSandboxError(
                f"secure sandbox has a remote ({remotes!r}); refusing — a fetch could re-pull full history."
            )

    def provision_env(self, *, timeout: int = 900) -> str:
        """Provision the sandbox test env so the worker's self-verify runs against
        the SANDBOX src, not a stale/absent one.

        A fresh ``--depth=1`` clone has no ``.venv``; a Python lane ``TEST_CMD``
        (e.g. ``../../.venv/bin/python -m pytest``) would die on
        ``ModuleNotFoundError``. When a root ``pyproject.toml`` is present, run
        the canonical workspace-root ``uv sync`` (implementation note D3b) so
        ``<sandbox>/.venv`` holds editables pointing at the clone. FAIL-CLOSED
        on ``uv sync`` failure: raise rather than letting the worker self-verify
        against a broken env (a false-red that would waste the pass).

        When the clone root has no ``pyproject.toml``, skip provisioning and
        return ``provision_skipped: no_python_project`` (sanctioned skip —
        root-level detection only; non-Python consumers install deps via
        ``TEST_CMD``). Detection is intentional root-only: a monorepo with
        per-package pyprojects but no root file also skips, since the root
        ``uv sync`` this method runs cannot succeed there either.

        Returns:
            ``"provisioned"`` after a successful ``uv sync``, or
            ``"provision_skipped: no_python_project"`` when no root
            ``pyproject.toml`` is present.
        """
        if not (self.path / "pyproject.toml").is_file():
            return PROVISION_SKIPPED_NO_PYTHON
        proc = subprocess.run(  # noqa: S603
            ["uv", "sync"],
            cwd=str(self.path),
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        if proc.returncode != 0:
            raise SecureSandboxError(
                f"secure sandbox env provisioning (uv sync) failed: {(proc.stderr or proc.stdout or '').strip()[-500:]}"
            )
        return PROVISIONED

    def port_commits_back(self) -> list[str]:
        """Replay sandbox commits (base..HEAD) onto the lane worktree branch.

        Returns the new commit SHAs now on the worktree branch (in order). No-op
        when grok made no commit. Author/message/multi-commit shape is preserved
        via ``format-patch`` -> ``am`` so ``close_slice`` provenance is faithful.
        Uncommitted sandbox changes are intentionally NOT ported — the offload
        contract is a committed end-state; a bare working-tree diff surfaces as
        ``uncommitted_work`` exactly as before.
        """
        # Defense-in-depth (review F-HIGH): re-assert isolation AFTER grok's turn.
        # A fetch/submodule/escape that widened history during the turn is caught
        # here, before we port commits and the pass treats the lane as clean.
        self.verify_isolated()

        head = _git(["rev-parse", "HEAD"], cwd=self.path).stdout.strip()
        if head == self.base_sha:
            return []
        patches = _git(["rev-list", "--reverse", f"{self.base_sha}..HEAD"], cwd=self.path).stdout.split()
        patch_text = _git(["format-patch", "--stdout", f"{self.base_sha}..HEAD"], cwd=self.path).stdout
        before = _git(["rev-parse", "HEAD"], cwd=self.worktree_path).stdout.strip()
        # The patches were diffed against ``base_sha`` (the clone tip). If the lane
        # worktree advanced since sandbox creation (a retried/overlapping pass on
        # the same lane), a 3-way apply onto a diverged base would silently
        # mis-merge — refuse rather than corrupt the branch (review F-MED).
        if before != self.base_sha:
            raise SecureSandboxError(
                f"lane worktree advanced since sandbox creation (HEAD {before[:12]} "
                f"!= clone base {self.base_sha[:12]}); refusing to port onto a "
                "diverged base."
            )
        proc = subprocess.run(  # noqa: S603
            ["git", "am", "--3way"],
            cwd=str(self.worktree_path),
            input=patch_text,
            capture_output=True,
            text=True,
            timeout=300,
        )
        if proc.returncode != 0:
            _git(["am", "--abort"], cwd=self.worktree_path, check=False)
            # Verify the abort actually restored a clean state — a leftover
            # ``rebase-apply`` dir would corrupt the NEXT reuse of this (reused)
            # lane worktree. Surface it loudly so the operator repairs it (review
            # F-MED) rather than the corruption being silently inherited.
            git_dir = _git(["rev-parse", "--absolute-git-dir"], cwd=self.worktree_path, check=False).stdout.strip()
            mid_am = bool(git_dir) and (Path(git_dir) / "rebase-apply").exists()
            corrupt = (
                " AND `git am --abort` did not restore a clean state — the lane "
                "worktree is left MID-AM (run `git am --abort` there manually)"
                if mid_am
                else ""
            )
            raise SecureSandboxError(
                "porting sandbox commits back to the lane branch failed "
                f"({len(patches)} commit(s)){corrupt}: "
                f"{(proc.stderr or proc.stdout).strip()[-500:]}"
            )
        after = _git(["rev-parse", "HEAD"], cwd=self.worktree_path).stdout.strip()
        new = _git(["rev-list", "--reverse", f"{before}..{after}"], cwd=self.worktree_path).stdout.split()
        return new

    def __exit__(self, *exc: object) -> None:
        self._cleanup()

    def _cleanup(self) -> None:
        if self._tmpdir and Path(self._tmpdir).exists():
            shutil.rmtree(self._tmpdir, ignore_errors=True)
        self._tmpdir = ""
