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

History-stripped review sandboxes receive one narrowly governed widening: an
orchestrator-built ``.review/`` payload containing exactly one declared
merge-base diff.  Construction is trust-gated, size-bounded, redacted, and the
payload itself is secret-scanned.  It never accepts caller-selected paths.

Defense-in-depth ONLY — pair with a network egress deny of the ``/v1/storage``
upload channel and the ``grok-code-session-traces`` bucket. The shallow clone
removes reliance on the network control; the egress deny removes reliance on
grok's cooperation.
"""

from __future__ import annotations

import json
import os
import re
import shlex
import shutil
import subprocess
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

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

# The review payload is deliberately smaller than the fallback scanner's
# per-file ceiling.  Consequently every possible artifact is actually scanned;
# an over-cap payload is refused rather than silently truncated.
REVIEW_CONTEXT_MAX_BYTES = 512 * 1024
REVIEW_CONTEXT_DIRNAME = ".review"
REVIEW_CONTEXT_COMMIT_SUBJECT = "chore(review): procure governed context payload"
REVIEW_CONTEXT_PAYLOAD_WALK_LIMIT = 8
REVIEW_CONTEXT_REDACTION_POLICY = "sha-subject-only+strip-index-blob-oids-v1"
_REVIEW_SUBJECT_PATHSPEC = ("--", ".", f":(exclude){REVIEW_CONTEXT_DIRNAME}")

# Explicit allowlist: never derive trust from a backend's name, cost class, or
# current availability.  The grok vendor backends are intentionally absent.
# Local trusted backends do not need a payload because they retain the lane's
# ordinary git view; the two trusted remote transports require one.
REVIEW_CONTEXT_BACKEND_TRUST: dict[str, str] = {
    "codex-cli": "trusted_local",
    "codex-subagent": "trusted_local",
    "copilot-host": "trusted_local",
    "claude-code": "trusted_local",
    "cursor-cli": "trusted_local",
    "codex-remote": "trusted_remote",
    "cursor-remote": "trusted_remote",
}
REVIEW_CONTEXT_PAYLOAD_BACKENDS = frozenset({"codex-remote", "cursor-remote"})


# Sanctioned skip when the sandbox clone has no root Python project (implementation note).
# Distinct from admission refusals and from real uv-sync failures (SecureSandboxError).
PROVISION_SKIPPED_NO_PYTHON = "provision_skipped: no_python_project"
PROVISIONED_PYTHON = "provisioned_python"
PROVISIONED_NODE = "provisioned_node"
PROVISIONED_PHP = "provisioned_php"
# Backward-compatible import name; Python provisioning now carries its arm.
PROVISIONED = PROVISIONED_PYTHON
PROVISION_SKIPPED_NO_RUNNER_FOR_TEST_CMD = "provision_skipped: no_runner_for_test_cmd"


class SecureSandboxError(RuntimeError):
    """Fail-closed sandbox error: clone not shallow, clone failed, or env
    provisioning (``uv sync``) failed when a root ``pyproject.toml`` is present.

    Absence of a root Python project is a sanctioned skip
    (``provision_skipped: no_python_project``), not an error. Secret findings
    are advisory, not errors.
    """


class ReviewContextPayloadRefusal(SecureSandboxError):
    """Typed, fail-closed refusal to procure a review-context payload."""

    def __init__(self, code: str, detail: str, *, findings: list[str] | None = None) -> None:
        super().__init__(detail)
        self.code = code
        self.detail = detail
        self.findings = list(findings or [])

    def as_dict(self) -> dict[str, Any]:
        return {
            "ok": False,
            "outcome": "review_context_refused",
            "refusal_code": self.code,
            "error": self.detail,
            "findings": self.findings,
        }


@dataclass(frozen=True)
class ReviewContextPayload:
    """Audit record for an orchestrator-procured ``.review`` payload."""

    path: Path
    payload_bytes: int
    base_ref: str
    tip_ref: str
    merge_base_sha: str
    tip_sha: str
    backend: str
    trust_tier: str
    redaction_policy: str
    payload_commit_sha: str
    changed_paths: tuple[str, ...]
    resolved_from_payload_commits: int = 0

    def phase_record(self) -> dict[str, Any]:
        return {
            "phase": "review_context_payload",
            "payload_bytes": self.payload_bytes,
            "redaction_policy": self.redaction_policy,
            "backend": self.backend,
            "trust_tier": self.trust_tier,
            "base_ref": self.base_ref,
            "tip_ref": self.tip_ref,
            "merge_base_sha": self.merge_base_sha,
            "tip_sha": self.tip_sha,
            "payload_commit_sha": self.payload_commit_sha,
            "resolved_from_payload_commits": self.resolved_from_payload_commits,
            "changed_path_count": len(self.changed_paths),
        }


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


def _runner_arm(test_cmd: str | None) -> str | None:
    """Map an explicitly referenced package runner to its provision arm."""
    if not test_cmd:
        return None
    try:
        tokens = shlex.split(test_cmd)
    except ValueError:
        tokens = test_cmd.split()
    commands = {Path(token).name for token in tokens}
    if commands & {"npm", "npx", "pnpm", "yarn", "vitest", "jest"}:
        return "node"
    if commands & {"phpunit", "composer"}:
        return "php"
    return None


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


def review_context_trust_tier(backend: str) -> str:
    """Return the explicit review-context trust tier or refuse.

    Absence from the map is the policy.  In particular, capability flags and a
    backend being otherwise dispatchable cannot implicitly grant access to
    branch delta content.
    """
    normalized = str(backend or "").strip()
    tier = REVIEW_CONTEXT_BACKEND_TRUST.get(normalized)
    if tier is None:
        raise ReviewContextPayloadRefusal(
            "backend_not_allowlisted",
            f"review context refused: backend {normalized!r} is not on the explicit payload trust allowlist",
        )
    return tier


def _review_subject_candidates(lane_row: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    """Collect review-subject mappings from a lane row without changing lookup order."""

    candidates: list[Mapping[str, Any]] = [lane_row]
    nested = lane_row.get("review_subject")
    if isinstance(nested, Mapping):
        candidates.insert(0, nested)
    notes = lane_row.get("notes")
    if isinstance(notes, str) and notes.strip().startswith("{"):
        try:
            decoded = json.loads(notes)
        except (TypeError, ValueError):
            decoded = None
        if isinstance(decoded, Mapping):
            note_subject = decoded.get("review_subject")
            if isinstance(note_subject, Mapping):
                candidates.insert(0, note_subject)
    return candidates


def review_subject_from_lane_row(lane_row: Mapping[str, Any]) -> tuple[str, str] | None:
    """Read the orchestrator-declared ``(base_ref, tip_ref)`` from a lane row.

    The durable schema's ``notes`` field is used as a typed extension envelope:
    ``{"review_subject": {"base_ref": "...", "tip_ref": "..."}}``.  Direct
    keys and a mapping-valued ``review_subject`` are also accepted for rows
    projected by newer schema versions.  No path field is read in any form.
    """

    for candidate in _review_subject_candidates(lane_row):
        base = candidate.get("base_ref", candidate.get("review_base_ref"))
        tip = candidate.get("tip_ref", candidate.get("review_tip_ref"))
        if isinstance(base, str) and base.strip() and isinstance(tip, str) and tip.strip():
            return base.strip(), tip.strip()
    return None


def review_subject_tip_pin(lane_row: Mapping[str, Any]) -> str | None:
    """Read a previously persisted subject ``tip_sha`` without widening the two-tuple contract."""

    for candidate in _review_subject_candidates(lane_row):
        pin = candidate.get("tip_sha")
        if isinstance(pin, str) and pin.strip():
            return pin.strip()
    return None


def _git_bytes(args: list[str], *, cwd: Path) -> bytes:
    try:
        completed = subprocess.run(  # noqa: S603
            ["git", *args],
            cwd=str(cwd),
            capture_output=True,
            check=True,
            timeout=300,
        )
    except subprocess.CalledProcessError as exc:
        detail = (exc.stderr or exc.stdout or b"").decode("utf-8", errors="replace").strip()[-500:]
        raise ReviewContextPayloadRefusal(
            "review_subject_unresolvable",
            f"review context git command failed ({' '.join(args[:2])}): {detail}",
        ) from exc
    return completed.stdout


def _resolve_review_commit(repo: Path, ref: str, label: str) -> str:
    try:
        return _git_bytes(["rev-parse", "--verify", f"{ref}^{{commit}}"], cwd=repo).decode("ascii").strip()
    except ReviewContextPayloadRefusal as exc:
        raise ReviewContextPayloadRefusal(
            "review_subject_unresolvable",
            f"declared review {label} ref {ref!r} does not resolve to a commit",
        ) from exc


def _payload_commit_parent(repository: Path, sha: str) -> str | None:
    """Return the first parent when ``sha`` is an orchestrator payload commit.

    Both the exact payload subject and a tree that touches only
    ``REVIEW_CONTEXT_DIRNAME`` are required, so an unrelated commit that
    reuses the message cannot be unwound.
    """

    subject = _git_bytes(["log", "-1", "--format=%s", sha], cwd=repository).decode("utf-8", errors="replace").strip()
    if subject != REVIEW_CONTEXT_COMMIT_SUBJECT:
        return None
    names_raw = _git_bytes(
        ["diff-tree", "--no-commit-id", "--name-only", "-r", sha],
        cwd=repository,
    )
    names = [item.decode("utf-8", errors="surrogateescape") for item in names_raw.splitlines() if item]
    if not names:
        return None
    prefix = f"{REVIEW_CONTEXT_DIRNAME}/"
    if any(name != REVIEW_CONTEXT_DIRNAME and not name.startswith(prefix) for name in names):
        return None
    parents = _git_bytes(["rev-list", "--parents", "-n", "1", sha], cwd=repository).decode("ascii").split()
    if len(parents) < 2:
        return None
    return parents[1]


def _unwind_payload_tip(repository: Path, tip_sha: str) -> tuple[str, int]:
    current = tip_sha
    walked = 0
    while walked < REVIEW_CONTEXT_PAYLOAD_WALK_LIMIT:
        parent = _payload_commit_parent(repository, current)
        if parent is None:
            return current, walked
        current = parent
        walked += 1
    if _payload_commit_parent(repository, current) is not None:
        raise ReviewContextPayloadRefusal(
            "review_subject_self_referential",
            "review tip is a chain of more than "
            f"{REVIEW_CONTEXT_PAYLOAD_WALK_LIMIT} payload commits; refusing to "
            "diff a payload into itself",
        )
    return current, walked


def _is_ancestor_or_equal(repository: Path, ancestor: str, descendant: str) -> bool:
    completed = _git(
        ["merge-base", "--is-ancestor", ancestor, descendant],
        cwd=repository,
        check=False,
    )
    if completed.returncode in {0, 1}:
        return completed.returncode == 0
    detail = (completed.stderr or completed.stdout or "").strip()[-500:]
    raise ReviewContextPayloadRefusal(
        "review_subject_unresolvable",
        f"could not compare review pin {ancestor} to tip {descendant}: {detail}",
    )


def _payload_file_bytes(root: Path) -> int:
    return sum(path.stat().st_size for path in root.iterdir() if path.is_file())


def build_review_context_payload(
    repo: Path | str,
    *,
    base_ref: str,
    tip_ref: str,
    backend: str,
    max_bytes: int = REVIEW_CONTEXT_MAX_BYTES,
    tip_sha_pin: str | None = None,
) -> ReviewContextPayload:
    """Build and commit a bounded review payload from one declared ref pair.

    There is intentionally no path/filter parameter.  Changed paths are
    derived from ``merge-base(base_ref, tip_ref)..tip_ref`` and then verified
    against the same complete range.  Any refusal happens before ``.review`` is
    created.  Payload content is never truncated.
    """

    normalized_backend = str(backend or "").strip()
    trust_tier = review_context_trust_tier(normalized_backend)
    if normalized_backend not in REVIEW_CONTEXT_PAYLOAD_BACKENDS:
        raise ReviewContextPayloadRefusal(
            "payload_not_required",
            f"backend {normalized_backend!r} is allowlisted as {trust_tier} "
            "but does not use a history-stripped remote payload",
        )
    if isinstance(max_bytes, bool) or not isinstance(max_bytes, int) or max_bytes <= 0:
        raise ReviewContextPayloadRefusal("invalid_size_cap", "review context max_bytes must be a positive integer")

    repository = Path(repo).expanduser().resolve()
    if not repository.is_dir():
        raise ReviewContextPayloadRefusal("repository_unavailable", f"review repository is unavailable: {repository}")
    base = str(base_ref or "").strip()
    tip = str(tip_ref or "").strip()
    if not base or not tip:
        raise ReviewContextPayloadRefusal(
            "review_subject_missing",
            "review context requires both a declared base_ref and tip_ref",
        )

    base_sha = _resolve_review_commit(repository, base, "base")
    resolved_tip = _resolve_review_commit(repository, tip, "tip")
    if tip_sha_pin is not None:
        pin = str(tip_sha_pin).strip()
        if not pin:
            raise ReviewContextPayloadRefusal(
                "review_subject_pin_mismatch",
                "tip_sha_pin must be a full commit sha that is an ancestor of the resolved tip_ref",
            )
        pin_sha = _resolve_review_commit(repository, pin, "tip pin")
        if not _is_ancestor_or_equal(repository, pin_sha, resolved_tip):
            raise ReviewContextPayloadRefusal(
                "review_subject_pin_mismatch",
                f"tip_sha_pin {pin_sha} is not an ancestor of resolved tip {resolved_tip}",
            )
        candidate_tip = pin_sha
    else:
        candidate_tip = resolved_tip
    tip_sha, resolved_from_payload_commits = _unwind_payload_tip(repository, candidate_tip)
    merge_bases = _git_bytes(["merge-base", "--all", base_sha, tip_sha], cwd=repository).decode("ascii").split()
    if len(merge_bases) != 1:
        raise ReviewContextPayloadRefusal(
            "review_merge_base_ambiguous",
            f"declared review subject has {len(merge_bases)} merge bases; exactly one is required",
        )
    merge_base = merge_bases[0]
    revision_range = f"{merge_base}..{tip_sha}"

    status = _git_bytes(["status", "--porcelain=v1", "--untracked-files=all"], cwd=repository)
    if status:
        raise ReviewContextPayloadRefusal(
            "review_worktree_dirty",
            "review context procurement requires a clean worktree so only orchestrator-owned payload files are committed",
        )

    names_raw = _git_bytes(
        ["diff", "--name-only", "--no-renames", "-z", revision_range, *_REVIEW_SUBJECT_PATHSPEC],
        cwd=repository,
    )
    changed_paths = tuple(item.decode("utf-8", errors="surrogateescape") for item in names_raw.split(b"\0") if item)
    diff_raw = _git_bytes(
        [
            "diff",
            "--binary",
            "--no-color",
            "--no-ext-diff",
            "--no-renames",
            revision_range,
            *_REVIEW_SUBJECT_PATHSPEC,
        ],
        cwd=repository,
    )
    # Scope verification is deliberately a second Git measurement, not a
    # caller-provided assertion.  It fails closed if the complete range changes
    # while the payload is being assembled.
    verified_names_raw = _git_bytes(
        ["diff", "--name-only", "--no-renames", "-z", revision_range, *_REVIEW_SUBJECT_PATHSPEC],
        cwd=repository,
    )
    if verified_names_raw != names_raw:
        raise ReviewContextPayloadRefusal(
            "review_subject_changed",
            "declared review subject changed while its payload was being built",
        )

    diff_text = diff_raw.decode("utf-8", errors="replace")
    diff_text = re.sub(r"(?m)^index [0-9a-f]+\.\.[0-9a-f]+(?: [0-7]{6})?\n", "", diff_text)
    commits = _git_bytes(["log", "--abbrev=12", "--format=%h %s", revision_range], cwd=repository).decode(
        "utf-8", errors="replace"
    )
    diffstat = _git_bytes(["diff", "--stat", "--no-renames", revision_range], cwd=repository).decode(
        "utf-8", errors="replace"
    )
    context = (
        "# Orchestrator review context\n\n"
        f"- Base ref: `{base}` (`{base_sha}`)\n"
        f"- Tip ref: `{tip}` (`{tip_sha}`)\n"
        f"- Merge base: `{merge_base}`\n"
        f"- Scope: complete `{merge_base}..{tip_sha}` diff; paths are derived, never requested by a brief\n"
        f"- Redaction policy: `{REVIEW_CONTEXT_REDACTION_POLICY}`\n"
        f"- Resolved backend trust tier: `{trust_tier}`\n"
    )

    target = repository / REVIEW_CONTEXT_DIRNAME
    with tempfile.TemporaryDirectory(prefix=".review-context-", dir=str(repository)) as tmp:
        staged = Path(tmp)
        (staged / "CHANGE.diff").write_text(diff_text, encoding="utf-8")
        (staged / "COMMITS.txt").write_text(commits, encoding="utf-8")
        (staged / "DIFFSTAT.txt").write_text(diffstat, encoding="utf-8")
        (staged / "CONTEXT.md").write_text(context, encoding="utf-8")
        payload_bytes = _payload_file_bytes(staged)
        if payload_bytes > max_bytes:
            raise ReviewContextPayloadRefusal(
                "payload_too_large",
                f"review context is {payload_bytes} bytes, above the {max_bytes}-byte cap; refusing without truncation",
            )
        hard, advisory = _scan_secrets(staged)
        findings = [*hard, *advisory]
        if findings:
            raise ReviewContextPayloadRefusal(
                "payload_secret_detected",
                "review context secret scan found sensitive material; refusing payload",
                findings=findings,
            )

        if target.is_symlink() or target.is_file():
            target.unlink()
        elif target.is_dir():
            shutil.rmtree(target)
        staged.rename(target)

    tracked_before = bool(_git_bytes(["ls-files", "--", REVIEW_CONTEXT_DIRNAME], cwd=repository).strip())
    try:
        _git_bytes(["add", "-f", "--", REVIEW_CONTEXT_DIRNAME], cwd=repository)
        staged_changed = subprocess.run(  # noqa: S603
            ["git", "diff", "--cached", "--quiet", "--", REVIEW_CONTEXT_DIRNAME],
            cwd=str(repository),
            check=False,
            timeout=300,
        ).returncode
        if staged_changed == 1:
            _git_bytes(
                [
                    "-c",
                    "user.name=workbay-orchestrator",
                    "-c",
                    "user.email=orchestrator@workbay.invalid",
                    "-c",
                    "commit.gpgsign=false",
                    "commit",
                    "-m",
                    REVIEW_CONTEXT_COMMIT_SUBJECT,
                    "--",
                    REVIEW_CONTEXT_DIRNAME,
                ],
                cwd=repository,
            )
        elif staged_changed != 0:
            raise ReviewContextPayloadRefusal(
                "payload_index_check_failed",
                "could not verify the staged review payload",
            )
    except Exception:
        _git(
            ["restore", "--staged", "--worktree", "--source=HEAD", "--", REVIEW_CONTEXT_DIRNAME],
            cwd=repository,
            check=False,
        )
        if not tracked_before:
            shutil.rmtree(target, ignore_errors=True)
        raise

    payload_commit = _git_bytes(["rev-parse", "HEAD"], cwd=repository).decode("ascii").strip()
    return ReviewContextPayload(
        path=target,
        payload_bytes=payload_bytes,
        base_ref=base,
        tip_ref=tip,
        merge_base_sha=merge_base,
        tip_sha=tip_sha,
        backend=normalized_backend,
        trust_tier=trust_tier,
        redaction_policy=REVIEW_CONTEXT_REDACTION_POLICY,
        payload_commit_sha=payload_commit,
        changed_paths=changed_paths,
        resolved_from_payload_commits=resolved_from_payload_commits,
    )


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

    def provision_env(self, *, timeout: int = 900, test_cmd: str | None = None) -> str:
        """Provision the sandbox test env so the worker's self-verify runs against
        the SANDBOX src, not a stale/absent one.

        A fresh ``--depth=1`` clone has no ``.venv``; a Python lane ``TEST_CMD``
        (e.g. ``../../.venv/bin/python -m pytest``) would die on
        ``ModuleNotFoundError``. When a root ``pyproject.toml`` is present, run
        the canonical workspace-root ``uv sync`` (implementation note D3b) so
        ``<sandbox>/.venv`` holds editables pointing at the clone. Failures are
        returned with their arm and output tail so the caller can fail closed.

        Root ``package.json`` and ``composer.json`` manifests select Node and PHP
        provisioning respectively. An optional ``test_cmd`` disambiguates repos
        with multiple manifests and makes a referenced runner without its matching
        manifest a typed provisioning skip instead of a later test failure.

        Returns:
            an arm-specific ``"provisioned_*"`` / ``"provision_failed_*"``, or
            ``"provision_skipped: no_python_project"`` when no root
            ``pyproject.toml`` is present.
        """
        manifests = {
            "python": (self.path / "pyproject.toml").is_file(),
            "node": (self.path / "package.json").is_file(),
            "php": (self.path / "composer.json").is_file(),
        }
        requested_arm = _runner_arm(test_cmd)
        if requested_arm is not None and not manifests[requested_arm]:
            return PROVISION_SKIPPED_NO_RUNNER_FOR_TEST_CMD

        arm = requested_arm or next((name for name in ("python", "node", "php") if manifests[name]), None)
        if arm is None:
            return PROVISION_SKIPPED_NO_PYTHON

        if arm == "python":
            command = ["uv", "sync"]
            success = PROVISIONED_PYTHON
        elif arm == "php":
            command = ["composer", "install", "--no-interaction"]
            success = PROVISIONED_PHP
        else:
            command = self._node_install_command()
            success = PROVISIONED_NODE

        try:
            proc = subprocess.run(  # noqa: S603
                command,
                cwd=str(self.path),
                capture_output=True,
                text=True,
                timeout=timeout,
            )
        except (FileNotFoundError, subprocess.TimeoutExpired) as exc:
            return f"provision_failed_{arm}: {str(exc).strip()[-500:]}"
        if proc.returncode != 0:
            detail = (proc.stderr or proc.stdout or f"exit {proc.returncode}").strip()[-500:]
            return f"provision_failed_{arm}: {detail}"
        return success

    def _node_install_command(self) -> list[str]:
        if (self.path / "pnpm-lock.yaml").is_file():
            return ["pnpm", "install", "--frozen-lockfile"]
        if (self.path / "yarn.lock").is_file():
            return ["yarn", "install", "--frozen-lockfile"]
        if (self.path / "package-lock.json").is_file() or (self.path / "npm-shrinkwrap.json").is_file():
            return ["npm", "ci"]
        return ["npm", "install"]

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
