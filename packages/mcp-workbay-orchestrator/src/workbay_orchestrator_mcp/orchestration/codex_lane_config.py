"""Codex remote-lane pins measured on the OCI VM (implementation note S1).

Mirrors :mod:`grok_lane_config` / :mod:`cursor_lane_config` as the single source
for codex-remote path/version/timeout/auth-match so ``build_agent_spec`` never
re-derives them ([REF-26]).

**S1a (2026-08-03):** binary on the lane VM (Linux aarch64) via user npm
prefix ``~/.local``.

**S1b (2026-08-03, remeasured):** three review-brief turns at ~4k/16k/64k with
``gpt-5.6-sol`` + ``model_reasoning_effort=high`` under a *successful* tool
regime. Initial measure under ``workspace-write`` hit broken bwrap
(``RTM_NEWADDR`` / ``apparmor_restrict_unprivileged_userns=1``); remeasure used
``danger-full-access`` as apparatus only — see :data:`MEASUREMENT_SANDBOX`.

**Host fix (2026-08-03):** ``kernel.apparmor_restrict_unprivileged_userns=0``
persisted in ``/etc/sysctl.d/99-workbay-codex-userns.conf``; ``codex exec -s
workspace-write`` file reads verified as ``gate``. Lane argv stays
``workspace-write`` ([SEC-04]); never widen to full-access.
Raw evidence lives under ``tests/fixtures/plan0182_s1b/``.
"""

from __future__ import annotations

import os

from workbay_orchestrator_mcp.orchestration.offload_timeout_ssot import CODEX_TIMEOUT_CAP

# Home-relative CLI path (resolved under remote $HOME by remote_agent.sh).
# Absolute /home/<user>/... is a privacy leak and breaks non-matching accounts.
# S2 preflight must still verify this path exists on the live VM ([AGT-02]).
CODEX_REMOTE_BIN = ".local/bin/codex"

# Directory injected into PATH for the remote heredoc (home-relative, like
# grok ``.grok/bin`` and cursor ``.local/bin``).
CODEX_PATH_PREPEND = ".local/bin"

# Pinned CLI version that passed S1a install + S1b D10 model coupling.
CODEX_CLI_VERSION = "0.145.0"

# One-shot S1a observation that `command -v timeout` succeeded. Not durable —
# S2/S6 must re-probe ([FM-08], [RES-02]).
VM_TIMEOUT_AVAILABLE = True

# Evidence sample (2026-08-03): byte delta on `/` for the user-prefix install.
INSTALL_DISK_DELTA_BYTES = 318_877_696
INSTALL_DISK_DELTA_BUDGET_BYTES = 1_073_741_824

# D10: slug confirmed on CLI 0.145.0. Tracked pin (implementation note M3); env
# WORKBAY_CODEX_MODEL overrides. Discovery (M2) publishes onto
# BACKENDS[].allowed_model / the offload profile only — DEFAULT_* stays
# the flock/CLI snapshot by design.
TRACKED_CODEX_MODEL = "gpt-5.6-sol"
WORKBAY_CODEX_MODEL_ENV = "WORKBAY_CODEX_MODEL"
# Measured on codex-cli 0.145.0 (2026-08-30).  The CLI silently drops an
# unadvertised service tier while still exiting zero, so this model-specific
# allow-list is an authorization boundary, not merely documentation.
CODEX_MODEL_ALLOWED_SERVICE_TIERS: dict[str, frozenset[str]] = {
    TRACKED_CODEX_MODEL: frozenset({"default", "fast"}),
}
# Orchestration speaks intent; only the argv builder translates that intent to
# codex's process-level ``service_tier`` vocabulary.
CODEX_SPEED_TO_SERVICE_TIER: dict[str, str] = {
    "standard": "default",
    "fast": "fast",
}
# Codex exits zero after silently omitting an unsupported service tier. The
# remote classifier must treat this diagnostic as failed confirmation so a
# requested tier is never recorded as applied.
SERVICE_TIER_WARNING_PATTERNS: tuple[str, ...] = ("warning: Configured service tier",)
CODEX_ALLOWED_SPEEDS = frozenset(CODEX_SPEED_TO_SERVICE_TIER)
# VM catalogue probe (implementation note M2). Fixture-driven in tests; never invent a slug.
CODEX_LIST_MODELS_ARGV: tuple[str, ...] = ("codex", "models")


def resolve_tracked_or_env_codex_model() -> str:
    """Env override if set, else the tracked pin. Does not read settings.local.json."""
    raw = (os.environ.get(WORKBAY_CODEX_MODEL_ENV) or "").strip()
    return raw or TRACKED_CODEX_MODEL


DEFAULT_CODEX_MODEL = resolve_tracked_or_env_codex_model()

# --- Sandbox posture ---------------------------------------------------------
# Plan argv wants workspace-write and forbids --dangerously-bypass-* /
# danger-full-access for lanes ([SEC-04]). Host fix restored unprivileged
# userns; workspace-write file I/O verified for gate (blocker 166 resolved).
# Evidence: tests/fixtures/plan0182_s1b/workspace_write_probe.*.
PLAN_PREFERRED_SANDBOX = "workspace-write"
# Lane-facing sandbox pin — builders must use this, never the historical
# measure apparatus below ([CARD-12]).
LANE_SANDBOX = PLAN_PREFERRED_SANDBOX
WORKSPACE_WRITE_BROKEN_ON_VM = False
# Static False is observational, not a durable capability claim ([FM-08]).
# S2 build_agent_spec / dispatch MUST re-probe userns or workspace-write and
# fail closed if the host regresses (same class as VM_TIMEOUT_AVAILABLE).
WORKSPACE_WRITE_REQUIRES_LIVE_PREFLIGHT = True
WORKSPACE_WRITE_HOST_FIX = (
    "kernel.apparmor_restrict_unprivileged_userns=0 via "
    "/etc/sysctl.d/99-workbay-codex-userns.conf "
    "(was: bwrap RTM_NEWADDR / uid_map EPERM on Ubuntu 24.04 Oracle aarch64); "
    "residual risk: host-wide unprivileged userns enabled (not Codex-only); "
    "rollback: sudo sysctl -w kernel.apparmor_restrict_unprivileged_userns=1 "
    "&& sudo rm -f /etc/sysctl.d/99-workbay-codex-userns.conf"
)
# --- .git write access under workspace-write ---------------------------------
# ``workspace-write`` leaves the working tree writable but mounts ``.git``
# read-only, so a lane can do all of its work and then lose every byte: the
# commit dies with ``fatal: Unable to create '.git/index.lock': Read-only file
# system`` (exit 128) and ``turn.patch``, being derived from git state, comes
# back 0 bytes. That made every "commit your work" brief unsatisfiable on this
# backend and silently zeroed the review roster's remote-codex slot.
#
# The sandbox POLICY is unchanged: ``writable_roots`` adds ``.git`` back under
# the same ``workspace-write`` policy. The lane argv still never uses
# ``danger-full-access`` or ``--dangerously-bypass-*``, so [SEC-04] holds.
# The grant itself is not narrow: ``writable_roots`` is a directory root, so
# ``[".git"]`` is a full-gitdir write (``.git/config``, ``.git/hooks/**``,
# ``.git/info/alternates``, ``.git/commondir``, ``.git/index``). Residual
# risk (hook / config / alternates tampering) is contained by (a) host
# commit hook-neutralization in ``_apply_and_commit`` and (b) harvest
# gitdir sanitization in ``remote_agent.sh``. Least privilege still means
# grant ``.git`` and nothing else ([CARD-12] perceived boundary must equal
# the enforced boundary).
#
# Relative, not absolute: codex resolves ``writable_roots`` entries against the
# ``-C`` working directory, so ``.git`` is correct for every lane worktree and
# needs no path placeholder threaded through remote_agent.sh.
#
# Measured on the lane VM (codex-cli 0.145.0), A/B against the same repo with a
# control arm, because the schema alone cannot say whether ``writable_roots``
# overrides the built-in ``.git`` deny:
#   control  -s workspace-write                      -> "Read-only file system", exit 128, no commit
#   absolute -c ...writable_roots=["<workdir>/.git"]  -> commit landed, exit 0
#   relative -c ...writable_roots=[".git"]            -> commit landed, exit 0
#
# Residual risk, stated plainly: a lane granted ``.git`` write can rewrite
# history in its own sandbox copy (amend, reset, force-update refs) and can
# plant hook/config/alternates payloads. That copy is history-stripped and
# disposable; the host re-derives the patch and the two named containments
# above keep those payloads from executing on the operator host or harvest.
LANE_WRITABLE_ROOTS: tuple[str, ...] = (".git",)
# Observational like WORKSPACE_WRITE_BROKEN_ON_VM — a codex release could
# change the policy, so dispatch must keep failing closed rather than assuming
# the commit succeeded ([FM-08], [RES-02]).
WRITABLE_ROOTS_REQUIRES_LIVE_PREFLIGHT = True

# Historical S1b remeasure apparatus only (pre-host-fix). Never lane argv.
HISTORICAL_S1B_MEASURE_SANDBOX = "danger-full-access"
# Back-compat alias for fixture summary key ``measure_sandbox``.
MEASUREMENT_SANDBOX = HISTORICAL_S1B_MEASURE_SANDBOX

# --- Wall-clock / spend pins (S1b remeasure; plan D5 Layer 1 + R2-H07) --------
# Raw elapsed from tests/fixtures/plan0182_s1b/turns/*-meta.json.
# Rates are NOT read from meta precomputes — they are
# (output_tokens + reasoning_output_tokens) / wall_s from turn.completed.
MEASURED_WALL_CLOCK_SAMPLES_S: tuple[float, float, float] = (40.367, 37.872, 39.65)
MEASURED_OUTPUT_RATES_PER_S: tuple[float, float, float] = (
    58.56268734362227,  # (1522 + 842) / 40.367
    45.28411491339248,  # (1097 + 618) / 37.872
    56.69609079445145,  # (1449 + 799) / 39.65
)

LANE_TIMEOUT_HEADROOM = 1.5
LANE_TIMEOUT_MIN_S = 300
LANE_TIMEOUT_MAX_S = CODEX_TIMEOUT_CAP

# clamp(ceil(p95 * 1.5), 300, 3600) with p95 = 40.367 → 300.
LANE_TIMEOUT_S = min(300, CODEX_TIMEOUT_CAP)

# Maximum observed rate, rounded conservatively UPWARD (4 dp) so a worst-case
# spend bound never under-shoots the true max rate.
PEAK_OUTPUT_TOKENS_PER_S = 58.5627

# Plan flock *example* est_tokens cell (docs/plans/0182 … r17-a … 120000).
# Default when the manifest omits est_tokens (implementation note S6 / D5 layer 3).
DEFAULT_EST_TOKENS = 120_000
EXAMPLE_MANIFEST_EST_TOKENS = DEFAULT_EST_TOKENS

# --- Auth-failure detector pins (S1b; plan R2-M03) ---------------------------
# Lane argv uses --json: some strings land on stderr, some on the JSON stream
# (stdout). Detector must search both.
AUTH_MATCH_STREAMS: tuple[str, ...] = ("stderr", "stdout")

# Literal substrings (not regex). Fixtures under tests/fixtures/plan0182_s1b/auth/.
# Order: missing credential (stream); revoked refresh (stderr); workspace policy
# (stderr). Prefer "Missing bearer…" over bare "401 Unauthorized" ([OBS-04]).
AUTH_MATCH_PATTERNS: tuple[str, ...] = (
    "Missing bearer or basic authentication in header",
    "Your access token could not be refreshed. Please log out and sign in again.",
    "Login is restricted to workspace(s) ",
)

# Negative fixture: invalid_json_schema (400). Must not match AUTH_MATCH_PATTERNS.
AUTH_MATCH_NEGATIVE_PATTERNS: tuple[str, ...] = ("invalid_json_schema",)


def auth_text_matches(text: str) -> bool:
    """Return True if any positive auth_match pattern is a substring of text."""
    return any(pattern in text for pattern in AUTH_MATCH_PATTERNS)


def auth_text_is_negative_fixture(text: str) -> bool:
    """Return True if text looks like the unrelated API-failure fixture class."""
    return any(pattern in text for pattern in AUTH_MATCH_NEGATIVE_PATTERNS)
