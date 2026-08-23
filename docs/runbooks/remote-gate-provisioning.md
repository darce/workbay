# Remote test-gate provisioning runbook (internal)

Target state: frictionless programmatic test offload to the OCI VM over Tailscale, executing ONLY as an unprivileged, resource-capped `gate` user. Fixes review findings 1, 2, 3, 4, 7, 8, 10 (round r0712-wb-remote-gate).

Host: your gate VM (`<your-gate-host>` / <your-gate-host-ip>), VM.Standard.A1.Flex 4 OCPU / 24 GB, co-resident dev backend + Postgres (host port 55432).

## Phase 1 — gate user (operator, one-time, as ubuntu on the VM)

```bash
# 1.1 user: no password login, no sudo, no service groups
sudo adduser --disabled-password --gecos "remote test gate" gate
groups gate   # expect: gate  (nothing else — verify no docker/adm/sudo)

# 1.2 resource fence: cap EVERY session/process of the gate user via its user slice
GATE_UID=$(id -u gate)
sudo mkdir -p /etc/systemd/system/user-${GATE_UID}.slice.d
sudo tee /etc/systemd/system/user-${GATE_UID}.slice.d/50-gate-caps.conf >/dev/null <<'EOF'
[Slice]
MemoryMax=8G
MemorySwapMax=1G
CPUQuota=250%
IOWeight=20
TasksMax=512
EOF
sudo systemctl daemon-reload

# 1.3 allow gate's user manager to run without an active login (background runs)
sudo loginctl enable-linger gate

# 1.4 tooling: pinned uv install (no curl|sh; SEC-10)
UV_VER=0.11.21
sudo -u gate mkdir -p /home/gate/.local/bin
curl -LsSf -o /tmp/uv.tar.gz \
  "https://github.com/astral-sh/uv/releases/download/${UV_VER}/uv-aarch64-unknown-linux-gnu.tar.gz"
# verify checksum against the release's published sha256 before extracting:
sha256sum /tmp/uv.tar.gz   # compare manually with GitHub release checksums file
sudo -u gate tar -xzf /tmp/uv.tar.gz -C /home/gate/.local/bin --strip-components=1
sudo -u gate /home/gate/.local/bin/uv --version
```

Verify fence: `sudo -u gate systemd-run --user --scope -p MemoryMax=1M cat /dev/zero` should be OOM-killed instantly once the slice is live.

### 1.5 consumer-suite service prerequisites (HIGH for context-alt-text-monorepo)

Its `test-integration` lane needs Postgres at `localhost:55432` with `pgvector`
and a role able to `CREATE DATABASE` on `*_test` names — otherwise the conftest
skips on any DB error and the gate greenwashes (make exits 0 with the whole pg
suite silently dropped). Provision before routing that repo's integration lane
here (scratch role on the existing PG, or a second small PG container owned by
`gate`); `remote_gate.sh doctor` probes the DSN's port but NOT auth/pgvector
usability — a reachable port is necessary, not sufficient.

## Phase 2 — Tailscale ACL (admin console)

Add/merge in the tailnet policy file:

```jsonc
"ssh": [
  // programmatic gate path: promptless, unprivileged user only
  { "action": "accept",
    "src":    ["autogroup:member"],          // add "tag:ci" later for GHA reuse
    "dst":    ["tag:oci-vm"],           // ensure the VM carries this tag
    "users":  ["gate"] },
  // interactive admin stays check-gated (re-auth prompt)
  { "action": "check",
    "src":    ["autogroup:member"],
    "dst":    ["tag:oci-vm"],
    "users":  ["ubuntu"] }
]
```

Verify from laptop: `ssh gate@<your-gate-host> true` succeeds with no prompt; `ssh ubuntu@…` triggers check re-auth.

Optional hardening (only if a non-Tailscale OpenSSH path must stay open): forced-command key in `/home/gate/.ssh/authorized_keys`:
`command="/home/gate/bin/gate-shell",no-pty,no-port-forwarding,no-agent-forwarding,no-X11-forwarding ssh-ed25519 AAAA…`
where `gate-shell` accepts only `git-receive-pack` for the clone path and an allowlisted runner invocation.

## Phase 3 — script fixes on feature/wb-remote-gate-02 (blocking before bootstrap)

`scripts/remote_gate.sh` changes, mapped to findings:

1. Default host becomes `gate@<your-gate-host>` (F1/F10). Never `ubuntu@`.
2. Validate `REMOTE_DIR` locally: reject empty, `.`, absolute, or paths containing `..`; require it to end with the repo slug (F3).
3. Derive default `REMOTE_DIR` per repo: `src/$(basename "$(git rev-parse --show-toplevel)")` (portability collision fix).
4. Remote body: `cd "$HOME/$REMOTE_DIR" || exit 1`; before any `git clean`, assert sentinel: `[ -f .remote-gate-clone ] || { echo "refusing: sentinel missing"; exit 1; }` — bootstrap creates the sentinel (F3/F6).
5. Serialize the clone: wrap the remote run body in `flock -n "$HOME/$REMOTE_DIR/.gate.lock" … || { echo "gate busy"; exit 75; }` (F4).
6. Validate `targets` against `^[A-Za-z0-9._-]+$`, `NICENESS`/`WORKERS` as integers, before interpolation (F7).
7. Replace bare `nice` with `systemd-run --user --scope -p MemoryMax=6G -p CPUQuota=200% nice -n "$NICENESS" ionice -c3 env …` (F2; per-run cap inside the user-slice outer cap).
8. SSH opts: add `-o ServerAliveInterval=30 -o ServerAliveCountMax=4` (F9 hang fix).
9. Generalization (portability review): read `.workbay/remote-gate.env` if present; keys `REMOTE_GATE_HOST`, `REMOTE_GATE_DIR`, `REMOTE_GATE_WORKDIR` (cd before uv sync + make), `REMOTE_GATE_TARGETS`, `REMOTE_GATE_ENV` (KEY=VALUE list injected into env line). Env vars override file values. `uv sync` runs in WORKDIR and is skipped when no pyproject.toml there.
10. `doctor` additionally probes each service URL named in `REMOTE_GATE_ENV` (e.g. Postgres DSN reachability) so silent pytest skips surface as doctor warnings.
11. Bootstrap: drop curl|sh (uv preinstalled in Phase 1); create clone dir + `git init` + `receive.denyCurrentBranch ignore` + touch `.remote-gate-clone`; refuse to run if `$PWD == $HOME` after cd.
12. Memory admission (internal integration): the remote run body calls
    `workbay-hostgov probe --json --workspace-root "$PWD"` **after `uv sync`**
    (so the console script the sync installs into the clone's `.venv/bin` is on
    the lookup path), before `make`. The hook searches, in order,
    `$PWD/.venv/bin/workbay-hostgov`, `$HOME/.local/bin/workbay-hostgov`, then
    `PATH`. A defer/refuse exits **74** (distinct from the lock-busy **75**); an
    absent CLI logs a `memory admission SKIPPED` line (never silent) and the
    systemd caps remain the backstop. `doctor` reports which path (if any) it
    found. **Activation:** merging internal is sufficient — the next
    `check-remote` run's `uv sync` installs `workbay-hostgov` into the clone
    `.venv/bin` and the hook picks it up automatically (no separate
    `~/.local/bin` install step required). The Linux probe branch
    (`/proc/meminfo` MemAvailable + PSI `/proc/pressure/memory`; a blind PSI
    probe degrades to `warn`, never a silent allow) is the single admission
    implementation shared by laptop and VM.

## Phase 4 — bootstrap + validation (agent-runnable once Phases 1–3 land)

```bash
scripts/remote_gate.sh doctor       # expect: uv version, mem, disk, clone MISSING
scripts/remote_gate.sh bootstrap    # as gate@…, creates ~/src/<repo>/ + sentinel
scripts/remote_gate.sh run check-protocol   # ~1 min end-to-end proof
```

Then route the pending 0115 merge-gate suites through `make check-remote`.

## Phase 5 — harness permission scope

Add ONLY wrapper-scoped rules to the harness allowlist:
`Bash(scripts/remote_gate.sh *)`, `Bash(make check-remote*)`.
Do NOT add `ssh gate@… *` (and never `ssh ubuntu@… *`) — the wrapper + target validation is the local boundary.

## Phase 6 — optional follow-ups

- Per-repo consumer config for context-alt-text-monorepo (after overlay sync):
  `REMOTE_GATE_WORKDIR=apps/prototype-description-service`, `REMOTE_GATE_TARGETS="test test-integration"`, `REMOTE_GATE_ENV="IDENTITY_PG_TEST_URL=postgresql+psycopg://context:context@localhost:55432/acx_identity_test"`.
  Do **not** put reserved knobs (`PYTEST_WORKERS`, `TMPDIR`, `WORKBAY_*`) in
  `REMOTE_GATE_ENV` — the script's own values win over the file (env-over-file
  precedence). Set `PYTEST_WORKERS` via the environment when calling
  `make check-remote`. Note `.workbay/remote-gate.env` is **dot-sourced (executed)
  as bash**, not parsed — it is operator-local and gitignored, same trust tier
  as a dotfile; never paste untrusted content into it.
- PG scratch role for gate (least privilege): `CREATE ROLE gate_test LOGIN CREATEDB PASSWORD NULL` restricted via pg_hba to localhost, or a second PG container owned by gate.
- GHA reuse: add `tag:ci` to the ACL ssh rule src; CI then runs the same `remote_gate.sh run` path.
- Tailscale SSH session recording for `gate` sessions (audit trail).
- Upgrade path: dedicated `tag:dev` VM; move ACL dst, add network ACL denying tag:dev → tag:oci-vm.

## Acceptance checklist

- [ ] `ssh gate@…` promptless; `groups gate` shows no service groups; slice caps active (OOM probe).
- [ ] `ssh ubuntu@…` still check-gated.
- [ ] remote_gate.sh: dir validation, sentinel, flock, target validation, systemd-run caps, ServerAlive, config file support — each covered by a shellcheck-clean implementation + a smoke test.
- [ ] doctor surfaces PG unreachable as a warning (silent-skip fix proven).
- [ ] `run check-protocol` green end-to-end from a laptop worktree.
- [ ] Harness allowlist contains wrapper rules only.
- [ ] Findings F1–F10 updated in handoff with fix evidence; review re-run recorded.

## Migration note (2026-07-12)

The pre-rework validation used an `ubuntu`-owned clone at `/home/ubuntu/src/agentic`
(receive repo only; no service state). After Phase 1 provisions the `gate` user,
remove it: `sudo rm -rf /home/ubuntu/src/agentic`. The reworked script derives
`src/<repo-slug>` under the gate user's home instead.

## Distribution note

`scripts/remote_gate.sh` is repo-local (in zero overlay/packaging manifests).
Consumer repos copy the script + add their own make wrapper until a
workbay-system packaging slice ships it as a synced surface. Without a
`.workbay/remote-gate.env`, absent-file defaults run this repo's default
targets — a consumer repo without those targets gets `No rule to make target`
failures; drop the per-repo env file in first.

## CI reporting from the gate host (`make ci-report`)

The gate host can publish the verdict to GitHub instead of only to the invoking
terminal, which is what lets it stand in for a hosted Actions run. `make
ci-report` runs `scripts/remote_gate.sh` over a CI-parity target set and POSTs
one commit status per target.

`.github/workflows/test.yml` stays on disk either way — the file is a pinned
contract (`packages/workbay-system/tests/make/test_workbay_ci_gate.py`), and
whether GitHub actually runs it is a repository setting, not something this
script changes.

### Token (operator, one-time, on the VM)

Authenticate `gh` on the gate host with a **fine-grained** PAT, not a classic
`repo` token: the same VM runs codex/cursor/grok agent turns, and a classic
token would hand every one of them write access to every repo on the account.

Minimum scope — this repository only, two permissions:

| Setting | Value |
| --- | --- |
| Repository access | Only select repositories → `<owner>/<this-repo>` |
| Commit statuses | Read and write |
| Metadata | Read (mandatory, auto-selected) |

The public mirror does not belong in the selection. It has no CI and no pull
requests, and it is pushed to over SSH from the laptop, not from the VM.

Install it as the operator, directly on the VM — never paste a token into an
agent session:

```bash
# On the VM, as gate. The absolute path matters: bash resolves a command name
# against the CURRENT PATH before applying a `VAR=value` assignment prefix, so
# `PATH=$HOME/.local/bin:$PATH gh ...` still reports `gh: command not found`
# over a non-interactive ssh, where ~/.local/bin is off PATH.
~/.local/bin/gh auth login --hostname github.com --with-token < token.txt
~/.local/bin/gh auth status
rm -f token.txt
```

Verify the scope actually resolves before trusting it. A fine-grained token
returns **404, not 403**, for a repo outside its selection, so a missing repo
reads as "no such repository":

```bash
~/.local/bin/gh api repos/<owner>/<repo> --jq '.private'          # → true
~/.local/bin/gh api repos/<owner>/<repo>/commits/<sha>/status --jq '.state'
```

The token can be re-scoped in place from the GitHub UI (repository selection
and permissions are both editable) without minting a new token string.

### Usage

```bash
make ci-report                                   # CI-parity set against HEAD
make ci-report TARGETS="check-system"            # subset
make ci-report CI_REPORT_FLAGS=--dry-run         # runs the gate, posts nothing
scripts/ci_report.sh --from-log <path>           # re-report; no gate run
```

`--dry-run` suppresses only the posting — it still spends a full gate run. Pair
it with `--from-log` to exercise the reporter for free, and use `--from-log`
alone to re-publish after a status POST fails on an otherwise green run.

Run it from the checkout you want gated: `remote_gate.sh` pushes `HEAD`, so
invoking from a linked worktree gates that worktree's branch. The commit must
already be on an origin branch or GitHub rejects the status with a 422.

### Contract

- One status per target, context `remote-gate/<target>`: `pending` →
  `success` | `failure`. A target that never ran gets `error`, not `failure`.
- One rollup context `remote-gate`, the AND of every target.
- Gate exit **74** (host-memory admission deferred) or **75** (clone lock busy)
  is not a verdict: per-target statuses are left `pending` and the reporter
  exits with the same code. Re-run.
- Commit statuses are append-only. Re-running supersedes a context in the
  combined view; it never deletes the earlier status.

`CI_PARITY_TARGETS` in `scripts/ci_report.sh` carries a `# job:` annotation per
entry, and `packages/workbay-system/tests/make/test_ci_report_parity.py` fails
when a `test.yml` job has no gate target — otherwise a job added to CI would go
ungated with every status still green.

## Remote agent execution (grok on the VM)

Operator-gated provisioning for the remote-exec backend (`grok-remote`): run
agent worker turns on the gate VM instead of the laptop. No live provisioning
from this tree — install key + CLI on the VM out-of-band, then point the
laptop wrapper at the host. Exit 75 is the unified retryable defer (VM memory
floor, lane cap, or residual-timeout exhausted pre-grok); callers should
re-dispatch when the VM has headroom.

### Why a dedicated VM-scoped key

Issue a **separate** xAI API key for the gate VM, distinct from primary
operator keys. Scope of compromise and rotation stay independent: revoking the
VM key does not rotate laptop credentials, and a laptop-side leak does not
hand the VM credential. Never commit the key to the repo, overlay, export, or
handoff logs ([SEC-06], [WEB-16]).

### Where the key lives on the VM

Store credentials **out of tree** on the VM only (never under the clone
directory that remote runners push into):

- grok auth material under the gate user's home (e.g. `~/.grok/auth.json` after
  `grok login --device-auth`, or the grok CLI's documented env for API keys)
- permissions restricted to the gate user (e.g. `0600` on auth files)
- not in `~/src/<repo-slug>/`, not in gitignored repo-local env files that
  get archived or synced by mistake

Placeholder form only when documenting: `<your-xai-key>` (never a key-shaped
`xai-…` literal — the public export scrub-gate fails closed on those).

### Install grok CLI on the VM

On the gate VM, as the unprivileged `gate` user (after Phase 1 tooling):

1. Install the grok CLI into a user-local bin (e.g. `~/.grok/bin/grok`) per
   current xAI / grok CLI install docs for the VM architecture.
2. Authenticate once with the dedicated VM-scoped key / device auth so
   `~/.grok/auth.json` (or equivalent) is present and mode-restricted.
3. Confirm: `~/.grok/bin/grok --version` prints a version; auth file present.

Do **not** bake host identity or keys into scripts under version control.

### Readiness check

From a laptop worktree with host configured:

```bash
# host via env (placeholder only)
export WORKBAY_REMOTE_GATE_HOST='gate@<your-host>'
# or: REMOTE_GATE_HOST=gate@<your-host> in .workbay/remote-gate.env (gitignored)

scripts/remote_agent.sh doctor
```

Expect: grok binary present, auth present, `uv` present, systemd-run caps
available (or a clear MISSING line for each). Exit `78` means host not
configured — set `WORKBAY_REMOTE_GATE_HOST` or the config-file host first.
`make doctor` reports a local offline `grok_remote` facet (configured vs
skip); it does **not** SSH — use `scripts/remote_agent.sh doctor` for the
live probe.

### Env knobs (`scripts/remote_agent.sh`)

Precedence: **process env always wins over the config file for every knob**.
The script snapshots all `WORKBAY_*` knobs before sourcing
`.workbay/remote-gate.env` at the **git common-dir root** (same host as
`remote_gate.sh`), so a file that exports a `WORKBAY_*` value cannot override
the operator's environment (including silently zeroing `MEM_FLOOR_MB`).

Config-file keys exist **only** for HOST/DIR fallbacks (`REMOTE_GATE_HOST`,
`REMOTE_GATE_DIR`). Caps / floor / lanes / sandbox-root are env-only (script
defaults when the corresponding env var is unset).

| Variable | Role | Source |
| --- | --- | --- |
| `WORKBAY_REMOTE_GATE_HOST` | Required SSH destination (`gate@<your-host>`); no baked-in default | env, else `REMOTE_GATE_HOST` in config file |
| `WORKBAY_REMOTE_GATE_DIR` | Remote clone dir (default `src/<repo-slug>`) | env, else `REMOTE_GATE_DIR` in config file |
| `WORKBAY_REMOTE_AGENT_ROOT` | Sandbox parent dir on the VM (default `grok-sandbox`) | env-only (defaults when unset) |
| `WORKBAY_REMOTE_GATE_MEMORY_MAX` | Per-run memory cap (default `6G`) | env-only (defaults when unset) |
| `WORKBAY_REMOTE_GATE_CPU_QUOTA` | Per-run CPU quota (default `200%`) | env-only (defaults when unset) |
| `WORKBAY_REMOTE_GATE_MEM_FLOOR_MB` | VM MemAvailable floor in MiB (default `2048`); below floor the lane defers (exit 75) | env-only (defaults when unset) |
| `WORKBAY_REMOTE_AGENT_MAX_LANES` | Concurrent named `grok-lane-*` systemd scopes on the VM (script default `3`, but env-only — this deployment sets `19`); at/above the cap the lane defers (exit 75). Read the live value; do not assume the default | env-only (defaults when unset) |
| `WORKBAY_REMOTE_AGENT_MAX_LANE_VENVS` | Retained per-lane venvs on the VM. Must be kept strictly greater than `MAX_LANES` — the cross-lane LRU reap protects only the reaping lane's own venv, so other live lanes survive only by ranking inside this cap by mtime. Coupling is unenforced | env-only (defaults when unset) |

Optional file keys (when not set in the environment): `REMOTE_GATE_HOST`,
`REMOTE_GATE_DIR` in `.workbay/remote-gate.env` at the git common-dir root.
Do not put `WORKBAY_*` knob assignments in the config file — they are ignored
when env is set and are not the supported file surface.

### Operator checklist (agent-exec)

- [ ] Dedicated VM-scoped xAI key issued; primary keys untouched
- [ ] Key / auth only on the VM under `~/.grok/` (out of tree), mode-restricted
- [ ] grok CLI installed for the gate user; `scripts/remote_agent.sh doctor` green
- [ ] Laptop has `WORKBAY_REMOTE_GATE_HOST=gate@<your-host>` (or config file)
- [ ] No host, tailnet, IP, or key-shaped literal committed to the repo

## Remote agent execution (cursor on the VM)

Operator-gated provisioning for the remote-exec backend (`cursor-remote`, implementation note). Same thin adapter surface as grok/codex: binary + auth live on the gate
VM; the laptop only SSHes. Do **not** copy host Cursor IDE credentials onto the
VM. Do **not** use Cursor Cloud Agents for this plan (separate plan if needed).

### Credential port: provision / rotate / probe

Every off-box backend declares an `AuthPort` in
`packages/mcp-workbay-orchestrator/src/workbay_orchestrator_mcp/orchestration/backend_registry.py`
(implementation note). `scripts/provision_remote_auth.sh` reads that port with one
registry call and holds **no** table of its own; the rendered auth probe
(`render_auth_probe`) is the smoke, streamed to the VM over ssh stdin.

| id | kind | var | path (VM `$HOME`-relative) |
| --- | --- | --- | --- |
| `cursor-remote` | `env_file` | `CURSOR_API_KEY` | `~/.config/cursor-agent/env` (mode `0600`) |
| `0xalpha-remote` | `env_file` + key-info | `WORKBAY_0XALPHA_API_KEY` | `~/.config/0xalpha/env` (mode `0600`); probe also calls OpenRouter `/api/v1/key` |
| `codex-remote` | `device_login` | — | `~/.codex/auth.json` (`~/.local/bin/codex login`) |
| `grok-remote` | `device_login` | — | `~/.grok/auth.json` (`~/.grok/bin/grok login`) |

**Provision** (laptop; the key never appears on any argv or in any child
environment — it is streamed over ssh stdin into a `0600` file):

```bash
# Create a VM-scoped key first (Cursor: https://cursor.com/dashboard/api,
# Settings -> API Keys, not the IDE login). Never reuse a laptop key.
CURSOR_API_KEY='…' ./scripts/provision_remote_auth.sh --backend cursor-remote --host gate@<your-host>
# or: ./scripts/provision_remote_auth.sh --backend cursor-remote --host gate@<your-host> --key-file ./cursor.key
# on the VM as gate (local write + local probe):
CURSOR_API_KEY='…' ./scripts/provision_remote_auth.sh --backend cursor-remote
```

Key source order: process env `$<var>` -> `--key-file` -> no-echo prompt
(TTY only). Empty, whitespace, newline-bearing and quoted values are refused
(exit 2). The script then appends an idempotent `~/.profile` loader
(`set -a; . <path>; set +a`) for login shells and runs the probe.

**Rotate** = re-run it with the new value; the file is overwritten in place:

```bash
WORKBAY_0XALPHA_API_KEY="$(security find-generic-password -s 0xalpha -a workbay -w)" \
  ./scripts/provision_remote_auth.sh --backend 0xalpha-remote --host gate@<host>
```

**Probe only** (no key touched, but the idempotent `~/.profile` loader is
still appended; exit 0 iff the VM is authenticated, else the probe's own exit
code: 10 binary missing, 11 env file / artifact missing, 12 env file
malformed, 13 logged out, 14 unverified; 255 is reported as an ssh transport
failure, not a probe verdict):

```bash
./scripts/provision_remote_auth.sh --backend cursor-remote --host gate@<your-host> --env-already-written
# stdout: CURSOR_AUTH_OK
```

`--skip-smoke` skips the probe (stderr: `smoke skipped (--skip-smoke)`). A
`device_login` backend (`codex-remote`, `grok-remote`) exits 2 with *backend
`<id>` authenticates by device login; run `"$HOME/<binary> login"` on the VM* —
this script provisions nothing for it, by design (no automation path prompts
for something it cannot deliver). Interpreter: `$WORKBAY_PYTHON`, else
`.venv/bin/python`, else `uv run --frozen --no-sync --project
packages/mcp-workbay-orchestrator python`; the registry query (and the
`git rev-parse` that locates the repo) run under an allow-listed `env -i`, so
the interpreter tree (uv, `.pth` hooks) never sees the key. VM paths are the
registry-rendered ones (`$HOME/...` or absolute); bash does not re-derive them.
`scripts/provision_cursor_remote_auth.sh` remains as a shim for
`--backend cursor-remote`.

`scripts/remote_agent.sh` sources `~/.config/cursor-agent/env` before
agent-exec **only when** `AGENT_SPEC_BIN=cursor-agent` (non-interactive
`bash -s` does not load `.profile`). Missing file is a no-op; grok/codex
lanes never load the Cursor key. Never commit a key to the repo, overlay,
export, or handoff logs ([SEC-06], [WEB-16]); revoking the VM key must not
rotate laptop auth.

### Install cursor-agent (if not already)

On the lane VM as the unprivileged `gate` user (linux/arm64 tarball — pin from
implementation note S7a / `CURSOR_REMOTE_VERSION` in packaged `cursor_lane_config.py`).

**Measured install (2026-08-03) on the provisioned remote gate host:**

| Fact | Value |
| --- | --- |
| Version pin | `2026.07.23-e383d2b` (must match `CURSOR_REMOTE_VERSION`) |
| Download URL | `https://downloads.cursor.com/lab/2026.07.23-e383d2b/linux/arm64/agent-cli-package.tar.gz` |
| `INSTALL_ROOT` | `~/.local/share/cursor-agent/versions/2026.07.23-e383d2b` |
| Symlink layout | `~/.local/bin/cursor-agent` → `$INSTALL_ROOT/cursor-agent` |

```bash
VER=2026.07.23-e383d2b
INSTALL_ROOT="$HOME/.local/share/cursor-agent/versions/${VER}"
mkdir -p "$INSTALL_ROOT" "$HOME/.local/bin"
curl -fsSL -o /tmp/agent-cli-package.tar.gz \
  "https://downloads.cursor.com/lab/${VER}/linux/arm64/agent-cli-package.tar.gz"
tar -xzf /tmp/agent-cli-package.tar.gz -C "$INSTALL_ROOT" --strip-components=1
ln -sfn "$INSTALL_ROOT/cursor-agent" "$HOME/.local/bin/cursor-agent"
~/.local/bin/cursor-agent --version   # expect the pinned version
```

**Rollback** (not an npm global — [R4-M01]): remove the symlink and the version
dir only:

```bash
rm -f "$HOME/.local/bin/cursor-agent"
rm -rf "$HOME/.local/share/cursor-agent/versions/2026.07.23-e383d2b"
```

These install facts must stay in this runbook (not in packaged Python source):
the wheel privacy gate forbids operator infrastructure identifiers in shipped
artifacts.

### Headless smoke (operator evidence)

Interactive login shell (after env file exists). **Measured 2026-08-03** on the
provisioned remote gate host after `~/.config/cursor-agent/env` (`CURSOR_API_KEY`,
mode `0600`):

```bash
set -a
# shellcheck disable=SC1090
. "$HOME/.config/cursor-agent/env"
set +a
~/.local/bin/cursor-agent -p --force --trust --workspace /tmp \
  --output-format json "ping"
# → success JSON with "result":"pong" (is_error=false).
```

Expect a JSON result with `"result":"pong"` (and `"is_error":false`). Example
shape (values vary):

```json
{"type":"result","subtype":"success","is_error":false,"result":"pong",…}
```

Non-interactive path (matches lane dispatch): `scripts/remote_agent.sh doctor`
should report `cursor  : <version>` and `cursor-env: present` when the binary
and env file are installed.

### Operator checklist (cursor-remote)

- [ ] Cursor API key created at <https://cursor.com/dashboard/api> (VM-scoped)
- [ ] `~/.config/cursor-agent/env` on the VM only, mode `0600`, out of tree
- [ ] `cursor-agent` installed for `gate`; version matches plan pin when required
- [ ] `provision_remote_auth.sh --backend cursor-remote --host … --env-already-written` prints `CURSOR_AUTH_OK` (or the manual `result=pong` smoke above)
- [ ] `scripts/remote_agent.sh doctor` shows `cursor  :` + `cursor-env: present`
- [ ] No key-shaped literal, host, or env file committed to the repo

## Remote agent execution (0xAlpha on the VM)

Operator-gated provisioning for `0xalpha-remote` (implementation note): the **same codex
binary** the `codex-remote` backend uses (installed per implementation note S4, nothing
new to install) pointed at OpenRouter's `stealth/ox-alpha` listing through a
`model_providers.0xalpha.*` override set and an OpenRouter API key. The key
lives only on the VM in the declared `env_file` port; the laptop only SSHes.
Config module:
`packages/mcp-workbay-orchestrator/src/workbay_orchestrator_mcp/orchestration/oxalpha_lane_config.py`
(slug, env var, env-file path, key-info URL, `OXALPHA_MIN_REMAINING_USD`,
advertised effort set).

### Data-handling note (read before routing anything here)

A $0 stealth listing is paid for with prompts. Every lane brief, every file the
agent reads from the checked-out tree, and every tool output reaches an
**undisclosed provider** ("Stealth") under OpenRouter's terms. Treat it as
publication: do **not** route lanes that touch private canon, credentials,
secrets-adjacent files (`.env*`, keychain exports, `~/.config/*`, handoff
exports with operator notes), or unreleased customer material through this
backend. This is operator policy within the [SEC-08] scope — the orchestrator
does not classify content for you. When in doubt, use `codex-remote`.

### Prerequisites

- codex installed for `gate` on the VM (implementation note S4); `scripts/remote_agent.sh
  doctor` already shows it. `0xalpha-remote` reuses that binary — the auth
  probe runs `codex --version` before it touches the env file.
- An OpenRouter API key created for this VM only, **with a spend cap set in
  the OpenRouter dashboard** (Keys -> Credit limit). The probe refuses an
  uncapped key (`limit: null` -> exit 15) by policy; the cap is the hard
  ceiling, `OXALPHA_MIN_REMAINING_USD` (default `1.0`) is the soft floor below
  which dispatch is refused and a task blocker is raised ([SEC-08], [AGT-10]).
- Keep the key in the macOS keychain, never in a file in the repo or a shell
  rc:

```bash
security add-generic-password -s 0xalpha -a workbay -w     # prompts for the value
security find-generic-password -s 0xalpha -a workbay -w    # reads it back
```

### Provision / probe / rotate / revoke

**Provision** (laptop; the key is read from the keychain into the process env
of the provisioning script only, streamed over ssh stdin into a `0600` file,
never placed on an ssh or curl argv):

```bash
WORKBAY_0XALPHA_API_KEY="$(security find-generic-password -s 0xalpha -a workbay -w)" \
  ./scripts/provision_remote_auth.sh --backend 0xalpha-remote --host gate@<your-host>
# or from a file you then shred:
./scripts/provision_remote_auth.sh --backend 0xalpha-remote --host gate@<your-host> --key-file ./0xalpha.key
```

**Probe only** (no key touched; exit 0 iff authenticated with usable budget):

```bash
./scripts/provision_remote_auth.sh --backend 0xalpha-remote --host gate@<your-host> --env-already-written
# stdout: limit=<n> usage=<n> remaining=<n>
#         WORKBAY_0XALPHA_AUTH_OK
```

**Rotate** = create the new key in the OpenRouter dashboard, update the
keychain item (`security add-generic-password -U …`), re-run **Provision**; the
env file is overwritten in place.

**Revoke** = delete the key in the OpenRouter dashboard **and** remove the env
file on the VM (`rm ~/.config/0xalpha/env` as `gate`); the next probe then
reports `WORKBAY_0XALPHA_AUTH_MISSING` (exit 11) and the backend is not
dispatchable.

### Exit-code ladder (probe and `provision_remote_auth.sh`)

| exit | marker | meaning |
| --- | --- | --- |
| 0 | `WORKBAY_0XALPHA_AUTH_OK` | key valid, `remaining >= OXALPHA_MIN_REMAINING_USD`; the `limit=… usage=… remaining=…` line precedes the marker |
| 10 | `WORKBAY_0XALPHA_INSTALL_MISSING` / `_INSTALL_BROKEN` | codex absent, not executable, or `--version` fails (checked **before** the env file is read) |
| 11 | `WORKBAY_0XALPHA_AUTH_MISSING` | env file absent or unreadable |
| 12 | `WORKBAY_0XALPHA_AUTH_INVALID` | env file has no `WORKBAY_0XALPHA_API_KEY=` assignment, the value is empty, or it contains a double quote / backslash |
| 13 | `WORKBAY_0XALPHA_AUTH_FAILED` | OpenRouter answered 401 — key revoked or wrong |
| 14 | `WORKBAY_0XALPHA_AUTH_UNVERIFIED` | curl/python3 missing on the VM, non-2xx other than 401, non-JSON or non-finite body — never treated as green |
| 15 | `WORKBAY_0XALPHA_BUDGET_EXHAUSTED` | key valid but `limit` null (uncapped, refused), `limit_remaining` null, or below the threshold; the reading line is still printed when present |
| 255 | — | ssh transport failure — `scripts/provision_remote_auth.sh` reports it distinctly (`ssh transport failure (rc 255), not a probe verdict`, its `255)` case) |

### Budget alert and refusal

The budget is enforced at the daemon spawn edge, not only in the optional
`offload_preflight` tool: `worker_start` (which `dispatch_lane_work`,
`manage_worker start` and `worker_start_all` all route through) and
`run_offload_pass` call `key_info_admission_gate` in
`packages/mcp-workbay-orchestrator/src/workbay_orchestrator_mcp/orchestration/offload_preflight.py`
right after host-memory admission. For backends without a key-info port the
gate is inert (no probe, no change). For `0xalpha-remote` it reads the
TTL-cached probe's `key_info` (30 s cache — not a new ssh round-trip per
dispatch); when `remaining` is below `OXALPHA_MIN_REMAINING_USD` (or the probe
reports exit 15) it records an open **blocker** on the lane's task —
`0xalpha budget below threshold: remaining=… limit=… usage=… (threshold 1.0
USD; backend 0xalpha-remote); dispatch refused …` — and returns
`outcome=admission_refused` with that wording, so the worker is never spawned.
`offload_preflight` applies the same check and raises. The blocker is keyed on
the stable `0xalpha budget below threshold` prefix: a retry loop refreshes the
numbers on the existing open row instead of stacking rows, and a call without
a `task_ref` records on the workspace's active task (an unresolvable task
logs a `budget alert dropped` warning — the refusal still stands). Clear it by
topping up / rotating the key and resolving the blocker; a fresh reading needs
the probe cache to expire or the orchestrator to restart.

### Doctor

`scripts/remote_agent.sh doctor` prints one `auth` line **per declared port**
(grok, codex, 0xalpha, cursor — rendered from the registry, never the value or
perms; env-file ports read `present` / `empty` / `unreadable` / `MISSING`,
device-login ports `exists` / `MISSING`; the rendering interpreter is found via
`WORKBAY_PYTHON`, the checkout's `.venv`, `uv`, then a PATH `python3` that can
import the orchestrator package — a total miss prints a loud
`auth : registry unavailable … last error: …` line on stderr) and, for 0xalpha, a `budget` line with the
probe's `limit/usage/remaining` plus a `price` line with the slug's current
list price from the public `https://openrouter.ai/api/v1/models` index
(`curl --max-time 10`; `price : … unavailable` on any failure, never a hang).
Four `auth` lines on the VM is the S5 acceptance check.

### Known benign noise (never a failure signal)

Both of the following appear on **successful** authenticated turns (observed
live 2026-08-23) and are excluded from the auth / model-unavailable match
patterns in `backend_spec.py` by a pinned test; do not add them back:

- JSONL item `{"type":"item.completed","item":{"type":"error","message":"Model
  metadata for \`stealth/ox-alpha\` not found. Defaulting to fallback
  metadata…"}}` — codex has no metadata for a non-OpenAI slug; the turn still
  completes.
- stderr `ERROR codex_models_manager::manager: failed to refresh available
  models: … missing field \`models\`` — codex probing OpenRouter's `/models`
  endpoint with a schema it does not recognise; harmless.

The only model-unavailable signal is OpenRouter's `No endpoints found` body;
the only auth signals are the full missing-env-var line and `No auth
credentials`. Bare `401`/`404` or the env-var name are deliberately **not**
patterns — the VM classifier matches substrings with no rc gate.

### Efforts

Only `low` and `high` ship (`OXALPHA_ALLOWED_EFFORTS`): the model advertises
`max/high/low`, `max` is outside the remote effort set, and `medium`/`xhigh`
are refused because the transport accepts any value silently.

### Operator checklist (0xalpha-remote)

- [ ] OpenRouter key created for the VM only, **credit limit set** in the dashboard
- [ ] Keychain item `-s 0xalpha -a workbay` holds the key; no file copy left behind
- [ ] `provision_remote_auth.sh --backend 0xalpha-remote --host … --env-already-written` prints `WORKBAY_0XALPHA_AUTH_OK` with a `limit=` line
- [ ] `scripts/remote_agent.sh doctor` shows four `auth` lines, `budget  : 0xalpha-remote limit=…`, and a `price` line
- [ ] Data-handling note read; no private-canon or secrets-adjacent lane routed here
- [ ] No key-shaped literal, host, or env file committed to the repo
