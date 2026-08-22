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

### Why a dedicated VM-scoped API key

Create a **Cursor API key** at <https://cursor.com/dashboard/api> (Settings →
API Keys — not the IDE “login” session). Issue a key scoped to the gate VM /
lane use, distinct from any laptop Cursor session. Revoking the VM key must not
rotate laptop auth. Never commit the key to the repo, overlay, export, or
handoff logs ([SEC-06], [WEB-16]).

IDE login alone is insufficient for headless `cursor-agent` on the VM; the CLI
expects `CURSOR_API_KEY` (or an interactive `agent login` that does not fit
non-interactive SSH).

### Where the key lives on the VM

Store credentials **out of tree** on the VM only (never under the clone):

| Path | Role |
| --- | --- |
| `~/.config/cursor-agent/env` | `CURSOR_API_KEY=…` only; mode `0600` |
| `~/.profile` (optional loader) | `set -a; . ~/.config/cursor-agent/env; set +a` for login shells |

`scripts/remote_agent.sh` sources `~/.config/cursor-agent/env` before
agent-exec **only when** `AGENT_SPEC_BIN=cursor-agent` (non-interactive
`bash -s` does not load `.profile`). Missing file is a no-op; grok/codex
lanes never load the Cursor key.

### Provision (preferred)

From a laptop worktree (key never on the ssh argv):

```bash
# Create the key at https://cursor.com/dashboard/api first.
CURSOR_API_KEY='…' ./scripts/provision_cursor_remote_auth.sh --host gate@<your-host>
# or: ./scripts/provision_cursor_remote_auth.sh --host gate@<your-host> --key-file ./cursor.key
```

On the VM as `gate`:

```bash
CURSOR_API_KEY='…' ./scripts/provision_cursor_remote_auth.sh
# or interactive prompt / --key-file
```

The script writes `~/.config/cursor-agent/env` (mode `0600`), appends the
`.profile` loader if missing, and runs a headless smoke unless `--skip-smoke`.

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
- [ ] Headless smoke returns `result=pong` (script or manual commands above)
- [ ] `scripts/remote_agent.sh doctor` shows `cursor  :` + `cursor-env: present`
- [ ] No key-shaped literal, host, or env file committed to the repo
