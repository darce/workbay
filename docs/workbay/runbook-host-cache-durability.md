# Runbook: host cache durability (cross-harness)

Operator playbook for keeping heavy artifact caches off the boot volume and
ensuring cache redirection survives **non-login shells**, **GUI-launched MCP
servers**, and **`make`/lifecycle subprocesses** — not just interactive login
zsh sessions.

This runbook complements internal (`make check-disk-space` preflight
and `uv cache prune` at task-finish). implementation note catches a full boot/tmp volume;
this slice fixes the **delivery** gap that split caches across login shells vs
everything else.

**Scope:** host-level dotfiles the operator creates manually. The
`workbay-bootstrap` installer does **not** write these paths; treat this doc as
the source of truth.

**Sibling:** [runbook-mcp-server-launch.md](runbook-mcp-server-launch.md) (MCP
launch surfaces).

---

## Problem (recap)

Login shells sourced `~/.zshrc` and pointed caches at a roomy directory, but
`make`, MCP subprocesses, and other non-login processes never sourced that file.
Result: duplicate caches on the boot volume (e.g. `~/.cache/uv`) until the tmp
volume filled and pytest failed with `OSError: could not create numbered dir`.

Fix: tool-native config files (read by every process) **plus** `~/.zshenv`
(sourced by all zsh, including non-login/non-interactive).

---

## Configuration — pick your cache location

This runbook uses two placeholders. Substitute your own values everywhere they
appear below (they are deliberately generic — the cache root can be any
large-capacity path, not necessarily an external/removable volume):

| Placeholder | Meaning | Examples |
| --- | --- | --- |
| `<CACHE_ROOT>` | Directory where caches live. | `/Volumes/<your-volume>/cache` (removable volume), `/data/cache` (fixed second disk), `${HOME}/.cache-big` |
| `<CACHE_MOUNT>` | **Only if `<CACHE_ROOT>` lives on a mountable/removable volume:** the mountpoint to health-check. Leave **empty** when `<CACHE_ROOT>` is always present (a fixed path). | `/Volumes/<your-volume>`, or `""` |

The mount guard in Step 3 reads these as shell variables, so you set them in one
place. The tool-native config files in Step 1 are static and cannot read shell
variables — substitute `<CACHE_ROOT>` literally there.

Create the cache directories once:

```bash
mkdir -p <CACHE_ROOT>/{uv,npm,pip,fallback}
```

Optional — migrate existing boot-volume caches after verifying free space:

```bash
df -h /System/Volumes/Data      # macOS boot data volume; adjust path for your OS
# then move or delete stale boot-volume caches, e.g.:
# rm -rf ~/.cache/uv            # only after redirection is working
```

---

## Step 1 — Tool-native cache files (shell-independent)

These files are honored regardless of shell, harness, or login mode. They are
static — paste your real `<CACHE_ROOT>` in place of the placeholder.

### `~/.config/uv/uv.toml`

```toml
cache-dir = "<CACHE_ROOT>/uv"
```

uv 0.11+ auto-discovers this file; no env var required.

### `~/.npmrc`

```ini
cache=<CACHE_ROOT>/npm
```

### `~/.config/pip/pip.conf`

```ini
[global]
cache-dir = <CACHE_ROOT>/pip
```

Install:

```bash
mkdir -p ~/.config/uv ~/.config/pip
# paste the three files above (with <CACHE_ROOT> substituted) into:
#   ~/.config/uv/uv.toml
#   ~/.npmrc
#   ~/.config/pip/pip.conf
```

---

## Step 2 — Remove the cache exports from `~/.zshrc`

**Every zsh** sources `~/.zshenv`, including non-login and non-interactive
shells — closing the gap for local `make` targets and zsh-backed tooling. The
mount-guard block in **Step 3** is the single place these vars are (re-)exported
in `~/.zshenv`; it sets them **dynamically** so they track mount state.

So here you only **remove** the static exports from `~/.zshrc` — do **not**
re-add them statically anywhere. Re-adding a static `export
XDG_CACHE_HOME="<CACHE_ROOT>"` (etc.) in `~/.zshenv` would override the Step 3
guard and defeat the unmount fallback.

Delete cache-related exports like these from `~/.zshrc` (yours may name a
specific path):

```bash
export XDG_CACHE_HOME=...
export NPM_CONFIG_CACHE=...
export OLLAMA_MODELS=...
export HOMEBREW_CACHE=...
export PIP_CACHE_DIR=...
export DOCKER_CONFIG=...
export COLIMA_HOME=...
```

Keep `~/.zshrc` for interactive-only settings (prompt, plugins, aliases). The
Step 3 guard below is the only re-export site.

---

## Step 3 — Mount guard (cache root unmounted / missing)

When `<CACHE_ROOT>` lives on a removable volume and that volume is not mounted,
silently falling back to an unbounded boot-volume cache refills
`/System/Volumes/Data`. The guard:

1. Detects whether the configured mountpoint is actually mounted (skipped when
   `<CACHE_MOUNT>` is empty, i.e. the cache root is a fixed path).
2. Falls back to a **capped** boot-volume directory under
   `~/Library/Caches/workbay-fallback/` (or similar).
3. **Warns loudly** on stderr when fallback is active.

Add this block to `~/.zshenv` (it is the only place the cache vars are
exported — Step 2 removed them from `~/.zshrc`):

```bash
# --- mount guard (implementation note) ---
# Set these two for your machine (see the runbook's Configuration table):
WORKBAY_CACHE_ROOT="<CACHE_ROOT>"     # where caches live, e.g. /data/cache
WORKBAY_CACHE_MOUNT="<CACHE_MOUNT>"   # mountpoint to health-check; "" if always present
WORKBAY_FALLBACK_ROOT="${HOME}/Library/Caches/workbay-fallback"
WORKBAY_FALLBACK_MAX_MB=2048

# Use the cache root when no mountpoint is configured (fixed path) or when the
# configured mountpoint is actually mounted. Test real mountedness via `mount`,
# not directory existence: an unmounted volume can leave an empty mountpoint dir
# behind, so `[ -d ... ]` would false-positive and write to the boot volume.
if [ -z "${WORKBAY_CACHE_MOUNT}" ] || mount | grep -q " on ${WORKBAY_CACHE_MOUNT} "; then
  export WORKBAY_CACHE_ACTIVE="${WORKBAY_CACHE_ROOT}"
else
  export WORKBAY_CACHE_ACTIVE="${WORKBAY_FALLBACK_ROOT}"
  mkdir -p "${WORKBAY_CACHE_ACTIVE}"/{uv,npm,ollama,homebrew,pip,docker,colima}
  echo "workbay-cache-guard: WARN — ${WORKBAY_CACHE_MOUNT} not mounted; using fallback ${WORKBAY_CACHE_ACTIVE} (cap ${WORKBAY_FALLBACK_MAX_MB} MB)." >&2
  # Advisory size cap — interactive shells only, so `du` never walks the cache
  # on every non-interactive zsh (e.g. each `make`-spawned shell).
  if [[ -o interactive ]]; then
    _used_mb="$(du -sm "${WORKBAY_CACHE_ACTIVE}" 2>/dev/null | awk '{print $1}')"
    if [ -n "${_used_mb}" ] && [ "${_used_mb}" -gt "${WORKBAY_FALLBACK_MAX_MB}" ]; then
      echo "workbay-cache-guard: WARN — fallback cache ${_used_mb} MB exceeds cap ${WORKBAY_FALLBACK_MAX_MB} MB; prune ${WORKBAY_CACHE_ACTIVE} or remount ${WORKBAY_CACHE_MOUNT}." >&2
    fi
    unset _used_mb
  fi
fi

# Point every cache at the active root. These env vars OVERRIDE the Step 1
# config files (UV_CACHE_DIR > uv.toml; NPM_CONFIG_CACHE > .npmrc;
# PIP_CACHE_DIR > pip.conf), so on fallback uv/npm/pip follow it too —
# including uv, the tool whose cache originally filled the boot volume.
export UV_CACHE_DIR="${WORKBAY_CACHE_ACTIVE}/uv"
export XDG_CACHE_HOME="${WORKBAY_CACHE_ACTIVE}"
export NPM_CONFIG_CACHE="${WORKBAY_CACHE_ACTIVE}/npm"
export OLLAMA_MODELS="${WORKBAY_CACHE_ACTIVE}/ollama"
export HOMEBREW_CACHE="${WORKBAY_CACHE_ACTIVE}/homebrew"
export PIP_CACHE_DIR="${WORKBAY_CACHE_ACTIVE}/pip"
export DOCKER_CONFIG="${WORKBAY_CACHE_ACTIVE}/docker"
export COLIMA_HOME="${WORKBAY_CACHE_ACTIVE}/colima"
# --- end mount guard ---
```

`XDG_CACHE_HOME` is set to the cache **root** (its consumers nest their own
`uv/`, `pip/`, … subdirs under it), while the tool-specific vars point at named
**subdirs** — this asymmetry is intentional, not a typo.

Precedence note: for each tool the env var wins over its config file
(`UV_CACHE_DIR` over `~/.config/uv/uv.toml`, `NPM_CONFIG_CACHE` over `~/.npmrc`,
`PIP_CACHE_DIR` over `~/.config/pip/pip.conf`). So the Step 1 files give
shell-independent redirection to `<CACHE_ROOT>` for **non-zsh** processes
(e.g. a GUI-launched MCP server), while this guard redirects **zsh-spawned**
processes (terminal, `make`, lifecycle subprocesses) and — crucially — flips
them to the capped fallback when a removable cache root is unmounted.

**Residual gap (document, don't hide):** a purely GUI-launched process that never
sources `~/.zshenv` still reads only the Step 1 config files, which hardcode
`<CACHE_ROOT>`. If that root is on a removable volume and the volume is
unmounted, such a process has no shell-side fallback; rely on implementation note's
`make check-disk-space` preflight to catch boot-volume pressure, and prefer
launching MCP/tooling from a zsh-rooted parent so the guard applies.

**Pair with implementation note:** `make check-disk-space` fails fast if the tmp/boot volume
is nearly full — catching fallback refill before a 16-minute `check-all` run.

---

## Step 4 — Verification (non-login inheritance)

After applying Steps 1–3, prove redirection works **without** sourcing
`~/.zshrc`:

```bash
# uv must resolve to your cache root (tool-native config)
env -i HOME="$HOME" PATH="$PATH" uv cache dir
# expect: <CACHE_ROOT>/uv

# Boot volume health (macOS data volume; adjust path for your OS)
df -h /System/Volumes/Data

# Repo preflight (implementation note)
make check-disk-space
```

If `env -i ... uv cache dir` still prints `~/.cache/uv`, confirm
`~/.config/uv/uv.toml` exists and typos in `cache-dir` are absent.

---

## Harness-neutral delivery notes

MCP servers and lifecycle tools inherit the **subprocess environment** their
parent harness spawns — not your interactive shell rc files.

| Surface | Where heavy-artifact env belongs |
| --- | --- |
| Codex | `[mcp_servers.<id>.env]` in `~/.codex/config.toml` |
| Cursor / Claude Code | `"env": { ... }` on the server entry in `.mcp.json` / `.cursor/mcp.json` |
| Grok | Root `.mcp.json` server `env` block (when used) |

For **cache location** (this runbook), prefer Steps 1–3 so every process —
including MCP stdio children — picks up tool-native paths without per-harness
duplication. Per-server `env` blocks remain the right channel for **opt-in
feature flags** (see internal embeddings vars), not for replacing
`uv.toml` / `~/.zshenv`.

---

## Troubleshooting

| Symptom | Check |
| --- | --- |
| Boot volume fills again | `df -h /System/Volumes/Data`; run `make check-disk-space`; inspect `~/.cache/uv` for split-brain |
| `uv cache dir` differs in Terminal vs `make` | Compare `env -i HOME=$HOME PATH=$PATH uv cache dir` vs login shell; fix `uv.toml` not `.zshrc` alone |
| Cache root unmounted | Expect mount-guard stderr warnings; remount `<CACHE_MOUNT>` or prune fallback dir |
| Long `check-all` then tmp `OSError` | implementation note preflight should fail in seconds — if not, confirm `check-system` / `check-all` invoke `check-disk-space` |

---

## Related

- implementation note (cache and context retrieval cross-harness durability) — private monorepo planning artifact.
- implementation note — root `Makefile` `check-disk-space`; `task-finish` `cache_pruned` event
- implementation note — embeddings env via MCP `env` blocks (`semantic-reinjection.md`)
