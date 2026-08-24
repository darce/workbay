# Runbook: MCP server launch & regenerating `.mcp.json`

Operator playbook for the WorkBay MCP servers (`workbay-handoff-mcp`,
`workbay-orchestrator-mcp`, and the opt-in `codebase-graph-mcp`) and how
to (re)generate the harness launch surfaces — primarily the Claude Code
`.mcp.json`.

`.mcp.json` is **gitignored and generated** by the `workbay-bootstrap`
installer. Never hand-edit it for a durable change; regenerate it with the
commands below so the managed-server ledger stays consistent.

---

## Symptom: a WorkBay MCP server's tools never appear

You open a harness session and the tools from one server are missing — e.g.
`mcp__workbay-handoff-mcp__*` never surface — even though `claude mcp list`
reports the server **✓ Connected**.

This is **not** a deprecation or naming problem (legacy `agent*-handoff-mcp`
names are fully purged; `workbay-*` is correct). `claude mcp list` runs a
fresh *warm* health probe, so it succeeds while the live session's tool
registry — built at session startup — is empty for that server.

### Root cause

Each harness cold-launches every MCP server as a stdio subprocess at session
start and registers its tools **only if the `initialize` → `tools/list`
handshake finishes inside the MCP startup deadline** (~30 s; see below). Two
launcher generations chased that deadline:

1. **`uv run --project … serve-stdio`** (original) re-resolved *and re-synced*
   the project env on every launch — rebuilding the editable `workbay-protocol`
   path dep whenever the installer rewrote `.workbay/remote` with fresh mtimes.
   That cold sync is ~10 s warm, 30–45 s cold, with the two servers contending on
   uv's global lock when the harness launches them together.
2. **`uv run --no-sync --project …`** (internal) removed the re-sync, but
   `uv run` still pays a per-invocation **project-resolution + import** cost every
   launch (warm ~3–7.7 s; the ~14.5 s cold figure implementation note first cited did **not**
   reproduce under a fresh uv *cache* — implementation note measured 7.7 s — so the
   deadline-breaking cold tail is driven by the cross-session herd, not a cold
   cache). Necessary, but **not sufficient**: under
   *N* concurrent sessions each cold-launching **two** `uv run` processes, the
   thundering herd drives whichever session loses the race past the deadline. The
   miss is therefore **intermittent and contention-driven**, not a config
   regression — a sequentially probed session (or `claude mcp list`) registers
   fine.

Confirm in the harness MCP logs:

```
~/Library/Caches/claude-cli-nodejs/<url-encoded-repo-path>/mcp-logs-workbay-handoff-mcp/*.jsonl
# look for: "Connection timeout triggered after NNNNNms (limit: 30000ms)"
```

### The fix (shipped for in-tree installs): the launcher shim

For in-tree / worktree installs, the generated launch command no longer calls
`uv run` directly. It points at a **stdlib-only launcher shim** (implementation note,
`scripts/hooks/mcp_launch.py`), emitted to every harness surface (`.mcp.json`,
`.vscode/mcp.json`, `.codex/config.toml`, `.cursor/mcp.json`; Grok reads the
root `.mcp.json`). The shim is wrapped in a cwd-independent `sh -c` launcher
(internal) so the server starts from any cwd inside the repo —
a linked worktree, a subdir session — not only the repo root
(`command` `sh`, the shim path resolved against the git repo root at launch,
the server id forwarded as the trailing `"$@"` positional):

```
sh -c 'root="$(git rev-parse --show-toplevel 2>/dev/null || echo .)"; exec python3 "$root/scripts/hooks/mcp_launch.py" "$@"' sh workbay-handoff-mcp
```

(The `git rev-parse` resolves the repo root from any cwd inside the repo; it
falls back to the launch cwd `.` for a non-git install target, mirroring the
shim's own root resolution.)

To run the shim by hand from the repo root you can still call it directly —
`python3 scripts/hooks/mcp_launch.py workbay-handoff-mcp`.

Per launch the shim (no workbay deps required — a bare `python3` runs it):

1. resolves the repo root via `git` (agent-agnostic; no vendor env var);
2. probes the deps-bearing console script for the server — per-package
   `packages/<pkg>/.venv/{bin/<console>,Scripts/<console>.exe}` then the shared
   root `.venv`, in the worktree and then the **primary** checkout (so a linked
   worktree without its own venv still heals);
3. if found, `execvp`s it directly — the **fast path**: no `uv run`
   project-resolution, so the launch carries no cold-resolution tail to balloon
   under the boot herd (the handshake itself is app-build-bound, ~7–9 s warm —
   see the deadline note below);
4. else `execvp`s `uv run --no-sync --project <pkg> <console> …` — the
   provisioning **fallback**, so no environment regresses to "won't start".

The per-package venvs are still **pre-built at install time**
(`_presync_local_mcp_envs`), so the fast path is ready on a fresh install. The
shim, the `mcp_servers.yaml` source, and every emitted surface are kept in sync
by the generator and the generated-surface coherence checks — never hand-edit
`.mcp.json`.

### Catch it early: `workbay-bootstrap doctor`

If a per-package venv/console script is missing or stale the shim still starts
the server (down the slow `uv run` fallback) — which is exactly the boot-miss
risk under contention. `doctor` flags it **before** it bites:

```bash
uv run --project packages/workbay-bootstrap workbay-bootstrap doctor --target .
# warning mcp_console_missing: packages/mcp-workbay-handoff/.venv/bin/mcp-workbay-handoff — shim fast-path console script is missing; MCP launch falls back to the slower uv run
```

A `mcp_console_missing` warning means re-run `install`/`update` (which presyncs
the per-package venv) before the next session boots.

### The MCP startup deadline (`MCP_TIMEOUT`)

The deadline is the wall-clock budget the harness allows for the stdio
`initialize` → `tools/list` handshake before it abandons the server and
registers **zero** of its tools. Claude Code's default is **~30 s**, overridable
with the `MCP_TIMEOUT` env var (see `docs/workbay/environment-variables.md`).

**Keep the default.** The shim removes `uv run`'s cold project-resolution tail —
the part that spikes past the deadline under the boot herd — not the intrinsic
app-build cost. The direct fast-path handshake is app-build-bound at ~7–9 s warm
(implementation note baseline: contended p≤8 max ≈ 12 s), clearing 30 s with wide margin, and
— unlike `uv run` — has no cold-resolution step to balloon under contention. (The
2.28 s figure implementation note first cited was a `uv run … --help` micro-bench with no
app build, not the handshake latency.) Raising `MCP_TIMEOUT` only masks a slow
launcher or a missing venv — fix the launch path (shim + presync) instead.

---

## Regenerating `.mcp.json` (push the launch fix live)

The launch command lives in the generated surface, so you regenerate the
surface and **restart the harness**. Pick the path that matches your situation.

### A. Fresh in-tree install — nothing to do

`workbay-bootstrap install --source worktree` already emits the shim command
and pre-builds the per-package venvs. New in-tree installs are correct out of
the box. Published/package installs keep their `uvx` launcher; clone-based
overlay installs keep the `uv run --no-sync` launcher because their projects live
under `.workbay/remote`, outside the shim's in-tree registry.

### B. Existing install — refresh (canonical)

`update` preserves the existing managed-server mapping, re-builds any local
venvs, and refreshes every surface. For a worktree-source install this rewrites
managed entries to the shim form; for clone/package installs it leaves the
appropriate non-shim launcher in place:

```bash
# from the consumer repo root; remote-ref defaults to the recorded manifest ref
uv run --project packages/workbay-bootstrap \
  workbay-bootstrap update --target . --remote-ref <ref>
```

Note: `update` re-fetches and re-checks-out the shared `.workbay/remote`
overlay. In a busy multi-session repo, prefer the surgical path (C) to avoid
disturbing other sessions' overlay reads.

### C. Config-only, overlay-safe (surgical)

Rewrites only the launch surfaces from an explicit managed map — no overlay
re-fetch, no `init-state`, no skill regeneration. Use this to push the shim into
an in-tree `.mcp.json` in place.

```bash
# 1. write the in-tree shim map (cwd-independent sh -c wrapper)
python3 - <<'PY'
import json
launch = 'root="$(git rev-parse --show-toplevel 2>/dev/null || echo .)"; exec python3 "$root/scripts/hooks/mcp_launch.py" "$@"'
def spec(server):
    return {"type": "stdio", "command": "sh", "args": ["-c", launch, "sh", server]}
servers = {
    "workbay-handoff-mcp": spec("workbay-handoff-mcp"),
    "workbay-orchestrator-mcp": spec("workbay-orchestrator-mcp"),
}
json.dump({"mcpServers": servers}, open("/tmp/ws_shim_mcp.json", "w"), indent=2)
PY

# 2. preview drift (read-only; exit 1 == would change)
uv run --project packages/workbay-bootstrap \
  workbay-bootstrap mcp-sync --target . \
  --mcp-servers /tmp/ws_shim_mcp.json --surfaces claude --check --json

# 3. apply
uv run --project packages/workbay-bootstrap \
  workbay-bootstrap mcp-sync --target . \
  --mcp-servers /tmp/ws_shim_mcp.json --surfaces claude --apply --json
```

`mcp-sync` preserves third-party servers and updates the managed-server ledger.
Add `vscode codex` to `--surfaces` to fix the VS Code / Codex launchers too.

### D. Optional codebase-graph MCP (opt-in)

`codebase-graph-mcp` is managed like the two WorkBay servers but **excluded
by default**. It launches `codebase-memory-mcp` from `PATH` via the shim's
`external-binary` resolution kind. Opt in at install:

```bash
workbay install --target . --with-codebase-graph
```

Refresh surfaces without a full install:

```bash
workbay mcp-sync --target . --with-codebase-graph --apply
```

Restart the harness after materializing. If the binary is absent at launch,
the shim exits with a clear message — install `codebase-memory-mcp` to
`~/.local/bin` (or another `PATH` directory) before expecting tools to
register.

### Then: restart the harness, and verify

Surfaces are read at startup, so **restart Claude Code** (and re-open Codex /
VS Code) to pick up the new launch command.

```bash
# launch command now starts with: sh -c 'exec python3 "$(git rev-parse …)/scripts/hooks/mcp_launch.py" "$@"' …
python3 -c "import json;s=json.load(open('.mcp.json'))['mcpServers'];[print(n,s[n]['args']) for n in s]"
```

---

## Gotchas

- **The shim's fast path needs a pre-built venv.** If `packages/<pkg>/.venv`
  (or `.workbay/remote/packages/<pkg>/.venv` for a clone-based install) is
  missing or stale, the shim silently drops to the slow `uv run` path — the
  boot-miss risk under contention. `workbay-bootstrap doctor` reports this as
  `mcp_console_missing`; re-run `install`/`update` to presync it, or as a
  one-off `uv sync --project packages/<pkg>`.
- **`claude mcp list` ✓ Connected is not proof the session registered the
  tools** — it is a separate warm probe. Trust the live session's tool list and
  the MCP logs.
- **Server health check (independent of the harness):** a direct stdio
  `tools/list` handshake against the server proves the server itself exposes
  tools, isolating client-side registration failures.
- **Do not raise `MCP_TIMEOUT`** as the fix — see above.

## See also

- `packages/workbay-system/workbay_system/payload/scripts/hooks/mcp_launch.py`
  — the launcher shim (`_console_path` probe order; `resolve_launch` fast path /
  `uv run` fallback).
- `packages/workbay-bootstrap/src/workbay_bootstrap/install.py` —
  `_LOCAL_MCP_SERVERS`, `_build_local_default_mcp_servers`,
  `_presync_local_mcp_envs`.
- `packages/workbay-bootstrap/src/workbay_bootstrap/subcommands.py` —
  `_doctor_local_mcp_console_scripts` (the `mcp_console_missing` check).
- implementation note (MCP stdio launch latency / boot miss) — private monorepo planning artifact.
- `docs/workbay/environment-variables.md`
- `docs/UPGRADING.md`
