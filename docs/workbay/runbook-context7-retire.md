# Runbook: context7 MCP retire (or hardened keep)

Operator playbook for removing the `@upstash/context7-mcp` server from every
harness launch surface — or, if a real external-library workflow exists,
hardening it so it no longer orphans `npx` workers or pins cache to a removable
volume.

This runbook implements internal. It complements
[runbook-host-cache-durability.md](runbook-host-cache-durability.md) (implementation note):
context7's `npx @upstash/context7-mcp@2.1.4` launch pattern was a live instance
of the orphan + removable-cache failure class (18 stray procs, 9 spinning at
67–99% CPU for days).

**Default path:** retire. Only choose **hardened keep** when you have a
documented external-library workflow that actually uses
`resolve-library-id → get-library-docs` follow-through.

**Stop it at the source.** context7 has no daemon and is not "installed" — each
worker is spawned on demand by `npx -y @upstash/context7-mcp@…` *only* because a
harness MCP config lists it. Remove every registration stanza and nothing can
respawn it. The reaper LaunchAgent (Step 3) is **not** part of the default path:
it is an hourly `pkill` that papers over an undiscovered registration instead of
removing it. Install it only if you genuinely cannot locate a respawn source,
and delete it the moment you do.

**Scope:** host-level MCP configs and a macOS LaunchAgent the operator installs
manually. The `workbay-bootstrap` installer does **not** manage context7.

**Sibling:** [runbook-mcp-server-launch.md](runbook-mcp-server-launch.md) (WorkBay
MCP launch surfaces).

---

## Problem (recap)

context7 is launched via `npx -y @upstash/context7-mcp@…` from harness MCP
configs. Two failure modes:

1. **Orphan processes** — the `npx` parent is not reaped when a harness exits;
   workers accumulate across sessions.
2. **Removable-cache spin** — when the npm/npx cache lives on an unmounted
   volume, orphaned workers busy-loop at high CPU.

Usage in this monorepo was low (~1.3% `get-library-docs` follow-through on
`resolve-library-id` calls) and went unnoticed for days — default retire is
appropriate unless you rely on external library API docs daily.

---

## Step 1 — Discover every registration site

Run this **before** editing anything. Record every hit in your slice receipt.

```bash
grep -rn context7 \
  ~/.codex/config.toml \
  ~/.grok/config.toml \
  ~/.claude.json \
  ~/.mcp.json \
  ~/.cursor/mcp.json \
  "$HOME/Library/Application Support/cmux" \
  2>/dev/null
```

Also check live processes and the less-obvious spawn surfaces (the source the
original incident could not immediately locate hid here):

```bash
pgrep -fl context7-mcp || echo "no context7-mcp processes"
# Claude desktop app config + any scheduled spawner that survives restarts:
grep -l context7 "$HOME/Library/Application Support/Claude/claude_desktop_config.json" 2>/dev/null
grep -rIl -E "context7|upstash" ~/Library/LaunchAgents /Library/LaunchAgents /Library/LaunchDaemons 2>/dev/null
crontab -l 2>/dev/null | grep context7
```

Distinguish **registrations** (an MCP `command/args` stanza, a LaunchAgent, a
cron line — these spawn the server) from **mentions** (transcripts, logs, search
or retrieval indexes under `cmux`/`Cursor` — inert text that never launches
anything). Only registrations need removal.

**Example discovery receipt** (yours will differ):

| Site | Registered? | Notes |
| --- | --- | --- |
| Codex `~/.codex/config.toml` | often YES | `[mcp_servers.context7]` stanza |
| Grok `~/.grok/config.toml` | usually NO | plugins-only config |
| Claude `~/.claude.json` | varies | search `mcpServers` |
| Cursor `~/.cursor/mcp.json` | varies | generated or hand-edited |
| Repo `.mcp.json` | usually NO | gitignored; regenerate via bootstrap |
| cmux | varies | under Application Support |

Do not remove stanzas until every real site is listed.

---

## Step 2 — Retire (default path)

For each site that registered context7:

### Codex

Edit `~/.codex/config.toml` and **delete** the entire block:

```toml
[mcp_servers.context7]
command = "npx"
args = ["-y", "@upstash/context7-mcp@2.1.4"]
```

Restart Codex (quit and reopen the app / CLI session).

### Claude Code

If `~/.claude.json` or the repo `.mcp.json` contains a `context7` entry under
`mcpServers`, remove that object. Prefer regenerating repo `.mcp.json` via
`workbay-bootstrap` rather than hand-editing when WorkBay servers are
involved — see [runbook-mcp-server-launch.md](runbook-mcp-server-launch.md).

Restart Claude Code.

### Cursor

If `~/.cursor/mcp.json` lists context7, remove the entry. Restart Cursor.

### Grok

If `~/.grok/config.toml` ever gains a context7 stanza, remove it and restart
Grok.

### Verify retire (across several restart cycles)

A deregistered context7 has no respawn source, so the real test is that it
stays gone across **multiple** full harness restarts — not a single check
immediately after one quit. After removing every stanza:

```bash
# Repeat after restarting each harness, ideally over a few sessions/reboots:
pgrep -fl context7-mcp || echo "OK: no context7-mcp processes"
```

Expected: empty every time. **If a worker ever reappears after a clean restart,
you missed a registration site — return to Step 1 and find it.** A reappearance
is positive evidence of an un-removed source, not a reason to install a killer.
Run `pkill -9 -f context7-mcp` once to clear the current orphans, then keep
hunting the source. Only fall through to Step 3 if you have genuinely exhausted
discovery and still cannot locate what is spawning it.

---

## Step 3 (fallback only) — Reaper LaunchAgent

> **Skip this in the normal case.** If Step 2 verification stays empty across
> restarts, context7 is fully retired and there is nothing for a reaper to do —
> an hourly `pkill` that never fires is dead weight that hides the fact that you
> never proved the source was gone. Install it **only** when workers keep
> reappearing after restarts and you have exhausted Step 1 discovery without
> finding the registration. It is a stopgap to buy time while you keep hunting
> the source — **not** the end state. Once you locate and remove the source
> (verified by Step 2), uninstall the reaper (see "To uninstall" below).

This user LaunchAgent sweeps orphan `context7-mcp` workers hourly. Model on
Homebrew's `~/Library/LaunchAgents/homebrew.mxcl.*.plist` pattern.

Create `~/Library/LaunchAgents/com.<your-user>.context7-mcp-reaper.plist`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
	<key>Label</key>
	<string>com.<your-user>.context7-mcp-reaper</string>
	<key>ProgramArguments</key>
	<array>
		<string>/usr/bin/pkill</string>
		<string>-9</string>
		<string>-f</string>
		<string>context7-mcp</string>
	</array>
	<key>RunAtLoad</key>
	<true/>
	<key>StartInterval</key>
	<integer>3600</integer>
	<key>StandardOutPath</key>
	<string>/dev/null</string>
	<key>StandardErrorPath</key>
	<string>/dev/null</string>
</dict>
</plist>
```

Load it (macOS 13+):

```bash
launchctl bootstrap "gui/$(id -u)" \
  ~/Library/LaunchAgents/com.<your-user>.context7-mcp-reaper.plist
```

Older macOS:

```bash
launchctl load ~/Library/LaunchAgents/com.<your-user>.context7-mcp-reaper.plist
```

Verify:

```bash
launchctl list | grep context7
pgrep -fl context7-mcp || echo "OK: no context7-mcp processes"
```

Expected: the reaper label appears in `launchctl list`; `pgrep` stays empty
under normal operation (the reaper only acts when orphans exist).

To uninstall (do this as soon as the source is located and removed — leaving a
permanent reaper running is the anti-pattern this step exists to avoid):

```bash
launchctl bootout "gui/$(id -u)/com.<your-user>.context7-mcp-reaper"
# or, by path / on older macOS:
#   launchctl bootout "gui/$(id -u)" ~/Library/LaunchAgents/com.<your-user>.context7-mcp-reaper.plist
#   launchctl unload  ~/Library/LaunchAgents/com.<your-user>.context7-mcp-reaper.plist
rm ~/Library/LaunchAgents/com.<your-user>.context7-mcp-reaper.plist
launchctl list | grep context7 || echo "OK: reaper removed"
```

---

## Alternative — hardened keep (only with a real workflow)

Choose this path **only** when external library API docs are load-bearing. All
four conditions must hold:

1. **Drop `npx`** — install context7 locally and pin an exact version in the
   harness command (same pattern as other pinned MCP servers).
2. **Cache off removable volume** — apply
   [runbook-host-cache-durability.md](runbook-host-cache-durability.md) so npm
   cache redirection survives non-login processes and unmount fallback is guarded.
3. **Teach follow-through** — add a short skill or rule that the workflow is
   `resolve-library-id` → `get-library-docs`, not resolve-only.
4. **Monitor** — `pgrep -fl context7-mcp` after each harness session; orphans
   mean the launch pattern still leaks.

Without all four, retire (Step 2) is safer.

---

## Harness coverage

This runbook is harness-neutral. Apply discovery and retire steps on every
surface you use:

| Harness | Config surface |
| --- | --- |
| Codex | `~/.codex/config.toml` |
| Claude Code | `~/.claude.json`, repo `.mcp.json` |
| Cursor | `~/.cursor/mcp.json` |
| Grok | `~/.grok/config.toml` |

---

## Related

- internal — context7 retire or hardened keep
- [runbook-host-cache-durability.md](runbook-host-cache-durability.md) — npm/npx cache delivery (implementation note)
- [runbook-mcp-server-launch.md](runbook-mcp-server-launch.md) — WorkBay MCP launch & shim
