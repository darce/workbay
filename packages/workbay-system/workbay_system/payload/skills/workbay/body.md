# WorkBay harness control

## Overview

Use this skill when the operator wants to inspect or change WorkBay install-time settings from inside a harness session. The first supported control is the **semantic embeddings gate** (SSOT: `WORKBAY_HANDOFF_EMBEDDINGS_DISABLED` in `.workbay/embedding.env`, managed by `workbay embeddings` from the worktree root).

## Trigger

Use this skill when:

- the operator invokes `/workbay` or asks to enable, disable, or check semantic embeddings
- embeddings should be toggled without hand-editing env files
- install consent already ran but post-install control is needed

Do not use it for embedding model provisioning (`workbay-bootstrap provision-embeddings`), full install/repair, or compaction toggles.

## Goal

Apply one embeddings choice through the SSOT CLI and report the resulting state clearly.

## Canonical policy

- [../../../docs/workbay/instructions.md](../../../docs/workbay/instructions.md)
- `packages/workbay-bootstrap/README.md` — `embeddings` verb and install consent
- SSOT gate: `workbay embeddings --status|--enable|--disable` (cwd = workspace root; no `--target`)
- Deprecated alias: `workbay-bootstrap embeddings --target <workspace> …`
- Semantic recall / reinjection: honor install-time `embeddings_mode` and packet `semantic_degrade_reason` per [semantic-capability](https://github.com/darce/heuristics-canon); runtime `workbay embeddings` toggles do not rewrite the ledger.
- PreToolUse / Bash guard contract: harness hooks are registered via `.github/hooks/terminal-guard.json` (targeted guards such as worktree-drift, main-branch, and MCP write validators — not a broad shell allowlist). When a tool call is blocked, read the guard stderr, match it to [lifecycle-recovery](../../../docs/workbay/rules/lifecycle-recovery.md) for the named surface, and take the documented escape hatch; do not disable hooks. Telemetry for blocked Bash may land in `terminal_guard_events` (dormant rows are expected and are not orientation input).

## Capability branch (harness)

### Claude Code (interactive menu)

When `AskUserQuestion` is available, **do not** require positional args. Present a short menu:

1. **Embeddings status** — run status and summarize `enabled` / `disabled` / `source`
2. **Turn embeddings on** — `workbay embeddings --enable` (cwd = workspace)
3. **Turn embeddings off** — `workbay embeddings --disable`

Use `AskUserQuestion` with those options before running shell commands.

### Codex, Cursor, Grok, and other harnesses (positional)

Parse the slash tail: `/workbay embeddings <on|off|status>`.

| Positional action | CLI delegation |
| --- | --- |
| `status` | `workbay embeddings --status` (cwd = workspace) |
| `on` | `workbay embeddings --enable` |
| `off` | `workbay embeddings --disable` |

Resolve workspace as the consumer repo root (git top-level or the directory containing `.workbay-bootstrap.json`) and **cd there** (or set cwd) before invoking `workbay`. Emit JSON from `--status` verbatim when the operator asked for status.

## Core process

1. Confirm the workspace root that owns `.workbay/embedding.env` (or explain that bootstrap install has not run).
2. Branch per **Capability branch** above.
3. Run the delegated `workbay embeddings` command once per operator choice (commands are idempotent).
4. Summarize the outcome: enabled vs disabled, `source` from status when relevant, and whether semantic reinjection will honor the gate.

## Common rationalizations

| Rationalization | Why it fails | Required action |
| --- | --- | --- |
| "I'll set `WORKBAY_HANDOFF_EMBEDDINGS_DISABLED` in settings.local.json only." | Bypasses the SSOT file hooks load; state drifts across harnesses. | Use `workbay embeddings` from the workspace root. |
| "I'll skip the menu and guess on Claude Code." | Violates the consent UX for interactive harnesses. | Use `AskUserQuestion` first. |
| "A PreToolUse block means I should turn the guard off." | Guards protect branch isolation and write integrity; empty `terminal_guard_events` is not evidence that validation is inactive. | Follow lifecycle-recovery for the named surface. |
