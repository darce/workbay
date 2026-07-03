# WorkBay harness control

## Overview

Use this skill when the operator wants to inspect or change WorkBay install-time settings from inside a harness session. The first supported control is the **semantic embeddings gate** (SSOT: `WORKBAY_HANDOFF_EMBEDDINGS_DISABLED` in `.workbay/embedding.env`, managed by `workbay-bootstrap embeddings`).

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
- SSOT gate: `workbay-bootstrap embeddings --status|--enable|--disable --target <workspace>`

## Capability branch (harness)

### Claude Code (interactive menu)

When `AskUserQuestion` is available, **do not** require positional args. Present a short menu:

1. **Embeddings status** — run status and summarize `enabled` / `disabled` / `source`
2. **Turn embeddings on** — `workbay-bootstrap embeddings --enable`
3. **Turn embeddings off** — `workbay-bootstrap embeddings --disable`

Use `AskUserQuestion` with those options before running shell commands.

### Codex, Cursor, Grok, and other harnesses (positional)

Parse the slash tail: `/workbay embeddings <on|off|status>`.

| Positional action | CLI delegation |
| --- | --- |
| `status` | `workbay-bootstrap embeddings --status --target <workspace>` |
| `on` | `workbay-bootstrap embeddings --enable --target <workspace>` |
| `off` | `workbay-bootstrap embeddings --disable --target <workspace>` |

Resolve `<workspace>` from the consumer repo root (git top-level or the directory containing `.workbay-bootstrap.json`). Emit JSON from `--status` verbatim when the operator asked for status.

## Core process

1. Confirm the workspace root that owns `.workbay/embedding.env` (or explain that bootstrap install has not run).
2. Branch per **Capability branch** above.
3. Run the delegated `workbay-bootstrap embeddings` command once per operator choice (commands are idempotent).
4. Summarize the outcome: enabled vs disabled, `source` from status when relevant, and whether semantic reinjection will honor the gate.

## Common rationalizations

| Rationalization | Why it fails | Required action |
| --- | --- | --- |
| "I'll set `WORKBAY_HANDOFF_EMBEDDINGS_DISABLED` in settings.local.json only." | Bypasses the SSOT file hooks load; state drifts across harnesses. | Use `workbay-bootstrap embeddings` against the workspace target. |
| "I'll skip the menu and guess on Claude Code." | Violates the consent UX for interactive harnesses. | Use `AskUserQuestion` first. |
