# Document Sync

## Overview

Use this skill to update project documentation after code changes have been committed, ensuring docs stay in sync with the current state of the codebase.

## Trigger

Use this skill when the request matches any of:

- "update docs", "sync docs", "refresh documentation"
- "docs are stale", "update README", "update instructions"
- After a multi-commit session when the user asks to "clean up" or "finalize"
- When a review finding identifies stale documentation

Do **not** use this skill for:
- Writing new task plans or epics (those are authored directly).
- Review work (use the `review` skill instead).
- Commit work (use `commit2git` or `subfeature-committer` instead).

## Goal

Identify documentation that has drifted from the codebase and apply factual corrections. Record the sync as an MCP decision so the update survives handoff.

## Canonical Policy

- Use [../../instructions.md](../../instructions.md) for startup, handoff, and evidence-logging policy.
- Use [../../rules/development-workflow.md](../../rules/development-workflow.md) for slice and documentation conventions.
- Use this skill for doc-sync phasing only; broader process policy lives in the linked canonical docs.

## Scope

Documentation surfaces in this monorepo, ordered by sync priority:

1. **Contracts** (`docs/workbay/contracts/`) — MCP tool signatures, parameter shapes, return types
2. **Context maps** (`docs/workbay/maps/`) — tech stack, file paths, module boundaries
3. **CLAUDE.md** — role table, naming conventions, short rules, key triggers
4. **Instructions** (`docs/workbay/instructions.md`) — startup protocol, handoff contract
5. **Rules** (`docs/workbay/rules/`) — review guides, development workflow, guidelines
6. **Playbooks** (`docs/workbay/playbooks/`) — operating procedures
7. **Skills** (`docs/workbay/skills/`) — skill triggers and phase descriptions
8. **Templates** (`docs/workbay/templates/`) — structural templates
9. **Package READMEs** (`packages/*/README.md`) — installation, usage, development
10. **Task plans** (`docs/tasks/`) — only update status/completion markers, never rewrite scope

Out of scope: `docs/literature/`, `docs/roadmaps/` (authored, not synced), app-level READMEs unless explicitly requested.

## Core Process

1. Start from the actual code or branch diff, not memory of what changed.
2. Audit the highest-priority documentation surfaces first.
3. Apply only factual sync updates; do not smuggle in new scope.
4. Record the doc-sync outcome durably when the update matters for handoff continuity.

## Phase 1 — Diff Analysis

Determine what changed since the docs were last in sync.

```bash
# What changed on this branch
git diff --name-only main...HEAD
git diff --stat main...HEAD
git log --oneline main..HEAD

# Or since a specific commit if the user specifies one
git diff --name-only <ref>...HEAD
```

Classify changes into:
- **New modules/packages**: need new contract entries, map updates, README sections
- **Renamed/moved files**: need path updates across all docs
- **Changed APIs/tool signatures**: need contract and map updates
- **Removed modules**: need reference cleanup
- **Changed behavior**: need rule/guideline/playbook updates

## Phase 2 — Per-Surface Audit

For each documentation surface (in priority order), read the current doc and cross-reference against the diff.

### Audit checklist per surface

**Contracts**: Do tool signatures match the actual API? Are parameter names, types, and descriptions accurate? Are new tools documented? Are removed tools cleaned up?

**Context maps**: Do file paths match the actual directory structure? Are module boundaries accurate? Is the tech stack description current?

**CLAUDE.md**: Does the role table reference correct file paths? Are naming conventions current? Do short rules reference existing code patterns? Are key triggers accurate?

**Instructions**: Does the startup protocol reference available MCP tools? Are handoff contract steps accurate?

**Rules**: Do review guides reference existing code patterns and tools? Are regression guards still applicable?

**Package READMEs**: Do installation instructions work? Are development commands accurate? Do usage examples reflect current API?

### Classification

For each drift found, classify:

- **Auto-update**: factual correction derivable from the diff (file path changed, tool renamed, parameter added). Apply directly.
- **Ask user**: narrative change, section removal, or semantic rewrite that requires judgment. Present the drift and ask before changing.

## Phase 3 — Apply Auto-Updates

Apply factual corrections using Edit. One file at a time, one logical change per edit.

For each update, note what changed:

```
<file>: <one-line description of what was updated>
```

**Never auto-update**:
- Project philosophy or design rationale sections
- Security model descriptions (ask first)
- Entire section removals (ask first)
- Task plan scope or acceptance criteria (those are authored, not synced)

## Phase 4 — Cross-Doc Consistency Check

After individual updates, verify cross-document references:

- Do contract tool lists match the actual `api.py` exports?
- Do map file paths match `ls` output?
- Do CLAUDE.md role table links resolve?
- Do skill triggers in CLAUDE.md match skill SKILL.md triggers?
- Do rule file references in instructions.md point to existing files?

Fix any broken cross-references found.

## Phase 5 — Record and Close

1. Record the sync decision:

```
record_event(event={
  "event_kind": "decision",
  "session": "<session-id>",
  "decision": "document_sync_<date>_<slug>",
  "rationale": "## Changes\n<list of files updated with one-line summaries>\n\n## Verification\n- Cross-doc consistency check: pass\n- Broken references fixed: <count>\n\n## Schema / Contract Changes\n<contract updates if any, else '- none.'>\n\n## Open Threads\n<remaining stale docs if any, else '- none.'>"
})
```

2. If any docs are still stale but require user input, record them as findings:

```
review_findings(review={
  "operation": "record",
  "session": "<session-id>",
  "finding_id": "DOCSYNC-<n>",
  "severity": "low",
  "file_path": "<stale-doc-path>",
  "description": "Doc drift detected but requires user judgment: <description>.",
  "review_mode": "planning"
})
```

3. Refresh the operator view with `render_handoff(kind='dashboard')`; render `CURRENT_TASK.json` only for an explicit legacy export request.

## Response Format

Present a doc health summary:

```
Documentation sync complete:
  contracts/workbay-handoff-mcp.md  [Updated] (added new_tool signature)
  maps/tech-stack.md              [Current] (no drift)
  CLAUDE.md                       [Updated] (fixed role table path)
  ...

Updated: <count> files
Current: <count> files
Needs user input: <count> items
```

End with `Handoff updated: yes`.

## Common Rationalizations

- "This doc drift is small, so it can wait." Small documentation mismatches compound quickly across instructions, skills, and contracts.
- "I already know what changed." Diff-based verification is safer than relying on session memory.
- "Task plans should be rewritten while I am here." Task plans only get status and factual-sync updates unless the user explicitly asks for scope changes.

## Red Flags

- The requested "sync" would actually change scope or design intent.
- A contract or instruction surface disagrees with the implementation and the discrepancy was not verified from code.
- The update spans multiple documentation layers but only one was checked.

## Recovery

- If MCP is unavailable, apply doc updates directly and record the decision when MCP returns.
- If a referenced file no longer exists, remove or update the reference rather than leaving a broken link.
- If the diff is empty (no code changes), check for doc-only drift by reading current docs against live file structure. This can still find stale paths from prior changes that were never synced.

## Convergence Criteria

- All auto-update corrections have been applied.
- Cross-doc consistency check passes (no broken references).
- A `record_event(event={"event_kind": "decision", ...})` entry exists with the sync summary using slice-complete template structure.
- Any deferred items are recorded as MCP findings.
- `DASHBOARD.txt` has been refreshed when the sync changed handoff-visible state.
- Response includes `Handoff updated: yes`.

## See Also

- [../review/SKILL.md](../review/SKILL.md)
- [../../rules/development-workflow.md](../../rules/development-workflow.md)
- [../../instructions.md](../../instructions.md)
