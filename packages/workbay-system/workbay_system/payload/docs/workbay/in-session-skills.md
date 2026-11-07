# In-session skills

WorkBay has fifteen live public slash commands, all in the `/wb-*` namespace.
The source of truth is
[`portable_commands.json`](../../config/agent-workflows/portable_commands.json):
`command_id` names the public command; `skill` selects its internal source
slug. An internal slug is a source and override-composition identity, not a
second public slash-command alias.

| Public command | Internal source skill slug |
| --- | --- |
| `/wb-scope` | `scope` |
| `/wb-refactor` | `refactor` |
| `/wb-auto-fix` | `auto-fix` |
| `/wb-land` | `branch-lifecycle` |
| `/wb-review-code` | `branch-review` |
| `/wb-handoff` | `handoff-lifecycle` |
| `/wb-investigate` | `investigate` |
| `/wb-implement` | `incremental-implementation` |
| `/wb-draft` | `plan-draft` |
| `/wb-review-plan` | `planning-review` |
| `/wb-review-slice` | `review-parallel` |
| `/wb-tdd` | `tdd` |
| `/wb-offload` | `offload` |
| `/wb-harness` | `workbay` |
| `/wb-ux-map` | `ux-map` |

Canonical cross-harness guidance lives in
[`skills/<slug>/body.md` and `skill.yaml`](../../skills/).
Keep override composition keyed by source identity; public discovery must
project the manifest's `command_id`. Harnesses materialize the same canonical
guidance. Generated `SKILL.md`, command adapters, prompts, and root routers
are derived outputs; update canonical inputs and regenerate rather than
authoring separate vendor bodies or editing generated copies.

Legacy unprefixed command ids remain fail-closed discovery metadata in the
manifest. Their `wb-tombstone` handler announces the named `replaces` command
and stops; a legacy redirect does not execute the replacement automatically.
For example, live `/wb-draft` selects the canonical source slug `plan-draft`.
Historical adapter copies and
source-slug discovery surfaces do not establish another public namespace;
managed reconciliation and regeneration own their migration.

The additional source skills below can be requested by name inside a session
("use the `<slug>` skill") when available in the installed surface. They are
situational tooling or shared checklists and have no public slash-command
entry in the manifest.

| Internal source skill slug | Use when |
| --- | --- |
| `commit2git` | splitting uncommitted changes into clean per-slice commits |
| `subfeature-committer` | committing one already-isolated finished slice |
| `daemon-lifecycle` | starting, stopping, or inspecting orchestrator and worker daemons |
| `worktree-orchestrator` | splitting a task across worktree lanes |
| `worktree-worker` | executing a delegated slice inside a worktree lane |
| `rescue-lane` | recovering a broken or blocked lane branch |
| `document-sync` | bringing repo docs back in line with current code and workflow |
| `spec` | drafting or revising a repository spec after scoping |
| `security-audit` | running a structured security audit over the monorepo |
| `review` | the shared review checklist behind branch and plan review |

To expose an additional skill as a public command, add a live `wb-*`
`command_id` pointing to its source slug in the manifest, then regenerate
the managed surfaces. Keep any retired ids as tombstones with explicit
replacements.

Routing choices come from the live backend registry and capability/preflight
receipts, including resolved model, effort, and timeout bounds. `/wb-offload`
requires an explicit agent and preserves capability and optional-dependency
refusals. The source index is not a backend or model inventory.

Resolve guidance through the
[heuristics canon pointer](rules/heuristics-canon.md) and its pinned,
digest-verified release. Follow its named artifact route, fired-row and
reasoning-card verification, and source citation contracts. Apply relevant
stable rule ids at use time:
DBG-01/DBG-02 for reproduction and observed behavior (Debugging: The 9
Indispensable Rules; Modern Software Engineering, ch. 7), GRPH-31 for
dependency scheduling within measured capacity (The Algorithm Design Manual,
ch. 17; Scheduling: Theory, Algorithms, and Systems, ch. 5 and section D.4),
and GRPH-32/GRPH-33 for real input edges and explicit lane contracts
(unsourced practice labels). Independent groups need no global serial chain.
Behavior changes require relevant RED evidence before dependent implementation;
documentation protocol corrections use existing materialization and contracts.
