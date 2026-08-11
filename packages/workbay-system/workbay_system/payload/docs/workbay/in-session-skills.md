# In-session skills

WorkBay ships skills beyond the twelve portable `/commands` (including `/refactor`). The skills
below are callable by name inside a session — ask the agent to "use the
`<name>` skill" — but are intentionally not wired to a portable command,
because they are situational operator tooling rather than the everyday
lifecycle. They exist and are installed; this index makes them findable.

| Skill | Use when |
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

None of these is dead code; routing nothing to them is deliberate, since they
are heavier or narrower than the lifecycle commands. To promote one to a
portable `/command`, add it to `config/agent-workflows/portable_commands.json`
and regenerate the agent surfaces.
