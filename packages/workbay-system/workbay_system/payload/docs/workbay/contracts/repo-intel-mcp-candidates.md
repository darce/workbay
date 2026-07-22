# Repo-Intel MCP Candidate Inventory

## Purpose

This document classifies the non-handoff MCP tools that previously existed in the legacy `unified_server.py` implementation (deleted in the internal followon cleanup). The portable [`workbay-handoff-mcp`](workbay-handoff-mcp.md) package does not expose these tools.

The goal is to make the boundary explicit:

- handoff state remains in the dedicated handoff server
- generic file/search/navigation helpers stay with the host harness
- only curated repo-intel workflows that materially beat generic tools should become future MCP servers

## Disposition

| Tool | Family | Disposition | Rationale |
| --- | --- | --- | --- |
| `get_context_map` | docs | drop | It reads a known markdown file by name. Native file-open and search tools already cover this cleanly. |
| `get_api_contract` | docs | drop | It is a thin wrapper around reading a contract file under `docs/workbay/contracts/`. No curated resolution logic justifies a server. |
| `get_instructions` | docs | drop | It returns one fixed file. Native read/open tools are sufficient. |
| `find_react_component` | react | drop | This is a convenience ripgrep wrapper. Native code search and definition lookup are better and more flexible. |
| `find_react_hook` | react | drop | Same overlap as `find_react_component`; not enough curated value for a separate MCP surface. |
| `list_frontend_tests` | react | drop | Generic file listing and search already solve this without a dedicated tool contract. |
| `find_framework_action` | php/framework | drop | This is a specialized grep over framework hook strings. Generic search is already the right primitive. |
| `find_framework_route` | php/framework | drop | Native grep/read tools already expose route registration sites without requiring MCP mediation. |
| `find_php_class` | php/framework | drop | Generic code navigation is sufficient. |
| `trace_api_endpoint` | cross-boundary repo intel | future_server | This is the only tool that composes multiple codebases and languages into one workflow, which may justify a curated `repo-intel` companion server later. |

## Follow-Up Shape

Do not create separate framework, React, or docs MCP servers from the current helper set. The existing helpers in those families are convenience wrappers, not durable contracts.

If a future MCP companion is justified, it should start as one narrow `repo-intel` server centered on cross-boundary workflows such as `trace_api_endpoint`, with a small contract validated by real usage first.

## Current Decision

- `workbay-handoff-mcp` remains handoff-only.
- The non-handoff helpers stay out of the packaged handoff server.
- No stub servers are created in this phase.
- `unified_server.py` was deleted in the internal followon cleanup. Any future repo-intel MCP must be re-derived from this disposition table, not from the deleted code.
