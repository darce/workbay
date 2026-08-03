# internal Compaction vs. Host-Harness Compaction

This explainer answers a recurring operator question: *Should I run internal's
compaction advisory if my harness (Claude Code, Codex, Cursor) already does
its own context compaction?* Short answer: **yes, both — they solve
different problems.** internal compaction is **not** a replacement for
host-harness compaction; it runs alongside it and produces an artifact the
host compaction does not.

This document also names what the existing
`tests/test_compression_ratio.py` benchmark actually measures, so the
number it prints is not mistaken for an in-conversation token-cost claim.

## TL;DR

| | Host-harness compaction | internal compaction |
|---|---|---|
| Where it runs | Inside the live model loop | Out-of-band, after the session ends (Stop hook) or on demand |
| What it consumes | The current in-memory message stream | The transcript file plus `handoff_state` rows for the task |
| What it produces | A collapsed conversation summary, re-injected as the next turn's context | A `StructuredSummary` row in `session_compactions` (queryable, durable) |
| When it fires | When the harness's context-pressure heuristic crosses an internal threshold | When the internal advisory's token/char thresholds are crossed (or on operator command) |
| Lifetime of the output | This conversation only | Durable across sessions; cold-start consumers dereference by `compaction_id` |
| Operator visibility | None inside the harness; you see only that the chat got summarized | `compaction(operation="get", compaction_id=...)`, `render_cold_start_compaction(...)`, dashboard `compaction_advisory` |

## Where each mechanism runs

- **Host-harness compaction** runs *inside* the model loop. Claude Code's
  `/compact` and its automatic context-window-pressure compaction, Codex's
  internal summarization passes, and Cursor's equivalent all read the
  in-memory conversation stream and rewrite it before the next turn. The
  harness itself decides when to fire — there is no MCP-side hook.
- **internal compaction** runs *outside* the model loop. The
  `compact-session` Stop hook fires when the user ends a turn (or session)
  and reads the harness transcript file from disk. The
  `compaction(operation="record", ...)` MCP op can also be invoked on
  demand from a CLI or operator script. Either path writes a
  `StructuredSummary` row into the `session_compactions` table.

## What each consumes

- **Host-harness compaction** consumes the live model context — the same
  bytes the model is currently looking at. It has the most accurate view
  of what the model has seen, but it has no access to the cross-session
  `handoff_state` rows that internal tracks (decisions, blockers, slice
  closures, review findings).
- **internal compaction** consumes the harness transcript file plus the
  `handoff_state` decisions/blockers/findings recorded during the
  session. That intentionally crosses the in-conversation boundary: the
  resulting `StructuredSummary` ties transcript turns back to the
  structured task state and is meant to survive the session.

## What each produces

- **Host-harness compaction** produces prose. The conversation is collapsed
  into a shorter conversation that re-enters the next turn's context. It
  exists only in the harness's memory; once the session ends, the prose
  goes with it.
- **internal compaction** produces a typed `StructuredSummary` (decisions,
  open threads, residual prose, plus the `compaction_id` operators can
  cite later). The summary lives in SQLite and is dereferenceable through
  `compaction(operation="get", compaction_id=...)` or the cold-start
  renderer `render_cold_start_compaction(task_ref=...)`. A fresh session
  on the same task can rehydrate the prior session's state from that row.

On a successful internal record, operator-facing surfaces keep
`compaction_id=<id>` as the first receipt line and then expose the existing
value fields as separate `key=value` lines: `tokens_saved_estimate`,
`input_chars`, `summary_chars`, and `prose_residual_chars`. These fields
describe the durable internal artifact; they are not a claim that the host
harness removed that many tokens from the live model window.

During the same live session, callers can opt into
`load_session(include_context_refresh=True)`. When a latest compaction exists,
the response includes a bounded `context_refresh` packet rendered from
`render_cold_start_compaction(...)` with policy
`supersedes_prior_session_detail` and a `dedupe_key`. Pass that key back as
`last_injected_compaction_id` to avoid re-injecting the same packet. This is a
soft grounding aid, not harness-native context replacement.

## When each fires

- **Host-harness compaction** fires on the harness's internal
  context-pressure heuristic. Operators can sometimes prod it (e.g.
  `/compact`) but cannot inspect the threshold or override it from
  outside the harness.
- **internal compaction** fires when the advisory's token or char thresholds
  are crossed (see `packages/workbay-system/docs/workbay/contracts/harness-protocol.yaml`
  — defaults 70,000 tokens / 280,000 chars after internal). The
  thresholds are operator-tunable per-deployment (internal overlay/env
  overrides). The advisory can also be silenced per-task or
  workspace-wide via `compaction(operation="disable", ...)` /
  `AGENT_HANDOFF_COMPACTION_DISABLED=1`.

## What the compression-ratio benchmark actually measures

The benchmark at
[`packages/mcp-workbay-handoff/tests/test_compression_ratio.py`](../../tests/test_compression_ratio.py)
prints a "reduction" percentage. It is easy to read that number as "internal
compaction saves X% of in-conversation tokens." **That is not what the
benchmark measures.**

The benchmark compares:

- The token count of the **prose handoff** that a cold-start session
  would otherwise need to ingest (the slice-complete rationale, decisions,
  open threads — rendered as plain prose), versus
- The token count of the **structured cold-start summary** (the same
  information rendered through the `StructuredSummary` schema plus the
  ID-only `cold_start_compaction` block).

So the metric is **cold-start retained context efficiency** — how much
smaller the cross-session rehydration payload is when it goes through the
structured pathway instead of being inlined as prose. It says nothing
about how much the host harness's in-conversation compaction saves
(positive or negative) on the *active* session. The two mechanisms
target different cost centers and the benchmark only measures one of
them. The floor in the benchmark assertions (≥50% per sample, ≥50% mean)
is a regression guard on that cold-start ratio, not a feature claim.

## Recommendation

Enable both:

1. Leave host-harness compaction at its defaults. It is the only mechanism
   that can keep an actively running session under the context window.
2. Leave the internal advisory enabled. It fires earlier (at the configured
   threshold, not at hard context pressure), surfaces the
   recommendation in `DASHBOARD.txt` and `get_handoff_state`, and on
   record produces a `StructuredSummary` row your next session can
   actually query.

If you need to silence the internal advisory in a specific environment, see
the README's *Disabling internal compaction* subsection — but understand it
silences only the internal advisory and Stop-hook write. The host
harness's own compaction continues to run.

## Related surfaces

- `compaction(operation="record"|"get"|"get_latest")` — record a
  `StructuredSummary` or fetch an existing one.
- `compaction(operation="disable"|"enable"|"status")` — runtime-disable
  surface; see CHANGELOG entry "Unified compaction runtime-disable
  surface (internal)".
- `make compaction-{disable,enable,status} [TASK=<ref>]` — operator
  wrappers.
- `render_cold_start_compaction(task_ref=...)` — the ID-only block a
  fresh session can read to rehydrate prior context.
- `load_session(include_context_refresh=True, last_injected_compaction_id=...)`
  — opt-in same-session refresh packet with caller-managed dedupe.
- [`tests/test_compression_ratio.py`](../../tests/test_compression_ratio.py)
  — the cold-start ratio benchmark.
- [`packages/workbay-system/docs/workbay/contracts/harness-protocol.yaml`](../../../workbay-system/docs/workbay/contracts/harness-protocol.yaml)
  — canonical thresholds (`compaction.thresholds.{tokens,chars}`).
