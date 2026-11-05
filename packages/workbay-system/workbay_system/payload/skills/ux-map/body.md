# UX Map

## Overview

Use this skill to maintain a **text-first UX map** (`.uxmap.json`) as the single
source of truth for screens, zones, states, flows, and actions. Render ASCII /
Mermaid / Markdown for humans and agents; run the heuristic critique pack before
decomposing UI tasks; optionally project one flow onto a FREEFORM canvas for
spatial review. The canvas is **not** the design SSOT — maps live on the filesystem.

## Trigger

Use this skill when:

- the operator invokes `/wb-ux-map` (portable command → this skill)
- decomposing Workbench / Roster / SPA UI work into screens, overlays, exits, and flows
- drafting or updating a `.uxmap.json` (package fixture or consumer `docs/ux-maps/`)
- planning needs zone/state inventory or job-entry primary actions
- checking IA against the curated rule pack (`RLSE-04`, `NAV-11`, `HAI-01`, …)
- reviewing a consumer repo's design inventory: tokens, components and assets

Reading tokens, components and assets in the read-only Library is in scope.
Editing tokens or production CSS/TSX through Design Canvas is out of scope, as
are freeform drag layout, screenshot annotation and REST sequence diagrams
(those stay in existing Mermaid data-flow docs).

## Goal

Produce a validated map, a critique report agents can cite in task plans, and
optional deterministic renders — so UI slices start from inventory, not invented IA.

## Canonical Policy

- Package guide: the Design Canvas package `docs/ux-map.md`
- Golden fixtures: `workbench-operator-loop.uxmap.json`, `roster-people.uxmap.json`
- Heuristics: follow the [heuristics canon pointer](../../../docs/workbay/rules/heuristics-canon.md) (one pinned, digest-verified release). Route this skill's artifact as **ui or frontend change** (the critique step as **usability evaluation or metric**), keep only the rows whose trigger fires, apply the Verification of every card that lists a kept ID, and cite `[FAM-NN]` with the rule's bold name and Source cell. `B` blocks, `S` needs a named exemption, and a `J` rule you lack context for is escalated, not skipped.
- Flow projection is FREEFORM-only, one-way, never reverse-sync; the Library is read-only.

This skill owns the map authoring / critique / render workflow. Task plans and
branch work own the code changes those maps inform.

## Core Process

1. **Locate the map SSOT**
   - WorkBay golden fixture (tool behavior):  
     the Design Canvas fixture `workbench-operator-loop.uxmap.json`
   - Consumer product map (dogfood):  
     `<consumer-root>/<maps-dir>/*.uxmap.json`
   - Set `--workspace-root` to the consumer root (`$CONSUMER_ROOT` for dogfood)
     and `--maps-dir` to its maps directory. The default is `docs/ux-maps`;
     pass the consumer's actual maps-dir explicitly for dogfood.

2. **Load / upsert** via CLI or MCP:
   - CLI (Design Canvas package): `ux-map render|critique|project --path …`
   - `map_ref` is the identifier inside the map, not a filesystem path.
   - MCP tools on Design Canvas MCP: `get_ux_map`, `upsert_ux_map`, `render_ux_map`,
     `critique_ux_map`, `project_ux_flow`
   - Template `user-flow` only wraps projection for `instantiate_template` symmetry.

3. **Author structure first** (sharpie-first): jobs → screens (`screen|overlay|exit`) →
   zones → states (`default|loading|empty|error|…`) → actions → flows.
   - `code_ref` is workspace-root-relative into the **consumer** tree.
   - Do not invent a confirm tab for Workbench (explicit not-doing).

4. **Render** Markdown (embeds ASCII + Mermaid) for PR/task-plan paste.
   - `--out-dir` writes the **Markdown bundle only** (`write_renders`); format
     selection applies to stdout.

5. **Critique** (advisory, exit 0): fix high/medium findings before planning UI slices.
   - Optional handoff emit: `critique_ux_map(..., task_ref=…, emit_to_handoff=True)` or
     CLI `ux-map critique --task-ref … --emit-handoff` batch-records findings under the
     active task via `review_findings` (stable `uxmap-…` finding ids; idempotent re-emit).
     For the CLI, pass `--workspace-root` for the consumer's handoff runtime;
     a failed emit returns a receipt and exit 1.
   - Record planning decisions with `record_event(event={"event_kind":"decision",…})`
     when durability matters beyond critique rows.

6. **Optional project**: `project_ux_flow` / `ux-map project --flow-id …` → canvas
   `uxmap:{map_ref}:flow:{flow_id}` (FREEFORM nodes/edges). Export via existing
   `export_canvas(..., mermaid)` if needed.

7. **Dogfood** (when `$CONSUMER_ROOT` is set): copy fixture → consumer maps dir,
   re-render, leave consumer git commit to the operator (separate monorepo).

8. Review the consumer's design sources when the repo has design assets.
   - Author `docs/design/design-sources.json` under the consumer root using its
     existing maps, CSS inputs, registered components and asset roots. Follow
     the Design Canvas README section "Use Canvas in another repository" for
     the schema, installation requirements and error recovery.
   - Launch `workbay canvas --workspace-root <consumer-root>`, select the map,
     choose Refresh, then read Map review and the Library. A review bundle is
     generated JSON combining the map, critique, source checks and inventory.
     Refresh writes review artifacts under `docs/design/generated/` by default.
   - Agents review headlessly with `ux-map bundle list|get|status|refresh
     --workspace-root <consumer-root>` (`--map-ref <id>` for get/status/refresh)
     and `ux-map inventory collect|check --workspace-root <consumer-root>`.
     These commands accept workspace-relative `--config` overrides. The collector
     is the Node script that reads declared sources. Node and collector dependencies are needed
     for Refresh and inventory collect/check, not viewing a generated bundle.
   - If the repo has no design assets, keep the text-first map CLI/MCP workflow
     and leave `design-sources.json` absent. [REF-12] **YAGNI / speculative-generality
     gate** (Refactoring: Improving the Design of Existing Code (2nd ed), ch. 3):
     avoid configuration for an anticipated case with no current requirement.

## Common Rationalizations

| Rationalization | Why it fails | Required action |
|---|---|---|
| "I'll invent screens in the task plan without a map." | Plans diverge from the real SPA; slices re-litigate IA. | Author or update `.uxmap.json` first. |
| "Canvas should become the map SSOT." | Dual writers violate DATA-14 and reverse-sync is undefined. | Keep map files SSOT; project is one-way. |
| "Critique can wait until after the PR." | Findings are cheapest before code; late critique rewrites UI mid-implementation. | Run `ux-map critique` before slice-start on UI work. |
| "Default maps-dir is fine for every consumer." | The default `<workspace>/docs/ux-maps` may differ from the consumer's map location. | Locate `<consumer-root>/<maps-dir>` and pass `--workspace-root` + `--maps-dir` explicitly for dogfood. |

## Red Flags

| Flag | Re-entry point |
|---|---|
| UI task plan with no screen/zone/state inventory | Step 3: open or create the map. |
| `code_ref` missing or not under consumer monorepo root | Step 3: set workspace-root-relative paths. |
| Critique empty pack or engine disabled | Do not ship; fail closed — fix package RULE_PACK. |
| Editing canvas-web SPA for layout mapping | Stop — out of MVP; use text map + optional FREEFORM project. |
| `design-sources.json` authored in a repo with no design assets | Step 8: leave speculative config absent; use the text-first map workflow. |
| Attempting to edit tokens or production CSS through Design Canvas | Step 8: read the Library; make source edits through the consumer's code workflow. |
| `config_invalid` with an empty pointer treated as a schema bug | Step 8: check that the file exists and is readable under the selected consumer root first; malformed JSON can also have an empty pointer. |

## Recovery

- Invalid map → fix JSON / Pydantic validation errors; re-load.
- Missing maps-dir → locate `<consumer-root>/<maps-dir>`; pass `--workspace-root`
  + `--maps-dir`, creating the consumer maps directory if needed.
- Consumer tree absent → keep WorkBay fixture tests green; skip dogfood gate.
- MCP canvas server not wired → use the Design Canvas `ux-map` CLI against the file path.

## Convergence Criteria

- Map loads and validates; golden fixture has ≥3 screens including ≥1 overlay and flows.
- Critique run completed (zero high findings preferred for planning handoff).
- Renders available for PR/task-plan paste.
- `code_ref`s are workspace-root-relative; resolve when `$CONSUMER_ROOT` is available.

## See Also

- Design Canvas package guide: `docs/ux-map.md`
- Skill `scope` (intake) before first map; `planning-review` when the map drives a task plan
