# Scope

## Overview

Use this skill to resolve missing constraints for a new feature before assessment, spec, or task planning. Intake owns scope, success criteria, explicit boundaries, and the decision trail; downstream planning owns solution design, execution contracts, and scheduling.

## Trigger

Use for a new feature or epic whose scope still has decision-changing gaps. For an approved spec, traced bug fix, or sufficiently specified request, reuse the existing constraints and continue with the appropriate workflow.

## Goal

Produce a bounded scope note that separates known constraints, assumptions, unresolved decisions, success criteria, and a Not-Doing list.

## Canonical Policy

- [../../../docs/workbay/instructions.md](../../../docs/workbay/instructions.md)
- [../../../docs/workbay/rules/planning-review-guide.md#planning-pipeline-and-lifecycle-compliance](../../../docs/workbay/rules/planning-review-guide.md#planning-pipeline-and-lifecycle-compliance)
- [../../../docs/workbay/rules/development-workflow.md](../../../docs/workbay/rules/development-workflow.md)
- Heuristics: follow the [heuristics canon pointer](../../../docs/workbay/rules/heuristics-canon.md) (one pinned, digest-verified release). Route this skill's artifact as a **scope/intake note**, and keep only rows named in the Non-Functional Intake table whose triggers fire. Apply each kept row's card Verification and cite `[FAM-NN]` with the rule's bold name and Source cell. `B` blocks, `S` needs a named exemption, and a `J` rule you lack context for is escalated, not skipped. Design-level `ARCH-*` rows are out of scope here; downstream planning artifacts own solution design.

## Core Process

1. Read the request, existing authorization, and relevant approved artifacts. Reuse answered constraints instead of asking again. When a codebase-graph capability is available, ground coupling questions in the package layout before intake.
2. Ask only missing questions whose answers would change scope or success criteria, using the available question capability. There is no fixed question count or required host-specific tool. If existing constraints and authorization are sufficient, continue without a permission pause. Use the **Non-Functional Intake** bank only for triggered, unanswered constraints.
3. Record answers and consequential assumptions with `record_event(event={"event_kind": "decision", ...})`. Follow [handoff-lifecycle](../handoff-lifecycle/body.md) for durable evidence and unavailable-transport handling.
4. Summarize the MVP scope, assumptions, success criteria, edge cases, and Not-Doing list in one scope note; use `docs/scopes/<slug>.md` when a durable artifact is needed. Name any unresolved decision and the work it blocks; continue authorized work independent of it.
5. Map each success criterion to an observable acceptance check and evidence source. Reuse existing checks where suitable. Downstream planning selects test commands and any eval contracts from their current owners; intake does not implement executors or require new eval suites.
6. Hand the accepted scope and decision trail to planning. [incremental-implementation](../incremental-implementation/body.md) owns slice dependencies and dispatch readiness; [tdd](../tdd/body.md) owns RED before production for each affected behavior. Intake does not prescribe a dispatch count.
7. When the on-main `MAINT-*` intake row is no longer needed, close it with `make plan-done TASK=<maint-ref>` (not `make task-finish`).

## Non-Functional Intake

Ask only when the trigger applies (keeps `scope` within budget). The last column names the canon row each answer feeds, per the [heuristics canon pointer](../../../docs/workbay/rules/heuristics-canon.md); cite a row in the intake note only when its own trigger holds and no named exemption covers the feature.

| Trigger | Question | Canon row |
| --- | --- | --- |
| User-facing response-time expectation | Target **p95/p99** (not average)? | [PERF-01] **Percentiles, not averages** (Latency: Reduce Delay in Software Systems, ch. 2 + Designing Data-Intensive Applications, ch. 1) |
| Write triggered by network/client/queue | **Idempotency / retry semantics** — exactly-once required, or safe to re-run? | Only when the write is retried (a network or DB call retry, or hedging) or delivered by an at-least-once queue: [RES-01] **Retry without idempotency** (Designing Data-Intensive Applications, ch. 8). A single write that is never retried cites no row. |
| Reads of cached/replicated/derived data | **Acceptable staleness** — how fresh; must user see their own write? | Only when the plan adds a replica, read-replica, cache or multi-region copy: [DATA-01] **Consistency model must be stated** (Designing Data-Intensive Applications, ch. 5). Derived data alone cites no row; the staleness question still applies. |
| New feature/service with non-trivial traffic | **Scale / load** — req/sec, read:write, fan-out; what breaks at 10×? | [ARCH-12] **Quality attributes drive the skeleton** (Just Enough Software Architecture, ch. 2) |
| Dependency on external/back-end service | **Failure-mode / degraded behavior** — degrade, fail-fast, or queue when X is down/slow? | workbay practice (no canon row) |
| Launch / promo / email blast / public endpoint | **Spike / self-denial** — worst-case peak and where it lands? | workbay practice (no canon row) |
| Writes behind replication/async ack | **Durability / data-loss tolerance** — ok to lose last N seconds on crash? | workbay practice (no canon row) |

Row conditions: the `DATA-01` row applies only when the plan adds a replica, read-replica, cache or multi-region copy, and is exempt for a single-node single-store design; derived data alone does not qualify. At intake, cite `ARCH-12` when a non-trivial-traffic feature is accepted with the load risk named but no quality-attribute target recorded. A retried write or a replicated/cached read is also a **concurrency or async code** artifact, so route that part of the feature through that label before citing `RES-01` or `DATA-01`.

**Process meta** (from Refactoring UI / Farley): be a pessimist → smallest shippable cut; start with a real feature not the shell; define success metrics up front; YAGNI at scope time; frame value by DORA stability+throughput.

## Recovery

- If a required answer is unavailable, expose the assumption or named blocker and hold only the work that depends on it. Do not silently invent a constraint.
- If the ask is already a bug fix or spec-derived task, exit intake and use its implementation flow.
- If shared MCP is unavailable, emit the decision trail in the durable worker report for coordinator recording; do not claim successful writes or invent receipt ids.

## Convergence Criteria

- Every decision-changing gap is answered, explicitly assumed within authorization, or named as a blocker for dependent work.
- The scope note includes success criteria with observable checks and explicit Not-Doing boundaries.
- The decision trail is durable and the work is ready for downstream planning without hidden assumptions.

## See Also

- [../../../docs/workbay/rules/planning-review-guide.md#planning-pipeline-and-lifecycle-compliance](../../../docs/workbay/rules/planning-review-guide.md#planning-pipeline-and-lifecycle-compliance)
- [../../../docs/workbay/instructions.md](../../../docs/workbay/instructions.md)
