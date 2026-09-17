---
name: skill-name
description: One-sentence description of the repeatable workflow this skill standardizes.
mode: advisory
context_budget: 150
makefile_target: null
mcp_tools: []
tdd_gate: false
disable-model-invocation: false
---

# Skill Title

## Overview

State when this skill should be loaded, what problem it solves, and whether it is advisory guidance or an execution loop. Keep the opening compact so an agent can decide quickly whether this is the right skill.

## Trigger

List the request shapes, file targets, or workflow situations that should activate the skill. Include nearby non-matches when confusion is likely.

## Goal

Describe the concrete end state. Focus on the outcome the agent must achieve, not the implementation details.

## Canonical Policy

Link to the repo-level policy documents this skill relies on instead of duplicating them. Call out the specific boundaries this skill owns so agents do not confuse it with broader process guidance.

## Core Process

Describe the numbered workflow the agent should follow. Execution skills should define the gated loop, required MCP writes, and stop conditions. Advisory skills should define the recommended sequence and decision points without pretending to own the whole workflow.

Execution-skill expectation:

1. Load the minimum required context for the target.
2. Perform the workflow steps in order.
3. Record required MCP state changes before reporting completion.
4. Re-check the convergence gate before exiting.

Advisory-skill expectation:

1. Inspect the current situation.
2. Recommend the smallest safe next actions.
3. Escalate only when the boundary or risk is unclear.

## Common Rationalizations

List the shortcuts or excuses that commonly lead to bad outcomes in this workflow. Phrase them as recognizable anti-patterns an agent might be tempted to follow.

## Red Flags

List the warning signs that mean the agent should pause, narrow scope, or escalate. Prefer observable conditions over vague cautions.

## Recovery

Document what to do when the ideal path breaks down: missing context, dirty branches, unavailable MCP, mixed ownership, or failed preconditions. Recovery guidance should preserve safety and traceability.

## Convergence Criteria

Define how the agent knows the skill is honestly done. Execution skills must include any required verification evidence, MCP writes, and gating checks. Advisory skills should define the point where the recommendation is complete and bounded.

## See Also

Link to adjacent skills, guides, templates, or Makefile targets. Use this section to help the next agent load the right follow-on workflow without duplicating those documents here.
