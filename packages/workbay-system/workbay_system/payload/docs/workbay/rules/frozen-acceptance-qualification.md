# Frozen acceptance qualification

This is the authoritative qualification order for normal implementation,
offload, branch review, and parallel review. The coordinator applies it before
spending on another author or merge reviewer. It is a portable workflow rule;
instructions alone do not enforce evaluation in the lifecycle runtime.

## Required order

1. **Hold the original contract before authoring.** An independent coordinator
   owns the original criteria, logical case IDs, immutable criteria/case hashes,
   denominator, expected outcomes, and required scope. Include required real
   entry points, runtime paths, and negative cases. Read semantic prior work,
   current codemap/source, and the current pinned canon first; record retrieval
   limitations. The author cannot redefine the bar or qualify their own work.
   Preserve an existing prose freeze byte-for-byte. If it has no executable
   cases, mark that PROCESS coverage gap and independently implement its
   criteria in the existing harness; never claim executable acceptance was
   frozen earlier. Retain the prose hash and case-to-criterion mapping.
2. **Observe the baseline independently, before authoring where possible.**
   Record actual original-case results on the base. Setup, fixture errors,
   unavailable environments, and missing runtime capabilities are UNKNOWN,
   separately classified; they are not causal product RED. If baseline cannot
   run yet, name the missing capability and next action. Do not manufacture a
   RED from an import error or infer a baseline from the author's receipt.
3. **Compute original evaluations on the exact candidate before model merge
   review or repair dispatch.** The independent observer runs the held cases
   and retains raw observations bound to base/candidate commits and trees,
   tests, harness, runtime, environment, and contract identity. A new candidate
   needs a new source binding, not new criteria. Author TDD, smoke reviews,
   fixtures, and receipts support only the scope they actually exercise;
   author support never becomes self-qualification. Hand the independent
   packet below to every reviewer and repair assignment.
4. **Classify before dispatching another fix.** Every finding and dispatch
   cites an original case or an independently demonstrated newly introduced
   regression, with a causal base/candidate reproduction. Keep fixture,
   environment/setup, new scope, and unknown evidence separate from product
   defects. New scope stays `follow_up` until an explicit versioned contract
   disposition; it cannot silently enlarge the original release bar. Genuine
   introduced defects still require repair even when old tests missed them.
   Missing original coverage is a PROCESS gap: repair the harness/workflow and
   recompute the original criteria before sending more product authors. A list
   of individual blockers is not a substitute for that correction. Fixture
   technicalities alone do not automatically become product release blockers.
5. **Use the results to control repetition and integration.** Before a third
   or later author dispatch in the same cohort, record a canon-grounded proposal
   containing previous paths tried, semantic prior work, and frozen results.
   More than two review runs require a new demonstrated reason; do not open a
   broad review while the same unchanged case is pending. Changing a label,
   branch, author, or run ID does not reset history. One independent owner
   evaluates the assembled train and its exact component tuple. Reuse evidence
   only when all source/test/harness/runtime/environment/contract identities
   match. Preserve original counters, outcomes, raw evidence, and custody.
6. **Report coverage with the outcome.** Show PASS, FAIL, or UNKNOWN for each
   original case and the explicit completion coverage (observed/required,
   missing paths, owner, next action). FAIL means demonstrated behavior violates
   the held criterion; UNKNOWN means qualification is incomplete. Do not infer
   whole-feature PASS from passing subcases. A bounded `full40377`
   `qualified_pass` proves only that battery's scope: it does not prove public
   release, live installed behavior, or detach survival when required runtime
   evidence is absent. Apply that profile only where the contract requires its
   categories, never universally to unrelated work.

## Minimum independent packet

Use a small table in the existing review/dispatch evidence; this is not a new
JSON schema or a new security gate. The coordinator owns the packet and the
observer owns the results, independently of the product author.

| Field | Required content |
| --- | --- |
| Original case and scope | Logical case ID, criteria SHA-256, case hash, fixed denominator, expected outcome, required entry/runtime/negative scope; prose-only gaps explicitly named |
| Exact identities | Source base/candidate commits and trees; harness/test hashes; runtime and environment identities; original contract version/hash and candidate-bound manifest digest |
| Independent observation | Baseline and candidate PASS/FAIL/UNKNOWN, observer, actual command, raw receipt/log locator and hash, explicit completion coverage |
| Finding and cause | Original case mapping or newly introduced regression; product/fixture/environment/new-scope/unknown classification; base/candidate causal reproduction or unresolved limitation |
| Ownership and action | Independent coordinator/observer, next action and owner; versioned disposition for proposed scope changes; prior dispatch/review lineage |

## Reuse the existing runner

Use the independently held harness installation. Read its API and `--help`
before invoking it; the existing Python module entry points are:

```sh
python -m workbay_orchestrator_mcp.orchestration.acceptance_contract freeze \
  --repo REPO --spec SPEC.json --cases-root EXTERNAL_CASES \
  --manifest NEW_MANIFEST.json --digest-out NEW_MANIFEST.sha256
python -m workbay_orchestrator_mcp.orchestration.acceptance_contract run \
  --repo REPO --manifest NEW_MANIFEST.json --cases-root EXTERNAL_CASES \
  --expected-digest HELD_SHA256 --base BASE --tip CANDIDATE --out NEW_RECEIPT_DIR
```

These placeholders name coordinator-held inputs and fresh outputs. The existing
`freeze_manifest` API takes a `workbay/acceptance-spec@1` with full base/tip IDs
and hashes the external Python case scripts. `run_contract` checks that exact
pair and runs both arms. Keep the expected digest outside author control.
For another candidate, retain every old manifest/receipt and create a new
manifest binding the unchanged logical IDs, criteria, case bytes and expectations
to the new source pair. Its digest changes; the original release bar does not.
Do not substitute a post-failure freeze for the original freeze.

The runner supports stdlib Python cases with declared revision-local imports,
not arbitrary shell/pytest/runtime workflows. Only its observed assertion
failure is RED; import errors, early exits and timeouts are UNKNOWN. A fix claim
requires RED on base and GREEN on candidate for every listed case;
characterization records preserved behavior separately. Running both arms after
authoring does not establish a pre-author baseline. Required real runtime paths
outside this grammar still need independent observation and explicit coverage;
a script result cannot certify an unexecuted installed/native path.

When the task calls for the broader battery, use the existing
`validation_battery` CLI with `--profile full40377` and its held acceptance triple,
suite, regression selection/catalog and explicit finite MODEL controls. Consult
`python -m workbay_orchestrator_mcp.orchestration.validation_battery --help`
and the producer's `docs/contracts/validation-battery-full40377-v1.md`.
Legacy exit 0 is unqualified; full40377 still reports partial coverage and
`full_proposal_complete: false`. Neither runner replaces coordinator judgment
about missing original scope, nor automatically controls lifecycle dispatch.

## Example: preserve the original conclusion

NWI04 cases **04-1/04-8** require the real remote driver and typed delivery.
The genuine 556-second model run and coordinator-disconnect survival are useful
observations, but typed harvest returned **rc 4**: that workflow is **FAIL**,
not whole-feature PASS. The **118-case B4 lifecycle freeze is a different
contract**; the later **347 receipt probe is not original acceptance**. Preserve
both histories. A shallow-clone receiver setup refusal is environment evidence,
not product RED. Sources 353/354 stopped before RED/production edits; that
history does not authorize another individual repair in place of qualification.

The ownership rule applies Canon v0.26.2 [OBS-09, **Independent watcher the
optimizer cannot tune**](https://github.com/darce/heuristics-canon/blob/v0.26.2/lexicons/engineering.md#obs-09)
(*Thinking in Systems*, ch. 6), CARD-11 / Principle 15. Train qualification applies
[TEST-22, **Test the assembled version tuple**](https://github.com/darce/heuristics-canon/blob/v0.26.2/lexicons/engineering.md#test-22)
(*Continuous Delivery*, ch. 13). Follow the installed
`docs/workbay/rules/heuristics-canon.md` consumer pointer for the currently
pinned, digest-verified release before each proposal.
