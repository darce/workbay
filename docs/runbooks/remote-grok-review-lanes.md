# Runbook — Remote grok review & web-verification lanes

> Operational knowledge from the internal review passes
> (r1 rev-1 fail, r2 rev-2 conditional_pass). Companion to
> `grok-offload-egress-deny.md` and `remote-gate-provisioning.md`. Ledger
> anchors: `agent_error` #210 (fabrication), #212 (lane-key collision);
> review runs `pr-maint-assess-master-orch-20260719-r1`/`-r2`.

## 1. The grounded-review recipe (anti-fabrication)

A schema-valid grok review is **not** a grounded one: run 1 of r1 returned seven
perfectly-shaped findings citing files that do not exist, in `num_turns=1`,
without opening a single file (`agent_error` #210). `--json-schema` constrains
*form*, not *grounding*. The recipe that has now worked twice (16 turns each,
zero fabricated paths):

1. **Inline the complete artifact, line-numbered** (`cat -n`) into the brief.
   The sandbox clone has the repo, but the document under review must be the
   text you number — findings cite those line numbers.
2. **Forbid naming any repository path the agent has not opened** in the
   sandbox; require a grep before any "anchor missing" claim.
3. **Legitimize ignorance**: instruct that "unverifiable in sandbox" is an
   expected, acceptable outcome (git history is stripped; canon and the web are
   absent — inline the canon rules it needs, verbatim).
4. **Gate on `num_turns > 1`** before trusting the result, and spot-check that
   cited paths resolve locally.
5. Severity rubric and exact `file_path` string go in the brief; output is
   JSON-only against the schema.

## 2. Dispatch mechanics (`scripts/remote_agent.sh build`)

```
scripts/remote_agent.sh build --branch <br> --brief <file> --schema <file> \
    [--model grok-4.5] [--effort high] [--max-turns 40] \
    [--result-out <json>] [--debug-out <log>]
```

- **`--schema` must be single-line JSON.** A pretty-printed file fails at grok
  start with `--json-schema: invalid JSON: trailing characters`. Compact it
  (`jq -c` / `json.dumps`).
- **Parse `structuredOutput`, not `result`/`text`.** The fetched result JSON
  carries the schema-validated object under `structuredOutput`; `text` is a
  string duplicate and `result` may be absent.
- **Exit codes**: 0 ok · 3 grok run failed · 4 self-verify failed · 75
  admission deferred · 78 host not configured · 2 usage error. A review lane
  legitimately ends with "grok produced no committed changes".
- Non-`feature/*` branches need `WORKBAY_ALLOW_NONCONFORMING_BRANCH_PUSH=1`
  (the commit-side override does not carry to push).

## 3. Concurrent lanes: the lane-key collision (`agent_error` #212)

The VM lane key is derived **from the pushed ref slug, on the VM** — the local
`LANE_KEY` env does not cross ssh. Consequences of dispatching two lanes from
the same branch:

- The second dispatch fails with `Failed to start transient scope unit:
  Unit grok-lane-<slug>.scope was already loaded`.
- Worse, a **retry re-materializes the sandbox keyed by that same slug and
  deletes it under the running lane** — the first lane dies mid-run with
  `fatal: Unable to read current working directory`.

**Workaround** until `feature/wb-remote-lanekey-collision-01` lands: push the
same commit under one throwaway alias branch per lane, then delete them:

```
git branch -f scratch-<lane-name> <sha>
WORKBAY_ALLOW_NONCONFORMING_BRANCH_PUSH=1 scripts/remote_agent.sh build \
    --branch scratch-<lane-name> --brief … --schema …
git branch -D scratch-<lane-name>
```

VM admission cap is 3 lanes (`WORKBAY_REMOTE_AGENT_MAX_LANES`).

## 4. The web-verification lane pattern

Grok lanes have live web access; a fact-check lane is cheap insurance for any
document making external vendor claims (r2: 21 claims, 19 turns, ~$1.67, five
drift corrections a local pass could never catch). Brief shape:

- Number every claim; demand a per-claim verdict from a closed enum
  (`verified / contradicted / partially_wrong / unfetchable`) plus quoted
  evidence — "quote what you actually saw; never paraphrase from memory".
- Distinguish **drift** (stars, versions — `partially_wrong` with the new
  value) from **contradiction** (the source says otherwise).
- Cap attempts per URL at 2 and make `unfetchable` an acceptable verdict, or
  the lane burns turns on Cloudflare walls.

## 5. Review-cost reference points (2026-07)

| lane | turns | cost |
|---|---|---|
| grounded doc review (900-line artifact inlined, anchors verified) | 16 | ~$0.59 |
| web verification, 21 claims | 19 | ~$1.67 |

Both on `grok-4.5`, `--effort high`, `--max-turns 40`, warm lane venv.
