# ROUND 7 evidence — gitonly repair polarity

Lane: `wb-p0194-gitonly-01` / task `internal`.

## Command run

```text
uv run pytest packages/workbay-bootstrap/tests/test_install_receipt.py packages/workbay-bootstrap/tests/test_gitonly_mcp_install.py -q
```

## Real output

```text
.................................................                        [100%]
49 passed in 7.69s
```

`uv.lock` was not modified by the suite run (no restore needed).

## D1 — non-convergence polarity

- Production: `_repair_deferred_install_steps` now returns `(repaired, skipped)`.
- Non-convergence branches (empty `mcp_servers`, empty `member_specs`, install exception)
  emit `kind=install_step_repair_skipped` into the skipped list.
- Success branch keeps `kind=install_step_repair`.
- CLI already special-cases `install_step_repair_skipped` and prints the detail
  (not the `--force-dirty` hint). Call site merges deferred skips with presync skips.
- Tests pin kind + post-state (`failed`/`abort` unchanged on non-convergence).

## D2 — prewarm silent cells

- Empty `mcp_servers` with a deferred `prewarm_uvx_mcp` row emits
  `install_step_repair_skipped` (was `[]`).
- Empty `warmed` from `_prewarm_uvx_mcp_envs` emits
  `install_step_repair_skipped` (was silent leave-deferred).
- Covered by `test_repair_prewarm_empty_servers_surfaces_skipped` and
  `test_repair_prewarm_empty_warmed_surfaces_skipped`.

## D3 — bare exception must not kill doctor/repair

- Trailing `except Exception` records degraded `install_step_repair_skipped`
  and continues the walk.
- Covered by `test_repair_gitonly_keyerror_surfaces_skipped_not_abort`
  (`KeyError("workbay-protocol")`).

## D4 — offline wording

- `DeferredExternalCall` uses offline lead phrase + "re-run when online".
- Other exceptions keep "retryable system failure" wording.
- Covered by `test_repair_gitonly_offline_uses_offline_wording`.

## D5 — assertions detect false-success

- Non-convergence tests assert `kind == install_step_repair_skipped`, empty
  repaired list, and receipt post-state still `failed`/`abort` (or deferred).
- Success tests assert `kind == install_step_repair` and status `ok`.
- Prose checks bind to invariant lead phrases (`startswith("installed ")`,
  exact token `no_resolvable_member_specs`) rather than bare payload words.

## Claim correction (round 6)

Round 6 claimed "an install that legitimately installs zero tools is treated as
success". That state is unreachable in production: `GITONLY_MCP_PACKAGES` is the
fixed 2-tuple `("mcp-workbay-handoff", "mcp-workbay-orchestrator")` and
`_install_gitonly_mcp_tools` has no early `return []` — on success it returns one
entry per package or raises.

The `if installed is not None` guard is **defensive only** and is **not** a
demonstrated production closure of a reachable empty-success path. The guard is
kept to avoid truthiness conflation of `[]`; the unit test that mocks
`return_value=[]` remains a defensive pin only.
