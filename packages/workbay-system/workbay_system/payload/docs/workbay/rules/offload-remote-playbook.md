# Remote-delegation playbook (`grok-remote`)

Operational rules for offloading work to the remote execution gate — an
operator-provisioned VM reached over SSH, carrying the grok CLI. This doc is the
durable home of knowledge that previously lived in per-agent memory; skills link
here instead of restating it. Numbers below are descriptions of code-owned
constants — the named symbol is authoritative, not this page.

## When remote preference applies

- The preference is **install-scoped, not ambient**: it applies iff the bootstrap
  ledger records `execution_mode: remote_only` (written by
  `workbay-bootstrap install --with-remote`, read only via
  `workbay_protocol.bootstrap.load_execution_mode`). No ledger, or `local_ok`,
  means today's explicit-backend contract — `grok-remote` is then merely a valid
  explicit choice.
- Under `remote_only`, offload defaults resolve to `--agent grok-remote
  --effort high` (model pin `DEFAULT_GROK_MODEL`, currently grok-4.5).
- **Flag, never substitute**: when the remote gate is unavailable under
  `remote_only`, the engine refuses with the typed `remote_required` outcome and
  skills surface it (recorded decision/blocker). Falling back to a local backend
  is prohibited — dropping the policy is `repair --no-remote`, an operator act.

## Availability

- Configured by `WORKBAY_REMOTE_GATE_HOST` (`user@host` or ssh alias). Probe:
  `workbay_protocol.remote_probe.probe_remote_gate` — typed states
  `available | not_configured | malformed_host | unreachable | cli_absent`.
  The orchestrator's `list_available_backends(probe=true)` wraps the same probe
  with a short TTL cache.
- Install `--with-remote` hard-fails on a failed probe (deferred setup = install
  without the flag, then `repair --with-remote`).

## Bounds (code-owned)

- Single-cycle bounds derive from `token_budget`:
  `derive_grok_single_cycle_bounds` in `offload_profiles.py`
  (`GROK_MAX_TURNS_CAP`, `GROK_TIMEOUT_CAP` — order of 30 turns / 900 s).
- The VM enforces its own transport-side guards in `remote_agent.sh`:
  memory floor (`WORKBAY_REMOTE_GATE_MEM_FLOOR_MB`, exit 75 defer), lane cap
  (`WORKBAY_REMOTE_AGENT_MAX_LANES`, exit 75), unconfigured host (exit 78),
  per-scope `MemoryMax`/`CPUQuota`. Exit codes are **transport** semantics —
  policy refusals are typed outcomes (`remote_required`), never exit-code reuse.

## Dispatch discipline

- Brief must carry a **scoped `TEST_CMD`** (never a whole-package suite — it
  times out a pass; dispatch warns `brief_test_cmd_full_suite`), the known-red
  baseline, and the versionless heuristics link.
- Default `include_context_packet=true` with `context_targets` = the slice's
  files, so the worker cold-starts oriented.
- Judgment work stays inline (golden recapture, hermeticity, normalizers —
  see the offload skill's inline-only list); remote lanes get **mechanical
  multi-file slices** with deterministic verification.

## Verify at the gate, never re-dispatch

- `commit_landed: true` + a post-commit `failed_stage`
  (`review | handoff | attestation | null`) means the worker's self-verified
  commit already landed — inspect it (`git log`/diff on the lane branch) and
  route it to the review gate. Re-dispatching an already-green tree livelocks.
- Re-dispatch is correct only when `commit_landed: false`
  (`self_verify_failed`, or `failed_stage ∈ {execute, self_verify}`).
- Known misfit: a **read-only review brief** through the work-lane pass engine
  ends `outcome=error / failed_stage=review` even when the reviewer finished.
  Recover the verdict from the pass artifacts on the VM
  (`$WORKBAY_REMOTE_AGENT_ROOT/<branch-hash>/.grok-result.json`) instead of
  re-running; treat the recovered findings as the review output.

## Review lanes

- Per-lane adversarial review runs on `grok-remote` at high effort too, citing
  stable rule IDs from the engineering heuristics lexicon (link the canon;
  never paste rule bodies, never pin a canon version).
- `/review-parallel` remains the orchestrator's branch-complete merge gate; a
  remote reviewer lane is pinned via `materialize_offload_lane_manifest
  (preferred_backend=grok-remote)`.
