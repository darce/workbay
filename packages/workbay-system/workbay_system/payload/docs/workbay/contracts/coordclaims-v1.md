# coordclaims v1: one local authority journal

`workbay_orchestrator_mcp.orchestration.coordclaims.execute(root: Path,
request: dict, *, authority: dict, principal: str, session_id: str) -> dict`
is the public core. The module CLI calls this same function. The coordinator
owns one root and supplies trusted configuration and identity. All participants
must reach that one authority; copying the root does not preserve exclusivity.
There is no network listener, authentication adapter, automatic failover,
external callback, or remote actuation enforcement in this implementation.

## Request and identity

Trusted `authority` contains exactly `authority_id`, `authority_epoch`,
`task_ref`, and `wave_id`. Epoch is a positive integer, excluding booleans.
Other fields, principal and session ID are nonblank strings. Session ID denotes
an incarnation: restarting with the same principal and a new session cannot
renew, release or complete the previous incarnation's claim.

A request is a JSON object with exactly these required fields:

```json
{"schema_version":1,"authority_id":"coordinator","authority_epoch":1,"task_ref":"task","wave_id":"wave","operation_id":"request-1","work_item_id":"item-1","operation":"claim"}
```

Optional fields are `ttl_seconds` (claim/renew only) and `token`
(required for renew/release/complete; forbidden for claim/get). Tokens are exact
positive JSON integers, excluding booleans. TTL is a finite number, excluding
booleans, greater than zero and at most 300 seconds; default is 60. Renew sets
expiry to server time plus requested TTL, retaining the token; a shorter TTL
can shorten the remaining lease. Unknown fields, operations, versions, invalid
types and nonfinite JSON values fail with `invalid_request`. Requests cannot
supply root, time, principal or session. Strings are compared exactly, without
trimming or normalization; whitespace-only identity/key strings are rejected.

The core limits the compact UTF-8 JSON request to 64 KiB. The CLI additionally
limits raw input, including whitespace, to 64 KiB and rejects duplicate keys,
invalid UTF-8, trailing JSON documents and nonfinite numbers. Trusted CLI flags
are configuration, not authenticated credentials.

Every request must match all four trusted authority fields. The first committed
mutation (including a durable business refusal) binds `(task_ref, wave_id)` to
that authority ID and epoch. Later configuration changes for that namespace
fail with `scope_mismatch`; there is no online epoch rollover or authority
replacement API. Different task/wave namespaces may coexist in the same root.
An empty `get` does not establish a durable binding.

## State and lineage (GRPH27, GRPH14)

Work identity is `(task_ref, wave_id, work_item_id)`. The implemented transition
matrix is:

| Before | Operation / predicate | After | Event |
| --- | --- | --- | --- |
| available | claim | claimed | claim.granted |
| claimed | renew with active owner and token | claimed | claim.renewed |
| claimed | release with active owner and token | available | claim.released |
| claimed | expiry (`expires_at <= server time`) | available | claim.expired |
| claimed | complete with active owner and token | completed | claim.completed |

Completed is terminal. Any new mutation on a completed item returns `completed`.
Claiming an occupied item returns `held_by`, even for its current owner. Other
mutations without the exact active principal, session and token return
`stale_token`. A grant consumes the journal's global `next_token`, shared with
legacy holds; independent keys and waves receive strictly increasing tokens.
Renewal does not consume another token. Tokens describe this journal's admission
order; they do not fence external Git, shell, database or remote operations.

Expiry is lazy and affects the requested key. An admitted mutation persists an
expiry event once, together with its own outcome and receipt. A `get` returns
an expired item as available without persisting expiry. Stored available and
completed records retain the previous owner/token/expiry for lineage; those
fields confer no current ownership. Each event has the existing journal event
ID, sequence and timestamp, plus fields for all four authority values,
work item ID, token, operation ID and owner `{principal, session_id}`. The actor
is the trusted requesting principal; an expiry's operation ID identifies the
request which materialized it and its owner identifies the expired holder.
Events are generated internally; they are not an external action API.

## Durable replay and compatibility (DATA14)

There is one `.task-state/coordination` journal, one `coord.lock` and one global
token counter. `holds.json` remains schema v1 and gains an optional expansion:

```text
coordclaims: {
  schema_version: 1,
  waves: {
    compact_JSON([task_ref,wave_id]): {
      authority, last_time,
      items: {work_item_id: {state,owner,token,expires_at}},
      receipts: {operation_id: {fingerprint,response}}
    }
  }
}
```

Claim state, receipt and events use the existing `_commit_transition` journal
snapshot and recovery/projection path under the same lock. No public legacy
hold API or nested lock is involved. New code accepts legacy state with no
extension, preserves legacy holds/resources/events, and validates the extension
on every load, commit and pending-journal recovery, including legacy-only
operations. Unknown extension versions and malformed structures fail closed.
This is an expand compatibility change, not permission to run older writers
which do not validate the extension. Upgrade all writers before enabling claims;
no downgrade or receipt-pruning migration is provided.

A receipt's SHA-256 fingerprint covers canonical JSON of the entire request,
trusted authority, principal and session. Object field order is immaterial;
omitted default TTL versus explicit TTL, and integer versus floating-point TTL,
are distinct requests. Identical mutation retries return the original response,
including the original expiry, after expiry/release/completion. Changing any
fingerprinted field under a committed operation ID returns
`idempotency_conflict`. Operation IDs are unique across the entire task/wave,
not per item. A get colliding with an existing mutation ID also conflicts.

Successful mutations and business refusals (`held_by`, `stale_token`,
`completed`) have receipts. Validation, scope, clock, capacity, contention and
storage failures do not independently create receipts. A storage error can occur
*after* journal commit, so its outcome is uncertain: retry the identical ID and
request. Recovery projects committed state and deduplicates events by event ID;
no second transition is executed for a committed retry. Receipts are never
pruned. At 10,000 receipts per task/wave, all new mutations return `capacity`;
existing receipts can still replay, conflicts are still rejected, and get works.

## Time, admission and reads (RES06, RES09)

Production wall-clock time is sampled after lock acquisition and recovery.
Each committed mutation, including a business refusal, records a per-wave time
watermark. A later request with server time below that watermark fails closed
with `state_corrupt` and `detail: clock_rollback`, including replay and get.
The watermark cannot detect reversals entirely between persisted samples;
read-only calls do not update it. Recovery/reconciliation and stale-backup
restoration are operational responsibilities, not automatic failover features.

Lock admission uses the existing nonblocking flock deadline with a two-second
monotonic budget. `deadline` means lock admission failed. Filesystem operations,
fsync, journal recovery and whole-log scans have no hard OS-level time bound.
A CLI read is byte-bounded, not time-bounded. There is no queue/service capacity
claim beyond the receipt limit and lock-admission deadline.

Get creates no receipt, state, expiry event, authority binding or watermark.
The existing lock path may create coordination directories/lock file, and a
pending committed journal is recovered and projected before any operation,
including get. Thus get is business-state read-only, not a promise of zero
filesystem writes during recovery. The same recovery exception applies before
persisted binding/clock checks; wire scope mismatch is rejected before locking.

## Response and CLI

All responses have `schema_version: 1` and boolean `ok`. Successful mutations
return `work_item_id`, `state`, `owner`, `token`, `expires_at`; get returns the
same stored view or only `work_item_id` and `state: available` for missing or
logically expired items. Errors carry `error_code`: `invalid_request`,
`scope_mismatch`, `stale_token`, `held_by`, `completed`, `idempotency_conflict`,
`capacity`, `deadline`, `state_corrupt`, or `storage_error`. `held_by` also carries
the occupied item view. No raw exception or filesystem path is returned.

```sh
printf '%s' '{"schema_version":1,"authority_id":"coordinator","authority_epoch":1,"task_ref":"task","wave_id":"wave","operation_id":"request-1","work_item_id":"item-1","operation":"claim"}' |
  .venv/bin/python -m workbay_orchestrator_mcp.orchestration.coordclaims \
    --root /trusted/coordinator/root --authority-id coordinator \
    --authority-epoch 1 --task-ref task --wave-id wave \
    --principal alice --session-id incarnation-1
```

The CLI reads one bounded JSON request from stdin and writes one core JSON
response to stdout. Exit codes: 0 success, 2 invalid request/arguments, 3 other
refusal/error. It is a trusted same-user local tool: access to the root and CLI
configuration is the authority boundary. This contract makes no claim of
network authentication, remote fencing, or protected external actuation.
