# Review blocking scope

Reviews find every defect they can. Only findings that cause real problems block a verdict. Edge cases, nitpicks, hardening, and style are still reported in full, but they are recorded as tracked follow-ups and never fail a review.

Source principle: "QA is for the edges" (Shape Up, ch. 14). Edge cases are worth recording. They are not a reason to hold a change.

## What a review judges

A review judges exactly three things:

1. Every listed prior finding is closed by executed evidence.
2. Regressions the change introduces, or makes worse.
3. Identity, test weakening, and reds the change causes in the existing suites.

Anything outside these three is still reported, but it is classified by impact and origin below. Its severity label does not make it blocking.

## Origin tags

Tag every finding summary with exactly one origin tag:

- `[listed-open]`: a prior finding that is still open and was not closed by executed evidence.
- `[introduced]`: the change introduces the defect.
- `[pre-existing]`: the defect already exists on the base tree. Reproduce it on the base tree before using this tag. When unsure, use `[introduced]`.

## Impact tags

Tag every finding summary with exactly one impact tag:

- `[blocking]`: a realistic operator or production path (normal use, ordinary concurrency, an ordinary crash or restart, or input the system actually produces) loses or corrupts data, deletes unintegrated work, hangs, crashes, leaks a secret or privilege, or makes an existing test red. Name the realistic path in one sentence.
- `[follow-up]`: everything else. Examples: adversarial setups no real workflow produces (a hand-crafted ref name such as a tag named `refs/heads/<b>`, a deliberately deleted integration branch, an injected seam, a race that needs a deliberately paused process); hardening and defense-in-depth; style and naming; missing extra tests; docs.

Do not upgrade an edge case to `[blocking]` by raising its severity. Severity says how bad the defect is if the path occurs. Impact decides whether that path is realistic.

## Verdict rule

A review holds or fails only for:

- a `[blocking]` finding tagged `[listed-open]` or `[introduced]`; or
- a `[pre-existing]` finding that is high severity and `[blocking]` (the critical class: data loss, privilege, or secret exposure on a realistic path).

Every `[follow-up]` finding, at any severity, is reported in full and recorded as a tracked follow-up. It never holds or fails a verdict. A review whose only findings are follow-ups passes, and the follow-ups stay on record.

## Canon basis

- [PROD-47] **Scope hammering**: judge a change on the paths it can realistically break, not on every construction a reviewer can imagine.
- [REF-25] **A visible board-up is an accepted resolution**: a known defect that is recorded and visible is an accepted outcome when it is not a realistic-path problem.
- [RLSE-17] **Build quality into the delivery path**: repair a failed check at the change that caused it, rather than leave it for a later stage.
- [SECD-05] **Fail-safe defaults**: an invalid value fails closed. That is why an invalid explicit impact reads as `blocking` and an invalid origin reads as `introduced` (see Recording).
- [TEST-15] **Prove the green can go red**: a test that cannot fail is not evidence. That is why weakened or removed assertions are judged under item 3 of the scope.

## Recording

- Every finding carries two optional fields. Set both on every new finding:
  - `impact`: `blocking` or `follow_up`.
  - `origin`: `introduced`, `listed_open`, or `pre_existing`.
- `impact` and `severity` are matched case-insensitively after stripping whitespace. `origin` is matched exactly: it must be the lowercase value, so `PRE_EXISTING` or ` pre_existing ` reads as `introduced`. An explicit `impact` that is not one of the two values, including an empty string, reads as `blocking`. An `origin` that is absent or invalid reads as `introduced`. These fail-closed defaults keep an invalid value from loosening the gate.
- A finding with no `impact` is legacy and keeps the old severity gate: `high` and `medium` read as `blocking`, anything else reads as `follow_up`. Legacy results are not loosened.
- A `pre_existing` finding blocks only when it is high severity and `blocking`.
- Record the finding's impact and origin tags in its summary as well as in the fields, so the summary reads on its own.
- Reviewers and the coordinator file every `[follow-up]` finding as a tracked open finding. It is never dropped, and it is never closed without its own fix or an explicit disposition.
- A `[follow-up]` finding does not block a merge, a close check, or a verdict. It stays visible in the finding list until it is resolved.

## Related

- [Branch review guide](branch-review-guide.md): severity classification and finding fields.
- [Heuristics canon](heuristics-canon.md): the pinned canon release that the canon rows above come from.
