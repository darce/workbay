#!/usr/bin/env python3
"""Gate: every acceptance-pin arm carries an EXECUTED red-arm receipt.

Canon: TEST-15 (prove the green can go red), TEST-06 (watch it fail once),
AGT-04 (evidence verbatim). A predicted failure is not a receipt.

Usage: check_red_arm_ledger.py <plan.md> <min_arms>

Contract enforced on the plan file:

  1. A `## Red-arm receipts` section exists.
  2. Directly under it, an arm index fenced block listing one arm id per line:
         arms: <id> <id> <id> ...
     with at least <min_arms> ids.
  3. Every arm id in the index appears somewhere in the `## Acceptance pins`
     section (an arm the plan does not actually declare cannot be receipted).
  4. One `### RED-ARM RECEIPT - <arm id>` block per indexed arm, exactly.
  5. Each receipt carries tree_sha (40 hex), command (non-empty),
     exit_code (INTEGER, NON-ZERO), decisive_output (non-empty, not a
     placeholder).

Exit 0 only when all hold. Any violation exits 1 and names the arm.
"""

from __future__ import annotations

import re
import sys

PLACEHOLDERS = {
    "", "tbd", "todo", "n/a", "na", "none", "predicted", "pending",
    "<fill>", "...", "xxx",
}
HEX40 = re.compile(r"^[0-9a-f]{40}$")


def fail(msg: str) -> None:
    print(f"RED-ARM-LEDGER FAIL: {msg}")
    sys.exit(1)


def section(text: str, heading: str) -> str:
    m = re.search(rf"^{re.escape(heading)}\s*$", text, re.M)
    if not m:
        return ""
    rest = text[m.end():]
    nxt = re.search(r"^## ", rest, re.M)
    return rest[: nxt.start()] if nxt else rest


def main() -> None:
    if len(sys.argv) != 3:
        fail("usage: check_red_arm_ledger.py <plan.md> <min_arms>")
    path, min_arms = sys.argv[1], int(sys.argv[2])
    try:
        text = open(path, encoding="utf-8").read()
    except OSError as exc:
        fail(f"cannot read plan: {exc}")

    ledger = section(text, "## Red-arm receipts")
    if not ledger.strip():
        fail("no `## Red-arm receipts` section")

    pins = section(text, "## Acceptance pins")
    if not pins.strip():
        fail("no `## Acceptance pins` section to cross-check arm ids against")

    idx = re.search(r"^arms:[ \t]+(.+)$", ledger, re.M)
    if not idx:
        fail("no `arms: <id> <id> ...` index line under `## Red-arm receipts`")
    arms = idx.group(1).split()
    if len(arms) < min_arms:
        fail(f"arm index lists {len(arms)} arms, floor is {min_arms}")
    if len(set(arms)) != len(arms):
        dupes = sorted({a for a in arms if arms.count(a) > 1})
        fail(f"duplicate arm ids in index: {dupes}")

    for arm in arms:
        if arm not in pins:
            fail(f"arm id {arm!r} is in the index but appears nowhere in `## Acceptance pins`")

    blocks = dict(
        re.findall(r"^### RED-ARM RECEIPT - (\S+)\s*$\n(.*?)(?=^### |\Z)", ledger, re.M | re.S)
    )
    missing = [a for a in arms if a not in blocks]
    if missing:
        fail(f"no receipt block for arms: {missing}")
    extra = [a for a in blocks if a not in arms]
    if extra:
        fail(f"receipt blocks for un-indexed arms: {sorted(extra)}")

    for arm in arms:
        body = blocks[arm]
        fields = dict(re.findall(r"^(tree_sha|command|exit_code|decisive_output):[ \t]*(.*)$", body, re.M))
        for key in ("tree_sha", "command", "exit_code", "decisive_output"):
            if key not in fields:
                fail(f"arm {arm}: missing field {key!r}")
            if fields[key].strip().lower() in PLACEHOLDERS:
                fail(f"arm {arm}: field {key!r} is a placeholder ({fields[key]!r})")
        if not HEX40.match(fields["tree_sha"].strip()):
            fail(f"arm {arm}: tree_sha is not a 40-hex sha ({fields['tree_sha']!r})")
        raw = fields["exit_code"].strip()
        if not re.fullmatch(r"-?\d+", raw):
            fail(f"arm {arm}: exit_code is not an integer ({raw!r})")
        if int(raw) == 0:
            fail(
                f"arm {arm}: exit_code is 0 — the pin PASSES on the untouched tree, "
                "so it cannot detect the work being absent. Rewrite or delete the pin."
            )

    print(f"RED-ARM-LEDGER OK: {len(arms)} arms, all receipted with non-zero exit")


if __name__ == "__main__":
    main()
