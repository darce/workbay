# WorkBay canon lint

Synchronize the WorkBay checkout with `uv sync --frozen`. Run these commands from the checkout.

```sh
uv run --frozen workbay canon check-drift --json
uv run --frozen workbay canon lint --json
```

The workspace installs the `workbay[canon]` extra. Consumer workspaces using
the extra need Python 3.12 or later and access to a WorkBay source checkout
that includes `workbay-canon-lint`.

A tracked `.workbay/canon-lint.toml` selects the expected canon release,
its manifest SHA-256, and the adapters and Git glob paths to check. An
optional `canon_root` may point at a pinned local release. Override the
location with `--canon-root`, or set `WORKBAY_HEURISTICS_CANON_ROOT` for a
workspace whose configuration leaves the location open. The default is
the matching immutable `~/Development/heuristics-canon-v<version>` directory
when present, with the published `heuristics-canon-current` link as a fallback.
The linter verifies the configured version and manifest digest before reading rule
rows. Row hashes cover the exact UTF-8 table line without its line ending.

`check-drift` checks every registered rule pin, and `lint` additionally
scans tracked paths selected by the repository adapter configuration.
Use `lint --files PATH ...` to scan named workspace files, including untracked
and ignored files that match configured routes. `lint --staged` scans the
Git index, and `lint --changed` scans working copies of staged, unstaged,
and non-ignored untracked paths. The selectors are mutually exclusive.
`--quiet` suppresses stdout when a completed scan has zero findings, including
with `--json`.

Each finding carries the rule, check, adapter, release, digests and source
location. A tier S exemption has an exact repository path, check and rule
IDs, owner, rationale, and expiry date.

The Python timeout check covers direct `requests`, `httpx`, and
`urllib.request.urlopen` calls without an explicit bounded timeout. Other
checks cover broad catches that discard an error before immediately
returning an unassigned result and isolated placeholder text in finished Markdown.

`workbay canon lint-decision --decision TEXT [--rationale TEXT] [--files PATH ...]`
checks the decision and rationale for policy signals. It accepts associated
file paths as context and evaluates only the supplied text. It works without
a Git workspace or local Canon release. Advisories exit 0. Use `--json` for
the structured report and `--quiet` to suppress output when it has no advisories.

The decision checks and Python timeout check use Canon v0.26.2, pinned by
`meta/release-manifest.json` (SHA-256 prefix `46636aa71f2e`).

- [DATA-14] **No dual writes** (Designing Data-Intensive Applications, ch. 11)
  flags proposals to author the same change in two places.
- [AGT-13] **No silent ADR override** (Software Architecture: The Hard Parts, ch. 1)
  flags explicit ADR contradictions without an amendment.
- [SECD-05] **Fail-safe defaults** (Security Engineering) flags proposals
  to bypass verification or fail open.
- [RES-02] **Timeout on every blocking call** (Release It!, ch. 5) flags
  explicit unbounded waits and supports the Python timeout check.
- [AGT-10] **Degrade loudly** (The Art of UNIX Programming, ch. 1) informs
  the silent exception check and typed errors for inaccessible Canon roots.

An enabled gate requires exit `0` and a valid report. Findings from tier B
exit `1`, CLI usage errors exit `2`, verification or runtime errors exit
`3`, and unexempted tier S findings exit `5`. Successful J-only reports
exit `0`.
