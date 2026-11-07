#!/bin/sh
# One-shot workbay stack update (internal).
#
# Deterministic consumer updater — no LLM, no version archeology:
#   1. locate the Python runtime that owns the workbay-bootstrap console
#      script (its shebang), and pick one explicit upgrade adapter;
#   2. derive the complete member closure from its recorded source (the
#      manifest's Git pin or the owning runtime's install receipts) and
#      reinstall it in THAT runtime; with no grounded source, or an editable
#      checkout (a worktree source or editable receipts: update it with
#      `uv sync`), refuse before any mutation (exit 3). A failed install
#      adapter exits 4 and prints the adapter's own status. WorkBay is
#      released git-only, so no member is resolved from a package index. To
#      move to another release, re-run install with --remote-ref;
#   3. refresh the overlay: `workbay-bootstrap update --target <repo>`
#      (package sources take no ref; worktree sources are refused in step 2;
#      git_overlay takes REMOTE_REF=<tag> or refuses before mutation and
#      exits 2);
#   4. `workbay-bootstrap doctor`;
#   5. print the stack version table and exit with doctor's code.
#
# Usage: update.sh [TARGET]            (TARGET defaults to the git toplevel)
#   REMOTE_REF=<tag>           git_overlay consumers: ref to update to
#   WORKBAY_UPDATE_DRY_RUN=1 print each mutating step instead of running it
#                            (a preview never clones the recorded ref)
#
# This file is a managed payload surface: it self-updates with the stack.

set -eu

DRY_RUN="${WORKBAY_UPDATE_DRY_RUN:-}"
# The `workbay` front door is the version anchor: it carries the exact `==`
# member pins (workbay-stack was retired). Step 2 installs it from its recorded
# source; this name is only read back from installed metadata to verify pins.
STACK_DIST="workbay"

log() { printf 'workbay-update: %s\n' "$*"; }
fail() { printf 'workbay-update: ERROR: %s\n' "$*" >&2; exit 1; }

run() {
    # Print every mutating command; execute unless dry-run.
    printf 'workbay-update: + %s\n' "$*"
    if [ -z "$DRY_RUN" ]; then
        "$@"
    fi
}

TARGET="${1:-}"
if [ -z "$TARGET" ]; then
    TARGET="$(git rev-parse --show-toplevel 2>/dev/null)" ||
        fail "no TARGET given and not inside a git repository"
fi
[ -d "$TARGET" ] || fail "target directory does not exist: $TARGET"

MANIFEST="$TARGET/.workbay-bootstrap.json"
[ -f "$MANIFEST" ] || fail "no .workbay-bootstrap.json in $TARGET — run workbay-bootstrap install first"

# --- 1. resolve the runtime owning workbay-bootstrap --------------------
BOOTSTRAP_BIN="$(command -v workbay-bootstrap)" ||
    fail "workbay-bootstrap not on PATH — install the workbay stack first (git-only: uv tool install the workbay closure per docs/CONSUMER.md)"

# Console-script shebang names the owning interpreter; `#!/usr/bin/env X`
# resolves through PATH.
SHEBANG="$(head -n 1 "$BOOTSTRAP_BIN")"
RUNTIME_PY="${SHEBANG#\#!}"
case "$RUNTIME_PY" in
    *"/env "*)
        RUNTIME_PY="$(command -v "${RUNTIME_PY##*/env }")" ||
            fail "cannot resolve interpreter from shebang: $SHEBANG"
        ;;
esac
# Trim a trailing argument-free spaces form like "/usr/bin/python3 -s".
RUNTIME_PY="${RUNTIME_PY%% *}"
[ -x "$RUNTIME_PY" ] ||
    fail "runtime interpreter for workbay-bootstrap is not executable: $RUNTIME_PY (from $BOOTSTRAP_BIN)"
# Python < 3.11 rejects -P as a usage error (exit 2, the REMOTE_REF usage
# status), so check the version first with a call that imports only sys.
RUNTIME_VERSION="$("$RUNTIME_PY" -c 'import sys; print("%d.%d" % sys.version_info[:2]); raise SystemExit(sys.version_info < (3, 11))')" ||
    fail "runtime $RUNTIME_PY is Python ${RUNTIME_VERSION:-of unknown version}; the updater requires Python >= 3.11"
log "runtime: $RUNTIME_PY (owns $BOOTSTRAP_BIN)"

# --- 2. ground the member closure in its recorded source, then install it --
# WorkBay is released git-only: every member is installed from the source the
# manifest pin or the owning runtime's install receipts record, never from a
# bare distribution name (no package index is consulted for WorkBay members).
# Resolution runs in the owning interpreter through workbay_bootstrap's own
# closure helpers; with no grounded source the update refuses before any
# mutation. Editable checkouts are refused, not reinstalled as static copies.
# Exit 2: REMOTE_REF usage error; exit 3: no grounded, a contradictory or an
# editable source; exit 4: the install adapter failed (its status is printed).
# Every embedded interpreter call after the version check passes -P (Python
# >= 3.11, as tomllib already requires) so a module or dist-info in the
# caller's cwd cannot shadow imports or the installed-metadata grounding.
"$RUNTIME_PY" -P - "$MANIFEST" "$TARGET" "$RUNTIME_PY" "$BOOTSTRAP_BIN" \
    "$DRY_RUN" "${REMOTE_REF:-}" <<'PY'
import hashlib
import importlib.metadata as metadata
import json
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import tomllib
import zipfile
from email.parser import Parser
from pathlib import Path
from urllib.parse import unquote, urlparse

USAGE_STATUS = 2
SOURCE_STATUS = 3
ADAPTER_STATUS = 4


def refuse(code, detail, status=SOURCE_STATUS, remedy=None):
    print(f"workbay-update: ERROR: source_provenance[{code}]: {detail}", file=sys.stderr)
    print(
        "workbay-update: no package was changed; "
        + (
            remedy
            or "WorkBay members are never installed from a package index. Re-establish a "
            "grounded source with `workbay-bootstrap install --source package --remote-url "
            "<url> --remote-ref <ref> --target <repo>` (docs/CONSUMER.md)."
        ),
        file=sys.stderr,
    )
    raise SystemExit(status)


def refuse_editable(what, checkout):
    remedy = f"update the checkout with `uv sync` in {checkout}"
    # `uv sync` only updates the checkout's own environment. The venv's bin
    # directory, not the interpreter (a venv python is a symlink to its base
    # interpreter), says whether the runtime lives inside the checkout, as
    # written or resolved (the checkout's .venv may itself be a symlink).
    bin_dir = Path(runtime).parent
    if not (bin_dir.is_relative_to(checkout) or bin_dir.resolve().is_relative_to(Path(checkout).resolve())):
        remedy += (
            f"; the runtime {runtime} is outside that checkout, so reinstall it from there "
            f"(e.g. `uv tool install --force --editable {checkout}/packages/workbay "
            "--with-executables-from workbay-bootstrap`)"
        )
    refuse(
        "editable_source",
        f"{what} is an editable checkout; reinstalling it would replace it with static copies",
        remedy=remedy + f", then run `workbay-bootstrap update --target {target_arg}`.",
    )


manifest_path, target_arg, runtime, bootstrap, dry_run, requested_ref = sys.argv[1:]
try:
    from workbay_bootstrap.gitonly_closure import (
        GITONLY_BRIDGE_MEMBER,
        GITONLY_FRONT_DOOR,
        GITONLY_RUNTIME_MEMBERS,
        _directory_field_to_member_spec,
        _git_url_field_to_member_spec,
        build_uv_tool_install_argv,
        member_specs_from_git_ref,
        member_specs_from_uv_receipt,
    )
    from workbay_bootstrap.install import (
        SCHEMA_VERSION,
        _closure_sibling_spec,
        _persisted_embeddings_opted_out,
        _resolve_git_overlay_remote_url,
        validate_stack_source,
    )
    from workbay_bootstrap.subcommands import _persisted_gitonly_closure_pin
except ImportError as exc:
    refuse("bootstrap_unavailable", f"the owning runtime lacks the workbay_bootstrap closure helpers ({exc})")

# The members base's workspace maps: runtime members, front door, codex bridge.
MEMBERS = (*GITONLY_RUNTIME_MEMBERS, GITONLY_FRONT_DOOR, GITONLY_BRIDGE_MEMBER)


def with_front_door(specs):
    specs[GITONLY_FRONT_DOOR] = _closure_sibling_spec(
        specs["workbay-bootstrap"], "workbay-bootstrap", GITONLY_FRONT_DOOR
    )
    return specs


def validate_paths(specs):
    for name, spec in specs.items():
        path = Path(spec)
        if not path.is_absolute() or not (path / "pyproject.toml").is_file():
            refuse("no_grounded_source", f"missing local source for {name}: {spec}")
        project = tomllib.loads((path / "pyproject.toml").read_text(encoding="utf-8")).get("project", {})
        if project.get("name") != name:
            refuse("invalid_source", f"local source {spec} does not name {name}")


def validate_git_pin(url, ref):
    """Refuse a partial or selector-bearing Git pin; return the URL to install from."""
    if not isinstance(url, str) or not url.strip() or not isinstance(ref, str) or not ref.strip():
        refuse("invalid_source", "the recorded Git pin is partial: both a URL and a ref are required")
    if Path(url).is_absolute():
        # A local mirror recorded as a filesystem path; relative paths stay refused.
        url = Path(url).as_uri()
    # SSH shorthand is a Git source; a bare name or a URL carrying a query or
    # fragment (which could override the member subdirectory/ref) is not.
    parsed = urlparse(url)
    if not (parsed.scheme in {"https", "http", "ssh", "file"} or re.fullmatch(r"[^\s/:]+@[^\s/:]+:.+", url)):
        refuse("invalid_source", f"{url!r} is not an explicit Git URL")
    if parsed.scheme in {"https", "http", "ssh"} and not parsed.netloc:
        refuse("invalid_source", "the Git URL has no host")
    if parsed.scheme == "file" and not Path(unquote(parsed.path)).is_absolute():
        refuse("invalid_source", "a file:// Git URL must be absolute")
    if any(c.isspace() for c in url + ref) or "#" in url or "?" in url or any(c in ref for c in "#?@"):
        refuse("invalid_source", "invalid Git URL/ref selector")
    return url


def git_specs(url, ref):
    url = validate_git_pin(url, ref)
    return with_front_door(member_specs_from_git_ref(repo_url=url, tag=ref))


def ledger_pin(manifest, target):
    """The manifest's explicit closure pin, or None when none is recorded."""
    url, ref = _persisted_gitonly_closure_pin(manifest)
    raw = (
        manifest.get("gitonly_closure_url", manifest.get("remote_url")),
        manifest.get("gitonly_closure_ref", manifest.get("remote_ref")),
    )
    if all(value is None for value in raw):
        return None
    if raw[0] is None and ref:
        # The default `install --remote-ref REF` records no URL; install
        # resolves it (managed clone origin, manifest, bootstrap default), so
        # the update resolves it the same way. A URL without a ref still fails.
        url = _resolve_git_overlay_remote_url(target, None)
    return validate_git_pin(url, ref), ref


def receipt_anchor_spec(row):
    if isinstance(row.get("git"), str):
        return _git_url_field_to_member_spec(GITONLY_FRONT_DOOR, row["git"])
    return _directory_field_to_member_spec(GITONLY_FRONT_DOOR, row.get("directory", ""))


def grounded_anchor(specs, anchor_spec):
    expected = with_front_door(dict(specs))[GITONLY_FRONT_DOOR]
    if anchor_spec != expected:
        refuse("invalid_source", "the recorded workbay anchor contradicts its member closure")
    specs[GITONLY_FRONT_DOOR] = expected
    return specs


def uv_receipt_specs(receipt):
    entries = tomllib.loads(receipt.read_text(encoding="utf-8"))["tool"]["requirements"]
    if not isinstance(entries, list):
        refuse("invalid_source", "the owning uv receipt has no requirements list")
    known = [e for e in entries if isinstance(e, dict) and e.get("name") in MEMBERS]
    names = [e["name"] for e in known]
    if len(names) != len(set(names)) or any("git" in e and "directory" in e for e in known):
        refuse("invalid_source", "the owning uv receipt has ambiguous member sources")
    specs = member_specs_from_uv_receipt(receipt)
    if specs is None:
        refuse("invalid_source", "the owning uv receipt is incomplete or contradictory")
    # The receipt reader validates the runtime siblings but does not return the
    # anchor; the anchor's own row must name the same closure.
    anchor = [e for e in known if e["name"] == GITONLY_FRONT_DOOR]
    if len(anchor) != 1:
        refuse("invalid_source", "the owning uv receipt lacks one grounded workbay anchor")
    return grounded_anchor(specs, receipt_anchor_spec(anchor[0]))


def pep610_specs():
    # pipx/pip runtimes carry PEP 610 direct_url.json receipts. A complete
    # Git closure must share one URL and commit; a directory closure goes
    # through the receipt reader.
    entries, git_pins, artifact_specs, artifact_roots = [], set(), {}, set()
    for name in MEMBERS:
        try:
            raw = metadata.distribution(name).read_text("direct_url.json")
        except metadata.PackageNotFoundError:
            if name == GITONLY_BRIDGE_MEMBER:
                continue  # optional orchestrator extra; derived from the closure
            refuse("no_grounded_source", f"{name} is not installed in the owning runtime")
        data = json.loads(raw) if raw else {}
        url = data.get("url", "")
        parsed = urlparse(url)
        vcs = data.get("vcs_info", {})
        if vcs.get("vcs") == "git":
            ref = vcs.get("commit_id")
            url = validate_git_pin(url, ref)
            if not re.fullmatch(r"[0-9a-f]{40}", ref):
                refuse("invalid_source", f"the Git receipt for {name} lacks an exact commit")
            if data.get("subdirectory") != f"packages/{name}":
                refuse("invalid_source", f"the Git receipt for {name} names the wrong member subdirectory")
            git_pins.add((url, ref))
        elif parsed.scheme == "file" and "dir_info" in data:
            if parsed.netloc not in {"", "localhost"}:
                refuse("invalid_source", f"the directory receipt for {name} is not local")
            directory = Path(unquote(parsed.path))
            if data["dir_info"].get("editable"):
                checkout = directory.parent.parent if directory.parent.name == "packages" else directory
                refuse_editable(f"the installed {name}", checkout)
            entries.append({"name": name, "directory": str(directory)})
        elif parsed.scheme == "file" and "archive_info" in data:
            path = Path(unquote(parsed.path)).resolve()
            digest = data["archive_info"].get("hashes", {}).get("sha256")
            if parsed.netloc not in {"", "localhost"} or not path.is_file() or path.suffix != ".whl":
                refuse("invalid_source", f"the artifact receipt for {name} names no existing local wheel")
            if not digest or hashlib.sha256(path.read_bytes()).hexdigest() != digest:
                refuse("invalid_source", f"the artifact hash for {name} is missing or mismatched")
            with zipfile.ZipFile(path) as wheel:
                metas = [n for n in wheel.namelist() if n.endswith(".dist-info/METADATA")]
                if len(metas) != 1:
                    refuse("invalid_source", f"the artifact metadata for {name} is ambiguous")
                declared = Parser().parsestr(wheel.read(metas[0]).decode())["Name"]
            if re.sub(r"[-_.]+", "-", declared or "").lower() != name:
                refuse("invalid_source", f"the artifact for {name} names another distribution")
            artifact_roots.add(path.parent)
            artifact_specs[name] = f"{name} @ {path.as_uri()}"
        else:
            refuse("no_grounded_source", f"{name} has no Git, directory or local-artifact receipt")
    if artifact_specs:
        # An artifact closure has no source to derive a missing member from.
        if entries or git_pins or len(artifact_roots) != 1 or set(artifact_specs) != set(MEMBERS):
            refuse("invalid_source", "the local artifact closure is mixed or incomplete")
        return artifact_specs
    if git_pins:
        # Build the specs from the receipt's URL and commit: round-tripping a
        # git+ spec needs a host, which a file:// mirror does not have. Every
        # installed member, the anchor included, shares the one pin.
        if entries or len(git_pins) != 1:
            refuse("invalid_source", "the owning runtime's receipts are mixed or incomplete")
        url, ref = git_pins.pop()
        return with_front_door(member_specs_from_git_ref(repo_url=url, tag=ref))
    # The temporary TOML only adapts the receipts to the canonical coherence
    # reader; no receipt or runtime is modified.
    with tempfile.TemporaryDirectory(prefix="workbay-update-source-") as temp:
        receipt = Path(temp) / "uv-receipt.toml"
        receipt.write_text(
            "[tool]\nrequirements = [\n"
            + ",\n".join("{" + ", ".join(f"{k} = {json.dumps(v)}" for k, v in e.items()) + "}" for e in entries)
            + "\n]\n",
            encoding="utf-8",
        )
        specs = member_specs_from_uv_receipt(receipt)
    if specs is None:
        refuse("invalid_source", "the owning runtime's receipts are mixed or incomplete")
    anchor = next(e for e in entries if e["name"] == GITONLY_FRONT_DOOR)
    return grounded_anchor(specs, str(Path(anchor["directory"]).resolve()))


def owning_receipt_specs():
    # Only the owning runtime's own receipt: a default uv tools root could
    # belong to a different interpreter.
    receipt = Path(runtime).parent.parent / "uv-receipt.toml"
    return uv_receipt_specs(receipt) if receipt.exists() else pep610_specs()


try:
    manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    if not isinstance(manifest, dict) or manifest.get("schema_version") != SCHEMA_VERSION:
        refuse("invalid_manifest", "unsupported manifest schema; re-run workbay-bootstrap install to regenerate it")
    if "stack_source" in manifest:
        validate_stack_source(manifest["stack_source"])
    source_kind = manifest.get("source_kind") or "git_overlay"
    target = Path(target_arg).resolve()
    if source_kind not in {"package", "worktree", "git_overlay"}:
        refuse("invalid_manifest", f"unrecognized source_kind={source_kind!r}")
    if source_kind == "git_overlay" and not requested_ref:
        refuse("remote_ref_required", "this overlay is git_overlay-sourced; pass REMOTE_REF=<tag> to update it",
               USAGE_STATUS)
    if source_kind != "git_overlay" and requested_ref:
        refuse("remote_ref_invalid", f"REMOTE_REF is invalid for source_kind={source_kind}; replace the pin "
               "with install --remote-url/--remote-ref instead", USAGE_STATUS)

    # Every local availability check precedes the source resolution (a URL
    # resolver may read the managed clone) and the FastMCP pin resolution
    # below, which may clone the recorded ref.
    pipx_owner = None
    if "/pipx/venvs/" in bootstrap or "/pipx/venvs/" in runtime:
        if not shutil.which("pipx"):
            refuse("adapter_unavailable", "the runtime is pipx-owned but pipx is not on PATH")
        pipx_owner = Path(runtime if "/pipx/venvs/" in runtime else bootstrap).parent.parent.name
        adapter = "pipx inject"
    elif shutil.which("uv"):
        adapter = "uv pip (runtime-pinned)"
    else:
        adapter = "python -m pip (runtime-pinned)"

    if source_kind == "worktree":
        refuse_editable(f"the worktree source {target}", target)
    elif source_kind == "git_overlay":
        # install resolves a missing URL (managed clone origin, manifest,
        # bootstrap default) before writing the overlay; resolve it the same way.
        specs = git_specs(_resolve_git_overlay_remote_url(target, manifest.get("remote_url")), requested_ref)
    else:
        pin = ledger_pin(manifest, target)
        specs = git_specs(*pin) if pin else owning_receipt_specs()
    if not specs["workbay-bootstrap"].startswith(("git+", "workbay-bootstrap @ ")):
        validate_paths(specs)

    if dry_run:
        # A preview never clones: the declared FastMCP pin comes from the
        # native resolver's local sources only (a local member checkout, the
        # installed handoff metadata), else install resolves it from the ref.
        import workbay_bootstrap.gitonly_closure as closure
        from workbay_bootstrap._declared_fastmcp import resolve_declared_fastmcp_requirement

        def preview_fastmcp_requirement(member_specs=None):
            local = {k: v for k, v in (member_specs or {}).items() if not v.startswith("git+")}
            return resolve_declared_fastmcp_requirement(local) or "<fastmcp pin resolved at install time>"

        closure._require_declared_fastmcp_requirement = preview_fastmcp_requirement

    # The native installer owns the closure argv and the declared FastMCP pin;
    # reuse its --with/--from requirements for this runtime's adapter.
    argv = build_uv_tool_install_argv(package=GITONLY_FRONT_DOOR, from_spec=specs[GITONLY_FRONT_DOOR],
                                      member_specs=specs)
    requirements = [argv[i + 1] for i, value in enumerate(argv) if value in {"--with", "--from"}]
    requirements.append(specs[GITONLY_BRIDGE_MEMBER])
    owner_at = requirements.index(specs[pipx_owner]) if pipx_owner in specs else None
    if manifest.get("embeddings_mode") == "verified" and not _persisted_embeddings_opted_out(target):
        handoff = specs["mcp-workbay-handoff"]
        source = handoff.removeprefix("mcp-workbay-handoff @ ")
        if Path(source).is_absolute():
            source = Path(source).as_uri()
        requirements[requirements.index(handoff)] = "mcp-workbay-handoff[embeddings] @ " + source

    commands = []
    if pipx_owner is not None:
        # pipx inject installs each spec in its own pip call, in sorted order,
        # so the front door's `==` member pins would be resolved by name before
        # those members are reinstalled. Install the whole closure (an owner
        # extra included, so its dependencies resolve) in ONE resolution first;
        # the pipx commands below then only record already-satisfied packages.
        commands.append(["pipx", "runpip", pipx_owner, "install", "--upgrade", *requirements])
        if owner_at is not None:
            # The venv's main package is itself a member: an inject would also
            # record it as injected, so reinstall it as the main package.
            # --no-deps: its dependencies come from the resolution above,
            # never from resolving their names.
            commands.append(["pipx", "install", "--force", "--pip-args=--no-deps", requirements.pop(owner_at)])
        # --force: pipx skips a package it already tracks, so without it the
        # anchor could keep its previous build instead of the recorded source.
        commands.append(["pipx", "inject", "--force", "--include-apps", pipx_owner, *requirements,
                         "--pip-args=--upgrade"])
    elif adapter.startswith("uv pip"):
        commands.append(["uv", "pip", "install", "--no-sources", "--python", runtime, "--upgrade", *requirements])
    else:
        commands.append([runtime, "-P", "-m", "pip", "install", "--upgrade", *requirements])
except (OSError, ValueError, KeyError, TypeError, AttributeError, RuntimeError, zipfile.BadZipFile) as exc:
    refuse("invalid_source", str(exc))

print(f"workbay-update: adapter: {adapter}", flush=True)
for applied, command in enumerate(commands):
    print("workbay-update: + " + shlex.join(command), flush=True)
    if dry_run:
        continue
    try:
        status = subprocess.run(command).returncode
    except OSError as exc:
        print(f"workbay-update: ERROR: cannot run {command[0]}: {exc}", file=sys.stderr)
        raise SystemExit(1)
    if status:
        # The adapter's own status (uv exits 2 on install errors) would
        # collide with the REMOTE_REF usage status, so report it and exit 4.
        message = f"adapter_failed: {command[0]} exited with status {status}; the overlay was not updated"
        if command[:2] == ["pipx", "install"]:
            # pipx removes the owner venv when its install fails, so a re-run
            # stops at "workbay-bootstrap not on PATH": reinstall the closure first.
            message += (f"; pipx removes the {pipx_owner} venv when its install fails: reinstall the "
                        "closure (docs/CONSUMER.md), then re-run update.sh")
        elif applied:
            message += f"; {applied} earlier step(s) already applied: re-run update.sh to converge"
        print(f"workbay-update: ERROR: {message}", file=sys.stderr)
        raise SystemExit(ADAPTER_STATUS)
PY

# Verify the anchor and its exact member pins resolve in the SAME runtime before
# touching the repo.
if [ -z "$DRY_RUN" ]; then
    "$RUNTIME_PY" -P - "$STACK_DIST" <<'PY'
import importlib.metadata as metadata
import re
import sys

stack_dist = sys.argv[1]
pin_re = re.compile(r"^\s*([A-Za-z0-9._-]+)\s*==\s*([^\s;]+)\s*$")

try:
    stack_version = metadata.version(stack_dist)
except metadata.PackageNotFoundError:
    raise SystemExit(
        f"{stack_dist} did not install into this runtime; adapter mismatch"
    )

pins = []
for requirement in metadata.requires(stack_dist) or []:
    match = pin_re.match(requirement)
    if match:
        pins.append(match.groups())

if not pins:
    raise SystemExit(f"{stack_dist} metadata has no exact member pins")

errors = []
for distribution, expected in pins:
    try:
        installed = metadata.version(distribution)
    except metadata.PackageNotFoundError:
        errors.append(f"{distribution} not installed (requires =={expected})")
        continue
    if installed != expected:
        errors.append(f"{distribution} installed {installed} != required {expected}")

if errors:
    raise SystemExit(
        "stack member verification failed:\n  " + "\n  ".join(errors)
    )

print(
    f"workbay-update: verified {stack_dist}=={stack_version} "
    f"with {len(pins)} pinned member(s)"
)
PY
fi

# --- 3. refresh the overlay from its recorded source -----------------------
SOURCE_KIND="$("$RUNTIME_PY" -P -c "
import json, sys
print(json.load(open(sys.argv[1])).get('source_kind') or 'git_overlay')
" "$MANIFEST")"

# Step 2 refuses a worktree source (exit 3); this guard is defence-in-depth for a
# manifest rewritten to a worktree between the two reads.
if [ "$SOURCE_KIND" = "package" ]; then
    run "$BOOTSTRAP_BIN" update --target "$TARGET"
elif [ "$SOURCE_KIND" = "worktree" ]; then
    fail "source_kind changed to worktree during the update; re-run update.sh"
elif [ -n "${REMOTE_REF:-}" ]; then
    run "$BOOTSTRAP_BIN" update --target "$TARGET" --remote-ref "$REMOTE_REF"
else
    log "this overlay is git_overlay-sourced; pass REMOTE_REF=<tag> to update it,"
    log "or migrate to the package source (recommended):"
    log "    workbay-bootstrap install --source package --target $TARGET"
    exit 2
fi

# --- 4 + 5. doctor, then the version table ---------------------------------
DOCTOR_STATUS=0
run "$BOOTSTRAP_BIN" doctor --target "$TARGET" || DOCTOR_STATUS=$?

"$RUNTIME_PY" -P -c "
import importlib.metadata as metadata
import json
import sys

manifest = json.load(open(sys.argv[1]))
rows = [('workbay-system', manifest.get('package_version'))]
if manifest.get('stack_distribution'):
    rows.append((manifest['stack_distribution'], manifest.get('stack_version')))
    rows.extend(sorted((manifest.get('stack_members') or {}).items()))
print('workbay-update: stack versions (installed / manifest):')
seen = set()
for dist, recorded in rows:
    if dist in seen:
        continue
    seen.add(dist)
    try:
        installed = metadata.version(dist)
    except metadata.PackageNotFoundError:
        installed = 'NOT INSTALLED'
    marker = '' if installed == (recorded or installed) else '  <-- drift'
    print(f'  {dist:32} {installed:12} / {recorded or \"-\"}{marker}')
" "$MANIFEST" || log "version table unavailable (manifest unreadable)"
# ^ informational only — must not let set -e preempt the doctor exit code.

exit "$DOCTOR_STATUS"
