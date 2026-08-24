#!/usr/bin/env python3
"""implementation note — dev-install bypass: live unscrubbed redirects for WorkBay members."""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import stat
import subprocess
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path

_SCRIPTS_DIR = Path(__file__).resolve().parent
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))
from workspace_members import iter_workspace_members  # noqa: E402


@dataclass(frozen=True)
class DevEditableMember:
    package_relpath: str
    dist_name: str
    import_names: tuple[str, ...]
    path_entry: str  # repo-relative segment written into .pth (resolved at install)
    exempt: bool = False
    exempt_reason: str = ""


# Membership + import surface are DERIVED from the single registry
# (scripts/workspace_members.py), so this list cannot drift from pyproject. Only
# the dev-install redirect *policy* lives here: members with no dedicated
# live-editable redirect stay copy-editable. ``workbay`` is the umbrella
# meta-dist (the version-anchor pins).
_COPY_ONLY_EXEMPTIONS: dict[str, str] = {
    "workbay": "umbrella meta-dist; copy-editable only (no dedicated redirect)",
}


def _build_members() -> tuple[DevEditableMember, ...]:
    members: list[DevEditableMember] = []
    for member in iter_workspace_members(_SCRIPTS_DIR.parent):
        reason = _COPY_ONLY_EXEMPTIONS.get(member.dist_name, "")
        members.append(
            DevEditableMember(
                package_relpath=member.package_relpath,
                dist_name=member.dist_name,
                import_names=member.import_names,
                path_entry=member.src_relpath,
                exempt=bool(reason),
                exempt_reason=reason,
            )
        )
    return tuple(members)


MEMBERS: tuple[DevEditableMember, ...] = _build_members()



# ---------------------------------------------------------------------------
# Validated repo-root ascent.
#
# Canonical source (keep the three copies findable together) [REF-20]:
#   packages/mcp-workbay-handoff/src/workbay_handoff_mcp/backlog_triage.py
#   (_validate_headref / _is_git_directory / _ascend_to_repo_root)
# Duplicated here on purpose: this entrypoint must run before any package is
# installed, so it cannot import the handoff helper.
# ---------------------------------------------------------------------------

_GIT_HEADREF_WS = " \t\n\r"


def _validate_headref(head: Path) -> bool:
    """Port of git's ``validate_headref`` (refs.c): is *head* a well-formed HEAD.

    Accepts a symlink whose link text begins ``refs/`` (unresolved — a dangling
    symlink to ``refs/heads/main`` is a valid unborn branch), a regular file
    whose first 40 characters are hexadecimal (detached HEAD), or a regular
    file beginning ``ref:`` whose remainder, after leading space/tab/LF/CR only,
    begins ``refs/``. ``PermissionError`` is treated as success (unreadable entry
    pins as a repository; deliberate divergence from git, which walks past —
    this helper has no fatal error channel); other ``OSError`` fails.
    """
    try:
        st = os.lstat(head)
    except PermissionError:
        return True
    except OSError:
        return False

    mode = st.st_mode
    if stat.S_ISLNK(mode):
        try:
            link = os.readlink(head)
        except PermissionError:
            return True
        except OSError:
            return False
        return link.startswith("refs/")

    if not stat.S_ISREG(mode):
        return False

    try:
        with open(head, "rb") as fh:
            data = fh.read(255)
    except PermissionError:
        return True
    except OSError:
        return False

    text = data.decode("utf-8", errors="replace")
    if len(text) >= 40 and all(c in "0123456789abcdefABCDEF" for c in text[:40]):
        return True
    if text.startswith("ref:"):
        return text[4:].lstrip(_GIT_HEADREF_WS).startswith("refs/")
    return False


def _is_git_directory(suspect: Path) -> bool:
    """Port of git's ``is_git_directory`` (setup.c): three filesystem checks.

    Requires a well-formed ``HEAD`` via :func:`_validate_headref`, then that
    ``objects`` and ``refs`` are accessible under the *common* directory with
    ``os.access(..., X_OK)`` (git's probe — executable bit, not is-dir). The
    common directory defaults to *suspect* only when ``suspect/commondir`` is
    absent. When the entry is present it must be a readable regular file
    (after following a symlink): a zero-length file is refused (git dies with
    ``failed to read commondir``); otherwise the whole body is read and a
    trailing run of CR/LF only is stripped. A non-empty result names the
    common directory, resolved against *suspect* when relative; a body that
    is only trailing CR/LF leaves the default (*suspect*) in place (git
    accepts that shape). A present but unusable entry rejects the candidate
    (returns False): non-regular target (directory, FIFO, whether named
    directly or reached through a link — so ``open`` never blocks), broken
    symlink, undecodable body, or any other read/probe failure. Only a missing
    ``commondir`` entry keeps the default common directory. Linked worktrees
    hold ``HEAD`` + ``commondir`` without local ``objects``/``refs``; submodule
    gitdirs under ``.git/modules`` hold all three locally with no
    ``commondir``. ``PermissionError`` while reading ``commondir`` is the sole
    deliberate pin (returns True; git walks past — this helper has no fatal
    error channel); every other present-but-unusable probe failure rejects.
    """
    if not _validate_headref(suspect / "HEAD"):
        return False

    common = suspect
    try:
        commondir = suspect / "commondir"
        try:
            # Existence without following: a missing entry keeps the default
            # common dir. A present entry that cannot be opened as a regular
            # file must reject — not fall through as if the entry were absent.
            os.lstat(commondir)
        except FileNotFoundError:
            pass
        else:
            # Dereference deliberately: git's open() follows a symlink to a
            # regular file. S_ISREG still refuses a FIFO target (stat never
            # blocks on a FIFO; only open does), so the long-lived-server
            # hang guard survives for both a named FIFO and a link to one.
            try:
                cd_st = os.stat(commondir)
            except FileNotFoundError:
                # Broken symlink: entry exists, target does not.
                return False
            if not stat.S_ISREG(cd_st.st_mode):
                return False
            # Zero-length file: git refuses with "failed to read commondir".
            # Newlines-only (non-zero size, empty after CR/LF strip) keeps the
            # default common dir and must not take this arm.
            if cd_st.st_size == 0:
                return False
            with open(commondir, "rb") as fh:
                raw = fh.read().decode("utf-8")
            # Whole file, trailing CR/LF run only (not a first-line read; not
            # spaces/tabs). Matches git's commondir parse.
            body = raw.rstrip("\r\n")
            if body:
                common_path = Path(body)
                common = common_path if common_path.is_absolute() else suspect / common_path
    except PermissionError:
        return True
    except (OSError, UnicodeDecodeError, ValueError):
        # ValueError: NUL in the path must not abort the walk (not an OSError).
        # Present-but-unusable commondir rejects the candidate (git: failed to
        # read commondir); only PermissionError above is the deliberate pin.
        return False

    for name in ("objects", "refs"):
        try:
            # git probes with access(X_OK): accepts an executable regular file,
            # rejects mode-644 files and unsearchable (mode-000) directories.
            # os.access returns False on permission failure rather than raising.
            if not os.access(common / name, os.X_OK):
                return False
        except (OSError, ValueError):
            return False
    return True


def _ascend_to_repo_root(start: Path) -> Path | None:
    """Walk up from *start* and return the first ancestor that is a real repo.

    Ports the core of git's ``is_git_directory`` / ``validate_headref`` so litter
    ``.git`` shapes git refuses do not truncate the walk. Returns ``None`` when
    no repository is found (callers choose their own fallback).

    Deliberate divergence from git, carried from the canonical source: a
    ``PermissionError`` while reading ``commondir`` pins the candidate (returns
    True from :func:`_is_git_directory`) because this helper has no fatal error
    channel; git walks past.
    """
    try:
        base = start.resolve()
    except (OSError, RuntimeError, ValueError):
        return None
    for candidate in (base, *base.parents):
        try:
            git_entry = candidate / ".git"
            try:
                entry_st = os.lstat(git_entry)
            except FileNotFoundError:
                continue
            except PermissionError:
                # Unreadable .git (or unsearchable candidate) pins only when
                # the candidate itself is reachable. PermissionError under a
                # deeper path means an unreadable *ancestor* — keep walking
                # until that ancestor is the candidate.
                try:
                    os.lstat(candidate)
                except PermissionError:
                    continue
                except OSError:
                    continue
                return candidate
            except OSError:
                continue

            entry_mode = entry_st.st_mode
            if stat.S_ISLNK(entry_mode):
                # A .git symlink denotes a repository when it resolves, so
                # the type comes from a dereferencing probe; lstat above
                # reports the link itself and matches neither arm below.
                try:
                    entry_mode = os.stat(git_entry).st_mode
                except FileNotFoundError:
                    continue
                except PermissionError:
                    return candidate
                except OSError:
                    continue

            # Linked worktree: .git is a file with a gitdir: pointer.
            if stat.S_ISREG(entry_mode):
                try:
                    # Binary read so universal-newlines cannot swallow CR as
                    # a newline; we only rstrip an explicit CR/LF run below.
                    text = git_entry.read_bytes().decode("utf-8")
                except PermissionError:
                    return candidate
                except (OSError, UnicodeDecodeError):
                    continue
                # Exact gitfile prefix at byte zero (git rejects all variants).
                # Whole buffer after prefix; strip only a trailing CR/LF run
                # (spaces/tabs stay; interior newlines stay — same as
                # commondir). First-line partition would false-accept a valid
                # path followed by a second-line garbage that git refuses.
                text = text.rstrip("\r\n")
                if not text.startswith("gitdir: "):
                    continue
                payload = text[len("gitdir: ") :]
                if not payload:
                    continue
                try:
                    target = Path(payload)
                    if not target.is_absolute():
                        target = candidate / target
                    if not _is_git_directory(target):
                        continue
                except (OSError, ValueError):
                    continue
                return candidate

            # Primary checkout: .git is a directory; require is_git_directory
            # (well-formed HEAD + common objects/refs), not mere HEAD presence.
            if stat.S_ISDIR(entry_mode):
                if not _is_git_directory(git_entry):
                    continue
                return candidate
        except OSError:
            continue
    return None


def repo_root(start: Path | None = None) -> Path:
    """Locate the enclosing repository root via validated ``.git`` ascent.

    Uses the same predicate family as the lifecycle re-exec walker so a litter
    ``.git`` cannot redirect editable-install paths at the wrong tree.
    """
    cur = (start or Path.cwd()).resolve()
    found = _ascend_to_repo_root(cur)
    if found is not None:
        return found
    raise RuntimeError("could not locate repo root")


def site_packages(venv_root: Path) -> Path:
    py = venv_root / "bin" / "python"
    proc = subprocess.run(
        [str(py), "-c", "import site; print(site.getsitepackages()[0])"],
        check=True,
        capture_output=True,
        text=True,
    )
    return Path(proc.stdout.strip())


def dist_info_dir(site: Path, dist_name: str) -> Path | None:
    normalized = dist_name.replace("-", "_").lower()
    matches = sorted(site.glob(f"{normalized}-*.dist-info"))
    return matches[0] if matches else None


def _dist_info_name(dist_name: str, version: str) -> str:
    normalized_name = re.sub(r"[-_.]+", "_", dist_name).lower()
    normalized_version = re.sub(r"[^A-Za-z0-9.]+", "_", version)
    return f"{normalized_name}-{normalized_version}.dist-info"


def ensure_dist_metadata(*, site: Path, repo: Path, member: DevEditableMember) -> None:
    pyproject = repo / member.package_relpath / "pyproject.toml"
    data = tomllib.loads(pyproject.read_text(encoding="utf-8"))
    project = data["project"]
    version = project["version"]
    info = site / _dist_info_name(member.dist_name, version)

    if info.is_dir():
        return

    normalized = member.dist_name.replace("-", "_").lower()
    for stale in site.glob(f"{normalized}-*.dist-info"):
        shutil.rmtree(stale)

    info.mkdir()

    metadata = [
        "Metadata-Version: 2.1",
        f"Name: {project['name']}",
        f"Version: {version}",
    ]
    for dependency in project.get("dependencies", []):
        metadata.append(f"Requires-Dist: {dependency}")
    (info / "METADATA").write_text("\n".join(metadata) + "\n", encoding="utf-8")
    scripts = project.get("scripts", {})
    if scripts:
        lines = ["[console_scripts]"]
        lines.extend(f"{name} = {target}" for name, target in scripts.items())
        (info / "entry_points.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")
    (info / "INSTALLER").write_text("workbay-dev-install\n", encoding="utf-8")
    (info / "RECORD").write_text("", encoding="utf-8")


def remove_installed_copy(site: Path, member: DevEditableMember) -> None:
    for import_name in member.import_names:
        for target in (site / import_name, site / f"{import_name}.py"):
            if target.exists():
                if target.is_dir():
                    shutil.rmtree(target)
                else:
                    target.unlink()
        for pth in site.glob(f"*{import_name}*.pth"):
            pth.unlink()


def redirect_pth_line(*, site: Path, src: Path) -> str:
    """Build the ``.pth`` line that points an import name at its in-tree src.

    A ``.pth`` holding a bare path is *appended* to ``sys.path``, so it loses to
    anything already in site-packages. ``remove_installed_copy`` clears the copy
    here, but ``uv sync`` re-materializes it whenever the dist-info ``RECORD``
    still lists the module files — and the appended redirect is then powerless.
    Importing the stale copy is what wedges the MCP with
    ``schema_version_mismatch`` when its ``HANDOFF_SCHEMA_VERSION`` trails the DB.

    So emit an executable line instead, inserting ``src`` immediately *before*
    site-packages. Deliberately not ``insert(0, ...)``: the ``workbay-system``
    entry is a package root exposing generic top-level names (``config``,
    ``docs``, ``scripts``, ``tests``), and hoisting it above cwd would silently
    re-point ``import scripts`` / ``import tests`` for the whole repo. Beating
    site-packages is the entire requirement; the fallback keeps today's append
    semantics for the unreachable case where site-packages is not on the path.
    """
    site_repr, src_repr = repr(str(site)), repr(str(src))
    return (
        "import sys; sys.path.insert("
        f"sys.path.index({site_repr}) if {site_repr} in sys.path else len(sys.path), {src_repr})\n"
    )


def install_member_redirect(*, repo: Path, venv_root: Path, member: DevEditableMember) -> Path | None:
    if member.exempt:
        return None
    site = site_packages(venv_root)
    remove_installed_copy(site, member)
    ensure_dist_metadata(site=site, repo=repo, member=member)
    pth = site / f"zz_dev_redirect_{member.import_names[0]}.pth"
    pth.write_text(
        redirect_pth_line(site=site, src=(repo / member.path_entry).resolve()),
        encoding="utf-8",
    )
    return pth


def install_all(*, repo: Path, venv_root: Path) -> list[dict[str, str]]:
    results: list[dict[str, str]] = []
    for member in MEMBERS:
        if member.exempt:
            results.append({"member": member.dist_name, "status": "exempt", "reason": member.exempt_reason})
            continue
        pth = install_member_redirect(repo=repo, venv_root=venv_root, member=member)
        results.append({"member": member.dist_name, "status": "redirect", "pth": str(pth)})
    return results


def probe_import_is_live(*, venv_root: Path, import_name: str) -> bool:
    py = venv_root / "bin" / "python"
    script = (
        "import importlib, inspect; "
        f"mod = importlib.import_module({import_name!r}); "
        "path = inspect.getfile(mod).replace('\\\\', '/'); "
        "print('site-packages' not in path)"
    )
    proc = subprocess.run([str(py), "-c", script], check=True, capture_output=True, text=True)
    return proc.stdout.strip() == "True"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=None)
    parser.add_argument("--venv", type=Path, default=None)
    parser.add_argument("--emit-json", action="store_true")
    args = parser.parse_args(argv)
    repo = (args.repo or repo_root()).resolve()
    venv = (args.venv or repo / ".venv").resolve()
    if not venv.is_dir():
        raise SystemExit(f"venv not found: {venv}")
    payload = {"repo": str(repo), "venv": str(venv), "results": install_all(repo=repo, venv_root=venv)}
    if args.emit_json:
        print(json.dumps(payload, indent=2))
    else:
        for row in payload["results"]:
            print(f"{row['member']}: {row['status']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
