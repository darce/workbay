"""Candidate custody export: bundle first, report owned_paths scope as data (decision 48627 Fix C).

A committed remote candidate must reach custody outside its VM job dir before
any scope verdict is acted on. An ``owned_paths`` violation is therefore a
structured quarantine/review classification on a candidate that is already
bundled, never an export refusal that leaves the VM clone as the sole copy.

The bundle is a full bundle of the candidate (no prerequisites), so a fresh
repository can fetch it and ``git cat-file -e <sha>^{commit}`` proves custody;
``scripts/vm/workbay_vm_reclaim.py`` applies the same predicate to the
``CUSTODY.json`` claim written by :func:`write_custody_claim`, and the claim is
placed only where that gate's own ``custody_location_forbidden`` predicate allows.

Lane kits run without a venv, so this module is stdlib-only and doubles as a CLI::

    python3 candidate_custody.py export-and-claim REPO CANDIDATE [--base B] \\
        --owned-paths-file F --custody-root R --job-dir D

It exports, then claims, prints one JSON line and exits non-zero without a
claim when the export, the claim placement or the gate's own check fails.

The custody set is the gate's: ``custody_candidates(job_dir)`` plus the requested
candidate, all bundled together through temporary ``refs/workbay-custody/<sha>``
refs, and the staged claim must pass the gate's ``custody_verified`` before it is
renamed into place, so a claim the gate would refuse is never published [DATA-14].
"""

from __future__ import annotations

import argparse
import functools
import hashlib
import importlib.util
import json
import os
import subprocess
import sys
import tempfile
from collections.abc import Sequence
from pathlib import Path
from typing import Any

__all__ = ["CandidateCustodyError", "export_candidate", "write_custody_claim", "main"]

_GIT_TIMEOUT_SECONDS = 300
_GIT_ENV = {"GIT_CONFIG_NOSYSTEM": "1", "GIT_TERMINAL_PROMPT": "0"}
_HASH_CHUNK = 1024 * 1024
_RECLAIM_GATE = Path("scripts", "vm", "workbay_vm_reclaim.py")
_CUSTODY_REF_PREFIX = "refs/workbay-custody/"


class CandidateCustodyError(RuntimeError):
    """The candidate could not be bundled or verified; never raised for scope.

    The message is ``<code>[:<detail>]``; ``uncovered`` lists gate custody candidates
    a refused staged claim would not have covered.
    """

    def __init__(self, message: str, *, uncovered: Sequence[str] = ()) -> None:
        super().__init__(message)
        self.uncovered = list(uncovered)

    @property
    def code(self) -> str:
        return str(self).split(":", 1)[0]


def _git_env() -> dict[str, str]:
    """The caller's environment minus every variable that redirects git (the gate's own list)."""
    try:
        drop = set(_gate().CUSTODY_GIT_ENV_DROP)
    except CandidateCustodyError:
        # No gate list to share: drop every repository-locating GIT_* variable (a superset) [SECD-05].
        drop = {key for key in os.environ if key.startswith("GIT_") and not key.startswith(("GIT_CONFIG", "GIT_TRACE"))}
    return {**{key: value for key, value in os.environ.items() if key not in drop}, **_GIT_ENV}


def _git(*arguments: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    try:
        result = subprocess.run(
            ["git", *arguments],
            capture_output=True,
            text=True,
            timeout=_GIT_TIMEOUT_SECONDS,
            check=False,
            env=_git_env(),
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise CandidateCustodyError(f"git_unavailable:{type(exc).__name__}") from exc
    if check and result.returncode != 0:
        raise CandidateCustodyError(f"git_failed:rc={result.returncode}:{result.stderr.strip()[-300:]}")
    return result


def _fsync_dir(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(_HASH_CHUNK), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _reclaim_gate_paths() -> list[Path]:
    """Where the VM reclaim gate lives: the installed workbay-system payload, else this source checkout."""
    paths = []
    try:
        spec = importlib.util.find_spec("workbay_system")
    except (ImportError, ValueError):
        spec = None
    for location in (spec.submodule_search_locations or []) if spec is not None else []:
        paths.append(Path(location) / "payload" / _RECLAIM_GATE)
    here = Path(__file__).resolve().parents
    if len(here) > 5:
        paths += [here[4] / "workbay-system" / "workbay_system" / "payload" / _RECLAIM_GATE, here[5] / _RECLAIM_GATE]
    return paths


_GATE_API = ("custody_location_forbidden", "custody_candidates", "custody_verified", "CustodyUnknown")


@functools.lru_cache(maxsize=1)
def _gate() -> Any:
    """The reclaim gate module: its placement predicate, custody set, custody check and git env list."""
    for path in _reclaim_gate_paths():
        if not path.is_file():
            continue
        spec = importlib.util.spec_from_file_location("_workbay_custody_reclaim_gate", path)
        if spec is None or spec.loader is None:
            continue
        module = importlib.util.module_from_spec(spec)
        try:
            spec.loader.exec_module(module)
        except (OSError, SyntaxError, ImportError):
            continue
        if all(callable(getattr(module, name, None)) for name in _GATE_API) and isinstance(
            getattr(module, "CUSTODY_GIT_ENV_DROP", None), tuple
        ):
            return module
    # No gate predicate means placement and custody are unknown: refuse the claim [CAL-02].
    raise CandidateCustodyError("custody_placement_predicate_unavailable")


def _gate_location_forbidden() -> Any:
    """The reclaim gate's own placement predicate (one predicate, never a copy) [DATA-14]."""
    return _gate().custody_location_forbidden


def _gate_custody_candidates(job_dir: str | os.PathLike[str]) -> list[str]:
    """Every commit the gate will require custody for in ``job_dir`` (its own predicate)."""
    gate = _gate()
    try:
        return list(gate.custody_candidates(Path(job_dir).absolute()))
    except gate.CustodyUnknown as exc:
        raise CandidateCustodyError(f"custody_candidates_unknown:{exc}") from exc


def _verify_bundle(bundle: Path, shas: Sequence[str]) -> None:
    """Custody proof: a fresh repository fetches the bundle and holds every commit and its closure."""
    with tempfile.TemporaryDirectory(prefix=".verify-", dir=bundle.parent) as scratch:
        _git("init", "-q", "--bare", scratch)
        _git(f"--git-dir={scratch}", "fetch", "-q", str(bundle), "+refs/*:refs/*")
        for sha in shas:
            if _git(f"--git-dir={scratch}", "cat-file", "-e", f"{sha}^{{commit}}", check=False).returncode != 0:
                raise CandidateCustodyError(f"bundle_lacks_candidate:{sha}")
            if _git(f"--git-dir={scratch}", "rev-list", "--objects", "--quiet", sha, check=False).returncode != 0:
                raise CandidateCustodyError(f"bundle_lacks_content:{sha}")


def _resolve_commit(git_dir: Sequence[str], revision: str) -> str | None:
    """``revision`` as a commit sha, never parsed as an option (``--end-of-options``)."""
    resolved = _git(*git_dir, "rev-parse", "--verify", "-q", "--end-of-options", f"{revision}^{{commit}}", check=False)
    sha = resolved.stdout.strip()
    return sha if resolved.returncode == 0 and sha else None


def export_candidate(
    repo: str | os.PathLike[str],
    candidate: str,
    *,
    base: str | None,
    owned_paths: Sequence[str],
    custody_root: str | os.PathLike[str],
    job_dir: str | os.PathLike[str] | None = None,
) -> dict[str, Any]:
    """Bundle ``candidate`` into ``custody_root``, verify it, then classify its scope.

    With ``job_dir`` the bundle holds the gate's whole custody set for that job
    (``custody_candidates(job_dir)`` plus ``candidate``), each tip through a temporary
    ``refs/workbay-custody/<sha>`` ref removed again even on failure; a gate candidate
    ``repo`` lacks fails the export.

    Returns ``{candidate, candidates, base, base_commit, bundle, sha256, verified,
    owned_paths_violations, classification}``. ``owned_paths_violations`` lists changed paths (``base..candidate``)
    outside ``owned_paths``; a nonempty list yields the ``owned_paths_violation``
    quarantine classification on an already custodied bundle. When the scope cannot be
    computed (``base`` is None or not a commit (``base_invalid``), or the diff fails) the
    verified export is still returned, with ``owned_paths_violations`` None and the
    ``scope_unknown`` quarantine classification: unknown scope is never in scope [CAL-02].
    """
    git_dir = ["-C", os.fspath(repo)]
    sha = _resolve_commit(git_dir, candidate)
    if sha is None:
        raise CandidateCustodyError("candidate_not_a_commit")
    tips = sorted({sha, *(_gate_custody_candidates(job_dir) if job_dir is not None else ())})
    for tip in tips:
        if _git(*git_dir, "cat-file", "-e", f"{tip}^{{commit}}", check=False).returncode != 0:
            raise CandidateCustodyError(f"custody_candidate_absent_from_repo:{tip}", uncovered=[tip])
    root = Path(custody_root).absolute()
    root.mkdir(parents=True, exist_ok=True)
    bundle = root / f"{sha}.bundle"

    # A bundle needs refs: a branch at the candidate (when there is one) and a private
    # temporary ref for every custody tip, so one bundle carries the gate's whole set.
    branches = _git(*git_dir, "for-each-ref", "--format=%(refname)", "--points-at", sha, "refs/heads").stdout.split()
    created: list[tuple[str, str]] = []
    staged: Path | None = None
    try:
        for tip in tips:
            reference = f"{_CUSTODY_REF_PREFIX}{tip}"
            _git(*git_dir, "update-ref", reference, tip)
            created.append((reference, tip))
        descriptor, staged_name = tempfile.mkstemp(prefix=f".{sha}.", suffix=".bundle.tmp", dir=root)
        os.close(descriptor)
        staged = Path(staged_name)
        _git(*git_dir, "bundle", "create", "-q", str(staged), *branches[:1], *(ref for ref, _ in created))
        with staged.open("rb") as stream:
            os.fsync(stream.fileno())
        _verify_bundle(staged, tips)
        digest = _sha256_file(staged)
        os.replace(staged, bundle)
        _fsync_dir(root)
    finally:
        if staged is not None:
            staged.unlink(missing_ok=True)
        for reference, tip in created:
            _git(*git_dir, "update-ref", "-d", reference, tip, check=False)

    # Scope is evaluated only after custody exists, and only ever reported.
    violations: list[str] | None = None
    scope_error = "base_missing"
    base_commit = _resolve_commit(git_dir, base) if base else None
    if base and base_commit is None:
        scope_error = "base_invalid"
    elif base_commit is not None:
        try:
            changed = _git(
                *git_dir, "diff", "--no-ext-diff", "--no-renames", "--name-only", "-z", base_commit, sha, "--"
            ).stdout.split("\0")
        except CandidateCustodyError as exc:
            scope_error = f"scope_diff_failed:{exc}"[:500]
        else:
            owned = set(owned_paths)
            violations = sorted(path for path in changed if path and path not in owned)
    classification: dict[str, Any]
    if violations is None:
        classification = {
            "kind": "scope_unknown",
            "disposition": "quarantine",
            "review_finding": {
                "category": "scope_unknown",
                "reason": scope_error,
                "candidate": sha,
                "bundle": str(bundle),
            },
        }
    elif violations:
        classification = {
            "kind": "owned_paths_violation",
            "disposition": "quarantine",
            "review_finding": {
                "category": "owned_paths_violation",
                "paths": violations,
                "candidate": sha,
                "bundle": str(bundle),
            },
        }
    else:
        classification = {"kind": "in_scope", "disposition": "custodied"}
    return {
        "candidate": sha,
        "candidates": tips,
        "base": base,
        "base_commit": base_commit,
        "bundle": str(bundle),
        "sha256": digest,
        "verified": True,
        "owned_paths_violations": violations,
        "classification": classification,
    }


def write_custody_claim(job_dir: str | os.PathLike[str], *, owner: str, export: dict[str, Any]) -> Path:
    """Publish ``<job_dir>/CUSTODY.json`` for a verified export (the reclaim gate re-verifies it).

    The bundle must lie where the gate accepts custody: the gate's own
    ``custody_location_forbidden`` refuses this job dir, any VM job dir beside it and
    any reclaim tombstone. The claim is staged, then the gate's own ``custody_verified``
    checks it against the gate's custody set for this job (``custody_candidates`` plus
    the export's candidates); only a staged claim the gate accepts is renamed into
    place, so a claim the gate would reject is never published [CON-11][SECD-05].
    """
    if export.get("verified") is not True:
        raise CandidateCustodyError("custody_claim_requires_verified_export")
    job = Path(job_dir).absolute()
    bundle = Path(export["bundle"])
    gate = _gate()
    if not bundle.is_absolute() or gate.custody_location_forbidden(Path(os.path.realpath(bundle)), job, job.parent):
        raise CandidateCustodyError("custody_bundle_inside_job_dir")
    exported = sorted({export["candidate"], *(export.get("candidates") or ())})
    required = sorted({*_gate_custody_candidates(job), *exported})
    violations = export.get("owned_paths_violations", [])
    claim = {
        "schema": 1,
        "owner": owner,
        "candidates": required,
        "bundle": str(bundle),
        "bundle_sha256": export["sha256"],
        "owned_paths_violations": None if violations is None else list(violations),
        "classification": (export.get("classification") or {}).get("kind"),
    }
    path = job / "CUSTODY.json"
    # The gate reads <dir>/CUSTODY.json: stage the claim under that name in a private dir
    # inside the job (same filesystem, so the publishing rename stays atomic).
    stage = Path(tempfile.mkdtemp(prefix=".CUSTODY.json-", dir=job))
    staged = stage / "CUSTODY.json"
    try:
        with staged.open("x", encoding="utf-8") as stream:
            json.dump(claim, stream, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        try:
            ok, detail = gate.custody_verified(stage, job.parent, {"owner": owner}, required)
        except gate.CustodyUnknown as exc:
            ok, detail = False, f"unknown:{exc}"
        if not ok:
            raise CandidateCustodyError(
                f"custody_claim_unverified:{detail}", uncovered=[sha for sha in required if sha not in exported]
            )
        os.replace(staged, path)
        _fsync_dir(job)
    finally:
        staged.unlink(missing_ok=True)
        stage.rmdir()
    return path


def _owned_paths(path: str) -> list[str]:
    """A JSON list, a JSON object with ``owned_paths`` (a kit's meta.json), or one path per line."""
    text = Path(path).read_text(encoding="utf-8")
    try:
        value = json.loads(text)
    except ValueError:
        return [line.strip() for line in text.splitlines() if line.strip()]
    if isinstance(value, dict):
        value = value.get("owned_paths")
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise CandidateCustodyError("owned_paths_file_invalid")
    return value


def _retention_owner(job: Path) -> str:
    try:
        owner = json.loads((job / "RETENTION.json").read_text(encoding="utf-8")).get("owner")
    except (OSError, ValueError, AttributeError) as exc:
        raise CandidateCustodyError(f"retention_unreadable:{type(exc).__name__}") from exc
    if not isinstance(owner, str) or not owner:
        raise CandidateCustodyError("retention_owner_missing")
    return owner


def main(argv: Sequence[str] | None = None) -> int:
    """``export-and-claim``: export first, claim only from a verified export, one JSON line out."""
    parser = argparse.ArgumentParser(prog="candidate_custody.py", description="Candidate custody producer.")
    commands = parser.add_subparsers(dest="command", required=True)
    claim = commands.add_parser(
        "export-and-claim", help="bundle CANDIDATE outside the job dir, then write CUSTODY.json"
    )
    claim.add_argument("repo")
    claim.add_argument("candidate")
    claim.add_argument("--base", default=None, help="scope base; absent means scope_unknown")
    claim.add_argument("--owned-paths-file", required=True)
    claim.add_argument("--custody-root", required=True)
    claim.add_argument("--job-dir", required=True)
    arguments = parser.parse_args(argv)
    result: dict[str, Any] = {"command": arguments.command, "job_dir": arguments.job_dir}
    stage = "export"
    try:
        job = Path(arguments.job_dir).absolute()
        owner = _retention_owner(job)
        export = export_candidate(
            arguments.repo,
            arguments.candidate,
            base=arguments.base or None,
            owned_paths=_owned_paths(arguments.owned_paths_file),
            custody_root=arguments.custody_root,
            job_dir=job,
        )
        result["export"] = export
        stage = "claim"
        result["claim"] = str(write_custody_claim(job, owner=owner, export=export))
    except (CandidateCustodyError, OSError, ValueError) as exc:
        # ValueError covers undecodable input (UnicodeDecodeError): typed, one line, no claim.
        if isinstance(exc, CandidateCustodyError):
            code = exc.code
            if exc.uncovered:
                result["uncovered"] = exc.uncovered
        else:
            code = "invalid_input" if isinstance(exc, ValueError) else "os_error"
        result.update(outcome=f"{stage}_failed", error_code=code, error=f"{type(exc).__name__}:{exc}"[:1000])
        print(json.dumps(result, sort_keys=True))
        return 1
    result["outcome"] = "claimed"
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
