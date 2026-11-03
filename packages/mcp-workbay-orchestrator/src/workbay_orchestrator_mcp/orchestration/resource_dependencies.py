"""R1 read-only, bounded dependency producers and pure eligibility checks.

R5 supplies qualified scope proofs: an explicit root/PID list alone cannot prove
that foreign consumers, environments or mount namespaces have been inventoried.
No environment, command line, credentials, database or ledger is read here.
Inventory payloads are returned for private persistence outside the resource;
these functions never write, authorize effects, acquire an effect lock or retire.

Canon v0.26.2: SECD-05 (Security Engineering), GRPH-02 (Graph Theory with
Applications ch. 10), GRPH-03 (The Algorithm Design Manual ch. 18), GRPH-14
(Building Knowledge Graphs ch. 8 + Mental Models ch. 8). Published rows are the
pinned authority; original distilled corpus was unavailable.
"""

from __future__ import annotations

import configparser
import hashlib
import io
import json
import os
import re
import stat
import subprocess
import tarfile
from collections import deque
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Literal

from workbay_protocol.resource_lifecycle import (
    CustodyAcknowledgement,
    DependencyEdge,
    DependencySnapshot,
    EvidenceRef,
    Owner,
    Proof,
    ProofState,
    Registration,
    ResourceIdentity,
    canonical_digest,
)


@dataclass(frozen=True)
class Diagnostic:
    code: str
    subject: str


@dataclass(frozen=True)
class InventoryResult:
    edges: tuple[DependencyEdge, ...]
    inventory_complete: Proof
    no_live_use: Mapping[ResourceIdentity, Proof]
    holds: tuple[Diagnostic, ...]
    evidence: EvidenceRef
    payload: bytes
    effects_authorized: Literal[False] = False


@dataclass(frozen=True)
class DependencyAnalysis:
    snapshots: Mapping[ResourceIdentity, DependencySnapshot]
    eligible_order: tuple[ResourceIdentity, ...]
    components: tuple[tuple[ResourceIdentity, ...], ...]
    holds: tuple[Diagnostic, ...]
    evidence: EvidenceRef
    payload: bytes
    effects_authorized: Literal[False] = False


@dataclass(frozen=True)
class CustodyResult:
    custody: CustodyAcknowledgement | None
    proof: Proof
    holds: tuple[Diagnostic, ...]
    payload: bytes = b""
    effects_authorized: Literal[False] = False


def _clock(now: datetime) -> None:
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")


def proof_problem(proof: Proof, *, now: datetime) -> str | None:
    """Public freshness check, retaining R0's inclusive window semantics."""
    _clock(now)
    if proof.state != "verified":
        return "proof_" + proof.state
    if not proof.evidence:
        return "proof_missing_evidence"
    if proof.observed_at is None or proof.valid_until is None:
        return "proof_missing_freshness"
    if proof.valid_until < proof.observed_at:
        return "proof_invalid_window"
    if proof.observed_at > now:
        return "proof_future"
    if now > proof.valid_until:
        return "proof_stale"
    return None


def _evidence(value: object, namespace: str) -> tuple[bytes, EvidenceRef]:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
    digest = hashlib.sha256(payload).hexdigest()
    return payload, EvidenceRef(ref=f"{namespace}:{digest}", digest="sha256:" + digest)


def _proof(evidence: EvidenceRef, now: datetime, until: datetime, state: ProofState = "verified") -> Proof:
    return Proof(state=state, evidence=(evidence,), observed_at=now, valid_until=until)


def _combined(proofs: Sequence[Proof], evidence: EvidenceRef, now: datetime, *, held: bool = False) -> Proof:
    if held or not proofs or any(proof_problem(p, now=now) for p in proofs):
        return Proof(state="unknown", evidence=(evidence,))
    return Proof(
        state="verified",
        evidence=tuple(dict.fromkeys((evidence, *(e for p in proofs for e in p.evidence)))),
        observed_at=max(p.observed_at for p in proofs),
        valid_until=min(p.valid_until for p in proofs),
    )


def _root(resource: ResourceIdentity, owner: Owner) -> Path:
    if resource.owner != owner:
        raise ValueError("foreign_owner")
    if resource.resource_kind not in {"directory", "git_repository", "git_worktree", "artifact"}:
        raise ValueError("root_kind_unsupported")
    path = Path(resource.identity)
    if not path.is_absolute() or ".." in path.parts or path == Path(path.anchor):
        raise ValueError("root_scope_invalid")
    if path.resolve(strict=True) != path or not path.is_dir():
        raise ValueError("root_symlink_escape")
    return path


def _open_beneath(root: Path, relative: str, *, uid: int) -> int:
    """Open through directory FDs, refusing symlinks in every path component."""
    parts = PurePosixPath(relative).parts
    if not parts or relative.startswith("/") or ".." in parts:
        raise ValueError("path_escape")
    fd = os.open("/", os.O_RDONLY | os.O_DIRECTORY)
    try:
        for part in (*root.parts[1:], *parts[:-1]):
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            os.close(fd)
            fd = child
        result = os.open(parts[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd)
        info = os.fstat(result)
        if info.st_uid != uid or not stat.S_ISREG(info.st_mode):
            os.close(result)
            raise ValueError("file_owner_or_type_unknown")
        return result
    finally:
        os.close(fd)


def _read(root: Path, relative: str, *, uid: int, limit: int) -> bytes:
    fd = _open_beneath(root, relative, uid=uid)
    with os.fdopen(fd, "rb") as stream:
        before = os.fstat(stream.fileno())
        data = stream.read(limit + 1)
        after = os.fstat(stream.fileno())
    if len(data) > limit:
        raise ValueError("inventory_byte_limit")
    if (before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (
        after.st_size,
        after.st_mtime_ns,
        after.st_ctime_ns,
    ):
        raise ValueError("observation_changed")
    return data


def _directory_fd(root: Path) -> int:
    if not root.is_absolute() or ".." in root.parts:
        raise ValueError("path_escape")
    fd = os.open("/", os.O_RDONLY | os.O_DIRECTORY)
    try:
        for part in root.parts[1:]:
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            os.close(fd)
            fd = child
        return fd
    except BaseException:
        os.close(fd)
        raise


def _walk(root: Path, *, uid: int, max_entries: int):
    count = 0

    def failed(error):
        raise error

    root_fd = _directory_fd(root)
    try:
        for directory, dirs, files, fd in os.fwalk(".", dir_fd=root_fd, follow_symlinks=False, onerror=failed):
            info = os.fstat(fd)
            if info.st_uid != uid:
                raise ValueError("foreign_filesystem_owner")
            dirs.sort()
            files.sort()
            for name in (*dirs, *files):
                count += 1
                if count > max_entries:
                    raise ValueError("inventory_entry_limit")
                info = os.stat(name, dir_fd=fd, follow_symlinks=False)
                if info.st_uid != uid:
                    raise ValueError("foreign_filesystem_owner")
                link = os.readlink(name, dir_fd=fd) if stat.S_ISLNK(info.st_mode) else None
                yield (Path(directory) / name).as_posix(), info, link
    finally:
        os.close(root_fd)


def _local_target(text: str, base: Path) -> Path:
    if not text or any(ord(c) < 32 for c in text) or "$" in text or "~" in text or "\\" in text:
        raise ValueError("dependency_path_unsupported")
    path = Path(text)
    # Resolve lexically only: provider symlinks are inventoried separately.
    return Path(os.path.abspath(path if path.is_absolute() else base / path))


def git_config_targets(data: str, *, base: Path, git_dir: Path | None = None) -> tuple[Path, ...]:
    """Read local path references only. Includes/expansions stay unknown.

    No git command is run and no include file or remote URL is followed. Unknown
    syntax is a hold rather than treating a partially parsed config as complete.
    """
    parser = configparser.RawConfigParser(strict=True, delimiters=("=",), interpolation=None)
    parser.read_string(data)
    result = []
    for section in parser.sections():
        if section.lower().startswith(("include", "extensions")):
            raise ValueError("git_config_inventory_unsupported")
        for key, raw in parser.items(section):
            value = raw.strip().strip('"')
            if key in {"worktree", "gitdir", "objectdirectory", "alternatesfile"}:
                result.append(_local_target(value, git_dir or base))
            if key in {"url", "pushurl"}:
                if value.startswith("file://"):
                    result.append(_local_target(value[7:], base))
                elif not re.match(r"[A-Za-z][A-Za-z0-9+.-]*://|[^/]+:", value):
                    if not Path(value).is_absolute():
                        raise ValueError("git_relative_remote_unknown")
                    result.append(_local_target(value, base))
    return tuple(result)


def mount_targets(text: str) -> tuple[Path, ...]:
    """Parse kernel mountinfo root + mountpoint paths, including bind subroots.

    This parser does not assert that a supplied namespace list is exhaustive.
    """
    targets = []
    for line in text.splitlines():
        if not line.strip():
            continue
        fields = line.split()
        if len(fields) < 10 or "-" not in fields:
            raise ValueError("mount_inventory_malformed")
        sep = fields.index("-")
        if sep < 6 or len(fields) < sep + 4:
            raise ValueError("mount_inventory_malformed")
        for raw in (fields[3], fields[4], fields[sep + 2]):
            decoded = re.sub(r"\\([0-7]{3})", lambda m: chr(int(m[1], 8)), raw)
            if decoded.startswith("/"):
                targets.append(Path(decoded))
    return tuple(targets)


def inventory_dependencies(
    registrations: Sequence[Registration],
    *,
    owner: Owner,
    now: datetime,
    valid_until: datetime,
    root_coverage: Proof = Proof(),
    process_coverage: Proof = Proof(),
    mount_coverage: Proof = Proof(),
    pids: Sequence[int] = (),
    max_entries: int = 10000,
    max_bytes: int = 1048576,
) -> InventoryResult:
    """Inventory explicit registration roots and explicit observable Linux PIDs.

    Scope proofs must cover incoming consumers, all process uses (including
    environments not read here), and all mount namespaces. Missing scope proofs
    ALWAYS hold, even when the bounded local observations are empty. UID and
    repository ownership are independent checks. No global scan is performed.
    Call again under R4's same effect lock; compare the complete snapshot digest.
    """
    _clock(now)
    _clock(valid_until)
    if valid_until < now or not 0 < max_entries <= 100000 or not 0 < max_bytes <= 64 * 1024 * 1024:
        raise ValueError("invalid inventory bounds")
    if len(registrations) > 1024 or len(pids) > 1024:
        raise ValueError("inventory_scope_limit")
    uid = os.getuid()
    holds = []
    roots: dict[ResourceIdentity, Path] = {}
    edges: list[DependencyEdge] = []
    uses: dict[ResourceIdentity, list[str]] = {r.resource: [] for r in registrations}
    observations = []
    for name, proof in (("roots", root_coverage), ("processes", process_coverage), ("mounts", mount_coverage)):
        if reason := proof_problem(proof, now=now):
            holds.append(Diagnostic("inventory_" + reason, name))
    for registration in registrations:
        try:
            path = _root(registration.resource, owner)
            if path.stat().st_uid != uid:
                raise ValueError("foreign_filesystem_owner")
            if registration.resource in roots or path in roots.values():
                raise ValueError("duplicate_registration_root")
            roots[registration.resource] = path
        except (OSError, ValueError) as error:
            holds.append(
                Diagnostic(
                    str(error) if isinstance(error, ValueError) else "root_unobservable", registration.resource.identity
                )
            )

    # Withdrawing a containing root also withdraws every nested registration.
    # A retained child therefore requires the parent materialization.
    references = [
        (child, parent, "other", "registered_root_containment")
        for child, child_root in roots.items()
        for parent, parent_root in roots.items()
        if child != parent and child_root.is_relative_to(parent_root)
    ]

    def reference(consumer, target, relation, locator):
        try:
            target = target.resolve(strict=True)
        except (OSError, ValueError, RuntimeError):
            holds.append(Diagnostic("dependency_target_unobservable", locator))
            return
        matches = [r for r, root in roots.items() if target == root or target.is_relative_to(root)]
        # A nested registered provider wins over its containing registration.
        if matches:
            provider = max(matches, key=lambda r: len(roots[r].parts))
            if consumer != provider:
                references.append((consumer, provider, relation, locator))
        else:
            holds.append(Diagnostic("dependency_root_unknown", locator))

    for resource, root in roots.items():
        try:
            before = root.stat()
            for relative, info, link in _walk(root, uid=uid, max_entries=max_entries):
                observations.append(
                    (canonical_digest(resource), relative, info.st_ino, info.st_size, info.st_mtime_ns, link)
                )
                path = root / relative
                if link is not None:
                    reference(resource, _local_target(link, path.parent), "symlink", relative)
                    continue
                if not stat.S_ISREG(info.st_mode):
                    if not stat.S_ISDIR(info.st_mode):
                        raise ValueError("inventory_type_unsupported")
                    continue
                if path.suffix == ".pth":
                    for line in _read(root, relative, uid=uid, limit=max_bytes).decode().splitlines():
                        line = line.strip()
                        if not line or line.startswith("#"):
                            continue
                        if line.startswith(("import ", "import\t")):
                            raise ValueError("executable_pth_unknown")
                        reference(resource, _local_target(line, path.parent), "editable", relative)
                parts = PurePosixPath(relative).parts
                in_git = (
                    ".git" in parts
                    or (root / "HEAD").is_file()
                    or (path.parent / "HEAD").is_file()
                    or tuple(parts[-3:]) == ("objects", "info", "alternates")
                )
                if path.name == ".git":
                    text = _read(root, relative, uid=uid, limit=max_bytes).decode().strip()
                    if not text.startswith("gitdir: "):
                        raise ValueError("gitdir_inventory_unsupported")
                    reference(resource, _local_target(text[8:], path.parent), "git", relative)
                elif in_git and path.name in {"alternates", "commondir", "gitdir"}:
                    for line in _read(root, relative, uid=uid, limit=max_bytes).decode().splitlines():
                        base = path.parent.parent if path.name == "alternates" else path.parent
                        reference(resource, _local_target(line, base), "git", relative)
                elif in_git and path.name in {"config", "config.worktree"}:
                    text = _read(root, relative, uid=uid, limit=max_bytes).decode()
                    for target in git_config_targets(text, base=root, git_dir=path.parent):
                        reference(resource, target, "git", relative)
            after = root.stat()
            if (before.st_ino, before.st_mtime_ns, before.st_ctime_ns) != (
                after.st_ino,
                after.st_mtime_ns,
                after.st_ctime_ns,
            ):
                raise ValueError("observation_changed")
        except (OSError, ValueError, UnicodeError, configparser.Error) as error:
            holds.append(
                Diagnostic(str(error) if isinstance(error, ValueError) else "root_inventory_unknown", resource.identity)
            )

    def live(target: Path, label: str):
        for resource, root in roots.items():
            if target == root or target.is_relative_to(root):
                uses[resource].append(label)

    for pid in pids:
        if type(pid) is not int or pid <= 0:
            holds.append(Diagnostic("process_identity_unknown", "pid"))
            continue
        proc = Path("/proc") / str(pid)
        try:
            if proc.stat().st_uid != uid:
                raise ValueError("foreign_process_unknown")
            stamp = (proc / "stat").read_text()
            if os.readlink(proc / "root") != "/":
                raise ValueError("process_root_namespace_unknown")
            os.readlink(proc / "ns" / "mnt")
            # Probe only path-bearing fields; never environ/cmdline.
            for name in ("cwd", "exe"):
                live(Path(os.readlink(proc / name).removesuffix(" (deleted)")), f"process:{pid}:{name}")
            fds = list((proc / "fd").iterdir())
            if len(fds) > max_entries:
                raise ValueError("process_fd_limit")
            for fd in fds:
                target = os.readlink(fd).removesuffix(" (deleted)")
                if target.startswith("/"):
                    live(Path(target), f"process:{pid}:fd")
            with (proc / "maps").open() as stream:
                maps = stream.read(max_bytes + 1)
            with (proc / "mountinfo").open() as stream:
                mounts = stream.read(max_bytes + 1)
            if len(maps) > max_bytes or len(mounts) > max_bytes:
                raise ValueError("process_inventory_byte_limit")
            for line in maps.splitlines():
                fields = line.split(maxsplit=5)
                if len(fields) == 6 and fields[5].startswith("/"):
                    live(Path(fields[5].removesuffix(" (deleted)")), f"process:{pid}:map")
            for target in mount_targets(mounts):
                live(target, f"mount:{pid}")
            # starttime survives comm spaces/parentheses; other stat counters vary.
            if stamp.rsplit(")", 1)[1].split()[19] != (proc / "stat").read_text().rsplit(")", 1)[1].split()[19]:
                raise ValueError("process_identity_changed")
        except (OSError, ValueError, IndexError) as error:
            code = str(error) if isinstance(error, ValueError) else "process_or_mount_unobservable"
            holds.append(Diagnostic(code, f"pid:{pid}"))

    payload, evidence = _evidence(
        {
            "registrations": [r.model_dump(mode="json") for r in registrations],
            "observed_at": now.isoformat(),
            "valid_until": valid_until.isoformat(),
            "observations": observations,
            "references": [
                (c.model_dump(mode="json"), p.model_dump(mode="json"), rel, loc) for c, p, rel, loc in references
            ],
            "uses": [(r.model_dump(mode="json"), sorted(set(v))) for r, v in uses.items()],
            "coverage": [p.model_dump(mode="json") for p in (root_coverage, process_coverage, mount_coverage)],
            "holds": [(h.code, h.subject) for h in holds],
        },
        "r1-inventory",
    )
    local = _proof(evidence, now, valid_until)
    complete = _combined((root_coverage, process_coverage, mount_coverage, local), evidence, now, held=bool(holds))
    for consumer, provider, relation, _ in references:
        edge = DependencyEdge(consumer=consumer, provider=provider, relation=relation, state="required", proof=local)
        if edge not in edges:
            edges.append(edge)
    no_live = {r: _combined((complete,), evidence, now, held=bool(v)) for r, v in uses.items()}
    for resource, labels in uses.items():
        for label in sorted(set(labels)):
            holds.append(Diagnostic("live_use", resource.identity + ":" + label))
    return InventoryResult(tuple(edges), complete, no_live, tuple(holds), evidence, payload)


def analyze_dependencies(
    registrations: Sequence[Registration],
    *,
    owner: Owner,
    edges: Sequence[DependencyEdge],
    inventory_complete: Proof,
    clearances: Mapping[ResourceIdentity, Proof],
    now: datetime,
) -> DependencyAnalysis:
    """Condense consumer -> provider SCCs and propagate holds to all providers.

    Clearances are operation-specific release/no-live-use proofs from qualified
    callers, never age or terminal execution. A required edge holds its provider
    even if the consumer is eligible; a later fresh inventory must prove release.
    Cycles are held whole because frozen R0 admits only single-resource plans.
    """
    _clock(now)
    resources = tuple(r.resource for r in registrations)
    if len(set(resources)) != len(resources):
        raise ValueError("duplicate resource")
    nodes = set(resources) | {e.consumer for e in edges} | {e.provider for e in edges}
    key = canonical_digest
    graph = {r: set() for r in nodes}
    reverse = {r: set() for r in nodes}
    reasons: dict[ResourceIdentity, set[str]] = {r: set() for r in nodes}
    for resource in nodes:
        if resource not in resources:
            reasons[resource].add("dependency_registration_unknown")
        if resource.owner != owner:
            reasons[resource].add("foreign_owner")
        if problem := proof_problem(clearances.get(resource, Proof()), now=now):
            reasons[resource].add(problem)
        if problem := proof_problem(inventory_complete, now=now):
            reasons[resource].add("inventory_" + problem)
    for edge in edges:
        problem = proof_problem(edge.proof, now=now)
        if edge.state != "released" or problem:
            graph[edge.consumer].add(edge.provider)
            reverse[edge.provider].add(edge.consumer)
            reasons[edge.provider].add(
                "dependency_unknown" if problem or edge.state == "unknown" else "dependency_required"
            )
    # Iterative Kosaraju avoids call-stack failure on long retained chains.
    visited, finish = set(), []
    for start in sorted(nodes, key=key):
        stack = [(start, False)]
        while stack:
            node, done = stack.pop()
            if done:
                finish.append(node)
            elif node not in visited:
                visited.add(node)
                stack.append((node, True))
                stack.extend(
                    (child, False) for child in sorted(graph[node], key=key, reverse=True) if child not in visited
                )
    assigned, components = set(), []
    for start in reversed(finish):
        if start in assigned:
            continue
        members, stack = [], [start]
        assigned.add(start)
        while stack:
            node = stack.pop()
            members.append(node)
            for parent in sorted(reverse[node], key=key):
                if parent not in assigned:
                    assigned.add(parent)
                    stack.append(parent)
        component = tuple(sorted(members, key=key))
        components.append(component)
        if len(component) > 1 or start in graph[start]:
            for node in component:
                reasons[node].add("scc_held")
    pending = deque(r for r in nodes if reasons[r])
    while pending:
        consumer = pending.popleft()
        for provider in graph[consumer]:
            if "consumer_held" not in reasons[provider]:
                reasons[provider].add("consumer_held")
                pending.append(provider)
    payload, evidence = _evidence(
        {
            "registrations": [r.model_dump(mode="json") for r in registrations],
            "edges": [e.model_dump(mode="json") for e in edges],
            "inventory": inventory_complete.model_dump(mode="json"),
            "clearances": [
                (r.model_dump(mode="json"), p.model_dump(mode="json"))
                for r, p in sorted(clearances.items(), key=lambda item: key(item[0]))
            ],
            "now": now.isoformat(),
        },
        "r1-analysis",
    )
    snapshots = {}
    for component in components:
        for resource in component:
            if resource not in resources:
                continue
            ancestors, pending_ancestors = {resource}, [resource]
            while pending_ancestors:
                target = pending_ancestors.pop()
                for parent in reverse[target]:
                    if parent not in ancestors:
                        ancestors.add(parent)
                        pending_ancestors.append(parent)
            relevant_edges = tuple(e for e in edges if e.provider in ancestors)
            proofs = (inventory_complete, clearances.get(resource, Proof()), *(e.proof for e in relevant_edges))
            proof = _combined(proofs, evidence, now, held=bool(reasons[resource]))
            snapshots[resource] = DependencySnapshot(
                edges=relevant_edges,
                inventory_complete=inventory_complete,
                no_live_consumers=proof,
                scc_clear=proof,
                component_members=component,
            )
    # Ordering includes verified released edges where acyclic. A released cycle
    # imposes no present dependency; use deterministic tie-breaking in that case.
    eligible = {r for r in resources if not reasons[r]}
    order = []
    while eligible:
        ready = sorted(
            (
                r
                for r in eligible
                if not any(e.provider == r and e.consumer in eligible and e.consumer != r for e in edges)
            ),
            key=key,
        )
        if not ready:
            ready = [min(eligible, key=key)]
        for resource in ready:
            eligible.remove(resource)
            order.append(resource)
    holds = tuple(Diagnostic(reason, r.identity) for r in sorted(nodes, key=key) for reason in sorted(reasons[r]))
    return DependencyAnalysis(snapshots, tuple(order), tuple(components), holds, evidence, payload)


def evaluate_custody(
    registration: Registration,
    custody: CustodyAcknowledgement | None,
    *,
    owner: Owner,
    archive: EvidenceRef | None,
    now: datetime,
) -> CustodyResult:
    """Validate qualified custody claims, not filename existence or a hash claim.

    Fidelity and restored-fsck require independent digest-bearing audit evidence;
    using only the archive's own digest for those claims is insufficient. Use
    inspect_archive for supported local tar/standalone-Git fidelity, or supply an
    independently qualified producer. No archive is restored or written here.
    """
    _clock(now)
    holds = []
    if registration.resource.owner != owner:
        holds.append(Diagnostic("foreign_owner", "custody"))
    if custody is None:
        return CustodyResult(None, Proof(), tuple((*holds, Diagnostic("custody_missing", "custody"))))
    if custody.resource != registration.resource:
        holds.append(Diagnostic("custody_resource_mismatch", "custody.resource"))
    if custody.source != registration.source:
        holds.append(Diagnostic("custody_source_mismatch", "custody.source"))
    if archive is None or custody.archive != archive:
        holds.append(Diagnostic("custody_archive_mismatch", "custody.archive"))
    required = {"full_bytes", "types", "links", "ignored"}
    if registration.source.kind == "git":
        required.update(("refs", "reflogs"))
    if not required.issubset(custody.scope):
        holds.append(Diagnostic("custody_scope_missing", "custody.scope"))
    proofs = [custody.acknowledgement, custody.fidelity]
    if registration.source.kind == "git":
        if custody.restored_fsck is None:
            holds.append(Diagnostic("custody_fsck_missing", "custody.restored_fsck"))
        else:
            proofs.append(custody.restored_fsck)
    for name, proof in (
        ("acknowledgement", custody.acknowledgement),
        ("fidelity", custody.fidelity),
        ("restored_fsck", custody.restored_fsck),
    ):
        if proof is None:
            continue
        if problem := proof_problem(proof, now=now):
            holds.append(Diagnostic(problem, "custody." + name))
        if name != "acknowledgement" and not any(e != custody.archive for e in proof.evidence):
            holds.append(Diagnostic("custody_audit_missing", "custody." + name))
    payload, evidence = _evidence(
        {
            "registration": registration.model_dump(mode="json"),
            "custody": custody.model_dump(mode="json"),
            "archive": archive.model_dump(mode="json") if archive else None,
        },
        "r1-custody",
    )
    return CustodyResult(custody, _combined(proofs, evidence, now, held=bool(holds)), tuple(holds), payload)


def _tree_manifest(root: Path, *, uid: int, max_entries: int, max_bytes: int) -> dict:
    manifest = {}
    total = 0
    for relative, info, link in _walk(root, uid=uid, max_entries=max_entries):
        mode = stat.S_IMODE(info.st_mode)
        if stat.S_ISREG(info.st_mode):
            if info.st_nlink != 1:
                raise ValueError("custody_hardlink_fidelity_unsupported")
            data = _read(root, relative, uid=uid, limit=max_bytes - total)
            total += len(data)
            manifest[relative] = ("file", mode, len(data), hashlib.sha256(data).hexdigest())
        elif link is not None:
            manifest[relative] = ("symlink", mode, link)
        elif stat.S_ISDIR(info.st_mode):
            manifest[relative] = ("directory", mode)
        else:
            raise ValueError("custody_type_fidelity_unsupported")
    return manifest


def _git_read(root: Path, args: list[str]) -> str:
    # No inherited GIT_*, global config, fsmonitor hook, prompt or network command.
    result = subprocess.run(
        [
            "git",
            "--no-optional-locks",
            "-c",
            "core.fsmonitor=false",
            "-C",
            str(root),
            "--git-dir=" + str(root / ".git" if (root / ".git").is_dir() else root),
            *args,
        ],
        env={
            "PATH": os.defpath,
            "LC_ALL": "C",
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_CONFIG_GLOBAL": "/dev/null",
            "GIT_TERMINAL_PROMPT": "0",
            "GIT_NO_REPLACE_OBJECTS": "1",
        },
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )
    if result.returncode:
        # Keep potentially private Git output out of diagnostics/logs.
        raise ValueError("custody_git_restore_or_fsck_unsupported")
    return result.stdout


def inspect_archive(
    registration: Registration,
    *,
    owner: Owner,
    archive_path: Path,
    archive: EvidenceRef,
    restored_root: Path,
    acknowledgement: Proof,
    now: datetime,
    valid_until: datetime,
    max_entries: int = 10000,
    max_bytes: int = 64 * 1024 * 1024,
) -> CustodyResult:
    """Read a plain tar, compare EVERY member to source + independent restore.

    Supports regular files, directories and symbolic links, including ignored
    files and full standalone .git metadata. Hardlinks/FIFOs, compressed archives,
    external Git stores, config includes, unborn Git and unsupported member types
    return typed holds. The caller performs an independently qualified restore;
    this function never extracts, changes files, or treats existence as custody.
    The archive must contain root-relative members (optional '.' root entry).
    Limits bound all file/member bytes and counts. Git fsck runs read-only on the
    independent restore only, with a minimal environment and a 20-second timeout.
    """
    _clock(now)
    _clock(valid_until)
    if valid_until < now or not 0 < max_entries <= 100000 or not 0 < max_bytes <= 128 * 1024 * 1024:
        raise ValueError("invalid custody bounds")
    try:
        source_root = _root(registration.resource, owner)
        if registration.resource.owner != owner:
            raise ValueError("foreign_owner")
        restored_root = Path(restored_root)
        archive_path = Path(archive_path)
        for path in (restored_root, archive_path):
            if not path.is_absolute() or path.resolve(strict=True) != path:
                raise ValueError("custody_path_escape")
        if (
            source_root == restored_root
            or source_root.is_relative_to(restored_root)
            or restored_root.is_relative_to(source_root)
        ):
            raise ValueError("custody_restore_not_independent")
        if archive_path.is_relative_to(source_root) or archive_path.is_relative_to(restored_root):
            raise ValueError("custody_archive_inside_resource")
        uid = os.getuid()
        if source_root.stat().st_uid != uid or restored_root.stat().st_uid != uid or not restored_root.is_dir():
            raise ValueError("custody_owner_unknown")
        data = _read(archive_path.parent, archive_path.name, uid=uid, limit=max_bytes)
        if archive.digest != "sha256:" + hashlib.sha256(data).hexdigest():
            raise ValueError("custody_archive_digest_mismatch")
        source = _tree_manifest(source_root, uid=uid, max_entries=max_entries, max_bytes=max_bytes)
        restored = _tree_manifest(restored_root, uid=uid, max_entries=max_entries, max_bytes=max_bytes)
        members, expanded = {}, 0
        with tarfile.open(fileobj=io.BytesIO(data), mode="r:") as tar:
            for member in tar:
                if len(members) >= max_entries:
                    raise ValueError("custody_archive_entry_limit")
                path = PurePosixPath(member.name)
                if path.is_absolute() or ".." in path.parts or not member.name or "\x00" in member.name:
                    raise ValueError("custody_archive_member_escape")
                name = path.as_posix()
                if name == "." and member.isdir():
                    continue
                if name in members:
                    raise ValueError("custody_archive_duplicate_member")
                if member.isfile():
                    expanded += member.size
                    if member.size < 0 or expanded > max_bytes:
                        raise ValueError("custody_archive_byte_limit")
                    stream = tar.extractfile(member)
                    if stream is None:
                        raise ValueError("custody_archive_bytes_unknown")
                    with stream:
                        content = stream.read(max_bytes + 1)
                    if len(content) != member.size:
                        raise ValueError("custody_archive_bytes_unknown")
                    members[name] = ("file", member.mode, member.size, hashlib.sha256(content).hexdigest())
                elif member.issym():
                    members[name] = ("symlink", member.mode, member.linkname)
                elif member.isdir():
                    members[name] = ("directory", member.mode)
                else:
                    raise ValueError("custody_archive_fidelity_unsupported")
        if members != source or restored != source:
            raise ValueError("custody_full_fidelity_mismatch")
        fsck = None
        if registration.source.kind == "git":
            if registration.resource.resource_kind not in {"git_repository", "git_worktree", "directory"}:
                raise ValueError("custody_git_kind_unsupported")
            # A successful fsck that borrowed the original external object store
            # is not a self-contained restore. Unsupported arrangements hold.
            git_prefix = ".git/" if ".git" in source else ""
            if not git_prefix and not {"HEAD", "config", "objects", "refs"}.issubset(source):
                raise ValueError("custody_git_metadata_missing")
            if ".git" in source and source[".git"][0] != "directory":
                raise ValueError("custody_external_git_unsupported")
            for name in source:
                if name.startswith(git_prefix) and (
                    name.endswith("/alternates") or name.endswith("/commondir") or name.endswith("/gitdir")
                ):
                    raise ValueError("custody_external_git_unsupported")
                if name.startswith(git_prefix) and source[name][0] == "symlink":
                    raise ValueError("custody_git_symlink_unsupported")
                if name == git_prefix + "config" or name == git_prefix + "config.worktree":
                    config = _read(source_root, name, uid=uid, limit=max_bytes).decode()
                    targets = git_config_targets(config, base=source_root, git_dir=(source_root / name).parent)
                    if any(not target.is_relative_to(source_root) for target in targets):
                        raise ValueError("custody_external_git_unsupported")
            expected = [registration.source.revision, registration.source.tree]
            for root in (source_root, restored_root):
                if _git_read(root, ["rev-parse", "--verify", "HEAD"]).split() != expected[:1]:
                    raise ValueError("custody_source_mismatch")
                if _git_read(root, ["rev-parse", "--verify", "HEAD^{tree}"]).split() != expected[1:]:
                    raise ValueError("custody_tree_mismatch")
            fsck = _git_read(restored_root, ["fsck", "--full", "--strict"])
        # Detect edits during Git reads/manifest comparison; never hide an ignored
        # file, ref, reflog or link that appeared while inspection was in flight.
        if _tree_manifest(source_root, uid=uid, max_entries=max_entries, max_bytes=max_bytes) != source:
            raise ValueError("custody_source_changed")
        if _tree_manifest(restored_root, uid=uid, max_entries=max_entries, max_bytes=max_bytes) != restored:
            raise ValueError("custody_restore_changed")
        if _read(archive_path.parent, archive_path.name, uid=uid, limit=max_bytes) != data:
            raise ValueError("custody_archive_changed")
        payload, evidence = _evidence(
            {
                "registration": registration.model_dump(mode="json"),
                "archive": archive.model_dump(mode="json"),
                "source_manifest": source,
                "restored_root": str(restored_root),
                "fsck_digest": hashlib.sha256(fsck.encode()).hexdigest() if fsck is not None else None,
                "observed_at": now.isoformat(),
                "valid_until": valid_until.isoformat(),
            },
            "r1-archive-audit",
        )
        proof = _proof(evidence, now, valid_until)
        scope = (
            ("full_bytes", "types", "links", "ignored", "refs", "reflogs")
            if registration.source.kind == "git"
            else ("full_bytes", "types", "links", "ignored")
        )
        custody = CustodyAcknowledgement(
            resource=registration.resource,
            source=registration.source,
            scope=scope,
            archive=archive,
            acknowledgement=acknowledgement,
            fidelity=proof,
            restored_fsck=proof if fsck is not None else None,
        )
        evaluated = evaluate_custody(registration, custody, owner=owner, archive=archive, now=now)
        # Preserve the actual producer payload behind fidelity/fsck evidence.
        return CustodyResult(custody, evaluated.proof, evaluated.holds, payload)
    except (
        OSError,
        ValueError,
        UnicodeError,
        configparser.Error,
        tarfile.TarError,
        subprocess.SubprocessError,
    ) as error:
        code = str(error) if isinstance(error, ValueError) else "custody_inspection_unknown"
        return CustodyResult(None, Proof(), (Diagnostic(code, "custody.inspection"),))
