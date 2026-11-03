"""Registered resource runtime and public facade, dry-run/refusal by default.

Host configuration: WORKBAY_RESOURCE_RUNTIME_ROOT names an owned external
(non-retirable) authority directory. Its runtime.json declares owner and exact
registrations; optional custody locations and Docker profiles are expectations,
never permission or coverage proofs. Example payload for plan:
{"resource_key":"resource:<digest>","plan_id":"...","operation_id":"..."}.
probe/apply take plan_id; apply also requires expected_plan_digest. Registration
provisioning is host configuration, not arbitrary caller-supplied paths.

Trusted Python callers may supply native producers/probe qualifications and a
bounded executor to RegisteredResourceRuntime. No wire field injects these.
Public effects stay refused without that independently installed host bridge.
"""

from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

from pydantic import Field, StrictBool, StrictInt
from workbay_protocol.resource_lifecycle import (
    ApplyRequest,
    ContractModel,
    Correlation,
    CustodyAcknowledgement,
    EvidenceRef,
    OrcaIdentity,
    Owner,
    Proof,
    Registration,
    RetentionProbe,
    Text,
    WorkBayIdentity,
    canonical_digest,
)

from . import resource_dependencies as r1
from . import resource_docker as r2
from . import resource_events as r3
from .resource_native_admission import NativeAdmissionBridge
from .resource_retention_cli import (
    FileResourceLocks,
    RetentionCommands,
    build_plan,
    run_command,
)
from .resource_retention_ledger import SQLiteRetentionLedger, private_path

MAX_PACKET = 1024 * 1024
MAX_OUTPUT = 64 * 1024


class CustodyLocation(ContractModel):
    resource_key: Text
    archive_path: Path
    archive: EvidenceRef
    restored_root: Path


class RuntimeConfiguration(ContractModel):
    owner: Owner
    registrations: tuple[Registration, ...] = Field(min_length=1, max_length=1024)
    custody: tuple[CustodyLocation, ...] = ()
    docker_profiles: tuple[r2.DockerHostProfile, ...] = ()
    pids: tuple[StrictInt, ...] = Field(default=(), max_length=1024)


class PublicRequest(ContractModel):
    resource_key: Text | None = None
    task_ref: Text | None = None
    lane_id: Text | None = None
    orca_run: Text | None = None
    orca_task: Text | None = None
    plan_id: Text | None = None
    operation_id: Text | None = None
    expected_plan_digest: Text | None = None
    dry_run: StrictBool = True
    cursor: StrictInt = Field(default=0, ge=0)
    limit: StrictInt = Field(default=25, ge=1, le=100)


class RegisteredProbes:
    def __init__(self, configuration, ledger, *, clock, qualifications=None):
        self.configuration, self.ledger, self.clock = configuration, ledger, clock
        self.qualifications = qualifications
        self.last = {}

    def _qualification(self, registration, now):
        # Only a trusted in-process native caller can supply this dependency.
        # runtime.json and request payload cannot supply proof states.
        return self.qualifications.current(registration, now=now) if self.qualifications else {}

    def inspect(self, registration):
        now = self.clock()
        q = self._qualification(registration, now)
        roots = tuple(
            r
            for r in self.configuration.registrations
            if r.resource.resource_kind not in ("docker_image", "docker_volume")
        )
        inventory = r1.inventory_dependencies(
            roots,
            owner=self.configuration.owner,
            now=now,
            valid_until=now + timedelta(seconds=10),
            pids=self.configuration.pids,
            root_coverage=q.get("root_coverage", Proof()),
            process_coverage=q.get("process_coverage", Proof()),
            mount_coverage=q.get("mount_coverage", Proof()),
        )
        self.ledger.put_evidence(inventory.payload, "r1-inventory")
        analysis = r1.analyze_dependencies(
            self.configuration.registrations,
            owner=self.configuration.owner,
            edges=inventory.edges,
            inventory_complete=inventory.inventory_complete,
            clearances=q.get("clearances", {}),
            now=now,
        )
        self.ledger.put_evidence(analysis.payload, "r1-analysis")
        location = next(
            (c for c in self.configuration.custody if c.resource_key == r3.resource_key(registration.resource)), None
        )
        custody = None
        diagnostics = [{"code": h.code, "subject": h.subject} for h in (*inventory.holds, *analysis.holds)]
        if location:
            result = r1.inspect_archive(
                registration,
                owner=self.configuration.owner,
                archive_path=location.archive_path,
                archive=location.archive,
                restored_root=location.restored_root,
                acknowledgement=q.get("custody_acknowledgement", Proof()),
                now=now,
                valid_until=now + timedelta(seconds=10),
            )
            if result.payload:
                self.ledger.put_evidence(result.payload, "r1-archive-audit")
            custody = result.custody
            diagnostics.extend({"code": h.code, "subject": h.subject} for h in result.holds)
            if custody is None:
                custody = CustodyAcknowledgement(
                    resource=registration.resource,
                    source=registration.source,
                    scope=(),
                    archive=location.archive,
                    acknowledgement=Proof(),
                    fidelity=Proof(),
                )
        docker = None
        if registration.resource.resource_kind in ("docker_image", "docker_volume"):
            profile = next(
                (p for p in self.configuration.docker_profiles if registration in p.allowed_registrations), None
            )
            if profile:
                docker = r2.probe_docker(
                    r2.DockerProbeRequest(
                        registration=registration,
                        expected_registration_digest=canonical_digest(registration),
                        host_profile=profile.profile_ref,
                    ),
                    profile=profile,
                    now=now,
                    releases=q.get("docker_releases"),
                )
                for packet in docker.evidence:
                    # R2's canonical digest is over sorted compact ASCII JSON.
                    data = json.dumps(
                        packet.model_dump(mode="json"), sort_keys=True, separators=(",", ":"), ensure_ascii=True
                    ).encode()
                    self.ledger.put_evidence(data, "docker-read")
                diagnostics.extend({"code": h.code, "subject": h.subject} for h in docker.holds)
            else:
                diagnostics.append({"code": "docker_profile_unconfigured", "subject": "docker"})
        self.last = dict(
            inventory=inventory,
            analysis=analysis,
            custody=custody,
            docker=docker,
            diagnostics=diagnostics,
            qualifications=q,
            observed_at=now,
        )
        return self.last

    def probe(self, plan):
        state = self.inspect(plan.registration)
        q, docker = state["qualifications"], state["docker"]
        custody = r1.evaluate_custody(
            plan.registration,
            state["custody"],
            owner=self.configuration.owner,
            archive=state["custody"].archive if state["custody"] else None,
            now=self.clock(),
        )
        if custody.payload:
            self.ledger.put_evidence(custody.payload, "r1-custody")
        snapshot = state["analysis"].snapshots[plan.registration.resource]

        def shape(value):
            return (
                tuple(
                    sorted(
                        (canonical_digest(e.consumer), canonical_digest(e.provider), e.relation, e.state)
                        for e in value.edges
                    )
                ),
                tuple(sorted(canonical_digest(r) for r in value.component_members)),
            )

        # Timestamp-bearing evidence changes at every read. Rebind the original
        # immutable snapshot only after fresh qualified reachability/SCC results
        # have the same structural meaning; persist the comparison evidence.
        dependency_same = shape(snapshot) == shape(plan.dependencies) and all(
            r1.proof_problem(getattr(snapshot, n), now=self.clock()) is None
            for n in ("inventory_complete", "no_live_consumers", "scc_clear")
        )

        def audited_bytes(value):
            if value is None:
                return None
            for evidence in value.fidelity.evidence:
                if evidence.ref.startswith("r1-archive-audit:"):
                    audit = json.loads(self.ledger.evidence(evidence))
                    for temporal in ("observed_at", "valid_until"):
                        audit.pop(temporal, None)
                    return json.dumps(audit, sort_keys=True)
            return None

        old_audit, new_audit = audited_bytes(plan.custody), audited_bytes(state["custody"])
        custody_same = old_audit is not None and old_audit == new_audit and custody.proof.state == "verified"
        comparison = json.dumps(
            {
                "plan_digest": canonical_digest(plan),
                "observed_dependencies_digest": canonical_digest(snapshot),
                "bound_dependencies_digest": canonical_digest(plan.dependencies),
                "dependency_equivalent": dependency_same,
                "observed_custody_digest": canonical_digest(state["custody"]) if state["custody"] else None,
                "bound_custody_digest": canonical_digest(plan.custody),
                "custody_equivalent": custody_same,
            },
            sort_keys=True,
        ).encode()
        self.ledger.put_evidence(comparison, "retention-revalidation")
        return RetentionProbe(
            plan_digest=canonical_digest(plan),
            registration=plan.registration,
            dependencies_digest=canonical_digest(plan.dependencies) if dependency_same else canonical_digest(snapshot),
            custody_digest=canonical_digest(plan.custody)
            if custody_same
            else canonical_digest(state["custody"])
            if state["custody"]
            else canonical_digest(plan.custody),
            owner_verified=docker.owner_verified if docker else q.get("owner_verified", Proof()),
            source_verified=docker.source_verified if docker else q.get("source_verified", Proof()),
            policy_allowed=q.get("policy_allowed", Proof()),
            completion=q.get("completion", Proof()),
            no_live_use=state["inventory"].no_live_use.get(plan.registration.resource, Proof()),
            dependencies_clear=snapshot.no_live_consumers,
            custody_valid=custody.proof,
            docker=docker.docker if docker else None,
        )


class RegisteredResourceRuntime:
    def __init__(
        self,
        *,
        root,
        configuration,
        clock=None,
        qualifications=None,
        authority=None,
        executor=None,
        native_bridge=None,
        native_context=None,
    ):
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.native_bridge = native_bridge or NativeAdmissionBridge()
        self.native_context = native_context
        self.configuration = RuntimeConfiguration.model_validate(configuration.model_dump(mode="json"))
        if len({r3.resource_key(r.resource) for r in configuration.registrations}) != len(configuration.registrations):
            raise ValueError("duplicate_registration")
        if any(r.resource.owner != configuration.owner for r in configuration.registrations):
            raise ValueError("foreign_owner")
        # Validate all locations BEFORE opening an authority or probing targets.
        root = private_path(Path(root))
        for reg in configuration.registrations:
            if reg.resource.resource_kind not in ("docker_image", "docker_volume"):
                target = Path(reg.resource.identity)
                if not target.is_absolute() or ".." in target.parts or target.resolve() != target:
                    raise ValueError("resource_path_invalid")
                if root == target or root.is_relative_to(target):
                    raise ValueError("receipt_storage_unsafe")
        self.ledger = SQLiteRetentionLedger(root, clock=self.clock)
        for reg in configuration.registrations:
            if not self.ledger.register(reg):
                raise ValueError("registration_changed")
        lock_root = private_path(root / "locks")
        lock_root.mkdir(mode=0o700, exist_ok=True)
        private_path(lock_root)
        self.probes = RegisteredProbes(configuration, self.ledger, clock=self.clock, qualifications=qualifications)
        self.commands = RetentionCommands(
            owner=configuration.owner,
            probes=self.probes,
            ledger=self.ledger,
            locks=FileResourceLocks(lock_root),
            authority=authority,
            executor=executor,
            clock=self.clock,
        )

    def run(self, command, payload):
        request = PublicRequest.model_validate(payload)
        lifecycle = None
        if command in ("status", "history"):
            result = run_command(command, {"cursor": request.cursor, "limit": request.limit}, commands=self.commands)
            resource = next(
                (
                    r.resource
                    for r in self.configuration.registrations
                    if r3.resource_key(r.resource) == request.resource_key
                ),
                None,
            )
            if request.resource_key is not None and resource is None:
                return _hold("registration_unknown")
            correlation = Correlation(
                workbay=WorkBayIdentity(task=request.task_ref, lane=request.lane_id)
                if request.task_ref or request.lane_id
                else None,
                orca=OrcaIdentity(run=request.orca_run, task=request.orca_task)
                if request.orca_run or request.orca_task
                else None,
            )
            with self.ledger.reader() as conn:
                lifecycle = r3.resource_history(
                    conn,
                    owner=self.configuration.owner,
                    resource=resource,
                    correlation=correlation
                    if any((request.task_ref, request.lane_id, request.orca_run, request.orca_task))
                    else None,
                    after_id=request.cursor,
                    limit=request.limit,
                )
                if command == "status" and any(
                    (resource, request.task_ref, request.lane_id, request.orca_run, request.orca_task)
                ):
                    clauses, values = ["owner_repository=?"], [self.configuration.owner.repository]
                    for value, column in (
                        (r3.resource_key(resource) if resource else None, "resource_key"),
                        (request.task_ref, "wb_task"),
                        (request.lane_id, "wb_lane"),
                        (request.orca_run, "orca_run"),
                        (request.orca_task, "orca_task"),
                    ):
                        if value is not None:
                            clauses.append(column + "=?")
                            values.append(value)
                    row = conn.execute(
                        "SELECT MAX(id) FROM resource_events WHERE " + " AND ".join(clauses), values
                    ).fetchone()
                    latest = (
                        r3.resource_history(conn, owner=self.configuration.owner, after_id=row[0] - 1, limit=1)[
                            "events"
                        ][0]
                        if row[0]
                        else None
                    )
                    lifecycle = {"knowledge": "observed" if latest else "unknown", "latest": latest}
        elif command == "verification":
            native = self.native_bridge.verify_current(self.native_context, now=self.clock())
            result = {
                "ok": native.outcome.kind == "allow",
                "status": "verified" if native.outcome.kind == "allow" else "held",
                "effects_authorized": False,
                "holds": [{"code": code, "subject": "native.execution"} for code in native.holds],
                "outcome": native.outcome.model_dump(mode="json"),
                "native_failure_reason": native.native_failure_reason,
                "automatic_viewer_acceptance": "unverified",
                "connection": "unknown",
                "resources": [],
            }
            selected = [
                r
                for r in self.configuration.registrations
                if request.resource_key is None or r3.resource_key(r.resource) == request.resource_key
            ]
            with self.ledger.reader() as conn:
                for registration in selected[: request.limit]:
                    result["resources"].append(
                        {
                            "resource_key": r3.resource_key(registration.resource),
                            "status": r3.resource_status(conn, registration.resource),
                            # The authority query is connected; viewer connectivity
                            # is unknown until an actual consumer reports it.
                            "freshness": r3.resource_freshness(
                                conn, registration.resource, now=self.clock(), connected=None
                            ),
                        }
                    )
            return result
        elif command == "plan":
            registration = next(
                (r for r in self.configuration.registrations if r3.resource_key(r.resource) == request.resource_key),
                None,
            )
            if registration is None:
                return _hold("registration_unknown")
            if not request.plan_id or not request.operation_id:
                return _hold("plan_identity_missing")
            state = self.probes.inspect(registration)
            if state["custody"] is None:
                return {**_hold("custody_unconfigured"), "probe_diagnostics": state["diagnostics"][:100]}
            plan = build_plan(
                registration,
                state["analysis"].snapshots[registration.resource],
                state["custody"],
                plan_id=request.plan_id,
                operation_id=request.operation_id,
                now=self.clock(),
            )
            result = run_command("plan", plan.model_dump(mode="json"), commands=self.commands)
        elif command in ("probe", "apply"):
            plan = self.ledger.read_plan(self.configuration.owner, request.plan_id)
            if plan is None:
                return _hold("plan_unknown")
            if command == "probe":
                result = run_command(command, plan.model_dump(mode="json"), commands=self.commands)
            else:
                if request.expected_plan_digest is None:
                    return _hold("expected_plan_digest_missing")
                # Terminal recovery uses the original durable intent, not a
                # reconstructed probe from a target that may already be gone.
                previous = self.ledger.lookup(plan.registration.resource, plan.operation_id)
                probe = previous.request.probe if previous and not request.dry_run else self.probes.probe(plan)
                value = ApplyRequest(
                    plan=plan, probe=probe, expected_plan_digest=request.expected_plan_digest, dry_run=request.dry_run
                )
                result = run_command(command, value.model_dump(mode="json"), commands=self.commands)
        else:
            return _hold("invalid_command")
        data = result.model_dump(mode="json")
        if any((request.resource_key, request.task_ref, request.lane_id, request.orca_run, request.orca_task)):
            data["page"] = None

        return {
            "ok": result.status not in ("held", "pending"),
            "effects_authorized": False,
            **data,
            "probe_diagnostics": self.probes.last.get("diagnostics", [])[:100],
            "lifecycle": lifecycle,
        }


def _hold(code):
    return {
        "ok": False,
        "status": "held",
        "effects_authorized": False,
        "holds": [{"code": code, "subject": "resource_runtime"}],
    }


def run_public(command: str, payload: dict | None = None) -> dict:
    """Actual MCP/CLI entry: bounded packets, host root, no injectable services."""
    try:
        payload = {} if payload is None else payload
        if len(json.dumps(payload).encode()) > MAX_PACKET:
            return _hold("input_too_large")
        root_text = os.environ.get("WORKBAY_RESOURCE_RUNTIME_ROOT")
        if not root_text:
            return _hold("runtime_unconfigured")
        root = private_path(Path(root_text))
        path = private_path(root / "runtime.json", directory=False)
        with path.open("rb") as stream:
            raw = stream.read(MAX_PACKET + 1)
        if len(raw) > MAX_PACKET:
            return _hold("configuration_too_large")
        configuration = RuntimeConfiguration.model_validate_json(raw)
        result = RegisteredResourceRuntime(root=root, configuration=configuration).run(command, payload)
        if len(json.dumps(result).encode()) > MAX_OUTPUT:
            return _hold("output_too_large")
        return result
    except (ValueError, TypeError):
        return _hold("invalid_configuration_or_packet")
    except (OSError, RuntimeError, sqlite3.Error):
        return _hold("authority_unavailable")


class PinnedArchiveExecutor:
    """Bounded, descriptor-pinned archive only; never deletes or renames targets.

    A trusted host resource producer supplies a current ResourceEffectBinding
    after native authorization. This class is never constructed by run_public.
    Only owned regular files/directories/symlinks are supported. Unsupported
    special files, hardlinks, changed metadata and limits refuse the operation.
    The archive artifact and operation-specific evidence share the SQLite store.
    """

    def __init__(self, *, ledger, resource_producer, native_identity, max_entries=10000, max_bytes=8 * 1024 * 1024):
        if not 1 <= max_entries <= 10000 or not 1 <= max_bytes <= 8 * 1024 * 1024:
            raise ValueError("archive_bounds")
        self.ledger, self.resource_producer, self.native_identity = ledger, resource_producer, native_identity
        self.max_entries, self.max_bytes = max_entries, max_bytes

    def execute(self, plan, probe, *, operation_key):
        import hashlib
        import io
        import stat
        import tarfile
        import time

        from .resource_native_admission import ResourceEffectBinding

        if plan.registration.operation != "archive" or plan.registration.resource.resource_kind not in (
            "directory",
            "git_repository",
        ):
            raise ValueError("executor_operation_unsupported")
        if operation_key != r3.event_identity(plan.registration.resource, plan.operation_id, "applied"):
            raise ValueError("operation_key_changed")
        binding = self.resource_producer.current(plan.registration, self.native_identity)
        now = datetime.now(timezone.utc)
        if (
            not isinstance(binding, ResourceEffectBinding)
            or binding.native_identity != self.native_identity
            or binding.resource != plan.registration.resource
            or binding.source != plan.registration.source
            or binding.operation != "archive"
            or binding.policy_version != plan.registration.policy_version
            or r1.proof_problem(binding.authorization, now=now)
        ):
            raise ValueError("resource_authority_unknown")
        target = Path(binding.resource.identity)
        self.ledger._outside(plan.registration)
        # Open each ancestor without following links, then pin the target FD.
        fd = os.open("/", os.O_RDONLY | os.O_DIRECTORY)
        try:
            for part in target.parts[1:]:
                child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
                os.close(fd)
                fd = child
            info = os.fstat(fd)
            if (info.st_dev, info.st_ino, info.st_uid) != (binding.device, binding.inode, os.geteuid()):
                raise ValueError("descriptor_identity_changed")
            output = io.BytesIO()
            entries = 0
            total = 0
            deadline = time.monotonic() + 10

            def stamp(st):
                return (
                    st.st_dev,
                    st.st_ino,
                    st.st_mode,
                    st.st_uid,
                    st.st_nlink,
                    st.st_size,
                    st.st_mtime_ns,
                    st.st_ctime_ns,
                )

            with tarfile.open(fileobj=output, mode="w", format=tarfile.PAX_FORMAT) as archive:

                def walk(directory, prefix=""):
                    nonlocal entries, total
                    before = stamp(os.fstat(directory))
                    names = sorted(os.listdir(directory))
                    if len(names) > self.max_entries - entries:
                        raise ValueError("archive_entry_limit")
                    for name in names:
                        if time.monotonic() > deadline:
                            raise ValueError("archive_timeout")
                        entries += 1
                        if entries > self.max_entries:
                            raise ValueError("archive_entry_limit")
                        st = os.stat(name, dir_fd=directory, follow_symlinks=False)
                        if st.st_uid != os.geteuid():
                            raise ValueError("archive_owner_unknown")
                        member = tarfile.TarInfo(prefix + name)
                        member.mode = stat.S_IMODE(st.st_mode)
                        member.uid = st.st_uid
                        member.gid = st.st_gid
                        member.mtime = st.st_mtime
                        if stat.S_ISLNK(st.st_mode):
                            member.type = tarfile.SYMTYPE
                            member.linkname = os.readlink(name, dir_fd=directory)
                            archive.addfile(member)
                        elif stat.S_ISDIR(st.st_mode):
                            member.type = tarfile.DIRTYPE
                            archive.addfile(member)
                            child = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=directory)
                            try:
                                if stamp(os.fstat(child)) != stamp(st):
                                    raise ValueError("archive_source_changed")
                                walk(child, prefix + name + "/")
                            finally:
                                os.close(child)
                        elif stat.S_ISREG(st.st_mode) and st.st_nlink == 1:
                            total += st.st_size
                            if total > self.max_bytes:
                                raise ValueError("archive_byte_limit")
                            child = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
                            with os.fdopen(child, "rb") as stream:
                                if stamp(os.fstat(stream.fileno())) != stamp(st):
                                    raise ValueError("archive_source_changed")
                                member.size = st.st_size
                                archive.addfile(member, stream)
                                if stamp(os.fstat(stream.fileno())) != stamp(st):
                                    raise ValueError("archive_source_changed")
                        else:
                            raise ValueError("archive_type_unsupported")
                        if stamp(os.stat(name, dir_fd=directory, follow_symlinks=False)) != stamp(st):
                            raise ValueError("archive_source_changed")
                        if output.tell() > 16 * 1024 * 1024:
                            raise ValueError("archive_output_limit")
                    if names != sorted(os.listdir(directory)) or before != stamp(os.fstat(directory)):
                        raise ValueError("archive_source_changed")

                walk(fd)
            # Compare the complete descriptor-read archive against the retained
            # independent R1 audit, including ignored bytes/links/Git metadata.
            # Filename hashes or a registration label are not custody proof.
            audit = None
            for evidence in plan.custody.fidelity.evidence:
                if evidence.ref.startswith("r1-archive-audit:"):
                    audit = json.loads(self.ledger.evidence(evidence))
                    break
            if audit is None or audit.get("registration") != plan.registration.model_dump(mode="json"):
                raise ValueError("custody_audit_missing")
            manifest = {}
            with tarfile.open(fileobj=io.BytesIO(output.getvalue()), mode="r:") as archive:
                for member in archive:
                    if member.isfile():
                        data = archive.extractfile(member).read()
                        manifest[member.name] = ["file", member.mode, len(data), hashlib.sha256(data).hexdigest()]
                    elif member.isdir():
                        manifest[member.name.rstrip("/")] = ["directory", member.mode]
                    elif member.issym():
                        manifest[member.name] = ["symlink", member.mode, member.linkname]
                    else:
                        raise ValueError("archive_type_unsupported")
            if manifest != audit.get("source_manifest"):
                raise ValueError("archive_source_changed")
            # Fresh identity check prevents a replaced path from being accepted.
            if (target.stat().st_dev, target.stat().st_ino) != (binding.device, binding.inode):
                raise ValueError("descriptor_identity_changed")
            identity = binding.native_identity
            qualification = json.dumps(
                {
                    "resource": binding.resource.model_dump(mode="json"),
                    "source": binding.source.model_dump(mode="json"),
                    "policy_version": binding.policy_version,
                    "operation": binding.operation,
                    "device": binding.device,
                    "inode": binding.inode,
                    "authorization": binding.authorization.model_dump(mode="json"),
                    "native_identity": {
                        name: getattr(identity, name)
                        for name in (
                            "task_ref",
                            "lane_id",
                            "lane_row_id",
                            "identity_seq",
                            "cycle_epoch",
                            "source_ordinal",
                            "branch",
                            "policy_version",
                        )
                    },
                    "plan_digest": canonical_digest(plan),
                    "independent_custody_audit": evidence.model_dump(mode="json"),
                },
                sort_keys=True,
            ).encode()
            return self.ledger.archive_effect(
                plan, output.getvalue(), now=datetime.now(timezone.utc), qualification=qualification
            )
        finally:
            os.close(fd)

    def reconcile(self, record, *, operation_key):
        if operation_key != r3.event_identity(
            record.request.plan.registration.resource, record.request.plan.operation_id, "applied"
        ):
            raise ValueError("operation_key_changed")
        return self.ledger.archived_effect(record)
