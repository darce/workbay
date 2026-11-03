"""Read-only Docker evidence producer for the frozen R0 retention contract.

R5 supplies a trusted host allowlist and independently qualified rollback/DB
release proofs. Requests cannot supply argv, a shell, environment or endpoints.
The built-in runner reads only projected, noncredential Docker metadata, uses
an empty temporary CLI config, and never escalates privileges. No prune/remove,
generic filesystem read, event writer, or native admission is exposed here.

Observations are bounded snapshots, not an engine-wide lock. R5 must retain the
returned evidence outside the resource and re-probe under its effect lock.
Docker volume metadata cannot establish a content digest: source verification
for volumes deliberately remains unknown, pending qualified recovery evidence.
"""

from __future__ import annotations

import json
import os
import re
import selectors
import signal
import subprocess
import tempfile
import time
from datetime import datetime, timedelta
from typing import Annotated, Literal, Protocol

from pydantic import Field, StrictInt, StrictStr, model_validator
from workbay_protocol.convergence import Sha256Digest
from workbay_protocol.resource_lifecycle import (
    ContractModel,
    DockerFacts,
    DockerSource,
    EvidenceRef,
    Hold,
    NoEffects,
    OpaqueRef,
    Proof,
    Registration,
    Timestamp,
    canonical_digest,
)

_IMAGE = re.compile(r"sha256:[0-9a-f]{64}\Z")
_CONTAINER = re.compile(r"[0-9a-f]{64}\Z")
_VOLUME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,254}\Z")
OWNER_LABEL = "io.workbay.owner.repository"
ROLLBACK_LABEL = "io.workbay.retention.rollback"
DB_LABEL = "io.workbay.retention.db-recovery"

# These fixed Go templates are the entire privileged read surface. In
# particular, container Config.Env, mount options, and arbitrary labels never
# enter the process. The field shapes below are the actual projected stdout.
_CONTAINER_FORMAT = (
    '{"Id":{{json .Id}},"Image":{{json .Image}},'
    '"State":{"Running":{{json .State.Running}}},"Mounts":['
    "{{range $i,$m := .Mounts}}{{if $i}},{{end}}"
    '{"Type":{{json $m.Type}},"Name":{{json $m.Name}}}{{end}}]}'
)
_IMAGE_FORMAT = (
    '{"Id":{{json .Id}},"RepoDigests":{{json .RepoDigests}},'
    '"Parent":{{json .Parent}},"RootFS":{{json .RootFS}},'
    '"Config":{"Labels":{"'
    + OWNER_LABEL
    + '":{{json (index .Config.Labels "'
    + OWNER_LABEL
    + '")}},"'
    + ROLLBACK_LABEL
    + '":{{json (index .Config.Labels "'
    + ROLLBACK_LABEL
    + '")}}}}}'
)
_VOLUME_FORMAT = (
    '{"Name":{{json .Name}},"Labels":{"'
    + OWNER_LABEL
    + '":{{json (index .Labels "'
    + OWNER_LABEL
    + '")}},"'
    + DB_LABEL
    + '":{{json (index .Labels "'
    + DB_LABEL
    + '")}}}}'
)


class DockerHostProfile(ContractModel):
    """Trusted local policy, never constructed from an external probe packet.

    allowed_registrations binds owner, source, operation and policy as well as
    resource identity. A privileged helper may implement DockerRunner, but it
    must enforce the same typed read surface and bounds at its own boundary.
    """

    profile_ref: OpaqueRef
    docker_binary: Literal["/usr/bin/docker", "/usr/local/bin/docker"] = "/usr/bin/docker"
    endpoint: Literal["unix:///var/run/docker.sock", "unix:///run/docker.sock"] = "unix:///var/run/docker.sock"
    execution_uid: Annotated[StrictInt, Field(ge=0)]
    allowed_registrations: tuple[Registration, ...] = Field(min_length=1, max_length=1024)
    timeout_seconds: Annotated[StrictInt, Field(ge=1, le=30)] = 5
    total_timeout_seconds: Annotated[StrictInt, Field(ge=1, le=120)] = 30
    max_output_bytes: Annotated[StrictInt, Field(ge=1, le=8_388_608)] = 1_048_576
    max_inventory_bytes: Annotated[StrictInt, Field(ge=1, le=16_777_216)] = 4_194_304
    max_objects: Annotated[StrictInt, Field(ge=1, le=4096)] = 256
    freshness_seconds: Annotated[StrictInt, Field(ge=1, le=60)] = 10


class DockerProbeRequest(ContractModel):
    registration: Registration
    expected_registration_digest: Sha256Digest
    host_profile: OpaqueRef

    @model_validator(mode="after")
    def _docker_identity(self):
        resource = self.registration.resource
        if not isinstance(self.registration.source, DockerSource):
            raise ValueError("Docker probes require a DockerSource")
        pattern = {"docker_image": _IMAGE, "docker_volume": _VOLUME}.get(resource.resource_kind)
        if pattern is None or pattern.fullmatch(resource.identity) is None:
            raise ValueError("require an exact image ID or a single volume name")
        return self


class DockerReadRequest(ContractModel):
    operation: Literal["container_ids", "image_ids", "container", "image", "volume"]
    identity: StrictStr | None = None

    @model_validator(mode="after")
    def _read_identity(self):
        if self.operation.endswith("_ids"):
            if self.identity is not None:
                raise ValueError("inventory reads take no external arguments")
        else:
            pattern = {"container": _CONTAINER, "image": _IMAGE, "volume": _VOLUME}[self.operation]
            if self.identity is None or pattern.fullmatch(self.identity) is None:
                raise ValueError("invalid exact Docker identity")
        return self


class DockerReadResult(ContractModel):
    status: Literal[
        "ok", "permission_denied", "missing_docker", "timeout", "output_limit", "io_error", "command_failed"
    ]
    stdout: bytes = b""
    returncode: StrictInt | None = None


class DockerRunner(Protocol):
    def __call__(
        self, read: DockerReadRequest, *, profile: DockerHostProfile, timeout_seconds: float
    ) -> DockerReadResult: ...


class DockerReleaseEvidence(ContractModel):
    """Qualified R1/R5 claims, never inferred from age, labels or an empty list.

    Required refs override a claimed release. The producer must inventory owned
    rollback references and DB/recovery requirements before asserting release.
    A proof of release alone grants no deletion permission.
    """

    registration_digest: Sha256Digest
    host_profile: OpaqueRef
    rollback_released: Proof = Field(default_factory=Proof)
    db_volume_released: Proof = Field(default_factory=Proof)
    db_volume_permission: Proof | None = None
    required_rollback_images: tuple[Sha256Digest, ...] = ()
    required_db_volumes: tuple[StrictStr, ...] = ()


class DockerReadEvidence(ContractModel):
    registration_digest: Sha256Digest
    host_profile: OpaqueRef
    observed_at: Timestamp
    read: DockerReadRequest
    selected_stdout: StrictStr


class DockerProbeResult(ContractModel):
    registration: Registration
    registration_digest: Sha256Digest
    host_profile: OpaqueRef
    docker: DockerFacts
    owner_verified: Proof
    source_verified: Proof
    holds: tuple[Hold, ...]
    evidence: tuple[DockerReadEvidence, ...]
    effects_authorized: NoEffects = False


def _argv(read: DockerReadRequest, profile: DockerHostProfile, config: str) -> tuple[str, ...]:
    prefix = (profile.docker_binary, "--host", profile.endpoint, "--config", config)
    if read.operation == "container_ids":
        return prefix + ("container", "ls", "--all", "--quiet", "--no-trunc")
    if read.operation == "image_ids":
        return prefix + ("image", "ls", "--all", "--quiet", "--no-trunc")
    template = {"container": _CONTAINER_FORMAT, "image": _IMAGE_FORMAT, "volume": _VOLUME_FORMAT}[read.operation]
    return prefix + (read.operation, "inspect", "--format", template, read.identity)


def run_docker_read(read: DockerReadRequest, *, profile: DockerHostProfile, timeout_seconds: float) -> DockerReadResult:
    """Bound both pipes and wall time; no inherited auth/config/environment.

    This is a local read runner, not sudo or a remote-command transport. Callers
    must not substitute this function for a qualified privileged helper when
    socket access is denied. Error text is classified, never returned/logged.
    """
    read = DockerReadRequest.model_validate(read.model_dump())
    profile = DockerHostProfile.model_validate(profile.model_dump())
    if os.geteuid() != profile.execution_uid:
        return DockerReadResult(status="permission_denied")
    if not 0 < timeout_seconds <= profile.timeout_seconds:
        raise ValueError("timeout must be positive and within the host profile")
    deadline = time.monotonic() + timeout_seconds
    try:
        with tempfile.TemporaryDirectory(prefix="workbay-docker-read-") as config:
            with subprocess.Popen(
                _argv(read, profile, config),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=config,
                env={"PATH": "/usr/bin:/bin", "LANG": "C", "LC_ALL": "C"},
                shell=False,
                start_new_session=True,
            ) as process:
                try:
                    buffers = {"stdout": bytearray(), "stderr": bytearray()}
                    with selectors.DefaultSelector() as selector:
                        for name in buffers:
                            selector.register(getattr(process, name), selectors.EVENT_READ, name)
                        total = 0
                        while selector.get_map():
                            remaining = deadline - time.monotonic()
                            if remaining <= 0:
                                return DockerReadResult(status="timeout")
                            for key, _ in selector.select(remaining):
                                data = os.read(key.fileobj.fileno(), min(65536, profile.max_output_bytes + 1))
                                if not data:
                                    selector.unregister(key.fileobj)
                                    continue
                                total += len(data)
                                if total > profile.max_output_bytes:
                                    return DockerReadResult(status="output_limit")
                                buffers[key.data].extend(data)
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        return DockerReadResult(status="timeout")
                    code = process.wait(timeout=remaining)
                    if code:
                        denied = any(
                            phrase in bytes(buffers["stderr"]).lower()
                            for phrase in (b"permission denied", b"operation not permitted", b"access denied")
                        )
                        return DockerReadResult(
                            status="permission_denied" if denied else "command_failed", returncode=code
                        )
                    return DockerReadResult(status="ok", returncode=0, stdout=bytes(buffers["stdout"]))
                finally:
                    # Also close inherited pipes if an unexpected helper child
                    # outlives the CLI. This group contains only our new child.
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    process.wait(timeout=1)
    except FileNotFoundError:
        return DockerReadResult(status="missing_docker")
    except PermissionError:
        return DockerReadResult(status="permission_denied")
    except subprocess.TimeoutExpired:
        return DockerReadResult(status="timeout")
    except OSError:
        return DockerReadResult(status="io_error")


class _InventoryError(Exception):
    def __init__(self, reason: str):
        self.reason = reason


def _keys(value, expected: set[str]):
    if not isinstance(value, dict) or set(value) != expected:
        raise _InventoryError("invalid_shape")


def _digest(value):
    if not isinstance(value, str) or _IMAGE.fullmatch(value) is None:
        raise _InventoryError("invalid_identity")
    return value


def _image(value):
    _keys(value, {"Id", "RepoDigests", "Parent", "RootFS", "Config"})
    _digest(value["Id"])
    if value["Parent"] != "":
        _digest(value["Parent"])
    _keys(value["RootFS"], {"Type", "Layers"})
    if value["RootFS"]["Type"] != "layers" or not isinstance(value["RootFS"]["Layers"], list):
        raise _InventoryError("child_lineage_unknown")
    for layer in value["RootFS"]["Layers"]:
        _digest(layer)
    if value["RepoDigests"] is not None:
        if not isinstance(value["RepoDigests"], list):
            raise _InventoryError("invalid_shape")
        for ref in value["RepoDigests"]:
            if not isinstance(ref, str) or "@" not in ref:
                raise _InventoryError("invalid_identity")
            _digest(ref.rsplit("@", 1)[1])
    _keys(value["Config"], {"Labels"})
    _keys(value["Config"]["Labels"], {OWNER_LABEL, ROLLBACK_LABEL})
    labels = value["Config"]["Labels"]
    if labels[ROLLBACK_LABEL] not in (None, "", "required", "released"):
        raise _InventoryError("rollback_unknown")
    if labels[OWNER_LABEL] is not None and not isinstance(labels[OWNER_LABEL], str):
        raise _InventoryError("owner_unknown")


def _container(value):
    _keys(value, {"Id", "Image", "State", "Mounts"})
    if not isinstance(value["Id"], str) or _CONTAINER.fullmatch(value["Id"]) is None:
        raise _InventoryError("invalid_identity")
    _digest(value["Image"])
    _keys(value["State"], {"Running"})
    if type(value["State"]["Running"]) is not bool or not isinstance(value["Mounts"], list):
        raise _InventoryError("invalid_shape")
    for mount in value["Mounts"]:
        _keys(mount, {"Type", "Name"})
        if mount["Type"] not in ("volume", "bind", "tmpfs") or not isinstance(mount["Name"], str):
            raise _InventoryError("mount_unknown")
        if mount["Type"] == "volume" and _VOLUME.fullmatch(mount["Name"]) is None:
            raise _InventoryError("mount_unknown")


def _volume(value):
    _keys(value, {"Name", "Labels"})
    if not isinstance(value["Name"], str) or _VOLUME.fullmatch(value["Name"]) is None:
        raise _InventoryError("invalid_identity")
    _keys(value["Labels"], {OWNER_LABEL, DB_LABEL})
    if value["Labels"][DB_LABEL] not in (None, "", "required", "released"):
        raise _InventoryError("db_recovery_unknown")
    owner = value["Labels"][OWNER_LABEL]
    if owner is not None and not isinstance(owner, str):
        raise _InventoryError("owner_unknown")


def _qualified(proof: Proof | None, now: datetime) -> Proof:
    if proof is None:
        return Proof()
    if proof.state != "verified":
        return proof
    if (
        not proof.evidence
        or proof.observed_at is None
        or proof.valid_until is None
        or not proof.observed_at <= now <= proof.valid_until
    ):
        return Proof()
    return proof


def probe_docker(
    request: DockerProbeRequest,
    *,
    profile: DockerHostProfile,
    now: datetime,
    releases: DockerReleaseEvidence | None = None,
    runner: DockerRunner = run_docker_read,
) -> DockerProbeResult:
    """Produce R0 DockerFacts plus owner/source proofs and named holds.

    All container/image IDs are enumerated, including stopped/untagged ones.
    Child images are conservatively held by Parent or a shared layer prefix;
    absent lineage is unknown. A second census detects membership changes.
    Positive freshness is capped by the start time, never extended by slow I/O.
    An injected runner is a trusted transport, not user-controlled JSON.
    """
    request = DockerProbeRequest.model_validate(request.model_dump())
    profile = DockerHostProfile.model_validate(profile.model_dump())
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    if releases is not None:
        releases = DockerReleaseEvidence.model_validate(releases.model_dump())
    registration = request.registration
    digest = canonical_digest(registration)
    evidence: list[DockerReadEvidence] = []
    refs: list[EvidenceRef] = []
    holds: list[Hold] = []
    facts = {name: Proof() for name in DockerFacts.model_fields if name != "db_volume_permission"}
    owner, source = Proof(), Proof()
    permission = None
    start = time.monotonic()
    deadline = start + profile.total_timeout_seconds
    inventory_bytes = 0

    def result():
        return DockerProbeResult(
            registration=registration,
            registration_digest=digest,
            host_profile=request.host_profile,
            docker=DockerFacts(**facts, db_volume_permission=permission),
            owner_verified=owner,
            source_verified=source,
            holds=tuple(holds),
            evidence=tuple(evidence),
        )

    def proof(state="verified"):
        return Proof(
            state=state,
            evidence=tuple(refs),
            observed_at=now,
            valid_until=now + timedelta(seconds=profile.freshness_seconds),
        )

    if request.expected_registration_digest != digest:
        holds.append(Hold(code="plan_changed", subject="docker.registration_digest"))
    if request.host_profile != profile.profile_ref:
        holds.append(Hold(code="proof_denied", subject="docker.host_profile"))
    if registration not in profile.allowed_registrations:
        allowed_owners = {r.resource.owner for r in profile.allowed_registrations}
        holds.append(
            Hold(
                code="foreign_owner" if registration.resource.owner not in allowed_owners else "resource_mismatch",
                subject="docker.allowlist",
            )
        )
    if os.geteuid() != profile.execution_uid:
        holds.append(Hold(code="proof_unknown", subject="docker.inventory_complete.execution_uid"))
    if holds:
        return result()

    def read(operation, identity=None):
        nonlocal inventory_bytes
        call = DockerReadRequest(operation=operation, identity=identity)
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise _InventoryError("timeout")
        try:
            response = runner(call, profile=profile, timeout_seconds=min(profile.timeout_seconds, remaining))
        except PermissionError:
            raise _InventoryError("permission_denied") from None
        except FileNotFoundError:
            raise _InventoryError("missing_docker") from None
        except (TimeoutError, subprocess.TimeoutExpired):
            raise _InventoryError("timeout") from None
        except OSError:
            raise _InventoryError("io_error") from None
        if time.monotonic() > deadline:
            raise _InventoryError("timeout")
        if not isinstance(response, DockerReadResult):
            raise _InventoryError("invalid_runner_result")
        if response.status != "ok":
            raise _InventoryError(response.status)
        if response.returncode != 0:
            raise _InventoryError("command_failed")
        if len(response.stdout) > profile.max_output_bytes:
            raise _InventoryError("output_limit")
        inventory_bytes += len(response.stdout)
        if inventory_bytes > profile.max_inventory_bytes:
            raise _InventoryError("inventory_byte_limit")
        try:
            raw = response.stdout.decode("utf-8")
            if operation.endswith("_ids"):
                ids = raw.splitlines()
                pattern = _CONTAINER if operation == "container_ids" else _IMAGE
                if any(pattern.fullmatch(item) is None for item in ids):
                    raise _InventoryError("invalid_identity")
                value = sorted(set(ids))
                if len(value) > profile.max_objects:
                    raise _InventoryError("inventory_limit")
            else:
                value = json.loads(raw, object_pairs_hook=_unique_keys)
                {"image": _image, "container": _container, "volume": _volume}[operation](value)
                if value["Name" if operation == "volume" else "Id"] != identity:
                    raise _InventoryError("identity_mismatch")
        except (UnicodeError, ValueError, RecursionError):
            raise _InventoryError("invalid_json") from None
        packet = DockerReadEvidence(
            registration_digest=digest,
            host_profile=request.host_profile,
            observed_at=now,
            read=call,
            selected_stdout=json.dumps(value, sort_keys=True, separators=(",", ":")),
        )
        evidence.append(packet)
        evidence_digest = canonical_digest(packet)
        refs.append(EvidenceRef(ref="docker-read:" + evidence_digest[7:], digest=evidence_digest))
        return value

    try:
        container_ids, image_ids = read("container_ids"), read("image_ids")
        if len(container_ids) + len(image_ids) > profile.max_objects:
            raise _InventoryError("inventory_limit")
        containers = [read("container", identity) for identity in container_ids]
        images = [read("image", identity) for identity in image_ids]
        is_image = registration.resource.resource_kind == "docker_image"
        identity = registration.resource.identity
        if is_image:
            target = next((image for image in images if image["Id"] == identity), None)
            if target is None:
                raise _InventoryError("target_missing")
            labels = target["Config"]["Labels"]
        else:
            target = read("volume", identity)
            labels = target["Labels"]
        if read("container_ids") != container_ids or read("image_ids") != image_ids:
            raise _InventoryError("inventory_changed")
        if time.monotonic() - start > profile.freshness_seconds:
            raise _InventoryError("observation_expired")
    except _InventoryError as exc:
        holds.append(Hold(code="proof_unknown", subject="docker.inventory_complete." + exc.reason))
        return result()

    facts["inventory_complete"] = proof()
    owner = proof("verified" if labels[OWNER_LABEL] == registration.resource.owner.repository else "denied")
    if owner.state != "verified":
        holds.append(Hold(code="foreign_owner" if labels[OWNER_LABEL] else "proof_unknown", subject="docker.owner"))
    if is_image:
        digests = {identity} | {ref.rsplit("@", 1)[1] for ref in (target["RepoDigests"] or [])}
        source = proof("verified" if registration.source.digest in digests else "contradictory")
        layers = target["RootFS"]["Layers"]
        children = [
            image
            for image in images
            if image["Id"] != identity
            and (image["Parent"] == identity or image["RootFS"]["Layers"][: len(layers)] == layers)
        ]
        facts["no_child_images"] = proof("denied" if children else "verified")
        references = [c for c in containers if c["Image"] == identity]
    else:
        digests = set()
        holds.append(Hold(code="source_unknown", subject="docker.volume_content_digest"))
        facts["no_child_images"] = proof()  # image lineage cannot reference a named volume
        references = [
            c for c in containers if any(m["Type"] == "volume" and m["Name"] == identity for m in c["Mounts"])
        ]
    if source.state == "contradictory":
        holds.append(Hold(code="source_mismatch", subject="docker.source.digest"))
    for name, running in (("no_running_containers", True), ("no_stopped_containers", False)):
        facts[name] = proof("denied" if any(c["State"]["Running"] is running for c in references) else "verified")

    if releases is not None:
        if releases.registration_digest == digest and releases.host_profile == request.host_profile:
            facts["rollback_released"] = _qualified(releases.rollback_released, now)
            facts["db_volume_released"] = _qualified(releases.db_volume_released, now)
            permission = _qualified(releases.db_volume_permission, now) if releases.db_volume_permission else None
            if digests.intersection(releases.required_rollback_images):
                facts["rollback_released"] = proof("denied")
            if not is_image and identity in releases.required_db_volumes:
                facts["db_volume_released"] = proof("denied")
        else:
            holds.append(Hold(code="registration_changed", subject="docker.release_evidence"))
    if is_image and labels[ROLLBACK_LABEL] == "required":
        facts["rollback_released"] = proof("denied")
    if not is_image and labels[DB_LABEL] == "required":
        facts["db_volume_released"] = proof("denied")
    for name, claim in facts.items():
        if claim.state != "verified":
            holds.append(Hold(code="proof_" + claim.state, subject="docker." + name))
    if not is_image and (permission is None or permission.state != "verified"):
        holds.append(
            Hold(
                code="db_volume_permission_missing" if permission is None else "proof_" + permission.state,
                subject="docker.db_volume_permission",
            )
        )
    return result()


def _unique_keys(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result
