"""Download and cache hash-pinned embedding artifacts (C1 / S2).

Stdlib-only HTTP fetch with redirect follow, optional ``HF_TOKEN``, and bounded
429 retry/backoff. Content-addressed cache under ``~/.cache/workbay/models/``
(XDG-aware). Idempotent: a cached file that verifies is never re-downloaded.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import sys
import tempfile
import time
import urllib.error
import urllib.request
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Literal

from workbay_protocol.bootstrap import BootstrapManifest
from workbay_protocol.embedding_policy import (
    OPERATOR_POLICY_FIELD,
    POLICY_REVISION_ENV_KEY,
    EmbeddingPolicyError,
    EmbeddingPolicyRead,
    derive_migration_decision,
    load_embeddings_policy,
    require_readable_policy,
)
from workbay_protocol.paths import MANIFEST_NAME_PRECEDENCE

_MAX_429_RETRIES = 3
_BACKOFF_BASE_SEC = 1.0
_HASH_CHUNK = 1 << 20


class EmbeddingProvisionError(RuntimeError):
    """Fatal provisioning failure (digest mismatch after download)."""


class EmbeddingProvisionUnavailable(RuntimeError):
    """Source unreachable; caller should warn and degrade (provider stays None)."""


@dataclass(frozen=True)
class ProvisionedArtifacts:
    """Paths to verified model + tokenizer bytes in the shared cache."""

    model_path: Path
    tokenizer_path: Path


@dataclass(frozen=True)
class _ArtifactPin:
    filename: str
    expected_sha256: str


def _load_model_pin():
    try:
        from workbay_handoff_mcp.embeddings.model_pin import MODEL_PIN
    except ImportError as exc:
        # A missing handoff package is an environment-unavailable condition, not
        # a digest mismatch: the two-tier contract says it must warn+degrade
        # (provider stays None), never crash a default-active install/repair.
        #
        # Distinguish the two distinct causes so the remedy is actionable (D5,
        # implementation note): the import above transitively runs
        # ``workbay_handoff_mcp.embeddings.__init__`` → ``provider`` (imports
        # numpy/onnxruntime/tokenizers), so a present package that lacks the
        # ``[embeddings]`` extra fails here even though the base package imports
        # fine. Probe the base package to tell "package missing" from
        # "extra missing" and name the real fix instead of chasing a phantom
        # missing package.
        # PMH-F1: a plain ImportError that is NOT ModuleNotFoundError means the
        # module imported but the *symbol* is gone (e.g. ``cannot import name
        # 'MODEL_PIN'``) — a code/version regression, NOT a missing package or
        # extra. Critically, by the time such an error fires the parent
        # ``workbay_handoff_mcp`` is already cached in ``sys.modules``, so the
        # base-package probe below would SUCCEED and misreport it as "extra
        # missing" (and reinstalling never fixes it). Split it out first.
        if not isinstance(exc, ModuleNotFoundError):
            message = (
                "mcp-workbay-handoff imported but the embedding digest pin symbol "
                f"is unavailable ({exc}); this is a code/version regression (a "
                "renamed or removed export), NOT a missing package or [embeddings] "
                "extra — reinstalling will not fix it. Verify the installed "
                "mcp-workbay-handoff version matches this bootstrap."
            )
        else:
            try:
                import workbay_handoff_mcp  # noqa: F401
            except ImportError:
                # Broad ImportError (not just ModuleNotFoundError): a base package
                # that is absent OR itself broken both mean "install/repair the
                # package", and neither must crash the warn+degrade contract.
                #
                # Importability is per-interpreter, so "not importable" does NOT
                # imply "not installed": the dogfood smoke hits this branch with
                # mcp-workbay-handoff installed and healthy in its own uv tool
                # venv, simply invisible to the interpreter running the installer.
                # A bare "install the handoff package" then points the operator at
                # a non-fix (they reinstall, nothing changes). Name the probed
                # interpreter — same convention as the doctor's version_of_skew
                # facet — so the scope of the claim is legible.
                message = (
                    "mcp-workbay-handoff is not importable from the installer "
                    f"interpreter ({sys.executable}); cannot read the embedding "
                    "digest pin — skipping provisioning (provider stays "
                    "unconfigured). This is interpreter scope, not necessarily "
                    "absence: the package may be installed and healthy in another "
                    "environment (e.g. its own uv tool venv) and still be "
                    "invisible here, in which case reinstalling it changes "
                    "nothing. Fix by making it importable from THIS interpreter "
                    "(git-only: uv tool install the workbay closure per "
                    "docs/CONSUMER.md)."
                )
            else:
                message = (
                    "mcp-workbay-handoff is installed but its [embeddings] extra "
                    "(onnxruntime/tokenizers/numpy) is missing; cannot read the "
                    "embedding digest pin — skipping provisioning (provider stays "
                    "unconfigured). Reinstall mcp-workbay-handoff WITH its "
                    "[embeddings] extra — attach the extra to the package spec "
                    "(e.g. 'mcp-workbay-handoff[embeddings]'; for a git spec put "
                    "the extra on the package name, not the URL). See "
                    "docs/CONSUMER.md for the base git-only install recipe to extend."
                )
        raise EmbeddingProvisionUnavailable(message) from exc
    return MODEL_PIN


def models_cache_root() -> Path:
    """Shared content-addressed cache root (honors ``XDG_CACHE_HOME``)."""
    xdg = os.environ.get("XDG_CACHE_HOME", "").strip()
    if xdg:
        return Path(xdg).expanduser() / "workbay" / "models"
    return Path.home() / ".cache" / "workbay" / "models"


def artifact_cache_path(content_sha256: str, basename: str) -> Path:
    """Destination path for one pinned artifact inside the shared cache."""
    return models_cache_root() / content_sha256 / basename


def hf_resolve_url(source_repo: str, source_revision: str, filename: str) -> str:
    return f"https://huggingface.co/{source_repo}/resolve/{source_revision}/{filename}"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(_HASH_CHUNK), b""):
            digest.update(chunk)
    return digest.hexdigest()


class _AuthStrippingRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Drop the ``Authorization`` header when a redirect crosses hosts.

    HuggingFace ``resolve`` URLs 302 to a CDN host; urllib otherwise copies the
    optional ``HF_TOKEN`` Bearer token onto the redirected request, leaking the
    credential to the CDN. Strip it on any cross-host hop.
    """

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        new_req = super().redirect_request(req, fp, code, msg, headers, newurl)
        if new_req is not None:
            old_host = urllib.parse.urlsplit(req.full_url).hostname
            new_host = urllib.parse.urlsplit(newurl).hostname
            if old_host != new_host:
                new_req.headers.pop("Authorization", None)
                new_req.unredirected_hdrs.pop("Authorization", None)
        return new_req


def _build_opener() -> urllib.request.OpenerDirector:
    return urllib.request.build_opener(_AuthStrippingRedirectHandler())


def _download_once(url: str, dest: Path, opener: urllib.request.OpenerDirector) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    request = urllib.request.Request(url)
    token = os.environ.get("HF_TOKEN", "").strip()
    if token:
        request.add_header("Authorization", f"Bearer {token}")
    # 429 (and other 4xx/5xx) are raised as urllib.error.HTTPError by the
    # opener's HTTPErrorProcessor before we get here; the retry/backoff in
    # _download_with_retry handles them. A returned response is already 2xx.
    with opener.open(request, timeout=120) as response:  # noqa: S310
        with dest.open("wb") as handle:
            while True:
                chunk = response.read(_HASH_CHUNK)
                if not chunk:
                    break
                handle.write(chunk)


def _download_with_retry(
    url: str,
    dest: Path,
    opener: urllib.request.OpenerDirector,
) -> None:
    last_exc: Exception | None = None
    for attempt in range(_MAX_429_RETRIES + 1):
        try:
            _download_once(url, dest, opener)
            return
        except urllib.error.HTTPError as exc:
            last_exc = exc
            if exc.code == 429 and attempt < _MAX_429_RETRIES:
                time.sleep(_BACKOFF_BASE_SEC * (2**attempt))
                continue
            raise
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            raise
    if last_exc is not None:
        raise last_exc


def _verify_or_raise(path: Path, expected_sha256: str) -> None:
    actual = sha256_file(path)
    if actual != expected_sha256:
        raise EmbeddingProvisionError(
            f"embedding artifact hash mismatch for {path}: expected {expected_sha256}, got {actual}"
        )


def _ensure_cached(
    pin: _ArtifactPin,
    *,
    source_repo: str,
    source_revision: str,
    opener: urllib.request.OpenerDirector,
) -> Path:
    basename = Path(pin.filename).name
    dest = artifact_cache_path(pin.expected_sha256, basename)
    if dest.is_file():
        # The cache key IS the expected digest, so a cached file that fails to
        # verify is corrupted (partial write from an older crash, bit-rot) — not
        # a pin mismatch. Self-heal: drop it and re-download rather than fail fatally.
        if sha256_file(dest) == pin.expected_sha256:
            return dest
        dest.unlink(missing_ok=True)

    url = hf_resolve_url(source_repo, source_revision, pin.filename)
    dest.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=".download-", dir=dest.parent)
    os.close(fd)
    tmp_path = Path(tmp_name)
    try:
        _download_with_retry(url, tmp_path, opener)
        _verify_or_raise(tmp_path, pin.expected_sha256)
        os.replace(tmp_path, dest)
        return dest
    except EmbeddingProvisionError:
        tmp_path.unlink(missing_ok=True)
        raise
    except Exception as exc:
        tmp_path.unlink(missing_ok=True)
        raise EmbeddingProvisionUnavailable(f"failed to download {pin.filename}: {exc}") from exc
    finally:
        if tmp_path.exists() and not dest.exists():
            tmp_path.unlink(missing_ok=True)


EMBEDDING_ENV_REL = Path(".workbay/embedding.env")
EMBEDDINGS_DISABLED_ENV_KEY = "WORKBAY_HANDOFF_EMBEDDINGS_DISABLED"
REINJECT_SEMANTIC_ENV_KEY = "WORKBAY_REINJECT_SEMANTIC"
_TRUE_DISABLED_VALUES = frozenset({"1", "true", "yes", "on"})
_FALSE_DISABLED_VALUES = frozenset({"0", "false", "no", "off"})


def _parse_env_line(line: str) -> tuple[str, str] | None:
    stripped = line.strip()
    if not stripped or stripped.startswith("#") or "=" not in stripped:
        return None
    key, _, value = stripped.partition("=")
    key, value = key.strip(), value.strip()
    if not key:
        return None
    return key, value


def parse_embedding_env_file(worktree_root: Path) -> dict[str, str]:
    """Read ``.workbay/embedding.env`` when present; else empty dict."""
    path = embedding_env_path(worktree_root)
    if not path.is_file():
        return {}
    parsed: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        item = _parse_env_line(line)
        if item is not None:
            parsed[item[0]] = item[1]
    return parsed


def embedding_env_path(worktree_root: Path) -> Path:
    """Per-worktree harness-neutral env surface for embedding artifact vars."""
    return worktree_root / EMBEDDING_ENV_REL


def render_embedding_env_content(artifacts: ProvisionedArtifacts, model_pin) -> str:
    """KEY=VALUE body for the four artifact vars plus semantic activation."""
    lines = [
        f"WORKBAY_HANDOFF_EMBEDDING_MODEL={artifacts.model_path}",
        f"WORKBAY_HANDOFF_EMBEDDING_TOKENIZER={artifacts.tokenizer_path}",
        f"WORKBAY_HANDOFF_EMBEDDING_MODEL_SHA256={model_pin.model_sha256}",
        f"WORKBAY_HANDOFF_EMBEDDING_TOKENIZER_SHA256={model_pin.tokenizer_sha256}",
        f"{REINJECT_SEMANTIC_ENV_KEY}=1",
    ]
    return "\n".join(lines) + "\n"


def write_embedding_env_file(
    worktree_root: Path,
    artifacts: ProvisionedArtifacts,
    *,
    model_pin=None,
) -> bool:
    """Atomically write ``.workbay/embedding.env`` when missing or changed.

    Returns ``True`` when the file was (re)written, ``False`` on no-op.
    """
    pin = model_pin if model_pin is not None else _load_model_pin()
    content = render_embedding_env_content(artifacts, pin)
    dest = embedding_env_path(worktree_root)
    if dest.is_file() and dest.read_text(encoding="utf-8") == content:
        return False
    dest.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=".embedding.env-", dir=dest.parent)
    os.close(fd)
    tmp_path = Path(tmp_name)
    try:
        tmp_path.write_text(content, encoding="utf-8")
        os.replace(tmp_path, dest)
    finally:
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)
    return True


def provision(
    *,
    worktree_root: Path | str | None = None,
    opener: urllib.request.OpenerDirector | None = None,
) -> ProvisionedArtifacts:
    """Download (if needed) and verify both pinned artifacts into the shared cache."""
    model_pin = _load_model_pin()
    effective_opener = opener or _build_opener()
    artifacts = (
        _ArtifactPin(model_pin.model_filename, model_pin.model_sha256),
        _ArtifactPin(model_pin.tokenizer_filename, model_pin.tokenizer_sha256),
    )
    paths: list[Path] = []
    for spec in artifacts:
        paths.append(
            _ensure_cached(
                spec,
                source_repo=model_pin.source_repo,
                source_revision=model_pin.source_revision,
                opener=effective_opener,
            )
        )
    artifacts = ProvisionedArtifacts(model_path=paths[0], tokenizer_path=paths[1])
    if worktree_root is not None:
        write_embedding_env_file(Path(worktree_root), artifacts, model_pin=model_pin)
    return artifacts


def _embeddings_disabled_value(raw: str) -> bool:
    """Strict boolean parse for ``WORKBAY_HANDOFF_EMBEDDINGS_DISABLED`` (OBS-08)."""
    token = raw.strip().lower()
    if token in _TRUE_DISABLED_VALUES:
        return True
    if token in _FALSE_DISABLED_VALUES:
        return False
    raise EmbeddingPolicyError(
        "invalid embeddings policy: "
        f"{EMBEDDINGS_DISABLED_ENV_KEY}={raw!r} is not a boolean "
        "(expected 1/true/yes/on or 0/false/no/off)"
    )


def embeddings_disabled() -> bool:
    """Runtime kill-switch honored at install/repair (Dist-2 owns provider gate)."""
    if EMBEDDINGS_DISABLED_ENV_KEY not in os.environ:
        return False
    return _embeddings_disabled_value(os.environ[EMBEDDINGS_DISABLED_ENV_KEY])


def _record_embedding_degrade(summary: str, *, worktree_root: Path | str) -> None:
    try:
        from workbay_handoff_mcp.agent_errors import record_agent_error_direct

        result = record_agent_error_direct(
            error_class="env_misconfig",
            summary=summary,
            detail=summary,
            tool_name="workbay-bootstrap",
            harness="installer",
            cwd=worktree_root,
        )
        if not result.get("ok"):
            sys.stderr.write(f"agent_errors installer capture failed: {result.get('error')}\n")
    except ImportError:
        # Telemetry is best-effort and the handoff package is not importable from
        # every interpreter that can run the installer (consumer-profile installs
        # omit it entirely; the dogfood smoke has it installed but in a separate
        # uv tool venv). Reporting that as a bare "capture failed:
        # ModuleNotFoundError" reads as a broken install, so say what actually
        # happened — the capture was skipped, the install is unaffected — and name
        # the interpreter the claim is scoped to.
        sys.stderr.write(
            "agent_errors installer capture skipped: mcp-workbay-handoff is not "
            f"importable from the installer interpreter ({sys.executable}); "
            "telemetry only, the install itself is unaffected\n"
        )
    except Exception as exc:  # noqa: BLE001 - installer telemetry must not block install
        sys.stderr.write(f"agent_errors installer capture failed: {type(exc).__name__}: {exc}\n")


def maybe_provision_embeddings(
    worktree_root: Path | str,
    *,
    no_embeddings: bool = False,
) -> list[str]:
    """Default-active provision hook for install/repair.

    Returns advisory warning lines on offline/unavailable degrade. Raises
    :class:`EmbeddingProvisionError` on digest mismatch (fatal).
    """
    if no_embeddings or embeddings_disabled():
        return []
    try:
        provision(worktree_root=worktree_root)
        return []
    except EmbeddingProvisionUnavailable as exc:
        warning = f"embedding provision skipped: {exc}"
        _record_embedding_degrade(warning, worktree_root=worktree_root)
        return [warning]


def embeddings_gate_disabled_from_file(worktree_root: Path) -> bool:
    """Whether ``WORKBAY_HANDOFF_EMBEDDINGS_DISABLED`` is set in ``embedding.env``."""
    raw = parse_embedding_env_file(worktree_root).get(EMBEDDINGS_DISABLED_ENV_KEY)
    if raw is None:
        return False
    return _embeddings_disabled_value(raw)


def _upsert_embedding_env_lines(
    lines: list[str],
    key: str,
    *,
    value: str | None,
) -> list[str]:
    """Update or remove ``key`` while preserving unrelated lines (incl. comments)."""
    out: list[str] = []
    found = False
    for line in lines:
        parsed = _parse_env_line(line)
        if parsed is not None and parsed[0] == key:
            found = True
            if value is not None:
                out.append(f"{key}={value}")
        else:
            out.append(line)
    if value is not None and not found:
        if out and out[-1].strip():
            out.append(f"{key}={value}")
        else:
            out.append(f"{key}={value}")
    return out


def _atomic_write_embedding_env_lines(path: Path, lines: list[str]) -> None:
    content = "\n".join(lines)
    if lines:
        content += "\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=".embedding.env-", dir=path.parent)
    os.close(fd)
    tmp_path = Path(tmp_name)
    try:
        tmp_path.write_text(content, encoding="utf-8")
        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)


def set_embeddings_gate(worktree_root: Path, *, enabled: bool) -> None:
    """Persist the SSOT disable gate in ``.workbay/embedding.env``."""
    path = embedding_env_path(worktree_root)
    if enabled:
        if not path.is_file():
            return
        lines = path.read_text(encoding="utf-8").splitlines()
        new_lines = _upsert_embedding_env_lines(lines, EMBEDDINGS_DISABLED_ENV_KEY, value=None)
        if new_lines == lines:
            return
        if not new_lines or all(not ln.strip() for ln in new_lines):
            path.unlink(missing_ok=True)
            return
        _atomic_write_embedding_env_lines(path, new_lines)
        return

    lines = path.read_text(encoding="utf-8").splitlines() if path.is_file() else []
    new_lines = _upsert_embedding_env_lines(lines, EMBEDDINGS_DISABLED_ENV_KEY, value="1")
    if path.is_file() and new_lines == lines:
        return
    _atomic_write_embedding_env_lines(path, new_lines)


def embeddings_gate_status(worktree_root: Path) -> dict[str, object]:
    """Report file-gate state plus process-env effective overlay.

    ``enabled``/``disabled``/``source`` describe the **file gate** only (script
    compatibility). ``process_env_disabled`` and ``effective_enabled`` surface
    whether the process-env kill-switch still suppresses the provider.
    """
    process_env_disabled = embeddings_disabled()
    parsed = parse_embedding_env_file(worktree_root)
    if EMBEDDINGS_DISABLED_ENV_KEY in parsed:
        disabled = _embeddings_disabled_value(parsed[EMBEDDINGS_DISABLED_ENV_KEY])
        file_enabled = not disabled
        return {
            "enabled": file_enabled,
            "disabled": disabled,
            "source": "embedding.env",
            "process_env_disabled": process_env_disabled,
            "effective_enabled": file_enabled and not process_env_disabled,
        }
    return {
        "enabled": True,
        "disabled": False,
        "source": "default",
        "process_env_disabled": process_env_disabled,
        "effective_enabled": not process_env_disabled,
    }


def _manifest_path_for_write(root: Path) -> Path | None:
    for name in MANIFEST_NAME_PRECEDENCE:
        candidate = root / name
        if candidate.is_file():
            return candidate
    return None


@contextmanager
def _bootstrap_writer_lock(root: Path) -> Iterator[None]:
    lock_dir = Path(root) / ".workbay"
    lock_dir.mkdir(parents=True, exist_ok=True)
    lock_path = lock_dir / "bootstrap-writer.lock"
    fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


def _atomic_write_json(path: Path, data: dict[str, object]) -> None:
    fd, tmp_name = tempfile.mkstemp(prefix=".manifest-", dir=path.parent)
    os.close(fd)
    tmp_path = Path(tmp_name)
    try:
        tmp_path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)


def _same_operator_intent(
    existing: dict[object, object],
    embeddings_policy: str,
    reinjection_policy: str,
    origin: str,
    intent: str,
) -> bool:
    return (
        existing.get("embeddings_policy") == embeddings_policy
        and existing.get("reinjection_policy") == reinjection_policy
        and existing.get("origin") == origin
        and existing.get("intent") == intent
    )


def _persist_operator_policy_locked(
    root: Path,
    *,
    embeddings_policy: Literal["off", "preferred", "required"],
    reinjection_policy: Literal["off", "on"],
    origin: Literal["explicit", "migrated", "default"],
    intent: Literal["known", "unknown"],
) -> EmbeddingPolicyRead:
    path = _manifest_path_for_write(root)
    if path is None:
        raise EmbeddingPolicyError("invalid embeddings operator policy: manifest missing; cannot persist")
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise EmbeddingPolicyError(f"invalid embeddings operator policy: unreadable ({exc})") from exc
    try:
        data = json.loads(raw)
    except ValueError as exc:
        raise EmbeddingPolicyError(f"invalid embeddings operator policy: malformed ({exc})") from exc
    if not isinstance(data, dict):
        raise EmbeddingPolicyError("invalid embeddings operator policy: malformed")

    existing = data.get(OPERATOR_POLICY_FIELD)
    existing_dict = existing if isinstance(existing, dict) else {}
    revision = 1
    if isinstance(existing_dict.get("policy_revision"), int) and not isinstance(
        existing_dict.get("policy_revision"), bool
    ):
        if _same_operator_intent(
            existing_dict,
            embeddings_policy,
            reinjection_policy,
            origin,
            intent,
        ):
            revision = existing_dict["policy_revision"]
        else:
            revision = existing_dict["policy_revision"] + 1

    record = dict(existing_dict)
    record.update(
        {
            "embeddings_policy": embeddings_policy,
            "reinjection_policy": reinjection_policy,
            "policy_revision": revision,
            "origin": origin,
            "intent": intent,
        }
    )
    data[OPERATOR_POLICY_FIELD] = record
    try:
        BootstrapManifest.model_validate(data)
    except Exception as exc:  # noqa: BLE001 - surface schema failures as policy errors
        raise EmbeddingPolicyError(f"invalid embeddings operator policy: {exc}") from exc
    _atomic_write_json(path, data)
    return load_embeddings_policy(root)


def _project_embedding_env_from_policy(root: Path, policy: EmbeddingPolicyRead) -> None:
    if policy.embeddings_policy is None or policy.policy_revision is None:
        raise EmbeddingPolicyError("invalid embeddings operator policy: missing policy for projection")
    path = embedding_env_path(root)
    policy_off = policy.embeddings_policy == "off"
    lines = path.read_text(encoding="utf-8").splitlines() if path.is_file() else []
    disabled_value = "1" if policy_off else None
    lines = _upsert_embedding_env_lines(lines, EMBEDDINGS_DISABLED_ENV_KEY, value=disabled_value)
    if policy.reinjection_policy is not None:
        reinject = "0" if policy.reinjection_policy == "off" else "1"
        lines = _upsert_embedding_env_lines(lines, REINJECT_SEMANTIC_ENV_KEY, value=reinject)
    lines = _upsert_embedding_env_lines(lines, POLICY_REVISION_ENV_KEY, value=str(policy.policy_revision))
    _atomic_write_embedding_env_lines(path, lines)


def embeddings_projection_disagreement(worktree_root: Path) -> dict[str, object] | None:
    """Named disagreement between persisted policy and derived embedding.env."""
    root = Path(worktree_root)
    policy = load_embeddings_policy(root)
    if policy.status != "ok" or policy.embeddings_policy is None or policy.policy_revision is None:
        return None
    parsed = parse_embedding_env_file(root)
    mismatched: list[str] = []

    expected_revision = str(policy.policy_revision)
    env_rev = parsed.get(POLICY_REVISION_ENV_KEY)
    if env_rev != expected_revision:
        mismatched.append(POLICY_REVISION_ENV_KEY)

    policy_off = policy.embeddings_policy == "off"
    raw_disabled = parsed.get(EMBEDDINGS_DISABLED_ENV_KEY)
    file_off = False if raw_disabled is None else _embeddings_disabled_value(raw_disabled)
    if file_off != policy_off:
        mismatched.append(EMBEDDINGS_DISABLED_ENV_KEY)

    if policy.reinjection_policy is not None:
        expected_reinject = "0" if policy.reinjection_policy == "off" else "1"
        if parsed.get(REINJECT_SEMANTIC_ENV_KEY) != expected_reinject:
            mismatched.append(REINJECT_SEMANTIC_ENV_KEY)

    if not mismatched:
        return None
    return {
        "kind": "embeddings_policy_projection_disagreement",
        "policy_revision": policy.policy_revision,
        "projected_revision": env_rev,
        "keys": mismatched,
        "repair": "re-derive embedding.env from embeddings_operator_policy",
    }


def repair_embeddings_projection(worktree_root: Path) -> EmbeddingPolicyRead:
    """Re-derive ``embedding.env`` from persisted operator policy."""
    root = Path(worktree_root)
    with _bootstrap_writer_lock(root):
        policy = require_readable_policy(load_embeddings_policy(root))
        if policy.status != "ok" or policy.embeddings_policy is None:
            raise EmbeddingPolicyError("invalid embeddings operator policy: nothing persisted to repair from")
        _project_embedding_env_from_policy(root, policy)
        return load_embeddings_policy(root)


def migrate_embeddings_policy(worktree_root: Path) -> str:
    """Idempotent migration: persist policy from receipt + file gate, then project.

    Once a policy is recorded, later migrate calls do not re-read the
    projection as intent and do not auto-repair an interrupted env file.
    """
    root = Path(worktree_root)
    with _bootstrap_writer_lock(root):
        current = require_readable_policy(load_embeddings_policy(root))
        # Parse the file gate even when policy is already persisted so a
        # malformed DISABLED value cannot report success (OBS-08).
        file_gate_disabled = embeddings_gate_disabled_from_file(root)
        if current.status == "ok":
            return "already_migrated"
        decision = derive_migration_decision(
            embeddings_mode=current.embeddings_mode,
            file_gate_disabled=file_gate_disabled,
        )
        if not decision.persist:
            return decision.report
        if decision.embeddings_policy is None or decision.reinjection_policy is None:
            return decision.report
        persisted = _persist_operator_policy_locked(
            root,
            embeddings_policy=decision.embeddings_policy,
            reinjection_policy=decision.reinjection_policy,
            origin=decision.origin,
            intent=decision.intent,
        )
        _project_embedding_env_from_policy(root, persisted)
        return decision.report


def embeddings_three_facts(worktree_root: Path) -> dict[str, object]:
    """Receipt, operator policy, and availability (availability is not inferred)."""
    policy = load_embeddings_policy(worktree_root)
    receipt_payload: dict[str, object] = {
        "embeddings_mode": policy.embeddings_mode,
        "inference": None
        if policy.inference_receipt is None
        else policy.inference_receipt.model_dump(exclude_none=True),
    }
    return {
        "receipt": receipt_payload,
        "policy": {
            "status": policy.status,
            "embeddings_policy": policy.embeddings_policy,
            "reinjection_policy": policy.reinjection_policy,
            "policy_revision": policy.policy_revision,
            "origin": policy.origin,
            "intent": policy.intent,
        },
        "availability": {
            "status": "unknown",
            "reason": "not_observed",
            "observed_at": None,
            "provider_id": None,
        },
    }


def _apply_operator_policy(
    root: Path,
    *,
    embeddings_policy: Literal["off", "preferred", "required"],
    reinjection_policy: Literal["off", "on"],
) -> None:
    with _bootstrap_writer_lock(root):
        if _manifest_path_for_write(root) is None:
            set_embeddings_gate(root, enabled=(embeddings_policy != "off"))
            return
        require_readable_policy(load_embeddings_policy(root))
        persisted = _persist_operator_policy_locked(
            root,
            embeddings_policy=embeddings_policy,
            reinjection_policy=reinjection_policy,
            origin="explicit",
            intent="known",
        )
        _project_embedding_env_from_policy(root, persisted)


def run_embeddings_cli(worktree_root: Path, *, status: bool, enable: bool, disable: bool) -> int:
    """Shared operator surface for ``workbay embeddings`` / bootstrap alias.

    Takes the three mutually-exclusive mode flags (as parsed by either front
    door's argparse group) and derives the mode string once here — the single
    place status/enable/disable maps to an action. Prints JSON or a short
    status line to stdout. Returns 0 on success, 2 on filesystem I/O or decode
    errors (permission, missing parent, corrupt ``embedding.env``). Reports the
    **file gate** in ``.workbay/embedding.env`` only — process-env kill-switch
    is an install-time override, not cleared by ``--enable``.
    Status also exposes receipt, operator policy, and availability facts.
    ``--enable`` never manufactures a verified inference receipt.
    ``--disable`` persists operator off before projecting the compatibility file.
    """
    if status:
        mode = "status"
    elif enable:
        mode = "enable"
    elif disable:
        mode = "disable"
    else:
        raise ValueError("run_embeddings_cli requires exactly one of status/enable/disable")
    root = worktree_root.resolve()
    try:
        if mode == "status":
            payload = dict(embeddings_gate_status(root))
            payload.update(embeddings_three_facts(root))
            payload["projection_disagreement"] = embeddings_projection_disagreement(root)
            print(json.dumps(payload), file=sys.stdout)
            return 0
        if mode == "disable":
            _apply_operator_policy(root, embeddings_policy="off", reinjection_policy="off")
        else:
            _apply_operator_policy(root, embeddings_policy="preferred", reinjection_policy="on")
        state = embeddings_gate_status(root)
        label = "enabled" if state["enabled"] else "disabled"
        print(f"embeddings: {label} (source={state['source']})", file=sys.stdout)
        return 0
    except EmbeddingPolicyError as exc:
        print(f"embeddings: error: {exc}", file=sys.stderr)
        return 2
    except (OSError, UnicodeError) as exc:
        print(f"embeddings: error: {exc}", file=sys.stderr)
        return 2
