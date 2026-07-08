"""Download and cache hash-pinned embedding artifacts (C1 / S2).

Stdlib-only HTTP fetch with redirect follow, optional ``HF_TOKEN``, and bounded
429 retry/backoff. Content-addressed cache under ``~/.cache/workbay/models/``
(XDG-aware). Idempotent: a cached file that verifies is never re-downloaded.
"""

from __future__ import annotations

import hashlib
import os
import sys
import tempfile
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
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
        raise EmbeddingProvisionUnavailable(
            "mcp-workbay-handoff is not importable; cannot read the embedding "
            "digest pin — skipping provisioning (provider stays unconfigured)"
        ) from exc
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
        "WORKBAY_REINJECT_SEMANTIC=1",
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


def embeddings_disabled() -> bool:
    """Runtime kill-switch honored at install/repair (Dist-2 owns provider gate)."""
    return os.environ.get("WORKBAY_HANDOFF_EMBEDDINGS_DISABLED", "").strip().lower() in {
        "1",
        "true",
        "yes",
    }


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

EMBEDDINGS_DISABLED_ENV_KEY = "WORKBAY_HANDOFF_EMBEDDINGS_DISABLED"
_TRUE_DISABLED_VALUES = frozenset({"1", "true", "yes"})


def _embeddings_disabled_value(raw: str) -> bool:
    return raw.strip().lower() in _TRUE_DISABLED_VALUES


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
        new_lines = _upsert_embedding_env_lines(
            lines, EMBEDDINGS_DISABLED_ENV_KEY, value=None
        )
        if new_lines == lines:
            return
        if not new_lines or all(not ln.strip() for ln in new_lines):
            path.unlink(missing_ok=True)
            return
        _atomic_write_embedding_env_lines(path, new_lines)
        return

    lines = path.read_text(encoding="utf-8").splitlines() if path.is_file() else []
    new_lines = _upsert_embedding_env_lines(
        lines, EMBEDDINGS_DISABLED_ENV_KEY, value="1"
    )
    if path.is_file() and new_lines == lines:
        return
    _atomic_write_embedding_env_lines(path, new_lines)


def embeddings_gate_status(worktree_root: Path) -> dict[str, object]:
    """Report enabled/disabled state and where it was read from."""
    parsed = parse_embedding_env_file(worktree_root)
    if EMBEDDINGS_DISABLED_ENV_KEY in parsed:
        disabled = _embeddings_disabled_value(parsed[EMBEDDINGS_DISABLED_ENV_KEY])
        return {
            "enabled": not disabled,
            "disabled": disabled,
            "source": "embedding.env",
        }
    return {"enabled": True, "disabled": False, "source": "default"}
