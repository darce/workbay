"""Read and update the repository's Canon pin."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import stat
import tomllib
from pathlib import Path
from tempfile import NamedTemporaryFile

from pydantic import ValidationError
from workbay_protocol.canon_lint import CanonAdapterConfig

from .canon_loader import CANON_MANIFEST_FILENAME, CanonLoadError, CanonRelease, _GROUPS, _read_release_file, load_canon
from .engine import CanonRunError

CONFIG_PATH = Path(".workbay/canon-lint.toml")
_PIN_KEYS = ("canon_version", "manifest_sha256")


def _parse_config(source: str) -> CanonAdapterConfig:
    try:
        return CanonAdapterConfig.model_validate(tomllib.loads(source))
    except ValidationError as exc:
        issues = exc.errors(include_url=False, include_context=False, include_input=False)
        detail = ", ".join(".".join(map(str, issue["loc"])) + " " + issue["msg"] for issue in issues)
        raise CanonRunError("config_invalid", f"Invalid {CONFIG_PATH}: {detail}") from exc
    except tomllib.TOMLDecodeError as exc:
        raise CanonRunError("config_invalid", f"Cannot load {CONFIG_PATH}: {type(exc).__name__}") from exc


def _read_config(path: Path) -> str:
    try:
        return path.read_bytes().decode("utf-8")
    except FileNotFoundError as exc:
        raise CanonRunError("config_missing", f"Expected repository config at {CONFIG_PATH}") from exc
    except (OSError, UnicodeError) as exc:
        raise CanonRunError("config_invalid", f"Cannot load {CONFIG_PATH}: {type(exc).__name__}") from exc


def load_config(root: Path) -> CanonAdapterConfig:
    return _parse_config(_read_config(root / CONFIG_PATH))


def verify_bump_target(root: Path) -> CanonRelease:
    """Read the candidate pin, then verify the release against that snapshot."""
    try:
        root = root.resolve(strict=True)
    except PermissionError as exc:
        raise CanonLoadError("canon_root_permission_denied", "Permission denied resolving the Canon root") from exc
    except (OSError, RuntimeError) as exc:
        raise CanonLoadError("canon_root_unavailable", "The candidate Canon release is unavailable") from exc
    raw = _read_release_file(root, CANON_MANIFEST_FILENAME, "manifest_missing")
    try:
        manifest = json.loads(raw)
    except (ValueError, UnicodeError) as exc:
        raise CanonLoadError("manifest_invalid", "Release manifest is not valid JSON") from exc
    version = manifest.get("version") if isinstance(manifest, dict) else None
    if not isinstance(version, str) or re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", version) is None:
        raise CanonLoadError("manifest_invalid", "Expected an explicit Canon release version")
    if not all(group in manifest for group in _GROUPS):
        raise CanonLoadError("manifest_invalid", "Expected all release file indexes")
    return load_canon(root, expected_version=version, expected_manifest_sha256=hashlib.sha256(raw).hexdigest())


def _render_pin(source: str, updated: CanonAdapterConfig) -> str:
    original = tomllib.loads(source)
    new_pin = {key: getattr(updated, key) for key in _PIN_KEYS}
    rendered = source
    for key, value in new_pin.items():
        if original[key] == value:
            continue
        assignment = re.compile(
            rf"""^([ \t]*(?:{key}|"{key}"|'{key}')[ \t]*=[ \t]*)"""
            r"""("(?:[^"\\\r\n]|\\[^\r\n])*"|'[^'\r\n]*')(?=[ \t]*(?:#[^\r\n]*)?\r?$)""",
            re.MULTILINE,
        )
        rendered, count = assignment.subn(lambda match: match[1] + json.dumps(value), rendered)
        if count != 1:
            raise CanonRunError("config_invalid", f"Expected one single-line {key} assignment in {CONFIG_PATH}")
    try:
        if tomllib.loads(rendered) == (original | new_pin):
            return rendered
    except tomllib.TOMLDecodeError as exc:
        raise CanonRunError("config_invalid", f"Cannot render the pin fields in {CONFIG_PATH}") from exc
    raise CanonRunError("config_invalid", f"Cannot update only the pin fields in {CONFIG_PATH}")


def update_pin(root: Path, release: CanonRelease) -> tuple[CanonAdapterConfig, str, str]:
    path = root / CONFIG_PATH
    temporary = None
    directory_fd = None
    try:
        if path.is_symlink() or path.parent.resolve() != path.parent:
            raise CanonRunError("config_invalid", f"Expected a repository file at {CONFIG_PATH}")
        try:
            metadata = path.stat()
        except FileNotFoundError as exc:
            raise CanonRunError("config_missing", f"Expected repository config at {CONFIG_PATH}") from exc
        if not stat.S_ISREG(metadata.st_mode):
            raise CanonRunError("config_invalid", f"Expected a regular repository file at {CONFIG_PATH}")
        directory_fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        # Lock the directory because replacing the file also replaces its inode.
        try:
            fcntl.flock(directory_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise CanonRunError("config_busy", f"Another pin update is writing {CONFIG_PATH}") from exc
        source = _read_config(path)
        previous = _parse_config(source)
        if (previous.canon_version, previous.manifest_sha256) == (release.version, release.manifest_sha256):
            return previous, source, source
        updated = previous.model_copy(
            update={"canon_version": release.version, "manifest_sha256": release.manifest_sha256}
        )
        rendered = _render_pin(source, updated)
        with NamedTemporaryFile(dir=path.parent, prefix=f".{path.name}.", delete=False) as stream:
            temporary = Path(stream.name)
            stream.write(rendered.encode("utf-8"))
            stream.flush()
            os.fchmod(stream.fileno(), stat.S_IMODE(metadata.st_mode))
            os.fsync(stream.fileno())
        if _read_config(path) != source:
            raise CanonRunError("config_changed", f"{CONFIG_PATH} changed during the pin update")
        temporary.replace(path)
        return previous, source, rendered
    except OSError as exc:
        raise CanonRunError("config_write_failed", f"Cannot update {CONFIG_PATH}: {type(exc).__name__}") from exc
    finally:
        try:
            if temporary is not None:
                temporary.unlink(missing_ok=True)
        finally:
            if directory_fd is not None:
                os.close(directory_fd)
