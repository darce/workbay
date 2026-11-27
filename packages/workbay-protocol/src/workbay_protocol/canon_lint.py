from __future__ import annotations

import re
from datetime import date, datetime
from enum import IntEnum
from typing import Final, Literal, Protocol

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    StrictInt,
    StrictStr,
    ValidationInfo,
    field_validator,
)

CANON_LINT_ADAPTER_SCHEMA: Final[str] = "workbay-canon-lint/adapter@1"
CANON_LINT_REPORT_SCHEMA: Final[str] = "workbay-canon-lint/report@1"

CanonLintTier = Literal["B", "S", "J"]
CanonLintCommand = Literal["lint", "check-drift"]
CanonScanRecord = tuple[str, int, str]

_SHA256_HEX_RE = re.compile(r"^[0-9a-f]{64}$")
_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_WINDOWS_DRIVE_RE = re.compile(r"^[A-Za-z]:")
_GLOB_META_CHARS = frozenset("*?[]")


class CanonLintExitCode(IntEnum):
    """Canonical CLI exit codes for workbay canon-lint."""

    OK = 0
    BLOCKING = 1
    USAGE = 2
    RUNTIME_ERROR = 3
    STRONG_UNEXEMPTED = 5


class CanonScanAdapter(Protocol):
    """Shared scan adapter interface for independent language/format scanners."""

    def scan(self, source: str, check_ids: frozenset[str]) -> list[CanonScanRecord]:
        """Scan source text and emit (check_id, 1-based line, evidence_snippet) records."""
        ...


def _require_nonblank(value: str, field_name: str) -> str:
    if not value.strip():
        raise ValueError(f"{field_name} must not be blank")
    if "\x00" in value:
        raise ValueError(f"{field_name} must not contain NUL bytes")
    return value


def _require_sha256_hex(value: str, field_name: str) -> str:
    if not _SHA256_HEX_RE.fullmatch(value):
        raise ValueError(f"{field_name} must be a 64-character lowercase hexadecimal SHA-256 digest")
    return value


def _require_repo_relative_posix(
    value: str,
    field_name: str,
    *,
    allow_globs: bool,
) -> str:
    _require_nonblank(value, field_name)
    if value != value.strip():
        raise ValueError(f"{field_name} must not have leading or trailing whitespace")
    if "\r" in value or "\n" in value:
        raise ValueError(f"{field_name} must not contain newline characters")
    if "\\" in value:
        raise ValueError(f"{field_name} must use POSIX forward slashes, not backslashes")
    if value.startswith("/"):
        raise ValueError(f"{field_name} must be repo-relative, not absolute")
    if value.startswith("~"):
        raise ValueError(f"{field_name} must be repo-relative, not home-relative")
    if _WINDOWS_DRIVE_RE.match(value):
        raise ValueError(f"{field_name} must be repo-relative, not drive-qualified")
    if value.endswith("/"):
        raise ValueError(f"{field_name} must not end with a trailing slash")
    for segment in value.split("/"):
        if segment == "":
            raise ValueError(f"{field_name} must not contain empty path segments")
        if segment == "..":
            raise ValueError(f"{field_name} must not contain parent directory traversal ('..')")
        if segment == ".":
            raise ValueError(f"{field_name} must not contain current directory segments ('.')")
    if not allow_globs and any(ch in _GLOB_META_CHARS for ch in value):
        raise ValueError(f"{field_name} must be an exact repo-relative POSIX file path without glob wildcards")
    return value


class CanonAdapterRoute(BaseModel):
    """Maps root-relative POSIX file globs to an adapter and check IDs."""

    model_config = ConfigDict(extra="forbid")

    adapter_id: StrictStr
    paths: list[StrictStr] = Field(min_length=1)
    checks: list[StrictStr] = Field(min_length=1)

    @field_validator("adapter_id")
    @classmethod
    def _validate_adapter_id(cls, value: str) -> str:
        return _require_nonblank(value, "adapter_id")

    @field_validator("paths")
    @classmethod
    def _validate_paths(cls, values: list[str]) -> list[str]:
        return [_require_repo_relative_posix(pattern, "paths", allow_globs=True) for pattern in values]

    @field_validator("checks")
    @classmethod
    def _validate_checks(cls, values: list[str]) -> list[str]:
        return [_require_nonblank(check_id, "checks") for check_id in values]


class CanonExemptionRecord(BaseModel):
    """Time-bounded exemption for an exact repo-relative POSIX file."""

    model_config = ConfigDict(extra="forbid")

    check_id: StrictStr
    rule_id: StrictStr
    path: StrictStr
    owner: StrictStr
    rationale: StrictStr
    expires_on: date

    @field_validator("check_id")
    @classmethod
    def _validate_check_id(cls, value: str) -> str:
        return _require_nonblank(value, "check_id")

    @field_validator("rule_id")
    @classmethod
    def _validate_rule_id(cls, value: str) -> str:
        return _require_nonblank(value, "rule_id")

    @field_validator("path")
    @classmethod
    def _validate_path(cls, value: str) -> str:
        return _require_repo_relative_posix(value, "path", allow_globs=False)

    @field_validator("owner")
    @classmethod
    def _validate_owner(cls, value: str) -> str:
        return _require_nonblank(value, "owner")

    @field_validator("rationale")
    @classmethod
    def _validate_rationale(cls, value: str) -> str:
        return _require_nonblank(value, "rationale")

    @field_validator("expires_on", mode="before")
    @classmethod
    def _validate_expires_on(cls, value: object) -> date:
        if isinstance(value, datetime):
            raise ValueError("expires_on must be a calendar date (YYYY-MM-DD), not a datetime")
        if isinstance(value, date):
            return value
        if isinstance(value, str):
            text = value.strip()
            if not _ISO_DATE_RE.fullmatch(text):
                raise ValueError("expires_on must be an ISO date in YYYY-MM-DD format")
            return date.fromisoformat(text)
        raise ValueError("expires_on must be a date or YYYY-MM-DD string")


class CanonLintConfig(BaseModel):
    """Adapter configuration schema for workbay canon-lint."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    schema_version: Literal["workbay-canon-lint/adapter@1"] = Field(
        default=CANON_LINT_ADAPTER_SCHEMA,
        alias="schema",
    )
    canon_version: StrictStr
    manifest_sha256: StrictStr
    canon_root: StrictStr | None = None
    routes: list[CanonAdapterRoute] = Field(default_factory=list)
    exemptions: list[CanonExemptionRecord] = Field(default_factory=list)

    @field_validator("canon_version")
    @classmethod
    def _validate_canon_version(cls, value: str) -> str:
        return _require_nonblank(value, "canon_version")

    @field_validator("manifest_sha256")
    @classmethod
    def _validate_manifest_sha256(cls, value: str) -> str:
        return _require_sha256_hex(value, "manifest_sha256")

    @field_validator("canon_root")
    @classmethod
    def _validate_canon_root(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _require_nonblank(value, "canon_root")


CanonAdapterConfig = CanonLintConfig


class CanonLintFinding(BaseModel):
    """Single lint finding anchored to a canon rule row digest."""

    model_config = ConfigDict(extra="forbid")

    rule_id: StrictStr
    canon_version: StrictStr
    manifest_sha256: StrictStr
    rule_row_sha256: StrictStr
    check_id: StrictStr
    check_version: StrictStr
    adapter_id: StrictStr
    path: StrictStr
    line: StrictInt = Field(gt=0)
    evidence_snippet: StrictStr
    tier: CanonLintTier

    @field_validator("rule_id")
    @classmethod
    def _validate_rule_id(cls, value: str) -> str:
        return _require_nonblank(value, "rule_id")

    @field_validator("canon_version")
    @classmethod
    def _validate_canon_version(cls, value: str) -> str:
        return _require_nonblank(value, "canon_version")

    @field_validator("manifest_sha256")
    @classmethod
    def _validate_manifest_sha256(cls, value: str) -> str:
        return _require_sha256_hex(value, "manifest_sha256")

    @field_validator("rule_row_sha256")
    @classmethod
    def _validate_rule_row_sha256(cls, value: str) -> str:
        return _require_sha256_hex(value, "rule_row_sha256")

    @field_validator("check_id")
    @classmethod
    def _validate_check_id(cls, value: str) -> str:
        return _require_nonblank(value, "check_id")

    @field_validator("check_version")
    @classmethod
    def _validate_check_version(cls, value: str) -> str:
        return _require_nonblank(value, "check_version")

    @field_validator("adapter_id")
    @classmethod
    def _validate_adapter_id(cls, value: str) -> str:
        return _require_nonblank(value, "adapter_id")

    @field_validator("path")
    @classmethod
    def _validate_path(cls, value: str) -> str:
        return _require_repo_relative_posix(value, "path", allow_globs=True)

    @field_validator("evidence_snippet")
    @classmethod
    def _validate_evidence_snippet(cls, value: str) -> str:
        return _require_nonblank(value, "evidence_snippet")


class CanonLintError(BaseModel):
    """Structured runtime, configuration, or integrity error record."""

    model_config = ConfigDict(extra="forbid")

    code: StrictStr
    message: StrictStr
    path: StrictStr | None = None

    @field_validator("code")
    @classmethod
    def _validate_code(cls, value: str) -> str:
        return _require_nonblank(value, "code")

    @field_validator("message")
    @classmethod
    def _validate_message(cls, value: str) -> str:
        return _require_nonblank(value, "message")

    @field_validator("path")
    @classmethod
    def _validate_path(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _require_nonblank(value, "path")


class CanonDecisionAdvisory(BaseModel):
    """A deterministic policy signal in decision text."""

    model_config = ConfigDict(extra="forbid")

    rule_id: StrictStr
    tier: CanonLintTier
    check_id: StrictStr
    message: StrictStr
    matched_signal: StrictStr

    @field_validator("rule_id", "check_id", "message", "matched_signal")
    @classmethod
    def _validate_text(cls, value: str, info: ValidationInfo) -> str:
        return _require_nonblank(value, info.field_name)


class CanonDecisionLintReport(BaseModel):
    """Advisory-only report emitted by workbay canon lint-decision."""

    model_config = ConfigDict(extra="forbid")

    ok: StrictBool
    command: Literal["lint-decision"] = "lint-decision"
    advisories: list[CanonDecisionAdvisory] = Field(default_factory=list)


class CanonLintReport(BaseModel):
    """Structured report emitted by workbay canon-lint commands."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    schema_version: Literal["workbay-canon-lint/report@1"] = Field(
        default=CANON_LINT_REPORT_SCHEMA,
        alias="schema",
    )
    command: CanonLintCommand
    ok: StrictBool
    canon_version: StrictStr | None = None
    manifest_sha256: StrictStr | None = None
    files_checked: StrictInt = Field(default=0, ge=0)
    checks_checked: StrictInt = Field(default=0, ge=0)
    exempted_count: StrictInt = Field(default=0, ge=0)
    findings: list[CanonLintFinding] = Field(default_factory=list)
    errors: list[CanonLintError] = Field(default_factory=list)

    @field_validator("canon_version")
    @classmethod
    def _validate_canon_version(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _require_nonblank(value, "canon_version")

    @field_validator("manifest_sha256")
    @classmethod
    def _validate_manifest_sha256(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _require_sha256_hex(value, "manifest_sha256")


__all__ = [
    "CANON_LINT_ADAPTER_SCHEMA",
    "CANON_LINT_REPORT_SCHEMA",
    "CanonAdapterConfig",
    "CanonAdapterRoute",
    "CanonDecisionAdvisory",
    "CanonDecisionLintReport",
    "CanonExemptionRecord",
    "CanonLintCommand",
    "CanonLintConfig",
    "CanonLintError",
    "CanonLintExitCode",
    "CanonLintFinding",
    "CanonLintReport",
    "CanonLintTier",
    "CanonScanAdapter",
    "CanonScanRecord",
]
