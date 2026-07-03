"""Bootstrap install manifest schema (Schema #5 from founding implementation note).

The wire shape ``workbay-bootstrap`` writes to
``<target>/.workbay-overlay.json``. Captures the contract between
bootstrap and target repos so target-side guardrails (lint hoisted
paths, harness sync check, drift detectors) can validate against a
single typed shape rather than per-key heuristics.
"""

from __future__ import annotations

from pathlib import PurePosixPath
from typing import Annotated, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    WithJsonSchema,
    field_validator,
    model_validator,
)


Sha40 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{40}$")]
Sha256Digest = Annotated[str, StringConstraints(pattern=r"^sha256:[0-9a-f]{64}$")]
PluginComponentName = Annotated[
    str,
    StringConstraints(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]*$"),
]
PluginOverrideRelativePath = Annotated[
    str,
    StringConstraints(min_length=1),
    WithJsonSchema(
        {
            "type": "string",
            "minLength": 1,
            "not": {
                "anyOf": [
                    {"pattern": "^/"},
                    {"pattern": "(^|/)\\.\\.(/|$)"},
                    {"pattern": "^\\.?$"},
                    {"pattern": "\\\\"},
                ]
            },
        }
    ),
]


def _normalize_plugin_override_path(value: str) -> str:
    if "\\" in value:
        raise ValueError("override paths must use POSIX '/' separators")
    path = PurePosixPath(value)
    if path.is_absolute() or value == "." or ".." in path.parts:
        raise ValueError(
            "override paths must be relative and stay under the override root"
        )
    return path.as_posix()


class OverlaySurface(BaseModel):
    """A single overlay surface entry (e.g. skills, commands, hooks)."""

    model_config = ConfigDict(extra="allow")

    path: str = Field(
        description="Repo-relative surface path (e.g. '.claude/skills/handoff-lifecycle')."
    )
    source: Literal["shared", "local", "overlapping", "generated", "lifecycle"] = Field(
        description=(
            "Origin tier: 'shared' (symlinked from workbay-system), "
            "'local' (target-owned), 'overlapping' (target overrides shared), "
            "'generated' (per-agent surface produced by generate_agent_workflows.py "
            "during install — implementation note step 1), "
            "'lifecycle' (implementation note hoisted Make fragment + runner package "
            "— copied, not symlinked, so the consumer can run `make context` "
            "without the workbay-system packaging tree)."
        ),
    )


class OverlayConfigEntry(BaseModel):
    """A consumer-tool config that bootstrap wrote (e.g. .vscode/mcp.json).

    ``action`` is the string the install flow emits today
    (``created``, ``updated``, ``set``, ``unchanged``). Modeled as a
    free string rather than a Literal so the contract does not block
    bootstrap from introducing new actions before the protocol bumps.
    """

    model_config = ConfigDict(extra="allow")

    path: str
    action: str = Field(
        min_length=1,
        description="What bootstrap did to the config (e.g. 'created', 'updated', 'set', 'unchanged').",
    )


class PluginSkillOverride(BaseModel):
    """Override contract for one plugin skill body."""

    model_config = ConfigDict(extra="forbid")

    mode: Literal["replace", "patch", "disable", "add"]
    path: PluginOverrideRelativePath | None = None
    base_path: PluginOverrideRelativePath | None = None
    upstream_digest: Sha256Digest | None = None
    on_upstream_change: Literal["warn", "error", "ignore"] | None = None

    @field_validator("path", "base_path")
    @classmethod
    def validate_override_paths(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _normalize_plugin_override_path(value)

    @model_validator(mode="after")
    def validate_mode_fields(self) -> PluginSkillOverride:
        if self.mode in {"replace", "patch", "add"} and not self.path:
            raise ValueError("path is required when mode is replace, patch, or add")
        if self.mode in {"replace", "patch"} and self.upstream_digest is None:
            raise ValueError(
                "upstream_digest is required when mode is replace or patch"
            )
        if self.mode == "patch" and not self.base_path:
            raise ValueError("base_path is required when mode is patch")
        if self.mode != "patch" and self.base_path is not None:
            raise ValueError("base_path is only valid when mode is patch")
        return self


class PluginPortableCommandOverride(BaseModel):
    """Override contract for one portable command manifest entry."""

    model_config = ConfigDict(extra="forbid")

    mode: Literal["add"]
    path: PluginOverrideRelativePath

    @field_validator("path")
    @classmethod
    def validate_override_path(cls, value: str) -> str:
        return _normalize_plugin_override_path(value)




class PluginDocOverride(BaseModel):
    """Override contract for one rules or guides doc under the override root."""

    model_config = ConfigDict(extra="forbid")

    mode: Literal["add", "replace"]
    path: PluginOverrideRelativePath

    @field_validator("path")
    @classmethod
    def validate_override_path(cls, value: str) -> str:
        return _normalize_plugin_override_path(value)


class PluginClaudeSettingsHooksOverride(BaseModel):
    """Override contract for compositional Claude settings hooks."""

    model_config = ConfigDict(extra="forbid")

    mode: Literal["add", "override"]
    path: PluginOverrideRelativePath

    @field_validator("path")
    @classmethod
    def validate_override_path(cls, value: str) -> str:
        return _normalize_plugin_override_path(value)


class PluginMcpServerOverride(BaseModel):
    """Override contract for one MCP server entry in the plugin manifest."""

    model_config = ConfigDict(extra="forbid")

    mode: Literal["patch", "disable", "add"]
    patch_path: PluginOverrideRelativePath | None = None
    requires_trust_ack: bool = False

    @field_validator("patch_path")
    @classmethod
    def validate_patch_path(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _normalize_plugin_override_path(value)

    @model_validator(mode="after")
    def validate_mode_fields(self) -> PluginMcpServerOverride:
        if self.mode in {"patch", "add"} and not self.patch_path:
            raise ValueError("patch_path is required when mode is patch or add")
        return self


class PluginOverrideComponents(BaseModel):
    """Declared override components grouped by plugin surface kind."""

    model_config = ConfigDict(extra="forbid")

    skills: dict[PluginComponentName, PluginSkillOverride] = Field(default_factory=dict)
    mcp_servers: dict[PluginComponentName, PluginMcpServerOverride] = Field(
        default_factory=dict
    )
    portable_commands: dict[PluginComponentName, PluginPortableCommandOverride] = Field(
        default_factory=dict
    )
    claude_settings_hooks: dict[
        PluginComponentName, PluginClaudeSettingsHooksOverride
    ] = Field(default_factory=dict)
    rules: dict[PluginComponentName, PluginDocOverride] = Field(default_factory=dict)
    guides: dict[PluginComponentName, PluginDocOverride] = Field(default_factory=dict)


class PluginOverrideManifest(BaseModel):
    """Tracked, repo-owned plugin override manifest."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1] = 1
    plugin: PluginComponentName
    components: PluginOverrideComponents = Field(
        default_factory=PluginOverrideComponents
    )


class ReplaceCommandOp(BaseModel):
    model_config = ConfigDict(extra="forbid")

    op: Literal["replace_command"]
    value: str = Field(min_length=1)


class ReplaceArgsOp(BaseModel):
    model_config = ConfigDict(extra="forbid")

    op: Literal["replace_args"]
    value: list[str] = Field(min_length=1)


class AppendArgsOp(BaseModel):
    model_config = ConfigDict(extra="forbid")

    op: Literal["append_args"]
    value: list[str] = Field(min_length=1)


class UpsertEnvOp(BaseModel):
    model_config = ConfigDict(extra="forbid")

    op: Literal["upsert_env"]
    name: PluginComponentName
    value: str


class RemoveEnvOp(BaseModel):
    model_config = ConfigDict(extra="forbid")

    op: Literal["remove_env"]
    name: PluginComponentName


class DisableServerOp(BaseModel):
    model_config = ConfigDict(extra="forbid")

    op: Literal["disable_server"]


PluginMcpPatchOperation = Annotated[
    ReplaceCommandOp
    | ReplaceArgsOp
    | AppendArgsOp
    | UpsertEnvOp
    | RemoveEnvOp
    | DisableServerOp,
    Field(discriminator="op"),
]


class PluginMcpServerPatch(BaseModel):
    """Typed patch grammar for MCP server mutations in consumer overrides."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1] = 1
    target_server: PluginComponentName
    ops: list[PluginMcpPatchOperation] = Field(min_length=1)


class PluginAcceptUpstreamProvenance(BaseModel):
    """Recorded ``overrides accept-upstream`` event for one component."""

    model_config = ConfigDict(extra="forbid")

    previous_upstream_digest: Sha256Digest
    new_upstream_digest: Sha256Digest
    accepted_at: str = Field(min_length=1)


class PluginOverrideLockEntry(BaseModel):
    """Tracked provenance for one consumer-owned plugin override."""

    model_config = ConfigDict(extra="forbid")

    component_kind: Literal[
        "skill",
        "command",
        "portable_command",
        "mcp_server",
        "claude_settings_hooks",
        "rules",
        "guides",
    ]
    name: PluginComponentName
    mode: Literal["replace", "disable", "add", "patch", "override"]
    local_path: str | None = None
    base_path: str | None = None
    patch_path: str | None = None
    upstream_digest: Sha256Digest | None = None
    last_accept_upstream: PluginAcceptUpstreamProvenance | None = None


class PluginOverrideLock(BaseModel):
    """Source-controlled lockfile for plugin overrides."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1] = 1
    plugin: PluginComponentName
    base_remote_sha: Sha40
    components: list[PluginOverrideLockEntry] = Field(default_factory=list)


class PluginEffectiveLockEntry(BaseModel):
    """Generated receipt entry for one component in the effective plugin tree."""

    model_config = ConfigDict(extra="forbid")

    component_kind: Literal[
        "skill",
        "command",
        "portable_command",
        "mcp_server",
        "claude_settings_hooks",
        "rules",
        "guides",
    ]
    name: PluginComponentName
    mode: Literal["replace", "disable", "add", "patch", "override", "passthrough"]
    effective_digest: Sha256Digest
    status: Literal["stale", "merge_conflict"] | None = None
    override_path: str | None = None
    recorded_upstream_digest: Sha256Digest | None = None
    current_base_digest: Sha256Digest | None = None


class PluginEffectiveLock(BaseModel):
    """Generated receipt describing the composed effective plugin tree."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1] = 1
    plugin: PluginComponentName
    base_remote_sha: Sha40
    effective_root: str = Field(min_length=1)
    components: list[PluginEffectiveLockEntry] = Field(default_factory=list)


class BootstrapManifest(BaseModel):
    """Top-level shape of ``.workbay-overlay.json``.

    Validated on write by ``workbay-bootstrap.install`` so the file
    cannot drift silently. Consumers (drift detectors, doctor commands)
    parse the same model on read.
    """

    model_config = ConfigDict(extra="allow")

    schema_version: int = Field(ge=1, description="Manifest schema version.")
    source_kind: Literal["git_overlay", "package", "worktree"] = Field(
        default="git_overlay",
        description=(
            "Overlay delivery source: 'git_overlay' (a clone checked out at "
            "remote_ref), 'package' (an installed workbay-system "
            "distribution), or 'worktree' (the repo's own working tree / "
            "HEAD). Defaults to 'git_overlay' so manifests written "
            "before internal validate unchanged."
        ),
    )
    remote_url: str | None = Field(default=None, min_length=1)
    remote_ref: str | None = Field(default=None, min_length=1)
    remote_sha: Sha40 | None = Field(
        default=None,
        description="Resolved 40-char git SHA at install time (git_overlay source).",
    )
    package_version: str | None = Field(
        default=None,
        description="Installed workbay-system distribution version (package source).",
    )
    stack_distribution: str | None = Field(
        default=None,
        description=(
            "Meta-package anchor distribution recorded by package-source "
            "install/update when it is installed (implementation note: "
            "'workbay'). None for legacy/pre-stack installs."
        ),
    )
    stack_version: str | None = Field(
        default=None,
        description=(
            "Installed version of the stack anchor distribution at "
            "install/update time (package source only)."
        ),
    )
    stack_members: dict[str, str] | None = Field(
        default=None,
        description=(
            "Mapping of stack member distribution name -> exact version "
            "pinned by the installed anchor's metadata. Doctor compares the "
            "live environment against this expectation (stack_drift)."
        ),
    )
    surfaces: list[OverlaySurface] = Field(default_factory=list)
    configs: list[OverlayConfigEntry] = Field(default_factory=list)
    mcp_servers: list[str] = Field(
        default_factory=list,
        description=(
            "Sorted list of managed MCP server names bootstrap last wrote "
            "into ``.mcp.json`` / ``.vscode/mcp.json`` / ``.codex/config.toml``. "
            "Read by ``sync_mcp_configs(prune_removed_managed=True)`` as the "
            "authoritative previously-managed provenance so third-party "
            "launchers in those files are preserved across syncs."
        ),
    )
    plugin_overrides_path: str | None = Field(
        default=None,
        description=(
            "Optional explicit plugin override root recorded by bootstrap so "
            "later doctor/update/repair runs can reuse a non-default internal "
            "override location."
        ),
    )

    @model_validator(mode="after")
    def _check_source_provenance(self) -> "BootstrapManifest":
        """Each delivery source requires its own provenance fields.

        ``git_overlay`` (the default, so pre-internal manifests keep
        validating) requires the git ``remote_*`` triple; ``package`` requires
        the installed distribution ``package_version``.
        """
        if self.source_kind == "git_overlay":
            missing = [
                name
                for name in ("remote_url", "remote_ref", "remote_sha")
                if not getattr(self, name)
            ]
            if missing:
                raise ValueError("git_overlay manifest requires " + ", ".join(missing))
        elif self.source_kind == "package" and not self.package_version:
            raise ValueError("package manifest requires package_version")
        elif self.source_kind == "worktree" and not self.remote_sha:
            raise ValueError("worktree manifest requires remote_sha")
        return self
