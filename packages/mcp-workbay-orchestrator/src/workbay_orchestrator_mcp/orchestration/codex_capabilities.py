"""Remote Codex capability admission, shared by preflight and argv construction.

Routing preferences are not provider capabilities. Discover the latter using the
same VM/user/binary as execution. The authenticated endpoint owns capabilities; WorkBay does not maintain a
persistent model list or scrape the TUI.
"""

from __future__ import annotations

import hashlib
import json
import re
import subprocess
import threading
import time
from dataclasses import dataclass
from typing import Any

CATALOGUE_MAX_AGE_SECONDS = 300
SNAPSHOT_TTL_SECONDS = 30
PROBE_TIMEOUT_SECONDS = 35

# Runs under the execution identity on the gate. Never emits credentials or
# account fields. model/list may fall back to bundled models on network failure;
# intersect it with a successful authenticated HTTP response from the provider.
_REMOTE_READER = r"""
import hashlib, json, os, pathlib, subprocess, time
p = subprocess.Popen(["codex", "app-server", "-c", 'model_provider="openai"'], stdin=subprocess.PIPE,
                     stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
def rpc(i, method, params):
    p.stdin.write(json.dumps({"id": i, "method": method, "params": params}) + "\n")
    p.stdin.flush()
    while True:
        line = p.stdout.readline()
        if not line:
            raise RuntimeError("app-server closed before response")
        item = json.loads(line)
        if item.get("id") == i:
            if "error" in item:
                raise RuntimeError("app-server request failed: " + method)
            return item["result"]
try:
    init = rpc(1, "initialize", {"clientInfo": {"name": "workbay_capabilities", "version": "1"},
                                "capabilities": {"experimentalApi": True}})
    p.stdin.write(json.dumps({"method": "initialized", "params": {}}) + "\n")
    p.stdin.flush()
    account = rpc(2, "account/read", {})
    if not account.get("account"):
        raise RuntimeError("no authenticated Codex account")
    models, cursor, seen = [], None, set()
    for page in range(100):
        result = rpc(3 + page, "model/list", {"limit": 100, "includeHidden": True, "cursor": cursor})
        models.extend(result["data"])
        cursor = result.get("nextCursor")
        if cursor is None:
            break
        if cursor in seen:
            raise RuntimeError("repeated catalogue cursor")
        seen.add(cursor)
    else:
        raise RuntimeError("catalogue pagination limit exceeded")
    home = pathlib.Path(init.get("codexHome") or os.environ.get("CODEX_HOME") or pathlib.Path.home() / ".codex")
    # Query the authenticated endpoint, not the CLI's disk cache: a fresh cache
    # can belong to a previous account, and model/list can serve bundled rows.
    # Credentials stay on the VM and are never included in stdout or errors.
    import urllib.request, urllib.error
    tokens = json.loads((home / "auth.json").read_text()).get("tokens", {})
    access_token, account_id = tokens.get("access_token"), tokens.get("account_id")
    if not access_token or not account_id:
        raise RuntimeError("authenticated ChatGPT credentials unavailable on gate")
    version = subprocess.check_output(["codex", "--version"], text=True).strip().split()[-1]
    endpoint = "https://chatgpt.com/backend-api/codex/models?client_version=" + version
    request = urllib.request.Request(endpoint, headers={
        "Authorization": "Bearer " + access_token,
        "ChatGPT-Account-Id": account_id,
        "User-Agent": "codex_cli_rs/" + version,
    })
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            payload = response.read(4 * 1024 * 1024 + 1)
        if len(payload) > 4 * 1024 * 1024:
            raise RuntimeError("capability response too large")
        catalogue = json.loads(payload)
    except urllib.error.HTTPError as exc:
        raise RuntimeError("capability endpoint HTTP " + str(exc.code)) from None
    raw = {m["slug"]: m for m in catalogue["models"]}
    verified = []
    for model in models:
        evidence = raw.get(model.get("model"))
        if evidence is None:
            continue
        model["supportedReasoningEfforts"] = [
            {"reasoningEffort": x["effort"]} for x in evidence.get("supported_reasoning_levels", [])]
        model["defaultReasoningEffort"] = evidence.get("default_reasoning_level")
        model["serviceTiers"] = evidence.get("service_tiers")
        # additionalSpeedTiers is the CLI spelling, distinct from API tier IDs.
        verified.append(model)
    identity = hashlib.sha256(json.dumps([account_id, str(home), endpoint], sort_keys=True).encode()).hexdigest()
    print(json.dumps({"cli_version": version, "codex_home": str(home), "identity": identity,
                      "fetched_at": time.time(), "models": verified}))
finally:
    p.terminate()
    try:
        p.wait(timeout=2)
    except subprocess.TimeoutExpired:
        p.kill()
        p.wait()
"""


class CodexCapabilityError(ValueError):
    """A typed capability refusal; never substitute a model, effort, or tier."""


@dataclass(frozen=True)
class CodexInvocation:
    model: str
    effort: str | None
    service_tier: str | None
    catalogue_digest: str
    identity: str
    cli_version: str


_lock = threading.Lock()
_snapshots: dict[str, tuple[float, dict[str, Any]]] = {}


def clear_capability_cache() -> None:
    with _lock:
        _snapshots.clear()


def _read_remote_catalogue(host: str) -> dict[str, Any]:
    from .offload_model_discovery import build_remote_list_models_argv

    argv = build_remote_list_models_argv(["timeout", "-k", "2", "30", "python3", "-c", _REMOTE_READER], host=host)
    try:
        result = subprocess.run(
            argv,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=PROBE_TIMEOUT_SECONDS,
            check=False,
        )
        if result.returncode:
            raise ValueError("remote Codex capability query failed")
        value = json.loads(result.stdout)
        if not isinstance(value, dict) or not isinstance(value.get("models"), list):
            raise ValueError("invalid catalogue shape")
        if not all(isinstance(value.get(k), str) and value[k] for k in ("identity", "cli_version", "codex_home")):
            raise ValueError("missing catalogue identity")
        # This response was fetched live inside the bounded remote command.
        # Measure its age on the receiving clock; VM and host wall clocks need
        # not agree (even subsecond skew falsely rejected a successful probe).
        value["remote_fetched_at"] = value.get("fetched_at")
        value["fetched_at"] = time.time()
        return value
    except (OSError, subprocess.TimeoutExpired, ValueError, KeyError, TypeError) as exc:
        raise CodexCapabilityError(f"catalogue_unavailable: {exc}") from exc


def remote_catalogue(*, refresh: bool = False) -> dict[str, Any]:
    """Bounded single-flight observation; no cached failure or stale fallback."""
    from .offload_model_discovery import resolve_probe_gate_host

    host = resolve_probe_gate_host("codex-remote")
    if not host:
        raise CodexCapabilityError("catalogue_unavailable: remote gate host is not configured")
    with _lock:
        cached = _snapshots.get(host)
        if not refresh and cached and time.monotonic() - cached[0] < SNAPSHOT_TTL_SECONDS:
            if 0 <= time.time() - float(cached[1]["fetched_at"]) <= CATALOGUE_MAX_AGE_SECONDS:
                return cached[1]
        value = _read_remote_catalogue(host)
        _snapshots[host] = (time.monotonic(), value)
        return value


def _observed_model(model: str, snapshot: dict[str, Any] | None = None):
    from .codex_lane_config import CODEX_MODEL_TIERS

    prefix = f"Refusing codex-remote dispatch with model {model!r}: "
    if not isinstance(model, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,199}", model):
        raise CodexCapabilityError(prefix + "invalid_model")
    policy = CODEX_MODEL_TIERS.get(model)
    if policy is not None and not policy.entitled:
        raise CodexCapabilityError(prefix + "tier_not_entitled: explicit model policy denies dispatch")
    try:
        observed = snapshot if snapshot is not None else remote_catalogue()
    except CodexCapabilityError as exc:
        raise CodexCapabilityError(prefix + str(exc)) from exc
    rows = [row for row in observed["models"] if isinstance(row, dict) and row.get("model") == model]
    if len(rows) != 1:
        raise CodexCapabilityError(prefix + "unknown_model: absent or ambiguous in authenticated remote catalogue")
    return rows[0], policy, observed


def resolve_codex_model(model: str) -> str:
    """Model-only callers must not validate an effort the request did not use."""
    _observed_model(model)
    return model


def resolve_codex_invocation(
    model: str,
    *,
    effort: str | None = None,
    service_tier: str | None = None,
    snapshot: dict[str, Any] | None = None,
) -> CodexInvocation:
    """Validate requested capabilities; explicit local policy can only narrow."""
    row, policy, observed = _observed_model(model, snapshot)
    prefix = f"Refusing codex-remote dispatch with model {model!r}: "
    efforts = {x.get("reasoningEffort") for x in (row.get("supportedReasoningEfforts") or []) if isinstance(x, dict)}
    if policy is not None:
        efforts &= policy.allowed_efforts
    selected_effort = effort
    if selected_effort is None:
        selected_effort = policy.default_effort if policy else row.get("defaultReasoningEffort")
    if selected_effort not in efforts:
        raise CodexCapabilityError(
            prefix + f"unsupported_effort {selected_effort!r}; advertises {sorted(e for e in efforts if e)}"
        )
    # Only the CLI's advertised spelling proves the fast override; API display
    # labels and a successful process exit alone do not prove tier application.
    if service_tier not in (None, "default"):
        speeds = set(row.get("additionalSpeedTiers") or [])
        if policy is not None:
            speeds &= policy.allowed_service_tiers
        provider_tiers = {item.get("id") for item in (row.get("serviceTiers") or []) if isinstance(item, dict)}
        if service_tier not in speeds or (service_tier == "fast" and "priority" not in provider_tiers):
            raise CodexCapabilityError(
                prefix + f"unsupported_service_tier {service_tier!r}; advertises {sorted(speeds)}"
            )
    digest = hashlib.sha256(json.dumps(observed, sort_keys=True).encode()).hexdigest()
    return CodexInvocation(model, selected_effort, service_tier, digest, observed["identity"], observed["cli_version"])


def discover_codex_models():
    """Project one observed catalogue into the existing role-admission contract."""
    import os

    from .codex_lane_config import TRACKED_CODEX_MODEL, WORKBAY_CODEX_MODEL_ENV
    from .offload_model_discovery import ModelDiscovery, resolve_probe_gate_host
    from .resolved_role_config import native_transport_for_backend

    snapshot = remote_catalogue()
    rows = snapshot["models"]
    names = tuple(row["model"] for row in rows if isinstance(row, dict) and isinstance(row.get("model"), str))
    transport = native_transport_for_backend("codex-remote")
    env_default = (os.environ.get(WORKBAY_CODEX_MODEL_ENV) or "").strip()
    return ModelDiscovery(
        backend_id="codex-remote",
        resolved_model=env_default or TRACKED_CODEX_MODEL,
        tracked_pin=TRACKED_CODEX_MODEL,
        catalogue=names,
        source="env" if env_default else "tracked",
        gate_host=resolve_probe_gate_host("codex-remote"),
        catalogue_source="probed",
        catalogue_version=f"codex:{snapshot['cli_version']}:{snapshot['identity']}",
        catalogue_captured_at=time.monotonic(),
        catalogue_capabilities=tuple((name, transport) for name in names),
        catalogue_advertised_efforts=tuple(
            (
                (row["model"], transport),
                tuple(
                    item["reasoningEffort"]
                    for item in (row.get("supportedReasoningEfforts") or [])
                    if isinstance(item, dict) and isinstance(item.get("reasoningEffort"), str)
                ),
            )
            for row in rows
            if isinstance(row, dict) and isinstance(row.get("model"), str)
        ),
    )
