#!/usr/bin/python3 -IB
"""Resolve the VM CI site configuration: the facts that tie the VM CI/CD app to one repository.

The single source of truth is config/vm-ci/site.json (schema "vm-ci-site/1"). It
holds only the stored keys below; every other value is derived from them here, so
a copy can never drift from its source. Hostnames, registry and namespace stay in
the environment (OCI_HOST, OCIR_REGISTRY, OCIR_NAMESPACE, the release origin) and
are never part of this file. No secret, secret name or credential belongs in it.

Library:   load(path=None) -> read-only mapping of every stored and derived key.
CLI:       vm_ci_site.py [--config PATH] get KEY | shell | json | check
           get    prints one value
           shell  prints sorted, POSIX-quoted "export VMCI_SITE_<KEY>=<value>" lines
                  for eval in bash
           json   prints the resolved mapping
           check  validates and prints "ok PATH"
File path: --config, else $VMCI_SITE_CONFIG, else config/vm-ci/site.json in the
           repository that holds this script.

Fail closed: a missing or unreadable file, malformed JSON, a duplicate key, an
unknown schema, an unknown or missing key, a non-string value, or a value that
fails its shape prints one line on stderr and exits 1 (load raises SiteError).

Hermetic (RLSE-33): standard library only, no network, no subprocess, and no
environment read other than VMCI_SITE_CONFIG.

Stored keys (all strings):
  slug                 project slug, ^[a-z][a-z0-9]{1,15}$; the stem of unit names,
                       schema ids, labels and environment variable prefixes
  display_name         human-facing name in systemd unit descriptions
  image_repository     backend image repository name in the registry namespace
  runner_home          home of the check-runner account that owns the CI root
  remote_app_root      application root on the deploy VM (environment dirs below it)
  demo_admin_user      demo WordPress admin principal that release smoke refuses
  public_domain        public DNS domain of the demo and API hosts
  plugin_slug          WordPress plugin slug (its directory name when installed)
  plugin_source_dir    repository-relative directory of the plugin app
  plugin_install_root  plugins directory inside the demo WordPress container

Derived keys are listed in DERIVED with their rule; `json` prints them all.
"""

import argparse
import json
import os
import re
import shlex
import sys
import types
from pathlib import Path

SCHEMA = "vm-ci-site/1"
ENV_CONFIG = "VMCI_SITE_CONFIG"
SHELL_PREFIX = "VMCI_SITE_"
DEFAULT_CONFIG = Path(__file__).resolve().parents[2] / "config" / "vm-ci" / "site.json"
MAX_BYTES = 64 * 1024
PROG = "vm-ci-site"

_COMPONENT = r"[A-Za-z0-9._+-]+"
_LABEL = r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?"
SHAPES = {
    "slug": re.compile(r"[a-z][a-z0-9]{1,15}"),
    "display": re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9 '.,&()+/_-]{0,62}[A-Za-z0-9'.)])?"),
    "dns_label": re.compile(_LABEL),
    "dns_name": re.compile(rf"{_LABEL}(?:\.{_LABEL})+"),
    "absolute_path": re.compile(rf"(?:/{_COMPONENT})+"),
    "relative_path": re.compile(rf"{_COMPONENT}(?:/{_COMPONENT})*"),
    "file_name": re.compile(_COMPONENT),
    "unit_name": re.compile(r"[A-Za-z0-9:_.-]{1,248}\.(?:service|timer)"),
    "unit_stem": re.compile(r"[A-Za-z0-9:_-][A-Za-z0-9:_.-]{0,239}"),
    "env_prefix": re.compile(r"[A-Z][A-Z0-9_]*_"),
    "schema_prefix": re.compile(r"[a-z][a-z0-9]*\."),
    "schema_id": re.compile(r"[a-z][a-z0-9]*\.[a-z0-9][a-z0-9-]*/[1-9][0-9]*"),
    "layout_id": re.compile(r"[a-z][a-z0-9-]*/[1-9][0-9]*"),
    "urn": re.compile(r"urn:[a-z0-9][a-z0-9-]*(?::[a-z0-9][a-z0-9-]*)+"),
}

# Stored key -> shape. The order is the documented order of site.json.
KEYS = {
    "slug": "slug",
    "display_name": "display",
    "image_repository": "dns_label",
    "runner_home": "absolute_path",
    "remote_app_root": "absolute_path",
    "demo_admin_user": "dns_label",
    "public_domain": "dns_name",
    "plugin_slug": "dns_label",
    "plugin_source_dir": "relative_path",
    "plugin_install_root": "absolute_path",
}

# Schema ids are "<slug>.<name>/1"; the names are the app's, the namespace is the site's.
SCHEMA_NAMES = (
    "vm-check-event", "vm-check-plan", "vm-check-result", "vm-check-claim",
    "vm-ci-poll-cursor", "vm-ci-poll-runs", "vm-ci-dev-deploy", "vm-ci-dev-deploy-record",
    "vm-ci-publish-outbox", "vm-ci-publish-summary", "vm-ci-publish-request", "vm-ci-publish-export",
    "vm-check-sandbox", "vm-check-fence", "vm-release-hold",
    "vm-check-routing", "vm-check-release-policy",
)


def _derived_rules():
    """(key, shape, rule) in dependency order; each rule reads earlier keys from the mapping."""
    rules = [
        ("ci_name", "dns_label", lambda s: f"{s['slug']}-vm-ci"),
        ("env_prefix", "env_prefix", lambda s: f"{s['slug'].upper()}_"),
        ("vm_env_prefix", "env_prefix", lambda s: f"{s['env_prefix']}VM_"),
        ("ci_env_prefix", "env_prefix", lambda s: f"{s['vm_env_prefix']}CI_"),
        ("ci_root", "absolute_path", lambda s: f"{s['runner_home']}/{s['ci_name']}"),
        ("evidence_dir", "absolute_path", lambda s: f"{s['ci_root']}/evidence"),
        ("state_dir", "absolute_path", lambda s: f"{s['ci_root']}/state"),
        ("outbox_dir", "absolute_path", lambda s: f"{s['ci_root']}/publish-outbox"),
        # Written by the release driver while a release is in flight; the poller
        # starts no dev deploy while it exists (schema_vm_release_hold).
        ("release_hold_path", "absolute_path", lambda s: f"{s['state_dir']}/release-hold.json"),
        ("deploy_env_relpath", "relative_path", lambda s: f".config/{s['ci_name']}/dev-deploy.env"),
        ("check_scope_prefix", "unit_stem", lambda s: f"{s['slug']}-vm-check-"),
        ("poll_unit", "unit_stem", lambda s: f"{s['ci_name']}-poll"),
        ("poll_service", "unit_name", lambda s: f"{s['poll_unit']}.service"),
        ("poll_timer", "unit_name", lambda s: f"{s['poll_unit']}.timer"),
        ("publish_unit", "unit_stem", lambda s: f"{s['ci_name']}-publish"),
        ("publish_service", "unit_name", lambda s: f"{s['publish_unit']}.service"),
        ("publish_timer", "unit_name", lambda s: f"{s['publish_unit']}.timer"),
        ("publisher_name", "dns_label", lambda s: f"{s['ci_name']}-publisher"),
        ("exporter_name", "dns_label", lambda s: f"{s['ci_name']}-publish-export"),
        ("publisher_dropin", "file_name", lambda s: f"50-{s['publisher_name']}-export.conf"),
        ("publisher_legacy_dropin", "file_name", lambda s: f"50-{s['publisher_name']}-acl.conf"),
        ("toolchain_layout", "layout_id", lambda s: f"{s['ci_name']}-toolchain/1"),
        ("delivery_namespace", "urn", lambda s: f"urn:{s['slug']}:vm-ci-poll:push"),
        ("remote_prod_dir", "absolute_path", lambda s: f"{s['remote_app_root']}/prod"),
        ("remote_demo_dir", "absolute_path", lambda s: f"{s['remote_app_root']}/demo"),
        ("remote_staging_prefix", "absolute_path", lambda s: f"/tmp/{s['slug']}-vm-release."),
        ("remote_restore_dirname", "file_name", lambda s: f".{s['slug']}-vm-restore"),
        ("demo_host", "dns_name", lambda s: f"demo.{s['public_domain']}"),
        ("api_host", "dns_name", lambda s: f"api.{s['public_domain']}"),
        ("schema_prefix", "schema_prefix", lambda s: f"{s['slug']}."),
    ]
    for name in SCHEMA_NAMES:
        rules.append((f"schema_{name.replace('-', '_')}", "schema_id",
                      lambda s, name=name: f"{s['schema_prefix']}{name}/1"))
    return tuple(rules)


DERIVED = _derived_rules()
ALL_KEYS = tuple(KEYS) + tuple(name for name, _, _ in DERIVED)
if len(set(ALL_KEYS)) != len(ALL_KEYS) or ENV_CONFIG in (SHELL_PREFIX + key.upper() for key in ALL_KEYS):
    raise AssertionError("site keys collide with each other or with the config variable")


class SiteError(ValueError):
    """The site configuration is missing or invalid; the message is one line."""


def _fail(message):
    raise SiteError(" ".join(str(message).split()))


def check_value(key, value, shape):
    pattern = SHAPES[shape]
    ok = pattern.fullmatch(value) is not None
    if ok and shape in ("absolute_path", "relative_path"):
        ok = not any(part in (".", "..") for part in value.split("/"))
    if ok and shape == "dns_name":
        ok = len(value) <= 253
    if ok and shape == "file_name":
        ok = value not in (".", "..")
    if not ok:
        _fail(f"{key} {value!r} is not a valid {shape.replace('_', ' ')}")


def resolve_path(path=None):
    """The config path: the argument, else $VMCI_SITE_CONFIG, else the repository's file."""
    if path is not None:
        if str(path) == "":
            _fail("the config path is empty")
        return Path(path)
    configured = os.environ.get(ENV_CONFIG)
    if configured is not None:
        if configured == "":
            _fail(f"{ENV_CONFIG} is set but empty")
        return Path(configured)
    return DEFAULT_CONFIG


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            _fail(f"duplicate key {key!r}")
        result[key] = value
    return result


def _read(path):
    try:
        with open(path, "rb") as handle:
            content = handle.read(MAX_BYTES + 1)
    except FileNotFoundError:
        _fail(f"site config not found: {str(path)!r}")
    except IsADirectoryError:
        _fail(f"site config is a directory: {str(path)!r}")
    except OSError as error:
        _fail(f"site config {str(path)!r} cannot be read: {error.strerror}")
    if len(content) > MAX_BYTES:
        _fail(f"site config {str(path)!r} is larger than {MAX_BYTES} bytes")
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError:
        _fail(f"site config {str(path)!r} is not UTF-8")
    try:
        data = json.loads(text, object_pairs_hook=_unique_object)
    except (json.JSONDecodeError, RecursionError) as error:
        _fail(f"site config {str(path)!r} is not valid JSON: {error}")
    if not isinstance(data, dict):
        _fail(f"site config {str(path)!r} must be a JSON object")
    return data


def resolve(data):
    """Validate a parsed site document and return every stored and derived value."""
    if not isinstance(data, dict):
        _fail("site config must be a JSON object")
    if "schema" not in data:
        _fail(f"missing schema; expected {SCHEMA!r}")
    if data["schema"] != SCHEMA:
        _fail(f"unknown schema {data['schema']!r}; expected {SCHEMA!r}")
    unknown = sorted(set(data) - set(KEYS) - {"schema"})
    if unknown:
        _fail(f"unknown key {unknown[0]!r}")
    missing = [key for key in KEYS if key not in data]
    if missing:
        _fail(f"missing key {missing[0]!r}")
    site = {}
    for key, shape in KEYS.items():
        value = data[key]
        if type(value) is not str:
            _fail(f"{key} must be a string, not {type(value).__name__}")
        check_value(key, value, shape)
        site[key] = value
    for key, shape, rule in DERIVED:
        site[key] = rule(site)
        check_value(key, site[key], shape)
    return site


def load(path=None):
    """Load, validate and derive the site configuration as a read-only mapping (keys sorted)."""
    site = resolve(_read(resolve_path(path)))
    return types.MappingProxyType(dict(sorted(site.items())))


def shell_lines(site):
    """export lines sorted by variable name; shlex.quote makes every value safe to eval."""
    return sorted(f"export {SHELL_PREFIX}{key.upper()}={shlex.quote(value)}" for key, value in site.items())


def _parser():
    parser = argparse.ArgumentParser(prog="vm_ci_site.py", description=__doc__.splitlines()[0])
    parser.add_argument("--config", help=f"site file (else ${ENV_CONFIG}, else {DEFAULT_CONFIG})")
    commands = parser.add_subparsers(dest="command", required=True)
    common = {"help": argparse.SUPPRESS, "default": argparse.SUPPRESS}
    get = commands.add_parser("get", help="print one value")
    get.add_argument("key")
    get.add_argument("--config", **common)
    for name, text in (("shell", "print sorted export lines for eval in bash"),
                       ("json", "print the resolved mapping"), ("check", "validate only")):
        commands.add_parser(name, help=text).add_argument("--config", **common)
    return parser


def main(argv=None, out=None, err=None):
    out, err = out or sys.stdout, err or sys.stderr
    args = _parser().parse_args(argv)
    try:
        path = resolve_path(args.config)
        site = load(path)
        if args.command == "get":
            if args.key not in site:
                _fail(f"unknown key {args.key!r}")
            print(site[args.key], file=out)
        elif args.command == "shell":
            print("\n".join(shell_lines(site)), file=out)
        elif args.command == "json":
            print(json.dumps(dict(site), indent=2, sort_keys=True), file=out)
        else:
            print(f"ok {path}", file=out)
    except SiteError as error:
        print(f"{PROG}: {error}", file=err)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
