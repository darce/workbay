#!/usr/bin/env python3
"""Build-time vendor of scripts/_scrub_core.py for hatch builds from sdist."""

from __future__ import annotations

import re

INTERNAL_REF_PREFIXES = (
    "AHMCP",
    "AOMCP",
    "APD",
    "DCMCP",
    "WBF",
    "CHC",
    "PA",
    "MAINT",
    "APM-SPEC-TR",
    "WORKSTATE-REF",
)

INTERNAL_REF_RE = re.compile(
    r"(?<![A-Za-z])(?:"
    + "|".join(re.escape(prefix) for prefix in INTERNAL_REF_PREFIXES)
    + r"|WS|WB|E[0-9]+)(?:[0-9]+)?(?:-[A-Z0-9]+)+"
)

PROCESS_REF_RES = (
    re.compile(r"\b[Pp]lan\s+[0-9]{4}\b"),
    re.compile(r"\b[Ss]lice\s+[0-9]+[A-Za-z]?\b"),
    re.compile(r"\b[Ss]tep\s+[0-9]+/[0-9]+\b"),
)

INLINE_INTERNAL_PREFIX_RE = re.compile(
    r"(?<![A-Za-z])(?:AHMCP|AOMCP|APD|DCMCP|WBF|CHC|APM-SPEC-TR)(?![A-Za-z])",
    re.IGNORECASE,
)

INLINE_EPIC_REF_RE = re.compile(r"E([0-9]+)-([0-9]+(?:-[A-Z0-9]+)*)")

_COLLAPSE_RE = re.compile(
    r"\b(?:internal|implementation note)(?:[ \t]+(?:internal|implementation note))+\b"
)


def scrub_text(text: str) -> str:
    scrubbed = INTERNAL_REF_RE.sub("internal", text)
    scrubbed = INLINE_INTERNAL_PREFIX_RE.sub("internal", scrubbed)
    scrubbed = INLINE_EPIC_REF_RE.sub("internal", scrubbed)
    for regex in PROCESS_REF_RES:
        scrubbed = regex.sub("implementation note", scrubbed)
    return _COLLAPSE_RE.sub("internal", scrubbed)