"""Bind executable checks to the published Canon rows they implement."""

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class CheckSpec:
    check_id: str
    check_version: str
    adapter_id: str
    rule_id: str
    canon_version: str
    row_sha256: str


_VERIFIED_RELEASE = "0.27.1"
CHECKS = (
    CheckSpec(
        "python.requests-timeout",
        "2",
        "python-ast",
        "RES-02",
        _VERIFIED_RELEASE,
        "25dda067e50d99d2b3bb314923015accb410473713da7220b7504bfe6488c9cc",
    ),
    CheckSpec(
        "python.silent-except",
        "1",
        "python-ast",
        "AGT-10",
        _VERIFIED_RELEASE,
        "2871530f9a8965e11d8584bce066f7e5f9c695f928c56d553377f50381ff881e",
    ),
    CheckSpec(
        "markdown.finished-scaffold",
        "1",
        "markdown-plan",
        "WRIT-42",
        _VERIFIED_RELEASE,
        "3966c43b60a97b1da4e431c64d5e569e6c5dea53b7db988a83f5876a9669beab",
    ),
)
