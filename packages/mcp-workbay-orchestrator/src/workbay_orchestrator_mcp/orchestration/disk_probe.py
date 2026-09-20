"""Shared disk probe seam for dispatch headroom checks."""

from __future__ import annotations

import os

DEFAULT_STATVFS = os.statvfs
