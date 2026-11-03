"""Import onnxruntime with telemetry disabled before the native library loads."""

from __future__ import annotations

import logging
import os
import threading
from types import ModuleType

logger = logging.getLogger(__name__)

_TELEMETRY_ENV = "ORT_DISABLE_TELEMETRY"
_disable_failure_lock = threading.Lock()
_disable_failure_logged = False


def import_onnxruntime() -> ModuleType:
    """Import ``onnxruntime`` after opting out of official-build telemetry.

    Official wheels enable the 1DS OneCollector client and read
    ``ORT_DISABLE_TELEMETRY`` when the native library loads ([SECD-05]). When
    the variable is unset, this sets it to ``"1"`` before the import. A value
    the operator already set is left unchanged, including ``"0"`` or ``""``.

    ``disable_telemetry_events()`` is called when the binding provides it.
    Older wheels omit that function; that is not an error. If the call raises,
    the exception class is logged once at WARNING ([OBS-08]) and the module is
    still returned. The environment variable has already opted out.
    """
    global _disable_failure_logged
    if _TELEMETRY_ENV not in os.environ:
        os.environ[_TELEMETRY_ENV] = "1"
    import onnxruntime

    disable = getattr(onnxruntime, "disable_telemetry_events", None)
    if disable is None:
        return onnxruntime
    try:
        disable()
    except Exception as exc:
        with _disable_failure_lock:
            if _disable_failure_logged:
                return onnxruntime
            _disable_failure_logged = True
        logger.warning(
            "onnxruntime.disable_telemetry_events failed (%s); ORT_DISABLE_TELEMETRY already opted out",
            type(exc).__name__,
        )
    return onnxruntime
