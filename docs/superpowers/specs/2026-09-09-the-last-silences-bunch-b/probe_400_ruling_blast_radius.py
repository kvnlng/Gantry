"""Blast-radius probe for the FINAL ruling: warn on FORCE_PROCESSES, refuse on
MAX_TASKS_PER_CHILD, both only on a :memory: store.

Wraps DicomSession.redact; no production file is touched.
"""
import os

from isocenter.logger import get_logger
from isocenter.session import DicomSession

_real_redact = DicomSession.redact
WARNED = []
REFUSED = []


def _recycling_lever():
    raw = os.environ.get("ISOCENTER_MAX_TASKS_PER_CHILD")
    if not raw:
        return None
    try:
        return "ISOCENTER_MAX_TASKS_PER_CHILD" if int(raw) >= 1 else None
    except ValueError:
        return None


def _processes_requested_by():
    """Mirrors the designed attribution: who asked for processes."""
    lever = _recycling_lever()
    if lever:
        return lever, False
    if os.environ.get("ISOCENTER_FORCE_THREADS", "").lower() == "1":
        return None, True
    if os.environ.get("ISOCENTER_FORCE_PROCESSES", "").lower() == "1":
        return "ISOCENTER_FORCE_PROCESSES", True
    return None, True


def _redact(self, show_progress=True, force=False):
    """See the module docstring."""
    lever, use_threads = _processes_requested_by()
    where = os.environ.get("PYTEST_CURRENT_TEST", "?")
    if lever and getattr(self.store_backend, "db_path", None) == ":memory:":
        if use_threads:
            WARNED.append((where, lever))
            get_logger().warning(
                "PROBE WARNING: %s had no effect on this run.", lever)
        else:
            REFUSED.append((where, lever))
            raise RuntimeError("PROBE REFUSAL via " + lever)
    return _real_redact(self, show_progress=show_progress, force=force)


DicomSession.redact = _redact


def pytest_sessionfinish(session, exitstatus):
    print("\n===== PROBE: warned " + str(len(WARNED))
          + " times, refused " + str(len(REFUSED)) + " times =====")
    for where, lever in WARNED:
        print("  WARN   " + lever + "  <-  " + where)
    for where, lever in REFUSED:
        print("  REFUSE " + lever + "  <-  " + where)
