"""Blast-radius probe: simulate the proposed #400 refusal, suite-wide.

Wraps `DicomSession.redact` with the proposed precondition and raises the
same RuntimeError it would raise. No production file is touched; the
plugin is loaded with `-p refusal_plugin` from the scratchpad.
"""
import os

from isocenter.session import DicomSession

_real_redact = DicomSession.redact
FIRED = []


def _forcing_lever():
    if os.environ.get("ISOCENTER_FORCE_PROCESSES", "").lower() == "1":
        return "ISOCENTER_FORCE_PROCESSES"
    raw = os.environ.get("ISOCENTER_MAX_TASKS_PER_CHILD")
    if raw:
        try:
            if int(raw) >= 1:
                return "ISOCENTER_MAX_TASKS_PER_CHILD"
        except ValueError:
            pass
    return None


def _redact(self, show_progress=True, force=False):
    lever = _forcing_lever()
    if lever and getattr(self.store_backend, "db_path", None) == ":memory:":
        FIRED.append((os.environ.get("PYTEST_CURRENT_TEST", "?"), lever))
        raise RuntimeError("PROBE REFUSAL via " + lever)
    return _real_redact(self, show_progress=show_progress, force=force)


DicomSession.redact = _redact


def pytest_sessionfinish(session, exitstatus):
    print("\n===== PROBE: refusal fired " + str(len(FIRED)) + " times =====")
    for where, lever in FIRED:
        print("  " + lever + "  <-  " + where)
