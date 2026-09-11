"""A spawned OCR worker runs the tesseract its caller configured (#458).

`_require_ocr` probes OCR in the calling process, and that probe honours
whatever `pytesseract.pytesseract.tesseract_cmd` the caller set. A spawned
worker imports a fresh pytesseract, whose `tesseract_cmd` is the bare
`"tesseract"`, and looks for it on `PATH`. Measured on e184933 with real
tesseract 5.5.3, `PATH=/usr/bin:/bin`, and the parent's `tesseract_cmd`
set to `/opt/homebrew/bin/tesseract`: the probe passed, and every worker
failed with `TesseractNotFoundError`, on both 3.12.14 and 3.14.7t --

- `scan_pixel_content()` under `ISOCENTER_FORCE_PROCESSES` and under
  `ISOCENTER_MAX_TASKS_PER_CHILD`;
- `discover_redaction_zones()` under `ISOCENTER_MAX_TASKS_PER_CHILD`.
  Discovery passes `force_threads=True`, and #458 said it therefore did
  not need this; but recycling outranks `force_threads=True` in
  `parallel._resolve_execution_choice`, so it runs in processes there.

The owner's ruling (Kevin, 2026-09-11): propagate. The caller's
`tesseract_cmd` travels with each work item and the worker adopts it.

**Threads were never affected**, and this file says so rather than
assuming it: a thread shares the caller's module, so the worker already
sees the caller's value, and the adopt step must not write it back. A
worker thread writing module state the caller owns is the shape that
leaked a stand-in across tests in #466.

**How tesseract is faked.** Neither local gate interpreter has
pytesseract and CI has the real one, so a test that ran real OCR would
pass for different reasons in the two places. `_TesseractStandIn` is the
one piece of pytesseract this depends on: `image_to_data` runs
`pytesseract.pytesseract.tesseract_cmd`, and a bare name is looked up on
`PATH`, which in the measured scenario does not have it. An absolute path
to an executable is found. The configured binary is a real executable
file in `tmp_path`, which is not on `PATH`. In processes the stand-in is
installed **inside the spawned child only**; its answer carries the pid,
which is how each test proves the boundary was crossed.
"""
import multiprocessing
import os
import stat
import threading
from datetime import date
from types import SimpleNamespace
from unittest.mock import patch

import numpy as np
import pytest

from isocenter import pixel_analysis
from isocenter import session as session_module
from isocenter.entities import Equipment, Instance, Patient, Series, Study
from isocenter.session import DicomSession

SC_SOP_CLASS = "1.2.840.10008.5.1.4.1.1.7"
SERIAL = "SN-458"
ZONE = [0, 20, 0, 20]
#: pytesseract's own default, and what a freshly imported one carries.
PYTESSERACT_DEFAULT_CMD = "tesseract"

_REAL_VERIFY_WORKER = session_module._verify_worker  # pylint: disable=protected-access
_REAL_DISCOVER_WORKER = session_module._discover_worker  # pylint: disable=protected-access


class _Cmd:
    """`pytesseract.pytesseract`: holds `tesseract_cmd`, and records writes."""

    def __init__(self, cmd):
        object.__setattr__(self, "writes", [])
        object.__setattr__(self, "tesseract_cmd", cmd)

    def __setattr__(self, name, value):
        self.writes.append((threading.current_thread().name, name, value))
        object.__setattr__(self, name, value)


class _TesseractStandIn:
    """The part of pytesseract a scan touches, resolving its binary as it does."""

    Output = SimpleNamespace(DICT="dict")

    def __init__(self, cmd):
        self.pytesseract = _Cmd(cmd)

    def get_tesseract_version(self):
        return "5.5.3"

    def image_to_data(self, _img, *_args, **_kwargs):
        cmd = self.pytesseract.tesseract_cmd
        if not (os.path.isabs(cmd) and os.access(cmd, os.X_OK)):
            # pytesseract's `TesseractNotFoundError` message, verbatim.
            raise RuntimeError(
                "tesseract is not installed or it's not in your PATH. "
                "See README file for more information.")
        return {"text": [f"PID{os.getpid()}"], "conf": [90], "left": [200],
                "top": [200], "width": [50], "height": [50]}


def _in_a_fresh_child(real, args):
    """Run `real` in a child whose pytesseract is freshly imported.

    Patched only in a spawned child. In the parent the stand-in the test
    installed with `monkeypatch` answers, and a pass that silently ran in
    threads fails the pid assertion.
    """
    if multiprocessing.parent_process() is None:
        return real(args)
    with patch.object(pixel_analysis, "HAS_OCR", True), \
            patch.object(pixel_analysis, "pytesseract",
                         _TesseractStandIn(PYTESSERACT_DEFAULT_CMD)):
        return real(args)


def _verify_in_a_fresh_child(args):
    return _in_a_fresh_child(_REAL_VERIFY_WORKER, args)


def _discover_in_a_fresh_child(args):
    return _in_a_fresh_child(_REAL_DISCOVER_WORKER, args)


@pytest.fixture
def configured_off_path(tmp_path, monkeypatch):
    """The caller's pytesseract, pointed at an executable that is not on PATH."""
    binary = tmp_path / "opt" / "tesseract"
    binary.parent.mkdir()
    binary.write_text("#!/bin/sh\nexit 1\n")
    binary.chmod(binary.stat().st_mode | stat.S_IXUSR)
    assert str(binary.parent) not in os.environ.get("PATH", "").split(os.pathsep)
    stand_in = _TesseractStandIn(str(binary))
    monkeypatch.setattr(pixel_analysis, "HAS_OCR", True)
    monkeypatch.setattr(pixel_analysis, "pytesseract", stand_in)
    return stand_in


def _route(monkeypatch, op, mode):
    monkeypatch.setenv("ISOCENTER_MAX_WORKERS", "2")
    for name in ("ISOCENTER_FORCE_THREADS", "ISOCENTER_FORCE_PROCESSES",
                 "ISOCENTER_MAX_TASKS_PER_CHILD"):
        monkeypatch.delenv(name, raising=False)
    if mode == "threads":
        monkeypatch.setenv("ISOCENTER_FORCE_THREADS", "1")
        return
    if mode == "processes":
        monkeypatch.setenv("ISOCENTER_FORCE_PROCESSES", "1")
    else:
        monkeypatch.setenv("ISOCENTER_MAX_TASKS_PER_CHILD", "1")
    worker = "_verify_worker" if op == "scan" else "_discover_worker"
    monkeypatch.setattr(session_module, worker,
                        _verify_in_a_fresh_child if op == "scan"
                        else _discover_in_a_fresh_child)


def _session(n=3):
    session = DicomSession(":memory:")
    patient = Patient("P458", "Cmd^Route")
    study = Study("1.2.826.0.1.458", date(2023, 1, 1))
    series = Series("1.2.826.0.1.458.1", "OT", 1)
    series.equipment = Equipment("Acme", "Model", SERIAL)
    for k in range(n):
        instance = Instance(f"1.2.826.0.1.458.1.{k}", SC_SOP_CLASS, k + 1)
        instance.file_path = None
        instance.set_attr("0018,1000", SERIAL)
        instance.set_pixel_data(np.full((32, 32), 1, dtype=np.uint8))
        series.instances.append(instance)
    study.series.append(series)
    patient.studies.append(study)
    session.store.patients.append(patient)
    session.configuration.rules = [{"serial_number": SERIAL, "redaction_zones": [ZONE]}]
    return session


def _pids(op, session):
    """Run the pass; return the pid each read instance was OCR'd in."""
    if op == "scan":
        report = session.scan_pixel_content()
        assert report.failures == [], report.failures
        return [int(f.value[3:]) for f in report]
    result = session.discover_redaction_zones(SERIAL)
    assert result.n_sources == 3
    return [int(c.text[3:]) for c in result.candidates]


@pytest.mark.parametrize("op,mode", [
    ("scan", "processes"),
    ("scan", "recycling"),
    ("discover", "recycling"),
])
def test_a_spawned_worker_runs_the_tesseract_its_caller_configured(
        configured_off_path, monkeypatch, op, mode):
    """P1: the route that was measured raising now reads every instance.

    Red before #458: the child's pytesseract keeps its bare `"tesseract"`,
    is not found on `PATH`, and every instance fails, so the pass raises
    `PixelScanError` -- the measured outcome, not a stand-in's quirk.
    """
    _route(monkeypatch, op, mode)
    session = _session()
    try:
        pids = _pids(op, session)
    finally:
        session.close()

    assert len(pids) == 3
    assert all(pid != os.getpid() for pid in pids), (
        "the pass ran in this process, so nothing crossed a boundary")


@pytest.mark.parametrize("op", ["scan", "discover"])
def test_a_worker_thread_shares_the_callers_tesseract_and_writes_nothing(
        configured_off_path, monkeypatch, op):
    """P2: threads needed no fix, and the fix must not touch them.

    The worker reads the caller's `tesseract_cmd` because it is the same
    module object. Writing the value back from each worker thread would
    be harmless only while it is the same value; it is module state the
    caller owns, and #466 is what a worker thread writing it looks like.
    """
    _route(monkeypatch, op, "threads")
    session = _session()
    try:
        pids = _pids(op, session)
    finally:
        session.close()

    assert pids == [os.getpid()] * 3
    assert configured_off_path.pytesseract.writes == [], (
        configured_off_path.pytesseract.writes)
