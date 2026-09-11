"""`scan_pixel_content()`'s findings name the live graph, in either strategy (#412).

`PhiFinding.entity` is frozen at 1.0, and until #412 it meant two things
on this path. Under threads it was the instance in `session.store`;
under processes it was the copy the worker unpickled, so
`finding.entity is <the live instance>` was `False`, an edit through it
reached nothing, and it carried the frame the worker had decoded for
OCR. Which one a caller got was decided by `run_parallel()`'s strategy,
which an environment variable or the interpreter build changes without
a line of code changing. The owner's ruling: rehydrate everywhere, the
way `audit()` always has.

**Why its own file.** `tests/test_scan_pixel_content_dispatches_its_worker.py`
forces threads with an autouse fixture, for reasons it records: a
`unittest.mock.patch` made in the test does not cross a process
boundary, and neither local gate interpreter has `pytesseract`. This
file gets past that by patching *inside* the worker instead of around
it: `_verify_worker_with_one_leak` is a module-level function, so a
spawned child imports this module by name and runs the patch itself.
In the parent -- the threads arm -- the test installs the stand-in with
`monkeypatch` and the worker patches nothing: its threads share the
parent's module, and a patch made in each of them raced (review of #466;
`test_two_workers_at_once_leave_the_parents_ocr_alone`).

**Why each finding carries the worker's pid.** Without it, a strategy
lever that was silently ignored would run the "processes" case in
threads, where the live instance was already the answer before #412,
and this file would pass for the wrong reason. The pid proves which arm
ran.

This file names no mutation-probe target module, so it needs no
`TARGETS` row (`tests/test_mutation_probe_targets._importers` matches
the text of a dotted module name anywhere in a test file).
"""
import multiprocessing
import os
import sys
import threading
import types
from datetime import date
from unittest.mock import patch

import numpy as np
import pytest

from isocenter import pixel_analysis
from isocenter import session as session_module
from isocenter.entities import Equipment, Instance, Patient, Series, Study
from isocenter.pixel_analysis import TextRegion, _InstanceOcr
from isocenter.session import DicomSession

CT_SOP_CLASS = "1.2.840.10008.5.1.4.1.1.2"
SERIAL = "SN-LIVE"
#: Zone space `(y1, y2, x1, x2)`; the leak below is outside it, in OCR
#: box space `(x, y, w, h)`, so every scanned instance raises one finding.
ZONE_TOP_LEFT = [0, 100, 0, 100]
TEXT_OUTSIDE_THE_ZONE = (200, 200, 50, 50)

#: Captured at import. In the parent that is before the test's
#: `monkeypatch` replaces the module attribute, and in a spawned child it
#: is the only `_verify_worker` there is -- so the wrapper never calls
#: itself.
_REAL_VERIFY_WORKER = session_module._verify_worker  # pylint: disable=protected-access


def _ocr_one_leak(_instance):
    """OCR's stand-in: one word, outside the zone, for every instance."""
    return _InstanceOcr(
        [TextRegion("LEAKTEXT", TEXT_OUTSIDE_THE_ZONE, 90.0)], True, None)


def _verify_worker_with_one_leak(args):
    """The real worker, with OCR answering one word, stamped with this pid.

    It replaces `pixel_analysis._ocr_instance`, which the worker reads
    through since #423; a patch on `verification.analyze_pixels` would now
    be inert. **Only in a spawned child**, where `unittest.mock.patch` in
    the test cannot reach and a pool child runs one task at a time. In the
    parent the test has already installed the stand-in with `monkeypatch`,
    and a patch here would be made in each worker thread at once: the
    threads share the parent's module, `mock.patch` is not thread-safe,
    and two interleaved enters and exits left the stand-in installed for
    every later test (review of #466).
    """
    if multiprocessing.parent_process() is None:
        outcome = _REAL_VERIFY_WORKER(args)
    else:
        with patch.object(pixel_analysis, "_ocr_instance", _ocr_one_leak):
            outcome = _REAL_VERIFY_WORKER(args)
    for finding in outcome.findings:
        finding.metadata["worker_pid"] = os.getpid()
    return outcome


def _session_with_three_instances():
    session = DicomSession(":memory:")
    patient = Patient("PAT1", "Live^Graph")
    study = Study("ST_1", date(2023, 1, 1))
    series = Series("SE_1", "CT", 1)
    series.equipment = Equipment("Acme", "Model", SERIAL)
    for n in range(3):
        instance = Instance(f"1.2.826.0.1.{n}", CT_SOP_CLASS, n + 1)
        instance.set_attr("0018,1000", SERIAL)
        instance.set_pixel_data(np.full((16, 16), 1000, dtype=np.uint16))
        series.instances.append(instance)
    study.series.append(series)
    patient.studies.append(study)
    session.store.patients.append(patient)
    session.configuration.rules = [
        {"serial_number": SERIAL, "redaction_zones": [ZONE_TOP_LEFT]}]
    return session, series.instances


@pytest.mark.parametrize("strategy", ["processes", "threads"])
def test_each_finding_is_the_instance_in_the_session_store(
        strategy, monkeypatch, ocr_present):
    """412-a: `finding.entity is session.store's instance`, under both strategies.

    Processes is the arm #412 was about: red before the fix, because the
    entity was the worker's unpickled copy. Threads was already right and
    is here so that "one meaning" is a pin rather than a claim.

    Killing mutation: the `_rehydrate_findings` call deleted from
    `scan_pixel_content` -- processes goes red (the entity is `None`
    once the worker strips it, a dead copy before that). A missing strip
    in the worker is *not* seen here: rehydration overwrites the copy,
    and the identity holds with the frame still crossing the pipe. That
    one is T-394a's.
    """
    monkeypatch.setenv("ISOCENTER_MAX_WORKERS", "2")
    monkeypatch.delenv("ISOCENTER_MAX_TASKS_PER_CHILD", raising=False)
    if strategy == "processes":
        monkeypatch.setenv("ISOCENTER_FORCE_PROCESSES", "1")
        monkeypatch.delenv("ISOCENTER_FORCE_THREADS", raising=False)
    else:
        monkeypatch.setenv("ISOCENTER_FORCE_THREADS", "1")
        monkeypatch.delenv("ISOCENTER_FORCE_PROCESSES", raising=False)
    monkeypatch.setattr(session_module, "_verify_worker", _verify_worker_with_one_leak)
    # The threads arm's stand-in, on the parent's module and undone with
    # the test. A child installs its own (see the wrapper).
    monkeypatch.setattr(pixel_analysis, "_ocr_instance", _ocr_one_leak)

    session, instances = _session_with_three_instances()
    try:
        report = session.scan_pixel_content()
    finally:
        session.close()

    live = {instance.sop_instance_uid: instance for instance in instances}
    # Before the identity loop, so an empty report is red rather than a
    # loop over nothing.
    assert len(report) == len(live), list(report)

    parent = os.getpid()
    for finding in report:
        pid = finding.metadata["worker_pid"]
        if strategy == "processes":
            assert pid != parent, "the processes case ran in this process"
        else:
            assert pid == parent, "the threads case ran in another process"
        assert finding.entity is live[finding.entity_uid], (
            f"{strategy}: finding.entity for {finding.entity_uid} is a "
            f"{type(finding.entity).__name__} that is not the instance in "
            f"session.store")


def test_two_workers_at_once_leave_the_parents_ocr_alone(monkeypatch):
    """The wrapper must not touch the parent's `_ocr_instance` (review of #466).

    It used to install its stand-in with `with patch(...)` in whichever
    thread ran it, and `mock.patch` is not thread-safe: worker A enters
    (saving the real function), B enters (saving A's stand-in as "the
    original"), A exits, B exits -- and restores A's stand-in for the rest
    of the session. Every later scan then read it: in CI run 34587592174
    (3.14t) twelve tests in `test_scan_reports_what_it_could_not_read.py`
    and `test_voi_lut_integration.py` went red, far from the cause. Rare,
    not ordered: measured on 3.14t, this file left the stand-in behind in
    2 of 20 runs on the #466 branch and 1 of 20 on 0e3e38c. This test
    forces that interleave with events rather than waiting for it.
    """
    real = pixel_analysis._ocr_instance  # pylint: disable=protected-access
    a_inside, b_inside, a_done = threading.Event(), threading.Event(), threading.Event()

    def inner(who):
        if who == "A":
            a_inside.set()
            b_inside.wait(5)
        else:
            b_inside.set()
            a_done.wait(5)
        return types.SimpleNamespace(findings=[])

    monkeypatch.setattr(sys.modules[__name__], "_REAL_VERIFY_WORKER", inner)

    def run_a():
        _verify_worker_with_one_leak("A")
        a_done.set()

    first = threading.Thread(target=run_a)
    second = threading.Thread(target=_verify_worker_with_one_leak, args=("B",))
    first.start()
    assert a_inside.wait(5)
    second.start()
    first.join(10)
    second.join(10)
    assert not first.is_alive() and not second.is_alive()
    assert b_inside.is_set() and a_done.is_set(), "the interleave was not forced"

    assert pixel_analysis._ocr_instance is real, (  # pylint: disable=protected-access
        "a worker thread left its OCR stand-in on the parent's pixel_analysis")
