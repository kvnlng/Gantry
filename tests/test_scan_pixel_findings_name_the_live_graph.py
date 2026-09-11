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

**Why each finding carries the worker's pid.** Without it, a strategy
lever that was silently ignored would run the "processes" case in
threads, where the live instance was already the answer before #412,
and this file would pass for the wrong reason. The pid proves which arm
ran.

This file names no mutation-probe target module, so it needs no
`TARGETS` row (`tests/test_mutation_probe_targets._importers` matches
the text of a dotted module name anywhere in a test file).
"""
import os
from datetime import date
from unittest.mock import patch

import numpy as np
import pytest

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


def _verify_worker_with_one_leak(args):
    """The real worker, with OCR answering one word, stamped with this pid.

    The patch is made here, in whichever process runs the worker, so it
    holds in a spawned child as well as in a thread. It patches
    `pixel_analysis._ocr_instance`, which the worker reads through since
    #423; a patch on `verification.analyze_pixels` would now be inert.
    """
    with patch("isocenter.pixel_analysis._ocr_instance",
               return_value=_InstanceOcr(
                   [TextRegion("LEAKTEXT", TEXT_OUTSIDE_THE_ZONE, 90.0)], True, None)):
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
