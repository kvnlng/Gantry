"""`scan_pixel_content()` reports the instances it could not read (#423).

#422 made both OCR methods refuse up front when OCR cannot run in the
calling process. What happened after that check passed was still silent:
`analyze_pixels` caught every exception around the pixel load and
`detect_text_regions` every exception around each frame's OCR, each
logged an ERROR, and an instance nobody had read reported exactly like a
clean one. Measured on 6aabc7d with real tesseract: a spawned worker that
could not find the binary the caller could returned 0 findings for four
instances with burned-in text, and no error reached the caller.

The owner's ruling, which these tests hold:

- `PhiReport.failures` is a list of `(entity_uid, reason)`, one per
  instance whose pixels could not be read or whose OCR raised on any
  frame, and is never `None`;
- the count is warned;
- `pixel_analysis.PixelScanError`, a `RuntimeError` carrying `.failures`
  and `.attempted`, is raised after the pass **only** when something
  failed and nothing was read -- `ExportError`'s rule (#191), not "no
  findings";
- an instance with no pixel element is neither read nor failed.

**How OCR is faked.** Neither local gate interpreter has `pytesseract`,
and CI has the real one, so nothing here depends on real OCR: a result
that did would pass for different reasons in the two places.
`ocr_present` (in `conftest.py`) stands in for pytesseract in this
process, and its `image_to_data` is overridden where a test needs text or
a failure. Which frame fails is decided by the frame's *contents*, never
by a call counter, which races across the pool's threads. E6, E13, E14
and E15 run in processes, and each patches inside the child (see
`_worker_that_cannot_run_tesseract_for_one_instance`).

**Why this file imports what it does.** It reaches `PhiReport` through
`session_module.PhiReport`, never by the dotted name of the module that
defines it: `tests/test_mutation_probe_targets._importers` matches the
text of a dotted module name anywhere in a test file, and naming that
module would charge this file to every one of its mutants for no kill
signal. It does import `isocenter.session` and `isocenter.entities`, so it
sits in both of those `TARGETS` rows in `scripts/mutation_probe.py`.
"""
import logging
import os
import time
from datetime import date
from unittest.mock import patch

import numpy as np
import pytest
from pydicom.dataset import Dataset, FileMetaDataset
from pydicom.uid import ExplicitVRLittleEndian, generate_uid

from isocenter import pixel_analysis
from isocenter import session as session_module
from isocenter.entities import Equipment, Instance, Patient, Series, Study
from isocenter.pixel_analysis import TextRegion
from isocenter.session import DicomSession
from isocenter.verification import RedactionVerifier

SC_SOP_CLASS = "1.2.840.10008.5.1.4.1.1.7"
SR_SOP_CLASS = "1.2.840.10008.5.1.4.1.1.88.11"
SERIAL = "SN-423"
#: Zone space `(y1, y2, x1, x2)`. The one word the fake OCR answers sits
#: outside it, in box space `(x, y, w, h)`, so every read frame raises
#: exactly one finding.
ZONE = [0, 20, 0, 20]
WORD = {"text": ["LEAKTEXT"], "conf": [90], "left": [200], "top": [200],
        "width": [50], "height": [50]}
NO_WORDS = {"text": [], "conf": [], "left": [], "top": [], "width": [], "height": []}


@pytest.fixture(autouse=True)
def _threads(monkeypatch):
    """Threads, so `ocr_present` reaches the pool; E6 overrides this."""
    monkeypatch.setenv("ISOCENTER_MAX_WORKERS", "3")
    monkeypatch.setenv("ISOCENTER_FORCE_THREADS", "1")
    monkeypatch.delenv("ISOCENTER_FORCE_PROCESSES", raising=False)
    monkeypatch.delenv("ISOCENTER_MAX_TASKS_PER_CHILD", raising=False)


def _frame(fill, shape=(32, 32)):
    """A uint8 frame of one value: the value is how the fake OCR tells frames apart."""
    return np.full(shape, fill, dtype=np.uint8)


def _instance(uid, pixels=None):
    instance = Instance(uid, SC_SOP_CLASS, 1)
    instance.file_path = None
    instance.set_attr("0018,1000", SERIAL)
    if pixels is not None:
        instance.set_pixel_data(pixels)
    return instance


def _unreadable(uid):
    """An instance whose sidecar read fails, as a real `OSError` would."""
    instance = _instance(uid, _frame(1))
    instance.pixel_array = None

    def broken_loader():
        raise OSError("sidecar read failed: [Errno 5] Input/output error")
    instance._pixel_loader = broken_loader  # pylint: disable=protected-access
    return instance


def _session(instances):
    session = DicomSession(":memory:")
    patient = Patient("P423", "Scan^Failure")
    study = Study("1.2.826.0.1.423", date(2023, 1, 1))
    series = Series("1.2.826.0.1.423.1", "OT", 1)
    series.equipment = Equipment("Acme", "Model", SERIAL)
    series.instances.extend(instances)
    study.series.append(series)
    patient.studies.append(study)
    session.store.patients.append(patient)
    session.configuration.rules = [{"serial_number": SERIAL, "redaction_zones": [ZONE]}]
    return session


def _one_word_per_frame(ocr_present, failing_fill=None):
    """Answer one word per frame; raise for the frame filled with `failing_fill`."""
    def image_to_data(img, *_args, **_kwargs):
        if failing_fill is not None and np.asarray(img).flat[0] == failing_fill:
            raise RuntimeError("Tesseract process timeout")
        return WORD
    ocr_present.image_to_data = image_to_data


def _scan(instances):
    session = _session(instances)
    try:
        return session.scan_pixel_content()
    finally:
        session.close()


def test_an_instance_whose_pixels_cannot_be_read_is_a_failure_not_a_clean_frame(ocr_present):
    """E1: a load failure is named, keyed on its own UID, and costs no one else.

    Red before #423: the report had no `failures`, and the victim was
    simply absent (measured: 6 findings of 8, one ERROR line).
    """
    _one_word_per_frame(ocr_present)
    others = [_instance(f"1.2.826.0.1.423.1.{n}", _frame(1)) for n in (1, 2, 3)]
    victim = _unreadable("1.2.826.0.1.423.1.0")

    report = _scan([victim] + others)

    assert len(report.failures) == 1, report.failures
    uid, reason = report.failures[0]
    assert uid == "1.2.826.0.1.423.1.0"
    assert "OSError" in reason and "sidecar read failed" in reason, reason
    assert {f.entity_uid for f in report} == {i.sop_instance_uid for i in others}


def test_a_frame_whose_ocr_fails_is_named_and_the_other_frames_still_report(ocr_present):
    """E2: frame 1 of 3 fails; frames 0 and 2 keep their findings, and no raise.

    Red before #423: frame 1's text was silently absent. A `try` hoisted
    around the frame loop would lose frame 2 as well; `read` set only when
    every frame succeeded would make this single instance raise.
    """
    _one_word_per_frame(ocr_present, failing_fill=11)
    instance = _instance("1.2.826.0.1.423.2.0",
                         np.stack([_frame(10), _frame(11), _frame(12)]))

    report = _scan([instance])

    assert {f.field_name for f in report} == {"PixelData[Frame=0]", "PixelData[Frame=2]"}
    assert len(report.failures) == 1, report.failures
    uid, reason = report.failures[0]
    assert uid == "1.2.826.0.1.423.2.0"
    assert "frame 1" in reason and "frame 0" not in reason and "frame 2" not in reason, reason


def test_nothing_read_raises_after_the_pass_carrying_every_failure(ocr_present, caplog):
    """E3: every instance failed, so the scan raises -- after it warned.

    Five attempted: four unreadable and one with no pixel element, which
    is neither read nor failed. So `.attempted` is 5 and `.failures` is 4,
    and a raise that counted `attempted` from the failures reads 4.
    """
    _one_word_per_frame(ocr_present)
    instances = [_unreadable(f"1.2.826.0.1.423.3.{n}") for n in range(4)]
    instances.append(_instance("1.2.826.0.1.423.3.9"))

    with caplog.at_level(logging.WARNING):
        with pytest.raises(RuntimeError) as raised:
            _scan(instances)
        # Inside the `raises` block's scope, so a raise placed before the
        # warning is red here: the warning must already have been emitted.
        warned = [r for r in caplog.records
                  if r.levelno == logging.WARNING and "failures" in r.getMessage()]

    exc = raised.value
    assert isinstance(exc, pixel_analysis.PixelScanError), type(exc)
    assert len(exc.failures) == 4, exc.failures
    assert {uid for uid, _ in exc.failures} == {f"1.2.826.0.1.423.3.{n}" for n in range(4)}
    assert exc.attempted == 5
    assert warned, "the scan raised without warning the count first"


def _write_sr(folder):
    """An SR: a file with no pixel element, as a real session holds one."""
    ds = Dataset()
    ds.file_meta = FileMetaDataset()
    ds.file_meta.TransferSyntaxUID = ExplicitVRLittleEndian
    ds.file_meta.MediaStorageSOPClassUID = SR_SOP_CLASS
    ds.SOPInstanceUID = generate_uid()
    ds.file_meta.MediaStorageSOPInstanceUID = ds.SOPInstanceUID
    ds.SOPClassUID = SR_SOP_CLASS
    ds.PatientID = "PSR"
    ds.PatientName = "Sr^Test"
    ds.StudyInstanceUID = generate_uid()
    ds.SeriesInstanceUID = generate_uid()
    ds.Modality = "SR"
    ds.StudyDate = "20230101"
    ds.Manufacturer = "Acme"
    ds.DeviceSerialNumber = SERIAL
    ds.save_as(os.path.join(folder, "sr.dcm"), enforce_file_format=True)
    return ds.SOPInstanceUID


@pytest.mark.parametrize("shape", ["ingested_sr", "hand_built"])
def test_an_instance_with_no_pixel_element_is_neither_read_nor_failed(
        shape, ocr_present, tmp_path):
    """E4: no pixels to read is not a failure, in either of its two presentations.

    An ingested SR carries its source file and no loader, and
    `get_pixel_data()` answers `None`. A hand-built instance with no file
    and no loader makes `get_pixel_data()` raise `FileNotFoundError`. Each
    alone in its scan, so counting either as a failure would also raise.
    """
    _one_word_per_frame(ocr_present)
    if shape == "hand_built":
        report = _scan([_instance("1.2.826.0.1.423.4.0")])
    else:
        source = tmp_path / "src"
        source.mkdir()
        sr_uid = _write_sr(str(source))
        with DicomSession(str(tmp_path / "sr.db")) as session:
            summary = session.ingest(str(source))
            assert summary.ingested == 1, summary
            (instance,) = [i for p in session.store.patients for st in p.studies
                           for se in st.series for i in se.instances]
            assert instance.sop_instance_uid == sr_uid
            assert instance.file_path and instance.get_pixel_data() is None, (
                "the fixture is not an SR as a real session holds one")
            session.configuration.rules = [
                {"serial_number": SERIAL, "redaction_zones": [ZONE]}]
            report = session.scan_pixel_content()

    assert len(report) == 0
    assert report.failures == []


def test_a_scan_that_read_instances_and_found_nothing_does_not_raise(ocr_present):
    """E5: three read with no text, one failed: a result, not an error.

    The raise is `failures and read == 0`. Written `failures and not
    findings`, it would raise here, on a scan that read three instances.
    """
    clean = [_instance(f"1.2.826.0.1.423.5.{n}", _frame(1)) for n in (1, 2, 3)]
    assert ocr_present.image_to_data(None) == NO_WORDS

    report = _scan([_unreadable("1.2.826.0.1.423.5.0")] + clean)

    assert len(report) == 0
    assert [uid for uid, _ in report.failures] == ["1.2.826.0.1.423.5.0"]


VICTIM_IN_CHILD = "1.2.826.0.1.423.6.0"

#: Captured at import. In the parent that is before the test's
#: `monkeypatch` replaces the module attribute; in a spawned child it is
#: the only `_verify_worker` there is -- so the wrapper never calls itself.
_REAL_VERIFY_WORKER = session_module._verify_worker  # pylint: disable=protected-access


def _worker_that_cannot_run_tesseract_for_one_instance(args):
    """The real worker, run in a child whose OCR fails for one instance.

    Every patch is made here, in the process that runs the worker, because
    `unittest.mock.patch` does not cross a process boundary. The child
    imports this module by name and has no pytesseract locally, so
    `HAS_OCR` and `pytesseract` are patched here too -- otherwise every
    instance fails for that reason, the scan raises, and the test is red
    for the wrong reason. The victim's error carries this pid, which is
    how the test proves the failure came back from another process.
    """
    instance = args[0]

    def detect(_frame_data, frame_idx=0):
        if instance.sop_instance_uid == VICTIM_IN_CHILD:
            raise RuntimeError(
                "tesseract is not installed or it's not in your PATH "
                f"(pid={os.getpid()})")
        return [TextRegion("LEAKTEXT", (200, 200, 50, 50), 90.0, frame_idx)]

    with patch.object(pixel_analysis, "HAS_OCR", True), \
            patch.object(pixel_analysis, "pytesseract", object()), \
            patch.object(pixel_analysis, "_detect_text_regions_or_raise", detect):
        return _REAL_VERIFY_WORKER(args)


def test_a_worker_process_that_cannot_read_reports_it_back(monkeypatch, ocr_present):
    """E6: the failure crosses the process boundary and lands in `failures`.

    `ocr_present` is for the parent: `_require_ocr` runs there before
    dispatch and refuses on both local interpreters without it. Red before
    #423: the worker returned findings only, and the spawned-worker route
    #422's review found reported 0 findings and no error.
    """
    monkeypatch.setenv("ISOCENTER_MAX_WORKERS", "2")
    monkeypatch.setenv("ISOCENTER_FORCE_PROCESSES", "1")
    monkeypatch.delenv("ISOCENTER_FORCE_THREADS", raising=False)
    monkeypatch.setattr(session_module, "_verify_worker",
                        _worker_that_cannot_run_tesseract_for_one_instance)
    others = [_instance(f"1.2.826.0.1.423.6.{n}", _frame(1)) for n in (1, 2, 3)]

    report = _scan([_instance(VICTIM_IN_CHILD, _frame(1))] + others)

    assert len(report.failures) == 1, report.failures
    uid, reason = report.failures[0]
    assert uid == VICTIM_IN_CHILD
    assert "pid=" in reason, reason
    child_pid = int(reason.split("pid=")[1].split(")")[0])
    assert child_pid != os.getpid(), "the processes case ran in this process"
    assert {f.entity_uid for f in report} == {i.sop_instance_uid for i in others}


def _processes(monkeypatch, workers="2"):
    monkeypatch.setenv("ISOCENTER_MAX_WORKERS", workers)
    monkeypatch.setenv("ISOCENTER_FORCE_PROCESSES", "1")
    monkeypatch.delenv("ISOCENTER_FORCE_THREADS", raising=False)


def _worker_without_pytesseract(args):
    """The real worker, in a child that cannot import pytesseract.

    Forced rather than left to the environment: neither local gate
    interpreter's children have pytesseract and CI's do, so a test that
    relied on the difference would pin this in one of the two places.
    """
    with patch.object(pixel_analysis, "HAS_OCR", False):
        return _REAL_VERIFY_WORKER(args)


def test_a_pixelless_instance_is_not_a_failure_in_a_worker_without_ocr(
        monkeypatch, ocr_present):
    """E13: no pixel element is neither read nor failed on the worker route too.

    #423's route: the parent passes `_require_ocr` and a spawned child
    cannot import pytesseract. The instance with pixels is a failure
    naming that; the one with no pixel element had nothing to read, and
    reporting it as a failure is the false positive E4 pins in threads.
    Red while `_ocr_instance` checked `HAS_OCR` before the pixel-less
    check: the reviewer measured 5 of 5 failed, the pixel-less one among
    them. Also kills the mutant that makes a worker without pytesseract
    report neither (the instance with pixels would then not be a failure,
    and nothing would raise).
    """
    _processes(monkeypatch)
    monkeypatch.setattr(session_module, "_verify_worker", _worker_without_pytesseract)
    pixelless_uid, readable_uid = "1.2.826.0.1.423.14.0", "1.2.826.0.1.423.14.1"

    with pytest.raises(pixel_analysis.PixelScanError) as raised:
        _scan([_instance(pixelless_uid), _instance(readable_uid, _frame(1))])

    exc = raised.value
    assert [uid for uid, _ in exc.failures] == [readable_uid], exc.failures
    assert "pytesseract could not be imported" in exc.failures[0][1], exc.failures
    assert exc.attempted == 2


def test_an_ingested_sr_is_not_a_failure_in_a_worker_without_ocr(
        monkeypatch, ocr_present, tmp_path):
    """E13, the ingested-SR arm: only the load says an SR has no pixel element.

    It carries its source file, so the pre-check cannot tell it from an
    image, and the `HAS_OCR` check must come after the load that answers
    `None` -- moving it below the pre-check alone left this arm reporting
    a failure, and, alone in its scan, raising.
    """
    _processes(monkeypatch)
    monkeypatch.setattr(session_module, "_verify_worker", _worker_without_pytesseract)
    source = tmp_path / "src"
    source.mkdir()
    _write_sr(str(source))

    with DicomSession(str(tmp_path / "sr.db")) as session:
        assert session.ingest(str(source)).ingested == 1
        session.configuration.rules = [
            {"serial_number": SERIAL, "redaction_zones": [ZONE]}]
        report = session.scan_pixel_content()

    assert len(report) == 0
    assert report.failures == []


def _worker_whose_ocr_fails_on_every_frame(args):
    """The real worker, in a child whose tesseract raises for every frame.

    The load and the preparation succeed, so this is the per-frame catch
    alone: every instance reaches the loop and no frame is read.
    """
    def detect(_frame_data, frame_idx=0):
        raise RuntimeError(f"Tesseract process timeout (pid={os.getpid()})")

    with patch.object(pixel_analysis, "HAS_OCR", True), \
            patch.object(pixel_analysis, "pytesseract", object()), \
            patch.object(pixel_analysis, "_detect_text_regions_or_raise", detect):
        return _REAL_VERIFY_WORKER(args)


def test_a_scan_whose_ocr_failed_on_every_frame_raises(monkeypatch, ocr_present):
    """E14: an instance is read only when a frame went through OCR.

    E3 raises on instances whose *load* failed. Here every load succeeds
    and every frame's OCR raises, so nothing was read either; an instance
    counted as read because it had frames to try would return an empty
    report for a scan that saw no text at all.
    """
    _processes(monkeypatch)
    monkeypatch.setattr(session_module, "_verify_worker",
                        _worker_whose_ocr_fails_on_every_frame)
    uids = [f"1.2.826.0.1.423.15.{n}" for n in range(3)]

    with pytest.raises(pixel_analysis.PixelScanError) as raised:
        _scan([_instance(uid, _frame(1)) for uid in uids])

    exc = raised.value
    assert sorted(uid for uid, _ in exc.failures) == uids, exc.failures
    assert all("OCR failed on frame 0" in why and "pid=" in why
               for _, why in exc.failures), exc.failures
    assert exc.attempted == 3


#: Dispatched first and slow: readable, and still reading when the fast
#: failures dispatched behind them come back.
SLOW_READABLE = ("1.2.826.0.1.423.16.0", "1.2.826.0.1.423.16.1")
FAST_FAILING = ("1.2.826.0.1.423.16.2", "1.2.826.0.1.423.16.3")
FAST_READABLE = ("1.2.826.0.1.423.16.4", "1.2.826.0.1.423.16.5")


def _worker_whose_outcomes_come_back_out_of_order(args):
    """The real worker, slow for the first instances and failing fast for two.

    The failure names the instance the child actually read, so a
    failure filed under another instance's UID is visible in its own
    entry, not only in the set.
    """
    uid = args[0].sop_instance_uid

    def detect(_frame_data, frame_idx=0):
        if uid in SLOW_READABLE:
            time.sleep(2.0)
        if uid in FAST_FAILING:
            raise RuntimeError(f"Tesseract process timeout reading {uid}")
        return [TextRegion("LEAKTEXT", (200, 200, 50, 50), 90.0, frame_idx)]

    with patch.object(pixel_analysis, "HAS_OCR", True), \
            patch.object(pixel_analysis, "pytesseract", object()), \
            patch.object(pixel_analysis, "_detect_text_regions_or_raise", detect):
        return _REAL_VERIFY_WORKER(args)


def test_each_outcome_is_filed_under_its_own_instance_on_the_recycling_pool(
        monkeypatch, ocr_present):
    """E15: outcomes are keyed by the UID they carry, never by position.

    `ISOCENTER_MAX_TASKS_PER_CHILD` routes the pass through
    `multiprocessing.Pool.imap_unordered`, which yields results as
    workers finish. The two slow readable instances are dispatched
    first, so the fast failures behind them come back ahead of them: a
    loop that zipped the outcomes onto the dispatch order would file the
    first failure under the first slow instance. The 2 s sleep is the
    margin over the gap between the pool's workers starting, which are
    spawned together.
    """
    _processes(monkeypatch, workers="3")
    monkeypatch.setenv("ISOCENTER_MAX_TASKS_PER_CHILD", "1")
    monkeypatch.setattr(session_module, "_verify_worker",
                        _worker_whose_outcomes_come_back_out_of_order)
    order = SLOW_READABLE + FAST_FAILING + FAST_READABLE

    report = _scan([_instance(uid, _frame(1)) for uid in order])

    assert sorted(uid for uid, _ in report.failures) == list(FAST_FAILING), report.failures
    for uid, why in report.failures:
        assert why.endswith(f"reading {uid}"), (uid, why)
    assert sorted(f.entity_uid for f in report) == sorted(SLOW_READABLE + FAST_READABLE)
    assert all(f.entity is not None and f.entity.sop_instance_uid == f.entity_uid
               for f in report)


def test_the_count_is_warned(ocr_present, caplog):
    """E7: one of four failed, and a WARNING says so; a clean scan warns nothing."""
    _one_word_per_frame(ocr_present)

    with caplog.at_level(logging.WARNING):
        _scan([_unreadable("1.2.826.0.1.423.7.0")]
              + [_instance(f"1.2.826.0.1.423.7.{n}", _frame(1)) for n in (1, 2, 3)])
    warned = [r.getMessage() for r in caplog.records
              if r.levelno == logging.WARNING and "failures" in r.getMessage()]
    assert len(warned) == 1, warned
    assert "1 of 4" in warned[0], warned[0]

    caplog.clear()
    with caplog.at_level(logging.WARNING):
        report = _scan([_instance(f"1.2.826.0.1.423.8.{n}", _frame(1)) for n in range(4)])
    assert len(report) == 4
    assert not [r for r in caplog.records
                if r.levelno == logging.WARNING and "failures" in r.getMessage()]


def test_audit_reports_no_failures_and_still_raises_on_a_worker_error(monkeypatch):
    """E8: `audit()`'s `failures` is `[]` by construction, because it raises.

    `run_parallel` re-raises a worker's exception, so `audit()` has no
    silent drop to report (measured on 6aabc7d: `OSError boom` reaches
    the caller). Both halves are pinned: the attribute is a list, not
    `None`, and the raise is not traded for a `failures` entry.
    """
    session = _session([_instance("1.2.826.0.1.423.9.0")])
    try:
        session.configuration.phi_tags = {
            "0010,0010": {"name": "Patient Name", "action": "EMPTY"}}
        report = session.audit()
        assert report.failures == []

        def boom(*_args, **_kwargs):
            raise OSError("boom")
        monkeypatch.setattr(session_module.PhiInspector, "scan_patient", boom)
        with pytest.raises(OSError, match="boom"):
            session.audit()
    finally:
        session.close()


def test_the_tier_two_functions_still_return_a_list(ocr_present, caplog):
    """E9: `analyze_pixels` and `verify_instance` still log and return `[]`.

    They run per instance inside workers, where raising would turn one
    precondition into N worker failures (#422); the Session path stopped
    relying on them instead. A guard, green before and after #423.
    """
    _one_word_per_frame(ocr_present)
    victim = _unreadable("1.2.826.0.1.423.10.0")

    with caplog.at_level(logging.ERROR):
        assert pixel_analysis.analyze_pixels(victim) == []
    assert any(r.levelno == logging.ERROR and "sidecar read failed" in r.getMessage()
               for r in caplog.records), [r.getMessage() for r in caplog.records]

    assert RedactionVerifier([]).verify_instance(victim) == []


def test_an_unexpected_worker_error_is_a_failure_not_a_lost_pass(ocr_present, monkeypatch):
    """E12: an exception `_ocr_instance` does not catch costs one instance, not the pass.

    `run_parallel` re-raises a worker's exception by default (that is how
    `audit()` fails, E8), so without the boundary catch in `_verify_worker`
    one instance failing outside `_ocr_instance`'s own catches would take
    every other instance's findings down with it. Here the helper itself
    raises for one instance: that instance is a failure naming the error,
    and the other three still report.
    """
    _one_word_per_frame(ocr_present)
    real = pixel_analysis._ocr_instance  # pylint: disable=protected-access
    victim_uid = "1.2.826.0.1.423.11.0"

    def ocr_instance(instance):
        if instance.sop_instance_uid == victim_uid:
            raise MemoryError("could not allocate the decoded frame")
        return real(instance)
    monkeypatch.setattr(pixel_analysis, "_ocr_instance", ocr_instance)
    others = [_instance(f"1.2.826.0.1.423.11.{n}", _frame(1)) for n in (1, 2, 3)]

    report = _scan([_instance(victim_uid, _frame(1))] + others)

    assert len(report.failures) == 1, report.failures
    uid, reason = report.failures[0]
    assert uid == victim_uid
    assert "MemoryError" in reason and "could not allocate" in reason, reason
    assert {f.entity_uid for f in report} == {i.sop_instance_uid for i in others}


# --- discovery (#423, owner question Q1, recommended answer (b)) ----------
#
# Discovery has no failure field -- growing `DiscoveryResult` was the other
# half of the 0.9.5 decision the #423 ruling reversed for `PhiReport` only --
# so the same rule reaches it through the warning, `n_sources` and the raise.


def _discover(instances):
    session = _session(instances)
    try:
        return session.discover_redaction_zones(SERIAL)
    finally:
        session.close()


#: An instance with no pixel element, added to a sample by the arms below:
#: neither read nor failed, so it is not a source and it does not stand
#: between a sample whose every *read* failed and the raise.
#: A count rather than the instance itself, so each test builds its own
#: rather than two sessions sharing one live object built at import.
WITH_PIXELLESS = pytest.mark.parametrize(
    "n_pixelless", [0, 1], ids=["all_have_pixels", "one_has_no_pixel_element"])


def _pixelless(n):
    return [_instance(f"1.2.826.0.1.423.12.9{k}") for k in range(n)]


@WITH_PIXELLESS
def test_discovery_that_read_nothing_raises(ocr_present, n_pixelless):
    """E10: every sampled instance failed, so discovery raises rather than
    returning an empty result for a scan that saw nothing.

    The pixel-less arm is the rule's own wording, "failed and none read":
    a raise conditioned on every sampled instance failing would not fire
    there, since the pixel-less instance did not fail.
    """
    _one_word_per_frame(ocr_present)
    pixelless = _pixelless(n_pixelless)
    failing = [_unreadable(f"1.2.826.0.1.423.12.{n}") for n in range(3)]
    with pytest.raises(RuntimeError) as raised:
        _discover(failing + pixelless)
    exc = raised.value
    assert isinstance(exc, pixel_analysis.PixelScanError), type(exc)
    assert len(exc.failures) == 3
    assert exc.attempted == 3 + len(pixelless)


@WITH_PIXELLESS
def test_discovery_counts_only_instances_it_read(ocr_present, caplog, n_pixelless):
    """E11: one of four failed, so three are sources, and the count is warned.

    `n_sources` is the denominator of every zone's occurrence rate. Counting
    the instance nobody read made a zone on every readable frame look like
    a zone on three quarters of them -- and that holds for the instance
    with no pixel element as much as for the one that failed.
    """
    _one_word_per_frame(ocr_present)
    pixelless = _pixelless(n_pixelless)
    instances = [_unreadable("1.2.826.0.1.423.13.0")] + [
        _instance(f"1.2.826.0.1.423.13.{n}", _frame(1)) for n in (1, 2, 3)]
    with caplog.at_level(logging.WARNING):
        result = _discover(instances + pixelless)
    assert result.n_sources == 3
    assert len(result) == 3
    expected = f"1 of {4 + len(pixelless)}"
    warned = [r.getMessage() for r in caplog.records
              if r.levelno == logging.WARNING and expected in r.getMessage()]
    assert len(warned) == 1, [r.getMessage() for r in caplog.records]


def test_an_unexpected_discovery_error_costs_one_instance(ocr_present, monkeypatch):
    """E17: discovery's boundary catch, as E12 is the scan's.

    `run_parallel` re-raises a worker's exception, so without the catch in
    `_discover_worker` one instance failing outside `_ocr_instance`'s own
    catches would end the discovery; with it, that instance is a failure
    and the other three are sources.
    """
    _one_word_per_frame(ocr_present)
    real = pixel_analysis._ocr_instance  # pylint: disable=protected-access
    victim_uid = "1.2.826.0.1.423.17.0"

    def ocr_instance(instance):
        if instance.sop_instance_uid == victim_uid:
            raise MemoryError("could not allocate the decoded frame")
        return real(instance)
    monkeypatch.setattr(pixel_analysis, "_ocr_instance", ocr_instance)

    result = _discover([_instance(victim_uid, _frame(1))] + [
        _instance(f"1.2.826.0.1.423.17.{n}", _frame(1)) for n in (1, 2, 3)])

    assert result.n_sources == 3
    assert len(result) == 3
