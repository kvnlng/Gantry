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
by a call counter, which races across the pool's threads. E6 is the one
test in processes, and it patches inside the child (see its wrapper).

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
