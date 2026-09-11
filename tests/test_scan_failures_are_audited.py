"""A pixel-scan failure writes one WARNING audit row and costs the run its PASS (#479).

#423 made `scan_pixel_content()` and `discover_redaction_zones()` say which
instances they could not read: `PhiReport.failures` (or discovery's ERROR
log), a WARNING with the count, and `PixelScanError` when none was read.
None of that reached the audit table. A session whose OCR verification
failed on half its instances wrote no row about it, and its compliance
report graded `PASS` beside "No exceptions or errors were recorded." --
measured on e184933 with real tesseract, where a clean scan and a scan that
read nothing wrote the same zero rows.

The owner's ruling (Kevin, 2026-09-11), which these tests hold:

- each failure writes **one** `WARNING` row, as export failures write an
  `ERROR` row each (`DicomExporter._report_export_failures`), carrying the
  entity UID and the `describe_exception` reason;
- in both methods, under threads and under processes;
- on the zero-read path the rows are written **before** `PixelScanError`
  is raised, so a caller who catches it has an audit log that already
  says so;
- a run with any scan failure grades `REVIEW_REQUIRED`.

`WARNING` is not a new word: it is in the frozen audit vocabulary
(`docs/api/stability.md`) and `get_audit_errors()` already selects it,
which is what feeds the report's `exceptions` and its grade.

**The grade needs a PASS baseline.** An empty audit summary grades
`REVIEW_REQUIRED` on its own, and a scan that succeeds writes nothing, so
a grade test that only scanned would pass without the fix. Every session
here anonymizes first, which writes remediation rows; the control arm
proves that baseline really is `PASS`.

**How OCR is faked**, as in `test_scan_reports_what_it_could_not_read.py`:
the frame whose first pixel is `FAILING_FILL` fails in OCR, decided by
contents rather than a counter. In threads the stand-in is the parent's
`ocr_present`, installed by `monkeypatch`; in processes it is installed
**inside the spawned child only** (`_in_a_child_with_fake_ocr`), because
`mock.patch` from worker threads is not thread-safe and leaked a stand-in
across tests once (#466). The failure message carries the pid it was
raised in, which is how the processes arms prove a boundary was crossed.
Discovery forces threads, so its processes arm is the recycling pool:
`ISOCENTER_MAX_TASKS_PER_CHILD` outranks `force_threads=True`.
"""
import multiprocessing
import os
import re
import sqlite3
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
SERIAL = "SN-479"
ZONE = [0, 20, 0, 20]
#: Outside `ZONE`, so every frame read raises one finding.
WORD = {"text": ["LEAKTEXT"], "conf": [90], "left": [200], "top": [200],
        "width": [50], "height": [50]}
FAILING_FILL = 7
READABLE_FILL = 1
PASS_LINE = "| **Validation Status** | **PASS** |"
REVIEW_LINE = "| **Validation Status** | **REVIEW_REQUIRED** |"

#: Captured at import: in a spawned child these are the only workers there
#: are, so the wrappers below never call themselves.
_REAL_VERIFY_WORKER = session_module._verify_worker  # pylint: disable=protected-access
_REAL_DISCOVER_WORKER = session_module._discover_worker  # pylint: disable=protected-access


def _image_to_data(img, *_args, **_kwargs):
    if np.asarray(img).flat[0] == FAILING_FILL:
        raise RuntimeError(f"Tesseract process timeout (pid={os.getpid()})")
    return WORD


def _fake_pytesseract():
    return SimpleNamespace(get_tesseract_version=lambda: "5.5.3",
                           image_to_data=_image_to_data,
                           Output=SimpleNamespace(DICT="dict"))


def _in_a_child_with_fake_ocr(real, args):
    """Run `real` with the fake OCR, patched only if this is a spawned child.

    In the parent the wrapper patches nothing, so a pass that silently ran
    in threads fails in the pid assertion rather than passing on the
    parent's `ocr_present`.
    """
    if multiprocessing.parent_process() is None:
        return real(args)
    with patch.object(pixel_analysis, "HAS_OCR", True), \
            patch.object(pixel_analysis, "pytesseract", _fake_pytesseract()):
        return real(args)


def _verify_in_child(args):
    return _in_a_child_with_fake_ocr(_REAL_VERIFY_WORKER, args)


def _discover_in_child(args):
    return _in_a_child_with_fake_ocr(_REAL_DISCOVER_WORKER, args)


@pytest.fixture(autouse=True)
def _threads(monkeypatch):
    monkeypatch.setenv("ISOCENTER_MAX_WORKERS", "2")
    monkeypatch.setenv("ISOCENTER_FORCE_THREADS", "1")
    monkeypatch.delenv("ISOCENTER_FORCE_PROCESSES", raising=False)
    monkeypatch.delenv("ISOCENTER_MAX_TASKS_PER_CHILD", raising=False)


def _use(mode, op, monkeypatch, ocr_present):
    """Route the pass through `mode`, with the fake OCR where it will run."""
    ocr_present.image_to_data = _image_to_data
    if mode == "threads":
        return
    monkeypatch.delenv("ISOCENTER_FORCE_THREADS", raising=False)
    if op == "scan":
        monkeypatch.setenv("ISOCENTER_FORCE_PROCESSES", "1")
        monkeypatch.setattr(session_module, "_verify_worker", _verify_in_child)
    else:
        # `discover_redaction_zones()` passes `force_threads=True`, which
        # beats `ISOCENTER_FORCE_PROCESSES`; only recycling outranks it.
        monkeypatch.setenv("ISOCENTER_MAX_TASKS_PER_CHILD", "1")
        monkeypatch.setattr(session_module, "_discover_worker", _discover_in_child)


def _session(tmp_path, fills):
    """One series, one instance per fill, and a patient name to anonymize."""
    session = DicomSession(str(tmp_path / "scan.db"))
    patient = Patient("P479", "Original^Name")
    study = Study("1.2.826.0.1.479", date(2023, 1, 1))
    series = Series("1.2.826.0.1.479.1", "OT", 1)
    series.equipment = Equipment("Acme", "Model", SERIAL)
    for n, fill in enumerate(fills):
        instance = Instance(f"1.2.826.0.1.479.1.{n}", SC_SOP_CLASS, n + 1)
        instance.file_path = None
        instance.set_attr("0018,1000", SERIAL)
        instance.set_pixel_data(np.full((32, 32), fill, dtype=np.uint8))
        series.instances.append(instance)
    study.series.append(series)
    patient.studies.append(study)
    session.store.patients.append(patient)
    session.configuration.rules = [{"serial_number": SERIAL, "redaction_zones": [ZONE]}]
    return session


def _failing_uids(session):
    return sorted(i.sop_instance_uid
                  for p in session.store.patients for st in p.studies
                  for se in st.series for i in se.instances
                  if i.get_pixel_data().flat[0] == FAILING_FILL)


def _warning_rows(session):
    """`(entity_uid, details)` of every WARNING row, after the queue drains."""
    session.store_backend.flush_audit_queue()
    with sqlite3.connect(session.store_backend.db_path) as conn:
        return sorted(conn.execute(
            "SELECT entity_uid, details FROM audit_log WHERE action_type='WARNING'"
        ).fetchall())


def _run(op, session):
    if op == "scan":
        return session.scan_pixel_content()
    return session.discover_redaction_zones(SERIAL)


MODES = pytest.mark.parametrize("mode", ["threads", "processes"])
OPS = pytest.mark.parametrize("op", ["scan", "discover"])


@MODES
@OPS
def test_each_failure_writes_one_warning_row_naming_the_instance_and_why(
        tmp_path, monkeypatch, ocr_present, op, mode):
    """R1: one row per failed instance, none for the ones read.

    The UID is in `entity_uid` **and** in `details`: `get_audit_errors()`
    returns `(timestamp, action_type, details)` with no UID column, so the
    report's exceptions section names the instance only if `details` does.
    Red before #479: zero WARNING rows on every arm.
    """
    _use(mode, op, monkeypatch, ocr_present)
    session = _session(tmp_path, [FAILING_FILL, READABLE_FILL, READABLE_FILL])
    try:
        result = _run(op, session)
        rows = _warning_rows(session)
        victim = _failing_uids(session)
    finally:
        session.close()

    assert len(result) == 2, "the two readable instances were read"
    assert [uid for uid, _ in rows] == victim, rows
    details = rows[0][1]
    assert victim[0] in details, details
    # The `describe_exception` spelling: the type, then the message.
    assert "OCR failed on frame 0: RuntimeError: Tesseract process timeout" in details, details
    pid = int(details.split("pid=")[1].split(")")[0])
    if mode == "processes":
        assert pid != os.getpid(), "the processes arm ran in this process"
    if op == "scan":
        # The row says what `report.failures` says, not a paraphrase of it.
        assert result.failures[0][1] in details, (result.failures, details)


@MODES
@OPS
def test_nothing_read_writes_every_row_before_the_raise(
        tmp_path, monkeypatch, ocr_present, op, mode):
    """R2: `PixelScanError` is raised after the rows, not instead of them.

    A caller who catches the raise has already been told; so has the audit
    log. Moving the audit below the raise makes it unreachable on exactly
    this path, and this test red.
    """
    _use(mode, op, monkeypatch, ocr_present)
    session = _session(tmp_path, [FAILING_FILL] * 3)
    try:
        with pytest.raises(pixel_analysis.PixelScanError) as raised:
            _run(op, session)
        rows = _warning_rows(session)
        victims = _failing_uids(session)
    finally:
        session.close()

    assert sorted(uid for uid, _ in raised.value.failures) == victims
    assert [uid for uid, _ in rows] == victims, rows
    assert all(uid in details for uid, details in rows), rows


@OPS
def test_a_scan_failure_costs_the_run_its_pass_and_the_report_names_it(
        tmp_path, monkeypatch, ocr_present, op):
    """R3: the grade moves, and the exceptions section names the instance.

    The compliance report did not grade OCR verification at all: before
    #479 this arm graded `PASS` beside "No exceptions or errors were
    recorded." The control below is what makes that assertion mean
    something -- the same session with no failure grades `PASS`.
    """
    _use("threads", op, monkeypatch, ocr_present)
    session = _session(tmp_path, [FAILING_FILL, READABLE_FILL])
    report = tmp_path / "report.md"
    try:
        session.anonymize()
        _run(op, session)
        victim = _failing_uids(session)[0]
        session.generate_report(str(report))
    finally:
        session.close()

    content = report.read_text(encoding="utf-8")
    assert REVIEW_LINE in content, content
    exceptions = content.split("## 4. Exceptions & Errors")[1]
    assert "WARNING" in exceptions and victim in exceptions, exceptions


@OPS
def test_a_clean_scan_writes_no_row_and_still_grades_pass(
        tmp_path, monkeypatch, ocr_present, op):
    """R4, the control: the grade moves for a failure, not for scanning."""
    _use("threads", op, monkeypatch, ocr_present)
    session = _session(tmp_path, [READABLE_FILL, READABLE_FILL])
    report = tmp_path / "report.md"
    try:
        session.anonymize()
        _run(op, session)
        rows = _warning_rows(session)
        session.generate_report(str(report))
    finally:
        session.close()

    assert rows == []
    content = report.read_text(encoding="utf-8")
    assert PASS_LINE in content, content


def _image_to_data_with_markup(img, *_args, **_kwargs):
    """A reason carrying the two characters a markdown table row cannot hold."""
    if np.asarray(img).flat[0] == FAILING_FILL:
        raise RuntimeError(f"tesseract said:\nbox 3 | conf -1 (pid={os.getpid()})")
    return WORD


@OPS
def test_a_reason_with_a_newline_and_a_pipe_is_one_escaped_report_row(
        tmp_path, monkeypatch, ocr_present, op):
    """R5: the row reaches section 4 as one table row, with the pipe escaped.

    The report writes each exception as `| ts | action | details |` and
    renders `details` as given, so the flattening and the pipe-escape in
    `_audit_unread_instances` are all that keep a reason off a second
    line or out of a fourth column. Nothing else pinned them: an OCR
    error is free text, and every other reason in this file is one line
    with no pipe. Dropping either step kills this test and no other.
    """
    _use("threads", op, monkeypatch, ocr_present)
    ocr_present.image_to_data = _image_to_data_with_markup
    session = _session(tmp_path, [FAILING_FILL, READABLE_FILL])
    report = tmp_path / "report.md"
    try:
        _run(op, session)
        victim = _failing_uids(session)[0]
        session.generate_report(str(report))
    finally:
        session.close()

    section = report.read_text(encoding="utf-8").split("## 4. Exceptions & Errors")[1]
    section = section.split("\n## ")[0]
    rows = [line for line in section.splitlines() if victim in line]
    assert len(rows) == 1, section
    row = rows[0]
    # Flattened: the text after the newline is on the victim's own line.
    assert "tesseract said: box 3 \\| conf -1" in row, row
    # Escaped: an unescaped pipe would add a column to this row.
    cells = re.split(r"(?<!\\)\|", row)
    assert len(cells) == 5 and cells[2].strip() == "WARNING", cells
