"""compact() refuses while a redact() or ingest() pass is open (#368).

The pass-lock, `<sidecar>.pass.lock`: `Session.redact()` and
`Session.ingest()` hold it `LOCK_SH` for the whole pass, and
`Session.compact()` takes `LOCK_EX|LOCK_NB` under the gate and refuses
while any pass holds it. Two observable behaviours of frozen public
methods, which #379 records as contract:

1. `compact()` raises `RuntimeError` while a `redact()` or `ingest()`
   pass is open on the same store, from any thread of this session.
2. `redact()` and `ingest()` block, bounded by `_SIDECAR_GATE_TIMEOUT_S`,
   while a `compact()` is rewriting, and then proceed.

**Why a refusal and not a smarter predicate.** Reproduced on 0.9.3
through the front door (2026-09-08 spec §4.1): with every worker's
outcome materialised and the parent parked before applying them,
`session.compact()` on another thread *returned success* and deleted
the three redacted rows at offsets 120/143/166 -- because each worker
had committed its blob row under a regenerated UID that no `instances`
row named yet, and compaction's orphan predicate is *correct* to reclaim
a row nothing references. The graph carried references the store had
not been told about; the fix is to keep compaction out until it has,
not to teach the predicate a second answer to "what is live". The same
lock closes the loader-offset span (§7.2: a row moved to 8214 under a
loader the parent later bound at 12321) and the ingest variant, where
blob rows are written before the `instances` row exists.

**Executor-independent.** The refusal is a kernel fact about two fds on
one file, so these tests run the same on the processes path (3.12) and
the threads path (3.14t). What differs is only where the pass's
mutations sit while parked -- on copies or on the live objects -- and
neither changes what `compact()` may do.

**Seams.** Test 1 parks `_apply_redaction_outcomes` after materialising
its outcomes (a `staticmethod`, so the patch is installed as one). Test
2 parks the compaction inside `_rewrite_live_frames` and counts
dispatches on the *parent's* `run_parallel` -- under processes the child
unpickles the unpatched class, so a counter on the worker would read
zero for the wrong reason. Test 3 parks the ingest loop at the first
`Equipment.from_parts`, which runs after the result's frames are
appended (outside the gate, inside the pass) and before its `instances`
row can exist -- the exact window in which 0.9.3's compaction reclaimed
a freshly ingested frame.
"""
import os
import sqlite3
import threading

import numpy as np
import pytest
from pydicom.dataset import FileDataset, FileMetaDataset
from pydicom.uid import ExplicitVRLittleEndian, generate_uid

from isocenter import io_handlers as io_module
from isocenter import session as session_module
from isocenter.entities import Equipment, Instance, Patient, Series, Study
from isocenter.persistence import SqliteStore
from isocenter.session import DicomSession

CT_STORAGE = "1.2.840.10008.5.1.4.1.1.2"
SERIAL = "SN_PASS"
ROIS = [[0, 10, 0, 10]]
_WAIT = 60.0


def _populate(session, count=3):
    patient = Patient("P_PASS", "Pass Test")
    study = Study("S_PASS", "20230101")
    series = Series("SE_PASS", "OT", 1, Equipment("Isocenter", "PassTest", SERIAL))
    instances = []
    for i in range(count):
        inst = Instance(f"I_PASS_{i}", CT_STORAGE, i + 1)
        inst.set_pixel_data(np.full((64, 64), 200 + i, dtype=np.uint8))
        instances.append(inst)
        series.instances.append(inst)
    study.series.append(series)
    patient.studies.append(study)
    session.store.patients.append(patient)
    return instances


def _blob_rows(store):
    with sqlite3.connect(store.db_path) as conn:
        return set(conn.execute(
            "SELECT instance_uid, kind, offset, length FROM instance_blobs"
        ).fetchall())


@pytest.fixture
def redactable(tmp_path):
    """A file-backed session with three redactable instances, saved."""
    session = DicomSession(persistence_file=str(tmp_path / "pass.db"))
    try:
        instances = _populate(session)
        session.configuration.rules = [
            {"serial_number": SERIAL, "redaction_zones": ROIS}]
        session.save(sync=True)
        yield session, instances
    finally:
        session.close()


class _Helper(threading.Thread):
    """Runs `work` once `parked` is set; keeps its exception; then releases."""

    def __init__(self, parked, released, work):
        super().__init__(daemon=True)
        self.parked, self.released, self.work = parked, released, work
        self.error = None
        self.result = None

    def run(self):
        try:
            if not self.parked.wait(_WAIT):
                self.error = AssertionError("the pass never parked")
                return
            self.result = self.work()
        except Exception as exc:      # pylint: disable=broad-except
            self.error = exc
        finally:
            self.released.set()


def _park_outcomes(monkeypatch, parked, released):
    """Materialise every worker's outcome, then park before applying any."""
    original = DicomSession._apply_redaction_outcomes

    def parked_apply(outcomes, instances, store_backend=None, passes=None):
        outcomes = list(outcomes)
        parked.set()
        assert released.wait(_WAIT), "the helper never released the pass"
        return original(outcomes, instances, store_backend, passes)

    monkeypatch.setattr(DicomSession, "_apply_redaction_outcomes",
                        staticmethod(parked_apply))


def test_compact_during_redact_raises_and_reclaims_nothing(
        redactable, monkeypatch):
    session, instances = redactable
    store = session.store_backend
    parked, released = threading.Event(), threading.Event()
    _park_outcomes(monkeypatch, parked, released)

    seen = {}

    def work():
        seen["rows_before"] = _blob_rows(store)
        session.compact()

    helper = _Helper(parked, released, work)
    helper.start()
    uids_before = [inst.sop_instance_uid for inst in instances]
    applied = session.redact(show_progress=False)
    helper.join(timeout=_WAIT)
    assert not helper.is_alive(), "the helper thread did not finish"

    assert isinstance(helper.error, RuntimeError), (
        "compact() during a redact() pass gave %r; it must refuse with "
        "RuntimeError naming the pass-lock, because every worker has "
        "committed a blob row under a UID no instances row names yet and "
        "the orphan predicate would reclaim all of them (#368)"
        % (helper.error,))
    assert store._pass_lock_path() in str(helper.error), (
        "the refusal does not name the pass-lock file: %s" % helper.error)
    assert "compact() refused" in str(helper.error)

    rows_after = _blob_rows(store)
    missing = seen["rows_before"] - rows_after
    assert not missing, (
        "compact() reclaimed rows that existed while the pass was open: "
        "%s (#368)" % (sorted(missing),))

    assert applied == len(instances)
    for inst, before in zip(instances, uids_before):
        assert inst.sop_instance_uid != before, (
            "a redacted instance kept its pre-redaction UID; the parked "
            "outcomes were not applied after the release")
        inst.discard_pixel_data()
        arr = inst.get_pixel_data()
        assert arr is not None and arr[0, 0] == 0, (
            "a redacted instance does not read back its redacted frame "
            "after a refused compaction (#368)")


def test_redact_during_compact_waits_then_proceeds(redactable, monkeypatch):
    session, instances = redactable
    store = session.store_backend
    parked, released = threading.Event(), threading.Event()

    original_rewrite = SqliteStore._rewrite_live_frames

    def parked_rewrite(self, *args, **kwargs):
        parked.set()
        assert released.wait(_WAIT), "the test never released the compaction"
        return original_rewrite(self, *args, **kwargs)

    monkeypatch.setattr(SqliteStore, "_rewrite_live_frames", parked_rewrite)

    dispatches = []
    real_run_parallel = session_module.run_parallel

    def counting_run_parallel(*args, **kwargs):
        dispatches.append(True)
        return real_run_parallel(*args, **kwargs)

    monkeypatch.setattr(session_module, "run_parallel", counting_run_parallel)

    compaction = {}

    def compact_work():
        try:
            session.compact()
            compaction["error"] = None
        except Exception as exc:      # pylint: disable=broad-except
            compaction["error"] = exc

    compact_thread = threading.Thread(target=compact_work, daemon=True)
    compact_thread.start()
    assert parked.wait(_WAIT), "the compaction never parked"

    redaction = {}

    def redact_work():
        try:
            redaction["applied"] = session.redact(show_progress=False)
        except Exception as exc:      # pylint: disable=broad-except
            redaction["error"] = exc

    redact_thread = threading.Thread(target=redact_work, daemon=True)
    redact_thread.start()
    # Long enough for an ungated redact() to have dispatched and
    # finished three tiny redactions; short enough not to matter.
    redact_thread.join(timeout=1.0)
    dispatched_during = len(dispatches)
    still_waiting = redact_thread.is_alive()

    released.set()
    compact_thread.join(timeout=_WAIT)
    redact_thread.join(timeout=_WAIT)
    assert not compact_thread.is_alive() and not redact_thread.is_alive()

    assert compaction["error"] is None, compaction["error"]
    assert "error" not in redaction, redaction.get("error")
    assert dispatched_during == 0 and still_waiting, (
        "redact() dispatched %d time(s) while the compaction was parked; "
        "a pass starting mid-compaction must wait at the pass-lock before "
        "any worker runs, or its rows are reclaimed by the rewrite (#368)"
        % dispatched_during)
    assert redaction["applied"] == len(instances)
    for inst in instances:
        inst.discard_pixel_data()
        arr = inst.get_pixel_data()
        assert arr is not None and arr[0, 0] == 0


def _write_ct(folder, name, patient="P_ING"):
    meta = FileMetaDataset()
    meta.MediaStorageSOPClassUID = CT_STORAGE
    meta.MediaStorageSOPInstanceUID = generate_uid()
    meta.TransferSyntaxUID = ExplicitVRLittleEndian
    ds = FileDataset(None, {}, file_meta=meta, preamble=b"\0" * 128)
    ds.PatientID, ds.PatientName = patient, "DOE^ING"
    ds.StudyInstanceUID, ds.SeriesInstanceUID = generate_uid(), generate_uid()
    ds.SOPInstanceUID = meta.MediaStorageSOPInstanceUID
    ds.SOPClassUID = CT_STORAGE
    ds.Modality, ds.SeriesNumber, ds.InstanceNumber = "CT", 1, 1
    ds.StudyDate = "20230101"
    ds.DeviceSerialNumber = "SN_ING"
    ds.Manufacturer, ds.ManufacturerModelName = "ACME", "SCAN"
    ds.Rows = ds.Columns = 32
    ds.BitsAllocated = ds.BitsStored = 8
    ds.HighBit = 7
    ds.SamplesPerPixel = 1
    ds.PhotometricInterpretation = "MONOCHROME2"
    ds.PixelRepresentation = 0
    ds.PixelData = np.random.default_rng(len(name)).integers(
        0, 256, (32, 32), dtype=np.uint8).tobytes()
    path = os.path.join(folder, name)
    ds.save_as(path, enforce_file_format=True)
    return path


def test_compact_during_ingest_raises(tmp_path, monkeypatch):
    """Owner's option D1: `ingest()` holds the pass-lock too.

    The park is at the first `Equipment.from_parts`, reached while
    linking the first result: its pixel frame is already appended (site
    1, gate released) and no `instances` row names it yet -- so on
    0.9.3 a compaction here did not copy the frame into the rewritten
    file, and the loader the ingest had already bound read past the
    end of it.
    """
    src = tmp_path / "src"
    src.mkdir()
    # Two files with distinct study/series UIDs, so two series are
    # created and the park at the first `from_parts` is not also the
    # last thing the loop does.
    _write_ct(str(src), "a.dcm")
    _write_ct(str(src), "b.dcm")
    session = DicomSession(persistence_file=str(tmp_path / "ingest_pass.db"))
    try:
        store = session.store_backend
        parked, released = threading.Event(), threading.Event()
        real_from_parts = Equipment.from_parts
        calls = []

        def parking_from_parts(*args, **kwargs):
            calls.append(True)
            if len(calls) == 1:
                parked.set()
                assert released.wait(_WAIT), "the helper never released"
            return real_from_parts(*args, **kwargs)

        monkeypatch.setattr(Equipment, "from_parts", parking_from_parts)

        helper = _Helper(parked, released, session.compact)
        helper.start()
        summary = session.ingest(str(src))
        helper.join(timeout=_WAIT)
        assert not helper.is_alive()

        assert isinstance(helper.error, RuntimeError), (
            "compact() during an ingest() pass gave %r; it must refuse "
            "with RuntimeError naming the pass-lock: ingest appends frames "
            "before the instances rows that reference them exist (#368)"
            % (helper.error,))
        assert store._pass_lock_path() in str(helper.error)

        assert summary.failed == 0, summary.failures
        assert summary.ingested == 2
        loaded = [inst for p in session.store.patients for st in p.studies
                  for se in st.series for inst in se.instances]
        assert len(loaded) == 2
        for inst in loaded:
            inst.discard_pixel_data()
            arr = inst.get_pixel_data()
            assert arr is not None and arr.shape == (32, 32), (
                "an ingested instance does not read back after a compaction "
                "ran during its ingest (#368)")
    finally:
        session.close()
