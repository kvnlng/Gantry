"""compact() refuses while a redact() or ingest() pass is open (#368).

The pass-lock, `<sidecar>.pass.lock`: `Session.redact()` and
`Session.ingest()` hold it `LOCK_SH` for the whole pass, and
`Session.compact()` takes `LOCK_EX|LOCK_NB` first -- before its leading
save, holding nothing -- and refuses while any pass holds it, holding it
through the rewrite and the rewire otherwise. Two observable behaviours
of frozen public methods, which #379 records as contract:

1. `compact()` raises `RuntimeError` while a `redact()` or `ingest()`
   pass is open on the same store, from any thread of this session, and
   has done nothing -- no save, no rewrite -- when it does.
2. `redact()` and `ingest()` block, bounded by `_SIDECAR_GATE_TIMEOUT_S`,
   while a `compact()` is saving or rewriting, and then proceed.

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

**Executor-independent, and how that was learned.** The refusal is a
kernel fact about two fds on one file, so these tests run the same on
the processes path (3.12) and the threads path (3.14t). What differs is
where the pass's mutations sit while parked: on copies under processes,
on the *live* instances under threads (the worker is handed
`task['instance']` itself, and its `regenerate_uid()` moves the live
attribute). While the refusal sat *after* `compact()`'s leading save,
that difference reached the store: under threads the save wrote the
three live instances under their regenerated UIDs and retired the
pre-redaction rows, so test 1's "every row present before the park is
present after" was red on 3.14.7t and had to be narrowed (spec §15
item 8). The review of PR #385 then showed the save was the defect,
not a side effect: a pass that opened and closed *inside* it was
admitted (§15 item 9, and the fourth test below). With the refusal
before the save, a refused `compact()` does nothing, and test 1 pins
the whole pre-park row set and the sidecar's inode again, on both
paths.

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


def _sidecar_identity(store):
    """(inode, size): compaction's `os.replace` changes the first."""
    st = os.stat(store.sidecar_path)
    return st.st_ino, st.st_size


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
        seen["sidecar_before"] = _sidecar_identity(store)
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

    # Non-vacuity: the workers had written one row per instance under a
    # regenerated UID before the park -- the rows 0.9.3 reclaimed.
    worker_rows = {r for r in seen["rows_before"] if r[0] not in uids_before}
    assert len(worker_rows) == len(instances), (
        "expected one worker-written blob row per instance while parked, "
        "saw %s" % (sorted(worker_rows),))
    # The whole pre-park row set, every key. A refused compact() refuses
    # before its leading save, so it writes nothing at all: no row is
    # reclaimed, none moved, none retired. (The narrower form this held
    # while the refusal sat after the save is spec §15 item 8.)
    rows_after = _blob_rows(store)
    assert rows_after == seen["rows_before"], (
        "a refused compact() changed the blob rows: gone %s, new %s (#368)"
        % (sorted(seen["rows_before"] - rows_after),
           sorted(rows_after - seen["rows_before"])))
    assert _sidecar_identity(store) == seen["sidecar_before"], (
        "a refused compact() rewrote the sidecar anyway (inode or size "
        "changed); the refusal must sit before compact_sidecar() (#368)")

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


def test_a_pass_that_opens_and_closes_inside_the_leading_save_is_kept_out(
        redactable, monkeypatch):
    """The window the review of PR #385 found (#368).

    `compact()` leads with `save(sync=True)`. With the refusal *after*
    that save, a pass that opens once `save_all` has returned and
    closes before the `LOCK_EX|LOCK_NB` attempt is admitted: `redact()`
    does not save at its end, so at that instant the `instances` rows
    are on the old UIDs, the blob rows on the regenerated ones, and the
    graph on the new ones. `_read_blob_index`'s EXISTS predicate then
    reclaims every worker frame; the next save re-emits the stale loader
    offsets as durable rows past the end of the file, and the next
    compaction erases the originals. Reproduced by the reviewer on
    3.12.13 and 3.14.7t: compact success, sidecar 84 -> 84, all three
    readbacks raising Integrity Error, the next save writing rows at
    84/121/158 into an 84-byte file.

    The park is *after* `save_all` returns, on `compact()`'s own call
    only (the redaction thread's saves pass straight through), and the
    whole `redact()` runs during the park. With the pass-lock taken
    before the leading save the pass waits at its `LOCK_SH` until the
    rewire is done, then appends its frames to the compacted file.
    With the refusal back under the gate, `redact()` completes inside
    the park and the readback assertion is what goes red.
    """
    session, instances = redactable
    store = session.store_backend
    parked, released = threading.Event(), threading.Event()
    compact_thread = threading.current_thread()
    real_save_all = SqliteStore.save_all
    parked_once = []

    def parking_save_all(self, *args, **kwargs):
        result = real_save_all(self, *args, **kwargs)
        if threading.current_thread() is compact_thread and not parked_once:
            parked_once.append(True)
            parked.set()
            assert released.wait(_WAIT), "the test never released the save"
        return result

    monkeypatch.setattr(SqliteStore, "save_all", parking_save_all)

    redaction, timing = {}, {}

    def redact_work():
        try:
            redaction["applied"] = session.redact(show_progress=False)
        except Exception as exc:      # pylint: disable=broad-except
            redaction["error"] = exc

    redact_thread = threading.Thread(target=redact_work, daemon=True)

    def run_the_pass_inside_the_park():
        if not parked.wait(_WAIT):
            timing["error"] = "compact()'s leading save never parked"
            released.set()
            return
        redact_thread.start()
        # Long enough for an admitted redact() to finish three tiny
        # redactions inside the park; a kept-out one is still waiting
        # at its LOCK_SH when this expires.
        redact_thread.join(timeout=1.0)
        timing["finished_inside_the_park"] = not redact_thread.is_alive()
        released.set()

    driver = threading.Thread(target=run_the_pass_inside_the_park, daemon=True)
    driver.start()
    size_before = os.path.getsize(store.sidecar_path)
    session.compact()
    driver.join(timeout=_WAIT)
    redact_thread.join(timeout=_WAIT)
    assert "error" not in timing, timing.get("error")
    assert not redact_thread.is_alive(), "redact() never returned"
    assert "error" not in redaction, redaction.get("error")
    assert redaction["applied"] == len(instances)

    for inst in instances:
        inst.discard_pixel_data()
        try:
            arr = inst.get_pixel_data()
        except RuntimeError as exc:
            pytest.fail(
                "a redacted instance does not read back after a compaction "
                "admitted a pass that opened and closed inside its leading "
                "save: %s (#368)" % exc)
        assert arr is not None and arr[0, 0] == 0
    assert not timing["finished_inside_the_park"], (
        "redact() ran to completion inside compact()'s leading save; the "
        "pass-lock must be taken before that save, or a pass that opens "
        "and closes inside it is admitted with its rows unnamed (#368)")
    assert os.path.getsize(store.sidecar_path) > size_before, (
        "the redacted frames did not land after the compaction; the pass "
        "was not kept waiting (#368)")


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
