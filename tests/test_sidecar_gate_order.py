"""The sidecar gate's lock order, and the six sites that must hold it (#368).

The gate (`SqliteStore._hold_sidecar_gate`: a `threading.Lock`, then
`fcntl.flock(LOCK_EX)` on `<sidecar>.lock`) is the one lock in this
codebase deliberately held across a sqlite write, and it sits *above*
`_pixel_swap_lock`. The order is an internal invariant -- every name is
private and lives in implementation files (2026-09-08 spec §3) -- so it
is pinned here by recording wrappers rather than frozen in the API:

    pass-lock EX|NB (holding nothing)  ->  _sidecar_gate   (compact, first)
    _sidecar_gate  ->  _pixel_swap_lock          (compact's rewire; sites 5, 6)
    _sidecar_gate  ->  sqlite                    (every site's row commit)

Reversing the second arm is the cycle the 2026-09-07 spec measured:
`_persist_pixels` calls `write_frame` under `_pixel_swap_lock`, and
`_rewire_sidecar_loaders` takes `_pixel_swap_lock` under the gate -- a
gate taken inside `write_frame` deadlocks against the rewire. Taking the
gate before `compact()`'s leading `save(sync=True)` deadlocks site 6
against itself. The pass-lock is taken exclusive *before* that save,
holding nothing (the review of PR #385 found that an attempt made after
the save admits a pass that opened and closed inside it), and shared
holding nothing by `redact()`/`ingest()`; nothing takes it under the
gate. The EX attempt is `LOCK_NB` because the refusal is an answer and
not a wait.

**What is recorded.** `_sidecar_gate` and `_pixel_swap_lock` on the
store are replaced with proxies that log every acquire and release with
the set of these locks the acquiring thread already holds; `fcntl.flock`
is wrapped to log the inode and flags of every lock call with the same
held-set; `SqliteStore.save_all` and `SidecarManager.write_frame` are
wrapped to log entry and exit. One session then runs every writer once:
`save(sync=True)` (site 6), `persist_pixel_data` (site 5),
`persist_blob` (site 4), an `ingest()` of a pixel file carrying an icon
and a waveform file (sites 1-3), and `compact()`.

**The seventh-site detector** is the last test: an AST walk of
`isocenter/` collects every `write_frame` call by enclosing function and
must equal the six the gate covers. A seventh `write_frame` anywhere is
red before anyone asks whether it is gated -- the gate is never inside
`write_frame` itself (`SidecarManager` is stateless by #366's design and
is called directly by fixture generators), so a new site is gated only
if its author gates it, and this is what makes forgetting loud.
"""
import ast
import collections
import fcntl
import os
import pathlib
import sys
import threading

import numpy as np
import pytest
from pydicom.dataset import Dataset, FileDataset, FileMetaDataset
from pydicom.sequence import Sequence
from pydicom.uid import ExplicitVRLittleEndian, generate_uid

from isocenter import persistence as persistence_module
from isocenter.entities import Equipment, Instance, Patient, Series, Study
from isocenter.persistence import SqliteStore
from isocenter.session import DicomSession
from isocenter.sidecar import SidecarManager
from scripts.generate_waveform_test_data import write_fixture

REPO = pathlib.Path(__file__).resolve().parent.parent
CT_IMAGE = "1.2.840.10008.5.1.4.1.1.2"

#: The six `write_frame` sites, by (module, enclosing function) and how
#: many calls each holds. The gate-order test asserts every one of these
#: functions wrote a frame with the gate held; the seventh-site detector
#: asserts the AST holds exactly this multiset. Keyed on function names
#: rather than line numbers so it is green through unrelated edits and
#: red on a new site wherever it lands.
_WRITE_FRAME_SITES = {
    ("isocenter/io_handlers.py", "import_files"): 3,   # pixel, nested, waveform
    ("isocenter/persistence.py", "persist_blob"): 1,
    # `persist_pixel_data`'s body: the public method takes the gate and
    # this helper does the swap under it.
    ("isocenter/persistence.py", "_swap_pixels_under_gate"): 1,
    ("isocenter/persistence.py", "_persist_pixels"): 1,
}
_SITE_FUNCTIONS = {name for _file, name in _WRITE_FRAME_SITES}


class _Recorder:
    """One log shared by every wrapper, plus per-thread held-lock sets."""

    def __init__(self):
        self.log = []
        self._local = threading.local()
        self._guard = threading.Lock()

    def held(self):
        return getattr(self._local, "held", frozenset())

    def _set_held(self, held):
        self._local.held = frozenset(held)

    def record(self, event, **fields):
        with self._guard:
            self.log.append(dict(event=event, thread=threading.get_ident(),
                                 held=self.held(), **fields))

    def took(self, name):
        self._set_held(self.held() | {name})

    def dropped(self, name):
        self._set_held(self.held() - {name})


def _sites_on_stack():
    """Which of the six site functions are on the calling thread's stack."""
    names = set()
    frame = sys._getframe(1)
    while frame is not None:
        if frame.f_code.co_name in _SITE_FUNCTIONS:
            names.add(frame.f_code.co_name)
        frame = frame.f_back
    return frozenset(names)


class _RecordingLock:
    """A `threading.Lock` stand-in that logs acquire/release with context.

    Exposes exactly what the store uses: `acquire(timeout=)`, `release()`
    and the context-manager protocol. Replaces the store's lock *object*
    because a `threading.Lock`'s methods cannot be wrapped in place.
    """

    def __init__(self, name, recorder):
        self.name, self.recorder = name, recorder
        self._lock = threading.Lock()

    def acquire(self, blocking=True, timeout=-1):
        ok = self._lock.acquire(blocking, timeout)
        if ok:
            self.recorder.record("acquire", lock=self.name,
                                 sites=_sites_on_stack())
            self.recorder.took(self.name)
        return ok

    def release(self):
        self.recorder.dropped(self.name)
        self.recorder.record("release", lock=self.name)
        self._lock.release()

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, *exc):
        self.release()
        return False


def _instrument(session, monkeypatch):
    """Install every wrapper on one session's store and return the recorder."""
    recorder = _Recorder()
    store = session.store_backend
    store._sidecar_gate = _RecordingLock("gate", recorder)
    store._pixel_swap_lock = _RecordingLock("swap", recorder)

    real_flock = fcntl.flock

    def recording_flock(fd, flags):
        fileno = fd if isinstance(fd, int) else fd.fileno()
        real_flock(fd, flags)
        recorder.record("flock", inode=os.fstat(fileno).st_ino, flags=flags)

    monkeypatch.setattr(fcntl, "flock", recording_flock)

    real_save_all = SqliteStore.save_all

    def recording_save_all(self, *args, **kwargs):
        recorder.record("save_all_enter")
        try:
            return real_save_all(self, *args, **kwargs)
        finally:
            recorder.record("save_all_exit")

    monkeypatch.setattr(SqliteStore, "save_all", recording_save_all)

    real_write = SidecarManager.write_frame

    def recording_write(self, data, compression='zlib'):
        recorder.record("write_frame", sites=_sites_on_stack())
        return real_write(self, data, compression)

    monkeypatch.setattr(SidecarManager, "write_frame", recording_write)
    return recorder


def _write_ct_with_icon(folder):
    """A CT file with a top-level frame and one decodable icon (sites 1, 2)."""
    meta = FileMetaDataset()
    meta.MediaStorageSOPClassUID = CT_IMAGE
    meta.MediaStorageSOPInstanceUID = generate_uid()
    meta.TransferSyntaxUID = ExplicitVRLittleEndian
    ds = FileDataset(None, {}, file_meta=meta, preamble=b"\0" * 128)
    ds.PatientID, ds.PatientName = "PAT_GATE", "DOE^GATE"
    ds.StudyInstanceUID, ds.SeriesInstanceUID = generate_uid(), generate_uid()
    ds.SOPInstanceUID = meta.MediaStorageSOPInstanceUID
    ds.SOPClassUID = CT_IMAGE
    ds.Modality, ds.SeriesNumber, ds.InstanceNumber = "CT", 1, 1
    ds.StudyDate = "20230101"
    ds.Rows = ds.Columns = 4
    ds.BitsAllocated = ds.BitsStored = 8
    ds.HighBit = 7
    ds.SamplesPerPixel = 1
    ds.PhotometricInterpretation = "MONOCHROME2"
    ds.PixelRepresentation = 0
    ds.PixelData = np.arange(16, dtype=np.uint8).tobytes()
    icon = Dataset()
    icon.Rows = icon.Columns = 2
    icon.BitsAllocated = icon.BitsStored = 8
    icon.HighBit = 7
    icon.SamplesPerPixel = 1
    icon.PhotometricInterpretation = "MONOCHROME2"
    icon.PixelRepresentation = 0
    icon.add_new(0x7FE00010, 'OB', bytes([11, 22, 33, 44]))
    ds.IconImageSequence = Sequence([icon])
    path = os.path.join(folder, "ct_icon.dcm")
    ds.save_as(path, enforce_file_format=True)
    return path


def _inode_or_none(path):
    try:
        return os.stat(path).st_ino
    except FileNotFoundError:
        return None


def _make_instance(uid):
    inst = Instance(uid, CT_IMAGE, 1, file_path=None)
    inst.set_attr("0028,0010", 8)
    inst.set_attr("0028,0011", 8)
    inst.set_attr("0028,0002", 1)
    inst.set_attr("0028,0100", 8)
    inst.set_attr("0028,0103", 0)
    inst.set_pixel_data(np.arange(64, dtype=np.uint8).reshape(8, 8))
    return inst


@pytest.fixture
def recorded(tmp_path, monkeypatch):
    """Every writer once, under instrumentation; yields the recorder."""
    # Nothing here contends for the gate, so 5 s is generous -- and it
    # is what makes an order violation *fail* instead of stall: a
    # `compact()` that took the gate before its leading `save(sync=True)`
    # would deadlock site 6 against itself for the full deadline.
    monkeypatch.setattr(persistence_module, "_SIDECAR_GATE_TIMEOUT_S", 5.0)
    session = DicomSession(persistence_file=str(tmp_path / "order.db"))
    try:
        patient = Patient("P_ORDER", "Order Test")
        study = Study("S_ORDER", "20230101")
        series = Series("SE_ORDER", "CT", 1,
                        Equipment("ACME", "SCAN", "SN_ORDER"))
        patient.studies.append(study)
        study.series.append(series)
        session.store.patients.append(patient)
        first, second = _make_instance("1.2.3.A"), _make_instance("1.2.3.B")
        series.instances.extend([first, second])

        recorder = _instrument(session, monkeypatch)

        session.save(sync=True)                                   # site 6
        first.set_pixel_data(np.full((8, 8), 9, dtype=np.uint8))
        session.store_backend.persist_pixel_data(first)           # site 5
        session.store_backend.persist_blob(
            second, 'waveform', np.arange(64, dtype=np.int16))    # site 4
        src = tmp_path / "src"
        src.mkdir()
        _write_ct_with_icon(str(src))
        write_fixture(str(src / "ecg.dcm"), num_samples=64)
        summary = session.ingest(str(src))                        # sites 1-3
        assert summary.failed == 0, summary.failures
        session.compact()

        # The lock files are created by their first acquisition. A path
        # nothing has taken has no inode to match, and `None` matches
        # no recorded call -- so a missing pass-lock reads as "compact()
        # never attempted the pass-lock" in the test that asks, rather
        # than as a fixture error that hides the other three.
        recorder.pass_lock_inode = _inode_or_none(
            session.store_backend._pass_lock_path())
        recorder.gate_inode = _inode_or_none(
            session.store_backend._gate_path())
        yield recorder
    finally:
        session.close()


def test_the_gate_is_never_taken_by_a_thread_holding_the_swap_lock(recorded):
    """`_sidecar_gate -> _pixel_swap_lock`, never the reverse (#368)."""
    gate_acquires = [e for e in recorded.log
                     if e["event"] == "acquire" and e["lock"] == "gate"]
    assert gate_acquires, "the gate was never acquired; nothing was measured"
    violations = [e for e in gate_acquires if "swap" in e["held"]]
    assert not violations, (
        "the gate was acquired by a thread already holding _pixel_swap_lock "
        "(sites on the stack: %s). That is the reverse of the order the "
        "rewire needs and deadlocks against it (#368)"
        % ([sorted(e["sites"]) for e in violations],))
    # And the forward arm is exercised, not merely un-violated.
    swap_under_gate = [e for e in recorded.log
                       if e["event"] == "acquire" and e["lock"] == "swap"
                       and "gate" in e["held"]]
    assert swap_under_gate, (
        "_pixel_swap_lock was never taken under the gate; the rewire and "
        "sites 5/6 are expected to do exactly that")


def test_every_write_frame_call_happens_with_the_gate_held(recorded):
    """Six sites, every call gated -- by the *calling* thread (#368).

    The gate is a threading.Lock in front of a flock, so "held" here
    means held by the thread that is writing, which is the only thing
    that stops that thread's frame landing in a file `compact()` is
    replacing.
    """
    writes = [e for e in recorded.log if e["event"] == "write_frame"]
    assert writes, "no frame was written; nothing was measured"
    ungated = [sorted(e["sites"]) for e in writes if "gate" not in e["held"]]
    assert not ungated, (
        "write_frame was called without the gate held, from %s (#368)"
        % (ungated,))
    covered = set().union(*(e["sites"] for e in writes))
    assert covered == _SITE_FUNCTIONS, (
        "these site functions never wrote a frame under this fixture, so "
        "their gating was not measured: %s" % (
            sorted(_SITE_FUNCTIONS - covered),))
    from_ingest = [e for e in writes if "import_files" in e["sites"]]
    assert len(from_ingest) >= 3, (
        "the ingest fixture reached %d of import_files' three sites "
        "(pixel, nested icon, waveform)" % len(from_ingest))


def test_compact_takes_the_pass_lock_before_its_leading_save_holding_nothing(
        recorded):
    """pass-lock `LOCK_EX|LOCK_NB`, holding nothing -> `save_all` -> gate (#368).

    The EX attempt is the first thing `compact()` does, before its
    leading `save(sync=True)`: a pass that opens after that save's rows
    are written and closes before the rewrite would otherwise be
    admitted with its `instances` rows on the old UIDs and its blob rows
    on the new ones, and the rewrite reclaims every worker frame (the
    window the review of PR #385 reproduced). Holding nothing, so the
    order stays acyclic -- `redact()`/`ingest()` take their SH holding
    nothing, and no site takes the pass-lock under the gate any more.
    `LOCK_NB` still: the refusal is an answer, not a wait.
    """
    log = recorded.log
    attempts = [i for i, e in enumerate(log)
                if e["event"] == "flock"
                and e.get("inode") == recorded.pass_lock_inode
                and e["flags"] & fcntl.LOCK_EX]
    assert attempts, "compact() never attempted the pass-lock"
    for i in attempts:
        attempt = log[i]
        assert not attempt["held"], (
            "the pass-lock EX attempt was made holding %s; compact() must "
            "take it before its leading save, holding nothing (#368)"
            % (sorted(attempt["held"]),))
        assert attempt["flags"] & fcntl.LOCK_NB, (
            "the pass-lock EX attempt is blocking; it must be LOCK_NB, the "
            "refusal is an answer and not a wait (#368)")
        after = log[i + 1:]
        save_enter = next((j for j, e in enumerate(after)
                           if e["event"] == "save_all_enter"), None)
        gate_take = next((j for j, e in enumerate(after)
                          if e["event"] == "acquire" and e["lock"] == "gate"),
                         None)
        assert save_enter is not None and gate_take is not None, (
            "after the pass-lock EX attempt compact() must run its leading "
            "save and then take the gate; saw save_all_enter=%r, gate=%r"
            % (save_enter, gate_take))
        assert save_enter < gate_take, (
            "compact() took the gate before its leading save_all; the EX "
            "attempt precedes the save, and the gate follows it (#368)")
    shared = [e for e in recorded.log
              if e["event"] == "flock"
              and e.get("inode") == recorded.pass_lock_inode
              and e["flags"] & fcntl.LOCK_SH]
    assert shared, "ingest() never took the pass-lock shared"
    for hold in shared:
        assert not hold["held"], (
            "the pass-lock was taken shared while holding %s; a pass "
            "opens holding nothing, or the order has a cycle (#368)"
            % (sorted(hold["held"]),))


def test_compact_takes_the_gate_only_after_its_leading_save_returns(recorded):
    """`save(sync=True)` runs site 6 on this thread; the gate must be free.

    A `compact()` that took the gate first would deadlock its own leading
    save on site 6 -- bounded by `_SIDECAR_GATE_TIMEOUT_S`, so a stall
    rather than a hang, but a stall on every compaction.
    """
    entries = [e for e in recorded.log if e["event"] == "save_all_enter"]
    assert entries, "save_all never ran"
    held_at_entry = [sorted(e["held"]) for e in entries if e["held"]]
    assert not held_at_entry, (
        "save_all was entered with %s held; compact() must take the gate "
        "only after its leading save(sync=True) has returned (#368)"
        % (held_at_entry,))


def _write_frame_sites():
    """Every `write_frame` call under isocenter/, by (file, enclosing def)."""
    found = collections.Counter()
    for path in sorted((REPO / "isocenter").rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        parents = {}
        for node in ast.walk(tree):
            for child in ast.iter_child_nodes(node):
                parents[child] = node
        for node in ast.walk(tree):
            if not (isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Attribute)
                    and node.func.attr == "write_frame"):
                continue
            scope = parents.get(node)
            while scope is not None and not isinstance(
                    scope, (ast.FunctionDef, ast.AsyncFunctionDef)):
                scope = parents.get(scope)
            found[(path.relative_to(REPO).as_posix(),
                   scope.name if scope is not None else "<module>")] += 1
    return dict(found)


def test_there_are_exactly_six_write_frame_sites_and_they_are_these():
    """The seventh-site detector (#368).

    Red before anyone asks whether a new site is gated. If this fails
    because you added a `write_frame` call: gate it (append *and* row
    commit, outside `_pixel_swap_lock`, never inside `write_frame`), add
    it to the fixture above so the gating is measured, and then add it
    to `_WRITE_FRAME_SITES`.
    """
    assert _write_frame_sites() == _WRITE_FRAME_SITES
