"""compact() and a concurrent sidecar write are serialised by the gate (#368).

`session.compact()` saves, refuses if the persistence manager still has
work, and then rewrites the sidecar. Until #368 a write that *started
after* that refusal ran concurrently with the rewrite, appending frames
to a file compaction was in the middle of replacing. This file was the
characterization of that window (#320): six orderings, each ending in
a loud integrity failure at the next read, accepted for 0.9.3 with the
correctly-scoped fix filed as #368. The owner ruled on 2026-09-08 that
the 1.0 tag waits on #368, and the gate is now built:
`SqliteStore._hold_sidecar_gate()` -- a `threading.Lock` for in-process
fairness followed by `fcntl.flock(LOCK_EX)` on the stable path
`<sidecar>.lock` -- held by every one of the **six** `write_frame` sites
across append *and* row commit, and by `compact()` across the rewrite
*and* the loader rewire.

**Every phase now asserts the opposite of what it characterized**: the
intruder's write is *blocked* until the compaction has returned, its
row survives `_apply_new_offsets` untouched, the row sits inside the
post-compaction file, and the instance reads back the *new* pixels.
Phase B, which used to see `FileNotFoundError` at the writer between
the two renames, sees nothing of the kind: the writer never reaches the
path while it names nothing.

**The scaffolding changed, and had to (2026-09-08 spec §10).** #320's
`_park_phase` parked `compact_sidecar()` *with the gate held*, and its
intruder released the park in its own `finally` -- after a write that,
under the gate, cannot return until the park is released. Measured with
a simulated gate: the intruder sat on the gate for the whole window and
the parked phase's `released.wait` returned `False`. So the park is
released by a **timer thread** instead, `_PARK_S` after the intruder
signals (`waiting`) that it is about to write. The timer records
`t_release` immediately before releasing, the intruder records
`t_write_returned` immediately after its write returns, and the
load-bearing assertion is `t_write_returned > t_release`: without the
gate the write completes in milliseconds, long before the timer fires;
with it the write cannot return until the compaction has released the
gate, which is after the park. The timer waits on `waiting` *before*
its delay, so the intruder is blocked on the gate before the park is
released and the "landed after" assertion is never vacuous.

**Deadline.** The gate is bounded by `_SIDECAR_GATE_TIMEOUT_S`, polled
`LOCK_NB`, and expiry raises `RuntimeError` naming the lock file and
the constant; the last test pins that with the constant patched to
0.5 s and the park held longer.

**`has_pending_saves()` is still `False` inside every window**, and each
phase still asserts it: the #295 refusal reads the persistence manager's
queue and in-flight set, and neither a caller-thread `save(sync=True)`
nor a redaction's `persist_pixel_data` enters either. That claim about
the #295 guard is unchanged by #368; what changed is that the population
it cannot see is now stopped by the gate instead.

**Style**: `tests/test_export_flushes_before_it_sweeps.py`. A helper
thread on a bounded daemon, the ordering forced by `threading.Event`s
from a monkeypatched phase method, and the helper's exception kept
rather than swallowed.

**Two traps that cost real time when the characterization was built.**
The new content must be produced *inside* the window: `compact()` leads
with `save(sync=True)`, so a `set_pixel_data` done before it is captured
by that save and the intruder appends a duplicate of a frame the index
already knows about. And the readback must go through the loader --
`persist_pixel_data` does not unload, so `get_pixel_data()` would return
the resident array and measure nothing. `discard_pixel_data()` first,
not `unload_pixel_data()`, which refuses a diverged array (#293).
"""
import os
import sqlite3
import threading
import time

import numpy as np
import pytest

from isocenter import persistence as persistence_module
from isocenter.entities import Instance, Patient, Series, Study
from isocenter.io_handlers import SidecarWaveformLoader
from isocenter.persistence import SqliteStore
from isocenter.session import DicomSession

CT_STORAGE = "1.2.840.10008.5.1.4.1.1.2"

#: Bounded, and generously so. A helper thread that waits forever is not
#: a test; a helper thread that times out on a loaded machine is a flake.
_WAIT = 60.0

#: How long the timer holds the park after the intruder says it is about
#: to write. Long enough that an ungated write (milliseconds) has landed
#: well before the release; short enough not to matter.
_PARK_S = 0.3

#: Big enough that compaction has real bytes to move and that a changed
#: frame compresses to a different length, small enough to stay quick.
_SHAPE = (128, 128)


def _frame(seed):
    """Incompressible bytes, so compaction has real work and two frames
    of the same shape do not land on identical lengths by accident."""
    return np.random.default_rng(seed).integers(
        0, 256, _SHAPE, dtype=np.uint8)


def _make_instance(uid, seed):
    """One instance with enough geometry for the loader to rebuild it."""
    inst = Instance(uid, CT_STORAGE, 1, file_path=None)
    inst.set_attr("0028,0010", _SHAPE[0])
    inst.set_attr("0028,0011", _SHAPE[1])
    inst.set_attr("0028,0002", 1)
    inst.set_attr("0028,0100", 8)
    inst.set_attr("0028,0103", 0)
    inst.set_pixel_data(_frame(seed))
    return inst


@pytest.fixture
def compactable(tmp_path, request):
    """A session whose sidecar has live frames and orphaned ones.

    Six instances persisted and saved, then three instance rows deleted
    so their frames become orphans. Without orphans `_rewrite_live_frames`
    has nothing to reclaim and the compaction is not the operation under
    test.
    """
    db = str(tmp_path / f"race320_{request.node.name}.db")
    session = DicomSession(persistence_file=db)

    patient = Patient("P320", "Race Test")
    study = Study("ST320", "20230101")
    series = Series("SE320", "CT", 1)
    patient.studies.append(study)
    study.series.append(series)
    session.store.patients.append(patient)

    live = [_make_instance(f"1.2.3.{i}", i + 1) for i in range(6)]
    series.instances.extend(live)
    for inst in live:
        session.store_backend.persist_pixel_data(inst)
    session.save(sync=True)

    # Orphan half of them. A blob is live only while its owning
    # `instances` row exists, so deleting the rows is what makes the
    # frames reclaimable -- and it leaves the in-memory graph alone,
    # which the intruder still needs.
    with session.store_backend._get_connection() as conn:
        for inst in live[3:]:
            conn.execute("DELETE FROM instances WHERE sop_instance_uid = ?",
                         (inst.sop_instance_uid,))

    try:
        yield session, series, live
    finally:
        session.close()


def _blob_row(session, uid, kind='pixels'):
    """The (offset, length, hash) `instance_blobs` records for a uid.

    The hash is read here rather than off the instance because that is
    where a *hydrated* loader gets it (`_hydrate_waveform_loader` reads
    `wref['hash']`), and because `persist_blob` does not write one onto
    the instance at all.
    """
    with sqlite3.connect(session.store_backend.db_path) as conn:
        row = conn.execute(
            "SELECT offset, length, hash FROM instance_blobs "
            "WHERE instance_uid = ? AND kind = ?", (uid, kind)).fetchone()
    return row


class _Intruder(threading.Thread):
    """Runs `work` once the compaction parks.

    Signals `done` when finished; it does **not** release the park --
    that is the timer's job, because under the gate the intruder's write
    cannot return until the park is released (the #320 scaffolding's
    deadlock, 2026-09-08 spec §10). The exception is kept rather than
    swallowed, so a failing write reads as a failed test rather than a
    green one over an unreported failure.
    """

    def __init__(self, parked, waiting, done, work):
        super().__init__(daemon=True)
        self.parked, self.waiting, self.done, self.work = (
            parked, waiting, done, work)
        self.error = None
        self.pending_during = None
        self.t_write_returned = None
        self.t_release = None

    def run(self):
        try:
            if not self.parked.wait(_WAIT):
                self.error = AssertionError("the compaction never parked")
                return
            self.work(self)
        except Exception as exc:      # pylint: disable=broad-except
            self.error = exc
        finally:
            self.done.set()

    def about_to_write(self):
        """Called by `work` immediately before the gated write."""
        self.waiting.set()

    def write_returned(self):
        """Called by `work` immediately after the gated write returns."""
        self.t_write_returned = time.monotonic()


class _Timer(threading.Thread):
    """Releases the park `_PARK_S` after the intruder is about to write.

    Waits on `waiting` first, bounded, so the release cannot race the
    intruder's *start*: the intruder is blocked on the gate before the
    park is lifted. If the intruder never signals (its work failed
    first), the bounded wait still releases the park so nothing hangs.
    """

    def __init__(self, waiting, released):
        super().__init__(daemon=True)
        self.waiting, self.released = waiting, released
        self.t_release = None

    def run(self):
        self.waiting.wait(_WAIT)
        time.sleep(_PARK_S)
        self.t_release = time.monotonic()
        self.released.set()


def _park_phase(monkeypatch, method_name, parked, released):
    """Park on entry to one of `compact_sidecar()`'s phase methods.

    The park is *inside* the gate: `compact()` holds it across
    `compact_sidecar()`, so anything that needs the gate is blocked for
    as long as the park is held. That is the ordering under test.
    """
    original = getattr(SqliteStore, method_name)

    def patched(self, *args, **kwargs):
        parked.set()
        assert released.wait(_WAIT), (
            "the timer never released the parked phase; the window "
            "would otherwise be held for the life of the test")
        return original(self, *args, **kwargs)

    monkeypatch.setattr(SqliteStore, method_name, patched)


def _run_race(session, monkeypatch, phase, work):
    """Force one ordering: park `phase`, run `work` against it, compact."""
    parked, released = threading.Event(), threading.Event()
    waiting, done = threading.Event(), threading.Event()
    _park_phase(monkeypatch, phase, parked, released)
    intruder = _Intruder(parked, waiting, done, work)
    timer = _Timer(waiting, released)
    intruder.start()
    timer.start()
    session.compact()
    assert done.wait(_WAIT), "the intruder thread did not finish"
    timer.join(timeout=_WAIT)
    intruder.t_release = timer.t_release
    return intruder


def _record_pending(session, intruder):
    """The #295 guard, read from inside the window."""
    intruder.pending_during = session.persistence_manager.has_pending_saves()


def _assert_guard_was_blind(intruder):
    """The assertion that pins what the #295 refusal does not cover."""
    assert intruder.pending_during is False, (
        "has_pending_saves() read %r inside the window. It is expected to "
        "read False: it sees only the persistence manager's queue and "
        "in-flight set, and neither a caller-thread save(sync=True) nor a "
        "redaction's persist_pixel_data enters either. If this ever reads "
        "True the #295 refusal has become reachable for this population "
        "and compact()'s docstring needs revising in the other direction "
        "(#320)." % (intruder.pending_during,))


def _assert_blocked_then_landed(intruder, session, uid, kind='pixels'):
    """The three assertions every phase shares under the gate (#368)."""
    assert intruder.error is None, intruder.error
    assert intruder.t_write_returned is not None, "the write never returned"
    assert intruder.t_write_returned > intruder.t_release, (
        "the intruder's write returned %.3f s BEFORE the park was "
        "released: it landed during the rewrite, so the gate is not held "
        "at this site (#368)"
        % (intruder.t_release - intruder.t_write_returned,))
    row_final = _blob_row(session, uid, kind)
    assert row_final == intruder.row_after, (
        "the row the intruder committed after the compaction was "
        "rewritten from a map computed before it existed: %r became %r "
        "(#368)" % (intruder.row_after, row_final))
    size = os.path.getsize(session.store_backend.sidecar_path)
    assert row_final[0] + row_final[1] <= size, (
        "the row at %r runs past the end of a %d-byte sidecar" % (
            row_final, size))
    return row_final


def _reads_back(instance):
    """Read through the loader, not from memory."""
    instance.discard_pixel_data()
    return instance.get_pixel_data()


# --------------------------------------------------------------------------
# Phase A -- the append would land during `_rewrite_live_frames`
# --------------------------------------------------------------------------

def test_phase_a_a_write_during_the_rewrite_waits_and_then_lands(
        compactable, monkeypatch):
    """`persist_pixel_data` blocks on the gate until the rewrite is done.

    Characterized (#320): the frame went into the inode compaction was
    about to delete, `_apply_new_offsets` overwrote the row from a
    pre-change map, and the next read raised `Integrity Error`. Under
    the gate the write waits, then appends to the compacted file and
    commits a row that is correct for it.
    """
    session, _series, live = compactable
    target = live[0]

    def work(intruder):
        _record_pending(session, intruder)
        # Produced INSIDE the window on purpose: compact() leads with
        # save(sync=True), so a change made before it is captured by that
        # save and there is nothing new for the intruder to append.
        target.set_pixel_data(_frame(200))
        intruder.about_to_write()
        session.store_backend.persist_pixel_data(target)
        intruder.write_returned()
        intruder.row_after = _blob_row(session, target.sop_instance_uid)

    intruder = _run_race(session, monkeypatch, "_rewrite_live_frames", work)

    _assert_guard_was_blind(intruder)
    _assert_blocked_then_landed(intruder, session, target.sop_instance_uid)
    assert np.array_equal(_reads_back(target), _frame(200)), (
        "the instance does not read back the pixels written during the "
        "compaction (#368)")


# --------------------------------------------------------------------------
# Phase B -- the append would land between the two `os.replace` calls
# --------------------------------------------------------------------------

def test_phase_b_a_write_between_the_two_renames_waits_for_the_swap(
        compactable, monkeypatch):
    """The writer never sees the instant in which the path names nothing.

    Characterized (#320): `write_frame` opens the sidecar `r+b` per call,
    and between `_swap_in_compacted_sidecar`'s two renames the path
    names nothing, so the writer raised `FileNotFoundError`. Under the
    gate the swap is inside the compaction's hold and the writer is
    blocked on the gate throughout it.

    The park is on `os.replace` as `persistence` sees it rather than on
    the phase method: parking *between* the two renames cannot be done by
    wrapping the method, and re-implementing the method in the test would
    make it a test of the copy rather than of the code.
    """
    session, _series, live = compactable
    target = live[1]
    parked, released = threading.Event(), threading.Event()
    waiting, done = threading.Event(), threading.Event()

    real_replace = os.replace
    seen = []

    def parking_replace(src, dst):
        real_replace(src, dst)
        seen.append((src, dst))
        if len(seen) == 1:
            parked.set()
            assert released.wait(_WAIT), "the timer never released"

    monkeypatch.setattr(persistence_module.os, "replace", parking_replace)

    def work(intruder):
        _record_pending(session, intruder)
        target.set_pixel_data(_frame(201))
        intruder.about_to_write()
        session.store_backend.persist_pixel_data(target)
        intruder.write_returned()
        intruder.row_after = _blob_row(session, target.sop_instance_uid)

    intruder = _Intruder(parked, waiting, done, work)
    timer = _Timer(waiting, released)
    intruder.start()
    timer.start()
    session.compact()
    assert done.wait(_WAIT), "the intruder thread did not finish"
    timer.join(timeout=_WAIT)
    intruder.t_release = timer.t_release

    _assert_guard_was_blind(intruder)
    _assert_blocked_then_landed(intruder, session, target.sop_instance_uid)
    assert np.array_equal(_reads_back(target), _frame(201))


# --------------------------------------------------------------------------
# Phase C -- the append would land after the swap, before `_apply_new_offsets`
# --------------------------------------------------------------------------

def test_phase_c_a_write_after_the_swap_waits_for_the_offsets_to_commit(
        compactable, monkeypatch):
    """The ordering with the largest gap between "correct" and "survived".

    Characterized (#320): the writer appended to the *new* file and
    committed a row that was correct for it, and `_apply_new_offsets`
    overwrote that row from a map computed before the write existed.
    Under the gate the offsets commit first and the write lands after.
    """
    session, _series, live = compactable
    target = live[2]

    def work(intruder):
        _record_pending(session, intruder)
        target.set_pixel_data(_frame(202))
        intruder.about_to_write()
        session.store_backend.persist_pixel_data(target)
        intruder.write_returned()
        intruder.row_after = _blob_row(session, target.sop_instance_uid)

    intruder = _run_race(session, monkeypatch, "_apply_new_offsets", work)

    _assert_guard_was_blind(intruder)
    _assert_blocked_then_landed(intruder, session, target.sop_instance_uid)
    assert np.array_equal(_reads_back(target), _frame(202))


# --------------------------------------------------------------------------
# Phase D -- a blob row `_read_blob_index` never saw
# --------------------------------------------------------------------------

def test_phase_d_a_first_write_for_a_new_instance_waits_for_the_index(
        compactable, monkeypatch):
    """A row INSERTed during the window used to dangle past EOF.

    Characterized (#320): a first `persist_pixel_data` for an instance
    created inside the window produced a blob row `_apply_new_offsets`
    had no entry for, left pointing into the pre-compaction layout of a
    now-smaller file, and the read ran off the end. Under the gate the
    insert waits until the compacted file is in place and its offsets
    are committed, so the new row describes the file it was written to.
    """
    session, series, _live = compactable
    newcomer = _make_instance("1.2.3.NEW", 42)

    def work(intruder):
        _record_pending(session, intruder)
        series.instances.append(newcomer)
        intruder.about_to_write()
        session.store_backend.persist_pixel_data(newcomer)
        intruder.write_returned()
        # The instances row has to exist or the blob is an orphan and the
        # next compaction reclaims it, which is a different outcome.
        with session.store_backend._get_connection() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO instances "
                "(sop_instance_uid, sop_class_uid) VALUES (?, ?)",
                (newcomer.sop_instance_uid, CT_STORAGE))
        intruder.row_after = _blob_row(session, newcomer.sop_instance_uid)

    intruder = _run_race(session, monkeypatch, "_rewrite_live_frames", work)

    _assert_guard_was_blind(intruder)
    _assert_blocked_then_landed(intruder, session, newcomer.sop_instance_uid)
    assert np.array_equal(_reads_back(newcomer), _frame(42))


# --------------------------------------------------------------------------
# Phase E -- the intruder is `save(sync=True)`, not `persist_pixel_data`
# --------------------------------------------------------------------------

def test_phase_e_a_sync_save_waits_at_the_gate_inside_save_all(
        compactable, monkeypatch):
    """The same window, reached through `save_all` (site 6).

    Phases A-D all write through `persist_pixel_data`. This one goes
    through `save_all`, whose gate must span `_prepare_pixel_frames`
    *and* the transaction's commit: characterized (#320) as a row
    committed after `_apply_new_offsets` from a frame appended before
    `_read_blob_index`, which only a gate spanning the commit closes.
    A gate released between the prepass and the commit reopens exactly
    this phase.

    `has_pending_saves()` is read *before* the save here: under the gate
    a whole `save(sync=True)` can no longer run start to finish inside
    the window, which is the point, and the guard's blindness to a
    caller-thread save is what is being pinned, not its timing.
    """
    session, _series, live = compactable
    target = live[0]

    def work(intruder):
        _record_pending(session, intruder)
        target.set_pixel_data(_frame(203))
        intruder.about_to_write()
        session.save(sync=True)
        intruder.write_returned()
        intruder.row_after = _blob_row(session, target.sop_instance_uid)

    intruder = _run_race(session, monkeypatch, "_rewrite_live_frames", work)

    _assert_guard_was_blind(intruder)
    _assert_blocked_then_landed(intruder, session, target.sop_instance_uid)
    assert np.array_equal(_reads_back(target), _frame(203))


# --------------------------------------------------------------------------
# The waveform path -- one test, not four
# --------------------------------------------------------------------------

def test_a_waveform_written_in_the_window_waits_and_reads_back(
        compactable, monkeypatch):
    """`persist_blob` (site 4) is gated too, and the hydrated loader reads.

    Characterized (#320): the intruder's row carried the *new* frame's
    sha256 while `_apply_new_offsets` pointed `offset`/`length` back at
    the *old* one, and `SidecarWaveformLoader.read_raw()` raised
    `ValueError: Waveform integrity check failed`. Under the gate the
    row and the bytes agree, and a loader hydrated from the post-race
    row -- which is what the next reopen of this store gets -- returns
    the replacement samples.

    `metadata=` rather than an instance, because the loader's only other
    source of geometry is a Waveform Sequence and building one here would
    be scaffolding for a code path this test is not about.
    """
    session, _series, live = compactable
    target = live[1]
    original = np.arange(4096, dtype=np.int16)
    replacement = np.arange(9000, 13096, dtype=np.int16)

    store = session.store_backend
    store.persist_blob(target, 'waveform', original)
    session.save(sync=True)

    before = _blob_row(session, target.sop_instance_uid, kind='waveform')
    # An in-memory loader so `_rewire_sidecar_loaders` exercises its
    # waveform branch during the compaction rather than skipping it.
    target._waveform_loader = SidecarWaveformLoader(
        store.sidecar_path, before[0], before[1], 'zlib',
        metadata={"num_samples": 4096, "num_channels": 1,
                  "interpretation": "SS", "waveform_hash": before[2]})

    def work(intruder):
        _record_pending(session, intruder)
        intruder.about_to_write()
        store.persist_blob(target, 'waveform', replacement)
        intruder.write_returned()
        intruder.row_after = _blob_row(
            session, target.sop_instance_uid, kind='waveform')

    intruder = _run_race(session, monkeypatch, "_rewrite_live_frames", work)

    _assert_guard_was_blind(intruder)
    final = _assert_blocked_then_landed(
        intruder, session, target.sop_instance_uid, kind='waveform')
    assert final[2] != before[2], (
        "the row's hash is still the original frame's; the intruder's "
        "write did not reach the store")

    hydrated = SidecarWaveformLoader(
        store.sidecar_path, final[0], final[1], 'zlib',
        metadata={"num_samples": 4096, "num_channels": 1,
                  "interpretation": "SS", "waveform_hash": final[2]})
    raw = hydrated.read_raw()
    assert np.array_equal(np.frombuffer(raw, dtype=np.int16), replacement), (
        "a loader hydrated from the post-compaction row does not read the "
        "samples written during the compaction (#368)")


# --------------------------------------------------------------------------
# The deadline names the lock
# --------------------------------------------------------------------------

def test_a_writer_that_cannot_take_the_gate_in_time_raises_naming_the_lock(
        compactable, monkeypatch):
    """Expiry is `RuntimeError` naming `<sidecar>.lock` and the constant.

    `_SIDECAR_GATE_TIMEOUT_S` is patched to 0.5 s and the park is held
    until the intruder has raised, so the writer cannot get the gate in
    time. The error must name the lock file (so the reader knows what
    is stuck) and the constant (so the reader knows what bounded it);
    both are what the redaction ERROR row and the `Background save
    failed` log line carry downstream (#368, 2026-09-08 spec §2.3).

    What this test does *not* pin, measured: the flock half's polled
    `LOCK_NB`. The intruder and the compaction are two threads of one
    process, so the gate's `threading.Lock` half -- `acquire(timeout=)`
    -- is what expires here, before the flock is reached; a blocking
    `flock` survives this test. It is killed by
    `tests/test_sidecar_gate_crosses_processes.py`, where the holder is
    another process, the thread lock is free, and the flock is the only
    wait there is.
    """
    session, _series, live = compactable
    target = live[0]
    monkeypatch.setattr(persistence_module, "_SIDECAR_GATE_TIMEOUT_S", 0.5)
    parked, released = threading.Event(), threading.Event()
    _park_phase(monkeypatch, "_rewrite_live_frames", parked, released)

    outcome = {}

    def work():
        if not parked.wait(_WAIT):
            outcome["error"] = AssertionError("the compaction never parked")
            released.set()
            return
        target.set_pixel_data(_frame(204))
        started = time.monotonic()
        try:
            session.store_backend.persist_pixel_data(target)
        except Exception as exc:      # pylint: disable=broad-except
            outcome["raised"] = exc
        else:
            outcome["raised"] = None
        outcome["elapsed"] = time.monotonic() - started
        # Only now, so the park is held for longer than the deadline.
        released.set()

    intruder = threading.Thread(target=work, daemon=True)
    intruder.start()
    session.compact()
    intruder.join(timeout=_WAIT)
    assert not intruder.is_alive(), "the intruder thread did not finish"
    assert "error" not in outcome, outcome.get("error")

    raised = outcome["raised"]
    assert isinstance(raised, RuntimeError), (
        "the writer got %r where the gate deadline should have raised "
        "RuntimeError (#368)" % (raised,))
    lock_path = session.store_backend._gate_path()
    assert lock_path in str(raised), (
        "the gate error does not name the lock file %s: %s" % (
            lock_path, raised))
    assert "_SIDECAR_GATE_TIMEOUT_S" in str(raised), (
        "the gate error does not name the constant that bounded it: %s"
        % (raised,))
    assert outcome["elapsed"] < 10.0, (
        "the writer waited %.1f s against a 0.5 s deadline; the deadline "
        "is not being read from the module constant" % outcome["elapsed"])
    # The frame was never appended and no row moved, so the instance
    # still reads back what the compaction rewired it to.
    assert np.array_equal(_reads_back(target), _frame(1))
