"""compact() races a write that starts after its guard, and how loudly (#320).

`session.compact()` saves, refuses if the persistence manager still has
work, and then rewrites the sidecar. A write that *starts after* that
refusal runs concurrently with the rewrite, appending frames to a file
compaction is in the middle of replacing. #295 closed the torn rebind and
the entered-with-a-save-in-flight case; #317 said in the changelog that
it closed neither of the rest, and #320 is that statement as an issue.

**This file is a characterization test, not a red-then-green one.** The
race is accepted for now, deliberately and with the costs measured: the
window is `live_bytes / storage_throughput` (0.024 s at 200 MB, 0.221 s
at 2 GB on the hardware this was measured on), it is reachable only by
violating a precondition `compact()` has always documented, and the
correctly-scoped fix is a *sidecar-generation lock* over five separate
write-frame-then-commit-row sites plus a cross-process question at the
two ingest sites -- materially larger than the one lock around `save_all`
that #320 prices. That fix is filed as a post-1.0 item. What is landed
here is the reproduction, because a race nobody can reproduce is a race
nobody can verify fixed.

**The half that is load-bearing is the failure *mode*, not the failure.**
#320 inherits from #295 the framing "reads the wrong bytes or runs off
the end of the file". That was true of the torn rebind, which #295
closed. It is **not** true of the residual: every ordering below ends in
an exception the caller cannot miss -- `RuntimeError: Integrity Error`
from the pixel loader, `IOError: Incomplete read from sidecar` where the
row dangles past EOF, `FileNotFoundError` at the writer during the swap,
and `ValueError: Waveform integrity check failed` on the waveform path.
Loud data loss, not silent wrong data. That is the finding the decision
to accept the window rests on, and it holds only because `_pixel_hash`
(and the waveform sha256) are written alongside the frame and are not
touched by the rewrite, which rewrites `offset` and `length` alone. Drop
either hash and the residual becomes silent wrong pixels; these tests are
where that would be caught rather than discovered.

**`has_pending_saves()` is `False` in every one of these windows**, and
each test asserts it. The #295 refusal reads only the persistence
manager's queue and in-flight set. A `Session.save(sync=True)` from
another thread runs `save_all` on the *caller's* thread and enters
neither; a redaction's `persist_pixel_data` is not a save at all. So the
refusal cannot fire for the population that reaches this window -- which
is the correction #320's amendment 1 makes to `compact()`'s docstring.
Phase E is what makes that a measurement rather than an inspection: a
whole `save(sync=True)` runs start to finish inside the window and the
guard still reads `False`.

**Style**: `tests/test_export_flushes_before_it_sweeps.py`. A helper
thread on a bounded daemon, the ordering forced by a pair of
`threading.Event`s from a monkeypatched phase method, and the helper's
exception kept rather than swallowed.

**Two traps that cost real time when this was built, per the spec.**
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
    """Runs `work` once the compaction parks, then releases it.

    The exception is kept rather than swallowed: on phase B the *writer*
    is what raises, and a helper that ate it would read as a green test
    over an unreported failure.
    """

    def __init__(self, parked, released, work):
        super().__init__(daemon=True)
        self.parked, self.released, self.work = parked, released, work
        self.error = None
        self.pending_during = None

    def run(self):
        try:
            if not self.parked.wait(_WAIT):
                self.error = AssertionError("the compaction never parked")
                return
            self.work(self)
        except Exception as exc:      # pylint: disable=broad-except
            self.error = exc
        finally:
            self.released.set()


def _park_phase(monkeypatch, method_name, parked, released):
    """Park on entry to one of `compact_sidecar()`'s four phase methods."""
    original = getattr(SqliteStore, method_name)

    def patched(self, *args, **kwargs):
        parked.set()
        assert released.wait(_WAIT), (
            "the intruder never released the parked phase; the window "
            "would otherwise be held for the life of the test")
        return original(self, *args, **kwargs)

    monkeypatch.setattr(SqliteStore, method_name, patched)


def _run_race(session, monkeypatch, phase, work):
    """Force one ordering: park `phase`, run `work` inside it, compact."""
    parked, released = threading.Event(), threading.Event()
    _park_phase(monkeypatch, phase, parked, released)
    intruder = _Intruder(parked, released, work)
    intruder.start()
    session.compact()
    intruder.join(timeout=_WAIT)
    assert not intruder.is_alive(), "the intruder thread did not finish"
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


def _readback_raises(instance):
    """Read through the loader, not from memory, and return the error."""
    instance.discard_pixel_data()
    try:
        instance.get_pixel_data()
    except Exception as exc:          # pylint: disable=broad-except
        return exc
    return None


# --------------------------------------------------------------------------
# Phase A -- the append lands during `_rewrite_live_frames`
# --------------------------------------------------------------------------

def test_phase_a_an_append_during_the_rewrite_is_overwritten_and_reads_loud(
        compactable, monkeypatch):
    """The frame goes into the inode compaction is about to delete.

    `_read_blob_index` has already run, so `updates` carries the
    *pre-change* frame's compacted position for this instance's blob row.
    `_apply_new_offsets` writes that over the row the intruder just
    committed, while the `_pixel_hash` `persist_pixel_data` stored is the
    new frame's. Store and sidecar now disagree, and the disagreement is
    detected on the next read rather than returned as pixels.
    """
    session, _series, live = compactable
    target = live[0]

    def work(intruder):
        _record_pending(session, intruder)
        # Produced INSIDE the window on purpose: compact() leads with
        # save(sync=True), so a change made before it is captured by that
        # save and there is nothing new for the intruder to append.
        target.set_pixel_data(_frame(200))
        session.store_backend.persist_pixel_data(target)
        intruder.row_after = _blob_row(session, target.sop_instance_uid)

    intruder = _run_race(session, monkeypatch, "_rewrite_live_frames", work)

    assert intruder.error is None, intruder.error
    _assert_guard_was_blind(intruder)

    row_final = _blob_row(session, target.sop_instance_uid)
    assert row_final != intruder.row_after, (
        "the intruder's row survived the compaction; this ordering is "
        "expected to have it overwritten from a map computed before the "
        "write existed (#320)")

    error = _readback_raises(target)
    assert isinstance(error, RuntimeError), (
        "the read after the race returned %r instead of raising. The "
        "whole case for accepting this window is that its outcome is "
        "loud: if _pixel_hash is ever dropped from the loader this "
        "becomes silent wrong pixels (#320)." % (error,))
    assert "Integrity Error" in str(error) and "hash mismatch" in str(error), (
        "the read raised %r, which is not the integrity failure this "
        "window is characterized by" % (error,))


# --------------------------------------------------------------------------
# Phase B -- the append lands between the two `os.replace` calls
# --------------------------------------------------------------------------

def test_phase_b_a_write_between_the_two_renames_fails_at_the_writer(
        compactable, monkeypatch):
    """For one instant the sidecar path names nothing, and the writer says so.

    `_swap_in_compacted_sidecar` renames the sidecar to `.compact.bak`
    before renaming the temp file in, and `SidecarManager.write_frame`
    opens the path `r+b`, per call. So the writer raises rather than
    creating a stray file -- loud at the writer, where on the redaction
    path it surfaces as a failed redaction. The compaction itself still
    reports success, which is the "error at a distance" half of this
    window's cost.

    The park is on `os.replace` as `persistence` sees it rather than on
    the phase method: parking *between* the two renames cannot be done by
    wrapping the method, and re-implementing the method in the test would
    make it a test of the copy rather than of the code.
    """
    session, _series, live = compactable
    target = live[1]
    parked, released = threading.Event(), threading.Event()

    real_replace = os.replace
    seen = []

    def parking_replace(src, dst):
        real_replace(src, dst)
        seen.append((src, dst))
        if len(seen) == 1:
            parked.set()
            assert released.wait(_WAIT), "the intruder never released"

    monkeypatch.setattr(persistence_module.os, "replace", parking_replace)

    def work(intruder):
        _record_pending(session, intruder)
        target.set_pixel_data(_frame(201))
        try:
            session.store_backend.persist_pixel_data(target)
        except Exception as exc:      # pylint: disable=broad-except
            intruder.writer_error = exc
        else:
            intruder.writer_error = None

    intruder = _Intruder(parked, released, work)
    intruder.start()
    session.compact()
    intruder.join(timeout=_WAIT)
    assert not intruder.is_alive()

    assert intruder.error is None, intruder.error
    _assert_guard_was_blind(intruder)
    assert isinstance(intruder.writer_error, FileNotFoundError), (
        "the writer got %r where a FileNotFoundError is expected: for the "
        "instant between the two renames the sidecar path names nothing, "
        "and write_frame opens it 'r+b' rather than creating it (#320)"
        % (intruder.writer_error,))


# --------------------------------------------------------------------------
# Phase C -- the append lands after the swap, before `_apply_new_offsets`
# --------------------------------------------------------------------------

def test_phase_c_a_correct_row_written_after_the_swap_is_overwritten_anyway(
        compactable, monkeypatch):
    """The writer does everything right and is overwritten regardless.

    The swap has already happened, so the intruder appends to the *new*
    file and commits a row that is correct for it. `_apply_new_offsets`
    then writes over that row from a map computed before the write
    existed, and the fresh frame is orphaned in the sidecar until the next
    compaction reclaims it. This is the ordering with the largest gap
    between "the writer was correct" and "the data survived".
    """
    session, _series, live = compactable
    target = live[2]

    def work(intruder):
        _record_pending(session, intruder)
        target.set_pixel_data(_frame(202))
        session.store_backend.persist_pixel_data(target)
        intruder.row_after = _blob_row(session, target.sop_instance_uid)
        intruder.size_at_append = os.path.getsize(
            session.store_backend.sidecar_path)

    intruder = _run_race(session, monkeypatch, "_apply_new_offsets", work)

    assert intruder.error is None, intruder.error
    _assert_guard_was_blind(intruder)

    assert intruder.row_after[0] < intruder.size_at_append, (
        "the intruder's row is not inside the file it wrote to; this "
        "ordering is the one where the writer was correct")
    row_final = _blob_row(session, target.sop_instance_uid)
    assert row_final != intruder.row_after, (
        "the intruder's correct row survived _apply_new_offsets; this "
        "ordering is characterized by it being overwritten (#320)")

    error = _readback_raises(target)
    assert isinstance(error, RuntimeError) and "Integrity Error" in str(error), (
        "the read after the race gave %r rather than raising loudly; the "
        "case for accepting this window rests on it being detected (#320)"
        % (error,))


# --------------------------------------------------------------------------
# Phase D -- a blob row `_read_blob_index` never saw
# --------------------------------------------------------------------------

def test_phase_d_a_row_inserted_after_the_index_read_dangles_past_eof(
        compactable, monkeypatch):
    """A row INSERTed during the window is not in `updates` at all.

    A concurrent ingest, or a first `persist_pixel_data` for an instance
    created inside the window, produces a blob row whose id
    `_apply_new_offsets` has no entry for -- so it is left alone, pointing
    into the pre-compaction layout of a file that is now smaller. The read
    runs off the end rather than landing on plausible bytes, because the
    recorded length no longer fits.
    """
    session, series, _live = compactable
    newcomer = _make_instance("1.2.3.NEW", 42)

    def work(intruder):
        _record_pending(session, intruder)
        series.instances.append(newcomer)
        session.store_backend.persist_pixel_data(newcomer)
        # The instances row has to exist or the blob is an orphan and the
        # next compaction reclaims it, which is a different outcome.
        with session.store_backend._get_connection() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO instances "
                "(sop_instance_uid, sop_class_uid) VALUES (?, ?)",
                (newcomer.sop_instance_uid, CT_STORAGE))
        intruder.size_before = os.path.getsize(
            session.store_backend.sidecar_path)
        intruder.row_after = _blob_row(session, newcomer.sop_instance_uid)

    intruder = _run_race(session, monkeypatch, "_rewrite_live_frames", work)

    assert intruder.error is None, intruder.error
    _assert_guard_was_blind(intruder)

    row_final = _blob_row(session, newcomer.sop_instance_uid)
    assert row_final == intruder.row_after, (
        "the newcomer's row was rewritten; this ordering is the one where "
        "_apply_new_offsets has no entry for it and leaves it alone")

    size_after = os.path.getsize(session.store_backend.sidecar_path)
    assert row_final[0] + row_final[1] > size_after, (
        "the row at %r is still inside a file of %d bytes, so this "
        "ordering did not reproduce" % (row_final, size_after))

    error = _readback_raises(newcomer)
    assert isinstance(error, RuntimeError), (
        "the read gave %r rather than raising; a row past EOF must not "
        "come back as data (#320)" % (error,))
    assert "Incomplete read from sidecar" in str(error), (
        "the read raised %r rather than the short-read this ordering is "
        "characterized by" % (error,))


# --------------------------------------------------------------------------
# Phase E -- the intruder is `save(sync=True)`, not `persist_pixel_data`
# --------------------------------------------------------------------------

def test_phase_e_a_whole_sync_save_runs_inside_the_window_unseen(
        compactable, monkeypatch):
    """The same window, reached through `save_all` instead.

    Phases A-D all write through `persist_pixel_data`, which is not a site
    the coarse lock #320 prices would cover. This one is: the helper does
    `set_pixel_data` and then a full `session.save(sync=True)`, so the
    frame is appended and its row committed by `save_all` -- and the
    outcome is identical. That is why #320's Arm B closes *one* of five
    ways in rather than none, and why the correctly-scoped fix is a
    sidecar-generation lock over all five sites rather than one lock
    around `save_all`.

    It is also the ordering that turns the `has_pending_saves()` claim
    from an inspection into a measurement: an entire synchronous save runs
    start to finish inside the window and the guard still reads `False`,
    because `save(sync=True)` runs `save_all` on the caller's thread and
    never enters the manager's queue or in-flight set.
    """
    session, _series, live = compactable
    target = live[0]

    def work(intruder):
        target.set_pixel_data(_frame(203))
        session.save(sync=True)
        # Read the guard AFTER the save has run to completion. That is
        # the whole point of this phase.
        _record_pending(session, intruder)
        intruder.row_after = _blob_row(session, target.sop_instance_uid)

    intruder = _run_race(session, monkeypatch, "_rewrite_live_frames", work)

    assert intruder.error is None, intruder.error
    _assert_guard_was_blind(intruder)

    row_final = _blob_row(session, target.sop_instance_uid)
    assert row_final != intruder.row_after, (
        "the row save_all committed survived the compaction; this "
        "ordering is characterized by it being overwritten (#320)")

    error = _readback_raises(target)
    assert isinstance(error, RuntimeError) and "Integrity Error" in str(error), (
        "a save(sync=True) that ran entirely inside the window left %r "
        "rather than a loud integrity failure (#320)" % (error,))


# --------------------------------------------------------------------------
# The waveform path -- one test, not four
# --------------------------------------------------------------------------

def test_a_waveform_written_in_the_window_is_caught_by_its_own_sha256(
        compactable, monkeypatch):
    """The "loud, not silent" claim is only as good as its weakest path.

    `compact()` rewires waveform loaders as well as pixel ones, and
    `persist_blob` is one of the five write-frame-then-commit-row sites,
    so the waveform half reaches the same window. It is a different loader
    with a different exception type -- `ValueError: Waveform integrity
    check failed` rather than `RuntimeError: Integrity Error`. Had this
    check been absent the residual race would have been *silent wrong
    samples* for waveforms, and the decision to accept the window would
    not stand as written.

    **The loader is armed from the blob row, which is the only place it
    can be armed from.** `persist_blob` -- unlike `persist_pixel_data` --
    writes no hash onto the instance and rebinds no loader, so an
    in-memory waveform loader keeps whatever hash it was built with. The
    hash that matters is the one `_hydrate_waveform_loader` reads out of
    `instance_blobs` (`wref['hash']`), which the intruder's write has
    updated to the *new* frame's while `_apply_new_offsets` has pointed
    `offset`/`length` back at the *old* one. This test therefore builds
    the loader the way hydration does, from the post-race row -- which is
    what the next reopen of this store gets, and the reason the spec
    calls the failure one that survives a reopen.

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
        store.persist_blob(target, 'waveform', replacement)
        intruder.row_after = _blob_row(
            session, target.sop_instance_uid, kind='waveform')

    intruder = _run_race(session, monkeypatch, "_rewrite_live_frames", work)

    assert intruder.error is None, intruder.error
    _assert_guard_was_blind(intruder)

    final = _blob_row(session, target.sop_instance_uid, kind='waveform')
    assert final[2] == intruder.row_after[2] != before[2], (
        "the row's hash is not the intruder's; _apply_new_offsets rewrites "
        "offset and length alone and this test depends on that")
    assert (final[0], final[1]) != (intruder.row_after[0],
                                    intruder.row_after[1]), (
        "the intruder's offset survived the compaction, so the row and the "
        "bytes it names do not disagree and there is nothing to detect")

    hydrated = SidecarWaveformLoader(
        store.sidecar_path, final[0], final[1], 'zlib',
        metadata={"num_samples": 4096, "num_channels": 1,
                  "interpretation": "SS", "waveform_hash": final[2]})

    with pytest.raises(ValueError) as caught:
        hydrated.read_raw()
    assert "Waveform integrity check failed" in str(caught.value), (
        "the waveform read raised %r; the sha256 in read_raw() is what "
        "keeps this window from being silent wrong samples (#320)"
        % (caught.value,))
