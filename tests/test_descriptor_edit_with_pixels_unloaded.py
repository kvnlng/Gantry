"""A descriptor edit made with the pixels unloaded is honoured by the live session (#417).

`SidecarPixelLoader` captured Rows, Columns, SamplesPerPixel,
NumberOfFrames, BitsAllocated, PixelRepresentation and the
`_ISOCENTER_PIXEL_DTYPE` carrier **once**, when it was built, and every
later read rebuilt the frame from that capture rather than from
`instance.attributes`. So after `set_attr` on an unloaded instance:

* a PixelRepresentation 0 -> 1 edit still read as `uint16`;
* a Rows/Columns 4x4 -> 2x8 edit still read as (4, 4), and `export()`
  wrote Rows 4 / Columns 4 back out -- the edit silently reverted;
* an edit the stored bytes cannot satisfy (BitsAllocated 16 -> 8) read
  and exported as though it had not happened.

The same store, reopened, honoured every one of those edits or refused
it, because a reopened session builds its loader from the attributes as
they now are. The live session and the reopened one disagreed about the
same bytes. #417's issue text says the *exported file* carried a
container that did not match its bytes; measured, it did not -- the
export was consistent, and consistently wrong about the edit.

The fix compares the loader's capture with the instance on every read
and, when they differ, reads the same stored, hash-checked bytes through
a loader built from the instance as it is now. That loader is used for
the one read and **not stored back** (see
`test_the_stored_loader_object_is_never_replaced_by_a_read`).

The fixture's values are >= 32768 throughout. Below that a `uint16` and
an `int16` reading agree and every signedness comparison collapses to a
tautology; `min() < 0` is asserted separately for that reason. Every test
also asserts that the array is **not resident** before it reads, since a
resident array is handed back without consulting the loader and every
test would then pass on unfixed code.
"""
import glob
import os
import sqlite3

import numpy as np
import pydicom
import pytest
from pydicom.dataset import FileDataset, FileMetaDataset
from pydicom.uid import ExplicitVRLittleEndian, generate_uid

from isocenter.io_handlers import ExportError, SidecarPixelLoader
from isocenter.session import DicomSession

ROWS, COLS, PR, BITS = "0028,0010", "0028,0011", "0028,0103", "0028,0100"
ORIGINAL = (np.arange(16, dtype=np.uint16) + 40000).reshape(4, 4)


def _write_src(folder):
    meta = FileMetaDataset()
    meta.MediaStorageSOPClassUID = "1.2.840.10008.5.1.4.1.1.7"
    meta.MediaStorageSOPInstanceUID = generate_uid()
    meta.TransferSyntaxUID = ExplicitVRLittleEndian
    ds = FileDataset(None, {}, file_meta=meta, preamble=b"\0" * 128)
    ds.PatientID, ds.PatientName = "PAT417", "DOE^JOHN"
    ds.StudyInstanceUID, ds.SeriesInstanceUID = generate_uid(), generate_uid()
    ds.SOPInstanceUID = meta.MediaStorageSOPInstanceUID
    ds.SOPClassUID = meta.MediaStorageSOPClassUID
    ds.Modality, ds.SeriesNumber, ds.InstanceNumber = "OT", 1, 1
    ds.StudyDate = "20230101"
    ds.SamplesPerPixel = 1
    ds.PhotometricInterpretation = "MONOCHROME2"
    ds.Rows, ds.Columns = ORIGINAL.shape
    ds.BitsAllocated = ds.BitsStored = 16
    ds.HighBit = 15
    ds.PixelRepresentation = 0
    ds.PixelData = ORIGINAL.tobytes()
    ds.save_as(os.path.join(folder, "one.dcm"), enforce_file_format=True)


def _only_instance(session):
    for pt in session.store.patients:
        for st in pt.studies:
            for se in st.series:
                for inst in se.instances:
                    return inst
    raise AssertionError("the fixture ingested no instance")


@pytest.fixture
def ingested(tmp_path):
    """An ingested, saved, *unloaded* instance and its session."""
    src = tmp_path / "src"
    src.mkdir()
    _write_src(str(src))
    db = str(tmp_path / "s.db")
    session = DicomSession(persistence_file=db)
    try:
        session.ingest(str(src))
        session.save(sync=True)
        inst = _only_instance(session)
        assert isinstance(inst._pixel_loader, SidecarPixelLoader)
        assert inst.unload_pixel_data() is True
        assert inst.pixel_array is None
        yield session, inst, db
    finally:
        session.close()


def _reopened_read(db):
    """What a fresh session over the same store reads for the instance."""
    session = DicomSession(persistence_file=db)
    try:
        inst = _only_instance(session)
        assert inst.pixel_array is None
        return inst.get_pixel_data(), inst
    finally:
        session.close()


def _not_resident(inst):
    assert inst.unload_pixel_data() is True
    assert inst.pixel_array is None


# ---------------------------------------------------------------------------
# A1-A4 -- the live read honours the edit, before and after a save
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("save_between", [True, False],
                         ids=["saved", "unsaved"])
def test_a_pixel_representation_edit_reads_as_signed(ingested, save_between):
    """A1 (and A4 unsaved): the dtype is re-derived from the instance."""
    session, inst, db = ingested
    inst.set_attr(PR, 1)
    if save_between:
        session.save(sync=True)
    _not_resident(inst)

    got = inst.get_pixel_data()

    assert got.dtype == np.int16
    assert got.min() < 0
    assert np.array_equal(got, ORIGINAL.view(np.int16))
    if save_between:
        reopened, _ = _reopened_read(db)
        assert reopened.dtype == got.dtype
        assert np.array_equal(reopened, got)


@pytest.mark.parametrize("save_between", [True, False],
                         ids=["saved", "unsaved"])
def test_a_rows_and_columns_edit_reads_at_the_new_geometry(ingested,
                                                          save_between):
    """A2 (and A4 unsaved): geometry is compared, not only the dtype.

    Every other descriptor edit changes a compared field *other than*
    Rows/Columns, so a check that forgot geometry would still re-derive
    for them. This is the test that sees it.
    """
    session, inst, db = ingested
    inst.set_attr(ROWS, 2)
    inst.set_attr(COLS, 8)
    if save_between:
        session.save(sync=True)
    _not_resident(inst)

    got = inst.get_pixel_data()

    assert got.shape == (2, 8)
    assert got.dtype == np.uint16
    assert np.array_equal(got, ORIGINAL.reshape(2, 8))
    if save_between:
        reopened, _ = _reopened_read(db)
        assert reopened.shape == got.shape
        assert np.array_equal(reopened, got)


def test_an_edit_the_stored_bytes_cannot_satisfy_is_refused(ingested):
    """A3: BitsAllocated 16 -> 8 names 32 samples where 4x4 needs 16.

    The refusal is #373's Integrity Error, surfaced as `Pixel Loader
    failed` -- the same one a reopened session gives. No new channel.
    """
    session, inst, db = ingested
    inst.set_attr(BITS, 8)
    session.save(sync=True)
    _not_resident(inst)

    with pytest.raises(RuntimeError, match="Integrity Error") as live:
        inst.get_pixel_data()
    assert "Pixel Loader failed" in str(live.value)
    assert "holds 32 samples" in str(live.value)

    with pytest.raises(RuntimeError, match="Integrity Error"):
        _reopened_read(db)


# ---------------------------------------------------------------------------
# A5-A6 -- export, which runs in worker processes
# ---------------------------------------------------------------------------

def test_export_writes_the_edited_geometry(ingested, tmp_path):
    """A5: Rows and Columns are asserted, not only the pixel values.

    Before the fix the exported file was internally consistent -- Rows 4,
    Columns 4 and 16 samples -- because the writer takes the geometry
    from the array. So a pixel-only assertion that reshaped the output
    would pass on unfixed code; the header is where the reversion shows.
    """
    session, inst, _db = ingested
    inst.set_attr(ROWS, 2)
    inst.set_attr(COLS, 8)
    session.save(sync=True)
    _not_resident(inst)

    out = str(tmp_path / "out")
    session.export(out, show_progress=False, use_compression=False)

    files = glob.glob(os.path.join(out, "**", "*.dcm"), recursive=True)
    assert len(files) == 1
    written = pydicom.dcmread(files[0])
    assert written.Rows == 2
    assert written.Columns == 8
    assert written.pixel_array.shape == (2, 8)
    assert written.pixel_array.min() >= 32768
    assert np.array_equal(written.pixel_array, ORIGINAL.reshape(2, 8))


def test_export_refuses_an_edit_the_stored_bytes_cannot_satisfy(ingested,
                                                               tmp_path):
    """A6: the refusal reaches the export's ERROR row, not a clean file."""
    session, inst, db = ingested
    inst.set_attr(BITS, 8)
    session.save(sync=True)
    _not_resident(inst)

    with pytest.raises(ExportError):
        session.export(str(tmp_path / "out"), show_progress=False,
                       use_compression=False)
    session.store_backend.flush_audit_queue()

    conn = sqlite3.connect(db)
    try:
        errors = [d for (d,) in conn.execute(
            "SELECT details FROM audit_log WHERE action_type='ERROR'")]
    finally:
        conn.close()
    assert any("Integrity Error" in d for d in errors), errors


# ---------------------------------------------------------------------------
# A7-A9 -- the mechanism: no store-back, no spurious rebuild, the hash
# ---------------------------------------------------------------------------

def test_the_stored_loader_object_is_never_replaced_by_a_read(ingested):
    """A7: a read that re-derives uses the fresh loader once and drops it.

    Behaviour cannot see a store-back -- the next read would re-derive to
    the same answer -- so identity is the only pin. It matters because
    `Session._apply_redaction_outcomes` rebinds `_pixel_loader` under
    `_pixel_swap_lock`; a read writing the same slot outside that lock
    can publish a stale loader after the redacted one, which is #274's
    shape (unredacted pixels under a full redaction attestation).
    """
    _session, inst, _db = ingested
    loader = inst._pixel_loader
    inst.set_attr(PR, 1)
    _not_resident(inst)
    assert not loader.describes(inst)

    got = inst.get_pixel_data()

    assert got.dtype == np.int16  # the read did need the rebuild
    assert inst._pixel_loader is loader, (
        "get_pixel_data() stored the rebuilt loader back on the instance; "
        "reads must not write `_pixel_loader` -- only "
        "`_apply_redaction_outcomes` does, under `_pixel_swap_lock` (#274)")


def test_an_edit_to_no_pixel_descriptor_rebuilds_nothing(ingested,
                                                         monkeypatch):
    """A8: the control. A SeriesDescription edit changes no reading.

    Asserted by counting `for_instance` calls, because a rebuild on
    every read returns the same array and no behavioural assertion can
    tell it from the fix.
    """
    session, inst, db = ingested
    calls = []
    real = SidecarPixelLoader.for_instance

    def counting(self, instance):
        calls.append(instance.sop_instance_uid)
        return real(self, instance)

    monkeypatch.setattr(SidecarPixelLoader, "for_instance", counting)

    inst.set_attr("0008,103e", "an edit to no pixel descriptor")
    assert inst._pixel_loader.describes(inst)
    got = inst.get_pixel_data()
    assert got.shape == (4, 4) and got.dtype == np.uint16
    assert np.array_equal(got, ORIGINAL)
    assert calls == []

    # And a reopened session's loader, built from the stored attributes,
    # describes its instance: no read after reopen rebuilds either.
    session.save(sync=True)
    reopened, reopened_inst = _reopened_read(db)
    assert reopened_inst._pixel_loader.describes(reopened_inst)
    assert np.array_equal(reopened, ORIGINAL)
    assert calls == []


def test_a_rebuilt_loader_carries_the_old_hash_verbatim(ingested):
    """A9: the bytes did not move, so the hash question does not either.

    Built from the instance, a loader falls back to `inst._pixel_hash`
    when it is handed no hash -- and `_pixel_hash` can drift from the
    bytes at this offset (it is set outside `if loader:` in
    `_apply_redaction_outcomes`). That fallback caused #212 once already.
    """
    session, inst, _db = ingested
    ingested_loader = inst._pixel_loader
    # The fixture guard: an ingested loader carries no hash while the
    # instance does, so a fallback would be visible here.
    assert ingested_loader.pixel_hash is None
    assert inst._pixel_hash is not None
    assert ingested_loader.for_instance(inst).pixel_hash is None

    # A loader a save built, which does carry one; then a different
    # `_pixel_hash` on the instance, which must not leak in.
    inst.set_pixel_data((ORIGINAL + 1).astype(np.uint16))
    session.save(sync=True)
    hashed = inst._pixel_loader
    assert hashed.pixel_hash is not None
    inst._pixel_hash = "0" * 64
    assert hashed.for_instance(inst).pixel_hash == hashed.pixel_hash


def test_describes_names_every_field_the_loader_reads(ingested):
    """Each captured descriptor, one at a time, and the SOP Instance UID.

    The UID is compared so that after `regenerate_uid` the loader's
    Integrity Error names the UID the caller now knows the instance by.
    """
    _session, inst, _db = ingested
    loader = inst._pixel_loader
    assert loader.describes(inst)
    for tag, value in ((ROWS, 2), (COLS, 8), ("0028,0002", 3),
                       ("0028,0008", 2), (BITS, 8), (PR, 1)):
        before = inst.attributes.get(tag)
        inst.set_attr(tag, value)
        assert not loader.describes(inst), tag
        inst.set_attr(tag, before)
        assert loader.describes(inst), tag

    # The float carrier, which `set_attr` cannot reach (it lowercases).
    from isocenter.pixel_geometry import PIXEL_DTYPE_ATTR
    inst.attributes[PIXEL_DTYPE_ATTR] = "float32"
    assert not loader.describes(inst)
    del inst.attributes[PIXEL_DTYPE_ATTR]
    assert loader.describes(inst)

    original_uid = inst.sop_instance_uid
    inst.sop_instance_uid = generate_uid()
    assert not loader.describes(inst)
    assert loader.for_instance(inst).sop_instance_uid == inst.sop_instance_uid
    inst.sop_instance_uid = original_uid


# ---------------------------------------------------------------------------
# A10 -- set_pixel_data -> discard: one answer, whatever the save state
# ---------------------------------------------------------------------------

def test_a_discarded_replacement_reads_the_same_before_and_after_a_save(
        ingested):
    """A10: no gate on `_pixel_array_unwritten`.

    `set_pixel_data(view)` writes PixelRepresentation 1; `discard` drops
    the array but not the descriptor. The stored bytes under the
    instance's descriptors are the int16 reading -- which is also what a
    save and a reopen give. A check gated on "not unwritten" leaves the
    first read uint16 and flips it on the second, with no save between:
    two answers to one question.
    """
    session, inst, db = ingested
    signed = inst.get_pixel_data().view(np.int16)
    inst.set_pixel_data(signed)
    assert inst.discard_pixel_data() is True
    assert inst.pixel_array is None

    reads = [inst.get_pixel_data()]
    _not_resident(inst)
    reads.append(inst.get_pixel_data())
    session.save(sync=True)
    _not_resident(inst)
    reads.append(inst.get_pixel_data())
    reopened, _ = _reopened_read(db)
    reads.append(reopened)

    for i, got in enumerate(reads):
        assert got.dtype == np.int16, i
        assert got.min() < 0, i
        assert np.array_equal(got, ORIGINAL.view(np.int16)), i
