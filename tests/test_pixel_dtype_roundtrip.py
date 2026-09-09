"""The dtype a frame goes in as is the dtype it comes back as (#386).

Two defects, one root, and the root is on the **write** side.

`set_pixel_data()` recorded a frame's *width* -- `BitsAllocated` from
`array.itemsize` -- and never its *signedness*. Nothing recorded it: not
`PixelRepresentation (0028,0103)`, not the dtype carrier. So an `int16`
array set in memory reloaded from the sidecar as `uint16` (`-8` read back
as `65528`), and `_export_instance_worker`'s
`ds.PixelRepresentation = inst.attributes.get("0028,0103", 0)` wrote a
file declaring `PixelRepresentation 0` beside signed bytes -- while the
audit log said `wrote 1 of 1 planned instances`. No `DATA_LOSS` row, no
`ERROR` row, no warning. That silence is why this is a Last Silences item:
fix the loader alone and the file keeps lying.

The second defect is the loader's dtype table. It bucketed every integer
frame as `uint16 if bits > 8 else uint8`, so a 32- or 64-bit frame was
decoded two or four times too wide and died on #373's byte check:

    RuntimeError: Integrity Error: frame for <uid> holds 32 bytes, which
    is not a whole number of 2-byte samples (dtype uint16)

which is #373's bound doing exactly what its entry says, on exactly the
population that entry names. The table is now keyed on `BitsAllocated`
over {8, 16, 32, 64} crossed with `PixelRepresentation` -- **with the old
rule kept as the fallback**, because `BitsAllocated 1` is a real ingested
population (a binary Segmentation, whose bytes pydicom unpacks to one
uint8 per pixel) and `BitsAllocated 12` arrives as uint16. A dict-only
rewrite is a regression, which is what `test_a_one_bit_frame_still_reloads_as_uint8`
is here to say.

The carrier widens by exactly one dtype, `bool`, and no more. Once the
signedness is written, `BitsAllocated` plus `PixelRepresentation` name
every integer dtype exactly, and a carrier recorded *as well* would be a
second answer to a question the descriptor pair already answers -- and the
authoritative one, so a graph whose descriptors were later corrected would
decode against the stale carrier. `bool` is the one dtype no DICOM
descriptor can name: numpy `bool_` and `uint8` both declare
`BitsAllocated 8`, `PixelRepresentation 0`.

**`set_pixel_data()` now refuses a dtype the sidecar cannot honestly
carry**, with a `ValueError`, and the accepted set is defined positively
so a numpy release that adds a kind is refused by default rather than
admitted by omission. Byte order is the one "merely unusual but
round-trippable" case and is **normalized, not refused**. The check runs
before any mutation, so a caught `ValueError` leaves the instance exactly
as it was -- which `test_a_refused_dtype_leaves_the_instance_exactly_as_it_was`
asserts on the *frame*, not only on the attributes dict: only the frame
assertion catches a pixel replacement that happened before the raise.

Every dtype assertion here compares against an **absolute** dtype and
every value assertion against a **literal** list. `got.dtype == arr.dtype`
where `arr` is built in the same test is true whenever both sides are
wrong the same way, which is the shape this whole file exists to catch.
"""
import glob
import os
import sqlite3

import numpy as np
import pydicom
import pytest
from pydicom.dataset import FileDataset, FileMetaDataset
from pydicom.uid import ExplicitVRLittleEndian, ImplicitVRLittleEndian, generate_uid

from isocenter.pixel_geometry import PIXEL_DTYPE_ATTR
from isocenter.session import DicomSession

#: The 4x4 frame every round trip uses, as a literal. Signed values are
#: chosen negative so an unsigned reload is not merely a different dtype
#: but a different number: -8 reads back as 65528 at 16 bits.
SIGNED_ROWS = [[-8, -7, -6, -5],
               [-4, -3, -2, -1],
               [0, 1, 2, 3],
               [4, 5, 6, 7]]

UNSIGNED_ROWS = [[0, 1, 2, 3],
                 [4, 5, 6, 7],
                 [8, 9, 10, 11],
                 [12, 13, 14, 15]]


def _write_src(folder, arr, pixel_representation, name="one.dcm",
               transfer_syntax=ExplicitVRLittleEndian):
    """One instance carrying `arr` as (7fe0,0010), declared honestly."""
    meta = FileMetaDataset()
    meta.MediaStorageSOPClassUID = "1.2.840.10008.5.1.4.1.1.7"
    meta.MediaStorageSOPInstanceUID = generate_uid()
    meta.TransferSyntaxUID = transfer_syntax

    ds = FileDataset(None, {}, file_meta=meta, preamble=b"\0" * 128)
    ds.PatientID, ds.PatientName = "PAT386", "DOE^JOHN"
    ds.StudyInstanceUID, ds.SeriesInstanceUID = generate_uid(), generate_uid()
    ds.SOPInstanceUID = meta.MediaStorageSOPInstanceUID
    ds.SOPClassUID = meta.MediaStorageSOPClassUID
    ds.Modality, ds.SeriesNumber, ds.InstanceNumber = "OT", 1, 1
    ds.StudyDate = "20230101"

    ds.Rows, ds.Columns = arr.shape
    ds.SamplesPerPixel = 1
    ds.PhotometricInterpretation = "MONOCHROME2"
    ds.BitsAllocated = ds.BitsStored = arr.dtype.itemsize * 8
    ds.HighBit = ds.BitsStored - 1
    ds.PixelRepresentation = pixel_representation
    ds.PixelData = arr.tobytes()

    path = os.path.join(folder, name)
    ds.save_as(path, enforce_file_format=True)
    return ds.SOPInstanceUID


def _write_binary_segmentation(path, arr):
    """A `BitsAllocated=1` source: 1 bit per pixel, packed, as PS3.5 requires.

    Built the way `tests/test_pixel_geometry_pipeline.py` builds it -- a
    declared width no numpy array can have, which is precisely the
    population the `_INTEGER_DTYPE_BY_BITS` fallback exists for.
    """
    sop_uid = generate_uid()
    meta = FileMetaDataset()
    meta.MediaStorageSOPClassUID = "1.2.840.10008.5.1.4.1.1.66.4"
    meta.MediaStorageSOPInstanceUID = sop_uid
    meta.TransferSyntaxUID = ImplicitVRLittleEndian

    ds = FileDataset(str(path), {}, file_meta=meta, preamble=b"\0" * 128)
    ds.PatientName, ds.PatientID = "TestBits386", "PID_BITS386"
    ds.SOPInstanceUID = sop_uid
    ds.SOPClassUID = meta.MediaStorageSOPClassUID
    ds.StudyInstanceUID, ds.SeriesInstanceUID = generate_uid(), generate_uid()
    ds.Modality, ds.SeriesNumber, ds.InstanceNumber = "SEG", 1, 1
    ds.StudyDate = "20230101"

    frames, rows, cols = arr.shape
    ds.NumberOfFrames = frames
    ds.Rows, ds.Columns = rows, cols
    ds.SamplesPerPixel = 1
    ds.PhotometricInterpretation = "MONOCHROME2"
    ds.BitsAllocated = ds.BitsStored = 1
    ds.HighBit = 0
    ds.PixelRepresentation = 0
    ds.PixelData = np.packbits(arr.ravel(), bitorder="little").tobytes()
    ds.save_as(str(path), enforce_file_format=True)
    return sop_uid


def _only_instance(session):
    for patient in session.store.patients:
        for study in patient.studies:
            for series in study.series:
                for instance in series.instances:
                    return instance
    raise AssertionError("no instance in the graph")


def _read_only_written(out):
    written = glob.glob(os.path.join(str(out), "**", "*.dcm"), recursive=True)
    assert written, "export produced no .dcm files"
    return pydicom.dcmread(written[0])


def _audit_rows(db_path):
    with sqlite3.connect(db_path) as conn:
        return conn.execute(
            "SELECT action_type, entity_uid, details FROM audit_log").fetchall()


def _ingest_save_reopen(tmp_path, write, prefix):
    """Ingest, `save()`, `close()`, reopen -- the sidecar round trip.

    Returns the reopened session; the caller closes it.
    """
    src = tmp_path / f"{prefix}_src"
    src.mkdir(exist_ok=True)
    write(str(src))
    db = str(tmp_path / f"{prefix}.db")

    session = DicomSession(persistence_file=db)
    try:
        session.ingest(str(src))
        session.save()
    finally:
        session.close()

    reopened = DicomSession(persistence_file=db)
    return reopened, db


# ---------------------------------------------------------------------------
# The loader's dtype table
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("dtype_name,pixel_representation,rows", [
    ("uint32", 0, UNSIGNED_ROWS),
    ("int32", 1, SIGNED_ROWS),
])
def test_a_32_bit_integer_frame_reloads_as_the_dtype_it_was_ingested_as(
        tmp_path, dtype_name, pixel_representation, rows):
    """Red when the `32` row is removed from `_INTEGER_DTYPE_BY_BITS`.

    Before the table this raised, and loudly: the frame was decoded as
    `uint16`, its byte count was not a whole number of 2-byte samples, and
    #373's check turned that into
    `RuntimeError: Integrity Error: ... which is not a whole number of
    2-byte samples`. The bound was right; the dtype it was checking
    against was not.
    """
    arr = np.array(rows, dtype=dtype_name)
    session, _db = _ingest_save_reopen(
        tmp_path, lambda folder: _write_src(folder, arr, pixel_representation),
        f"b32_{dtype_name}")
    try:
        got = _only_instance(session).get_pixel_data()
    finally:
        session.close()

    assert got.dtype == np.dtype(dtype_name)
    assert got.tolist() == rows


@pytest.mark.parametrize("dtype_name,pixel_representation,rows", [
    ("uint64", 0, UNSIGNED_ROWS),
    ("int64", 1, SIGNED_ROWS),
])
def test_a_64_bit_integer_frame_reloads_as_the_dtype_it_was_ingested_as(
        tmp_path, dtype_name, pixel_representation, rows):
    """Red when the `64` row is removed.

    Measured on pydicom 3.0.2: `BitsAllocated 64` writes and reads back
    fine (`pixel_array.dtype == uint64`), so this is a full-pipeline test
    rather than a loader-level one.
    """
    arr = np.array(rows, dtype=dtype_name)
    session, _db = _ingest_save_reopen(
        tmp_path, lambda folder: _write_src(folder, arr, pixel_representation),
        f"b64_{dtype_name}")
    try:
        got = _only_instance(session).get_pixel_data()
    finally:
        session.close()

    assert got.dtype == np.dtype(dtype_name)
    assert got.tolist() == rows


def test_a_one_bit_frame_still_reloads_as_uint8(tmp_path):
    """The fallback, and why a dict-only rewrite is a regression.

    A binary Segmentation declares `BitsAllocated 1`, which no numpy array
    can have; pydicom unpacks the packed bits to one uint8 per pixel and
    that is what the sidecar holds. `_INTEGER_DTYPE_BY_BITS` has no `1`
    row, so this frame reaches the legacy `uint16 if bits > 8 else uint8`
    rule -- deliberately. Red when the fallback is replaced by a bare
    `_INTEGER_DTYPE_BY_BITS[self.bits]` (`KeyError`) or by a raise.
    """
    arr = np.zeros((2, 4, 8), dtype=np.uint8)
    arr[0, 1, 2] = 1
    arr[1, 3, 7] = 1

    session, _db = _ingest_save_reopen(
        tmp_path,
        lambda folder: _write_binary_segmentation(
            os.path.join(folder, "seg.dcm"), arr),
        "onebit")
    try:
        instance = _only_instance(session)
        assert instance.attributes["0028,0100"] in (1, "1"), (
            "fixture never entered the arm under test: BitsAllocated is not 1")
        got = instance.get_pixel_data()
    finally:
        session.close()

    assert got.dtype == np.dtype("uint8")
    assert got.shape == (2, 4, 8)
    assert got.tolist() == arr.tolist()


def _ingest_set_save_reopen(tmp_path, array, prefix, source_dtype="uint8",
                            source_pixrep=0):
    """Ingest a plain frame, replace its pixels in memory, save, reopen.

    The setter arm: what `set_pixel_data()` recorded is the only thing the
    sidecar has to decode by, so this is where a missing
    `PixelRepresentation` shows up.
    """
    src = tmp_path / f"{prefix}_src"
    src.mkdir(exist_ok=True)
    base = np.array(UNSIGNED_ROWS, dtype=source_dtype)
    db = str(tmp_path / f"{prefix}.db")

    session = DicomSession(persistence_file=db)
    try:
        _write_src(str(src), base, source_pixrep)
        session.ingest(str(src))
        instance = _only_instance(session)
        instance.set_pixel_data(array)
        after = dict(instance.attributes)
        session.save()
    finally:
        session.close()

    reopened = DicomSession(persistence_file=db)
    return reopened, db, after


# ---------------------------------------------------------------------------
# The signedness nothing recorded
# ---------------------------------------------------------------------------

def test_an_int16_array_set_in_memory_records_its_signedness(tmp_path):
    """The smallest test here, and the one that pins the root cause.

    `set_pixel_data()` wrote `BitsAllocated` from `array.itemsize` and
    wrote *nothing* about signedness -- not `PixelRepresentation`, not the
    dtype carrier -- so there was nothing recorded for the loader to read
    and nothing for the export to write. Red when the `0028,0103` write is
    removed.
    """
    src = tmp_path / "t4_src"
    src.mkdir()
    session = DicomSession(persistence_file=str(tmp_path / "t4.db"))
    try:
        _write_src(str(src), np.array(UNSIGNED_ROWS, dtype="uint8"), 0)
        session.ingest(str(src))
        instance = _only_instance(session)
        assert instance.attributes["0028,0103"] in (0, "0"), (
            "fixture never entered the arm under test: the source already "
            "declared a signed frame")

        instance.set_pixel_data(np.array(SIGNED_ROWS, dtype="int16"))

        assert instance.attributes["0028,0103"] == 1
        assert instance.attributes["0028,0100"] == 16
    finally:
        session.close()


def test_an_int16_array_set_in_memory_reloads_signed(tmp_path):
    """Red when the `0028,0103` write is removed: today it reloads `uint16`
    and `-8` comes back as `65528`."""
    session, _db, _after = _ingest_set_save_reopen(
        tmp_path, np.array(SIGNED_ROWS, dtype="int16"), "t5")
    try:
        got = _only_instance(session).get_pixel_data()
    finally:
        session.close()

    assert got.dtype == np.dtype("int16")
    assert got.tolist() == SIGNED_ROWS


def test_a_uint32_array_set_in_memory_reloads_unsigned(tmp_path):
    """The setter half of the 32-bit gap, which #386's text does not
    mention: `set_pixel_data(uint32_array)` raised the same loader
    `RuntimeError` on reload, because the loader had no 32-bit arm
    whatever put the bytes there. Red when the `32` row is removed."""
    session, _db, after = _ingest_set_save_reopen(
        tmp_path, np.array(UNSIGNED_ROWS, dtype="uint32"), "t9")
    try:
        got = _only_instance(session).get_pixel_data()
    finally:
        session.close()

    assert after["0028,0100"] == 32
    assert after["0028,0103"] == 0
    assert got.dtype == np.dtype("uint32")
    assert got.tolist() == UNSIGNED_ROWS


def test_the_exported_file_declares_the_signedness_of_an_array_set_in_memory(
        tmp_path):
    """The milestone test.

    Before the fix the file on disk declared `PixelRepresentation 0` beside
    signed bytes, a reader got 65528 where the caller wrote -8, and the
    audit log said `wrote 1 of 1 planned instances`. The point is not that
    the row disappears -- it is that the sentence becomes **true**, so the
    row is asserted still present and still saying 1 of 1.

    `flush_audit_queue()` first: the audit writer is a background thread,
    so an unflushed `SELECT` returns `[]` and "the row is missing" cannot
    be told from "the row has not landed yet" (§11.8).

    **`use_compression=False` is load-bearing, and P1 is why.** Pillow's
    JPEG 2000 encoder accepts mode `L` (uint8) and `I;16` (uint16) and
    nothing else, so *every* signed frame -- int8, int16, int32 alike --
    fails the *default* compressed path with `broken data stream when
    writing image file`. That is a pre-existing defect, verified on `main`
    at c925829 unmodified, and it is **#404**, not this issue: signedness
    is the axis rather than width, the loader is correct, and the fix is
    at the encoder. Leaving compression on here would turn this test red
    for a reason that has nothing to do with recording signedness, and it
    stays correct once #404 lands.
    """
    src = tmp_path / "t6_src"
    src.mkdir()
    out = tmp_path / "t6_out"
    db = str(tmp_path / "t6.db")

    session = DicomSession(persistence_file=db)
    try:
        _write_src(str(src), np.array(UNSIGNED_ROWS, dtype="uint8"), 0)
        session.ingest(str(src))
        _only_instance(session).set_pixel_data(
            np.array(SIGNED_ROWS, dtype="int16"))
        summary = session.export(str(out), format="dicom",
                                 use_compression=False, show_progress=False)
        session.store_backend.flush_audit_queue()
    finally:
        session.close()

    assert len(summary.written_uids) == 1
    assert summary.failures == []

    ds = _read_only_written(out)
    assert ds.PixelRepresentation == 1
    assert ds.BitsAllocated == 16
    assert ds.pixel_array.dtype == np.dtype("int16")
    assert ds.pixel_array.tolist() == SIGNED_ROWS

    rows = _audit_rows(db)
    exports = [details for action, _uid, details in rows if action == 'EXPORT']
    assert exports, "no EXPORT row in the audit log (was the queue flushed?)"
    assert any("wrote 1 of 1 planned instances" in details
               for details in exports), (
        f"the EXPORT row no longer claims a complete export: {exports}")
    assert not [r for r in rows if r[0] in ('DATA_LOSS', 'ERROR')], (
        f"an unexpected loss or error row was filed: "
        f"{[r for r in rows if r[0] in ('DATA_LOSS', 'ERROR')]}")


def test_a_float_array_leaves_pixel_representation_alone_and_nothing_reads_it(
        tmp_path):
    """The deliberate float exclusion, and the proof it is harmless.

    `set_pixel_data()` does not touch `0028,0103` for a float array --
    PS3.5 Section 8.2 forbids Pixel Representation beside a float pixel
    element and the export's float arm deletes it -- so an instance
    ingested as signed `int16` and then handed a `float32` array keeps a
    **stale `PixelRepresentation 1`** in the graph. That is only safe if
    nothing reads it, which is the assertion rather than the argument:
    the dtype carrier wins on the way back in (a float frame can only be
    rebuilt from a carried dtype, #183), and the exported file carries no
    Pixel Representation at all.

    Red if the exclusion is ever "tidied" into a `pop`, or if the loader
    is reordered to consult the descriptors before the carrier.
    """
    src = tmp_path / "flt_src"
    src.mkdir()
    out = tmp_path / "flt_out"
    db = str(tmp_path / "flt.db")

    session = DicomSession(persistence_file=db)
    try:
        _write_src(str(src), np.array(SIGNED_ROWS, dtype="int16"), 1)
        session.ingest(str(src))
        instance = _only_instance(session)
        assert instance.attributes["0028,0103"] in (1, "1"), (
            "fixture never entered the arm under test: the source is not signed")

        instance.set_pixel_data(np.array(UNSIGNED_ROWS, dtype="float32"))
        assert instance.attributes["0028,0103"] in (1, "1"), (
            "the float arm wrote or popped PixelRepresentation; it is "
            "documented as leaving it alone")
        assert instance.attributes[PIXEL_DTYPE_ATTR] == "float32"
        session.save()
    finally:
        session.close()

    session = DicomSession(persistence_file=db)
    try:
        got = _only_instance(session).get_pixel_data()
        assert got.dtype == np.dtype("float32"), (
            "the stale PixelRepresentation was read on the load path")
        assert got.tolist() == [[float(v) for v in row] for row in UNSIGNED_ROWS]
        session.export(str(out), format="dicom", use_compression=False,
                       show_progress=False)
    finally:
        session.close()

    ds = _read_only_written(out)
    assert "PixelRepresentation" not in ds, (
        "the stale PixelRepresentation reached the exported file")
    assert "BitsStored" not in ds and "HighBit" not in ds


# ---------------------------------------------------------------------------
# The carrier widens by exactly one dtype
# ---------------------------------------------------------------------------

def test_a_bool_array_set_in_memory_reloads_as_bool(tmp_path):
    """The one dtype no DICOM descriptor can name.

    numpy `bool_` and `uint8` both declare `BitsAllocated 8` with
    `PixelRepresentation 0`, so the descriptor pair cannot tell them
    apart and a mask set in memory came back as `uint8`.

    **The assertion is the dtype, and deliberately not the values.**
    Measured: a bool array already round-trips to `uint8` with
    `np.array_equal` True, because `True == 1`. A values-only test passes
    on unfixed code and pins nothing. Red when `"bool"` is dropped from
    `SIDECAR_DTYPE_NAMES`, or when the kind test in `set_pixel_data`
    narrows back to `== 'f'`.
    """
    mask = np.array([[True, False, True, False],
                     [False, True, False, True],
                     [True, True, False, False],
                     [False, False, True, True]])
    assert mask.dtype == np.dtype(bool)

    session, _db, after = _ingest_set_save_reopen(tmp_path, mask, "boolrt")
    try:
        got = _only_instance(session).get_pixel_data()
    finally:
        session.close()

    assert after[PIXEL_DTYPE_ATTR] == "bool"
    assert after["0028,0100"] == 8
    assert after["0028,0103"] == 0
    assert got.dtype == np.dtype(bool)
    assert got.tolist() == mask.tolist()


def test_a_bool_array_still_survives_the_default_compressed_export(tmp_path):
    """The regression the `bool` carrier would otherwise have introduced.

    Before the carrier widened, a bool frame reloaded as `uint8` and
    compressed cleanly. Once it reloads as `bool` it reaches
    `Image.fromarray` as mode `1`, which Pillow's JPEG 2000 encoder
    refuses -- so the widening would have turned a working default export
    into `Compression failed: broken data stream when writing image file`.
    `_compress_j2k` therefore views a bool frame as `uint8` before
    encoding: exact, and byte-identical to what the uncompressed path
    writes, since `set_pixel_data` already declares the frame
    `BitsAllocated 8, PixelRepresentation 0`.

    Compression is left at its default here on purpose -- that default is
    the whole point of the test.
    """
    mask = np.array([[True, False, True, False],
                     [False, True, False, True],
                     [True, True, False, False],
                     [False, False, True, True]])
    src = tmp_path / "boolx_src"
    src.mkdir()
    out = tmp_path / "boolx_out"
    db = str(tmp_path / "boolx.db")

    session = DicomSession(persistence_file=db)
    try:
        _write_src(str(src), np.array(UNSIGNED_ROWS, dtype="uint8"), 0)
        session.ingest(str(src))
        _only_instance(session).set_pixel_data(mask)
        summary = session.export(str(out), format="dicom", show_progress=False)
    finally:
        session.close()

    assert summary.failures == []
    assert len(summary.written_uids) == 1

    ds = _read_only_written(out)
    assert ds.file_meta.TransferSyntaxUID.name == \
        "JPEG 2000 Image Compression (Lossless Only)", (
            "fixture never entered the arm under test: the export was not "
            "compressed")
    assert ds.BitsAllocated == 8
    assert ds.PixelRepresentation == 0
    assert ds.pixel_array.tolist() == [[1, 0, 1, 0], [0, 1, 0, 1],
                                       [1, 1, 0, 0], [0, 0, 1, 1]]


def test_an_integer_array_deletes_the_dtype_carrier_a_float_array_left(tmp_path):
    """The half of #183 the widening could most easily break.

    The carrier DELETES as well as writes: replacing a float instance's
    pixels with an integer array and leaving the carrier behind would have
    the loader read those integers back as floats -- the same silent
    corruption from the other direction. Red when the `pop` becomes a
    no-op, and the reload assertion is what makes that more than a
    statement about a dict.
    """
    src = tmp_path / "carrier_src"
    src.mkdir()
    db = str(tmp_path / "carrier.db")

    session = DicomSession(persistence_file=db)
    try:
        _write_src(str(src), np.array(UNSIGNED_ROWS, dtype="uint8"), 0)
        session.ingest(str(src))
        instance = _only_instance(session)

        instance.set_pixel_data(np.array(UNSIGNED_ROWS, dtype="float32"))
        assert instance.attributes[PIXEL_DTYPE_ATTR] == "float32"

        instance.set_pixel_data(np.array(SIGNED_ROWS, dtype="int16"))
        assert PIXEL_DTYPE_ATTR not in instance.attributes
        session.save()
    finally:
        session.close()

    session = DicomSession(persistence_file=db)
    try:
        got = _only_instance(session).get_pixel_data()
    finally:
        session.close()

    assert got.dtype == np.dtype("int16")
    assert got.tolist() == SIGNED_ROWS


# ---------------------------------------------------------------------------
# The third copy of the bucketing rule
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("bits,pixrep,expected", [
    (8, 0, "uint8"), (8, 1, "int8"),
    (16, 0, "uint16"), (16, 1, "int16"),
    (32, 0, "uint32"), (32, 1, "int32"),
    (64, 0, "uint64"), (64, 1, "int64"),
    # The two declared widths no numpy array can have, which is why the
    # legacy bucketing survives as a fallback rather than being replaced.
    (1, 0, "uint8"), (12, 0, "uint16"), (12, 1, "int16"),
])
def test_the_integer_dtype_rule_has_one_spelling(bits, pixrep, expected):
    """`_integer_dtype` is the single rule the loader and `_compress_j2k`'s
    reconstruction branch both call.

    They held two copies of `uint16 if bits > 8 else uint8` and only one
    of them was fixed the last time; a signed frame rebuilt in the second
    came back unsigned for exactly the reason the loader's did. This pins
    the table *and* its fallback at the level where both callers share it.
    """
    from isocenter.io_handlers import _integer_dtype

    assert np.dtype(_integer_dtype(bits, pixrep)) == np.dtype(expected)


def test_the_j2k_reconstruction_branch_honours_pixel_representation():
    """The reconstruction branch, called directly.

    `_finalize_dataset` passes `pixel_array=arr` at its one production
    call site and the arms that leave `arr` as None leave `ds` without
    PixelData, so this branch is **not reachable from
    `session.export()`** -- it is reached only by a direct call, which is
    how `tests/test_compress_j2k_coverage.py` exercises it and how this
    test does. It is fixed anyway, because a second copy of a rule that
    disagrees with the first is what #386 was.

    Asserting on the array Pillow is handed rather than on the encoded
    bytes: the encoder itself refuses a signed frame outright (#404), so
    the bytes cannot be the observable here.
    """
    import isocenter.io_handlers as io_handlers
    from pydicom.dataset import Dataset

    seen = {}

    class _FakeImage:
        @staticmethod
        def fromarray(frame):
            seen['dtype'] = frame.dtype
            seen['values'] = frame.tolist()
            raise RuntimeError("stop here: the array is what is under test")

    ds = Dataset()
    ds.Rows = ds.Columns = 4
    ds.SamplesPerPixel = 1
    ds.NumberOfFrames = 1
    ds.BitsAllocated = 16
    ds.PixelRepresentation = 1
    ds.PixelData = np.array(SIGNED_ROWS, dtype="int16").tobytes()

    with pytest.raises(RuntimeError):
        with pytest.MonkeyPatch.context() as patcher:
            patcher.setattr(io_handlers, "Image", _FakeImage)
            io_handlers._compress_j2k(ds, pixel_array=None)

    assert seen['dtype'] == np.dtype("int16"), (
        "the reconstruction branch rebuilt a signed frame as unsigned")
    assert seen['values'] == SIGNED_ROWS


# ---------------------------------------------------------------------------
# The dtypes the sidecar cannot honestly carry
# ---------------------------------------------------------------------------

def _exotic_dtypes():
    """The refused population, built rather than listed where numpy varies."""
    cases = [
        ("complex64", np.zeros((4, 4), dtype="complex64")),
        ("complex128", np.zeros((4, 4), dtype="complex128")),
        ("object", np.zeros((4, 4), dtype=object)),
        ("str", np.array([["a", "b"], ["c", "d"]])),
        ("structured", np.zeros((4, 4), dtype=[("x", "u1"), ("y", "u1")])),
        ("datetime64", np.zeros((4, 4), dtype="datetime64[s]")),
    ]
    if hasattr(np, "float128"):
        cases.append(("float128", np.zeros((4, 4), dtype=np.float128)))
    return cases


@pytest.mark.parametrize("label,array", _exotic_dtypes(),
                         ids=[label for label, _ in _exotic_dtypes()])
def test_set_pixel_data_refuses_a_dtype_the_sidecar_cannot_carry(label, array):
    """`ValueError`, at the call, naming the dtype and the accepted set.

    `set_pixel_data` accepted anything with a `.dtype`. A `complex64`
    array recorded no carrier, declared `BitsAllocated 128` and reloaded
    through the loader's fallback as `uint16` -- silently, and with the
    values wrong. The guard states the accepted set **positively**, so a
    numpy release that adds a kind is refused by default rather than
    admitted by omission; `float128` is the worked example, since it is
    kind `'f'` and the positive rule catches it with no special case.

    Red when the guard is removed.
    """
    from isocenter.entities import Instance

    instance = Instance(sop_instance_uid="1.2.386.refuse")
    with pytest.raises(ValueError) as excinfo:
        instance.set_pixel_data(array)

    message = str(excinfo.value)
    assert array.dtype.name in message, (
        f"the refusal does not name the dtype it refused: {message}")
    # The accepted set, stated: three anchors rather than one word, so a
    # message that merely contains "dtype" cannot satisfy this.
    for anchor in ("float32", "bool", "PixelRepresentation"):
        assert anchor in message, (
            f"the refusal does not state the accepted set ({anchor!r} "
            f"missing): {message}")


def test_a_refused_dtype_leaves_the_instance_exactly_as_it_was(tmp_path):
    """Validate first, as a fact rather than a comment.

    `set_pixel_data` assigns `self.pixel_array` as its *first* statement
    and writes several descriptors after it, so a `ValueError` raised
    part-way leaves an instance describing an array it does not hold -- a
    new silence inside the fix.

    **The load-bearing assertion is that `get_pixel_data()` still returns
    the original `int16` frame.** The attributes comparison catches a
    guard moved below the first `_write_int_if_changed`; only the frame
    assertion catches one moved below `self.pixel_array = array`, which is
    the worse of the two and leaves every descriptor untouched.
    """
    from isocenter.entities import Instance

    instance = Instance(sop_instance_uid="1.2.386.intact")
    good = np.array(SIGNED_ROWS, dtype="int16")
    instance.set_pixel_data(good)

    before_attributes = dict(instance.attributes)
    before_revision = instance._revision
    assert before_attributes["0028,0100"] == 16
    assert before_attributes["0028,0103"] == 1
    assert PIXEL_DTYPE_ATTR not in before_attributes

    with pytest.raises(ValueError):
        instance.set_pixel_data(np.zeros((4, 4), dtype="complex64"))

    assert instance.attributes == before_attributes, (
        "a refused array left descriptors behind: the instance now "
        "describes an array it never took")
    assert instance._revision == before_revision, (
        "a refused array advanced the revision, so the next save would "
        "rewrite a row for a change that did not happen")
    got = instance.get_pixel_data()
    assert got.dtype == np.dtype("int16")
    assert got.tolist() == SIGNED_ROWS


def test_a_big_endian_array_is_normalized_not_refused(tmp_path):
    """Byte order is the round-trippable case, and is normalized.

    A big-endian `>i2` is kind `'i'` and itemsize 2, so it passes every
    clause of the accept rule -- yet the sidecar stores raw bytes and the
    loader reads them with a native-order dtype, so before this it
    reloaded byte-swapped. Refusing it would reject an array that is
    exactly representable and merely spelled unusually.

    The reload is compared against a **literal**, never against a `>i2`
    array built in the same test: that comparison is true even when both
    sides are wrong. Red when the normalization is deleted, and red when
    the accept rule is tightened to refuse non-native order.
    """
    big = np.array(SIGNED_ROWS, dtype=">i2")
    assert big.dtype.byteorder in ('>',), "fixture is not actually big-endian"

    session, _db, after = _ingest_set_save_reopen(tmp_path, big, "bigend")
    try:
        got = _only_instance(session).get_pixel_data()
    finally:
        session.close()

    assert after["0028,0103"] == 1
    assert after["0028,0100"] == 16
    assert got.tolist() == SIGNED_ROWS


def test_a_big_endian_and_a_little_endian_array_store_identical_bytes():
    """What "normalized" means, said in bytes.

    `>i2` and `<i2` describe the same numbers; after normalization they
    produce the same frame, which is what a caller means by handing over
    either.
    """
    from isocenter.entities import Instance

    big, little = Instance("1.2.386.be"), Instance("1.2.386.le")
    big.set_pixel_data(np.array(SIGNED_ROWS, dtype=">i2"))
    little.set_pixel_data(np.array(SIGNED_ROWS, dtype="<i2"))

    assert big.pixel_array.tobytes() == little.pixel_array.tobytes()
    assert big.pixel_array.dtype.byteorder in ('=', '|')
    assert big.attributes["0028,0103"] == little.attributes["0028,0103"] == 1
