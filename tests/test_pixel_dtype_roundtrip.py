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
            "SELECT action_type, status, details FROM audit_log").fetchall()


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

    return DicomSession(persistence_file=db), db


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
