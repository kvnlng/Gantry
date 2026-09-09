"""#404 end to end: signed 16-bit through session.export(), three ways.

  1. default (use_compression=True) on unmodified code -- the defect
  2. use_compression=False           -- the control that already works
  3. default, with _compress_j2k monkeypatched to the proposed imagecodecs
     encoder -- the proof that the design fixes it, measured rather than argued

Nothing here edits the package. Case 3 replaces a module attribute inside this
process only.
"""
import io
import os
import sqlite3
import sys
import tempfile

import numpy as np
import pydicom
from pydicom.dataset import FileDataset, FileMetaDataset
from pydicom.encaps import encapsulate
from pydicom.uid import ExplicitVRLittleEndian, JPEG2000Lossless, generate_uid

import isocenter
from isocenter import io_handlers
from isocenter.session import DicomSession

CT = None


def ct_frame():
    """A CT-like signed frame: Hounsfield units, air through bone."""
    a = np.linspace(-1024, 3071, 64 * 64).astype(np.int16).reshape(64, 64)
    a[0, 0] = -1024
    a[0, 1] = 3071
    a[1, 0] = -32768
    a[1, 1] = 32767
    return a


def write_src(folder, arr):
    meta = FileMetaDataset()
    meta.MediaStorageSOPClassUID = "1.2.840.10008.5.1.4.1.1.2"
    meta.MediaStorageSOPInstanceUID = generate_uid()
    meta.TransferSyntaxUID = ExplicitVRLittleEndian
    ds = FileDataset(None, {}, file_meta=meta, preamble=b"\0" * 128)
    ds.PatientID = "PAT404"
    ds.PatientName = "DOE^JANE"
    ds.StudyInstanceUID = generate_uid()
    ds.SeriesInstanceUID = generate_uid()
    ds.SOPInstanceUID = meta.MediaStorageSOPInstanceUID
    ds.SOPClassUID = meta.MediaStorageSOPClassUID
    ds.Modality = "CT"
    ds.SeriesNumber = 1
    ds.InstanceNumber = 1
    ds.StudyDate = "20230101"
    ds.StudyTime = "120000"
    ds.SliceThickness = "1.0"
    ds.KVP = "120"
    ds.ImagePositionPatient = ["0.0", "0.0", "0.0"]
    ds.ImageOrientationPatient = ["1.0", "0.0", "0.0", "0.0", "1.0", "0.0"]
    ds.PixelSpacing = ["0.5", "0.5"]
    ds.Rows, ds.Columns = arr.shape
    ds.BitsAllocated = 16
    ds.BitsStored = 16
    ds.HighBit = 15
    ds.SamplesPerPixel = 1
    ds.PhotometricInterpretation = "MONOCHROME2"
    ds.PixelRepresentation = 1
    ds.PixelData = arr.tobytes()
    ds.save_as(os.path.join(folder, "ct.dcm"), enforce_file_format=True)


def find_dcm(folder):
    out = []
    for root, _dirs, names in os.walk(folder):
        for n in names:
            if n.endswith(".dcm"):
                out.append(os.path.join(root, n))
    return out


def patched_compress_j2k(ds, pixel_array=None):
    """The proposed encoder, in the smallest form that can be measured.

    imagecodecs is already an install_requires dependency and is already
    imported by isocenter/imagecodecs_handler.py for the decode side.
    """
    import imagecodecs
    arr = pixel_array
    if arr is None:
        return
    frames = int(getattr(ds, "NumberOfFrames", 1) or 1)
    samples = int(getattr(ds, "SamplesPerPixel", 1) or 1)
    rows = int(ds.Rows)
    cols = int(ds.Columns)
    if arr.ndim == 1:
        shape = (frames, rows, cols, samples) if samples > 1 else (frames, rows, cols)
        arr = arr.reshape(shape)
    elif frames == 1:
        arr = arr.reshape((1,) + arr.shape)
    blobs = []
    for i in range(frames):
        blobs.append(imagecodecs.jpeg2k_encode(
            np.ascontiguousarray(arr[i]), level=0, codecformat="J2K"))
    ds.PixelData = encapsulate(blobs)
    ds.file_meta.TransferSyntaxUID = JPEG2000Lossless


def run(label, arr, compression, patch):
    tmp = tempfile.mkdtemp(prefix="p404_")
    src = os.path.join(tmp, "src")
    os.makedirs(src)
    write_src(src, arr)
    db = os.path.join(tmp, "s.db")
    out = os.path.join(tmp, "out")
    print("")
    print("=== " + label)
    original = io_handlers._compress_j2k
    if patch:
        io_handlers._compress_j2k = patched_compress_j2k
    summary = None
    err = None
    s = DicomSession(persistence_file=db)
    try:
        s.ingest(src)
        s.save()
        try:
            summary = s.export(out, format="dicom", show_progress=False,
                               use_compression=compression)
        except Exception as exc:
            err = "%s: %s" % (type(exc).__name__, str(exc)[:160])
    finally:
        s.close()
        io_handlers._compress_j2k = original

    print("  export raised      :", err)
    print("  written            :", getattr(summary, "written", None))
    print("  written_uids       :", getattr(summary, "written_uids", None))
    print("  failures           :", getattr(summary, "failures", None))
    files = find_dcm(out)
    print("  files on disk      :", len(files))
    if files:
        d = pydicom.dcmread(files[0])
        print("  TransferSyntaxUID  :", d.file_meta.TransferSyntaxUID,
              "(" + str(d.file_meta.TransferSyntaxUID.name) + ")")
        print("  BitsAllocated      :", d.BitsAllocated,
              " PixelRepresentation:", d.PixelRepresentation,
              " BitsStored:", getattr(d, "BitsStored", None))
        raw = d.PixelData
        head = raw[:16].hex() if isinstance(raw, (bytes, bytearray)) else "?"
        print("  PixelData head     :", head)
        try:
            back = d.pixel_array
            print("  pixel_array dtype  :", back.dtype)
            print("  BIT-EXACT          :", bool(np.array_equal(back, arr)),
                  " (dtype match:", back.dtype == arr.dtype, ")")
            if not np.array_equal(back, arr):
                diff = np.argwhere(back != arr)
                print("  first mismatch     :", diff[:3].tolist())
        except Exception as exc:
            print("  pixel_array raised :", type(exc).__name__, str(exc)[:120])
    with sqlite3.connect(db) as conn:
        rows = conn.execute(
            "SELECT action_type, details FROM audit_log").fetchall()
    for a, dtl in rows:
        if a in ("EXPORT", "ERROR", "DATA_LOSS"):
            print("  AUDIT %-9s %s" % (a, (dtl or "")[:200]))


def build_ds(arr):
    meta = FileMetaDataset()
    meta.MediaStorageSOPClassUID = "1.2.840.10008.5.1.4.1.1.2"
    meta.MediaStorageSOPInstanceUID = generate_uid()
    meta.TransferSyntaxUID = ExplicitVRLittleEndian
    ds = FileDataset(None, {}, file_meta=meta, preamble=b"\0" * 128)
    ds.PatientID = "PAT404"
    ds.PatientName = "DOE^JANE"
    ds.StudyInstanceUID = generate_uid()
    ds.SeriesInstanceUID = generate_uid()
    ds.SOPInstanceUID = meta.MediaStorageSOPInstanceUID
    ds.SOPClassUID = meta.MediaStorageSOPClassUID
    ds.Modality = "CT"
    ds.SeriesNumber = 1
    ds.InstanceNumber = 1
    ds.StudyDate = "20230101"
    ds.StudyTime = "120000"
    ds.SliceThickness = "1.0"
    ds.KVP = "120"
    ds.ImagePositionPatient = ["0.0", "0.0", "0.0"]
    ds.ImageOrientationPatient = ["1.0", "0.0", "0.0", "0.0", "1.0", "0.0"]
    ds.PixelSpacing = ["0.5", "0.5"]
    ds.Rows, ds.Columns = arr.shape
    ds.BitsAllocated = 16
    ds.BitsStored = 16
    ds.HighBit = 15
    ds.SamplesPerPixel = 1
    ds.PhotometricInterpretation = "MONOCHROME2"
    ds.PixelRepresentation = 1
    return ds


def unit_case(arr):
    """The unit that changes: _finalize_dataset(ds, 'j2k', pixel_array=arr).

    export() runs its workers in spawned processes, so an in-process patch of
    _compress_j2k cannot reach them (CLAUDE.md: ISOCENTER_FORCE_THREADS does
    not reach export(), #185). The process boundary is not what is under test;
    the encoder is. This calls the real _finalize_dataset -- IOD validation
    included -- with the current encoder and then with the proposed one.
    """
    print("")
    print("=== 3. THE UNIT: _finalize_dataset(ds, 'j2k', pixel_array=arr)")
    tmp = tempfile.mkdtemp(prefix="p404u_")

    for label, fn in (("current  (Pillow)", io_handlers._compress_j2k),
                      ("proposed (imagecodecs)", patched_compress_j2k)):
        ds = build_ds(arr)
        original = io_handlers._compress_j2k
        io_handlers._compress_j2k = fn
        try:
            io_handlers.DicomExporter._finalize_dataset(ds, "j2k", pixel_array=arr)
            path = os.path.join(tmp, label.split()[0] + ".dcm")
            ds.save_as(path, enforce_file_format=True)
            d = pydicom.dcmread(path)
            back = d.pixel_array
            print("  %-24s : WROTE %s" % (label, os.path.basename(path)))
            print("      TransferSyntaxUID  : %s" % d.file_meta.TransferSyntaxUID)
            print("      BitsAllocated %s  BitsStored %s  PixelRepresentation %s"
                  % (d.BitsAllocated, d.BitsStored, d.PixelRepresentation))
            print("      first fragment head: %s"
                  % bytes(d.PixelData[8:16]).hex())
            print("      decoded dtype      : %s" % back.dtype)
            print("      BIT-EXACT          : %s   dtype match: %s"
                  % (bool(np.array_equal(back, arr)), back.dtype == arr.dtype))
        except Exception as exc:
            print("  %-24s : %s: %s"
                  % (label, type(exc).__name__, str(exc)[:110]))
        finally:
            io_handlers._compress_j2k = original


def main():
    print("isocenter:", isocenter.__file__)
    print("python   :", sys.version.split()[0])
    arr = ct_frame()
    run("1. DEFECT: default export (use_compression=True), unmodified code",
        arr, True, False)
    run("2. CONTROL: use_compression=False, unmodified code",
        arr, False, False)
    unit_case(arr)


if __name__ == "__main__":
    main()
