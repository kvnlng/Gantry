"""#406 severe case, both compression settings, with the audit log.

`use_compression=True` is the default and is on the frozen surface, so
what changes there matters; `use_compression=False` is the setting the
regression test can assert a written FILE on, because a 32-bit integer
frame is refused by `_J2K_ENCODABLE_FRAMES` once the dtype is read
correctly.

Run once on `main` and once with the #406 fix applied in the worktree.
"""
import glob
import os
import sqlite3
import sys
import tempfile

import numpy as np
import pydicom
from pydicom.dataset import FileDataset, FileMetaDataset
from pydicom.uid import ExplicitVRLittleEndian, generate_uid

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))))

import isocenter  # noqa: E402
from isocenter.session import DicomSession  # noqa: E402


def write_float_src(folder, arr):
    meta = FileMetaDataset()
    meta.MediaStorageSOPClassUID = "1.2.840.10008.5.1.4.1.1.7"
    meta.MediaStorageSOPInstanceUID = generate_uid()
    meta.TransferSyntaxUID = ExplicitVRLittleEndian
    ds = FileDataset(None, {}, file_meta=meta, preamble=b"\0" * 128)
    ds.PatientID, ds.PatientName = "PAT406C", "DOE^JOHN"
    ds.StudyInstanceUID, ds.SeriesInstanceUID = generate_uid(), generate_uid()
    ds.SOPInstanceUID = meta.MediaStorageSOPInstanceUID
    ds.SOPClassUID = meta.MediaStorageSOPClassUID
    ds.Modality, ds.SeriesNumber, ds.InstanceNumber = "OT", 1, 1
    ds.StudyDate = "20230101"
    ds.Rows, ds.Columns = arr.shape
    ds.SamplesPerPixel = 1
    ds.PhotometricInterpretation = "MONOCHROME2"
    ds.BitsAllocated = ds.BitsStored = 32
    ds.HighBit = 31
    ds.FloatPixelData = arr.tobytes()
    ds.save_as(os.path.join(folder, "one.dcm"), enforce_file_format=True)


def only_instance(session):
    for pt in session.store.patients:
        for st in pt.studies:
            for se in st.series:
                for inst in se.instances:
                    return inst
    return None


def run(compress):
    print(f"\n=== use_compression={compress}")
    tmp = tempfile.mkdtemp(prefix="p406c_")
    src = os.path.join(tmp, "src")
    os.makedirs(src)
    floats = (np.arange(16, dtype=np.float32) + 0.5).reshape(4, 4)
    want = floats.view(np.int32)
    write_float_src(src, floats)
    db = os.path.join(tmp, "s.db")
    out = os.path.join(tmp, "out")
    s = DicomSession(persistence_file=db)
    try:
        s.ingest(src)
        inst = only_instance(s)
        s.save(sync=True)
        sidecar = db.replace(".db", "_pixels.bin")
        size_before = os.path.getsize(sidecar)
        inst.set_pixel_data(want)
        s.save(sync=True)
        size_after = os.path.getsize(sidecar)
        print("  sidecar grew by:", size_after - size_before,
              "bytes (0 == the dedup arm ran)")
        try:
            s.export(out, format="dicom", show_progress=False,
                     use_compression=compress)
            print("  export: OK")
        except Exception as e:
            print("  export RAISED:", type(e).__name__, str(e)[:200])
    finally:
        s.close()
    files = glob.glob(os.path.join(out, "**", "*.dcm"), recursive=True)
    print("  files:", len(files))
    if files:
        d = pydicom.dcmread(files[0])
        print("   FloatPixelData:", "FloatPixelData" in d,
              " PixelData:", "PixelData" in d,
              " BitsAllocated:", getattr(d, "BitsAllocated", None),
              " PixelRepresentation:", getattr(d, "PixelRepresentation", None))
        try:
            got = d.pixel_array
            print("   dtype:", got.dtype, " equals the int32 array set:",
                  np.array_equal(got.reshape(want.shape), want))
        except Exception as e:
            print("   pixel_array RAISED:", type(e).__name__, str(e)[:120])
    with sqlite3.connect(db) as conn:
        rows = conn.execute(
            "SELECT action_type, details FROM audit_log").fetchall()
    for a, dtl in rows:
        print(f"   AUDIT {a}: {str(dtl)[:150]}")


def main():
    print("isocenter:", isocenter.__file__)
    run(True)
    run(False)


if __name__ == "__main__":
    main()
