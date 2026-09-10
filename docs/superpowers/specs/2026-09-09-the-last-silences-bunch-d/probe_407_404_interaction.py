"""#407 x #404: does fixing the BOT join move the `(2, True)` refusal?

#404 refuses 16-bit multi-sample J2K on the ground that this library
cannot read its own export back. #407 says that ground is contingent on
the BOT-join bug. This measures the four readers separately:

  A. `pydicom.dcmread(f).pixel_array`      -- what a user sees
  B. `isocenter.io_handlers._decode_pixels` -- what `ingest_worker` uses
  C. `imagecodecs_handler.get_pixel_data`   -- what #407 is about
  D. `Instance.get_pixel_data()` on a file  -- the graph-level read
  E. `session.ingest(folder)`               -- the #404 docstring's claim

Run it once on `main` and once with the #407 fix applied in the worktree.
"""
import glob
import os
import sys
import tempfile

import numpy as np
import pydicom
from pydicom.dataset import FileDataset, FileMetaDataset
from pydicom.encaps import encapsulate
from pydicom.uid import JPEG2000Lossless, generate_uid

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))))

import imagecodecs  # noqa: E402
import isocenter  # noqa: E402
from isocenter import imagecodecs_handler  # noqa: E402
from isocenter.entities import Instance  # noqa: E402
from isocenter.io_handlers import _decode_pixels  # noqa: E402
from isocenter.session import DicomSession  # noqa: E402


def write_16bit_rgb_j2k(folder):
    """A file `_J2K_ENCODABLE_FRAMES` refuses to write, written by hand."""
    arr = (np.arange(4 * 4 * 3, dtype=np.uint16) * 1000).reshape(4, 4, 3)
    codestream = imagecodecs.jpeg2k_encode(arr, level=0, codecformat="J2K")
    meta = FileMetaDataset()
    meta.MediaStorageSOPClassUID = "1.2.840.10008.5.1.4.1.1.7"
    meta.MediaStorageSOPInstanceUID = generate_uid()
    meta.TransferSyntaxUID = JPEG2000Lossless
    ds = FileDataset(None, {}, file_meta=meta, preamble=b"\0" * 128)
    ds.PatientID, ds.PatientName = "PAT407", "DOE^JANE"
    ds.StudyInstanceUID, ds.SeriesInstanceUID = generate_uid(), generate_uid()
    ds.SOPInstanceUID = meta.MediaStorageSOPInstanceUID
    ds.SOPClassUID = meta.MediaStorageSOPClassUID
    ds.Modality, ds.SeriesNumber, ds.InstanceNumber = "OT", 1, 1
    ds.StudyDate = "20230101"
    ds.Rows = ds.Columns = 4
    ds.SamplesPerPixel = 3
    ds.PhotometricInterpretation = "RGB"
    ds.PlanarConfiguration = 0
    ds.BitsAllocated = ds.BitsStored = 16
    ds.HighBit = 15
    ds.PixelRepresentation = 0
    ds.PixelData = encapsulate([codestream])
    path = os.path.join(folder, "rgb16.dcm")
    ds.save_as(path, enforce_file_format=True)
    return path, arr


def main():
    print("isocenter:", isocenter.__file__)
    print("imagecodecs:", imagecodecs.__version__, "pydicom:", pydicom.__version__)
    tmp = tempfile.mkdtemp(prefix="p407x_")
    src = os.path.join(tmp, "src")
    os.makedirs(src)
    path, want = write_16bit_rgb_j2k(src)

    ds = pydicom.dcmread(path)
    print("\nA. pydicom .pixel_array:")
    try:
        got = ds.pixel_array
        print("   OK", got.dtype, got.shape, "equal:", np.array_equal(got, want))
    except Exception as e:
        print("   RAISED:", type(e).__name__, str(e)[:180])

    print("B. io_handlers._decode_pixels (what ingest_worker calls):")
    try:
        got, pi = _decode_pixels(pydicom.dcmread(path))
        print("   OK", got.dtype, got.shape, pi, "equal:",
              np.array_equal(got.reshape(want.shape), want))
    except Exception as e:
        print("   RAISED:", type(e).__name__, str(e)[:180])

    print("C. imagecodecs_handler.get_pixel_data:")
    try:
        got = imagecodecs_handler.get_pixel_data(pydicom.dcmread(path))
        print("   OK", got.dtype, got.shape, "equal:",
              np.array_equal(got.reshape(want.shape), want))
    except Exception as e:
        print("   RAISED:", type(e).__name__, str(e)[:180])

    print("D. Instance.get_pixel_data() with file_path:")
    inst = Instance(sop_instance_uid=str(ds.SOPInstanceUID))
    inst.file_path = path
    try:
        got = inst.get_pixel_data()
        print("   OK", None if got is None else (got.dtype, got.shape),
              "equal:", got is not None and
              np.array_equal(got.reshape(want.shape), want))
    except Exception as e:
        print("   RAISED:", type(e).__name__, str(e)[:180])

    print("E. session.ingest():")
    db = os.path.join(tmp, "s.db")
    s = DicomSession(persistence_file=db)
    try:
        res = s.ingest(src)
        print("   result:", res)
        n = sum(len(se.instances) for p in s.store.patients
                for st in p.studies for se in st.series)
        print("   instances in graph:", n)
    finally:
        s.close()
    import sqlite3
    with sqlite3.connect(db) as conn:
        for a, d in conn.execute(
                "SELECT action_type, details FROM audit_log").fetchall():
            print(f"   AUDIT {a}: {str(d)[:160]}")
    for f in glob.glob(os.path.join(tmp, "*")):
        pass


if __name__ == "__main__":
    main()
