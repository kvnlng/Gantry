"""#386 export-side speech: what does the session say about the dtype it lost?"""
import glob, os, sqlite3, sys, tempfile
import numpy as np, pydicom
from pydicom.dataset import FileDataset, FileMetaDataset
from pydicom.uid import ExplicitVRLittleEndian, generate_uid

import isocenter
from isocenter.session import DicomSession


def write_src(folder, dtype, bits, pixrep):
    meta = FileMetaDataset()
    meta.MediaStorageSOPClassUID = "1.2.840.10008.5.1.4.1.1.7"
    meta.MediaStorageSOPInstanceUID = generate_uid()
    meta.TransferSyntaxUID = ExplicitVRLittleEndian
    ds = FileDataset(None, {}, file_meta=meta, preamble=b"\0"*128)
    ds.PatientID, ds.PatientName = "PAT386", "DOE^JOHN"
    ds.StudyInstanceUID, ds.SeriesInstanceUID = generate_uid(), generate_uid()
    ds.SOPInstanceUID = meta.MediaStorageSOPInstanceUID
    ds.SOPClassUID = meta.MediaStorageSOPClassUID
    ds.Modality, ds.SeriesNumber, ds.InstanceNumber = "OT", 1, 1
    ds.StudyDate = "20230101"
    ds.Rows = ds.Columns = 4
    ds.BitsAllocated, ds.BitsStored, ds.HighBit = bits, bits, bits - 1
    ds.SamplesPerPixel = 1
    ds.PhotometricInterpretation = "MONOCHROME2"
    ds.PixelRepresentation = pixrep
    arr = np.arange(16, dtype=dtype).reshape(4, 4)
    ds.PixelData = arr.tobytes()
    ds.save_as(os.path.join(folder, "one.dcm"), enforce_file_format=True)
    return arr


def instances(session):
    for pt in session.store.patients:
        for st in pt.studies:
            for se in st.series:
                yield from se.instances


def run(label, src_dtype, bits, pixrep, setter=None):
    tmp = tempfile.mkdtemp(prefix="p386e_")
    src = os.path.join(tmp, "src"); os.makedirs(src)
    src_arr = write_src(src, src_dtype, bits, pixrep)
    db = os.path.join(tmp, "s.db")
    out = os.path.join(tmp, "out")
    print(f"\n=== {label}")
    s = DicomSession(persistence_file=db)
    try:
        s.ingest(src)
        if setter is not None:
            for inst in instances(s):
                inst.set_pixel_data(setter)
        s.save()
        s.export(out, format="dicom", show_progress=False)
        rep = None
    finally:
        s.close()
    files = glob.glob(os.path.join(out, "**", "*.dcm"), recursive=True)
    print("  files written:", len(files))
    if files:
        d = pydicom.dcmread(files[0])
        print("  file BitsAllocated:", d.BitsAllocated,
              "PixelRepresentation:", getattr(d, "PixelRepresentation", None))
        try:
            print("  file pixel_array dtype:", d.pixel_array.dtype,
                  "first 4:", d.pixel_array.ravel()[:4].tolist())
            want = setter if setter is not None else src_arr
            print("  file values == intended:", np.array_equal(d.pixel_array, want))
        except Exception as e:
            print("  file pixel_array raised:", type(e).__name__, str(e)[:150])
    with sqlite3.connect(db) as conn:
        rows = conn.execute("SELECT action_type, details FROM audit_log").fetchall()
    for a, dtl in rows:
        print(f"  AUDIT {a}: {dtl[:200]}")
    print("  report grade:", getattr(rep, "grade", None) or
          (rep.get("grade") if isinstance(rep, dict) else rep))


def main():
    print("isocenter:", isocenter.__file__)
    import traceback
    for args, kw in [
        (("A: ingested uint32 (RTDOSE-shaped), untouched, exported", np.uint32, 32, 0), {}),
        (("B: ingested uint8, set_pixel_data(int16), exported", np.uint8, 8, 0),
         {"setter": (np.arange(16, dtype=np.int16) - 8).reshape(4, 4)}),
        (("C: ingested uint8, set_pixel_data(bool), exported", np.uint8, 8, 0),
         {"setter": (np.arange(16) % 2 == 0).reshape(4, 4)}),
    ]:
        try:
            run(*args, **kw)
        except Exception:
            traceback.print_exc()


if __name__ == '__main__':
    main()
