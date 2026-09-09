"""#386: dtype round-trip through the sidecar, measured on both sides."""
import glob, os, sqlite3, sys, tempfile, traceback
import numpy as np, pydicom
from pydicom.dataset import FileDataset, FileMetaDataset
from pydicom.uid import ExplicitVRLittleEndian, generate_uid

import isocenter
from isocenter.session import DicomSession
from isocenter.pixel_geometry import PIXEL_DTYPE_ATTR


def write_src(folder, name, dtype, bits, pixrep, rows=4, cols=4):
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
    ds.Rows, ds.Columns = rows, cols
    ds.BitsAllocated, ds.BitsStored, ds.HighBit = bits, bits, bits - 1
    ds.SamplesPerPixel = 1
    ds.PhotometricInterpretation = "MONOCHROME2"
    ds.PixelRepresentation = pixrep
    arr = np.arange(rows*cols, dtype=dtype).reshape(rows, cols)
    if np.dtype(dtype).kind == 'i':
        arr = arr - 3
    ds.PixelData = arr.tobytes()
    p = os.path.join(folder, name)
    ds.save_as(p, enforce_file_format=True)
    return arr, ds.SOPInstanceUID


def instances(session):
    for pt in session.store.patients:
        for st in pt.studies:
            for se in st.series:
                yield from se.instances


def ingest_case(label, dtype, bits, pixrep):
    tmp = tempfile.mkdtemp(prefix="p386_")
    src = os.path.join(tmp, "src"); os.makedirs(src)
    arr, uid = write_src(src, "one.dcm", dtype, bits, pixrep)
    back = pydicom.dcmread(os.path.join(src, "one.dcm"))
    print(f"\n--- INGEST {label}: source dtype={np.dtype(dtype).name} "
          f"BitsAllocated={bits} PixelRepresentation={pixrep} "
          f"pydicom pixel_array dtype={back.pixel_array.dtype}")
    db = os.path.join(tmp, "s.db")
    s = DicomSession(persistence_file=db)
    try:
        s.ingest(src)
        s.save()
    finally:
        s.close()
    s = DicomSession(persistence_file=db)
    try:
        for inst in instances(s):
            print("   stored PIXEL_DTYPE_ATTR:", inst.attributes.get(PIXEL_DTYPE_ATTR))
            print("   stored 0028,0100:", inst.attributes.get("0028,0100"),
                  " 0028,0103:", inst.attributes.get("0028,0103"))
            try:
                got = inst.get_pixel_data()
                print("   RELOADED dtype:", got.dtype, "shape", got.shape,
                      "first row", got.ravel()[:4].tolist())
                print("   VALUES EQUAL SOURCE:", np.array_equal(got, arr))
            except Exception as e:
                print("   RELOAD RAISED:", type(e).__name__, str(e)[:220])
    finally:
        s.close()


def setter_case(label, arr):
    tmp = tempfile.mkdtemp(prefix="p386s_")
    src = os.path.join(tmp, "src"); os.makedirs(src)
    write_src(src, "one.dcm", np.uint8, 8, 0)
    db = os.path.join(tmp, "s.db")
    print(f"\n--- SET_PIXEL_DATA {label}: array dtype={arr.dtype} shape={arr.shape}")
    s = DicomSession(persistence_file=db)
    try:
        s.ingest(src)
        for inst in instances(s):
            inst.set_pixel_data(arr)
            print("   after set: PIXEL_DTYPE_ATTR=", inst.attributes.get(PIXEL_DTYPE_ATTR),
                  " 0028,0100=", inst.attributes.get("0028,0100"),
                  " 0028,0103=", inst.attributes.get("0028,0103"))
        s.save()
    finally:
        s.close()
    s = DicomSession(persistence_file=db)
    try:
        for inst in instances(s):
            try:
                got = inst.get_pixel_data()
                print("   RELOADED dtype:", got.dtype, "shape", got.shape)
                print("   VALUES EQUAL SOURCE:", got.shape == arr.shape and np.array_equal(got, arr))
                print("   raw first 4:", got.ravel()[:4].tolist(), "vs", arr.ravel()[:4].tolist())
            except Exception as e:
                print("   RELOAD RAISED:", type(e).__name__, str(e)[:220])
    finally:
        s.close()


def main():
    print("isocenter:", isocenter.__file__)
    print("python:", sys.version.split()[0], "gil:", sys._is_gil_enabled(),
          "numpy:", np.__version__, "pydicom:", pydicom.__version__)
    for label, dtype, bits, pixrep in [
        ("uint8", np.uint8, 8, 0),
        ("int8", np.int8, 8, 1),
        ("uint16", np.uint16, 16, 0),
        ("int16", np.int16, 16, 1),
        ("uint32", np.uint32, 32, 0),
        ("int32", np.int32, 32, 1),
    ]:
        try:
            ingest_case(label, dtype, bits, pixrep)
        except Exception:
            traceback.print_exc()
    for label, arr in [
        ("int16", (np.arange(16, dtype=np.int16) - 8).reshape(4, 4)),
        ("bool", (np.arange(16) % 2 == 0).reshape(4, 4)),
        ("uint32", np.arange(16, dtype=np.uint32).reshape(4, 4)),
        ("float32", np.arange(16, dtype=np.float32).reshape(4, 4)),
        ("int8", (np.arange(16, dtype=np.int8) - 8).reshape(4, 4)),
    ]:
        try:
            setter_case(label, arr)
        except Exception:
            traceback.print_exc()


if __name__ == '__main__':
    main()
