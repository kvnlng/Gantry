"""#406: a dtype-only `set_pixel_data()` and the frame dedup.

Three questions, measured on one graph each:

  1. does the dedup arm leave `SidecarPixelLoader.pixel_dtype` stale?
  2. does the live object then hand back the OLD dtype after an unload?
  3. does the EXPORTED file carry the wrong pixel container?

Two carriers, because the boundary differs:
  * signed/unsigned (uint16 -> int16): descriptors say it, the export
    declares from descriptors, so only the live object should lie.
  * float carrier (float32 -> int32): no descriptor says "float", so a
    stale carrier reaches the file.
"""
import glob
import os
import sys
import tempfile

import numpy as np
import pydicom
from pydicom.dataset import FileDataset, FileMetaDataset
from pydicom.uid import ExplicitVRLittleEndian, generate_uid

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))))

import isocenter  # noqa: E402
from isocenter.io_handlers import SidecarPixelLoader  # noqa: E402
from isocenter.pixel_geometry import PIXEL_DTYPE_ATTR  # noqa: E402
from isocenter.session import DicomSession  # noqa: E402


def write_src(folder, arr, float_element=False):
    meta = FileMetaDataset()
    meta.MediaStorageSOPClassUID = "1.2.840.10008.5.1.4.1.1.7"
    meta.MediaStorageSOPInstanceUID = generate_uid()
    meta.TransferSyntaxUID = ExplicitVRLittleEndian
    ds = FileDataset(None, {}, file_meta=meta, preamble=b"\0" * 128)
    ds.PatientID, ds.PatientName = "PAT406", "DOE^JOHN"
    ds.StudyInstanceUID, ds.SeriesInstanceUID = generate_uid(), generate_uid()
    ds.SOPInstanceUID = meta.MediaStorageSOPInstanceUID
    ds.SOPClassUID = meta.MediaStorageSOPClassUID
    ds.Modality, ds.SeriesNumber, ds.InstanceNumber = "OT", 1, 1
    ds.StudyDate = "20230101"
    ds.Rows, ds.Columns = arr.shape
    ds.SamplesPerPixel = 1
    ds.PhotometricInterpretation = "MONOCHROME2"
    ds.BitsAllocated = ds.BitsStored = arr.dtype.itemsize * 8
    ds.HighBit = ds.BitsAllocated - 1
    if float_element:
        ds.FloatPixelData = arr.tobytes()
    else:
        ds.PixelRepresentation = 1 if arr.dtype.kind == 'i' else 0
        ds.PixelData = arr.tobytes()
    ds.save_as(os.path.join(folder, "one.dcm"), enforce_file_format=True)
    return ds


def only_instance(session):
    for pt in session.store.patients:
        for st in pt.studies:
            for se in st.series:
                for inst in se.instances:
                    return inst
    return None


def run(label, source, replacement, float_element=False):
    print(f"\n================ {label}")
    tmp = tempfile.mkdtemp(prefix="p406_")
    src = os.path.join(tmp, "src")
    os.makedirs(src)
    write_src(src, source, float_element=float_element)
    db = os.path.join(tmp, "s.db")
    out = os.path.join(tmp, "out")

    s = DicomSession(persistence_file=db)
    try:
        s.ingest(src)
        inst = only_instance(s)
        print("  after ingest: carrier =",
              inst.attributes.get(PIXEL_DTYPE_ATTR),
              " 0028,0100 =", inst.attributes.get("0028,0100"),
              " 0028,0103 =", inst.attributes.get("0028,0103"))
        s.save(sync=True)
        loader = inst._pixel_loader
        print("  loader dtype after first save:",
              getattr(loader, "pixel_dtype", None),
              "bits:", getattr(loader, "bits", None),
              "pixrep:", getattr(loader, "pixel_representation", None))
        hash_before = inst._pixel_hash

        inst.set_pixel_data(replacement)
        print("  after set_pixel_data: carrier =",
              inst.attributes.get(PIXEL_DTYPE_ATTR),
              " 0028,0100 =", inst.attributes.get("0028,0100"),
              " 0028,0103 =", inst.attributes.get("0028,0103"),
              " unwritten =", inst._pixel_array_unwritten)
        s.save(sync=True)
        loader2 = inst._pixel_loader
        print("  loader object unchanged:", loader2 is loader,
              " hash unchanged (dedup hit):",
              inst._pixel_hash == hash_before)
        print("  loader dtype after second save:",
              getattr(loader2, "pixel_dtype", None),
              "bits:", getattr(loader2, "bits", None),
              "pixrep:", getattr(loader2, "pixel_representation", None))

        freed = inst.unload_pixel_data()
        print("  unload_pixel_data() ->", freed)
        live = inst.get_pixel_data()
        print("  LIVE after unload: dtype", live.dtype,
              " equals what the caller set:",
              np.array_equal(live, replacement))

        try:
            s.export(out, format="dicom", show_progress=False)
            print("  export: OK")
        except Exception as e:
            print("  export RAISED:", type(e).__name__, str(e)[:220])
    finally:
        s.close()

    files = glob.glob(os.path.join(out, "**", "*.dcm"), recursive=True)
    print("  files written:", len(files))
    if files:
        d = pydicom.dcmread(files[0])
        print("   EXPORTED: FloatPixelData:", "FloatPixelData" in d,
              " PixelData:", "PixelData" in d,
              " TS:", d.file_meta.TransferSyntaxUID)
        print("   EXPORTED: BitsAllocated", getattr(d, "BitsAllocated", None),
              " PixelRepresentation", getattr(d, "PixelRepresentation", None))
        try:
            got = d.pixel_array
            print("   EXPORTED dtype:", got.dtype,
                  " equals the array the caller set:",
                  np.array_equal(got.reshape(replacement.shape), replacement))
        except Exception as e:
            print("   EXPORTED pixel_array RAISED:",
                  type(e).__name__, str(e)[:140])

    # And what a REOPENED session says.
    s2 = DicomSession(persistence_file=db)
    try:
        inst2 = only_instance(s2)
        if inst2 is not None:
            print("  REOPENED loader dtype:",
                  getattr(inst2._pixel_loader, "pixel_dtype", None))
            arr2 = inst2.get_pixel_data()
            print("  REOPENED dtype:", arr2.dtype, " equal:",
                  np.array_equal(arr2, replacement))
    finally:
        s2.close()


def main():
    print("isocenter:", isocenter.__file__)
    src16 = (np.arange(16, dtype=np.uint16) + 40000).reshape(4, 4)
    run("MILD: uint16 ingested, set_pixel_data(int16 view)",
        src16, src16.view(np.int16))

    floats = (np.arange(16, dtype=np.float32) + 0.5).reshape(4, 4)
    run("SEVERE: float32 ingested, set_pixel_data(int32 view)",
        floats, floats.view(np.int32), float_element=True)

    # Is the stale snapshot only about dtype? Same bytes, new GEOMETRY.
    src8 = np.arange(16, dtype=np.uint8).reshape(4, 4)
    run("GEOMETRY: uint8 4x4 ingested, set_pixel_data(same bytes as 2x8)",
        src8, src8.reshape(2, 8))


if __name__ == "__main__":
    main()
