"""#406's sibling: the `arr is None` arm of `_persist_pixels`.

The dedup arm is not the only place that returns the loader's frame
without rebuilding the loader. `arr is None` -- pixels already swapped
out -- does the same, and a descriptor written through `set_attr()`
while the pixels are unloaded goes stale in exactly the same way, with
no `set_pixel_data()` anywhere in the story.

Measured so the adjacent issue is filed with evidence rather than with a
reading of the code.
"""
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
from isocenter.session import DicomSession  # noqa: E402


def write_src(folder, arr):
    meta = FileMetaDataset()
    meta.MediaStorageSOPClassUID = "1.2.840.10008.5.1.4.1.1.7"
    meta.MediaStorageSOPInstanceUID = generate_uid()
    meta.TransferSyntaxUID = ExplicitVRLittleEndian
    ds = FileDataset(None, {}, file_meta=meta, preamble=b"\0" * 128)
    ds.PatientID, ds.PatientName = "PAT406B", "DOE^JOHN"
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
    ds.PixelRepresentation = 0
    ds.PixelData = arr.tobytes()
    ds.save_as(os.path.join(folder, "one.dcm"), enforce_file_format=True)


def only_instance(session):
    for pt in session.store.patients:
        for st in pt.studies:
            for se in st.series:
                for inst in se.instances:
                    return inst
    return None


def main():
    print("isocenter:", isocenter.__file__)
    src = (np.arange(16, dtype=np.uint16) + 40000).reshape(4, 4)
    tmp = tempfile.mkdtemp(prefix="p406b_")
    folder = os.path.join(tmp, "src")
    os.makedirs(folder)
    write_src(folder, src)
    s = DicomSession(persistence_file=os.path.join(tmp, "s.db"))
    try:
        s.ingest(folder)
        inst = only_instance(s)
        s.save(sync=True)
        print("  loader bits/pixrep after save:",
              inst._pixel_loader.bits, inst._pixel_loader.pixel_representation)
        print("  unload:", inst.unload_pixel_data(),
              " resident:", inst.pixel_array is not None)
        inst.set_attr("0028,0103", 1)
        print("  set_attr(0028,0103 -> 1); dirty:",
              inst.has_unsaved_changes)
        s.save(sync=True)
        print("  loader bits/pixrep after second save:",
              inst._pixel_loader.bits, inst._pixel_loader.pixel_representation)
        got = inst.get_pixel_data()
        print("  get_pixel_data dtype:", got.dtype,
              " attributes say PixelRepresentation",
              inst.attributes.get("0028,0103"))
        print("  values match a signed read:",
              np.array_equal(got, src.view(np.int16)))
    finally:
        s.close()


if __name__ == "__main__":
    main()
