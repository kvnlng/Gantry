"""#407: the `entities.get_pixel_data()` imagecodecs fallback arm, and
two edges the fix has to have an answer for.

1. The fallback arm's `self._pixel_array_unwritten = False`
   (`entities.py`, the third of three sibling clears) carries a comment
   saying it "SURVIVES DELETION UNTESTED", because reaching it needs a
   transfer syntax pydicom cannot decode and imagecodecs can. With #407
   fixed, 16-bit multi-sample JPEG 2000 is exactly that syntax. This
   measures whether a test can now kill that line.

2. A multi-frame encapsulated dataset with NO NumberOfFrames takes the
   single-frame branch. What does the fixed branch do with it?

Run with the #407 fix applied. Section 1 needs it; section 2 is
informative either way.
"""
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


def build(folder, arr, frames=1, declare_frames=True):
    codestreams = [imagecodecs.jpeg2k_encode(a, level=0, codecformat="J2K")
                   for a in (arr if frames > 1 else [arr])]
    meta = FileMetaDataset()
    meta.MediaStorageSOPClassUID = "1.2.840.10008.5.1.4.1.1.7"
    meta.MediaStorageSOPInstanceUID = generate_uid()
    meta.TransferSyntaxUID = JPEG2000Lossless
    ds = FileDataset(None, {}, file_meta=meta, preamble=b"\0" * 128)
    ds.PatientID, ds.PatientName = "PAT407B", "DOE^JANE"
    ds.StudyInstanceUID, ds.SeriesInstanceUID = generate_uid(), generate_uid()
    ds.SOPInstanceUID = meta.MediaStorageSOPInstanceUID
    ds.SOPClassUID = meta.MediaStorageSOPClassUID
    ds.Modality, ds.SeriesNumber, ds.InstanceNumber = "OT", 1, 1
    ds.StudyDate = "20230101"
    one = arr[0] if frames > 1 else arr
    ds.Rows, ds.Columns = one.shape[0], one.shape[1]
    ds.SamplesPerPixel = one.shape[2] if one.ndim == 3 else 1
    ds.PhotometricInterpretation = "RGB" if ds.SamplesPerPixel > 1 \
        else "MONOCHROME2"
    if ds.SamplesPerPixel > 1:
        ds.PlanarConfiguration = 0
    ds.BitsAllocated = ds.BitsStored = one.dtype.itemsize * 8
    ds.HighBit = ds.BitsAllocated - 1
    ds.PixelRepresentation = 0
    if frames > 1 and declare_frames:
        ds.NumberOfFrames = frames
    ds.PixelData = encapsulate(codestreams)
    path = os.path.join(folder, f"f{frames}_{declare_frames}.dcm")
    ds.save_as(path, enforce_file_format=True)
    return path


def main():
    print("isocenter:", isocenter.__file__)
    tmp = tempfile.mkdtemp(prefix="p407b_")

    # --- 1. the fallback arm, end to end -------------------------------
    rgb16 = (np.arange(4 * 4 * 3, dtype=np.uint16) * 1000).reshape(4, 4, 3)
    path = build(tmp, rgb16)
    print("\n=== 1. Instance.get_pixel_data() through the fallback")
    inst = Instance(sop_instance_uid="1.2.3")
    inst.file_path = path
    replacement = np.zeros((4, 4, 3), dtype=np.uint16)
    inst.set_pixel_data(replacement)
    print("  after set_pixel_data: unwritten =", inst._pixel_array_unwritten)
    print("  discard_pixel_data() ->", inst.discard_pixel_data(),
          " unwritten still:", inst._pixel_array_unwritten)
    try:
        got = inst.get_pixel_data()
        print("  get_pixel_data OK:", got.dtype, got.shape,
              " equal to the file's pixels:", np.array_equal(got, rgb16))
    except Exception as e:
        print("  get_pixel_data RAISED:", type(e).__name__, str(e)[:160])
    print("  unwritten after the read:", inst._pixel_array_unwritten)
    print("  unload_pixel_data() ->", inst.unload_pixel_data(),
          "  <-- False if the fallback arm's clear is deleted")

    # --- 2. multi-frame with no NumberOfFrames -------------------------
    print("\n=== 2. two frames, NumberOfFrames NOT declared")
    two = np.stack([np.arange(16, dtype=np.uint8).reshape(4, 4),
                    (np.arange(16, dtype=np.uint8) + 100).reshape(4, 4)])
    p2 = build(tmp, two, frames=2, declare_frames=False)
    ds2 = pydicom.dcmread(p2)
    print("  NumberOfFrames present:", "NumberOfFrames" in ds2)
    try:
        got = imagecodecs_handler.get_pixel_data(ds2)
        print("  handler returned:", got.dtype, got.shape,
              " == frame 0:", np.array_equal(got, two[0]),
              " == both frames:", np.array_equal(got, two))
    except Exception as e:
        print("  handler RAISED:", type(e).__name__, str(e)[:160])

    print("\n=== 3. two frames, NumberOfFrames declared (unchanged arm)")
    p3 = build(tmp, two, frames=2, declare_frames=True)
    ds3 = pydicom.dcmread(p3)
    try:
        got = imagecodecs_handler.get_pixel_data(ds3)
        print("  handler returned:", got.dtype, got.shape,
              " == both frames:", np.array_equal(got, two))
    except Exception as e:
        print("  handler RAISED:", type(e).__name__, str(e)[:160])


if __name__ == "__main__":
    main()
