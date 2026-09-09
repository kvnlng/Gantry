"""#404: which encoder can carry a signed frame through JPEG 2000 lossless.

Three candidates measured on the same array per dtype:
  A. Pillow  Image.fromarray -> save(format='JPEG2000', compression='lossless')
     (what io_handlers._compress_j2k does today, at :3417)
  B. imagecodecs.jpeg2k_encode(..., level=0)  -- already an install_requires
     dependency (setup.py: "imagecodecs>=2023.9.18") and already imported by
     isocenter/imagecodecs_handler.py for the DECODE side.
  C. pydicom Dataset.compress(JPEG2000Lossless, arr) -- needs a pydicom
     encoding plugin (pylibjpeg-openjpeg or gdcm).

Bit-exactness is asserted by decoding again and comparing to the source array,
including dtype. A round trip that returns the right numbers in the wrong dtype
is not a pass: the DICOM file's PixelRepresentation is written from the dtype.
"""
import io
import sys

import numpy as np

DTYPES = ["uint8", "int8", "uint16", "int16", "uint32", "int32",
          "uint64", "int64", "float32", "bool"]


def sample(name):
    """A 16x16 frame with values that expose signedness and truncation."""
    dt = np.dtype(name)
    if dt == np.bool_:
        a = np.zeros((16, 16), dtype=bool)
        a[::2] = True
        return a
    if dt.kind == "f":
        return (np.arange(256, dtype=dt).reshape(16, 16) / 7.0).astype(dt)
    info = np.iinfo(dt)
    lo = max(info.min, -2048)
    hi = min(info.max, 2047)
    a = np.linspace(lo, hi, 256).astype(dt).reshape(16, 16)
    a[0, 0] = info.min if dt.kind == "i" else info.min
    a[0, 1] = hi
    return a


def try_pillow(arr):
    from PIL import Image
    bio = io.BytesIO()
    Image.fromarray(arr).save(bio, format="JPEG2000", compression="lossless")
    blob = bio.getvalue()
    back = np.asarray(Image.open(io.BytesIO(blob)))
    return blob, back


def try_imagecodecs(arr):
    import imagecodecs
    blob = imagecodecs.jpeg2k_encode(arr, level=0, codecformat="J2K")
    back = imagecodecs.jpeg2k_decode(blob)
    return blob, back


def try_pydicom(arr):
    import pydicom
    from pydicom.dataset import Dataset, FileMetaDataset
    from pydicom.uid import JPEG2000Lossless, ExplicitVRLittleEndian
    ds = Dataset()
    ds.file_meta = FileMetaDataset()
    ds.file_meta.TransferSyntaxUID = ExplicitVRLittleEndian
    ds.Rows, ds.Columns = arr.shape
    ds.SamplesPerPixel = 1
    ds.PhotometricInterpretation = "MONOCHROME2"
    ds.BitsAllocated = arr.dtype.itemsize * 8
    ds.BitsStored = ds.BitsAllocated
    ds.HighBit = ds.BitsAllocated - 1
    ds.PixelRepresentation = 1 if arr.dtype.kind == "i" else 0
    ds.NumberOfFrames = 1
    ds.compress(JPEG2000Lossless, arr, encoding_plugin="")
    return ds.PixelData, ds.pixel_array


def verdict(arr, back):
    if back is None:
        return "-"
    same_dtype = np.dtype(back.dtype) == np.dtype(arr.dtype)
    same_vals = back.shape == arr.shape and np.array_equal(
        back.astype(np.float64), arr.astype(np.float64))
    if same_vals and same_dtype:
        return "EXACT"
    if same_vals:
        return "values ok, dtype %s" % back.dtype
    return "WRONG VALUES (%s)" % back.dtype


def main():
    print("pillow / imagecodecs / pydicom+plugin, JPEG 2000 lossless\n")
    row = "%-9s | %-38s | %-38s | %s"
    print(row % ("dtype", "A Pillow", "B imagecodecs", "C pydicom.compress"))
    print("-" * 130)
    for name in DTYPES:
        arr = sample(name)
        cells = []
        for fn in (try_pillow, try_imagecodecs, try_pydicom):
            try:
                blob, back = fn(arr)
                cells.append("%s (%d B)" % (verdict(arr, back), len(blob)))
            except Exception as exc:
                cells.append("%s: %s" % (type(exc).__name__, str(exc)[:60]))
        print(row % (name, cells[0], cells[1], cells[2]))


if __name__ == "__main__":
    main()
