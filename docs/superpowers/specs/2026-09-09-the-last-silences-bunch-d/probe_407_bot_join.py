"""#407: does imagecodecs_handler decode a single-frame encapsulated frame?

Builds the codestream with the project's own encoder (`_compress_j2k`,
bare J2K post-#404), then asks the handler for the array. Also shows the
fragment structure the join flattens, and what the fix would produce.
"""
import os
import sys

import numpy as np
import pydicom
from pydicom.dataset import FileDataset, FileMetaDataset
from pydicom.encaps import generate_fragments, generate_frames
from pydicom.uid import ExplicitVRLittleEndian, generate_uid

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))))

import isocenter  # noqa: E402
from isocenter import imagecodecs_handler  # noqa: E402
from isocenter.io_handlers import _compress_j2k  # noqa: E402


def build(arr, samples=1, photometric="MONOCHROME2"):
    meta = FileMetaDataset()
    meta.MediaStorageSOPClassUID = "1.2.840.10008.5.1.4.1.1.7"
    meta.MediaStorageSOPInstanceUID = generate_uid()
    meta.TransferSyntaxUID = ExplicitVRLittleEndian
    ds = FileDataset(None, {}, file_meta=meta, preamble=b"\0" * 128)
    ds.SOPInstanceUID = meta.MediaStorageSOPInstanceUID
    ds.SOPClassUID = meta.MediaStorageSOPClassUID
    ds.Rows, ds.Columns = arr.shape[0], arr.shape[1]
    ds.SamplesPerPixel = samples
    ds.PhotometricInterpretation = photometric
    ds.BitsAllocated = arr.dtype.itemsize * 8
    ds.BitsStored = ds.BitsAllocated
    ds.HighBit = ds.BitsAllocated - 1
    ds.PixelRepresentation = 1 if arr.dtype.kind == 'i' else 0
    ds.PixelData = arr.tobytes()
    return ds


def report(label, arr, samples=1, photometric="MONOCHROME2"):
    print(f"\n=== {label}  dtype={arr.dtype} shape={arr.shape}")
    ds = build(arr, samples, photometric)
    try:
        _compress_j2k(ds, pixel_array=arr)
    except Exception as e:
        print("  _compress_j2k refused:", type(e).__name__, str(e)[:160])
        return
    frags = list(generate_fragments(ds.PixelData))
    joined = b"".join(frags)
    print("  fragments:", len(frags), "lens:", [len(f) for f in frags])
    print("  joined head:", joined[:8].hex())
    print("  last-fragment head:", frags[-1][:8].hex())
    frames = list(generate_frames(ds.PixelData, number_of_frames=1))
    print("  generate_frames count:", len(frames),
          "head:", frames[0][:8].hex() if frames else None)
    try:
        out = imagecodecs_handler.get_pixel_data(ds)
        print("  handler OK dtype:", out.dtype, "shape:", out.shape,
              "equal:", np.array_equal(out.reshape(arr.shape), arr))
    except Exception as e:
        print("  handler RAISED:", type(e).__name__, str(e)[:200])
    import imagecodecs
    try:
        direct = imagecodecs.jpeg2k_decode(frags[-1])
        print("  hand-decoded last fragment: dtype", direct.dtype,
              "shape", direct.shape,
              "equal:", np.array_equal(direct.reshape(arr.shape), arr))
    except Exception as e:
        print("  hand decode RAISED:", type(e).__name__, str(e)[:200])
    if frames:
        try:
            viaf = imagecodecs.jpeg2k_decode(frames[0])
            print("  hand-decoded generate_frames[0]: dtype", viaf.dtype,
                  "equal:", np.array_equal(viaf.reshape(arr.shape), arr))
        except Exception as e:
            print("  generate_frames decode RAISED:",
                  type(e).__name__, str(e)[:200])


def main():
    print("isocenter:", isocenter.__file__)
    print("pydicom:", pydicom.__version__)
    import imagecodecs
    print("imagecodecs:", imagecodecs.__version__)
    report("8-bit grayscale", np.arange(16, dtype=np.uint8).reshape(4, 4))
    report("16-bit grayscale",
           (np.arange(16, dtype=np.uint16) * 4096).reshape(4, 4))
    report("8-bit RGB", np.arange(48, dtype=np.uint8).reshape(4, 4, 3),
           samples=3, photometric="RGB")
    report("16-bit RGB (refused by _J2K_ENCODABLE_FRAMES)",
           (np.arange(48, dtype=np.uint16) * 1000).reshape(4, 4, 3),
           samples=3, photometric="RGB")


if __name__ == "__main__":
    main()
