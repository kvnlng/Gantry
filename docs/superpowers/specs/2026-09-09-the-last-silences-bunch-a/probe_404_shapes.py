"""#404: multi-frame and RGB through the real _finalize_dataset.

The dtype matrix was measured single-frame and monochrome. The export worker
also hands `_compress_j2k` multi-frame stacks and 3-sample RGB, and the current
function carries a squeeze block for (1,H,W)/(H,W,1)/(F,H,W,1). Both shapes are
measured here against the proposed encoder, through the real
DicomExporter._finalize_dataset, so the matrix has no unmeasured column.
"""
import os
import tempfile

import numpy as np
import pydicom
from pydicom.dataset import FileDataset, FileMetaDataset
from pydicom.encaps import encapsulate
from pydicom.uid import ExplicitVRLittleEndian, JPEG2000Lossless, generate_uid

import imagecodecs
from isocenter import io_handlers

TMP = tempfile.mkdtemp(prefix="p404s_")


def proposed(ds, pixel_array=None):
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
    elif arr.ndim == 2:
        arr = arr.reshape((1, rows, cols))
    elif arr.ndim == 3 and samples > 1:
        arr = arr.reshape((1, rows, cols, samples))
    blobs = []
    for i in range(frames):
        blobs.append(imagecodecs.jpeg2k_encode(
            np.ascontiguousarray(arr[i]), level=0, codecformat="J2K"))
    ds.PixelData = encapsulate(blobs)
    ds.file_meta.TransferSyntaxUID = JPEG2000Lossless


def build(rows, cols, samples, frames, bits, pixrep, photometric):
    meta = FileMetaDataset()
    meta.MediaStorageSOPClassUID = "1.2.840.10008.5.1.4.1.1.7"
    meta.MediaStorageSOPInstanceUID = generate_uid()
    meta.TransferSyntaxUID = ExplicitVRLittleEndian
    ds = FileDataset(None, {}, file_meta=meta, preamble=b"\0" * 128)
    ds.SOPInstanceUID = meta.MediaStorageSOPInstanceUID
    ds.SOPClassUID = meta.MediaStorageSOPClassUID
    ds.PatientID = "P"
    ds.PatientName = "X"
    ds.StudyInstanceUID = generate_uid()
    ds.SeriesInstanceUID = generate_uid()
    ds.StudyDate = "20230101"
    ds.StudyTime = "120000"
    ds.Modality = "OT"
    ds.ConversionType = "WSD"
    ds.SeriesNumber = 1
    ds.InstanceNumber = 1
    ds.Rows = rows
    ds.Columns = cols
    ds.SamplesPerPixel = samples
    ds.PhotometricInterpretation = photometric
    if samples > 1:
        ds.PlanarConfiguration = 0
    if frames > 1:
        ds.NumberOfFrames = frames
    ds.BitsAllocated = bits
    ds.BitsStored = bits
    ds.HighBit = bits - 1
    ds.PixelRepresentation = pixrep
    return ds


def case(label, arr, ds, encoder):
    original = io_handlers._compress_j2k
    io_handlers._compress_j2k = encoder
    try:
        io_handlers.DicomExporter._finalize_dataset(ds, "j2k", pixel_array=arr)
        path = os.path.join(TMP, label.replace(" ", "_") + ".dcm")
        ds.save_as(path, enforce_file_format=True)
        back = pydicom.dcmread(path).pixel_array
        same = back.shape == arr.shape and np.array_equal(back, arr)
        print("  %-34s : %s  read %s %s" % (
            label, "EXACT" if same else "MISMATCH", back.shape, back.dtype))
        if not same:
            print("      wrote shape %s dtype %s" % (arr.shape, arr.dtype))
    except Exception as exc:
        print("  %-34s : %s: %s" % (label, type(exc).__name__, str(exc)[:80]))
    finally:
        io_handlers._compress_j2k = original


def main():
    rng = np.random.default_rng(3)

    print("=== proposed encoder (imagecodecs) ===")
    stack = rng.integers(-2000, 3000, (3, 32, 32)).astype(np.int16)
    case("int16 3-frame stack", stack,
         build(32, 32, 1, 3, 16, 1, "MONOCHROME2"), proposed)

    mono = rng.integers(-2000, 3000, (32, 32)).astype(np.int16)
    case("int16 single frame", mono,
         build(32, 32, 1, 1, 16, 1, "MONOCHROME2"), proposed)

    rgb = rng.integers(0, 256, (32, 32, 3)).astype(np.uint8)
    case("uint8 RGB single frame", rgb,
         build(32, 32, 3, 1, 8, 0, "RGB"), proposed)

    rgbstack = rng.integers(0, 256, (2, 32, 32, 3)).astype(np.uint8)
    case("uint8 RGB 2-frame stack", rgbstack,
         build(32, 32, 3, 2, 8, 0, "RGB"), proposed)

    print("")
    print("=== current encoder (Pillow), same shapes, for comparison ===")
    case("uint16 3-frame stack", rng.integers(0, 3000, (3, 32, 32)).astype(np.uint16),
         build(32, 32, 1, 3, 16, 0, "MONOCHROME2"), io_handlers._compress_j2k)
    case("uint8 RGB single frame", rgb,
         build(32, 32, 3, 1, 8, 0, "RGB"), io_handlers._compress_j2k)
    case("int16 3-frame stack", stack,
         build(32, 32, 1, 3, 16, 1, "MONOCHROME2"), io_handlers._compress_j2k)


if __name__ == "__main__":
    main()
