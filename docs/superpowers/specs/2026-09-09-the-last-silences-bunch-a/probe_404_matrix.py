"""#404: the dtype matrix the fix must satisfy, measured end to end.

For each dtype: encode with imagecodecs (the proposed encoder), wrap in a
DICOM dataset the way the export worker does, write it, read it back with
pydicom, and compare to the source array. This is the table the parametrized
tests come from -- it is measured, not asserted.
"""
import os
import tempfile

import numpy as np
import pydicom
from pydicom.dataset import FileDataset, FileMetaDataset
from pydicom.encaps import encapsulate
from pydicom.uid import ExplicitVRLittleEndian, JPEG2000Lossless, generate_uid

import imagecodecs

TMP = tempfile.mkdtemp(prefix="p404m_")


def frame(name):
    dt = np.dtype(name)
    if dt == np.bool_:
        a = np.zeros((32, 32), dtype=bool)
        a[::3] = True
        return a
    if dt.kind == "f":
        return np.linspace(-5.5, 5.5, 1024).astype(dt).reshape(32, 32)
    info = np.iinfo(dt)
    a = np.linspace(info.min, info.max, 1024).astype(dt).reshape(32, 32)
    a[0, 0] = info.min
    a[0, 1] = info.max
    return a


def build(arr, bits, pixrep):
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
    ds.SeriesNumber = 1
    ds.InstanceNumber = 1
    ds.ConversionType = "WSD"
    ds.Rows, ds.Columns = arr.shape[:2]
    ds.SamplesPerPixel = 1
    ds.PhotometricInterpretation = "MONOCHROME2"
    ds.BitsAllocated = bits
    ds.BitsStored = bits
    ds.HighBit = bits - 1
    ds.PixelRepresentation = pixrep
    return ds


def attempt(name):
    src = frame(name)
    enc = src
    note = ""
    if src.dtype == np.bool_:
        enc = src.astype(np.uint8)
        note = "bool encoded as uint8"
    bits = enc.dtype.itemsize * 8
    pixrep = 1 if enc.dtype.kind == "i" else 0
    try:
        blob = imagecodecs.jpeg2k_encode(enc, level=0, codecformat="J2K")
    except Exception as exc:
        return "REFUSED at encode", "%s: %s" % (type(exc).__name__,
                                                str(exc)[:56]), bits, pixrep
    ds = build(enc, bits, pixrep)
    ds.PixelData = encapsulate([blob])
    ds.file_meta.TransferSyntaxUID = JPEG2000Lossless
    path = os.path.join(TMP, name + ".dcm")
    try:
        ds.save_as(path, enforce_file_format=True)
        back = pydicom.dcmread(path).pixel_array
    except Exception as exc:
        return "WROTE, unreadable", "%s: %s" % (type(exc).__name__,
                                                str(exc)[:56]), bits, pixrep
    exact = np.array_equal(back, enc) and back.dtype == enc.dtype
    if exact:
        return "EXACT", note or "-", bits, pixrep
    return "SILENTLY WRONG", "decoded %s, first bad %s" % (
        back.dtype, np.argwhere(back != enc)[:1].tolist()), bits, pixrep


def main():
    print("proposed encoder: imagecodecs.jpeg2k_encode(level=0, codecformat='J2K')")
    print("")
    fmt = "%-9s | %-3s | %-6s | %-16s | %s"
    print(fmt % ("dtype", "bits", "pixrep", "verdict", "note"))
    print("-" * 96)
    for name in ("uint8", "int8", "uint16", "int16", "uint32", "int32",
                 "uint64", "int64", "float32", "float64", "bool"):
        verdict, note, bits, pixrep = attempt(name)
        print(fmt % (name, bits, pixrep, verdict, note))

    print("")
    print("=== where int32 stops being exact (the silent arm) ===")
    for b in (16, 24, 25, 26, 32):
        lo = -pow(2, b - 1)
        hi = pow(2, b - 1) - 1
        a = np.linspace(lo, hi, 1024).astype(np.int32).reshape(32, 32)
        a[0, 0] = lo
        a[0, 1] = hi
        try:
            blob = imagecodecs.jpeg2k_encode(a, level=0, codecformat="J2K")
            ok = np.array_equal(imagecodecs.jpeg2k_decode(blob), a)
        except Exception as exc:
            ok = "%s" % type(exc).__name__
        print("  int32 holding %2d bits of data : %s" % (b, ok))


if __name__ == "__main__":
    main()
