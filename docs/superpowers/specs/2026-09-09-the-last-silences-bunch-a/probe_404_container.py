"""#404 adjacent: what does a shipped compressed export actually contain?

DICOM PS3.5 A.4.4 encapsulates a JPEG 2000 *codestream*. Pillow's default,
called with no filename, wraps the codestream in a JP2 box. This measures the
bytes Isocenter writes today, through the real export path, for a dtype the
current encoder accepts (uint16).
"""
import os
import tempfile

import numpy as np
import pydicom
from pydicom.dataset import FileDataset, FileMetaDataset
from pydicom.uid import ExplicitVRLittleEndian, generate_uid

import isocenter
from isocenter.session import DicomSession


def write_src(folder, arr):
    meta = FileMetaDataset()
    meta.MediaStorageSOPClassUID = "1.2.840.10008.5.1.4.1.1.7"
    meta.MediaStorageSOPInstanceUID = generate_uid()
    meta.TransferSyntaxUID = ExplicitVRLittleEndian
    ds = FileDataset(None, {}, file_meta=meta, preamble=b"\0" * 128)
    ds.SOPInstanceUID = meta.MediaStorageSOPInstanceUID
    ds.SOPClassUID = meta.MediaStorageSOPClassUID
    ds.PatientID = "PAT404C"
    ds.PatientName = "DOE^JOE"
    ds.StudyInstanceUID = generate_uid()
    ds.SeriesInstanceUID = generate_uid()
    ds.StudyDate = "20230101"
    ds.StudyTime = "120000"
    ds.Modality = "OT"
    ds.ConversionType = "WSD"
    ds.SeriesNumber = 1
    ds.InstanceNumber = 1
    ds.Rows, ds.Columns = arr.shape
    ds.SamplesPerPixel = 1
    ds.PhotometricInterpretation = "MONOCHROME2"
    ds.BitsAllocated = 16
    ds.BitsStored = 16
    ds.HighBit = 15
    ds.PixelRepresentation = 0
    ds.PixelData = arr.tobytes()
    ds.save_as(os.path.join(folder, "a.dcm"), enforce_file_format=True)


def main():
    print("isocenter:", isocenter.__file__)
    arr = np.linspace(0, 4000, 64 * 64).astype(np.uint16).reshape(64, 64)
    tmp = tempfile.mkdtemp(prefix="p404c_")
    src = os.path.join(tmp, "src")
    os.makedirs(src)
    write_src(src, arr)
    out = os.path.join(tmp, "out")
    s = DicomSession(persistence_file=os.path.join(tmp, "s.db"))
    try:
        s.ingest(src)
        s.save()
        s.export(out, format="dicom", show_progress=False)
    finally:
        s.close()
    found = []
    for root, _d, names in os.walk(out):
        for n in names:
            if n.endswith(".dcm"):
                found.append(os.path.join(root, n))
    print("files:", len(found))
    d = pydicom.dcmread(found[0])
    print("TransferSyntaxUID:", d.file_meta.TransferSyntaxUID)
    frags = list(pydicom.encaps.generate_fragments(d.PixelData))
    first = frags[1] if len(frags)> 1 and len(frags[0]) == 4 else frags[0]
    print("fragment count   :", len(frags))
    print("codestream head  :", first[:12].hex())
    if first[:4] == b"\x00\x00\x00\x0c" and first[4:8] == b"jP  ":
        print("VERDICT          : JP2 box (a file format), not a bare codestream")
    elif first[:2] == b"\xff\x4f":
        print("VERDICT          : J2K codestream (SOC marker ff4f)")
    else:
        print("VERDICT          : unrecognised")
    print("readback ok      :", np.array_equal(d.pixel_array, arr))


if __name__ == "__main__":
    main()
