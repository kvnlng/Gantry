"""#392 adjacent population: an empty private SQ under Implicit VR."""
import glob, os, sqlite3, tempfile
import numpy as np, pydicom
from pydicom.dataset import FileDataset, FileMetaDataset
from pydicom.tag import Tag
from pydicom.uid import ImplicitVRLittleEndian, generate_uid

import isocenter
from isocenter.session import DicomSession

PRIV = Tag(0x0009, 0x1005)


def write_src(folder):
    meta = FileMetaDataset()
    meta.MediaStorageSOPClassUID = "1.2.840.10008.5.1.4.1.1.7"
    meta.MediaStorageSOPInstanceUID = generate_uid()
    meta.TransferSyntaxUID = ImplicitVRLittleEndian
    ds = FileDataset(None, {}, file_meta=meta, preamble=b"\0"*128)
    ds.PatientID, ds.PatientName = "PAT392I", "DOE^JOHN"
    ds.StudyInstanceUID, ds.SeriesInstanceUID = generate_uid(), generate_uid()
    ds.SOPInstanceUID = meta.MediaStorageSOPInstanceUID
    ds.SOPClassUID = meta.MediaStorageSOPClassUID
    ds.Modality, ds.SeriesNumber, ds.InstanceNumber = "OT", 1, 1
    ds.StudyDate = "20230101"
    ds.add_new(0x00090010, 'LO', 'ACME_HEADER')
    ds.add_new(PRIV, 'SQ', [])
    ds.Rows = ds.Columns = 4
    ds.BitsAllocated = ds.BitsStored = 8
    ds.HighBit = 7
    ds.SamplesPerPixel = 1
    ds.PhotometricInterpretation = "MONOCHROME2"
    ds.PixelRepresentation = 0
    ds.PixelData = np.zeros((4, 4), dtype=np.uint8).tobytes()
    p = os.path.join(folder, "one.dcm")
    ds.save_as(p, enforce_file_format=True)
    back = pydicom.dcmread(p)
    e = back[PRIV]
    print("SOURCE (implicit): VR", e.VR, "value", repr(e.value))
    return p


def main():
    print("isocenter:", isocenter.__file__)
    tmp = tempfile.mkdtemp(prefix="p392i_")
    src = os.path.join(tmp, "src"); os.makedirs(src)
    write_src(src)
    db = os.path.join(tmp, "s.db")
    out = os.path.join(tmp, "out")
    s = DicomSession(persistence_file=db)
    try:
        s.ingest(src)
        for pt in s.store.patients:
            for st in pt.studies:
                for se in st.series:
                    for inst in se.instances:
                        print("GRAPH attributes has 0009,1005:",
                              "0009,1005" in inst.attributes,
                              repr(inst.attributes.get("0009,1005")))
                        print("GRAPH sequences:", sorted(inst.sequences))
                        print("GRAPH recorded VR:",
                              inst.attribute_vrs.get("0009,1005"))
        s.export(out, format="dicom", show_progress=False)
    finally:
        s.close()
    d = pydicom.dcmread(glob.glob(os.path.join(out, "**", "*.dcm"), recursive=True)[0])
    print("EXPORT has 0009,1005:", PRIV in d,
          (d[PRIV].VR, repr(d[PRIV].value)) if PRIV in d else None)
    with sqlite3.connect(db) as conn:
        print("audit:", conn.execute(
            "SELECT action_type, substr(details,1,120) FROM audit_log").fetchall())


if __name__ == '__main__':
    main()
