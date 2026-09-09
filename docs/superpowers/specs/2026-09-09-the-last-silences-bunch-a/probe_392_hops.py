"""#392 hop trace: which hop drops an empty SQ that IS in the graph?"""
import glob, json, os, sqlite3, sys, tempfile
import numpy as np, pydicom
from pydicom.dataset import FileDataset, FileMetaDataset
from pydicom.tag import Tag
from pydicom.uid import ExplicitVRLittleEndian, generate_uid

import isocenter
from isocenter.entities import DicomSequence
from isocenter.session import DicomSession

PRIV = "0009,1005"
STD = "0008,1140"


def write_src(folder):
    meta = FileMetaDataset()
    meta.MediaStorageSOPClassUID = "1.2.840.10008.5.1.4.1.1.7"
    meta.MediaStorageSOPInstanceUID = generate_uid()
    meta.TransferSyntaxUID = ExplicitVRLittleEndian
    ds = FileDataset(None, {}, file_meta=meta, preamble=b"\0"*128)
    ds.PatientID, ds.PatientName = "PAT392", "DOE^JOHN"
    ds.StudyInstanceUID, ds.SeriesInstanceUID = generate_uid(), generate_uid()
    ds.SOPInstanceUID = meta.MediaStorageSOPInstanceUID
    ds.SOPClassUID = meta.MediaStorageSOPClassUID
    ds.Modality, ds.SeriesNumber, ds.InstanceNumber = "OT", 1, 1
    ds.StudyDate = "20230101"
    ds.add_new(0x00090010, 'LO', 'ACME_HEADER')
    ds.Rows = ds.Columns = 4
    ds.BitsAllocated = ds.BitsStored = 8
    ds.HighBit = 7
    ds.SamplesPerPixel = 1
    ds.PhotometricInterpretation = "MONOCHROME2"
    ds.PixelRepresentation = 0
    ds.PixelData = np.zeros((4, 4), dtype=np.uint8).tobytes()
    p = os.path.join(folder, "one.dcm")
    ds.save_as(p, enforce_file_format=True)
    return p


def instances(session):
    for pt in session.store.patients:
        for st in pt.studies:
            for se in st.series:
                yield from se.instances


def plant(session):
    for inst in instances(session):
        inst.sequences[PRIV] = DicomSequence(tag=PRIV)
        inst.sequences[STD] = DicomSequence(tag=STD)
        inst.mark_modified()


def main():
    print("isocenter:", isocenter.__file__)
    tmp = tempfile.mkdtemp(prefix="p392h_")
    src = os.path.join(tmp, "src"); os.makedirs(src)
    write_src(src)
    db = os.path.join(tmp, "s.db")

    # HOP: graph -> export, no store round trip
    s = DicomSession(persistence_file=db)
    try:
        s.ingest(src)
        plant(s)
        out1 = os.path.join(tmp, "out_fresh")
        s.export(out1, format="dicom", show_progress=False)
        s.save()
    finally:
        s.close()
    f = glob.glob(os.path.join(out1, "**", "*.dcm"), recursive=True)[0]
    d = pydicom.dcmread(f)
    print("FRESH EXPORT: priv empty SQ present:", Tag(0x0009, 0x1005) in d,
          "VR", (d[Tag(0x0009, 0x1005)].VR if Tag(0x0009, 0x1005) in d else None),
          "len", (len(d[Tag(0x0009, 0x1005)].value) if Tag(0x0009, 0x1005) in d else None))
    print("FRESH EXPORT: std empty SQ present:", Tag(0x0008, 0x1140) in d,
          "len", (len(d[Tag(0x0008, 0x1140)].value) if Tag(0x0008, 0x1140) in d else None))

    # HOP: store -> hydrate
    with sqlite3.connect(db) as conn:
        raw = conn.execute("SELECT attributes_json FROM instances").fetchone()[0]
    data = json.loads(raw)
    print("STORED __sequences__:", json.dumps(data.get("__sequences__")))

    s = DicomSession(persistence_file=db)
    try:
        for inst in instances(s):
            print("HYDRATED sequences:", {t: len(q.items) for t, q in inst.sequences.items()})
        out2 = os.path.join(tmp, "out_reloaded")
        s.export(out2, format="dicom", show_progress=False)
    finally:
        s.close()
    f2 = glob.glob(os.path.join(out2, "**", "*.dcm"), recursive=True)[0]
    d2 = pydicom.dcmread(f2)
    print("RELOADED EXPORT: priv empty SQ present:", Tag(0x0009, 0x1005) in d2)
    print("RELOADED EXPORT: std empty SQ present:", Tag(0x0008, 0x1140) in d2)
    print("tmp:", tmp)


if __name__ == '__main__':
    main()
