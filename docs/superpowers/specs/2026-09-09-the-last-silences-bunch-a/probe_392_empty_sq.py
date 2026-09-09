"""#392: does a zero-item SQ survive ingest -> graph -> store -> export?"""
import glob, json, os, sqlite3, sys, tempfile
import numpy as np, pydicom
from pydicom.dataset import Dataset, FileDataset, FileMetaDataset
from pydicom.tag import Tag
from pydicom.uid import ExplicitVRLittleEndian, generate_uid

import isocenter
from isocenter.session import DicomSession
print("isocenter:", isocenter.__file__)
print("python:", sys.version.split()[0], "gil:", sys._is_gil_enabled())
print("pydicom:", pydicom.__version__, "numpy:", np.__version__)

PRIV_EMPTY = Tag(0x0009, 0x1005)      # private empty SQ
STD_EMPTY  = Tag(0x0008, 0x1140)      # ReferencedImageSequence, standard, empty
STD_FULL   = Tag(0x0008, 0x1110)      # ReferencedStudySequence, one item

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
    ds.add_new(PRIV_EMPTY, 'SQ', [])
    ds.add_new(STD_EMPTY, 'SQ', [])
    item = Dataset()
    item.ReferencedSOPClassUID = "1.2.840.10008.5.1.4.1.1.7"
    item.ReferencedSOPInstanceUID = generate_uid()
    ds.add_new(STD_FULL, 'SQ', [item])
    ds.Rows = ds.Columns = 4
    ds.BitsAllocated = ds.BitsStored = 8
    ds.HighBit = 7
    ds.SamplesPerPixel = 1
    ds.PhotometricInterpretation = "MONOCHROME2"
    ds.PixelRepresentation = 0
    ds.PixelData = np.zeros((4,4), dtype=np.uint8).tobytes()
    p = os.path.join(folder, "one.dcm")
    ds.save_as(p, enforce_file_format=True)
    # re-read to prove the source really carries them
    back = pydicom.dcmread(p)
    print("SOURCE: priv empty SQ present:", PRIV_EMPTY in back,
          "len", len(back[PRIV_EMPTY].value), "VR", back[PRIV_EMPTY].VR)
    print("SOURCE: std empty SQ present:", STD_EMPTY in back,
          "len", len(back[STD_EMPTY].value))
    return ds.SOPInstanceUID, p

def instances(session):
    for pt in session.store.patients:
        for st in pt.studies:
            for se in st.series:
                yield from se.instances

def audit_rows(db, action=None):
    with sqlite3.connect(db) as conn:
        if action:
            return conn.execute("SELECT action_type, details FROM audit_log WHERE action_type=?", (action,)).fetchall()
        return conn.execute("SELECT action_type, details FROM audit_log").fetchall()

def main():
    tmp = tempfile.mkdtemp(prefix="p392_")
    src = os.path.join(tmp, "src"); os.makedirs(src)
    uid, path = write_src(src)
    db = os.path.join(tmp, "s.db")
    out = os.path.join(tmp, "out")

    s = DicomSession(persistence_file=db)
    try:
        losses = s.ingest(src)
        print("ingest() returned:", repr(losses)[:400])
        for inst in instances(s):
            print("GRAPH sequences keys:", sorted(inst.sequences.keys()))
            print("GRAPH has 0009,1005 in attributes:", "0009,1005" in inst.attributes)
            print("GRAPH has 0008,1140 in attributes:", "0008,1140" in inst.attributes)
            for t, seq in inst.sequences.items():
                print("   seq", t, "items", len(seq.items))
        s.save()
        s.export(out, format="dicom", show_progress=False)
    finally:
        s.close()

    written = glob.glob(os.path.join(out, "**", "*.dcm"), recursive=True)
    print("exported files:", written)
    ds2 = pydicom.dcmread(written[0])
    print("EXPORT: priv empty SQ present:", PRIV_EMPTY in ds2)
    print("EXPORT: std empty SQ present:", STD_EMPTY in ds2)
    print("EXPORT: std full SQ present:", STD_FULL in ds2,
          (len(ds2[STD_FULL].value) if STD_FULL in ds2 else None))
    rows = audit_rows(db)
    from collections import Counter
    print("audit action counts:", Counter(a for a, _ in rows))
    for a, d in rows:
        if a in ("DATA_LOSS", "ERROR"):
            print("  ", a, d[:300])

    # what did the store hold?
    with sqlite3.connect(db) as conn:
        print("attributes_json:", conn.execute("SELECT attributes_json FROM instances").fetchone()[0][:600])
        try:
            print("instance_attributes rows:", conn.execute("SELECT tag_group, tag_element, value_text FROM instance_attributes").fetchall()[:20])
        except Exception as e:
            print("instance_attributes:", e)
    print("tmp:", tmp)

if __name__ == '__main__':
    main()
