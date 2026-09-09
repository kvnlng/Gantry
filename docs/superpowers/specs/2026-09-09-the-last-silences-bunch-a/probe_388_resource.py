"""#388: what does a session do when the shipped resource is missing?"""
import io, logging, os, sqlite3, sys, tempfile
import numpy as np, pydicom
from pydicom.dataset import FileDataset, FileMetaDataset
from pydicom.uid import ExplicitVRLittleEndian, generate_uid

import isocenter
from isocenter import session as sess_mod
from isocenter.session import DicomSession

SERIAL = "SN-SCANNER-01"


def write_src(folder):
    meta = FileMetaDataset()
    meta.MediaStorageSOPClassUID = "1.2.840.10008.5.1.4.1.1.7"
    meta.MediaStorageSOPInstanceUID = generate_uid()
    meta.TransferSyntaxUID = ExplicitVRLittleEndian
    ds = FileDataset(None, {}, file_meta=meta, preamble=b"\0"*128)
    ds.PatientID, ds.PatientName = "PAT388", "DOE^JOHN"
    ds.StudyInstanceUID, ds.SeriesInstanceUID = generate_uid(), generate_uid()
    ds.SOPInstanceUID = meta.MediaStorageSOPInstanceUID
    ds.SOPClassUID = meta.MediaStorageSOPClassUID
    ds.Modality, ds.SeriesNumber, ds.InstanceNumber = "OT", 1, 1
    ds.StudyDate = "20230101"
    ds.Manufacturer, ds.ManufacturerModelName = "GE MEDICAL SYSTEMS", "Revolution CT"
    ds.DeviceSerialNumber = SERIAL
    ds.Rows = ds.Columns = 4
    ds.BitsAllocated = ds.BitsStored = 8
    ds.HighBit = 7
    ds.SamplesPerPixel = 1
    ds.PhotometricInterpretation = "MONOCHROME2"
    ds.PixelRepresentation = 0
    ds.PixelData = np.zeros((4, 4), dtype=np.uint8).tobytes()
    ds.save_as(os.path.join(folder, "one.dcm"), enforce_file_format=True)


def scaffold(tmp, resources_dir):
    src = os.path.join(tmp, "src")
    os.makedirs(src, exist_ok=True)
    write_src(src)
    db = os.path.join(tmp, "s.db")
    cfg = os.path.join(tmp, "cfg.yaml")
    old = sess_mod.RESOURCES_DIR
    sess_mod.RESOURCES_DIR = resources_dir
    buf = io.StringIO()
    h = logging.StreamHandler(buf)
    h.setLevel(logging.DEBUG)
    root = logging.getLogger("Isocenter")
    root.addHandler(h)
    root.setLevel(logging.DEBUG)
    try:
        s = DicomSession(persistence_file=db)
        try:
            s.ingest(src)
            s.create_config(cfg)
        finally:
            s.close()
    finally:
        sess_mod.RESOURCES_DIR = old
        root.removeHandler(h)
    with sqlite3.connect(db) as conn:
        rows = conn.execute(
            "SELECT action_type, details FROM audit_log").fetchall()
    return open(cfg).read(), buf.getvalue(), rows


def main():
    print("isocenter:", isocenter.__file__)
    print("RESOURCES_DIR:", sess_mod.RESOURCES_DIR)
    print("direct call, real dir:",
          len(sess_mod._load_redaction_knowledge_base()), "machines")
    missing = tempfile.mkdtemp(prefix="p388_empty_")
    old = sess_mod.RESOURCES_DIR
    sess_mod.RESOURCES_DIR = missing
    try:
        print("direct call, missing dir:",
              repr(sess_mod._load_redaction_knowledge_base()))
        print("direct call ctp, missing dir:",
              repr(sess_mod._load_ctp_rules()))
    finally:
        sess_mod.RESOURCES_DIR = old

    good_cfg, good_log, good_rows = scaffold(
        tempfile.mkdtemp(prefix="p388_good_"), old)
    bad_cfg, bad_log, bad_rows = scaffold(
        tempfile.mkdtemp(prefix="p388_bad_"), missing)
    print("\n--- WITH the resource, scaffolded config:")
    print(good_cfg[good_cfg.find("machine"):][:600])
    print("--- WITHOUT the resource, scaffolded config:")
    print(bad_cfg[bad_cfg.find("machine"):][:600])
    print("--- configs identical:", good_cfg == bad_cfg)
    print("--- log lines while the resource was missing:",
          repr([l for l in bad_log.splitlines() if l.strip()]))
    print("--- audit rows while missing:", [(a, d[:60]) for a, d in bad_rows])


if __name__ == '__main__':
    main()
