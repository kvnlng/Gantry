"""#399 -- does an ingested file's own (0400,0500) reach the graph?

The fix replaces the token item rather than appending it. Which item it
replaces matters only if something other than `embed_identity_token` can
put an item in that sequence. A source file de-identified by another tool
may already carry an Encrypted Attributes Sequence; this asks whether
`ingest()` carries it into `Instance.sequences`, and what
`recover_original_data` does with it today.

Run:
    PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=<repo> <venv>/bin/python -u \
      docs/superpowers/specs/2026-09-10-the-last-silences-bunch-e/\
probe_399_foreign_sequence_at_ingest.py
"""
import pathlib
import sys
import tempfile

REPO = pathlib.Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO))

import numpy as np  # noqa: E402
import pydicom  # noqa: E402
from pydicom.dataset import Dataset, FileMetaDataset  # noqa: E402
from pydicom.uid import CTImageStorage, ExplicitVRLittleEndian  # noqa: E402

from isocenter.session import DicomSession  # noqa: E402

SEQ = "0400,0500"
PID = "FOREIGN_399"


def _write_source(path):
    ds = Dataset()
    ds.file_meta = FileMetaDataset()
    ds.file_meta.MediaStorageSOPClassUID = CTImageStorage
    ds.file_meta.MediaStorageSOPInstanceUID = "1.2.826.0.1.3680043.10.1.399.1"
    ds.file_meta.TransferSyntaxUID = ExplicitVRLittleEndian
    ds.SOPClassUID = CTImageStorage
    ds.SOPInstanceUID = "1.2.826.0.1.3680043.10.1.399.1"
    ds.StudyInstanceUID = "1.2.826.0.1.3680043.10.1.399.2"
    ds.SeriesInstanceUID = "1.2.826.0.1.3680043.10.1.399.3"
    ds.PatientID = PID
    ds.PatientName = "Foreign^Source"
    ds.StudyDate = "20230101"
    ds.StudyTime = "120000"
    ds.Modality = "CT"
    ds.SeriesNumber = 1
    ds.InstanceNumber = 1

    # An Encrypted Attributes Sequence this library did not write.
    foreign = Dataset()
    foreign.EncryptedContent = b"NOT-OUR-TOKEN"
    foreign.EncryptedContentTransferSyntaxUID = "1.2.840.10008.1.2"
    ds.EncryptedAttributesSequence = [foreign]

    arr = np.zeros((8, 8), dtype=np.uint16)
    ds.Rows, ds.Columns = 8, 8
    ds.SamplesPerPixel = 1
    ds.PhotometricInterpretation = "MONOCHROME2"
    ds.BitsAllocated = 16
    ds.BitsStored = 16
    ds.HighBit = 15
    ds.PixelRepresentation = 0
    ds.PixelData = arr.tobytes()
    ds.save_as(path, enforce_file_format=True)


def main():
    with tempfile.TemporaryDirectory() as tmp:
        tmp = pathlib.Path(tmp)
        src = tmp / "src"
        src.mkdir()
        _write_source(str(src / "foreign.dcm"))
        print("source items:",
              len(pydicom.dcmread(str(src / "foreign.dcm"))
                  .EncryptedAttributesSequence))

        with DicomSession(str(tmp / "p399f.db")) as session:
            session.ingest(str(src))
            session.enable_reversible_anonymization(str(tmp / "isocenter.key"))
            inst = session.store.patients[0].studies[0].series[0].instances[0]
            seq = inst.sequences.get(SEQ)
            print("after ingest, graph items:",
                  len(seq.items) if seq is not None else "sequence absent")
            if seq is not None and seq.items:
                print("  item 0 attrs:", sorted(seq.items[0].attributes))
            print("  recover before any lock ->",
                  session.reversibility_service.recover_original_data(inst))

            session.lock_identities(PID)
            seq = inst.sequences.get(SEQ)
            print("after lock, graph items:",
                  len(seq.items) if seq is not None else "sequence absent")
            print("  recover ->",
                  session.reversibility_service.recover_original_data(inst))


if __name__ == "__main__":
    main()
