"""§11.6 step P1: what the compressed export path does with a 32-bit frame.

Compression is the *default* -- `Session._export_dicom(folder,
use_compression=True, ...)` maps to `compression='j2k'` -- so once
`_INTEGER_DTYPE_BY_BITS` lets a 32-bit frame load instead of raising at
the loader, a plain `session.export(folder)` puts it through
`_compress_j2k` for the first time. This probe records, for a
`(uint32, pixrep=0)` and an `(int32, pixrep=1)` instance, and for each of
`use_compression=True` and `use_compression=False`:

  * whether a file was written;
  * `dcmread`'s BitsAllocated, PixelRepresentation, `pixel_array.dtype`
    and values;
  * `ExportSummary.written` and `.failures`;
  * every audit row for that UID, its status and its details -- read
    **after `flush_audit_queue()`**, because without the flush "no row"
    and "row not yet written" are the same observation and the probe
    cannot distinguish its own arms.

Run it with the project .venv and this worktree on PYTHONPATH; print
`isocenter.__file__` and read it. The `if __name__ == '__main__'` guard is
required: `spawn` re-imports `__main__` in every worker, and a script that
ingests at module scope re-enters ingest in each child and dies as
`BrokenProcessPool` with no child traceback.
"""
import os
import sqlite3
import sys
import tempfile
import traceback

import numpy as np
import pydicom
from pydicom.dataset import FileDataset, FileMetaDataset
from pydicom.uid import ExplicitVRLittleEndian, generate_uid

from isocenter.session import DicomSession

ROWS_U = [[0, 1, 2, 3], [4, 5, 6, 7], [8, 9, 10, 11], [12, 13, 14, 15]]
ROWS_S = [[-8, -7, -6, -5], [-4, -3, -2, -1], [0, 1, 2, 3], [4, 5, 6, 7]]


def write_src(folder, arr, pixrep):
    meta = FileMetaDataset()
    meta.MediaStorageSOPClassUID = "1.2.840.10008.5.1.4.1.1.7"
    meta.MediaStorageSOPInstanceUID = generate_uid()
    meta.TransferSyntaxUID = ExplicitVRLittleEndian

    ds = FileDataset(None, {}, file_meta=meta, preamble=b"\0" * 128)
    ds.PatientID, ds.PatientName = "P1PROBE", "DOE^JOHN"
    ds.StudyInstanceUID, ds.SeriesInstanceUID = generate_uid(), generate_uid()
    ds.SOPInstanceUID = meta.MediaStorageSOPInstanceUID
    ds.SOPClassUID = meta.MediaStorageSOPClassUID
    ds.Modality, ds.SeriesNumber, ds.InstanceNumber = "OT", 1, 1
    ds.StudyDate = "20230101"
    ds.Rows, ds.Columns = arr.shape
    ds.SamplesPerPixel = 1
    ds.PhotometricInterpretation = "MONOCHROME2"
    ds.BitsAllocated = ds.BitsStored = arr.dtype.itemsize * 8
    ds.HighBit = ds.BitsStored - 1
    ds.PixelRepresentation = pixrep
    ds.PixelData = arr.tobytes()
    path = os.path.join(folder, "one.dcm")
    ds.save_as(path, enforce_file_format=True)
    return ds.SOPInstanceUID


def written_files(out):
    found = []
    for root, _dirs, files in os.walk(out):
        found += [os.path.join(root, f) for f in files if f.endswith(".dcm")]
    return found


def one_arm(base, dtype_name, rows, pixrep, use_compression):
    label = f"{dtype_name} pixrep={pixrep} use_compression={use_compression}"
    print(f"\n=== {label}")
    arr = np.array(rows, dtype=dtype_name)
    work = os.path.join(base, f"{dtype_name}_{int(use_compression)}")
    src = os.path.join(work, "src")
    out = os.path.join(work, "out")
    os.makedirs(src)
    db = os.path.join(work, "p1.db")

    uid = write_src(src, arr, pixrep)
    session = DicomSession(persistence_file=db)
    summary = None
    try:
        session.ingest(src)
        session.save()
        try:
            summary = session.export(out, format="dicom",
                                     use_compression=use_compression,
                                     show_progress=False)
        except Exception as exc:  # noqa: BLE001 -- the arm being measured
            print("  export RAISED:", type(exc).__name__, exc)
        session.store_backend.flush_audit_queue()
    finally:
        session.close()

    print("  ExportSummary:", summary)
    if summary is not None:
        print("  written:", getattr(summary, "written", "<none>"),
              " failures:", getattr(summary, "failures", "<none>"))

    files = written_files(out)
    print("  files written:", len(files))
    for path in files:
        ds = pydicom.dcmread(path)
        print("   TransferSyntax:", ds.file_meta.TransferSyntaxUID,
              ds.file_meta.TransferSyntaxUID.name)
        print("   BitsAllocated:", ds.BitsAllocated,
              " PixelRepresentation:", ds.PixelRepresentation)
        try:
            got = ds.pixel_array
            print("   pixel_array dtype:", got.dtype,
                  " values:", got.tolist())
            print("   values == intended:", got.tolist() == rows)
        except Exception as exc:  # noqa: BLE001
            print("   pixel_array RAISED:", type(exc).__name__, exc)

    with sqlite3.connect(db) as conn:
        audit = conn.execute(
            "SELECT action_type, entity_uid, details FROM audit_log").fetchall()
    print("  audit rows naming the UID or the export:")
    for action, entity, details in audit:
        if uid in (details or "") or uid == entity or action in ("EXPORT", "ERROR", "DATA_LOSS"):
            print(f"    {action} [{entity}] {details}")


def main():
    print("isocenter:", __import__("isocenter").__file__)
    print("python:", sys.version)
    print("numpy:", np.__version__, " pydicom:", pydicom.__version__)
    with tempfile.TemporaryDirectory() as base:
        for use_compression in (True, False):
            for dtype_name, rows, pixrep in (("uint32", ROWS_U, 0),
                                             ("int32", ROWS_S, 1)):
                try:
                    one_arm(base, dtype_name, rows, pixrep, use_compression)
                except Exception:  # noqa: BLE001
                    traceback.print_exc()


if __name__ == '__main__':
    main()
