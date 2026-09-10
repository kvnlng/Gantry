"""#399 -- what a second `lock_identities()` does to (0400,0500).

Measures the asymmetry the issue names: `embed_identity_token` appends an
item per call, `recover_original_data` reads item 0. Four questions:

1. how many items does the sequence carry after 1, 2 and 3 locks?
2. what does `recover_original_data` return after a re-lock whose
   `tags_to_lock` differs from the first?
3. does the same hold after `save(sync=True)` + reload?
4. what reaches the exported file?

The graph is built by hand, the way `tests/test_reversibility.py` does, so
the probe measures the lock rather than the ingest.

Run:
    PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=<repo> <venv>/bin/python -u \
      docs/superpowers/specs/2026-09-10-the-last-silences-bunch-e/\
probe_399_relock_token_items.py
"""
import pathlib
import sys
import tempfile
from datetime import date

REPO = pathlib.Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO))

import numpy as np  # noqa: E402
import pydicom  # noqa: E402

from isocenter.entities import Instance, Patient, Series, Study  # noqa: E402
from isocenter.session import DicomSession  # noqa: E402

SEQ = "0400,0500"
CONTENT = "0400,0510"
PID = "REV_399"


def _items(instance):
    seq = instance.sequences.get(SEQ)
    return list(seq.items) if seq is not None else []


def _build(session):
    patient = Patient(PID, "Original^Name")
    study = Study("ST_1", date(2023, 1, 1))
    study.study_time = "120000"
    series = Series("SE_1", "CT", 1)
    inst = Instance("SOP_1", "1.2.840.10008.5.1.4.1.1.2", 1)
    inst.file_path = None
    inst.set_attr("0010,0010", "Original^Name")
    inst.set_attr("0010,0020", PID)
    # Type 1 on the exported dataset; `Study.study_time` alone does not
    # survive the reload below, and the export refuses without it.
    inst.set_attr("0008,0030", "120000")
    inst.set_attr("0018,0050", "1.0")
    inst.set_attr("0018,0060", "120")
    inst.set_attr("0020,0032", ["0", "0", "0"])
    inst.set_attr("0020,0037", ["1", "0", "0", "0", "1", "0"])
    inst.set_attr("0028,0030", ["0.5", "0.5"])
    inst.set_pixel_data(np.zeros((8, 8), dtype=np.uint16))
    series.instances.append(inst)
    study.series.append(series)
    patient.studies.append(study)
    session.store.patients.append(patient)
    return inst


def main():
    with tempfile.TemporaryDirectory() as tmp:
        tmp = pathlib.Path(tmp)
        db = str(tmp / "p399.db")
        key = str(tmp / "isocenter.key")
        out = tmp / "out"

        with DicomSession(db) as session:
            session.enable_reversible_anonymization(key)
            inst = _build(session)
            session.save(sync=True)
            engine = session.reversibility_service.engine

            session.lock_identities(PID, tags_to_lock=["0010,0010"])
            print("lock #1: items =", len(_items(inst)))
            print("  recover ->",
                  session.reversibility_service.recover_original_data(inst))

            # Change the visible attribute, then re-lock a *different* tag
            # set. If the re-lock were honoured, recovery would answer with
            # the second capture.
            inst.set_attr("0010,0010", "CHANGED^Value")
            session.lock_identities(PID, tags_to_lock=["0010,0010", "0010,0020"])
            print("lock #2: items =", len(_items(inst)))
            print("  recover ->",
                  session.reversibility_service.recover_original_data(inst))
            if len(_items(inst)) > 1:
                print("  item 1 (skipped by recovery) ->",
                      engine.decrypt(_items(inst)[1].attributes[CONTENT]))

            session.lock_identities(PID, tags_to_lock=["0010,0010"])
            print("lock #3: items =", len(_items(inst)))
            print("  revision/persisted/dirty:", inst._revision,
                  inst._persisted_revision, inst.has_unsaved_changes)
            session.save(sync=True)

        with DicomSession(db) as reloaded:
            reloaded.enable_reversible_anonymization(key)
            inst2 = reloaded.store.patients[0].studies[0].series[0].instances[0]
            print("after reload: items =", len(_items(inst2)))
            print("  recover ->",
                  reloaded.reversibility_service.recover_original_data(inst2))
            reloaded.export(str(out), format="dicom", show_progress=False,
                            check_reversibility=False)

        files = sorted(out.rglob("*.dcm"))
        print("exported files:", len(files))
        if files:
            ds = pydicom.dcmread(str(files[0]))
            seq = ds.get((0x0400, 0x0500))
            print("exported sequence items:",
                  len(seq.value) if seq is not None else "absent")


if __name__ == "__main__":
    main()
