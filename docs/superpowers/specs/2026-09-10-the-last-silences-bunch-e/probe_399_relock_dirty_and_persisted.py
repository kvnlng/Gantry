"""#399 -- does a re-lock still tell the store it changed?

`add_sequence_item()` calls `mark_modified()`; `add_sequence()` calls it
**only when it creates**. So a fix that reaches into `sequence.items`
in place advances no revision on the second and later locks, and the
next `save()` skips the instance -- the in-memory token is replaced and
the stored one is not (the #173 shape, one module over).

This probe locks, saves to a quiet store, re-locks with **no other
mutation in between**, and reads `has_unsaved_changes` before saving
again and reloading. Run it once on `main` and once with the candidate
fix in the worktree.

Run:
    PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=<repo> <venv>/bin/python -u \
      docs/superpowers/specs/2026-09-10-the-last-silences-bunch-e/\
probe_399_relock_dirty_and_persisted.py
"""
import pathlib
import sys
import tempfile
from datetime import date

REPO = pathlib.Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO))

import numpy as np  # noqa: E402

from isocenter.entities import Instance, Patient, Series, Study  # noqa: E402
from isocenter.session import DicomSession  # noqa: E402

SEQ = "0400,0500"
PID = "REV_399D"


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
    inst.set_attr("0008,0030", "120000")
    inst.set_pixel_data(np.zeros((8, 8), dtype=np.uint16))
    series.instances.append(inst)
    study.series.append(series)
    patient.studies.append(study)
    session.store.patients.append(patient)
    return inst


def main():
    with tempfile.TemporaryDirectory() as tmp:
        tmp = pathlib.Path(tmp)
        db = str(tmp / "p399d.db")
        key = str(tmp / "isocenter.key")

        with DicomSession(db) as session:
            session.enable_reversible_anonymization(key)
            inst = _build(session)

            session.lock_identities(PID, tags_to_lock=["0010,0010"])
            session.save(sync=True)
            print("after lock #1 + save: dirty =", inst.has_unsaved_changes,
                  "rev/persisted =", inst._revision, inst._persisted_revision)

            # The whole point: no other mutation between the two locks.
            session.lock_identities(PID, tags_to_lock=["0010,0020"])
            print("after lock #2 (no other mutation): dirty =",
                  inst.has_unsaved_changes,
                  "rev/persisted =", inst._revision, inst._persisted_revision,
                  "items =", len(_items(inst)))
            print("  in-memory recover ->",
                  session.reversibility_service.recover_original_data(inst))
            session.save(sync=True)

        with DicomSession(db) as reloaded:
            reloaded.enable_reversible_anonymization(key)
            inst2 = reloaded.store.patients[0].studies[0].series[0].instances[0]
            print("after reload: items =", len(_items(inst2)))
            print("  stored recover ->",
                  reloaded.reversibility_service.recover_original_data(inst2))


if __name__ == "__main__":
    main()
