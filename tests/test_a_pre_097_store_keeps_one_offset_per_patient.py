"""A store de-identified before 0.9.7 keeps one offset per patient, and says
what that costs.

Before 0.9.7 the pseudonym and the date offset were unkeyed: anyone with
an exported file could compute the offset from its PatientID. A patient
such a release already de-identified has had that offset published in
every file exported for it, so giving it a keyed offset now protects
nothing and puts two offsets on one patient's dates. So each patient is
classed **once**, when the store is opened, into `patients.jitter_scheme`:

- `unkeyed-sha256` if its id has exactly the shape the old scheme minted
  (`ANON_` + 12 lowercase hex), or any of its dates was shifted (a study's
  `date_shifted`, or a `__shifted__` record at any depth);
- `keyed-hmac-v1` otherwise, and for every patient written since.

A legacy patient keeps the whole old scheme -- the old mint as well as
the old offset -- and every load says how many there are in one
`WARNING` row, which grades the report `REVIEW_REQUIRED`.

**Why a persisted column, not inference at load.** Once a keyed shift is
saved, "has a shifted date" is true of keyed patients too; only a class
fixed before any keyed work can tell them apart. So the backfill touches
NULL rows only, and a save never overwrites a class.

**How the fixture is built.** A store written by this release, then aged:
`patients.jitter_scheme` dropped and the `project_secret` table dropped,
which is exactly what a 0.9.6 store lacks (it has `shifted_study_date`
and `shift_provenance`, so those stay). Reopening re-adds the column as
NULL and runs the classification.

**Literals.** Every offset and pseudonym was computed once and pasted:
the unkeyed ones match 0.9.6's own literals (`P1`'s pseudonym
`ANON_fbeae7c18667` reads back to -286), the keyed ones are under
`FIXED_A`.

**Why this file imports what it does.** The classification, hydration
and notice live in `isocenter.persistence`, the legacy arms in
`isocenter.privacy` and `isocenter.remediation`, driven through
`isocenter.session`; see `test_mutation_probe_targets.py`.
"""
import sqlite3
from datetime import date

import pytest

from isocenter.entities import Instance, Patient, Series, Study
from isocenter.privacy import JITTER_SCHEME_KEYED, JITTER_SCHEME_UNKEYED
from isocenter.session import DicomSession

from support.project_secret import FIXED_A, load_fixed_secret

ACQ_DATE = "0008,0022"

#: One patient per arm of the classification, so dropping an arm
#: misclassifies exactly one of them.
BY_ID = "ANON_fbeae7c18667"       # the 0.9.6 pseudonym of P1; no shifted date
BY_STUDY = "RESTORED1"            # raw id, study `date_shifted`
BY_RECORD = "RESTORED2"           # raw id, a nested `__shifted__` record only
KEYED_RAW = "Q1"                  # nothing: keyed
KEYED_29 = "ANON_45d22d6f6acfa0c277223a06"  # minted under FIXED_A, no witness
PREFIX_ONLY = "ANON_not_hex"      # prefixed, not the old shape, no witness


@pytest.fixture(autouse=True)
def _threads(monkeypatch):
    monkeypatch.setenv("ISOCENTER_FORCE_THREADS", "1")
    monkeypatch.setenv("ISOCENTER_MAX_WORKERS", "2")
    monkeypatch.delenv("ISOCENTER_FORCE_PROCESSES", raising=False)
    monkeypatch.delenv("ISOCENTER_MAX_TASKS_PER_CHILD", raising=False)


def _add_study(patient, suffix, study_date):
    study = Study(f"1.2.826.0.1.97.{suffix}", study_date)
    series = Series(f"1.2.826.0.1.97.{suffix}.1", "OT", 1)
    instance = Instance(f"1.2.826.0.1.97.{suffix}.1.0",
                        "1.2.840.10008.5.1.4.1.1.7", 1)
    series.instances.append(instance)
    study.series.append(series)
    patient.studies.append(study)
    return study, instance


def _write_pre_097_store(tmp_path, name="legacy.db", extra=()):
    """A store in the shape 0.9.6 left: the witnesses it wrote, no class
    column and no secret table."""
    from isocenter.entities import DicomItem
    db = str(tmp_path / name)
    session = DicomSession(db)
    with session:
        by_id = Patient(BY_ID, "ANONYMIZED")
        _add_study(by_id, "id", date(2022, 3, 21))

        by_study = Patient(BY_STUDY, "Restored^Name")
        study, _ = _add_study(by_study, "study", date(2022, 3, 21))
        study.date_shifted = True
        study.record_date_shift(date(2022, 3, 21))

        by_record = Patient(BY_RECORD, "Restored^Two")
        _, instance = _add_study(by_record, "record", date(2023, 1, 1))
        nested = DicomItem()
        nested.set_attr(ACQ_DATE, "20220101")
        nested.record_date_shift(ACQ_DATE, "20220101")
        instance.add_sequence_item("0040,a730", nested)

        session.store.patients.extend([by_id, by_study, by_record,
                                       Patient(KEYED_RAW, "Raw^Name"),
                                       Patient(KEYED_29, "ANONYMIZED"),
                                       Patient(PREFIX_ONLY, "Raw^Three"),
                                       *extra])
        _add_study(session.store.patients[3], "q1", date(2023, 1, 1))
        session.save(sync=True)
    _age(db)
    return db


def _age(db):
    with sqlite3.connect(db) as conn:
        conn.execute("ALTER TABLE patients DROP COLUMN jitter_scheme")
        conn.execute("DROP TABLE project_secret")


def _schemes(db):
    with sqlite3.connect(db) as conn:
        return dict(conn.execute(
            "SELECT patient_id, jitter_scheme FROM patients").fetchall())


def _notices(db, needle="under the unkeyed scheme (GHSA"):
    with sqlite3.connect(db) as conn:
        return [r[0] for r in conn.execute(
            "SELECT details FROM audit_log WHERE action_type='WARNING'")
            if needle in r[0]]


def _patient(session, pid):
    [patient] = [p for p in session.store.patients if p.patient_id == pid]
    return patient


def test_opening_classifies_each_patient_once(tmp_path):
    """T13, the classification, one arm per patient; T13c, a 29-character
    keyed pseudonym with a NULL class and no witness stays keyed.

    Red on: the id arm dropped; the `date_shifted` arm dropped; the
    `__shifted__` arm dropped; the id arm widened to a prefix match (the
    29-character and non-hex ids go unkeyed).
    """
    db = _write_pre_097_store(tmp_path)
    with DicomSession(db) as session:
        in_memory = {p.patient_id: p._jitter_scheme
                     for p in session.store.patients}
    expected = {
        BY_ID: JITTER_SCHEME_UNKEYED,
        BY_STUDY: JITTER_SCHEME_UNKEYED,
        BY_RECORD: JITTER_SCHEME_UNKEYED,
        KEYED_RAW: JITTER_SCHEME_KEYED,
        KEYED_29: JITTER_SCHEME_KEYED,
        PREFIX_ONLY: JITTER_SCHEME_KEYED,
    }
    assert _schemes(db) == expected
    assert in_memory == expected


def test_a_pre_fix_store_keeps_one_offset_per_patient_and_says_so(tmp_path):
    """T13. A legacy patient's new study takes the old offset; a keyed
    patient's takes the keyed one; the load says so once, counting the
    legacy patients, and the report grades REVIEW_REQUIRED.

    Red on: the legacy arm removed from the offset (BY_ID's new study
    moves by the keyed -336, not -286); the notice written per patient;
    `_make_lightweight_copy` not copying the class (the scan stamps the
    keyed scheme on BY_ID's proposal).
    """
    db = _write_pre_097_store(tmp_path)
    with DicomSession(db) as session:
        session.store_backend.flush_audit_queue()
        assert len(_notices(db)) == 1
        load_fixed_secret(session, tmp_path, FIXED_A)
        by_id = _patient(session, BY_ID)
        new_study, _ = _add_study(by_id, "id.b", date(2023, 6, 1))
        q1_study = _patient(session, KEYED_RAW).studies[0]
        session.anonymize(session.audit())

        assert (new_study.study_date - date(2023, 6, 1)).days == -286
        assert (q1_study.study_date - date(2023, 1, 1)).days == -276
        assert by_id.patient_id == BY_ID, "a legacy pseudonym is never replaced"
        out = tmp_path / "report.md"
        session.generate_report(str(out))
        report = out.read_text(encoding="utf-8")

    [notice] = _notices(db)
    assert "3 patients in this store were de-identified before 0.9.7" in notice
    # Two of the three (RESTORED1, RESTORED2) carry no pseudonym, so the
    # notice cannot say each has one (F8).
    assert "their `ANON_` pseudonyms, where they have one, are" in notice
    assert "or from the original Patient ID where the pseudonym" in notice
    assert "1 further patient carries an unkeyed pseudonym" not in notice
    assert "Re-ingesting the source files into a new store" in notice
    assert "REVIEW_REQUIRED" in report
    assert "de-identified before 0.9.7" in report

    with DicomSession(db):
        pass
    assert len(_notices(db)) == 2, "one notice per load"


def test_a_legacy_patient_keeps_its_offset_under_processes(tmp_path, monkeypatch):
    """T13 under processes: the class crosses the pickle boundary with
    the clone, or the worker stamps the keyed scheme.

    Red on: `_make_lightweight_copy` not copying the class.
    """
    monkeypatch.delenv("ISOCENTER_FORCE_THREADS", raising=False)
    monkeypatch.setenv("ISOCENTER_FORCE_PROCESSES", "1")
    db = _write_pre_097_store(tmp_path)
    with DicomSession(db) as session:
        load_fixed_secret(session, tmp_path, FIXED_A)
        new_study, _ = _add_study(_patient(session, BY_ID), "id.p",
                                  date(2023, 6, 1))
        session.anonymize(session.audit())
        assert (new_study.study_date - date(2023, 6, 1)).days == -286


CONTENT_DATE = "0008,0023"


@pytest.mark.parametrize("mode", ["threads", "processes"])
def test_a_legacy_patients_instance_date_takes_the_legacy_offset(
        tmp_path, monkeypatch, mode):
    """T13, the instance-level arm. A date the scan finds on an instance
    (here ContentDate, ruled JITTER) is proposed from `_scan_instance`,
    not `_scan_study`, and carries its own copy of the patient's scheme:
    it must be the legacy one, or a legacy patient's instance dates move
    by the keyed offset while its study dates move by the unkeyed one.
    Measured on a real 0.9.6 store by the review: -163 instead of -167.
    Under threads and processes, because the scheme reaches the worker
    on the clone.

    Red on: the instance SHIFT_DATE proposal stamped keyed (BY_ID's
    ContentDate moves by the keyed -336, not -286).
    """
    if mode == "processes":
        monkeypatch.delenv("ISOCENTER_FORCE_THREADS", raising=False)
        monkeypatch.setenv("ISOCENTER_FORCE_PROCESSES", "1")
    db = _write_pre_097_store(tmp_path)
    with DicomSession(db) as session:
        load_fixed_secret(session, tmp_path, FIXED_A)
        session.configuration.set_phi_tag(CONTENT_DATE, "JITTER")
        _, instance = _add_study(_patient(session, BY_ID), "id.i",
                                 date(2023, 6, 1))
        instance.set_attr(CONTENT_DATE, "20230601")
        session.anonymize(session.audit())
        # 20230601 - 286 days; the keyed -336 would read 20220630.
        assert instance.attributes[CONTENT_DATE] == "20220819", (
            instance.attributes[CONTENT_DATE])


def test_load_patient_keeps_the_stored_class(tmp_path):
    """T13, the single-patient hydration. `store_backend.load_patient` is
    a second way into the graph (tier 2), and a legacy patient hydrated
    through it must come back legacy, or its next shift is keyed.

    Red on: `load_patient` ignoring `patients.jitter_scheme`.
    """
    db = _write_pre_097_store(tmp_path)
    with DicomSession(db) as session:
        assert (session.store_backend.load_patient(BY_ID)._jitter_scheme
                == JITTER_SCHEME_UNKEYED)
        assert (session.store_backend.load_patient(BY_RECORD)._jitter_scheme
                == JITTER_SCHEME_UNKEYED)
        assert (session.store_backend.load_patient(KEYED_RAW)._jitter_scheme
                == JITTER_SCHEME_KEYED)


def test_a_legacy_patient_whose_id_is_not_anon_re_mints_the_legacy_pseudonym(tmp_path):
    """T13b. A legacy patient with a raw id is proposed the old
    pseudonym, and the offset holds across the pass that writes it.

    Red on: a keyed mint for a legacy patient (the proposal is 29
    characters and the next pass reads back a different offset).
    """
    db = _write_pre_097_store(tmp_path)
    with DicomSession(db) as session:
        load_fixed_secret(session, tmp_path, FIXED_A)
        patient = _patient(session, BY_STUDY)
        second, _ = _add_study(patient, "study.b", date(2023, 6, 1))
        report = session.audit()
        [proposal] = [f.remediation_proposal.new_value for f in report
                      if f.field_name == "patient_id"
                      and f.patient_id == BY_STUDY]
        assert proposal == "ANON_2a14024a15f7", proposal
        session.anonymize(report)
        assert patient.patient_id == "ANON_2a14024a15f7"
        assert (second.study_date - date(2023, 6, 1)).days == -152

        third, _ = _add_study(patient, "study.c", date(2024, 1, 1))
        session.anonymize(session.audit())
        assert (third.study_date - date(2024, 1, 1)).days == -152


def test_a_save_never_reclassifies_a_patient(tmp_path):
    """T14. `ON CONFLICT` keeps the stored class, and the open-time
    classification touches NULL rows only.

    Red on: `excluded.jitter_scheme` winning on conflict; the
    classification losing its `WHERE jitter_scheme IS NULL`.
    """
    db = _write_pre_097_store(tmp_path)
    with DicomSession(db) as session:
        by_id = _patient(session, BY_ID)
        by_id._jitter_scheme = JITTER_SCHEME_KEYED  # what a new entity carries
        by_id.patient_name = "Renamed"
        by_id.mark_modified()
        session.save(sync=True)
    assert _schemes(db)[BY_ID] == JITTER_SCHEME_UNKEYED

    # A keyed patient whose dates this release shifted has a witness now;
    # reopening must not read that as legacy.
    with DicomSession(db) as session:
        load_fixed_secret(session, tmp_path, FIXED_A)
        session.anonymize(session.audit())
        session.save(sync=True)
    keyed_q1 = "ANON_5b5ce7b47f254ef3a0d90c0f"
    assert _schemes(db)[keyed_q1] == JITTER_SCHEME_KEYED
    with DicomSession(db) as session:
        assert _patient(session, keyed_q1)._jitter_scheme == JITTER_SCHEME_KEYED
    assert _schemes(db)[keyed_q1] == JITTER_SCHEME_KEYED


def test_a_reingested_pre_fix_export_is_keyed_and_named(tmp_path):
    """T15, case F. A 12-hex `ANON_` id this release wrote (a re-ingested
    0.9.6 export) is keyed, is shifted with the keyed offset of its own
    text, and is named in the load notice and in the audit's warning.
    """
    db = str(tmp_path / "reingested.db")
    with DicomSession(db) as session:
        patient = Patient("ANON_0123456789ab", "ANONYMIZED")
        study, _ = _add_study(patient, "f", date(2023, 1, 1))
        session.store.patients.append(patient)
        session.save(sync=True)
    assert _schemes(db) == {"ANON_0123456789ab": JITTER_SCHEME_KEYED}

    with DicomSession(db) as session:
        session.store_backend.flush_audit_queue()
        [notice] = _notices(db, "unkeyed pseudonym from an export")
        assert notice.startswith("1 patient carries an unkeyed pseudonym")
        load_fixed_secret(session, tmp_path, FIXED_A)
        study = session.store.patients[0].studies[0]
        session.anonymize(session.audit())
        assert (study.study_date - date(2023, 1, 1)).days == -274
    assert len(_notices(db, "unkeyed pseudonym from an export")) == 2


def test_new_raw_data_for_a_legacy_patient_is_keyed_and_warned(tmp_path):
    """Q2. Raw files for a patient this store de-identified before 0.9.7
    become a second, keyed subject, with a WARNING saying so.
    """
    db = _write_pre_097_store(tmp_path, extra=[Patient("P1", "Orig^Name")])
    with DicomSession(db) as session:
        load_fixed_secret(session, tmp_path, FIXED_A)
        raw = _patient(session, "P1")
        assert raw._jitter_scheme == JITTER_SCHEME_KEYED
        study, _ = _add_study(raw, "p1.raw", date(2023, 1, 1))
        session.anonymize(session.audit())
        assert raw.patient_id == "ANON_d932e13c0c2fa7dafbd43b77"
        assert (study.study_date - date(2023, 1, 1)).days == -364
    [warning] = _notices(db, "added under an original Patient ID")
    assert warning.startswith("1 patient in this store was added")
