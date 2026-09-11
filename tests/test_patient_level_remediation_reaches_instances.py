"""A patient- or study-level remediation lands on each instance's own tags (#492).

`anonymize()` wrote the replacement to the `Patient` or `Study` entity
and left every instance's own copy of the tag as ingested. The exported
`.dcm` was clean only because the export worker stamps the patient and
study modules from the entities at write time (`session._patient_attributes`,
`_study_attributes`); two frozen readers read the instance dict instead
and handed the originals out. Measured on 168fdd6, pydicom's CT_small,
`DicomSession()` -> `ingest()` -> `audit()` -> `anonymize()` with no
config loaded: `patient.patient_name == "ANONYMIZED"` while
`instance.attributes["0010,0010"] == "CompressedSamples^CT1"`,
`export_dataframe(expand_metadata=True)` wrote `PatientName=ANONYMIZED`
beside `0010,0010=CompressedSamples^CT1` in the same row, and
`get_flattened_instances()` returned `patient_name=ANONYMIZED` beside an
`attributes_json` holding the original name and ID.

**The rule these tests hold.** When the Python-attribute arm of
`RemediationService._apply_single_remediation` fires -- the arm for an
entity with no `set_attr`, which is `Patient` and `Study` -- the value
the entity now holds is written onto the same tag of every instance
beneath it that carries the tag, through `set_attr`, so the revision and
dirty tracking see it. The tags are the ones the exporter stamps from
real entity fields, and the value written is the entity's, not a
relative edit of the instance's own copy, so there is one truth.

Each instance keeps the PHI status it had before the write, re-recorded
at the new revision. Not stamped REMEDIATED: `anonymize(findings=[...])`
with only the patient's findings would then vouch for an instance whose
own IDENTIFIED findings were never applied.

**Why this file imports what it does.** It reaches `RemediationService`
through `isocenter.remediation`, so it charges that module's probe row;
see `test_mutation_probe_targets.py`.
"""
import json
import shutil
import sqlite3

import pydicom
import pytest
from pydicom.data import get_testdata_file

from isocenter.entities import Instance, Patient, PhiStatus, Series, Study
from isocenter.io_handlers import format_study_date
from isocenter.privacy import PhiFinding, PhiRemediation
from isocenter.remediation import RemediationService
from isocenter.session import DicomSession

SC_SOP_CLASS = "1.2.840.10008.5.1.4.1.1.7"
ORIGINAL_NAME = "CompressedSamples^CT1"
ORIGINAL_ID = "1CT1"
ORIGINAL_STUDY_DATE = "20040119"
BUILT_NAME = "Original^Name"
BUILT_ID = "P492"


@pytest.fixture(autouse=True)
def _threads(monkeypatch):
    monkeypatch.setenv("ISOCENTER_FORCE_THREADS", "1")
    monkeypatch.setenv("ISOCENTER_MAX_WORKERS", "2")
    monkeypatch.delenv("ISOCENTER_FORCE_PROCESSES", raising=False)
    monkeypatch.delenv("ISOCENTER_MAX_TASKS_PER_CHILD", raising=False)


def _ingested(tmp_path):
    src = tmp_path / "in"
    src.mkdir()
    shutil.copy(get_testdata_file("CT_small.dcm"), src / "ct.dcm")
    session = DicomSession(str(tmp_path / "m.db"))
    session.ingest(str(src))
    return session


def _only_instance(session):
    return session.store.patients[0].studies[0].series[0].instances[0]


def _built(tmp_path, *, study_date="20240101", instance_dates=("20240101",),
           second_study_date=None):
    """A hand-built graph: one patient, one study, N instances.

    Each instance carries the patient's name and ID and its own
    `instance_dates[i]` as Study Date. `second_study_date` adds a second
    study with one instance carrying that date.
    """
    session = DicomSession(str(tmp_path / "b.db"))
    patient = Patient(BUILT_ID, BUILT_NAME)
    study = Study("1.2.826.0.1.492", study_date)
    series = Series("1.2.826.0.1.492.1", "OT", 1)
    for i, inst_date in enumerate(instance_dates):
        instance = Instance(f"1.2.826.0.1.492.1.{i}", SC_SOP_CLASS, i + 1)
        instance.set_attr("0010,0010", BUILT_NAME)
        instance.set_attr("0010,0020", BUILT_ID)
        instance.set_attr("0008,0020", inst_date)
        series.instances.append(instance)
    study.series.append(series)
    patient.studies.append(study)
    if second_study_date is not None:
        study2 = Study("1.2.826.0.1.492.2", second_study_date)
        series2 = Series("1.2.826.0.1.492.2.1", "OT", 1)
        instance = Instance("1.2.826.0.1.492.2.1.0", SC_SOP_CLASS, 1)
        instance.set_attr("0010,0010", BUILT_NAME)
        instance.set_attr("0010,0020", BUILT_ID)
        instance.set_attr("0008,0020", second_study_date)
        series2.instances.append(instance)
        study2.series.append(series2)
        patient.studies.append(study2)
    session.store.patients.append(patient)
    return session


def _instances(patient):
    return [inst for st in patient.studies for se in st.series
            for inst in se.instances]


# ---------------------------------------------------------------------------
# The documented path on a real file
# ---------------------------------------------------------------------------

def test_anonymize_writes_the_patient_and_study_replacement_onto_the_instance(tmp_path):
    """The defect, on the file it was measured on."""
    with _ingested(tmp_path) as session:
        inst = _only_instance(session)
        assert inst.attributes["0010,0010"] == ORIGINAL_NAME
        assert inst.attributes["0008,0020"] == ORIGINAL_STUDY_DATE
        session.audit()
        session.anonymize()
        patient = session.store.patients[0]
        study = patient.studies[0]
        assert patient.patient_name == "ANONYMIZED"
        assert patient.patient_id.startswith("ANON_")
        assert inst.attributes["0010,0010"] == "ANONYMIZED"
        assert inst.attributes["0010,0020"] == patient.patient_id
        # The study's shifted date, in the DA spelling the instance
        # carried and the exporter writes -- not the original.
        assert inst.attributes["0008,0020"] == format_study_date(study.study_date)
        assert inst.attributes["0008,0020"] != ORIGINAL_STUDY_DATE


def test_export_dataframe_expand_metadata_returns_the_replacement(tmp_path):
    """The first frozen reader. Its shape is unchanged; its content is."""
    with _ingested(tmp_path) as session:
        session.audit()
        session.anonymize()
        patient = session.store.patients[0]
        df = session.export_dataframe(str(tmp_path / "x.csv"), expand_metadata=True)
        row = df.iloc[0]
        assert row["PatientName"] == "ANONYMIZED"
        assert row["0010,0010"] == "ANONYMIZED"
        assert row["0010,0020"] == patient.patient_id
        assert row["0008,0020"] == format_study_date(patient.studies[0].study_date)
        assert ORIGINAL_NAME not in df.to_csv()
        # Not `ORIGINAL_ID not in df.to_csv()`: CT_small's StudyID
        # (0020,0010) is also "1CT1", and no policy is loaded on this
        # path to touch it -- the no-config policy gap, #495, not this
        # fix. The column this fix owns is asserted above.


def test_get_flattened_instances_returns_the_replacement(tmp_path):
    """The second frozen reader, off the store after a save."""
    with _ingested(tmp_path) as session:
        session.audit()
        session.anonymize()
        session.save(sync=True)
        patient = session.store.patients[0]
        rows = list(session.store_backend.get_flattened_instances())
        assert len(rows) == 1
        attrs = json.loads(rows[0]["attributes_json"])
        assert rows[0]["patient_name"] == "ANONYMIZED"
        assert attrs["0010,0010"] == "ANONYMIZED"
        assert attrs["0010,0020"] == patient.patient_id
        assert attrs["0008,0020"] == format_study_date(patient.studies[0].study_date)


def test_the_documented_path_leaves_every_level_remediated(tmp_path):
    """The write must not leave the instance UNSCANNED: bunch K's manifest
    reads each instance's status, and an UNSCANNED instance reads as not
    anonymized (#486)."""
    with _ingested(tmp_path) as session:
        session.audit()
        session.anonymize()
        patient = session.store.patients[0]
        assert patient.phi_status is PhiStatus.REMEDIATED
        assert patient.studies[0].phi_status is PhiStatus.REMEDIATED
        assert _only_instance(session).phi_status is PhiStatus.REMEDIATED


# ---------------------------------------------------------------------------
# One truth: the entity's value, not a relative edit of the instance's copy
# ---------------------------------------------------------------------------

def test_an_instance_whose_date_differs_from_the_studys_gets_the_studys_date(tmp_path):
    """Kills the relative-shift mutant: shifting the instance's own value by
    the study's offset leaves it different from the study; writing the
    study's value makes them agree, which is what the exporter writes."""
    with _built(tmp_path, instance_dates=("20240101", "20240105")) as session:
        session.audit()
        session.anonymize()
        study = session.store.patients[0].studies[0]
        assert study.date_shifted
        expected = format_study_date(study.study_date)
        assert expected != "20240101"
        for inst in _instances(session.store.patients[0]):
            assert inst.attributes["0008,0020"] == expected


def test_a_study_finding_writes_only_to_its_own_studys_instances(tmp_path):
    """Kills the walk-the-whole-patient mutant."""
    with _built(tmp_path, second_study_date="20230601") as session:
        patient = session.store.patients[0]
        first, second = patient.studies
        findings = [f for f in session.audit()
                    if f.entity_type == "Study"
                    and f.entity_uid == first.study_instance_uid]
        assert len(findings) == 1
        session.anonymize(findings)
        assert first.date_shifted and not second.date_shifted
        assert (first.series[0].instances[0].attributes["0008,0020"]
                == format_study_date(first.study_date))
        assert second.series[0].instances[0].attributes["0008,0020"] == "20230601"


def test_a_patient_finding_reaches_every_study_of_the_patient(tmp_path):
    """A Patient walks all of its studies; the ID written is the hashed
    replacement the Patient now carries, not a second spelling."""
    with _built(tmp_path, second_study_date="20230601") as session:
        patient = session.store.patients[0]
        findings = [f for f in session.audit() if f.entity_type == "Patient"]
        assert {f.tag for f in findings} == {"0010,0010", "0010,0020"}
        session.anonymize(findings)
        for inst in _instances(patient):
            assert inst.attributes["0010,0010"] == "ANONYMIZED"
            assert inst.attributes["0010,0020"] == patient.patient_id
            assert inst.attributes["0010,0020"].startswith("ANON_")


def test_an_instance_that_does_not_carry_the_tag_is_not_given_one(tmp_path):
    """Belt-and-braces stays with the exporter: it stamps the patient
    module on every file, which is its job. Remediation only replaces a
    copy that exists -- writing a tag the file never had fabricates an
    element, the shape #57 rejected for nested findings."""
    with _built(tmp_path) as session:
        inst = _only_instance(session)
        del inst.attributes["0010,0010"]
        inst.mark_modified()
        session.audit()
        session.anonymize()
        assert "0010,0010" not in inst.attributes
        assert inst.attributes["0010,0020"] == session.store.patients[0].patient_id


def test_a_patient_level_remove_clears_the_tag_on_each_instance(tmp_path):
    """The REMOVE_TAG arm for a Python attribute sets it to None; the
    instances' copies go with it, and the write is still a change the
    store must see."""
    with _built(tmp_path, instance_dates=("20240101", "20240101")) as session:
        patient = session.store.patients[0]
        session.audit()
        for inst in _instances(patient):
            inst.mark_persisted()
        finding = PhiFinding(
            entity_uid=patient.patient_id, entity_type="Patient",
            field_name="patient_name", value=patient.patient_name,
            reason="test", tag="0010,0010", patient_id=patient.patient_id,
            entity=patient,
            remediation_proposal=PhiRemediation(
                action_type="REMOVE_TAG", target_attr="patient_name"))
        assert RemediationService().apply_remediation([finding]) == 1
        assert patient.patient_name is None
        for inst in _instances(patient):
            assert "0010,0010" not in inst.attributes
            assert inst.has_unsaved_changes


def test_the_instance_write_goes_through_set_attr_and_marks_the_instance_dirty(tmp_path):
    """A saved instance with no findings of its own was not dirty after
    `anonymize()` before this fix; now it holds a changed tag and must
    say so, or the next save skips it and the store keeps the original."""
    with _built(tmp_path) as session:
        session.save(sync=True)
        inst = _only_instance(session)
        session.audit()
        session.save(sync=True)
        assert not inst.has_unsaved_changes
        session.anonymize()
        assert inst.has_unsaved_changes
        session.save(sync=True)
        rows = list(session.store_backend.get_flattened_instances())
        assert json.loads(rows[0]["attributes_json"])["0010,0010"] == "ANONYMIZED"


# ---------------------------------------------------------------------------
# The instance keeps the status it had, re-recorded at the new revision
# ---------------------------------------------------------------------------

def test_an_instance_with_unapplied_findings_of_its_own_stays_identified(tmp_path):
    """Kills the stamp-REMEDIATED mutant. CT_small carries private tags,
    so the scan marks its instance IDENTIFIED; remediating the patient's
    findings alone must not vouch for it."""
    with _ingested(tmp_path) as session:
        inst = _only_instance(session)
        report = session.audit()
        assert inst.phi_status is PhiStatus.IDENTIFIED
        patient_findings = [f for f in report if f.entity_type == "Patient"]
        assert len(patient_findings) == 2
        session.anonymize(patient_findings)
        assert inst.attributes["0010,0010"] == "ANONYMIZED"
        assert inst.phi_status is PhiStatus.IDENTIFIED
        assert session.store.patients[0].phi_status is PhiStatus.REMEDIATED


def test_an_instance_the_scan_cleared_stays_cleared_after_the_write(tmp_path):
    """Kills the drop-the-re-record mutant. With no tag policy loaded and
    no private tags, the hand-built instance has no findings of its own
    and is CLEARED; the patient's write must leave it CLEARED at its new
    revision, not UNSCANNED -- an UNSCANNED instance reads as not
    anonymized in the manifest (#486)."""
    with _built(tmp_path) as session:
        inst = _only_instance(session)
        session.audit()
        assert inst.phi_status is PhiStatus.CLEARED
        # The public spelling of "the write moved the revision": persisted
        # at the scanned revision, unsaved again after the write.
        inst.mark_persisted()
        assert not inst.has_unsaved_changes
        session.anonymize()
        assert inst.attributes["0010,0010"] == "ANONYMIZED"
        assert inst.has_unsaved_changes
        assert inst.phi_status is PhiStatus.CLEARED


def test_an_instance_edited_after_the_scan_stays_unscanned(tmp_path):
    """The rule's other half: a status the entity already left is not
    revived by the write. The findings are handed in, because a bare
    `anonymize()` re-audits first and that scan would record CLEARED
    over the edit on its own."""
    with _built(tmp_path) as session:
        inst = _only_instance(session)
        findings = list(session.audit())
        inst.set_attr("0008,0080", "Somewhere General")
        assert inst.phi_status is PhiStatus.UNSCANNED
        session.anonymize(findings)
        assert inst.attributes["0010,0010"] == "ANONYMIZED"
        assert inst.phi_status is PhiStatus.UNSCANNED


# ---------------------------------------------------------------------------
# Reversibility and the audit trail
# ---------------------------------------------------------------------------

def test_a_reversible_round_trip_restores_the_originals_on_the_instance(tmp_path):
    """`lock_identities` stashes the first instance's tags before
    `anonymize()` touches them, and `recover_patient_identity` writes
    them back onto every instance and the Patient. The instance now
    carries the replacement in between, and the round trip still ends
    where it started."""
    with _ingested(tmp_path) as session:
        session.enable_reversible_anonymization(str(tmp_path / "k.key"))
        inst = _only_instance(session)
        session.lock_identities(ORIGINAL_ID)
        session.audit()
        session.anonymize()
        patient = session.store.patients[0]
        assert inst.attributes["0010,0010"] == "ANONYMIZED"
        assert inst.attributes["0010,0020"] == patient.patient_id
        session.recover_patient_identity(patient.patient_id, restore=True)
        assert inst.attributes["0010,0010"] == ORIGINAL_NAME
        assert inst.attributes["0010,0020"] == ORIGINAL_ID
        assert patient.patient_name == ORIGINAL_NAME
        assert patient.patient_id == ORIGINAL_ID


def test_the_audit_row_says_how_many_instance_copies_were_written(tmp_path):
    """One row per finding, as before; its `details` now carries the count
    of instance copies the write reached, so the report's story about
    the instances is in the table and not only in memory."""
    with _built(tmp_path, instance_dates=("20240101", "20240101", "20240101")) as session:
        session.audit()
        session.anonymize()
        session.save(sync=True)
    with sqlite3.connect(str(tmp_path / "b.db")) as conn:
        details = [row[0] for row in conn.execute(
            "SELECT details FROM audit_log WHERE action_type='REMEDIATION_REPLACE'")]
    assert any("patient_name -> ANONYMIZED" in d and "3 instance" in d
               for d in details), details


def test_the_export_still_stamps_the_entity_values(tmp_path):
    """Belt-and-braces: the exporter's write-time overwrite is kept."""
    with _ingested(tmp_path) as session:
        session.audit()
        session.anonymize()
        patient = session.store.patients[0]
        out = tmp_path / "out"
        session.export(str(out))
        files = list(out.rglob("*.dcm"))
        assert len(files) == 1
        ds = pydicom.dcmread(files[0])
        assert str(ds.PatientName) == "ANONYMIZED"
        assert ds.PatientID == patient.patient_id
        assert ds.StudyDate == format_study_date(patient.studies[0].study_date)
