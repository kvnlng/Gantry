"""The date jitter is seeded on the patient, not on the PatientID text (#517).

`_get_date_shift` hashed whatever PatientID the scan saw, and pass 1 of
`anonymize()` replaces that id with `ANON_<12 hex of its digest>` -- so
every later pass hashed a different string and computed a different
offset. Measured on `927cb2b`: `P1` gives **-286 days** and the
replacement it produces, `ANON_fbeae7c18667`, gives **-47**. Any date
first shifted in a later pass therefore landed off the offset its
siblings got, and the documented promise -- "date jitter is deterministic
per patient so intervals survive" -- held only within a single pass.

**The rule these tests hold.** One identity, one offset, whichever
spelling of its id the pass happens to read. The canonical key is the
digest the `ANON_` replacement already carries (the replacement is
`ANON_` plus the first 12 hex characters of the same SHA-256 the jitter
reads 8 of), so an un-replaced id seeds *exactly* as it did in 0.9.5 --
`_get_date_shift("P1") == -286` is asserted as a literal here so a change
to the arithmetic cannot pass by moving both sides of an equality
together. That bit-for-bit property is what makes this fix carry no
migration question: a date newly shifted in a store whose earlier dates
were shifted before the upgrade lands on the same offset as those dates.

The coupling between the two modules' spellings is load-bearing and is
pinned in both directions: shorten the digest the replacement carries
below the eight characters the jitter reads, or hash the replacement
string instead of reading its digest, and these tests go red.

**Why this file imports what it does.** It reaches the seed through
`isocenter.privacy` and the offset through `isocenter.remediation`, and
the end-to-end cases run a whole `isocenter.session`, so it charges all
three modules' probe rows; see `test_mutation_probe_targets.py`.
"""
import hashlib
import os
import shutil
from datetime import date

import pydicom
import pytest

from isocenter.entities import Instance, Patient, Series, Study
from isocenter.privacy import _replacement_id_for, jitter_digest
from isocenter.remediation import RemediationService
from isocenter.session import DicomSession

SC_SOP_CLASS = "1.2.840.10008.5.1.4.1.1.7"

#: Ids whose canonical key must survive replacement. The last one is the
#: awkward one: it starts with the prefix and is not a digest, so it is
#: hashed on both sides of the replacement rather than read back.
#: An id that *is* digest-shaped (`ANON_deadbeefcafe`) is deliberately
#: absent -- see
#: `test_an_id_that_is_already_replacement_shaped_seeds_from_its_own_text`.
IDS = ["P1", "1CT1", "", "12345", "MRN-00-77", "patient/with,commas",
       "ANON_not_hex"]


@pytest.fixture(autouse=True)
def _threads(monkeypatch):
    monkeypatch.setenv("ISOCENTER_FORCE_THREADS", "1")
    monkeypatch.setenv("ISOCENTER_MAX_WORKERS", "2")
    monkeypatch.delenv("ISOCENTER_FORCE_PROCESSES", raising=False)
    monkeypatch.delenv("ISOCENTER_MAX_TASKS_PER_CHILD", raising=False)


@pytest.mark.parametrize("patient_id", IDS)
def test_the_replacement_carries_the_canonical_key(patient_id):
    """Red before: the replacement hashed to its own, different key.

    Both halves, because either alone stays green while the other
    drifts: the key itself, and the offset the key produces.
    """
    replacement = _replacement_id_for(patient_id)
    assert jitter_digest(replacement) == jitter_digest(patient_id), (
        f"{patient_id!r} and its replacement {replacement!r} are one "
        f"identity and must give one seed")
    service = RemediationService()
    assert (service._get_date_shift(replacement)
            == service._get_date_shift(patient_id)), (
        f"the offset moved when {patient_id!r} was replaced")


def test_the_replacement_still_carries_enough_of_the_digest():
    """The coupling, stated as the inequality it depends on.

    `jitter_digest` reads its 8 characters out of the replacement's 12.
    Shorten the 12 and the read-back stops being the original's digest,
    silently -- so the relation is asserted rather than left implied.
    """
    from isocenter.privacy import (_JITTER_DIGEST_CHARS,
                                   _REPLACEMENT_DIGEST_CHARS)
    assert _REPLACEMENT_DIGEST_CHARS >= _JITTER_DIGEST_CHARS
    replacement = _replacement_id_for("P1")
    assert replacement == "ANON_fbeae7c18667", replacement
    assert (replacement[5:5 + _JITTER_DIGEST_CHARS]
            == hashlib.sha256(b"P1").hexdigest()[:_JITTER_DIGEST_CHARS])


def test_the_offset_is_what_0_9_5_computed_for_an_unreplaced_id():
    """The literal, so the arithmetic cannot move with the assertion.

    Measured on 0.9.5 and on `927cb2b` with the default jitter config
    (`min_days=-365`, `max_days=-1`): `P1` is -286 days and `1CT1`,
    `CT_small.dcm`'s PatientID, is -239.
    """
    service = RemediationService()
    assert service._get_date_shift("P1") == -286
    assert service._get_date_shift("1CT1") == -239
    assert jitter_digest("P1") == "fbeae7c1"


@pytest.mark.parametrize("patient_id", [
    "ANON_not_hex", "ANON_", "ANON_abc", "ANON_ABCDEF12", "ANON_deadbee"])
def test_an_id_that_only_looks_like_a_replacement_is_hashed(patient_id):
    """The guard, without which `int(...)` on those eight characters
    either raises or reads a wrong key. Uppercase hex is refused too:
    the constructor emits lowercase, so anything else is a user's id.
    """
    assert jitter_digest(patient_id) == hashlib.sha256(
        patient_id.encode()).hexdigest()[:8]
    assert RemediationService()._get_date_shift(patient_id) == (
        (int(hashlib.sha256(patient_id.encode()).hexdigest()[:8], 16) % 365) - 365)


def test_an_id_that_is_already_replacement_shaped_seeds_from_its_own_text():
    """The pre-existing edge, stated so it is not mistaken for the fix.

    A real PatientID whose eight characters after `ANON_` are all
    lowercase hex is read as a replacement and seeds from the digest it
    appears to carry, not from a hash of itself -- eight characters read,
    not eight characters long, so the 12-hex id below qualifies. That is
    consistent with the rest of the scan --
    `_is_replacement_id` already treats such an id as anonymized and
    `scan_patient` never proposes a replacement for it, so the
    "replacement" spelling is the only spelling this pipeline will ever
    see. The behaviour is stable, merely arbitrary; #517 does not widen
    it and does not narrow it.
    """
    assert jitter_digest("ANON_deadbeefcafe") == "deadbeef"
    assert jitter_digest("ANON_deadbeefcafe") != hashlib.sha256(
        b"ANON_deadbeefcafe").hexdigest()[:8]


def _built(tmp_path, pid="P1"):
    session = DicomSession(str(tmp_path / "seed.db"))
    patient = Patient(pid, "Orig^Name")
    session.store.patients.append(patient)
    return session, patient


def _with_study(patient, suffix, study_date):
    study = Study(f"1.2.826.0.1.517.{suffix}", study_date)
    series = Series(f"1.2.826.0.1.517.{suffix}.1", "OT", 1)
    instance = Instance(f"1.2.826.0.1.517.{suffix}.1.0", SC_SOP_CLASS, 1)
    series.instances.append(instance)
    study.series.append(series)
    patient.studies.append(study)
    return study


def test_a_study_shifted_in_a_later_pass_keeps_the_interval(tmp_path):
    """The defect, end to end, on a path that needs nothing else fixed.

    A study added to the graph after pass 1 is raised by pass 2's scan --
    its own flag is clear -- but by then PatientID is the replacement.
    Red before: study A moved -286 days and study B -47, so the five
    months between them became nine.
    """
    session, patient = _built(tmp_path)
    with session:
        first = _with_study(patient, "a", date(2023, 1, 1))
        session.anonymize(session.audit())
        shifted_a = first.study_date
        assert (shifted_a - date(2023, 1, 1)).days == -286, shifted_a

        # Pass 1 replaced PatientID, so pass 2 reads the replacement.
        assert patient.patient_id.startswith("ANON_")
        second = _with_study(patient, "b", date(2023, 6, 1))
        session.anonymize(session.audit())
        shifted_b = second.study_date

        assert (shifted_b - date(2023, 6, 1)).days == -286, shifted_b
        assert (shifted_b - shifted_a) == (date(2023, 6, 1) - date(2023, 1, 1)), (
            "the interval between two studies of one patient did not survive")


def test_a_reingested_export_keeps_its_patients_offset(tmp_path):
    """One patient, two stores: the anonymized export's `ANON_` id
    canonicalizes to the digest the first store seeded on, so the same
    patient keeps the same offset. Red before: -239 then -47.
    """
    source = tmp_path / "in"
    source.mkdir()
    shutil.copy(pydicom.data.get_testdata_file("CT_small.dcm"),
                os.path.join(str(source), "CT_small.dcm"))
    out = str(tmp_path / "out")

    first = DicomSession(str(tmp_path / "one.db"))
    with first:
        first.ingest(str(source))
        study = first.store.patients[0].studies[0]
        before_one = study.study_date
        first.anonymize(first.audit())
        offset_one = (study.study_date - before_one).days
        assert offset_one == -239, offset_one
        first.export(out)

    second = DicomSession(str(tmp_path / "two.db"))
    with second:
        second.ingest(out)
        study_two = second.store.patients[0].studies[0]
        assert str(study_two.study_date).replace("-", "") == \
            str(study.study_date).replace("-", "")
        before_two = study_two.study_date
        second.anonymize(second.audit())
        offset_two = (study_two.study_date - before_two).days

    assert offset_two == offset_one, (
        "a re-ingested anonymized export got a second offset for one patient")
