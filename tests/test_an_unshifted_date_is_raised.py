"""A date under a SHIFT rule that the pipeline never shifted is raised (#510).

The scan's "already shifted" shortcut asked the *entity*: it skipped
every `SHIFT`/`JITTER` tag on an instance once the instance or its study
was `date_shifted`. A flag set by one date said nothing about the next
one, so a value the pipeline never touched was skipped for ever and the
instance was recorded `CLEARED` over it. Measured on `927cb2b` on
3.12.14 (threads and processes) and 3.14.7t: study date `2023-01-01`,
instance `0008,0023='20230515'`, an empty policy in pass 1 and a `SHIFT`
rule on that tag in pass 2 -- pass 2's `audit()` raised **0** findings,
the value stayed `'20230515'`, the instance read `CLEARED` and the
manifest said `anonymized: true` beside it.

Three ordinary calls reach that shape, which is why it is not a corner:
a rule added to the policy after the first `anonymize()`, a value left
out of `anonymize(findings=[...])`, and any value inside a sequence
(#513's mirror -- no flag exists on a `DicomItem`, so the instance's own
top-level shift hid every nested date it had).

**The rule these tests hold.** The scan asks the value, never the
entity: a value this pipeline shifted is skipped while its tag still
holds what the shift wrote, and everything else under a `SHIFT` rule is
raised, at any depth and however many passes have run. `Study.date_shifted`
is not consulted in the instance arm any more -- a second reading of it
beside the record would be a second answer to one question -- and
`Instance.date_shifted` is gone entirely.

The assertions are intervals, not "the value changed": #517 landed first
so a date first shifted in a later pass lands on the same offset as its
pass-1 siblings, which is the assertion a reader expects. The one-pass
control is here too, asserting what the base already did, because a fix
to the multi-pass path that quietly changed the ordinary one would be
the worse outcome.

**Why this file imports what it does.** It reaches the scan through
`isocenter.privacy`, the arm through `isocenter.remediation` and the
whole pipeline through `isocenter.session`, so it charges those modules'
probe rows; see `test_mutation_probe_targets.py`.
"""
import json
from datetime import date

import pytest

from isocenter.entities import (DicomItem, DicomSequence, Instance, Patient,
                                PhiStatus, Series, Study)
from isocenter.session import DicomSession

SC_SOP_CLASS = "1.2.840.10008.5.1.4.1.1.7"
STUDY_DATE = "0008,0020"
CONTENT_DATE = "0008,0023"
ACQ_DATE = "0008,0022"
SEQ_TAG = "0040,a730"

SHIFT_CONTENT = {CONTENT_DATE: {"name": "ContentDate", "action": "SHIFT"}}
SHIFT_ACQ = {ACQ_DATE: {"name": "AcquisitionDate", "action": "SHIFT"}}
SHIFT_BOTH = {**SHIFT_CONTENT, **SHIFT_ACQ}

#: The PatientID every case here uses, and the offset it seeds with the
#: default jitter config. `P1` rather than `P510`, so the values these
#: tests assert are the values the repro measured on `927cb2b`. One
#: offset for the whole file: every value of this patient must move by
#: it, in whichever pass it is first shifted, which is the promise #510
#: and #517 make together.
PID = "P1"
OFFSET = -286


@pytest.fixture(autouse=True)
def _threads(monkeypatch):
    monkeypatch.setenv("ISOCENTER_FORCE_THREADS", "1")
    monkeypatch.setenv("ISOCENTER_MAX_WORKERS", "2")
    monkeypatch.delenv("ISOCENTER_FORCE_PROCESSES", raising=False)
    monkeypatch.delenv("ISOCENTER_MAX_TASKS_PER_CHILD", raising=False)


def _session(tmp_path, *, study_date=None, attrs=None, nested_date=None,
             tags=None, name="m.db"):
    session = DicomSession(str(tmp_path / name))
    patient = Patient(PID, "Orig^Name")
    study = Study("1.2.826.0.1.510", study_date)
    series = Series("1.2.826.0.1.510.1", "OT", 1)
    instance = Instance("1.2.826.0.1.510.1.0", SC_SOP_CLASS, 1)
    for tag, value in (attrs or {}).items():
        instance.set_attr(tag, value)
    if nested_date is not None:
        item = DicomItem()
        item.set_attr(CONTENT_DATE, nested_date)
        instance.sequences[SEQ_TAG] = DicomSequence(tag=SEQ_TAG, items=[item])
    series.instances.append(instance)
    study.series.append(series)
    patient.studies.append(study)
    session.store.patients.append(patient)
    session.configuration.phi_tags = dict(tags or {})
    return session


def _instance(session):
    return session.store.patients[0].studies[0].series[0].instances[0]


def _nested(session):
    return _instance(session).sequences[SEQ_TAG].items[0]


def _days(after, before):
    """The interval between two DA strings, in days."""
    def _as_date(text):
        return date(int(text[:4]), int(text[4:6]), int(text[6:8]))
    return (_as_date(after) - _as_date(before)).days


def test_a_rule_added_after_the_first_pass_shifts_its_value(tmp_path):
    """#510 as filed. Red before: pass 2 raised 0, the value stayed
    `'20230515'` and the instance read CLEARED over it."""
    session = _session(tmp_path, study_date=date(2023, 1, 1),
                       attrs={CONTENT_DATE: "20230515"}, tags={})
    with session:
        session.anonymize(session.audit())
        study = session.store.patients[0].studies[0]
        assert study.date_shifted and study.study_date != date(2023, 1, 1)
        assert _instance(session).attributes[CONTENT_DATE] == "20230515"

        session.configuration.phi_tags = dict(SHIFT_CONTENT)
        report = session.audit()
        assert len([f for f in report.findings if f.tag == CONTENT_DATE]) == 1
        session.anonymize(report)

        shifted = _instance(session).attributes[CONTENT_DATE]
        assert _days(shifted, "20230515") == OFFSET, shifted
        # The same offset the study's own date got in pass 1, which is
        # what #517 makes true across passes.
        assert (study.study_date - date(2023, 1, 1)).days == OFFSET
        assert _instance(session).phi_status is PhiStatus.REMEDIATED


def test_a_value_left_out_of_a_targeted_anonymize_is_raised_again(tmp_path):
    """The second ordinary route in: `anonymize(findings=[...])` with one
    of two dates. The other one was hidden by the first one's flag."""
    session = _session(tmp_path, attrs={ACQ_DATE: "20230601",
                                        CONTENT_DATE: "20230515"},
                       tags=SHIFT_BOTH)
    with session:
        report = session.audit()
        picked = [f for f in report.findings if f.tag == ACQ_DATE]
        assert len(picked) == 1
        session.anonymize(picked)
        assert _instance(session).attributes[CONTENT_DATE] == "20230515"

        again = session.audit()
        assert len([f for f in again.findings if f.tag == CONTENT_DATE]) == 1
        session.anonymize(again)

    assert _days(_instance(session).attributes[CONTENT_DATE], "20230515") == OFFSET
    assert _days(_instance(session).attributes[ACQ_DATE], "20230601") == OFFSET


def test_a_nested_date_the_instances_own_shift_used_to_hide_is_raised(tmp_path):
    """#513's mirror, which is #510 reached through a sequence: the
    instance's top-level date shifts in pass 1, the nested tag joins the
    policy in pass 2. Red before: pass 2 raised 0 and the real date was
    exported while the instance read CLEARED."""
    session = _session(tmp_path, attrs={ACQ_DATE: "20230601"},
                       nested_date="20230515", tags=SHIFT_ACQ)
    with session:
        session.anonymize(session.audit())
        assert _nested(session).attributes[CONTENT_DATE] == "20230515"

        session.configuration.phi_tags = dict(SHIFT_BOTH)
        report = session.audit()
        assert len([f for f in report.findings if f.tag == CONTENT_DATE]) == 1
        session.anonymize(report)

        top = _instance(session).attributes[ACQ_DATE]
        nested = _nested(session).attributes[CONTENT_DATE]

    assert _days(top, "20230601") == OFFSET
    assert _days(nested, "20230515") == OFFSET, nested


def test_the_manifest_stops_claiming_a_value_it_never_handled(tmp_path):
    """What a user actually read: the JSON manifest said
    `anonymized: true` beside an untouched date. Now the instance is
    IDENTIFIED until the value is really shifted."""
    session = _session(tmp_path, study_date=date(2023, 1, 1),
                       attrs={CONTENT_DATE: "20230515"}, tags={})
    with session:
        session.anonymize(session.audit())
        session.configuration.phi_tags = dict(SHIFT_CONTENT)
        session.audit()
        assert _instance(session).phi_status is PhiStatus.IDENTIFIED
        out = tmp_path / "manifest.json"
        session.generate_manifest(str(out), format="json")
        items = json.loads(out.read_text(encoding="utf-8"))["items"]
        assert [i["anonymized"] for i in items] == [False]


def test_the_instance_arm_does_not_read_the_studys_date_shifted_flag(tmp_path):
    """Stated directly, because the fix is exactly the deletion of those
    two reads. The study's flag is set by hand with no record anywhere,
    and the value must still be raised; the instance's own flag, the
    other half of the old condition, no longer exists to set."""
    session = _session(tmp_path, study_date=date(2023, 1, 1),
                       attrs={CONTENT_DATE: "20230515"}, tags=SHIFT_CONTENT)
    study = session.store.patients[0].studies[0]
    study.date_shifted = True
    with session:
        report = session.audit()
        assert len([f for f in report.findings if f.tag == CONTENT_DATE]) == 1
        session.anonymize(report)
        assert _days(_instance(session).attributes[CONTENT_DATE],
                     "20230515") == OFFSET


def test_one_pass_still_moves_every_value_by_one_offset(tmp_path):
    """The control, green on the base too. The study's date, a top-level
    instance date and a nested date, all under `SHIFT`, in a single
    pass: three values, one offset. A fix to the multi-pass path that
    changed the ordinary one would be the worse outcome."""
    session = _session(tmp_path, study_date=date(2023, 1, 1),
                       attrs={ACQ_DATE: "20230601"}, nested_date="20230515",
                       tags={**SHIFT_BOTH,
                             STUDY_DATE: {"name": "StudyDate", "action": "SHIFT"}})
    with session:
        session.anonymize(session.audit())
        study = session.store.patients[0].studies[0]
        assert (study.study_date - date(2023, 1, 1)).days == OFFSET
        assert _days(_instance(session).attributes[ACQ_DATE], "20230601") == OFFSET
        assert _days(_nested(session).attributes[CONTENT_DATE],
                     "20230515") == OFFSET


def test_three_passes_over_a_settled_graph_change_nothing(tmp_path):
    """Idempotence, as the shape a user hits: `audit()` + `anonymize()`
    repeated produces the value the first pass produced, with no further
    audit rows. The counterpart to #513's drift, from #510's side."""
    session = _session(tmp_path, study_date=date(2023, 1, 1),
                       attrs={ACQ_DATE: "20230601"}, nested_date="20230515",
                       tags=SHIFT_BOTH)
    with session:
        session.anonymize(session.audit())
        settled = (session.store.patients[0].studies[0].study_date,
                   _instance(session).attributes[ACQ_DATE],
                   _nested(session).attributes[CONTENT_DATE])
        for _ in range(3):
            report = session.audit()
            assert [f for f in report.findings
                    if f.tag in (ACQ_DATE, CONTENT_DATE)] == []
            session.anonymize(report)
        assert (session.store.patients[0].studies[0].study_date,
                _instance(session).attributes[ACQ_DATE],
                _nested(session).attributes[CONTENT_DATE]) == settled
