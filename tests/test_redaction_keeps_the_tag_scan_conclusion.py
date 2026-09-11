"""Redaction keeps an instance's tag-scan conclusion across its own writes (#486).

**Confirmed by the owner on 2026-09-11.** This is option 2 of the two put to
the owner on #486, landed in its own commit so it could be reverted whole.
The owner picked it, so it stands; option 1 would have been the docs gaining
"call `audit()` after `redact()` before `generate_manifest()`" instead.

`PhiStatus` is valid only at the revision it was recorded at, and
`redact()` writes to every instance it redacts: the pixels, `ImageType`,
`BurnedInAnnotation`, `DerivationDescription`, a Derivation Code Sequence
and a new SOP Instance UID. Measured on e184933, 3.12.14: after
`anonymize()` an instance read CLEARED at revision 12, and after `redact()`
UNSCANNED at revision 19. So on the documented anonymize -> redact ->
export path, every redacted instance's manifest item would read
`"anonymized": false` under #486's rule.

What these tests hold:

- **Both call sites of `_apply_redaction_flags`**: `Session.redact()`'s
  parallel pass, under threads (the worker writes the live instance) and
  under processes (the parent copies the worker's result across), and the
  serial `RedactionService.redact_machine_instances`.
- **The guard.** The status is re-recorded only when every attribute and
  every nested item outside redaction's own writes is exactly as it was
  before the pass. An edit to any other tag during the pass, top-level or
  nested, leaves the instance UNSCANNED: that edit is what the revision rule
  exists to catch, and redaction must not launder it.
- **Only an assurance is carried.** An instance that was never scanned
  stays UNSCANNED after redaction.
"""
from datetime import date

import numpy as np
import pytest

from isocenter.entities import (DicomItem, DicomSequence, Equipment, Instance,
                                Patient, PhiStatus, Series, Study)
from isocenter.services import RedactionService
from isocenter.session import DicomSession

SC_SOP_CLASS = "1.2.840.10008.5.1.4.1.1.7"
SERIAL = "SN-486"
ZONE = [0, 8, 0, 8]
UID = "1.2.826.0.1.486.1.0"


@pytest.fixture
def strategy(request, monkeypatch):
    monkeypatch.setenv("ISOCENTER_MAX_WORKERS", "2")
    monkeypatch.delenv("ISOCENTER_MAX_TASKS_PER_CHILD", raising=False)
    if request.param == "processes":
        monkeypatch.delenv("ISOCENTER_FORCE_THREADS", raising=False)
        monkeypatch.setenv("ISOCENTER_FORCE_PROCESSES", "1")
    else:
        monkeypatch.delenv("ISOCENTER_FORCE_PROCESSES", raising=False)
        monkeypatch.setenv("ISOCENTER_FORCE_THREADS", "1")
    return request.param


@pytest.fixture
def threads(monkeypatch):
    monkeypatch.setenv("ISOCENTER_MAX_WORKERS", "2")
    monkeypatch.setenv("ISOCENTER_FORCE_THREADS", "1")
    monkeypatch.delenv("ISOCENTER_FORCE_PROCESSES", raising=False)
    monkeypatch.delenv("ISOCENTER_MAX_TASKS_PER_CHILD", raising=False)


def _session(tmp_path, nested=False):
    session = DicomSession(str(tmp_path / "carry.db"))
    patient = Patient("P486", "Original^Name")
    study = Study("1.2.826.0.1.486", date(2023, 1, 1))
    series = Series("1.2.826.0.1.486.1", "OT", 1)
    series.equipment = Equipment("Acme", "Model", SERIAL)
    instance = Instance(UID, SC_SOP_CLASS, 1)
    instance.file_path = None
    instance.set_pixel_data(np.full((16, 16), 200, dtype=np.uint8))
    if nested:
        item = DicomItem()
        item.set_attr("0040,0007", "Original step description")
        seq = DicomSequence(tag="0040,0275")
        seq.items.append(item)
        instance.sequences["0040,0275"] = seq
    series.instances.append(instance)
    study.series.append(series)
    patient.studies.append(study)
    session.store.patients.append(patient)
    session.configuration.rules = [{"serial_number": SERIAL, "redaction_zones": [ZONE]}]
    session.save(sync=True)
    return session, instance


def _redacted(instance):
    """Proof the pass wrote the instance, so a kept status is not a skip."""
    return (instance.attributes.get("0028,0301") == "NO"
            and instance.sop_instance_uid != UID)


@pytest.mark.parametrize("strategy", ["threads", "processes"], indirect=True)
def test_redact_keeps_the_status_anonymize_recorded(tmp_path, strategy, capsys):
    """Red before: UNSCANNED after `redact()`, and the manifest says false.

    Kills: the carry in `_apply_redaction_rules` removed.

    **The strategy is asserted, not assumed.** The processes arm is the one
    where the parent copies a worker's result onto the live instance, and
    an arm that silently resolved threads would pass here while proving
    nothing about that path. `redact()` prints the strategy it resolved.
    """
    session, instance = _session(tmp_path)
    with session:
        session.anonymize()
        before = instance.phi_status
        assert before in (PhiStatus.REMEDIATED, PhiStatus.CLEARED)
        session.save(sync=True)
        capsys.readouterr()
        session.redact()
        assert f"({strategy})" in capsys.readouterr().out
        assert _redacted(instance), instance.attributes
        assert instance.phi_status is before
        # And so the manifest, which is what #486 is about.
        assert session._manifest_anonymized(  # pylint: disable=protected-access
            session.store.patients[0], session.store.patients[0].studies[0],
            instance)


def test_the_serial_path_keeps_it_too(tmp_path, threads):
    """`redact_machine_instances`, the other `_apply_redaction_flags` site.

    Kills: the carry in `redact_machine_instances` removed.
    """
    session, instance = _session(tmp_path)
    with session:
        session.anonymize()
        before = instance.phi_status
        service = RedactionService(session.store, session.store_backend)
        service.redact_machine_instances(SERIAL, [tuple(ZONE)], targets=[instance],
                                         show_progress=False)
        assert _redacted(instance), instance.attributes
        assert instance.phi_status is before


def _edit_during_the_pass(monkeypatch, edit):
    """Make redaction's own flag write also perform `edit` on the instance.

    `_apply_redaction_flags` runs on the live instance under threads and
    on the serial path, between the capture and the carry, so an edit made
    here is an edit made during the pass by someone other than redaction.
    """
    real = RedactionService._apply_redaction_flags  # pylint: disable=protected-access

    def flags_and_an_edit(self, inst):
        real(self, inst)
        edit(inst)
    monkeypatch.setattr(RedactionService, "_apply_redaction_flags", flags_and_an_edit)


def _top_level_edit(inst):
    inst.set_attr("0008,103e", "Edited during the pass")


def _nested_edit(inst):
    inst.sequences["0040,0275"].items[0].set_attr("0040,0007", "Edited during the pass")


@pytest.mark.parametrize("edit", [_top_level_edit, _nested_edit],
                         ids=["top-level", "nested"])
@pytest.mark.parametrize("path", ["session", "serial"])
def test_an_edit_during_the_pass_is_not_carried(tmp_path, threads, monkeypatch,
                                                edit, path):
    """The guard: anything but redaction's own writes, and no carry.

    Kills: the fingerprint comparison removed (top-level and nested), and a
    fingerprint that ignores nested items (nested).
    """
    session, instance = _session(tmp_path, nested=True)
    with session:
        session.anonymize()
        assert instance.phi_status in (PhiStatus.REMEDIATED, PhiStatus.CLEARED)
        session.save(sync=True)
        _edit_during_the_pass(monkeypatch, edit)
        if path == "session":
            session.redact()
        else:
            RedactionService(session.store, session.store_backend).redact_machine_instances(
                SERIAL, [tuple(ZONE)], targets=[instance], show_progress=False)
        assert _redacted(instance), instance.attributes
        assert instance.phi_status is PhiStatus.UNSCANNED


def test_an_instance_never_scanned_stays_unscanned(tmp_path, threads):
    """Only an assurance is carried, and there was none to carry."""
    session, instance = _session(tmp_path)
    with session:
        assert instance.phi_status is PhiStatus.UNSCANNED
        session.redact()
        assert _redacted(instance), instance.attributes
        assert instance.phi_status is PhiStatus.UNSCANNED
