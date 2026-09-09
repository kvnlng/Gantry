"""`lock_identities` has a closed signature, and the README's call works (#379, Q7).

Until 0.9.4 the signature was `(patient_id, persist=False,
_patient_obj=None, verbose=True, **kwargs)`: a private-named
optimisation argument in the public parameter list, and an open
`**kwargs` that carried two different things to two different places --
`tags_to_lock` was read on the single-patient path, and everything was
forwarded to `lock_identities_batch` on the batch path. Measured before
the change, on 3.12.13:

* `session.lock_identities(report, tags_to_lock=[...])` -- the call
  `README.md` and `docs/quickstart.md` teach -- raised
  `TypeError: DicomSession.lock_identities_batch() got an unexpected
  keyword argument 'tags_to_lock'`, because the batch method took only
  `auto_persist_chunk_size`. The documented pipeline's reversible step
  did not run. Nothing caught it: `tests/test_documented_api_exists.py`
  grades the *method names* in the README's fences, not their keyword
  arguments.
* `session.lock_identities("P1", bogus=1)` was accepted in silence, so a
  misspelled `tags_to_lock` locked the default tags and said nothing.

The freeze (#379) pins parameter names, and a pin on `_patient_obj` and
`**kwargs` would have frozen both defects. The owner chose to strip them
before the tag (Q7(b)). What replaces them: `tags_to_lock` is an
explicit parameter on both methods, `lock_identities_batch` passes it
down per patient, and the patient lookup lives in a private helper that
takes the resolved `Patient`. `auto_persist_chunk_size` is the batch
method's alone -- on the single-patient path it did nothing, and a
parameter that does nothing on a path is a dead argument there (CLAUDE.md,
"one spelling per behaviour").

Its own file: `tests/test_reversibility.py` is listed under two probe
targets and these tests buy no kill signal against either. The report
and finding classes are reached through `isocenter.session`, which
binds both, so this file names no probe target's module.
"""
import inspect

import pytest

from isocenter import session as session_module
from isocenter.session import DicomSession
from isocenter.entities import Instance, Patient, Series, Study

PhiFinding = session_module.PhiFinding
PhiReport = session_module.PhiReport

TAGS = ["0010,0010", "0010,0020", "0010,0030"]


@pytest.fixture
def session(tmp_path):
    with DicomSession(str(tmp_path / "lock.db")) as s:
        s.enable_reversible_anonymization(str(tmp_path / "k.key"))
        for pid in ("P1", "P2"):
            patient = Patient(pid, f"Name^{pid}")
            study = Study(f"ST_{pid}", None)
            series = Series(f"SE_{pid}", "CT", 1)
            inst = Instance(f"1.2.3.{pid}", "1.2.840.10008.5.1.4.1.1.2", 1)
            inst.set_attr("0010,0010", f"Name^{pid}")
            inst.set_attr("0010,0020", pid)
            series.instances.append(inst)
            study.series.append(series)
            patient.studies.append(study)
            s.store.patients.append(patient)
        yield s


def _report_for(*pids):
    return PhiReport([
        PhiFinding(entity_uid=f"1.2.3.{pid}", entity_type="Instance",
                   field_name="PatientName", value=f"Name^{pid}",
                   reason="test", tag="0010,0010", patient_id=pid)
        for pid in pids])


def test_the_readme_call_locks_every_patient_in_the_report(session):
    """`lock_identities(report, tags_to_lock=[...])`, as README.md:160 spells it.

    Red before the change with the `TypeError` in the module docstring.
    """
    result = session.lock_identities(_report_for("P1", "P2"), tags_to_lock=TAGS)

    assert len(result) == 2, "one instance per patient should be locked"
    recovered = [session.reversibility_service.recover_original_data(inst)
                 for inst in result]
    assert sorted(r["0010,0020"] for r in recovered) == ["P1", "P2"], (
        f"each patient's instance must carry its own identity token: {recovered}")


def test_tags_to_lock_reaches_each_patient_on_the_batch_path(session):
    """The tags asked for are the tags embedded, patient by patient.

    A batch that accepted `tags_to_lock` and dropped it on the floor
    would pass the test above; this one decrypts the token and looks.
    Killing mutation: `tags_to_lock` not forwarded from the batch loop.
    """
    session.lock_identities(["P1"], tags_to_lock=["0010,0020"])
    inst = session.store.patients[0].studies[0].series[0].instances[0]
    recovered = session.reversibility_service.recover_original_data(inst)

    assert set(recovered) == {"0010,0020"}, recovered


def test_the_private_optimisation_argument_is_gone(session):
    with pytest.raises(TypeError, match="_patient_obj"):
        session.lock_identities("P1", _patient_obj=None)


def test_an_unknown_keyword_is_refused_rather_than_swallowed(session):
    """A misspelled `tags_to_lock` used to lock the defaults in silence."""
    with pytest.raises(TypeError, match="tags_to_lokc"):
        session.lock_identities("P1", tags_to_lokc=TAGS)


def test_chunked_persistence_is_the_batch_methods_alone(session):
    """`auto_persist_chunk_size` does nothing for one patient and is not accepted there."""
    with pytest.raises(TypeError, match="auto_persist_chunk_size"):
        session.lock_identities("P1", auto_persist_chunk_size=1)

    assert session.lock_identities_batch(["P1", "P2"], auto_persist_chunk_size=1) == []


def test_the_old_positional_slot_fails_loudly(session):
    """`lock_identities(pid, False, patient)` filled `_patient_obj` positionally.

    With that parameter gone, the third slot would be `verbose` and a
    `Patient` would be read as a truthy flag in silence. `verbose` and
    `tags_to_lock` are keyword-only so the old call is a `TypeError`
    instead. Killing mutation: the `*` removed from the signature.
    """
    patient = session.store.patients[0]
    with pytest.raises(TypeError, match="positional"):
        session.lock_identities("P1", False, patient)


def test_the_signatures_are_closed():
    """No `**kwargs`, no underscored name, on either method; the rest keyword-only."""
    for method in (DicomSession.lock_identities, DicomSession.lock_identities_batch):
        params = inspect.signature(method).parameters
        assert not any(p.kind is inspect.Parameter.VAR_KEYWORD for p in params.values()), (
            f"{method.__name__} still takes **kwargs")
        assert not any(name.startswith("_") for name in params), (
            f"{method.__name__} exposes a private-named parameter: {list(params)}")

    params = inspect.signature(DicomSession.lock_identities).parameters
    assert [p.kind for p in params.values()][3:] == [inspect.Parameter.KEYWORD_ONLY] * 2, (
        "verbose and tags_to_lock must be keyword-only (see the docstring)")
