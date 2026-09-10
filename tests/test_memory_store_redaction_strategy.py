"""`redact()` on a `:memory:` store runs in threads, on every interpreter (#381).

The redaction worker is the only worker that *writes to the store* from
inside the child: `execute_redaction_task` ends in
`store_backend.persist_pixel_data(inst)`. `SqliteStore.__setstate__`
hands a spawned child `_memory_conn = None`, and `_get_connection` then
opens a fresh, empty in-memory database with no `instance_blobs` table,
so on the processes path -- the default on 3.12, the `python_requires`
floor, and `ISOCENTER_FORCE_PROCESSES=1` elsewhere -- every task failed
with `sqlite3.OperationalError: no such table: instance_blobs` and the
pass raised `RedactionError` naming it. `ingest()`, `audit()`, `save()`
and `export()` on the same store succeed, because their workers read
from a file or return results the parent persists (spec §1.1).

The fix is one keyword argument: `redact()` passes
`force_threads=True` to `_resolve_strategy()` when
`store_backend.db_path == ":memory:"`, and hands the resolved strategy
to `run_parallel()` (#384 moved the resolution fifty lines up; it used
to be the `force_threads=` keyword on the `run_parallel(` call in
`_apply_redaction_rules`). The *argument*, not `ISOCENTER_FORCE_THREADS` -- the
variable is process-global, so set from inside `redact()` it would leak
into every later pool in the process, and #390 measured that it does
not reach every pool anyway. `force_threads` beats
`ISOCENTER_FORCE_PROCESSES` by the documented order, so that variable
does not reopen the failure.

**Every test here sets `ISOCENTER_FORCE_PROCESSES=1`** and deletes the
other two levers, so the processes path is selected structurally on the
free-threaded build too. Without that, 3.14t's default of threads would
decide the test before the fix is consulted, and the suite would be
green there for the wrong reason.

That is a claim about the **killing mutation**, not about the passing
run: with `force_threads=` deleted, `ISOCENTER_FORCE_PROCESSES=1` does
select processes and these tests do go red, on both interpreters. With
the fix in place the lever loses, which is the behaviour under test.
Since #400 that losing lever also logs a `WARNING` naming itself on
every `:memory:` pass here. These tests neither assert it -- that
belongs to `tests/test_memory_store_reports_its_processes_lever.py` --
nor suppress it: a fixture that filtered it would hide the one message
that bunch adds from the only existing tests that trigger it.

The fixture's pixels are **non-zero** and the image is larger than the
zone, on purpose: a zero image redacted to zero is green with the fix
reverted, and a zone that covers the whole frame cannot tell "redacted"
from "replaced with anything".

The spy patches `isocenter.session`'s binding of `run_parallel`, not the
parallel helpers' own module, so this file does not match the text
pattern `tests/test_mutation_probe_targets.py` uses for that module and
needs no `TARGETS` entry (spec §3.1) -- which is also why that module is
not named with its dotted spelling anywhere in this file.
"""
from datetime import date

import numpy as np
import pytest

from isocenter.entities import Equipment, Instance, Patient, Series, Study
from isocenter.session import DicomSession
from isocenter import session as session_module

CT_STORAGE = "1.2.840.10008.5.1.4.1.1.2"
SERIAL = "SN_PROBE"
FILL = 1000
ZONE = [0, 10, 0, 10]  # [y1, y2, x1, x2]


@pytest.fixture(autouse=True)
def processes_path(monkeypatch):
    """Select the processes path on every interpreter (see module docstring)."""
    monkeypatch.setenv("ISOCENTER_FORCE_PROCESSES", "1")
    monkeypatch.delenv("ISOCENTER_FORCE_THREADS", raising=False)
    monkeypatch.delenv("ISOCENTER_MAX_TASKS_PER_CHILD", raising=False)


def _populate(session):
    """Three CT instances on one machine, 16x16, every pixel `FILL`."""
    patient = Patient("PAT1", "Probe^Patient")
    study = Study("ST_1", date(2023, 1, 1))
    study.study_time = "120000"
    series = Series("SE_1", "CT", 1)
    # The redaction index keys on `series.equipment.device_serial_number`
    # (`RedactionIndex.index_store`), not on the (0018,1000) attribute.
    series.equipment = Equipment("Probe", "Model", SERIAL)
    for n in range(3):
        inst = Instance(f"1.2.826.0.1.{n}", CT_STORAGE, n + 1)
        inst.file_path = None
        inst.set_attr("0018,1000", SERIAL)
        inst.set_attr("0008,0060", "CT")
        inst.set_pixel_data(np.full((16, 16), FILL, dtype=np.uint16))
        series.instances.append(inst)
    study.series.append(series)
    patient.studies.append(study)
    session.store.patients.append(patient)
    session.save(sync=True)
    session.configuration.rules = [
        {"serial_number": SERIAL, "redaction_zones": [ZONE]}]
    return session


def _instances(session):
    return [inst
            for p in session.store.patients
            for st in p.studies
            for se in st.series
            for inst in se.instances]


def test_a_memory_store_redacts_through_the_front_door_when_processes_are_requested():
    """The whole pass, on `Session(":memory:")`, with processes requested.

    **Requested, not taken.** The old name said the pass ran under
    processes; measured, it never has, on either interpreter, since #381
    landed -- `force_threads=True` outranks the variable. The name was a
    misnomer and #400's warning is about to say so out loud on every run
    of this test.

    Red on 0.9.3 on both 3.12.13 and 3.14.7t with
    `RedactionError: Redaction failed for 3 of 3 instances ... no such
    table: instance_blobs`. Killing mutation: the `force_threads=`
    argument deleted from the `_resolve_strategy(` call in `redact()` --
    the same one edit, fifty lines further up than it used to be (#384).
    """
    with DicomSession(":memory:") as session:
        _populate(session)

        assert session.redact() == 3

        for inst in _instances(session):
            pixels = inst.get_pixel_data()
            assert not pixels[0:10, 0:10].any(), (
                f"{inst.sop_instance_uid}: the zone was not zeroed")
            assert pixels[12, 12] == FILL, (
                f"{inst.sop_instance_uid}: a pixel outside the zone changed")


def test_the_memory_store_asks_for_threads_per_call_and_a_file_store_does_not(
        tmp_path, monkeypatch):
    """The mechanism, not just the outcome: the keyword, per call.

    Three mutations are red here and the front-door test alone would
    miss two of them. The argument deleted: the first half is red (and
    the front-door test). `force_threads=True` unconditionally: the
    second half is red -- a file store on the floor interpreter must
    keep processes, the only path whose recycling and memory behaviour
    #185 measured. `os.environ["ISOCENTER_FORCE_THREADS"] = "1"` in
    place of the argument: the **second** half is red, and the
    front-door test is green -- exactly the combination that means the
    wrong fix landed (spec §9 item 1). The `:memory:` half stays green
    under it, because the variable does obtain threads for that pass.
    What the variable cannot do is stop: it is process-global, so the
    file store later in the same process inherits what an earlier
    `redact()` leaked into the environment and asks for threads too.
    Measured: `1 failed, 1 passed`, the failure reading
    `it dispatched [True]` against `[False]` on the file store.

    That clause used to say the *first* half went red "because the spy
    sees no argument", and that stopped being true at #384: the spy
    reads `strategy.use_threads` off the resolved strategy, and a
    strategy resolved from the environment carries the same `True` as
    one resolved from the keyword. The test kept killing the mutant --
    the account of how had gone stale, which is its own kind of false
    sentence in a milestone about those.

    The spy reads `kwargs["strategy"].use_threads` since #384, because
    the dispatch now hands `run_parallel` an already-resolved strategy
    rather than the keywords it was resolved from. The two recorded
    expectations are unchanged in meaning. It keys on the strategy's own
    `desc` for the same reason: `desc=` is no longer a keyword of the
    call.
    """
    recorded = []
    real = session_module.run_parallel

    def spy(*args, **kwargs):
        strategy = kwargs.get("strategy")
        if strategy is not None and strategy.desc == "Redacting Pixels":
            recorded.append(strategy.use_threads)
        return real(*args, **kwargs)

    monkeypatch.setattr(session_module, "run_parallel", spy)

    with DicomSession(":memory:") as session:
        _populate(session)
        assert session.redact() == 3
    assert recorded == [True], (
        f"redact() on a :memory: store must resolve a threads strategy "
        f"through the force_threads argument; it dispatched {recorded}")

    recorded.clear()
    with DicomSession(str(tmp_path / "file_store.db")) as session:
        _populate(session)
        assert session.redact() == 3
    assert recorded == [False], (
        f"redact() on a file-backed store must not ask for threads -- the "
        f"processes path is the one #185 measured; it dispatched "
        f"{recorded}")
