"""Probe B: what redact() prints, and what it actually ran on."""
import contextlib
import io
import os
import sys
import tempfile
from datetime import date

import numpy as np

import isocenter
from isocenter import parallel
from isocenter.entities import Equipment, Instance, Patient, Series, Study
from isocenter.session import DicomSession

CT = "1.2.840.10008.5.1.4.1.1.2"
SERIAL = "SN_PROBE"
FILL = 1000
ZONE = [0, 10, 0, 10]
LEVERS = ("ISOCENTER_FORCE_THREADS", "ISOCENTER_FORCE_PROCESSES",
          "ISOCENTER_MAX_TASKS_PER_CHILD")
seen = dict()


def populate(session):
    p = Patient("PAT1", "Probe^Patient")
    st = Study("ST_1", date(2023, 1, 1))
    st.study_time = "120000"
    se = Series("SE_1", "CT", 1)
    se.equipment = Equipment("Probe", "Model", SERIAL)
    for n in range(3):
        i = Instance("1.2.826.0.1." + str(n), CT, n + 1)
        i.file_path = None
        i.set_attr("0018,1000", SERIAL)
        i.set_attr("0008,0060", "CT")
        i.set_pixel_data(np.full((16, 16), FILL, dtype=np.uint16))
        se.instances.append(i)
    st.series.append(se)
    p.studies.append(st)
    session.store.patients.append(p)
    session.save(sync=True)
    rule = dict(serial_number=SERIAL, redaction_zones=[ZONE])
    session.configuration.rules = [rule]


def install_spies():
    """Record the strategy resolved, and which of the three paths ran."""
    real_resolve = parallel._resolve_strategy
    real_new = parallel._run_on_new_executor
    real_recycle = parallel._run_on_recycling_pool
    real_shared = parallel._run_on_shared_executor

    def spy_resolve(*a, **k):
        s = real_resolve(*a, **k)
        if s.desc == "Redacting Pixels":
            seen["strategy"] = s
        return s

    def spy_new(func, items, strategy):
        if strategy.desc == "Redacting Pixels":
            seen["path"] = ("ThreadPoolExecutor" if strategy.use_threads
                            else "ProcessPoolExecutor")
        return real_new(func, items, strategy)

    def spy_recycle(func, items, strategy):
        if strategy.desc == "Redacting Pixels":
            seen["path"] = "recycling multiprocessing.Pool"
        return real_recycle(func, items, strategy)

    def spy_shared(executor, func, items, strategy):
        if strategy.desc == "Redacting Pixels":
            seen["path"] = "shared executor"
        return real_shared(executor, func, items, strategy)

    parallel._resolve_strategy = spy_resolve
    parallel._run_on_new_executor = spy_new
    parallel._run_on_recycling_pool = spy_recycle
    parallel._run_on_shared_executor = spy_shared


def arm(label, db_path, env):
    for name in LEVERS:
        os.environ.pop(name, None)
    os.environ.update(env)
    seen.clear()
    out = io.StringIO()
    zone_zeroed = outside_kept = None
    try:
        with contextlib.redirect_stdout(out):
            with DicomSession(db_path) as s:
                populate(s)
                result = ("returned", s.redact())
                px = [i.get_pixel_data()
                      for p in s.store.patients for st in p.studies
                      for se in st.series for i in se.instances]
                zone_zeroed = all(not a[0:10, 0:10].any() for a in px)
                outside_kept = all(a[12, 12] == FILL for a in px)
    except Exception as exc:
        result = ("raised", type(exc).__name__ + ": " + str(exc))
    st = seen.get("strategy")
    print("--- " + label)
    print("    env                : " + str(sorted(env.items())))
    print("    strategy.use_threads: " + str(st.use_threads if st else "NO DISPATCH")
          + "   max_workers=" + str(st.max_workers if st else "-")
          + "   maxtasksperchild=" + str(st.maxtasksperchild if st else "-"))
    print("    executor path      : " + str(seen.get("path", "none")))
    print("    outcome            : " + str(result))
    print("    zone zeroed / outside kept: "
          + str(zone_zeroed) + " / " + str(outside_kept))
    print("    stdout, verbatim:")
    for line in out.getvalue().splitlines():
        print("      | " + line)
    print()


if __name__ == "__main__":
    os.environ["ISOCENTER_SHOW_PROGRESS"] = "0"
    print("isocenter.__file__ =", isocenter.__file__)
    print("python =", sys.version.replace("\n", " "))
    print("gil_enabled =",
          sys._is_gil_enabled() if hasattr(sys, "_is_gil_enabled") else "no attr")
    print()
    install_spies()
    FP = dict(ISOCENTER_FORCE_PROCESSES="1")
    MT = dict(ISOCENTER_MAX_TASKS_PER_CHILD="1")
    FT = dict(ISOCENTER_FORCE_THREADS="1")
    BOTH = dict(ISOCENTER_FORCE_THREADS="1", ISOCENTER_FORCE_PROCESSES="1")
    with tempfile.TemporaryDirectory() as tmp:
        arm("A  :memory:  no levers", ":memory:", dict())
        arm("B  :memory:  FORCE_PROCESSES=1", ":memory:", FP)
        arm("C  :memory:  MAX_TASKS_PER_CHILD=1", ":memory:", MT)
        arm("D  :memory:  FORCE_THREADS=1 + FORCE_PROCESSES=1", ":memory:", BOTH)
        arm("E  file      no levers", os.path.join(tmp, "e.db"), dict())
        arm("F  file      FORCE_PROCESSES=1", os.path.join(tmp, "f.db"), FP)
        arm("G  file      FORCE_THREADS=1", os.path.join(tmp, "g.db"), FT)
