"""Probe A: the real threads-or-processes decision table, measured.

Calls the shipped `_resolve_strategy` (not a re-implementation) for every
combination of the three levers, plus the two argument spellings, and
reports `use_threads` and which pool `run_parallel` would dispatch to.
"""
import itertools
import os
import sys

import isocenter
from isocenter import parallel

print("isocenter.__file__ =", isocenter.__file__)
print("python            =", sys.version.replace("\n", " "))
print("has _is_gil_enabled =", hasattr(sys, "_is_gil_enabled"),
      "gil_enabled =",
      sys._is_gil_enabled() if hasattr(sys, "_is_gil_enabled") else "n/a")
print()

LEVERS = ("ISOCENTER_FORCE_THREADS", "ISOCENTER_FORCE_PROCESSES",
          "ISOCENTER_MAX_TASKS_PER_CHILD")


def clear():
    for name in LEVERS:
        os.environ.pop(name, None)


def dispatch(strategy):
    if strategy.maxtasksperchild is not None:
        return "recycling multiprocessing.Pool"
    return ("ThreadPoolExecutor" if strategy.use_threads
            else "ProcessPoolExecutor(spawn)")


rows = []
for ft, fp, mt, arg in itertools.product(
        (None, "1"), (None, "1"), (None, "25"), (False, True)):
    clear()
    if ft:
        os.environ["ISOCENTER_FORCE_THREADS"] = ft
    if fp:
        os.environ["ISOCENTER_FORCE_PROCESSES"] = fp
    if mt:
        os.environ["ISOCENTER_MAX_TASKS_PER_CHILD"] = mt
    s = parallel._resolve_strategy(
        max_workers=4, chunksize=1, maxtasksperchild=None, disable_gc=False,
        force_threads=arg, show_progress=False, desc="probe", total=None)
    rows.append((ft or "-", fp or "-", mt or "-", str(arg),
                 str(s.use_threads), dispatch(s)))
clear()

hdr = ("FORCE_THREADS", "FORCE_PROCESSES", "MAX_TASKS", "force_threads=",
       "use_threads", "dispatch")
w = [max(len(h), max(len(r[i]) for r in rows)) for i, h in enumerate(hdr)]
print(" | ".join(h.ljust(w[i]) for i, h in enumerate(hdr)))
print("-+-".join("-" * x for x in w))
for r in rows:
    print(" | ".join(r[i].ljust(w[i]) for i in range(len(hdr))))
