"""Does audit() on a :memory: store survive ISOCENTER_FORCE_PROCESSES=1?

The 'fires once per pass' test in the brief runs audit() before redact() to
kill a parallel.py placement of the new warning. That only works if audit()
itself succeeds on this store under this lever.
"""
import os
import sys

os.environ["ISOCENTER_FORCE_PROCESSES"] = "1"
os.environ["ISOCENTER_SHOW_PROGRESS"] = "0"
os.environ.pop("ISOCENTER_FORCE_THREADS", None)
os.environ.pop("ISOCENTER_MAX_TASKS_PER_CHILD", None)

import isocenter
from isocenter import parallel
from isocenter.session import DicomSession

sys.path.insert(0, "/Users/kevin/Developer/Isocenter/.claude/worktrees/"
                   "agent-ae2825b99ea90773c/docs/superpowers/specs/"
                   "2026-09-09-the-last-silences-bunch-b")
from probe_384_400_redact_speech import populate, install_spies, seen

if __name__ == "__main__":
    print("isocenter.__file__ =", isocenter.__file__)
    print("python =", sys.version.replace("\n", " "))
    install_spies()
    with DicomSession(":memory:") as s:
        populate(s)
        report = s.audit()
        print("audit() returned", type(report).__name__, "with", len(report), "findings")
        print("audit dispatch use_threads:",
              seen.get("strategy").use_threads if seen.get("strategy") else "n/a")
        print("redact() returned", s.redact())
