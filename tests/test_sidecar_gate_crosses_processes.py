"""The sidecar gate reaches a writer in another process (#368).

Half of the gate is a `threading.Lock`, which is what gives two threads
in one process a fair queue. The other half is `fcntl.flock(LOCK_EX)`
on `<sidecar>.lock`, and that half is the one that matters for the
population #368 was opened for: `execute_redaction_task` runs in a
*spawned* child on GIL builds, and `SqliteStore.__setstate__` hands that
child a new `threading.Lock` that shares nothing with the parent's. A
gate that were only the thread lock would let a redaction worker append
into a sidecar the parent's `compact()` is replacing, exactly as before.

So the holder here is a real second process -- a `subprocess` that
takes `LOCK_EX` on the gate's path and blocks on stdin, which needs no
pickling and is released by the kernel if it dies -- and the parent's
`persist_pixel_data` must raise the gate error while it holds, then
succeed once it lets go.

The second test is the pickle twin of
`tests/test_save_redact_race.py::test_the_store_still_pickles_with_its_pixel_swap_lock`:
the gate's thread lock must be dropped by `__getstate__` (a lock raises
`TypeError: cannot pickle '_thread.lock' object`), the clone's gate must
be a fresh, usable object, and the clone must open its *own* fd on the
same path -- which is what `_hold_sidecar_gate` does on every
acquisition -- rather than inherit one.
"""
import pickle
import subprocess
import sys
import threading

import numpy as np
import pytest

from isocenter import persistence as persistence_module
from isocenter.entities import Instance
from isocenter.persistence import SqliteStore

CT_STORAGE = "1.2.840.10008.5.1.4.1.1.2"

#: The holder. Argument 1 is the gate's lock path. Prints "held" once
#: the flock is taken, releases on the first line of stdin, prints
#: "released" and exits.
_HOLDER = r"""
import fcntl, os, sys
fd = os.open(sys.argv[1], os.O_RDWR | os.O_CREAT, 0o644)
fcntl.flock(fd, fcntl.LOCK_EX)
print("held", flush=True)
sys.stdin.readline()
fcntl.flock(fd, fcntl.LOCK_UN)
os.close(fd)
print("released", flush=True)
"""


def _instance(uid="1.2.3.X"):
    inst = Instance(uid, CT_STORAGE, 1, file_path=None)
    inst.set_attr("0028,0010", 4)
    inst.set_attr("0028,0011", 4)
    inst.set_attr("0028,0002", 1)
    inst.set_attr("0028,0100", 8)
    inst.set_attr("0028,0103", 0)
    inst.set_pixel_data(np.arange(16, dtype=np.uint8).reshape(4, 4))
    return inst


def test_a_gate_held_by_another_process_blocks_this_one(tmp_path, monkeypatch):
    store = SqliteStore(str(tmp_path / "cross.db"))
    holder = None
    try:
        holder = subprocess.Popen(
            [sys.executable, "-c", _HOLDER, store._gate_path()],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
        assert holder.stdout.readline().strip() == "held", (
            "the holder process never took the gate")

        monkeypatch.setattr(persistence_module, "_SIDECAR_GATE_TIMEOUT_S", 0.5)
        inst = _instance()

        # On a helper thread with a bounded join, deliberately. The
        # in-process half of the gate is a `threading.Lock` whose
        # `acquire(timeout=)` bounds a wait *between threads*; against a
        # holder in another process that lock is free and the flock is
        # the only wait there is. So this is the one place a blocking
        # `flock` (instead of the polled `LOCK_NB` loop) is observable,
        # and it would show as a hang -- which the join turns into a
        # failure rather than a stall.
        outcome = {}

        def attempt():
            try:
                store.persist_pixel_data(inst)
            except Exception as exc:      # pylint: disable=broad-except
                outcome["raised"] = exc
            else:
                outcome["raised"] = None

        attempt_thread = threading.Thread(target=attempt, daemon=True)
        attempt_thread.start()
        attempt_thread.join(timeout=10.0)
        blocked = attempt_thread.is_alive()

        # Release the holder whatever happened above, so a blocked
        # attempt can finish and the process is reaped.
        holder.stdin.write("\n")
        holder.stdin.flush()
        assert holder.stdout.readline().strip() == "released"
        holder.wait(timeout=10)
        attempt_thread.join(timeout=10.0)

        assert not blocked, (
            "the parent's write blocked for over 10 s against a 0.5 s "
            "deadline: the gate's flock is blocking instead of the polled "
            "LOCK_NB loop, so a stuck holder in another process is a "
            "silent stall rather than an error (#368)")
        raised = outcome["raised"]
        assert isinstance(raised, RuntimeError), (
            "the parent's write went through while another process held "
            "the gate: the flock half of the gate is missing, and a "
            "spawned redaction worker would append into a sidecar the "
            "parent is compacting (#368); got %r" % (raised,))
        assert store._gate_path() in str(raised), (
            "the gate error does not name the lock file: %s" % raised)
        assert "_SIDECAR_GATE_TIMEOUT_S" in str(raised)
        assert inst._pixel_loader is None

        store.persist_pixel_data(inst)
        assert inst._pixel_loader is not None, (
            "the write did not go through once the gate was released")
    finally:
        if holder is not None and holder.poll() is None:
            holder.kill()
            holder.wait(timeout=10)
        store.stop()


def test_the_store_pickles_without_its_gate_and_the_clone_gets_a_fresh_one(
        tmp_path):
    store = SqliteStore(str(tmp_path / "pickle_gate.db"))
    try:
        assert store._sidecar_gate is not None

        clone = pickle.loads(pickle.dumps(store))
        try:
            assert clone._sidecar_gate is not None
            assert clone._sidecar_gate is not store._sidecar_gate
            assert clone._owns_temp_sidecar is False
            # Usable end to end on the same path, not merely present: the
            # clone opens its own fd inside the hold.
            with clone._hold_sidecar_gate():
                pass
        finally:
            clone.stop()
    finally:
        store.stop()
