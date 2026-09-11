"""A mutant's timeout is earned by its module's control, and a timeout is a verdict (#442).

`run()` gave every mutant a flat 900 s, and `main()` printed a timeout as
`skipped` and left it out of the tally. Two things were wrong with that.
A non-terminating mutant of a module whose whole test list passes in one
second cost fifteen minutes. And a mutant the tests noticed by never
finishing was counted as neither killed nor survived, so a module whose
mutants all hung would read `0/0`.

The limit is now `max(MUTANT_TIMEOUT_FLOOR_S, MUTANT_TIMEOUT_FACTOR *
control_s)`, where `control_s` is the time the unmutated control pass
took. A timeout prints `TIMEOUT` and counts as detected (owner ruling,
Q3). The control runs under `CONTROL_TIMEOUT_S`, because it sets every
mutant's limit and must not be the thing that times out on a loaded
machine. If it does time out anyway, the run says so and moves on to the
next module, where the exception used to escape `main()` and end the run.

A timeout also has to take the whole pytest with it (#476).
`subprocess.run(timeout=)` kills the direct child only, and a mutant that
spins inside a `run_parallel()` worker left that worker running, at
about 77% CPU in the PR #480 review, reparented to init, into every
later mutant's measurement. `run()` now starts pytest in a session of
its own and KILLs the process group on a timeout.

This imports the probe script only, never a package module, so it needs
no `TARGETS` row.
"""
import ast
import inspect
import os
import pathlib
import signal
import subprocess
import sys
import textwrap
import time

import pytest

from scripts import mutation_probe

#: Five mutation sites, the same source `test_mutation_probe_targets.py`
#: samples: three comparisons, one bool-op and one return.
_FIVE_SITE_SRC = "def f(a, b):\n    return (a == 1) and (b == 2) and (a < b)\n"


@pytest.mark.parametrize("control_s, expected", [
    (1, 30),     # discovery-sized: the floor, not 3 s
    (10, 30),    # still the floor
    (100, 300),  # three controls
    (310, 930),  # session-sized: today's flat 900 s, to within 3%
])
def test_the_mutant_timeout_is_three_controls_with_a_floor(control_s, expected):
    """The arithmetic, and both constants, pinned by value.

    The floor is there for the fast rows. Without it a 1 s control gives
    a 3 s limit, and on a loaded machine that kills healthy mutants and
    scores them as detected.
    """
    assert mutation_probe.mutant_timeout(control_s) == expected


def _probe(tmp_path, monkeypatch, targets, fake_run, clock=None):
    """Run `main()` over `targets` (name -> source) with `run` faked."""
    rows = {}
    for i, (name, src) in enumerate(targets.items()):
        (tmp_path / name).write_text(src, encoding="utf-8")
        rows[name] = ([f"t{i}.py"], 5)
    monkeypatch.setattr(mutation_probe, "REPO", tmp_path)
    monkeypatch.setattr(mutation_probe, "TARGETS", rows)
    monkeypatch.setattr(mutation_probe, "NOT_PROBED", {})
    monkeypatch.setattr(mutation_probe, "subprocess_cache_path",
                        lambda p: pathlib.Path("/sentinel/none.pyc"))
    monkeypatch.setattr(mutation_probe, "assert_fresh", lambda p, c: None)
    monkeypatch.setattr(mutation_probe, "run", fake_run)
    if clock is not None:
        monkeypatch.setattr(mutation_probe, "_clock", clock)
    monkeypatch.setattr(sys, "argv", ["mutation_probe"])
    mutation_probe.main()


def _is_control(tmp_path, name):
    """True while `main()` has the control, not a mutant, on disk.

    The control is `ast.unparse` of the original, so the file on disk
    matches it exactly; every mutant differs from it.
    """
    on_disk = (tmp_path / name).read_text(encoding="utf-8")
    return on_disk == ast.unparse(ast.parse(_FIVE_SITE_SRC))


def test_every_mutant_runs_under_the_timeout_its_control_earned(tmp_path, monkeypatch):
    """The control's own duration, read from the clock `main()` uses, sets the limit.

    The fake clock moves only while the control runs, by 12 s, so the
    limit every mutant must get is `mutant_timeout(12)`, which is 36. That
    is **above the 30 s floor on purpose**: if `main()` timed the control
    with a clock this test did not patch, the real fake-run would take
    about 0 s, the limit would come out at the floor, and 30 != 36 would
    catch it. An assertion of 30 would pass for that wrong reason.
    """
    now = [1000.0]
    seen = []

    def fake_run(tests, timeout):
        control = _is_control(tmp_path, "mod.py")
        seen.append((control, timeout))
        if control:
            now[0] += 12.0
        return control   # the control passes; every mutant is killed

    _probe(tmp_path, monkeypatch, {"mod.py": _FIVE_SITE_SRC}, fake_run,
           clock=lambda: now[0])

    assert seen[0] == (True, mutation_probe.CONTROL_TIMEOUT_S), seen
    assert mutation_probe.mutant_timeout(12.0) == 36
    assert seen[1:] == [(False, 36)] * 5, seen


def test_a_mutant_that_times_out_is_reported_and_counted(tmp_path, monkeypatch, capsys):
    """`TIMEOUT` is its own line, and it counts as detected (Q3).

    The tests did notice that mutant: they noticed by not finishing. The
    tally says how many kills were timeouts, so a row that is mostly
    timeouts can be told apart from a row that is mostly red tests.
    Another exception is still `skipped` and still outside `n`, because
    it says nothing about the mutant.
    """
    calls = []

    def times_out_once(tests, timeout):
        calls.append(timeout)
        if len(calls) == 3:   # the second mutant; call 1 is the control
            raise subprocess.TimeoutExpired(cmd="pytest", timeout=timeout)
        return _is_control(tmp_path, "mod.py")

    _probe(tmp_path, monkeypatch, {"mod.py": _FIVE_SITE_SRC}, times_out_once)
    out = capsys.readouterr().out
    timeout_lines = [line for line in out.splitlines() if "TIMEOUT" in line]
    assert len(timeout_lines) == 1, out
    assert "limit 30s" in timeout_lines[0], out
    assert "skipped" not in out, out
    assert "=> killed 5/5 (1 by timeout), SURVIVED 0/5" in out, out

    calls.clear()

    def oserror_once(tests, timeout):
        calls.append(timeout)
        if len(calls) == 3:
            raise OSError("not a verdict about the mutant")
        return _is_control(tmp_path, "mod.py")

    _probe(tmp_path, monkeypatch, {"mod.py": _FIVE_SITE_SRC}, oserror_once)
    out = capsys.readouterr().out
    assert "skipped" in out and "OSError" in out, out
    assert "TIMEOUT" not in out, out
    assert "=> killed 4/4, SURVIVED 0/4" in out, out


def test_a_control_that_times_out_is_reported_and_the_run_goes_on(
        tmp_path, monkeypatch, capsys):
    """A control timeout makes one module's results unusable, not the whole run's.

    Before #442 the `TimeoutExpired` escaped `main()` from the control, so
    one slow module ended a run of many hours and printed nothing for
    every module after it.
    """
    probed = []

    def fake_run(tests, timeout):
        probed.append(tests[0])
        if tests[0] == "t0.py":
            raise subprocess.TimeoutExpired(cmd="pytest", timeout=timeout)
        return _is_control(tmp_path, "second.py")   # control passes, mutants die

    _probe(tmp_path, monkeypatch,
           {"first.py": _FIVE_SITE_SRC, "second.py": _FIVE_SITE_SRC}, fake_run)
    out = capsys.readouterr().out

    assert probed.count("t0.py") == 1, probed   # the control, and no mutant
    assert probed.count("t1.py") == 6, probed   # control + five mutants
    assert (f"control (unparsed, unmutated): TIMEOUT after "
            f"{mutation_probe.CONTROL_TIMEOUT_S}s -- results unusable") in out, out
    assert "=> killed 5/5, SURVIVED 0/5" in out, out
    assert (tmp_path / "first.py").read_text(encoding="utf-8") == _FIVE_SITE_SRC


def test_run_takes_its_timeout_from_every_caller():
    """`run()`'s `timeout` has no default, so no caller can forget it.

    A default is the flat 900 s coming back through the one call site
    that omits the argument (#442). Every other test here passes it, so
    a default would pass all of them.
    """
    param = inspect.signature(mutation_probe.run).parameters["timeout"]
    assert param.default is inspect.Parameter.empty, param


def _gone(pid, within=5.0):
    """True once `pid` no longer exists, or is a zombie awaiting its reaper."""
    deadline = time.monotonic() + within
    while time.monotonic() < deadline:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return True
        state = subprocess.run(["ps", "-o", "stat=", "-p", str(pid)],
                               capture_output=True, text=True).stdout.strip()
        if state.startswith("Z"):
            return True
        time.sleep(0.05)
    return False


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX process groups")
def test_a_timed_out_run_takes_its_whole_process_group_with_it(tmp_path, monkeypatch):
    """A mutant's timeout kills pytest's workers too, not pytest alone (#476).

    The stand-in for pytest starts a child, as `run_parallel()` starts its
    pool workers, and both then sleep past the timeout. A plain child is
    enough: a spawn pool worker is in pytest's process group for the same
    reason this one is, by inheritance, and costs a second to start. The
    PR #480 review measured the real shape: after
    `subprocess.run(timeout=)`, busy spawn workers still ran at about 77%
    CPU, reparented to init.

    The stand-in sleeps 30 s and its worker 120 s, and `run()` must raise
    within seconds of its 2 s limit. Without that bound, a `run()` that
    kills nothing passes: `Popen`'s `with` waits for the child, and if
    both slept equally long they would exit together, before `_gone`
    looked.
    """
    pidfile = tmp_path / "grandchild.pid"
    child = tmp_path / "fake_pytest.py"
    child.write_text(textwrap.dedent(f"""\
        import subprocess, sys, time
        worker = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(120)"])
        with open({str(pidfile)!r}, "w") as f:
            f.write(str(worker.pid))
        time.sleep(30)
        """), encoding="utf-8")
    monkeypatch.setattr(mutation_probe, "PYTEST", [sys.executable, str(child)])
    monkeypatch.setattr(mutation_probe, "REPO", tmp_path)
    grandchild = None
    try:
        started = time.monotonic()
        with pytest.raises(subprocess.TimeoutExpired):
            mutation_probe.run([], 2)
        assert time.monotonic() - started < 2 + 8, (
            "run() waited for the timed-out pytest to exit by itself: "
            "nothing was killed")
        assert pidfile.exists(), "the stand-in never started its worker"
        grandchild = int(pidfile.read_text())
        assert _gone(grandchild), (
            "the timed-out pytest's worker outlived it: the timeout killed "
            "the direct child only, not its process group")
    finally:
        if grandchild is None and pidfile.exists():
            grandchild = int(pidfile.read_text())
        if grandchild is not None:
            try:
                os.kill(grandchild, signal.SIGKILL)
            except ProcessLookupError:
                pass
