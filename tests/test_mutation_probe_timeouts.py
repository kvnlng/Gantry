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

This imports the probe script only, never a package module, so it needs
no `TARGETS` row.
"""
import ast
import pathlib
import subprocess
import sys

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
