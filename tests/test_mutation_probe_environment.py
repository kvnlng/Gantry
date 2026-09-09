"""The mutation probe's two environment guarantees, checked (#311).

`python -m scripts.mutation_probe` protects itself from two traps that a
hand-run gets none of. Both have already cost this project a wrong
answer, and a wrong answer here is the expensive kind: it says a test
kills a mutant when it does not, or the reverse, and the conclusion is
acted on.

*Stale bytecode.* The probe sets `PYTHONDONTWRITEBYTECODE=1` for the
tests it launches and checks every write against CPython's own `.pyc`
validation rule before pytest sees it, because a stale cache once made
it report mutations that were never in the code it tested (#174). Edit a
module by hand, run pytest by hand, and a `.pyc` written by the previous
run can be what actually executes. So: set that variable on every
invocation, and the variable rather than `-B`, because `run_parallel()`
spawns workers and an interpreter flag reaches a spawned child only
through `_args_from_interpreter_flags`.

*The wrong copy of the package.* Work happens in git worktrees here, and
`isocenter` is installed editable from the main checkout. Run
`python something.py` inside a worktree and `import isocenter` can
resolve to the main checkout: the measurement is real, it is just about
a tree you did not edit. `PYTHONPATH=<worktree>` wins *here* because
this install appends its finder to `sys.meta_path`, after the stdlib
path finder -- had it been prepended, neither `PYTHONPATH` nor a
`sys.path.insert(0, ...)` would help. That is not visible from the
outside, which is why the instruction is to print `isocenter.__file__`
and read it rather than to trust the recipe.

`#311` calls this "prose only; nothing to test". This file disagrees:
both facts are properties of the code and the install, so both can be
checked against them rather than against memory. The second test does
exactly that -- it runs the recipe against the live environment, not
against a hermetic fixture, because the install flavour is the thing the
recipe depends on.

No `scripts/mutation_probe.py` `TARGETS` entry, for the reason
`tests/test_documented_api_exists.py`'s docstring gives: this file
exercises no target module, so a `TARGETS`-covered home would re-run it
against every mutant of that module for zero kill signal.
"""
import inspect
import subprocess
import sys

from scripts import mutation_probe

def test_the_bytecode_advice_names_the_mechanism_the_probe_actually_uses():
    """The advice must match the probe's own protection, not resemble it.

    Read out of `run()`'s source rather than asserted as a string, the
    same way `tests/test_packaging_contract.py` pins its SQLite busy
    timeout by name: if the probe ever switches to `-B`, or to a
    pre-flight cache wipe, the runbook's instruction becomes advice for a
    tool that no longer works that way and this goes red.
    """
    source = inspect.getsource(mutation_probe.run)

    assert "PYTHONDONTWRITEBYTECODE" in source, (
        "scripts/mutation_probe.run() no longer sets "
        "PYTHONDONTWRITEBYTECODE, so this module's advice to set it on "
        "every hand-run no longer describes what the probe does (#311)")


def test_a_path_on_pythonpath_wins_over_the_installed_package(tmp_path):
    """`PYTHONPATH=<worktree>` must actually beat the installed copy.

    This runs the runbook's recipe against the environment it is advice
    for, rather than against a hermetic fixture, and that is the
    deliberate choice. A self-contained version -- two fake packages, one
    on `PYTHONPATH` -- would pin `sys.path` ordering, which nobody
    doubts, and would stay green in precisely the situation that makes
    the advice false: an editable install whose `_EditableFinder` is
    *prepended* to `sys.meta_path` rather than appended is consulted
    before the path finder, and then no `PYTHONPATH` entry can win. The
    install flavour is what the recipe depends on, so the install flavour
    is what this checks.

    The control run comes first. Without it, "the fake won" would also be
    the result if `isocenter` were not importable at all, and the test
    would pass while measuring nothing.
    """
    fake = tmp_path / "fake"
    (fake / "isocenter").mkdir(parents=True)
    (fake / "isocenter" / "__init__.py").write_text(
        "MARKER = 'fake'\n", encoding="utf-8")

    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    script = elsewhere / "where_is_isocenter.py"
    script.write_text(
        "import isocenter\nprint(isocenter.__file__)\n", encoding="utf-8")

    def resolved(pythonpath):
        env = {"PATH": "/usr/bin:/bin", "HOME": str(tmp_path)}
        if pythonpath is not None:
            env["PYTHONPATH"] = str(pythonpath)
        env["PYTHONDONTWRITEBYTECODE"] = "1"
        proc = subprocess.run(
            [sys.executable, "-u", str(script)],
            cwd=elsewhere, env=env, capture_output=True, text=True,
            timeout=120, check=False)
        return proc.returncode, proc.stdout.strip(), proc.stderr.strip()

    code, installed, err = resolved(None)
    if code != 0:
        # Nothing is installed to beat, so the recipe has nothing to say
        # here. Skipping rather than passing: a pass would be a claim.
        import pytest
        pytest.skip(
            f"isocenter is not importable without PYTHONPATH, so there "
            f"is no installed copy for the recipe to beat: {err}")

    code, chosen, err = resolved(fake)

    assert code == 0, err
    assert chosen.startswith(str(fake)), (
        "A hand mutation is run with PYTHONPATH set to the worktree, "
        "but a path on PYTHONPATH did "
        "NOT win here: the interpreter resolved `isocenter` to "
        f"{chosen!r} rather than to {fake}. That happens when the "
        "install's meta-path finder is prepended rather than appended, "
        "and it means the recipe measures the wrong tree (#311)")
    assert chosen != installed
