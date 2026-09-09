"""`ISOCENTER_MAX_WORKERS` is read in two files, and #335 guarded one (#341).

`_resolve_strategy` in `isocenter/parallel.py` is where the variable is
documented to be read, and #335 taught that read to reject `0` and every
negative with a warning naming the variable and the value.
`_redaction_worker_count` in `isocenter/session.py` -- called from
`redact()` and nowhere else -- reads the same variable itself and passes
the answer to `run_parallel` as an explicit `max_workers`, so the guarded
read never sees the environment on that path. It settled the override
with `max(1, override)`: `ISOCENTER_MAX_WORKERS=0` became one worker, in
silence, and so did `-1`. The `docs/environment.md` row said a value below
1 "is reported with a warning naming the variable and the value, and the
default is used instead", which was true of `run_parallel` and false of
`redact()`.

A floor at a call site is a floor at one of the places a variable is
read. The floor now lives in `_env_int` itself, and these tests hold the
redaction read to the same answer as the other three.

In its own file rather than in `tests/test_parallel_contract.py`: that
file is about `parallel.py`, and this function lives in `session.py`.
"""
import logging
import os

import pytest

from isocenter import session as session_module


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    monkeypatch.delenv("ISOCENTER_MAX_WORKERS", raising=False)


def _documented_default():
    """Half the CPUs, capped at eight, never below one -- the docstring's
    own expression. Not the `os.cpu_count()` default `run_parallel`
    uses: the redaction pool is a memory ceiling, and the two defaults
    differing is a documented fact this file does not decide."""
    return max(1, min((os.cpu_count() or 1) // 2, 8))


@pytest.mark.parametrize("value", ["0", "-1"])
def test_the_redaction_worker_count_rejects_a_value_below_one_the_way_run_parallel_does(
        value, monkeypatch, caplog):
    """`0` and a negative are reported and replaced by this path's default.

    **Red on both assertions when written, on a multi-CPU box**: the
    function returned `1` -- `max(1, override)` -- and said nothing.
    On a one- or two-CPU runner the documented default is also `1`, so
    there only the silence assertion is red; a CI log from a small
    runner showing one red assertion here is not evidence that the
    value was ever right.

    The default asserted is the redaction path's own, not
    `run_parallel`'s `os.cpu_count()`: a rejected value falls back to
    the default *of the read that rejected it*, and this path's default
    is different on purpose (each worker holds a decoded image).
    """
    monkeypatch.setenv("ISOCENTER_MAX_WORKERS", value)

    with caplog.at_level(logging.WARNING):
        count = session_module._redaction_worker_count()

    assert count == _documented_default(), (
        f"ISOCENTER_MAX_WORKERS={value} must fall back to the redaction "
        f"default {_documented_default()}, not be clamped to a single "
        f"worker in silence; got {count}")
    naming = [record.message for record in caplog.records
              if "ISOCENTER_MAX_WORKERS" in record.message]
    assert any(f"set to {value}" in message for message in naming), (
        f"ISOCENTER_MAX_WORKERS={value} was rejected on the redaction "
        "path without a warning naming the variable and quoting the "
        f"value; docs/environment.md promises one (#341): {naming}")


def test_a_usable_override_and_a_malformed_one_behave_as_they_did(
        monkeypatch, caplog):
    """Characterization: green on both sides of #341.

    `3` is honoured and `banana` is reported and replaced, exactly as
    before. Here so the floor's arrival cannot be mistaken for a change
    to either of the answers that were already right.
    """
    monkeypatch.setenv("ISOCENTER_MAX_WORKERS", "3")
    with caplog.at_level(logging.WARNING):
        assert session_module._redaction_worker_count() == 3
    assert not [record for record in caplog.records
                if "ISOCENTER_MAX_WORKERS" in record.message]

    caplog.clear()
    monkeypatch.setenv("ISOCENTER_MAX_WORKERS", "banana")
    with caplog.at_level(logging.WARNING):
        assert session_module._redaction_worker_count() == _documented_default()
    assert any("ISOCENTER_MAX_WORKERS" in record.message
               and "'banana'" in record.message
               for record in caplog.records)


@pytest.mark.parametrize("cpus, expected", [
    (32, 8), (16, 8), (14, 7), (6, 3), (2, 1), (1, 1), (None, 1)])
def test_the_default_redaction_worker_count_is_half_the_cpus_capped_at_eight(
        cpus, expected, monkeypatch):
    """The second default `docs/environment.md`'s `ISOCENTER_MAX_WORKERS` row states (#363).

    `run_parallel`'s default is one per CPU and
    `tests/test_parallel_contract.py::test_the_default_worker_count_is_one_per_cpu`
    holds it still; this is the sibling for `redact()`'s own default,
    half the CPUs capped at eight and never below one -- a memory ceiling,
    because each redaction worker holds a decoded frame. The row used to
    state one default and call `_resolve_strategy` "the only place it is
    computed", which was true of that expression and false of the
    default. Characterization: green on the code it was written against,
    and the row is written from it (the #333 convention).

    **At fixed CPU counts, on purpose.** `_documented_default()` above
    computes from the live `os.cpu_count()` and stays as the oracle for
    the *override* tests, where the question is "did the rejected value
    fall back to this path's default, whatever it is here". It is blind
    to the cap on any box under sixteen CPUs -- on this 14-CPU box the
    default is 7 and a `8 -> 16` mutant survives by hardware. Setting the
    count is what makes each mutation red on any runner: `8 -> 16`
    (32 -> 16), `// 2 -> // 1` (6 -> 6), `min -> max` (6 -> 8),
    `max(1, ...)` removed (1 -> 0, None -> 0), `or 1` removed (None ->
    `TypeError`). The function reads `os.cpu_count()` through the module,
    so patching `os.cpu_count` is patching what it reads.
    """
    monkeypatch.setattr(os, "cpu_count", lambda: cpus)

    assert session_module._redaction_worker_count() == expected
