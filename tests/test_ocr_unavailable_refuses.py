"""`scan_pixel_content()` and `discover_redaction_zones()` refuse without OCR (#422).

Both methods used to answer "nothing found" when OCR could not run at
all. Without `pytesseract`, `scan_pixel_content()` printed `OCR Scan
Complete. Found 0 suspicious regions (Uncovered).` for instances it had
never read; with `pytesseract` installed and no `tesseract` binary on
PATH, the same fixture gave 0 findings and 0 candidates behind one
`OCR failed` log line per frame. An empty result that cannot be told
apart from a clean one is the worst answer a scan for burned-in PHI can
give, so both now raise `pixel_analysis.OcrUnavailableError`, a
`RuntimeError`, before any worker is dispatched.

**Every condition here is forced by patching.** CI installs the `ocr`
extra and the binary; neither local gate interpreter has either. A test
that relied on the environment would pass for different reasons in the
two places.

**Why the fixture graph has a configured serial with zones.** The
"nothing was dispatched" assertions spy on the pool. On a graph with no
configured instances the pre-#422 code would not have dispatched either,
and the spy assertion would hold by accident. Here the pre-#422 code
dispatches exactly once per call, so a guard placed after the pool is red.

**Why this file imports what it does.** Findings are reached through
`len()` on the returned objects and the pool through `session_module`,
never through a mutation-probe target's dotted name:
`tests/test_mutation_probe_targets._importers` matches that text
anywhere in a test file, and naming one would charge this file to every
one of that module's mutants for no kill signal.
"""
import os
import pathlib
import subprocess
import sys
from datetime import date
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from isocenter import pixel_analysis
from isocenter import session as session_module
from isocenter.entities import Equipment, Instance, Patient, Series, Study
from isocenter.pixel_analysis import TextRegion
from isocenter.session import DicomSession

REPO = pathlib.Path(__file__).resolve().parent.parent

CT_SOP_CLASS = "1.2.840.10008.5.1.4.1.1.2"
SERIAL = "SN-A"
UIDS = ["1.2.826.0.422.1", "1.2.826.0.422.2"]
ZONE_TOP_LEFT = [0, 100, 0, 100]   # zone space: y1, y2, x1, x2

#: pytesseract's own text for a missing binary, measured on 0.3.13.
BINARY_MISSING = ("tesseract is not installed or it's not in your PATH. "
                  "See README file for more information.")
IMPORT_FAILED = "No module named 'pytesseract'"


class _TesseractNotFoundError(OSError):
    """pytesseract's `TesseractNotFoundError` is an `OSError` subclass."""


@pytest.fixture(autouse=True)
def _threads_not_processes(monkeypatch):
    """The present-OCR test patches in the parent; threads keep it visible.

    Same block, and same reason, as
    `tests/test_scan_pixel_content_dispatches_its_worker.py`.
    """
    monkeypatch.setenv("ISOCENTER_MAX_WORKERS", "3")
    monkeypatch.setenv("ISOCENTER_FORCE_THREADS", "1")
    monkeypatch.delenv("ISOCENTER_FORCE_PROCESSES", raising=False)
    monkeypatch.delenv("ISOCENTER_MAX_TASKS_PER_CHILD", raising=False)


def _no_pytesseract(monkeypatch):
    monkeypatch.setattr(pixel_analysis, "HAS_OCR", False)
    monkeypatch.setattr(pixel_analysis, "pytesseract", None)
    monkeypatch.setattr(pixel_analysis, "_OCR_IMPORT_ERROR", IMPORT_FAILED)


def _pytesseract_whose_probe_raises(monkeypatch, exc):
    def get_tesseract_version():
        raise exc

    monkeypatch.setattr(pixel_analysis, "HAS_OCR", True)
    monkeypatch.setattr(pixel_analysis, "pytesseract", SimpleNamespace(
        get_tesseract_version=get_tesseract_version))


def _no_binary(monkeypatch):
    _pytesseract_whose_probe_raises(
        monkeypatch, _TesseractNotFoundError(BINARY_MISSING))


#: cause name -> (patcher, the text of that cause the message must carry)
CAUSES = {
    "no-pytesseract": (_no_pytesseract, IMPORT_FAILED),
    "no-binary": (_no_binary, BINARY_MISSING),
}

#: method name -> how to call it on the fixture session
METHODS = {
    "scan_pixel_content": lambda s: s.scan_pixel_content(),
    "discover_redaction_zones": lambda s: s.discover_redaction_zones(SERIAL),
}


@pytest.fixture
def session(tmp_path):
    """One configured serial with zones, two instances on it.

    Both halves are load-bearing: the rule with zones is what makes the
    pre-#422 `scan_pixel_content()` dispatch, and instances on `SERIAL`
    are what make the pre-#422 `discover_redaction_zones()` dispatch.
    """
    s = DicomSession(persistence_file=str(tmp_path / "ocr422.db"))
    patient = Patient("PAT422", "Ocr^Patient")
    study = Study("ST_422", date(2023, 1, 1))
    study.study_time = "120000"
    series = Series("SE_422", "CT", 1)
    series.equipment = Equipment("Acme", "Model", SERIAL)
    for uid in UIDS:
        instance = Instance(uid, CT_SOP_CLASS, 1)
        instance.file_path = None
        instance.set_attr("0018,1000", SERIAL)
        series.instances.append(instance)
    study.series.append(series)
    patient.studies.append(study)
    s.store.patients.append(patient)
    s.configuration.rules = [
        {"serial_number": SERIAL, "redaction_zones": [ZONE_TOP_LEFT]}]
    try:
        yield s
    finally:
        s.close()


@pytest.mark.parametrize("cause", sorted(CAUSES))
@pytest.mark.parametrize("method", sorted(METHODS))
def test_each_method_refuses_before_its_pool_is_dispatched(
        session, monkeypatch, method, cause):
    """N1-N4: the raise, its class, and "before dispatch", per method x cause.

    The spy returns `[]`, so on the pre-#422 code both methods complete
    and return an empty result: `pytest.raises` is red there, and a guard
    moved below the pool raises *after* one call, which the count reads.
    """
    CAUSES[cause][0](monkeypatch)
    with patch.object(session_module, "run_parallel",
                      return_value=[]) as pool:
        with pytest.raises(pixel_analysis.OcrUnavailableError):
            METHODS[method](session)
    assert pool.call_count == 0, (
        f"{method}() reached its pool {pool.call_count} time(s) with OCR "
        f"unavailable ({cause}); the refusal must come first")


@pytest.mark.parametrize("cause", sorted(CAUSES))
@pytest.mark.parametrize("method", sorted(METHODS))
def test_the_refusal_names_the_method_the_cause_and_both_installs(
        session, monkeypatch, method, cause):
    """N5: what the message must carry, each fragment for its own reason.

    Two assertions that look right are decoys, measured: `"tesseract" in
    msg` holds on a message that never names the binary, because
    `"pytesseract"` contains it, and `"isocenter[ocr]" in msg` holds on
    the unquoted install that zsh refuses (`zsh: no matches found`). So
    the quoted command is asserted whole, and the binary by its install
    commands.
    """
    patcher, cause_text = CAUSES[cause]
    patcher(monkeypatch)
    with pytest.raises(pixel_analysis.OcrUnavailableError) as raised:
        METHODS[method](session)
    msg = str(raised.value)

    assert f"{method}()" in msg, msg
    assert cause_text in msg, (
        f"the reason the probe got ({cause_text!r}) did not reach the "
        f"message: {msg!r}")
    assert "Nothing was scanned" in msg, msg
    assert 'pip install "isocenter[ocr]"' in msg, msg
    assert "brew install tesseract" in msg, msg
    assert "apt-get install tesseract-ocr" in msg, msg


def test_a_scaffolded_config_is_refused_before_the_graph_is_read(
        session, monkeypatch, capsys):
    """A rule with no zones yet still needs OCR to be scanned later.

    Without the check first, `scan_pixel_content()` walks the graph, finds
    no configured instance with zones, prints "No matching configured
    instances found to scan." and returns an empty report -- never
    reaching a guard placed at the pool -- so the missing extra surfaces
    only once the zones are filled in. Empty stdout is what pins "before
    the graph is read": the banner and the skip line are both prints.
    Killing edit: the guard moved to just above `run_parallel`.
    """
    _no_pytesseract(monkeypatch)
    session.configuration.rules = [
        {"serial_number": SERIAL, "redaction_zones": []}]
    capsys.readouterr()
    with pytest.raises(pixel_analysis.OcrUnavailableError):
        session.scan_pixel_content()
    assert capsys.readouterr().out == ""


def test_discovery_for_an_unknown_serial_is_refused_before_the_graph_is_read(
        session, monkeypatch, capsys):
    """"No instances found for serial" is an answer that needs no OCR.

    A guard below that early return lets discovery answer it without OCR,
    so a typo'd serial and a missing extra read the same until the serial
    is corrected. Killing edit: the guard moved below that return.
    """
    _no_pytesseract(monkeypatch)
    capsys.readouterr()
    with pytest.raises(pixel_analysis.OcrUnavailableError):
        session.discover_redaction_zones("SN-NOBODY")
    assert capsys.readouterr().out == ""


def test_the_refusal_is_a_runtime_error():
    """N6: the frozen promise is `RuntimeError`; the subclass is tier 2."""
    assert issubclass(pixel_analysis.OcrUnavailableError, RuntimeError)


def test_a_too_old_binary_is_a_refusal_not_an_exit(session, monkeypatch):
    """N9: pytesseract signals an unparseable version with `SystemExit`.

    Letting that through would exit the caller's script from inside a
    scan, which is worse than the silence #422 removes.
    """
    _pytesseract_whose_probe_raises(
        monkeypatch, SystemExit("Invalid tesseract version"))
    with pytest.raises(pixel_analysis.OcrUnavailableError) as raised:
        session.scan_pixel_content()
    assert "Invalid tesseract version" in str(raised.value)


def test_the_probe_does_not_swallow_an_interrupt(monkeypatch):
    """The probe is broad on purpose, and no broader than that.

    `(Exception, SystemExit)` is what "can OCR run" needs. Widening it to
    `BaseException` would turn a Ctrl+C during the probe into a refusal
    and keep the script running.
    """
    _pytesseract_whose_probe_raises(monkeypatch, KeyboardInterrupt())
    with pytest.raises(KeyboardInterrupt):
        pixel_analysis._ocr_unavailable_reason()


def test_discovery_still_dispatches_when_ocr_is_present(
        session, ocr_present, monkeypatch):
    """N7: the guard against over-refusal.

    Locally `HAS_OCR` is really `False`, so a guard reading a copy of it
    taken at import (`from .pixel_analysis import HAS_OCR` in the session
    module) would refuse here while `ocr_present` says OCR can run. In CI
    the same mutant is caught the other way round, by the refusal tests
    above. The expected count is non-empty on purpose: `0 == 0` would be
    green on a discovery that never dispatched.
    """
    monkeypatch.setattr(pixel_analysis, "analyze_pixels", lambda _instance: [
        TextRegion("LEAKTEXT", (200, 200, 50, 50), 90.0)])
    result = session.discover_redaction_zones(SERIAL)
    assert len(result) == len(UIDS)


def test_importing_without_pytesseract_is_silent():
    """N8: the import-time warning is gone, in a fresh interpreter.

    Three assertions, in order. The child must import *this* checkout --
    `isocenter` is installed editable from the main checkout, and a child
    that inherits no `PYTHONPATH` would test that tree instead. It must
    actually have taken the no-pytesseract arm, or a silent stderr proves
    nothing. Only then does the stderr assertion count. Not `stderr ==
    ""`: that would fail on any unrelated warning a dependency prints.
    """
    child = (
        "import sys\n"
        "sys.modules['pytesseract'] = None\n"
        "import isocenter\n"
        "from isocenter import pixel_analysis as p\n"
        "print(isocenter.__file__)\n"
        "print(p.HAS_OCR)\n"
        "print(p._ocr_unavailable_reason())\n"
    )
    env = {**os.environ, "PYTHONPATH": str(REPO),
           "PYTHONDONTWRITEBYTECODE": "1"}
    done = subprocess.run([sys.executable, "-c", child], env=env, cwd=REPO,
                          capture_output=True, text=True, timeout=120,
                          check=True)
    lines = done.stdout.splitlines()

    assert lines[0].startswith(str(REPO)), (
        f"the child imported {lines[0]}, not this checkout")
    assert lines[1] == "False", (
        f"the child did not take the no-pytesseract arm: {done.stdout!r}")
    assert "pytesseract could not be imported" in lines[2], lines[2]
    assert "pytesseract" not in done.stderr, (
        f"importing isocenter without pytesseract still says so on "
        f"stderr: {done.stderr!r}")
