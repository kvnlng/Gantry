"""An OCR pass frees every frame it loaded, and only those (#428).

OCR reads each frame through `Instance.get_pixel_data()`, which caches it
on `pixel_array`, and until #428 nothing released it. Measured on 6aabc7d
with four loader-backed 256x256 instances: after one
`scan_pixel_content()` under threads all four held their frame, where one
had been resident before, and `discover_redaction_zones()` -- which runs
in threads on every strategy -- did the same. Same figures on 3.12.14 and
3.14.7t. That breaks "heavy arrays are never kept resident by default"
once per scanned instance, on the free-threaded build's default path.

The release is `unload_pixel_data()`, not `discard_pixel_data()` (#293):
"free it if it is safe", not "throw it away". **What protects the
caller's array is the gate, not the method.** A frame the scan itself
loaded came through the loader or the file, so it is never an unwritten
replacement and the #293 refusal cannot fire on it; `discard` with the
gate intact is therefore an equivalent mutant and is not tested here.
The gate is what R2 pins: a frame resident *before* the scan -- an unsaved
replacement, or a written frame the caller loaded -- is left exactly as
it was.

**No processes arm for the scan.** `ocr_present` does not reach a spawned
child, so locally every child would fail every instance for want of
pytesseract, and since #423 that raises `PixelScanError` -- red for the
wrong reason. Under processes the scan never touched the live graph
anyway (measured before #428: resident count unchanged), so there is no
red-first case to pin. Discovery asks for threads on every strategy, so
R3 covers it whatever the environment says.

This file imports `isocenter.session` and `isocenter.entities`, so it sits
in both of those `TARGETS` rows in `scripts/mutation_probe.py`
(`tests/test_mutation_probe_targets._importers` matches the text of a
dotted module name anywhere in a test file).
"""
from datetime import date

import numpy as np
import pytest

from isocenter import pixel_analysis
from isocenter.entities import Equipment, Instance, Patient, Series, Study
from isocenter.session import DicomSession

SC_SOP_CLASS = "1.2.840.10008.5.1.4.1.1.7"
SERIAL = "SN-428"
UIDS = [f"1.2.826.0.1.428.1.{n}" for n in range(4)]
#: Zone space `(y1, y2, x1, x2)`: a zone must exist or the scan skips the
#: series as scaffolded. It covers nothing the stub OCR reports.
ZONE = [0, 20, 0, 20]


@pytest.fixture(autouse=True)
def _threads(monkeypatch):
    """Threads, so `ocr_present` reaches the pool (see the module docstring)."""
    monkeypatch.setenv("ISOCENTER_MAX_WORKERS", "3")
    monkeypatch.setenv("ISOCENTER_FORCE_THREADS", "1")
    monkeypatch.delenv("ISOCENTER_FORCE_PROCESSES", raising=False)
    monkeypatch.delenv("ISOCENTER_MAX_TASKS_PER_CHILD", raising=False)


@pytest.fixture
def reopened(tmp_path):
    """A session whose four instances are all loader-backed, nothing resident.

    Saved and reopened, so every frame is in the sidecar and the instances
    carry a `SidecarPixelLoader` -- the shape a real session holds. Frame
    `n` is filled with `n + 1`, so a fake OCR can tell the frames apart.
    """
    path = str(tmp_path / "reopened.db")
    session = DicomSession(path)
    patient = Patient("P428", "Resident^Frames")
    study = Study("1.2.826.0.1.428", date(2023, 1, 1))
    series = Series("1.2.826.0.1.428.1", "OT", 1)
    series.equipment = Equipment("Acme", "Model", SERIAL)
    for n, uid in enumerate(UIDS):
        instance = Instance(uid, SC_SOP_CLASS, n + 1)
        instance.set_attr("0018,1000", SERIAL)
        instance.set_pixel_data(np.full((64, 64), n + 1, dtype=np.uint8))
        series.instances.append(instance)
    study.series.append(series)
    patient.studies.append(study)
    session.store.patients.append(patient)
    session.save(sync=True)
    session.close()

    session = DicomSession(path)
    session.configuration.rules = [{"serial_number": SERIAL, "redaction_zones": [ZONE]}]
    instances = {i.sop_instance_uid: i for p in session.store.patients
                 for st in p.studies for se in st.series for i in se.instances}
    assert sorted(instances) == UIDS
    for instance in instances.values():
        assert instance._pixel_loader is not None  # pylint: disable=protected-access
        assert instance.pixel_array is None, "the fixture left a frame resident"
    try:
        yield session, instances
    finally:
        session.close()


def _resident(instances):
    return sorted(uid for uid, i in instances.items() if i.pixel_array is not None)


def test_scan_pixel_content_frees_every_frame_it_loaded(reopened, ocr_present):
    """R1: nothing resident before the scan, nothing resident after it.

    Red before #428 (measured: all four resident, 262144 bytes at 256x256).
    """
    session, instances = reopened
    report = session.scan_pixel_content()
    assert report.failures == []
    assert _resident(instances) == []


@pytest.mark.parametrize("how", ["replaced_not_written", "loaded_by_the_caller"])
def test_a_frame_resident_before_the_scan_is_still_resident_after(
        reopened, ocr_present, how):
    """R2: the gate. A frame the caller had is the caller's, whatever it is.

    (a) An unsaved `set_pixel_data()` replacement: `unload_pixel_data()`
    refuses it anyway (#293), so this half is green on an ungated unload
    and red only on an ungated `discard`. (b) A written frame the caller
    loaded with `get_pixel_data()`: freeable, so an ungated unload frees
    it and only the gate keeps it. (b) is the half that sees the gate.
    """
    session, instances = reopened
    target = instances[UIDS[0]]
    if how == "replaced_not_written":
        target.set_pixel_data(np.full((64, 64), 99, dtype=np.uint8))
        held = target.pixel_array
        assert target._pixel_array_unwritten  # pylint: disable=protected-access
    else:
        held = target.get_pixel_data()
        assert not target._pixel_array_unwritten  # pylint: disable=protected-access

    session.scan_pixel_content()

    assert target.pixel_array is held, f"{how}: the caller's frame was released"
    if how == "replaced_not_written":
        assert target._pixel_array_unwritten  # pylint: disable=protected-access
    assert _resident(instances) == [UIDS[0]]


def test_discover_redaction_zones_frees_every_frame_it_loaded(reopened, ocr_present):
    """R3: discovery too, which runs in threads on every strategy.

    Red before #428 under either strategy (measured: all four resident),
    because discovery passes `force_threads=True`. The release lives in
    the shared per-instance helper, so a release placed in the scan's
    worker instead would leave this red.
    """
    session, instances = reopened
    session.discover_redaction_zones(SERIAL)
    assert _resident(instances) == []


@pytest.mark.parametrize("where", ["ocr_raises_on_a_frame", "preparation_raises_after_the_load"])
def test_a_frame_whose_ocr_fails_is_still_freed(reopened, ocr_present, monkeypatch, where):
    """R4: a failure after the load does not strand the frame.

    Instance 0 fails, the other three are read, so the scan returns rather
    than raising. (a) is a frame whose OCR raises, which the per-frame
    catch absorbs, so the pass still reaches the end of the helper. (b)
    fails between the load and the OCR (windowing or the geometry split
    raising) and leaves the helper early: it is the case that tells a
    release in a `finally` from one written after the frame loop.
    """
    session, instances = reopened
    victim_fill = 1   # UIDS[0]'s frame
    if where == "ocr_raises_on_a_frame":
        def image_to_data(img, *_args, **_kwargs):
            if np.asarray(img).flat[0] == victim_fill:
                raise RuntimeError("Tesseract process timeout")
            return {"text": [], "conf": [], "left": [], "top": [],
                    "width": [], "height": []}
        ocr_present.image_to_data = image_to_data
    else:
        real = pixel_analysis._frames_for_ocr  # pylint: disable=protected-access

        def frames_for_ocr(instance, pixel_array):
            if instance.sop_instance_uid == UIDS[0]:
                raise ValueError("geometry could not be resolved")
            return real(instance, pixel_array)
        monkeypatch.setattr(pixel_analysis, "_frames_for_ocr", frames_for_ocr)

    report = session.scan_pixel_content()

    assert [uid for uid, _ in report.failures] == [UIDS[0]]
    assert _resident(instances) == []


def test_analyze_pixels_called_directly_frees_what_it_loaded(reopened, ocr_present):
    """R5: the tier-2 call behaves the same, frees its own load, keeps the caller's."""
    _session, instances = reopened
    loaded_here = instances[UIDS[0]]
    held_by_caller = instances[UIDS[1]]
    held = held_by_caller.get_pixel_data()

    assert pixel_analysis.analyze_pixels(loaded_here) == []
    assert pixel_analysis.analyze_pixels(held_by_caller) == []

    assert loaded_here.pixel_array is None
    assert held_by_caller.pixel_array is held
