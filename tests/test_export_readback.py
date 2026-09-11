"""`export(verify_readback=True)` re-reads what it wrote (#209).

"The write did not raise" is a weaker claim than "a file exists that
decodes to what we meant", and the compliance report presents the
stronger one. Opting in makes the worker `dcmread` each file straight
after writing it and compare the descriptors that tie the pixels to
their meaning -- Rows, Columns, SamplesPerPixel, NumberOfFrames,
BitsAllocated -- against the dataset it just serialized. An unreadable
file or a mismatch is an export failure: it travels back through
`ExportOutcome(ok=False)`, files an `ERROR` audit row and takes the
grade to `REVIEW_REQUIRED`, exactly as a write that raised does (#181).

The check runs against the temporary file, *before* the rename that
publishes it (#199) -- so a file that fails verification never appears
under its real name, and "Instances Written" keeps counting only files
a recipient can trust.

Worker-level arms call `_export_instance_worker` in this process and
booby-trap `pydicom.dcmread`, because a real readback failure means the
serializer lied about what it wrote -- precisely the defect the suite
cannot produce on demand. The session-level arms force threads
(`ISOCENTER_FORCE_THREADS`) so the same trap is visible inside the
workers; `_run_export_batch` pins `maxtasksperchild=25`, which rules
threads out, so those arms route the real batch through
`DicomExporter.export_batch` without it.
"""
import itertools
import logging
import sqlite3
from datetime import date

import numpy as np
import pydicom
from pydicom.errors import InvalidDicomError
from pydicom.pixels import get_decoder

from isocenter import io_handlers
from isocenter.entities import Instance, Patient, Series, Study
import pytest

from isocenter.io_handlers import (DicomExporter, ExportContext, ExportError,
                                   _export_instance_worker)
from isocenter.session import DicomSession

#: Taken at import, before any test traps `pydicom.dcmread`, so a test's
#: own look at the written file is never answered by its trap.
_REAL_DCMREAD = pydicom.dcmread

CT_STORAGE = "1.2.840.10008.5.1.4.1.1.2"
SC_STORAGE = "1.2.840.10008.5.1.4.1.1.7"
PM_STORAGE = "1.2.840.10008.5.1.4.1.1.30"
SR_STORAGE = "1.2.840.10008.5.1.4.1.1.88.11"
GSPS_STORAGE = "1.2.840.10008.5.1.4.1.1.11.1"
SIGNED = (("0028,0103", 1),)
BITS_STORED_12 = (("0028,0101", 12), ("0028,0102", 11))

CT_REQUIRED = (
    ("0008,0020", "20230101"), ("0008,0030", "120000"),
    ("0008,0060", "CT"),
    ("0018,0050", "1.0"), ("0018,0060", "120"),
    ("0020,0032", ["0", "0", "0"]),
    ("0020,0037", ["1", "0", "0", "0", "1", "0"]),
    ("0028,0030", ["0.5", "0.5"]),
)


def _instance(n=0, arr=None):
    inst = Instance(f"1.2.826.0.1.{n}", CT_STORAGE, n + 1)
    inst.file_path = None
    for tag, value in CT_REQUIRED:
        inst.set_attr(tag, value)
    inst.set_pixel_data(np.zeros((8, 8), dtype=np.uint16)
                        if arr is None else arr)
    return inst


def _ct_like(seed, shape=(64, 64)):
    """Signed CT-like samples. Not a constant: a lossy codestream can
    carry a constant frame exactly, and then the lossy arms prove nothing."""
    rng = np.random.default_rng(seed)
    return (rng.normal(0, 40, shape) + 40).astype(np.int16)


_serial = itertools.count(1000)


def _image(arr, attrs=(), sop=CT_STORAGE, modality="CT"):
    """A hand-built instance with the CT tags, `attrs`, and `arr` (or none)."""
    inst = Instance(f"1.2.826.0.1.449.{next(_serial)}", sop, 1)
    inst.file_path = None
    for tag, value in CT_REQUIRED:
        inst.set_attr(tag, value)
    inst.set_attr("0008,0060", modality)
    for tag, value in attrs:
        inst.set_attr(tag, value)
    if arr is not None:
        inst.set_pixel_data(arr)
    return inst


def _ctx_for(tmp_path, inst, **kwargs):
    return ExportContext(
        instance=inst,
        output_path=str(tmp_path / "out" / f"{inst.sop_instance_uid}.dcm"),
        patient_attributes={"0010,0010": "ANON", "0010,0020": "PAT1"},
        study_attributes={"0020,000d": "1.2.826.0.2.1"},
        series_attributes={"0020,000e": "1.2.826.0.3.1"},
        **kwargs)


def _stored_samples(path):
    """The written file's samples as stored -- no colour conversion."""
    written = _REAL_DCMREAD(path)
    return get_decoder(written.file_meta.TransferSyntaxUID).as_array(
        written, as_rgb=False)[0]


def _flip_on_read(monkeypatch, keyword, byte_index):
    """Make the readback's `dcmread` see one bit flipped in `keyword`.

    The file on disk is untouched; only what the check reads differs.
    That is the serializer lying about what it wrote, which is the defect
    the suite cannot produce on demand (see the module docstring).
    """
    def _flipped(*args, **kwargs):
        ds = _REAL_DCMREAD(*args, **kwargs)
        holder = (ds.WaveformSequence[0] if keyword == "WaveformData"
                  else ds)
        value = bytearray(getattr(holder, keyword))
        value[byte_index] ^= 1
        setattr(holder, keyword, bytes(value))
        return ds
    monkeypatch.setattr(pydicom, "dcmread", _flipped)


def _lossy_j2k(monkeypatch, only=None):
    """Swap the worker's JPEG 2000 encoder for a lossy one (level=30).

    `only`, when given, is the one array to encode lossily; every other
    frame is encoded as before. The encoder sees frames, not instances,
    so the instance is picked by its pixels.
    """
    real = io_handlers.jpeg2k_encode

    def _encode(frame, level=0, codecformat="J2K"):
        if only is None or (frame.shape == only.shape
                            and np.array_equal(frame, only)):
            level = 30
        return real(frame, level=level, codecformat=codecformat)
    monkeypatch.setattr(io_handlers, "jpeg2k_encode", _encode)


def _ctx(tmp_path, **kwargs):
    return ExportContext(
        instance=_instance(),
        output_path=str(tmp_path / "out" / "1.2.826.0.1.0.dcm"),
        patient_attributes={"0010,0010": "ANON", "0010,0020": "PAT1"},
        study_attributes={"0020,000d": "1.2.826.0.2.1"},
        series_attributes={"0020,000e": "1.2.826.0.3.1"},
        **kwargs)


def _session(tmp_path, arrays=None):
    session = DicomSession(str(tmp_path / "readback.db"))
    patient = Patient("PAT1", "Original Name")
    study = Study("ST_1", date(2023, 1, 1))
    study.study_time = "120000"
    series = Series("SE_1", "CT", 1)
    for n in range(3):
        series.instances.append(
            _instance(n, None if arrays is None else arrays[n]))
    study.series.append(series)
    patient.studies.append(study)
    session.store.patients.append(patient)
    session.save()
    return session


def _files(out):
    return sorted(p.name for p in out.rglob("*") if p.is_file())


# ---------------------------------------------------------------------------
# Worker-level: what the readback accepts, rejects, and never runs.
# ---------------------------------------------------------------------------

def test_readback_is_off_by_default_and_reads_nothing_back(
        tmp_path, monkeypatch):
    """Off means off: no second parse and no decode, whatever state pydicom is in.

    The decode is trapped as well as the parse since #449, because the
    decode is where the cost is: an option that stayed off but decoded
    anyway would double the default J2K export's time in silence.
    """
    def _boom(*_a, **_k):
        raise AssertionError("readback ran without being asked for")
    monkeypatch.setattr(pydicom, "dcmread", _boom)
    monkeypatch.setattr(io_handlers, "_decode_pixels", _boom)

    outcome = _export_instance_worker(_ctx(tmp_path))

    assert outcome.ok, outcome.error
    assert (tmp_path / "out" / "1.2.826.0.1.0.dcm").exists()


def test_readback_passes_a_faithful_file(tmp_path):
    outcome = _export_instance_worker(_ctx(tmp_path, verify_readback=True))

    assert outcome.ok, outcome.error
    assert _files(tmp_path / "out") == ["1.2.826.0.1.0.dcm"]


def test_an_unreadable_file_is_an_export_failure(tmp_path, monkeypatch):
    def _unreadable(*_a, **_k):
        raise InvalidDicomError("no DICM marker")
    monkeypatch.setattr(pydicom, "dcmread", _unreadable)

    outcome = _export_instance_worker(_ctx(tmp_path, verify_readback=True))

    assert not outcome.ok
    assert "read back" in str(outcome.error), outcome.error
    # A file that failed verification is not delivered, and the temp it
    # was verified under is cleaned up by the worker.
    assert _files(tmp_path / "out") == [], _files(tmp_path / "out")


def test_a_descriptor_mismatch_is_an_export_failure_naming_the_descriptor(
        tmp_path, monkeypatch):
    real = pydicom.dcmread

    def _lying(*args, **kwargs):
        ds = real(*args, **kwargs)
        ds.Rows = ds.Rows + 1
        return ds
    monkeypatch.setattr(pydicom, "dcmread", _lying)

    outcome = _export_instance_worker(_ctx(tmp_path, verify_readback=True))

    assert not outcome.ok
    assert "Rows" in str(outcome.error), outcome.error
    assert _files(tmp_path / "out") == [], _files(tmp_path / "out")


# ---------------------------------------------------------------------------
# Worker-level, #449: the readback decodes what it wrote and compares every
# sample, bit for bit, with the array the pixel element was written from.
# ---------------------------------------------------------------------------

def test_a_file_whose_pixel_bytes_differ_fails_readback(tmp_path, monkeypatch):
    """One sample's low bit, and the file is not what was meant (#449).

    Before #449 this passed: the descriptors all agree, and the check
    never looked at the pixels. Killing mutations: the pixel compare
    deleted; a tolerance (`allclose`, `atol>=1`) -- the flip is 1; the
    readback compared with itself.
    """
    src = np.random.default_rng(1).integers(0, 65536, (16, 16),
                                            dtype=np.uint16)
    # Byte 10 is the low byte of sample 5, little-endian.
    _flip_on_read(monkeypatch, "PixelData", 10)

    outcome = _export_instance_worker(
        _ctx_for(tmp_path, _image(src), verify_readback=True))

    assert not outcome.ok
    message = str(outcome.error)
    assert "Readback verification failed" in message, message
    assert "1 of 256 differ" in message, message
    assert "first at flat index 5" in message, message
    assert _files(tmp_path / "out") == [], _files(tmp_path / "out")


def test_a_lossy_codestream_fails_readback(tmp_path, monkeypatch):
    """The story: an encoder that is not lossless, and nothing noticed (#449).

    Measured on a00ef0f with this encoder: 4056 of 4096 samples wrong,
    and `verify_readback=True` passed the file. The first export is the
    precondition -- the encoder really is lossy on this array -- and it
    is also the old behaviour, since without the option nothing reads
    the file back. Killing mutations: the pixel compare deleted; the
    compare skipped for a compressed file.
    """
    _lossy_j2k(monkeypatch)
    src = _ct_like(2)
    unverified = _export_instance_worker(
        _ctx_for(tmp_path / "unverified", _image(src.copy(), SIGNED),
                 compression="j2k"))
    assert unverified.ok, unverified.error
    assert not np.array_equal(_stored_samples(unverified.output_path), src), (
        "precondition: the lossy encoder must change this array")

    outcome = _export_instance_worker(
        _ctx_for(tmp_path, _image(src.copy(), SIGNED), compression="j2k",
                 verify_readback=True))

    assert not outcome.ok
    assert "differ" in str(outcome.error), outcome.error
    assert _files(tmp_path / "out") == [], _files(tmp_path / "out")


def test_a_16_bit_rgb_j2k_export_passes_readback(tmp_path):
    """The `(2, True)` cell #416 opened decodes only through ingest's door.

    pydicom with only Pillow cannot decode 16-bit colour JPEG 2000, and
    the imagecodecs fallback in `_decode_pixels` can -- which is how
    ingest reads the same file. A readback on `pixel_array` would fail
    every healthy 16-bit colour export. The precondition keeps this from
    passing by accident in an install where some plugin can decode it.
    Killing mutation: decoding with `pixel_array` or pydicom's decoder
    alone.
    """
    src = np.random.default_rng(3).integers(0, 65536, (32, 32, 3),
                                            dtype=np.uint16)
    inst = _image(src, (("0028,0002", 3), ("0028,0004", "RGB")))

    outcome = _export_instance_worker(
        _ctx_for(tmp_path, inst, compression="j2k", verify_readback=True))

    assert outcome.ok, outcome.error
    with pytest.raises(RuntimeError):
        _ = _REAL_DCMREAD(outcome.output_path).pixel_array
    assert _files(tmp_path / "out") == [f"{inst.sop_instance_uid}.dcm"]


@pytest.mark.parametrize("compression", [None, "j2k"])
def test_readback_compares_stored_samples_not_a_colour_conversion(
        tmp_path, compression):
    """8-bit `YBR_FULL` passes, and is still labelled `YBR_FULL` (#449).

    pydicom's default `as_rgb=True` converts every 8-bit YBR family to
    RGB, so a readback through the default door compares RGB with the
    YBR samples that were written and fails every correct YBR file
    (measured: bytes-exact False under both syntaxes). Killing mutation:
    `as_rgb=False` not passed.
    """
    src = np.random.default_rng(4).integers(0, 256, (16, 16, 3),
                                            dtype=np.uint8)
    inst = _image(src, (("0028,0002", 3), ("0028,0004", "YBR_FULL")),
                  sop=SC_STORAGE, modality="OT")

    outcome = _export_instance_worker(
        _ctx_for(tmp_path, inst, compression=compression,
                 verify_readback=True))

    assert outcome.ok, outcome.error
    written = _REAL_DCMREAD(outcome.output_path)
    assert written.PhotometricInterpretation == "YBR_FULL"


@pytest.mark.parametrize("compression", [None, "j2k"])
def test_a_redacted_export_passes_readback_and_holds_the_redacted_pixels(
        tmp_path, compression):
    """The comparison is with the pixels after the zones, not before (#449).

    The worker redacts in place when it can, so the source array is made
    non-writeable: the worker then copies before redacting, and a
    capture taken before the zone block keeps the unredacted source --
    which is the mutant this kills. Without the flag the in-place
    redaction would change the captured reference too, and that mutant
    would pass. `expected` is a copy taken before the worker runs.
    """
    src = np.random.default_rng(5).integers(1, 65536, (64, 64),
                                            dtype=np.uint16)
    inst = _image(src)
    inst.pixel_array.flags.writeable = False
    assert inst.pixel_array.flags.writeable is False
    expected = src.copy()
    expected[0:8, 0:8] = 0

    outcome = _export_instance_worker(
        _ctx_for(tmp_path, inst, compression=compression,
                 verify_readback=True, redaction_zones=[(0, 8, 0, 8)]))

    assert outcome.ok, outcome.error
    np.testing.assert_array_equal(_stored_samples(outcome.output_path),
                                  expected)


@pytest.mark.parametrize("dtype, keyword",
                         [(np.float32, "FloatPixelData"),
                          (np.float64, "DoubleFloatPixelData")])
@pytest.mark.parametrize("damaged", [False, True],
                         ids=["healthy", "one-bit-flipped"])
def test_float_pixel_data_is_verified(tmp_path, monkeypatch, dtype, keyword,
                                      damaged):
    """Float pixel elements are compared too, bitwise (#449).

    The healthy array holds a NaN and a -0.0: NaN is not equal to itself
    and -0.0 equals 0.0, so a value compare fails a correct file and
    passes a wrong one, where the bit patterns are exact (measured).
    Killing mutations: the float capture taken after the branch's
    `arr = None` (floats unchecked, the flipped file passes); a value
    compare (`==`, `array_equal` on floats -- the healthy file fails);
    the differing samples counted on the float dtype (the NaN counts, and
    the message reads 2 of 1024).
    """
    src = np.random.default_rng(6).standard_normal((32, 32)).astype(dtype)
    src[0, 0] = np.nan
    src[0, 1] = -0.0
    if damaged:
        # Low byte of sample 10: a tiny change no descriptor sees.
        _flip_on_read(monkeypatch, keyword, 10 * src.itemsize)

    outcome = _export_instance_worker(
        _ctx_for(tmp_path, _image(src, sop=PM_STORAGE, modality="OT"),
                 verify_readback=True))

    if damaged:
        assert not outcome.ok
        assert "1 of 1024 differ" in str(outcome.error), outcome.error
        assert "first at flat index 10" in str(outcome.error), outcome.error
    else:
        assert outcome.ok, outcome.error


@pytest.mark.parametrize("case", ["sr-no-pixels", "float16-no-element"])
def test_instances_with_no_pixel_element_are_not_decoded(tmp_path,
                                                         monkeypatch, case):
    """No pixel element written, no decode attempted (#449).

    An SR has no pixels; a float16 array has no DICOM element that can
    carry it, so the float arm writes none and files a DATA_LOSS row. Its
    modality is PR because the arm refuses outright for an image modality
    (`_IMAGE_MODALITIES` includes OT and CT). Both decoders raise
    `AttributeError: The dataset has no 'Pixel Data'` on these files, so
    "decode and catch" would be the wrong shape -- it would also pass a
    file whose pixel element vanished. Killing mutation: decoding
    unconditionally.
    """
    if case == "sr-no-pixels":
        inst = _image(None, sop=SR_STORAGE, modality="SR")
    else:
        inst = _image(np.ones((8, 8), dtype=np.float16), sop=GSPS_STORAGE,
                      modality="PR")
    real = io_handlers._decode_pixels
    calls = []

    def _spy(*args, **kwargs):
        calls.append(args)
        return real(*args, **kwargs)
    monkeypatch.setattr(io_handlers, "_decode_pixels", _spy)

    outcome = _export_instance_worker(
        _ctx_for(tmp_path, inst, verify_readback=True))

    assert outcome.ok, outcome.error
    assert calls == [], "a file with no pixel element was decoded"


def _ingested_ecg(tmp_path):
    """A DICOM ECG ingested by a real session; returns (session, instance)."""
    from scripts.generate_waveform_test_data import write_fixture

    src = tmp_path / "src"
    src.mkdir()
    write_fixture(str(src / "ecg.dcm"), num_samples=400)
    session = DicomSession(str(tmp_path / "ecg.db"))
    session.ingest(str(src))
    (inst,) = [i for p in session.store.patients for st in p.studies
               for se in st.series for i in se.instances]
    return session, inst


@pytest.mark.parametrize("case", ["healthy", "odd-length", "one-byte-flipped",
                                  "pad-on-an-even-length-value"])
def test_a_dicom_waveform_is_verified(tmp_path, monkeypatch, case):
    """A DICOM waveform's `WaveformData` is held against the bytes written (#449).

    `save_as` pads an odd-length OB/OW value with one `\\x00` (measured:
    6401 bytes written, 6402 read back), so a plain `==` fails every
    correct odd-length waveform -- an 8-bit one, say. The compare allows
    exactly that one pad byte, on exactly an odd-length value, and no
    other tail: the last case hands the check a one-byte `\\x00` tail on
    an even-length value, which no writer produces. Killing mutations:
    a plain `==` (odd-length fails); the waveform compare deleted
    (flipped passes); a tail test that accepts `\\x00` whatever the
    length (the last case passes).
    """
    session, inst = _ingested_ecg(tmp_path)
    try:
        raw = inst.get_waveform_bytes()
        assert len(raw) % 2 == 0, "precondition: the fixture is even-length"
        if case == "odd-length":
            # Slotted: the method is patched on the class, as an
            # instance attribute raises AttributeError.
            monkeypatch.setattr(Instance, "get_waveform_bytes",
                                lambda self: raw + b"\x07")
        elif case == "one-byte-flipped":
            _flip_on_read(monkeypatch, "WaveformData", 3)
        elif case == "pad-on-an-even-length-value":
            def _padded(*args, **kwargs):
                ds = _REAL_DCMREAD(*args, **kwargs)
                item = ds.WaveformSequence[0]
                item.WaveformData = bytes(item.WaveformData) + b"\x00"
                return ds
            monkeypatch.setattr(pydicom, "dcmread", _padded)

        outcome = _export_instance_worker(
            _ctx_for(tmp_path, inst, verify_readback=True))
    finally:
        session.close()

    if case in ("healthy", "odd-length"):
        assert outcome.ok, outcome.error
    else:
        assert not outcome.ok
        assert "WaveformData" in str(outcome.error), outcome.error


@pytest.mark.parametrize("compression", [None, "j2k"])
def test_values_outside_bits_stored_fail_an_uncompressed_readback(
        tmp_path, recwarn, compression):
    """A value the declared BitsStored cannot hold is read back as another (#449).

    Uncompressed, every conformant reader masks a native sample to
    BitsStored: -3024 at BitsStored 12 reads as 1072. The file says
    something other than what was meant, so under "decode everything,
    bit-exactly" it fails -- measured, 8 of these 16 samples. Under JPEG
    2000 the decoder does not mask and the samples come back exact.
    That decode, a 16-bit codestream under BitsStored 12, is where
    pydicom could warn on the caller's stream (#144, #248), and nothing
    does. Killing mutations: comparing under a BitsStored mask; the
    compare skipped for native syntaxes; a readback that warns.
    """
    src = np.array([[-3024, 3000, -1, 0]] * 4, dtype=np.int16)

    outcome = _export_instance_worker(
        _ctx_for(tmp_path, _image(src, SIGNED + BITS_STORED_12),
                 compression=compression, verify_readback=True))

    if compression is None:
        assert not outcome.ok
        message = str(outcome.error)
        assert "8 of 16 differ" in message, message
        assert ("first at flat index 0 (1072 read back where -3024 was "
                "written)") in message, message
    else:
        assert outcome.ok, outcome.error
        assert [str(w.message) for w in recwarn] == []


def test_a_message_less_decode_failure_still_names_its_type(tmp_path,
                                                             monkeypatch):
    """An exception with no message is still a reason (#449, #435's class).

    `({exc})` on `StopIteration()` is `()`, and a row that says the file
    "could not be decoded ()" names nothing an operator can act on.
    """
    def _silent(*_a, **_k):
        raise StopIteration()
    monkeypatch.setattr(io_handlers, "_decode_pixels", _silent)

    outcome = _export_instance_worker(_ctx(tmp_path, verify_readback=True))

    assert not outcome.ok
    assert "could not be decoded (StopIteration" in str(outcome.error), (
        outcome.error)


# ---------------------------------------------------------------------------
# Session-level: the option threads through, and the failure reaches the
# report through the same channel a failed write does (#181).
# ---------------------------------------------------------------------------

def _thread_visible_batch(monkeypatch):
    """Route the export batch where a parent monkeypatch can see it.

    `_run_export_batch` pins `maxtasksperchild=25`, which forces
    processes however the environment is set; dropping it and forcing
    threads keeps the whole pipeline real from `export_batch` down.
    """
    monkeypatch.setenv("ISOCENTER_FORCE_THREADS", "1")

    def _batch(tasks, show_progress, store_backend=None):
        return DicomExporter.export_batch(
            tasks, show_progress=show_progress, total=len(tasks),
            store_backend=store_backend)
    monkeypatch.setattr(DicomSession, "_run_export_batch",
                        staticmethod(_batch))


def test_a_readback_failure_files_an_error_row_and_fails_the_grade(
        tmp_path, monkeypatch):
    _thread_visible_batch(monkeypatch)
    real = pydicom.dcmread

    def _lying(*args, **kwargs):
        ds = real(*args, **kwargs)
        ds.Rows = ds.Rows + 1
        return ds
    monkeypatch.setattr(pydicom, "dcmread", _lying)

    session = _session(tmp_path)
    out = tmp_path / "out"
    report = tmp_path / "report.md"
    try:
        session.anonymize()
        # Every readback mismatches, so no instance is published and the
        # export raises (#191). The raise is last, after the ERROR rows
        # and the delivery counters the report below reads.
        with pytest.raises(ExportError):
            session.export(str(out), show_progress=False,
                           verify_readback=True)
        session.generate_report(str(report))
        db_path = session.store_backend.db_path
    finally:
        session.close()

    assert _files(out) == [], _files(out)

    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            "SELECT details FROM audit_log WHERE action_type='ERROR'"
        ).fetchall()
    assert len(rows) == 3, rows
    assert all("Rows" in details for (details,) in rows), rows

    content = report.read_text(encoding="utf-8")
    assert "| **Validation Status** | **REVIEW_REQUIRED** |" in content
    assert "| Instances Written | 0 of 3 requested |" in content


def test_with_readback_disabled_the_same_trap_changes_nothing(
        tmp_path, monkeypatch):
    """Default off must mean the exact pre-#209 behavior, cost included."""
    _thread_visible_batch(monkeypatch)

    def _boom(*_a, **_k):
        raise AssertionError("readback ran without being asked for")
    monkeypatch.setattr(pydicom, "dcmread", _boom)

    session = _session(tmp_path)
    out = tmp_path / "out"
    report = tmp_path / "report.md"
    try:
        session.anonymize()
        session.export(str(out), show_progress=False)
        session.generate_report(str(report))
    finally:
        session.close()

    assert len(_files(out)) == 3, _files(out)
    content = report.read_text(encoding="utf-8")
    assert "| **Validation Status** | **PASS** |" in content


def test_a_healthy_export_with_readback_enabled_stays_pass(tmp_path):
    """The check must be free of false positives on the ordinary path.

    Real subprocess workers, no traps: this is the arm that proves the
    option survives pickling into `ExportContext` and that a verified
    clean run reads exactly like an unverified one. Since #449 each worker
    also decodes every file it wrote -- here on the default JPEG 2000
    path -- so this is the in-worker proof that a healthy export passes
    its own decode.
    """
    session = _session(tmp_path)
    out = tmp_path / "out"
    report = tmp_path / "report.md"
    try:
        session.anonymize()
        session.export(str(out), show_progress=False, verify_readback=True)
        session.generate_report(str(report))
        db_path = session.store_backend.db_path
    finally:
        session.close()

    assert len(_files(out)) == 3, _files(out)
    with sqlite3.connect(db_path) as conn:
        errors = conn.execute(
            "SELECT COUNT(*) FROM audit_log WHERE action_type='ERROR'"
        ).fetchone()[0]
    assert errors == 0
    content = report.read_text(encoding="utf-8")
    assert "| **Validation Status** | **PASS** |" in content
    assert "| Instances Written | 3 of 3 requested |" in content


@pytest.mark.parametrize("lossy", ["all", "one"])
def test_a_pixel_readback_failure_takes_the_export_failure_channel(
        tmp_path, monkeypatch, lossy):
    """A pixel mismatch is an export failure like any other (#449, #181, #209).

    One `ERROR` row per instance, the grade to `REVIEW_REQUIRED`, nothing
    delivered for the instance that failed, and `ExportError` when no
    instance was written (#191). Measured on a00ef0f with the same
    encoder: 3 files and `PASS`. Killing mutations: the mismatch logged
    and swallowed (`ok=True`); a raise after `os.replace`, which delivers
    the file it rejects.
    """
    _thread_visible_batch(monkeypatch)
    arrays = [_ct_like(10 + n) for n in range(3)]
    _lossy_j2k(monkeypatch, only=None if lossy == "all" else arrays[1])

    session = _session(tmp_path, arrays)
    out = tmp_path / "out"
    report = tmp_path / "report.md"
    try:
        session.anonymize()
        if lossy == "all":
            with pytest.raises(ExportError, match="wrote 0 of 3"):
                session.export(str(out), show_progress=False,
                               verify_readback=True)
        else:
            session.export(str(out), show_progress=False,
                           verify_readback=True)
        session.generate_report(str(report))
        db_path = session.store_backend.db_path
    finally:
        session.close()

    failed = 3 if lossy == "all" else 1
    assert len(_files(out)) == 3 - failed, _files(out)
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            "SELECT details FROM audit_log WHERE action_type='ERROR'"
        ).fetchall()
    assert len(rows) == failed, rows
    assert all("Readback verification failed" in details
               for (details,) in rows), rows
    content = report.read_text(encoding="utf-8")
    assert "| **Validation Status** | **REVIEW_REQUIRED** |" in content
    assert (f"| Instances Written | {3 - failed} of 3 requested |"
            in content)
