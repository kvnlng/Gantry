"""Signed JPEG Lossless and JPEG-LS pixels decode as the values they are (#446).

`imagecodecs.ljpeg_decode` and `jpegls_decode` return the masked unsigned
bit pattern of every sample, at every BitsStored, and never sign-extend.
So a signed (PixelRepresentation 1) 12-bit frame holding -800 came back
from `Instance.get_pixel_data()` as `uint16` 3296, with no error, while
`ingest()` refused the same file since #416 ("decoded to uint16, where
... declare int16"). Two doors, two answers, and one of them wrong.

The ruling: decode correctly. The handler sign-extends from BitsStored,
the rule pydicom applies with its own plugins, measured bit-exact against
pydicom with pylibjpeg-libjpeg and pyjpegls at 8, 12 and 16 bits. It
lives in the handler's `_decode_frame`, which both doors reach -- ingest
through `decode_declared_frames`, the read door through `get_pixel_data`
-- so there is one rule, not a guard at one door and a fix at the other.
Every case here is asserted at all three: `ingest()`, the store's
answer after it; `Instance(file_path).get_pixel_data()`; and the
handler's own `get_pixel_data`.

JPEG 2000 is not touched: `jpeg2k_decode` returns signed samples already
(S4).

**Every fixture first asserts that `pydicom.dcmread(p).pixel_array`
raises**, so no case passes through pydicom's door and says nothing about
this one. Every value assertion is against a module literal.
"""
import os
import struct

import imagecodecs
import numpy as np
import pydicom
import pytest
from pydicom.dataset import FileDataset, FileMetaDataset
from pydicom.encaps import encapsulate
from pydicom.uid import generate_uid

from isocenter import imagecodecs_handler
from isocenter.entities import Instance
from isocenter.session import DicomSession

LJPEG = "1.2.840.10008.1.2.4.57"
LJPEG_SV1 = "1.2.840.10008.1.2.4.70"
JPEGLS = "1.2.840.10008.1.2.4.80"
JPEGLS_NEAR = "1.2.840.10008.1.2.4.81"
J2K_LOSSLESS = "1.2.840.10008.1.2.4.90"

#: One row per BitsStored: the extremes, -800 clipped to fit, and values
#: either side of zero, so a missing sign extension (3296 for -800), a
#: shift from the wrong width, and an unsigned view all show. Tiled to
#: 16x16 below.
SIGNED_ROWS = {
    8: [-128, -128, -1, 0, 127, 5, -5, 100],
    12: [-2048, -800, -1, 0, 2047, 5, -5, 100],
    16: [-32768, -800, -1, 0, 32767, 5, -5, 100],
}
SIGNED = {bs: np.tile(np.array(row, dtype=np.int8 if bs <= 8 else np.int16),
                      (16, 2))
          for bs, row in SIGNED_ROWS.items()}

#: A JPEG-LS stream of `SIGNED[12]`'s 12-bit pattern written by pyjpegls
#: (CharLS) at precision 12 -- what a conformant encoder writes for
#: BitsStored 12. `imagecodecs.jpegls_encode` cannot write it: it always
#: writes precision 16 for `uint16`. Checked in as bytes so the test needs
#: no encoder this package does not install.
P12_JPEGLS = bytes.fromhex(
    "ffd8fff7000b0c0010001001011100ffda0008010100000000000000001ffd000000"
    "0019be63b8027fe8ff0a933c000000007cd8b76c6f004ffd1ff294cb81020410421082"
    "084444408108888921092490410924922249504115551155421155511554212aaa4af0"
    "84fe4fc213f93f113f93f113f93f113fafc45fd7e22febf117f5f88bfafc00ffd9")


def _pattern(signed, bits_stored):
    """The two's-complement pattern masked to BitsStored, unsigned."""
    unsigned = np.dtype(f"u{signed.dtype.itemsize}")
    return (signed.astype(np.int64) & ((1 << bits_stored) - 1)).astype(
        unsigned)


def _ljpeg(pattern, bits_stored):
    return imagecodecs.ljpeg_encode(pattern, bitspersample=bits_stored)


def _sof3_predictor_6(pattern, bits_stored):
    # libjpeg-turbo's lossless mode at predictor 6, a second encoder
    # beside lj92's own, so the rule is not pinned on one encoder's
    # streams alone.
    return imagecodecs.jpeg8_encode(pattern, lossless=True, predictor=6,
                                    bitspersample=bits_stored)


def _jpegls(pattern, _bits_stored):
    return imagecodecs.jpegls_encode(pattern)


def _dataset(ts, codestream, shape, bits_stored, *, bits_allocated=None,
             pixel_representation=1, high_bit=None,
             photometric="MONOCHROME2", samples=1):
    meta = FileMetaDataset()
    meta.MediaStorageSOPClassUID = "1.2.840.10008.5.1.4.1.1.7"
    meta.MediaStorageSOPInstanceUID = generate_uid()
    meta.TransferSyntaxUID = ts
    ds = FileDataset(None, {}, file_meta=meta, preamble=b"\0" * 128)
    ds.PatientID, ds.PatientName = "PAT446", "DOE^JOHN"
    ds.StudyInstanceUID, ds.SeriesInstanceUID = generate_uid(), generate_uid()
    ds.SOPInstanceUID = meta.MediaStorageSOPInstanceUID
    ds.SOPClassUID = meta.MediaStorageSOPClassUID
    ds.Modality, ds.SeriesNumber, ds.InstanceNumber = "OT", 1, 1
    ds.StudyDate = "20230101"
    ds.Rows, ds.Columns = shape[0], shape[1]
    ds.SamplesPerPixel = samples
    if samples > 1:
        ds.PlanarConfiguration = 0
    ds.PhotometricInterpretation = photometric
    ds.BitsAllocated = bits_allocated or (8 if bits_stored <= 8 else 16)
    ds.BitsStored = bits_stored
    ds.HighBit = bits_stored - 1 if high_bit is None else high_bit
    ds.PixelRepresentation = pixel_representation
    ds.PixelData = encapsulate([codestream], has_bot=True)
    ds["PixelData"].is_undefined_length = True
    return ds


def _write(folder, ds):
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, "one.dcm")
    ds.save_as(path, enforce_file_format=True)
    with pytest.raises(RuntimeError):
        _ = pydicom.dcmread(path).pixel_array
    return path


@pytest.fixture
def doors(tmp_path):
    """Write `ds`; return what each of the three doors makes of it.

    `ingest` is `(ingested, failures, stored)`, `stored` being the store's
    array after `unload_pixel_data()` -- the sidecar's answer, not a
    resident one -- or None when nothing ingested. The two read doors are
    the array or the exception.
    """
    sessions = []

    def _run(ds, name="one"):
        src = tmp_path / f"src_{name}"
        path = _write(str(src), ds)
        session = DicomSession(persistence_file=str(tmp_path / f"{name}.db"))
        sessions.append(session)
        summary = session.ingest(str(src))
        stored = None
        if summary.ingested:
            inst = [i for p in session.store.patients for st in p.studies
                    for se in st.series for i in se.instances][0]
            assert inst.unload_pixel_data() is True
            stored = inst.get_pixel_data()
        out = {"ingest": (summary.ingested, summary.failures, stored)}
        for door, read in (
                ("instance", lambda: Instance(
                    generate_uid(), "1.2.840.10008.5.1.4.1.1.7", 1,
                    file_path=path).get_pixel_data()),
                ("handler", lambda: imagecodecs_handler.get_pixel_data(
                    pydicom.dcmread(path)))):
            try:
                out[door] = read()
            except Exception as exc:  # pylint: disable=broad-except
                out[door] = exc
        return out

    yield _run
    for session in sessions:
        session.close()


def _assert_reads(got, want):
    """Each door returned exactly `want`, dtype included."""
    ingested, failures, stored = got["ingest"]
    assert (ingested, failures) == (1, []), failures
    for door, arr in (("ingest", stored), ("instance", got["instance"]),
                      ("handler", got["handler"])):
        assert isinstance(arr, np.ndarray), f"{door}: {arr!r}"
        assert arr.dtype == want.dtype, f"{door}: {arr.dtype}"
        assert arr.tolist() == want.tolist(), f"{door}: {arr[0].tolist()}"


# ---------------------------------------------------------------------------
# S1 -- signed frames read their values, at all three doors
# ---------------------------------------------------------------------------

#: `(ts, bits_stored, encoder)`. Every stream here is written at precision
#: = BitsStored, so the stream and the header agree on what a sample is.
#: The JPEG-LS 12-bit case `imagecodecs.jpegls_encode` writes is
#: precision 16 under BitsStored 12, where they disagree; it is S1b.
_S1_CASES = [
    (LJPEG, 8, _ljpeg), (LJPEG, 12, _ljpeg), (LJPEG, 16, _ljpeg),
    (LJPEG_SV1, 8, _ljpeg), (LJPEG_SV1, 12, _ljpeg), (LJPEG_SV1, 16, _ljpeg),
    (LJPEG, 8, _sof3_predictor_6), (LJPEG, 12, _sof3_predictor_6),
    (LJPEG, 16, _sof3_predictor_6),
    (JPEGLS, 8, _jpegls), (JPEGLS, 16, _jpegls),
    (JPEGLS_NEAR, 8, _jpegls), (JPEGLS_NEAR, 16, _jpegls),
]


@pytest.mark.parametrize(
    "ts,bits_stored,encode", _S1_CASES,
    ids=[f"{ts[-3:]}-{bs}-{enc.__name__.strip('_')}"
         for ts, bs, enc in _S1_CASES])
def test_a_signed_lossless_jpeg_frame_reads_its_values_at_both_doors(
        doors, ts, bits_stored, encode):
    """S1: ingest, the Instance door and the handler agree, on the values.

    Before: ingest refused (`decoded to uint16 ... declare int16`) and
    both read doors returned the unsigned pattern -- 3296 for -800 at
    BitsStored 12, 64736 at 16. Near-lossless (.81) is encoded at NEAR 0
    so the values are exact.
    """
    want = SIGNED[bits_stored]
    codestream = encode(_pattern(want, bits_stored), bits_stored)
    _assert_reads(doors(_dataset(ts, codestream, want.shape, bits_stored)),
                  want)


# ---------------------------------------------------------------------------
# S2 -- a precision-12 JPEG-LS stream, as a conformant encoder writes it
# ---------------------------------------------------------------------------

def test_a_precision_12_jpeg_ls_stream_from_another_encoder_reads_exactly(
        doors):
    """S2: the one JPEG-LS fixture whose stream precision is BitsStored.

    `imagecodecs.jpegls_encode` always writes precision 16, so every other
    JPEG-LS fixture here disagrees with a 12-bit header by construction.
    This one is CharLS's, at precision 12, and it is what a scanner's
    encoder writes for BitsStored 12.
    """
    # The SOF55 precision byte, so the literal cannot drift into another
    # stream without this line saying so.
    assert P12_JPEGLS[P12_JPEGLS.index(b"\xff\xf7") + 4] == 12
    _assert_reads(doors(_dataset(JPEGLS, P12_JPEGLS, (16, 16), 12)),
                  SIGNED[12])


# ---------------------------------------------------------------------------
# S6 -- a decode narrower than BitsStored is refused, never shifted
# ---------------------------------------------------------------------------

def test_a_signed_decode_narrower_than_bits_stored_is_refused_at_both_doors(
        doors):
    """S6: an 8-bit JPEG-LS stream under a signed BitsStored 12 header.

    The codec returns `uint8`, and 12 bits cannot be sign-extended inside
    8: the shift the rule computes would be -4. The handler refuses, in
    its words, at the read door, and ingest refuses. Found by the probe
    (#446 review): with `and` in place of `or` in the guard, a `uint8`
    decode passed it and came back as whatever a negative shift made of
    it, with no error at the read door.
    """
    source = np.arange(16 * 16, dtype=np.uint8).reshape(16, 16)
    got = doors(_dataset(JPEGLS, imagecodecs.jpegls_encode(source),
                         source.shape, 12, bits_allocated=16))
    assert got["ingest"][0] == 0
    for door in ("instance", "handler"):
        words = str(got[door])
        assert isinstance(got[door], Exception), f"{door}: {got[door]!r}"
        assert "cannot sign-extend a uint8 decode from BitsStored 12" in \
            words, f"{door}: {words}"


# ---------------------------------------------------------------------------
# S3 -- a JPEG Lossless fragment of odd length decodes
# ---------------------------------------------------------------------------

def _item(value):
    return struct.pack("<HHI", 0xFFFE, 0xE000, len(value)) + value


def test_an_odd_length_jpeg_lossless_fragment_decodes(doors):
    """S3: lj92 needs the pad byte DICOM framing is supposed to add.

    `imagecodecs.ljpeg_decode` (lj92) reads one byte past the end of its
    input, so an odd-length codestream with nothing after it raises
    `LJ92_ERROR_CORRUPT`. A conformant writer pads every item to even
    length (PS3.5 7.5) and the pad is that byte, so a conformant file
    never shows it -- but pydicom writes and reads an odd item length
    without complaint, and `generate_frames` hands the frame over as
    stored. This is that file: an 8x8 flat 8-bit frame, whose codestream
    is 59 bytes, framed by hand with its odd length. Before the handler
    padded, all three doors refused it; `jpegsof3_decode` reads the same
    59 bytes exactly, so nothing about the stream is wrong.
    """
    want = np.full((8, 8), 7, dtype=np.uint8)
    codestream = imagecodecs.ljpeg_encode(want)
    assert len(codestream) % 2 == 1, len(codestream)
    ds = _dataset(LJPEG_SV1, codestream, want.shape, 8,
                  pixel_representation=0)
    # Replaced by hand: `encapsulate` pads the fragment, which is the
    # whole difference. Empty offset table, the fragment at its odd
    # length, the sequence delimiter.
    ds.PixelData = (_item(b"") + _item(codestream)
                    + struct.pack("<HHI", 0xFFFE, 0xE0DD, 0))
    _assert_reads(doors(ds), want)


# ---------------------------------------------------------------------------
# S4 -- JPEG 2000 keeps its own signedness
# ---------------------------------------------------------------------------

def test_a_j2k_codestream_keeps_its_own_signedness(doors):
    """S4: the correction is keyed on the syntax, never applied to J2K.

    `jpeg2k_decode` returns signed samples, already sign-extended, from a
    signed codestream (F1's `int16` cases in
    `tests/test_ingest_imagecodecs_fallback.py`). So an *unsigned*
    codestream under PixelRepresentation 1 is a contradiction the file
    makes, and the fallback refuses it. A correction reaching J2K would
    admit it with wrong values -- and raise on F1's signed output.

    Three samples, because only 16-bit multi-sample J2K reaches the
    fallback here: pydicom's Pillow plugin decodes 16-bit monochrome
    J2K itself, and a monochrome fixture never leaves pydicom's door.
    """
    want = np.stack([SIGNED[16]] * 3, axis=-1)
    codestream = imagecodecs.jpeg2k_encode(_pattern(want, 16), level=0,
                                           codecformat="J2K")
    got = doors(_dataset(J2K_LOSSLESS, codestream, want.shape, 16,
                         photometric="RGB", samples=3))
    ingested, failures, _stored = got["ingest"]
    assert ingested == 0
    reason = failures[0][1]
    assert "decoded to uint16" in reason, reason
    assert "declare int16" in reason, reason
