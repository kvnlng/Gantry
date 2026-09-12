"""A full-sample array is not written under the `YBR_FULL_422` label (#470).

`YBR_FULL_422` names a layout with two chroma samples per two pixels:
four bytes per pixel pair. The graph cannot hold that layout -- the
array is `(rows, cols, 3)`, three full-resolution samples per pixel --
so an instance labelled `YBR_FULL_422` exported its full-resolution
samples under the subsampled label. Measured on 168fdd6, 3.12 and
3.14t: uncompressed, pydicom refused the file at every shape (8-bit,
16-bit, two frames, odd columns): `ValueError: The number of bytes of
pixel data is a third larger than expected (192 vs 128 bytes) ...`.
Under JPEG 2000 the file decoded, beneath a label still claiming a
layout the codestream does not have.

Now the exporter writes `YBR_FULL` for a 3-sample array declared
`YBR_FULL_422`, on both transfer syntaxes. That corrects only what the
shape proves -- the sampling -- and leaves the colour-space claim, YBR,
as declared: no label without its bytes (#372, #448, #482). The graph
keeps its label; `set_pixel_data()` and
`resolve_photometric_interpretation` are untouched, which is the
`PlanarConfiguration` split (#210, #217): the graph-side question is
theirs, and the exporter describes the element it just wrote.

Only hand-built graphs reach this. Ingest relabels every 8-bit YBR
source from the decoder's output (#372) and refuses 16-bit YBR.
"""
import itertools
from datetime import date

import numpy as np
import pydicom
import pytest
from pydicom.pixels import get_decoder

from isocenter.entities import Instance, Patient, Series, Study
from isocenter.io_handlers import (DicomExporter, ExportContext,
                                   _export_instance_worker)
from isocenter.session import DicomSession

SC_STORAGE = "1.2.840.10008.5.1.4.1.1.7"
REQUIRED = (
    ("0008,0020", "20230101"), ("0008,0030", "120000"),
    ("0008,0060", "OT"),
)
_serial = itertools.count(1)


def _ybr(frames=None, cols=8):
    """Full-range YBR samples, one corner distinct from the rest."""
    arr = np.zeros((8, cols, 3), np.uint8)
    arr[..., 0], arr[..., 1], arr[..., 2] = 200, 50, 90
    arr[0, 0] = (10, 20, 30)
    return arr if frames is None else np.stack([arr] * frames)


def _image(arr, attrs=(), *, before=()):
    inst = Instance(f"1.2.826.0.1.470.{next(_serial)}", SC_STORAGE, 1)
    inst.file_path = None
    for tag, value in REQUIRED:
        inst.set_attr(tag, value)
    for tag, value in before:
        inst.set_attr(tag, value)
    inst.set_pixel_data(arr)
    for tag, value in attrs:
        inst.set_attr(tag, value)
    return inst


def _export(tmp_path, inst, **kwargs):
    return _export_instance_worker(ExportContext(
        instance=inst,
        output_path=str(tmp_path / "out" / f"{inst.sop_instance_uid}.dcm"),
        patient_attributes={"0010,0010": "ANON", "0010,0020": "PAT1"},
        study_attributes={"0020,000d": "1.2.826.0.2.1"},
        series_attributes={"0020,000e": "1.2.826.0.3.1"},
        **kwargs))


def _stored(path):
    """The written file's samples as stored -- no colour conversion."""
    ds = pydicom.dcmread(path)
    return get_decoder(ds.file_meta.TransferSyntaxUID).as_array(
        ds, as_rgb=False)[0]


def _assert_full_sample_ybr(outcome, arr, inst):
    assert outcome.ok, outcome.error
    written = pydicom.dcmread(outcome.output_path)
    assert written.PhotometricInterpretation == "YBR_FULL"
    assert written.PlanarConfiguration == 0
    read = _stored(outcome.output_path)
    assert read.shape == arr.shape, (read.shape, arr.shape)
    assert np.array_equal(read, arr)
    assert inst.attributes["0028,0004"] == "YBR_FULL_422", \
        "the export must not write the graph"


@pytest.mark.parametrize("compression", [None, "j2k"])
def test_a_full_sample_422_instance_is_written_as_ybr_full(
        tmp_path, compression):
    """8-bit, one frame, both syntaxes (#470).

    Killing mutations: the relabel dropped (pydicom refuses the native
    file, and the JPEG 2000 file keeps the false label); the relabel to
    `RGB` (a colour-space claim the bytes do not make).
    """
    arr = _ybr()
    inst = _image(arr, (("0028,0004", "YBR_FULL_422"),))

    outcome = _export(tmp_path, inst, compression=compression)

    _assert_full_sample_ybr(outcome, arr, inst)


@pytest.mark.parametrize("compression", [None, "j2k"])
def test_two_frames_and_a_declared_planar_configuration(tmp_path, compression):
    """Multi-frame, with the source claiming planar bytes (#470, #210).

    The bytes written are interleaved whatever the source said, and the
    exporter already writes PlanarConfiguration 0 for that reason; the
    relabel rides beside it, not instead of it.
    """
    arr = _ybr(frames=2)
    inst = _image(arr, (("0028,0004", "YBR_FULL_422"), ("0028,0006", 1)),
                  before=(("0028,0008", 2),))

    outcome = _export(tmp_path, inst, compression=compression)

    _assert_full_sample_ybr(outcome, arr, inst)
    assert pydicom.dcmread(outcome.output_path).NumberOfFrames == 2


def test_odd_columns_and_sixteen_bit_samples(tmp_path):
    """The shapes the 422 layout could never hold at all (#470).

    A 4:2:2 file has an even number of columns by construction, and the
    16-bit case is what pydicom refuses with `384 vs 256 bytes`.
    """
    for arr in (_ybr(cols=7), _ybr().astype(np.uint16) * 200):
        inst = _image(arr, (("0028,0004", "YBR_FULL_422"),))

        outcome = _export(tmp_path, inst)

        _assert_full_sample_ybr(outcome, arr, inst)


def test_the_label_is_matched_after_normalisation(tmp_path):
    """` ybr_full_422 ` is the same declaration (#470).

    `resolve_photometric_interpretation` already compares its labels
    stripped and upper-cased; the relabel has to read the declaration
    the same way or a padded label slips past it into a refused file.
    Killing mutation: an exact string compare.
    """
    arr = _ybr()
    inst = _image(arr, (("0028,0004", " ybr_full_422 "),))

    outcome = _export(tmp_path, inst)

    assert outcome.ok, outcome.error
    assert pydicom.dcmread(outcome.output_path).PhotometricInterpretation \
        == "YBR_FULL"
    assert np.array_equal(_stored(outcome.output_path), arr)


@pytest.mark.parametrize("compression", [None, "j2k"])
def test_readback_passes_the_relabelled_file(tmp_path, compression):
    """`verify_readback=True` now agrees: the stored samples are the array (#470).

    Before the fix the native readback failed with `could not be decoded
    (ValueError: ... a third larger than expected ...)`.
    """
    arr = _ybr()

    outcome = _export(tmp_path, _image(arr, (("0028,0004", "YBR_FULL_422"),)),
                      compression=compression, verify_readback=True)

    assert outcome.ok, outcome.error
    assert pydicom.dcmread(outcome.output_path).PhotometricInterpretation \
        == "YBR_FULL"


@pytest.mark.parametrize("label", ["YBR_FULL", "RGB", "YBR_RCT"])
def test_other_colour_labels_are_left_as_declared(tmp_path, label):
    """The relabel is the 422 rule and nothing wider (#470).

    `YBR_FULL` already round-trips (the resolver's None arm, #186), and
    this is the control: the relabel must not become a `samples >= 3`
    rewrite, which is the rule #186 removed. Killing mutation: the
    relabel keyed on the sample count alone.
    """
    arr = _ybr()
    inst = _image(arr, (("0028,0004", label),))

    outcome = _export(tmp_path, inst)

    assert outcome.ok, outcome.error
    assert pydicom.dcmread(outcome.output_path).PhotometricInterpretation \
        == label


@pytest.mark.parametrize("samples", [1, 2])
def test_fewer_than_three_samples_under_the_label_are_not_relabelled(
        tmp_path, samples):
    """Only a 3-sample array proves the sampling claim false (#470).

    One sample beside a 422 label is the resolver's contradiction and
    comes out MONOCHROME2, as before. Two samples is a shape no
    photometric interpretation names, and the exporter writes the
    declaration it was given rather than a YBR_FULL it cannot vouch
    for. Killing mutation: the `samples >= 3` gate dropped.
    """
    arr = np.zeros((8, 8, samples), np.uint8) if samples > 1 \
        else np.zeros((8, 8), np.uint8)
    inst = _image(arr, (("0028,0004", "YBR_FULL_422"),),
                  before=(("0028,0002", samples),))

    outcome = _export(tmp_path, inst)

    assert outcome.ok, outcome.error
    written = pydicom.dcmread(outcome.output_path)
    assert written.SamplesPerPixel == samples
    assert written.PhotometricInterpretation == (
        "MONOCHROME2" if samples == 1 else "YBR_FULL_422")


def test_both_public_export_paths_write_the_same_label(tmp_path):
    """`session.export()` and `DicomExporter.write_tree()` agree (#470)."""
    def graph():
        patient = Patient("PAT1", "Original Name")
        study = Study("ST_1", date(2023, 1, 1))
        study.study_time = "120000"
        series = Series("SE_1", "OT", 1)
        series.instances.append(_image(_ybr(), (("0028,0004", "YBR_FULL_422"),)))
        study.series.append(series)
        patient.studies.append(study)
        return patient

    def labels(root):
        return sorted(pydicom.dcmread(p).PhotometricInterpretation
                      for p in root.rglob("*.dcm"))

    via_exporter = tmp_path / "via_exporter"
    DicomExporter.write_tree(graph(), str(via_exporter), compression=None,
                             show_progress=False)

    via_session = tmp_path / "via_session"
    with DicomSession(str(tmp_path / "l.db")) as session:
        session.store.patients.append(graph())
        session.save()
        session.export(str(via_session), use_compression=False,
                       show_progress=False)

    assert labels(via_exporter) == ["YBR_FULL"]
    assert labels(via_session) == ["YBR_FULL"]
