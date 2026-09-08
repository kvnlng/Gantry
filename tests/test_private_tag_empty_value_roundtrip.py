"""A zero-length private element survives the store and reaches the file.

Two issues, one file. #339 is the store half: a `None` must not reload as
the word "None". #344 is the export half: the element the source said was
present-and-empty must be *written*, not dropped.

`save_vertical_attributes` renders every atom with `str()`, so a `None`
value -- which is what pydicom hands back for a zero-length element under
a *numeric* VR -- was stored as the four-character text `None` and
reloaded as that text. `"None"` is a conformant `LO` value, so the
reloaded export wrote it into the file as though the source had said it.

**The affected population is not the one the issue names.** #339 says
"a string VR with no value", and that is the one population that does
not reproduce: `pydicom.config.use_none_as_empty_text_VR_value` is
`False`, so a zero-length `LO`/`SH`/`UT` reads back as `''` and a `PN`
as `PersonName('')`, and those round-trip correctly today and after.
What reproduces is **eleven** VRs whose zero-length value pydicom reads
back as `None` -- `DS`, `IS`, `US`, `SS`, `UL`, `SL`, `UV`, `SV`, `FL`,
`FD`, `AT` -- plus `UN`, whose zero-length value is `None` rather than
`b''` and so misses the binary arm that would have retained it as empty
bytes. `SL`, `SV` and `UV` are named by neither #339 nor #344 and were
missing from this file's own list until #344 measured the population
instead of listing it: they are not in `BINARY_VRS`, are not `SQ`, `PN`
or `UN`, so they take `populate_attrs`' final `else` arm with
`elem.value is None`, and `_record_private_vr` records them because the
value is `None` rather than `bytes`.

**Fresh and reloaded disagreed in the audit trail as well as in the
file**, which is what picks the fix. Exported from the session that
ingested, the element is dropped with a `DATA_LOSS` row and the run
grades REVIEW_REQUIRED; exported after a save/close/reopen it was
written as `LO 'None'` with no row and a PASS. Skipping the tag at
`_split_core_and_private` -- the fix the issue proposes -- would have
made the *file* agree and left the *report* divergent: reloaded would
then drop the element in silence where fresh drops it loudly.
Preserving the `None` makes both halves agree, and forecloses nothing:
if the export encoder is ever taught to write a zero-length element,
both paths gain it at once, where a skip destroys the information for
good. The export writer already keeps a zero-length element rather
than dropping it, for the same reason:
`value = b""` at io_handlers.py line 1053.

**The file now carries the tag on both paths (#344)**, as a zero-length
element under the VR the source recorded for it -- or under `UN` where no
VR was ever recorded, which is every private element of an Implicit VR
source. That is not fabrication: the source asserted the tag's presence
and gave it no value, and a zero-length element is the one encoding DICOM
has for saying exactly that. #60's rule against inventing a value the
source never gave is untouched -- and its second clause, "not one we
invent **and not one we discard**", is the half this repo had been
failing since #118. No `DATA_LOSS` row is filed for the population any
more, because nothing is lost.

The VR half of that is observable only under an explicit-VR transfer
syntax; see `_write_src`.
"""
import glob
import os
import sqlite3

import numpy as np
import pydicom
import pytest
from pydicom.dataset import FileDataset, FileMetaDataset
from pydicom.tag import Tag
from pydicom.uid import ExplicitVRLittleEndian, generate_uid

from isocenter.persistence import SqliteStore
from isocenter.session import DicomSession

#: The eleven VRs whose zero-length value pydicom reads back as `None`
#: **and** for which `_record_private_vr` keeps the source's answer. Every
#: one of them reached `str(None)` on the way into `value_text` (#339),
#: and every one of them is now written back out under the VR named here
#: (#344). `SL`, `SV` and `UV` joined the list in #344: they behave
#: exactly like the other eight and were missing from #339's test and
#: from #344's own text.
EMPTY_RECORDED_VR = [
    (0x1005, 'DS'),
    (0x1006, 'US'),
    (0x1007, 'UL'),
    (0x1008, 'FL'),
    (0x1009, 'AT'),
    (0x100a, 'IS'),
    (0x100b, 'SS'),
    (0x100c, 'FD'),
    (0x100e, 'SL'),
    (0x100f, 'SV'),
    (0x1010, 'UV'),
]

#: The twelfth case, and the one with a different mechanism.
#: `_record_private_vr` refuses to record `UN` by design -- it is the
#: absence of an answer, not an answer -- so nothing is in `attribute_vrs`
#: for this tag and the export has to supply `UN` itself. This is also the
#: arm *every* private element of an Implicit VR Little Endian source
#: takes, so it is kept apart from the eleven rather than folded in.
EMPTY_NO_RECORDED_VR = (0x100d, 'UN')

#: The whole zero-length-`None` population, which is what the fixture
#: writes and what the loss accounting is measured over.
EMPTY_NUMERIC = EMPTY_RECORDED_VR + [EMPTY_NO_RECORDED_VR]

#: The VRs whose zero-length value pydicom reads back as `''` (or as an
#: empty `PersonName`). These were never affected; see the module
#: docstring and `test_a_text_vr_private_element_was_never_affected`.
EMPTY_TEXT = [
    (0x1020, 'LO'),
    (0x1021, 'SH'),
    (0x1022, 'PN'),
]


def _write_src(folder):
    """One instance whose private block is entirely zero-length.

    Explicit VR on purpose: it is what puts a real VR on each private
    element in the source file, so the numeric and text populations can
    be told apart at all. Under Implicit VR every one of them arrives as
    `UN` and the question does not arise.

    The pixel data is there so the export takes its compressed branch,
    which writes an explicit-VR transfer syntax -- under the
    uncompressed branch's Implicit VR Little Endian no private element
    carries a VR in the file and the assertions below could not be made.
    """
    meta = FileMetaDataset()
    meta.MediaStorageSOPClassUID = "1.2.840.10008.5.1.4.1.1.7"
    meta.MediaStorageSOPInstanceUID = generate_uid()
    meta.TransferSyntaxUID = ExplicitVRLittleEndian

    ds = FileDataset(None, {}, file_meta=meta, preamble=b"\0" * 128)
    ds.PatientID, ds.PatientName = "PAT339", "DOE^JOHN"
    ds.StudyInstanceUID, ds.SeriesInstanceUID = generate_uid(), generate_uid()
    ds.SOPInstanceUID = meta.MediaStorageSOPInstanceUID
    ds.SOPClassUID = meta.MediaStorageSOPClassUID
    ds.Modality, ds.SeriesNumber, ds.InstanceNumber = "OT", 1, 1
    ds.StudyDate = "20230101"

    ds.add_new(0x00090010, 'LO', 'ACME_HEADER')     # Private Creator
    for element, vr in EMPTY_NUMERIC + EMPTY_TEXT:
        ds.add_new(Tag(0x0009, element), vr, None)

    ds.Rows = ds.Columns = 4
    ds.BitsAllocated = ds.BitsStored = 8
    ds.HighBit = 7
    ds.SamplesPerPixel = 1
    ds.PhotometricInterpretation = "MONOCHROME2"
    ds.PixelRepresentation = 0
    ds.PixelData = np.zeros((4, 4), dtype=np.uint8).tobytes()

    path = os.path.join(folder, "one.dcm")
    ds.save_as(path, enforce_file_format=True)
    return ds.SOPInstanceUID


def _data_loss_tags(db_path):
    """The tags named by this store's `DATA_LOSS` rows."""
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            "SELECT details FROM audit_log WHERE action_type='DATA_LOSS'"
        ).fetchall()
    return sorted({
        f"0009,{element:04x}"
        for element, _vr in EMPTY_NUMERIC + EMPTY_TEXT
        for (details,) in rows
        if f"0009,{element:04x}" in details})


def _read_only_written(out):
    written = glob.glob(os.path.join(str(out), "**", "*.dcm"), recursive=True)
    assert written, "export produced no .dcm files"
    return pydicom.dcmread(written[0])


def _export_fresh(tmp_path):
    """Export from the session that ingested, with no store round trip."""
    src = tmp_path / "src"
    src.mkdir(exist_ok=True)
    _write_src(str(src))
    out = tmp_path / "out"
    db = str(tmp_path / "fresh.db")

    session = DicomSession(persistence_file=db)
    try:
        session.ingest(str(src))
        session.export(str(out), format="dicom", show_progress=False)
    finally:
        session.close()

    return _read_only_written(out), _data_loss_tags(db)


def _export_reloaded(tmp_path):
    """Export from a session that opened an existing database."""
    src = tmp_path / "src"
    src.mkdir(exist_ok=True)
    _write_src(str(src))
    db = str(tmp_path / "reloaded.db")
    out = tmp_path / "out"

    session = DicomSession(persistence_file=db)
    try:
        session.ingest(str(src))
        session.save()
    finally:
        session.close()

    session = DicomSession(persistence_file=db)
    try:
        session.export(str(out), format="dicom", show_progress=False)
    finally:
        session.close()

    return _read_only_written(out), _data_loss_tags(db)


@pytest.fixture(scope="module")
def fresh_export(tmp_path_factory):
    return _export_fresh(tmp_path_factory.mktemp("fresh339"))


@pytest.fixture(scope="module")
def reloaded_export(tmp_path_factory):
    return _export_reloaded(tmp_path_factory.mktemp("reloaded339"))


def test_a_zero_length_private_element_is_not_reloaded_as_the_word_none():
    """The store-method red, measured exactly as #339 reports it.

    `str(None)` is `'None'`, and there is no arm in `_vertical_atom_text`
    that does not go through `str()` -- the `AT` arm reaches the same
    place by a longer route, because `int(None)` raises `TypeError` and
    the handler documented for "a value that is no longer a tag" catches
    it and returns `str(atom)`.
    """
    store = SqliteStore(":memory:")
    try:
        store.save_vertical_attributes("I339", {("0029", "1010"): None})
        loaded = store.load_vertical_attributes("I339")
    finally:
        store.stop()

    assert loaded == {("0029", "1010"): None}, (
        "a value that was None came back as %r; the store invented "
        "content for an element that had none (#339)"
        % (loaded.get(("0029", "1010")),))


def test_the_at_arm_reaches_the_same_answer_as_every_other_vr():
    """`AT` is the one arm with a branch of its own, so it gets a case.

    Its `str()` is the display spelling `'(0010,0010)'`, so it stores the
    decimal integer instead -- and `int(None)` raises, which drops a
    `None` into the `except` arm whose comment is about a value that is
    no longer a tag. Guarding `None` before that arm is what keeps the
    normal path out of an exception handler documented for something
    else.
    """
    store = SqliteStore(":memory:")
    try:
        store.save_vertical_attributes(
            "I339AT", {("0029", "1011"): None}, vrs={("0029", "1011"): "AT"})
        loaded = store.load_vertical_attributes("I339AT")
    finally:
        store.stop()

    assert loaded[("0029", "1011")] is None


def test_a_none_atom_inside_a_list_keeps_its_place():
    """One `None` among siblings is stored and reloaded in position.

    The export then reports the whole element as loss -- siblings
    included -- because `_fallback_multivalue` returns `None` for the
    element as soon as one atom has no text encoding. That is that
    function's documented rule and it is the same on the fresh path, so
    the two agree here too.
    """
    store = SqliteStore(":memory:")
    try:
        store.save_vertical_attributes(
            "I339L", {("0029", "1012"): [None, "B"]})
        loaded = store.load_vertical_attributes("I339L")
    finally:
        store.stop()

    assert loaded[("0029", "1012")] == [None, "B"], (
        "a None atom inside a multi-valued element must keep its place "
        "rather than becoming the text 'None'")


def _assert_zero_length_under(exported, element, expected_vr, path):
    """The element is present, empty, and wearing `expected_vr`.

    All three halves matter and each fails differently. *Present* is the
    #344 fix. *Empty* is the part that keeps it from being fabrication --
    `is_empty` is one predicate over both readback shapes, `None` for a
    numeric VR and `''` for a text one. *The VR* is the half the owner
    decided: the source's own answer where it gave one, `UN` where it did
    not.
    """
    tag = Tag(0x0009, element)
    assert tag in exported, (
        "(0009,%04x) was a zero-length %s in the source and the %s export "
        "dropped it; the source asserted the tag was present and DICOM has "
        "an encoding for a present element with no value (#344)"
        % (element, expected_vr, path))
    written = exported[tag]
    assert written.is_empty, (
        "(0009,%04x) was zero-length in the source and the %s export wrote "
        "%r into it; a value was invented where the source gave none (#60)"
        % (element, path, written.value))
    assert written.VR == expected_vr, (
        "(0009,%04x) came out of the %s export as %s where the source said "
        "%s; a private element's VR is a fact the source file gave us and "
        "the fallback's LO throws it away (#154, #344)"
        % (element, path, written.VR, expected_vr))


@pytest.mark.parametrize("element, vr", EMPTY_RECORDED_VR)
def test_a_reloaded_export_writes_the_empty_element_under_its_recorded_vr(
        reloaded_export, element, vr):
    """The end-to-end green #344 buys, on the reloaded path.

    This change has no persistence half: #339 already preserved the
    `None` and the recorded VR through the store, on the explicit
    argument that "if the export encoder is ever taught to emit a
    zero-length element, both paths gain it at once". This is that, and
    it is asserted here rather than assumed -- the VR read off the file,
    not the tag's presence alone.
    """
    exported, _losses = reloaded_export
    _assert_zero_length_under(exported, element, vr, "reloaded")


@pytest.mark.parametrize("element, vr", EMPTY_RECORDED_VR)
def test_a_fresh_export_writes_the_empty_element_under_its_recorded_vr(
        fresh_export, element, vr):
    """The same assertion on the path that never touched the store.

    Nothing asserted the *fresh* path wrote the element before #344 --
    the two paths were compared only through their loss counts, which
    would stay equal if both regressed together and would name no
    element if only one did.
    """
    exported, _losses = fresh_export
    _assert_zero_length_under(exported, element, vr, "fresh")


@pytest.mark.parametrize("path", ["fresh", "reloaded"])
def test_the_element_with_no_recorded_vr_is_written_as_un(
        fresh_export, reloaded_export, path):
    """The twelfth case, kept apart because its mechanism is different.

    `_record_private_vr` refuses `UN` -- it is the absence of an answer,
    not an answer -- so `attribute_vrs` holds nothing for this tag and
    `_merge` has to supply the VR itself. `UN` is what PS3.5 6.2.2 says
    an element of unknown VR is, and writing a zero-length `LO` there
    would assert the element is text, which is a claim the source never
    made. This is also the arm *every* private element of an Implicit VR
    Little Endian source takes, so it is the common case in the field
    even though it is one row here.
    """
    element, vr = EMPTY_NO_RECORDED_VR
    exported, _losses = fresh_export if path == "fresh" else reloaded_export
    _assert_zero_length_under(exported, element, vr, path)


def test_neither_export_reports_a_loss_for_the_empty_block(
        fresh_export, reloaded_export):
    """The audit half: nothing is lost, so nothing is reported.

    Both halves are asserted in one place on purpose. The rows that stop
    being filed are exactly the elements that stop being dropped -- so a
    reviewer comparing two runs across #344 can tell an export that got
    better from a report that got quieter. Twelve rows before, zero
    after, and twelve elements in the file where there were none.

    The equality is #339's and is kept rather than spent: a skip at
    `_split_core_and_private` would have made the two files agree and
    left the *reports* divergent, and that is still the property being
    held here, now at zero instead of at twelve.
    """
    fresh_ds, fresh_losses = fresh_export
    reloaded_ds, reloaded_losses = reloaded_export

    assert fresh_losses == [], (
        "the fresh export still reports %r as data loss; nothing is lost "
        "-- the elements are in the file (#344)" % (fresh_losses,))
    assert reloaded_losses == fresh_losses, (
        "the reloaded export reported %r and the fresh one %r for the "
        "same source file: an element dropped loudly on one path and "
        "silently on the other (#339)" % (reloaded_losses, fresh_losses))

    for exported, path in ((fresh_ds, "fresh"), (reloaded_ds, "reloaded")):
        for element, _vr in EMPTY_NUMERIC:
            assert Tag(0x0009, element) in exported, (
                "the %s export files no DATA_LOSS row for (0009,%04x) and "
                "does not write it either: a quieter report over the same "
                "loss is the one outcome this change must not have"
                % (path, element))


@pytest.mark.parametrize("element, vr", EMPTY_TEXT)
def test_a_text_vr_private_element_was_never_affected(
        fresh_export, reloaded_export, element, vr):
    """A characterization test: what #339's own text got wrong.

    `pydicom.config.use_none_as_empty_text_VR_value` is `False`, so a
    zero-length `LO`/`SH` reads back as `''` and a zero-length `PN` as an
    empty `PersonName`. An empty string has a text encoding, so it
    survives the store as `''` and is written as a present, zero-length
    element on both paths -- which is what the file said. Nothing here
    changed; it is held still because a flip of that pydicom setting in
    some consumer's process would turn the whole text block into the
    population #339 describes, and this is where that would be caught
    rather than discovered.
    """
    for exported, _losses in (fresh_export, reloaded_export):
        element_out = exported[Tag(0x0009, element)]
        assert str(element_out.value) == "", (
            "(0009,%04x) was a zero-length %s in the source and came out "
            "as %r" % (element, vr, element_out.value))


def test_a_none_written_by_hand_onto_a_text_vr_private_tag_takes_that_vr(
        tmp_path):
    """A hand-set `None` follows the same rule, and that is deliberate.

    This test asserted the opposite direction until #344, and its name
    said so. The reversal is worth stating rather than editing away.

    The `None` here came from a caller, not from a source file -- and not
    from anywhere inside `isocenter/` either: the removal arms *delete*
    the key (`del entity.attributes[proposal.target_attr]`,
    `inst.attributes.pop(tag, None)`) and none of them writes a `None`
    value, so this shape is reachable only from user code. It is still
    not fabrication. `LO ''` says "this tag is present and has no value",
    which is what the graph says; the recorded VR is a fact about the
    *source element*, which was present, and `set_attr` does not clear
    `attribute_vrs`.

    Following the rule uniformly is also what keeps the implementation
    one branch rather than two: telling a file-borne `None` from a
    hand-set one needs a provenance flag the graph does not carry.

    What is *not* reachable this way is the old fabrication: the store no
    longer renders `None` as the four-character string, so nothing writes
    the word (#339).
    """
    src = tmp_path / "src"
    src.mkdir()
    _write_src(str(src))
    db = str(tmp_path / "byhand.db")
    out = tmp_path / "out"

    session = DicomSession(persistence_file=db)
    try:
        session.ingest(str(src))
        for patient in session.store.patients:
            for study in patient.studies:
                for series in study.series:
                    for instance in series.instances:
                        instance.set_attr("0009,1020", None)
        session.save()
    finally:
        session.close()

    session = DicomSession(persistence_file=db)
    try:
        session.export(str(out), format="dicom", show_progress=False)
    finally:
        session.close()

    exported = _read_only_written(out)
    _assert_zero_length_under(exported, 0x1020, 'LO', "hand-set")
    assert exported[Tag(0x0009, 0x1020)].value != 'None', (
        "the word 'None' was written for a hand-set None value (#339)")


def _loss_details(db_path):
    with sqlite3.connect(db_path) as conn:
        return [row[0] for row in conn.execute(
            "SELECT details FROM audit_log WHERE action_type='DATA_LOSS'")]


def test_a_none_among_siblings_is_the_same_loud_loss_on_both_paths(tmp_path):
    """The export half of the `[None, 'B']` edge, not only the store half.

    `_fallback_multivalue` returns `None` for the whole element as soon
    as one atom has no text encoding, and reports it as loss with its
    siblings -- "there is no half-written element in DICOM", which is
    that function's own documented rule and not something this fix
    changes. What matters here is that the reloaded path now reaches
    that rule at all: before, the `None` came back as the text `'None'`,
    every atom encoded, and the element was written as a two-value
    string with a word the source never said in it.

    **Since #344 this is also the test that catches the wrong mechanism
    for writing a zero-length element.** #344 intercepts a scalar `None`
    in `_merge` rather than widening `_value_fits_vr` to admit `None`,
    and the widen is the obvious alternative. It would make
    `_value_fits_vr([None, 'B'], 'LO')` return `True` through the
    recursive list arm; `add_new(tag, 'LO', [None, 'B'])` then
    *succeeds*, and `filewriter` raises `TypeError: sequence item 0:
    expected a bytes-like object, NoneType found` **past** `_merge`'s
    `try` -- failing the whole file rather than the element. That is the
    second failure class `_value_fits_vr`'s own docstring exists to
    prevent. Under the narrow route the two populations cannot overlap: a
    scalar `None` is intercepted one level above `_value_fits_vr`, and a
    `None` inside a list never reaches the interception because
    `[None, 'B'] is not None`.
    """
    tag = "0009,1050"
    src = tmp_path / "src"
    src.mkdir()
    _write_src(str(src))

    fresh_db = str(tmp_path / "siblings_fresh.db")
    session = DicomSession(persistence_file=fresh_db)
    try:
        session.ingest(str(src))
        for patient in session.store.patients:
            for study in patient.studies:
                for series in study.series:
                    for instance in series.instances:
                        instance.set_attr(tag, [None, "B"])
        session.export(str(tmp_path / "fresh_out"), format="dicom",
                       show_progress=False)
    finally:
        session.close()

    reloaded_db = str(tmp_path / "siblings_reloaded.db")
    session = DicomSession(persistence_file=reloaded_db)
    try:
        session.ingest(str(src))
        for patient in session.store.patients:
            for study in patient.studies:
                for series in study.series:
                    for instance in series.instances:
                        instance.set_attr(tag, [None, "B"])
        session.save(sync=True)
    finally:
        session.close()

    session = DicomSession(persistence_file=reloaded_db)
    try:
        exported = session.store.patients[0].studies[0].series[0].instances[0]
        assert exported.attributes.get(tag) == [None, "B"], (
            "the reloaded graph did not carry the None atom, so the "
            "export comparison below would not be about this edge")
        session.export(str(tmp_path / "reloaded_out"), format="dicom",
                       show_progress=False)
    finally:
        session.close()

    fresh_losses = [d for d in _loss_details(fresh_db) if tag in d]
    reloaded_losses = [d for d in _loss_details(reloaded_db) if tag in d]

    assert len(fresh_losses) == 1, fresh_losses
    assert len(reloaded_losses) == 1, (
        "the reloaded export reported %d losses for %s where the fresh "
        "one reported 1" % (len(reloaded_losses), tag))
    assert fresh_losses == reloaded_losses, (
        "the two paths report the element differently: %r vs %r"
        % (fresh_losses, reloaded_losses))
