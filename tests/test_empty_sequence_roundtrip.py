"""A zero-item sequence is carried end to end, and no loss row is filed.

#392. A source element that says "this sequence is present and has no
items" vanished at ingest, and the session's own accounting said nothing:
`losses == []`, no `DATA_LOSS` row, no `ERROR` row, and an `EXPORT` row
reading `wrote 1 of 1 planned instances`. Two elements the source
asserted were absent from the exported file and nothing anywhere said so.

**The fix is preservation, not a loss row, and the hop trace is the whole
argument.** With an empty `DicomSequence` planted directly in the graph,
each of the four hops was measured separately:

| Hop | Code | Before |
| --- | --- | --- |
| A. source to graph | `process_sequence` | **dropped** -- the only way a sequence reached the graph was one `add_sequence_item` per item, and zero items made zero calls |
| B. graph to `attributes_json` | `_serialize_item` | preserved -- `{"0009,1005": [], "0008,1140": []}` |
| C. `attributes_json` to graph | `_deserialize_into` | **dropped** -- `for item_data in items_list` over an empty list never called `add_sequence_item` |
| D. graph to file | `_merge_sequences` | preserved -- `ds.add_new(tag, 'SQ', Sequence())` writes an empty SQ |

Two hops dropped it and two already carried it, so preservation is
possible end to end with two one-line edits and nothing is lost. A fix
that preserved the element *and* filed a `DATA_LOSS` row would have been
a new lie, which is why T5 asserts the row's absence -- against a
positive control, because an unflushed `SELECT` returns `[]` whether the
row is absent or merely late.

**The boundary these tests draw is private-versus-standard, not the
transfer syntax.** An empty *private* `SQ` under Implicit VR never
reaches `process_sequence` at all: the transfer syntax carries no VR and
the standard dictionary has no entry, so pydicom hands it over as `UN`
with value `None`, it lands in `attributes`, and #344's zero-length arm
exports it as a zero-length `UN` -- present in the file, exactly as
PS3.5 6.2.2 prescribes for an element whose VR was never known. That is
not this fix and does not become an `SQ` after it;
`test_an_implicit_vr_empty_private_sequence_is_still_the_un_population`
pins it as a characterization test. A *standard* tag under Implicit VR
resolves its VR from the dictionary, does reach `process_sequence`, and
is covered -- which is why the fixture has an Implicit VR arm.

`add_sequence()` is the new name on `DicomItem` that both edits call, and
`add_sequence_item()` now delegates to it, so adding the first item to a
brand-new sequence advances `_revision` twice where it advanced once.
Harmless -- `_revision` is a monotonic counter and `has_unsaved_changes`
is a comparison, not arithmetic -- and
`test_adding_the_first_item_to_a_new_sequence_still_leaves_one_dirty_entity`
is what says so rather than a comment.
"""
import glob
import os
import sqlite3

import numpy as np
import pydicom
import pytest
from pydicom.dataset import Dataset, FileDataset, FileMetaDataset
from pydicom.sequence import Sequence
from pydicom.tag import Tag
from pydicom.uid import (ExplicitVRLittleEndian, ImplicitVRLittleEndian,
                         generate_uid)

from isocenter.session import DicomSession

#: An empty private `SQ`. Under Explicit VR the source carries the VR, so
#: it reaches `process_sequence`; under Implicit VR it does not, and that
#: is the `UN` population above.
EMPTY_PRIVATE_SQ = "0009,1005"

#: An empty standard `SQ` -- Referenced Image Sequence. Its VR comes from
#: the dictionary, so it reaches `process_sequence` under either transfer
#: syntax.
EMPTY_STANDARD_SQ = "0008,1140"

#: The control: Referenced Study Sequence, written with exactly one item,
#: which itself carries an empty `(0008,1140)` for the nesting test.
FULL_STANDARD_SQ = "0008,1110"


def _write_src(folder, transfer_syntax=ExplicitVRLittleEndian):
    """One instance carrying two empty sequences, one full one, and a nested empty one.

    The pixel data is there so the export takes its compressed branch,
    which writes an explicit-VR transfer syntax. Under the uncompressed
    branch's Implicit VR Little Endian the exported private tag would come
    back as `UN` whatever the graph held, and the `VR == 'SQ'` assertions
    could not be made at all.
    """
    meta = FileMetaDataset()
    meta.MediaStorageSOPClassUID = "1.2.840.10008.5.1.4.1.1.7"
    meta.MediaStorageSOPInstanceUID = generate_uid()
    meta.TransferSyntaxUID = transfer_syntax

    ds = FileDataset(None, {}, file_meta=meta, preamble=b"\0" * 128)
    ds.PatientID, ds.PatientName = "PAT392", "DOE^JOHN"
    ds.StudyInstanceUID, ds.SeriesInstanceUID = generate_uid(), generate_uid()
    ds.SOPInstanceUID = meta.MediaStorageSOPInstanceUID
    ds.SOPClassUID = meta.MediaStorageSOPClassUID
    ds.Modality, ds.SeriesNumber, ds.InstanceNumber = "OT", 1, 1
    ds.StudyDate = "20230101"

    ds.add_new(0x00090010, 'LO', 'ACME_HEADER')          # Private Creator
    ds.add_new(Tag(0x0009, 0x1005), 'SQ', Sequence())    # empty, private
    ds.ReferencedImageSequence = Sequence()              # empty, standard

    # One item, so the control has something to be unchanged about -- and
    # that item carries an empty sequence of its own, which is the whole
    # of the nesting test. Both edits sit in recursive functions, so the
    # nested case is covered by construction; T6 is what proves it.
    inner = Dataset()
    inner.ReferencedSOPClassUID = "1.2.840.10008.5.1.4.1.1.7"
    inner.ReferencedSOPInstanceUID = "1.2.826.0.1.3680043.8.498.392"
    inner.ReferencedImageSequence = Sequence()
    ds.ReferencedStudySequence = Sequence([inner])

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


def _read_only_written(out):
    written = glob.glob(os.path.join(str(out), "**", "*.dcm"), recursive=True)
    assert written, "export produced no .dcm files"
    return pydicom.dcmread(written[0])


def _the_instance(session):
    for patient in session.store.patients:
        for study in patient.studies:
            for series in study.series:
                for instance in series.instances:
                    return instance
    raise AssertionError("no instance in the graph")


def _audit_rows(db_path):
    with sqlite3.connect(db_path) as conn:
        return conn.execute(
            "SELECT action_type, details FROM audit_log").fetchall()


def _tag_of(ds, tag_str):
    g, e = (int(part, 16) for part in tag_str.split(','))
    return ds.get(Tag(g, e))


def _export_fresh(tmp_path, transfer_syntax=ExplicitVRLittleEndian):
    """Export from the session that ingested, with no store round trip.

    Isolates hop A: only `process_sequence` stands between the source and
    the file.
    """
    src = tmp_path / "src"
    src.mkdir(exist_ok=True)
    _write_src(str(src), transfer_syntax)
    out = tmp_path / "out"
    db = str(tmp_path / "fresh.db")

    session = DicomSession(persistence_file=db)
    try:
        session.ingest(str(src))
        graph = {tag: list(seq.items)
                 for tag, seq in _the_instance(session).sequences.items()}
        attributes = dict(_the_instance(session).attributes)
        session.export(str(out), format="dicom", show_progress=False)
        session.store_backend.flush_audit_queue()
    finally:
        session.close()

    return _read_only_written(out), graph, attributes, _audit_rows(db), str(out)


def _export_reloaded(tmp_path, transfer_syntax=ExplicitVRLittleEndian):
    """Export from a session that opened an existing database.

    Adds hops B and C, so this is the arm the `_deserialize_into` edit
    shows up in and the fresh arm does not.
    """
    src = tmp_path / "src"
    src.mkdir(exist_ok=True)
    _write_src(str(src), transfer_syntax)
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
        instance = _the_instance(session)
        graph = {tag: list(seq.items) for tag, seq in instance.sequences.items()}
        attributes = dict(instance.attributes)
        session.export(str(out), format="dicom", show_progress=False)
        session.store_backend.flush_audit_queue()
    finally:
        session.close()

    return _read_only_written(out), graph, attributes, _audit_rows(db), str(out)


@pytest.fixture(scope="module")
def fresh(tmp_path_factory):
    return _export_fresh(tmp_path_factory.mktemp("fresh392"))


@pytest.fixture(scope="module")
def reloaded(tmp_path_factory):
    return _export_reloaded(tmp_path_factory.mktemp("reloaded392"))


@pytest.fixture(scope="module")
def fresh_implicit(tmp_path_factory):
    return _export_fresh(tmp_path_factory.mktemp("freshimp392"),
                         ImplicitVRLittleEndian)


@pytest.fixture(scope="module")
def reloaded_implicit(tmp_path_factory):
    return _export_reloaded(tmp_path_factory.mktemp("reloadimp392"),
                            ImplicitVRLittleEndian)


# The three (fixture-name, tag) pairs the preservation tests run over.
# The third is the §11.10 arm: a *standard* tag under Implicit VR resolves
# its VR from the dictionary and does reach `process_sequence`, so the
# boundary is private-versus-standard rather than a property of the
# transfer syntax.
EMPTY_CASES = [
    ("fresh", EMPTY_PRIVATE_SQ),
    ("fresh", EMPTY_STANDARD_SQ),
    ("fresh_implicit", EMPTY_STANDARD_SQ),
]
RELOADED_CASES = [
    ("reloaded", EMPTY_PRIVATE_SQ),
    ("reloaded", EMPTY_STANDARD_SQ),
    ("reloaded_implicit", EMPTY_STANDARD_SQ),
]


@pytest.mark.parametrize("fixture_name,tag", EMPTY_CASES + RELOADED_CASES)
def test_an_empty_sequence_from_the_source_is_in_the_graph_after_ingest(
        fixture_name, tag, request):
    """Hop A, and the two tiers must not both claim the tag."""
    _ds, graph, attributes, _rows, _out = request.getfixturevalue(fixture_name)
    assert tag in graph, (
        f"{tag} was asserted by the source and is not in the graph's "
        f"sequences; present: {sorted(graph)}")
    assert graph[tag] == []
    assert tag not in attributes, (
        f"{tag} is in both `sequences` and `attributes`; one tag, one tier")


@pytest.mark.parametrize("fixture_name,tag", EMPTY_CASES)
def test_an_empty_sequence_reaches_the_exported_file_fresh(
        fixture_name, tag, request):
    """Red when the `process_sequence` edit is removed; green when only
    the `_deserialize_into` edit is. That asymmetry is what tells the two
    hops apart."""
    ds, _graph, _attributes, _rows, _out = request.getfixturevalue(fixture_name)
    element = _tag_of(ds, tag)
    assert element is not None, f"{tag} is absent from the exported file"
    assert element.VR == 'SQ'
    assert len(element.value) == 0


@pytest.mark.parametrize("fixture_name,tag", RELOADED_CASES)
def test_an_empty_sequence_reaches_the_exported_file_reloaded(
        fixture_name, tag, request):
    """Red when *either* edit is removed. With the fresh test above, the
    pair discriminates hop A from hop C; neither alone does."""
    ds, _graph, _attributes, _rows, _out = request.getfixturevalue(fixture_name)
    element = _tag_of(ds, tag)
    assert element is not None, f"{tag} is absent from the exported file"
    assert element.VR == 'SQ'
    assert len(element.value) == 0


@pytest.mark.parametrize("tag", [EMPTY_PRIVATE_SQ, EMPTY_STANDARD_SQ])
def test_an_empty_sequence_survives_the_store(tag, reloaded):
    """The graph, not just the file, so a future export-side workaround
    cannot make this pass."""
    _ds, graph, _attributes, _rows, _out = reloaded
    assert tag in graph
    assert graph[tag] == []


def test_no_loss_row_is_filed_for_an_empty_sequence(reloaded):
    """Nothing is lost, so nothing is filed -- asserted against a positive
    control.

    An assertion that a `SELECT` returns nothing passes identically when
    the audit queue has simply not been flushed. The fixture flushes; this
    test then reads the `EXPORT` row in the *same* query set before
    asserting the absence, so "no `DATA_LOSS` row" is a fact about the log
    rather than about the thread that writes it.
    """
    _ds, _graph, _attributes, rows, out = reloaded

    exports = [details for action, details in rows if action == 'EXPORT']
    assert exports, "positive control: no EXPORT row in the audit log"
    assert any(out in details for details in exports), (
        f"positive control: no EXPORT row names {out}; rows: {exports}")

    offenders = [(action, details) for action, details in rows
                 if action == 'DATA_LOSS'
                 and (EMPTY_PRIVATE_SQ in details or EMPTY_STANDARD_SQ in details)]
    assert not offenders, (
        "a DATA_LOSS row was filed for a sequence that was carried end to "
        f"end: {offenders}")


@pytest.mark.parametrize("fixture_name", ["fresh", "reloaded"])
def test_an_empty_sequence_nested_in_a_sequence_item_survives(
        fixture_name, request):
    """Both edits sit in recursive functions, so nesting is covered by
    construction. Red when either is special-cased to the root item."""
    ds, graph, _attributes, _rows, _out = request.getfixturevalue(fixture_name)

    item = graph[FULL_STANDARD_SQ][0]
    assert EMPTY_STANDARD_SQ in item.sequences
    assert item.sequences[EMPTY_STANDARD_SQ].items == []

    outer = _tag_of(ds, FULL_STANDARD_SQ)
    assert outer is not None and len(outer.value) == 1
    inner = outer.value[0].get(Tag(0x0008, 0x1140))
    assert inner is not None, "the nested empty sequence is absent from the file"
    assert inner.VR == 'SQ'
    assert len(inner.value) == 0


@pytest.mark.parametrize("fixture_name", ["fresh", "reloaded"])
def test_a_sequence_with_items_is_unchanged(fixture_name, request):
    """The control. Red when `add_sequence_item`'s delegation loses the append."""
    ds, graph, _attributes, _rows, _out = request.getfixturevalue(fixture_name)

    assert len(graph[FULL_STANDARD_SQ]) == 1
    assert graph[FULL_STANDARD_SQ][0].attributes["0008,1155"] == \
        "1.2.826.0.1.3680043.8.498.392"

    outer = _tag_of(ds, FULL_STANDARD_SQ)
    assert outer is not None and len(outer.value) == 1
    assert outer.value[0].ReferencedSOPInstanceUID == \
        "1.2.826.0.1.3680043.8.498.392"


def test_an_implicit_vr_empty_private_sequence_is_still_the_un_population(
        fresh_implicit):
    """A characterization test, not a promise about the fix.

    An empty *private* `SQ` under Implicit VR never reaches
    `process_sequence`: the transfer syntax carries no VR and the standard
    dictionary has no entry, so pydicom hands the element over as `UN`
    with value `None` and it lands in `attributes`. #344's zero-length arm
    then exports it as a zero-length `UN`, which is what PS3.5 6.2.2
    prescribes for an element whose VR was never known. Red when someone
    "extends" #392's fix into `_sequence_from_un_bytes`.
    """
    ds, graph, attributes, _rows, _out = fresh_implicit

    assert EMPTY_PRIVATE_SQ not in graph
    assert EMPTY_PRIVATE_SQ in attributes
    assert attributes[EMPTY_PRIVATE_SQ] is None

    element = _tag_of(ds, EMPTY_PRIVATE_SQ)
    assert element is not None
    assert element.VR == 'UN'
    assert element.value is None


def test_adding_the_first_item_to_a_new_sequence_still_leaves_one_dirty_entity():
    """`add_sequence_item` delegates to `add_sequence`, so the first item
    on a brand-new sequence advances `_revision` twice where it advanced
    once. `has_unsaved_changes` is a comparison, not arithmetic, so one
    entity is dirty either way -- this is what says so rather than a
    comment, and it is where a future `mark_persisted(revision=captured)`
    straddling the extra bump would show up."""
    from isocenter.entities import DicomItem

    parent = DicomItem()
    parent.mark_persisted()
    assert not parent.has_unsaved_changes

    captured = parent._revision
    parent.add_sequence_item("0008,1110", DicomItem())
    assert parent.has_unsaved_changes
    assert parent._revision > captured

    # A commit that captured the revision *before* the append must not
    # write the append off: `mark_persisted(captured)` leaves it dirty.
    parent.mark_persisted(revision=captured)
    assert parent.has_unsaved_changes

    parent.mark_persisted()
    assert not parent.has_unsaved_changes


def test_an_empty_private_sequence_is_stripped_by_remove_private_tags(tmp_path):
    """The de-identified export must not gain a vendor tag from this fix.

    An empty private `SQ` is a new shape for the private sweep: before
    #392 it never reached `sequences` at all, so `remove_private_tags`
    could not have seen it and the question did not arise. The sweep asks
    `owner.sequences.keys()` directly (#167) and does not care whether the
    sequence has items, so it is stripped with the rest of the vendor
    block -- but "should be" is not "is", and this is the assertion rather
    than the reasoning.
    """
    src = tmp_path / "src"
    src.mkdir()
    _write_src(str(src))
    out = tmp_path / "out"

    session = DicomSession(persistence_file=str(tmp_path / "priv.db"))
    try:
        session.configuration.remove_private_tags = True
        session.ingest(str(src))
        assert EMPTY_PRIVATE_SQ in _the_instance(session).sequences, (
            "fixture never entered the arm under test: the empty private "
            "sequence is not in the graph")
        session.audit()
        session.anonymize()
        session.export(str(out), format="dicom", show_progress=False)
    finally:
        session.close()

    ds = _read_only_written(out)
    leftover = [str(elem.tag) for elem in ds if elem.tag.group % 2]
    assert leftover == [], f"private elements survived the sweep: {leftover}"

    # The standard empty sequence is not private and must be untouched by
    # the same pass -- otherwise "stripped" would be indistinguishable
    # from "dropped again".
    standard = _tag_of(ds, EMPTY_STANDARD_SQ)
    assert standard is not None and standard.VR == 'SQ'
    assert len(standard.value) == 0


def test_add_sequence_is_idempotent_and_only_dirties_when_it_creates():
    """A sequence that newly exists is a change the store must hold; a
    second call on a tag that already has one is not.

    Both halves are asserted, and only one of them loses data. This test
    asserted the harmless half alone until review of #405: deleting
    `mark_modified()` from `add_sequence()` left the **whole suite**
    green at 1770 passed, while dirtying unconditionally reddened one
    test. The `mark_persisted()` before the first call is what makes the
    load-bearing assertion say anything -- a fresh `DicomItem` starts at
    revision 1 against 0 persisted, so `has_unsaved_changes` is already
    True and asserting it there would hold for a reason that has nothing
    to do with `add_sequence`.
    """
    from isocenter.entities import DicomItem

    parent = DicomItem()
    parent.mark_persisted()
    assert not parent.has_unsaved_changes, (
        "fixture never entered the arm under test: the baseline the next "
        "assertion is measured against is not clean")

    created = parent.add_sequence("0008,1140")
    assert parent.sequences["0008,1140"] is created
    assert created.items == []
    assert parent.has_unsaved_changes, (
        "add_sequence created a sequence and left the entity reporting "
        "nothing to save")

    parent.mark_persisted()
    again = parent.add_sequence("0008,1140")
    assert again is created
    assert not parent.has_unsaved_changes, (
        "add_sequence dirtied an entity without creating anything")

    # Case-insensitive, like every other tag entry point.
    assert parent.add_sequence("0008,1140".upper()) is created
    assert len(parent.sequences) == 1


#: A standard sequence tag the fixture source does **not** carry, so
#: `add_sequence()` genuinely creates one rather than finding it.
ADDED_STANDARD_SQ = "0008,1115"     # Referenced Series Sequence


def test_a_sequence_added_through_the_public_method_reaches_the_store(tmp_path):
    """`add_sequence()`'s dirtying, pinned by consequence rather than flag.

    `has_unsaved_changes` is a flag a mutation can satisfy by accident;
    what it exists for is that the next `save()` writes the row. So this
    goes through the store: reload a saved graph, add an empty sequence
    through the published method, save, reopen, and read it back.

    *Red when:* `mark_modified()` is deleted from `add_sequence()`.
    Measured with it deleted -- the reloaded instance reports nothing to
    save, `save(sync=True)` skips it, and the reopened graph has no
    `0008,1115` at all. That is #392's own failure mode ("this sequence
    is present and has no items" becoming nothing at all) reintroduced
    through the tier-2 method this change publishes.
    """
    src = tmp_path / "addseq_src"
    src.mkdir()
    db = str(tmp_path / "addseq.db")
    _write_src(str(src))

    session = DicomSession(persistence_file=db)
    try:
        session.ingest(str(src))
        session.save(sync=True)
    finally:
        session.close()

    session = DicomSession(persistence_file=db)
    try:
        instance = _the_instance(session)
        assert not instance.has_unsaved_changes, (
            "fixture never entered the arm under test: the reloaded "
            "instance was already dirty, so the assertion below would "
            "hold whatever add_sequence did")
        assert ADDED_STANDARD_SQ not in instance.sequences, (
            "fixture never entered the arm under test: the tag already "
            "exists, so add_sequence creates nothing")

        added = instance.add_sequence(ADDED_STANDARD_SQ)
        assert added.items == []
        assert instance.has_unsaved_changes, (
            "add_sequence created a sequence and left the instance "
            "reporting nothing to save; the next save skips it")
        session.save(sync=True)
    finally:
        session.close()

    session = DicomSession(persistence_file=db)
    try:
        reloaded = _the_instance(session)
        assert ADDED_STANDARD_SQ in reloaded.sequences, (
            "the sequence added through add_sequence() never reached the "
            "store: the instance said it had nothing to save")
        assert reloaded.sequences[ADDED_STANDARD_SQ].items == [], (
            "the sequence came back with items it never had")
        # The control: the sequences that were already there are still
        # there, so "reached the store" is not "rewrote the row wrong".
        assert EMPTY_STANDARD_SQ in reloaded.sequences
        assert reloaded.sequences[EMPTY_STANDARD_SQ].items == []
    finally:
        session.close()
