"""Pixel data inside a sequence item is carried by the store (#183).

An Icon Image Sequence item carries its own (7fe0,0010), and until now
nothing carried it anywhere: the item's `0028,xxxx` descriptors reached the
graph and were exported while the bytes did not, producing a file that
declares a 2x2 icon and holds nothing for it. Pixel Data is Type 1 in the
Icon Image Macro (PS3.3 C.7.6.1.1.6), so that file is nonconformant, and it
is #160's shape at a second site. #169 made the drop *audible*; this makes
it stop happening.

Three properties this module exists to hold down, in descending order of how
badly a regression would hurt:

1. **A store that redacts anything exports no nested icons at all.** An icon
   is a downsampled copy of a frame, and *nothing* in this pipeline scans or
   redacts one: every pixel consumer reads `instance.get_pixel_data()`,
   which is the top-level frame and only that. So carrying icon bytes out of
   a session that redacted would re-export a thumbnail of exactly what
   redaction removed. The gate is store-wide rather than per-instance
   because an icon under Referenced Image Sequence is a thumbnail of a
   *different* SOP instance (PS3.3 C.7.6.16), and redaction calls
   `regenerate_uid()` -- so "look up the referenced instance and ask if it
   was redacted" returns nothing for precisely the instances that were.
   That lookup fails open, which is the worst available answer.

2. **Position is the only identity a sequence item has.** The blob's key is
   a path recorded at ingest and resolved at export, and everything in
   between can change the sequence. An item *removed* makes the path resolve
   to None; an earlier sibling removed makes it resolve to the **wrong
   item**. Silent wrong bytes is this repo's worst failure class.

3. **Carried or reported, never both and never neither.** A decode that
   fails must still file its `DATA_LOSS` row, and bytes that are in the
   store must stop filing one -- reporting a loss that did not happen is
   #194's defect, and section 3 of the compliance report is headed "present
   in the source and not in the exported data".
"""

import os
import sqlite3

import numpy as np
import pydicom
import pytest
from pydicom.dataset import Dataset, FileDataset, FileMetaDataset
from pydicom.sequence import Sequence
from pydicom.uid import (ExplicitVRLittleEndian, JPEGBaseline8Bit,
                         RLELossless, generate_uid)

from isocenter.io_handlers import (DicomExporter, LOSS_SCOPE_STANDARD,
                                   _CARRIABLE_TRANSFER_SYNTAXES)
from isocenter.blob_kind import serialize_blob_kind
from isocenter.session import DicomSession

CT_IMAGE = "1.2.840.10008.5.1.4.1.1.2"

#: The icon's own bytes, distinct from anything the top-level frame holds
#: so a mix-up cannot pass a byte comparison.
ICON_BYTES = bytes([11, 22, 33, 44])

#: Icon Image Sequence and Referenced Image Sequence, in the lowercase-hex
#: spelling the graph and the blob kind both use.
ICON_SEQ = "0088,0200"
REF_IMAGE_SEQ = "0008,1140"


def _icon_item(payload=ICON_BYTES, rows=2, cols=2, samples=1,
               photometric="MONOCHROME2", planar=None, bits=8):
    """One Icon Image Sequence item, complete enough to decode.

    "Complete enough" is the whole difference between this and the
    bare-descriptor icon in `tests/test_private_binary_ingest.py`, whose
    missing BitsAllocated makes it undecodable and so keeps its loss row.
    """
    item = Dataset()
    item.Rows, item.Columns = rows, cols
    item.BitsAllocated = item.BitsStored = bits
    item.HighBit = bits - 1
    item.SamplesPerPixel = samples
    item.PhotometricInterpretation = photometric
    item.PixelRepresentation = 0
    if planar is not None:
        item.PlanarConfiguration = planar
    item.add_new(0x7FE00010, 'OW' if bits > 8 else 'OB', payload)
    return item


def _write_src(folder, icons=(), referenced_icons=(), serial="SN-1",
               transfer_syntax=ExplicitVRLittleEndian, top_level_pixels=True):
    """A CT instance carrying icons at depth 1 and/or depth 2.

    `icons` go under Icon Image Sequence directly. `referenced_icons` go
    under Referenced Image Sequence items -- the depth-2 shape, and the one
    whose thumbnail is of a *different* SOP instance.

    `top_level_pixels=False` is for the lossy-transfer-syntax fixture: no
    encoder plugin exists in this environment, so a lossy file with
    top-level Pixel Data would fail the *whole* ingest at the top-level
    decode and never reach the nested candidate at all.
    """
    meta = FileMetaDataset()
    meta.MediaStorageSOPClassUID = CT_IMAGE
    meta.MediaStorageSOPInstanceUID = generate_uid()
    meta.TransferSyntaxUID = transfer_syntax

    ds = FileDataset(None, {}, file_meta=meta, preamble=b"\0" * 128)
    ds.PatientID, ds.PatientName = "PAT1", "DOE^JOHN"
    ds.StudyInstanceUID, ds.SeriesInstanceUID = generate_uid(), generate_uid()
    ds.SOPInstanceUID = meta.MediaStorageSOPInstanceUID
    ds.SOPClassUID = CT_IMAGE
    ds.Modality, ds.SeriesNumber, ds.InstanceNumber = "CT", 1, 1
    ds.StudyDate, ds.StudyTime = "20230101", "120000"
    ds.DeviceSerialNumber = serial
    ds.Manufacturer, ds.ManufacturerModelName = "ACME", "SCAN9000"
    # `IODValidator` refuses to write a CT Image that is missing these, so
    # without them every export in this module fails before it reaches the
    # subject under test -- with an `ERROR` row about geometry rather than a
    # visible assertion about icons.
    ds.SliceThickness, ds.KVP = "1.0", "120"
    ds.ImagePositionPatient = [0.0, 0.0, 0.0]
    ds.ImageOrientationPatient = [1.0, 0.0, 0.0, 0.0, 1.0, 0.0]
    ds.PixelSpacing = [1.0, 1.0]

    if top_level_pixels:
        ds.Rows = ds.Columns = 4
        ds.BitsAllocated = ds.BitsStored = 8
        ds.HighBit = 7
        ds.SamplesPerPixel = 1
        ds.PhotometricInterpretation = "MONOCHROME2"
        ds.PixelRepresentation = 0
        ds.PixelData = np.arange(16, dtype=np.uint8).tobytes()
    else:
        # A conformant-enough non-image shape: no Image Pixel Module at all,
        # so nothing asks pydicom to decode the top level.
        ds.Modality = "SR"
        ds.SOPClassUID = "1.2.840.10008.5.1.4.1.1.88.11"
        meta.MediaStorageSOPClassUID = ds.SOPClassUID

    if icons:
        ds.IconImageSequence = Sequence(list(icons))
    if referenced_icons:
        items = []
        for icon in referenced_icons:
            ref = Dataset()
            ref.ReferencedSOPClassUID = CT_IMAGE
            ref.ReferencedSOPInstanceUID = generate_uid()
            ref.IconImageSequence = Sequence([icon])
            items.append(ref)
        ds.ReferencedImageSequence = Sequence(items)

    path = os.path.join(folder, "one.dcm")
    ds.save_as(path, enforce_file_format=True)
    return path


def _ingest(tmp_path, name, **kwargs):
    """Write a source, ingest it, save, close. Returns (db, src_dir)."""
    src = tmp_path / f"src_{name}"
    src.mkdir()
    _write_src(str(src), **kwargs)
    db = str(tmp_path / f"{name}.db")

    session = DicomSession(persistence_file=db)
    try:
        session.ingest(str(src))
        session.save()
    finally:
        session.close()
    return db, str(src)


def _exported(out_dir):
    """The one written .dcm, read back."""
    written = [os.path.join(r, f) for r, _d, files in os.walk(str(out_dir))
               for f in files if f.endswith(".dcm")]
    assert len(written) == 1, written
    return pydicom.dcmread(written[0])


def _data_loss_rows(db):
    with sqlite3.connect(db) as conn:
        return conn.execute(
            "SELECT details, loss_scope FROM audit_log "
            "WHERE action_type='DATA_LOSS'").fetchall()


def _blob_kinds(db):
    with sqlite3.connect(db) as conn:
        return [k for k, in conn.execute("SELECT kind FROM instance_blobs")]


#: `[y1, y2, x1, x2]`, the shape `redact_by_machine` documents. Covers the
#: whole 4x4 top-level frame, so the redaction is unmistakable.
WHOLE_FRAME = [0, 4, 0, 4]


def _export(db, out):
    """Reopen the store and export."""
    session = DicomSession(persistence_file=db)
    try:
        session.export(str(out), format="dicom", use_compression=False)
    finally:
        session.close()


# --- 1. The bytes survive the store -------------------------------------

def test_a_nested_icon_survives_a_close_and_reopen(tmp_path):
    """Ingest, save, close, reopen, export -- the icon's bytes come back.

    The reopen is the point. Carriage that only works while the session is
    still open is carriage by the pydicom Dataset that ingest happened to
    still be holding, not by the store.
    """
    db, _src = _ingest(tmp_path, "reopen", icons=[_icon_item()])
    out = tmp_path / "out"
    _export(db, out)

    exported = _exported(out)
    assert "IconImageSequence" in exported
    icon = exported.IconImageSequence[0]
    assert icon.PixelData == ICON_BYTES
    assert not [d for d, _s in _data_loss_rows(db) if "7fe0,0010" in d]


def test_the_source_file_can_go_away(tmp_path):
    """The whole point of carrying the bytes rather than re-reading them.

    Float pixel data was carried by the *source file* until #327, and the
    failure mode that motivated moving it was exactly this: move the file,
    delete it, or reopen the session on another machine, and the bytes are
    gone. An icon held only in a pydicom Dataset has the same problem.
    """
    db, src = _ingest(tmp_path, "gone", icons=[_icon_item()])
    os.remove(os.path.join(src, "one.dcm"))

    out = tmp_path / "out"
    _export(db, out)

    assert _exported(out).IconImageSequence[0].PixelData == ICON_BYTES


def test_a_depth_two_icon_round_trips(tmp_path):
    """`0008,1140/3/0088,0200/0/7fe0,0010` -- the path loop, exercised.

    Depth 1 alone would leave the loop unexercised, and a rule that is only
    right at the depth it was tested is the shape #169 started from. Four
    Referenced Image Sequence items so the ordinal under test is not 0:
    an index that is always zero cannot tell a working path walk from one
    that ignores the index entirely.
    """
    payloads = [bytes([i, i + 1, i + 2, i + 3]) for i in (1, 5, 9, 13)]
    db, _src = _ingest(
        tmp_path, "depth2",
        referenced_icons=[_icon_item(p) for p in payloads])

    kinds = _blob_kinds(db)
    assert serialize_blob_kind(
        "pixels", ((REF_IMAGE_SEQ, 3), (ICON_SEQ, 0)), "7fe0,0010") in kinds

    out = tmp_path / "out"
    _export(db, out)
    exported = _exported(out)
    for index, payload in enumerate(payloads):
        icon = exported.ReferencedImageSequence[index].IconImageSequence[0]
        assert icon.PixelData == payload, index


def test_a_planar_colour_icon_is_stored_and_declared_interleaved(tmp_path):
    """PlanarConfiguration must be corrected on the item, as at the top level.

    pydicom de-planarises on read, so the bytes the sidecar holds are
    interleaved whatever the source declared. `ingest_worker` already forces
    the top-level (0028,0006) to 0 for that reason. Leaving a nested 1 in
    place would export interleaved bytes under a planar declaration -- a
    colour icon read as garbage by a conformant reader, which is worse than
    the drop this change replaces.
    """
    planar = np.array([[[1, 2, 3], [4, 5, 6]],
                       [[7, 8, 9], [10, 11, 12]]], dtype=np.uint8)
    # Planar on the wire: all reds, then all greens, then all blues.
    wire = planar.transpose(2, 0, 1).tobytes()

    db, _src = _ingest(
        tmp_path, "planar",
        icons=[_icon_item(wire, samples=3, photometric="RGB", planar=1)])

    out = tmp_path / "out"
    _export(db, out)
    icon = _exported(out).IconImageSequence[0]

    assert icon.PlanarConfiguration == 0
    assert icon.PixelData == planar.tobytes()


def test_compaction_preserves_a_nested_blob(tmp_path):
    """A loader left on a pre-compaction offset reads the wrong bytes.

    `compact_sidecar`'s uid_map is pixels-only *and keyed by UID alone*, so
    it cannot carry a second pixel payload for one instance. Nested refs are
    repointed from the blob table for the same reason waveform loaders are.
    The failure this catches is silent wrong bytes, which only a byte
    comparison after a compaction can see.
    """
    db, _src = _ingest(tmp_path, "compact", icons=[_icon_item()])

    session = DicomSession(persistence_file=db)
    try:
        session.compact()
        inst = session.store.patients[0].studies[0].series[0].instances[0]
        key = (((ICON_SEQ, 0),), "7fe0,0010")
        assert key in inst._nested_pixel_refs, inst._nested_pixel_refs
        session.export(str(tmp_path / "out"), format="dicom",
                       use_compression=False)
    finally:
        session.close()

    assert _exported(tmp_path / "out").IconImageSequence[0].PixelData \
        == ICON_BYTES


def test_a_redacted_instance_keeps_its_nested_row_under_the_new_uid(tmp_path):
    """`instance_blobs` is keyed by UID and `regenerate_uid()` changes it.

    This went wrong once already for the top-level blob -- see
    `tests/test_redaction_identity.py` -- and a row left under the retired
    UID is an orphan only `compact()` notices. The nested rows follow for
    free only because `save_all` re-emits them from
    `inst.sop_instance_uid` on every save; a design that wrote them once at
    ingest and never again would strand them here.
    """
    db, _src = _ingest(tmp_path, "reduid", icons=[_icon_item()])

    session = DicomSession(persistence_file=db)
    try:
        inst = session.store.patients[0].studies[0].series[0].instances[0]
        old_uid = inst.sop_instance_uid
        session.redact_by_machine("SN-1", WHOLE_FRAME)
        new_uid = session.store.patients[0].studies[0].series[0]\
            .instances[0].sop_instance_uid
        session.save()
    finally:
        session.close()

    assert new_uid != old_uid
    with sqlite3.connect(db) as conn:
        rows = conn.execute(
            "SELECT instance_uid, kind FROM instance_blobs").fetchall()

    nested = [(u, k) for u, k in rows if k.startswith("pixels:")]
    assert nested == [(new_uid, "pixels:0088,0200/0/7fe0,0010")], rows


# --- 2. The de-identification gate ---------------------------------------

def test_a_redacted_store_exports_no_icon_item_at_all(tmp_path):
    """Not descriptors without bytes -- the whole item goes (#183 Q2/Q10).

    Nothing scans or redacts an icon, so the drop is what protects the
    export and carrying the bytes would remove that protection silently.
    Leaving the descriptors behind would reintroduce the Type 1 violation
    this change exists to fix, at a fourth site; #160 settled the identical
    question for discarded multiplex groups and chose exactly this.
    """
    db, _src = _ingest(tmp_path, "redacted", icons=[_icon_item()])

    out = tmp_path / "out"
    session = DicomSession(persistence_file=db)
    try:
        session.configuration.rules = [
            {"serial_number": "SN-1", "redaction_zones": [WHOLE_FRAME]}]
        session.redact()
        session.export(str(out), format="dicom", use_compression=False)
    finally:
        session.close()

    exported = _exported(out)
    assert "IconImageSequence" not in exported
    assert ICON_BYTES not in exported.PixelData
    rows = [d for d, s in _data_loss_rows(db)
            if "7fe0,0010" in d and "redact" in d.lower()]
    assert rows, _data_loss_rows(db)
    assert all(s == LOSS_SCOPE_STANDARD
               for d, s in _data_loss_rows(db) if "redact" in d.lower())


def test_the_gate_fires_on_the_attestation_with_no_configured_zones(tmp_path):
    """`ctx.redaction_zones` alone is not the gate, and this proves it.

    `_redaction_zones_for` looks the zones up **at export time**, from the
    **current** configuration, keyed on the series' device serial number.
    `RedactionService` does not consult that -- it redacts whatever `rois`
    its caller passed. So the zones list is empty at export while the pixels
    are redacted whenever the rule was edited, the serial changed, the
    service was driven directly, or the series has no equipment at all. In
    each of those the top-level frame ships zeroed and, without this half of
    the gate, the icon ships intact.

    The instance itself carries the answer: `_ISOCENTER_REDACTION_HASH` is
    written on every path that actually modified pixels.
    """
    db, _src = _ingest(tmp_path, "attest", icons=[_icon_item()])

    out = tmp_path / "out"
    session = DicomSession(persistence_file=db)
    try:
        # Redacted through the direct `rois` door, so nothing is configured
        # and `_redaction_zones_for` returns [] at export.
        session.redact_by_machine("SN-1", WHOLE_FRAME)
        # `redact_by_machine` restores the original rules in its `finally`,
        # so nothing is configured by the time the export looks.
        assert not session.configuration.rules
        session.export(str(out), format="dicom", use_compression=False)
    finally:
        session.close()

    assert "IconImageSequence" not in _exported(out)


def test_an_unredacted_instance_loses_its_icon_to_a_redaction_elsewhere(
        tmp_path):
    """The store-wide half, and the reason a per-instance gate cannot work.

    An icon under Referenced Image Sequence is a thumbnail of the SOP
    instance being *referenced* (PS3.3 C.7.6.16), not of the one carrying
    it. If the referenced image was redacted and this one was not, both
    halves of a per-instance condition pass and the export ships a thumbnail
    of exactly what redaction removed, one file over.

    It cannot be resolved by following the reference either: redaction calls
    `regenerate_uid()`, so `ReferencedSOPInstanceUID` names a UID that is no
    longer in the store, and the lookup returns nothing for precisely the
    instances that were redacted. It fails *open*.

    So the condition is store-wide, and this test is what says so: the
    instance carrying the icon is never redacted, and its icon is dropped
    anyway because a different instance in the same store was.
    """
    src = tmp_path / "src"
    src.mkdir()
    _write_src(str(src), referenced_icons=[_icon_item()], serial="SN-CARRIER")
    # A second, unrelated instance in its own series -- the one that gets
    # redacted. Different device serial, so no configured rule can reach the
    # carrier even if one existed.
    src2 = tmp_path / "src2"
    src2.mkdir()
    _write_src(str(src2), serial="SN-OTHER")
    os.replace(os.path.join(str(src2), "one.dcm"),
               os.path.join(str(src), "two.dcm"))

    db = str(tmp_path / "storewide.db")
    out = tmp_path / "out"
    session = DicomSession(persistence_file=db)
    try:
        session.ingest(str(src))
        carrier = None
        other = None
        for patient in session.store.patients:
            for study in patient.studies:
                for series in study.series:
                    for inst in series.instances:
                        if inst.sequences.get(REF_IMAGE_SEQ):
                            carrier = inst
                        else:
                            other = inst
        assert carrier is not None and other is not None

        # Redacted through `redact_by_machine`, which restores the
        # (empty) original rules in its `finally` -- so at export time
        # `_redaction_zones_for` returns [] for BOTH instances and the
        # attestation on `other` is the only thing the gate can see.
        session.redact_by_machine("SN-OTHER", WHOLE_FRAME)
        assert "_ISOCENTER_REDACTION_HASH" in other.attributes
        assert "_ISOCENTER_REDACTION_HASH" not in carrier.attributes
        assert not session.configuration.rules

        session.export(str(out), format="dicom", use_compression=False)
    finally:
        session.close()

    written = [os.path.join(r, f) for r, _d, files in os.walk(str(out))
               for f in files if f.endswith(".dcm")]
    assert len(written) == 2, written
    for path in written:
        ds = pydicom.dcmread(path)
        for ref in ds.get("ReferencedImageSequence", []):
            assert "IconImageSequence" not in ref, path


def test_the_gate_removes_the_icon_item_not_its_referencing_parent(tmp_path):
    """Drop the icon's item; the Referenced Image Sequence item stays.

    The item at the full path goes. Its parent carries
    `ReferencedSOPInstanceUID`, which is a reference and not a thumbnail --
    removing it would delete information the icon was merely attached to.
    """
    db, _src = _ingest(tmp_path, "parent", referenced_icons=[_icon_item()])

    out = tmp_path / "out"
    session = DicomSession(persistence_file=db)
    try:
        session.redact_by_machine("SN-1", WHOLE_FRAME)
        session.export(str(out), format="dicom", use_compression=False)
    finally:
        session.close()

    exported = _exported(out)
    assert "ReferencedImageSequence" in exported
    assert len(exported.ReferencedImageSequence) == 1
    assert "ReferencedSOPInstanceUID" in exported.ReferencedImageSequence[0]
    assert "IconImageSequence" not in exported.ReferencedImageSequence[0]


def test_removing_one_icon_item_does_not_shift_the_next_one_out_of_reach(
        tmp_path):
    """Two icons under one parent sequence, both dropped by the gate.

    Removing item 0 slides item 1 into index 0. A loop that resolves each
    path as it removes would then find item 1's path resolving to None,
    file a "gone" row for it, and leave it in the file -- descriptors with
    no bytes, which is the Type 1 violation the gate is supposed to avoid.
    So every path is resolved and every outcome decided before anything is
    removed.

    Two icons under ONE parent is the shape that can see this; one icon
    each under two different parents cannot.
    """
    db, _src = _ingest(
        tmp_path, "twoicons",
        referenced_icons=[_icon_item(b"\x01\x02\x03\x04"),
                          _icon_item(b"\x05\x06\x07\x08")])

    out = tmp_path / "out"
    session = DicomSession(persistence_file=db)
    try:
        session.redact_by_machine("SN-1", WHOLE_FRAME)
        session.export(str(out), format="dicom", use_compression=False)
    finally:
        session.close()

    exported = _exported(out)
    assert len(exported.ReferencedImageSequence) == 2
    for ref in exported.ReferencedImageSequence:
        assert "IconImageSequence" not in ref


# --- 3. Carried or reported, never both and never neither ----------------

def test_a_shifted_index_refuses_rather_than_writing_the_wrong_icon(tmp_path):
    """Position is the only identity a sequence item has.

    The path is recorded at ingest and resolved at export. Remove an earlier
    sibling in between and the path still *resolves* -- to the wrong item --
    so the icon would be written into a neighbour's descriptors, silently.
    A `DATA_LOSS` row is the honest outcome; a wrong icon is not.

    The check is the reshape the loader already performs against the
    resolved item's own descriptors, which is why the two icons here have
    different geometry: equal-geometry items are indistinguishable by it,
    and this test is about the detectable half.
    """
    db, _src = _ingest(
        tmp_path, "shift",
        referenced_icons=[_icon_item(bytes(range(16)), rows=4, cols=4),
                          _icon_item(b"\x09\x0a\x0b\x0c", rows=2, cols=2)])

    out = tmp_path / "out"
    session = DicomSession(persistence_file=db)
    try:
        inst = session.store.patients[0].studies[0].series[0].instances[0]
        # Remove Referenced Image Sequence item 0 after ingest. The 2x2
        # icon's path now names index 0, which holds the 4x4 icon's item.
        del inst.sequences[REF_IMAGE_SEQ].items[0]
        session.export(str(out), format="dicom", use_compression=False)
    finally:
        session.close()

    exported = _exported(out)
    assert len(exported.ReferencedImageSequence) == 1
    survivor = exported.ReferencedImageSequence[0]
    assert "IconImageSequence" not in survivor, (
        "an icon whose path resolved to a different item must not be written")
    assert [d for d, _s in _data_loss_rows(db) if "7fe0,0010" in d], \
        _data_loss_rows(db)


def test_an_item_removed_outright_files_a_loss_row_and_writes_nothing(
        tmp_path):
    """`resolve -> None` means "this item is gone", never "use the root".

    Writing an icon's pixels onto the instance would fabricate a top-level
    element that was never in the file, which is #57's defect exactly.
    """
    db, _src = _ingest(tmp_path, "removed", icons=[_icon_item()])

    out = tmp_path / "out"
    session = DicomSession(persistence_file=db)
    try:
        inst = session.store.patients[0].studies[0].series[0].instances[0]
        del inst.sequences[ICON_SEQ]
        session.export(str(out), format="dicom", use_compression=False)
    finally:
        session.close()

    exported = _exported(out)
    assert "IconImageSequence" not in exported
    assert exported.PixelData == np.arange(16, dtype=np.uint8).tobytes(), (
        "the icon's bytes must not be fabricated onto the instance")
    assert [d for d, _s in _data_loss_rows(db) if "7fe0,0010" in d], \
        _data_loss_rows(db)


def test_an_rle_encapsulated_icon_is_decoded_and_written_raw(tmp_path):
    """Decode at ingest, store raw, write raw -- one rule for both depths.

    Verbatim carriage of the element value is wrong and must not be
    implemented: the export writes Implicit VR Little Endian, so 90 bytes of
    encapsulated fragments would produce an icon no reader can decode, under
    a transfer syntax that says there are no fragments. The top-level path
    already decodes at ingest for exactly this reason.
    """
    src = tmp_path / "src"
    src.mkdir()
    ds = pydicom.dcmread(_write_src(str(src), icons=[_icon_item()]))

    # The icon first, while the borrowed `file_meta` still says
    # ExplicitVRLittleEndian: pydicom refuses to compress a dataset whose
    # transfer syntax is already encapsulated, and `ds.compress` rewrites
    # the very `file_meta` the icon has to borrow.
    icon = ds.IconImageSequence[0]
    icon.file_meta = FileMetaDataset()
    icon.file_meta.TransferSyntaxUID = ExplicitVRLittleEndian
    icon.compress(RLELossless)
    del icon.file_meta

    ds.compress(RLELossless)
    ds.save_as(os.path.join(str(src), "one.dcm"), enforce_file_format=True)

    db = str(tmp_path / "rle.db")
    session = DicomSession(persistence_file=db)
    try:
        session.ingest(str(src))
        session.save()
    finally:
        session.close()

    out = tmp_path / "out"
    _export(db, out)
    exported = _exported(out)

    assert exported.file_meta.TransferSyntaxUID == "1.2.840.10008.1.2"
    assert exported.IconImageSequence[0].PixelData == ICON_BYTES


def test_a_lossy_source_keeps_its_loss_row_and_carries_nothing(tmp_path):
    """Refused at ingest, because the decode changes what the file declares.

    A lossy-JPEG icon decodes to RGB from a declared `YBR_FULL_422`, so
    carrying it means rewriting the exported item's Photometric
    Interpretation to match the bytes -- a correctness claim with no
    measurement behind it, since no encoder plugin exists here to build such
    a fixture and observe the result. The refusal is by allow-list rather
    than by a list of the lossy syntaxes: a deny-list is wrong the moment
    the standard adds one, and it is wrong in the direction that ships
    pixels.

    The fixture carries no top-level Pixel Data on purpose. With one, the
    missing decoder would fail the whole ingest at the top level and the
    nested candidate would never be reached.
    """
    db, _src = _ingest(tmp_path, "lossy", icons=[_icon_item()],
                       transfer_syntax=JPEGBaseline8Bit,
                       top_level_pixels=False)

    assert not [k for k in _blob_kinds(db) if k.startswith("pixels:")]
    assert [d for d, _s in _data_loss_rows(db) if "7fe0,0010" in d], \
        _data_loss_rows(db)

    out = tmp_path / "out"
    _export(db, out)
    assert "PixelData" not in _exported(out).IconImageSequence[0]


def test_the_carriable_transfer_syntaxes_are_the_uids_pydicom_names(tmp_path):
    """The allow-list is UID strings, so it needs checking against pydicom.

    Written as strings rather than `pydicom.uid` names because the names are
    not stable -- a draft of #183's spec cited
    `JPEGLossyCompressedPixelTransferSyntaxes`, which does not exist in
    pydicom 3.0.2 -- but a typo in a UID would silently refuse every file of
    that syntax, which reads exactly like "this codec is not supported".
    """
    from pydicom import uid

    for name in ("ImplicitVRLittleEndian", "ExplicitVRLittleEndian",
                 "DeflatedExplicitVRLittleEndian", "ExplicitVRBigEndian",
                 "RLELossless", "JPEGLossless", "JPEGLosslessSV1",
                 "JPEGLSLossless", "JPEG2000Lossless", "HTJ2KLossless",
                 "HTJ2KLosslessRPCL"):
        assert str(getattr(uid, name)) in _CARRIABLE_TRANSFER_SYNTAXES, name

    for name in ("JPEGBaseline8Bit", "JPEGExtended12Bit",
                 "JPEGLSNearLossless", "JPEG2000", "HTJ2K"):
        assert str(getattr(uid, name)) not in _CARRIABLE_TRANSFER_SYNTAXES, \
            name


# --- 4. Both export paths ------------------------------------------------

def test_write_tree_carries_the_icon_too(tmp_path):
    """`session.export()` and `DicomExporter.write_tree()` agree.

    A post-pass on one path only is the divergence `tests/test_api_coherence
    .py` exists to catch, and it catches it by comparing trees rather than
    contents -- so an icon written by one path and not the other would slip
    past it. Asserted here directly.
    """
    db, _src = _ingest(tmp_path, "writetree", icons=[_icon_item()])

    out = tmp_path / "out"
    session = DicomSession(persistence_file=db)
    try:
        patient = session.store.patients[0]
        DicomExporter.write_tree(patient, str(out),
                                 studies=patient.studies)
    finally:
        session.close()

    assert _exported(out).IconImageSequence[0].PixelData == ICON_BYTES


def test_write_tree_honours_the_redaction_attestation(tmp_path):
    """The serializer skips the pipeline's gates; this is not one of them.

    `write_tree` applies no burned-in scan, no subset filter and no
    redaction zones -- but dropping an icon out of a graph that carries a
    redaction attestation is a property of carrying icon bytes at all, not a
    pipeline step. The configuration half of the condition is structurally
    unavailable here (there is no session), so the attestation half is what
    this path can see, and it must see it.
    """
    db, _src = _ingest(tmp_path, "writetreeredact", icons=[_icon_item()])

    out = tmp_path / "out"
    session = DicomSession(persistence_file=db)
    try:
        session.redact_by_machine("SN-1", WHOLE_FRAME)
        patient = session.store.patients[0]
        DicomExporter.write_tree(patient, str(out),
                                 studies=patient.studies)
    finally:
        session.close()

    assert "IconImageSequence" not in _exported(out)
