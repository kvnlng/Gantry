"""The 1.0 surface, pinned (#379, for #26).

`docs/api/stability.md` says what the tag promises, in three tiers. This
file holds the tier-1 half of that page still: the set of public
`Session` methods equals the frozen list in **both directions** -- a new
public method must be added to the freeze (a CHANGELOG entry and a row
on the page) or given a leading underscore, and a removed one is a 2.0
-- every parameter name and default matches, the frozen shapes' fields
match, and the API reference renders every frozen method.

**Why its own file, and not `tests/test_api_coherence.py`**, which #379
named as the natural home: that file is listed under `io_handlers.py`
in `scripts/mutation_probe.py`'s `TARGETS`, so every test in it re-runs
against every mutant of that module. A pure `inspect.signature` pin buys
zero kill signal there and costs on every mutant.
`tests/test_documented_api_exists.py` records the same reasoning for
#234. This file imports no probe target's module -- the return shapes it
pins are reached through `isocenter.session`, which binds them, or
through the facade itself -- and needs no `TARGETS` entry.

The literals below are transcribed from the spec's §5.3 table, as
amended for Q7 (`lock_identities` stripped of `_patient_obj` and
`**kwargs` before the tag). They are literals on purpose: a pin derived
from the code would be green on any code.
"""
import dataclasses
import inspect
import pathlib
import re

import pytest

import isocenter
from isocenter import session as session_module
from isocenter.discovery import DiscoveryResult
from isocenter.entities import (DicomItem, Equipment, Instance, Patient,
                                Series, Study)
from isocenter.session import DicomSession

REPO = pathlib.Path(__file__).resolve().parent.parent

_NO_DEFAULT = inspect.Parameter.empty

#: `name -> parameters`, spelled exactly as `docs/api/stability.md`'s
#: table spells them (`_spell` renders `inspect.signature` that way):
#: `self` omitted, defaults as `repr`, `*` before the first keyword-only
#: parameter, `**options` for the open export options. The kind is part
#: of the pin: a parameter moved across the `*` in either direction is a
#: different call and a red test, which `(name, default)` pairs missed.
FROZEN_SESSION_METHODS = {
    "ingest": "directory",
    "save": "sync=False",
    "close": "",
    "examine": "",
    "create_config": "output_path",
    "load_config": "config_file",
    "preview_config": "",
    "audit": "config_path=None",
    "auto_remediate_config": "report",
    "anonymize": "findings=None",
    "enable_reversible_anonymization": "key_path='isocenter.key'",
    "lock_identities": "patient_id, persist=False, *, verbose=True, tags_to_lock=None",
    "lock_identities_batch": (
        "patient_ids, auto_persist_chunk_size=0, tags_to_lock=None, "
        "*, persist=False, verbose=True"),
    "recover_patient_identity": "patient_id, restore=True",
    "redact": "show_progress=True, force=False",
    "redact_by_machine": "serial_number, roi",
    "scan_pixel_content": "serial_number=None",
    "discover_redaction_zones": "serial_number, sample_size=50, min_confidence=80.0",
    "reconcile_private_tags": "",
    "export": "folder, format='dicom', **options",
    "export_dataframe": "output_path='export_metadata.csv', expand_metadata=False, patient_ids=None",
    "get_cohort_report": "expand_metadata=False, patient_ids=None",
    "phi_status_summary": "",
    "generate_report": "output_path, format='markdown'",
    "generate_manifest": "output_path, format='html'",
    "save_analysis": "report",
    "compact": "",
    "release_memory": "",
}

#: `export(format="dicom", **options)`: the names `_export_dicom` accepts
#: after `folder`. `**options` hides them from the pin above, so they
#: are pinned here (spec §9 item 6).
FROZEN_DICOM_EXPORT_OPTIONS = (
    "use_compression=True, check_burned_in=False, check_reversibility=True, "
    "patient_ids=None, show_progress=True, subset=None, verify_readback=False")

#: The `Instance` fields that are frozen (`pixel_array` and
#: `waveform_array` are public fields too, and tier 2).
FROZEN_INSTANCE_FIELDS = [
    "sop_instance_uid", "sop_class_uid", "instance_number", "file_path",
    "source_path", "attributes", "sequences", "attribute_vrs", "date_shifted"]

FROZEN_ALL = ["Session", "Builder", "Equipment", "RedactionError", "ExportError"]

#: Words that reach users through tier-1 outputs (spec Q9): never renamed
#: or removed in 1.x. Pinned as literals the package still spells.
FROZEN_ACTION_TYPES = [
    "DATA_LOSS", "ERROR", "EXPORT", "RECONCILE_PRIVATE", "REDACTION",
    "REMOVE_TAG", "REPLACE_TAG", "REVERSIBLE_EXPORT", "RISK", "SCAN_GAP",
    "SHIFT_DATE", "WARNING", "COMPLIANCE_CHECK",
]
FROZEN_LOSS_SCOPES = ["STANDARD", "PRIVATE", "SIGNAL"]
FROZEN_GRADES = ["PASS", "REVIEW_REQUIRED"]


def _spell(method):
    """`inspect.signature(method)` rendered the way the stability page's
    table spells it: `self` dropped, `name` or `name=<repr>`, `/` after
    positional-only parameters, `*` before the first keyword-only one
    (`*args` plays that role when present), `**name` last."""
    kinds = inspect.Parameter
    out = []
    params = list(inspect.signature(method).parameters.values())[1:]
    star_seen = False
    for i, param in enumerate(params):
        if param.kind is kinds.KEYWORD_ONLY and not star_seen:
            out.append("*")
            star_seen = True
        if param.kind is kinds.VAR_POSITIONAL:
            out.append(f"*{param.name}")
            star_seen = True
        elif param.kind is kinds.VAR_KEYWORD:
            out.append(f"**{param.name}")
        elif param.default is _NO_DEFAULT:
            out.append(param.name)
        else:
            out.append(f"{param.name}={param.default!r}")
        if param.kind is kinds.POSITIONAL_ONLY and (
                i + 1 == len(params) or params[i + 1].kind is not kinds.POSITIONAL_ONLY):
            out.append("/")
    return ", ".join(out)


def _signature_rows(page: str) -> dict:
    """`docs/api/stability.md`'s Session table as `name -> params`.

    The duplicate check is here and not in the caller on purpose.
    `dict()` keeps the *last* match for a repeated key, so a false row
    placed above the true one leaves the page carrying a wrong signature
    with the pin green (#401, measured on 0.9.4: `6 passed`). Asserting
    on the pairs before the dict exists is the only place the second row
    is still visible -- the natural one-liner
    `len(rows) == len(FROZEN_SESSION_METHODS)` is green *with* the
    duplicate present, because `dict()` collapsed the two rows before
    anything counted them.

    Row *order* stays unpinned: the comparison is a dict and the page
    claims no order, so two swapped rows are green and correctly so.
    """
    pairs = re.findall(r"^\| `(\w+)` \| (?:`([^`]*)`|—) \|$", page, re.MULTILINE)
    names = [name for name, _ in pairs]
    duplicated = sorted({n for n in names if names.count(n) > 1})
    assert not duplicated, (
        f"stability.md's Session table repeats {duplicated}; `dict()` keeps "
        f"the last, so a false row above the true one would be invisible")
    return dict(pairs)


def _frozen_instance_fields_in_dataclass_order():
    return [f for f in _public_fields(Instance) if f in FROZEN_INSTANCE_FIELDS]


def _public_fields(cls):
    return [f.name for f in dataclasses.fields(cls) if not f.name.startswith("_")]


def test_the_frozen_session_surface_is_exactly_this():
    """T-F1: the method set, both directions, and every parameter.

    Killing mutations: any public method renamed, added or removed; any
    parameter renamed, reordered, or given a different default.
    """
    public = {n for n in vars(DicomSession) if not n.startswith("_")}
    assert public == set(FROZEN_SESSION_METHODS), (
        f"new public names must be frozen (a row here and on "
        f"docs/api/stability.md) or underscored; missing from the freeze: "
        f"{sorted(public - set(FROZEN_SESSION_METHODS))}; frozen but gone "
        f"(a 2.0): {sorted(set(FROZEN_SESSION_METHODS) - public)}")

    for name, params in FROZEN_SESSION_METHODS.items():
        assert _spell(getattr(DicomSession, name)) == params, (
            f"Session.{name}'s parameters changed (name, order, default or "
            f"kind); that is a 2.0")

    assert _spell(DicomSession._export_dicom).split(", ", 1) == [
        "folder", FROZEN_DICOM_EXPORT_OPTIONS], (
        "export(format='dicom', **options) accepts different option names")

    assert list(isocenter.__all__) == FROZEN_ALL
    assert isocenter.Session is DicomSession
    assert isinstance(isocenter.__version__, str) and isocenter.__version__
    assert callable(isocenter.Builder.start_patient)


def test_the_frozen_shapes_have_these_fields(tmp_path):
    """T-F2: the return shapes, the entity graph, the two exceptions.

    `IngestSummary` is reached through the facade -- `ingest()` on an
    empty directory returns one before any pool starts -- because the
    class is bound only in `io_handlers`, a probe target this file must
    not name. Killing mutation: any field renamed.
    """
    (tmp_path / "empty").mkdir()
    with DicomSession(str(tmp_path / "shapes.db")) as session:
        summary = session.ingest(str(tmp_path / "empty"))
    assert _public_fields(type(summary)) == ["ingested", "failures", "declined", "skipped"]
    assert hasattr(summary, "failed")

    assert _public_fields(session_module.ExportSummary) == ["written_uids", "failures"]
    for prop in ("written", "failed"):
        assert isinstance(getattr(session_module.ExportSummary, prop), property)

    assert _public_fields(session_module.PhiFinding) == [
        "entity_uid", "entity_type", "field_name", "value", "reason", "tag",
        "patient_id", "entity", "remediation_proposal", "metadata", "entity_path"]
    for dunder in ("__len__", "__iter__", "__getitem__"):
        assert dunder in vars(session_module.PhiReport)
    assert callable(session_module.PhiReport.to_dataframe)

    for method in ("filter", "to_zones", "to_dataframe"):
        assert callable(getattr(DiscoveryResult, method))
    assert issubclass(session_module.LockingResult, list)

    assert _public_fields(Equipment) == ["manufacturer", "model_name", "device_serial_number"]
    assert _public_fields(Patient) == ["patient_id", "patient_name", "studies"]
    assert _public_fields(Study) == [
        "study_instance_uid", "study_date", "series", "date_shifted", "study_time"]
    assert _public_fields(Series) == [
        "series_instance_uid", "modality", "series_number", "equipment", "instances"]
    # A superset, not equality: `pixel_array` and `waveform_array` are
    # public fields of `Instance` and tier 2 (stability.md).
    assert set(FROZEN_INSTANCE_FIELDS) <= set(_public_fields(Instance))
    # Which of them a constructor call can set is part of the shape: the
    # page used to write `Instance(..., attributes, sequences,
    # attribute_vrs, date_shifted)`, and none of those four is an
    # argument of `__init__`.
    init = {f.name: f.init for f in dataclasses.fields(Instance)}
    assert [n for n in FROZEN_INSTANCE_FIELDS if init[n]] == [
        "sop_instance_uid", "sop_class_uid", "instance_number", "file_path", "source_path"]
    assert [n for n in FROZEN_INSTANCE_FIELDS if not init[n]] == [
        "attributes", "sequences", "attribute_vrs", "date_shifted"]
    for method in ("get_pixel_data", "set_pixel_data", "unload_pixel_data",
                   "discard_pixel_data", "get_waveform_data"):
        assert callable(getattr(Instance, method))
    assert callable(DicomItem.set_attr)

    config = session_module.IsocenterConfiguration
    for method in ("save", "add_rule", "update_rule", "delete_rule",
                   "set_phi_tag", "get_rule"):
        assert callable(getattr(config, method))
    assert {"rules", "phi_tags", "date_jitter", "remove_private_tags",
            "privacy_profile"} <= set(_public_fields(config))

    assert issubclass(isocenter.RedactionError, RuntimeError)
    assert list(inspect.signature(isocenter.RedactionError.__init__).parameters) == [
        "self", "failures", "attempted"]
    assert issubclass(isocenter.ExportError, RuntimeError)
    assert list(inspect.signature(isocenter.ExportError.__init__).parameters) == [
        "self", "failures", "attempted", "folder"]


def test_the_api_reference_renders_every_frozen_session_method():
    """T-F3: `docs/api/session.md`'s `members:` block is a superset of the freeze.

    **Red on 0.9.3**: the page listed 16 of the 28, omitting `anonymize`,
    `lock_identities`, `enable_reversible_anonymization`,
    `recover_patient_identity`, `generate_report`, `export_dataframe` and
    six more -- half the pipeline the README teaches. Killing mutation: a
    method removed from `members:`.
    """
    page = (REPO / "docs" / "api" / "session.md").read_text(encoding="utf-8")
    block = page.split("members:", 1)[1]
    rendered = set(re.findall(r"^\s+-\s+(\w+)\s*$", block, re.M))

    missing = set(FROZEN_SESSION_METHODS) - rendered
    assert not missing, (
        f"docs/api/session.md does not render these frozen methods: "
        f"{sorted(missing)}")


def test_the_stability_page_names_every_tier_one_session_method():
    """T-F4: the page users read names each frozen method, and mkdocs renders it.

    One direction only -- the page may name more. `mkdocs.yml`'s nav must
    list it, because `tests/test_doc_anchors.py` renders nav pages and an
    unlisted page is unchecked.
    """
    page = (REPO / "docs" / "api" / "stability.md").read_text(encoding="utf-8")
    unnamed = [name for name in FROZEN_SESSION_METHODS if f"`{name}`" not in page]
    assert not unnamed, f"docs/api/stability.md does not name {unnamed}"

    # The page's table *is* the pin, row for row: a `| `name` | `params` |`
    # row per method, `—` for no parameters.
    rows = _signature_rows(page)
    assert rows == FROZEN_SESSION_METHODS, (
        "stability.md's Session table and the pins disagree: "
        f"{ {k: (rows.get(k), v) for k, v in FROZEN_SESSION_METHODS.items() if rows.get(k) != v} }")
    # Prose wraps at 72 columns; a list of names may cross a line break.
    flat = " ".join(page.split())
    assert f"`{FROZEN_DICOM_EXPORT_OPTIONS}`" in flat, "the dicom export options on the page moved"

    # Entity fields as the page lists them: dataclass order, which is
    # `dataclasses.fields` order, not the order someone remembers.
    for cls in (Equipment, Patient, Study, Series):
        assert f"`{', '.join(_public_fields(cls))}`" in flat, (
            f"stability.md does not list {cls.__name__}'s fields in dataclass order: "
            f"{_public_fields(cls)}")
    ordered = _frozen_instance_fields_in_dataclass_order()
    assert ordered == ["attributes", "sequences", "attribute_vrs", "sop_instance_uid",
                       "sop_class_uid", "instance_number", "file_path", "source_path",
                       "date_shifted"], ordered
    for group in (ordered[:3], ordered[3:8], ordered[8:]):
        assert f"`{', '.join(group)}`" in flat, f"stability.md does not list {group} together"

    for word in FROZEN_ACTION_TYPES + FROZEN_LOSS_SCOPES + FROZEN_GRADES:
        assert f"`{word}`" in page, f"stability.md does not list the vocabulary word {word}"

    nav = (REPO / "mkdocs.yml").read_text(encoding="utf-8")
    assert "api/stability.md" in nav, "docs/api/stability.md is not in mkdocs.yml's nav"


def test_a_duplicate_signature_row_is_not_silently_collapsed():
    """T-F4's parser must see a second row for a method, not keep the last.

    The defect this pins (#401): `dict(re.findall(...))` keeps the last
    match, so a false `| `save` | `wrong=True` |` row inserted *above*
    the true one left this file at `6 passed` while the page a user
    reads carried a signature the code does not have.

    The clean half is not decoration: a `_signature_rows` that raised
    unconditionally would satisfy the `pytest.raises` half alone, and a
    duplicate-detector that rejects every page detects nothing.
    """
    clean = "| `save` | `sync=False` |\n| `close` | — |\n"
    assert _signature_rows(clean) == {"save": "sync=False", "close": ""}

    duplicated = "| `save` | `wrong=True` |\n| `save` | `sync=False` |\n"
    with pytest.raises(AssertionError, match="repeats"):
        _signature_rows(duplicated)


def test_the_output_vocabularies_are_still_spelled_by_the_package():
    """Q9: an existing grade, `action_type` or `loss_scope` string is never renamed.

    Pinned as quoted literals somewhere under `isocenter/` -- the
    cheapest honest check that the word the report prints is still the
    word the code writes. Killing mutation: any of them respelled.
    """
    source = "\n".join(p.read_text(encoding="utf-8")
                       for p in (REPO / "isocenter").rglob("*.py"))
    for word in FROZEN_ACTION_TYPES + FROZEN_LOSS_SCOPES + FROZEN_GRADES:
        assert f'"{word}"' in source or f"'{word}'" in source, (
            f"the package no longer spells {word!r} as a literal")


def test_the_two_pass_behaviours_are_stated_as_contract():
    """T-F5: the #368 behaviours are written where a user reads them.

    Weak by design: it pins that the promise is *stated* in the three
    docstrings, as the #368 CHANGELOG entry says it is; the behaviour
    itself is pinned by `tests/test_compact_refuses_during_a_pass.py`.
    """
    for name in ("compact", "redact", "ingest"):
        doc = inspect.getdoc(getattr(DicomSession, name)) or ""
        assert "RuntimeError" in doc, f"Session.{name}'s docstring does not name RuntimeError"
    assert "pass" in inspect.getdoc(DicomSession.compact)
