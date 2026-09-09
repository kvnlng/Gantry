"""A missing shipped resource is a broken install, and now says so (#388).

Three loaders returned an empty collection when a file the package
*ships* was not on disk: `_load_redaction_knowledge_base` and
`_load_ctp_rules` in `session.py`, and `ConfigLoader.load_phi_config` in
`config_manager.py`. Measured before-state, with
`session.RESOURCES_DIR` pointed at an empty directory and one instance
ingested whose `DeviceSerialNumber` is a serial the shipped knowledge
base names:

    --- WITH the resource, scaffolded config:
    machines:
    - serial_number: SN-SCANNER-01
      redaction_zones: [{roi: [50, 100, 50, 200], ...}]
    --- WITHOUT the resource, scaffolded config:
    machines:
    - serial_number: SN-SCANNER-01
      redaction_zones: []
    --- log lines while the resource was missing: []
    --- audit rows while missing: []

A config instructing the pipeline to redact nothing, written without a
single log line or audit row. The `load_phi_config` version is louder
still: `publish.yml`'s own comment says a wheel without `phi_tags.json`
"audited against an empty PHI tag list and reported clean", which is the
worst thing a de-identification tool can do quietly.

**Raise, not warn** (#400's ruling, which this reads as one with). A
warning in front of a run that then succeeds is a line nobody reads, and
there is nothing to *annotate*, because a missing shipped resource is
never a correct state. CLAUDE.md's degrade-gracefully rule is about the
optional extras (`ocr`, `nlp`, `docs`); a shipped package resource is the
opposite kind of thing -- `setup.py`'s `package_data` promises it and
`publish.yml` refuses to release a wheel without it, so this is the
runtime half of a promise CI already makes. **No audit row is written**,
on the same ground.

**`RuntimeError`, not `FileNotFoundError`**, for two reasons.
`ConfigLoader._load_yaml` already raises `FileNotFoundError` for a
*user's* config file, which is a different failure with a different
remedy, and a caller writing `except FileNotFoundError` around
`load_config` would silently swallow "your install is broken". The
sharper reason is that `_load_redaction_knowledge_base`'s own handler is
`except (OSError, json.JSONDecodeError)` and `FileNotFoundError` **is** an
`OSError` -- a raise that drifted inside that `try` would be caught and
turned straight back into `return []`, with every test still green. The
raise sits before the `try`, and
`test_the_refusal_is_not_swallowed_by_the_loaders_own_handler` is what
kills that pair of edits.

Two things deliberately unchanged. A resource that is **present but
unreadable or malformed** keeps its warning-and-empty-collection: that is
a different failure with a different remedy and is out of scope here.
And `ctp_rules.yaml`'s absence is still legitimate -- it is deliberately
not shipped and its absence is the ordinary case, so only the JSON
fallback goes through the helper.
"""
import os

import pytest

import isocenter.config_manager as config_manager
import isocenter.session as session_module
from isocenter.config_manager import ConfigLoader
from isocenter.session import DicomSession

#: The consequence clause each call site supplies. A refusal must be
#: accurate rather than generic (§11.3), and these are the three anchors
#: -- with the basename and the searched path -- that make each test pin
#: the speech rather than match any string containing "missing".
CONSEQUENCES = {
    "redaction_rules.json": "scanned every frame with no machine redaction rules",
    "ctp_rules.json": "matched no CTP de-identification rules",
    # `publish.yml`'s own wording for the same failure, so the runtime
    # refusal and the release gate say the same thing about the file.
    "phi_tags.json": "audited against an empty PHI tag list",
}


def _write_src(folder):
    """One instance whose serial the shipped knowledge base names."""
    import numpy as np
    from pydicom.dataset import FileDataset, FileMetaDataset
    from pydicom.uid import ExplicitVRLittleEndian, generate_uid

    meta = FileMetaDataset()
    meta.MediaStorageSOPClassUID = "1.2.840.10008.5.1.4.1.1.7"
    meta.MediaStorageSOPInstanceUID = generate_uid()
    meta.TransferSyntaxUID = ExplicitVRLittleEndian

    ds = FileDataset(None, {}, file_meta=meta, preamble=b"\0" * 128)
    ds.PatientID, ds.PatientName = "PAT388", "DOE^JOHN"
    ds.StudyInstanceUID, ds.SeriesInstanceUID = generate_uid(), generate_uid()
    ds.SOPInstanceUID = meta.MediaStorageSOPInstanceUID
    ds.SOPClassUID = meta.MediaStorageSOPClassUID
    ds.Modality, ds.SeriesNumber, ds.InstanceNumber = "CT", 1, 1
    ds.StudyDate = "20230101"
    ds.Manufacturer = "GE MEDICAL SYSTEMS"
    ds.ManufacturerModelName = "Revolution CT"
    ds.DeviceSerialNumber = "SN-SCANNER-01"

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


def _assert_three_anchors(message, basename, directory):
    """The basename, the resolved path, and the consequence clause.

    Three anchors rather than one, because a one-word match like
    `"missing"` is satisfiable by half the strings in this codebase and
    would pass against a refusal that named the wrong file.
    """
    assert basename in message, f"the refusal does not name the file: {message}"
    assert os.path.join(directory, basename) in message, (
        f"the refusal does not say where it looked: {message}")
    assert CONSEQUENCES[basename] in message, (
        f"the refusal does not say what continuing would have done: {message}")


# ---------------------------------------------------------------------------
# session.py's two loaders
# ---------------------------------------------------------------------------

def test_a_missing_redaction_knowledge_base_refuses_instead_of_returning_empty(
        tmp_path, monkeypatch):
    """Red when `return []` is restored."""
    monkeypatch.setattr(session_module, "RESOURCES_DIR", str(tmp_path))

    with pytest.raises(RuntimeError) as excinfo:
        session_module._load_redaction_knowledge_base()

    _assert_three_anchors(str(excinfo.value), "redaction_rules.json",
                          str(tmp_path))


def test_the_refusal_is_not_swallowed_by_the_loaders_own_handler(
        tmp_path, monkeypatch):
    """The two-part mutation this kills, named so it is not kept for looks.

    `_load_redaction_knowledge_base` ends in
    `except (OSError, json.JSONDecodeError): ... return []`, and
    `FileNotFoundError` **is** an `OSError`. So a refusal that was both
    moved inside that `try` *and* respelled as `FileNotFoundError` would
    be caught by the loader's own handler and turned straight back into
    the empty list this issue is about -- with every other test in this
    file still green, because they only assert that *something* raises.

    This test is kept as a separate test rather than folded into T1
    because it asserts a different thing: not that a `RuntimeError` is
    raised, but that **no** exception the handler catches is, and that
    the handler did not run. Both halves are asserted; either edit alone
    leaves one of them true.
    """
    import json

    monkeypatch.setattr(session_module, "RESOURCES_DIR", str(tmp_path))

    with pytest.raises(RuntimeError) as excinfo:
        session_module._load_redaction_knowledge_base()

    assert not isinstance(excinfo.value, (OSError, json.JSONDecodeError)), (
        "the refusal is of a type the loader's own `except` clause catches, "
        "so moving it inside the `try` would silently restore `return []`")

    # The handler's own message is what a swallowed-and-rewarned refusal
    # would produce; its absence says the `try` was never entered.
    assert "Could not read the redaction knowledge base" not in str(excinfo.value)


def test_a_missing_ctp_json_is_a_broken_install(tmp_path, monkeypatch):
    """Red when the JSON path keeps its `return []`."""
    monkeypatch.setattr(session_module, "RESOURCES_DIR", str(tmp_path))

    with pytest.raises(RuntimeError) as excinfo:
        session_module._load_ctp_rules()

    _assert_three_anchors(str(excinfo.value), "ctp_rules.json", str(tmp_path))


def test_a_missing_ctp_yaml_is_not_a_broken_install(tmp_path, monkeypatch):
    """`ctp_rules.yaml` is deliberately not shipped; its absence is ordinary.

    Red when the YAML `exists` check is routed through the helper too --
    the over-eager version of this fix, which would make every correct
    installation a broken one.
    """
    import json

    (tmp_path / "ctp_rules.json").write_text(
        json.dumps({"rules": [{"manufacturer": "ACME"}]}), encoding="utf-8")
    (tmp_path / "redaction_rules.json").write_text(
        json.dumps({"machines": [{"serial_number": "SN-1"}]}), encoding="utf-8")
    assert not (tmp_path / "ctp_rules.yaml").exists(), "fixture planted a YAML"

    monkeypatch.setattr(session_module, "RESOURCES_DIR", str(tmp_path))

    assert session_module._load_ctp_rules() == [{"manufacturer": "ACME"}]
    assert session_module._load_redaction_knowledge_base() == [
        {"serial_number": "SN-1"}]


# ---------------------------------------------------------------------------
# config_manager.py's third loader
# ---------------------------------------------------------------------------

def test_a_missing_phi_tag_policy_refuses_instead_of_auditing_against_nothing(
        tmp_path, monkeypatch):
    """The loudest of the three silences.

    An empty PHI tag list makes `audit()` report success on data full of
    PHI. Red when `return {}` is restored.

    The monkeypatch targets `config_manager.RESOURCES_DIR`, which #388
    hoisted out of `load_phi_config`'s body. Patching the `filepath`
    argument instead would exercise the *user-config* branch and never
    enter the arm under test.
    """
    monkeypatch.setattr(config_manager, "RESOURCES_DIR", str(tmp_path))

    with pytest.raises(RuntimeError) as excinfo:
        ConfigLoader.load_phi_config(None)

    _assert_three_anchors(str(excinfo.value), "phi_tags.json", str(tmp_path))


def test_the_default_phi_policy_refuses_through_the_scan_machinery(
        tmp_path, monkeypatch):
    """`PhiInspector()` with no policy is the arm `privacy.py:161` takes.

    This is where the empty-tag-list silence actually lands: an inspector
    built with neither `config_tags` nor `config_path` loads the shipped
    default, and a `{}` there is a scan that finds nothing and a run that
    reports clean.

    A note for the next reader, because the brief this test was written
    from says otherwise: **`session.audit()` does not reach this loader.**
    It passes `config_tags=self.configuration.phi_tags`, and `{}` is not
    `None`, so `PhiInspector.__init__` takes its first branch and the
    shipped default is never consulted. A bare session already audits
    against an empty policy and says so with its own
    `"PHI Scan Warning: No PHI tags defined"` -- a different question from
    this one, and not a silence.
    """
    from isocenter.privacy import PhiInspector

    monkeypatch.setattr(config_manager, "RESOURCES_DIR", str(tmp_path / "gone"))

    with pytest.raises(RuntimeError) as excinfo:
        PhiInspector()

    _assert_three_anchors(str(excinfo.value), "phi_tags.json",
                          str(tmp_path / "gone"))


def test_a_broken_phi_policy_refuses_where_a_handler_would_have_hidden_it(
        tmp_path, monkeypatch):
    """The speech, through the public API, past four `except` clauses.

    A session on a broken install can still be *constructed* -- none of
    the six `load_phi_config` call sites is `Session.__init__`, which is
    what keeps `docs/api/stability.md`'s prose about construction true --
    and then refuses at the first call that needs the policy. Here that is
    `create_config()`, whose `_scaffold_phi_tags` reads the default tag
    set.

    Four of the six sites sit inside `except (OSError, ValueError)`
    handlers that fall back to `{}` or warn. `RuntimeError` is neither, so
    it propagates -- deliberately, and that is the whole reason the
    exception is not a `FileNotFoundError`. Red when any of those handlers
    is widened to catch it, which would reproduce this exact bug one layer
    up. Only `config_manager`'s resources directory is broken here, so the
    redaction knowledge base loads and this is unambiguously the PHI
    policy's refusal.
    """
    src = tmp_path / "src"
    src.mkdir()
    _write_src(str(src))
    output_path = tmp_path / "phi_scaffold.yaml"

    monkeypatch.setattr(config_manager, "RESOURCES_DIR", str(tmp_path / "gone"))

    session = DicomSession(persistence_file=str(tmp_path / "phi.db"))
    try:
        session.ingest(str(src))
        with pytest.raises(RuntimeError) as excinfo:
            session.create_config(str(output_path))
    finally:
        session.close()

    _assert_three_anchors(str(excinfo.value), "phi_tags.json",
                          str(tmp_path / "gone"))
    assert not output_path.exists()


def test_a_broken_install_cannot_scaffold_a_config(tmp_path, monkeypatch):
    """The speech, through the public API, and nothing is written.

    Today this writes a config carrying `redaction_zones: []` and prints
    `Scaffolded Unified Config to ...`; the run then redacts nothing by
    serial and reports clean. Red when `return []` is restored.

    The `RuntimeError` surfaces from `create_config()` **unwrapped**: no
    domain exception class and no translation layer, because a broken
    install is not a domain condition and `__all__` is frozen at five
    names (§11.4).
    """
    src = tmp_path / "src"
    src.mkdir()
    _write_src(str(src))
    output_path = tmp_path / "scaffold.yaml"

    monkeypatch.setattr(session_module, "RESOURCES_DIR",
                        str(tmp_path / "gone"))

    session = DicomSession(persistence_file=str(tmp_path / "scaffold.db"))
    try:
        session.ingest(str(src))
        with pytest.raises(RuntimeError) as excinfo:
            session.create_config(str(output_path))
    finally:
        session.close()

    _assert_three_anchors(str(excinfo.value), "redaction_rules.json",
                          str(tmp_path / "gone"))
    assert not output_path.exists(), (
        "a config was written despite the broken install; a file that "
        "instructs the pipeline to redact nothing is worse than no file")


def test_the_helper_reads_the_directory_it_is_given(tmp_path):
    """The helper takes its directory as a parameter, not from a global.

    If it closed over `RESOURCES_DIR` at import, every monkeypatched test
    above would pass against the real source tree and this whole file
    would be decoration.
    """
    from isocenter.config_manager import require_package_resource

    with pytest.raises(RuntimeError) as excinfo:
        require_package_resource(str(tmp_path), "phi_tags.json",
                                 CONSEQUENCES["phi_tags.json"])
    _assert_three_anchors(str(excinfo.value), "phi_tags.json", str(tmp_path))

    (tmp_path / "phi_tags.json").write_text("{}", encoding="utf-8")
    assert require_package_resource(
        str(tmp_path), "phi_tags.json",
        CONSEQUENCES["phi_tags.json"]) == str(tmp_path / "phi_tags.json")
