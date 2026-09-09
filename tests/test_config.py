import pytest
import json
import os
from isocenter.config_manager import ConfigLoader

def test_load_valid_config(tmp_path):
    data = {
        "version": "1.0",
        "machines": [{"serial_number": "SN1", "redaction_zones": []}]
    }
    p = tmp_path / "valid.yaml"
    import yaml
    p.write_text(yaml.dump(data))

    rules = ConfigLoader.load_redaction_rules(str(p))
    assert len(rules) == 1
    assert rules[0]["serial_number"] == "SN1"

def test_missing_file():
    # Verify FileNotFoundError is raised
    with pytest.raises(FileNotFoundError):
        ConfigLoader.load_redaction_rules("/non/existent/path.yaml")

def test_invalid_yaml(tmp_path):
    p = tmp_path / "bad.yaml"
    p.write_text("unclosed: { brace")

    with pytest.raises(ValueError, match="Invalid YAML"):
        ConfigLoader.load_redaction_rules(str(p))

def test_validation_logic(tmp_path):
    import yaml
    # 1. Missing SN
    data = {"machines": [{"redaction_zones": []}]}
    p = tmp_path / "bs1.yaml"
    p.write_text(yaml.dump(data))
    with pytest.raises(ValueError, match="Missing 'serial_number'"):
        ConfigLoader.load_redaction_rules(str(p))

    # 2. Invalid ROI Type
    data = {"machines": [{"serial_number": "S", "redaction_zones": [{"roi": "bad"}]}]}
    p = tmp_path / "bs2.yaml"
    p.write_text(yaml.dump(data))
    with pytest.raises(ValueError, match="ROI must be a list"):
        ConfigLoader.load_redaction_rules(str(p))

    # 3. Invalid ROI Range (Start > End)
    data = {"machines": [{"serial_number": "S", "redaction_zones": [{"roi": [10, 5, 0, 10]}]}]}
    p = tmp_path / "bs3.yaml"
    p.write_text(yaml.dump(data))
    with pytest.raises(ValueError, match="Invalid ROI logic"):
        ConfigLoader.load_redaction_rules(str(p))

def test_phi_config_default():
    # The positive control for #388, and kept as-is on purpose. In a
    # correct checkout the shipped `phi_tags.json` is present, parseable,
    # and returns a mapping; without this assertion,
    # `tests/test_shipped_resource_is_required.py`'s negative test proves
    # only that *something* raises. Converting this one to
    # `pytest.raises` would assert the same refusal in two files, which is
    # the duplicate spelling this project deletes on sight -- the refusal
    # when the resource is absent is that file's
    # `test_a_missing_phi_tag_policy_refuses_instead_of_auditing_against_nothing`.
    tags = ConfigLoader.load_phi_config(None)
    assert isinstance(tags, dict)

def test_phi_config_override(tmp_path):
    data = {"phi_tags": {"0010,0010": "PatientName"}}
    p = tmp_path / "phi.yaml"
    import yaml
    p.write_text(yaml.dump(data))

    tags = ConfigLoader.load_phi_config(str(p))
    assert "0010,0010" in tags
    assert tags["0010,0010"] == "PatientName"
