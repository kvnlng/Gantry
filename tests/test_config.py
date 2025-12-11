import pytest
import json
import os
from gantry.config_manager import ConfigLoader

def test_load_valid_config(tmp_path):
    data = {
        "version": "1.0",
        "machines": [{"serial_number": "SN1", "redaction_zones": []}]
    }
    p = tmp_path / "valid.json"
    p.write_text(json.dumps(data))
    
    rules = ConfigLoader.load_redaction_rules(str(p))
    assert len(rules) == 1
    assert rules[0]["serial_number"] == "SN1"

def test_missing_file():
    # Verify FileNotFoundError is raised
    with pytest.raises(FileNotFoundError):
        ConfigLoader.load_redaction_rules("/non/existent/path.json")

def test_invalid_json(tmp_path):
    p = tmp_path / "bad.json"
    p.write_text("{ unclosed brace")
    
    with pytest.raises(ValueError, match="Invalid JSON"):
        ConfigLoader.load_redaction_rules(str(p))

def test_validation_logic(tmp_path):
    # 1. Missing SN
    data = {"machines": [{"redaction_zones": []}]}
    p = tmp_path / "bs1.json"
    p.write_text(json.dumps(data))
    with pytest.raises(ValueError, match="Missing 'serial_number'"):
        ConfigLoader.load_redaction_rules(str(p))
        
    # 2. Invalid ROI Type
    data = {"machines": [{"serial_number": "S", "redaction_zones": [{"roi": "bad"}]}]}
    p = tmp_path / "bs2.json"
    p.write_text(json.dumps(data))
    with pytest.raises(ValueError, match="ROI must be a list"):
        ConfigLoader.load_redaction_rules(str(p))

    # 3. Invalid ROI Range (Start > End)
    data = {"machines": [{"serial_number": "S", "redaction_zones": [{"roi": [10, 5, 0, 10]}]}]}
    p = tmp_path / "bs3.json"
    p.write_text(json.dumps(data))
    with pytest.raises(ValueError, match="Invalid ROI logic"):
        ConfigLoader.load_redaction_rules(str(p))

def test_phi_config_default():
    # Calling with None should attempt to load default.
    # We can't easily assert content unless we know it, but it shouldn't crash.
    tags = ConfigLoader.load_phi_config(None)
    assert isinstance(tags, dict)

def test_phi_config_override(tmp_path):
    data = {"phi_tags": {"0010,0010": "PatientName"}}
    p = tmp_path / "phi.json"
    p.write_text(json.dumps(data))
    
    tags = ConfigLoader.load_phi_config(str(p))
    assert "0010,0010" in tags
    assert tags["0010,0010"] == "PatientName"
