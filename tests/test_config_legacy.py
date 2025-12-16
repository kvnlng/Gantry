
import pytest
import json
from gantry.session import DicomSession

def test_legacy_list_config_loading(tmp_path):
    """
    Regression Test: Ensures that loading a configuration file formatted 
    as a list of rules (Legacy/v1 format) does not crash the loader 
    and correctly interprets the list as machine rules.
    """
    # 1. Create a legacy config file (List at root)
    legacy_config_path = tmp_path / "legacy_config.yaml"
    legacy_config_data = [
        {
            "serial_number": "LEGACY_123", 
            "model_name": "Legacy Machine",
            "redaction_zones": []
        }
    ]
    with open(legacy_config_path, "w") as f:
        import yaml
        yaml.dump(legacy_config_data, f)
        
    # 2. Attempt to load
    session = DicomSession(":memory:")
    
    # This should NOT raise exception
    session.load_config(str(legacy_config_path))
    
    # 3. Verify it was loaded as rules
    assert len(session.active_rules) == 1
    assert session.active_rules[0]["serial_number"] == "LEGACY_123"
    
    # Verify other defaults were set safely
    assert isinstance(session.active_phi_tags, dict)
    assert session.active_phi_tags == {}
