
import pytest
import os
import json
from gantry.config_manager import load_unified_config, ConfigLoader

def test_load_basic_profile(tmp_path):
    # 1. Create config using "privacy_profile": "basic"
    config_path = tmp_path / "config_basic.json"
    with open(config_path, "w") as f:
        json.dump({
            "version": "2.0",
            "privacy_profile": "basic",
            # No user overrides
        }, f)
        
    # 2. Load via top-level
    config = load_unified_config(str(config_path))
    
    # 3. Verify Basic Profile Rules
    tags = config["phi_tags"]
    assert tags["0010,0010"]["action"] == "REMOVE" # Patient Name
    assert tags["0008,0020"]["action"] == "REMOVE" # Study Date
    
def test_profile_override(tmp_path):
    # 1. Create config using "basic" but override Patient Name to KEEP
    config_path = tmp_path / "config_override.json"
    with open(config_path, "w") as f:
        json.dump({
            "version": "2.0",
            "privacy_profile": "basic",
            "phi_tags": {
                "0010,0010": {"action": "KEEP", "name": "Patient Name (Kept)"},
                "0008,0090": {"action": "EMPTY"} # Referring Physician overridden to EMPTY (default REMOVE)
            }
        }, f)
        
    config = load_unified_config(str(config_path))
    tags = config["phi_tags"]
    
    # Verify Override
    assert tags["0010,0010"]["action"] == "KEEP"
    assert tags["0008,0090"]["action"] == "EMPTY"
    
    # Verify other profile tags (not overridden) still exist
    assert tags["0010,0020"]["action"] == "REMOVE" # Patient ID

def test_legacy_loader_integration(tmp_path):
    # Verify that ConfigLoader.load_unified_config returns tuple correctly wrapped
    config_path = tmp_path / "config_legacy_adapter.json"
    with open(config_path, "w") as f:
        json.dump({
            "privacy_profile": "basic"
        }, f)
        
    phi_tags, _, _, _ = ConfigLoader.load_unified_config(str(config_path))
    
    assert isinstance(phi_tags, dict)
    assert phi_tags["0010,0010"]["action"] == "REMOVE"

def test_unknown_profile(tmp_path):
    config_path = tmp_path / "config_unknown.json"
    with open(config_path, "w") as f:
        json.dump({"privacy_profile": "super_secret_profile"}, f)
        
    config = load_unified_config(str(config_path))
    # Should just ignore and load empty/rules
    assert config.get("phi_tags", {}) == {}
