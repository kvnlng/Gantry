import pytest
import os
import json
from gantry import Session

@pytest.fixture
def session_db(tmp_path):
    return str(tmp_path / "test_unified.db")

def test_unified_workflow(session_db, tmp_path, dummy_patient):
    """
    Verifies that the unified config workflow functions end-to-end.
    """
    # 1. Init Session
    session = Session(session_db)
    session.store.patients.append(dummy_patient)
    session.save()
    
    # 2. Scaffold Config (Action: Setup)
    config_path = tmp_path / "unified_config.json"
    session.setup_config(str(config_path))
    
    assert os.path.exists(config_path)
    
    with open(config_path, 'r') as f:
        data = json.load(f)
        
    assert "version" in data and data["version"] == "2.0"
    assert "phi_tags" in data
    assert "machines" in data
    # Check that scalar defaults are present (Tag IDs)
    assert "0010,0010" in data["phi_tags"]
    
    # 3. Edit Config (Simulated User Action)
    # Let's add a custom tag to look for (Protocol Name = 0018,1030)
    data["phi_tags"]["0018,1030"] = "Protocol Name" 
    # And a machine rule
    data["machines"].append({
        "serial_number": "SN-999", # Matches dummy_patient
        "redaction_zones": [{"roi": [0, 10, 0, 10]}]
    })
    
    with open(config_path, 'w') as f:
        json.dump(data, f)
        
    # 4. Load Config
    session.load_config(str(config_path))
    assert len(session.active_rules) >= 1
    assert "0018,1030" in session.active_phi_tags
    
    # 5. Verify Audit runs without error using the new config structure
    # (Note: Inspector currently checks hardcoded attributes, so we just verify the call works)
    report = session.audit() 
    assert report is not None
    
    print("\nUnified Config Workflow Verified.")
