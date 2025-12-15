
import pytest
import json
from gantry.session import DicomSession

def test_reproduce_config_load_error(tmp_path):
    # 1. Create a "bad" config file (List at root)
    bad_config_path = tmp_path / "research_config.json"
    bad_config_data = [
        {"serial_number": "123", "redaction_zones": []}
    ]
    with open(bad_config_path, "w") as f:
        json.dump(bad_config_data, f)
        
    # 2. Attempt to load
    session = DicomSession(":memory:")
    
    # We expect this to print an error stack trace or raise an exception to the user
    # Or result in empty rules.
    session.load_config(str(bad_config_path))
    
    # If the bug exists (and we want to support this file), this assertion fails if len=0
    # But currently it fails with len=0 because of the crash.
    # To verify the FIX, we will want len > 0.
    # To verify the BUG, we check that it failed efficiently?
    # Actually, let's just assert that it DOES load successfully (our goal state).
    
    if len(session.active_rules) == 0:
         pytest.fail("Reproduction: Failed to load legacy list-based config.")
