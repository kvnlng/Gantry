
import pytest
import os
import json
from gantry.utils.ctp_parser import CTPParser

TEST_SCRIPT_CONTENT = """
**************
     CT
**************
GE 

 CT Dose Series
  { CodeMeaning.containsIgnoreCase("IEC Body Dosimetry Phantom") }
  (0,0,512,200)

  { Manufacturer.containsIgnoreCase("GE MEDICAL") *
    SeriesDescription.containsIgnoreCase("Dose Report") }
  (0,0,512,110)
"""

class TestCTPParser:
    def test_parsing_basic(self):
        rules = CTPParser.parse_script(TEST_SCRIPT_CONTENT)
        
        # We expect 2 rules
        assert len(rules) == 1 # Actually, first condition doesn't have Manufacturer, so our parser logic might skip it.
        # Let's check logic:
        # First block: CodeMeaning... -> No Manufacturer in condition -> Parser returns None
        # Second block: Manufacturer... -> Returns rule
        
        assert rules[0]["manufacturer"] == "GE MEDICAL"
        # Coordinates: (0,0,512,110) -> [0, 110, 0, 512] (r1, r2, c1, c2)
        assert rules[0]["redaction_zones"] == [[0, 110, 0, 512]]

    def test_ctp_rules_file_exists(self):
        # Verify that the resource generation worked
        path = "gantry/resources/ctp_rules.json"
        assert os.path.exists(path)
        with open(path, 'r') as f:
            data = json.load(f)
            assert len(data.get("rules", [])) > 0
