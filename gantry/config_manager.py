import json
import os
from typing import List, Dict, Any


class ConfigLoader:
    @staticmethod
    @staticmethod
    def load_redaction_rules(filepath: str) -> List[Dict[str, Any]]:
        """
        Parses the JSON config and returns a list of machine rules.
        """
        data = ConfigLoader._load_json(filepath)
        
        # Basic Validation
        if "machines" not in data:
            print("⚠️ Config warning: 'machines' key missing.")
            return []
        
        rules = data["machines"]
        for i, rule in enumerate(rules):
            ConfigLoader._validate_rule(rule, i)
            
        return rules

    @staticmethod
    def load_phi_config(filepath: str = None) -> Dict[str, str]:
        """
        Loads PHI tags from a JSON file.
        If filepath is None, loads the default 'resources/phi_tags.json' from the package.
        """
        if filepath:
            data = ConfigLoader._load_json(filepath)
        else:
            # Load default from package resources
            base = os.path.dirname(os.path.abspath(__file__))
            filepath = os.path.join(base, "resources", "phi_tags.json")
            if os.path.exists(filepath):
                 data = ConfigLoader._load_json(filepath)
            else:
                 data = {}

        return data.get("phi_tags", {})

    @staticmethod
    def _load_json(filepath: str) -> Dict[str, Any]:
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"Configuration file not found: {filepath}")

        try:
            with open(filepath, 'r') as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON format in {filepath}: {e}")

    @staticmethod
    def _validate_rule(rule: Dict[str, Any], index: int):
        sn = rule.get("serial_number")
        if not sn:
            raise ValueError(f"Rule #{index}: Missing 'serial_number'.")

        zones = rule.get("redaction_zones", [])
        if not isinstance(zones, list):
            raise ValueError(f"Rule #{index} ({sn}): 'redaction_zones' must be a list.")

        for z_idx, zone in enumerate(zones):
            roi = zone.get("roi")
            if not roi or not isinstance(roi, list) or len(roi) != 4:
                raise ValueError(f"Rule #{index} ({sn}), Zone #{z_idx}: ROI must be a list of 4 integers.")
            
            r1, r2, c1, c2 = roi
            if any(x < 0 for x in roi):
                 raise ValueError(f"Rule #{index} ({sn}), Zone #{z_idx}: ROI values must be non-negative.")
            
            if r1 > r2 or c1 > c2:
                 raise ValueError(f"Rule #{index} ({sn}), Zone #{z_idx}: Invalid ROI logic (Start > End).")