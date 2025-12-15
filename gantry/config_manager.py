import json
import os
from typing import List, Dict, Any
from .logger import get_logger


class ConfigLoader:
    @staticmethod
    def load_unified_config(filepath: str) -> tuple[Dict[str, str], List[Dict[str, Any]], Dict[str, Any], bool]:
        """
        Parses the unified JSON config (v2.0).
        Returns: (phi_tags, machine_rules, date_jitter_config, remove_private_tags)
        """
        data = ConfigLoader._load_json(filepath)
        
        data = ConfigLoader._load_json(filepath)
        
        # Legacy Support: List of rules
        if isinstance(data, list):
            get_logger().info("Legacy config detected (list format). Treating as machine rules.")
            machine_rules = data
            phi_tags = {}
            date_jitter = {"min_days": -365, "max_days": -1}
            remove_private_tags = True
        else:
            phi_tags = data.get("phi_tags", {})
            machine_rules = data.get("machines", [])
            date_jitter = data.get("date_jitter", {"min_days": -365, "max_days": -1})
            remove_private_tags = data.get("remove_private_tags", True) # Default to True for now?
        
        # Validate machines
        for i, rule in enumerate(machine_rules):
            ConfigLoader._validate_rule(rule, i)
            
        return phi_tags, machine_rules, date_jitter, remove_private_tags

    @staticmethod
    def load_redaction_rules(filepath: str) -> List[Dict[str, Any]]:
        """
        Legacy/Convenience support. 
        If v2 file, extracts 'machines'. If v1 file (list or dict), tries to parse.
        """
        data = ConfigLoader._load_json(filepath)
        
        rules = []
        if isinstance(data, list):
            rules = data # Old v0 list?
        elif "machines" in data:
            rules = data["machines"] # v1 or v2
        else:
             get_logger().warning("Config Warning: Could not find 'machines' list.")

        for i, rule in enumerate(rules):
            ConfigLoader._validate_rule(rule, i)
            
        return rules

    @staticmethod
    def load_phi_config(filepath: str = None) -> Dict[str, str]:
        """
        Legacy/Convenience support.
        """
        if filepath:
            data = ConfigLoader._load_json(filepath)
            # Support v2 unified file used as simple PHI config
            if "phi_tags" in data:
                return data["phi_tags"]
            return data.get("phi_tags", data) # Fallback to assumes root dict is tags if no key
        else:
            # Load default from package resources
            base = os.path.dirname(os.path.abspath(__file__))
            filepath = os.path.join(base, "resources", "phi_tags.json")
            if os.path.exists(filepath):
                 data = ConfigLoader._load_json(filepath)
            else:
                 data = {}
            return data.get("phi_tags", data)

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