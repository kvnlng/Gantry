import yaml
import os
import logging
import copy
from typing import Dict, Any, Union, List

from .profiles import PRIVACY_PROFILES

CONFIG_VERSION = "2.0"

def get_logger():
    return logging.getLogger("gantry")

def load_unified_config(path: str) -> Dict[str, Any]:
    """
    Loads the unified configuration file (YAML).
    Supports legacy list-based config (machine rules only) and new dict-based config.
    Merges 'privacy_profile' if specified.
    """
    if not (path.endswith('.yaml') or path.endswith('.yml')):
        raise ValueError("Configuration file must be a YAML file (.yaml or .yml)")

    with open(path, "r") as f:
        data = yaml.safe_load(f)
        
    # Handle Standard Config
    config = data
    
    # Merge Privacy Profile
    if "privacy_profile" in config:
        profile_name = config["privacy_profile"]
        
        profile_rules = {}
        
        # 1. Check Built-in Profiles
        if profile_name in PRIVACY_PROFILES:
            profile_rules = copy.deepcopy(PRIVACY_PROFILES[profile_name])
            get_logger().info(f"Loaded built-in privacy profile '{profile_name}' with {len(profile_rules)} rules.")
            
        # 2. Check External File (Custom Profile)
        elif os.path.exists(profile_name):
            try:
                # We reuse load_phi_config logic to parse just the tags
                profile_rules = ConfigLoader.load_phi_config(profile_name)
                get_logger().info(f"Loaded custom privacy profile from '{profile_name}' with {len(profile_rules)} rules.")
            except Exception as e:
                get_logger().error(f"Failed to load custom profile '{profile_name}': {e}")
                
        else:
            get_logger().warning(f"Unknown privacy profile reference '{profile_name}' (not a built-in or file). Ignoring.")

        if profile_rules:
            # User rules override profile rules
            user_rules = config.get("phi_tags", {})
            profile_rules.update(user_rules)
            config["phi_tags"] = profile_rules
            
    return config



class ConfigLoader:
    @staticmethod
    def load_unified_config(filepath: str) -> tuple[Dict[str, Any], List[Dict[str, Any]], Dict[str, Any], bool]:
        """
        Parses the unified YAML config (v2.0).
        Returns: (phi_tags, machine_rules, date_jitter_config, remove_private_tags)
        """
        # Call the top-level loader which handles YAML, Legacy List, and Privacy Profiles
        data = load_unified_config(filepath)
        
        phi_tags = data.get("phi_tags", {})
        # Support 'machines' (v2) or 'machine_rules' (legacy internal)
        machine_rules = data.get("machines", data.get("machine_rules", []))
        
        # Date Jitter Normalization
        dj = data.get("date_jitter", {"min_days": -365, "max_days": -1})
        if isinstance(dj, int):
             # Legacy support or user provided int. Convert to fixed shift.
             date_jitter_config = {"min_days": dj, "max_days": dj}
        else:
             date_jitter_config = dj
        
        remove_private_tags = data.get("remove_private_tags", True)
        
        # Validate machines
        for i, rule in enumerate(machine_rules):
            ConfigLoader._validate_rule(rule, i)
            
        return phi_tags, machine_rules, date_jitter_config, remove_private_tags

    @staticmethod
    def load_redaction_rules(filepath: str) -> List[Dict[str, Any]]:
        """
        Legacy/Convenience support. 
        If v2 file, extracts 'machines'. If v1 file (list or dict), tries to parse.
        """
        data = ConfigLoader._load_yaml(filepath)
        
        rules = []
        rules = []
        if "machines" in data:
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
            data = ConfigLoader._load_yaml(filepath)
            
            # Support v2 unified file used as simple PHI config
            if "phi_tags" in data:
                return data["phi_tags"]
            return data.get("phi_tags", data) # Fallback to assumes root dict is tags if no key
        else:
            # Load default from package resources
            base = os.path.dirname(os.path.abspath(__file__))
            filepath = os.path.join(base, "resources", "phi_tags.json")
            if os.path.exists(filepath):
                 # Resource is likely still JSON for internal defaults unless we change it too. 
                 # But sticking to JSON for internal resources is fine, OR we change helper to handle both?
                 # User asked to drop JSON support for *config files*. 
                 # Let's support JSON just for internal resources via simple json load if yaml fails or extension check?
                 # Actually, clearer to migrate the resource to YAML too?
                 # Or just use json.load here explicitly since it's internal.
                 import json
                 with open(filepath, 'r') as f:
                     return json.load(f).get("phi_tags", {})
            return {}

    @staticmethod
    def _load_yaml(filepath: str) -> Dict[str, Any]:
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"Configuration file not found: {filepath}")

        try:
            with open(filepath, 'r') as f:
                return yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML format in {filepath}: {e}")

    @staticmethod
    def _validate_rule(rule: Dict[str, Any], index: int):
        sn = rule.get("serial_number")
        if not sn:
            raise ValueError(f"Rule #{index}: Missing 'serial_number'.")

        zones = rule.get("redaction_zones", [])
        if not isinstance(zones, list):
            raise ValueError(f"Rule #{index} ({sn}): 'redaction_zones' must be a list.")

        for z_idx, zone in enumerate(zones):
            if isinstance(zone, list):
                roi = zone
            elif isinstance(zone, dict):
                roi = zone.get("roi")
            else:
                raise ValueError(f"Rule #{index} ({sn}), Zone #{z_idx}: Invalid zone format (must be list or dict).")
            
            if not roi or not isinstance(roi, list) or len(roi) != 4:
                raise ValueError(f"Rule #{index} ({sn}), Zone #{z_idx}: ROI must be a list of 4 integers.")
            
            r1, r2, c1, c2 = roi
            if any(x < 0 for x in roi):
                 raise ValueError(f"Rule #{index} ({sn}), Zone #{z_idx}: ROI values must be non-negative.")
            
            if r1 > r2 or c1 > c2:
                 raise ValueError(f"Rule #{index} ({sn}), Zone #{z_idx}: Invalid ROI logic (Start > End).")