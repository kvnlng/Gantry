from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional
import copy

@dataclass
class GantryConfiguration:
    """
    Encapsulates the runtime configuration for a DicomSession.
    Includes machine redaction rules, PHI tags, and global settings.
    """
    rules: List[Dict[str, Any]] = field(default_factory=list)
    phi_tags: Dict[str, Any] = field(default_factory=dict)
    date_jitter: Dict[str, int] = field(default_factory=lambda: {"min_days": -365, "max_days": -1})
    remove_private_tags: bool = True

    def add_rule(self, serial_number: str, manufacturer: str = "Unknown", model: str = "Unknown", zones: List[Any] = None) -> None:
        """
        Adds a new machine redaction rule.
        Overrides any existing rule for the same serial number.
        """
        # Remove existing if any
        self.delete_rule(serial_number)
        
        new_rule = {
            "serial_number": serial_number,
            "manufacturer": manufacturer,
            "model_name": model,
            "redaction_zones": zones or []
        }
        self.rules.append(new_rule)

    def update_rule(self, serial_number: str, updates: Dict[str, Any]) -> None:
        """
        Updates an existing rule identified by serial_number.
        """
        rule = self.get_rule(serial_number)
        if not rule:
            raise ValueError(f"No rule found for serial number '{serial_number}'")
        
        # Prevent changing the serial number via update to avoid identity mismatch logic
        if "serial_number" in updates and updates["serial_number"] != serial_number:
            raise ValueError("Values for 'serial_number' cannot be changed via update_rule.")
            
        rule.update(updates)

    def delete_rule(self, serial_number: str) -> bool:
        """
        Removes a rule by serial number. Returns True if found and removed.
        """
        initial_len = len(self.rules)
        self.rules = [r for r in self.rules if r.get("serial_number") != serial_number]
        return len(self.rules) < initial_len

    def set_phi_tag(self, tag: str, action: str, replacement: str = None) -> None:
        """
        Sets or updates a PHI tag policy.
        Action examples: 'KEEP', 'REMOVE', 'REPLACE', 'JITTER', 'EMPTY'
        """
        tag = tag.upper()
        # Simple string format storage check? 
        # config_manager uses dicts or strings. We standardize on dict for API set?
        # Or store as the system expects. System expects:
        # { "0010,0010": "Patient Name" } OR { "0010,0010": {"name": "...", "action": "..."} }
        
        # If the backend supports structured tags, use that.
        # Based on config_manager.py line 523, it supports structured.
        
        val = {
            "name": "Custom Tag", # We might not know the name easily without lookup
            "action": action
        }
        if replacement:
            val["replacement"] = replacement
            
        self.phi_tags[tag] = val

    def get_rule(self, serial_number: str) -> Optional[Dict[str, Any]]:
        """Retrieves a specific rule dictionary (reference)."""
        for r in self.rules:
            if r.get("serial_number") == serial_number:
                return r
        return None
