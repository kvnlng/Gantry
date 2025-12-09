import json
import os
from typing import List, Dict, Any


class ConfigLoader:
    @staticmethod
    def load_rules(filepath: str) -> List[Dict[str, Any]]:
        """
        Parses the JSON config and returns a list of machine rules.
        """
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"Configuration file not found: {filepath}")

        try:
            with open(filepath, 'r') as f:
                data = json.load(f)

            # Basic Validation
            if "machines" not in data:
                print("⚠️ Config warning: 'machines' key missing.")
                return []

            return data["machines"]

        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON format in {filepath}: {e}")