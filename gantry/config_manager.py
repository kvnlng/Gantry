import json
import os
from typing import List, Dict, Any


class ConfigValidationError(Exception):
    """Raised when configuration validation fails."""
    pass


class ROIValidator:
    """
    Validator for Region of Interest (ROI).

    ROI represents a rectangular region in format:
    [row_start, row_end, col_start, col_end]
    """

    EXPECTED_LENGTH = 4

    @classmethod
    def validate(cls, roi: Any, context: str = "") -> List[str]:
        """
        Validates ROI coordinates.

        Args:
            roi: ROI value to validate. Expected list of 4 numbers.
            context: Context for error messages (e.g., path to ROI).

        Returns:
            List of error descriptions. Empty list means valid.
        """
        errors: List[str] = []
        prefix = f"{context}: " if context else ""

        if not isinstance(roi, list):
            errors.append(f"{prefix}ROI must be a list, got {type(roi).__name__}")
            return errors

        if len(roi) != cls.EXPECTED_LENGTH:
            errors.append(
                f"{prefix}ROI must have exactly 4 values "
                f"[row_start, row_end, col_start, col_end], got {len(roi)}"
            )
            return errors

        for i, value in enumerate(roi):
            if not isinstance(value, (int, float)):
                errors.append(f"{prefix}ROI[{i}] must be a number, got {type(value).__name__}")

        if errors:
            return errors

        r1, r2, c1, c2 = roi

        if r1 < 0:
            errors.append(f"{prefix}row_start ({r1}) cannot be negative")
        if r2 < 0:
            errors.append(f"{prefix}row_end ({r2}) cannot be negative")
        if c1 < 0:
            errors.append(f"{prefix}col_start ({c1}) cannot be negative")
        if c2 < 0:
            errors.append(f"{prefix}col_end ({c2}) cannot be negative")

        if r2 <= r1:
            errors.append(f"{prefix}row_end ({r2}) must be greater than row_start ({r1})")
        if c2 <= c1:
            errors.append(f"{prefix}col_end ({c2}) must be greater than col_start ({c1})")

        return errors


class ConfigSchemaValidator:
    """
    Schema validator for configuration files.

    Validates structure and values of machine configurations and redaction zones.
    """

    @classmethod
    def validate(cls, data: Dict[str, Any]) -> List[str]:
        """
        Validates complete configuration structure.

        Args:
            data: Configuration data dictionary.

        Returns:
            List of error descriptions. Empty list means valid.
        """
        errors: List[str] = []

        if "machines" not in data:
            return errors

        machines = data["machines"]

        if not isinstance(machines, list):
            errors.append("'machines' must be a list")
            return errors

        for idx, machine in enumerate(machines):
            machine_errors = cls._validate_machine(machine, idx)
            errors.extend(machine_errors)

        return errors

    @classmethod
    def _validate_machine(cls, machine: Any, index: int) -> List[str]:
        """
        Validates a single machine entry.

        Args:
            machine: Machine data to validate.
            index: Machine index in the list (for error messages).

        Returns:
            List of error descriptions.
        """
        errors: List[str] = []
        context = f"machines[{index}]"

        if not isinstance(machine, dict):
            errors.append(f"{context}: must be an object, got {type(machine).__name__}")
            return errors

        if "serial_number" not in machine:
            errors.append(f"{context}: missing required field 'serial_number'")
        elif not isinstance(machine["serial_number"], str):
            errors.append(
                f"{context}.serial_number: must be a string, "
                f"got {type(machine['serial_number']).__name__}"
            )

        if "redaction_zones" in machine:
            zones = machine["redaction_zones"]
            if not isinstance(zones, list):
                errors.append(
                    f"{context}.redaction_zones: must be a list, "
                    f"got {type(zones).__name__}"
                )
            else:
                for zone_idx, zone in enumerate(zones):
                    zone_errors = cls._validate_zone(zone, context, zone_idx)
                    errors.extend(zone_errors)

        return errors

    @classmethod
    def _validate_zone(cls, zone: Any, machine_context: str, zone_index: int) -> List[str]:
        """
        Validates a redaction zone.

        Args:
            zone: Zone data to validate.
            machine_context: Machine context for error messages.
            zone_index: Zone index in the list.

        Returns:
            List of error descriptions.
        """
        errors: List[str] = []
        context = f"{machine_context}.redaction_zones[{zone_index}]"

        if not isinstance(zone, dict):
            errors.append(f"{context}: must be an object, got {type(zone).__name__}")
            return errors

        if "roi" not in zone:
            errors.append(f"{context}: missing required field 'roi'")
            return errors

        roi_errors = ROIValidator.validate(zone["roi"], f"{context}.roi")
        errors.extend(roi_errors)

        return errors


class ConfigLoader:
    """
    Configuration file loader for machine rules.

    Loads JSON configuration files and validates their structure and values.
    """

    @staticmethod
    def load_rules(filepath: str, validate: bool = True) -> List[Dict[str, Any]]:
        """
        Parses JSON configuration and returns list of machine rules.

        Args:
            filepath: Path to JSON configuration file.
            validate: If True, performs schema and ROI validation.
                     If False, loads without validation.

        Returns:
            List of dictionaries with machine rules.

        Raises:
            FileNotFoundError: If configuration file not found.
            ValueError: If file contains invalid JSON.
            ConfigValidationError: If configuration fails schema validation.
        """
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"Configuration file not found: {filepath}")

        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)

            if "machines" not in data:
                print("⚠️ Config warning: 'machines' key missing.")
                return []

            if validate:
                errors = ConfigSchemaValidator.validate(data)
                if errors:
                    error_msg = "Configuration validation failed:\n" + "\n".join(
                        f"  - {e}" for e in errors
                    )
                    raise ConfigValidationError(error_msg)

            return data["machines"]

        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON format in {filepath}: {e}")
